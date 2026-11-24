// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/md5" //nolint:gosec // GCS reports MD5
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine"
)

// fakeGoogle serves the endpoints the agent's store can talk to: STS, the
// metadata server and the GCS (or emulator) upload API. It counts token
// requests, and records the Authorization header of each upload.
type fakeGoogle struct {
	mu               sync.Mutex
	exchanges, metas int
	uploadTokens     []string
	uploadAuth       []string
	refuseSTS        bool
}

func (f *fakeGoogle) server(t *testing.T) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/token":
			f.exchanges++
			if f.refuseSTS {
				http.Error(w, `{"error":"invalid_target","error_description":"wrong audience"}`, http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "sts-token", "expires_in": 3600})
		case r.Method == http.MethodGet && (r.URL.Path == "/token" || r.URL.Path == "/computeMetadata/v1/instance/service-accounts/default/token"):
			f.metas++
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "node-token", "expires_in": 3600})
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/"):
			f.uploadAuth = append(f.uploadAuth, r.Header.Get("Authorization"))
			f.uploadTokens = append(f.uploadTokens, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
			data, _ := io.ReadAll(r.Body)
			sum := md5.Sum(data) //nolint:gosec
			_ = json.NewEncoder(w).Encode(map[string]string{"size": fmt.Sprint(len(data)), "md5Hash": base64.StdEncoding.EncodeToString(sum[:])})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.Error(w, "unexpected", http.StatusBadRequest)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// fakeInternet stands in for http.DefaultTransport, which the store's
// clients use: it sends requests for Google's STS, metadata server and GCS
// to a fakeGoogle, and requests for the fake itself on, and refuses, and
// records, any other.
type fakeInternet struct {
	next    http.RoundTripper
	fake    string // the fake's host:port
	mu      sync.Mutex
	refused []string
}

func (n *fakeInternet) RoundTrip(r *http.Request) (*http.Response, error) {
	switch r.URL.Host {
	case n.fake:
	case "sts.googleapis.com", "metadata.google.internal", "storage.googleapis.com":
		r = r.Clone(r.Context())
		r.URL.Scheme, r.URL.Host = "http", n.fake
	default:
		n.mu.Lock()
		n.refused = append(n.refused, r.URL.String())
		n.mu.Unlock()
		return nil, fmt.Errorf("refused %s", r.URL)
	}
	return n.next.RoundTrip(r)
}

// useFakeInternet routes the process's default transport to srv for the
// test.
func useFakeInternet(t *testing.T, srv *httptest.Server) *fakeInternet {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	saved := http.DefaultTransport
	n := &fakeInternet{next: saved, fake: u.Host}
	http.DefaultTransport = n
	t.Cleanup(func() { http.DefaultTransport = saved })
	return n
}

// pointAt sends store's requests, and its STS source's if it has one, to
// srv.
func pointAt(store *psengine.GCSStore, srv *httptest.Server) {
	store.HTTP, store.UploadBase, store.APIBase, store.TokenURL = srv.Client(), srv.URL+"/upload", srv.URL+"/api", srv.URL+"/token"
	if sts, ok := store.Tokens.(*psengine.STSTokenSource); ok {
		sts.Endpoint, sts.HTTP = srv.URL+"/v1/token", srv.Client()
	}
}

// gcsFlagsFrom parses no arguments, so every flag comes from env.
func gcsFlagsFrom(t *testing.T, env map[string]string) *gcsFlags {
	t.Helper()
	fs := flag.NewFlagSet("agent", flag.ContinueOnError)
	f := bindGCSFlags(fs, func(key string) string { return env[key] })
	if err := fs.Parse(nil); err != nil {
		t.Fatal(err)
	}
	return f
}

// The agent's store (#244): with workload-identity it uploads with the
// token STS exchanged for its own service account and never asks the
// metadata server, which answers as the node; with node it uses the
// metadata server; with STORAGE_EMULATOR_HOST it has no credentials and
// skips the workload-identity checks; and a configuration error gives no
// store at all.
func TestNewAgentStore(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"), 0o400); err != nil {
		t.Fatal(err)
	}
	const (
		pool     = "example-project.svc.id.goog"
		provider = "https://container.googleapis.com/v1/projects/example-project/locations/us-central1/clusters/example-cluster"
	)
	ctx := context.Background()

	t.Run("workload-identity", func(t *testing.T) {
		google := &fakeGoogle{}
		srv := google.server(t)
		store, err := newAgentStore(gcsFlagsFrom(t, map[string]string{"WORKLOAD_POOL": pool, "IDENTITY_PROVIDER": provider, "GCS_TOKEN_FILE": token}), func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		sts, ok := store.Tokens.(*psengine.STSTokenSource)
		if !ok || sts.WorkloadPool != pool || sts.IdentityProvider != provider || sts.TokenFile != token {
			t.Fatalf("store.Tokens = %#v, want an STS source for the flags", store.Tokens)
		}
		pointAt(store, srv)
		if err := warmAtStartup(ctx, store); err != nil {
			t.Fatal(err)
		}
		if _, err := psengine.VerifiedPut(ctx, store, "bkt", "o", strings.NewReader("data")); err != nil {
			t.Fatal(err)
		}
		if google.exchanges != 1 || google.metas != 0 || len(google.uploadTokens) != 1 || google.uploadTokens[0] != "sts-token" {
			t.Fatalf("%d exchanges, %d metadata requests, uploads with %v; want one exchange, its token, and never the metadata server",
				google.exchanges, google.metas, google.uploadTokens)
		}
	})

	t.Run("node", func(t *testing.T) {
		google := &fakeGoogle{}
		srv := google.server(t)
		store, err := newAgentStore(gcsFlagsFrom(t, map[string]string{"GCS_CREDENTIALS": "node"}), func(string) string { return "" })
		if err != nil || store.Tokens != nil {
			t.Fatalf("newAgentStore = %#v, %v; want the metadata server", store, err)
		}
		pointAt(store, srv)
		if _, err := psengine.VerifiedPut(ctx, store, "bkt", "o", strings.NewReader("data")); err != nil {
			t.Fatal(err)
		}
		if google.exchanges != 0 || google.metas != 1 || google.uploadTokens[0] != "node-token" {
			t.Fatalf("%d exchanges, %d metadata requests, uploads with %v; want the node's token", google.exchanges, google.metas, google.uploadTokens)
		}
	})

	t.Run("STS refusing at startup only logs", func(t *testing.T) {
		google := &fakeGoogle{refuseSTS: true}
		srv := google.server(t)
		store, err := newAgentStore(gcsFlagsFrom(t, map[string]string{"WORKLOAD_POOL": pool, "IDENTITY_PROVIDER": provider, "GCS_TOKEN_FILE": token}), func(string) string { return "" })
		if err != nil {
			t.Fatal(err)
		}
		pointAt(store, srv)
		if err := warmAtStartup(ctx, store); err == nil || !strings.Contains(err.Error(), "invalid_target") {
			t.Fatalf("warmAtStartup = %v, want STS's refusal", err)
		}
		if google.metas != 0 {
			t.Fatalf("the metadata server was asked %d times after STS refused", google.metas)
		}
	})

	t.Run("a GCS emulator: no credentials, no workload-identity checks", func(t *testing.T) {
		google := &fakeGoogle{}
		srv := google.server(t)
		internet := useFakeInternet(t, srv)
		host := strings.TrimPrefix(srv.URL, "http://")
		// host:port, as the client libraries take it, and a URL.
		for _, emulator := range []string{host, srv.URL + "/"} {
			env := map[string]string{"STORAGE_EMULATOR_HOST": emulator,
				"WORKLOAD_POOL": "<YOUR_WORKLOAD_POOL>", "IDENTITY_PROVIDER": "<YOUR_IDENTITY_PROVIDER>"}
			store, err := newAgentStore(gcsFlagsFrom(t, env), func(key string) string { return env[key] })
			if err != nil {
				t.Fatalf("newAgentStore with %s=%s = %v, want the checks skipped", psengine.StorageEmulatorHostEnv, emulator, err)
			}
			if store.Tokens != nil || store.TokenURL != "" || store.UploadBase != srv.URL+"/upload/storage/v1" || store.APIBase != srv.URL+"/storage/v1" {
				t.Fatalf("store = %+v; want the emulator's endpoints and no credentials", store)
			}
			if err := warmAtStartup(ctx, store); err != nil {
				t.Fatalf("warmAtStartup without credentials = %v", err)
			}
			if _, err := psengine.VerifiedPut(ctx, store, "bkt", "o", strings.NewReader("data")); err != nil {
				t.Fatal(err)
			}
		}
		google.mu.Lock()
		defer google.mu.Unlock()
		if len(google.uploadAuth) != 2 || google.uploadAuth[0] != "" || google.uploadAuth[1] != "" {
			t.Fatalf("uploads carried Authorization %q, want two uploads with none", google.uploadAuth)
		}
		if google.exchanges != 0 || google.metas != 0 || len(internet.refused) != 0 {
			t.Fatalf("%d exchanges, %d metadata requests, refused %v; want no request for credentials and nothing else",
				google.exchanges, google.metas, internet.refused)
		}
	})

	t.Run("a configuration error gives no store", func(t *testing.T) {
		store, err := newAgentStore(gcsFlagsFrom(t, map[string]string{"WORKLOAD_POOL": "<YOUR_WORKLOAD_POOL>", "IDENTITY_PROVIDER": provider, "GCS_TOKEN_FILE": token}), func(string) string { return "" })
		if err == nil || store != nil {
			t.Fatalf("newAgentStore = %#v, %v; want an error and no store", store, err)
		}
	})
}

// setUpAgentStore gets the agent's first token in the background, from the
// production endpoints (routed to a fake here): STS with workload-identity,
// the metadata server with node, nobody with an emulator. A store it
// returns then uploads with that token.
func TestSetUpAgentStoreWarmsAtStartup(t *testing.T) {
	token := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(token, []byte("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln"), 0o400); err != nil {
		t.Fatal(err)
	}
	wi := map[string]string{"WORKLOAD_POOL": "example-project.svc.id.goog",
		"IDENTITY_PROVIDER": "https://container.googleapis.com/v1/projects/example-project/locations/us-central1/clusters/example-cluster",
		"GCS_TOKEN_FILE":    token}
	for _, tc := range []struct {
		name                     string
		env                      map[string]string
		wantExchanges, wantMetas int
		wantUploadAuthorization  string
		emulator                 bool
	}{
		{name: "workload-identity", env: wi, wantExchanges: 1, wantUploadAuthorization: "Bearer sts-token"},
		{name: "node", env: map[string]string{"GCS_CREDENTIALS": "node"}, wantMetas: 1, wantUploadAuthorization: "Bearer node-token"},
		{name: "emulator", env: map[string]string{}, emulator: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			google := &fakeGoogle{}
			srv := google.server(t)
			internet := useFakeInternet(t, srv)
			if tc.emulator {
				tc.env[psengine.StorageEmulatorHostEnv] = strings.TrimPrefix(srv.URL, "http://")
			}
			store, warmed, err := setUpAgentStore(context.Background(), gcsFlagsFrom(t, tc.env), func(key string) string { return tc.env[key] })
			if err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-warmed:
				if err != nil {
					t.Fatalf("the startup warm = %v", err)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("no startup warm")
			}
			google.mu.Lock()
			exchanges, metas := google.exchanges, google.metas
			google.mu.Unlock()
			if exchanges != tc.wantExchanges || metas != tc.wantMetas {
				t.Fatalf("at startup: %d exchanges, %d metadata requests; want %d and %d", exchanges, metas, tc.wantExchanges, tc.wantMetas)
			}
			if _, err := psengine.VerifiedPut(context.Background(), store, "bkt", "o", strings.NewReader("data")); err != nil {
				t.Fatal(err)
			}
			google.mu.Lock()
			defer google.mu.Unlock()
			if len(google.uploadAuth) != 1 || google.uploadAuth[0] != tc.wantUploadAuthorization ||
				google.exchanges != tc.wantExchanges || google.metas != tc.wantMetas || len(internet.refused) != 0 {
				t.Fatalf("upload with Authorization %q after %d exchanges and %d metadata requests (refused %v); want %q with the startup token",
					google.uploadAuth, google.exchanges, google.metas, internet.refused, tc.wantUploadAuthorization)
			}
		})
	}
}

// The bucket credential flags (#244): workload-identity, the default, needs
// a workload pool, an identity provider and a readable token file, none of
// them a placeholder, and never falls back to the node's service account;
// node needs none of them. Each flag falls back to its environment
// variable.
func TestGCSFlags(t *testing.T) {
	dir := t.TempDir()
	token := filepath.Join(dir, "token")
	if err := os.WriteFile(token, []byte("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.c2ln\n"), 0o400); err != nil {
		t.Fatal(err)
	}
	empty := filepath.Join(dir, "empty")
	if err := os.WriteFile(empty, nil, 0o400); err != nil {
		t.Fatal(err)
	}
	const (
		pool     = "example-project.svc.id.goog"
		provider = "https://container.googleapis.com/v1/projects/example-project/locations/us-central1/clusters/example-cluster"
	)
	valid := map[string]string{"WORKLOAD_POOL": pool, "IDENTITY_PROVIDER": provider, "GCS_TOKEN_FILE": token}
	with := func(kv ...string) map[string]string {
		env := map[string]string{}
		for k, v := range valid {
			env[k] = v
		}
		for i := 0; i < len(kv); i += 2 {
			env[kv[i]] = kv[i+1]
		}
		return env
	}

	for _, tc := range []struct {
		name    string
		args    []string
		env     map[string]string
		wantErr string // "" means no error
		wantSTS bool
	}{
		{name: "workload identity by default, from the environment", env: valid, wantSTS: true},
		{name: "workload identity from flags", args: []string{"--gcs-credentials=workload-identity",
			"--workload-pool=" + pool, "--identity-provider=" + provider, "--gcs-token-file=" + token}, wantSTS: true},
		{name: "a flag wins over its environment variable", args: []string{"--workload-pool=" + pool},
			env: with("WORKLOAD_POOL", "<YOUR_WORKLOAD_POOL>"), wantSTS: true},
		{name: "no workload pool", env: with("WORKLOAD_POOL", ""), wantErr: "--workload-pool (env WORKLOAD_POOL) is empty"},
		{name: "blank workload pool", args: []string{"--workload-pool= "}, env: valid, wantErr: "--workload-pool (env WORKLOAD_POOL) is empty"},
		{name: "no identity provider", env: with("IDENTITY_PROVIDER", ""), wantErr: "--identity-provider (env IDENTITY_PROVIDER) is empty"},
		{name: "workload pool left as the placeholder", env: with("WORKLOAD_POOL", "<YOUR_WORKLOAD_POOL>"),
			wantErr: "--workload-pool (env WORKLOAD_POOL) is still a placeholder (<YOUR_WORKLOAD_POOL>)"},
		{name: "identity provider left as the placeholder", env: with("IDENTITY_PROVIDER", "<YOUR_IDENTITY_PROVIDER>"),
			wantErr: "--identity-provider (env IDENTITY_PROVIDER) is still a placeholder (<YOUR_IDENTITY_PROVIDER>)"},
		{name: "no token file", env: with("GCS_TOKEN_FILE", filepath.Join(dir, "absent")), wantErr: "no such file"},
		{name: "empty token file", env: with("GCS_TOKEN_FILE", empty), wantErr: "is empty"},
		{name: "the default token file, absent here", env: with("GCS_TOKEN_FILE", ""), wantErr: psengine.DefaultGCSTokenFile},
		{name: "node, which needs none of them", args: []string{"--gcs-credentials=node"}},
		{name: "node from the environment", env: map[string]string{"GCS_CREDENTIALS": "node", "WORKLOAD_POOL": "<YOUR_WORKLOAD_POOL>"}},
		{name: "an unknown mode", args: []string{"--gcs-credentials=gsa"}, env: valid, wantErr: `--gcs-credentials (env GCS_CREDENTIALS) is "gsa"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fs := flag.NewFlagSet("agent", flag.ContinueOnError)
			fs.SetOutput(io.Discard)
			f := bindGCSFlags(fs, func(key string) string { return tc.env[key] })
			if err := fs.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			sts, err := f.tokenSource()
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("tokenSource() = %v, want an error saying %q", err, tc.wantErr)
				}
				if sts != nil {
					t.Fatal("a token source came with the error")
				}
			case err != nil:
				t.Fatalf("tokenSource() = %v", err)
			case tc.wantSTS:
				if sts == nil || sts.WorkloadPool != pool || sts.IdentityProvider != provider || sts.TokenFile != token {
					t.Fatalf("tokenSource() = %+v, want an STS source for %s, %s and %s", sts, pool, provider, token)
				}
			default:
				if sts != nil {
					t.Fatalf("tokenSource() = %+v, want none: node uses the metadata server", sts)
				}
			}
		})
	}
}
