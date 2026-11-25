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

package psengine

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	k8syaml "sigs.k8s.io/yaml"
)

const (
	testWorkloadPool     = "example-project.svc.id.goog"
	testIdentityProvider = "https://container.googleapis.com/v1/projects/example-project/locations/us-central1/clusters/example-cluster"
	// testSubjectToken and testAccessToken stand for the secrets that must
	// never reach an error.
	testSubjectToken = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJzeXN0ZW06c2VydmljZWFjY291bnQifQ.c2lnbmF0dXJlLTE"
	testAccessToken  = "fake-tok" // what fakeGCS accepts
)

// fakeSTS is Google's STS token endpoint: it records each exchange and
// answers with respond, or with testAccessToken for an hour.
type fakeSTS struct {
	mu       sync.Mutex
	requests []map[string]string
	headers  []http.Header
	respond  func(w http.ResponseWriter, r *http.Request)
}

func (f *fakeSTS) exchanges() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeSTS) last() map[string]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests[len(f.requests)-1]
}

// newFakeSTS starts a fakeSTS and returns a source that exchanges the token
// in a file of its own at it.
func newFakeSTS(t *testing.T) (*fakeSTS, *STSTokenSource) {
	t.Helper()
	f := &fakeSTS{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/token" {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		var req map[string]string
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.requests = append(f.requests, req)
		f.headers = append(f.headers, r.Header.Clone())
		respond := f.respond
		f.mu.Unlock()
		if respond != nil {
			respond(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": testAccessToken, "issued_token_type": "urn:ietf:params:oauth:token-type:access_token",
			"token_type": "Bearer", "expires_in": 3600,
		})
	}))
	t.Cleanup(srv.Close)
	tokenFile := filepath.Join(t.TempDir(), "token")
	writeToken(t, tokenFile, testSubjectToken+"\n")
	return f, &STSTokenSource{
		TokenFile: tokenFile, WorkloadPool: testWorkloadPool, IdentityProvider: testIdentityProvider,
		Endpoint: srv.URL + "/v1/token", HTTP: srv.Client(),
	}
}

func writeToken(t *testing.T, path, token string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(token), 0o600); err != nil {
		t.Fatal(err)
	}
}

// fakeClock is a settable clock for the store and the source.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = t
}

// The exchange is the one Google's STS takes for a GKE service account
// token, field for field, and the token is refreshed 5 minutes before it
// expires, counting from before the request: the token can't expire sooner.
func TestSTSTokenSourceExchange(t *testing.T) {
	sts, src := newFakeSTS(t)
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	start := clock.now()
	src.now = clock.now
	// The request takes a minute.
	sts.respond = func(w http.ResponseWriter, _ *http.Request) {
		clock.set(start.Add(time.Minute))
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": testAccessToken, "expires_in": 3600})
	}

	tok, refreshAt, err := src.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok != testAccessToken {
		t.Fatalf("token = %q, want %q", tok, testAccessToken)
	}
	if want := start.Add(55 * time.Minute); !refreshAt.Equal(want) {
		t.Fatalf("refreshAt = %v, want %v (5 minutes before the hour from the request runs out)", refreshAt, want)
	}
	want := map[string]string{
		"grantType":          "urn:ietf:params:oauth:grant-type:token-exchange",
		"audience":           "identitynamespace:" + testWorkloadPool + ":" + testIdentityProvider,
		"scope":              "https://www.googleapis.com/auth/devstorage.read_write",
		"requestedTokenType": "urn:ietf:params:oauth:token-type:access_token",
		"subjectToken":       testSubjectToken,
		"subjectTokenType":   "urn:ietf:params:oauth:token-type:jwt",
	}
	if got := sts.last(); !reflect.DeepEqual(got, want) {
		t.Fatalf("exchange request:\n got %v\nwant %v", got, want)
	}
	if ct := sts.headers[0].Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
}

// The kubelet rotates the projected token: every exchange reads the file
// again.
func TestSTSTokenSourceRereadsTheTokenFile(t *testing.T) {
	sts, src := newFakeSTS(t)
	if _, _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	const rotated = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJyb3RhdGVkIn0.c2lnbmF0dXJlLTI"
	writeToken(t, src.TokenFile, rotated)
	if _, _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := sts.last()["subjectToken"]; got != rotated {
		t.Fatalf("the second exchange sent %q, want the rotated token", got)
	}
}

// The store exchanges once for concurrent callers, reuses the token until
// 5 minutes before it expires, then exchanges again, and never asks the
// metadata server.
func TestGCSStoreCachesSTSTokens(t *testing.T) {
	store, gcs := newFakeStore(t)
	sts, src := newFakeSTS(t)
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	start := clock.now()
	store.Tokens, store.now, src.now = src, clock.now, clock.now

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := VerifiedPut(context.Background(), store, "bkt", "o", strings.NewReader("data")); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := sts.exchanges(); n != 1 {
		t.Fatalf("%d exchanges for concurrent callers, want 1", n)
	}

	clock.set(start.Add(55*time.Minute - time.Second))
	if _, err := store.bearer(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := sts.exchanges(); n != 1 {
		t.Fatalf("%d exchanges before the refresh margin, want the cached token", n)
	}

	clock.set(start.Add(55 * time.Minute))
	if _, err := VerifiedPut(context.Background(), store, "bkt", "o", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if n := sts.exchanges(); n != 2 {
		t.Fatalf("%d exchanges once the margin is reached, want 2", n)
	}
	if gcs.tokens != 0 {
		t.Fatalf("the metadata server was asked %d times; the store must use its STS source", gcs.tokens)
	}
}

func TestRefreshAfter(t *testing.T) {
	for lifetime, want := range map[time.Duration]time.Duration{
		time.Hour:        55 * time.Minute,
		10 * time.Minute: 5 * time.Minute,
		5 * time.Minute:  150 * time.Second, // under twice the margin: halfway
		0:                0,                 // no lifetime: never reused
	} {
		if got := refreshAfter(lifetime); got != want {
			t.Errorf("refreshAfter(%v) = %v, want %v", lifetime, got, want)
		}
	}
}

// A failed exchange says why with the HTTP status and STS's error and
// error_description only, and never with the service account token or an
// access token, even when the response quotes them.
func TestSTSTokenSourceErrors(t *testing.T) {
	respondWith := func(status int, body string) func(http.ResponseWriter, *http.Request) {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		}
	}
	for _, tc := range []struct {
		name       string
		respond    func(http.ResponseWriter, *http.Request)
		tokenFile  string // written instead of testSubjectToken, when set
		want       []string
		noExchange bool
	}{{
		name: "refused, with STS's error JSON",
		respond: respondWith(http.StatusBadRequest, `{"error":"invalid_grant","error_description":"The audience in ID Token [`+
			testSubjectToken+`] does not match the expected audience."}`),
		want: []string{"HTTP 400", `"invalid_grant"`, "does not match the expected audience", "[service account token]"},
	}, {
		name:    "refused, with a token-shaped value in the description",
		respond: respondWith(http.StatusUnauthorized, `{"error":"invalid_request","error_description":"bad token eyJhbGciOiJub25lIn0.eyJzdWIiOiJ4In0."}`),
		want:    []string{"HTTP 401", `"invalid_request"`, "bad token [token]"},
	}, {
		name:    "refused, without JSON",
		respond: respondWith(http.StatusBadGateway, "<html>upstream echoed "+testSubjectToken+"</html>"),
		want:    []string{"HTTP 502"},
	}, {
		name:    "malformed JSON",
		respond: respondWith(http.StatusOK, `{"access_token":"`+testAccessToken+`","expires_in":`),
		want:    []string{"HTTP 200", "isn't the JSON that STS returns"},
	}, {
		name:    "no access_token",
		respond: respondWith(http.StatusOK, `{"token_type":"Bearer","expires_in":3600}`),
		want:    []string{"no access_token"},
	}, {
		name:       "empty token file",
		tokenFile:  " \n",
		want:       []string{"is empty"},
		noExchange: true,
	}} {
		t.Run(tc.name, func(t *testing.T) {
			sts, src := newFakeSTS(t)
			sts.respond = tc.respond
			if tc.tokenFile != "" {
				writeToken(t, src.TokenFile, tc.tokenFile)
			}
			// A TokenURL that is never asked: the store uses its source.
			store := &GCSStore{Tokens: src, TokenURL: "http://metadata.invalid/token"}
			_, err := store.bearer(context.Background())
			if err == nil {
				t.Fatal("want an error")
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q doesn't say %q", err, w)
				}
			}
			for _, secret := range []string{testSubjectToken, testAccessToken, "eyJhbGciOiJub25lIn0"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error %q holds a secret (%s)", err, secret)
				}
			}
			if got := sts.exchanges(); tc.noExchange && got != 0 {
				t.Errorf("%d exchanges, want none", got)
			}
			if store.token != "" {
				t.Errorf("the store kept a token after a failed exchange")
			}
		})
	}
	t.Run("missing token file", func(t *testing.T) {
		sts, src := newFakeSTS(t)
		src.TokenFile = filepath.Join(t.TempDir(), "absent")
		if _, _, err := src.Token(context.Background()); err == nil || !strings.Contains(err.Error(), "absent") || sts.exchanges() != 0 {
			t.Fatalf("Token = %v after %d exchanges, want an error naming the file and no exchange", err, sts.exchanges())
		}
	})
}

// Each exchange has its own short deadline: a hung STS can't hold the
// store's mutex, and with it every bucket call, for the upload timeout.
func TestSTSTokenSourceTimesOut(t *testing.T) {
	saved := stsTimeout
	stsTimeout = 50 * time.Millisecond
	t.Cleanup(func() { stsTimeout = saved })
	sts, src := newFakeSTS(t)
	// The handler hangs until the client gives up, or the test ends.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	sts.respond = func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := src.Token(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Fatalf("Token = %v, want a deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exchange wasn't cut off by its deadline")
	}
}

// deployObjects reads deploy.yaml's objects by kind and name.
func deployObjects(t *testing.T) map[string][]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	objects := map[string][]byte{}
	for _, doc := range strings.Split(string(raw), "\n---") {
		var meta struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := k8syaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatal(err)
		}
		objects[meta.Kind+"/"+meta.Metadata.Name] = []byte(doc)
	}
	return objects
}

// projectedTokenAudiences returns the audiences of the service account tokens
// that pod spec projects, by volume name.
func projectedTokenAudiences(spec corev1.PodSpec) map[string]*corev1.ServiceAccountTokenProjection {
	out := map[string]*corev1.ServiceAccountTokenProjection{}
	for _, v := range spec.Volumes {
		if v.Projected == nil {
			continue
		}
		for _, s := range v.Projected.Sources {
			if s.ServiceAccountToken != nil {
				out[v.Name] = s.ServiceAccountToken
			}
		}
	}
	return out
}

// The agent DaemonSet projects a token of its service account for the
// Workload Identity pool it names in WORKLOAD_POOL, read-only where
// --gcs-token-file looks by default. The engine controller, which GKE
// Workload Identity serves through the metadata server, has no such token.
func TestDeployManifestProjectsTheAgentsGCSToken(t *testing.T) {
	objects := deployObjects(t)
	var agent appsv1.DaemonSet
	if err := k8syaml.Unmarshal(objects["DaemonSet/pod-migrate-agent"], &agent); err != nil || agent.Name == "" {
		t.Fatalf("deploy.yaml has no pod-migrate-agent DaemonSet (%v)", err)
	}
	spec := agent.Spec.Template.Spec
	if spec.ServiceAccountName != "pod-migrate-agent" {
		t.Fatalf("the agent runs as %q; the bucket grants pod-migrate-agent", spec.ServiceAccountName)
	}
	env := map[string]string{}
	var mounts []corev1.VolumeMount
	for _, c := range spec.Containers {
		if c.Name == "agent" {
			for _, e := range c.Env {
				env[e.Name] = e.Value
			}
			mounts = c.VolumeMounts
		}
	}
	if env["WORKLOAD_POOL"] != "<YOUR_WORKLOAD_POOL>" || env["IDENTITY_PROVIDER"] != "<YOUR_IDENTITY_PROVIDER>" {
		t.Fatalf("the agent's WORKLOAD_POOL and IDENTITY_PROVIDER = %q, %q, want the install placeholders", env["WORKLOAD_POOL"], env["IDENTITY_PROVIDER"])
	}
	tokens := projectedTokenAudiences(spec)
	if len(tokens) != 1 {
		t.Fatalf("the agent projects %d service account tokens, want 1", len(tokens))
	}
	for volume, tok := range tokens {
		if tok.Audience != env["WORKLOAD_POOL"] {
			t.Errorf("the projected token's audience is %q, want the agent's WORKLOAD_POOL (%q)", tok.Audience, env["WORKLOAD_POOL"])
		}
		if tok.ExpirationSeconds == nil || *tok.ExpirationSeconds != 3600 {
			t.Errorf("the projected token's expirationSeconds = %v, want 3600", tok.ExpirationSeconds)
		}
		if tok.Path != filepath.Base(DefaultGCSTokenFile) {
			t.Errorf("the projected token's path is %q, want %q", tok.Path, filepath.Base(DefaultGCSTokenFile))
		}
		for _, v := range spec.Volumes {
			if v.Name == volume && (v.Projected.DefaultMode == nil || *v.Projected.DefaultMode != 0o400) {
				t.Errorf("the projected token's defaultMode = %v, want 0400: only the agent reads it", v.Projected.DefaultMode)
			}
		}
		mounted := false
		for _, m := range mounts {
			if m.Name == volume {
				mounted = true
				if m.MountPath != filepath.Dir(DefaultGCSTokenFile) || !m.ReadOnly {
					t.Errorf("the token is mounted at %s (readOnly %v), want %s read-only", m.MountPath, m.ReadOnly, filepath.Dir(DefaultGCSTokenFile))
				}
			}
		}
		if !mounted {
			t.Errorf("the agent doesn't mount the %s volume", volume)
		}
	}

	var engine appsv1.Deployment
	if err := k8syaml.Unmarshal(objects["Deployment/pod-migrate-psengine"], &engine); err != nil || engine.Name == "" {
		t.Fatalf("deploy.yaml has no pod-migrate-psengine Deployment (%v)", err)
	}
	if got := projectedTokenAudiences(engine.Spec.Template.Spec); len(got) != 0 {
		t.Errorf("the engine controller projects service account tokens %v; GKE Workload Identity serves it through the metadata server", got)
	}
	if engine.Spec.Template.Spec.HostNetwork {
		t.Error("the engine controller runs with hostNetwork, where GKE Workload Identity can't serve it")
	}
}
