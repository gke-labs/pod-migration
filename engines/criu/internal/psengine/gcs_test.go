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
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// fakeGCS implements the three JSON-API endpoints the store uses, plus the
// metadata token endpoint.
type fakeGCS struct {
	mu      sync.Mutex
	objects map[string][]byte // "bucket/object" -> data
	tamper  bool              // corrupt reported md5 to exercise verification
	tokens  int
}

func (f *fakeGCS) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing metadata header", http.StatusForbidden)
			return
		}
		f.mu.Lock()
		f.tokens++
		f.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"access_token": "fake-tok", "expires_in": 3600})
	})
	mux.HandleFunc("/upload/b/", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer fake-tok" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		bucket := strings.Split(strings.TrimPrefix(r.URL.Path, "/upload/b/"), "/")[0]
		name := r.URL.Query().Get("name")
		data, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.objects[bucket+"/"+name] = data
		f.mu.Unlock()
		sum := md5.Sum(data) //nolint:gosec
		md5b64 := base64.StdEncoding.EncodeToString(sum[:])
		if f.tamper {
			md5b64 = base64.StdEncoding.EncodeToString([]byte("bogusbogusbogusb"))
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"size": fmt.Sprint(len(data)), "md5Hash": md5b64})
	})
	mux.HandleFunc("/api/b/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/compose") && r.Method == http.MethodPost {
			if r.Header.Get("Content-Type") != "application/json" {
				http.Error(w, "missing content-type application/json", http.StatusBadRequest)
				return
			}
			parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/b/"), "/")
			bucket := parts[0]
			destPath := strings.TrimSuffix(strings.Join(parts[2:], "/"), "/compose")
			var req struct {
				SourceObjects []struct {
					Name string `json:"name"`
				} `json:"sourceObjects"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			f.mu.Lock()
			var combined []byte
			for _, s := range req.SourceObjects {
				data, ok := f.objects[bucket+"/"+s.Name]
				if !ok {
					f.mu.Unlock()
					http.Error(w, "source not found: "+s.Name, http.StatusNotFound)
					return
				}
				combined = append(combined, data...)
			}
			f.objects[bucket+"/"+destPath] = combined
			f.mu.Unlock()
			sum := md5.Sum(combined) //nolint:gosec
			_ = json.NewEncoder(w).Encode(map[string]string{
				"size":    fmt.Sprint(len(combined)),
				"md5Hash": base64.StdEncoding.EncodeToString(sum[:]),
			})
			return
		}

		parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/api/b/"), "/o/", 2)
		if len(parts) != 2 {
			http.Error(w, "bad path", http.StatusBadRequest)
			return
		}
		key := parts[0] + "/" + parts[1]
		f.mu.Lock()
		defer f.mu.Unlock()
		switch r.Method {
		case http.MethodGet:
			data, ok := f.objects[key]
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			rangeHdr := r.Header.Get("Range")
			if rangeHdr != "" && strings.HasPrefix(rangeHdr, "bytes=") {
				rangeSpec := strings.TrimPrefix(rangeHdr, "bytes=")
				var start, end int64
				if _, err := fmt.Sscanf(rangeSpec, "%d-%d", &start, &end); err == nil {
					if end >= int64(len(data)) {
						end = int64(len(data)) - 1
					}
					w.WriteHeader(http.StatusPartialContent)
					_, _ = w.Write(data[start : end+1])
					return
				}
			}
			_, _ = w.Write(data)
		case http.MethodDelete:
			delete(f.objects, key)
			w.WriteHeader(http.StatusNoContent)
		}
	})
	return mux
}

func newFakeStore(t *testing.T) (*GCSStore, *fakeGCS) {
	t.Helper()
	f := &fakeGCS{objects: map[string][]byte{}}
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return &GCSStore{
		HTTP:       srv.Client(),
		UploadBase: srv.URL + "/upload",
		APIBase:    srv.URL + "/api",
		TokenURL:   srv.URL + "/token",
	}, f
}

func TestGCSStoreRoundTrip(t *testing.T) {
	store, fake := newFakeStore(t)
	ctx := context.Background()

	n, err := VerifiedPut(ctx, store, "bkt", "snaps/ps-x/spec.json", strings.NewReader("hello-spec"))
	if err != nil || n != 10 {
		t.Fatalf("VerifiedPut: n=%d err=%v", n, err)
	}
	rc, err := store.Get(ctx, "bkt", "snaps/ps-x/spec.json")
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "hello-spec" {
		t.Fatalf("round trip: %q", got)
	}
	if err := store.Delete(ctx, "bkt", "snaps/ps-x/spec.json"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(ctx, "bkt", "snaps/ps-x/spec.json"); err == nil {
		t.Fatal("expected 404 after delete")
	}
	// Token must be cached, not re-fetched per call.
	if fake.tokens != 1 {
		t.Fatalf("token fetches: %d", fake.tokens)
	}
}

func TestVerifiedPutCatchesCorruption(t *testing.T) {
	store, fake := newFakeStore(t)
	fake.tamper = true
	if _, err := VerifiedPut(context.Background(), store, "bkt", "o", strings.NewReader("data")); err == nil {
		t.Fatal("tampered md5 must fail verification")
	}
}

func TestGCSProber(t *testing.T) {
	store, fake := newFakeStore(t)
	probe := NewGCSProber(store)
	if err := probe(context.Background(), "bkt", "snapshots"); err != nil {
		t.Fatalf("probe: %v", err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.objects) != 0 {
		t.Fatalf("probe must clean up after itself: %v", len(fake.objects))
	}
}

func TestGCSStore_ComposeAndGetRange(t *testing.T) {
	store, _ := newFakeStore(t)
	ctx := context.Background()

	// Put two parts
	_, err := store.Put(ctx, "bkt", "snaps/ps-1/checkpoint.tar.part-000", strings.NewReader("hello-"))
	if err != nil {
		t.Fatalf("Put part 0: %v", err)
	}
	_, err = store.Put(ctx, "bkt", "snaps/ps-1/checkpoint.tar.part-001", strings.NewReader("world!"))
	if err != nil {
		t.Fatalf("Put part 1: %v", err)
	}

	// Compose
	info, err := store.Compose(ctx, "bkt", "snaps/ps-1/checkpoint.tar", []string{
		"snaps/ps-1/checkpoint.tar.part-000",
		"snaps/ps-1/checkpoint.tar.part-001",
	})
	if err != nil {
		t.Fatalf("Compose: %v", err)
	}
	if info.Bytes != 12 {
		t.Fatalf("composed bytes = %d, want 12", info.Bytes)
	}

	// Test GetRange with valid range [0, 4] -> "hello"
	rc, err := store.GetRange(ctx, "bkt", "snaps/ps-1/checkpoint.tar", 0, 4)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	got, _ := io.ReadAll(rc)
	rc.Close()
	if string(got) != "hello" {
		t.Fatalf("GetRange(0, 4) = %q, want %q", got, "hello")
	}

	// Test GetRange requires 206: if server returns 200 on range request, GetRange returns error
	badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK) // returns 200 instead of 206
		_, _ = w.Write([]byte("full-object"))
	}))
	defer badServer.Close()
	badStore := &GCSStore{
		HTTP:     badServer.Client(),
		APIBase:  badServer.URL,
		TokenURL: store.TokenURL,
	}
	_, err = badStore.GetRange(ctx, "bkt", "obj", 0, 4)
	if err == nil {
		t.Fatal("expected GetRange to reject HTTP 200 when 206 is required")
	}
	if !strings.Contains(err.Error(), "HTTP 200") {
		t.Fatalf("expected error mentioning HTTP 200, got: %v", err)
	}
}

// Without STORAGE_EMULATOR_HOST the store talks to GCS with a metadata token.
// With it, the store sends the same JSON API calls to the emulator, without
// credentials (the kind E2E runs fake-gcs-server).
func TestNewGCSStore_Emulator(t *testing.T) {
	t.Setenv(StorageEmulatorHostEnv, "")
	if s := NewGCSStore(); s.APIBase != "https://storage.googleapis.com/storage/v1" || s.TokenURL == "" {
		t.Fatalf("default store = %q with token URL %q, want GCS with a metadata token", s.APIBase, s.TokenURL)
	}

	var mu sync.Mutex
	objects := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if auth := r.Header.Get("Authorization"); auth != "" {
			t.Errorf("%s %s sent Authorization %q to the emulator", r.Method, r.URL.Path, auth)
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/upload/storage/v1/b/bkt/o":
			data, _ := io.ReadAll(r.Body)
			objects[r.URL.Query().Get("name")] = data
			sum := md5.Sum(data) //nolint:gosec
			_ = json.NewEncoder(w).Encode(map[string]string{"size": fmt.Sprint(len(data)), "md5Hash": base64.StdEncoding.EncodeToString(sum[:])})
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/storage/v1/b/bkt/o/"):
			data, ok := objects[strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bkt/o/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(data)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/compose"):
			_ = json.NewEncoder(w).Encode(map[string]string{"size": "10"})
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL)
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	// host:port as the client libraries take it, and a URL with a trailing slash.
	for _, host := range []string{strings.TrimPrefix(srv.URL, "http://"), srv.URL + "/"} {
		t.Setenv(StorageEmulatorHostEnv, host)
		store := NewGCSStore()
		ctx := context.Background()
		if _, err := VerifiedPut(ctx, store, "bkt", "snaps/ps-x/spec.json", strings.NewReader("hello-spec")); err != nil {
			t.Fatalf("%s: VerifiedPut: %v", host, err)
		}
		rc, err := store.Get(ctx, "bkt", "snaps/ps-x/spec.json")
		if err != nil {
			t.Fatalf("%s: Get: %v", host, err)
		}
		got, _ := io.ReadAll(rc)
		rc.Close()
		if string(got) != "hello-spec" {
			t.Fatalf("%s: round trip = %q", host, got)
		}
		if _, err := store.Compose(ctx, "bkt", "snaps/ps-x/checkpoint.tar", []string{"snaps/ps-x/spec.json"}); err != nil {
			t.Fatalf("%s: Compose: %v", host, err)
		}
	}
}
