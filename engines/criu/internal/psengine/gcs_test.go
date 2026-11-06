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
