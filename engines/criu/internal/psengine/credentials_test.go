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
	"crypto/md5" //nolint:gosec // GCS reports MD5
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// stsStore is a GCSStore against fakeGCS that gets its tokens from a
// fakeSTS, both on clock. Its metadata server is fakeGCS's, which must never
// be asked.
func stsStore(t *testing.T, clock *fakeClock) (*GCSStore, *fakeGCS, *fakeSTS) {
	t.Helper()
	store, gcs := newFakeStore(t)
	sts, src := newFakeSTS(t)
	store.Tokens, store.now, src.now = src, clock.now, clock.now
	return store, gcs, sts
}

func refuse(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusBadRequest)
	_, _ = w.Write([]byte(`{"error":"invalid_grant","error_description":"refused"}`))
}

// Warm gets a token when the store has none, keeps one that stays usable for
// warmValidity, and refreshes one that wouldn't, so neither the first token
// nor the hourly refresh is fetched while a checkpoint's source is frozen.
func TestGCSStoreWarm(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	start := clock.now()
	store, gcs, sts := stsStore(t, clock)

	if err := store.Warm(ctx); err != nil || sts.exchanges() != 1 {
		t.Fatalf("Warm without a token = %v after %d exchanges, want one exchange", err, sts.exchanges())
	}
	// The token is refreshed at 55 min: until 40 min, it has 15 left.
	clock.set(start.Add(40*time.Minute - time.Second))
	if err := store.Warm(ctx); err != nil || sts.exchanges() != 1 {
		t.Fatalf("Warm with 15 min left = %v after %d exchanges, want the cached token", err, sts.exchanges())
	}
	clock.set(start.Add(40*time.Minute + time.Second))
	if err := store.Warm(ctx); err != nil || sts.exchanges() != 2 {
		t.Fatalf("Warm with under 15 min left = %v after %d exchanges, want a new token", err, sts.exchanges())
	}

	// A refresh that fails fails Warm, and leaves the token, which is still
	// good until its refresh time, to bearer.
	sts.respond = refuse
	clock.set(start.Add(40*time.Minute + time.Second + 41*time.Minute))
	if err := store.Warm(ctx); err == nil || !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("Warm with STS refusing = %v, want the refusal", err)
	}
	if tok, err := store.bearer(ctx); err != nil || tok != testAccessToken {
		t.Fatalf("bearer before the refresh time = %q, %v, want the cached token", tok, err)
	}
	// Past its refresh time, a token is never used again, even when the
	// refresh fails.
	clock.set(start.Add(40*time.Minute + time.Second + 55*time.Minute))
	if tok, err := store.bearer(ctx); err == nil || tok != "" {
		t.Fatalf("bearer past the refresh time with STS refusing = %q, %v, want an error and no token", tok, err)
	}
	if gcs.tokens != 0 {
		t.Fatalf("the metadata server was asked %d times", gcs.tokens)
	}

	// A store without credentials (a GCS emulator) has nothing to warm.
	if err := (&GCSStore{}).Warm(ctx); err != nil {
		t.Fatalf("Warm without credentials = %v", err)
	}
}

// warmingStore is a memStore whose Warm is recorded with the node operations,
// so a test sees what came first.
type warmingStore struct {
	*memStore
	ops *fakeNodeOps
	err error
	// block, when set, makes Warm wait for its context to end.
	block bool
}

func (s *warmingStore) Warm(ctx context.Context) error {
	s.ops.events = append(s.ops.events, "warm")
	if s.block {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.err
}

// A Warm that hangs is cut off by warmTimeout, and fails the work order with
// the source untouched.
func TestPipelineBoundsTheWarm(t *testing.T) {
	saved := warmTimeout
	warmTimeout = 50 * time.Millisecond
	t.Cleanup(func() { warmTimeout = saved })
	c := signedFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &warmingStore{memStore: newMemStore(), ops: ops, block: true}
	done := make(chan struct{})
	go func() {
		defer close(done)
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the work order waited past warmTimeout for its credentials")
	}
	requireUntouched(t, c, ops, store.memStore, "no credentials for the checkpoint bucket: context deadline exceeded")
}

// A store without credentials (a GCS emulator: no Tokens, no TokenURL) asks
// nobody for a token and sends its requests without an Authorization
// header; a store with Tokens uses them even without a TokenURL.
func TestGCSStoreWithoutCredentials(t *testing.T) {
	var mu sync.Mutex
	var auth []string
	objects := map[string][]byte{}
	emulator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		auth = append(auth, r.Header.Get("Authorization"))
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/compose"):
			_ = json.NewEncoder(w).Encode(map[string]string{"size": "4"})
		case r.Method == http.MethodPost:
			data, _ := io.ReadAll(r.Body)
			objects[r.URL.Query().Get("name")] = data
			sum := md5.Sum(data) //nolint:gosec
			_ = json.NewEncoder(w).Encode(map[string]string{"size": fmt.Sprint(len(data)), "md5Hash": base64.StdEncoding.EncodeToString(sum[:])})
		case r.Method == http.MethodGet:
			_, _ = w.Write(objects[strings.TrimPrefix(r.URL.Path, "/storage/v1/b/bkt/o/")])
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusNoContent)
		}
	}))
	defer emulator.Close()
	// Any token request at all fails the test: the metadata server's client
	// refuses and counts.
	var asked int
	saved := metadataClient
	metadataClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		asked++
		return nil, fmt.Errorf("asked %s for a token", r.URL)
	})}
	t.Cleanup(func() { metadataClient = saved })

	ctx := context.Background()
	store := &GCSStore{HTTP: emulator.Client(), UploadBase: emulator.URL + "/upload/storage/v1", APIBase: emulator.URL + "/storage/v1"}
	if err := store.Warm(ctx); err != nil {
		t.Fatalf("Warm without credentials = %v", err)
	}
	if _, err := VerifiedPut(ctx, store, "bkt", "o", strings.NewReader("data")); err != nil {
		t.Fatal(err)
	}
	if rc, err := store.Get(ctx, "bkt", "o"); err != nil {
		t.Fatal(err)
	} else {
		rc.Close()
	}
	if _, err := store.Compose(ctx, "bkt", "c", []string{"o"}); err != nil {
		t.Fatal(err)
	}
	if err := store.Delete(ctx, "bkt", "o"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(auth) != 4 || asked != 0 {
		t.Fatalf("%d emulator requests, %d token requests; want 4 and none", len(auth), asked)
	}
	for _, a := range auth {
		if a != "" {
			t.Fatalf("a request without credentials carried Authorization %q", a)
		}
	}

	// Tokens wins, with or without a TokenURL.
	sts, src := newFakeSTS(t)
	withTokens := &GCSStore{Tokens: src}
	if tok, err := withTokens.bearer(ctx); err != nil || tok != testAccessToken || sts.exchanges() != 1 {
		t.Fatalf("bearer with Tokens and no TokenURL = %q, %v after %d exchanges, want the STS token", tok, err, sts.exchanges())
	}
}

// In node mode (the metadata server's token), Warm gets a token, keeps one
// that stays usable for 15 minutes, and refreshes one that doesn't.
func TestGCSStoreWarmNodeMode(t *testing.T) {
	ctx := context.Background()
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	start := clock.now()
	store, gcs := newFakeStore(t)
	store.now = clock.now
	if err := store.Warm(ctx); err != nil || gcs.tokenCount() != 1 {
		t.Fatalf("Warm = %v after %d metadata requests, want one", err, gcs.tokenCount())
	}
	// The metadata server's hour-long token is refreshed 30 s before it
	// expires: at 59.5 min. Until 44.5 min, it has 15 left.
	clock.set(start.Add(44 * time.Minute))
	if err := store.Warm(ctx); err != nil || gcs.tokenCount() != 1 {
		t.Fatalf("Warm with over 15 min left = %v after %d metadata requests, want the cached token", err, gcs.tokenCount())
	}
	clock.set(start.Add(45 * time.Minute))
	if err := store.Warm(ctx); err != nil || gcs.tokenCount() != 2 {
		t.Fatalf("Warm with under 15 min left = %v after %d metadata requests, want a new token", err, gcs.tokenCount())
	}
}

// The pipeline warms the store's credentials before it freezes the source.
// A store that can't authenticate fails the work order with the source
// untouched and nothing uploaded.
func TestPipelineWarmsTheStoreBeforeTheFreeze(t *testing.T) {
	t.Run("warmed, then frozen", func(t *testing.T) {
		c := signedFixture(t)
		ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
		store := &warmingStore{memStore: newMemStore(), ops: ops}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
		if len(ops.events) < 2 || ops.events[0] != "warm" || ops.events[1] != "freeze" {
			t.Fatalf("events %v, want warm, then freeze", ops.events)
		}
		if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) {
			t.Fatalf("snapshot not Ready: %+v", contract.Conditions(ps))
		}
	})
	t.Run("no credentials", func(t *testing.T) {
		c := signedFixture(t)
		ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
		store := &warmingStore{memStore: newMemStore(), ops: ops, err: errors.New("sts token exchange: HTTP 400")}
		runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
		requireUntouched(t, c, ops, store.memStore, "no credentials for the checkpoint bucket: sts token exchange: HTTP 400")
	})
}

// requireUntouched checks a work order that failed before its source was
// touched: Checkpoint=False/Failed saying why, ending "the source container
// was not touched", no freeze, no SourceFrozen and nothing uploaded.
func requireUntouched(t *testing.T, c client.Client, ops *fakeNodeOps, store *memStore, want string) {
	t.Helper()
	for _, e := range ops.events {
		if e == "freeze" || e == "checkpoint" {
			t.Fatalf("the source was touched: %v", ops.events)
		}
	}
	if len(store.order) != 0 {
		t.Fatalf("objects were uploaded: %v", store.order)
	}
	ps := getSnapshot(t, c, cancelSnapshot)
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonFailed ||
		!strings.Contains(cond.Message, want) || !strings.HasSuffix(cond.Message, "the source container was not touched") {
		t.Fatalf("want Checkpoint=False/Failed saying %q and that the source wasn't touched, got %+v", want, cond)
	}
	if contract.IsSourceFrozen(ps) {
		t.Fatal("SourceFrozen recorded for a source that was never frozen")
	}
}

// With the real store, an exchange that STS refuses fails the work order
// before the freeze: the source is never frozen for a checkpoint whose
// archive couldn't be uploaded.
func TestPipelineNeverFreezesWhenSTSRefuses(t *testing.T) {
	c := signedFixture(t)
	clock := &fakeClock{t: time.Now()}
	store, gcs, sts := stsStore(t, clock)
	sts.respond = refuse
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	runAgent(t, &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: store})
	for _, e := range ops.events {
		if e == "freeze" {
			t.Fatalf("Freeze was called although STS refused: %v", ops.events)
		}
	}
	cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonFailed || !strings.Contains(cond.Message, `HTTP 400: "invalid_grant"`) ||
		!strings.HasSuffix(cond.Message, "the source container was not touched") {
		t.Fatalf("want Failed naming STS's refusal, with the source untouched, got %+v", cond)
	}
	if strings.Contains(cond.Message, testSubjectToken) {
		t.Fatalf("the condition holds the service account token: %q", cond.Message)
	}
	if len(gcs.objects) != 0 {
		t.Fatalf("objects were uploaded: %d", len(gcs.objects))
	}
}

// A 307 or 308 from the STS endpoint isn't followed: it would send the
// request body, with the service account token, wherever it points.
func TestSTSTokenSourceRefusesRedirects(t *testing.T) {
	var hits int
	elsewhere := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "elsewhere", "expires_in": 3600})
	}))
	defer elsewhere.Close()
	for _, status := range []int{http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		sts, src := newFakeSTS(t)
		sts.respond = func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, elsewhere.URL+"/v1/token", status)
		}
		for _, client := range []*http.Client{src.HTTP, nil} { // injected, and the default
			src.HTTP = client
			_, _, err := src.Token(context.Background())
			if err == nil || !strings.Contains(err.Error(), "HTTP 30") {
				t.Fatalf("Token after a %d = %v, want the redirect refused", status, err)
			}
		}
	}
	if hits != 0 {
		t.Fatalf("the redirect was followed %d times", hits)
	}
}

// Errors carry STS's error in either of its forms, scrubbed and clipped.
func TestSTSErrorForms(t *testing.T) {
	long := strings.Repeat("a", 1000)
	for _, tc := range []struct {
		name, body string
		want       []string
		notWant    []string
	}{{
		name:    "Google's API form",
		body:    `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Permission denied for ` + testSubjectToken + `"}}`,
		want:    []string{"HTTP 403", `"PERMISSION_DENIED"`, `"Permission denied for [service account token]"`},
		notWant: []string{testSubjectToken, "403,"},
	}, {
		name:    "OAuth form, clipped to 256 bytes",
		body:    `{"error":"invalid_request","error_description":"` + long + `"}`,
		want:    []string{`"invalid_request"`, `"` + strings.Repeat("a", stsErrorMaxLen) + `..."`},
		notWant: []string{strings.Repeat("a", stsErrorMaxLen+1)},
	}, {
		name: "an error that is neither",
		body: `{"error":42}`,
		want: []string{"sts token exchange: HTTP 400"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			err := stsError(http.StatusBadRequest, []byte(tc.body), testSubjectToken)
			if strings.Contains(tc.body, `"code":403`) {
				err = stsError(http.StatusForbidden, []byte(tc.body), testSubjectToken)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q doesn't say %q", err, w)
				}
			}
			for _, w := range tc.notWant {
				if strings.Contains(err.Error(), w) {
					t.Errorf("error %q holds %q", err, w)
				}
			}
		})
	}
}

// roundTripFunc is an http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Without an Endpoint, the exchange goes to Google's STS.
func TestSTSTokenSourceDefaultEndpoint(t *testing.T) {
	_, src := newFakeSTS(t)
	src.Endpoint = ""
	var got string
	src.HTTP = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		got = r.URL.String()
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"access_token":"x","expires_in":3600}`))}, nil
	})}
	if _, _, err := src.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if DefaultSTSEndpoint != "https://sts.googleapis.com/v1/token" {
		t.Fatalf("DefaultSTSEndpoint = %q, want Google's STS", DefaultSTSEndpoint)
	}
	if got != DefaultSTSEndpoint {
		t.Fatalf("the exchange went to %q, want %s", got, DefaultSTSEndpoint)
	}
}

// The metadata server's token comes through a client of its own, not the
// store's upload client, each fetch with a short deadline, and an empty one
// is an error.
func TestMetadataTokens(t *testing.T) {
	store, _ := newFakeStore(t)
	store.HTTP = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("the upload client was used for a token")
	})}
	if tok, err := store.bearer(context.Background()); err != nil || tok != "fake-tok" {
		t.Fatalf("bearer = %q, %v, want the metadata server's token", tok, err)
	}
	if metadataTimeout > 10*time.Second {
		t.Fatalf("metadataTimeout = %v, want a short one: the store's mutex is held meanwhile", metadataTimeout)
	}

	saved := metadataTimeout
	metadataTimeout = 50 * time.Millisecond
	t.Cleanup(func() { metadataTimeout = saved })
	release := make(chan struct{})
	hung := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer hung.Close()
	defer close(release)
	done := make(chan error, 1)
	go func() {
		_, _, err := metadataTokens{url: hung.URL, now: time.Now}.Token(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "deadline exceeded") {
			t.Fatalf("Token from a hung metadata server = %v, want a deadline error", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the metadata fetch wasn't cut off by its deadline")
	}

	empty := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"access_token":"","expires_in":3600}`))
	}))
	defer empty.Close()
	src := metadataTokens{url: empty.URL, now: time.Now}
	if _, _, err := src.Token(context.Background()); err == nil || !strings.Contains(err.Error(), "no access_token") {
		t.Fatalf("Token with an empty access_token = %v", err)
	}
}

// tokenCount is how many tokens the fake metadata server handed out.
func (f *fakeGCS) tokenCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tokens
}
