// Copyright 2026 Microsoft Corporation
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

package cooperative

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func marshalState(t *testing.T, state State) string {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestClientLifecycle(t *testing.T) {
	for _, transition := range []string{"resume", "stop"} {
		t.Run(transition, func(t *testing.T) {
			c := newCounter(t)
			server := httptest.NewServer(c.Handler())
			defer server.Close()
			client := &Client{BaseURL: server.URL}
			ctx := context.Background()
			initial, err := client.State(ctx)
			if err != nil || initial != c.State() {
				t.Fatalf("state: %+v, %v", initial, err)
			}
			if _, err := client.Resume(ctx, initial); err != nil {
				t.Fatal(err)
			}
			for range 10 {
				c.Tick()
			}
			request := PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"}
			captured, err := client.Pause(ctx, request)
			if err != nil || captured.Counter != 10 || captured.Epoch != 1 || !captured.Paused {
				t.Fatalf("pause: %+v, %v", captured, err)
			}
			if retry, err := client.Pause(ctx, request); err != nil || retry != captured {
				t.Fatalf("pause retry: %+v, %v", retry, err)
			}
			if _, err := client.Resume(ctx, initial); err == nil || !strings.Contains(err.Error(), "409") {
				t.Fatalf("stale resume: %v", err)
			}
			if transition == "stop" {
				if state, err := client.Stop(ctx, captured); err != nil || state != captured {
					t.Fatalf("stop response: %+v, %v", state, err)
				}
				select {
				case <-c.Done():
				case <-time.After(time.Second):
					t.Fatal("HTTP stop did not signal shutdown")
				}
				if _, err := client.Resume(ctx, captured); err == nil || !strings.Contains(err.Error(), "409") {
					t.Fatalf("stopped process resumed: %v", err)
				}
			} else {
				if _, err := client.Resume(ctx, captured); err != nil {
					t.Fatal(err)
				}
				c.Tick()
				if state, err := client.Resume(ctx, captured); err != nil || state.Counter != 11 || state.Paused || state.Epoch != 1 {
					t.Fatalf("late resume retry: %+v, %v", state, err)
				}
				state, err := client.Pause(ctx, request)
				if !errors.Is(err, ErrConflict) || state.Counter != 11 || state.Paused || state.Epoch != 1 {
					t.Fatalf("late pause retry must report the running state as a conflict: %+v, %v", state, err)
				}
				if got := c.State(); got.Paused || got.Counter != 11 {
					t.Fatalf("late pause retry changed the activated process: %+v", got)
				}
				if _, err := client.Pause(ctx, PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-2"}); err == nil || !strings.Contains(err.Error(), "409") {
					t.Fatalf("second migration ID accepted: %v", err)
				}
			}
		})
	}
}

func TestHandlerRejectsInvalidJSON(t *testing.T) {
	c := newCounter(t)
	state := c.State()
	valid := marshalState(t, state)
	cases := map[string]string{
		"empty":          "",
		"null":           "null",
		"array":          "[]",
		"missing":        `{"nonce":"` + state.Nonce + `"}`,
		"unknown":        strings.TrimSuffix(valid, "}") + `,"restore":true}`,
		"duplicate":      strings.TrimSuffix(valid, "}") + `,"nonce":"` + state.Nonce + `"}`,
		"wrong case":     strings.Replace(valid, `"nonce"`, `"Nonce"`, 1),
		"null nonce":     strings.Replace(valid, `"`+state.Nonce+`"`, "null", 1),
		"null zero":      strings.Replace(valid, `"counter":0`, `"counter":null`, 1),
		"negative":       strings.Replace(valid, `"counter":0`, `"counter":-1`, 1),
		"overflow":       strings.Replace(valid, `"counter":0`, `"counter":18446744073709551616`, 1),
		"fraction":       strings.Replace(valid, `"counter":0`, `"counter":0.5`, 1),
		"empty nonce":    strings.Replace(valid, state.Nonce, "", 1),
		"blank nonce":    strings.Replace(valid, state.Nonce, "  ", 1),
		"trailing JSON":  valid + `{}`,
		"trailing junk":  valid + "!",
		"too large":      valid + strings.Repeat(" ", maxJSONBytes),
		"unterminated":   strings.TrimSuffix(valid, "}"),
		"wrong boolean":  strings.Replace(valid, `"paused":true`, `"paused":"true"`, 1),
		"missing epoch":  strings.Replace(valid, `"epoch":0,`, "", 1),
		"missing paused": strings.Replace(valid, `"paused":true,`, "", 1),
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			for _, path := range []string{"/resume", "/stop"} {
				w := httptest.NewRecorder()
				c.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
				if w.Code != http.StatusBadRequest {
					t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body)
				}
				if c.State() != state {
					t.Fatal("invalid request changed state")
				}
			}
		})
	}
	for _, body := range []string{
		`{"nonce":"` + state.Nonce + `","migrationID":""}`,
		`{"nonce":"","migrationID":"migration-1"}`,
		`{"nonce":"` + state.Nonce + `","migrationID":"migration-1","extra":0}`,
		`{"nonce":"` + state.Nonce + `","migrationID":"migration-1"} {}`,
		`{"nonce":"` + state.Nonce + `"}`,
		"null",
	} {
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/pause", strings.NewReader(body)))
		if w.Code != http.StatusBadRequest || c.State() != state {
			t.Fatalf("invalid pause: status=%d body=%s", w.Code, w.Body)
		}
	}
	select {
	case <-c.Done():
		t.Fatal("invalid request stopped counter")
	default:
	}
}

func TestHandlerRoutesAndColdMismatch(t *testing.T) {
	c := newCounter(t)
	for _, test := range []struct {
		method, path, allow string
		status              int
	}{
		{http.MethodPost, "/state", http.MethodGet, http.StatusMethodNotAllowed},
		{http.MethodGet, "/pause", http.MethodPost, http.StatusMethodNotAllowed},
		{http.MethodGet, "/resume", http.MethodPost, http.StatusMethodNotAllowed},
		{http.MethodGet, "/stop", http.MethodPost, http.StatusMethodNotAllowed},
		{http.MethodPost, "/restore", "", http.StatusNotFound},
	} {
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, httptest.NewRequest(test.method, test.path, nil))
		if w.Code != test.status || w.Header().Get("Allow") != test.allow {
			t.Fatalf("%s %s: %d, Allow=%q", test.method, test.path, w.Code, w.Header().Get("Allow"))
		}
		if w.Header().Get("Content-Type") != "application/json" {
			t.Fatal("error did not use JSON")
		}
	}
	unrelated := newCounter(t).State()
	for _, path := range []string{"/resume", "/stop"} {
		w := httptest.NewRecorder()
		c.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader(marshalState(t, unrelated))))
		if w.Code != http.StatusConflict {
			t.Fatalf("cold-start mismatch %s: %d", path, w.Code)
		}
	}
}

type orderedResponse struct {
	*httptest.ResponseRecorder
	t       *testing.T
	done    <-chan struct{}
	flushed bool
}

func (w *orderedResponse) assertNotDone() {
	w.t.Helper()
	select {
	case <-w.done:
		w.t.Fatal("shutdown signaled before stop response was flushed")
	default:
	}
}

func (w *orderedResponse) WriteHeader(status int) {
	w.assertNotDone()
	w.ResponseRecorder.WriteHeader(status)
}

func (w *orderedResponse) Write(data []byte) (int, error) {
	w.assertNotDone()
	return w.ResponseRecorder.Write(data)
}

func (w *orderedResponse) Flush() {
	w.assertNotDone()
	if w.Code != http.StatusOK || w.Body.Len() == 0 {
		w.t.Fatal("flushed without a success response")
	}
	w.flushed = true
	w.ResponseRecorder.Flush()
}

func TestStopAcknowledgedBeforeShutdown(t *testing.T) {
	c := newCounter(t)
	w := &orderedResponse{ResponseRecorder: httptest.NewRecorder(), t: t, done: c.Done()}
	c.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/stop", strings.NewReader(marshalState(t, c.State()))))
	if !w.flushed {
		t.Fatal("stop response was not flushed")
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("stop did not signal shutdown")
	}
}

func TestClientRejectsBadResponses(t *testing.T) {
	state := newCounter(t).State()
	valid := marshalState(t, state)
	for name, test := range map[string]struct {
		status int
		body   string
		want   string
	}{
		"failure":       {http.StatusServiceUnavailable, "unavailable", "HTTP 503"},
		"huge failure":  {http.StatusBadGateway, strings.Repeat("x", maxJSONBytes+1), "HTTP 502"},
		"empty":         {http.StatusNoContent, "", "JSON"},
		"no nonce":      {http.StatusOK, strings.Replace(valid, state.Nonce, "", 1), "nonce"},
		"null":          {http.StatusOK, "null", "JSON"},
		"array":         {http.StatusOK, "[]", "JSON"},
		"extra field":   {http.StatusOK, strings.TrimSuffix(valid, "}") + `,"other":true}`, "JSON"},
		"extra object":  {http.StatusOK, valid + valid, "JSON"},
		"huge success":  {http.StatusOK, valid + strings.Repeat(" ", maxJSONBytes), "exceeds"},
		"missing field": {http.StatusOK, `{"nonce":"nonempty"}`, "missing"},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client := &Client{BaseURL: server.URL}
			if _, err := client.State(context.Background()); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("response unexpectedly accepted: %v", err)
			}
		})
	}
}

func TestClientDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/state" {
			redirected.Store(true)
		}
		http.Redirect(w, r, "/other", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	client := &Client{BaseURL: server.URL, HTTPClient: server.Client()}
	if _, err := client.State(context.Background()); err == nil || !strings.Contains(err.Error(), "307") {
		t.Fatalf("redirect not treated as failure: %v", err)
	}
	if redirected.Load() {
		t.Fatal("client followed redirect")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestClientValidationAndTransportErrors(t *testing.T) {
	sentinel := errors.New("transport unavailable")
	httpClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, sentinel
	})}
	client := &Client{BaseURL: "http://127.0.0.1", HTTPClient: httpClient}
	if _, err := client.State(context.Background()); !errors.Is(err, sentinel) {
		t.Fatalf("transport error not preserved: %v", err)
	}
	for _, base := range []string{"", "://", "ftp://localhost", "http://", "http://localhost?x=y", "http://localhost/#fragment", "http://user:password@localhost"} {
		client := &Client{BaseURL: base, HTTPClient: httpClient}
		if _, err := client.State(context.Background()); err == nil || errors.Is(err, sentinel) {
			t.Fatalf("invalid base URL reached transport: %q, %v", base, err)
		}
	}
	ctx := context.Background()
	for _, operation := range []func() (State, error){
		func() (State, error) { return client.Pause(ctx, PauseRequest{MigrationID: "migration-1"}) },
		func() (State, error) { return client.Pause(ctx, PauseRequest{Nonce: "nonce"}) },
		func() (State, error) { return client.Resume(ctx, State{Paused: true}) },
		func() (State, error) { return client.Stop(ctx, State{Paused: true}) },
		func() (State, error) { return client.Resume(ctx, State{Nonce: "nonce"}) },
	} {
		if _, err := operation(); !errors.Is(err, ErrInvalidRequest) {
			t.Fatalf("invalid input reached transport: %v", err)
		}
	}
}

func TestClientTimeoutAndContext(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client := &Client{BaseURL: server.URL}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := client.State(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context deadline not preserved: %v", err)
	}
	ctx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if _, err := client.State(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("context cancellation not preserved: %v", err)
	}
	client.HTTPClient = &http.Client{Timeout: 30 * time.Millisecond}
	if _, err := client.State(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("custom HTTP timeout not preserved: %v", err)
	}
	state := marshalState(t, newCounter(t).State())
	client.HTTPClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > defaultHTTPTimeout {
			t.Error("default timeout not applied to provided zero-timeout client")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(state)), Header: make(http.Header)}, nil
	})}
	if _, err := client.State(context.Background()); err != nil {
		t.Fatal(err)
	}
	if client.HTTPClient.Timeout != 0 {
		t.Fatal("caller-owned HTTP client was mutated")
	}
}
