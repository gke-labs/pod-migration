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

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/cooperative"
)

const validExpected = `{"nonce":"n","counter":0,"paused":true,"epoch":0,"migrationID":""}`

func TestConfiguration(t *testing.T) {
	cfg, err := parseConfig([]string{"-action", "state"}, io.Discard)
	if err != nil || cfg.endpoint != "http://127.0.0.1:8080" || cfg.timeout != 5*time.Second {
		t.Fatalf("defaults: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-action", "pause", "-nonce", "n", "-migration-id", "m"},
		{"-action", "resume", "-expected", validExpected},
		{"-action", "stop", "-expected", validExpected},
		{"-action", "state", "-endpoint", "http://[::1]:8080/", "-timeout", "1s"},
	} {
		if _, err := parseConfig(args, io.Discard); err != nil {
			t.Fatalf("valid flags %v: %v", args, err)
		}
	}
	for _, args := range [][]string{
		nil,
		{"-action", "restore"},
		{"-action", "pause"},
		{"-action", "pause", "-nonce", "n"},
		{"-action", "pause", "-migration-id", "m"},
		{"-action", "pause", "-nonce", " ", "-migration-id", "m"},
		{"-action", "pause", "-nonce", "n", "-migration-id", " "},
		{"-action", "pause", "-nonce", "n", "-migration-id", "m", "-expected", validExpected},
		{"-action", "resume"},
		{"-action", "stop"},
		{"-action", "resume", "-expected", ""},
		{"-action", "resume", "-expected", validExpected, "-nonce", "n"},
		{"-action", "stop", "-expected", validExpected, "-migration-id", "m"},
		{"-action", "state", "-expected", validExpected},
		{"-action", "state", "-nonce", ""},
		{"-action", "state", "-timeout", "0"},
		{"-action", "state", "-timeout", "-1s"},
		{"-action", "state", "-timeout", "bad"},
		{"-action", "state", "-unknown"},
		{"-action", "state", "unexpected"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
	var output, diagnostics bytes.Buffer
	if err := run(context.Background(), []string{"-help"}, &output, &diagnostics); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	if output.Len() != 0 {
		t.Fatalf("help contaminated stdout: %s", output.String())
	}
	for _, text := range []string{"-expected", "stdin is not read", "CRI exec", "no state import or restore", "loopback"} {
		if !strings.Contains(diagnostics.String(), text) {
			t.Errorf("help missing %q", text)
		}
	}
}

func TestLoopbackEndpoint(t *testing.T) {
	for _, endpoint := range []string{
		"http://127.0.0.1:8080", "http://127.0.0.2:8080/", "http://[::1]:8080",
		"http://127.0.0.1", "http://[::ffff:127.0.0.1]:8080",
	} {
		if err := validateEndpoint(endpoint); err != nil {
			t.Errorf("loopback endpoint %q: %v", endpoint, err)
		}
	}
	for _, endpoint := range []string{
		"", "127.0.0.1:8080", "https://127.0.0.1:8080", "http:127.0.0.1",
		"http://localhost:8080", "http://127.0.0.1.example:8080", "http://10.0.0.1:8080",
		"http://0.0.0.0:8080", "http://[::]:8080", "http://[fe80::1]:8080",
		"http://[::1%25lo0]:8080", "http://user@127.0.0.1:8080",
		"http://127.0.0.1:8080/prefix", "http://127.0.0.1:8080/%2f",
		"http://127.0.0.1:8080?x=y", "http://127.0.0.1:8080?",
		"http://127.0.0.1:8080#fragment", "http://127.0.0.1:8080#",
		"http://127.0.0.1:bad", "http://[::1",
	} {
		if err := validateEndpoint(endpoint); err == nil {
			t.Errorf("unsafe endpoint accepted: %q", endpoint)
		}
	}
}

func TestDecodeExpected(t *testing.T) {
	want := cooperative.State{Nonce: "n", Paused: true}
	if got, err := decodeExpected(validExpected); err != nil || got != want {
		t.Fatalf("valid state: %+v, %v", got, err)
	}
	maximum := `{"epoch":18446744073709551615,"counter":18446744073709551615,"migrationID":"m","paused":true,"nonce":"n"}`
	if got, err := decodeExpected(maximum); err != nil || got.Counter != ^uint64(0) || got.Epoch != ^uint64(0) {
		t.Fatalf("uint64 precision lost: %+v, %v", got, err)
	}
	bounded := validExpected + strings.Repeat(" ", maxExpectedBytes-len(validExpected))
	if _, err := decodeExpected(bounded); err != nil {
		t.Fatalf("exactly 16 KiB rejected: %v", err)
	}
	for name, input := range map[string]string{
		"empty":             "",
		"array":             "[]",
		"null object":       "null",
		"scalar":            "true",
		"empty object":      "{}",
		"unterminated":      strings.TrimSuffix(validExpected, "}"),
		"unknown":           strings.Replace(validExpected, `"nonce":`, `"other":0,"nonce":`, 1),
		"case alias":        strings.Replace(validExpected, `"nonce"`, `"Nonce"`, 1),
		"escaped duplicate": strings.Replace(validExpected, `"nonce":`, `"\u006eonce":"x","nonce":`, 1),
		"trailing object":   validExpected + "{}",
		"trailing null":     validExpected + "null",
		"trailing junk":     validExpected + "garbage",
		"oversized":         bounded + " ",
		"negative counter":  strings.Replace(validExpected, `"counter":0`, `"counter":-1`, 1),
		"overflow":          strings.Replace(validExpected, `"counter":0`, `"counter":18446744073709551616`, 1),
		"fraction":          strings.Replace(validExpected, `"counter":0`, `"counter":0.5`, 1),
		"string counter":    strings.Replace(validExpected, `"counter":0`, `"counter":"0"`, 1),
		"wrong bool":        strings.Replace(validExpected, `"paused":true`, `"paused":"true"`, 1),
		"wrong nonce":       strings.Replace(validExpected, `"nonce":"n"`, `"nonce":{}`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := decodeExpected(input); err == nil {
				t.Fatalf("invalid expected state accepted: %s", input)
			}
		})
	}
	fields := []string{`"nonce":"n"`, `"counter":0`, `"paused":true`, `"epoch":0`, `"migrationID":""`}
	for i, field := range fields {
		key, _, _ := strings.Cut(field, ":")
		for name, input := range map[string]string{
			"missing":   "{" + strings.Join(append(append([]string{}, fields[:i]...), fields[i+1:]...), ",") + "}",
			"null":      strings.Replace(validExpected, field, key+":null", 1),
			"duplicate": strings.Replace(validExpected, field, field+","+field, 1),
		} {
			t.Run(key+"/"+name, func(t *testing.T) {
				if _, err := decodeExpected(input); err == nil {
					t.Fatalf("invalid field accepted: %s", input)
				}
			})
		}
	}
}

func TestRunControlsExistingCounter(t *testing.T) {
	counter, err := cooperative.New()
	if err != nil {
		t.Fatal(err)
	}
	handler := counter.Handler()
	requests := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r.Method + " " + r.URL.Path
		handler.ServeHTTP(w, r)
	}))
	defer server.Close()
	invoke := func(action string, args ...string) cooperative.State {
		t.Helper()
		var output, diagnostics bytes.Buffer
		flags := append([]string{"-endpoint", server.URL, "-action", action}, args...)
		if err := run(context.Background(), flags, &output, &diagnostics); err != nil {
			t.Fatalf("%s: %v; %s", action, err, diagnostics.String())
		}
		var got cooperative.State
		if err := json.Unmarshal(output.Bytes(), &got); err != nil {
			t.Fatalf("stdout is not one State JSON document: %q: %v", output.String(), err)
		}
		if diagnostics.Len() != 0 || got != counter.State() {
			t.Fatalf("unexpected output: %+v; diagnostics: %s", got, diagnostics.String())
		}
		method := http.MethodPost
		if action == "state" {
			method = http.MethodGet
		}
		if got := <-requests; got != method+" /"+action || len(requests) != 0 {
			t.Fatalf("unexpected control requests: %q (%d extra)", got, len(requests))
		}
		return got
	}
	initial := invoke("state")
	active := invoke("resume", "-expected", stateJSON(t, initial))
	if active.Paused {
		t.Fatal("resume left counter paused")
	}
	counter.Tick()
	paused := invoke("pause", "-nonce", initial.Nonce, "-migration-id", "migration")
	if !paused.Paused || paused.Counter != 1 || paused.Epoch != 1 || paused.MigrationID != "migration" {
		t.Fatalf("pause did not capture exact state: %+v", paused)
	}
	counter.Tick()
	if got := invoke("state"); got != paused {
		t.Fatalf("paused counter advanced: %+v", got)
	}
	if got := invoke("stop", "-expected", stateJSON(t, paused)); got != paused {
		t.Fatalf("stop changed captured state: %+v", got)
	}
	select {
	case <-counter.Done():
	case <-time.After(time.Second):
		t.Fatal("stop did not reach existing HTTP handler")
	}
}

func TestRunPauseRetryAfterActivationFails(t *testing.T) {
	counter, err := cooperative.New()
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(counter.Handler())
	defer server.Close()
	initial := counter.State()
	if _, err := counter.Resume(initial); err != nil {
		t.Fatal(err)
	}
	captured, err := counter.Pause(cooperative.PauseRequest{Nonce: initial.Nonce, MigrationID: "migration"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := counter.Resume(captured); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	err = run(context.Background(), []string{"-endpoint", server.URL, "-action", "pause", "-nonce", initial.Nonce, "-migration-id", "migration"}, &output, io.Discard)
	if !errors.Is(err, cooperative.ErrConflict) || output.Len() != 0 {
		t.Fatalf("pause retry after activation must exit nonzero without a state document: %v, stdout=%q", err, output.String())
	}
	if counter.State().Paused {
		t.Fatal("pause retry re-paused the activated counter")
	}
}

func TestRunRejectsInvalidExpectedBeforeHTTP(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "must not be reached", http.StatusInternalServerError)
	}))
	defer server.Close()
	for _, action := range []string{"resume", "stop"} {
		for _, expected := range []string{
			"{}", validExpected + "{}",
			strings.Replace(validExpected, `"epoch":0`, `"epoch":0,"epoch":0`, 1),
			strings.Replace(validExpected, `"counter":0`, `"counter":null`, 1),
			strings.Replace(validExpected, `"nonce":"n"`, `"nonce":""`, 1),
			strings.Replace(validExpected, `"paused":true`, `"paused":false`, 1),
			strings.Repeat(" ", maxExpectedBytes+1),
		} {
			var output bytes.Buffer
			err := run(context.Background(), []string{"-endpoint", server.URL, "-action", action, "-expected", expected}, &output, io.Discard)
			if err == nil || output.Len() != 0 {
				t.Fatalf("%s accepted invalid expected state: error=%v stdout=%q", action, err, output.String())
			}
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid expected state made %d HTTP calls", calls.Load())
	}
}

func TestRunConflictDoesNotChangeState(t *testing.T) {
	for _, action := range []string{"resume", "stop"} {
		t.Run(action, func(t *testing.T) {
			counter, err := cooperative.New()
			if err != nil {
				t.Fatal(err)
			}
			before := counter.State()
			wrong := before
			wrong.Counter++
			server := httptest.NewServer(counter.Handler())
			defer server.Close()
			var output bytes.Buffer
			err = run(context.Background(), []string{"-endpoint", server.URL, "-action", action, "-expected", stateJSON(t, wrong)}, &output, io.Discard)
			if err == nil || !strings.Contains(err.Error(), "HTTP 409") || output.Len() != 0 {
				t.Fatalf("conflicting state: %v, stdout=%q", err, output.String())
			}
			if counter.State() != before {
				t.Fatal("conflicting command changed state")
			}
			select {
			case <-counter.Done():
				t.Fatal("conflicting stop terminated counter")
			default:
			}
		})
	}
}

func TestRunDoesNotFollowRedirectOrProxy(t *testing.T) {
	var redirected atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redirected.Add(1)
		fmt.Fprintln(w, validExpected)
	}))
	defer target.Close()
	t.Setenv("HTTP_PROXY", target.URL)
	t.Setenv("ALL_PROXY", target.URL)
	t.Setenv("NO_PROXY", "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/state", http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	var output bytes.Buffer
	err := run(context.Background(), []string{"-endpoint", server.URL, "-action", "state"}, &output, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "HTTP 307") || output.Len() != 0 || redirected.Load() != 0 {
		t.Fatalf("redirect or proxy followed: error=%v stdout=%q calls=%d", err, output.String(), redirected.Load())
	}
}

func TestRunCancellationAndTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	for _, canceled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		want := context.DeadlineExceeded
		if canceled {
			cancel()
			want = context.Canceled
		}
		var output bytes.Buffer
		start := time.Now()
		err := run(ctx, []string{"-endpoint", server.URL, "-action", "state", "-timeout", "25ms"}, &output, io.Discard)
		cancel()
		if !errors.Is(err, want) || output.Len() != 0 || time.Since(start) > time.Second {
			t.Fatalf("request not bounded/canceled: error=%v stdout=%q elapsed=%s", err, output.String(), time.Since(start))
		}
	}
}

func TestRunResponseAndOutputErrors(t *testing.T) {
	for _, body := range []string{"{}", validExpected + "{}", strings.Repeat(" ", maxExpectedBytes+1)} {
		t.Run(fmt.Sprintf("response bytes %d", len(body)), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, body)
			}))
			defer server.Close()
			var output bytes.Buffer
			err := run(context.Background(), []string{"-endpoint", server.URL, "-action", "state"}, &output, io.Discard)
			if err == nil || output.Len() != 0 {
				t.Fatalf("invalid server state emitted: error=%v stdout=%q", err, output.String())
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, validExpected)
	}))
	defer server.Close()
	err := run(context.Background(), []string{"-endpoint", server.URL, "-action", "state"}, failingWriter{}, io.Discard)
	if !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("output failure lost: %v", err)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func stateJSON(t *testing.T, state cooperative.State) string {
	t.Helper()
	data, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
