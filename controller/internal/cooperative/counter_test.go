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
	"errors"
	"sync"
	"testing"
)

func newCounter(t *testing.T) *Counter {
	t.Helper()
	c, err := New()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func requireResume(t *testing.T, c *Counter, expected State) State {
	t.Helper()
	state, err := c.Resume(expected)
	if err != nil {
		t.Fatal(err)
	}
	want := expected
	want.Paused = false
	if state != want {
		t.Fatalf("resume = %+v, want %+v", state, want)
	}
	return state
}

func requirePause(t *testing.T, c *Counter) State {
	t.Helper()
	state, err := c.Pause(PauseRequest{Nonce: c.State().Nonce, MigrationID: "migration-1"})
	if err != nil {
		t.Fatal(err)
	}
	if !state.Paused || state.Epoch != 1 || state.MigrationID != "migration-1" {
		t.Fatalf("unexpected paused state: %+v", state)
	}
	return state
}

func TestColdStartAndIdentity(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	if len(initial.Nonce) != 64 || initial.Counter != 0 || !initial.Paused || initial.Epoch != 0 || initial.MigrationID != "" {
		t.Fatalf("unexpected cold state: %+v", initial)
	}
	for range 100 {
		c.Tick()
	}
	if c.State() != initial {
		t.Fatal("cold counter advanced without explicit resume")
	}
	other := newCounter(t)
	if other.State().Nonce == initial.Nonce {
		t.Fatal("independent cold starts have the same nonce")
	}
	captured := requirePause(t, c)
	for _, expected := range []State{initial, captured} {
		if _, err := other.Resume(expected); !errors.Is(err, ErrConflict) {
			t.Fatalf("fresh process accepted another process's state: %v", err)
		}
	}
	if other.State().Counter != 0 || !other.State().Paused {
		t.Fatal("identity mismatch changed cold state")
	}
}

func TestPauseResumeReceipts(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	c.Tick()
	if state, err := c.Resume(initial); err != nil || state.Counter != 1 || state.Paused {
		t.Fatalf("cold activation retry failed after progress: %+v, %v", state, err)
	}
	captured := requirePause(t, c)
	request := PauseRequest{Nonce: captured.Nonce, MigrationID: captured.MigrationID}
	for range 10 {
		c.Tick()
		state, err := c.Pause(request)
		if err != nil || state != captured {
			t.Fatalf("paused retry changed capture: %+v, %v", state, err)
		}
	}
	if _, err := c.Resume(initial); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale epoch-zero activation accepted: %v", err)
	}
	if c.State() != captured {
		t.Fatal("stale activation changed paused state")
	}
	requireResume(t, c, captured)
	c.Tick()
	active := c.State()
	for range 10 {
		if state, err := c.Resume(captured); err != nil || state != active {
			t.Fatalf("activation retry failed after progress: %+v, %v", state, err)
		}
		if state, err := c.Pause(request); err != nil || state != active {
			t.Fatalf("pause retry re-paused activated destination: %+v, %v", state, err)
		}
	}
	for _, expected := range []State{initial, active} {
		if _, err := c.Resume(expected); err == nil {
			t.Fatalf("unrelated activation accepted: %+v", expected)
		}
	}
	for _, request := range []PauseRequest{
		{Nonce: captured.Nonce, MigrationID: "migration-2"},
		{Nonce: "wrong", MigrationID: captured.MigrationID},
		{MigrationID: captured.MigrationID},
		{Nonce: captured.Nonce},
		{Nonce: captured.Nonce, MigrationID: "  "},
	} {
		if _, err := c.Pause(request); err == nil {
			t.Fatalf("unrelated pause accepted: %+v", request)
		}
		if c.State() != active {
			t.Fatal("rejected pause changed state")
		}
	}
}

func TestExpectedStateMustMatch(t *testing.T) {
	changes := map[string]func(*State){
		"empty nonce":     func(s *State) { s.Nonce = "" },
		"wrong nonce":     func(s *State) { s.Nonce = "other" },
		"wrong counter":   func(s *State) { s.Counter++ },
		"not paused":      func(s *State) { s.Paused = false },
		"stale epoch":     func(s *State) { s.Epoch-- },
		"future epoch":    func(s *State) { s.Epoch++ },
		"wrong migration": func(s *State) { s.MigrationID = "other" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			c := newCounter(t)
			captured := requirePause(t, c)
			invalid := captured
			change(&invalid)
			for _, operation := range []func(State) (State, error){c.Resume, c.Stop} {
				if _, err := operation(invalid); err == nil {
					t.Fatal("mismatched state accepted")
				}
				if c.State() != captured {
					t.Fatal("mismatch changed state")
				}
			}
			select {
			case <-c.Done():
				t.Fatal("invalid stop signaled shutdown")
			default:
			}
			requireResume(t, c, captured)
			c.Tick()
			active := c.State()
			if _, err := c.Resume(invalid); err == nil || c.State() != active {
				t.Fatal("unrelated request accepted as an activation retry")
			}
		})
	}
}

func TestStopIsPermanent(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	if _, err := c.Stop(initial); !errors.Is(err, ErrConflict) {
		t.Fatalf("stop accepted a running counter: %v", err)
	}
	c.Tick()
	captured := requirePause(t, c)
	for range 3 {
		if state, err := c.Stop(captured); err != nil || state != captured {
			t.Fatalf("exact stop or retry failed: %+v, %v", state, err)
		}
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("successful stop did not signal shutdown")
	}
	for range 100 {
		c.Tick()
	}
	if _, err := c.Resume(captured); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped counter resumed: %v", err)
	}
	if _, err := c.Pause(PauseRequest{Nonce: captured.Nonce, MigrationID: captured.MigrationID}); !errors.Is(err, ErrStopped) {
		t.Fatalf("stopped counter accepted pause: %v", err)
	}
	if c.State() != captured {
		t.Fatal("stopped counter changed")
	}
}

func TestConcurrentTicksAndPauseBoundary(t *testing.T) {
	c := newCounter(t)
	requireResume(t, c, c.State())
	const workers, iterations = 8, 1000
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				c.Tick()
				c.State()
			}
		}()
	}
	wg.Wait()
	if got := c.State().Counter; got != workers*iterations {
		t.Fatalf("lost concurrent increments: %d", got)
	}
	start := make(chan struct{})
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range iterations {
				c.Tick()
			}
		}()
	}
	close(start)
	captured := requirePause(t, c)
	wg.Wait()
	if c.State() != captured {
		t.Fatal("write occurred beyond pause's exact capture boundary")
	}
	requireResume(t, c, captured)
	request := PauseRequest{Nonce: captured.Nonce, MigrationID: captured.MigrationID}
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range iterations {
				if state, err := c.Pause(request); err != nil || state.Paused {
					t.Errorf("delayed pause retry: %+v, %v", state, err)
					return
				}
				if state, err := c.Resume(captured); err != nil || state.Paused {
					t.Errorf("delayed resume retry: %+v, %v", state, err)
					return
				}
				c.Tick()
			}
		}()
	}
	wg.Wait()
	if got := c.State(); got.Counter != captured.Counter+workers*iterations || got.Epoch != captured.Epoch || got.Paused {
		t.Fatalf("concurrent retries disturbed activation: %+v", got)
	}
}

func TestStopResumeRace(t *testing.T) {
	for range 50 {
		c := newCounter(t)
		captured := requirePause(t, c)
		start := make(chan struct{})
		var resumeErr, stopErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, resumeErr = c.Resume(captured)
		}()
		go func() {
			defer wg.Done()
			<-start
			_, stopErr = c.Stop(captured)
		}()
		close(start)
		wg.Wait()
		if (resumeErr == nil) == (stopErr == nil) {
			t.Fatalf("exactly one transition must win: resume=%v, stop=%v", resumeErr, stopErr)
		}
		if stopErr == nil {
			c.Tick()
			if c.State() != captured {
				t.Fatal("write after concurrent stop")
			}
			if _, err := c.Resume(captured); !errors.Is(err, ErrStopped) {
				t.Fatal("stop was reversible")
			}
		} else {
			c.Tick()
			if c.State().Counter != captured.Counter+1 {
				t.Fatal("failed stop fenced an activated counter")
			}
		}
	}
}
