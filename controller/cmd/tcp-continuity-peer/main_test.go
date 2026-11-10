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
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/cooperative"
	"github.com/gke-labs/pod-migration/controller/internal/tcpcontinuity"
)

func TestConfiguration(t *testing.T) {
	args := []string{"-target", "127.0.0.1:9090", "-duration", "1m", "-max-silence", "10s"}
	cfg, err := parseConfig(args, io.Discard)
	if err != nil || cfg.Target != "127.0.0.1:9090" || cfg.Duration != time.Minute || cfg.MaxSilence != 10*time.Second || cfg.MaxFrameGap != 0 || cfg.Bidirectional {
		t.Fatalf("flags: %+v, %v", cfg, err)
	}
	cfg, err = parseConfig(append(args, "-bidirectional"), io.Discard)
	if err != nil || !cfg.Bidirectional {
		t.Fatalf("bidirectional opt-in flag: %+v, %v", cfg, err)
	}
	for _, budget := range []string{"0", "1s", "11s"} {
		cfg, err = parseConfig(append(args, "-max-frame-gap", budget), io.Discard)
		want, _ := time.ParseDuration(budget)
		if err != nil || cfg.MaxFrameGap != want || cfg.MaxSilence != 10*time.Second {
			t.Fatalf("independent frame-gap budget: %+v, %v", cfg, err)
		}
	}
	for _, args := range [][]string{
		nil,
		{"-target", "127.0.0.1:9090"},
		{"-target", "127.0.0.1:9090", "-duration", "1s"},
		{"-target", "127.0.0.1:9090", "-duration", "1s", "-max-silence", "0"},
		{"-target", "127.0.0.1:9090", "-duration", "-1s", "-max-silence", "1s"},
		{"-target", "127.0.0.1:9090", "-duration", "bad", "-max-silence", "1s"},
		{"-target", "127.0.0.1:9090", "-duration", "1s", "-max-silence", "1s", "-max-frame-gap", "-1ns"},
		{"-target", "127.0.0.1:9090", "-duration", "1s", "-max-silence", "1s", "-max-frame-gap", "bad"},
		{"-target", "localhost:9090", "-duration", "1s", "-max-silence", "1s"},
		{"-target", ":9090", "-duration", "1s", "-max-silence", "1s"},
		{"-target", "0.0.0.0:9090", "-duration", "1s", "-max-silence", "1s"},
		{"-target", "127.0.0.1:0", "-duration", "1s", "-max-silence", "1s"},
		{"-unknown"},
		{"unexpected"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
	var help bytes.Buffer
	if _, err := parseConfig([]string{"-help"}, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	for _, text := range []string{"lab-only", "exactly once", "same socket", "never reconnects", "migrationProven is always false", "No IP movement", "Bidirectional", "starting at 1", "max-frame-gap", "0 disables", "not immediately"} {
		if !strings.Contains(help.String(), text) {
			t.Fatalf("help missing %q", text)
		}
	}
}

func TestRunEmitsHonestJSONForSuccessAndFailure(t *testing.T) {
	for _, test := range []struct {
		active        bool
		bidirectional bool
	}{
		{false, false}, {true, false}, {false, true}, {true, true},
	} {
		active := test.active
		name := "silent fails"
		if active {
			name = "active completes"
		}

		if test.bidirectional {
			name += "/bidirectional"
		}
		t.Run(name, func(t *testing.T) {
			counter, err := cooperative.New()
			if err != nil {
				t.Fatal(err)
			}
			if active {
				if _, err := counter.Resume(counter.State()); err != nil {
					t.Fatal(err)
				}
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() {
				done <- counter.ServeTCP(ctx, listener, cooperative.TCPOptions{
					Interval: 2 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, Bidirectional: test.bidirectional,
				})
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(time.Second):
					t.Error("source server leaked")
				}
			})
			var output bytes.Buffer
			args := []string{
				"-target", listener.Addr().String(), "-duration", "150ms", "-max-silence", "100ms",
			}
			mode := "direct-single-tcp-connection"
			if test.bidirectional {
				args = append(args, "-bidirectional")
				mode += "-bidirectional"
			}
			err = run(context.Background(), args, &output, io.Discard)
			var result tcpcontinuity.Result
			if decodeErr := json.Unmarshal(output.Bytes(), &result); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			if result.MigrationProven || result.StartTuple == nil || result.EndTuple == nil || *result.StartTuple != *result.EndTuple ||
				result.ConnectionMode != mode {
				t.Fatalf("misleading JSON: %s", output.String())
			}
			if active {
				if err != nil || result.Outcome != "observation-complete" || result.ObservedFrames == 0 {
					t.Fatalf("valid stream rejected: %s, %v", output.String(), err)
				}
			} else if err == nil || result.Outcome != "failed" || result.Error == "" {
				t.Fatalf("silent source passed: %s, %v", output.String(), err)
			}
		})
	}
	if err := run(context.Background(), nil, io.Discard, io.Discard); err == nil {
		t.Fatal("missing arguments accepted")
	}
}

func TestRunReportsFrameGapBudgetJSON(t *testing.T) {
	for _, budget := range []time.Duration{0, 20 * time.Millisecond, time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
				_, _ = io.WriteString(conn, `{"nonce":"`+strings.Repeat("ab", 32)+`","sequence":1,"counter":1}`+"\n")
				_, _ = io.Copy(io.Discard, conn)
			}()
			t.Cleanup(func() {
				_ = listener.Close()
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Error("test server leaked")
				}
			})
			var output bytes.Buffer
			err = run(context.Background(), []string{
				"-target", listener.Addr().String(), "-duration", "150ms", "-max-silence", "1s",
				"-max-frame-gap", budget.String(),
			}, &output, io.Discard)
			var result tcpcontinuity.Result
			if decodeErr := json.Unmarshal(output.Bytes(), &result); decodeErr != nil {
				t.Fatal(decodeErr)
			}
			gap, parseErr := time.ParseDuration(result.MaxObservedFrameGap)
			if parseErr != nil || int64(gap) != result.MaxObservedFrameGapNanoseconds ||
				gap < 100*time.Millisecond || gap > 150*time.Millisecond ||
				result.MaxFrameGapBudget != budget.String() || result.MaxFrameGapBudgetNanoseconds != int64(budget) ||
				result.MaxFrameGapLocation != "trailing" || result.MaxFrameGapAfterSequence != 1 ||
				result.MaxFrameGapBeforeSequence != nil || result.ObservedFrames != 1 ||
				result.EndedAt.Sub(result.StartedAt) < 150*time.Millisecond || result.MigrationProven {
				t.Fatalf("incorrect JSON metrics: %s, %v", output.String(), err)
			}
			if budget == 20*time.Millisecond {
				if !errors.Is(err, tcpcontinuity.ErrFrameGapBudgetExceeded) || result.Outcome != "failed" ||
					result.Error == "" || result.FrameGapBudgetMet == nil || *result.FrameGapBudgetMet {
					t.Fatalf("missed budget did not fail CLI: %s, %v", output.String(), err)
				}
			} else if err != nil || result.Outcome != "observation-complete" ||
				(budget == 0 && result.FrameGapBudgetMet != nil) ||
				(budget > 0 && (result.FrameGapBudgetMet == nil || !*result.FrameGapBudgetMet)) {
				t.Fatalf("disabled or met budget failed CLI: %s, %v", output.String(), err)
			}
		})
	}
}
