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

package tcpcontinuity

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestFrameGapAccounting(t *testing.T) {
	start := time.Now()
	for _, test := range []struct {
		name          string
		frames        []time.Duration
		finish        time.Duration
		budget        time.Duration
		wantGap       time.Duration
		location      string
		after, before uint64
	}{
		{"initial", []time.Duration{3 * time.Second, 4 * time.Second}, 5 * time.Second, 3 * time.Second, 3 * time.Second, "initial", 0, 1},
		{"between", []time.Duration{time.Second, 5 * time.Second, 6 * time.Second}, 7 * time.Second, 4 * time.Second, 4 * time.Second, "between-frames", 1, 2},
		{"trailing", []time.Duration{time.Second, 2 * time.Second}, 8 * time.Second, 6 * time.Second, 6 * time.Second, "trailing", 2, 0},
		{"no frames", nil, 4 * time.Second, 3 * time.Second, 4 * time.Second, "no-valid-frames", 0, 0},
		{"early failure", []time.Duration{time.Second}, 3 * time.Second, 2 * time.Second, 2 * time.Second, "trailing", 1, 0},
		{"immediate cancellation", nil, time.Nanosecond, time.Second, time.Nanosecond, "no-valid-frames", 0, 0},
		{"trailing bounded by observation", []time.Duration{2 * time.Second, 3 * time.Second}, 15 * time.Second, 7 * time.Second, 7 * time.Second, "trailing", 2, 0},
		{"no frames exact boundary", nil, 15 * time.Second, 10 * time.Second, 10 * time.Second, "no-valid-frames", 0, 0},
		{"frame exactly at boundary", []time.Duration{time.Second, 10 * time.Second}, 15 * time.Second, 9 * time.Second, 9 * time.Second, "between-frames", 1, 2},
		{"buffered frame after boundary", []time.Duration{time.Second, 15 * time.Second}, 20 * time.Second, 9 * time.Second, 9 * time.Second, "trailing", 1, 0},
		{"first frame after boundary", []time.Duration{15 * time.Second}, 20 * time.Second, 10 * time.Second, 10 * time.Second, "no-valid-frames", 0, 0},
		{"first maximum wins ties", []time.Duration{2 * time.Second, 4 * time.Second}, 6 * time.Second, 2 * time.Second, 2 * time.Second, "initial", 0, 1},
		{"one nanosecond over budget", []time.Duration{time.Second + time.Nanosecond}, 2 * time.Second, time.Second, time.Second + time.Nanosecond, "initial", 0, 1},
		{"disabled still measures", nil, 15 * time.Second, 0, 10 * time.Second, "no-valid-frames", 0, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			gaps := frameGapTracker{lastFrameAt: start, observationEnd: start.Add(10 * time.Second)}
			for i, at := range test.frames {
				gaps.validFrame(start.Add(at), uint64(i+1))
			}
			var result Result
			err := gaps.finish(start.Add(test.finish), &result, test.budget)
			if result.MaxObservedFrameGap != test.wantGap.String() ||
				result.MaxObservedFrameGapNanoseconds != int64(test.wantGap) ||
				result.MaxFrameGapLocation != test.location || result.MaxFrameGapAfterSequence != test.after {
				t.Fatalf("incorrect gap accounting: %+v", result)
			}
			if test.before == 0 {
				if result.MaxFrameGapBeforeSequence != nil {
					t.Fatalf("silence ended with a fictitious frame: %+v", result)
				}
			} else if result.MaxFrameGapBeforeSequence == nil || *result.MaxFrameGapBeforeSequence != test.before {
				t.Fatalf("missing maximum-gap response sequence: %+v", result)
			}
			if test.budget == 0 {
				if err != nil || result.FrameGapBudgetMet != nil {
					t.Fatalf("disabled budget enforced: %+v, %v", result, err)
				}
				return
			}
			wantMet := test.wantGap <= test.budget
			if result.FrameGapBudgetMet == nil || *result.FrameGapBudgetMet != wantMet ||
				errors.Is(err, ErrFrameGapBudgetExceeded) == wantMet {
				t.Fatalf("incorrect inclusive budget boundary: %+v, %v", result, err)
			}
		})
	}
}

func TestFrameGapBudgetWaitsForObservationWithoutRedial(t *testing.T) {
	for _, bidirectional := range []bool{false, true} {
		for _, enabled := range []bool{false, true} {
			t.Run("bidirectional="+strconv.FormatBool(bidirectional)+"/budget="+strconv.FormatBool(enabled), func(t *testing.T) {
				pauseSequence := uint64(1)
				location := "initial"
				if bidirectional {
					pauseSequence = 2
					location = "between-frames"
				}
				listener := peerServer(t, func(conn net.Conn) {
					reader := bufio.NewReader(conn)
					for sequence := uint64(1); ; sequence++ {
						if bidirectional {
							line, err := reader.ReadString('\n')
							if err != nil {
								return
							}
							if line != strconv.FormatUint(sequence, 10)+"\n" {
								t.Errorf("unexpected request %q", line)
								return
							}
						}
						if sequence == pauseSequence {
							time.Sleep(120 * time.Millisecond)
						}
						if _, err := io.WriteString(conn, frameLine(sequence, sequence)); err != nil {
							return
						}
						time.Sleep(5 * time.Millisecond)
					}
				})
				cfg := Config{
					Target: listener.Addr().String(), Duration: 350 * time.Millisecond, MaxSilence: time.Second,
					Bidirectional: bidirectional,
				}
				if enabled {
					cfg.MaxFrameGap = 40 * time.Millisecond
				}
				result, err := Observe(context.Background(), cfg)
				if enabled {
					if !errors.Is(err, ErrFrameGapBudgetExceeded) || result.Outcome != "failed" ||
						result.FrameGapBudgetMet == nil || *result.FrameGapBudgetMet ||
						!strings.Contains(result.Error, ErrFrameGapBudgetExceeded.Error()) {
						t.Fatalf("missed budget not reported: %+v, %v", result, err)
					}
				} else if err != nil || result.Outcome != "observation-complete" || result.FrameGapBudgetMet != nil {
					t.Fatalf("disabled budget changed behavior: %+v, %v", result, err)
				}
				if result.EndedAt.Sub(result.StartedAt) < cfg.Duration || result.ObservedFrames <= pauseSequence+1 ||
					result.MaxObservedFrameGapNanoseconds < int64(100*time.Millisecond) ||
					result.MaxFrameGapLocation != location || result.MaxFrameGapAfterSequence != pauseSequence-1 ||
					result.MaxFrameGapBeforeSequence == nil || *result.MaxFrameGapBeforeSequence != pauseSequence ||
					result.StartTuple == nil || result.EndTuple == nil || *result.StartTuple != *result.EndTuple ||
					result.MigrationProven || result.MaxFrameGapBudget != cfg.MaxFrameGap.String() ||
					result.MaxFrameGapBudgetNanoseconds != int64(cfg.MaxFrameGap) {
					t.Fatalf("gap observation did not retain the socket and resume valid frames: %+v", result)
				}
			})
		}
	}
}

func TestFrameGapBudgetUnderBudgetLoopback(t *testing.T) {
	for _, bidirectional := range []bool{false, true} {
		t.Run(strconv.FormatBool(bidirectional), func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				if !bidirectional {
					streamFrames(conn)
					return
				}
				reader := bufio.NewReader(conn)
				for sequence := uint64(1); ; sequence++ {
					line, err := reader.ReadString('\n')
					if err != nil {
						return
					}
					if line != strconv.FormatUint(sequence, 10)+"\n" {
						t.Errorf("unexpected request %q", line)
						return
					}
					if _, err := io.WriteString(conn, frameLine(sequence, sequence)); err != nil {
						return
					}
				}
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: 200 * time.Millisecond, MaxSilence: time.Second,
				MaxFrameGap: 150 * time.Millisecond, Bidirectional: bidirectional,
			})
			if err != nil || result.Outcome != "observation-complete" || result.ObservedFrames < 2 ||
				result.FrameGapBudgetMet == nil || !*result.FrameGapBudgetMet ||
				result.MaxObservedFrameGapNanoseconds <= 0 ||
				result.MaxObservedFrameGapNanoseconds > int64(150*time.Millisecond) || result.MigrationProven {
				t.Fatalf("under-budget stream failed: %+v, %v", result, err)
			}
		})
	}
}

func TestFrameGapBudgetPreservesFailuresAndValidFrameClock(t *testing.T) {
	for _, test := range []struct {
		name, invalid, want string
		noFrames, hold      bool
	}{
		{name: "EOF", want: "EOF"},
		{name: "malformed", invalid: "not-json\n", want: "malformed frame"},
		{name: "nonce", invalid: strings.Replace(frameLine(2, 2), testNonce, strings.Repeat("cd", 32), 1), want: "nonce changed"},
		{name: "sequence", invalid: frameLine(3, 3), want: "sequence gap"},
		{name: "counter", invalid: frameLine(2, 0), want: "counter regressed"},
		{name: "partial at boundary", invalid: `{"nonce":`, want: "timeout", hold: true},
		{name: "no frames at boundary", want: "timeout", noFrames: true, hold: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				if !test.noFrames {
					_, _ = io.WriteString(conn, frameLine(1, 1))
				}
				time.Sleep(80 * time.Millisecond)
				_, _ = io.WriteString(conn, test.invalid)
				if test.hold {
					_, _ = io.Copy(io.Discard, conn)
				}
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: 140 * time.Millisecond, MaxSilence: time.Second,
				MaxFrameGap: 20 * time.Millisecond,
			})
			wantFrames, location := uint64(1), "trailing"
			if test.noFrames {
				wantFrames, location = 0, "no-valid-frames"
			}
			if !errors.Is(err, ErrFrameGapBudgetExceeded) || !strings.Contains(err.Error(), test.want) ||
				result.Outcome != "failed" || result.ObservedFrames != wantFrames ||
				result.MaxObservedFrameGapNanoseconds < int64(60*time.Millisecond) ||
				result.MaxFrameGapLocation != location || result.MaxFrameGapAfterSequence != wantFrames ||
				result.MaxFrameGapBeforeSequence != nil || result.FrameGapBudgetMet == nil ||
				*result.FrameGapBudgetMet || result.MigrationProven {
				t.Fatalf("invalid bytes reset the clock or hid a failure: %+v, %v", result, err)
			}
			if test.want == "EOF" && !errors.Is(err, io.EOF) {
				t.Fatalf("EOF identity was lost: %v", err)
			}
			var timeout net.Error
			if test.want == "timeout" && (!errors.As(err, &timeout) || !timeout.Timeout()) {
				t.Fatalf("timeout identity was lost: %v", err)
			}
		})
	}
}

func TestFrameGapBudgetPartialBytesDoNotExtendTransportSilence(t *testing.T) {
	listener := peerServer(t, func(conn net.Conn) {
		_, _ = io.WriteString(conn, frameLine(1, 1))
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if _, err := io.WriteString(conn, " "); err != nil {
				return
			}
		}
	})
	result, err := Observe(context.Background(), Config{
		Target: listener.Addr().String(), Duration: time.Second, MaxSilence: 150 * time.Millisecond,
		MaxFrameGap: 20 * time.Millisecond,
	})
	var timeout net.Error
	if !errors.Is(err, ErrFrameGapBudgetExceeded) || !errors.As(err, &timeout) || !timeout.Timeout() ||
		result.ObservedFrames != 1 || result.MaxFrameGapLocation != "trailing" ||
		result.MaxObservedFrameGapNanoseconds < int64(150*time.Millisecond) ||
		result.EndedAt.Sub(result.StartedAt) > 700*time.Millisecond {
		t.Fatalf("partial bytes reset a deadline or gap measurement: %+v, %v", result, err)
	}
}

func TestFrameGapBudgetCancellation(t *testing.T) {
	for _, budget := range []time.Duration{time.Nanosecond, time.Hour} {
		t.Run(budget.String(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			listener := peerServer(t, func(conn net.Conn) {
				if !expectRequest(t, bufio.NewReader(conn), 1) {
					return
				}
				cancel()
				_, _ = io.Copy(io.Discard, conn)
			})
			result, err := Observe(ctx, Config{
				Target: listener.Addr().String(), Duration: time.Hour, MaxSilence: time.Hour,
				MaxFrameGap: budget, Bidirectional: true,
			})
			wantMet := budget == time.Hour
			if !errors.Is(err, context.Canceled) || result.Outcome != "failed" || result.ObservedFrames != 0 ||
				result.MaxFrameGapLocation != "no-valid-frames" || result.MaxObservedFrameGapNanoseconds <= 0 ||
				result.FrameGapBudgetMet == nil || *result.FrameGapBudgetMet != wantMet ||
				errors.Is(err, ErrFrameGapBudgetExceeded) == wantMet ||
				result.EndedAt.Sub(result.StartedAt) > time.Second {
				t.Fatalf("early cancellation or budget error lost: %+v, %v", result, err)
			}
		})
	}
}

func TestFrameGapBudgetWithoutConnectionIsNotEvaluated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := Observe(ctx, Config{
		Target: "127.0.0.1:1234", Duration: time.Second, MaxSilence: time.Second, MaxFrameGap: time.Nanosecond,
	})
	if !errors.Is(err, context.Canceled) || errors.Is(err, ErrFrameGapBudgetExceeded) ||
		result.StartTuple != nil || result.FrameGapBudgetMet != nil ||
		result.MaxObservedFrameGap != "0s" || result.MaxObservedFrameGapNanoseconds != 0 ||
		result.MaxFrameGapLocation != "not-connected" || result.MaxFrameGapBeforeSequence != nil {
		t.Fatalf("unconnected observation claimed a measured budget: %+v, %v", result, err)
	}
}
