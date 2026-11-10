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
	"sync/atomic"
	"testing"
	"time"
)

func expectRequest(t *testing.T, reader *bufio.Reader, sequence uint64) bool {
	t.Helper()
	line, err := reader.ReadSlice('\n')
	if err != nil || string(line) != strconv.FormatUint(sequence, 10)+"\n" {
		t.Errorf("unexpected request after response %d: %q, %v", sequence-1, line, err)
		return false
	}
	return true
}

func TestBidirectionalRoundTripsHaveOneOutstandingRequest(t *testing.T) {
	var requests atomic.Uint64
	listener := peerServer(t, func(conn net.Conn) {
		reader := bufio.NewReaderSize(conn, 22)
		for sequence := uint64(1); ; sequence++ {
			line, err := reader.ReadSlice('\n')
			if err != nil {
				return
			}
			if string(line) != strconv.FormatUint(sequence, 10)+"\n" {
				t.Errorf("unexpected request: %q, want %d", line, sequence)
				return
			}
			requests.Add(1)
			// Withhold each response long enough to detect eager requests.
			_ = conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
			if data, err := reader.ReadSlice('\n'); err == nil || len(data) != 0 {
				t.Errorf("peer sent a request before its response: %q, %v", data, err)
				return
			} else if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			if _, err := io.WriteString(conn, frameLine(sequence, sequence)); err != nil {
				return
			}
		}
	})
	cfg := Config{
		Target: listener.Addr().String(), Duration: 150 * time.Millisecond, MaxSilence: 100 * time.Millisecond,
		Bidirectional: true,
	}
	result, err := Observe(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != "observation-complete" || result.ObservedFrames < 2 || result.MigrationProven ||
		result.ConnectionMode != "direct-single-tcp-connection-bidirectional" ||
		result.StartTuple == nil || result.EndTuple == nil || *result.StartTuple != *result.EndTuple ||
		result.Nonce != testNonce || result.LastSequence != result.ObservedFrames ||
		result.EndedAt.Sub(result.StartedAt) < cfg.Duration {
		t.Fatalf("misleading bidirectional result: %+v", result)
	}
	if sent := requests.Load(); sent < result.ObservedFrames || sent > result.ObservedFrames+1 {
		t.Fatalf("more than one outstanding request: sent=%d, result=%+v", sent, result)
	}
}

func TestBidirectionalPeerRejectsFailuresWithoutRedial(t *testing.T) {
	for name, test := range map[string]struct {
		response string
		reset    bool
		silent   bool
		want     string
	}{
		"EOF":                {want: "EOF"},
		"reset":              {reset: true, want: "reset"},
		"gap":                {response: frameLine(3, 11), want: "sequence gap"},
		"repeat":             {response: frameLine(1, 11), want: "sequence gap"},
		"counter regression": {response: frameLine(2, 9), want: "counter regressed"},
		"nonce drift":        {response: strings.Replace(frameLine(2, 11), testNonce, strings.Repeat("cd", 32), 1), want: "nonce changed"},
		"malformed":          {response: "not-json\n", want: "malformed frame"},
		"oversized":          {response: strings.Repeat("a", MaxFrameBytes+2), want: "TCP stream failed"},
		"truncated":          {response: strings.TrimSuffix(frameLine(2, 11), "\n"), want: "EOF"},
		"silence":            {silent: true, want: "timeout"},
		"partial silence":    {response: `{"nonce":`, silent: true, want: "timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				reader := bufio.NewReaderSize(conn, 22)
				if !expectRequest(t, reader, 1) {
					return
				}
				if _, err := io.WriteString(conn, frameLine(1, 10)); err != nil {
					t.Error(err)
					return
				}
				if !expectRequest(t, reader, 2) {
					return
				}
				if test.reset {
					if err := conn.(*net.TCPConn).SetLinger(0); err != nil {
						t.Error(err)
					}
					return
				}
				if test.response != "" {
					_, _ = io.WriteString(conn, test.response)
				}
				if test.silent {
					_, _ = io.Copy(io.Discard, conn)
				}
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: time.Second, MaxSilence: 100 * time.Millisecond,
				Bidirectional: true,
			})
			if err == nil || !strings.Contains(err.Error(), test.want) || result.Outcome != "failed" ||
				result.MigrationProven || result.Error == "" || result.ObservedFrames != 1 ||
				result.LastSequence != 1 || result.StartTuple == nil || result.EndTuple == nil ||
				*result.StartTuple != *result.EndTuple {
				t.Fatalf("bidirectional failure masked: %+v, %v", result, err)
			}
			// peerServer's cleanup checks the still-listening socket for redials.
		})
	}
}

func TestBidirectionalPeerCancellationWhileRequestPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	listener := peerServer(t, func(conn net.Conn) {
		if !expectRequest(t, bufio.NewReaderSize(conn, 22), 1) {
			return
		}
		cancel()
		_, _ = io.Copy(io.Discard, conn)
	})
	start := time.Now()
	result, err := Observe(ctx, Config{
		Target: listener.Addr().String(), Duration: time.Hour, MaxSilence: time.Hour, Bidirectional: true,
	})
	if !errors.Is(err, context.Canceled) || result.Outcome != "failed" || result.MigrationProven ||
		result.ObservedFrames != 0 || time.Since(start) > time.Second {
		t.Fatalf("pending request did not cancel: %+v, %v", result, err)
	}
}

func TestBidirectionalIncompleteObservationFails(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(strconv.FormatBool(partial), func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				reader := bufio.NewReaderSize(conn, 22)
				if !expectRequest(t, reader, 1) {
					return
				}
				if partial {
					if _, err := io.WriteString(conn, frameLine(1, 10)); err != nil {
						return
					}
					if !expectRequest(t, reader, 2) {
						return
					}
					_, _ = io.WriteString(conn, `{"nonce":`)
				}
				_, _ = io.Copy(io.Discard, conn)
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: 50 * time.Millisecond, MaxSilence: time.Second,
				Bidirectional: true,
			})
			if err == nil || result.Outcome != "failed" || result.MigrationProven {
				t.Fatalf("incomplete observation passed: %+v, %v", result, err)
			}
		})
	}
}

func TestBidirectionalPendingResponseGapAtObservationBoundary(t *testing.T) {
	for _, budget := range []time.Duration{0, 30 * time.Millisecond} {
		t.Run(budget.String(), func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				reader := bufio.NewReader(conn)
				if !expectRequest(t, reader, 1) {
					return
				}
				if _, err := io.WriteString(conn, frameLine(1, 1)); err != nil {
					return
				}
				if !expectRequest(t, reader, 2) {
					return
				}
				_, _ = io.Copy(io.Discard, conn)
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: 150 * time.Millisecond, MaxSilence: time.Second,
				MaxFrameGap: budget, Bidirectional: true,
			})
			if budget == 0 {
				if err != nil || result.Outcome != "observation-complete" || result.FrameGapBudgetMet != nil {
					t.Fatalf("pending response changed default completion behavior: %+v, %v", result, err)
				}
			} else if !errors.Is(err, ErrFrameGapBudgetExceeded) || result.Outcome != "failed" ||
				result.FrameGapBudgetMet == nil || *result.FrameGapBudgetMet {
				t.Fatalf("pending response silence escaped the budget: %+v, %v", result, err)
			}
			if result.ObservedFrames != 1 || result.MaxFrameGapLocation != "trailing" ||
				result.MaxFrameGapAfterSequence != 1 || result.MaxFrameGapBeforeSequence != nil ||
				result.MaxObservedFrameGapNanoseconds < int64(100*time.Millisecond) ||
				result.MaxObservedFrameGapNanoseconds > int64(150*time.Millisecond) ||
				result.EndedAt.Sub(result.StartedAt) < 150*time.Millisecond || result.MigrationProven {
				t.Fatalf("pending response was counted or trailing silence omitted: %+v", result)
			}
		})
	}
}

type requestWriteConn struct {
	net.Conn
	deadlineErr error
	writes      int
}

func (c *requestWriteConn) SetWriteDeadline(deadline time.Time) error {
	if c.deadlineErr != nil {
		return c.deadlineErr
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *requestWriteConn) Write(data []byte) (int, error) {
	c.writes++
	return len(data) - 1, nil
}

func TestRequestWriteErrorsAndDeadline(t *testing.T) {
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	deadlineFailure := errors.New("deadline unavailable")
	for _, test := range []struct {
		name        string
		deadlineErr error
		want        error
		writes      int
	}{
		{"deadline", deadlineFailure, deadlineFailure, 0},
		{"short write", nil, io.ErrShortWrite, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := &requestWriteConn{Conn: source, deadlineErr: test.deadlineErr}
			err := writeRequest(context.Background(), conn, 1, time.Now().Add(time.Second))
			if !errors.Is(err, test.want) || conn.writes != test.writes {
				t.Fatalf("request error ignored or retried: %v, writes=%d", err, conn.writes)
			}
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := writeRequest(ctx, source, 1, time.Now().Add(time.Hour))
	var timeout net.Error
	if !(errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &timeout) && timeout.Timeout())) ||
		time.Since(start) > time.Second {
		t.Fatalf("request write exceeded context deadline: %v", err)
	}
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	conn := &requestWriteConn{Conn: source}
	if err := writeRequest(cancelled, conn, 1, time.Now().Add(time.Second)); !errors.Is(err, context.Canceled) || conn.writes != 0 {
		t.Fatalf("canceled request wrote bytes: %v, writes=%d", err, conn.writes)
	}
}
