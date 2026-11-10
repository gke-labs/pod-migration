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
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// Each test server accepts one connection. A queued second connection is an
// unexpected redial, even if the peer already closed it.
func peerServer(t *testing.T, serve func(net.Conn)) *net.TCPListener {
	t.Helper()
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
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
		serve(conn)
	}()
	t.Cleanup(func() {
		_ = listener.SetDeadline(time.Now().Add(10 * time.Millisecond))
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("test server leaked")
		}
		_ = listener.SetDeadline(time.Now().Add(10 * time.Millisecond))
		if conn, err := listener.Accept(); err == nil {
			_ = conn.Close()
			t.Error("peer redialed")
		}
		_ = listener.Close()
	})
	return listener
}

func streamFrames(conn net.Conn) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for sequence := uint64(1); ; sequence++ {
		if _, err := io.WriteString(conn, frameLine(sequence, sequence)); err != nil {
			return
		}
		<-ticker.C
	}
}

func TestObservationCompletesOnlyAtRequestedDuration(t *testing.T) {
	listener := peerServer(t, streamFrames)
	cfg := Config{Target: listener.Addr().String(), Duration: 150 * time.Millisecond, MaxSilence: 100 * time.Millisecond}
	start := time.Now()
	result, err := Observe(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < cfg.Duration || result.EndedAt.Sub(result.StartedAt) < cfg.Duration {
		t.Fatal("observation completed prematurely")
	}
	if result.Outcome != "observation-complete" || result.ObservedFrames < 2 || result.MigrationProven ||
		result.ConnectionMode != "direct-single-tcp-connection" || result.StartTuple == nil || result.EndTuple == nil ||
		*result.StartTuple != *result.EndTuple || result.Nonce != testNonce || result.LastSequence != result.ObservedFrames {
		t.Fatalf("misleading result: %+v", result)
	}
}

func TestPeerRejectsBrokenStreamWithoutRedial(t *testing.T) {
	for name, behavior := range map[string]struct {
		serve func(net.Conn)
		error string
	}{
		"EOF": {func(conn net.Conn) { _, _ = io.WriteString(conn, frameLine(1, 7)) }, "EOF"},
		"reset": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7))
			_ = conn.(*net.TCPConn).SetLinger(0)
		}, "reset"},
		"gap": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7)+frameLine(3, 8))
		}, "sequence gap"},
		"repeat": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7)+frameLine(1, 8))
		}, "sequence gap"},
		"initial gap": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(2, 7))
		}, "sequence gap"},
		"counter regression": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7)+frameLine(2, 6))
		}, "counter regressed"},
		"cold nonce": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7)+strings.Replace(frameLine(2, 8), testNonce, strings.Repeat("cd", 32), 1))
		}, "nonce changed"},
		"malformed": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7)+"not-json\n")
		}, "malformed frame"},
		"oversized": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, strings.Repeat("a", MaxFrameBytes+2))
		}, "TCP stream failed"},
		"truncated": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, strings.TrimSuffix(frameLine(1, 7), "\n"))
		}, "EOF"},
		"silent": {func(conn net.Conn) {
			_, _ = io.Copy(io.Discard, conn)
		}, "timeout"},
		"silence after frame": {func(conn net.Conn) {
			_, _ = io.WriteString(conn, frameLine(1, 7))
			_, _ = io.Copy(io.Discard, conn)
		}, "timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			listener := peerServer(t, behavior.serve)
			cfg := Config{Target: listener.Addr().String(), Duration: time.Second, MaxSilence: 100 * time.Millisecond}
			result, err := Observe(context.Background(), cfg)
			if err == nil || !strings.Contains(err.Error(), behavior.error) {
				t.Fatalf("stream failure not reported: %+v, %v", result, err)
			}
			if result.Outcome != "failed" || result.Error == "" || result.MigrationProven {
				t.Fatalf("invalid failure result: %+v", result)
			}
			if result.StartTuple == nil || result.EndTuple == nil {
				// An immediate reset can arrive before DialContext returns.
				if name != "reset" || result.StartTuple != nil || result.EndTuple != nil {
					t.Fatalf("missing connection tuple: %+v", result)
				}
			} else if *result.StartTuple != *result.EndTuple {
				t.Fatalf("connection tuple changed: %+v", result)
			}
		})
	}
}

func TestNoFramesOrPartialFinalFrameCannotPass(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "no frames"
		if partial {
			name = "partial final frame"
		}
		t.Run(name, func(t *testing.T) {
			listener := peerServer(t, func(conn net.Conn) {
				if partial {
					_, _ = io.WriteString(conn, frameLine(1, 7)+`{"nonce":`)
				}
				_, _ = io.Copy(io.Discard, conn)
			})
			result, err := Observe(context.Background(), Config{
				Target: listener.Addr().String(), Duration: 50 * time.Millisecond, MaxSilence: time.Second,
			})
			if err == nil || result.Outcome != "failed" || result.MigrationProven {
				t.Fatalf("invalid end of observation accepted: %+v, %v", result, err)
			}
		})
	}
}

func TestPeerCancellationAndDialFailure(t *testing.T) {
	connected := make(chan struct{})
	listener := peerServer(t, func(conn net.Conn) {
		close(connected)
		_, _ = io.Copy(io.Discard, conn)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-connected
		cancel()
	}()
	start := time.Now()
	result, err := Observe(ctx, Config{Target: listener.Addr().String(), Duration: time.Hour, MaxSilence: time.Hour})
	<-done
	if !errors.Is(err, context.Canceled) || result.Outcome != "failed" || time.Since(start) > time.Second {
		t.Fatalf("read did not cancel promptly: %+v, %v", result, err)
	}

	unused, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	target := unused.Addr().String()
	_ = unused.Close()
	result, err = Observe(context.Background(), Config{Target: target, Duration: time.Second, MaxSilence: time.Second})
	if err == nil || result.StartTuple != nil || result.EndTuple != nil || result.ObservedFrames != 0 {
		t.Fatalf("failed dial misreported: %+v, %v", result, err)
	}
}

func TestInvalidConfigDoesNotDial(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{Target: "127.0.0.1:1234", Duration: 0, MaxSilence: time.Second},
		{Target: "127.0.0.1:1234", Duration: time.Second, MaxSilence: -time.Second},
		{Target: "127.0.0.1:1234", Duration: time.Second, MaxSilence: time.Second, MaxFrameGap: -time.Nanosecond},
	} {
		result, err := Observe(context.Background(), cfg)
		if err == nil || result.StartTuple != nil || result.ObservedFrames != 0 || result.MigrationProven {
			t.Fatalf("invalid configuration accepted: %+v, %v", result, err)
		}
	}
}

func TestPartialBytesDoNotResetSilenceBudget(t *testing.T) {
	listener := peerServer(t, func(conn net.Conn) {
		if _, err := io.WriteString(conn, frameLine(1, 7)); err != nil {
			return
		}
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			if _, err := io.WriteString(conn, " "); err != nil {
				return
			}
			<-ticker.C
		}
	})
	start := time.Now()
	result, err := Observe(context.Background(), Config{
		Target: listener.Addr().String(), Duration: time.Second, MaxSilence: 100 * time.Millisecond,
	})
	if err == nil || !strings.Contains(err.Error(), "timeout") || result.ObservedFrames != 1 ||
		result.Outcome != "failed" || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("partial bytes extended silence budget: %+v, %v", result, err)
	}
}
