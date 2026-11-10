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

func writeTCPRequest(t *testing.T, conn net.Conn, sequence uint64) {
	t.Helper()
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(conn, strconv.FormatUint(sequence, 10)+"\n"); err != nil {
		t.Fatal(err)
	}
}

func requireTCPSilence(t *testing.T, conn net.Conn, reader *bufio.Reader) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if data, err := reader.ReadSlice('\n'); !isTimeout(err) || len(data) != 0 {
		t.Fatalf("held connection emitted bytes or closed: %q, %v", data, err)
	}
}

func TestBidirectionalHeldRequestsSurvivePause(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	listener := tcpListener(t)
	startTCPWithOptions(t, c, listener, TCPOptions{
		Interval: 2 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, Bidirectional: true,
	})
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tuple := conn.LocalAddr().String() + "/" + conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)
	writeTCPRequest(t, conn, 1)
	requireTCPSilence(t, conn, reader)
	requireResume(t, c, initial)
	c.Tick()
	first := requireTCPFrame(t, conn, reader)
	if first.Sequence != 1 || first.Nonce != initial.Nonce || first.Counter > 1 {
		t.Fatalf("invalid first response: %+v", first)
	}
	requireTCPSilence(t, conn, reader)
	writeTCPRequest(t, conn, 2)
	second := requireTCPFrame(t, conn, reader)
	if second.Sequence != 2 || second.Counter != 1 {
		t.Fatalf("invalid second response: %+v", second)
	}
	captured := requirePause(t, c)
	writeTCPRequest(t, conn, 3)
	requireTCPSilence(t, conn, reader)
	c.Tick()
	if c.State() != captured {
		t.Fatal("pending request advanced or resumed the paused counter")
	}
	requireResume(t, c, captured)
	c.Tick()
	continued := requireTCPFrame(t, conn, reader)
	if continued.Sequence != 3 || continued.Nonce != initial.Nonce ||
		continued.Counter < captured.Counter || continued.Counter > captured.Counter+1 {
		t.Fatalf("pending request or sequence lost during pause: %+v", continued)
	}
	writeTCPRequest(t, conn, 4)
	last := requireTCPFrame(t, conn, reader)
	if last.Sequence != 4 || last.Counter != captured.Counter+1 || last.Nonce != initial.Nonce {
		t.Fatalf("post-resume request failed: %+v", last)
	}
	if got := conn.LocalAddr().String() + "/" + conn.RemoteAddr().String(); got != tuple {
		t.Fatal("connection tuple changed")
	}
}

func TestBidirectionalRejectsInvalidRequests(t *testing.T) {
	for _, test := range []struct {
		name    string
		input   string
		initial bool
		want    string
	}{
		{"empty", "\n", false, "canonical"},
		{"zero", "0\n", false, "canonical"},
		{"leading zero", "01\n", false, "canonical"},
		{"signed", "+1\n", false, "canonical"},
		{"negative", "-1\n", false, "canonical"},
		{"whitespace", "1 \n", false, "non-decimal"},
		{"CRLF", "1\r\n", false, "non-decimal"},
		{"non-ASCII", "1é\n", false, "non-decimal"},
		{"overflow", "18446744073709551616\n", false, "out of range"},
		{"oversize", strings.Repeat("1", maxTCPRequestBytes+1), false, "exceeds"},
		{"oversize line", strings.Repeat("1", maxTCPRequestBytes) + "\n", false, "canonical"},
		{"initial gap", "2\n", false, "sequence gap"},
		{"duplicate", "1\n", true, "sequence gap"},
		{"gap", "3\n", true, "sequence gap"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := newCounter(t)
			requireResume(t, c, c.State())
			source, peer := net.Pipe()
			defer source.Close()
			defer peer.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- c.respondTCP(ctx, source, TCPOptions{Interval: time.Millisecond, WriteTimeout: 100 * time.Millisecond})
			}()
			if test.initial {
				writeTCPRequest(t, peer, 1)
				if frame := requireTCPFrame(t, peer, bufio.NewReader(peer)); frame.Sequence != 1 {
					t.Fatal(frame)
				}
			}
			_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := io.WriteString(peer, test.input); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), test.want) {
					t.Fatalf("bad request not rejected: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("bad request blocked the connection handler")
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("bad request received a response or was not closed: %d, %v", n, err)
			}
		})
	}
}

func TestBidirectionalRejectsPipeliningWhilePaused(t *testing.T) {
	c := newCounter(t)
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- c.respondTCP(ctx, source, TCPOptions{Interval: time.Millisecond, WriteTimeout: time.Second})
	}()
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(peer, "1\n2\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "response still pending") {
			t.Fatalf("pipelined request was accepted: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("pipelined requests blocked shutdown")
	}
}

func TestBidirectionalShutdownJoinsReaders(t *testing.T) {
	for _, input := range []string{"", "1", "1\n"} {
		for _, action := range []string{"EOF", "cancel", "stop"} {
			t.Run(action+"/"+strconv.Quote(input), func(t *testing.T) {
				c := newCounter(t)
				initial := c.State()
				source, peer := net.Pipe()
				defer source.Close()
				defer peer.Close()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					done <- c.respondTCP(ctx, source, TCPOptions{Interval: time.Millisecond, WriteTimeout: time.Second})
				}()
				if input != "" {
					_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
					if _, err := io.WriteString(peer, input); err != nil {
						t.Fatal(err)
					}
				}
				var want error
				switch action {
				case "EOF":
					_ = peer.Close()
					want = io.EOF
					if input == "1" {
						want = io.ErrUnexpectedEOF
					}
				case "cancel":
					cancel()
					want = context.Canceled
				case "stop":
					if _, err := c.Stop(initial); err != nil {
						t.Fatal(err)
					}
					want = ErrStopped
				}
				select {
				case err := <-done:
					if !errors.Is(err, want) {
						t.Fatalf("shutdown not handled: %v; want %v", err, want)
					}
				case <-time.After(time.Second):
					t.Fatal("paused connection leaked a reader on shutdown")
				}
				if c.State() != initial {
					t.Fatal("cleanup changed the application state")
				}
			})
		}
	}
}

func TestBidirectionalPartialReadCannotBlockPause(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- c.respondTCP(ctx, source, TCPOptions{Interval: time.Millisecond, WriteTimeout: time.Second})
	}()
	_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
	if _, err := io.WriteString(peer, "1"); err != nil {
		t.Fatal(err)
	}
	paused := make(chan error, 1)
	go func() {
		_, err := c.Pause(PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"})
		paused <- err
	}()
	select {
	case err := <-paused:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("network read held the execution lock")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("partial reader did not cancel")
	}
}

func TestBidirectionalWriteFailureStillBoundsPause(t *testing.T) {
	for _, short := range []bool{false, true} {
		t.Run(strconv.FormatBool(short), func(t *testing.T) {
			c := newCounter(t)
			initial := c.State()
			requireResume(t, c, initial)
			source, peer := net.Pipe()
			defer source.Close()
			defer peer.Close()
			var conn net.Conn = source
			if short {
				conn = shortWriteConn{source}
			}
			observed := &observedWriteConn{Conn: conn, started: make(chan struct{})}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- c.respondTCP(ctx, observed, TCPOptions{Interval: time.Millisecond, WriteTimeout: 100 * time.Millisecond})
			}()
			writeTCPRequest(t, peer, 1)
			select {
			case <-observed.started:
			case <-time.After(time.Second):
				t.Fatal("response write did not start")
			}
			if short {
				_ = peer.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := io.ReadFull(peer, make([]byte, 5)); err != nil {
					t.Fatal(err)
				}
			}
			paused := make(chan error, 1)
			go func() {
				_, err := c.Pause(PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"})
				paused <- err
			}()
			select {
			case err := <-paused:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("response write hung pause")
			}
			select {
			case err := <-done:
				if (short && !errors.Is(err, io.ErrShortWrite)) || (!short && !isTimeout(err)) {
					t.Fatalf("write failure was not reported: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("failed response retained the reader goroutine")
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("failed response did not close: %d, %v", n, err)
			}
		})
	}
}
