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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/tcpcontinuity"
)

func startTCP(t *testing.T, c *Counter, listener net.Listener) {
	t.Helper()
	startTCPWithOptions(t, c, listener, TCPOptions{Interval: 2 * time.Millisecond, WriteTimeout: 100 * time.Millisecond})
}

func startTCPWithOptions(t *testing.T, c *Counter, listener net.Listener, options TCPOptions) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var serveErr error
	go func() {
		defer close(done)
		serveErr = c.ServeTCP(ctx, listener, options)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if serveErr != nil {
				t.Error(serveErr)
			}
		case <-time.After(2 * time.Second):
			t.Error("TCP server did not join its goroutines")
		}
	})
}

func tcpListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func requireTCPFrame(t *testing.T, conn net.Conn, reader *bufio.Reader) tcpcontinuity.Frame {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	frame, err := tcpcontinuity.DecodeFrame(line)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestTCPSilentPauseAndSameConnectionResume(t *testing.T) {
	c := newCounter(t)
	listener := tcpListener(t)
	startTCP(t, c, listener)
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tuple := conn.LocalAddr().String() + "/" + conn.RemoteAddr().String()
	reader := bufio.NewReader(conn)
	initial := c.State()
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if data, err := reader.ReadBytes('\n'); !isTimeout(err) || len(data) != 0 {
		t.Fatalf("cold paused stream emitted data or closed: %q, %v", data, err)
	}
	if c.State() != initial {
		t.Fatal("cold paused counter changed")
	}
	requireResume(t, c, initial)
	c.Tick()
	first := requireTCPFrame(t, conn, reader)
	second := requireTCPFrame(t, conn, reader)
	if first.Nonce != initial.Nonce || first.Sequence != 1 || second.Sequence != 2 || first.Counter > 1 || second.Counter < first.Counter || second.Counter > 1 {
		t.Fatalf("unexpected frames: %+v, %+v", first, second)
	}
	captured := requirePause(t, c)
	lastSequence := second.Sequence
	// Drain any writes accepted by the kernel BEFORE Pause returned.
	_ = conn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if !isTimeout(err) || len(line) != 0 {
				t.Fatalf("paused connection closed or partial frame: %q, %v", line, err)
			}
			break
		}
		frame, err := tcpcontinuity.DecodeFrame(line)
		if err != nil || frame.Sequence != lastSequence+1 || frame.Counter != captured.Counter {
			t.Fatalf("invalid queued frame: %+v, %v", frame, err)
		}
		lastSequence = frame.Sequence
	}
	_ = conn.SetReadDeadline(time.Now().Add(40 * time.Millisecond))
	if data, err := reader.ReadBytes('\n'); !isTimeout(err) || len(data) != 0 {
		t.Fatalf("paused source did not stay silent: %q, %v", data, err)
	}
	c.Tick()
	if c.State() != captured {
		t.Fatal("paused counter advanced")
	}
	requireResume(t, c, captured)
	c.Tick()
	continued := requireTCPFrame(t, conn, reader)
	if continued.Sequence != lastSequence+1 || continued.Nonce != initial.Nonce ||
		continued.Counter < captured.Counter || continued.Counter > captured.Counter+1 {
		t.Fatalf("same-connection resume lost process sequence: %+v", continued)
	}
	if got := conn.LocalAddr().String() + "/" + conn.RemoteAddr().String(); got != tuple {
		t.Fatal("socket tuple changed")
	}
}

func isTimeout(err error) bool {
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type observedWriteConn struct {
	net.Conn
	started chan struct{}
	written chan struct{}
	start   sync.Once
	finish  sync.Once
}

func (c *observedWriteConn) Write(data []byte) (int, error) {
	if c.started != nil {
		c.start.Do(func() { close(c.started) })
	}
	n, err := c.Conn.Write(data)
	if err == nil && n == len(data) && c.written != nil {
		c.finish.Do(func() { close(c.written) })
	}
	return n, err
}

func TestPauseDrainsAnAuthorizedWrite(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	conn := &observedWriteConn{Conn: source, started: make(chan struct{})}
	var sequence uint64
	writeDone := make(chan error, 1)
	go func() {
		_, err := c.writeTCPFrame(context.Background(), conn, &sequence, time.Second)
		writeDone <- err
	}()
	<-conn.started
	pauseDone := make(chan error, 1)
	go func() {
		_, err := c.Pause(PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"})
		pauseDone <- err
	}()
	select {
	case err := <-pauseDone:
		t.Fatalf("pause returned before the active write drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	frame := requireTCPFrame(t, peer, bufio.NewReader(peer))
	if frame.Sequence != 1 {
		t.Fatal(frame)
	}
	if err := <-writeDone; err != nil {
		t.Fatal(err)
	}
	if err := <-pauseDone; err != nil {
		t.Fatal(err)
	}
	for range 10 {
		wrote, err := c.writeTCPFrame(context.Background(), conn, &sequence, time.Second)
		if wrote || err != nil || sequence != 1 {
			t.Fatalf("paused write advanced: sequence=%d, wrote=%v, err=%v", sequence, wrote, err)
		}
	}
	_ = peer.SetReadDeadline(time.Now().Add(20 * time.Millisecond))
	if n, err := peer.Read(make([]byte, 1)); n != 0 || !isTimeout(err) {
		t.Fatalf("post-pause bytes or unexpected disconnect: %d, %v", n, err)
	}
}

func TestSlowOrPartialPeerCannotDeadlockPause(t *testing.T) {
	for _, partial := range []bool{false, true} {
		name := "blocked"
		if partial {
			name = "partial"
		}
		t.Run(name, func(t *testing.T) {
			c := newCounter(t)
			initial := c.State()
			requireResume(t, c, initial)
			source, peer := net.Pipe()
			defer source.Close()
			defer peer.Close()
			conn := &observedWriteConn{Conn: source, started: make(chan struct{})}
			var sequence uint64
			writeDone := make(chan error, 1)
			go func() {
				_, err := c.writeTCPFrame(context.Background(), conn, &sequence, 100*time.Millisecond)
				writeDone <- err
			}()
			<-conn.started
			if partial {
				_ = peer.SetReadDeadline(time.Now().Add(time.Second))
				if _, err := io.ReadFull(peer, make([]byte, 5)); err != nil {
					t.Fatal(err)
				}
			}
			pauseDone := make(chan error, 1)
			start := time.Now()
			go func() {
				_, err := c.Pause(PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"})
				pauseDone <- err
			}()
			select {
			case err := <-pauseDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("blocked write hung pause")
			}
			if err := <-writeDone; !isTimeout(err) {
				t.Fatalf("unbounded or silently dropped write: %v", err)
			}
			if time.Since(start) > time.Second || sequence != 0 || !c.State().Paused {
				t.Fatalf("failed write advanced sequence or auto-resumed: %d", sequence)
			}
			_ = peer.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("failed/partial write did not close socket: %d, %v", n, err)
			}
		})
	}
}

type countedListener struct {
	net.Listener
	accepted atomic.Int64
	written  chan struct{}
}

func (l *countedListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.accepted.Add(1)
		return &observedWriteConn{Conn: conn, written: l.written}, nil
	}
	return nil, err
}

func TestStoppedSourceFailsPeerWithoutReconnect(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	listener := &countedListener{Listener: tcpListener(t), written: make(chan struct{})}
	startTCP(t, c, listener)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type observation struct {
		result tcpcontinuity.Result
		err    error
	}
	peerDone := make(chan observation, 1)
	go func() {
		result, err := tcpcontinuity.Observe(ctx, tcpcontinuity.Config{
			Target: listener.Addr().String(), Duration: time.Second, MaxSilence: 500 * time.Millisecond,
		})
		peerDone <- observation{result, err}
	}()
	select {
	case <-listener.written:
	case <-ctx.Done():
		t.Fatal("server did not emit first frame")
	}
	captured := requirePause(t, c)
	if _, err := c.Stop(captured); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-peerDone:
		if got.err == nil || !strings.Contains(got.err.Error(), "EOF") || got.result.Outcome != "failed" ||
			got.result.ObservedFrames == 0 || got.result.MigrationProven || listener.accepted.Load() != 1 {
			t.Fatalf("stopped source was masked: %+v, %v, accepted=%d", got.result, got.err, listener.accepted.Load())
		}
	case <-ctx.Done():
		t.Fatal("peer did not fail when source stopped")
	}
	if conn, err := net.DialTimeout("tcp", listener.Addr().String(), 100*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("stopped source still accepts connections")
	}
}

func TestPeerObservesCooperativePauseOnOneSocket(t *testing.T) {
	t.Run("one-way", func(t *testing.T) { testPeerObservesCooperativePause(t, false) })
	t.Run("bidirectional", func(t *testing.T) { testPeerObservesCooperativePause(t, true) })
}

func testPeerObservesCooperativePause(t *testing.T, bidirectional bool) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	c.Tick()
	listener := &countedListener{Listener: tcpListener(t), written: make(chan struct{})}
	startTCPWithOptions(t, c, listener, TCPOptions{
		Interval: 2 * time.Millisecond, WriteTimeout: 100 * time.Millisecond, Bidirectional: bidirectional,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type observation struct {
		result tcpcontinuity.Result
		err    error
	}
	done := make(chan observation, 1)
	go func() {
		result, err := tcpcontinuity.Observe(ctx, tcpcontinuity.Config{
			Target: listener.Addr().String(), Duration: 250 * time.Millisecond, MaxSilence: 150 * time.Millisecond,
			Bidirectional: bidirectional,
		})
		done <- observation{result, err}
	}()
	select {
	case <-listener.written:
	case <-ctx.Done():
		t.Fatal("no initial frame")
	}
	captured := requirePause(t, c)
	time.Sleep(40 * time.Millisecond)
	if c.State() != captured {
		t.Fatal("paused counter changed")
	}
	requireResume(t, c, captured)
	c.Tick()
	select {
	case got := <-done:
		if got.err != nil || got.result.Outcome != "observation-complete" || got.result.MigrationProven ||
			got.result.ObservedFrames < 2 || got.result.LastSequence != got.result.ObservedFrames ||
			got.result.Nonce != initial.Nonce || got.result.LastCounter != captured.Counter+1 ||
			got.result.StartTuple == nil || got.result.EndTuple == nil || *got.result.StartTuple != *got.result.EndTuple ||
			listener.accepted.Load() != 1 {
			t.Fatalf("cooperative pause observation failed: %+v, %v", got.result, got.err)
		}
	case <-ctx.Done():
		t.Fatal("peer did not complete")
	}
}

func TestPausedPeerDisconnectReleasesStream(t *testing.T) {
	for _, action := range []string{"disconnect", "unexpected input", "cancel"} {
		t.Run(action, func(t *testing.T) {
			c := newCounter(t)
			initial := c.State()
			source, peer := net.Pipe()
			defer source.Close()
			defer peer.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				c.streamTCP(ctx, source, TCPOptions{Interval: time.Hour, WriteTimeout: time.Second})
			}()
			switch action {
			case "disconnect":
				_ = peer.Close()
			case "unexpected input":
				_ = peer.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := peer.Write([]byte("x")); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("paused stream leaked after peer disconnect or cancellation")
			}
			if c.State() != initial {
				t.Fatal("stream cleanup changed paused counter")
			}
		})
	}
}

func TestTCPContextDeadlineAndServerErrors(t *testing.T) {
	c := newCounter(t)
	requireResume(t, c, c.State())
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	var sequence uint64
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.writeTCPFrame(ctx, source, &sequence, time.Second); !isTimeout(err) || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("context did not bound write: %v", err)
	}
	listener := tcpListener(t)
	_ = listener.Close()
	if err := c.ServeTCP(context.Background(), listener, TCPOptions{Interval: time.Millisecond, WriteTimeout: time.Second}); err == nil {
		t.Fatal("accept error ignored")
	}
	for _, options := range []TCPOptions{
		{},
		{Interval: -time.Second, WriteTimeout: time.Second},
		{Interval: time.Second, WriteTimeout: 0},
		{Interval: time.Second, WriteTimeout: MaxTCPWriteTimeout + 1},
	} {
		if err := c.ServeTCP(context.Background(), tcpListener(t), options); err == nil {
			t.Fatalf("invalid options accepted: %+v", options)
		}
	}
}

func TestPauseDrainsConcurrentWritesWithoutSerialTimeouts(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	const connections = 4
	writeDone := make(chan error, connections)
	for range connections {
		source, peer := net.Pipe()
		defer source.Close()
		defer peer.Close()
		conn := &observedWriteConn{Conn: source, started: make(chan struct{})}
		go func() {
			var sequence uint64
			_, err := c.writeTCPFrame(context.Background(), conn, &sequence, 200*time.Millisecond)
			writeDone <- err
		}()
		select {
		case <-conn.started:
		case <-time.After(time.Second):
			t.Fatal("parallel write did not start")
		}
	}
	select {
	case err := <-writeDone:
		t.Fatalf("one write timed out before all concurrent writes started: %v", err)
	default:
	}
	start := time.Now()
	if _, err := c.Pause(PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"}); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("pause serialized the connections' timeout budgets")
	}
	for range connections {
		if err := <-writeDone; !isTimeout(err) {
			t.Fatalf("blocked connection write did not time out: %v", err)
		}
	}
}

type shortWriteConn struct{ net.Conn }

func (c shortWriteConn) Write(data []byte) (int, error) {
	return c.Conn.Write(data[:5])
}

func TestShortWriteClosesConnectionAndStoppedNeverWrites(t *testing.T) {
	c := newCounter(t)
	initial := c.State()
	requireResume(t, c, initial)
	source, peer := net.Pipe()
	defer source.Close()
	defer peer.Close()
	var sequence uint64
	done := make(chan error, 1)
	go func() {
		_, err := c.writeTCPFrame(context.Background(), shortWriteConn{source}, &sequence, time.Second)
		done <- err
	}()
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := io.ReadFull(peer, make([]byte, 5)); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, io.ErrShortWrite) || sequence != 0 {
		t.Fatalf("short write silently advanced: sequence=%d, err=%v", sequence, err)
	}
	if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("short write did not close the socket: %d, %v", n, err)
	}
	captured := requirePause(t, c)
	if _, err := c.Stop(captured); err != nil {
		t.Fatal(err)
	}
	source2, peer2 := net.Pipe()
	defer source2.Close()
	defer peer2.Close()
	if wrote, err := c.writeTCPFrame(context.Background(), source2, &sequence, time.Second); wrote || !errors.Is(err, ErrStopped) || sequence != 0 {
		t.Fatalf("stopped source wrote: %v, %v, %d", wrote, err, sequence)
	}
}

func TestConnectionsHaveIndependentSequenceMemory(t *testing.T) {
	c := newCounter(t)
	requireResume(t, c, c.State())
	listener := tcpListener(t)
	startTCP(t, c, listener)
	for range 2 {
		conn, err := net.Dial("tcp", listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for sequence := uint64(1); sequence <= 3; sequence++ {
			if got := requireTCPFrame(t, conn, reader); got.Sequence != sequence || got.Nonce != c.State().Nonce {
				t.Fatalf("invalid per-connection sequence: %+v", got)
			}
		}
	}
}
