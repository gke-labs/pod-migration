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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/tcpcontinuity"
)

const (
	DefaultTCPWriteTimeout = time.Second
	MaxTCPWriteTimeout     = 5 * time.Second
	maxTCPRequestBytes     = 21 // uint64 decimal plus newline
)

type TCPOptions struct {
	Interval      time.Duration
	WriteTimeout  time.Duration
	Bidirectional bool
}

func (o TCPOptions) Validate() error {
	if o.Interval <= 0 {
		return errors.New("TCP frame interval must be positive")
	}
	if o.WriteTimeout <= 0 || o.WriteTimeout > MaxTCPWriteTimeout {
		return fmt.Errorf("TCP write timeout must be positive and at most %s", MaxTCPWriteTimeout)
	}
	return nil
}

// ServeTCP owns and closes listener and every accepted connection, joining its
// goroutines before returning. This unauthenticated lab-only socket must bind
// to loopback or an explicitly chosen trusted, isolated address. A nonce is not
// a credential. Sequence state is process memory, never imported from JSON.
func (c *Counter) ServeTCP(ctx context.Context, listener net.Listener, options TCPOptions) error {
	defer listener.Close()
	if err := options.Validate(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	closing := false
	var workers sync.WaitGroup
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
		case <-c.Done():
		}
		mu.Lock()
		defer mu.Unlock()
		closing = true
		_ = listener.Close()
		for conn := range connections {
			_ = conn.Close()
		}
	}()
	var serveErr error
	for {
		conn, err := listener.Accept()
		mu.Lock()
		if err != nil {
			if !closing {
				serveErr = fmt.Errorf("accept TCP connection: %w", err)
			}
			mu.Unlock()
			break
		}
		if closing {
			mu.Unlock()
			_ = conn.Close()
			break
		}
		connections[conn] = struct{}{}
		workers.Add(1)
		mu.Unlock()
		go func() {
			defer workers.Done()
			defer func() {
				_ = conn.Close()
				mu.Lock()
				delete(connections, conn)
				mu.Unlock()
			}()
			c.streamTCP(ctx, conn, options)
		}()
	}
	cancel()
	<-shutdownDone
	workers.Wait()
	return serveErr
}

func (c *Counter) streamTCP(ctx context.Context, conn net.Conn, options TCPOptions) {
	if options.Bidirectional {
		_ = c.respondTCP(ctx, conn, options)
		return
	}
	// This is a one-way stream. Monitor the read side so a disconnected peer
	// does not retain a writer goroutine indefinitely while the counter pauses.
	readDone := make(chan struct{})
	go func() {
		var data [1]byte
		_, _ = conn.Read(data[:])
		_ = conn.Close()
		close(readDone)
	}()
	defer func() {
		_ = conn.Close()
		<-readDone
	}()
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	var sequence uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.Done():
			return
		case <-readDone:
			return
		case <-ticker.C:
			if _, err := c.writeTCPFrame(ctx, conn, &sequence, options.WriteTimeout); err != nil {
				return
			}
		}
	}
}

// respondTCP accepts canonical decimal sequences, starting at 1, with one
// outstanding request. Both the sequence and paused pending request live only
// in process memory, never Counter.State or a JSON import.
func (c *Counter) respondTCP(ctx context.Context, conn net.Conn, options TCPOptions) error {
	type request struct {
		sequence uint64
		err      error
	}
	requests := make(chan request)
	stopRead := make(chan struct{})
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		reader := bufio.NewReaderSize(conn, maxTCPRequestBytes+1)
		for {
			sequence, err := readTCPRequest(reader)
			select {
			case requests <- request{sequence, err}:
			case <-stopRead:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	defer func() {
		close(stopRead)
		_ = conn.Close()
		<-readDone
	}()
	ticker := time.NewTicker(options.Interval)
	defer ticker.Stop()
	var sequence, pending uint64
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Done():
			return ErrStopped
		case req := <-requests:
			if req.err != nil {
				return fmt.Errorf("read TCP request: %w", req.err)
			}
			if pending != 0 {
				return errors.New("TCP request received with a response still pending")
			}
			if sequence == ^uint64(0) || req.sequence != sequence+1 {
				return fmt.Errorf("TCP request sequence gap or regression: got %d after %d", req.sequence, sequence)
			}
			pending = req.sequence
		case <-ticker.C:
			if pending == 0 {
				continue
			}
			// Never hold execution while reading. Pause drains only bounded
			// writes; a paused request remains pending without writing bytes.
			wrote, err := c.writeTCPFrame(ctx, conn, &sequence, options.WriteTimeout)
			if err != nil {
				return err
			}
			if wrote {
				pending = 0
			}
		}
	}
}

func readTCPRequest(reader *bufio.Reader) (uint64, error) {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		if errors.Is(err, bufio.ErrBufferFull) {
			return 0, errors.New("TCP request exceeds the decimal sequence limit")
		}
		if errors.Is(err, io.EOF) && len(line) > 0 {
			return 0, io.ErrUnexpectedEOF
		}
		return 0, err
	}
	if len(line) < 2 || len(line) > maxTCPRequestBytes || line[0] < '1' || line[0] > '9' {
		return 0, errors.New("TCP request must be a canonical positive decimal sequence followed by newline")
	}
	for _, digit := range line[:len(line)-1] {
		if digit < '0' || digit > '9' {
			return 0, errors.New("TCP request contains a non-decimal character")
		}
	}
	sequence, err := strconv.ParseUint(string(line[:len(line)-1]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid TCP request sequence: %w", err)
	}
	return sequence, nil
}

func (c *Counter) writeTCPFrame(ctx context.Context, conn net.Conn, sequence *uint64, timeout time.Duration) (bool, error) {
	// A pending Pause excludes new writes and drains concurrent bounded writes,
	// rather than waiting one timeout per connection. Lock order is execution, mu.
	c.execution.RLock()
	defer c.execution.RUnlock()
	c.mu.Lock()
	state, stopped := c.state, c.stopped
	c.mu.Unlock()
	if stopped {
		return false, ErrStopped
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if state.Paused {
		return false, nil
	}
	if *sequence == ^uint64(0) {
		return false, errors.New("TCP sequence exhausted")
	}
	data, err := json.Marshal(tcpcontinuity.Frame{
		Nonce: state.Nonce, Sequence: *sequence + 1, Counter: state.Counter,
	})
	if err != nil {
		return false, err
	}
	data = append(data, '\n')
	deadline := time.Now().Add(timeout)
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		_ = conn.Close()
		return false, fmt.Errorf("set TCP write deadline: %w", err)
	}
	n, err := conn.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		// A partial frame may be visible to the peer. Never retry it or skip
		// its sequence; close this connection and let the peer fail explicitly.
		_ = conn.Close()
		return false, fmt.Errorf("write TCP frame: %w", err)
	}
	*sequence++
	return true, nil
}
