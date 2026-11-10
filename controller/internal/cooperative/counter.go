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

// Package cooperative implements an in-memory workload for process snapshots.
// It deliberately has no state import or application-level restore operation.
package cooperative

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	ErrInvalidRequest = errors.New("invalid request")
	ErrConflict       = errors.New("state conflict")
	ErrStopped        = errors.New("counter permanently stopped")
)

type State struct {
	Nonce       string `json:"nonce"`
	Counter     uint64 `json:"counter"`
	Paused      bool   `json:"paused"`
	Epoch       uint64 `json:"epoch"`
	MigrationID string `json:"migrationID"`
}

type PauseRequest struct {
	Nonce       string `json:"nonce"`
	MigrationID string `json:"migrationID"`
}

// Counter must be constructed with New and must not be copied.
type Counter struct {
	execution     sync.RWMutex
	mu            sync.Mutex
	state         State
	activation    State
	hasActivation bool
	stopped       bool
	done          chan struct{}
	doneOnce      sync.Once
}

func New() (*Counter, error) {
	var nonce [32]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, fmt.Errorf("generate counter nonce: %w", err)
	}
	return &Counter{
		state: State{Nonce: hex.EncodeToString(nonce[:]), Paused: true},
		done:  make(chan struct{}),
	}, nil
}

func (c *Counter) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

func (c *Counter) Tick() {
	c.execution.RLock()
	defer c.execution.RUnlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.stopped && !c.state.Paused {
		c.state.Counter++
	}
}

func (c *Counter) Pause(request PauseRequest) (State, error) {
	if strings.TrimSpace(request.Nonce) == "" || strings.TrimSpace(request.MigrationID) == "" {
		return State{}, fmt.Errorf("%w: nonce and migrationID are required", ErrInvalidRequest)
	}
	c.execution.Lock()
	defer c.execution.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return State{}, ErrStopped
	}
	if request.Nonce != c.state.Nonce {
		return State{}, fmt.Errorf("%w: nonce does not match", ErrConflict)
	}
	if c.state.MigrationID != "" {
		if request.MigrationID != c.state.MigrationID {
			return State{}, fmt.Errorf("%w: only one migration is supported", ErrConflict)
		}
		// A delayed retry after activation must not pause the destination.
		return c.state, nil
	}
	c.state.Paused = true
	c.state.Epoch++
	c.state.MigrationID = request.MigrationID
	c.hasActivation = false
	return c.state, nil
}

func (c *Counter) Resume(expected State) (State, error) {
	if err := validateExpected(expected); err != nil {
		return State{}, err
	}
	c.execution.Lock()
	defer c.execution.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.stopped {
		return State{}, ErrStopped
	}
	if c.hasActivation && expected == c.activation {
		return c.state, nil
	}
	if expected != c.state {
		return State{}, fmt.Errorf("%w: resume requires the exact paused state", ErrConflict)
	}
	// Remember the captured state, not the advancing counter, for retries.
	c.activation = expected
	c.hasActivation = true
	c.state.Paused = false
	return c.state, nil
}

// Stop permanently fences writes and signals Done. The HTTP handler delays the
// signal until after writing its response so graceful shutdown can acknowledge it.
func (c *Counter) Stop(expected State) (State, error) {
	state, err := c.stop(expected)
	if err == nil {
		c.signalDone()
	}
	return state, err
}

func (c *Counter) stop(expected State) (State, error) {
	if err := validateExpected(expected); err != nil {
		return State{}, err
	}
	c.execution.Lock()
	defer c.execution.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if expected != c.state {
		return State{}, fmt.Errorf("%w: stop requires the exact paused state", ErrConflict)
	}
	c.stopped = true
	return c.state, nil
}

func (c *Counter) signalDone() {
	c.doneOnce.Do(func() { close(c.done) })
}

func (c *Counter) Done() <-chan struct{} {
	return c.done
}

func validateExpected(expected State) error {
	if strings.TrimSpace(expected.Nonce) == "" || !expected.Paused {
		return fmt.Errorf("%w: an expected paused state with a nonce is required", ErrInvalidRequest)
	}
	return nil
}
