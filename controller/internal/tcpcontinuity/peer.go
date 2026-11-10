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
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

type Config struct {
	Target     string
	Duration   time.Duration
	MaxSilence time.Duration
	// MaxFrameGap is an optional responsiveness budget; zero disables enforcement.
	MaxFrameGap   time.Duration
	Bidirectional bool
}

func (c Config) Validate() error {
	if err := ValidateAddress(c.Target, false); err != nil {
		return fmt.Errorf("target: %w", err)
	}
	if c.Duration <= 0 || c.MaxSilence <= 0 {
		return errors.New("observation duration and maximum silence must both be positive")
	}
	if c.MaxFrameGap < 0 {
		return errors.New("maximum frame gap must not be negative (zero disables the budget)")
	}
	return nil
}

type Tuple struct {
	Local  string `json:"local"`
	Remote string `json:"remote"`
}

type Result struct {
	ConnectionMode  string    `json:"connectionMode"`
	Target          string    `json:"target"`
	StartTuple      *Tuple    `json:"startTuple"`
	EndTuple        *Tuple    `json:"endTuple"`
	StartedAt       time.Time `json:"startedAt"`
	EndedAt         time.Time `json:"endedAt"`
	Duration        string    `json:"requestedObservationDuration"`
	MaxSilence      string    `json:"maxAcceptableSilence"`
	ObservedFrames  uint64    `json:"observedFrameCount"`
	Nonce           string    `json:"nonce,omitempty"`
	LastSequence    uint64    `json:"lastSequence"`
	LastCounter     uint64    `json:"lastCounter"`
	Outcome         string    `json:"outcome"`
	Error           string    `json:"error,omitempty"`
	MigrationProven bool      `json:"migrationProven"`

	MaxObservedFrameGap            string `json:"maxObservedFrameGap"`
	MaxObservedFrameGapNanoseconds int64  `json:"maxObservedFrameGapNanoseconds"`
	MaxFrameGapLocation            string `json:"maxFrameGapLocation"`
	// Sequence zero denotes connection establishment; nil denotes no ending frame.
	MaxFrameGapAfterSequence     uint64  `json:"maxFrameGapAfterSequence"`
	MaxFrameGapBeforeSequence    *uint64 `json:"maxFrameGapBeforeSequence"`
	MaxFrameGapBudget            string  `json:"maxFrameGapBudget"`
	MaxFrameGapBudgetNanoseconds int64   `json:"maxFrameGapBudgetNanoseconds"`
	// Nil means the budget is disabled or no connection was established.
	FrameGapBudgetMet *bool `json:"frameGapBudgetMet"`
}

// ErrFrameGapBudgetExceeded does not terminate the held-socket observation early.
var ErrFrameGapBudgetExceeded = errors.New("maximum frame gap budget exceeded")

type frameGapTracker struct {
	lastFrameAt, observationEnd time.Time
	lastSequence                uint64
	maxGap                      time.Duration
	location                    string
	afterSequence               uint64
	beforeSequence              *uint64
}

func (g *frameGapTracker) boundedTime(at time.Time) time.Time {
	// Deadline wakeups and buffered-frame validation can run after the requested
	// interval. Neither extends the measurement window.
	if at.After(g.observationEnd) {
		return g.observationEnd
	}
	return at
}

func (g *frameGapTracker) record(at time.Time, location string, beforeSequence *uint64) {
	gap := g.boundedTime(at).Sub(g.lastFrameAt)
	// Retain the first location when maxima tie.
	if g.location == "" || gap > g.maxGap {
		g.maxGap = gap
		g.location = location
		g.afterSequence = g.lastSequence
		g.beforeSequence = beforeSequence
	}
}

func (g *frameGapTracker) validFrame(at time.Time, sequence uint64) {
	// Buffered frames still undergo strict validation after the deadline, but
	// cannot end a gap inside an observation window that has already closed.
	if at.After(g.observationEnd) {
		return
	}
	location := "between-frames"
	if g.lastSequence == 0 {
		location = "initial"
	}
	g.record(at, location, &sequence)
	g.lastFrameAt = g.boundedTime(at)
	g.lastSequence = sequence
}

func (g *frameGapTracker) finish(at time.Time, result *Result, budget time.Duration) error {
	location := "trailing"
	if g.lastSequence == 0 {
		location = "no-valid-frames"
	}
	g.record(at, location, nil)
	result.MaxObservedFrameGap = g.maxGap.String()
	result.MaxObservedFrameGapNanoseconds = int64(g.maxGap)
	result.MaxFrameGapLocation = g.location
	result.MaxFrameGapAfterSequence = g.afterSequence
	result.MaxFrameGapBeforeSequence = g.beforeSequence
	if budget == 0 {
		return nil
	}
	met := g.maxGap <= budget
	result.FrameGapBudgetMet = &met
	if !met {
		return fmt.Errorf("%w: observed %s > %s (%s after sequence %d)",
			ErrFrameGapBudgetExceeded, g.maxGap, budget, g.location, g.afterSequence)
	}
	return nil
}

// Observe dials exactly once, then holds that socket for the requested interval.
// Success means only that this observation completed, never that migration
// occurred. Silence is measured between complete valid frames, not raw bytes.
// The optional frame-gap budget is evaluated only when observation ends; it does
// not shorten transport deadlines. Gap arithmetic uses time.Now's monotonic clock.
func Observe(ctx context.Context, cfg Config) (result Result, err error) {
	result = Result{
		ConnectionMode: "direct-single-tcp-connection",
		Target:         cfg.Target, Duration: cfg.Duration.String(), MaxSilence: cfg.MaxSilence.String(),
		StartedAt: time.Now(), Outcome: "failed",
		MaxObservedFrameGap: "0s", MaxFrameGapLocation: "not-connected",
		MaxFrameGapBudget: cfg.MaxFrameGap.String(), MaxFrameGapBudgetNanoseconds: int64(cfg.MaxFrameGap),
	}
	if cfg.Bidirectional {
		result.ConnectionMode = "direct-single-tcp-connection-bidirectional"
	}
	defer func() {
		result.EndedAt = time.Now()
		if err != nil {
			result.Error = err.Error()
		}
	}()
	if err = cfg.Validate(); err != nil {
		return result, err
	}
	dialer := net.Dialer{Timeout: cfg.MaxSilence}
	conn, err := dialer.DialContext(ctx, "tcp", cfg.Target)
	if err != nil {
		return result, fmt.Errorf("single TCP dial failed: %w", err)
	}
	result.StartedAt = time.Now()
	defer conn.Close()
	tuple := func() *Tuple {
		return &Tuple{Local: conn.LocalAddr().String(), Remote: conn.RemoteAddr().String()}
	}
	result.StartTuple = tuple()
	defer func() { result.EndTuple = tuple() }()
	cancelDone := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.Close()
		close(cancelDone)
	})
	defer func() {
		if !stopCancel() {
			<-cancelDone
		}
	}()

	end := result.StartedAt.Add(cfg.Duration)
	lastFrameAt := result.StartedAt
	gaps := frameGapTracker{lastFrameAt: lastFrameAt, observationEnd: end}
	defer func() {
		if gapErr := gaps.finish(time.Now(), &result, cfg.MaxFrameGap); gapErr != nil {
			err = errors.Join(err, gapErr)
			result.Outcome = "failed"
		}
	}()
	reader := bufio.NewReaderSize(conn, MaxFrameBytes+1)
	pending := false
	for {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("observation canceled: %w", err)
		}
		silenceEnd := lastFrameAt.Add(cfg.MaxSilence)
		if !time.Now().Before(end) {
			if result.ObservedFrames == 0 || !end.Before(silenceEnd) {
				return result, errors.New("observation ended without valid frames within the silence limit")
			}
			// A prior read may already have buffered another frame or a partial
			// one. Validate those bytes before declaring the observation complete.
			if reader.Buffered() == 0 {
				result.Outcome = "observation-complete"
				return result, nil
			}
		}
		deadline := end
		if silenceEnd.Before(deadline) {
			deadline = silenceEnd
		}
		if cfg.Bidirectional && !pending {
			if reader.Buffered() != 0 {
				return result, errors.New("unsolicited TCP response before the next request")
			}
			if result.LastSequence == ^uint64(0) {
				return result, errors.New("TCP request sequence exhausted")
			}
			if err := writeRequest(ctx, conn, result.LastSequence+1, deadline); err != nil {
				return result, err
			}
			pending = true
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return result, fmt.Errorf("set read deadline: %w", err)
		}
		line, readErr := reader.ReadSlice('\n')
		frameAt := time.Now()
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("observation canceled: %w", err)
		}
		if readErr != nil {
			var timeout net.Error
			if errors.As(readErr, &timeout) && timeout.Timeout() &&
				!time.Now().Before(end) && end.Before(silenceEnd) &&
				result.ObservedFrames > 0 && len(line) == 0 {
				result.Outcome = "observation-complete"
				return result, nil
			}
			return result, fmt.Errorf("TCP stream failed (never reconnecting): %w", readErr)
		}
		frame, err := DecodeFrame(line)
		if err != nil {
			return result, fmt.Errorf("malformed frame: %w", err)
		}
		if result.ObservedFrames > 0 && frame.Nonce != result.Nonce {
			return result, errors.New("process nonce changed on the held connection")
		}
		if result.LastSequence == ^uint64(0) || frame.Sequence != result.LastSequence+1 {
			return result, fmt.Errorf("sequence gap or regression: got %d after %d", frame.Sequence, result.LastSequence)
		}
		if result.ObservedFrames > 0 && frame.Counter < result.LastCounter {
			return result, fmt.Errorf("counter regressed: got %d after %d", frame.Counter, result.LastCounter)
		}
		result.Nonce = frame.Nonce
		result.LastSequence = frame.Sequence
		result.LastCounter = frame.Counter
		result.ObservedFrames++
		pending = false
		lastFrameAt = time.Now()
		gaps.validFrame(frameAt, frame.Sequence)
	}
}

func writeRequest(ctx context.Context, conn net.Conn, sequence uint64, deadline time.Time) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("observation canceled: %w", err)
	}
	if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
		deadline = contextDeadline
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return fmt.Errorf("set request write deadline: %w", err)
	}
	data := strconv.AppendUint(nil, sequence, 10)
	data = append(data, '\n')
	n, err := conn.Write(data)
	if canceled := ctx.Err(); canceled != nil {
		return fmt.Errorf("observation canceled: %w", canceled)
	}
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return fmt.Errorf("TCP request write failed (never reconnecting): %w", err)
	}
	return nil
}
