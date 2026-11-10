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
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/gke-labs/pod-migration/controller/internal/tcpcontinuity"
)

func parseConfig(args []string, output io.Writer) (tcpcontinuity.Config, error) {
	var cfg tcpcontinuity.Config
	flags := flag.NewFlagSet("tcp-continuity-peer", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.Target, "target", "", "required direct target IP:port; no DNS fallback, proxy, or reconnect")
	flags.DurationVar(&cfg.Duration, "duration", 0, "required positive observation duration after connecting")
	flags.DurationVar(&cfg.MaxSilence, "max-silence", 0, "required positive maximum silence between valid frames; also bounds the single dial and bidirectional writes")
	flags.DurationVar(&cfg.MaxFrameGap, "max-frame-gap", 0, "optional maximum valid-response gap, including initial/trailing silence; 0 disables the budget; evaluated at observation end without shortening max-silence")
	flags.BoolVar(&cfg.Bidirectional, "bidirectional", false, "opt-in held-socket request/response protocol; requires a bidirectional counter")
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: tcp-continuity-peer -target 127.0.0.1:9090 -duration 60s -max-silence 10s [-max-frame-gap 1s] [-bidirectional]")
		fmt.Fprintln(output, "Unauthenticated lab-only stream: dials exactly once and holds the same socket.")
		fmt.Fprintln(output, "EOF, reset, timeout, malformed frames, nonce drift, or sequence gaps fail; never reconnects.")
		fmt.Fprintln(output, "JSON reports only the requested observation; migrationProven is always false.")
		fmt.Fprintln(output, "A missed frame-gap budget fails at observation end, not immediately; transport failures are still reported.")
		fmt.Fprintln(output, "No IP movement, cloud integration, or external migration evidence verification.")
		fmt.Fprintln(output, "Default is a one-way observer. Bidirectional sends decimal sequence + newline, starting at 1, and waits for each JSON response.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	return cfg, cfg.Validate()
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) error {
	cfg, err := parseConfig(args, diagnostics)
	if err != nil {
		return err
	}
	result, observationErr := tcpcontinuity.Observe(ctx, cfg)
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return fmt.Errorf("write observation result: %w", err)
	}
	return observationErr
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "tcp-continuity-peer:", err)
		os.Exit(1)
	}
}
