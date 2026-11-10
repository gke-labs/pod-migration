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
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/cooperative"
)

const maxExpectedBytes = 16 * 1024

type config struct {
	action      string
	endpoint    string
	nonce       string
	migrationID string
	expected    string
	timeout     time.Duration
}

func parseConfig(args []string, diagnostics io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("cooperative-control", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	flags.StringVar(&cfg.action, "action", "", "required: state, pause, resume, or stop")
	flags.StringVar(&cfg.endpoint, "endpoint", "http://127.0.0.1:8080", "literal loopback HTTP endpoint; no DNS or proxy")
	flags.StringVar(&cfg.nonce, "nonce", "", "required for pause: counter identity")
	flags.StringVar(&cfg.migrationID, "migration-id", "", "required for pause: migration identity")
	flags.StringVar(&cfg.expected, "expected", "", "required for resume/stop: exact paused State JSON, at most 16384 bytes")
	flags.DurationVar(&cfg.timeout, "timeout", 5*time.Second, "positive control request timeout")
	flags.Usage = func() {
		fmt.Fprintln(diagnostics, "Usage: cooperative-control -action state|pause|resume|stop [-endpoint http://127.0.0.1:8080] [-timeout 5s]")
		fmt.Fprintln(diagnostics, "Pause requires -nonce N -migration-id ID. Resume and stop require -expected '<State JSON>'.")
		fmt.Fprintln(diagnostics, "Pass JSON as one argument for exact-container CRI exec; stdin is not read.")
		fmt.Fprintln(diagnostics, "Only calls the existing HTTP controls; no state import or restore.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	switch cfg.action {
	case "state", "pause", "resume", "stop":
	default:
		return cfg, errors.New("-action must be state, pause, resume, or stop")
	}
	var flagErr error
	flags.Visit(func(f *flag.Flag) {
		if (f.Name == "nonce" || f.Name == "migration-id") && cfg.action != "pause" {
			flagErr = fmt.Errorf("-%s is only valid with -action pause", f.Name)
		}
		if f.Name == "expected" && cfg.action != "resume" && cfg.action != "stop" {
			flagErr = errors.New("-expected is only valid with -action resume or stop")
		}
	})
	if flagErr != nil {
		return cfg, flagErr
	}
	if cfg.action == "pause" && (strings.TrimSpace(cfg.nonce) == "" || strings.TrimSpace(cfg.migrationID) == "") {
		return cfg, errors.New("-action pause requires -nonce and -migration-id")
	}
	if (cfg.action == "resume" || cfg.action == "stop") && cfg.expected == "" {
		return cfg, errors.New("-action resume and stop require -expected '<State JSON>'")
	}
	if cfg.timeout <= 0 {
		return cfg, errors.New("-timeout must be positive")
	}
	if err := validateEndpoint(cfg.endpoint); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func validateEndpoint(endpoint string) error {
	base, err := url.Parse(endpoint)
	if err != nil || base.Scheme != "http" || base.Host == "" || base.User != nil ||
		base.Opaque != "" || (base.Path != "" && base.Path != "/") || base.RawPath != "" ||
		base.RawQuery != "" || base.ForceQuery || strings.Contains(endpoint, "#") {
		return errors.New("-endpoint must be a loopback HTTP origin without credentials, path, query, or fragment")
	}
	address, err := netip.ParseAddr(base.Hostname())
	if err != nil || !address.IsLoopback() || address.Zone() != "" {
		return errors.New("-endpoint requires a literal loopback IP address (127.0.0.1 or [::1]); DNS is not used")
	}
	return nil
}

// The cooperative decoder is unexported. Preserve its strict wire schema before
// unmarshalling, which otherwise accepts missing, duplicate, and null fields.
func decodeExpected(input string) (cooperative.State, error) {
	var state cooperative.State
	if len(input) > maxExpectedBytes {
		return state, fmt.Errorf("expected JSON exceeds %d bytes", maxExpectedBytes)
	}
	decoder := json.NewDecoder(strings.NewReader(input))
	first, err := decoder.Token()
	if err != nil || first != json.Delim('{') {
		return state, errors.New("expected a JSON object")
	}
	fields := []string{"nonce", "counter", "paused", "epoch", "migrationID"}
	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		seen[field] = false
	}
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return state, fmt.Errorf("decode JSON key: %w", err)
		}
		key, ok := token.(string)
		if !ok {
			return state, errors.New("expected a JSON object key")
		}
		present, allowed := seen[key]
		if !allowed || present {
			return state, fmt.Errorf("unknown or duplicate JSON field %q", key)
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return state, fmt.Errorf("decode JSON field %q: %w", key, err)
		}
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return state, fmt.Errorf("JSON field %q must not be null", key)
		}
		seen[key] = true
	}
	if _, err := decoder.Token(); err != nil {
		return state, fmt.Errorf("close JSON object: %w", err)
	}
	var trailing json.RawMessage
	if err := decoder.Decode(&trailing); err != io.EOF {
		return state, errors.New("unexpected trailing JSON data")
	}
	for _, field := range fields {
		if !seen[field] {
			return state, fmt.Errorf("missing JSON field %q", field)
		}
	}
	if err := json.Unmarshal([]byte(input), &state); err != nil {
		return cooperative.State{}, fmt.Errorf("decode expected state: %w", err)
	}
	return state, nil
}

func run(ctx context.Context, args []string, output, diagnostics io.Writer) error {
	cfg, err := parseConfig(args, diagnostics)
	if err != nil {
		return err
	}
	var expected cooperative.State
	if cfg.action == "resume" || cfg.action == "stop" {
		expected, err = decodeExpected(cfg.expected)
		if err != nil {
			return fmt.Errorf("-expected: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	// Use only the container's loopback, regardless of ambient proxy settings.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := cooperative.Client{
		BaseURL:    cfg.endpoint,
		HTTPClient: &http.Client{Transport: transport, Timeout: cfg.timeout},
	}
	var state cooperative.State
	switch cfg.action {
	case "state":
		state, err = client.State(ctx)
	case "pause":
		state, err = client.Pause(ctx, cooperative.PauseRequest{Nonce: cfg.nonce, MigrationID: cfg.migrationID})
	case "resume":
		state, err = client.Resume(ctx, expected)
	case "stop":
		state, err = client.Stop(ctx, expected)
	}
	if err != nil {
		return err
	}
	if err := json.NewEncoder(output).Encode(state); err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "cooperative-control:", err)
		os.Exit(1)
	}
}
