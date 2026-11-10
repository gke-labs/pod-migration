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
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/cooperative"
	"github.com/gke-labs/pod-migration/controller/internal/tcpcontinuity"
)

const shutdownTimeout = 5 * time.Second

type config struct {
	listen           string
	interval         time.Duration
	tcpListen        string
	tcpWriteTimeout  time.Duration
	tcpBidirectional bool
}

func parseConfig(args []string, output io.Writer) (config, error) {
	var cfg config
	flags := flag.NewFlagSet("cooperative-counter", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.listen, "listen", "127.0.0.1:8080", "HTTP address; unauthenticated controls, do not expose to untrusted networks")
	flags.DurationVar(&cfg.interval, "interval", time.Second, "positive counter tick interval")
	flags.StringVar(&cfg.tcpListen, "tcp-listen", "", "opt-in unauthenticated lab-only TCP stream; explicit IP:port, e.g. 127.0.0.1:9090; disabled by default")
	flags.DurationVar(&cfg.tcpWriteTimeout, "tcp-write-timeout", cooperative.DefaultTCPWriteTimeout, "bounded TCP frame write timeout, positive and at most 5s")
	flags.BoolVar(&cfg.tcpBidirectional, "tcp-bidirectional", false, "opt-in request/response TCP lab protocol; requires -tcp-listen")
	flags.Usage = func() {
		fmt.Fprintln(output, "Usage: cooperative-counter [-listen address] [-interval duration] [-tcp-listen IP:port] [-tcp-write-timeout duration] [-tcp-bidirectional]")
		fmt.Fprintln(output, "Starts paused. GET /state, then POST that exact state to /resume to start.")
		fmt.Fprintln(output, "No JSON restore endpoint: migration requires a real process snapshot.")
		fmt.Fprintln(output, "Control APIs have no authentication; the default bind is loopback only.")
		fmt.Fprintln(output, "TCP is disabled unless explicitly enabled; use loopback or a trusted isolated lab address.")
		fmt.Fprintln(output, "TCP uses the counter interval, stays silent while paused, and preserves sequence only in process memory.")
		fmt.Fprintln(output, "Default TCP is one-way; bidirectional requests are decimal sequence + newline, starting at 1, one outstanding at a time.")
		fmt.Fprintln(output, "Bidirectional pending requests survive application pause in memory; resume answers on the same socket.")
		fmt.Fprintln(output, "Pause drains bounded TCP writes; bytes already queued in the kernel may still reach the peer.")
		fmt.Fprintln(output, "No address handoff or migration proof is provided.")
		flags.PrintDefaults()
	}
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	if flags.NArg() != 0 {
		return cfg, fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}
	if cfg.interval <= 0 {
		return cfg, errors.New("-interval must be positive")
	}
	if cfg.listen == "" {
		return cfg, errors.New("-listen must not be empty")
	}
	if cfg.tcpBidirectional && cfg.tcpListen == "" {
		return cfg, errors.New("-tcp-bidirectional requires -tcp-listen")
	}
	if cfg.tcpListen != "" {
		if err := tcpcontinuity.ValidateAddress(cfg.tcpListen, true); err != nil {
			return cfg, fmt.Errorf("-tcp-listen: %w", err)
		}
	}
	if err := (cooperative.TCPOptions{Interval: cfg.interval, WriteTimeout: cfg.tcpWriteTimeout}).Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func run(ctx context.Context, args []string, output io.Writer) error {
	cfg, err := parseConfig(args, output)
	if err != nil {
		return err
	}
	counter, err := cooperative.New()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		return fmt.Errorf("listen on %q: %w", cfg.listen, err)
	}
	defer listener.Close()
	var tcpListener net.Listener
	if cfg.tcpListen != "" {
		tcpListener, err = net.Listen("tcp", cfg.tcpListen)
		if err != nil {
			return fmt.Errorf("TCP listen on %q: %w", cfg.tcpListen, err)
		}
		defer tcpListener.Close()
		mode := "stream"
		if cfg.tcpBidirectional {
			mode = "bidirectional request/response"
		}
		fmt.Fprintf(output, "TCP %s on %s; unauthenticated lab-only socket; no handoff or migration proof\n", mode, tcpListener.Addr())
	}
	fmt.Fprintf(output, "Listening on %s; initially paused; control API has no authentication\n", listener.Addr())
	return runServers(ctx, counter, listener, tcpListener, cfg.interval, cfg.tcpWriteTimeout, cfg.tcpBidirectional)
}

func runServer(ctx context.Context, counter *cooperative.Counter, listener net.Listener, interval time.Duration) error {
	return runServers(ctx, counter, listener, nil, interval, cooperative.DefaultTCPWriteTimeout, false)
}

func runServers(ctx context.Context, counter *cooperative.Counter, listener, tcpListener net.Listener, interval, tcpWriteTimeout time.Duration, bidirectional bool) error {
	server := &http.Server{
		Handler:           counter.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	defer server.Close()
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve(listener) }()
	var tcpDone chan struct{}
	var tcpErr error
	if tcpListener != nil {
		tcpContext, cancelTCP := context.WithCancel(ctx)
		tcpDone = make(chan struct{})
		go func() {
			tcpErr = counter.ServeTCP(tcpContext, tcpListener, cooperative.TCPOptions{Interval: interval, WriteTimeout: tcpWriteTimeout, Bidirectional: bidirectional})
			close(tcpDone)
		}()
		defer func() {
			cancelTCP()
			<-tcpDone
		}()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
running:
	for {
		select {
		case <-ctx.Done():
			break running
		case <-counter.Done():
			break running
		case <-tcpDone:
			if tcpErr != nil {
				return fmt.Errorf("serve counter TCP: %w", tcpErr)
			}
			break running
		case err := <-serveErrors:
			if !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve counter: %w", err)
			}
			return nil
		case <-ticker.C:
			counter.Tick()
		}
	}
	shutdownContext, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownContext); err != nil {
		return fmt.Errorf("shutdown counter: %w", err)
	}
	if err := <-serveErrors; !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve counter: %w", err)
	}
	return nil
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if err := run(ctx, os.Args[1:], os.Stderr); err != nil && !errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(os.Stderr, "cooperative-counter:", err)
		os.Exit(1)
	}
}
