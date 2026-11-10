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
	"errors"
	"flag"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/controller/internal/cooperative"
)

func TestConfiguration(t *testing.T) {
	cfg, err := parseConfig(nil, io.Discard)
	if err != nil || cfg.listen != "127.0.0.1:8080" || cfg.interval != time.Second ||
		cfg.tcpListen != "" || cfg.tcpWriteTimeout != cooperative.DefaultTCPWriteTimeout || cfg.tcpBidirectional {
		t.Fatalf("unsafe defaults: %+v, %v", cfg, err)
	}
	cfg, err = parseConfig([]string{"-listen", "127.0.0.1:0", "-interval", "10ms"}, io.Discard)
	if err != nil || cfg.listen != "127.0.0.1:0" || cfg.interval != 10*time.Millisecond {
		t.Fatalf("flags: %+v, %v", cfg, err)
	}
	cfg, err = parseConfig([]string{"-tcp-listen", "127.0.0.1:0", "-tcp-write-timeout", "50ms"}, io.Discard)
	if err != nil || cfg.tcpListen != "127.0.0.1:0" || cfg.tcpWriteTimeout != 50*time.Millisecond {
		t.Fatalf("TCP opt-in flags: %+v, %v", cfg, err)
	}
	cfg, err = parseConfig([]string{"-tcp-listen", "127.0.0.1:0", "-tcp-bidirectional"}, io.Discard)
	if err != nil || !cfg.tcpBidirectional {
		t.Fatalf("bidirectional opt-in flag: %+v, %v", cfg, err)
	}
	for _, args := range [][]string{
		{"-interval", "0"},
		{"-interval", "-1s"},
		{"-interval", "bad"},
		{"-listen", ""},
		{"-tcp-listen", ":9090"},
		{"-tcp-listen", "0.0.0.0:9090"},
		{"-tcp-listen", "localhost:9090"},
		{"-tcp-listen", "127.0.0.1:65536"},
		{"-tcp-write-timeout", "0"},
		{"-tcp-write-timeout", "6s"},
		{"-tcp-bidirectional"},
		{"unexpected"},
		{"-unknown"},
	} {
		if _, err := parseConfig(args, io.Discard); err == nil {
			t.Fatalf("invalid flags accepted: %v", args)
		}
	}
	var help bytes.Buffer
	if _, err := parseConfig([]string{"-help"}, &help); !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	for _, text := range []string{"Starts paused", "no authentication", "loopback", "No JSON restore", "lab-only", "disabled", "bounded TCP writes", "No address handoff", "one outstanding", "pending requests"} {
		if !strings.Contains(help.String(), text) {
			t.Fatalf("help missing %q", text)
		}
	}
}

func TestTCPAndHTTPShutdownTogether(t *testing.T) {
	for _, test := range []struct {
		shutdown      string
		bidirectional bool
	}{
		{"stop", false}, {"signal", false}, {"TCP error", false},
		{"stop", true}, {"signal", true}, {"TCP error", true},
	} {
		name := test.shutdown
		if test.bidirectional {
			name += "/bidirectional"
		}
		t.Run(name, func(t *testing.T) {
			shutdown := test.shutdown
			counter, err := cooperative.New()
			if err != nil {
				t.Fatal(err)
			}
			httpListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer httpListener.Close()
			tcpListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer tcpListener.Close()
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			var runErr error
			go func() {
				defer close(done)
				runErr = runServers(ctx, counter, httpListener, tcpListener, time.Millisecond, 100*time.Millisecond, test.bidirectional)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("combined server leaked")
				}
			})
			conn, err := net.DialTimeout("tcp", tcpListener.Addr().String(), time.Second)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if test.bidirectional {
				_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
				if _, err := io.WriteString(conn, "1\n"); err != nil {
					t.Fatal(err)
				}
			}
			client := &cooperative.Client{BaseURL: "http://" + httpListener.Addr().String()}
			requestContext, cancelRequest := context.WithTimeout(context.Background(), time.Second)
			defer cancelRequest()
			if _, err := client.State(requestContext); err != nil {
				t.Fatal(err)
			}
			switch shutdown {
			case "stop":
				if _, err := client.Stop(requestContext, counter.State()); err != nil {
					t.Fatalf("HTTP stop acknowledgement lost: %v", err)
				}
			case "signal":
				cancel()
			case "TCP error":
				_ = tcpListener.Close()
			}
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("shutdown timed out")
			}
			if shutdown == "TCP error" {
				if runErr == nil || !strings.Contains(runErr.Error(), "serve counter TCP") {
					t.Fatalf("TCP failure not reported: %v", runErr)
				}
			} else if runErr != nil {
				t.Fatal(runErr)
			}
			_ = conn.SetReadDeadline(time.Now().Add(time.Second))
			if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("TCP connection not closed cleanly: %d, %v", n, err)
			}
			if _, err := client.State(requestContext); err == nil {
				t.Fatal("HTTP listener not closed")
			}
			if conn, err := net.DialTimeout("tcp", tcpListener.Addr().String(), 100*time.Millisecond); err == nil {
				_ = conn.Close()
				t.Fatal("TCP listener not closed")
			}
		})
	}
}

func TestServerLifecycle(t *testing.T) {
	for _, shutdown := range []string{"stop", "signal"} {
		t.Run(shutdown, func(t *testing.T) {
			counter, err := cooperative.New()
			if err != nil {
				t.Fatal(err)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			finished := make(chan error, 1)
			go func() { finished <- runServer(ctx, counter, listener, time.Millisecond) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-finished:
				case <-time.After(2 * time.Second):
					t.Error("server did not stop")
				}
			})
			client := &cooperative.Client{BaseURL: "http://" + listener.Addr().String()}
			requestContext, cancelRequests := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancelRequests()
			initial, err := client.State(requestContext)
			if err != nil || !initial.Paused || initial.Counter != 0 {
				t.Fatalf("cold state: %+v, %v", initial, err)
			}
			time.Sleep(10 * time.Millisecond)
			if counter.State() != initial {
				t.Fatal("server automatically resumed")
			}
			if _, err := client.Resume(requestContext, initial); err != nil {
				t.Fatal(err)
			}
			deadline := time.After(time.Second)
			for counter.State().Counter == 0 {
				select {
				case <-deadline:
					t.Fatal("periodic tick did not advance")
				case <-time.After(time.Millisecond):
				}
			}
			captured, err := client.Pause(requestContext, cooperative.PauseRequest{Nonce: initial.Nonce, MigrationID: "migration-1"})
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			if counter.State() != captured {
				t.Fatal("server wrote after pause")
			}
			if shutdown == "stop" {
				if state, err := client.Stop(requestContext, captured); err != nil || state != captured {
					t.Fatalf("stop was not acknowledged before shutdown: %+v, %v", state, err)
				}
			} else {
				cancel()
			}
			select {
			case err := <-finished:
				if err != nil {
					t.Fatal(err)
				}
				finished <- nil
			case <-time.After(2 * time.Second):
				t.Fatal("graceful shutdown timed out")
			}
			if _, err := client.State(requestContext); err == nil {
				t.Fatal("HTTP server still serving after shutdown")
			}
		})
	}
}

func TestRunErrorsAndCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := run(context.Background(), []string{"-listen", listener.Addr().String()}, io.Discard); err == nil || !strings.Contains(err.Error(), "listen on") {
		t.Fatalf("bind failure not reported: %v", err)
	}
	if err := run(context.Background(), []string{"-listen", "127.0.0.1:0", "-tcp-listen", listener.Addr().String()}, io.Discard); err == nil || !strings.Contains(err.Error(), "TCP listen on") {
		t.Fatalf("TCP bind failure not reported: %v", err)
	}
	if err := run(context.Background(), []string{"-interval", "0"}, io.Discard); err == nil {
		t.Fatal("invalid configuration not reported")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := run(ctx, []string{"-listen", "127.0.0.1:0"}, io.Discard); err != nil {
		t.Fatalf("pre-canceled startup failed graceful shutdown: %v", err)
	}
	if err := run(ctx, []string{"-listen", "127.0.0.1:0", "-tcp-listen", "127.0.0.1:0"}, io.Discard); err != nil {
		t.Fatalf("pre-canceled TCP startup failed graceful shutdown: %v", err)
	}
	var output bytes.Buffer
	if err := run(ctx, []string{"-listen", "127.0.0.1:0", "-tcp-listen", "127.0.0.1:0", "-tcp-bidirectional"}, &output); err != nil {
		t.Fatalf("pre-canceled bidirectional startup failed: %v", err)
	}
	if !strings.Contains(output.String(), "TCP bidirectional request/response") {
		t.Fatalf("startup did not disclose TCP mode: %s", output.String())
	}
}

func TestServeError(t *testing.T) {
	counter, err := cooperative.New()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = listener.Close()
	if err := runServer(context.Background(), counter, listener, time.Second); err == nil || !strings.Contains(err.Error(), "serve counter") {
		t.Fatalf("serve failure not reported: %v", err)
	}
}
