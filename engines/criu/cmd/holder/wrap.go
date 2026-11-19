// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
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
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Annotation keys read from the downward-API annotations file (duplicated as
// literals so the holder stays stdlib-only / scratch-buildable).
const (
	// annPSName is the restore order the wrapper waits for unless
	// --ps-name-annotation names another: the key of the GKE Pod Snapshots
	// group. An engine on another group passes its own key (#135).
	annPSName        = "podsnapshot.gke.io/ps-name"
	annRestoreStatus = "pod-migrate.io/psengine-restore"
	statusRestored   = "restored"
	statusFailed     = "failed"

	// How long to wait at startup for the downward-API file to reflect a
	// ps-name annotation. The pod gate controller patches ps-name BEFORE
	// removing the scheduling gate, so the annotation is normally present
	// on first read; this window absorbs kubelet volume-projection lag on
	// the bypass path (gate removed with ps-name="").
	defaultAssignWait = 2 * time.Second
	// How long to wait for the node agent to finish the restore before
	// falling through to a cold start (fail-open: availability beats warm
	// state).
	defaultRestoreWait = 90 * time.Second
	defaultGracePeriod = 30 * time.Second
	pollInterval       = 100 * time.Millisecond
)

type wrapOutcome int

const (
	outcomeColdStart wrapOutcome = iota
	outcomeHold
)

// decideAction polls the downward-API annotations file and chooses between
// holding the sandbox (restore succeeded) and falling through to the
// original entrypoint (bypass, restore failed, or timeout). psNameAnnotation
// is the restore order to wait for. Pure over the file path + timeouts so
// unit tests run in milliseconds.
func decideAction(annotationsFile, psNameAnnotation string, assignWait, restoreWait, poll time.Duration) (wrapOutcome, string) {
	// Phase 1 — is this pod ordered to restore?
	assignDeadline := time.Now().Add(assignWait)
	psName := ""
	for {
		anns := readAnnotationsFile(annotationsFile)
		if v, ok := anns[psNameAnnotation]; ok {
			if v != "" {
				psName = v
				break
			}
			return outcomeColdStart, "explicit cold-start bypass (empty ps-name)"
		}
		if time.Now().After(assignDeadline) {
			return outcomeColdStart, "no ps-name within assign window; falling through to cold start"
		}
		time.Sleep(poll)
	}

	// Phase 2 — ps-name is set; wait for the node agent's restore verdict.
	restoreDeadline := time.Now().Add(restoreWait)
	for {
		anns := readAnnotationsFile(annotationsFile)
		switch anns[annRestoreStatus] {
		case statusRestored:
			return outcomeHold, fmt.Sprintf("restored from %s; holding sandbox", psName)
		case statusFailed:
			return outcomeColdStart, fmt.Sprintf("restore of %s reported failed; falling through to cold start", psName)
		}
		if time.Now().After(restoreDeadline) {
			return outcomeColdStart, fmt.Sprintf("restore of %s timed out after %s; falling through to cold start", psName, restoreWait)
		}
		time.Sleep(poll)
	}
}

// readAnnotationsFile parses a Kubernetes downward-API annotations file
// (one `key="value"` per line, Go-quoted value). Missing/unreadable returns
// an empty map.
func readAnnotationsFile(path string) map[string]string {
	out := map[string]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		if uq, err := strconv.Unquote(v); err == nil {
			v = uq
		} else {
			v = strings.Trim(v, `"`)
		}
		out[k] = v
	}
	return out
}

// wrapUsage is the wrap mode's command line.
const wrapUsage = "usage: holder wrap [--ps-name-annotation=<key>] [--grace-period=<duration>] <annotations-file> -- <cmd> [args...]"

// parseWrapArgs parses the wrap mode's arguments,
// [--ps-name-annotation=<key>] [--grace-period=<duration>] <annotations-file> -- <cmd> [args...], and
// returns the annotations file, the restore-order annotation (annPSName
// unless the flag names another), the termination grace period, and the original command.
func parseWrapArgs(args []string) (annFile, psNameAnnotation string, gracePeriod time.Duration, orig []string, err error) {
	fs := flag.NewFlagSet("wrap", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	key := fs.String("ps-name-annotation", annPSName, "restore-order annotation to wait for")
	gp := fs.Duration("grace-period", defaultGracePeriod, "grace period to wait for sibling container to exit on SIGTERM")
	if err := fs.Parse(args); err != nil {
		return "", "", 0, nil, err
	}
	rest := fs.Args()
	if len(rest) < 3 || rest[1] != "--" {
		return "", "", 0, nil, errors.New("want an annotations file, then --, then the command")
	}
	if *key == "" {
		return "", "", 0, nil, errors.New("--ps-name-annotation is empty")
	}
	if *gp < 0 {
		return "", "", 0, nil, errors.New("--grace-period cannot be negative")
	}
	return rest[0], *key, *gp, rest[2:], nil
}

func runWrap(args []string) {
	annFile, psNameAnnotation, gracePeriod, orig, err := parseWrapArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "holder wrap: %v\n%s\n", err, wrapUsage)
		os.Exit(2)
	}

	outcome, reason := decideAction(annFile, psNameAnnotation, defaultAssignWait, defaultRestoreWait, pollInterval)
	fmt.Fprintf(os.Stderr, "holder wrap: %s\n", reason)

	if outcome == outcomeHold {
		waitGracefulTermination(nil, gracePeriod)
		return
	}
	bin, err := exec.LookPath(orig[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "holder wrap: lookpath %s: %v\n", orig[0], err)
		os.Exit(127)
	}
	if err := syscall.Exec(bin, orig, os.Environ()); err != nil {
		fmt.Fprintf(os.Stderr, "holder wrap: exec %s: %v\n", bin, err)
		os.Exit(126)
	}
}

// waitGracefulTermination holds the pod sandbox alive until the restored
// sibling container finishes exiting, or until the grace period expires.
//
// When kubelet deletes a pod, it delivers SIGTERM to this holder container.
// If the holder exits immediately, kubelet marks all containers in the pod as
// terminated, tears down the pod sandbox (network/IPC namespaces), and issues
// orphaned-cgroup cleanup (SIGKILL), prematurely killing the restored sibling
// container before its grace period elapses (#242, #128).
//
// By intercepting SIGTERM, the holder keeps the pod sandbox and cgroups intact
// so the restored application process receives its termination signal, flushes
// in-flight requests or state, and terminates cleanly.
//
// Termination order:
//  1. Initial SIGTERM: enters graceful wait state.
//  2. Second signal (SIGINT or SIGTERM): sibling container has finished exiting
//     (signaled by node agent or manual intervention) -> exits immediately (0).
//  3. Grace period timeout: kubelet's termination grace period reached -> exits (0).
//
// An initial SIGINT exits immediately.
func waitGracefulTermination(sigCh <-chan os.Signal, gracePeriod time.Duration) {
	if sigCh == nil {
		ch := make(chan os.Signal, 2)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		sigCh = ch
	}
	sig, ok := <-sigCh
	if !ok {
		return
	}
	if sig == syscall.SIGINT || gracePeriod <= 0 {
		return
	}

	timer := time.NewTimer(gracePeriod)
	defer timer.Stop()

	select {
	case _, ok := <-sigCh:
		if !ok {
			return
		}
		// Subsequent signal received (sibling exited and signaled, or operator force-stop)
		return
	case <-timer.C:
		// Grace period expired
		return
	}
}
