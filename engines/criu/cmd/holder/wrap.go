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
const wrapUsage = "usage: holder wrap [--ps-name-annotation=<key>] <annotations-file> -- <cmd> [args...]"

// parseWrapArgs parses the wrap mode's arguments,
// [--ps-name-annotation=<key>] <annotations-file> -- <cmd> [args...], and
// returns the annotations file, the restore-order annotation (annPSName
// unless the flag names another) and the original command.
func parseWrapArgs(args []string) (annFile, psNameAnnotation string, orig []string, err error) {
	fs := flag.NewFlagSet("wrap", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	key := fs.String("ps-name-annotation", annPSName, "restore-order annotation to wait for")
	if err := fs.Parse(args); err != nil {
		return "", "", nil, err
	}
	rest := fs.Args()
	if len(rest) < 3 || rest[1] != "--" {
		return "", "", nil, errors.New("want an annotations file, then --, then the command")
	}
	if *key == "" {
		return "", "", nil, errors.New("--ps-name-annotation is empty")
	}
	return rest[0], *key, rest[2:], nil
}

func runWrap(args []string) {
	annFile, psNameAnnotation, orig, err := parseWrapArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "holder wrap: %v\n%s\n", err, wrapUsage)
		os.Exit(2)
	}

	outcome, reason := decideAction(annFile, psNameAnnotation, defaultAssignWait, defaultRestoreWait, pollInterval)
	fmt.Fprintf(os.Stderr, "holder wrap: %s\n", reason)

	if outcome == outcomeHold {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)
		<-ch
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
