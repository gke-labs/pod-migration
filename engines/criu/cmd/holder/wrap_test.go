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
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

func writeAnns(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestReadAnnotationsFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, "pod-migration.gke.io/assigned-pmj=\"pmj-1\"\npodsnapshot.gke.io/ps-name=\"ps-web-0-deadbeef\"\nempty=\"\"\n")

	m := readAnnotationsFile(f)
	if m[annPSName] != "ps-web-0-deadbeef" || m["empty"] != "" {
		t.Fatalf("parsed: %#v", m)
	}
}

func TestDecideActionEmptyPSNameBypasses(t *testing.T) {
	// Bypass path: ps-name is explicitly empty -> cold-start immediately.
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `podsnapshot.gke.io/ps-name=""`+"\n")

	got, reason := decideAction(f, annPSName, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "explicit cold-start bypass") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionNoPSNameFallsThrough(t *testing.T) {
	// Missing ps-name entirely: falls through to cold start after assign window.
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `other="foo"`+"\n")

	got, reason := decideAction(f, annPSName, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "no ps-name within assign window") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionRestoreSucceededHolds(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `podsnapshot.gke.io/ps-name="ps-web-0-deadbeef"`+"\n")
	go func() {
		time.Sleep(20 * time.Millisecond)
		writeAnns(t, f, "podsnapshot.gke.io/ps-name=\"ps-web-0-deadbeef\"\npod-migrate.io/psengine-restore=\"restored\"\n")
	}()

	got, reason := decideAction(f, annPSName, 50*time.Millisecond, 500*time.Millisecond, 5*time.Millisecond)
	if got != outcomeHold || !strings.Contains(reason, "restored from ps-web-0-deadbeef") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionRestoreFailedFallsThrough(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, "podsnapshot.gke.io/ps-name=\"ps-web-0-deadbeef\"\npod-migrate.io/psengine-restore=\"failed\"\n")

	got, reason := decideAction(f, annPSName, 30*time.Millisecond, 200*time.Millisecond, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "reported failed") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionRestoreTimeoutFallsThrough(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `podsnapshot.gke.io/ps-name="ps-web-0-deadbeef"`+"\n")

	got, reason := decideAction(f, annPSName, 20*time.Millisecond, 40*time.Millisecond, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "timed out") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

// An engine on its own API group names its restore order; the wrapper waits
// for that key and nothing else (#135).
func TestDecideActionWaitsForTheNamedAnnotation(t *testing.T) {
	const key = "criu.podmigration.gke.io/ps-name"
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, key+`="ps-web-0-deadbeef"`+"\n"+`pod-migrate.io/psengine-restore="restored"`+"\n")

	got, reason := decideAction(f, key, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeHold {
		t.Fatalf("want hold on the named key, got %v (%s)", got, reason)
	}

	got, reason = decideAction(f, annPSName, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "no ps-name") {
		t.Fatalf("a restore order under another key isn't this pod's, so it must start cold: %v (%s)", got, reason)
	}

	// The reverse: a holder waiting on the custom key ignores a restore order
	// under the default key.
	g := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, g, annPSName+`="ps-web-0-deadbeef"`+"\n"+`pod-migrate.io/psengine-restore="restored"`+"\n")
	got, reason = decideAction(g, key, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "no ps-name") {
		t.Fatalf("a restore order under the default key isn't this pod's, so it must start cold: %v (%s)", got, reason)
	}
}

func TestParseWrapArgs(t *testing.T) {
	const key = "criu.podmigration.gke.io/ps-name"
	for _, tc := range []struct {
		name      string
		args      []string
		wantFile  string
		wantKey   string
		wantGrace time.Duration
		wantOrig  []string
		wantErr   bool
	}{
		{
			name:     "no flag: the default key and default grace period",
			args:     []string{"/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"},
			wantFile: "/pminfo/annotations", wantKey: annPSName, wantGrace: defaultGracePeriod,
			wantOrig: []string{"redis-server", "--appendonly", "yes"},
		},
		{
			name:     "named key",
			args:     []string{"--ps-name-annotation=" + key, "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"},
			wantFile: "/pminfo/annotations", wantKey: key, wantGrace: defaultGracePeriod,
			wantOrig: []string{"redis-server", "--appendonly", "yes"},
		},
		{
			name:     "named key, single dash",
			args:     []string{"-ps-name-annotation", key, "/pminfo/annotations", "--", "redis-server"},
			wantFile: "/pminfo/annotations", wantKey: key, wantGrace: defaultGracePeriod,
			wantOrig: []string{"redis-server"},
		},
		{
			name:     "custom grace period",
			args:     []string{"--grace-period=45s", "/pminfo/annotations", "--", "redis-server"},
			wantFile: "/pminfo/annotations", wantKey: annPSName, wantGrace: 45 * time.Second,
			wantOrig: []string{"redis-server"},
		},
		{
			name:     "named key and custom grace period",
			args:     []string{"--ps-name-annotation=" + key, "--grace-period=1m", "/pminfo/annotations", "--", "redis-server"},
			wantFile: "/pminfo/annotations", wantKey: key, wantGrace: time.Minute,
			wantOrig: []string{"redis-server"},
		},
		{name: "negative grace period", args: []string{"--grace-period=-5s", "/pminfo/annotations", "--", "redis-server"}, wantErr: true},
		{name: "no separator", args: []string{"/pminfo/annotations", "redis-server"}, wantErr: true},
		{name: "no separator, longer command", args: []string{"/pminfo/annotations", "redis-server", "--appendonly", "yes"}, wantErr: true},
		{name: "no command", args: []string{"/pminfo/annotations", "--"}, wantErr: true},
		{name: "empty key", args: []string{"--ps-name-annotation=", "/pminfo/annotations", "--", "redis-server"}, wantErr: true},
		{name: "unknown flag", args: []string{"--bogus", "/pminfo/annotations", "--", "redis-server"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, key, grace, orig, err := parseWrapArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseWrapArgs(%q) = %q, %q, %v, %q, want an error", tc.args, file, key, grace, orig)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseWrapArgs(%q): %v", tc.args, err)
			}
			if file != tc.wantFile || key != tc.wantKey || grace != tc.wantGrace || !slices.Equal(orig, tc.wantOrig) {
				t.Fatalf("parseWrapArgs(%q) = %q, %q, %v, %q, want %q, %q, %v, %q", tc.args, file, key, grace, orig, tc.wantFile, tc.wantKey, tc.wantGrace, tc.wantOrig)
			}
		})
	}
}

func TestWaitGracefulTermination_ImmediateExitOnSIGINT(t *testing.T) {
	ch := make(chan os.Signal, 2)
	ch <- syscall.SIGINT
	done := make(chan struct{})
	go func() {
		waitGracefulTermination(ch, 5*time.Second)
		close(done)
	}()

	select {
	case <-done:
		// expected fast return
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitGracefulTermination did not exit immediately on SIGINT")
	}
}

func TestWaitGracefulTermination_ImmediateExitOnZeroGracePeriod(t *testing.T) {
	ch := make(chan os.Signal, 2)
	ch <- syscall.SIGTERM
	done := make(chan struct{})
	go func() {
		waitGracefulTermination(ch, 0)
		close(done)
	}()

	select {
	case <-done:
		// expected fast return
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitGracefulTermination did not exit immediately on gracePeriod <= 0")
	}
}

func TestWaitGracefulTermination_WaitsForSecondSignal(t *testing.T) {
	ch := make(chan os.Signal, 2)
	ch <- syscall.SIGTERM
	done := make(chan struct{})
	go func() {
		waitGracefulTermination(ch, 5*time.Second)
		close(done)
	}()

	// Confirm it is waiting and did NOT exit immediately
	select {
	case <-done:
		t.Fatal("waitGracefulTermination exited prematurely before second signal or timeout")
	case <-time.After(50 * time.Millisecond):
	}

	// Send second signal (simulating node agent signaling sibling container exit)
	ch <- syscall.SIGINT

	select {
	case <-done:
		// exited upon receiving second signal
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitGracefulTermination did not exit on second signal")
	}
}

func TestWaitGracefulTermination_WaitsForSecondSIGTERM(t *testing.T) {
	ch := make(chan os.Signal, 2)
	ch <- syscall.SIGTERM
	done := make(chan struct{})
	go func() {
		waitGracefulTermination(ch, 5*time.Second)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("waitGracefulTermination exited prematurely before second signal or timeout")
	case <-time.After(50 * time.Millisecond):
	}

	// Send second SIGTERM
	ch <- syscall.SIGTERM

	select {
	case <-done:
		// exited upon receiving second signal
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitGracefulTermination did not exit on second SIGTERM")
	}
}

func TestWaitGracefulTermination_ExpiresAfterGracePeriod(t *testing.T) {
	ch := make(chan os.Signal, 2)
	ch <- syscall.SIGTERM
	done := make(chan struct{})
	start := time.Now()
	go func() {
		waitGracefulTermination(ch, 50*time.Millisecond)
		close(done)
	}()

	select {
	case <-done:
		if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
			t.Fatalf("waitGracefulTermination returned too early: %v", elapsed)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("waitGracefulTermination timed out without honoring grace period")
	}
}

// The restore webhook leaves --ps-name-annotation out for the GKE Pod
// Snapshots group's key, so that key must be the holder's default.
func TestDefaultPSNameAnnotationIsTheGKEPodSnapshotsKey(t *testing.T) {
	if want := contract.RestoreAnnotation(contract.GKEPodSnapshotsGroup); annPSName != want {
		t.Fatalf("holder default restore order %q, want %q", annPSName, want)
	}
}

func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}
	args := os.Args
	for i, arg := range args {
		if arg == "--" {
			args = args[i+1:]
			break
		}
	}
	runWrap(args)
	os.Exit(0)
}

func TestRunWrap_HoldWaitsForSIGTERM_Subprocess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess test in short mode")
	}
	annFile := filepath.Join(t.TempDir(), "annotations")
	content := fmt.Sprintf("%s=%q\n%s=%q\n", annPSName, "snap-test", annRestoreStatus, statusRestored)
	if err := os.WriteFile(annFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "--", "--grace-period=5s", annFile, "--", "echo", "done")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stderrPipe)
	enteredHold := make(chan bool, 1)
	go func() {
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "holding sandbox") {
				enteredHold <- true
				return
			}
		}
		enteredHold <- false
	}()

	select {
	case ok := <-enteredHold:
		if !ok {
			_ = cmd.Process.Kill()
			t.Fatal("subprocess failed to enter hold mode")
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("timed out waiting for subprocess to enter hold mode")
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- cmd.Wait()
	}()

	// 1. Verify subprocess did not exit immediately upon entering outcomeHold
	// (pins that runWrap's hold path does not skip waitGracefulTermination).
	select {
	case err := <-waitErr:
		t.Fatalf("runWrap exited prematurely without waiting: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// 2. Deliver real SIGTERM (simulating kubelet pod deletion).
	// If waitGracefulTermination only registered SIGINT (and not SIGTERM),
	// the unhandled SIGTERM would kill the process with signal: terminated.
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	// Verify subprocess does NOT exit immediately upon receiving first SIGTERM:
	select {
	case err := <-waitErr:
		t.Fatalf("runWrap exited prematurely immediately after first SIGTERM: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// 3. Deliver second signal (SIGINT, simulating wake-up from node agent).
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatalf("failed to send SIGINT: %v", err)
	}

	// Verify subprocess exits cleanly with code 0:
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("runWrap did not exit cleanly (code 0) after second signal: %v", err)
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("timed out waiting for runWrap to exit after second signal")
	}
}

func TestRunWrap_HoldWaitsForSIGTERM_GracePeriodTimeout_Subprocess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping subprocess test in short mode")
	}
	annFile := filepath.Join(t.TempDir(), "annotations")
	content := fmt.Sprintf("%s=%q\n%s=%q\n", annPSName, "snap-test", annRestoreStatus, statusRestored)
	if err := os.WriteFile(annFile, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestHelperProcess", "--", "--grace-period=150ms", annFile, "--", "echo", "done")
	cmd.Env = append(os.Environ(), "GO_WANT_HELPER_PROCESS=1")
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	scanner := bufio.NewScanner(stderrPipe)
	enteredHold := make(chan bool, 1)
	go func() {
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "holding sandbox") {
				enteredHold <- true
				return
			}
		}
		enteredHold <- false
	}()

	select {
	case ok := <-enteredHold:
		if !ok {
			_ = cmd.Process.Kill()
			t.Fatal("subprocess failed to enter hold mode")
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("timed out waiting for subprocess to enter hold mode")
	}

	waitErr := make(chan error, 1)
	go func() {
		waitErr <- cmd.Wait()
	}()

	// Verify subprocess does not exit immediately upon entering outcomeHold:
	select {
	case err := <-waitErr:
		t.Fatalf("runWrap exited prematurely without waiting: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Deliver real SIGTERM:
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("failed to send SIGTERM: %v", err)
	}

	// Do NOT send second signal; verify it exits cleanly (code 0) after grace period timeout (~150ms):
	select {
	case err := <-waitErr:
		if err != nil {
			t.Fatalf("runWrap failed to exit cleanly on grace period timeout: %v", err)
		}
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("timed out waiting for runWrap to exit on grace period timeout")
	}
}
