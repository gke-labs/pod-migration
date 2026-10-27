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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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

func TestDecideActionNoPSNameHolds(t *testing.T) {
	// Bypass / non-snapshot path: ps-name is empty -> hold after the short
	// assign window (avoids zombie dual-start when downward API lags).
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `podsnapshot.gke.io/ps-name=""`+"\n")

	got, reason := decideAction(f, 30*time.Millisecond, time.Second, 5*time.Millisecond)
	if got != outcomeHold || !strings.Contains(reason, "no ps-name") {
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

	got, reason := decideAction(f, 50*time.Millisecond, 500*time.Millisecond, 5*time.Millisecond)
	if got != outcomeHold || !strings.Contains(reason, "restored from ps-web-0-deadbeef") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionRestoreFailedFallsThrough(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, "podsnapshot.gke.io/ps-name=\"ps-web-0-deadbeef\"\npod-migrate.io/psengine-restore=\"failed\"\n")

	got, reason := decideAction(f, 30*time.Millisecond, 200*time.Millisecond, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "reported failed") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}

func TestDecideActionRestoreTimeoutFallsThrough(t *testing.T) {
	f := filepath.Join(t.TempDir(), "annotations")
	writeAnns(t, f, `podsnapshot.gke.io/ps-name="ps-web-0-deadbeef"`+"\n")

	got, reason := decideAction(f, 20*time.Millisecond, 40*time.Millisecond, 5*time.Millisecond)
	if got != outcomeColdStart || !strings.Contains(reason, "timed out") {
		t.Fatalf("got %v (%s)", got, reason)
	}
}
