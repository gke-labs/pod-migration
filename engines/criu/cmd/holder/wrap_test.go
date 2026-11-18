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
	"slices"
	"strings"
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
		name     string
		args     []string
		wantFile string
		wantKey  string
		wantOrig []string
		wantErr  bool
	}{
		{
			name:     "no flag: the default key",
			args:     []string{"/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"},
			wantFile: "/pminfo/annotations", wantKey: annPSName,
			wantOrig: []string{"redis-server", "--appendonly", "yes"},
		},
		{
			name:     "named key",
			args:     []string{"--ps-name-annotation=" + key, "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"},
			wantFile: "/pminfo/annotations", wantKey: key,
			wantOrig: []string{"redis-server", "--appendonly", "yes"},
		},
		{
			name:     "named key, single dash",
			args:     []string{"-ps-name-annotation", key, "/pminfo/annotations", "--", "redis-server"},
			wantFile: "/pminfo/annotations", wantKey: key,
			wantOrig: []string{"redis-server"},
		},
		{name: "no separator", args: []string{"/pminfo/annotations", "redis-server"}, wantErr: true},
		{name: "no command", args: []string{"/pminfo/annotations", "--"}, wantErr: true},
		{name: "empty key", args: []string{"--ps-name-annotation=", "/pminfo/annotations", "--", "redis-server"}, wantErr: true},
		{name: "unknown flag", args: []string{"--bogus", "/pminfo/annotations", "--", "redis-server"}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			file, key, orig, err := parseWrapArgs(tc.args)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("parseWrapArgs(%q) = %q, %q, %q, want an error", tc.args, file, key, orig)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseWrapArgs(%q): %v", tc.args, err)
			}
			if file != tc.wantFile || key != tc.wantKey || !slices.Equal(orig, tc.wantOrig) {
				t.Fatalf("parseWrapArgs(%q) = %q, %q, %q, want %q, %q, %q", tc.args, file, key, orig, tc.wantFile, tc.wantKey, tc.wantOrig)
			}
		})
	}
}

// The restore webhook leaves --ps-name-annotation out for the GKE Pod
// Snapshots group's key, so that key must be the holder's default.
func TestDefaultPSNameAnnotationIsTheGKEPodSnapshotsKey(t *testing.T) {
	if want := contract.RestoreAnnotation(contract.GKEPodSnapshotsGroup); annPSName != want {
		t.Fatalf("holder default restore order %q, want %q", annPSName, want)
	}
}
