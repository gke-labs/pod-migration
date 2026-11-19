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

package contract

import (
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// contractGVKs returns the four contract GVKs by kind, as they are now.
func contractGVKs() map[string]schema.GroupVersionKind {
	return map[string]schema.GroupVersionKind{
		"PodSnapshotStorageConfig": GVKStorageConfig,
		"PodSnapshotPolicy":        GVKPolicy,
		"PodSnapshotManualTrigger": GVKManualTrigger,
		"PodSnapshot":              GVKPodSnapshot,
	}
}

// assertGroup checks that the contract is entirely in group.
func assertGroup(t *testing.T, group string) {
	t.Helper()
	if Group != group {
		t.Errorf("Group = %q, want %q", Group, group)
	}
	if want := group + "/ps-name"; AnnotationPSName != want {
		t.Errorf("AnnotationPSName = %q, want %q", AnnotationPSName, want)
	}
	for kind, got := range contractGVKs() {
		if want := (schema.GroupVersionKind{Group: group, Version: "v1", Kind: kind}); got != want {
			t.Errorf("GVK of %s = %v, want %v", kind, got, want)
		}
	}
	if GVKPodMigrationJob.Group != "podmigration.gke.io" {
		t.Errorf("the orchestrator's job kind must keep its own group, got %v", GVKPodMigrationJob)
	}
}

// Without SetGroup, the engine serves its dedicated CRIU API group (#135).
func TestDefaultGroup(t *testing.T) {
	if CRIUGroup != "criu.podmigration.gke.io" {
		t.Fatalf("CRIUGroup = %q, want criu.podmigration.gke.io", CRIUGroup)
	}
	if GKEPodSnapshotsGroup != "podsnapshot.gke.io" {
		t.Fatalf("GKEPodSnapshotsGroup = %q, want podsnapshot.gke.io", GKEPodSnapshotsGroup)
	}
	if DefaultGroup != CRIUGroup {
		t.Fatalf("DefaultGroup = %q, want %q", DefaultGroup, CRIUGroup)
	}
	assertGroup(t, DefaultGroup)

	want := []string{
		"podsnapshotstorageconfigs.criu.podmigration.gke.io",
		"podsnapshotpolicies.criu.podmigration.gke.io",
		"podsnapshotmanualtriggers.criu.podmigration.gke.io",
		"podsnapshots.criu.podmigration.gke.io",
	}
	if got := CRDNames(DefaultGroup); !slices.Equal(got, want) {
		t.Fatalf("CRDNames(%q) = %v, want %v", DefaultGroup, got, want)
	}
}

// SetGroup moves the whole contract, restore order included, to another
// group, and the objects built afterwards follow it.
func TestSetGroup(t *testing.T) {
	t.Cleanup(func() {
		if err := SetGroup(DefaultGroup); err != nil {
			t.Errorf("restoring the default group: %v", err)
		}
		assertGroup(t, DefaultGroup)
	})
	const group = GKEPodSnapshotsGroup
	if err := SetGroup(group); err != nil {
		t.Fatalf("SetGroup(%q): %v", group, err)
	}
	assertGroup(t, group)

	if got := New(GVKPodSnapshot).GetAPIVersion(); got != group+"/v1" {
		t.Errorf("New(GVKPodSnapshot) apiVersion = %q, want %q", got, group+"/v1")
	}
	if got := NewList(GVKPolicy).GetObjectKind().GroupVersionKind(); got != (schema.GroupVersionKind{Group: group, Version: "v1", Kind: "PodSnapshotPolicyList"}) {
		t.Errorf("NewList(GVKPolicy) GVK = %v", got)
	}
	for _, name := range CRDNames(Group) {
		if !strings.HasSuffix(name, "."+group) {
			t.Errorf("CRD name %q isn't in %s", name, group)
		}
	}
}

// A group that a CustomResourceDefinition can't declare is rejected, and
// leaves the contract on the group it was serving.
func TestSetGroupRejectsInvalidGroups(t *testing.T) {
	t.Cleanup(func() { _ = SetGroup(DefaultGroup) })
	// Start each case from a group other than the default, so that falling
	// back to the default on error fails the test.
	const serving = GKEPodSnapshotsGroup
	for name, group := range map[string]string{
		"empty":                        "",
		"no dot":                       "criu",
		"upper case":                   "CRIU.podmigration.gke.io",
		"underscore":                   "criu_engine.podmigration.gke.io",
		"leading dash":                 "-criu.podmigration.gke.io",
		"trailing dot":                 "criu.podmigration.gke.io.",
		"too long for its CRD names":   strings.Repeat(strings.Repeat("a", 56)+".", 4) + "io",
		"longer than a DNS subdomain":  strings.Repeat(strings.Repeat("a", 60)+".", 5) + "io",
		"slash, not a group":           "criu.podmigration.gke.io/v1",
		"whitespace around the group":  " criu.podmigration.gke.io",
		"a restore annotation instead": "criu.podmigration.gke.io/ps-name",
	} {
		t.Run(name, func(t *testing.T) {
			if err := SetGroup(serving); err != nil {
				t.Fatalf("SetGroup(%q): %v", serving, err)
			}
			if err := SetGroup(group); err == nil {
				t.Fatalf("SetGroup(%q) succeeded, want an error", group)
			}
			assertGroup(t, serving)
		})
	}
}

// RestoreAnnotation and CRDNames derive from the group they're given, not
// from Group.
func TestRestoreAnnotationAndCRDNames(t *testing.T) {
	if got := RestoreAnnotation("criu.podmigration.gke.io"); got != "criu.podmigration.gke.io/ps-name" {
		t.Errorf("RestoreAnnotation = %q", got)
	}
	if got := RestoreAnnotation(GKEPodSnapshotsGroup); got != "podsnapshot.gke.io/ps-name" {
		t.Errorf("RestoreAnnotation(%q) = %q", GKEPodSnapshotsGroup, got)
	}
	want := []string{
		"podsnapshotstorageconfigs.criu.podmigration.gke.io",
		"podsnapshotpolicies.criu.podmigration.gke.io",
		"podsnapshotmanualtriggers.criu.podmigration.gke.io",
		"podsnapshots.criu.podmigration.gke.io",
	}
	if got := CRDNames("criu.podmigration.gke.io"); !slices.Equal(got, want) {
		t.Errorf("CRDNames = %v, want %v", got, want)
	}
}
