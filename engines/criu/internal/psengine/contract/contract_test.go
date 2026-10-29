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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
)

// psscFixture mirrors exactly what the orchestrator's config reconciler
// writes (podmigration_controller.go:88-98).
func psscFixture(bucket, path string) *unstructured.Unstructured {
	u := New(GVKStorageConfig)
	u.SetName("pssc-abcdef0123456789")
	gcs := map[string]interface{}{"bucket": bucket}
	if path != "" {
		gcs["path"] = path
	}
	u.Object["spec"] = map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{"gcs": gcs},
	}
	return u
}

// pspFixture mirrors podmigration_controller.go:118-133.
func pspFixture() *unstructured.Unstructured {
	u := New(GVKPolicy)
	u.SetName("psp-config-manual")
	u.SetNamespace("default")
	u.Object["spec"] = map[string]interface{}{
		"storageConfigName": "pssc-abcdef0123456789",
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      LabelEnabled,
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	}
	return u
}

func TestGCSLocation(t *testing.T) {
	b, p, err := GCSLocation(psscFixture("bkt", "snapshots"))
	if err != nil || b != "bkt" || p != "snapshots" {
		t.Fatalf("got %q %q %v", b, p, err)
	}
	if _, _, err := GCSLocation(New(GVKStorageConfig)); err == nil {
		t.Fatal("expected error on missing bucket")
	}
	// path is optional (podmigration_controller.go:90-92).
	if _, p, err := GCSLocation(psscFixture("bkt", "")); err != nil || p != "" {
		t.Fatalf("path should be optional: %q %v", p, err)
	}
}

func TestPolicyAccessors(t *testing.T) {
	psp := pspFixture()
	if name, ok := PolicyStorageConfigName(psp); !ok || name != "pssc-abcdef0123456789" {
		t.Fatalf("storageConfigName: %q %v", name, ok)
	}
	if !PolicyIsManualStop(psp) {
		t.Fatal("fixture must be manual+stop")
	}
	// postCheckpoint != stop is rejected (eviction_webhook.go:318-321).
	_ = unstructured.SetNestedField(psp.Object, "keepRunning", "spec", "triggerConfig", "postCheckpoint")
	if PolicyIsManualStop(psp) {
		t.Fatal("non-stop policy must not qualify")
	}

	sel, err := PolicySelector(pspFixture())
	if err != nil {
		t.Fatal(err)
	}
	if !sel.Matches(labels.Set{LabelEnabled: "true"}) {
		t.Fatal("selector must match opted-in pods")
	}
	if sel.Matches(labels.Set{}) {
		t.Fatal("selector must not match unlabeled pods")
	}
}

func TestTriggerRoundTrip(t *testing.T) {
	psmt := New(GVKManualTrigger)
	psmt.SetName("psmt-web-0-deadbeef")
	psmt.SetNamespace("default")
	_ = unstructured.SetNestedField(psmt.Object, "web-0", "spec", "targetPod")

	if name, ok := TriggerTargetPod(psmt); !ok || name != "web-0" {
		t.Fatalf("targetPod: %q %v", name, ok)
	}
	if err := SetTriggerSnapshotCreated(psmt, "ps-web-0-deadbeef"); err != nil {
		t.Fatal(err)
	}
	if got := TriggerSnapshotCreated(psmt); got != "ps-web-0-deadbeef" {
		t.Fatalf("snapshotCreated.name: %q", got)
	}
}

func TestConditionsAndReadiness(t *testing.T) {
	ps := New(GVKPodSnapshot)
	ps.SetName("ps-web-0-deadbeef")

	if IsSnapshotReady(ps) {
		t.Fatal("empty snapshot must not be ready")
	}

	// The orchestrator accepts EITHER Ready=True OR Checkpoint=True/Succeeded
	// (podmigrationjob_controller.go:310-324). The engine sets both.
	if err := SetCondition(ps, metav1.Condition{Type: CondCheckpoint, Status: metav1.ConditionTrue, Reason: ReasonSucceeded, Message: "uploaded + verified"}); err != nil {
		t.Fatal(err)
	}
	if !IsSnapshotReady(ps) {
		t.Fatal("Checkpoint=True/Succeeded alone must satisfy readiness")
	}

	// Refused is terminal-not-ready.
	ps2 := New(GVKPodSnapshot)
	_ = SetCondition(ps2, metav1.Condition{Type: CondCheckpoint, Status: metav1.ConditionFalse, Reason: ReasonRefused, Message: "multi-container pods unsupported by criu engine v1"})
	_ = SetCondition(ps2, metav1.Condition{Type: CondReady, Status: metav1.ConditionFalse, Reason: ReasonRefused})
	if IsSnapshotReady(ps2) {
		t.Fatal("refused snapshot must not be ready")
	}
	if got := len(Conditions(ps2)); got != 2 {
		t.Fatalf("want 2 conditions, got %d", got)
	}

	// Upsert semantics: flipping the same type must not append.
	_ = SetCondition(ps2, metav1.Condition{Type: CondReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded})
	if got := len(Conditions(ps2)); got != 2 {
		t.Fatalf("upsert appended: %d conditions", got)
	}
	if !IsSnapshotReady(ps2) {
		t.Fatal("Ready=True must satisfy readiness")
	}
}

func TestPodSnapshotName(t *testing.T) {
	if got := PodSnapshotName("web-0", "deadbeef-1234-5678"); got != "ps-web-0-deadbeef" {
		t.Fatalf("got %q", got)
	}
	if got := PodSnapshotName("web-0", "abc"); got != "ps-web-0-abc" {
		t.Fatalf("short UID: %q", got)
	}
}
