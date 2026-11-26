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

package snapshot

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestStatusFromConditions_ReadyReportsWhenItBecameReady (#231): a Ready
// snapshot reports when it became Ready as its LastProgressTime, so that I4
// can tell how long a job has stayed in Snapshotting after it. That's the
// earliest lastTransitionTime of the conditions that make it Ready (Ready=True,
// or Checkpoint=True with reason Succeeded), and zero when they carry none.
func TestStatusFromConditions_ReadyReportsWhenItBecameReady(t *testing.T) {
	day := time.Date(2026, 11, 26, 0, 0, 0, 0, time.UTC)
	at := func(h, m int) time.Time { return day.Add(time.Duration(h)*time.Hour + time.Duration(m)*time.Minute) }
	cond := func(typ, status, reason string, ltt time.Time) metav1.Condition {
		return metav1.Condition{Type: typ, Status: metav1.ConditionStatus(status), Reason: reason, LastTransitionTime: metav1.NewTime(ltt)}
	}
	created, now := at(9, 50), at(10, 30)

	tests := map[string]struct {
		conditions []metav1.Condition
		want       time.Time
	}{
		"Ready=True": {
			conditions: []metav1.Condition{cond("Ready", "True", "AllSnapshotsAvailable", at(10, 0))},
			want:       at(10, 0),
		},
		// GKE reports the checkpoint done while it still uploads it. The
		// snapshot is Ready from then on, so the upload's later progress
		// doesn't move the time.
		"checkpoint succeeded, upload still running": {
			conditions: []metav1.Condition{
				cond("Checkpoint", "True", "Succeeded", at(10, 0)),
				cond("StorageReplicated", "False", "InProgress", at(10, 5)),
			},
			want: at(10, 0),
		},
		"checkpoint succeeded, then Ready=True once uploaded": {
			conditions: []metav1.Condition{
				cond("Checkpoint", "True", "Succeeded", at(10, 0)),
				cond("StorageReplicated", "True", "Succeeded", at(10, 15)),
				cond("Ready", "True", "AllSnapshotsAvailable", at(10, 20)),
			},
			want: at(10, 0),
		},
		"Ready=True listed before the checkpoint": {
			conditions: []metav1.Condition{
				cond("Ready", "True", "AllSnapshotsAvailable", at(10, 20)),
				cond("Checkpoint", "True", "Succeeded", at(10, 0)),
			},
			want: at(10, 0),
		},
		"Checkpoint=True without reason Succeeded doesn't make it Ready": {
			conditions: []metav1.Condition{
				cond("Checkpoint", "True", "Completed", at(10, 0)),
				cond("Ready", "True", "AllSnapshotsAvailable", at(10, 20)),
			},
			want: at(10, 20),
		},
		// A condition without a time doesn't hide the other one's.
		"checkpoint succeeded, then Ready=True without a lastTransitionTime": {
			conditions: []metav1.Condition{
				cond("Checkpoint", "True", "Succeeded", at(10, 0)),
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSnapshotsAvailable"},
			},
			want: at(10, 0),
		},
		// No guessing: neither the creation time nor another condition's
		// time stands in for an unknown Ready time.
		"Ready=True without a lastTransitionTime": {
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSnapshotsAvailable"},
				cond("StorageReplicated", "True", "Succeeded", at(10, 15)),
			},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := StatusFromConditions("snap", tc.conditions, created, "", now)
			if got.Phase != PhaseReady {
				t.Fatalf("Phase = %v, want %v", got.Phase, PhaseReady)
			}
			if !got.LastProgressTime.Equal(tc.want) {
				t.Errorf("LastProgressTime = %v, want %v", got.LastProgressTime, tc.want)
			}
		})
	}
}

// TestGKEProviderCheckStatus_ReadyReportsWhenItBecameReady covers the
// unstructured paths that feed the reconciler (CheckStatus) and the
// invariant inputs (CheckPodSnapshotStatus).
func TestGKEProviderCheckStatus_ReadyReportsWhenItBecameReady(t *testing.T) {
	readySince := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	snap := checkpointTestSnapshot("snap", true, [3]string{"Ready", "True", "AllSnapshotsAvailable"})
	conds, _, _ := unstructured.NestedSlice(snap.Object, "status", "conditions")
	conds[0].(map[string]interface{})["lastTransitionTime"] = readySince.Format(time.RFC3339)
	if err := unstructured.SetNestedSlice(snap.Object, conds, "status", "conditions"); err != nil {
		t.Fatalf("setting conditions: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(checkpointTestTrigger("snap"), snap.DeepCopy()).Build()

	got, err := NewGKEProvider(c, setupTestScheme()).CheckStatus(context.Background(), checkpointTestJob(""), checkpointTestPod)
	if err != nil {
		t.Fatalf("CheckStatus: %v", err)
	}
	if got.Phase != PhaseReady || !got.LastProgressTime.Equal(readySince) {
		t.Errorf("CheckStatus = %s since %v, want %s since %v", got.Phase, got.LastProgressTime, PhaseReady, readySince)
	}
	cached := CheckPodSnapshotStatus(snap, readySince.Add(30*time.Minute))
	if cached.Phase != PhaseReady || !cached.LastProgressTime.Equal(readySince) {
		t.Errorf("CheckPodSnapshotStatus = %s since %v, want %s since %v", cached.Phase, cached.LastProgressTime, PhaseReady, readySince)
	}
}
