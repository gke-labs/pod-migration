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
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// markDeleting gives snap a deletionTimestamp, held by a finalizer as the
// CRIU engine controller's archive cleanup (#260) or GKE's own would.
func markDeleting(snap *unstructured.Unstructured) *unstructured.Unstructured {
	snap.SetFinalizers([]string{"criu.podmigration.gke.io/archive-cleanup"})
	now := metav1.Now()
	snap.SetDeletionTimestamp(&now)
	return snap
}

// TestGKEProviderCheckStatus_DeletingSnapshotIsNeverReady: a PodSnapshot
// that's being deleted is not a durable checkpoint, even when it reports
// Ready (#265). Its archive is going away, so the job must not move to
// Evicting on it. It isn't a failure either: the checkpoint may have
// completed, so the source must stay fenced (C4), which the timeout and the
// fence decision after it take care of.
func TestGKEProviderCheckStatus_DeletingSnapshotIsNeverReady(t *testing.T) {
	checkpointed := [3]string{"Checkpoint", "True", "Succeeded"}
	ready := [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	snap := markDeleting(checkpointTestSnapshot("snap", true, checkpointed, ready))
	c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(checkpointTestTrigger("snap"), snap).Build()

	got, err := NewGKEProvider(c, setupTestScheme()).CheckStatus(context.Background(), checkpointTestJob(""), checkpointTestPod)
	if err != nil {
		t.Fatalf("CheckStatus: %v", err)
	}
	if got.Phase != PhaseInProgress {
		t.Fatalf("Phase = %v, want %v for a Ready snapshot that's being deleted", got.Phase, PhaseInProgress)
	}
	if got.Reason != ReasonSnapshotDeleting {
		t.Errorf("Reason = %q, want %q", got.Reason, ReasonSnapshotDeleting)
	}
	if got.SnapshotRef != "snap" {
		t.Errorf("SnapshotRef = %q, want snap", got.SnapshotRef)
	}
	if !got.LastProgressTime.IsZero() {
		t.Errorf("LastProgressTime = %v, want zero: a snapshot being deleted makes no progress, so it mustn't extend the deadline", got.LastProgressTime)
	}
}

// TestCheckPodSnapshotStatus_DeletingSnapshot covers the cached path the
// invariant inputs use, for each outcome a deleting snapshot can report.
func TestCheckPodSnapshotStatus_DeletingSnapshot(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Second).UTC().Format(time.RFC3339Nano)
	withRecentProgress := func(snap *unstructured.Unstructured) *unstructured.Unstructured {
		conds, _, _ := unstructured.NestedSlice(snap.Object, "status", "conditions")
		for _, c := range conds {
			c.(map[string]interface{})["lastTransitionTime"] = recent
		}
		_ = unstructured.SetNestedSlice(snap.Object, conds, "status", "conditions")
		return snap
	}

	tests := map[string]struct {
		snap       *unstructured.Unstructured
		wantPhase  Phase
		wantReason string
	}{
		"Ready": {
			snap:       checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "True", "Succeeded"}, [3]string{"Ready", "True", "AllSnapshotsAvailable"}),
			wantPhase:  PhaseInProgress,
			wantReason: ReasonSnapshotDeleting,
		},
		"checkpoint succeeded, upload still running": {
			snap:       withRecentProgress(checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "True", "Succeeded"}, [3]string{"StorageReplicated", "False", "InProgress"})),
			wantPhase:  PhaseInProgress,
			wantReason: ReasonSnapshotDeleting,
		},
		"still checkpointing, with recent progress": {
			snap:       withRecentProgress(checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "False", "InProgress"})),
			wantPhase:  PhaseInProgress,
			wantReason: ReasonSnapshotDeleting,
		},
		// A terminal outcome is still reported as it is: the snapshot can't
		// become usable either way, and the fence decision reads deletion
		// through CheckpointState, which holds the fence until it's gone.
		"failed": {
			snap:       checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "False", "Failed"}),
			wantPhase:  PhaseFailed,
			wantReason: "SnapshotFailed",
		},
		"refused": {
			snap:       checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "False", "Refused"}),
			wantPhase:  PhaseRefused,
			wantReason: "SnapshotFailed",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := CheckPodSnapshotStatus(markDeleting(tc.snap), now)
			if got.Phase != tc.wantPhase {
				t.Fatalf("Phase = %v, want %v", got.Phase, tc.wantPhase)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason = %q, want %q", got.Reason, tc.wantReason)
			}
			if tc.wantPhase == PhaseInProgress && !got.LastProgressTime.IsZero() {
				t.Errorf("LastProgressTime = %v, want zero for a snapshot being deleted", got.LastProgressTime)
			}
		})
	}

	// The same snapshots, not being deleted, keep today's outcomes.
	notDeleting := checkpointTestSnapshot("snap", true, [3]string{"Checkpoint", "True", "Succeeded"}, [3]string{"Ready", "True", "AllSnapshotsAvailable"})
	if got := CheckPodSnapshotStatus(notDeleting, now); got.Phase != PhaseReady {
		t.Errorf("Phase = %v, want %v for a Ready snapshot that isn't being deleted", got.Phase, PhaseReady)
	}
}

func TestPodSnapshotBeingDeleted(t *testing.T) {
	ready := [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	tests := map[string]struct {
		objects []client.Object
		funcs   interceptor.Funcs
		want    bool
		wantErr bool
	}{
		"being deleted": {
			objects: []client.Object{markDeleting(checkpointTestSnapshot("snap", false, ready))},
			want:    true,
		},
		"not being deleted": {
			objects: []client.Object{checkpointTestSnapshot("snap", false, ready)},
		},
		// Gone, or no such CRD: not this check's business. Callers keep
		// today's behavior for those.
		"gone": {},
		"no such CRD": {
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: DefaultAPIGroup, Kind: "PodSnapshot"}}
			}},
		},
		"read error": {
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return errors.New("apiserver unavailable")
			}},
			wantErr: true,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(tc.objects...).WithInterceptorFuncs(tc.funcs).Build()
			got, err := PodSnapshotBeingDeleted(context.Background(), c, DefaultAPIGroup, "default", "snap")
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("PodSnapshotBeingDeleted = %v, want %v", got, tc.want)
			}
		})
	}
}
