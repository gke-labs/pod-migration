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

package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// TestSnapshotting_ReadySnapshotPastDeadline_StillMovesToEvicting (#231): a
// Ready snapshot now reports when it became Ready as its LastProgressTime.
// The reconciler still counts any Ready snapshot as progressing, however long
// ago it became Ready: a job past its migration deadline whose snapshot became
// Ready longer ago than the timeout moves on to Evicting, as before, instead
// of timing out. With strict invariants on, nothing fails it either: the
// reconcile that sees the snapshot Ready moves the job before the checks run.
func TestSnapshotting_ReadySnapshotPastDeadline_StillMovesToEvicting(t *testing.T) {
	readySince := time.Now().Add(-14 * time.Minute).UTC().Truncate(time.Second)
	// The fixture names the job's snapshot deletingSnapName; this one isn't being deleted.
	snap := testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, false, condCheckpointed, condReady)
	conds, _, _ := unstructured.NestedSlice(snap.Object, "status", "conditions")
	for _, c := range conds {
		c.(map[string]interface{})["lastTransitionTime"] = readySince.Format(time.RFC3339)
	}
	if err := unstructured.SetNestedSlice(snap.Object, conds, "status", "conditions"); err != nil {
		t.Fatalf("setting conditions: %v", err)
	}
	// The job started Snapshotting 15 minutes ago, past the 10-minute
	// default timeout, so the timeout check reads the snapshot first.
	f := newDeletingSnapshotFixture(t, snap, true)
	f.r.InvariantEngine = invariants.NewEngine(invariants.ModeStrict, f.recorder)

	st, progressing := f.r.checkSnapshotProgressing(context.Background(), f.job(t), 10*time.Minute)
	if st == nil || st.Phase != snapshot.PhaseReady || !st.LastProgressTime.Equal(readySince) {
		t.Fatalf("snapshot status = %+v, want Ready since %v", st, readySince)
	}
	if !progressing {
		t.Fatalf("a Ready snapshot that became Ready %v ago isn't counted as progressing", time.Since(readySince).Round(time.Minute))
	}

	for i := 0; i < 2 && f.job(t).Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	if got := f.job(t); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Fatalf("phase = %s, want Evicting", got.Status.Phase)
	}
	if events := f.events(); hasEvent(events, invariants.EventReasonInvariantViolation) {
		t.Errorf("events = %v, want no invariant violation", events)
	}
}
