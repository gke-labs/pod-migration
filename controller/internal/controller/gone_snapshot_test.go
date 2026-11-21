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

// A PodSnapshot that's already gone is never restorable (#269). In
// Snapshotting, a snapshot that isn't found may not be created yet, so only
// one the job recorded seeing counts as gone, confirmed on the API server.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const goneSnapName = "snap-gone"

// newGoneSnapshotFixture is a fence fixture for job, with the real GKE
// provider reading the job's trigger and snapshot. objs adds the trigger and
// any snapshot.
func newGoneSnapshotFixture(t *testing.T, job *pmv1alpha1.PodMigrationJob, source bool, objs ...client.Object) *fenceFixture {
	t.Helper()
	all := append([]client.Object{job}, objs...)
	if source {
		all = append(all, fenceTestSource(fenceTestCID, 0))
	}
	f := newFenceFixture(t, interceptor.Funcs{}, all...)
	f.r.SnapshotProvider = snapshot.NewGKEProvider(f.client, fenceTestScheme())
	return f
}

// pastDeadlineFencedJob is a Snapshotting job past its migration deadline
// that fenced its source and recorded snapRef. observed records that it saw
// the snapshot.
func pastDeadlineFencedJob(snapRef string, observed bool) (*pmv1alpha1.PodMigrationJob, client.Object) {
	job := fenceTestSnapshottingJob()
	source := fencedPod(fenceTestSource(fenceTestCID, 0))
	started := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	job.Status.SnapshottingStartTime = &started
	job.Status.SnapshotRef = snapRef
	job.Status.Conditions = []metav1.Condition{{Type: ConditionSourceFenced, Status: metav1.ConditionTrue, Reason: util.ReasonFenced, LastTransitionTime: started}}
	if observed {
		job.Status.Conditions = append(job.Status.Conditions, metav1.Condition{Type: ConditionSnapshotObserved, Status: metav1.ConditionTrue, Reason: reasonSnapshotFound, LastTransitionTime: started})
	}
	data, _ := json.Marshal(baselineOf(source))
	job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}
	return job, source
}

// observedJob is a Snapshotting job that recorded seeing snapRef.
func observedJob(snapRef string) *pmv1alpha1.PodMigrationJob {
	job := fenceTestSnapshottingJob()
	job.Status.SnapshotRef = snapRef
	job.Status.Conditions = []metav1.Condition{{Type: ConditionSnapshotObserved, Status: metav1.ConditionTrue, Reason: reasonSnapshotFound, LastTransitionTime: metav1.Now()}}
	return job
}

func reconcileN(t *testing.T, f *fenceFixture, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
}

func deleteSnapshot(t *testing.T, c client.Client, group, name string) {
	t.Helper()
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	snap.SetNamespace(fenceTestNS)
	snap.SetName(name)
	if err := c.Delete(context.Background(), snap); err != nil {
		t.Fatalf("deleting the snapshot: %v", err)
	}
}

// TestSnapshotting_SnapshotNotCreatedYet_KeepsWaiting: the trigger can name
// its snapshot before the snapshot exists. A snapshot the job never saw
// isn't gone, even though the API server doesn't have it: the job keeps
// waiting, and moves on once the snapshot shows up Ready.
func TestSnapshotting_SnapshotNotCreatedYet_KeepsWaiting(t *testing.T) {
	job := fenceTestSnapshottingJob()
	f := newGoneSnapshotFixture(t, job, true, ownedTrigger(job, goneSnapName))
	reconcileN(t, f, 3)
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s (%+v), want Snapshotting: a snapshot not created yet isn't gone", got.Status.Phase, meta.FindStatusCondition(got.Status.Conditions, "Ready"))
	}
	if got.Status.SnapshotRef != goneSnapName {
		t.Errorf("SnapshotRef = %q, want %q", got.Status.SnapshotRef, goneSnapName)
	}
	if meta.FindStatusCondition(got.Status.Conditions, ConditionSnapshotObserved) != nil {
		t.Errorf("conditions = %+v, want no %s for a snapshot never read", got.Status.Conditions, ConditionSnapshotObserved)
	}
	assertFenced(t, f.pod(t))

	if err := f.client.Create(context.Background(), testSnapshot(snapshot.DefaultAPIGroup, goneSnapName, false, condCheckpointed, condReady)); err != nil {
		t.Fatalf("creating the snapshot: %v", err)
	}
	reconcileN(t, f, 1)
	if got := f.job(t); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Fatalf("phase = %s, want Evicting once the snapshot is Ready", got.Status.Phase)
	}
}

// TestSnapshotting_SnapshotNotCreatedYet_TimesOutAsBefore: at the deadline,
// a snapshot never seen times out as before, and the fence is decided as
// after any failure, not kept in the failing write.
func TestSnapshotting_SnapshotNotCreatedYet_TimesOutAsBefore(t *testing.T) {
	job, source := pastDeadlineFencedJob(goneSnapName, false)
	f := newGoneSnapshotFixture(t, job, false, source, ownedTrigger(job, goneSnapName))
	reconcileN(t, f, 1)
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Reason != "Timeout" {
		t.Errorf("Ready = %+v, want reason Timeout", ready)
	}
	if cond := fenceCondition(t, got); cond.Reason != util.ReasonFenced {
		t.Errorf("SourceFenced = %s/%s, want still %s, for the usual decision", cond.Status, cond.Reason, util.ReasonFenced)
	}
}

// TestSnapshotting_SnapshotGoneAfterSeen_FailsAndKeepsFence: a snapshot the
// job saw, and that's gone, can never become restorable. The job fails at
// once. Whether the checkpoint completed can't be read any more, so the
// same status write keeps the fence (C4), and the source is evicted cold,
// never unfenced.
func TestSnapshotting_SnapshotGoneAfterSeen_FailsAndKeepsFence(t *testing.T) {
	t.Run("within the deadline", func(t *testing.T) {
		job := fenceTestSnapshottingJob()
		f := newGoneSnapshotFixture(t, job, true, ownedTrigger(job, goneSnapName),
			testSnapshot(snapshot.DefaultAPIGroup, goneSnapName, false, condCheckpointing))
		reconcileN(t, f, 3)
		got := f.job(t)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting || !meta.IsStatusConditionTrue(got.Status.Conditions, ConditionSnapshotObserved) {
			t.Fatalf("phase = %s, conditions = %+v; want Snapshotting with %s recorded", got.Status.Phase, got.Status.Conditions, ConditionSnapshotObserved)
		}
		deleteSnapshot(t, f.client, snapshot.DefaultAPIGroup, goneSnapName)
		reconcileN(t, f, 1)
		assertGoneFailure(t, f)
	})
	t.Run("first seen gone at the migration deadline", func(t *testing.T) {
		job, source := pastDeadlineFencedJob(goneSnapName, true)
		f := newGoneSnapshotFixture(t, job, false, source, ownedTrigger(job, goneSnapName))
		reconcileN(t, f, 1)
		assertGoneFailure(t, f)
	})
}

func assertGoneFailure(t *testing.T, f *fenceFixture) {
	t.Helper()
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s, want Failed: a snapshot that's gone is never durable", got.Status.Phase)
	}
	if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Reason != snapshot.ReasonSnapshotGone {
		t.Errorf("Ready = %+v, want reason %s", ready, snapshot.ReasonSnapshotGone)
	}
	if cond := fenceCondition(t, got); cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenceKept {
		t.Fatalf("SourceFenced = %s/%s, want True/%s in the same write as Failed", cond.Status, cond.Reason, util.ReasonFenceKept)
	}
	for i := 0; i < 3 && f.pod(t) != nil; i++ {
		reconcileN(t, f, 1)
	}
	if f.pod(t) != nil {
		t.Error("source wasn't evicted")
	}
	events := f.events()
	if hasEvent(events, "SourceUnfenced") {
		t.Errorf("events = %v: the source was unfenced", events)
	}
	if !hasEvent(events, "SourceKeptFenced") {
		t.Errorf("events = %v, want SourceKeptFenced", events)
	}
}

// TestSnapshotting_GoneOnlyInTheCache_DoesNotFail: a cached miss can be a
// lagging informer. The job fails only once the API server confirms the
// snapshot is gone.
func TestSnapshotting_GoneOnlyInTheCache_DoesNotFail(t *testing.T) {
	job := observedJob(goneSnapName)
	f := newGoneSnapshotFixture(t, job, true, ownedTrigger(job, goneSnapName))
	f.r.APIReader = newRestoringClient(interceptor.Funcs{}, testSnapshot(snapshot.DefaultAPIGroup, goneSnapName, false, condCheckpointing))
	reconcileN(t, f, 3)
	if got := f.job(t); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want Snapshotting while the API server still has the snapshot", got.Status.Phase)
	}
}

// TestSnapshotting_TriggerNamesAnotherSnapshot_DropsTheObservation: the
// record that the job saw its snapshot is about the snapshot in
// status.snapshotRef. When the trigger names another one, not created yet,
// the record goes, and the job waits for the new one.
func TestSnapshotting_TriggerNamesAnotherSnapshot_DropsTheObservation(t *testing.T) {
	job := observedJob("snap-earlier")
	f := newGoneSnapshotFixture(t, job, true, ownedTrigger(job, goneSnapName))
	reconcileN(t, f, 3)
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want Snapshotting: the new snapshot was never seen", got.Status.Phase)
	}
	if got.Status.SnapshotRef != goneSnapName {
		t.Errorf("SnapshotRef = %q, want %q", got.Status.SnapshotRef, goneSnapName)
	}
	if meta.FindStatusCondition(got.Status.Conditions, ConditionSnapshotObserved) != nil {
		t.Errorf("conditions = %+v, want the earlier snapshot's %s dropped", got.Status.Conditions, ConditionSnapshotObserved)
	}
}

// TestPodGate_GoneSnapshot_HoldsThenReleasesCold: the pod gate never
// releases a replacement with a restore order naming a snapshot that's
// gone. A Restoring job got there on a Ready snapshot, so one the API
// server doesn't have is gone. The gate holds the replacement and records
// the hold; the job concludes without a restore; the gate releases cold.
func TestPodGate_GoneSnapshot_HoldsThenReleasesCold(t *testing.T) {
	c := newRestoringClient(interceptor.Funcs{}, deletingSnapRestoringJob(), heldReplacement(true))
	restoreKey := snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup)

	res, pod, err := reconcileGate(t, c, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertHeld(t, pod, restoreKey)
	if res.RequeueAfter == 0 {
		t.Error("RequeueAfter = 0, want the hold to look again")
	}
	got := getTestJob(t, c)
	if cond, held := snapshotUnrestorableHold(got); !held || cond.Reason != snapshot.ReasonSnapshotGone {
		t.Fatalf("conditions = %+v, want the hold recorded with reason %s", got.Status.Conditions, snapshot.ReasonSnapshotGone)
	}

	got = reconcileTestJob(t, c)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("phase = %s, want SucceededWithoutRestore", got.Status.Phase)
	}
	for _, condType := range []string{"Restored", "Ready"} {
		if cond := meta.FindStatusCondition(got.Status.Conditions, condType); cond == nil || cond.Reason != snapshot.ReasonSnapshotGone {
			t.Errorf("%s condition = %+v, want reason %s", condType, cond, snapshot.ReasonSnapshotGone)
		}
	}
	_, pod, err = reconcileGate(t, c, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if podHasMigrationGate(pod) {
		t.Fatal("the replacement is still gated after the job concluded")
	}
	if v, ok := pod.Annotations[restoreKey]; !ok || v != "" {
		t.Errorf("%s = %q (set %t), want the cold-start bypass", restoreKey, v, ok)
	}
	if got := getTestJob(t, c); !got.Status.Consumed || got.Status.RestoredPodUID != replacementUID {
		t.Errorf("Consumed = %t, RestoredPodUID = %q; want the cold start recorded on %s", got.Status.Consumed, got.Status.RestoredPodUID, replacementUID)
	}
}

// failingSnapshotReads is a reader whose PodSnapshot reads fail with err.
func failingSnapshotReads(err error, objs ...client.Object) client.Client {
	return newRestoringClient(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PodSnapshot" {
			return err
		}
		return cl.Get(ctx, k, obj, opts...)
	}}, objs...)
}

// TestSnapshotting_GoneAtTheDeadline: at the migration deadline, a snapshot
// the job saw is judged gone only on the API server's word. Past the 2h hard
// cap it's still checked, so the fence isn't decided without the completed
// checkpoint it can no longer see. A cache-only miss, or a failed live read,
// times out as before rather than holding the job past its deadline.
func TestSnapshotting_GoneAtTheDeadline(t *testing.T) {
	for name, tc := range map[string]struct {
		age        time.Duration
		apiReader  client.Client
		wantReason string
	}{
		"past the hard cap, gone": {age: 3 * time.Hour, wantReason: snapshot.ReasonSnapshotGone},
		"cache-only miss": {age: 15 * time.Minute, wantReason: "Timeout",
			apiReader: newRestoringClient(interceptor.Funcs{}, testSnapshot(snapshot.DefaultAPIGroup, goneSnapName, false, condCheckpointing))},
		"live read fails": {age: 15 * time.Minute, wantReason: "Timeout",
			apiReader: failingSnapshotReads(errors.New("apiserver unavailable"))},
	} {
		t.Run(name, func(t *testing.T) {
			job, source := pastDeadlineFencedJob(goneSnapName, true)
			started := metav1.NewTime(time.Now().Add(-tc.age))
			job.Status.SnapshottingStartTime = &started
			f := newGoneSnapshotFixture(t, job, false, source, ownedTrigger(job, goneSnapName))
			if tc.apiReader != nil {
				f.r.APIReader = tc.apiReader
			}
			reconcileN(t, f, 1)
			got := f.job(t)
			ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || ready == nil || ready.Reason != tc.wantReason {
				t.Fatalf("phase = %s, Ready = %+v; want Failed with reason %s", got.Status.Phase, ready, tc.wantReason)
			}
			if tc.wantReason == snapshot.ReasonSnapshotGone {
				if cond := fenceCondition(t, got); cond.Reason != util.ReasonFenceKept {
					t.Errorf("SourceFenced = %s/%s, want %s", cond.Status, cond.Reason, util.ReasonFenceKept)
				}
			}
		})
	}
}

// TestSnapshotting_GoneCheckReadError_KeepsWaiting: within the deadline, a
// failed live read leaves the job Snapshotting and returns the error.
func TestSnapshotting_GoneCheckReadError_KeepsWaiting(t *testing.T) {
	job := observedJob(goneSnapName)
	f := newGoneSnapshotFixture(t, job, true, ownedTrigger(job, goneSnapName))
	f.r.APIReader = failingSnapshotReads(errors.New("apiserver unavailable"))
	var lastErr error
	for i := 0; i < 3; i++ {
		_, lastErr = f.reconcile(t)
	}
	if lastErr == nil {
		t.Error("Reconcile returned no error for a failed live read")
	}
	if got := f.job(t); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want Snapshotting", got.Status.Phase)
	}
}

// TestPodGate_GoneSnapshot_Groups: the gate reads the snapshot in the group
// of the job's engine. A CRIU job whose snapshot exists only in the GKE Pod
// Snapshots group is held as gone. A group whose CRD isn't installed keeps
// the old behavior: the replacement is released with its restore order.
func TestPodGate_GoneSnapshot_Groups(t *testing.T) {
	criuJob := deletingSnapRestoringJob()
	criuJob.Annotations = map[string]string{engine.AnnotationEngine: engine.CRIUName}
	t.Run("CRIU job, snapshot only in the other group", func(t *testing.T) {
		c := newRestoringClient(interceptor.Funcs{}, criuJob.DeepCopy(), heldReplacement(true),
			testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, false, condCheckpointed, condReady))
		reg, criu := gkeAndCRIURegistry(t, c)
		_, pod, err := reconcileGate(t, c, reg)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		assertHeld(t, pod, criu.RestoreAnnotation())
		if cond, held := snapshotUnrestorableHold(getTestJob(t, c)); !held || cond.Reason != snapshot.ReasonSnapshotGone {
			t.Errorf("hold = %+v, want reason %s", cond, snapshot.ReasonSnapshotGone)
		}
	})
	t.Run("no such CRD", func(t *testing.T) {
		c := failingSnapshotReads(&meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: snapshot.DefaultAPIGroup, Kind: "PodSnapshot"}},
			deletingSnapRestoringJob(), heldReplacement(true))
		_, pod, err := reconcileGate(t, c, nil)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if podHasMigrationGate(pod) || pod.Annotations[snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup)] != deletingSnapName {
			t.Errorf("gated %t, annotations %v; want released with the restore order", podHasMigrationGate(pod), pod.Annotations)
		}
	})
}

func gkeAndCRIURegistry(t *testing.T, c client.Client) (*engine.Registry, engine.Engine) {
	t.Helper()
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(c, fenceTestScheme())); err != nil {
		t.Fatal(err)
	}
	criu, err := engine.NewCRIU(c, fenceTestScheme(), engine.CRIUAPIGroup)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(criu); err != nil {
		t.Fatal(err)
	}
	return reg, criu
}
