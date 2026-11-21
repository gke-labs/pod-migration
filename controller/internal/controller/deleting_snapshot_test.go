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

// A PodSnapshot that's being deleted is never restorable (#265).

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	deletingSnapName  = "snap-deleting"
	replacementName   = "web-0-replacement"
	replacementUID    = "replacement-uid"
	testSnapFinalizer = "criu.podmigration.gke.io/archive-cleanup"
)

var (
	condCheckpointed  = [3]string{"Checkpoint", "True", "Succeeded"}
	condReady         = [3]string{"Ready", "True", "AllSnapshotsAvailable"}
	condCheckpointing = [3]string{"Checkpoint", "False", "InProgress"}
)

// testSnapshot returns a PodSnapshot in group with the given conditions
// (type, status, reason). deleting gives it a deletionTimestamp held by a
// finalizer, as the CRIU engine controller's archive cleanup (#260) or
// GKE's own would.
func testSnapshot(group, name string, deleting bool, conditions ...[3]string) *unstructured.Unstructured {
	conds := make([]interface{}, 0, len(conditions))
	for _, c := range conditions {
		conds = append(conds, map[string]interface{}{
			"type": c[0], "status": c[1], "reason": c[2],
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		})
	}
	snap := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"conditions": conds},
	}}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	snap.SetNamespace(fenceTestNS)
	snap.SetName(name)
	snap.SetLabels(map[string]string{snapshot.TriggeredByLabel: util.FormatPSMTName(fenceTestPod, fenceTestUID)})
	if deleting {
		snap.SetFinalizers([]string{testSnapFinalizer})
		now := metav1.Now()
		snap.SetDeletionTimestamp(&now)
	}
	return snap
}

// ownedTrigger returns the job's trigger, owned by the job and naming
// snapName as the snapshot it created.
func ownedTrigger(job *pmv1alpha1.PodMigrationJob, snapName string) *unstructured.Unstructured {
	trigger := &unstructured.Unstructured{Object: map[string]interface{}{
		"spec":   map[string]interface{}{"targetPod": fenceTestPod},
		"status": map[string]interface{}{"snapshotCreated": map[string]interface{}{"name": snapName}},
	}}
	trigger.SetGroupVersionKind(snapshot.ManualTriggerGVK(snapshot.DefaultAPIGroup))
	trigger.SetNamespace(fenceTestNS)
	trigger.SetName(util.FormatPSMTName(fenceTestPod, fenceTestUID))
	isController := true
	trigger.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: pmv1alpha1.GroupVersion.String(), Kind: "PodMigrationJob", Name: job.Name, UID: job.UID, Controller: &isController,
	}})
	return trigger
}

// finishSnapshotDeletion removes the finalizer, so the snapshot goes away,
// as it does once the engine has deleted the archive.
func finishSnapshotDeletion(t *testing.T, c client.Client) {
	t.Helper()
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup))
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: deletingSnapName}, snap); err != nil {
		t.Fatalf("getting the snapshot: %v", err)
	}
	snap.SetFinalizers(nil)
	if err := c.Update(context.Background(), snap); err != nil {
		t.Fatalf("removing the snapshot's finalizer: %v", err)
	}
}

// newDeletingSnapshotFixture is a fence fixture for a Snapshotting job, with
// the real GKE provider reading the job's trigger and snapshot. pastDeadline
// gives a job that already fenced its source and is past its migration
// deadline, so the timeout check is the first to see the snapshot.
func newDeletingSnapshotFixture(t *testing.T, snap *unstructured.Unstructured, pastDeadline bool) *fenceFixture {
	t.Helper()
	job := fenceTestSnapshottingJob()
	source := fenceTestSource(fenceTestCID, 0)
	if pastDeadline {
		started := metav1.NewTime(time.Now().Add(-15 * time.Minute))
		job.Status.SnapshottingStartTime = &started
		// An earlier pass recorded the snapshot, before its deletion started.
		job.Status.SnapshotRef = deletingSnapName
		job.Status.Conditions = []metav1.Condition{{Type: ConditionSourceFenced, Status: metav1.ConditionTrue, Reason: util.ReasonFenced, LastTransitionTime: started}}
		data, _ := json.Marshal(baselineOf(source))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}
		source = fencedPod(source)
	}
	f := newFenceFixture(t, interceptor.Funcs{}, source, job, ownedTrigger(job, deletingSnapName), snap)
	f.r.SnapshotProvider = snapshot.NewGKEProvider(f.client, fenceTestScheme())
	return f
}

// TestSnapshotting_DeletingSnapshot_CompletedCheckpoint_KeepsFence: a job
// whose snapshot completed its checkpoint, and is being deleted, fails at
// once instead of moving to Evicting on it, or waiting out its deadline with
// the source out of service. The engine may stop the source after a
// completed checkpoint, so the same status write keeps the fence (C4). That
// record outlives the snapshot: once the snapshot is gone, CheckpointState
// can't tell that the checkpoint completed, and the source must still not be
// unfenced. It's evicted cold.
func TestSnapshotting_DeletingSnapshot_CompletedCheckpoint_KeepsFence(t *testing.T) {
	for name, pastDeadline := range map[string]bool{
		"within the deadline":                  false,
		"first seen at the migration deadline": true,
	} {
		t.Run(name, func(t *testing.T) {
			f := newDeletingSnapshotFixture(t, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointed, condReady), pastDeadline)
			for i := 0; i < 2 && f.job(t).Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting; i++ {
				if _, err := f.reconcile(t); err != nil {
					t.Fatalf("Reconcile: %v", err)
				}
			}
			got := f.job(t)
			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
				t.Fatalf("phase = %s, want Failed: a snapshot being deleted is never durable", got.Status.Phase)
			}
			if ready := meta.FindStatusCondition(got.Status.Conditions, "Ready"); ready == nil || ready.Reason != snapshot.ReasonSnapshotDeleting {
				t.Errorf("Ready condition = %+v, want reason %s", ready, snapshot.ReasonSnapshotDeleting)
			}
			if got.Status.SnapshotRef != deletingSnapName {
				t.Errorf("SnapshotRef = %q, want %q", got.Status.SnapshotRef, deletingSnapName)
			}
			if cond := fenceCondition(t, got); cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenceKept {
				t.Fatalf("SourceFenced = %s/%s, want True/%s in the same write as Failed", cond.Status, cond.Reason, util.ReasonFenceKept)
			}

			// The archive cleanup finishes, and the snapshot is gone.
			finishSnapshotDeletion(t, f.client)
			for i := 0; i < 3 && f.pod(t) != nil; i++ {
				if _, err := f.reconcile(t); err != nil {
					t.Fatalf("Reconcile after the snapshot is gone: %v", err)
				}
			}
			if cond := fenceCondition(t, f.job(t)); cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenceKept {
				t.Errorf("SourceFenced = %s/%s (%s), want True/%s", cond.Status, cond.Reason, cond.Message, util.ReasonFenceKept)
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
		})
	}
}

// TestSnapshotting_DeletingSnapshot_UnfinishedCheckpoint_FenceDecidedAsAfterAFailure:
// a snapshot being deleted whose checkpoint hasn't completed fails the job
// too, but the fence isn't kept in that write: it's decided as after any
// failure, which waits for the checkpoint to settle and checks the baseline.
func TestSnapshotting_DeletingSnapshot_UnfinishedCheckpoint_FenceDecidedAsAfterAFailure(t *testing.T) {
	f := newDeletingSnapshotFixture(t, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointing), false)
	for i := 0; i < 2 && f.job(t).Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if cond := fenceCondition(t, got); cond.Reason == util.ReasonFenceKept {
		t.Errorf("SourceFenced = %s/%s, want the fence left to the usual decision", cond.Status, cond.Reason)
	}
	// While the snapshot is still there, being deleted, the checkpoint
	// hasn't settled, so the decision waits with the source fenced.
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertFenced(t, f.pod(t))
	if cond := fenceCondition(t, f.job(t)); cond.Reason != util.ReasonFenced {
		t.Errorf("SourceFenced = %s/%s, want still %s while the checkpoint settles", cond.Status, cond.Reason, util.ReasonFenced)
	}
}

// TestSnapshotting_ReadySnapshotNotDeleting_MovesToEvicting pins the other
// side: the same snapshot, not being deleted, still moves the job on.
func TestSnapshotting_ReadySnapshotNotDeleting_MovesToEvicting(t *testing.T) {
	f := newDeletingSnapshotFixture(t, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, false, condCheckpointed, condReady), false)
	for i := 0; i < 2 && f.job(t).Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	if got := f.job(t); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Fatalf("phase = %s, want Evicting", got.Status.Phase)
	}
}

func deletingSnapRestoringJob() *pmv1alpha1.PodMigrationJob {
	start := metav1.NewTime(time.Now().Add(-10 * time.Second))
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: fenceTestJob, Namespace: fenceTestNS, UID: "job-uid"},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: fenceTestPod},
			TargetPodUID: fenceTestUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:        deletingSnapName,
			RestoringStartTime: &start,
		},
	}
}

// withSnapshotDeletingHold records the pod gate's hold on job.
func withSnapshotDeletingHold(job *pmv1alpha1.PodMigrationJob) *pmv1alpha1.PodMigrationJob {
	job.Status.Conditions = append(job.Status.Conditions, metav1.Condition{
		Type: ConditionSnapshotRestorable, Status: metav1.ConditionFalse, Reason: snapshot.ReasonSnapshotDeleting,
		Message: snapshotDeletingMessage(deletingSnapName), LastTransitionTime: metav1.Now(),
	})
	return job
}

// consumedBy records the replacement as the job's consumer.
func consumedBy(job *pmv1alpha1.PodMigrationJob, gateReleased bool) *pmv1alpha1.PodMigrationJob {
	job.Status.Consumed = true
	job.Status.RestoredPodName = replacementName
	job.Status.RestoredPodUID = replacementUID
	job.Status.GateReleased = gateReleased
	return job
}

func heldReplacement(gated bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   fenceTestNS,
			Name:        replacementName,
			UID:         replacementUID,
			Annotations: map[string]string{util.AnnotationAssignedPMJ: fenceTestJob},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}},
	}
	if gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: MigrationGateName}}
	}
	return pod
}

func newRestoringClient(funcs interceptor.Funcs, objs ...client.Object) client.WithWatch {
	return newFakeClientBuilderWithEventIndex(fenceTestScheme()).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(objs...).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(funcs).
		Build()
}

func getTestJob(t *testing.T, c client.Client) *pmv1alpha1.PodMigrationJob {
	t.Helper()
	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, got); err != nil {
		t.Fatalf("getting the job: %v", err)
	}
	return got
}

func reconcileTestJob(t *testing.T, c client.Client) *pmv1alpha1.PodMigrationJob {
	t.Helper()
	r := &PodMigrationJobReconciler{Client: c, Scheme: fenceTestScheme(), Recorder: record.NewFakeRecorder(20), RestoreStabilizationWindow: DefaultRestoreStabilizationWindow}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return getTestJob(t, c)
}

// TestRestoring_SnapshotDeletingHold: a Restoring job whose replacement the
// pod gate holds because the snapshot is being deleted concludes
// SucceededWithoutRestore, as long as no replacement was actually released,
// so the gate releases it cold instead of the restore ceiling waiting on it
// forever.
func TestRestoring_SnapshotDeletingHold(t *testing.T) {
	tests := map[string]struct {
		job          *pmv1alpha1.PodMigrationJob
		pod          *corev1.Pod
		wantConclude bool
	}{
		"held, nothing consumed yet": {
			job: withSnapshotDeletingHold(deletingSnapRestoringJob()), pod: heldReplacement(true), wantConclude: true,
		},
		"held, recorded as released but the gate removal failed and the pod is still gated": {
			job: withSnapshotDeletingHold(consumedBy(deletingSnapRestoringJob(), true)), pod: heldReplacement(true), wantConclude: true,
		},
		"held, the recorded consumer is gone and the gate was never released": {
			job: withSnapshotDeletingHold(consumedBy(deletingSnapRestoringJob(), false)), pod: nil, wantConclude: true,
		},
		"the replacement was released before GateReleased existed": {
			job: withSnapshotDeletingHold(consumedBy(deletingSnapRestoringJob(), false)), pod: heldReplacement(false),
		},
		"released and recorded": {
			job: withSnapshotDeletingHold(consumedBy(deletingSnapRestoringJob(), true)), pod: heldReplacement(false),
		},
		"no hold recorded": {
			job: deletingSnapRestoringJob(), pod: heldReplacement(true),
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			objs := []client.Object{tc.job}
			if tc.pod != nil {
				objs = append(objs, tc.pod)
			}
			got := reconcileTestJob(t, newRestoringClient(interceptor.Funcs{}, objs...))
			if !tc.wantConclude {
				if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
					t.Errorf("phase = %s, want Restoring", got.Status.Phase)
				}
				return
			}
			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				t.Fatalf("phase = %s, want SucceededWithoutRestore", got.Status.Phase)
			}
			for _, condType := range []string{"Restored", "Ready"} {
				if cond := meta.FindStatusCondition(got.Status.Conditions, condType); cond == nil || cond.Reason != snapshot.ReasonSnapshotDeleting {
					t.Errorf("%s condition = %+v, want reason %s", condType, cond, snapshot.ReasonSnapshotDeleting)
				}
			}
		})
	}
}

// TestRestoring_SnapshotDeletingHold_StaleViewDoesNotConclude: the
// conclusion checks the resourceVersion, so a reconcile that read the job
// before it changed can't conclude it.
func TestRestoring_SnapshotDeletingHold_StaleViewDoesNotConclude(t *testing.T) {
	key := types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}
	var stale *pmv1alpha1.PodMigrationJob
	c := newRestoringClient(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && stale != nil && k == key {
			stale.DeepCopyInto(job)
			return nil
		}
		return cl.Get(ctx, k, obj, opts...)
	}}, withSnapshotDeletingHold(deletingSnapRestoringJob()), heldReplacement(true))
	ctx := context.Background()
	held := getTestJob(t, c)
	changed := held.DeepCopy()
	changed.Status.Conditions = append(changed.Status.Conditions, metav1.Condition{Type: "Other", Status: metav1.ConditionTrue, Reason: "Changed", LastTransitionTime: metav1.Now()})
	if err := c.Status().Update(ctx, changed); err != nil {
		t.Fatalf("changing the job: %v", err)
	}
	stale = held
	r := &PodMigrationJobReconciler{Client: c, Scheme: fenceTestScheme(), Recorder: record.NewFakeRecorder(20)}
	_, _ = r.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	stale = nil
	if got := getTestJob(t, c); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || meta.FindStatusCondition(got.Status.Conditions, "Other") == nil {
		t.Errorf("job status = %+v, want it still Restoring with the change kept", got.Status)
	}
}

func reconcileGate(t *testing.T, c client.Client, engines *engine.Registry) (reconcile.Result, *corev1.Pod, error) {
	t.Helper()
	r := &PodGateReconciler{Client: c, Scheme: fenceTestScheme(), Engines: engines}
	key := types.NamespacedName{Namespace: fenceTestNS, Name: replacementName}
	res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key})
	pod := &corev1.Pod{}
	if getErr := c.Get(context.Background(), key, pod); getErr != nil {
		t.Fatalf("getting the pod: %v", getErr)
	}
	return res, pod, err
}

func assertHeld(t *testing.T, pod *corev1.Pod, restoreKey string) {
	t.Helper()
	if !podHasMigrationGate(pod) {
		t.Fatal("the replacement was released while its snapshot is being deleted")
	}
	if v := pod.Annotations[restoreKey]; v != "" {
		t.Errorf("%s = %q, want no restore order naming a deleting snapshot", restoreKey, v)
	}
}

// TestPodGate_DeletingSnapshot_HoldsThenReleasesCold: the pod gate never
// releases a replacement with a restore order naming a snapshot that's being
// deleted. It holds it and records the hold; the job reconciler concludes
// the job without a restore; the gate then releases the replacement cold.
func TestPodGate_DeletingSnapshot_HoldsThenReleasesCold(t *testing.T) {
	for name, job := range map[string]*pmv1alpha1.PodMigrationJob{
		"nothing consumed yet": deletingSnapRestoringJob(),
		// GateReleased is written before the gate is removed; the removal
		// failed, and the snapshot started being deleted before the retry.
		"recorded as released, still gated": consumedBy(deletingSnapRestoringJob(), true),
	} {
		t.Run(name, func(t *testing.T) {
			c := newRestoringClient(interceptor.Funcs{}, job, heldReplacement(true),
				testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointed, condReady))
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
			if _, held := snapshotUnrestorableHold(got); !held {
				t.Fatalf("conditions = %+v, want the hold recorded", got.Status.Conditions)
			}
			if got.Status.Consumed != job.Status.Consumed || got.Status.GateReleased != job.Status.GateReleased {
				t.Errorf("Consumed = %t, GateReleased = %t; want them unchanged by the hold", got.Status.Consumed, got.Status.GateReleased)
			}

			// The job reconciler concludes; the gate then releases cold.
			if got := reconcileTestJob(t, c); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				t.Fatalf("phase = %s, want SucceededWithoutRestore", got.Status.Phase)
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
		})
	}
}

// TestPodGate_DeletingSnapshot_UsesTheJobsEngineGroup: the gate reads the
// snapshot in the group of the job's engine. A CRIU job is held for a
// deleting CRIU snapshot, and isn't held for a deleting snapshot of the
// same name in the GKE Pod Snapshots group.
func TestPodGate_DeletingSnapshot_UsesTheJobsEngineGroup(t *testing.T) {
	criuJob := func() *pmv1alpha1.PodMigrationJob {
		job := deletingSnapRestoringJob()
		job.Annotations = map[string]string{engine.AnnotationEngine: engine.CRIUName}
		return job
	}
	for name, tc := range map[string]struct {
		snapGroup string
		wantHeld  bool
	}{
		"deleting CRIU snapshot":               {snapGroup: engine.CRIUAPIGroup, wantHeld: true},
		"deleting snapshot in the other group": {snapGroup: snapshot.DefaultAPIGroup},
	} {
		t.Run(name, func(t *testing.T) {
			objs := []client.Object{criuJob(), heldReplacement(true), testSnapshot(tc.snapGroup, deletingSnapName, true, condCheckpointed, condReady)}
			if !tc.wantHeld {
				// The job's own CRIU snapshot exists and isn't being deleted.
				objs = append(objs, testSnapshot(engine.CRIUAPIGroup, deletingSnapName, false, condCheckpointed, condReady))
			}
			c := newRestoringClient(interceptor.Funcs{}, objs...)
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
			_, pod, err := reconcileGate(t, c, reg)
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if tc.wantHeld {
				assertHeld(t, pod, criu.RestoreAnnotation())
				return
			}
			if podHasMigrationGate(pod) {
				t.Fatal("the replacement is still gated; its own snapshot isn't being deleted")
			}
			if got := pod.Annotations[criu.RestoreAnnotation()]; got != deletingSnapName {
				t.Errorf("restore order = %q, want %q", got, deletingSnapName)
			}
		})
	}
}

// TestPodGate_SnapshotReadError_StaysGated: a failed snapshot read keeps the
// replacement gated, with no restore order, and returns the error.
func TestPodGate_SnapshotReadError_StaysGated(t *testing.T) {
	c := newRestoringClient(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PodSnapshot" {
			return errors.New("apiserver unavailable")
		}
		return cl.Get(ctx, k, obj, opts...)
	}}, deletingSnapRestoringJob(), heldReplacement(true))
	_, pod, err := reconcileGate(t, c, nil)
	if err == nil {
		t.Error("Reconcile returned no error for a failed snapshot read")
	}
	assertHeld(t, pod, snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup))
}

// TestPodGate_SnapshotNotDeleting_ReleasesWithRestoreOrder pins the other
// side: the same job with a snapshot that isn't being deleted is released
// with its restore order.
func TestPodGate_SnapshotNotDeleting_ReleasesWithRestoreOrder(t *testing.T) {
	c := newRestoringClient(interceptor.Funcs{}, deletingSnapRestoringJob(), heldReplacement(true),
		testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, false, condCheckpointed, condReady))
	_, pod, err := reconcileGate(t, c, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if podHasMigrationGate(pod) {
		t.Fatal("the replacement is still gated")
	}
	if got := pod.Annotations[snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup)]; got != deletingSnapName {
		t.Errorf("restore order = %q, want %q", got, deletingSnapName)
	}
}

// TestPodGate_RecordedHoldSticksAfterTheSnapshotIsGone: once the gate has
// recorded its hold, the snapshot finishing its deletion (gone, so no
// longer "being deleted") doesn't let the gate release the replacement with
// a restore order before the job concludes.
func TestPodGate_RecordedHoldSticksAfterTheSnapshotIsGone(t *testing.T) {
	c := newRestoringClient(interceptor.Funcs{}, deletingSnapRestoringJob(), heldReplacement(true),
		testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointed, condReady))
	restoreKey := snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup)
	if _, pod, err := reconcileGate(t, c, nil); err != nil {
		t.Fatalf("Reconcile: %v", err)
	} else {
		assertHeld(t, pod, restoreKey)
	}
	finishSnapshotDeletion(t, c)
	res, pod, err := reconcileGate(t, c, nil)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertHeld(t, pod, restoreKey)
	if res.RequeueAfter == 0 {
		t.Error("RequeueAfter = 0, want the hold to look again")
	}
	if got := getTestJob(t, c); got.Status.Consumed || got.Status.GateReleased {
		t.Errorf("Consumed = %t, GateReleased = %t; want neither while held", got.Status.Consumed, got.Status.GateReleased)
	}
}

// TestPodGate_HoldRecordedOnAStaleViewConflicts: the gate records its hold
// with a resourceVersion check, so a stale view of the job can't drop a
// condition written since.
func TestPodGate_HoldRecordedOnAStaleViewConflicts(t *testing.T) {
	key := types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}
	var stale *pmv1alpha1.PodMigrationJob
	c := newRestoringClient(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && stale != nil && k == key {
			stale.DeepCopyInto(job)
			return nil
		}
		return cl.Get(ctx, k, obj, opts...)
	}}, deletingSnapRestoringJob(), heldReplacement(true),
		testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointed, condReady))
	before := getTestJob(t, c)
	changed := before.DeepCopy()
	changed.Status.Conditions = append(changed.Status.Conditions, metav1.Condition{Type: "Other", Status: metav1.ConditionTrue, Reason: "Changed", LastTransitionTime: metav1.Now()})
	if err := c.Status().Update(context.Background(), changed); err != nil {
		t.Fatalf("changing the job: %v", err)
	}
	stale = before
	_, pod, err := reconcileGate(t, c, nil)
	stale = nil
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	assertHeld(t, pod, snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup))
	if got := getTestJob(t, c); meta.FindStatusCondition(got.Status.Conditions, "Other") == nil {
		t.Errorf("conditions = %+v: the stale view dropped a condition written since", got.Status.Conditions)
	}
}

// TestSnapshotting_DeletingOnlyInTheCache_DoesNotFail: the cache can still
// show an earlier trigger's work order being deleted after the CRIU engine
// replaced it under the same name (#262). The job fails only once the API
// server confirms the deletion.
func TestSnapshotting_DeletingOnlyInTheCache_DoesNotFail(t *testing.T) {
	f := newDeletingSnapshotFixture(t, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointed, condReady), false)
	live := newRestoringClient(interceptor.Funcs{}, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, false, condCheckpointing))
	f.r.APIReader = live
	for i := 0; i < 2; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want Snapshotting until the API server confirms the deletion", got.Status.Phase)
	}
}

// checkpointStateErrProvider is the GKE provider with a CheckpointState
// that fails.
type checkpointStateErrProvider struct{ *snapshot.GKEProvider }

func (p checkpointStateErrProvider) CheckpointState(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (snapshot.CheckpointState, error) {
	return snapshot.CheckpointState{}, errors.New("apiserver unavailable")
}

// TestSnapshotting_DeletingSnapshot_CheckpointStateErrorKeepsFence: if the
// checkpoint's state can't be read, the job still fails, and the source
// stays fenced, as the fence decision after a failure does (C4).
func TestSnapshotting_DeletingSnapshot_CheckpointStateErrorKeepsFence(t *testing.T) {
	f := newDeletingSnapshotFixture(t, testSnapshot(snapshot.DefaultAPIGroup, deletingSnapName, true, condCheckpointing), false)
	f.r.SnapshotProvider = checkpointStateErrProvider{snapshot.NewGKEProvider(f.client, fenceTestScheme())}
	for i := 0; i < 2 && f.job(t).Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting; i++ {
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
	}
	got := f.job(t)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s, want Failed", got.Status.Phase)
	}
	if cond := fenceCondition(t, got); cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenceKept {
		t.Errorf("SourceFenced = %s/%s, want True/%s", cond.Status, cond.Reason, util.ReasonFenceKept)
	}
}
