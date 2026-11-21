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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	relNS        = "default"
	relPod       = "web-0"
	relSourceUID = "a1b2c3d4-0000-4000-8000-000000000001"
	relSnap      = "snap-web-0"
)

var relJobName = util.FormatPMJName(relPod, relSourceUID)

func relScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

// relJob returns a job concluded in phase, completedAgo ago, with relSnap as
// its snapshot.
func relJob(phase pmv1alpha1.PodMigrationJobPhase, completedAgo time.Duration) *pmv1alpha1.PodMigrationJob {
	done := metav1.NewTime(time.Now().Add(-completedAgo))
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: relJobName, CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour))},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: relPod}, TargetPodUID: relSourceUID},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          phase,
			SnapshotRef:    relSnap,
			CompletionTime: &done,
		},
	}
}

// relSnapshot returns a Ready PodSnapshot in group labelled as created by
// trigger (no label when trigger is empty).
func relSnapshot(group, name, trigger string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	u.SetNamespace(relNS)
	u.SetName(name)
	if trigger != "" {
		u.SetLabels(map[string]string{snapshot.TriggeredByLabel: trigger})
	}
	u.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded",
				"lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
		},
	}
	return u
}

func ownTrigger() string { return util.FormatPSMTName(relPod, relSourceUID) }

func relClient(objs ...client.Object) client.Client {
	return newFakeClientBuilderWithEventIndex(relScheme()).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(objs...).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()
}

func relReconcile(t *testing.T, r *PodMigrationJobReconciler) ctrl.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: relNS, Name: relJobName}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func snapExists(t *testing.T, c client.Client, group, name string) bool {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	err := c.Get(context.Background(), types.NamespacedName{Namespace: relNS, Name: name}, u)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("getting snapshot %s: %v", name, err)
	}
	return true
}

func releasedCond(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: relNS, Name: relJobName}, job); err != nil {
		t.Fatalf("getting job: %v", err)
	}
	return meta.FindStatusCondition(job.Status.Conditions, ConditionSnapshotReleased)
}

// A job that concluded with a verified restore deletes its own snapshot, and
// records that it did.
func TestSnapshotRelease_SucceededDeletesOwnSnapshot(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot of a Succeeded job still exists, want it deleted")
	}
	cond := releasedCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SnapshotReleased condition = %+v, want True", cond)
	}
	// Idempotent: a second pass with the snapshot gone is a no-op.
	relReconcile(t, r)
}

// A terminally failed job deletes its snapshot too.
func TestSnapshotRelease_FailedDeletesOwnSnapshot(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot of a Failed job still exists, want it deleted")
	}
}

// No snapshot is deleted before the job concludes: not while Restoring, and
// not when it concluded without a restore (the snapshot stays claimable and
// for recovery, #32).
func TestSnapshotRelease_NotBeforeSucceededOrFailed(t *testing.T) {
	for _, phase := range []pmv1alpha1.PodMigrationJobPhase{
		pmv1alpha1.PodMigrationJobPhaseEvicting,
		pmv1alpha1.PodMigrationJobPhaseRestoring,
		pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
	} {
		t.Run(string(phase), func(t *testing.T) {
			job := relJob(phase, time.Second)
			if phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				// CompletionTime stays set, so only the phase keeps the
				// snapshot.
				now := metav1.Now()
				job.Status.RestoringStartTime = &now
				job.Status.EvictingStartTime = &now
				job.Status.SnapshottingStartTime = &now
			}
			c := relClient(job, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
			r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme(), SnapshotProvider: &fakeSnapshotProvider{}}
			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: relNS, Name: relJobName}})
			if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatalf("snapshot deleted while the job is %s", phase)
			}
		})
	}
}

// The retention delay holds the snapshot, with a requeue for when it's due.
func TestSnapshotRelease_RetentionDelaysDeletion(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Minute), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme(), SnapshotRetention: 10 * time.Minute}
	res := relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted inside its retention period")
	}
	if res.RequeueAfter <= 0 || res.RequeueAfter > 9*time.Minute+time.Second {
		t.Fatalf("RequeueAfter = %v, want about 9m (when the retention ends)", res.RequeueAfter)
	}

	// Once the retention has passed, it's deleted.
	c2 := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, 11*time.Minute), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r2 := &PodMigrationJobReconciler{Client: c2, Scheme: relScheme(), SnapshotRetention: 10 * time.Minute}
	relReconcile(t, r2)
	if snapExists(t, c2, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot still exists after its retention period")
	}
}

// A PodSnapshot that another trigger made, for example a Kubeflow
// pause/resume on the shared policy (#60), is never deleted, even if the job
// names it.
func TestSnapshotRelease_NeverDeletesAnotherTriggersSnapshot(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, "ws-notebook-trigger"))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("deleted a PodSnapshot labelled as created by another trigger")
	}
	cond := releasedCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonSnapshotNotOwned {
		t.Fatalf("SnapshotReleased condition = %+v, want False/%s", cond, ReasonSnapshotNotOwned)
	}
}

// Ownership is checked before the deletion timestamp, so another trigger's
// snapshot that someone else is already deleting still reads as not ours.
func TestSnapshotRelease_AnotherTriggersDeletingSnapshotIsNotOwned(t *testing.T) {
	snap := relSnapshot(snapshot.DefaultAPIGroup, relSnap, "ws-notebook-trigger")
	snap.SetFinalizers([]string{"example.com/hold"})
	now := metav1.Now()
	snap.SetDeletionTimestamp(&now)
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), snap)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	cond := releasedCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonSnapshotNotOwned {
		t.Fatalf("SnapshotReleased condition = %+v, want False/%s", cond, ReasonSnapshotNotOwned)
	}
}

// A snapshot without the label (older CRIU snapshots, long trigger names) is
// identified by the job's snapshotRef alone.
func TestSnapshotRelease_UnlabelledSnapshotUsesSnapshotRef(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ""))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("unlabelled snapshot named by snapshotRef still exists, want it deleted")
	}
}

// A live pod that still carries a restore order for the snapshot may still
// restore from it (for example the replacement of a job that strict
// invariants failed in Restoring): wait until it's gone.
func TestSnapshotRelease_WaitsForPodsThatReferenceIt(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "web-0-repl", UID: "repl-uid",
		Annotations: map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap}}}
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), pod)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	res := relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while a live pod still carries a restore order for it")
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v, want a recheck", res.RequeueAfter)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatalf("deleting pod: %v", err)
	}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot still exists after the referencing pod went away")
	}
}

// A pod that the restore-completion step moved to the cold-start bypass
// (#152) no longer needs the snapshot.
func TestSnapshotRelease_CompletedRestoreDoesNotHoldIt(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: relPod, UID: "repl-uid",
		Annotations: map[string]string{
			snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): "",
			util.AnnotationRestoreCompleted:                      relSnap,
		}}}
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), pod)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot still exists, though the replacement's restore was completed")
	}
}

// A gated pod assigned to the job hasn't been through the pod gate yet: wait.
func TestSnapshotRelease_WaitsForGatedClaimant(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "web-0-new", UID: "new-uid",
			Annotations: map[string]string{util.AnnotationAssignedPMJ: relJobName}},
		Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}}},
	}
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), pod)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while a gated pod still claims the job")
	}
}

// Another job that names the same snapshot and hasn't concluded holds it.
func TestSnapshotRelease_WaitsForOtherJobsOnTheSnapshot(t *testing.T) {
	other := relJob(pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, time.Second)
	other.Name = "pmj-other"
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), other, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while another job that keeps it names it")
	}
}

// A checkpoint that hasn't finished, of a source that still exists, can still
// complete: wait. Once the source is gone, nothing can.
func TestSnapshotRelease_UnfinishedCheckpointWaitsForSource(t *testing.T) {
	snap := relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger())
	snap.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{"type": "Checkpoint", "status": "False", "reason": "InProgress"},
		},
	}
	source := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: relPod, UID: relSourceUID}}
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), snap, source)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("unfinished snapshot deleted while its source still exists")
	}
	if err := c.Delete(context.Background(), source); err != nil {
		t.Fatalf("deleting source: %v", err)
	}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("unfinished snapshot still exists after its source went away")
	}
}

// A snapshot that's already gone is recorded as released, without error.
func TestSnapshotRelease_AlreadyGone(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	cond := releasedCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("SnapshotReleased condition = %+v, want True", cond)
	}
}

// A source fence that's still undecided reads the checkpoint state from the
// snapshot: the snapshot stays until the fence is resolved.
func TestSnapshotRelease_WaitsForUnresolvedFence(t *testing.T) {
	job := relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second)
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: ConditionSourceFenced, Status: metav1.ConditionTrue, Reason: util.ReasonFenced})
	c := relClient(job, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	if _, err := r.releaseSnapshot(context.Background(), job); err != nil {
		t.Fatalf("releaseSnapshot: %v", err)
	}
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while the source fence is unresolved")
	}
}

// A CRIU job deletes the snapshot in the CRIU engine's group, never a
// same-named one in another group.
func TestSnapshotRelease_UsesTheJobsEngineGroup(t *testing.T) {
	job := relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second)
	job.Annotations = map[string]string{engine.AnnotationEngine: engine.CRIUName}
	c := relClient(job,
		relSnapshot(engine.CRIUAPIGroup, relSnap, ownTrigger()),
		relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(c, relScheme())); err != nil {
		t.Fatal(err)
	}
	criu, err := engine.NewCRIU(c, relScheme(), engine.CRIUAPIGroup)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(criu); err != nil {
		t.Fatal(err)
	}
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme(), Engines: reg}
	relReconcile(t, r)
	if snapExists(t, c, engine.CRIUAPIGroup, relSnap) {
		t.Fatal("CRIU snapshot of a Succeeded CRIU job still exists")
	}
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("deleted the GKE group's snapshot for a CRIU job")
	}
}

func TestValidateSnapshotRetention(t *testing.T) {
	for _, tc := range []struct {
		d  time.Duration
		ok bool
	}{{0, true}, {10 * time.Minute, true}, {MaxSnapshotRetention, true}, {-time.Second, false}, {MaxSnapshotRetention + time.Second, false}} {
		if err := ValidateSnapshotRetention(tc.d); (err == nil) != tc.ok {
			t.Errorf("ValidateSnapshotRetention(%v) = %v, want ok=%t", tc.d, err, tc.ok)
		}
	}
}

// releaseSnapshot itself refuses every phase but Succeeded and Failed, even
// if a caller reaches it with another phase.
func TestSnapshotRelease_HelperRefusesOtherPhases(t *testing.T) {
	for _, phase := range []pmv1alpha1.PodMigrationJobPhase{
		pmv1alpha1.PodMigrationJobPhasePending,
		pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		pmv1alpha1.PodMigrationJobPhaseEvicting,
		pmv1alpha1.PodMigrationJobPhaseRestoring,
		pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
	} {
		job := relJob(phase, time.Hour)
		c := relClient(job, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
		r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
		if _, err := r.releaseSnapshot(context.Background(), job); err != nil {
			t.Fatalf("%s: releaseSnapshot: %v", phase, err)
		}
		if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
			t.Errorf("snapshot deleted for a %s job", phase)
		}
	}
}

func relTrigger(name, answered string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.ManualTriggerGVK(snapshot.DefaultAPIGroup))
	u.SetNamespace(relNS)
	u.SetName(name)
	if answered != "" {
		u.Object["status"] = map[string]interface{}{"snapshotCreated": map[string]interface{}{"name": answered}}
	}
	return u
}

// While the job's trigger survives (a fire-and-forget Cleanup failed), the
// CRIU engine would answer it with a new work order once the snapshot is
// gone, and checkpoint the source again: Cleanup is retried, and the
// snapshot is deleted only once the trigger is gone.
func TestSnapshotRelease_WaitsForTheJobsTriggerToGo(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second),
		relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), relTrigger(ownTrigger(), relSnap))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while the job's trigger still exists")
	}
	// That pass retried Cleanup, so the next one deletes the snapshot.
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot still exists after the trigger was cleaned up")
	}
}

// Another live trigger answered with the same snapshot (CRIU names its
// snapshots after the pod) holds it.
func TestSnapshotRelease_WaitsForOtherTriggerAnsweredWithIt(t *testing.T) {
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second),
		relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), relTrigger("ws-notebook-trigger", relSnap))
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while another live trigger names it")
	}
}

// A terminating or finished pod no longer restores, so it doesn't hold the
// snapshot.
func TestSnapshotRelease_FinishedPodDoesNotHoldIt(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "web-0-old", UID: "old-uid",
		Annotations: map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap}},
		Status: corev1.PodStatus{Phase: corev1.PodFailed}}
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second), relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()), pod)
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	if snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("a finished pod held the snapshot")
	}
}

// A snapshot that's already being deleted (an engine finalizer holds it) is
// recorded as released, without a second delete.
func TestSnapshotRelease_BeingDeletedIsReleased(t *testing.T) {
	snap := relSnapshot(snapshot.DefaultAPIGroup, relSnap, "")
	snap.SetFinalizers([]string{"pod-migrate.io/archive-cleanup"})
	c := relClient(relJob(pmv1alpha1.PodMigrationJobPhaseSucceeded, time.Second), snap)
	if err := c.Delete(context.Background(), snap.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme()}
	relReconcile(t, r)
	cond := releasedCond(t, c)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Message != "PodSnapshot "+relSnap+" is being deleted" {
		t.Fatalf("SnapshotReleased = %+v, want True (being deleted)", cond)
	}
}

// A job whose engine isn't registered doesn't know its API group: nothing
// is recorded or deleted.
func TestSnapshotRelease_UnregisteredEngineSkips(t *testing.T) {
	job := relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second)
	job.Annotations = map[string]string{engine.AnnotationEngine: "gone-engine"}
	c := relClient(job, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(c, relScheme())); err != nil {
		t.Fatal(err)
	}
	r := &PodMigrationJobReconciler{Client: c, Scheme: relScheme(), Engines: reg}
	if _, err := r.releaseSnapshot(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) || releasedCond(t, c) != nil {
		t.Fatal("released a snapshot for a job whose engine isn't registered")
	}
}

// The cache can trail the pod gate: the job's recorded replacement is read
// from the API server too, and holds the snapshot if it carries the restore
// order there.
func TestSnapshotRelease_LiveReadsTheRecordedReplacement(t *testing.T) {
	job := relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Second)
	job.Status.RestoredPodName = "web-0-repl"
	c := relClient(job, relSnapshot(snapshot.DefaultAPIGroup, relSnap, ownTrigger()))
	live := relClient(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "web-0-repl", UID: "repl-uid",
		Annotations: map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap}}})
	r := &PodMigrationJobReconciler{Client: c, APIReader: live, Scheme: relScheme()}
	relReconcile(t, r)
	if !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("snapshot deleted while the API server shows the replacement carrying its restore order")
	}
}
