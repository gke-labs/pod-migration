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

// A PodSnapshot that's being deleted is never restorable (#265). Its
// deletionTimestamp is set, and a finalizer (the CRIU engine controller's
// archive cleanup, or GKE's own) holds it while its archive goes away.

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// ConditionSnapshotRestorable is False, with reason SnapshotDeleting, on a
// Restoring job whose replacement the pod gate holds because the job's
// snapshot is being deleted (#265). The job reconciler concludes such a job
// SucceededWithoutRestore, and the gate then releases the replacement cold.
const ConditionSnapshotRestorable = "SnapshotRestorable"

// snapshotDeletingHold returns the condition with which the pod gate
// recorded that it holds job's replacement because the job's snapshot is
// being deleted. It only counts while the job is Restoring.
func snapshotDeletingHold(job *pmv1alpha1.PodMigrationJob) (*metav1.Condition, bool) {
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		return nil, false
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionSnapshotRestorable)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != snapshot.ReasonSnapshotDeleting {
		return nil, false
	}
	return cond, true
}

// snapshotDeletingMessage says why a job can't restore its snapshot.
func snapshotDeletingMessage(snapshotRef string) string {
	return fmt.Sprintf("PodSnapshot %q is being deleted, so it can't be restored; the replacement starts cold", snapshotRef)
}

// replacementReleased reports whether job's replacement was released from
// its scheduling gate. The recorded consumer, read live, is the evidence:
// GateReleased is written before the gate is removed, so a replacement can
// be recorded as released and still be gated (the removal failed and is
// retried), and a job from before GateReleased existed can have a released
// replacement without the record. Without a recorded consumer that still
// exists, GateReleased decides.
func (r *PodMigrationJobReconciler) replacementReleased(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	if job.Status.Consumed && job.Status.RestoredPodName != "" {
		pod := &corev1.Pod{}
		err := r.liveReader().Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}, pod)
		switch {
		case err == nil && (job.Status.RestoredPodUID == "" || string(pod.UID) == job.Status.RestoredPodUID):
			return !podHasMigrationGate(pod), nil
		case err != nil && !apierrors.IsNotFound(err):
			return false, err
		}
	}
	return job.Status.GateReleased, nil
}

// failSnapshotDeleting fails a Snapshotting job whose snapshot is being
// deleted: it can never become restorable, so waiting out the migration
// deadline would only keep the source out of service. If the checkpoint
// completed, the engine may stop the source (C4), so the same status write
// keeps the source fenced, and the next pass evicts it cold. The record
// can't be lost once the snapshot is gone, when CheckpointState could no
// longer tell that the checkpoint completed. Otherwise the fence is decided
// as after any failure, from the checkpoint's state and the baseline.
func (r *PodMigrationJobReconciler) failSnapshotDeleting(ctx context.Context, req ctrl.Request, job, origJob *pmv1alpha1.PodMigrationJob, snapStatus *snapshot.Status) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	podName := job.Spec.PodRef.Name
	provider := r.getSnapshotProvider(job)
	if snapStatus.SnapshotRef != "" {
		job.Status.SnapshotRef = snapStatus.SnapshotRef
	}
	// The status came from the cache, where the trigger and the snapshot
	// are separate informers. The CRIU engine can delete an earlier
	// trigger's work order and create a new one under the same name for
	// this trigger (#262), and a lagging snapshot informer can still show
	// the old one being deleted. Confirm on the API server before failing.
	deleting, err := snapshot.PodSnapshotBeingDeleted(ctx, r.liveReader(), r.jobAPIGroup(job), job.Namespace, job.Status.SnapshotRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !deleting {
		logger.Info("The cache showed the job's snapshot being deleted, but the API server doesn't; looking again", "snapshot", job.Status.SnapshotRef)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	// Read the checkpoint's state while the trigger still exists. If it
	// can't be read, keep the fence, as the fence decision after a failure
	// does.
	completed := true
	why := "the snapshot engine doesn't report whether its checkpoint completed"
	if reporter, ok := provider.(snapshot.CheckpointReporter); ok {
		state, err := reporter.CheckpointState(ctx, job, podName)
		if err != nil {
			logger.Error(err, "Couldn't read the checkpoint's state for a snapshot being deleted; keeping the source fenced")
			why = fmt.Sprintf("the engine's checkpoint state couldn't be read: %v", err)
		} else {
			completed = state.Completed
			why = "the checkpoint completed before its snapshot was deleted, so the engine may stop the source"
		}
	}
	_ = provider.Cleanup(ctx, job, podName)

	message := fmt.Sprintf("PodSnapshot %q is being deleted, so it can't be restored", job.Status.SnapshotRef)
	logger.Info("The job's snapshot is being deleted; failing the migration", "snapshot", job.Status.SnapshotRef, "checkpointCompleted", completed)
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             snapshot.ReasonSnapshotDeleting,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	kept := false
	if fenced := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced); completed && fenced != nil &&
		fenced.Status == metav1.ConditionTrue && fenced.Reason == util.ReasonFenced {
		kept = true
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenceKept,
			Message:            fmt.Sprintf("The source stays fenced: %s. It's evicted so that its workload controller replaces it, unless it has no controller; delete such a pod to release it", why),
			ObservedGeneration: job.Generation,
		})
	}
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return r.handleStatusError(ctx, err, "Failed to update job status to Failed for a snapshot being deleted")
	}
	metrics.MarkPMJInactive(req.NamespacedName.String())
	metrics.RecordOutcome("failed")
	r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "%s", message)
	if kept {
		metrics.RecordSourceFence("kept")
		r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "SourceKeptFenced", "The source stays fenced: %s", why)
	}
	return ctrl.Result{Requeue: true}, nil
}
