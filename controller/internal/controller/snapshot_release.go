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

// Checkpoint garbage collection, the controller's half (C9, #107). Once a job
// is done with its checkpoint, the controller deletes the PodSnapshot that the
// job's own trigger created. The delete is the signal to the engine: the
// engine deletes the archive when its PodSnapshot goes away (for CRIU, a
// finalizer in the engine controller).

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	// ConditionSnapshotReleased records on a concluded job whether the
	// controller deleted the job's PodSnapshot (True), or won't because the
	// snapshot isn't the job's own (False).
	ConditionSnapshotReleased = "SnapshotReleased"
	// ReasonSnapshotDeleted: the controller deleted the PodSnapshot, or found
	// it already gone or being deleted.
	ReasonSnapshotDeleted = "SnapshotDeleted"
	// ReasonSnapshotNotOwned: the PodSnapshot the job names carries the
	// triggered-by label of another trigger, so it's left alone.
	ReasonSnapshotNotOwned = "SnapshotNotOwned"

	// MaxSnapshotRetention is the longest --snapshot-retention. Concluded
	// jobs are garbage collected 30 minutes after they concluded, and the
	// job is the only record of which snapshot is its own, so the release
	// needs room to retry before that.
	MaxSnapshotRetention = 20 * time.Minute

	// snapshotReleaseRecheck is how soon a release that's waiting on
	// something (a pod that still references the snapshot, a gated claimant,
	// an unfinished checkpoint, an undecided fence) looks again.
	snapshotReleaseRecheck = 15 * time.Second
)

// ValidateSnapshotRetention checks the --snapshot-retention flag.
func ValidateSnapshotRetention(d time.Duration) error {
	if d < 0 || d > MaxSnapshotRetention {
		return fmt.Errorf("snapshot-retention %v out of range [0s, %v]", d, MaxSnapshotRetention)
	}
	return nil
}

// releaseSnapshot deletes the PodSnapshot of a job that is done with it: the
// job is Succeeded (restore verified) or Failed, and SnapshotRetention has
// passed since it concluded. It returns how soon to look again, or 0 when
// there's nothing left to do. It's idempotent, and NotFound is success.
//
// The snapshot must be the job's own: the one in job.Status.SnapshotRef,
// which the controller only takes from its own trigger's status, in the
// job's engine API group. If the snapshot carries the triggered-by label,
// the label must name the job's trigger, so a snapshot of a Kubeflow
// pause/resume on the shared policy (#60) is never deleted.
//
// It never deletes a snapshot that a restore may still need:
//   - SucceededWithoutRestore keeps its snapshot: an unconsumed job can still
//     be claimed, and the snapshot stays for recovery (#32).
//   - a source fence that's still undecided reads the checkpoint state from
//     the snapshot, so it waits for the fence (#131).
//   - a checkpoint that hasn't finished can still complete while its source
//     exists.
//   - a live pod that carries a restore order naming the snapshot may still
//     restore from it. A verified restore replaces the replacement's order
//     with the cold-start bypass before Succeeded is recorded (#152), so it
//     doesn't hold the snapshot.
//   - a pod still gated on the job hasn't been through the pod gate.
//   - another job that names the snapshot and isn't Succeeded or Failed.
//   - the job's trigger still exists: the CRIU engine would answer it with a
//     new work order and checkpoint the source again. Cleanup is retried.
//   - another live trigger was answered with the snapshot (CRIU snapshot
//     names depend only on the pod).
func (r *PodMigrationJobReconciler) releaseSnapshot(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (time.Duration, error) {
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded &&
		job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		return 0, nil
	}
	if r.Engines != nil {
		if _, err := r.Engines.ForJob(job); err != nil {
			// Without its engine, the job's API group isn't known: a
			// snapshot looked up in the default group could be reported
			// gone while it's still there. Leave it alone. The orphan
			// sweep only covers registered engines' groups too, so it
			// stays until the engine is registered again.
			return 0, nil
		}
	}
	snapName := job.Status.SnapshotRef
	if snapName == "" || job.Status.CompletionTime == nil {
		return 0, nil
	}
	if meta.FindStatusCondition(job.Status.Conditions, ConditionSnapshotReleased) != nil {
		return 0, nil
	}
	if due := job.Status.CompletionTime.Add(r.SnapshotRetention); time.Now().Before(due) {
		return time.Until(due), nil
	}
	if cond := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced); cond != nil &&
		cond.Status == metav1.ConditionTrue && cond.Reason == util.ReasonFenced {
		return snapshotReleaseRecheck, nil
	}

	logger := log.FromContext(ctx).WithValues("snapshot", snapName)
	group := r.jobAPIGroup(job)
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: snapName}, snap); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return r.recordSnapshotReleased(ctx, job, metav1.ConditionTrue, ReasonSnapshotDeleted,
				fmt.Sprintf("PodSnapshot %s is gone", snapName))
		}
		return snapshotReleaseRecheck, fmt.Errorf("reading PodSnapshot %s: %w", snapName, err)
	}
	trigger := util.FormatPSMTName(job.Spec.PodRef.Name, job.Spec.TargetPodUID)
	if by, ok := snap.GetLabels()[snapshot.TriggeredByLabel]; ok && by != trigger {
		logger.Info("Not deleting a PodSnapshot that another trigger created", "triggeredBy", by, "trigger", trigger)
		r.eventf(job, corev1.EventTypeWarning, ReasonSnapshotNotOwned,
			"Not deleting PodSnapshot %s: it was created by trigger %s, not this job's trigger %s", snapName, by, trigger)
		return r.recordSnapshotReleased(ctx, job, metav1.ConditionFalse, ReasonSnapshotNotOwned,
			fmt.Sprintf("PodSnapshot %s was created by trigger %s, not %s, so it's left alone", snapName, by, trigger))
	}
	// Checked after ownership, so a snapshot another trigger made reads as
	// SnapshotNotOwned even when someone else is already deleting it.
	if snap.GetDeletionTimestamp() != nil {
		return r.recordSnapshotReleased(ctx, job, metav1.ConditionTrue, ReasonSnapshotDeleted,
			fmt.Sprintf("PodSnapshot %s is being deleted", snapName))
	}

	if held, why, err := r.triggersStillAround(ctx, job, group, trigger, snapName); err != nil {
		return snapshotReleaseRecheck, err
	} else if held {
		logger.Info("Keeping the PodSnapshot while a trigger still names it", "why", why)
		return snapshotReleaseRecheck, nil
	}

	if !snapshot.CheckpointFinished(snap) {
		gone, err := r.sourceGone(ctx, job)
		if err != nil {
			return snapshotReleaseRecheck, err
		}
		if !gone {
			logger.V(1).Info("Checkpoint hasn't finished and its source still exists; keeping the PodSnapshot for now")
			return snapshotReleaseRecheck, nil
		}
	}

	if held, why, err := r.snapshotStillNeeded(ctx, job, snapName); err != nil {
		return snapshotReleaseRecheck, err
	} else if held {
		logger.V(1).Info("PodSnapshot may still be needed; keeping it for now", "why", why)
		return snapshotReleaseRecheck, nil
	}

	uid := snap.GetUID()
	opts := []client.DeleteOption{}
	if uid != "" {
		opts = append(opts, client.Preconditions{UID: &uid})
	}
	if err := r.Delete(ctx, snap, opts...); err != nil && !apierrors.IsNotFound(err) {
		if apierrors.IsConflict(err) {
			// A PodSnapshot recreated under the same name isn't the one we
			// looked at. Look again.
			return snapshotReleaseRecheck, nil
		}
		return snapshotReleaseRecheck, fmt.Errorf("deleting PodSnapshot %s: %w", snapName, err)
	}
	logger.Info("Deleted the job's PodSnapshot", "phase", job.Status.Phase, "retention", r.SnapshotRetention)
	r.eventf(job, corev1.EventTypeNormal, ReasonSnapshotDeleted, "Deleted PodSnapshot %s", snapName)
	return r.recordSnapshotReleased(ctx, job, metav1.ConditionTrue, ReasonSnapshotDeleted,
		fmt.Sprintf("Deleted PodSnapshot %s", snapName))
}

// recordSnapshotReleased writes the SnapshotReleased condition. The patch
// checks the resourceVersion, so a stale view can't drop a condition written
// since; a conflict looks again soon, and the next pass finds the snapshot
// gone or being deleted.
func (r *PodMigrationJobReconciler) recordSnapshotReleased(ctx context.Context, job *pmv1alpha1.PodMigrationJob, status metav1.ConditionStatus, reason, message string) (time.Duration, error) {
	before := job.DeepCopy()
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionSnapshotReleased,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, before, client.MergeFromWithOptimisticLock{}); err != nil {
		*job = *before
		if apierrors.IsConflict(err) {
			return time.Second, nil
		}
		return snapshotReleaseRecheck, fmt.Errorf("recording %s: %w", ConditionSnapshotReleased, err)
	}
	return 0, nil
}

// sourceGone reports whether the job's source pod no longer exists: it's
// gone, or replaced under the same name.
func (r *PodMigrationJobReconciler) sourceGone(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Spec.PodRef.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("reading source pod %s: %w", job.Spec.PodRef.Name, err)
	}
	return job.Spec.TargetPodUID != "" && string(pod.UID) != job.Spec.TargetPodUID, nil
}

// snapshotStillNeeded reports whether a pod or another job may still need
// snapName, and why.
func (r *PodMigrationJobReconciler) snapshotStillNeeded(ctx context.Context, job *pmv1alpha1.PodMigrationJob, snapName string) (bool, string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(job.Namespace)); err != nil {
		return false, "", fmt.Errorf("listing pods: %w", err)
	}
	restoreKeys := map[string]bool{}
	for _, group := range r.apiGroups() {
		restoreKeys[snapshot.RestoreAnnotation(group)] = true
	}
	restoreKeys[snapshot.RestoreAnnotation(r.jobAPIGroup(job))] = true
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Annotations[util.AnnotationAssignedPMJ] == job.Name && podHasMigrationGate(pod) && pod.DeletionTimestamp == nil {
			return true, fmt.Sprintf("pod %s is still gated on the job", pod.Name), nil
		}
		if pod.DeletionTimestamp != nil || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for key := range restoreKeys {
			if pod.Annotations[key] == snapName {
				return true, fmt.Sprintf("pod %s carries the restore order %s=%s", pod.Name, key, snapName), nil
			}
		}
	}

	// The cache can trail the pod gate: read the job's recorded replacement
	// from the API server too, in case it was just released with a restore
	// order (#36 adoption racing a strict-mode failure).
	if name := job.Status.RestoredPodName; name != "" {
		pod := &corev1.Pod{}
		if err := r.liveReader().Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: name}, pod); err == nil {
			if pod.DeletionTimestamp == nil && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				if pod.Annotations[util.AnnotationAssignedPMJ] == job.Name && podHasMigrationGate(pod) {
					return true, fmt.Sprintf("pod %s is still gated on the job", pod.Name), nil
				}
				for key := range restoreKeys {
					if pod.Annotations[key] == snapName {
						return true, fmt.Sprintf("pod %s carries the restore order %s=%s", pod.Name, key, snapName), nil
					}
				}
			}
		} else if !apierrors.IsNotFound(err) {
			return false, "", fmt.Errorf("reading pod %s: %w", name, err)
		}
	}

	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, jobs, client.InNamespace(job.Namespace)); err != nil {
		return false, "", fmt.Errorf("listing PodMigrationJobs: %w", err)
	}
	for i := range jobs.Items {
		other := &jobs.Items[i]
		if other.Name == job.Name || other.Status.SnapshotRef != snapName || r.jobAPIGroup(other) != r.jobAPIGroup(job) {
			continue
		}
		if other.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded &&
			other.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			return true, fmt.Sprintf("job %s (%s) also names it", other.Name, other.Status.Phase), nil
		}
	}
	return false, "", nil
}

// triggersStillAround reports whether the job's own trigger still exists
// (then it retries Cleanup), or another live trigger in group was answered
// with snapName.
func (r *PodMigrationJobReconciler) triggersStillAround(ctx context.Context, job *pmv1alpha1.PodMigrationJob, group, trigger, snapName string) (bool, string, error) {
	triggers := &unstructured.UnstructuredList{}
	triggers.SetGroupVersionKind(snapshot.ManualTriggerGVK(group).GroupVersion().WithKind("PodSnapshotManualTriggerList"))
	if err := r.List(ctx, triggers, client.InNamespace(job.Namespace)); err != nil {
		if meta.IsNoMatchError(err) {
			return false, "", nil
		}
		return false, "", fmt.Errorf("listing triggers: %w", err)
	}
	for i := range triggers.Items {
		t := &triggers.Items[i]
		if t.GetName() == trigger {
			_ = r.getSnapshotProvider(job).Cleanup(ctx, job, job.Spec.PodRef.Name)
			return true, fmt.Sprintf("the job's trigger %s still exists", trigger), nil
		}
		if t.GetDeletionTimestamp() != nil {
			continue
		}
		if name, _, _ := unstructured.NestedString(t.Object, "status", "snapshotCreated", "name"); name == snapName {
			return true, fmt.Sprintf("trigger %s was answered with it", t.GetName()), nil
		}
	}
	return false, "", nil
}
