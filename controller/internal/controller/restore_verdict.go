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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

// Reasons of the Restored and Ready conditions that conclude a restore (C6,
// #79).
const (
	// ReasonRestoreVerified: the job's engine confirmed the restore with
	// positive evidence, and the replacement is Ready.
	ReasonRestoreVerified = "RestoreVerified"
	// ReasonRestoreInferred: the replacement is Ready, stayed up through the
	// stabilization window, and the engine saw no sign of a failed restore,
	// but it has no positive evidence of one either.
	ReasonRestoreInferred = "RestoreInferred"
	// ReasonRestoreLost: a container of the restored replacement restarted
	// before the restore was verified. It restarted without its restored
	// state.
	ReasonRestoreLost = "RestoreLost"
	// ReasonRestoreFailed: the engine reported that the restore failed.
	ReasonRestoreFailed = "RestoreFailed"
	// ReasonFallbackToColdStart: the engine reported that the replacement
	// started cold.
	ReasonFallbackToColdStart = "FallbackToColdStart"
)

const (
	// DefaultRestoreStabilizationWindow is the default of
	// --restore-stabilization-window: how long a replacement must stay Ready
	// without a container restart before an inferred restore is accepted.
	DefaultRestoreStabilizationWindow = 30 * time.Second
	// MaxRestoreStabilizationWindow bounds --restore-stabilization-window.
	MaxRestoreStabilizationWindow = 2 * time.Minute

	// restoreTimeout is how long a job may stay Restoring, from
	// RestoringStartTime, before the stabilization window is added: without
	// a verdict by then, the migration concludes without a restore.
	restoreTimeout = 5 * time.Minute
)

// restoreCeiling is how long a job may stay Restoring: restoreTimeout, plus
// the stabilization window, so that a replacement that becomes Ready just
// before restoreTimeout can stay up through the window.
func (r *PodMigrationJobReconciler) restoreCeiling() time.Duration {
	if r.RestoreStabilizationWindow > 0 {
		return restoreTimeout + r.RestoreStabilizationWindow
	}
	return restoreTimeout
}

// restoreVerdict asks the engine that serves job for its verdict on pod's
// restore (C6). An engine without the RestoreVerifier capability gets a
// verdict from the pod's readiness alone: InProgress until the pod is Ready,
// then an inferred Restored.
func (r *PodMigrationJobReconciler) restoreVerdict(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, eng engine.Engine) (engine.RestoreVerdict, error) {
	if verifier, ok := eng.(engine.RestoreVerifier); ok {
		return verifier.VerifyRestore(ctx, job, pod)
	}
	if !isPodReady(pod) {
		return engine.RestoreVerdict{Outcome: engine.RestoreInProgress}, nil
	}
	return engine.RestoreVerdict{
		Outcome:  engine.Restored,
		Evidence: "pod Ready; the engine reports no restore verdict",
	}, nil
}

// containerRestarts returns how many times pod's containers have restarted,
// and the name of the first one that did. It counts the app containers and
// the restartable init containers (native sidecars). A restore brings a
// container's processes back only when the pod's sandbox is created, so a
// container that restarted runs without its restored state.
func containerRestarts(pod *corev1.Pod) (int32, string) {
	sidecars := map[string]bool{}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			sidecars[c.Name] = true
		}
	}
	var total int32
	var first string
	count := func(s corev1.ContainerStatus) {
		if s.RestartCount > 0 {
			total += s.RestartCount
			if first == "" {
				first = s.Name
			}
		}
	}
	for _, s := range pod.Status.ContainerStatuses {
		count(s)
	}
	for _, s := range pod.Status.InitContainerStatuses {
		if sidecars[s.Name] {
			count(s)
		}
	}
	return total, first
}

// stabilizationRemaining returns how much longer pod must stay Ready before
// an inferred restore is accepted: the stabilization window, counted from
// the pod's Ready transition, or from job's RestoringStartTime when the pod
// records none.
func (r *PodMigrationJobReconciler) stabilizationRemaining(job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) time.Duration {
	if r.RestoreStabilizationWindow <= 0 {
		return 0
	}
	since := podReadySince(pod)
	if since.IsZero() && job.Status.RestoringStartTime != nil {
		since = job.Status.RestoringStartTime.Time
	}
	if since.IsZero() {
		return 0
	}
	if remaining := r.RestoreStabilizationWindow - time.Since(since); remaining > 0 {
		return remaining
	}
	return 0
}

// podReadySince returns when pod last became Ready, or zero if it isn't.
func podReadySince(pod *corev1.Pod) time.Time {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return cond.LastTransitionTime.Time
		}
	}
	return time.Time{}
}

// concludeRestored concludes job as Succeeded on the engine's Restored
// verdict for pod: RestoreVerified when the engine confirmed the restore,
// RestoreInferred otherwise (C6). The condition message names the engine
// and the evidence.
func (r *PodMigrationJobReconciler) concludeRestored(
	ctx context.Context,
	req ctrl.Request,
	job, origJob *pmv1alpha1.PodMigrationJob,
	pod *corev1.Pod,
	engineName string,
	verdict engine.RestoreVerdict,
) (ctrl.Result, error) {
	reason, label := ReasonRestoreInferred, "inferred"
	message := fmt.Sprintf("Replacement pod %s is Ready, and engine %s inferred the restore without positive evidence: %s",
		pod.Name, engineName, verdict.Evidence)
	if r.RestoreStabilizationWindow > 0 {
		message = fmt.Sprintf("Replacement pod %s stayed Ready for %s without a container restart, and engine %s inferred the restore without positive evidence: %s",
			pod.Name, r.RestoreStabilizationWindow, engineName, verdict.Evidence)
	}
	if verdict.Confirmed {
		reason, label = ReasonRestoreVerified, "confirmed"
		message = fmt.Sprintf("Pod state restored from snapshot and replacement pod %s is Ready; engine %s confirmed the restore: %s",
			pod.Name, engineName, verdict.Evidence)
	}
	log.FromContext(ctx).Info("Restore concluded", "pod", pod.Name, "engine", engineName, "reason", reason, "evidence", verdict.Evidence)

	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Restored",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	// The pod recovered and came up Ready, so any earlier unrecognised
	// start failure was transient after all.
	r.clearUnrecognizedRestoreCrash(job)
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return r.handleStatusError(ctx, err, "Failed to update job status to Succeeded")
	}
	metrics.MarkPMJInactive(req.NamespacedName.String())
	metrics.RecordOutcome("succeeded")
	metrics.RecordRestoreVerdict(engineName, label)
	r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseRestoring)
	return ctrl.Result{}, nil
}

// concludeWithoutRestore concludes job as SucceededWithoutRestore on the
// engine's ColdStart or Failed verdict for pod.
func (r *PodMigrationJobReconciler) concludeWithoutRestore(
	ctx context.Context,
	job, origJob *pmv1alpha1.PodMigrationJob,
	pod *corev1.Pod,
	engineName string,
	verdict engine.RestoreVerdict,
) error {
	reason, label := ReasonFallbackToColdStart, "cold_start"
	if verdict.Outcome == engine.RestoreFailed {
		reason, label = ReasonRestoreFailed, "failed"
	}
	message := verdict.Reason
	if message == "" {
		message = fmt.Sprintf("Engine %s reported %s for replacement pod %s", engineName, verdict.Outcome, pod.Name)
	}
	if verdict.Evidence != "" {
		message += " (" + verdict.Evidence + ")"
	}
	log.FromContext(ctx).Info("Replacement pod wasn't restored", "pod", pod.Name, "engine", engineName, "reason", reason, "message", message)
	if err := r.markSucceededWithoutRestore(ctx, job, origJob, reason, message); err != nil {
		return err
	}
	metrics.RecordRestoreVerdict(engineName, label)
	return nil
}

// concludeRestoreLost concludes job as SucceededWithoutRestore because a
// container of pod, its restored replacement, restarted before the restore
// was verified, and so runs without its restored state. It emits a Warning
// event on the job and on the pod.
func (r *PodMigrationJobReconciler) concludeRestoreLost(
	ctx context.Context,
	job, origJob *pmv1alpha1.PodMigrationJob,
	pod *corev1.Pod,
	engineName string,
	restarts int32,
	container string,
) error {
	message := fmt.Sprintf("Container %q of replacement pod %s restarted before the restore was verified (%d restart(s)), so it runs without its restored state",
		container, pod.Name, restarts)
	log.FromContext(ctx).Info("Restored state lost", "pod", pod.Name, "engine", engineName, "container", container, "restarts", restarts)
	if err := r.markSucceededWithoutRestore(ctx, job, origJob, ReasonRestoreLost, message); err != nil {
		return err
	}
	metrics.RecordRestoreVerdict(engineName, "lost")
	r.eventf(job, corev1.EventTypeWarning, ReasonRestoreLost, "%s", message)
	r.eventf(pod, corev1.EventTypeWarning, ReasonRestoreLost, "%s", message)
	return nil
}
