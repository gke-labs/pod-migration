package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// The disruption budget check (#42). Before the checkpoint, the controller asks
// the API server, with a dry-run eviction of the source, whether the pod's
// PodDisruptionBudgets allow the disruption now. A checkpoint is followed by
// the source's eviction, so checkpointing a pod whose eviction a budget then
// blocks leaves the workload down: the source is stopped, and the replacement
// waits for the source to go. While the budget is exhausted, the job waits in
// Pending and checkpoints nothing.
const (
	// ConditionDisruptionBudgetChecked reports the last budget check.
	ConditionDisruptionBudgetChecked = util.ConditionDisruptionBudgetChecked

	reasonBudgetAllowed    = "Allowed"
	reasonBudgetExhausted  = "BudgetExhausted"
	reasonBudgetCheckError = "CheckError"

	// budgetRecheckInterval is how often a waiting job checks the budget again.
	budgetRecheckInterval = 5 * time.Second
)

// DisruptionBudgetChecker checks whether the source's disruption budget allows
// its eviction now.
type DisruptionBudgetChecker interface {
	// Check returns the condition reason for the result: Allowed, or
	// BudgetExhausted or CheckError together with the error.
	Check(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (string, error)
}

// EvictionBudgetChecker checks the budget with a dry-run eviction of the source.
type EvictionBudgetChecker struct {
	Client client.Client
}

// Check sends a dry-run eviction of the source. The eviction carries the
// source's UID as a precondition, so a pod that was replaced under the same
// name is never checked in its place. The eviction webhook lets dry runs
// through without side effects.
func (c EvictionBudgetChecker) Check(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (string, error) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: job.Namespace}}
	eviction := &policyv1.Eviction{
		ObjectMeta:    metav1.ObjectMeta{Name: podName, Namespace: job.Namespace},
		DeleteOptions: &metav1.DeleteOptions{},
	}
	if job.Spec.TargetPodUID != "" {
		uid := types.UID(job.Spec.TargetPodUID)
		eviction.DeleteOptions.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	err := c.Client.SubResource("eviction").Create(ctx, pod, eviction, client.DryRunAll)
	switch {
	case err == nil:
		return reasonBudgetAllowed, nil
	case apierrors.IsTooManyRequests(err):
		return reasonBudgetExhausted, err
	default:
		return reasonBudgetCheckError, err
	}
}

// skipsDisruptionBudgetCheck reports whether the job skips the budget check.
// Spot preemption is an involuntary disruption, which budgets don't cover,
// and the node goes away whatever the budget says.
func skipsDisruptionBudgetCheck(job *pmv1alpha1.PodMigrationJob) bool {
	return job.Labels[util.LabelTriggerSource] == util.TriggerSourceSpotPreemption ||
		job.Annotations[util.AnnotationTriggerSource] == util.TriggerSourceSpotPreemption
}

// checkDisruptionBudget runs the budget check and counts the result. On
// Allowed it records the condition on job (the caller persists it) and returns
// proceed=true. A dry run that finds no source pod fails the job, since there's
// nothing left to migrate. On any other result it records the condition,
// persists the status, emits MigrationDeferredByPDB the first time the budget
// blocks the job, and returns the result that requeues the next check.
func (r *PodMigrationJobReconciler) checkDisruptionBudget(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, podName string) (res ctrl.Result, proceed bool, err error) {
	reason, checkErr := r.BudgetChecker.Check(ctx, job, podName)
	switch reason {
	case reasonBudgetAllowed:
		metrics.RecordBudgetCheck("allowed")
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               ConditionDisruptionBudgetChecked,
			Status:             metav1.ConditionTrue,
			Reason:             reasonBudgetAllowed,
			Message:            "A dry-run eviction of the source was allowed",
			ObservedGeneration: job.Generation,
		})
		return ctrl.Result{}, true, nil
	case reasonBudgetExhausted:
		metrics.RecordBudgetCheck("budget_exhausted")
	default:
		reason = reasonBudgetCheckError
		metrics.RecordBudgetCheck("check_error")
		if apierrors.IsNotFound(checkErr) {
			return ctrl.Result{}, false, r.failSourceGone(ctx, job, origJob)
		}
	}

	prev := meta.FindStatusCondition(job.Status.Conditions, ConditionDisruptionBudgetChecked)
	firstBlock := prev == nil || prev.Reason != reason
	status := metav1.ConditionFalse
	message := fmt.Sprintf("A dry-run eviction of the source was refused, so nothing is checkpointed yet: %v", checkErr)
	if reason == reasonBudgetCheckError {
		status = metav1.ConditionUnknown
		message = fmt.Sprintf("The dry-run eviction of the source failed, so nothing is checkpointed yet: %v", checkErr)
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionDisruptionBudgetChecked,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return ctrl.Result{}, false, err
	}
	if firstBlock && reason == reasonBudgetExhausted {
		r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "MigrationDeferredByPDB",
			"Migration is waiting for the PodDisruptionBudget to allow a disruption; nothing has been checkpointed")
	}
	log.FromContext(ctx).Info("Disruption budget doesn't allow the source's eviction yet; waiting in Pending", "reason", reason)
	return ctrl.Result{RequeueAfter: budgetRecheckInterval}, false, nil
}

// failSourceGone fails a Pending job whose dry-run eviction found no source
// pod, as the source check at the top of the Pending phase does when the pod
// is gone from the cache.
func (r *PodMigrationJobReconciler) failSourceGone(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob) error {
	const message = "Origin pod no longer exists in Pending state"
	log.FromContext(ctx).Info("The dry-run eviction found no origin pod in Pending state, failing migration job")
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             "PodNotFound",
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return err
	}
	metrics.MarkPMJInactive(job.Namespace + "/" + job.Name)
	metrics.RecordPhaseDuration("pending", time.Since(job.CreationTimestamp.Time).Seconds())
	metrics.RecordOutcome("failed")
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", message)
	return nil
}

// waitingForDisruptionBudget reports whether a Pending job's last budget check
// didn't allow the disruption.
func waitingForDisruptionBudget(job *pmv1alpha1.PodMigrationJob) bool {
	return util.WaitingForDisruptionBudget(job)
}
