package util

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

const (
	// ConditionDisruptionBudgetChecked is the job condition that reports the
	// last disruption budget check before the checkpoint (#42).
	ConditionDisruptionBudgetChecked = "DisruptionBudgetChecked"

	// ReasonDisruptionBudgetTimeout is the Ready reason of a job that waited in
	// Pending for the source's PodDisruptionBudget until its deadline. Such a job
	// checkpointed nothing, so the eviction webhook replaces it with a fresh job
	// on the next eviction instead of allowing a cold eviction (#42).
	ReasonDisruptionBudgetTimeout = "DisruptionBudgetTimeout"
)

// WaitingForDisruptionBudget reports whether a Pending job's last budget check
// didn't allow the disruption.
func WaitingForDisruptionBudget(job *pmv1alpha1.PodMigrationJob) bool {
	if job == nil || job.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
		return false
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionDisruptionBudgetChecked)
	return cond != nil && cond.Status != metav1.ConditionTrue
}

// FailedOnDisruptionBudget reports whether job failed waiting for the budget.
func FailedOnDisruptionBudget(job *pmv1alpha1.PodMigrationJob) bool {
	if job == nil || job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		return false
	}
	ready := meta.FindStatusCondition(job.Status.Conditions, "Ready")
	return ready != nil && ready.Reason == ReasonDisruptionBudgetTimeout
}
