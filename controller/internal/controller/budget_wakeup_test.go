package controller

import (
	"context"
	"testing"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestPDBMayHaveFreedBudget: only the PodDisruptionBudget changes after which
// a waiting job's check can pass wake the waiters.
func TestPDBMayHaveFreedBudget(t *testing.T) {
	pdb := func(allowed, healthy int32) *policyv1.PodDisruptionBudget {
		return &policyv1.PodDisruptionBudget{Status: policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: allowed, CurrentHealthy: healthy}}
	}
	for name, tc := range map[string]struct {
		got  bool
		want bool
	}{
		"more disruptions allowed": {pdbMayHaveFreedBudget.Update(event.UpdateEvent{ObjectOld: pdb(0, 2), ObjectNew: pdb(1, 2)}), true},
		"more healthy pods":        {pdbMayHaveFreedBudget.Update(event.UpdateEvent{ObjectOld: pdb(0, 1), ObjectNew: pdb(0, 2)}), true},
		"fewer healthy pods":       {pdbMayHaveFreedBudget.Update(event.UpdateEvent{ObjectOld: pdb(1, 3), ObjectNew: pdb(0, 2)}), false},
		"no status change":         {pdbMayHaveFreedBudget.Update(event.UpdateEvent{ObjectOld: pdb(0, 2), ObjectNew: pdb(0, 2)}), false},
		"PDB deleted":              {pdbMayHaveFreedBudget.Delete(event.DeleteEvent{Object: pdb(0, 2)}), true},
		"PDB created":              {pdbMayHaveFreedBudget.Create(event.CreateEvent{Object: pdb(1, 3)}), false},
	} {
		if tc.got != tc.want {
			t.Errorf("%s: predicate = %t, want %t", name, tc.got, tc.want)
		}
	}
}

// TestPodMigrationJobReconciler_BudgetWaitersForPDB: a PodDisruptionBudget
// change wakes the jobs in its namespace that wait for the budget, and no others.
func TestPodMigrationJobReconciler_BudgetWaitersForPDB(t *testing.T) {
	waiting := func(name, reason string, status metav1.ConditionStatus) *pmv1alpha1.PodMigrationJob {
		job := ifJob(name, name, pmv1alpha1.PodMigrationJobPhasePending)
		job.Status.Conditions = []metav1.Condition{{Type: ConditionDisruptionBudgetChecked, Status: status, Reason: reason, LastTransitionTime: metav1.Now()}}
		return job
	}
	elsewhere := waiting("elsewhere", reasonBudgetExhausted, metav1.ConditionFalse)
	elsewhere.Namespace = "other"
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).WithObjects(
		waiting("exhausted", reasonBudgetExhausted, metav1.ConditionFalse),
		waiting("in-flight", reasonMigrationsInFlight, metav1.ConditionFalse),
		waiting("check-error", reasonBudgetCheckError, metav1.ConditionUnknown),
		ifJob("unchecked", "unchecked", pmv1alpha1.PodMigrationJobPhasePending),
		ifJob("snapshotting", "snapshotting", pmv1alpha1.PodMigrationJobPhaseSnapshotting),
		elsewhere,
	).Build()
	r := &PodMigrationJobReconciler{Client: c, Scheme: c.Scheme()}

	reqs := r.budgetWaitersForPDB(context.Background(), ifPDB(2, "db", nil))
	got := map[types.NamespacedName]bool{}
	for _, req := range reqs {
		got[req.NamespacedName] = true
	}
	want := []string{"exhausted", "in-flight", "check-error"}
	if len(got) != len(want) {
		t.Errorf("woke %v, want %v", reqs, want)
	}
	for _, name := range want {
		if !got[types.NamespacedName{Namespace: "default", Name: name}] {
			t.Errorf("didn't wake %s; woke %v", name, reqs)
		}
	}
}
