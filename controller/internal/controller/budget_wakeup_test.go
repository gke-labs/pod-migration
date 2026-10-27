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
// change wakes the jobs in its namespace that wait for the budget whose source
// pod matches the PDB's selector, and no others.
func TestPodMigrationJobReconciler_BudgetWaitersForPDB(t *testing.T) {
	waiting := func(name, reason string, status metav1.ConditionStatus) *pmv1alpha1.PodMigrationJob {
		job := ifJob(name, name, pmv1alpha1.PodMigrationJobPhasePending)
		job.Status.Conditions = []metav1.Condition{{Type: ConditionDisruptionBudgetChecked, Status: status, Reason: reason, LastTransitionTime: metav1.Now()}}
		return job
	}
	elsewhere := waiting("elsewhere", reasonBudgetExhausted, metav1.ConditionFalse)
	elsewhere.Namespace = "other"
	otherAppPod := ifPod("other-app", true)
	otherAppPod.Labels = map[string]string{"app": "other"}
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).WithObjects(
		ifPod("exhausted", true),
		ifPod("in-flight", true),
		ifPod("check-error", true),
		otherAppPod,
		waiting("exhausted", reasonBudgetExhausted, metav1.ConditionFalse),
		waiting("in-flight", reasonMigrationsInFlight, metav1.ConditionFalse),
		waiting("check-error", reasonBudgetCheckError, metav1.ConditionUnknown),
		waiting("other-app", reasonBudgetExhausted, metav1.ConditionFalse),
		ifJob("unchecked", "unchecked", pmv1alpha1.PodMigrationJobPhasePending),
		ifJob("snapshotting", "snapshotting", pmv1alpha1.PodMigrationJobPhaseSnapshotting),
		elsewhere,
	).Build()
	r := &PodMigrationJobReconciler{Client: c, Scheme: c.Scheme()}

	// 1. Selector matches "app=db": only the db waiters are woken.
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
	if got[types.NamespacedName{Namespace: "default", Name: "other-app"}] {
		t.Errorf("woke other-app whose pod does not match PDB selector")
	}

	// 2. Empty selector ({}) covers all pods in the namespace: wakes other-app too.
	emptyPDB := ifPDB(2, "db", nil)
	emptyPDB.Spec.Selector = &metav1.LabelSelector{}
	emptyReqs := r.budgetWaitersForPDB(context.Background(), emptyPDB)
	if len(emptyReqs) != 4 {
		t.Errorf("empty selector woke %d jobs, want 4", len(emptyReqs))
	}

	// 3. Nil selector covers nothing: wakes none.
	nilPDB := ifPDB(2, "db", nil)
	nilPDB.Spec.Selector = nil
	nilReqs := r.budgetWaitersForPDB(context.Background(), nilPDB)
	if len(nilReqs) != 0 {
		t.Errorf("nil selector woke %d jobs, want 0", len(nilReqs))
	}
}
