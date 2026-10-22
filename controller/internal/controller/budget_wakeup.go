package controller

import (
	"context"

	policyv1 "k8s.io/api/policy/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// A job waiting for the disruption budget backs off its rechecks to 30 s.
// To still move on as soon as the budget frees, it's also woken up when a
// PodDisruptionBudget in its namespace may have freed some (#42).

// pdbMayHaveFreedBudget passes the PodDisruptionBudget events after which a
// waiting job's check can pass: more healthy pods, more disruptions allowed,
// or the PDB gone.
var pdbMayHaveFreedBudget = predicate.Funcs{
	CreateFunc: func(event.CreateEvent) bool { return false },
	UpdateFunc: func(e event.UpdateEvent) bool {
		oldPDB, okOld := e.ObjectOld.(*policyv1.PodDisruptionBudget)
		newPDB, okNew := e.ObjectNew.(*policyv1.PodDisruptionBudget)
		if !okOld || !okNew {
			return false
		}
		return newPDB.Status.DisruptionsAllowed > oldPDB.Status.DisruptionsAllowed ||
			newPDB.Status.CurrentHealthy > oldPDB.Status.CurrentHealthy
	},
	DeleteFunc:  func(event.DeleteEvent) bool { return true },
	GenericFunc: func(event.GenericEvent) bool { return false },
}

// budgetWaitersForPDB returns the jobs waiting in Pending for the disruption
// budget in the PodDisruptionBudget's namespace.
func (r *PodMigrationJobReconciler) budgetWaitersForPDB(ctx context.Context, pdb client.Object) []reconcile.Request {
	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, jobs, client.InNamespace(pdb.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list migration jobs to wake after a PodDisruptionBudget change", "pdb", client.ObjectKeyFromObject(pdb))
		return nil
	}
	var reqs []reconcile.Request
	for i := range jobs.Items {
		if waitingForDisruptionBudget(&jobs.Items[i]) {
			reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(&jobs.Items[i])})
		}
	}
	return reqs
}
