package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
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
// budget in the PodDisruptionBudget's namespace whose source pod matches
// the PDB's selector.
func (r *PodMigrationJobReconciler) budgetWaitersForPDB(ctx context.Context, pdb client.Object) []reconcile.Request {
	pdbObj, ok := pdb.(*policyv1.PodDisruptionBudget)
	if !ok {
		return nil
	}
	selector, err := metav1.LabelSelectorAsSelector(pdbObj.Spec.Selector)
	if err != nil {
		return nil
	}

	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, jobs, client.InNamespace(pdb.GetNamespace())); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list migration jobs to wake after a PodDisruptionBudget change", "pdb", client.ObjectKeyFromObject(pdb))
		return nil
	}
	var reqs []reconcile.Request
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !waitingForDisruptionBudget(job) {
			continue
		}
		source := &corev1.Pod{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Spec.PodRef.Name}, source); err != nil {
			continue
		}
		if !selector.Matches(labels.Set(source.Labels)) {
			continue
		}
		reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(job)})
	}
	return reqs
}
