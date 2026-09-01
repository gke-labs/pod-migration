package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// MigrationGateName is the scheduling gate the replacement webhook places on
// pods whose migration is still in flight.
const MigrationGateName = "gke.io/pod-migration-gate"

func podHasMigrationGate(pod *corev1.Pod) bool {
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == MigrationGateName {
			return true
		}
	}
	return false
}

// PodGateReconciler reconciles Pods to clean up scheduling gates on clean startups.
type PodGateReconciler struct {
	client.Client
	// APIReader reads directly from the API server, bypassing the informer
	// cache.  Used to distinguish "PMJ deleted" from "PMJ not yet synced".
	APIReader client.Reader
	Scheme    *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch

// Reconcile checks for active migration jobs and removes the scheduling gate if none exist.
func (r *PodGateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	// Fetch Pod
	pod := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Pod")
		return ctrl.Result{}, err
	}

	// Check if pod has the scheduling gate
	gateIndex := -1
	for i, gate := range pod.Spec.SchedulingGates {
		if gate.Name == MigrationGateName {
			gateIndex = i
			break
		}
	}

	if gateIndex == -1 {
		return ctrl.Result{}, nil
	}

	assignedPMJ := ""
	if pod.Annotations != nil {
		assignedPMJ = pod.Annotations["pod-migration.gke.io/assigned-pmj"]
	}

	if assignedPMJ == "" {
		logger.Info("Gated pod has no PMJ assignment, releasing gate immediately")
		r.removeGate(pod)
		return ctrl.Result{}, r.Update(ctx, pod)
	}

	// Resolve parent details to look up alternative PMJs in case of collision
	parentName, parentKind, parentUID, err := util.ResolveParentWorkload(ctx, r.Client, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Parent ReplicaSet not found (likely deleted), treating as bare pod")
			err = nil // Clear error to proceed
		} else {
			return ctrl.Result{}, err
		}
	}

	// Resolve any webhook assignment races
	correctedPMJ, changed, err := util.ResolveCollision(ctx, r.Client, pod, assignedPMJ, parentName, parentKind, parentUID)
	if err != nil {
		return ctrl.Result{}, err
	}

	if changed {
		if correctedPMJ != "" {
			pod.Annotations["pod-migration.gke.io/assigned-pmj"] = correctedPMJ
			logger.Info("Re-assigned pod to alternative PMJ", "alternativePMJ", correctedPMJ)
		} else {
			logger.Info("Releasing scheduling gate for scale-up pod (race loser)")
			r.releaseWithColdStartBypass(pod)
		}
		return ctrl.Result{}, r.Update(ctx, pod)
	}

	// Check the assigned PMJ status
	job := &pmv1alpha1.PodMigrationJob{}
	err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: correctedPMJ}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// The informer cache can lag PMJ creation by an unbounded amount under
			// load, so wall-clock heuristics cannot distinguish "deleted" from
			// "not yet synced".  Only a direct API server read can.
			apiErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: correctedPMJ}, job)
			if apiErr == nil {
				logger.Info("Assigned PMJ not yet in informer cache but live on API server, requeueing", "pmj", correctedPMJ)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			if !apierrors.IsNotFound(apiErr) {
				logger.Error(apiErr, "Failed to confirm PMJ deletion via API server", "pmj", correctedPMJ)
				return ctrl.Result{}, apiErr
			}
			logger.Info("Assigned PMJ deleted, releasing scheduling gate with cold-start bypass", "pmj", correctedPMJ)
			r.releaseWithColdStartBypass(pod)
			return ctrl.Result{}, r.Update(ctx, pod)
		}
		logger.Error(err, "Failed to get assigned PMJ")
		return ctrl.Result{}, err
	}

	phase := job.Status.Phase
	if phase == pmv1alpha1.PodMigrationJobPhaseRestoring ||
		phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
		phase == pmv1alpha1.PodMigrationJobPhaseFailed {
		if phase == pmv1alpha1.PodMigrationJobPhaseFailed ||
			phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			logger.Info("Assigned PMJ completed without restore; releasing scheduling gate for cold-start fallback",
				"pod", pod.Name, "pmj", correctedPMJ, "phase", phase)
			// Record which pod fell back so verification and triage can find it,
			// then release with the explicit bypass: without it GKE may attempt a
			// native restore from a stale or mismatched snapshot.
			if !job.Status.Consumed {
				job.Status.Consumed = true
				job.Status.RestoredPodUID = string(pod.UID)
				job.Status.RestoredPodName = pod.Name
				if updateErr := r.Status().Update(ctx, job); updateErr != nil {
					logger.Error(updateErr, "Failed to mark PMJ as consumed")
					return ctrl.Result{}, updateErr
				}
			}
			r.releaseWithColdStartBypass(pod)
			return ctrl.Result{}, r.Update(ctx, pod)
		}

		logger.Info("Assigned PMJ snapshot is durable; releasing scheduling gate and injecting snapshot ref", "pod", pod.Name, "pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef)

		if job.Status.Consumed && job.Status.RestoredPodUID != "" && job.Status.RestoredPodUID != string(pod.UID) {
			logger.Info("Assigned PMJ was already consumed by a different pod, releasing gate with cold-start bypass",
				"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID)
			r.releaseWithColdStartBypass(pod)
			return ctrl.Result{}, r.Update(ctx, pod)
		}

		// Inject GKE's native snapshot name annotation when snapshotRef is present and not Failed
		if (phase == pmv1alpha1.PodMigrationJobPhaseRestoring || phase == pmv1alpha1.PodMigrationJobPhaseSucceeded) && job.Status.SnapshotRef != "" {
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations["podsnapshot.gke.io/ps-name"] = job.Status.SnapshotRef
			logger.Info("Injected target snapshot ref to pod annotations", "snapshot", job.Status.SnapshotRef)
		}

		// Mark PMJ as consumed by this replacement pod to prevent stale resurrection
		if !job.Status.Consumed {
			job.Status.Consumed = true
			job.Status.RestoredPodUID = string(pod.UID)
			job.Status.RestoredPodName = pod.Name
			if updateErr := r.Status().Update(ctx, job); updateErr != nil {
				logger.Error(updateErr, "Failed to mark PMJ as consumed")
				return ctrl.Result{}, updateErr
			}
		}

		r.removeGate(pod)
		return ctrl.Result{}, r.Update(ctx, pod)
	}

	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// releaseWithColdStartBypass removes the gate and scrubs the migration
// assignment: the assigned-pmj annotation is deleted and the ps-name
// annotation is set to the explicit empty-string bypass, which tells the GKE
// runtime NOT to attempt a native snapshot restore.  Every release that is
// not backed by a durable snapshot must go through here — a bare gate removal
// leaves the pod exposed to native restore of a stale snapshot.
func (r *PodGateReconciler) releaseWithColdStartBypass(pod *corev1.Pod) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	delete(pod.Annotations, "pod-migration.gke.io/assigned-pmj")
	pod.Annotations["podsnapshot.gke.io/ps-name"] = ""
	r.removeGate(pod)
}

func (r *PodGateReconciler) apiReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *PodGateReconciler) removeGate(pod *corev1.Pod) {
	var newGates []corev1.PodSchedulingGate
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name != MigrationGateName {
			newGates = append(newGates, gate)
		}
	}
	pod.Spec.SchedulingGates = newGates
}

// SetupWithManager sets up the controller with the Manager.
//
// MaxConcurrentReconciles is hardcoded to 1 rather than accepted as an option:
// serialized reconciles narrow the window in which two pods can adopt the same
// PMJ via FindUnassignedActivePMJ, and no other value is ever legitimate here.
// Note this serialization is a mitigation, not a guarantee — two back-to-back
// reconciles can still read the same stale informer cache, so collision
// resolution (util.ResolveCollision) remains the correctness backstop.
func (r *PodGateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Watches(
			&pmv1alpha1.PodMigrationJob{},
			handler.EnqueueRequestsFromMapFunc(r.mapPMJToPods),
		).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Complete(r)
}

func (r *PodGateReconciler) mapPMJToPods(ctx context.Context, obj client.Object) []reconcile.Request {
	job, ok := obj.(*pmv1alpha1.PodMigrationJob)
	if !ok {
		return nil
	}

	var requests []reconcile.Request

	// 1. Enqueue the original pod name (handles StatefulSets and Bare Pods)
	requests = append(requests, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: job.Namespace,
			Name:      job.Spec.PodRef.Name,
		},
	})

	// 2. Query for replacement pods annotated with this PMJ (handles Jobs and
	// Deployments).  Uses the assigned-pmj cache index: a full-namespace scan
	// here runs on every PMJ event and does not scale.
	podList := &corev1.PodList{}
	err := r.List(ctx, podList,
		client.InNamespace(job.Namespace),
		client.MatchingFields{PodAssignedPMJIndex: job.Name})
	if err != nil {
		return requests
	}

	for _, pod := range podList.Items {
		requests = append(requests, reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: pod.Namespace,
				Name:      pod.Name,
			},
		})
	}

	return requests
}
