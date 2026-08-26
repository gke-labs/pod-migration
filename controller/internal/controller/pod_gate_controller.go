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
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// PodGateReconciler reconciles Pods to clean up scheduling gates on clean startups.
type PodGateReconciler struct {
	client.Client
	Scheme *runtime.Scheme
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
		if gate.Name == "gke.io/pod-migration-gate" {
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
			delete(pod.Annotations, "pod-migration.gke.io/assigned-pmj")
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations["podsnapshot.gke.io/ps-name"] = "" // Inject GKE restore-bypass
			r.removeGate(pod)
		}
		return ctrl.Result{}, r.Update(ctx, pod)
	}

	// Check the assigned PMJ status
	job := &pmv1alpha1.PodMigrationJob{}
	err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: correctedPMJ}, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Assigned PMJ completed and deleted, releasing scheduling gate", "pmj", correctedPMJ)
			r.removeGate(pod)
			return ctrl.Result{}, r.Update(ctx, pod)
		}
		logger.Error(err, "Failed to get assigned PMJ")
		return ctrl.Result{}, err
	}

	phase := job.Status.Phase
	if phase == pmv1alpha1.PodMigrationJobPhaseRestoring ||
		phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		phase == pmv1alpha1.PodMigrationJobPhaseFailed {
		if phase == pmv1alpha1.PodMigrationJobPhaseFailed {
			logger.Info("Assigned PMJ failed; releasing scheduling gate for cold-start fallback", "pod", pod.Name, "pmj", correctedPMJ)
		} else {
			logger.Info("Assigned PMJ snapshot is durable; releasing scheduling gate and injecting snapshot ref", "pod", pod.Name, "pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef)
		}

		if job.Status.Consumed && job.Status.RestoredPodUID != "" && job.Status.RestoredPodUID != string(pod.UID) {
			logger.Info("Assigned PMJ was already consumed by a different pod, releasing gate with cold-start bypass",
				"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID)
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			delete(pod.Annotations, "pod-migration.gke.io/assigned-pmj")
			pod.Annotations["podsnapshot.gke.io/ps-name"] = ""
			r.removeGate(pod)
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

func (r *PodGateReconciler) removeGate(pod *corev1.Pod) {
	var newGates []corev1.PodSchedulingGate
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name != "gke.io/pod-migration-gate" {
			newGates = append(newGates, gate)
		}
	}
	pod.Spec.SchedulingGates = newGates
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodGateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Watches(
			&pmv1alpha1.PodMigrationJob{},
			handler.EnqueueRequestsFromMapFunc(r.mapPMJToPods),
		).
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

	// 2. Query for replacement pods annotated with this PMJ (handles Jobs and Deployments)
	podList := &corev1.PodList{}
	err := r.List(ctx, podList, client.InNamespace(job.Namespace))
	if err != nil {
		return requests
	}

	for _, pod := range podList.Items {
		if pod.Annotations != nil && pod.Annotations["pod-migration.gke.io/assigned-pmj"] == job.Name {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: pod.Namespace,
					Name:      pod.Name,
				},
			})
		}
	}

	return requests
}
