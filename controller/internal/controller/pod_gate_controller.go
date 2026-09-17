package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
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
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs/status,verbs=get;update;patch

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

	// A terminating pod is going away regardless; releasing its gate is
	// pointless and the update just races the deletion.
	if pod.DeletionTimestamp != nil {
		return ctrl.Result{}, nil
	}

	assignedPMJ := ""
	if pod.Annotations != nil {
		assignedPMJ = pod.Annotations["pod-migration.gke.io/assigned-pmj"]
	}

	if assignedPMJ == "" {
		logger.Info("Gated pod has no PMJ assignment, releasing gate with cold-start bypass")
		r.releaseWithColdStartBypass(pod)
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
			phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
			job.Status.SnapshotRef == "" {
			if job.Status.SnapshotRef == "" &&
				phase != pmv1alpha1.PodMigrationJobPhaseFailed &&
				phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				logger.Info("Assigned PMJ is in a durable phase but has no snapshot ref; releasing scheduling gate with cold-start bypass",
					"pod", pod.Name, "pmj", correctedPMJ, "phase", phase)
			} else {
				logger.Info("Assigned PMJ completed without restore; releasing scheduling gate for cold-start fallback",
					"pod", pod.Name, "pmj", correctedPMJ, "phase", phase)
			}
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

		if job.Status.Consumed && job.Status.RestoredPodUID != string(pod.UID) {
			// Single-use isolation: a migration that already concluded (Succeeded)
			// or whose scheduling gate was already released on the consumer pod must
			// never be re-restored, preventing application state rollback to checkpoint time.
			// Only in-flight Restoring PMJs whose consumer pod disappeared BEFORE
			// its scheduling gate was released can be safely recovered (#25).
			if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || job.Status.GateReleased {
				logger.Info("Assigned PMJ was already consumed (phase is not Restoring or gate was already released), releasing gate with cold-start bypass",
					"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID, "phase", job.Status.Phase, "gateReleased", job.Status.GateReleased)
				r.releaseWithColdStartBypass(pod)
				return ctrl.Result{}, r.Update(ctx, pod)
			}

			consumerExists, checkErr := r.checkConsumerPodExists(ctx, req.Namespace, job)
			if checkErr != nil {
				logger.Error(checkErr, "Failed to check if recorded consumer pod exists", "consumingPodUID", job.Status.RestoredPodUID)
				return ctrl.Result{}, checkErr
			}

			if consumerExists {
				// The recorded consumer still exists (e.g. terminating with grace period).
				// Do NOT un-gate this candidate pod with cold-start bypass yet: waiting
				// costs nothing because the candidate is safely held in Pending by its
				// scheduling gate. Once the terminating consumer finishes and disappears from
				// the API server, this candidate will recover and adopt the PMJ snapshot.
				logger.Info("Assigned PMJ consumer pod still exists (possibly terminating); requeuing to wait for completion",
					"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}

			logger.Info("Recorded consumer pod no longer exists and gate was never released; recovering stranded PMJ for adoption",
				"deadConsumerPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID, "pmj", correctedPMJ)

			// 1. Assign PMJ status to this new candidate pod
			job.Status.Consumed = true
			job.Status.RestoredPodUID = string(pod.UID)
			job.Status.RestoredPodName = pod.Name
			job.Status.GateReleased = false
			now := metav1.Now()
			job.Status.RestoringStartTime = &now

			if updateErr := r.Status().Update(ctx, job); updateErr != nil {
				logger.Error(updateErr, "Failed to update PMJ status for adopted consumer pod")
				return ctrl.Result{}, updateErr
			}
		}

		// Inject GKE's native snapshot name annotation (SnapshotRef is non-empty
		// here; the empty case released with the cold-start bypass above)
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		pod.Annotations["podsnapshot.gke.io/ps-name"] = job.Status.SnapshotRef
		logger.Info("Injected target snapshot ref to pod annotations", "snapshot", job.Status.SnapshotRef)

		// Mark PMJ as consumed by this replacement pod to prevent stale resurrection
		if !job.Status.Consumed {
			job.Status.Consumed = true
			job.Status.RestoredPodUID = string(pod.UID)
			job.Status.RestoredPodName = pod.Name
			job.Status.GateReleased = false
			now := metav1.Now()
			job.Status.RestoringStartTime = &now
			if updateErr := r.Status().Update(ctx, job); updateErr != nil {
				logger.Error(updateErr, "Failed to mark PMJ as consumed")
				return ctrl.Result{}, updateErr
			}
		}

		// Remove scheduling gate on the candidate pod
		r.removeGate(pod)
		if err := r.Update(ctx, pod); err != nil {
			return ctrl.Result{}, err
		}

		// Record that the scheduling gate was successfully released on the consumer pod.
		// Wrapped in RetryOnConflict to guarantee persistence across concurrent writers,
		// and any terminal error is returned so the workqueue retries.
		if !job.Status.GateReleased {
			err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				latest := &pmv1alpha1.PodMigrationJob{}
				if err := r.Get(ctx, client.ObjectKeyFromObject(job), latest); err != nil {
					return err
				}
				if latest.Status.GateReleased {
					return nil // already recorded by another writer
				}
				latest.Status.GateReleased = true
				return r.Status().Update(ctx, latest)
			})
			if err != nil {
				logger.Error(err, "Failed to record GateReleased on PMJ after retries", "job", job.Name)
				return ctrl.Result{}, err
			}
		}

		return ctrl.Result{}, nil
	}

	// PMJ still in an active phase.  The PMJ watch (mapPMJToPods) enqueues this
	// pod on every PMJ transition, so that's the fast path for release; this
	// requeue is only a resync backstop and must stay coarse — at 2s, 2,000
	// gated pods turn the single worker into a full-time polling loop.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// checkConsumerPodExists verifies whether the pod recorded in job.Status.RestoredPodUID
// still exists. Distinguishes between active consumption and a dead/deleted consumer pod.
func (r *PodGateReconciler) checkConsumerPodExists(ctx context.Context, namespace string, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	if job.Status.RestoredPodUID == "" {
		return false, nil
	}

	// 1. If RestoredPodName is set, perform a targeted lookup by Name
	if job.Status.RestoredPodName != "" {
		consumingPod := &corev1.Pod{}
		err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: job.Status.RestoredPodName}, consumingPod)
		if err == nil {
			if string(consumingPod.UID) == job.Status.RestoredPodUID {
				// Cache hit: re-confirm live against APIReader to guard against stale cache hits
				// where the pod was deleted on the API server but hasn't been evicted from cache yet.
				apiPod := &corev1.Pod{}
				apiErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: job.Status.RestoredPodName}, apiPod)
				if apiErr == nil {
					return string(apiPod.UID) == job.Status.RestoredPodUID, nil
				}
				if apierrors.IsNotFound(apiErr) {
					return false, nil
				}
				return false, apiErr
			}
			// Cache shows a different UID for this pod name (recreated pod).
			// Confirm against APIReader before concluding the original consumer is dead.
			apiPod := &corev1.Pod{}
			apiErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: job.Status.RestoredPodName}, apiPod)
			if apiErr == nil {
				return string(apiPod.UID) == job.Status.RestoredPodUID, nil
			}
			if apierrors.IsNotFound(apiErr) {
				return false, nil
			}
			return false, apiErr
		}
		if !apierrors.IsNotFound(err) {
			return false, err
		}

		// Informer cache returned NotFound. Read directly from the API server to guard against cache lag.
		apiErr := r.apiReader().Get(ctx, types.NamespacedName{Namespace: namespace, Name: job.Status.RestoredPodName}, consumingPod)
		if apiErr == nil {
			if string(consumingPod.UID) == job.Status.RestoredPodUID {
				return true, nil
			}
			return false, nil
		}
		if apierrors.IsNotFound(apiErr) {
			return false, nil
		}
		return false, apiErr
	}

	// 2. If RestoredPodName is empty, query pods annotated with this PMJ via a LIVE list.
	// APIReader does not support field indexing, so query live namespace list and filter in memory.
	podList := &corev1.PodList{}
	if err := r.apiReader().List(ctx, podList, client.InNamespace(namespace)); err != nil {
		return false, err
	}
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.Annotations != nil && p.Annotations[util.AnnotationAssignedPMJ] == job.Name {
			if string(p.UID) == job.Status.RestoredPodUID {
				return true, nil
			}
		}
	}
	return false, nil
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
