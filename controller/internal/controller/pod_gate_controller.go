// SPDX-License-Identifier: Apache-2.0
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
	"errors"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
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
	APIReader       client.Reader
	Scheme          *runtime.Scheme
	Recorder        record.EventRecorder
	InvariantEngine *invariants.Engine

	// Engines provides the snapshot engine whose restore orders a released
	// pod gets: the job's engine for a restore, the pod's for a cold start.
	// Optional: nil selects GKE Pod Snapshots.
	Engines *engine.Registry

	// RestorePlacement places a restored replacement on a node that can
	// restore its checkpoint (C8, #14), if its ReplicaSet or StatefulSet
	// would recreate it: the job's engine's node constraints go into the
	// pod's required node affinity when the gate is released with a restore
	// order, even when no node meets them yet. If it isn't scheduled within
	// the placement deadline while a node without them could take it, or by
	// the restore ceiling, the job reconciler deletes it for that owner to
	// recreate, to start cold. Another pod is released without them, as
	// with placement off. Off adds no constraints; a pod that an earlier
	// version held because no node met them still starts cold.
	RestorePlacement bool
}

// jobRestoreOrderer returns how to order job's restore: through the engine
// that serves job. An engine without the RestoreOrderer capability, or no
// engine, gets GKE Pod Snapshots' restore orders, the ones used before the
// registry. It fails when job names an engine that isn't registered:
// another engine's restore order would restore the wrong snapshot, and the
// job reconciler fails such a job (#80).
func (r *PodGateReconciler) jobRestoreOrderer(job *pmv1alpha1.PodMigrationJob) (engine.RestoreOrderer, error) {
	var eng engine.Engine
	if r.Engines != nil {
		var err error
		if eng, err = r.Engines.ForJob(job); errors.Is(err, engine.ErrEngineNotRegistered) {
			return nil, err
		}
	}
	if orderer, ok := eng.(engine.RestoreOrderer); ok {
		return orderer, nil
	}
	return engine.NewGKEPodSnapshots(r.Client, r.Scheme), nil
}

// podRestoreOrderer returns how to order a cold start for pod: through the
// engine that serves pod, which its runtime class routes to through the
// namespace's PodMigration or the controller's routes. Like
// jobRestoreOrderer, it falls back to GKE Pod Snapshots' restore orders.
func (r *PodGateReconciler) podRestoreOrderer(ctx context.Context, pod *corev1.Pod) engine.RestoreOrderer {
	var eng engine.Engine
	if r.Engines != nil {
		pm, err := util.ActivePodMigration(ctx, r.Client, pod.Namespace)
		if err != nil {
			log.FromContext(ctx).Error(err, "Failed to look up the namespace's PodMigration; ordering the cold start through the controller's routes")
		}
		eng, _ = r.Engines.ForPod(pod, pm)
	}
	if orderer, ok := eng.(engine.RestoreOrderer); ok {
		return orderer
	}
	return engine.NewGKEPodSnapshots(r.Client, r.Scheme)
}

func (r *PodGateReconciler) evaluateInvariants(ctx context.Context, pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob, reconcileErr *error) {
	if !r.InvariantEngine.Enabled() || pod == nil {
		return
	}
	if reconcileErr != nil && *reconcileErr != nil {
		return
	}

	if job == nil && pod.Annotations != nil {
		if assigned := pod.Annotations[util.AnnotationAssignedPMJ]; assigned != "" {
			fetched := &pmv1alpha1.PodMigrationJob{}
			if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: assigned}, fetched); err == nil {
				job = fetched
			}
		}
	}

	// The job migrating this pod, if any, lets I2 tell a replacement that is
	// being migrated again from one that started cold (#151). Job creators
	// name the job after the pod's name and UID.
	var targeting []pmv1alpha1.PodMigrationJob
	if pod.UID != "" {
		migrating := &pmv1alpha1.PodMigrationJob{}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: util.FormatPMJName(pod.Name, string(pod.UID))}
		if err := r.Get(ctx, key, migrating); err == nil && migrating.Spec.TargetPodUID == string(pod.UID) {
			targeting = append(targeting, *migrating)
		}
	}

	var restoreOrders invariants.RestoreOrderFunc
	if r.Engines != nil {
		restoreOrders = r.Engines.InspectRestoreOrder
	}
	_, _ = r.InvariantEngine.Evaluate(ctx, &invariants.ReconcileSnapshot{
		Now:           time.Now(),
		Reconciler:    "PodGateReconciler",
		PrimaryPod:    pod,
		PrimaryPMJ:    job,
		TargetingPMJs: targeting,
		RestoreOrders: restoreOrders,
	})
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs/status,verbs=get;update;patch

// Reconcile checks for active migration jobs and removes the scheduling gate if none exist.
func (r *PodGateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, reconcileErr error) {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	// Fetch Pod
	pod := &corev1.Pod{}
	err := r.Get(ctx, req.NamespacedName, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.InvariantEngine.ForgetObject("PodGateReconciler", "pod", req.Namespace, req.Name)
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get Pod")
		return ctrl.Result{}, err
	}

	var observedJob *pmv1alpha1.PodMigrationJob
	defer func() {
		r.evaluateInvariants(ctx, pod, observedJob, &reconcileErr)
	}()

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
		r.releaseWithColdStartBypass(ctx, pod)
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
			r.releaseWithColdStartBypass(ctx, pod)
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
			r.releaseWithColdStartBypass(ctx, pod)
			return ctrl.Result{}, r.Update(ctx, pod)
		}
		logger.Error(err, "Failed to get assigned PMJ")
		return ctrl.Result{}, err
	}

	observedJob = job
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
			r.releaseWithColdStartBypass(ctx, pod, job)
			return ctrl.Result{}, r.Update(ctx, pod)
		}

		// An earlier version of the pod gate found no node that could
		// restore the checkpoint (C8, #14), and held the replacement: wait
		// for the job to conclude without a restore, which releases the gate
		// cold (above), as recorded. The PMJ watch enqueues the pod when it
		// does. A hold only counts while the job is Restoring, the only
		// phase that concludes on it.
		if _, held := noCompatibleNode(job); held {
			logger.Info("No node can restore the checkpoint; waiting for the job to conclude without a restore", "pmj", correctedPMJ)
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

		logger.Info("Assigned PMJ snapshot is durable; releasing scheduling gate and injecting snapshot ref", "pod", pod.Name, "pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef)

		adoptStranded := false
		if job.Status.Consumed && job.Status.RestoredPodUID != string(pod.UID) {
			// Single-use isolation: a migration that already concluded (Succeeded)
			// or whose scheduling gate was already released on the consumer pod must
			// never be re-restored, preventing application state rollback to checkpoint time.
			// Only in-flight Restoring PMJs whose consumer pod disappeared BEFORE
			// its scheduling gate was released can be safely recovered (#25).
			if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || job.Status.GateReleased {
				logger.Info("Assigned PMJ was already consumed (phase is not Restoring or gate was already released), releasing gate with cold-start bypass",
					"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID, "phase", job.Status.Phase, "gateReleased", job.Status.GateReleased)
				r.releaseWithColdStartBypass(ctx, pod, job)
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
				// the API server, this candidate will recover: it adopts the PMJ snapshot.
				logger.Info("Assigned PMJ consumer pod still exists (possibly terminating); requeuing to wait for completion",
					"consumingPodUID", job.Status.RestoredPodUID, "currentPodUID", pod.UID)
				return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
			}
			adoptStranded = true
		}

		// Never order a restore from a snapshot that's being deleted (#265)
		// or is gone (#269): its archive is going away, or went. Hold the
		// replacement instead. The job reconciler concludes such a job
		// SucceededWithoutRestore, and the PMJ watch brings the pod back here
		// to be released cold, recorded, above. A read error keeps the pod
		// gated too. A recorded hold sticks until the job concludes: a
		// snapshot being deleted can finish going away meanwhile. A Restoring
		// job only got there on a Ready snapshot, so one that isn't found on
		// the API server is gone, not yet to be created.
		if _, held := snapshotUnrestorableHold(job); held {
			logger.Info("Holding the replacement until the job concludes without a restore; its snapshot can't be restored",
				"pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef)
			return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
		}
		if phase == pmv1alpha1.PodMigrationJobPhaseRestoring {
			reason, err := snapshot.PodSnapshotUnrestorable(ctx, r.apiReader(), r.jobSnapshotGroup(job), job.Namespace, job.Status.SnapshotRef)
			if err != nil {
				logger.Error(err, "Failed to check whether the job's snapshot can be restored", "pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef)
				return ctrl.Result{}, err
			}
			if reason != "" {
				logger.Info("The job's snapshot can't be restored; holding the replacement until the job concludes without a restore",
					"pmj", correctedPMJ, "snapshot", job.Status.SnapshotRef, "reason", reason)
				// Record the hold for the job reconciler to conclude on, as
				// for NoCompatibleNode, so it needn't read the snapshot on
				// every pass. The resourceVersion check keeps a stale view
				// from dropping a condition written since.
				orig := job.DeepCopy()
				patch := client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})
				meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
					Type:               ConditionSnapshotRestorable,
					Status:             metav1.ConditionFalse,
					Reason:             reason,
					Message:            unrestorableMessage(reason, job.Status.SnapshotRef),
					ObservedGeneration: job.Generation,
				})
				if err := r.Status().Patch(ctx, job, patch); err != nil {
					*job = *orig
					if apierrors.IsConflict(err) {
						return ctrl.Result{RequeueAfter: time.Second}, nil
					}
					logger.Error(err, "Failed to record that the job's snapshot can't be restored", "pmj", correctedPMJ)
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
			}
		}

		// Place the restore on a node that can restore it (C8, #14). The
		// constraints go into the update that removes the gate, the only
		// time Kubernetes lets them be added, and they can't be removed
		// after it. Only a pod that its ReplicaSet or StatefulSet would
		// recreate gets them (restorePlacement), even if no node meets them
		// yet: a node can still come up, such as one that isn't Ready yet,
		// one being replaced, or one that the cluster autoscaler adds once it
		// sees the pod, which it doesn't while the pod is gated. If the pod
		// isn't scheduled within the placement deadline while a node without
		// them could take it, or by the restore ceiling, the job reconciler
		// concludes the job and deletes the pod for that owner to recreate,
		// and the new pod starts cold. The node check only records whether a
		// node met them at the release. Only a Restoring job gets
		// constraints, because only then does the job reconciler's placement
		// deadline back them; in another phase a new release gets none, as
		// without placement. A release that is already under way, this pod's
		// own, gets them below, without a new check.
		var placement []corev1.NodeSelectorRequirement
		underWay := releaseUnderWay(job, pod)
		noNodeYet := false
		if phase == pmv1alpha1.PodMigrationJobPhaseRestoring && !underWay {
			if placement, err = r.restorePlacement(ctx, job, pod); err != nil {
				logger.Error(err, "Failed to get the restore's node constraints", "pmj", correctedPMJ)
				return ctrl.Result{}, err
			}
			if len(placement) > 0 {
				fits, err := r.someNodeFits(ctx, pod, placement)
				if err != nil {
					logger.Error(err, "Failed to check the nodes against the restore's node constraints", "pmj", correctedPMJ)
					return ctrl.Result{}, err
				}
				if !fits {
					noNodeYet = true
					logger.Info("No node meets the restore's node constraints yet; releasing the replacement with them so that one can come up",
						"pmj", correctedPMJ, "constraints", describeRequirements(placement))
				}
			}
		}

		if adoptStranded {
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

		// Order the restore of the job's snapshot through its engine (GKE Pod
		// Snapshots: the snapshot name in podsnapshot.gke.io/ps-name).
		// SnapshotRef is non-empty here; the empty case released with the
		// cold-start bypass above.
		orderer, err := r.jobRestoreOrderer(job)
		if err != nil {
			logger.Error(err, "No engine to order the restore; waiting for the job to fail", "pmj", correctedPMJ)
			return ctrl.Result{}, err
		}
		if underWay {
			if placement, err = r.restorePlacement(ctx, job, pod); err != nil {
				logger.Error(err, "Failed to get the restore's node constraints", "pmj", correctedPMJ)
				return ctrl.Result{}, err
			}
		}
		if err := orderer.OrderRestore(pod, job); err != nil {
			logger.Error(err, "Failed to order the restore", "pmj", correctedPMJ)
			return ctrl.Result{}, err
		}
		var jobEng engine.Engine
		if r.Engines != nil {
			jobEng, _ = r.Engines.ForJob(job)
		}
		if isGVisorPod(pod) {
			gkeRestoreKey := engine.RestoreAnnotation(engine.DefaultAPIGroup)
			if _, hasGKE := pod.Annotations[gkeRestoreKey]; !hasGKE {
				if pod.Annotations == nil {
					pod.Annotations = make(map[string]string)
				}
				// Always stamp the GKE cold-start bypass when ordering a restore under a
				// non-GKE key on a gVisor pod (e.g. gvisor=criu) so GKE never starts with
				// podsnapshot.gke.io/ps-name absent (#151, #135).
				pod.Annotations[gkeRestoreKey] = ""
			}
		}
		// If the CRIU restore webhook speculatively wrapped a replacement pod whose
		// job is served by another engine on a distinct API group, stamp CRIU's
		// explicit cold-start bypass so the holder wrapper does not wait out its
		// assign window if the primary engine falls back to cold start (#135).
		if pod.Annotations[engine.CRIUInjectedAnnotation] == "true" && r.Engines != nil {
			criuEng, hasCRIU := r.Engines.Get(engine.CRIUName)
			if hasCRIU && criuEng != nil && (jobEng == nil || criuEng.RestoreAnnotation() != jobEng.RestoreAnnotation()) {
				if criuOrderer, ok := criuEng.(engine.RestoreOrderer); ok {
					_ = criuOrderer.OrderColdStartBypass(pod)
				}
			}
		}
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

		// 1. Durable record first: mark GateReleased = true on PMJ status before un-gating the pod.
		// If this fails, the pod is still gated, fails closed, and cannot restore until retried.
		// A merge patch with a resourceVersion check fails on any concurrent change instead of
		// overwriting it. That matters on a retry, where the Consumed write above is skipped: a
		// stale view must not release this pod with a restore order onto a job that has
		// concluded since, for example at the restore ceiling.
		// The same patch records the restore's node constraints (C8, #14), which the job
		// reconciler's placement deadline counts from, and keeps the record in step with
		// this release: a retry may add none where an earlier attempt recorded some (placement
		// turned off since, for example), and neither the placement deadline nor the check
		// after a RestoreTimeout conclusion may act on a pod without them.
		recorded := restorePlacementRecorded(job)
		if !job.Status.GateReleased || (len(placement) > 0) != recorded {
			orig := job.DeepCopy()
			patch := client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})
			job.Status.GateReleased = true
			if len(placement) > 0 {
				recordRestorePlacement(job, placement, noNodeYet)
			} else if recorded {
				meta.RemoveStatusCondition(&job.Status.Conditions, ConditionRestorePlacement)
			}
			if err := r.Status().Patch(ctx, job, patch); err != nil {
				// Nothing was written, so keep the job as it was read.
				*job = *orig
				if apierrors.IsConflict(err) {
					// The job changed after it was read, for example it concluded,
					// which a retry on a stale view can expect: decide again on the
					// current job.
					logger.Info("The job changed while recording the gate release; requeuing", "job", job.Name)
					return ctrl.Result{RequeueAfter: time.Second}, nil
				}
				logger.Error(err, "Failed to patch GateReleased on PMJ status", "job", job.Name)
				return ctrl.Result{}, err
			}
		}

		// 2. Irreversible step: remove scheduling gate on the candidate pod,
		// in the same update that adds the restore's node constraints.
		addRequiredNodeAffinity(pod, placement)
		r.removeGate(pod)
		if err := r.Update(ctx, pod); err != nil {
			return ctrl.Result{}, err
		}

		if r.Recorder != nil {
			switch {
			case len(placement) > 0 && noNodeYet:
				r.Recorder.Eventf(pod, corev1.EventTypeNormal, "MigrationRestoreReleased", "Removed scheduling gate %s, injected snapshot restore annotation and required node affinity %s; no node meets it yet, so the pod waits for one, and may be recreated to start cold if none comes in time", MigrationGateName, describeRequirements(placement))
			case len(placement) > 0:
				r.Recorder.Eventf(pod, corev1.EventTypeNormal, "MigrationRestoreReleased", "Removed scheduling gate %s, injected snapshot restore annotation and required node affinity %s", MigrationGateName, describeRequirements(placement))
			case r.RestorePlacement:
				r.Recorder.Eventf(pod, corev1.EventTypeNormal, "MigrationRestoreReleased", "Removed scheduling gate %s and injected snapshot restore annotation, without adding node constraints", MigrationGateName)
			default:
				r.Recorder.Eventf(pod, corev1.EventTypeNormal, "MigrationRestoreReleased", "Removed scheduling gate %s and injected snapshot restore annotation", MigrationGateName)
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
// assignment: the assigned-pmj annotation is deleted and the engine's
// cold-start bypass is ordered (GKE Pod Snapshots: ps-name set to the
// explicit empty string, which tells the GKE runtime NOT to attempt a native
// snapshot restore).  Every release that is not backed by a durable snapshot
// must go through here — a bare gate removal leaves the pod exposed to native
// restore of a stale snapshot.
func (r *PodGateReconciler) releaseWithColdStartBypass(ctx context.Context, pod *corev1.Pod, job ...*pmv1alpha1.PodMigrationJob) {
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	delete(pod.Annotations, "pod-migration.gke.io/assigned-pmj")
	var j *pmv1alpha1.PodMigrationJob
	if len(job) > 0 {
		j = job[0]
	}
	if j != nil {
		if orderer, err := r.jobRestoreOrderer(j); err == nil && orderer != nil {
			_ = orderer.OrderColdStartBypass(pod)
		}
	}
	// The bypass is ordered through the engine that serves the pod. It can
	// only fail for a nil pod.
	_ = r.podRestoreOrderer(ctx, pod).OrderColdStartBypass(pod)
	if pod.Annotations[engine.CRIUInjectedAnnotation] == "true" && r.Engines != nil {
		if criuEng, ok := r.Engines.Get(engine.CRIUName); ok && criuEng != nil {
			if criuOrderer, ok := criuEng.(engine.RestoreOrderer); ok {
				_ = criuOrderer.OrderColdStartBypass(pod)
			}
		}
	}
	if isGVisorPod(pod) {
		gkeRestoreKey := engine.RestoreAnnotation(engine.DefaultAPIGroup)
		if _, hasGKE := pod.Annotations[gkeRestoreKey]; !hasGKE {
			pod.Annotations[gkeRestoreKey] = ""
		}
	}
	r.removeGate(pod)
}

func isGVisorPod(pod *corev1.Pod) bool {
	return pod != nil && pod.Spec.RuntimeClassName != nil && *pod.Spec.RuntimeClassName == "gvisor"
}

// jobSnapshotGroup returns the API group of job's PodSnapshot: its engine's,
// or GKE Pod Snapshots' without a registry or for an engine that isn't
// registered.
func (r *PodGateReconciler) jobSnapshotGroup(job *pmv1alpha1.PodMigrationJob) string {
	if r.Engines != nil {
		if eng, err := r.Engines.ForJob(job); err == nil && eng != nil {
			if g := strings.TrimSpace(eng.APIGroup()); g != "" {
				return g
			}
		}
	}
	return snapshot.DefaultAPIGroup
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
