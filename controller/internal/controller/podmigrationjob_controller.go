package controller

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// fallbackEventCheckInterval throttles the uncached Events API probe for
// not-yet-Ready replacement pods: often enough to surface a crashlooping
// cold-start fallback promptly, coarse enough that a large drain does not
// spend the QPS budget on event LISTs.
const fallbackEventCheckInterval = 30 * time.Second

// Condition types and reasons for the restore-crash cold-start fallback.
const (
	// ConditionRestoreCrashed distinguishes a PMJ that we failed *and acted on*
	// (the replacement pod was deleted so its controller recreates it cold)
	// from every other route to Phase=Failed.  It is deliberately not
	// SucceededWithoutRestore: that phase means "the same pod came up without
	// the snapshot", which we can assert for a native GKE fallback but not
	// here — we destroy the pod and never observe its successor.
	ConditionRestoreCrashed = "RestoreCrashed"
	// ReasonRestoreCrashFallback marks the condition set when a recognised
	// restore-failure signature triggered the destructive fallback.
	ReasonRestoreCrashFallback = "RestoreCrashFallback"

	// ConditionUnrecognizedRestoreCrash records a StartError/128 crash whose
	// message matched no known signature.  It doubles as the once-only guard
	// for the corresponding Warning event: the event is emitted only on the
	// reconcile that actually transitions the condition.
	ConditionUnrecognizedRestoreCrash = "UnrecognizedRestoreCrash"
	// ReasonUnrecognizedRestoreCrash is the reason paired with
	// ConditionUnrecognizedRestoreCrash.
	ReasonUnrecognizedRestoreCrash = "UnrecognizedRestoreCrash"
)

// PodMigrationJobReconciler reconciles a PodMigrationJob object.
type PodMigrationJobReconciler struct {
	client.Client
	APIReader        client.Reader
	Scheme           *runtime.Scheme
	SnapshotProvider snapshot.Provider
	// Recorder emits Warning events for restore crashes we recognise as
	// start failures but cannot attribute to a known signature.  Optional:
	// nil disables event emission (used by unit tests that don't assert on
	// events).
	Recorder record.EventRecorder

	// RestoreEngines classifies restore failures.  Optional: nil selects the
	// default engine set.  Overridden in tests.
	RestoreEngines []restore.Engine

	// fallbackEventChecks records the last fallback-event probe per PMJ
	// (namespace/name -> time.Time).  In-memory only; a restart just means
	// one extra probe per in-flight migration.
	fallbackEventChecks sync.Map
}

func (r *PodMigrationJobReconciler) restoreEngines() []restore.Engine {
	if r.RestoreEngines != nil {
		return r.RestoreEngines
	}
	return restore.DefaultEngines()
}

// liveReader returns a reader that bypasses the informer cache.
//
// Falls back to the cached client only when APIReader is unset, which in a
// manager-built reconciler never happens — cmd/main.go always wires
// mgr.GetAPIReader().  The fallback exists for unit tests that construct the
// reconciler directly with a single fake client standing in for both.
func (r *PodMigrationJobReconciler) liveReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// eventf emits an event if a Recorder is wired, and is a no-op otherwise.
func (r *PodMigrationJobReconciler) eventf(obj runtime.Object, eventType, reason, messageFmt string, args ...any) {
	if r.Recorder == nil {
		return
	}
	r.Recorder.Eventf(obj, eventType, reason, messageFmt, args...)
}

func (r *PodMigrationJobReconciler) recordPodEvent(ctx context.Context, job *pmv1alpha1.PodMigrationJob, eventType, reason, messageFmt string, args ...any) {
	if r.Recorder == nil || job == nil {
		return
	}
	podName := job.Spec.PodRef.Name
	if podName == "" {
		return
	}
	pod := &corev1.Pod{}
	err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, pod)
	if err == nil {
		r.Recorder.Eventf(pod, eventType, reason, messageFmt, args...)
		return
	}
	fallbackPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      podName,
			UID:       types.UID(job.Spec.TargetPodUID),
		},
	}
	r.Recorder.Eventf(fallbackPod, eventType, reason, messageFmt, args...)
}

func (r *PodMigrationJobReconciler) recordPreviousPhaseDuration(job *pmv1alpha1.PodMigrationJob, prevPhase pmv1alpha1.PodMigrationJobPhase) {
	switch prevPhase {
	case pmv1alpha1.PodMigrationJobPhasePending:
		metrics.RecordPhaseDuration("pending", time.Since(job.CreationTimestamp.Time).Seconds())
	case pmv1alpha1.PodMigrationJobPhaseSnapshotting:
		if job.Status.SnapshottingStartTime != nil {
			metrics.RecordPhaseDuration("snapshotting", time.Since(job.Status.SnapshottingStartTime.Time).Seconds())
		}
	case pmv1alpha1.PodMigrationJobPhaseEvicting:
		// The evicting anchor is the pod-migration.gke.io/evicting-since annotation,
		// which is only written while the origin pod still exists. If the origin pod was
		// already gone when entering Evicting, this annotation is absent and no evicting
		// sample is recorded.
		if job.Annotations != nil {
			if s := job.Annotations["pod-migration.gke.io/evicting-since"]; s != "" {
				if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
					metrics.RecordPhaseDuration("evicting", time.Since(t).Seconds())
				} else if t, err := time.Parse(time.RFC3339, s); err == nil {
					metrics.RecordPhaseDuration("evicting", time.Since(t).Seconds())
				}
			}
		}
	case pmv1alpha1.PodMigrationJobPhaseRestoring:
		if job.Status.RestoringStartTime != nil {
			metrics.RecordPhaseDuration("restoring", time.Since(job.Status.RestoringStartTime.Time).Seconds())
		}
	}
}

// ensureCrashedReplacementPodDeleted performs the destructive half of the
// restore-crash fallback, idempotently.
//
// It is deliberately separate from the status write so it can be re-driven:
// the fallback persists Phase=Failed BEFORE deleting (so the verdict survives
// a crash), which means a Delete that fails — a throttled DELETE during a
// large evacuation is the realistic case — cannot be retried by the Restoring
// case, because the phase is no longer Restoring.  The Failed-phase completion
// path in Reconcile calls this until it succeeds.
//
// prefetched may carry a pod the caller has already read STRAIGHT FROM THE API
// SERVER, in which case no read is issued here.  Pass nil to have the helper
// fetch its own.  It never reads the informer cache: this function deletes, and
// a cache lagging behind a pod that has already been replaced would make it
// delete the successor.  (The UID precondition on the Delete would in fact
// catch that, but relying on a server-side guard to compensate for knowingly
// stale input is not a margin worth spending.)
//
// Returns nil when there is nothing left to do: no recorded pod, the pod is
// gone, the pod is already terminating, or the name now belongs to a successor
// with a different UID (which must never be deleted — it is the cold-start
// replacement the fallback exists to create).
func (r *PodMigrationJobReconciler) ensureCrashedReplacementPodDeleted(ctx context.Context, job *pmv1alpha1.PodMigrationJob, prefetched *corev1.Pod) error {
	if job.Status.RestoredPodName == "" {
		return nil
	}

	pod := prefetched
	if pod == nil {
		pod = &corev1.Pod{}
		if err := r.liveReader().Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("failed to get crashed replacement pod %s: %w", job.Status.RestoredPodName, err)
		}
	}

	if job.Status.RestoredPodUID != "" && string(pod.UID) != job.Status.RestoredPodUID {
		// The successor already took the name; the crashed instance is gone.
		return nil
	}
	if pod.DeletionTimestamp != nil {
		return nil
	}

	// The UID precondition closes the same race at the API server: between the
	// read above and this call the pod could be replaced by a same-named
	// successor, and deleting that would be a second, self-inflicted outage.
	if err := r.Delete(ctx, pod, client.Preconditions{UID: &pod.UID}); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			// Both mean the instance we targeted is already gone.
			return nil
		}
		return fmt.Errorf("failed to delete crashed replacement pod %s: %w", pod.Name, err)
	}
	return nil
}

// classifyRecordedPodLive reads the PMJ's recorded replacement pod STRAIGHT
// FROM THE API SERVER and classifies it, bypassing the informer cache.
//
// Every destructive decision in the restore-crash fallback goes through here.
// The per-reconcile classification runs off the cache — that is what makes a 2s
// requeue affordable — but a cache that is even one watch event behind can show
// a StartError on a pod that has since recovered, and the remedy for a fatal
// verdict is deletion.  So the cached verdict selects the candidate and this
// live read decides.
//
// Returns FailureNone (with a nil pod) when there is nothing to judge: no
// recorded pod, the pod is gone, or the name now belongs to a different
// instance.  A UID mismatch specifically must NOT be reported as a failure —
// that pod is the successor, not the casualty.
func (r *PodMigrationJobReconciler) classifyRecordedPodLive(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (*corev1.Pod, restore.Failure, error) {
	none := restore.Failure{Class: restore.FailureNone}
	if job.Status.RestoredPodName == "" {
		return nil, none, nil
	}

	pod := &corev1.Pod{}
	if err := r.liveReader().Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, none, nil
		}
		return nil, none, fmt.Errorf("failed to live-read replacement pod %s: %w", job.Status.RestoredPodName, err)
	}
	if job.Status.RestoredPodUID != "" && string(pod.UID) != job.Status.RestoredPodUID {
		return nil, none, nil
	}
	return pod, restore.Classify(pod, r.restoreEngines()...), nil
}

// concludeRestoreCrash performs the entire fatal-path transition: metric,
// status (Failed + RestoreCrashed, persisted BEFORE the deletion), Warning
// event, and the deletion itself.
//
// pod must be the instance the verdict was reached on, read live — it is handed
// straight to the deleter.
//
// Two callers reach this: the Restoring-phase detector, and the 5-minute
// ceiling when a crash only becomes observable after it. They must agree on
// every step, which is why this is a function and not two copies.
func (r *PodMigrationJobReconciler) concludeRestoreCrash(
	ctx context.Context,
	req ctrl.Request,
	job *pmv1alpha1.PodMigrationJob,
	origJob *pmv1alpha1.PodMigrationJob,
	pod *corev1.Pod,
	verdict restore.Failure,
) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// The snapshot cannot be restored on this pod and retrying is futile — the
	// kubelet will not even restart the container.  Fail the PMJ, then delete
	// the pod so its workload controller recreates it cold (the PMJ is already
	// Consumed, so the successor cannot re-adopt this snapshot).  Fires on the
	// first matched signature: a debounce would only extend the outage.
	metrics.RestoreCrashFallbackTotal.Inc()
	message := fmt.Sprintf("Replacement pod %s failed to restore from snapshot (engine %q, container %q, signature %q): %s",
		pod.Name, verdict.Engine, verdict.Container, verdict.Signature, verdict.Message)
	logger.Info("Restore crash detected; failing migration and deleting replacement pod for cold start",
		"pod", pod.Name, "engine", verdict.Engine, "signature", verdict.Signature)

	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionRestoreCrashed,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonRestoreCrashFallback,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Restored",
		Status:             metav1.ConditionFalse,
		Reason:             ReasonRestoreCrashFallback,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionFalse,
		Reason:             ReasonRestoreCrashFallback,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	// The crash has now been attributed to a signature, so any standing
	// "unrecognised" report is superseded.
	r.clearUnrecognizedRestoreCrash(job)

	// The status write MUST land before the Delete.  If we deleted first and
	// then crashed, the PMJ would stay in its pre-fallback phase with its
	// recorded pod gone and no record of why.
	if err := r.patchStatus(ctx, job, origJob); err != nil {
		return r.handleStatusError(ctx, err, "Failed to update job status on restore crash")
	}

	metrics.MarkPMJInactive(req.NamespacedName.String())
	metrics.RecordOutcome("fallback")
	r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseRestoring)
	r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Restore failed: %s", verdict.Signature)

	r.eventf(job, corev1.EventTypeWarning, ReasonRestoreCrashFallback, "%s", message)

	// Deletion goes through the shared helper so this first attempt and the
	// Failed-phase retry cannot drift apart.  A returned error requeues;
	// because the phase is now Failed, that requeue lands on the completion
	// path, not back here.
	if err := r.ensureCrashedReplacementPodDeleted(ctx, job, pod); err != nil {
		logger.Error(err, "Failed to delete crashed replacement pod", "pod", pod.Name)
		return ctrl.Result{}, err
	}
	r.fallbackEventChecks.Delete(req.NamespacedName.String())
	return ctrl.Result{}, nil
}

// clearUnrecognizedRestoreCrash retracts the UnrecognizedRestoreCrash condition
// once the PMJ reaches a conclusion, because the pod it described can recover:
// an unrecognised start failure is deliberately not acted on, so the container
// may well start on a later attempt and the pod go Ready.  Leaving the
// condition True would then permanently mislabel a successful migration.
//
// Mirrors clearEvictionBlockageConditions: only a condition currently reporting
// True is touched, so jobs that never saw one keep a status free of vacuous
// False entries.  Returns true if the caller must persist the status.
func (r *PodMigrationJobReconciler) clearUnrecognizedRestoreCrash(job *pmv1alpha1.PodMigrationJob) bool {
	existing := meta.FindStatusCondition(job.Status.Conditions, ConditionUnrecognizedRestoreCrash)
	if existing == nil || existing.Status != metav1.ConditionTrue {
		return false
	}
	return meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionUnrecognizedRestoreCrash,
		Status:             metav1.ConditionFalse,
		Reason:             "RestoreCrashResolved",
		Message:            "The unrecognised container start failure did not persist; the migration reached a conclusion",
		ObservedGeneration: job.Generation,
	})
}

// shouldProbeFallbackEvents reports whether the throttle window for this PMJ
// has elapsed, and if so, marks it probed now.
func (r *PodMigrationJobReconciler) shouldProbeFallbackEvents(key string) bool {
	now := time.Now()
	if v, ok := r.fallbackEventChecks.Load(key); ok {
		if last, ok := v.(time.Time); ok && now.Sub(last) < fallbackEventCheckInterval {
			return false
		}
	}
	r.fallbackEventChecks.Store(key, now)
	return true
}

func (r *PodMigrationJobReconciler) getSnapshotProvider() snapshot.Provider {
	if r.SnapshotProvider != nil {
		return r.SnapshotProvider
	}
	return snapshot.NewGKEProvider(r.Client, r.Scheme)
}

// +kubebuilder:rbac:groups=apps,resources=deployments;replicasets;statefulsets,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotmanualtriggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots/status,verbs=get
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattachments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods/eviction,verbs=create
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
// The recorder publishes restore-crash Warning events, which needs write access.
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch
// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch

// Reconcile drives the state machine of the PodMigrationJob.
func (r *PodMigrationJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("job", req.NamespacedName)
	logger.Info("Reconciling PodMigrationJob")

	// 1. Fetch PodMigrationJob
	job := &pmv1alpha1.PodMigrationJob{}
	err := r.Get(ctx, req.NamespacedName, job)
	if err != nil {
		if apierrors.IsNotFound(err) {
			r.fallbackEventChecks.Delete(req.NamespacedName.String())
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PodMigrationJob")
		return ctrl.Result{}, err
	}

	podName := job.Spec.PodRef.Name
	origJob := job.DeepCopy()

	// Set initial phase if empty
	if job.Status.Phase == "" {
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhasePending
		err = r.patchStatus(ctx, job, origJob)
		if err != nil {
			return r.handleStatusError(ctx, err, "Failed to initialize job phase")
		}
		metrics.MarkPMJActive(req.NamespacedName.String())
		return ctrl.Result{Requeue: true}, nil
	}

	// Enforce 10-minute timeout for active migrations (Pending, Snapshotting, and Evicting)
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhasePending ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
		const migrationTimeout = 10 * time.Minute
		if time.Since(job.CreationTimestamp.Time) > migrationTimeout {
			// If the job was observed to be blocked by PDB or eviction misconfiguration during Evicting phase,
			// conclude as SucceededWithoutRestore so the durable snapshot remains for operator recovery.
			if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
				pdbBlockedCond := meta.FindStatusCondition(job.Status.Conditions, "BlockedByPDB")
				isPDBBlocked := pdbBlockedCond != nil && pdbBlockedCond.Status == metav1.ConditionTrue

				misconfigCond := meta.FindStatusCondition(job.Status.Conditions, "EvictionMisconfigured")
				isMisconfigured := misconfigCond != nil && misconfigCond.Status == metav1.ConditionTrue

				if isPDBBlocked || isMisconfigured {
					reason := "PDBEvictionTimeout"
					message := "Origin pod eviction timed out while waiting for PDB budget"
					if isMisconfigured {
						reason = "EvictionMisconfiguredTimeout"
						message = "Origin pod eviction timed out due to eviction configuration error (500 InternalServerError / multiple PDBs)"
					}
					logger.Info("Evicting PMJ timed out due to eviction blockage; concluding as SucceededWithoutRestore", "job", job.Name, "reason", reason)

					// Annotate the origin pod to detect repeat eviction attempts and prevent re-snapshot churn
					pod := &corev1.Pod{}
					if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, pod); err == nil {
						if job.Spec.TargetPodUID == "" || string(pod.UID) == job.Spec.TargetPodUID {
							if pod.Annotations == nil {
								pod.Annotations = make(map[string]string)
							}
							pod.Annotations[util.AnnotationPDBEvictionTimeout] = "true"
							if err := r.Update(ctx, pod); err != nil {
								logger.Error(err, "Failed to annotate origin pod with pdb-eviction-timeout", "pod", podName)
								return ctrl.Result{}, err
							}
						}
					}

					if err := r.markSucceededWithoutRestore(ctx, job, origJob, reason, message); err != nil {
						return r.handleStatusError(ctx, err, "Failed to update job status on eviction timeout")
					}
					return ctrl.Result{}, nil
				}
			}

			logger.Info("Migration job timed out (exceeded 10 minutes limit), transitioning to Failed", "job", job.Name)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
			now := metav1.Now()
			job.Status.CompletionTime = &now

			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Timeout",
				Message:            "Migration job timed out (exceeded 10 minutes limit)",
				ObservedGeneration: job.Generation,
			})

			// Clean up snapshot trigger if it exists (best effort)
			_ = r.getSnapshotProvider().Cleanup(ctx, job, podName)

			prevPhase := job.Status.Phase
			if err := r.patchStatus(ctx, job, origJob); err != nil {
				return r.handleStatusError(ctx, err, "Failed to update job status to Failed on timeout")
			}
			metrics.MarkPMJInactive(req.NamespacedName.String())
			metrics.RecordOutcome("timeout")
			r.recordPreviousPhaseDuration(job, prevPhase)
			r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Migration job timed out (exceeded 10 minutes limit)")
			return ctrl.Result{}, nil
		}
	}

	// A restore-crash fallback writes Phase=Failed before it deletes the
	// crashed pod, so a Delete that failed (throttling is the realistic cause
	// at evacuation scale) can never be retried by the Restoring case below —
	// the phase no longer matches.  Re-drive it here, ahead of GC: leaving the
	// pod running is precisely the permanent wedge this feature exists to
	// break, and the GC block below would otherwise sit on a 30-minute requeue
	// and then delete the PMJ, destroying the only record of which pod is
	// stuck.
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed &&
		meta.IsStatusConditionTrue(job.Status.Conditions, ConditionRestoreCrashed) {
		// nil: no pod in hand here, so the helper does its own live read.
		if err := r.ensureCrashedReplacementPodDeleted(ctx, job, nil); err != nil {
			logger.Error(err, "Failed to complete restore-crash pod deletion; will retry", "pod", job.Status.RestoredPodName)
			return ctrl.Result{}, err
		}
	}

	// Garbage collect completed PMJs after 30 minutes to allow ample time for large queue drainage.
	// Note: Once Issue #19 (field indexers & transactional status.claimedBy) lands, gate lookups
	// will be O(1) and this TTL can be safely reduced to a shorter interval (e.g. 5-10 minutes).
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed {

		// Proactively clean up unassigned completed PMJs if the parent workload has scaled down.
		if (job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore) &&
			!job.Status.Consumed {
			cleanedUp, err := r.cleanupScaleDownZombie(ctx, job)
			if err != nil {
				return ctrl.Result{}, err
			}
			if cleanedUp {
				return ctrl.Result{}, nil
			}
		}

		if job.Status.CompletionTime != nil {
			ttl := time.Minute * 30 // Extended 30m window prevents premature deletion during large drains
			if time.Since(job.Status.CompletionTime.Time) > ttl {
				// A pod still gated on this PMJ has not yet been through gate
				// release: deleting the PMJ now would strand it (its eventual
				// release becomes a cold start with no snapshot ref).  Defer
				// until the PodGate worker has processed the claimant.
				gated, err := r.hasGatedClaimant(ctx, job)
				if err != nil {
					return ctrl.Result{}, err
				}
				if gated {
					logger.Info("GC TTL expired but a gated pod still claims this PMJ; deferring deletion", "job", job.Name)
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
				logger.Info("Garbage collecting completed PodMigrationJob", "job", job.Name)
				err = r.Delete(ctx, job)
				if err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
			requeueIn := ttl - time.Since(job.Status.CompletionTime.Time)
			return ctrl.Result{RequeueAfter: requeueIn}, nil
		}
		return ctrl.Result{}, nil
	}

	switch job.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhasePending:
		// Capture PV Names and origin node name before starting checkpoint (pod is guaranteed to exist)
		if len(job.Status.PVsToDetach) == 0 || job.Status.OriginNodeName == "" {
			originPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, originPod)
			if err != nil {
				if apierrors.IsNotFound(err) {
					logger.Info("Origin pod no longer exists in Pending state, failing migration job")
					job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
					now := metav1.Now()
					job.Status.CompletionTime = &now

					meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
						Type:               "Ready",
						Status:             metav1.ConditionFalse,
						Reason:             "PodNotFound",
						Message:            "Origin pod no longer exists in Pending state",
						ObservedGeneration: job.Generation,
					})

					if err := r.patchStatus(ctx, job, origJob); err != nil {
						return r.handleStatusError(ctx, err, "Failed to update job status to Failed on missing origin pod")
					}
					metrics.MarkPMJInactive(req.NamespacedName.String())
					metrics.RecordPhaseDuration("pending", time.Since(job.CreationTimestamp.Time).Seconds())
					metrics.RecordOutcome("failed")
					r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Origin pod no longer exists in Pending state")
					return ctrl.Result{}, nil
				}
				logger.Error(err, "Failed to get origin pod for PV analysis in Pending state")
				return ctrl.Result{}, err
			}

			// If the pod was replaced with a new instance (UID changed), fail the migration job.
			if string(originPod.UID) != job.Spec.TargetPodUID {
				logger.Info("Origin pod UID mismatch in Pending state, failing migration job", "expectedUID", job.Spec.TargetPodUID, "actualUID", originPod.UID)
				job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
				now := metav1.Now()
				job.Status.CompletionTime = &now

				meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "PodNotFound",
					Message:            "Origin pod UID mismatch in Pending state",
					ObservedGeneration: job.Generation,
				})

				if err := r.patchStatus(ctx, job, origJob); err != nil {
					return r.handleStatusError(ctx, err, "Failed to update job status to Failed on origin pod UID mismatch")
				}
				metrics.MarkPMJInactive(req.NamespacedName.String())
				metrics.RecordPhaseDuration("pending", time.Since(job.CreationTimestamp.Time).Seconds())
				metrics.RecordOutcome("failed")
				r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Origin pod UID mismatch in Pending state")
				return ctrl.Result{}, nil
			}

			if job.Status.OriginNodeName == "" && originPod.Spec.NodeName != "" {
				job.Status.OriginNodeName = originPod.Spec.NodeName
			}

			if len(job.Status.PVsToDetach) == 0 {
				var pvs []string
				for _, vol := range originPod.Spec.Volumes {
					if vol.PersistentVolumeClaim != nil {
						pvc := &corev1.PersistentVolumeClaim{}
						err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: vol.PersistentVolumeClaim.ClaimName}, pvc)
						if err != nil {
							if apierrors.IsNotFound(err) {
								logger.Info("PVC not found, skipping volume", "pvc", vol.PersistentVolumeClaim.ClaimName)
								continue
							}
							logger.Error(err, "Failed to get PVC for volume analysis", "pvc", vol.PersistentVolumeClaim.ClaimName)
							return ctrl.Result{}, err // Return error to trigger manager retry
						}
						if pvc.Spec.VolumeName != "" {
							pvs = append(pvs, pvc.Spec.VolumeName)
						}
					}
				}
				job.Status.PVsToDetach = pvs
			}
		}

		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
		now := metav1.Now()
		job.Status.SnapshottingStartTime = &now
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "Snapshotting",
			Message:            "Taking pod snapshot",
			ObservedGeneration: job.Generation,
		})
		err = r.patchStatus(ctx, job, origJob)
		if err != nil {
			return r.handleStatusError(ctx, err, "Failed to update job status to Snapshotting")
		}
		metrics.RecordPhaseDuration("pending", time.Since(job.CreationTimestamp.Time).Seconds())
		metrics.MarkPMJActive(req.NamespacedName.String())
		r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "MigrationStarted", "Pod migration started by PodMigrationJob %s", job.Name)
		return ctrl.Result{Requeue: true}, nil

	case pmv1alpha1.PodMigrationJobPhaseSnapshotting:
		res, err := r.getSnapshotProvider().EnsureTrigger(ctx, job, podName)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.Requeue || res.RequeueAfter != 0 {
			return res, nil
		}

		// Monitor snapshot readiness via the pluggable SnapshotProvider
		snapStatus, err := r.getSnapshotProvider().CheckStatus(ctx, job, podName)
		if err != nil {
			logger.Error(err, "Failed to check snapshot status")
			return ctrl.Result{}, err
		}

		switch snapStatus.Phase {
		case snapshot.PhaseFailed:
			logger.Info("Snapshot provider reported terminal failure, transitioning to Failed", "reason", snapStatus.Reason, "message", snapStatus.Message)
			_ = r.getSnapshotProvider().Cleanup(ctx, job, podName)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
			now := metav1.Now()
			job.Status.CompletionTime = &now
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             snapStatus.Reason,
				Message:            snapStatus.Message,
				ObservedGeneration: job.Generation,
			})
			err = r.patchStatus(ctx, job, origJob)
			if err != nil {
				return r.handleStatusError(ctx, err, "Failed to update job status to Failed on snapshot failure")
			}
			metrics.MarkPMJInactive(req.NamespacedName.String())
			metrics.RecordOutcome("failed")
			r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
			r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Snapshot failed: %s - %s", snapStatus.Reason, snapStatus.Message)
			return ctrl.Result{}, nil

		case snapshot.PhaseReady:
			logger.Info("GKE PodSnapshot is Ready, transitioning to Evicting phase", "snapshot", snapStatus.SnapshotRef)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
			job.Status.SnapshotRef = snapStatus.SnapshotRef
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Evicting",
				Message:            "Snapshot durable; waiting for origin pod eviction and volume detachment",
				ObservedGeneration: job.Generation,
			})
			err = r.patchStatus(ctx, job, origJob)
			if err != nil {
				return r.handleStatusError(ctx, err, "Failed to update job status to Evicting")
			}
			metrics.MarkPMJActive(req.NamespacedName.String())
			r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
			r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "CheckpointReady", "Snapshot %s is ready for migration", snapStatus.SnapshotRef)
			return ctrl.Result{Requeue: true}, nil

		case snapshot.PhaseInProgress:
			cond := metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             snapStatus.Reason,
				Message:            snapStatus.Message,
				ObservedGeneration: job.Generation,
			}
			condChanged := meta.SetStatusCondition(&job.Status.Conditions, cond)
			refChanged := false
			if snapStatus.SnapshotRef != "" && job.Status.SnapshotRef != snapStatus.SnapshotRef {
				job.Status.SnapshotRef = snapStatus.SnapshotRef
				refChanged = true
			}
			if condChanged || refChanged {
				if err := r.patchStatus(ctx, job, origJob); err != nil {
					return r.handleStatusError(ctx, err, "Failed to update snapshot status in progress")
				}
			}
			return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}

	case pmv1alpha1.PodMigrationJobPhaseEvicting:
		// Proactively clean up the manual trigger once the PMJ is durably Evicting.
		// This frees the target pod lock in the snapshot agent for sequential 2-hop migrations,
		// and runs idempotently without risk of trigger re-creation on Status().Update retry.
		_ = r.getSnapshotProvider().Cleanup(ctx, job, podName)

		// 4.2. Wait for external drain to evict origin Pod, or invoke PDB-safe eviction fallback
		pod := &corev1.Pod{}
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, pod)
		podExists := true
		if err != nil {
			if apierrors.IsNotFound(err) {
				podExists = false
			} else {
				logger.Error(err, "Failed to get origin pod")
				return ctrl.Result{}, err
			}
		}

		if podExists && string(pod.UID) == job.Spec.TargetPodUID {
			if job.Status.OriginNodeName == "" && pod.Spec.NodeName != "" {
				job.Status.OriginNodeName = pod.Spec.NodeName
				if err := r.patchStatus(ctx, job, origJob); err != nil {
					return r.handleStatusError(ctx, err, "Failed to persist OriginNodeName in Evicting phase")
				}
			}

			// If the origin pod is already terminating through its grace period, wait for deletion
			if pod.DeletionTimestamp != nil {
				logger.Info("Origin pod is already terminating; waiting for deletion", "pod", podName)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}

			// Pod still exists and is the target pod.
			// We wait for the eviction webhook to return Allowed and the API server to delete it.
			// Fallback: if it takes longer than 30s (e.g. manual trigger), we invoke the PDB-safe eviction subresource.
			const timeout = 30 * time.Second
			evictingSinceStr := job.Annotations["pod-migration.gke.io/evicting-since"]
			if evictingSinceStr == "" {
				if job.Annotations == nil {
					job.Annotations = make(map[string]string)
				}
				job.Annotations["pod-migration.gke.io/evicting-since"] = time.Now().Format(time.RFC3339Nano)
				logger.Info("Recording evicting start time, waiting for eviction webhook to trigger delete", "pod", podName)
				r.recordPodEvent(ctx, job, corev1.EventTypeNormal, "EvictedForMigration", "Origin pod marked for eviction following successful checkpoint")
				if err := r.Update(ctx, job); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}

			evictingSince, err := time.Parse(time.RFC3339Nano, evictingSinceStr)
			if err != nil {
				evictingSince, err = time.Parse(time.RFC3339, evictingSinceStr)
			}
			if err != nil {
				logger.Error(err, "Failed to parse evicting-since annotation", "val", evictingSinceStr)
				evictingSince = time.Time{} // fallback to immediate eviction
			}

			if time.Since(evictingSince) > timeout {
				logger.Info("Eviction webhook wait timed out (30s), requesting PDB-safe pod eviction (fallback)", "pod", podName)
				eviction := &policyv1.Eviction{
					ObjectMeta: metav1.ObjectMeta{
						Name:      pod.Name,
						Namespace: pod.Namespace,
					},
				}
				err = r.SubResource("eviction").Create(ctx, pod, eviction)
				switch {
				case err == nil || apierrors.IsNotFound(err):
					logger.Info("Successfully initiated PDB-safe pod eviction", "pod", podName)
					condPDB := metav1.Condition{
						Type:               "BlockedByPDB",
						Status:             metav1.ConditionFalse,
						Reason:             "EvictionInitiated",
						Message:            "Origin pod eviction accepted by eviction subresource",
						ObservedGeneration: job.Generation,
					}
					condMisconfig := metav1.Condition{
						Type:               "EvictionMisconfigured",
						Status:             metav1.ConditionFalse,
						Reason:             "EvictionInitiated",
						Message:            "Origin pod eviction accepted by eviction subresource",
						ObservedGeneration: job.Generation,
					}
					updatedPDB := meta.SetStatusCondition(&job.Status.Conditions, condPDB)
					updatedMisconfig := meta.SetStatusCondition(&job.Status.Conditions, condMisconfig)
					if updatedPDB || updatedMisconfig {
						if updateErr := r.patchStatus(ctx, job, origJob); updateErr != nil {
							return r.handleStatusError(ctx, updateErr, "Failed to update status on clearing eviction blockage conditions")
						}
					}
					return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
				case apierrors.IsTooManyRequests(err) || apierrors.IsConflict(err):
					logger.Info("Pod eviction delayed by PodDisruptionBudget (429/409); waiting for budget", "pod", podName)
					cond := metav1.Condition{
						Type:               "BlockedByPDB",
						Status:             metav1.ConditionTrue,
						Reason:             "PDBBudgetExhausted",
						Message:            "Origin pod eviction delayed by PodDisruptionBudget; waiting for budget",
						ObservedGeneration: job.Generation,
					}
					if meta.SetStatusCondition(&job.Status.Conditions, cond) {
						if updateErr := r.patchStatus(ctx, job, origJob); updateErr != nil {
							return r.handleStatusError(ctx, updateErr, "Failed to update status on BlockedByPDB condition")
						}
					}
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				case apierrors.IsInternalError(err):
					logger.Error(err, "Pod eviction failed with 500 InternalServerError (check if pod is covered by multiple conflicting PDBs)", "pod", podName)
					cond := metav1.Condition{
						Type:               "EvictionMisconfigured",
						Status:             metav1.ConditionTrue,
						Reason:             "MultiplePDBsOrInternalError",
						Message:            "Origin pod eviction failed with 500 InternalServerError; check if pod is covered by multiple conflicting PDBs",
						ObservedGeneration: job.Generation,
					}
					if meta.SetStatusCondition(&job.Status.Conditions, cond) {
						if updateErr := r.patchStatus(ctx, job, origJob); updateErr != nil {
							return r.handleStatusError(ctx, updateErr, "Failed to update status on EvictionMisconfigured condition")
						}
					}
					return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
				default:
					logger.Error(err, "Failed to evict origin pod via eviction subresource (fallback)")
					return ctrl.Result{}, err
				}
			} else {
				logger.Info("Waiting for eviction webhook to allow deletion", "pod", podName, "elapsed", time.Since(evictingSince))
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}

		// The origin pod is gone (or was replaced by a pod with a different UID), so any
		// eviction blockage recorded earlier no longer holds. The pod is frequently removed
		// by an external drain's own eviction call rather than by our fallback, in which
		// case the clearing path above never runs and a stale BlockedByPDB /
		// EvictionMisconfigured condition would be reported on a job that went on to
		// succeed. Retract those conditions before advancing.
		if r.clearEvictionBlockageConditions(job) {
			if err := r.patchStatus(ctx, job, origJob); err != nil {
				return r.handleStatusError(ctx, err, "Failed to update status on retracting eviction blockage conditions")
			}
		}

		// Once the origin Pod is gone after eviction, clean up trigger
		if err := r.getSnapshotProvider().Cleanup(ctx, job, podName); err != nil {
			return ctrl.Result{}, err
		}

		// 4.3. Wait for volume detachment from GCE node using VolumeAttachment API
		if len(job.Status.PVsToDetach) > 0 {
			activeAttachment := false
			for _, targetPV := range job.Status.PVsToDetach {
				if targetPV == "" {
					continue
				}
				vaList := &storagev1.VolumeAttachmentList{}
				if err := r.List(ctx, vaList, client.MatchingFields{VolumeAttachmentPVIndex: targetPV}); err != nil {
					logger.Error(err, "Failed to list VolumeAttachments for PV", "pv", targetPV)
					return ctrl.Result{}, err
				}

				for _, va := range vaList.Items {
					// If OriginNodeName is known, only wait for detachment from the origin node.
					// Other nodes (e.g. destination node or existing RWX attachments) must not block migration.
					if job.Status.OriginNodeName != "" && va.Spec.NodeName != job.Status.OriginNodeName {
						continue
					}
					if va.Status.Attached {
						logger.Info("Volume is still attached, waiting...", "pv", targetPV, "volumeAttachment", va.Name, "node", va.Spec.NodeName)
						activeAttachment = true
						break
					}
				}
				if activeAttachment {
					break
				}
			}

			if activeAttachment {
				cond := metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "WaitingForVolumeDetach",
					Message:            fmt.Sprintf("Waiting for PVs to detach: %v", job.Status.PVsToDetach),
					ObservedGeneration: job.Generation,
				}
				if meta.SetStatusCondition(&job.Status.Conditions, cond) {
					if updateErr := r.patchStatus(ctx, job, origJob); updateErr != nil {
						return r.handleStatusError(ctx, updateErr, "Failed to update status on volume detach wait")
					}
				}
				// Requeue in 3 seconds to check again
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}
			logger.Info("All volumes detached successfully")
		}

		// 4.4. Once all PVs are detached, transition the PMJ phase to Restoring.
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		restoringNow := metav1.Now()
		job.Status.RestoringStartTime = &restoringNow
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionFalse,
			Reason:             "RestoringState",
			Message:            "Snapshot durable and PVs detached; waiting for replacement pod restore",
			ObservedGeneration: job.Generation,
		})
		err = r.patchStatus(ctx, job, origJob)
		if err != nil {
			return r.handleStatusError(ctx, err, "Failed to update job status to Restoring")
		}
		metrics.MarkPMJActive(req.NamespacedName.String())
		r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseEvicting)
		logger.Info("PodMigrationJob transitioned to Restoring phase", "pod", podName)
		return ctrl.Result{}, nil

	case pmv1alpha1.PodMigrationJobPhaseRestoring:
		// 1. Ensure RestoringStartTime is initialized
		if job.Status.RestoringStartTime == nil {
			now := metav1.Now()
			job.Status.RestoringStartTime = &now
			if err := r.patchStatus(ctx, job, origJob); err != nil {
				return r.handleStatusError(ctx, err, "Failed to initialize RestoringStartTime")
			}
		}

		// 2. 5-minute safety ceiling measured from RestoringStartTime
		const restoreTimeout = 5 * time.Minute
		if time.Since(job.Status.RestoringStartTime.Time) > restoreTimeout {
			// A crash first OBSERVED after the ceiling is still a crash.  The
			// ceiling sits above the pod fetch, so without this check a
			// Consumed PMJ walks straight into SucceededWithoutRestore having
			// never looked at its pod: success is recorded, nothing is
			// deleted, and the pod stays wedged forever.  A controller
			// restart, a leader handoff, or an evacuation backlog is all it
			// takes to push first observation past five minutes — and an
			// evacuation backlog is the very scenario this feature was filed
			// for.
			//
			// Deliberately no Ready gate here, unlike step 5: this is reached
			// only after five minutes with the migration unconcluded, and the
			// verdict is taken from a live read rather than the cache, so a
			// pod that has since recovered classifies FailureNone and falls
			// through to the normal conclusion below.
			if job.Status.Consumed {
				crashedPod, verdict, err := r.classifyRecordedPodLive(ctx, job)
				if err != nil {
					return ctrl.Result{}, err
				}
				if verdict.Class == restore.FailureFatal {
					logger.Info("Restore crash observed after the 5m ceiling; running the cold-start fallback instead of concluding SucceededWithoutRestore",
						"job", job.Name, "pod", crashedPod.Name)
					return r.concludeRestoreCrash(ctx, req, job, origJob, crashedPod, verdict)
				}
			}

			// The ceiling exists to catch replacement pods that started but never
			// consumed the snapshot.  A pod still held by its scheduling gate has
			// not started at all — it is waiting on the single PodGate worker,
			// whose queue can exceed 5 minutes during large drains.  Flipping to
			// SucceededWithoutRestore here would silently discard a durable
			// snapshot, so defer while a gated claimant exists.
			if !job.Status.Consumed {
				gated, err := r.hasGatedClaimant(ctx, job)
				if err != nil {
					return ctrl.Result{}, err
				}
				if gated {
					logger.Info("Restore timeout reached but replacement pod is still gated awaiting release; deferring",
						"job", job.Name)
					// The deferral is unbounded by design (a gated pod cannot have
					// started), so it must be visible: operators can alert on this
					// condition if a wedged PodGate worker holds PMJs here.
					if meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
						Type:               "Ready",
						Status:             metav1.ConditionFalse,
						Reason:             "WaitingOnGateRelease",
						Message:            "Restore ceiling reached but replacement pod is still gated awaiting the PodGate worker",
						ObservedGeneration: job.Generation,
					}) {
						if updateErr := r.patchStatus(ctx, job, origJob); updateErr != nil {
							return r.handleStatusError(ctx, updateErr, "Failed to update status on WaitingOnGateRelease condition")
						}
					}
					return ctrl.Result{RequeueAfter: 15 * time.Second}, nil
				}
			}
			logger.Info("Restore timeout reached (5m), marking SucceededWithoutRestore", "job", job.Name, "pod", podName)
			if err := r.markSucceededWithoutRestore(ctx, job, origJob, "RestoreTimeout", "Restore timed out after 5 minutes"); err != nil {
				return r.handleStatusError(ctx, err, "Failed to update job status to SucceededWithoutRestore on restore timeout")
			}
			return ctrl.Result{}, nil
		}

		// 2. Wait until replacement pod has claimed the PMJ at the gate
		if !job.Status.Consumed || job.Status.RestoredPodUID == "" || job.Status.RestoredPodName == "" {
			logger.Info("PMJ is Restoring but not yet consumed by a replacement pod; waiting...", "job", job.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 3. Fetch replacement pod by Name
		replacementPod := &corev1.Pod{}
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: job.Status.RestoredPodName}, replacementPod)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Replacement pod not found yet; waiting...", "pod", job.Status.RestoredPodName)
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
			logger.Error(err, "Failed to get replacement pod in Restoring phase")
			return ctrl.Result{}, err
		}

		// 4. Strict UID Assertion: Ensure we are evaluating the exact pod instance that adopted the snapshot
		if string(replacementPod.UID) != job.Status.RestoredPodUID {
			logger.Info("Pod with name exists but UID mismatch (replacement pod was recreated)",
				"expectedUID", job.Status.RestoredPodUID, "actualUID", replacementPod.UID)

			const (
				mismatchGracePeriod = 30 * time.Second
				clockSkewTolerance  = 10 * time.Second
			)
			mismatchSinceStr := job.Annotations[util.AnnotationMismatchSince]
			if mismatchSinceStr == "" {
				if job.Annotations == nil {
					job.Annotations = make(map[string]string)
				}
				job.Annotations[util.AnnotationMismatchSince] = time.Now().Format(time.RFC3339)
				if err := r.Update(ctx, job); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}

			mismatchSince, err := time.Parse(time.RFC3339, mismatchSinceStr)
			if err != nil || time.Since(mismatchSince) > (mismatchGracePeriod+clockSkewTolerance) {
				logger.Info("Replacement pod UID mismatch persisted > 30s (+ skew tolerance); fast-failing to SucceededWithoutRestore", "job", job.Name)
				job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
				now := metav1.Now()
				job.Status.CompletionTime = &now
				meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
					Type:               "Restored",
					Status:             metav1.ConditionFalse,
					Reason:             "ReplacementPodMismatch",
					Message:            "Replacement pod was deleted and recreated (UID mismatch persisted > 30s)",
					ObservedGeneration: job.Generation,
				})
				meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionTrue,
					Reason:             "ReplacementPodMismatch",
					Message:            "Workload recreated with new instance; migration tracking concluded",
					ObservedGeneration: job.Generation,
				})
				if err := r.patchStatus(ctx, job, origJob); err != nil {
					return r.handleStatusError(ctx, err, "Failed to update status on replacement pod mismatch")
				}
				metrics.MarkPMJInactive(req.NamespacedName.String())
				metrics.RecordOutcome("failed")
				r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseRestoring)
				r.recordPodEvent(ctx, job, corev1.EventTypeWarning, "MigrationFailed", "Workload was recreated with replacement pod UID %s before restore completed", replacementPod.UID)
				return ctrl.Result{}, nil
			}

			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 4a. Self-healing mismatch-since cleanup: if a previous UID mismatch set
		// AnnotationMismatchSince, clear it now that the replacement pod matches the recorded consumer UID.
		if job.Annotations != nil && job.Annotations[util.AnnotationMismatchSince] != "" {
			delete(job.Annotations, util.AnnotationMismatchSince)
			if err := r.Update(ctx, job); err != nil {
				logger.Error(err, "Failed to clear mismatch-since annotation on PMJ")
				return ctrl.Result{}, err
			}
		}

		// 4b. Self-healing GateReleased: if the consumer pod has been un-gated
		// (scheduling gate removed) but GateReleased was not recorded on PMJ status
		// (e.g. due to a transient Conflict during PodGate reconciliation), stamp GateReleased=true.
		if !job.Status.GateReleased && !podHasMigrationGate(replacementPod) {
			logger.Info("Self-healing GateReleased on PMJ status for un-gated replacement pod",
				"job", job.Name, "pod", replacementPod.Name)
			job.Status.GateReleased = true
			if err := r.patchStatus(ctx, job, origJob); err != nil {
				return r.handleStatusError(ctx, err, "Failed to self-heal GateReleased on PMJ status")
			}
		}

		// 5. Restore-crash detection, for a pod that is NOT yet Ready.
		//
		// The Ready gate is load-bearing twice over.  A Ready pod is running,
		// so by definition its containers started and no verdict here could be
		// about the restore; letting it through means a stale start failure
		// anywhere in its history can either delete it (Fatal) or pin it in the
		// 2s Unrecognized requeue loop forever (Unrecognized), since nothing
		// below step 7 ever runs for it.  A Ready pod instead falls through to
		// step 7, which is the path that can actually conclude it.
		//
		// This MUST also stay below the strict UID assertion above: the matched
		// branch DELETES this pod, so it must be impossible to reach with any
		// pod other than the exact instance this PMJ restored.  The self-healing
		// steps 4a/4b in between are safe to interleave — they only touch PMJ
		// status and annotations, never pod identity — but nothing that
		// reassigns replacementPod may be added between the assertion and here.
		if !isPodReady(replacementPod) {
			switch verdict := restore.Classify(replacementPod, r.restoreEngines()...); verdict.Class {
			case restore.FailureFatal:
				// The cached verdict only nominates a candidate.  Confirm it
				// against the API server before acting: the informer cache can
				// trail a pod that has already recovered, and the remedy here
				// is destroying it.
				livePod, liveVerdict, err := r.classifyRecordedPodLive(ctx, job)
				if err != nil {
					return ctrl.Result{}, err
				}
				if liveVerdict.Class != restore.FailureFatal {
					logger.Info("Cached restore-crash verdict not confirmed by a live read; taking no action",
						"pod", job.Status.RestoredPodName, "cachedSignature", verdict.Signature)
					return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
				}
				return r.concludeRestoreCrash(ctx, req, job, origJob, livePod, liveVerdict)

			case restore.FailureUnrecognized:
				// A start failure we cannot attribute to a restore.  Taking a
				// destructive action on a signal we do not understand is strictly
				// worse than waiting, so we only report it.  This path is bounded:
				// the 5-minute restore ceiling above concludes the PMJ, so an
				// unrecognised crash cannot leak a PMJ stuck in Restoring forever.
				message := fmt.Sprintf("Replacement pod %s hit a container start failure with no recognised restore-failure signature (container %q): %s",
					replacementPod.Name, verdict.Container, verdict.Message)
				// The condition transition is the once-only guard: without it every
				// 2s requeue would emit another identical event.
				if meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
					Type:               ConditionUnrecognizedRestoreCrash,
					Status:             metav1.ConditionTrue,
					Reason:             ReasonUnrecognizedRestoreCrash,
					Message:            message,
					ObservedGeneration: job.Generation,
				}) {
					if err := r.patchStatus(ctx, job, origJob); err != nil {
						return r.handleStatusError(ctx, err, "Failed to record unrecognised restore crash")
					}
					logger.Info("Unrecognised StartError on restoring pod; taking no action",
						"pod", replacementPod.Name, "message", verdict.Message)
					r.eventf(job, corev1.EventTypeWarning, ReasonUnrecognizedRestoreCrash, "%s", message)
					metrics.RestoreCrashUnmatchedTotal.Inc()
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}

			// 6. Not Ready and not crashed: probe for a cold-start fallback
			// event on a throttle.  The query goes straight to the API server,
			// so per-2s-tick probing saturates the QPS budget at 50 workers,
			// but no probing at all leaves a crashlooping fallback pod
			// undetected until the 5-minute ceiling.
			if r.shouldProbeFallbackEvents(req.NamespacedName.String()) {
				hasFallback, err := r.hasColdStartFallbackEvent(ctx, job, replacementPod)
				if err != nil {
					logger.Error(err, "Failed to query events for replacement pod")
					return ctrl.Result{}, err
				}
				if hasFallback {
					logger.Info("GKE runtime skipped snapshot restore and fell back to cold start (pod not yet Ready)", "pod", replacementPod.Name)
					r.fallbackEventChecks.Delete(req.NamespacedName.String())
					return ctrl.Result{}, r.markSucceededWithoutRestore(ctx, job, origJob,
						"FallbackToColdStart", "GKE runtime skipped snapshot restore and fell back to cold start")
				}
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 7. Pod is Ready: a decisive fallback-event check picks between
		// verified restore and cold-start fallback.
		hasFallback, err := r.hasColdStartFallbackEvent(ctx, job, replacementPod)
		if err != nil {
			logger.Error(err, "Failed to query events for replacement pod")
			return ctrl.Result{}, err
		}
		r.fallbackEventChecks.Delete(req.NamespacedName.String())
		if hasFallback {
			logger.Info("GKE runtime skipped snapshot restore and fell back to cold start", "pod", replacementPod.Name)
			return ctrl.Result{}, r.markSucceededWithoutRestore(ctx, job, origJob,
				"FallbackToColdStart", "GKE runtime skipped snapshot restore and fell back to cold start")
		}

		// 8. Verified restore: the pod is Ready with no fallback event.
		logger.Info("Pod state restored from snapshot and replacement pod is Ready", "pod", replacementPod.Name)
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
		now := metav1.Now()
		job.Status.CompletionTime = &now
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               "Restored",
			Status:             metav1.ConditionTrue,
			Reason:             "RestoreVerified",
			Message:            "Pod state restored from snapshot and replacement pod is Ready",
			ObservedGeneration: job.Generation,
		})
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "RestoreVerified",
			Message:            "Pod state restored from snapshot and replacement pod is Ready",
			ObservedGeneration: job.Generation,
		})
		// The pod recovered and came up Ready, so any earlier unrecognised
		// start failure was transient after all.
		r.clearUnrecognizedRestoreCrash(job)
		if err := r.patchStatus(ctx, job, origJob); err != nil {
			return r.handleStatusError(ctx, err, "Failed to update job status to Succeeded")
		}
		metrics.MarkPMJInactive(req.NamespacedName.String())
		metrics.RecordOutcome("succeeded")
		r.recordPreviousPhaseDuration(job, pmv1alpha1.PodMigrationJobPhaseRestoring)
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// markSucceededWithoutRestore concludes the PMJ with the given reason on both
// the Restored (False) and Ready (True) conditions, updates Prometheus metrics
// (marking PMJ inactive, recording outcome, and recording previous phase duration).
func (r *PodMigrationJobReconciler) markSucceededWithoutRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, orig *pmv1alpha1.PodMigrationJob, reason, message string) error {
	prevPhase := job.Status.Phase
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
	now := metav1.Now()
	job.Status.CompletionTime = &now
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Restored",
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	// The job is concluding, so a standing "unrecognised start failure" report
	// no longer describes anything actionable.
	r.clearUnrecognizedRestoreCrash(job)
	if err := r.patchStatus(ctx, job, orig); err != nil {
		return err
	}
	metrics.MarkPMJInactive(types.NamespacedName{Namespace: job.Namespace, Name: job.Name}.String())
	outcome := metrics.OutcomeFor(pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, reason)
	metrics.RecordOutcome(outcome)
	r.recordPreviousPhaseDuration(job, prevPhase)
	return nil
}

// clearEvictionBlockageConditions retracts the BlockedByPDB and EvictionMisconfigured
// conditions once the origin pod is no longer present, since a blockage that was
// observed while the pod existed cannot still apply to a pod that is gone.
//
// Only conditions currently reporting True are touched, so a job that was never
// blocked keeps a status free of vacuous False conditions and no needless status
// write is issued.  Returns true if any condition was changed, in which case the
// caller is responsible for persisting the status.
func (r *PodMigrationJobReconciler) clearEvictionBlockageConditions(job *pmv1alpha1.PodMigrationJob) bool {
	changed := false
	for _, condType := range []string{"BlockedByPDB", "EvictionMisconfigured"} {
		existing := meta.FindStatusCondition(job.Status.Conditions, condType)
		if existing == nil || existing.Status != metav1.ConditionTrue {
			continue
		}
		if meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               condType,
			Status:             metav1.ConditionFalse,
			Reason:             "OriginPodRemoved",
			Message:            "Origin pod is no longer present; the eviction blockage no longer applies",
			ObservedGeneration: job.Generation,
		}) {
			changed = true
		}
	}
	return changed
}

// hasGatedClaimant reports whether any pod annotated as assigned to this PMJ
// is still held by the migration scheduling gate.  Such a pod has not been
// processed by the (single-worker) PodGate reconciler yet, so both the
// restore-timeout ceiling and GC must wait for it.  Uses the assigned-pmj
// cache index; this is a cache read, not an API call.
func (r *PodMigrationJobReconciler) hasGatedClaimant(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	pods := &corev1.PodList{}
	err := r.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingFields{PodAssignedPMJIndex: job.Name})
	if err != nil {
		if strings.Contains(err.Error(), "index") || strings.Contains(err.Error(), "indexer") {
			if listErr := r.List(ctx, pods, client.InNamespace(job.Namespace)); listErr != nil {
				return false, listErr
			}
			for i := range pods.Items {
				if pods.Items[i].Annotations != nil && pods.Items[i].Annotations[util.AnnotationAssignedPMJ] == job.Name {
					if podHasMigrationGate(&pods.Items[i]) {
						return true, nil
					}
				}
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to list pods assigned to PMJ %s: %w", job.Name, err)
	}
	for i := range pods.Items {
		if podHasMigrationGate(&pods.Items[i]) {
			return true, nil
		}
	}
	return false, nil
}

// hasAssignedClaimant reports whether any pod is annotated as assigned to this PMJ.
func (r *PodMigrationJobReconciler) hasAssignedClaimant(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	pods := &corev1.PodList{}
	err := r.List(ctx, pods,
		client.InNamespace(job.Namespace),
		client.MatchingFields{PodAssignedPMJIndex: job.Name})
	if err != nil {
		if strings.Contains(err.Error(), "index") || strings.Contains(err.Error(), "indexer") {
			if listErr := r.List(ctx, pods, client.InNamespace(job.Namespace)); listErr != nil {
				return false, listErr
			}
			for i := range pods.Items {
				if pods.Items[i].Annotations != nil && pods.Items[i].Annotations[util.AnnotationAssignedPMJ] == job.Name {
					return true, nil
				}
			}
			return false, nil
		}
		return false, fmt.Errorf("failed to list pods assigned to PMJ %s: %w", job.Name, err)
	}
	return len(pods.Items) > 0, nil
}

// countActiveWorkloadPods counts non-terminating, non-failed/succeeded pods for a workload,
// explicitly excluding the origin pod that was evicted/migrated by this PMJ.
func (r *PodMigrationJobReconciler) countActiveWorkloadPods(pods []corev1.Pod, job *pmv1alpha1.PodMigrationJob) int32 {
	active := int32(0)
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil ||
			p.Status.Phase == corev1.PodFailed ||
			p.Status.Phase == corev1.PodSucceeded {
			continue
		}
		// Exclude the origin pod of this PMJ if still lingering in cache
		if job.Spec.TargetPodUID != "" && string(p.UID) == job.Spec.TargetPodUID {
			continue
		}
		if job.Spec.PodRef.Name != "" && p.Name == job.Spec.PodRef.Name {
			continue
		}
		active++
	}
	return active
}

// cleanupScaleDownZombie proactively deletes unassigned completed PMJs (Succeeded or
// SucceededWithoutRestore) whose parent workload has scaled down and already has all
// target replicas satisfied by other active pods.
func (r *PodMigrationJobReconciler) cleanupScaleDownZombie(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (bool, error) {
	if job.Status.Consumed {
		return false, nil
	}

	parentName := job.Labels[util.LabelParentName]
	parentKind := job.Labels[util.LabelParentKind]
	if parentName == "" || parentKind == "" {
		return false, nil
	}

	assigned, err := r.hasAssignedClaimant(ctx, job)
	if err != nil {
		return false, err
	}
	if assigned {
		return false, nil
	}

	var targetReplicas int32
	var selector *metav1.LabelSelector

	switch parentKind {
	case "Deployment":
		deploy := &appsv1.Deployment{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: parentName}, deploy); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		targetReplicas = 1
		if deploy.Spec.Replicas != nil {
			targetReplicas = *deploy.Spec.Replicas
		}
		selector = deploy.Spec.Selector

	case "ReplicaSet":
		rs := &appsv1.ReplicaSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: parentName}, rs); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		targetReplicas = 1
		if rs.Spec.Replicas != nil {
			targetReplicas = *rs.Spec.Replicas
		}
		selector = rs.Spec.Selector

	case "StatefulSet":
		sts := &appsv1.StatefulSet{}
		if err := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: parentName}, sts); err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		targetReplicas = 1
		if sts.Spec.Replicas != nil {
			targetReplicas = *sts.Spec.Replicas
		}
		selector = sts.Spec.Selector

	default:
		return false, nil
	}

	if selector == nil {
		return false, nil
	}

	sel, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil {
		return false, err
	}

	podList := &corev1.PodList{}
	if err := r.List(ctx, podList, client.InNamespace(job.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return false, err
	}

	activeCount := r.countActiveWorkloadPods(podList.Items, job)

	// If the parent workload's active pod count satisfies or exceeds its target replicas,
	// no replacement pod will be created for scaled-down replicas.
	if activeCount >= targetReplicas {
		logger := log.FromContext(ctx)
		logger.Info("Proactively deleting unassigned completed PodMigrationJob after parent workload scale-down",
			"job", job.Name,
			"parentKind", parentKind,
			"parentName", parentName,
			"activePods", activeCount,
			"targetReplicas", targetReplicas)

		r.Recorder.Event(job, corev1.EventTypeNormal, "ScaleDownZombieCleaned",
			fmt.Sprintf("Proactively deleted unassigned completed PMJ for scaled-down %s %s (active pods: %d, target replicas: %d)",
				parentKind, parentName, activeCount, targetReplicas))

		if err := r.Delete(ctx, job); err != nil && !apierrors.IsNotFound(err) {
			return false, err
		}
		return true, nil
	}

	return false, nil
}

func deploymentScaleDownPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldDep, ok1 := e.ObjectOld.(*appsv1.Deployment)
			newDep, ok2 := e.ObjectNew.(*appsv1.Deployment)
			if !ok1 || !ok2 || oldDep == nil || newDep == nil {
				return false
			}
			oldReplicas := int32(1)
			if oldDep.Spec.Replicas != nil {
				oldReplicas = *oldDep.Spec.Replicas
			}
			newReplicas := int32(1)
			if newDep.Spec.Replicas != nil {
				newReplicas = *newDep.Spec.Replicas
			}
			return newReplicas < oldReplicas
		},
		CreateFunc:  func(e event.CreateEvent) bool { return false },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func replicaSetScaleDownPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldRS, ok1 := e.ObjectOld.(*appsv1.ReplicaSet)
			newRS, ok2 := e.ObjectNew.(*appsv1.ReplicaSet)
			if !ok1 || !ok2 || oldRS == nil || newRS == nil {
				return false
			}
			oldReplicas := int32(1)
			if oldRS.Spec.Replicas != nil {
				oldReplicas = *oldRS.Spec.Replicas
			}
			newReplicas := int32(1)
			if newRS.Spec.Replicas != nil {
				newReplicas = *newRS.Spec.Replicas
			}
			return newReplicas < oldReplicas
		},
		CreateFunc:  func(e event.CreateEvent) bool { return false },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func statefulSetScaleDownPredicate() predicate.Predicate {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldSTS, ok1 := e.ObjectOld.(*appsv1.StatefulSet)
			newSTS, ok2 := e.ObjectNew.(*appsv1.StatefulSet)
			if !ok1 || !ok2 || oldSTS == nil || newSTS == nil {
				return false
			}
			oldReplicas := int32(1)
			if oldSTS.Spec.Replicas != nil {
				oldReplicas = *oldSTS.Spec.Replicas
			}
			newReplicas := int32(1)
			if newSTS.Spec.Replicas != nil {
				newReplicas = *newSTS.Spec.Replicas
			}
			return newReplicas < oldReplicas
		},
		CreateFunc:  func(e event.CreateEvent) bool { return false },
		DeleteFunc:  func(e event.DeleteEvent) bool { return false },
		GenericFunc: func(e event.GenericEvent) bool { return false },
	}
}

func (r *PodMigrationJobReconciler) mapParentToPMJs(ctx context.Context, namespace, parentName, parentKind string) []ctrl.Request {
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, pmjList,
		client.InNamespace(namespace),
		client.MatchingLabels{
			util.LabelParentName: parentName,
			util.LabelParentKind: parentKind,
		}); err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for i := range pmjList.Items {
		job := &pmjList.Items[i]
		if (job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
			job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore) &&
			!job.Status.Consumed {
			reqs = append(reqs, ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: job.Namespace,
					Name:      job.Name,
				},
			})
		}
	}
	return reqs
}

func (r *PodMigrationJobReconciler) mapDeploymentToPMJs(ctx context.Context, obj client.Object) []ctrl.Request {
	if obj == nil {
		return nil
	}
	return r.mapParentToPMJs(ctx, obj.GetNamespace(), obj.GetName(), "Deployment")
}

func (r *PodMigrationJobReconciler) mapReplicaSetToPMJs(ctx context.Context, obj client.Object) []ctrl.Request {
	rs, ok := obj.(*appsv1.ReplicaSet)
	if !ok || rs == nil {
		return nil
	}
	for _, ref := range rs.OwnerReferences {
		if ref.Kind == "Deployment" {
			return r.mapParentToPMJs(ctx, obj.GetNamespace(), ref.Name, "Deployment")
		}
	}
	return r.mapParentToPMJs(ctx, obj.GetNamespace(), rs.Name, "ReplicaSet")
}

func (r *PodMigrationJobReconciler) mapStatefulSetToPMJs(ctx context.Context, obj client.Object) []ctrl.Request {
	if obj == nil {
		return nil
	}
	return r.mapParentToPMJs(ctx, obj.GetNamespace(), obj.GetName(), "StatefulSet")
}

func isPodReady(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *PodMigrationJobReconciler) hasColdStartFallbackEvent(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, nil
	}

	reader := r.liveReader()

	eventList := &corev1.EventList{}
	listOpts := []client.ListOption{
		client.InNamespace(pod.Namespace),
		client.MatchingFields{"involvedObject.uid": string(pod.UID)},
	}
	if err := reader.List(ctx, eventList, listOpts...); err != nil {
		return false, fmt.Errorf("failed to list events for replacement pod %s: %w", pod.Name, err)
	}

	for _, event := range eventList.Items {
		if event.InvolvedObject.UID != pod.UID {
			continue
		}

		// Ignore events timestamped before this migration's Restoring phase started
		if job != nil && job.Status.RestoringStartTime != nil && !event.LastTimestamp.IsZero() {
			if event.LastTimestamp.Before(job.Status.RestoringStartTime) {
				continue
			}
		}

		// Only Warning events indicate an abnormal restore fallback
		if event.Type == corev1.EventTypeWarning {
			msg := strings.ToLower(event.Message)
			reason := event.Reason
			if reason == "FallbackToColdStart" ||
				reason == "FailedRestore" ||
				reason == "RestoreFailed" ||
				reason == "SkipRestore" ||
				reason == "GKEPodSnapshotting" ||
				strings.Contains(msg, "falling back to a cold start") ||
				strings.Contains(msg, "failed to restore") ||
				strings.Contains(msg, "skipped restore") {
				return true, nil
			}
		}
	}
	return false, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodMigrationJobReconciler) SetupWithManager(mgr ctrl.Manager, options controller.Options) error {
	logger := mgr.GetLogger().WithName("podmigrationjob-setup")
	bldr := ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigrationJob{}).
		WithOptions(options).
		Watches(
			&appsv1.Deployment{},
			handler.EnqueueRequestsFromMapFunc(r.mapDeploymentToPMJs),
			builder.WithPredicates(deploymentScaleDownPredicate()),
		).
		Watches(
			&appsv1.ReplicaSet{},
			handler.EnqueueRequestsFromMapFunc(r.mapReplicaSetToPMJs),
			builder.WithPredicates(replicaSetScaleDownPredicate()),
		).
		Watches(
			&appsv1.StatefulSet{},
			handler.EnqueueRequestsFromMapFunc(r.mapStatefulSetToPMJs),
			builder.WithPredicates(statefulSetScaleDownPredicate()),
		)

	psmtGVK := schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	}
	snapGVK := schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshot",
	}

	restMapper := mgr.GetRESTMapper()

	// Check if PodSnapshotManualTrigger CRD is installed before watching
	watchPSMT, err := shouldWatchCRD(restMapper, psmtGVK)
	if err != nil {
		return err
	}
	if !watchPSMT {
		logger.Info("PodSnapshotManualTrigger CRD not registered; skipping watch and falling back to polling")
	} else {
		psmt := &unstructured.Unstructured{}
		psmt.SetGroupVersionKind(psmtGVK)
		bldr = bldr.Watches(psmt, handler.EnqueueRequestForOwner(mgr.GetScheme(), restMapper, &pmv1alpha1.PodMigrationJob{}))
	}

	// Check if PodSnapshot CRD is installed before watching
	watchSnap, err := shouldWatchCRD(restMapper, snapGVK)
	if err != nil {
		return err
	}
	if !watchSnap {
		logger.Info("PodSnapshot CRD not registered; skipping watch and falling back to polling")
	} else {
		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(snapGVK)
		bldr = bldr.Watches(snap, handler.EnqueueRequestsFromMapFunc(r.mapSnapshotToPMJ))
	}

	return bldr.Complete(r)
}

// shouldWatchCRD checks whether a CRD kind is registered in the RESTMapper.
// If registered, it returns (true, nil).
// If missing (meta.IsNoMatchError), it returns (false, nil) to allow safe fallback to polling.
// Any other error (e.g. transient apiserver discovery failure) is returned so setup fails and restarts.
func shouldWatchCRD(restMapper meta.RESTMapper, gvk schema.GroupVersionKind) (bool, error) {
	if _, err := restMapper.RESTMapping(gvk.GroupKind(), gvk.Version); err != nil {
		if meta.IsNoMatchError(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// mapSnapshotToPMJ maps changes on PodSnapshot CRDs to their corresponding PodMigrationJobs via the snapshotRef index.
func (r *PodMigrationJobReconciler) mapSnapshotToPMJ(ctx context.Context, obj client.Object) []ctrl.Request {
	snapName := obj.GetName()
	namespace := obj.GetNamespace()
	if snapName == "" || namespace == "" {
		return nil
	}
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := r.List(ctx, pmjList, client.InNamespace(namespace), client.MatchingFields{PMJSnapshotRefIndex: snapName}); err != nil {
		return nil
	}
	var reqs []ctrl.Request
	for _, job := range pmjList.Items {
		reqs = append(reqs, ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: job.Namespace,
				Name:      job.Name,
			},
		})
	}
	return reqs
}

// patchStatus updates the PodMigrationJob status using a MergeFrom patch,
// falling back to Update if orig is nil.
func (r *PodMigrationJobReconciler) patchStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, orig *pmv1alpha1.PodMigrationJob) error {
	if orig != nil {
		patch := client.MergeFrom(orig)
		return r.Status().Patch(ctx, job, patch)
	}
	return r.Status().Update(ctx, job)
}

// handleStatusError logs an error or gracefully requeues on optimistic lock conflicts.
// Note: client.MergeFrom patches do not assert resourceVersion unless optimistic locking is configured;
// this handler primarily guards fallback Update paths and custom interceptors.
func (r *PodMigrationJobReconciler) handleStatusError(ctx context.Context, err error, msg string) (ctrl.Result, error) {
	if apierrors.IsConflict(err) {
		log.FromContext(ctx).Info("Optimistic lock conflict updating status; requeuing silently", "error", err.Error())
		return ctrl.Result{Requeue: true}, nil
	}
	log.FromContext(ctx).Error(err, msg)
	return ctrl.Result{}, err
}
