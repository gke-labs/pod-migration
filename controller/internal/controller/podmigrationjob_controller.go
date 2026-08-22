package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// PodMigrationJobReconciler reconciles a PodMigrationJob object.
type PodMigrationJobReconciler struct {
	client.Client
	APIReader        client.Reader
	Scheme           *runtime.Scheme
	SnapshotProvider snapshot.Provider
}

func (r *PodMigrationJobReconciler) getSnapshotProvider() snapshot.Provider {
	if r.SnapshotProvider != nil {
		return r.SnapshotProvider
	}
	return snapshot.NewGKEProvider(r.Client, r.Scheme)
}

// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotmanualtriggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots/status,verbs=get
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattachments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch
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
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to get PodMigrationJob")
		return ctrl.Result{}, err
	}

	podName := job.Spec.PodRef.Name

	// Set initial phase if empty
	if job.Status.Phase == "" {
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhasePending
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to initialize job phase")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	// Enforce 10-minute timeout for active migrations (Pending, Snapshotting, and Evicting)
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhasePending ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
		const migrationTimeout = 10 * time.Minute
		if time.Since(job.CreationTimestamp.Time) > migrationTimeout {
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

			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to update job status to Failed on timeout")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Garbage collect completed PMJs after 5 minutes
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
		job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed {

		if job.Status.CompletionTime != nil {
			const gcDelay = 5 * time.Minute
			if time.Since(job.Status.CompletionTime.Time) > gcDelay {
				logger.Info("Garbage collecting completed PodMigrationJob", "job", job.Name)
				err := r.Delete(ctx, job)
				if err != nil && !apierrors.IsNotFound(err) {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}
			requeueIn := gcDelay - time.Since(job.Status.CompletionTime.Time)
			return ctrl.Result{RequeueAfter: requeueIn}, nil
		}
		return ctrl.Result{}, nil
	}

	switch job.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhasePending:
		// Capture PV Names before starting checkpoint (pod is guaranteed to exist)
		if len(job.Status.PVsToDetach) == 0 {
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

					if err := r.Status().Update(ctx, job); err != nil {
						logger.Error(err, "Failed to update job status to Failed on missing origin pod")
						return ctrl.Result{}, err
					}
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

				if err := r.Status().Update(ctx, job); err != nil {
					logger.Error(err, "Failed to update job status to Failed on origin pod UID mismatch")
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}

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

		res, err := r.getSnapshotProvider().EnsureTrigger(ctx, job, podName)
		if err != nil {
			return ctrl.Result{}, err
		}
		if res.Requeue || res.RequeueAfter != 0 {
			return res, nil
		}

		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to update job status to Snapshotting")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	case pmv1alpha1.PodMigrationJobPhaseSnapshotting:
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
			err = r.Status().Update(ctx, job)
			if err != nil {
				logger.Error(err, "Failed to update job status to Failed on snapshot failure")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil

		case snapshot.PhaseReady:
			logger.Info("GKE PodSnapshot is Ready, transitioning to Evicting phase", "snapshot", snapStatus.SnapshotRef)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
			job.Status.SnapshotRef = snapStatus.SnapshotRef
			err = r.Status().Update(ctx, job)
			if err != nil {
				logger.Error(err, "Failed to update job status to Evicting")
				return ctrl.Result{}, err
			}
			return ctrl.Result{Requeue: true}, nil

		case snapshot.PhaseInProgress:
			cond := metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             snapStatus.Reason,
				Message:            snapStatus.Message,
				ObservedGeneration: job.Generation,
			}
			if meta.SetStatusCondition(&job.Status.Conditions, cond) {
				_ = r.Status().Update(ctx, job)
			}
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}

	case pmv1alpha1.PodMigrationJobPhaseEvicting:

		// 4.2. Wait for Webhook to delete Pod, or delete it ourselves if it takes too long
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
			// Pod still exists and is the target pod.
			// We wait for the eviction webhook to return Allowed and the API server to delete it.
			// Fallback: if it takes longer than 30s (e.g. manual trigger), we delete it manually.
			const timeout = 30 * time.Second
			evictingSinceStr := job.Annotations["pod-migration.gke.io/evicting-since"]
			if evictingSinceStr == "" {
				if job.Annotations == nil {
					job.Annotations = make(map[string]string)
				}
				job.Annotations["pod-migration.gke.io/evicting-since"] = time.Now().Format(time.RFC3339)
				logger.Info("Recording evicting start time, waiting for eviction webhook to trigger delete", "pod", podName)
				if err := r.Update(ctx, job); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{Requeue: true}, nil
			}

			evictingSince, err := time.Parse(time.RFC3339, evictingSinceStr)
			if err != nil {
				logger.Error(err, "Failed to parse evicting-since annotation", "val", evictingSinceStr)
				evictingSince = time.Time{} // fallback to immediate delete
			}

			if time.Since(evictingSince) > timeout {
				logger.Info("Eviction webhook wait timed out (30s), deleting origin pod manually (fallback)", "pod", podName)
				err = r.Delete(ctx, pod)
				if err != nil && !apierrors.IsNotFound(err) {
					logger.Error(err, "Failed to delete origin pod (fallback)")
					return ctrl.Result{}, err
				}
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			} else {
				logger.Info("Waiting for eviction webhook to allow deletion", "pod", podName, "elapsed", time.Since(evictingSince))
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}

		// Once the Pod is gone (or we fallback-deleted it), clean up trigger
		if err := r.getSnapshotProvider().Cleanup(ctx, job, podName); err != nil {
			return ctrl.Result{}, err
		}

		// 4.3. Wait for volume detachment from GCE node using VolumeAttachment API
		if len(job.Status.PVsToDetach) > 0 {
			vaList := &storagev1.VolumeAttachmentList{}
			err = r.List(ctx, vaList)
			if err != nil {
				logger.Error(err, "Failed to list VolumeAttachments")
				return ctrl.Result{}, err
			}

			activeAttachment := false
			for _, va := range vaList.Items {
				if va.Spec.Source.PersistentVolumeName != nil {
					pvName := *va.Spec.Source.PersistentVolumeName
					for _, targetPV := range job.Status.PVsToDetach {
						if pvName == targetPV {
							// Check if the volume is still reported as attached in GKE status
							if va.Status.Attached {
								logger.Info("Volume is still attached, waiting...", "pv", pvName, "volumeAttachment", va.Name)
								activeAttachment = true
								break
							}
						}
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
					if updateErr := r.Status().Update(ctx, job); updateErr != nil {
						logger.Error(updateErr, "Failed to update status on volume detach wait")
						return ctrl.Result{}, updateErr
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
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to update job status to Restoring")
			return ctrl.Result{}, err
		}
		logger.Info("PodMigrationJob transitioned to Restoring phase", "pod", podName)
		return ctrl.Result{}, nil

	case pmv1alpha1.PodMigrationJobPhaseRestoring:
		// 1. Ensure RestoringStartTime is initialized
		if job.Status.RestoringStartTime == nil {
			now := metav1.Now()
			job.Status.RestoringStartTime = &now
			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to initialize RestoringStartTime")
				return ctrl.Result{}, err
			}
		}

		// 2. 5-minute safety ceiling measured from RestoringStartTime
		const restoreTimeout = 5 * time.Minute
		if time.Since(job.Status.RestoringStartTime.Time) > restoreTimeout {
			logger.Info("Restore timeout reached (5m), marking SucceededWithoutRestore", "job", job.Name, "pod", podName)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
			now := metav1.Now()
			job.Status.CompletionTime = &now
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Restored",
				Status:             metav1.ConditionFalse,
				Reason:             "RestoreTimeout",
				Message:            "Restore timed out after 5 minutes",
				ObservedGeneration: job.Generation,
			})
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "RestoreTimeout",
				Message:            "Restore timed out after 5 minutes",
				ObservedGeneration: job.Generation,
			})
			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to update job status on restore timeout")
				return ctrl.Result{}, err
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

			const mismatchGracePeriod = 30 * time.Second
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
			if err != nil || time.Since(mismatchSince) > mismatchGracePeriod {
				logger.Info("Replacement pod UID mismatch persisted > 30s; fast-failing to SucceededWithoutRestore", "job", job.Name)
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
				if err := r.Status().Update(ctx, job); err != nil {
					return ctrl.Result{}, err
				}
				return ctrl.Result{}, nil
			}

			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}

		// 5. Check for GKE PodSnapshots cold-start fallback Warning Event
		hasFallback, err := r.hasColdStartFallbackEvent(ctx, job, replacementPod)
		if err != nil {
			logger.Error(err, "Failed to query events for replacement pod")
			return ctrl.Result{}, err
		}
		if hasFallback {
			logger.Info("GKE runtime skipped snapshot restore and fell back to cold start", "pod", replacementPod.Name)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
			now := metav1.Now()
			job.Status.CompletionTime = &now
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Restored",
				Status:             metav1.ConditionFalse,
				Reason:             "FallbackToColdStart",
				Message:            "GKE runtime skipped snapshot restore and fell back to cold start",
				ObservedGeneration: job.Generation,
			})
			meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionTrue,
				Reason:             "FallbackToColdStart",
				Message:            "GKE runtime skipped snapshot restore and fell back to cold start",
				ObservedGeneration: job.Generation,
			})
			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to update job status to SucceededWithoutRestore")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		// 6. Check if replacement pod has achieved PodReady condition (Verified Restore)
		if isPodReady(replacementPod) {
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
			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to update job status to Succeeded")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}

		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	return ctrl.Result{}, nil
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

	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}

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
	return ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigrationJob{}).
		WithOptions(options).
		Complete(r)
}
