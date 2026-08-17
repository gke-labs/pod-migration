package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// PodMigrationJobReconciler reconciles a PodMigrationJob object.
type PodMigrationJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotmanualtriggers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots/status,verbs=get
// +kubebuilder:rbac:groups=storage.k8s.io,resources=volumeattachments,verbs=get;list;watch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
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
	triggerName := fmt.Sprintf("trigger-%s", podName)

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

			// Clean up GKE manual trigger if it exists (best effort)
			trigger := &unstructured.Unstructured{}
			trigger.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "podsnapshot.gke.io",
				Version: "v1",
				Kind:    "PodSnapshotManualTrigger",
			})
			trigger.SetName(triggerName)
			trigger.SetNamespace(req.Namespace)
			_ = r.Delete(ctx, trigger)

			if err := r.Status().Update(ctx, job); err != nil {
				logger.Error(err, "Failed to update job status to Failed on timeout")
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Garbage collect completed PMJs after 5 minutes
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded ||
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
			var pvs []string
			for _, vol := range originPod.Spec.Volumes {
				if vol.PersistentVolumeClaim != nil {
					pvc := &corev1.PersistentVolumeClaim{}
					err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: vol.PersistentVolumeClaim.ClaimName}, pvc)
					if err == nil && pvc.Spec.VolumeName != "" {
						pvs = append(pvs, pvc.Spec.VolumeName)
					}
				}
			}
			job.Status.PVsToDetach = pvs
		}

		_, res, err := r.ensureTrigger(ctx, job, podName)
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
		// 3. Monitor GKE Snapshot readiness
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: triggerName}, trigger)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Waiting for PodSnapshotManualTrigger to be created...", "trigger", triggerName)
				cond := metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Snapshotting",
					Message:            "Waiting for GKE PodSnapshotManualTrigger to be created",
					ObservedGeneration: job.Generation,
				}
				if meta.SetStatusCondition(&job.Status.Conditions, cond) {
					_ = r.Status().Update(ctx, job)
				}
				return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
			}
			logger.Error(err, "Failed to get PodSnapshotManualTrigger")
			return ctrl.Result{}, err
		}

		snapshotName, found, err := unstructured.NestedString(trigger.Object, "status", "snapshotCreated", "name")
		if err != nil {
			logger.Error(err, "Failed to read snapshotCreated.name from trigger status")
			return ctrl.Result{}, err
		}
		if !found || snapshotName == "" {
			logger.Info("Waiting for snapshot name to be populated in trigger status...", "trigger", triggerName)
			cond := metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Snapshotting",
				Message:            "Waiting for GKE to populate snapshot name in trigger status",
				ObservedGeneration: job.Generation,
			}
			if meta.SetStatusCondition(&job.Status.Conditions, cond) {
				_ = r.Status().Update(ctx, job)
			}
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}

		targetSnapshot := &unstructured.Unstructured{}
		targetSnapshot.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshot",
		})
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: snapshotName}, targetSnapshot)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Waiting for PodSnapshot to be created...", "snapshot", snapshotName)
				cond := metav1.Condition{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Snapshotting",
					Message:            fmt.Sprintf("Waiting for GKE PodSnapshot object %q to be created", snapshotName),
					ObservedGeneration: job.Generation,
				}
				if meta.SetStatusCondition(&job.Status.Conditions, cond) {
					_ = r.Status().Update(ctx, job)
				}
				return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
			}
			logger.Error(err, "Failed to get PodSnapshot")
			return ctrl.Result{}, err
		}

		// Check if snapshot is ready
		snapStatus, ok := targetSnapshot.Object["status"].(map[string]interface{})
		if !ok {
			logger.Info("Snapshot status subresource not found, waiting...")
			cond := metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Snapshotting",
				Message:            fmt.Sprintf("Waiting for GKE PodSnapshot %q status to be populated", snapshotName),
				ObservedGeneration: job.Generation,
			}
			if meta.SetStatusCondition(&job.Status.Conditions, cond) {
				_ = r.Status().Update(ctx, job)
			}
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}

		conditions, _ := snapStatus["conditions"].([]interface{})
		isReady := false
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if ok {
				if cond["type"] == "Ready" && cond["status"] == "True" {
					isReady = true
					break
				}
				if cond["type"] == "Checkpoint" && cond["status"] == "True" && cond["reason"] == "Succeeded" {
					isReady = true
					break
				}
			}
		}

		if !isReady {
			logger.Info("Snapshot is not ready yet, waiting...")
			cond := metav1.Condition{
				Type:               "Ready",
				Status:             metav1.ConditionFalse,
				Reason:             "Snapshotting",
				Message:            fmt.Sprintf("Waiting for GKE PodSnapshot %q checkpoint to complete", snapshotName),
				ObservedGeneration: job.Generation,
			}
			if meta.SetStatusCondition(&job.Status.Conditions, cond) {
				_ = r.Status().Update(ctx, job)
			}
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}

		logger.Info("GKE PodSnapshot is Ready, transitioning to Evicting phase", "snapshot", snapshotName)
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
		job.Status.SnapshotRef = snapshotName
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to update job status to Evicting")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

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
			} else {
				logger.Info("Waiting for eviction webhook to allow deletion", "pod", podName, "elapsed", time.Since(evictingSince))
				return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
			}
		}

		// Once the Pod is gone (or we fallback-deleted it), we can delete the manual trigger.
		logger.Info("Deleting manual trigger", "trigger", triggerName)
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: triggerName}, trigger)
		if err == nil {
			err = r.Delete(ctx, trigger)
			if err != nil {
				logger.Error(err, "Failed to delete trigger")
				return ctrl.Result{}, err
			}
			logger.Info("Successfully deleted trigger")
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get trigger")
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

		// 4.4. Once all PVs are detached, transition the PMJ phase to Succeeded.
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
		now := metav1.Now()
		job.Status.CompletionTime = &now
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
			Type:               "Ready",
			Status:             metav1.ConditionTrue,
			Reason:             "MigrationSucceeded",
			Message:            "Pod migration completed successfully",
			ObservedGeneration: job.Generation,
		})
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to update job status to Succeeded")
			return ctrl.Result{}, err
		}
		logger.Info("PodMigrationJob completed successfully!")
		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodMigrationJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&pmv1alpha1.PodMigrationJob{}).
		Complete(r)
}

func (r *PodMigrationJobReconciler) ensureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (string, ctrl.Result, error) {
	triggerName := fmt.Sprintf("trigger-%s", podName)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	trigger.SetName(triggerName)
	trigger.SetNamespace(job.Namespace)
	trigger.Object["spec"] = map[string]interface{}{
		"targetPod": podName,
	}

	// Set owner reference to allow cascade deletion and ownership verification
	if err := controllerutil.SetControllerReference(job, trigger, r.Scheme); err != nil {
		logger.Error(err, "Failed to set owner reference on trigger")
		return "", ctrl.Result{}, err
	}

	logger.Info("Creating PodSnapshotManualTrigger")
	err := r.Create(ctx, trigger)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Fetch existing trigger to verify ownership
			existingTrigger := &unstructured.Unstructured{}
			existingTrigger.SetGroupVersionKind(trigger.GroupVersionKind())
			getErr := r.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, existingTrigger)
			if getErr == nil {
				isOwned := false
				for _, ref := range existingTrigger.GetOwnerReferences() {
					if ref.UID == job.UID {
						isOwned = true
						break
					}
				}
				if isOwned {
					logger.Info("Trigger already exists and is owned by this job, proceeding")
					return triggerName, ctrl.Result{}, nil
				} else {
					logger.Info("Stale trigger found (owned by another job), deleting it first")
					_ = r.Delete(ctx, existingTrigger)
					return "", ctrl.Result{Requeue: true}, nil
				}
			} else {
				logger.Error(getErr, "Failed to fetch existing trigger on AlreadyExists")
				return "", ctrl.Result{}, getErr
			}
		} else {
			logger.Error(err, "Failed to create PodSnapshotManualTrigger")
			return "", ctrl.Result{}, err
		}
	}

	logger.Info("Successfully created PodSnapshotManualTrigger")
	return triggerName, ctrl.Result{}, nil
}

