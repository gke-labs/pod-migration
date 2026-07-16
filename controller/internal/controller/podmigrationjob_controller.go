package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
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

	switch job.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhasePending:
		// Fetch the target pod to get its node name
		pod := &corev1.Pod{}
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, pod)
		if err != nil {
			logger.Error(err, "Failed to get target pod for node analysis")
			return ctrl.Result{}, err
		}
		nodeName := pod.Spec.NodeName
		if nodeName == "" {
			return ctrl.Result{}, fmt.Errorf("target pod %s is not scheduled on any node", podName)
		}

		// 2. Trigger GKE Snapshot (Create PodSnapshotManualTrigger)
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		trigger.SetName(triggerName)
		trigger.SetNamespace(req.Namespace)
		trigger.SetLabels(map[string]string{
			"podsnapshot.gke.io/targetNode": nodeName,
		})
		trigger.Object["spec"] = map[string]interface{}{
			"targetPod": podName,
		}

		logger.Info("Creating PodSnapshotManualTrigger", "name", triggerName, "targetNode", nodeName)
		err = r.Create(ctx, trigger)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create manual trigger")
			return ctrl.Result{}, err
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
		snapshotList := &unstructured.UnstructuredList{}
		snapshotList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotList",
		})
		err = r.List(ctx, snapshotList, client.InNamespace(req.Namespace))
		if err != nil {
			logger.Error(err, "Failed to list PodSnapshots")
			return ctrl.Result{}, err
		}

		var targetSnapshot *unstructured.Unstructured
		for _, s := range snapshotList.Items {
			annotations := s.GetAnnotations()
			if annotations["podsnapshot.gke.io/origin-pod"] == podName {
				targetSnapshot = &s
				break
			}
		}

		if targetSnapshot == nil {
			logger.Info("Waiting for GKE PodSnapshot object to be generated...")
			return ctrl.Result{Requeue: true}, nil
		}

		// Check if snapshot is ready
		snapStatus, ok := targetSnapshot.Object["status"].(map[string]interface{})
		if !ok {
			logger.Info("Snapshot status subresource not found, waiting...")
			return ctrl.Result{Requeue: true}, nil
		}

		conditions, _ := snapStatus["conditions"].([]interface{})
		isReady := false
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if ok {
				if (cond["type"] == "Ready" || cond["type"] == "Checkpoint") && cond["status"] == "True" {
					isReady = true
					break
				}
			}
		}

		if !isReady {
			logger.Info("Snapshot is not ready yet, waiting...")
			return ctrl.Result{Requeue: true}, nil
		}

		logger.Info("GKE PodSnapshot is Ready, transitioning to Evicting phase", "snapshot", targetSnapshot.GetName())
		job.Status.SnapshotRef = targetSnapshot.GetName()
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
		err = r.Status().Update(ctx, job)
		if err != nil {
			logger.Error(err, "Failed to update job status to Evicting")
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil

	case pmv1alpha1.PodMigrationJobPhaseEvicting:
		// 4.1. Capture PV Names before deleting the origin pod (to trace their detachment later)
		if len(job.Status.PVsToDetach) == 0 {
			originPod := &corev1.Pod{}
			err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, originPod)
			if err == nil {
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
				if len(pvs) > 0 {
					job.Status.PVsToDetach = pvs
					err = r.Status().Update(ctx, job)
					if err != nil {
						logger.Error(err, "Failed to update job PVsToDetach status")
						return ctrl.Result{}, err
					}
					return ctrl.Result{Requeue: true}, nil
				}
			} else if !apierrors.IsNotFound(err) {
				logger.Error(err, "Failed to get origin pod for PV analysis")
				return ctrl.Result{}, err
			}
		}

		// 4.2. Terminate Origin Pod & Cleanup Trigger
		logger.Info("Deleting origin Pod", "pod", podName)
		pod := &corev1.Pod{}
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, pod)
		if err == nil {
			err = r.Delete(ctx, pod)
			if err != nil {
				logger.Error(err, "Failed to delete origin pod")
				return ctrl.Result{}, err
			}
			logger.Info("Successfully deleted pod")
		} else if !apierrors.IsNotFound(err) {
			logger.Error(err, "Failed to get origin pod")
			return ctrl.Result{}, err
		}

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
				// Requeue in 3 seconds to check again
				return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
			}
			logger.Info("All volumes detached successfully")
		}

		// 4.4. Once all PVs are detached, find the replacement pod and release its scheduling gate.
		parentName := job.Labels["pod-migration.gke.io/parent-name"]
		parentKind := job.Labels["pod-migration.gke.io/parent-kind"]

		if parentName != "" {
			podList := &corev1.PodList{}
			err = r.List(ctx, podList, client.InNamespace(req.Namespace))
			if err != nil {
				logger.Error(err, "Failed to list pods to locate replacement")
				return ctrl.Result{}, err
			}

			for _, p := range podList.Items {
				// Determine if this pod belongs to the same parent workload
				matchesParent := false
				for _, ref := range p.OwnerReferences {
					if ref.Kind == "ReplicaSet" {
						rs := &appsv1.ReplicaSet{}
						err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: ref.Name}, rs)
						if err == nil {
							for _, rsRef := range rs.OwnerReferences {
								if rsRef.Kind == "Deployment" && rsRef.Name == parentName && parentKind == "Deployment" {
									matchesParent = true
									break
								}
							}
						}
					} else if ref.Kind == "Job" && ref.Name == parentName && parentKind == "Job" {
						matchesParent = true
					} else if ref.Kind == "StatefulSet" && ref.Name == parentName && parentKind == "StatefulSet" {
						matchesParent = true
					}
					if matchesParent {
						break
					}
				}

				if matchesParent {
					// Check if scheduling gate is present and remove it
					gateIndex := -1
					for i, gate := range p.Spec.SchedulingGates {
						if gate.Name == "gke.io/pod-migration-gate" {
							gateIndex = i
							break
						}
					}
					if gateIndex != -1 {
						logger.Info("Releasing scheduling gate for replacement pod", "pod", p.Name)
						p.Spec.SchedulingGates = append(p.Spec.SchedulingGates[:gateIndex], p.Spec.SchedulingGates[gateIndex+1:]...)
						err = r.Update(ctx, &p)
						if err != nil {
							logger.Error(err, "Failed to remove scheduling gate from replacement pod")
							return ctrl.Result{}, err
						}
					}
				}
			}
		}

		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
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
