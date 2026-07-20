package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
)

// PodGateReconciler reconciles Pods to clean up scheduling gates on clean startups.
type PodGateReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=replicasets,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots/status,verbs=get;update;patch

// Reconcile checks for active migration jobs and removes the scheduling gate if none exist.
func (r *PodGateReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName)

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

	// Find the parent workload owner details (ReplicaSet -> Deployment, or Job)
	parentName := ""
	parentKind := ""
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" {
			rs := &appsv1.ReplicaSet{}
			err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: ref.Name}, rs)
			if err == nil {
				for _, rsRef := range rs.OwnerReferences {
					if rsRef.Kind == "Deployment" {
						parentName = rsRef.Name
						parentKind = "Deployment"
						break
					}
				}
			}
			if parentName != "" {
				break
			}
		} else if ref.Kind == "Job" {
			parentName = ref.Name
			parentKind = "Job"
			break
		} else if ref.Kind == "StatefulSet" {
			parentName = ref.Name
			parentKind = "StatefulSet"
			break
		}
	}

	hasActiveMigration := false

	if parentName != "" {
		// List all PodMigrationJob resources in namespace
		jobList := &pmv1alpha1.PodMigrationJobList{}
		err = r.List(ctx, jobList, client.InNamespace(req.Namespace))
		if err != nil {
			logger.Error(err, "Failed to list PodMigrationJobs")
			return ctrl.Result{}, err
		}

		for _, job := range jobList.Items {
			if job.Labels["pod-migration.gke.io/parent-name"] == parentName &&
				job.Labels["pod-migration.gke.io/parent-kind"] == parentKind {
				// Check phase: Pending, Snapshotting, Evicting
				phase := job.Status.Phase
				if phase == pmv1alpha1.PodMigrationJobPhasePending ||
					phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
					phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
					hasActiveMigration = true
					break
				}
			}
		}
	} else {
		// Bare pod case: check if there's an active PodMigrationJob targetting this pod name.
		job := &pmv1alpha1.PodMigrationJob{}
		err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: fmt.Sprintf("pmj-%s", req.Name)}, job)
		if err == nil {
			phase := job.Status.Phase
			if phase == pmv1alpha1.PodMigrationJobPhasePending ||
				phase == pmv1alpha1.PodMigrationJobPhaseSnapshotting ||
				phase == pmv1alpha1.PodMigrationJobPhaseEvicting {
				hasActiveMigration = true
			}
		}
	}

	if !hasActiveMigration {
		// Check if there is an active PodSnapshot for this pod.
		activeSnapshot, readyTime, latestSnap, err := r.hasActiveSnapshot(ctx, req.Namespace, req.Name, parentName, parentKind)
		if err != nil {
			logger.Error(err, "Failed to check active snapshots")
			return ctrl.Result{}, err
		}
		if activeSnapshot {
			logger.Info("Active snapshot found for pod; keeping scheduling gate", "pod", req.Name)
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		}
		if latestSnap != nil {
			// Check if we need to promote it
			hasReady := false
			if status, ok := latestSnap.Object["status"].(map[string]interface{}); ok {
				if conditions, ok := status["conditions"].([]interface{}); ok {
					for _, cond := range conditions {
						if condMap, ok := cond.(map[string]interface{}); ok {
							if condMap["type"] == "Ready" && condMap["status"] == "True" {
								hasReady = true
								break
							}
						}
					}
				}
			}
			if !hasReady {
				err := r.promoteSnapshotToReady(ctx, latestSnap)
				if err != nil {
					logger.Error(err, "Failed to promote snapshot to Ready", "snapshot", latestSnap.GetName())
					return ctrl.Result{}, err
				}
				// Requeue to let cache sync the status update
				return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
			}
		}
		if latestSnap != nil {
			// Check if we need to promote it
			hasReady := false
			if status, ok := latestSnap.Object["status"].(map[string]interface{}); ok {
				if conditions, ok := status["conditions"].([]interface{}); ok {
					for _, cond := range conditions {
						if condMap, ok := cond.(map[string]interface{}); ok {
							if condMap["type"] == "Ready" && condMap["status"] == "True" {
								hasReady = true
								break
							}
						}
					}
				}
			}
			if !hasReady {
				err := r.promoteSnapshotToReady(ctx, latestSnap)
				if err != nil {
					logger.Error(err, "Failed to promote snapshot to Ready", "snapshot", latestSnap.GetName())
					return ctrl.Result{}, err
				}
				// Requeue immediately to let cache sync the status update
				return ctrl.Result{Requeue: true}, nil
			}
		}
		if !readyTime.IsZero() {
			// Snapshot is ready. Check if we should wait for cache sync.
			elapsed := time.Since(readyTime)
			if elapsed < 5*time.Second {
				waitNeeded := 5*time.Second - elapsed
				logger.Info("Snapshot ready recently; waiting for cache sync", "pod", req.Name, "wait", waitNeeded)
				return ctrl.Result{RequeueAfter: waitNeeded}, nil
			}

			// Add the restore annotations
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations["gke-pod-snapshot-role"] = "restore"
			if latestSnap != nil {
				pod.Annotations["podsnapshot.gke.io/ps-name"] = latestSnap.GetName()
			}
			logger.Info("Adding restore annotations to pod", "pod", req.Name, "snapshot", latestSnap.GetName())
		}

		// Remove the scheduling gate
		logger.Info("No active migration job or snapshot found for parent; removing scheduling gate to allow startup", "pod", req.Name)
		pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates[:gateIndex], pod.Spec.SchedulingGates[gateIndex+1:]...)
		err = r.Update(ctx, pod)
		if err != nil {
			logger.Error(err, "Failed to remove scheduling gate from Pod")
			return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
		}
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodGateReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}

func (r *PodGateReconciler) hasActiveSnapshot(ctx context.Context, namespace, podName, parentName, parentKind string) (bool, time.Time, *unstructured.Unstructured, error) {
	uList := &unstructured.UnstructuredList{}
	uList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotList",
	})
	err := r.List(ctx, uList, client.InNamespace(namespace))
	if err != nil {
		if meta.IsNoMatchError(err) {
			return false, time.Time{}, nil, nil
		}
		return false, time.Time{}, nil, err
	}

	var latestTransitionTime time.Time
	var latestSnap *unstructured.Unstructured
	anyActive := false

	for i := range uList.Items {
		item := &uList.Items[i]
		annotations := item.GetAnnotations()
		if annotations != nil {
			if originPod, ok := annotations["podsnapshot.gke.io/origin-pod"]; ok {
				matches := false
				if originPod == podName {
					matches = true
				} else if parentName != "" {
					matches = r.podBelongsToParent(originPod, parentName, parentKind)
				}
				if !matches {
					continue
				}
				if item.GetDeletionTimestamp() != nil {
					continue
				}
				status, ok := item.Object["status"].(map[string]interface{})
				if !ok {
					anyActive = true
					continue
				}
				conditions, ok := status["conditions"].([]interface{})
				if !ok {
					anyActive = true
					continue
				}

				checkpointReady := false
				var transitionTime time.Time
				for _, cond := range conditions {
					condMap, ok := cond.(map[string]interface{})
					if !ok {
						continue
					}
					cType := condMap["type"]
					cStatus := condMap["status"]
					cReason := condMap["reason"]
					cTimeStr, _ := condMap["lastTransitionTime"].(string)

					if cType == "Checkpoint" {
						if cStatus == "True" && cReason == "Succeeded" {
							checkpointReady = true
							if cTimeStr != "" {
								t, err := time.Parse(time.RFC3339, cTimeStr)
								if err == nil {
									transitionTime = t
								}
							}
						}
						if cReason == "Failed" {
							checkpointReady = true
						}
					}
				}
				if !checkpointReady {
					anyActive = true
				} else if transitionTime.After(latestTransitionTime) {
					latestTransitionTime = transitionTime
					latestSnap = item
				}
			}
		}
	}
	if anyActive {
		return true, time.Time{}, nil, nil
	}
	return false, latestTransitionTime, latestSnap, nil
}

func (r *PodGateReconciler) promoteSnapshotToReady(ctx context.Context, snap *unstructured.Unstructured) error {
	logger := log.FromContext(ctx)
	logger.Info("Promoting snapshot to Ready", "name", snap.GetName())

	// Get existing conditions
	status, ok := snap.Object["status"].(map[string]interface{})
	if !ok {
		status = make(map[string]interface{})
		snap.Object["status"] = status
	}
	conditionsInterface, ok := status["conditions"]
	var conditions []interface{}
	if ok {
		conditions, _ = conditionsInterface.([]interface{})
	}

	now := time.Now().Format(time.RFC3339)

	// Create Ready condition
	readyCond := map[string]interface{}{
		"type":               "Ready",
		"status":             "True",
		"reason":             "Succeeded",
		"message":            "Promoted to Ready by gate controller",
		"lastTransitionTime": now,
	}

	// Create StorageReplicated condition
	storageCond := map[string]interface{}{
		"type":               "StorageReplicated",
		"status":             "True",
		"reason":             "Succeeded",
		"message":            "Promoted to StorageReplicated by gate controller",
		"lastTransitionTime": now,
	}

	newConditions := make([]interface{}, 0)
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if ok && (cond["type"] == "Ready" || cond["type"] == "StorageReplicated") {
			continue
		}
		newConditions = append(newConditions, c)
	}
	newConditions = append(newConditions, storageCond, readyCond)
	status["conditions"] = newConditions

	return r.Status().Update(ctx, snap)
}

func (r *PodGateReconciler) podBelongsToParent(podName, parentName, parentKind string) bool {
	if !strings.HasPrefix(podName, parentName+"-") {
		return false
	}
	suffix := podName[len(parentName)+1:]
	if parentKind == "StatefulSet" {
		_, err := strconv.Atoi(suffix)
		return err == nil
	}
	if parentKind == "Job" {
		if len(suffix) != 5 {
			return false
		}
		for _, r := range suffix {
			if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z')) {
				return false
			}
		}
		return true
	}
	if parentKind == "Deployment" {
		parts := strings.Split(suffix, "-")
		if len(parts) != 2 {
			return false
		}
		if len(parts[1]) != 5 {
			return false
		}
		return true
	}
	return false
}
