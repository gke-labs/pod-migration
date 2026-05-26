package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SnapshotReconciler reconciles PodSnapshots and handles deletion.
type SnapshotReconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles PodSnapshots.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.Infof("Reconciling PodSnapshot %s", req.NamespacedName)

	snapshot := &unstructured.Unstructured{}
	snapshot.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshot",
	})
	err := r.Client.Get(ctx, req.NamespacedName, snapshot)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if snapshot is ready
	snapStatus, ok := snapshot.Object["status"].(map[string]interface{})
	if !ok {
		return ctrl.Result{}, nil
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
		}
	}

	if !isReady {
		return ctrl.Result{}, nil
	}

	// Extract pod name from annotation
	annotations := snapshot.GetAnnotations()
	podName := annotations["podsnapshot.gke.io/origin-pod"]
	if podName == "" {
		klog.Infof("Snapshot %s has no origin-pod annotation", snapshot.GetName())
		return ctrl.Result{}, nil
	}

	klog.Infof("Snapshot %s is ready, handling pod %s", snapshot.GetName(), podName)

	// Fetch the Pod
	pod := &corev1.Pod{}
	err = r.Client.Get(ctx, types.NamespacedName{Namespace: snapshot.GetNamespace(), Name: podName}, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("Pod %s not found, maybe already deleted", podName)
		} else {
			return ctrl.Result{}, err
		}
	} else {
		// Check if feature is enabled for this pod
		if pod.Labels["pod-migration.gke.io/enabled"] != "true" ||
			pod.Annotations["pod-migration.gke.io/snapshot-requested"] != "true" {
			klog.Infof("Pod %s does not have migration labels/annotations, skipping deletion", podName)
			return ctrl.Result{}, nil
		}

		// Delete the Pod
		err = r.Client.Delete(ctx, pod)
		if err != nil {
			klog.Errorf("Failed to delete pod %s: %v", podName, err)
			return ctrl.Result{}, err
		}
		klog.Infof("Successfully deleted pod %s", podName)
	}

	// Delete the Trigger as well
	triggerName := "trigger-" + podName
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	err = r.Client.Get(ctx, types.NamespacedName{Namespace: snapshot.GetNamespace(), Name: triggerName}, trigger)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("Trigger %s not found, maybe already deleted", triggerName)
		} else {
			return ctrl.Result{}, err
		}
	} else {
		err = r.Client.Delete(ctx, trigger)
		if err != nil {
			klog.Errorf("Failed to delete trigger %s: %v", triggerName, err)
			return ctrl.Result{}, err
		}
		klog.Infof("Successfully deleted trigger %s", triggerName)
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *SnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshot",
	})

	return ctrl.NewControllerManagedBy(mgr).
		For(u).
		Complete(r)
}
