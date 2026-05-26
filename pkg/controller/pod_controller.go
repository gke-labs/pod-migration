package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// PodReconciler reconciles Pods and triggers snapshots.
type PodReconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles Pods with snapshot annotations.
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.Infof("Reconciling Pod %s", req.NamespacedName)

	pod := &corev1.Pod{}
	err := r.Client.Get(ctx, req.NamespacedName, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		return ctrl.Result{}, nil
	}

	if pod.Annotations["pod-migration.gke.io/snapshot-requested"] != "true" {
		return ctrl.Result{}, nil
	}

	// Trigger snapshot by creating PodSnapshotManualTrigger
	triggerName := "trigger-" + pod.Name

	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	u.SetName(triggerName)
	u.SetNamespace(pod.Namespace)
	u.Object["spec"] = map[string]interface{}{
		"targetPod": pod.Name,
	}

	if err := ctrl.SetControllerReference(pod, u, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}

	err = r.Client.Create(ctx, u)
	if err != nil {
		if errors.IsAlreadyExists(err) {
			klog.Infof("Trigger %s already exists", triggerName)
			return ctrl.Result{}, nil
		}
		klog.Errorf("Failed to create trigger %s: %v", triggerName, err)
		return ctrl.Result{}, err
	}

	klog.Infof("Created trigger %s for pod %s", triggerName, pod.Name)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pred := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false // Ignore create events
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Ignore updates if pod is being deleted
			if e.ObjectNew.GetDeletionTimestamp() != nil {
				return false
			}
			return e.ObjectNew.GetLabels()["pod-migration.gke.io/enabled"] == "true" &&
				e.ObjectNew.GetAnnotations()["pod-migration.gke.io/snapshot-requested"] == "true"
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		WithEventFilter(pred).
		Complete(r)
}
