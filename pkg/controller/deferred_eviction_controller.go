package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// DeferredEvictionReconciler reconciles Pods with Deferred resize status and evicts them.
type DeferredEvictionReconciler struct {
	Client client.Client
	Scheme *runtime.Scheme
}

// Reconcile handles Pods with Deferred resize status.
func (r *DeferredEvictionReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	klog.Infof("Reconciling Pod for deferred eviction: %s", req.NamespacedName)

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

	hasLabel := pod.Labels["pod-migration.gke.io/deferred-eviction-processed"] == "true"
	isDeferred := isPodDeferred(pod)

	// Handle cleanup case: Pod is no longer deferred but still has the processed label.
	if !isDeferred && hasLabel {
		klog.Infof("Pod %s is no longer deferred, removing processed label", pod.Name)
		podCopy := pod.DeepCopy()
		delete(podCopy.Labels, "pod-migration.gke.io/deferred-eviction-processed")
		if err := r.Client.Update(ctx, podCopy); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	// If it's not deferred or already has the label, nothing to do.
	if !isDeferred || hasLabel {
		return ctrl.Result{}, nil
	}

	nodeName := pod.Spec.NodeName
	if nodeName == "" {
		klog.Warningf("Pod %s is deferred but Spec.NodeName is empty", pod.Name)
		return ctrl.Result{}, nil
	}

	// 1. Look for other pods on the same node with migration enabled
	otherPods := &corev1.PodList{}
	err = r.Client.List(ctx, otherPods, client.InNamespace(""), client.MatchingFields{"spec.nodeName": nodeName})
	if err != nil {
		return ctrl.Result{}, err
	}

	var targetPod *corev1.Pod
	for i := range otherPods.Items {
		p := &otherPods.Items[i]
		if p.Name == pod.Name && p.Namespace == pod.Namespace {
			continue // Skip the deferred pod itself
		}
		if p.Labels["pod-migration.gke.io/enabled"] == "true" && p.DeletionTimestamp == nil {
			targetPod = p
			break // Pick the first available candidate
		}
	}

	if targetPod != nil {
		klog.Infof("Found candidate pod %s/%s on node %s to evict instead of %s", targetPod.Namespace, targetPod.Name, nodeName, pod.Name)

		// Evict the target pod
		eviction := &policyv1.Eviction{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: targetPod.Namespace,
				Name:      targetPod.Name,
			},
		}
		err = r.Client.SubResource("eviction").Create(ctx, targetPod, eviction)
		if err != nil {
			klog.Infof("Eviction request for target pod %s/%s returned: %v", targetPod.Namespace, targetPod.Name, err)
		} else {
			klog.Warningf("Target pod %s/%s was evicted successfully (unexpectedly not stopped by webhook)", targetPod.Namespace, targetPod.Name)
		}

		// 2. Add label to the deferred pod so we don't try to evict in subsequent loops
		podCopy := pod.DeepCopy()
		if podCopy.Labels == nil {
			podCopy.Labels = make(map[string]string)
		}
		podCopy.Labels["pod-migration.gke.io/deferred-eviction-processed"] = "true"
		if err := r.Client.Update(ctx, podCopy); err != nil {
			return ctrl.Result{}, err
		}

		return ctrl.Result{}, nil
	}

	// 3. Fallback: Evict the deferred pod immediately if no other migratable pods exist
	klog.Infof("No other migratable pods found on node %s, evicting deferred pod %s immediately", nodeName, pod.Name)

	eviction := &policyv1.Eviction{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pod.Namespace,
			Name:      pod.Name,
		},
	}

	err = r.Client.SubResource("eviction").Create(ctx, pod, eviction)
	if err != nil {
		klog.Infof("Eviction request for pod %s returned expected error (stopped by webhook): %v", pod.Name, err)
		return ctrl.Result{}, nil
	}

	klog.Warningf("Pod %s was evicted successfully (unexpectedly not stopped by webhook)", pod.Name)
	return ctrl.Result{}, nil
}

func isPodDeferred(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == "PodResizePending" && c.Reason == "Deferred" && c.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *DeferredEvictionReconciler) SetupWithManager(mgr ctrl.Manager) error {
	// Index pods by spec.nodeName to allow efficient listing per node.
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
		pod := rawObj.(*corev1.Pod)
		if pod.Spec.NodeName == "" {
			return nil
		}
		return []string{pod.Spec.NodeName}
	}); err != nil {
		return err
	}

	pred := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return false // Ignore create events, wait for status update
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			podOld := e.ObjectOld.(*corev1.Pod)
			podNew := e.ObjectNew.(*corev1.Pod)
			if podNew.GetDeletionTimestamp() != nil {
				return false
			}
			if podNew.GetLabels()["pod-migration.gke.io/enabled"] != "true" {
				return false
			}

			isNewDeferred := isPodDeferred(podNew)
			hasLabel := podNew.GetLabels()["pod-migration.gke.io/deferred-eviction-processed"] == "true"

			// Case 1: Pod is newly deferred or still deferred, and hasn't been processed yet.
			if isNewDeferred && !hasLabel {
				return true
			}

			// Case 2: Pod was deferred and processed, but now is NO LONGER deferred.
			// Trigger reconcile to clean up the label.
			isOldDeferred := isPodDeferred(podOld)
			if isOldDeferred && !isNewDeferred && hasLabel {
				return true
			}

			return false
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
		Named("deferred-eviction").
		WithEventFilter(pred).
		Complete(r)
}
