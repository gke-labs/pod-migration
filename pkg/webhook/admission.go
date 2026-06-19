package webhook

import (
	"context"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// EvictionGuard handles eviction requests and checks for snapshots.
type EvictionGuard struct {
	Client  client.Client
	decoder admission.Decoder
}

// Handle intercepts eviction requests.
func (a *EvictionGuard) Handle(ctx context.Context, req admission.Request) admission.Response {
	klog.Infof("Intercepted request for %s/%s, subresource: %s", req.Namespace, req.Name, req.SubResource)

	if req.SubResource != "eviction" {
		return admission.Allowed("not an eviction request")
	}

	// Fetch the Pod
	pod := &corev1.Pod{}
	err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, pod)
	if err != nil {
		klog.Errorf("Failed to get pod %s/%s: %v", req.Namespace, req.Name, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	// Check if feature is enabled for this pod
	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		klog.Infof("Feature not enabled for pod %s/%s, allowing eviction", req.Namespace, req.Name)
		return admission.Allowed("feature not enabled")
	}

	// Inspect volumes to determine if we should bypass the webhook for runtime interception (Approach 2)
	hasRWO := false
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc := &corev1.PersistentVolumeClaim{}
			err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: vol.PersistentVolumeClaim.ClaimName}, pvc)
			if err != nil {
				klog.Errorf("Failed to get PVC %s: %v", vol.PersistentVolumeClaim.ClaimName, err)
				return admission.Errored(http.StatusInternalServerError, err)
			}
			for _, mode := range pvc.Spec.AccessModes {
				if mode == corev1.ReadWriteOnce || mode == corev1.PersistentVolumeAccessMode("ReadWriteOncePod") {
					hasRWO = true
					break
				}
			}
			if hasRWO {
				break
			}
		}
	}

	if !hasRWO {
		klog.Infof("Pod %s/%s has no RWO volumes, bypassing eviction webhook for runtime interception", req.Namespace, req.Name)
		return admission.Allowed("bypassing eviction webhook for diskless/RWX workload")
	}

	// Check annotations
	if pod.Annotations["pod-migration.gke.io/snapshot-requested"] == "true" {
		klog.Infof("Pod %s/%s snapshot requested, denying eviction to allow snapshot and stop", req.Namespace, req.Name)
		return admission.Denied("snapshot in progress and pod will be stopped")
	}

	// Trigger snapshot by adding annotation
	klog.Infof("Triggering snapshot for Pod %s/%s", req.Namespace, req.Name)

	// We need to update the Pod. We can do it via the client.
	// Note: We are in a VALIDATING webhook, so we cannot mutate the object in the request (which is the eviction anyway).
	// We must update the Pod object in the API server.

	podCopy := pod.DeepCopy()
	if podCopy.Annotations == nil {
		podCopy.Annotations = make(map[string]string)
	}
	podCopy.Annotations["pod-migration.gke.io/snapshot-requested"] = "true"

	err = a.Client.Update(ctx, podCopy)
	if err != nil {
		klog.Errorf("Failed to update pod %s/%s annotations: %v", req.Namespace, req.Name, err)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.Denied("snapshot triggered, the pod will be terminated shortly")
}

// InjectDecoder injects the decoder.
func (a *EvictionGuard) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}
