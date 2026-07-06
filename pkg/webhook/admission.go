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

	// Bypass eviction webhook for all workloads under Approach 2 runtime-only interception
	klog.Infof("Pod %s/%s has pod-migration enabled, bypassing eviction webhook for runtime onDelete interception", req.Namespace, req.Name)
	return admission.Allowed("bypassing eviction webhook for runtime interception")
}

// InjectDecoder injects the decoder.
func (a *EvictionGuard) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}
