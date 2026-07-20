package webhook

import (
	"context"
	"encoding/json"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// PodGateInjector injects the scheduling gate into replacement pods.
type PodGateInjector struct {
	Client  client.Client
	decoder admission.Decoder
}

// Handle inspects pod creations and injects the pod-migration-gate scheduling gate.
func (a *PodGateInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)
	logger.Info("Intercepted pod creation for scheduling gate check")

	pod := &corev1.Pod{}
	err := a.decoder.Decode(req, pod)
	if err != nil {
		logger.Error(err, "Failed to decode pod")
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Check if pod opted in
	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		logger.Info("Pod not opted in, bypassing scheduling gate injection")
		return admission.Allowed("pod not opted in")
	}

	// Check if the gate is already present
	hasGate := false
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			hasGate = true
			break
		}
	}

	if hasGate {
		logger.Info("Pod already has the scheduling gate, bypassing")
		return admission.Allowed("scheduling gate already present")
	}

	// Inject scheduling gate
	logger.Info("Injecting scheduling gate gke.io/pod-migration-gate")
	pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{
		Name: "gke.io/pod-migration-gate",
	})

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		logger.Error(err, "Failed to marshal modified pod")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// InjectDecoder injects the decoder.
func (a *PodGateInjector) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}

// SetupReplacementWebhookWithManager registers the mutating webhook on the manager.
func SetupReplacementWebhookWithManager(mgr ctrl.Manager) error {
	dec := admission.NewDecoder(mgr.GetScheme())
	mgr.GetWebhookServer().Register(
		"/mutate-v1-pod-scheduling-gate",
		&admission.Webhook{
			Handler: &PodGateInjector{
				Client:  mgr.GetClient(),
				decoder: dec,
			},
		},
	)
	return nil
}
