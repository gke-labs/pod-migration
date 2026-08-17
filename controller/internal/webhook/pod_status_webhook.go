package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// PodStatusMutator intercepts pod status updates and mutates Succeeded to Failed for migrating pods.
type PodStatusMutator struct {
	Client  client.Client
	decoder admission.Decoder
}

// Handle inspects pod status updates and mutates the phase and exit codes if the pod is migrating.
func (a *PodStatusMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	// Only intercept status updates
	if req.SubResource != "status" {
		return admission.Allowed("not a status update")
	}

	pod := &corev1.Pod{}
	err := a.decoder.Decode(req, pod)
	if err != nil {
		logger.Error(err, "Failed to decode pod status update")
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Check if pod opted in
	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		return admission.Allowed("pod not opted in")
	}

	// Only intercept Pods owned by a Job (StatefulSets/Deployments handle rescheduling natively)
	isJob := false
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "Job" {
			isJob = true
			break
		}
	}
	if !isJob {
		return admission.Allowed("pod is not owned by a Job")
	}

	// We only care if the pod is transitioning to Succeeded phase
	if pod.Status.Phase != corev1.PodSucceeded {
		return admission.Allowed("pod phase is not Succeeded")
	}

	// Check if there is an active PodMigrationJob for this pod
	pmjName := fmt.Sprintf("pmj-%s", pod.Name)
	pmj := &pmv1alpha1.PodMigrationJob{}
	err = a.Client.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pmjName}, pmj)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Pod succeeded but no active PodMigrationJob found, allowing normal completion")
			return admission.Allowed("no active migration job")
		}
		logger.Error(err, "Failed to check for active PodMigrationJob")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Verify if pmj.Spec.TargetPodUID matches string(pod.UID). If not, log it and return admission.Allowed("PMJ UID mismatch").
	if pmj.Spec.TargetPodUID != string(pod.UID) {
		logger.Info("Pod UID mismatch with PodMigrationJob target UID, allowing normal completion", "pmjUID", pmj.Spec.TargetPodUID, "podUID", pod.UID)
		return admission.Allowed("PMJ UID mismatch")
	}

	// Verify if pod.DeletionTimestamp is nil. If it is nil, log it and return admission.Allowed("pod completed naturally").
	if pod.DeletionTimestamp == nil {
		logger.Info("Pod DeletionTimestamp is nil, allowing normal completion", "pod", pod.Name)
		return admission.Allowed("pod completed naturally")
	}

	// If PMJ exists, it means this pod is undergoing migration.
	// Since Kubelet is trying to mark it Succeeded (exit 0), we override it to Failed (exit 137)
	// to trigger rescheduling by the Job controller.
	logger.Info("Intercepted Succeeded status update for migrating pod; forcing failure state to trigger rescheduling", "pmj", pmjName)

	pod.Status.Phase = corev1.PodFailed
	pod.Status.Reason = "EvictedByMigration"
	pod.Status.Message = "Pod was evicted and checkpointed during migration. Forcing failure to trigger rescheduling."

	// Mutate container exit codes to 137 to ensure Job controller treats it as failure
	// and to allow podFailurePolicy to match it.
	for i := range pod.Status.ContainerStatuses {
		cs := &pod.Status.ContainerStatuses[i]
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode == 0 {
			cs.State.Terminated.ExitCode = 137
			cs.State.Terminated.Reason = "CheckpointBeforeDelete"
			cs.State.Terminated.Message = "Container was checkpointed and stopped during migration."
		}
	}

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		logger.Error(err, "Failed to marshal mutated pod status")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// InjectDecoder injects the decoder.
func (a *PodStatusMutator) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}

// SetupStatusWebhookWithManager registers the mutating status webhook on the manager.
func SetupStatusWebhookWithManager(mgr ctrl.Manager) error {
	dec := admission.NewDecoder(mgr.GetScheme())
	mgr.GetWebhookServer().Register(
		"/mutate-v1-pod-status",
		&admission.Webhook{
			Handler: &PodStatusMutator{
				Client:  mgr.GetClient(),
				decoder: dec,
			},
		},
	)
	return nil
}
