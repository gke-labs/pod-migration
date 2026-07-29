package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
)

// +kubebuilder:rbac:groups="",resources=persistentvolumeclaims,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs,verbs=get;list;watch
// EvictionGate handles eviction requests and creates PodMigrationJobs.
type EvictionGate struct {
	Client  client.Client
	decoder admission.Decoder
}

// Handle intercepts eviction requests.
func (a *EvictionGate) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)
	logger.Info("Intercepted eviction request", "subresource", req.SubResource)

	if req.SubResource != "eviction" {
		return admission.Allowed("not an eviction request")
	}

	// Fetch the Pod
	pod := &corev1.Pod{}
	err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Pod already deleted, allowing eviction")
			return admission.Allowed("pod already deleted")
		}
		logger.Error(err, "Failed to get pod")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Check if feature is enabled for this pod
	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		logger.Info("Feature not enabled for pod, allowing eviction")
		return admission.Allowed("feature not enabled")
	}

	// Check if pod uses gvisor runtime
	if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
		logger.Info("Pod does not use gvisor runtime, allowing eviction immediately", "runtimeClassName", pod.Spec.RuntimeClassName)
		return admission.Allowed("Pod does not use gvisor runtime, skipping migration")
	}

	// Inspect volumes to determine if we should orchestrate migration (Approach 1 for RWO) or let runtime intercept (Approach 2 for RWX/diskless)
	hasRWO := false
	for _, vol := range pod.Spec.Volumes {
		if vol.PersistentVolumeClaim != nil {
			pvc := &corev1.PersistentVolumeClaim{}
			err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: vol.PersistentVolumeClaim.ClaimName}, pvc)
			if err != nil {
				if apierrors.IsNotFound(err) {
					logger.Info("PVC not found, assuming RWO to be safe", "pvc", vol.PersistentVolumeClaim.ClaimName)
					hasRWO = true
					break
				}
				logger.Error(err, "Failed to get PVC", "pvc", vol.PersistentVolumeClaim.ClaimName)
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

	// if !hasRWO {
	// 	logger.Info("Workload is diskless or uses only RWX volumes; bypassing eviction webhook for runtime interception")
	// 	return admission.Allowed("bypassing eviction webhook for RWX/diskless workload")
	// }

	// Define migration job name
	jobName := fmt.Sprintf("pmj-%s", req.Name)

	// Check if PodMigrationJob already exists
	job := &pmv1alpha1.PodMigrationJob{}
	err = a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: jobName}, job)
	if err == nil {
		// Job exists, check its status
		logger.Info("Migration job already exists", "job", jobName, "phase", job.Status.Phase)
		if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded {
			logger.Info("Migration job already succeeded, allowing eviction (no-op)")
			return admission.Allowed("migration succeeded")
		}
		return denied429(fmt.Sprintf("migration job in progress: status %s", job.Status.Phase))
	}

	if !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get PodMigrationJob")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Resolve parent owner details
	parentName := ""
	parentKind := ""
	for _, ref := range pod.OwnerReferences {
		if ref.Kind == "ReplicaSet" {
			rs := &appsv1.ReplicaSet{}
			err := a.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: ref.Name}, rs)
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

	jobLabels := map[string]string{}
	if parentName != "" {
		jobLabels["pod-migration.gke.io/parent-name"] = parentName
		jobLabels["pod-migration.gke.io/parent-kind"] = parentKind
	}

	// 1. Find matching PodSnapshotPolicy (manual + stop)
	pspList := &unstructured.UnstructuredList{}
	pspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicyList",
	})
	if err := a.Client.List(ctx, pspList, client.InNamespace(req.Namespace)); err != nil {
		logger.Error(err, "Failed to list PodSnapshotPolicies")
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to list PSPs: %w", err))
	}

	var matchingPSPName string
	for _, psp := range pspList.Items {
		selectorMap, found, err := unstructured.NestedMap(psp.Object, "spec", "selector")
		if err != nil || !found {
			continue
		}
		jsonBytes, err := json.Marshal(selectorMap)
		if err != nil {
			continue
		}
		var labelSelector metav1.LabelSelector
		if err := json.Unmarshal(jsonBytes, &labelSelector); err != nil {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(&labelSelector)
		if err != nil {
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			triggerType, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "type")
			if err == nil && found && triggerType == "manual" {
				postCheckpoint, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "postCheckpoint")
				if err == nil && found && postCheckpoint == "stop" {
					matchingPSPName = psp.GetName()
					break
				}
			}
		}
	}

	if matchingPSPName == "" {
		logger.Info("No matching manual+stop PodSnapshotPolicy found, attempting to create default")
		// List PodSnapshotStorageConfigs to find one to use
		psscList := &unstructured.UnstructuredList{}
		psscList.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotStorageConfigList",
		})
		if err := a.Client.List(ctx, psscList); err != nil {
			logger.Error(err, "Failed to list PodSnapshotStorageConfigs")
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to list PSSCs: %w", err))
		}
		if len(psscList.Items) == 0 {
			logger.Error(nil, "No PodSnapshotStorageConfig found in cluster")
			return denied429("no matching manual PodSnapshotPolicy found")
		}
		psscName := psscList.Items[0].GetName()

		// Create default manual policy
		defaultPSPName := "psp-default-manual"
		defaultPSP := &unstructured.Unstructured{}
		defaultPSP.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotPolicy",
		})
		defaultPSP.SetName(defaultPSPName)
		defaultPSP.SetNamespace(req.Namespace)
		defaultPSP.Object["spec"] = map[string]interface{}{
			"storageConfigName": psscName,
			"selector": map[string]interface{}{
				"matchLabels": map[string]string{
					"pod-migration.gke.io/enabled": "true",
				},
			},
			"triggerConfig": map[string]interface{}{
				"type":           "manual",
				"postCheckpoint": "stop",
			},
		}
		err := a.Client.Create(ctx, defaultPSP)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			logger.Error(err, "Failed to create default PodSnapshotPolicy")
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("failed to create default PSP: %w", err))
		}
		matchingPSPName = defaultPSPName
		logger.Info("Created default manual+stop PodSnapshotPolicy", "name", defaultPSPName)
	}

	// Create new PodMigrationJob
	logger.Info("Creating PodMigrationJob", "job", jobName)
	newJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: req.Namespace,
			Labels:    jobLabels,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: req.Name,
			},
		},
	}

	err = a.Client.Create(ctx, newJob)
	if err != nil {
		logger.Error(err, "Failed to create PodMigrationJob")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return denied429("migration job spawned")
}

func denied429(msg string) admission.Response {
	return admission.Response{
		AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Code:    http.StatusTooManyRequests,
				Message: msg,
			},
		},
	}
}

// InjectDecoder injects the decoder.
func (a *EvictionGate) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}

// SetupEvictionWebhookWithManager registers the webhook on the manager.
func SetupEvictionWebhookWithManager(mgr ctrl.Manager) error {
	dec := admission.NewDecoder(mgr.GetScheme())
	mgr.GetWebhookServer().Register(
		"/validate--v1-pod-eviction",
		&admission.Webhook{
			Handler: &EvictionGate{
				Client:  mgr.GetClient(),
				decoder: dec,
			},
		},
	)
	return nil
}
