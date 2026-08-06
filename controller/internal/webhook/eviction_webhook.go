package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"time"

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
	// We must query the API server for the Pod because the eviction admission request object
	// only contains the Eviction subresource payload, which does not carry the parent Pod's labels.
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

	// 1. Find matching PodSnapshotPolicy (manual)
	matchingPSP, err := findLatestReadyPSP(ctx, a.Client, req.Namespace, pod.Labels)
	if err != nil {
		logger.Error(err, "Failed to resolve matching PodSnapshotPolicy")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// 2. Validate policy for Manual + Stop behavior
	isValid := false
	if matchingPSP != nil {
		postCheckpoint, found, err := unstructured.NestedString(matchingPSP.Object, "spec", "triggerConfig", "postCheckpoint")
		if err == nil && found && postCheckpoint == "stop" {
			isValid = true
		}
	}

	if !isValid {
		logger.Info("No matching ready manual+stop policy found, skipping migration and allowing eviction", "pod", req.Name)
		return admission.Allowed("skipping migration: no valid manual+stop policy found")
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
		"/validate-v1-pod-eviction",
		&admission.Webhook{
			Handler: &EvictionGate{
				Client:  mgr.GetClient(),
				decoder: dec,
			},
		},
	)
	return nil
}

// latestUpdate extracts the most recent "Update" timestamp from managed fields of unstructured object.
// Falls back to creation time if no update operation is found.
func latestUpdate(obj *unstructured.Unstructured) time.Time {
	var latest = obj.GetCreationTimestamp().Time
	for _, field := range obj.GetManagedFields() {
		if field.Operation != metav1.ManagedFieldsOperationUpdate {
			continue
		}
		if field.Time != nil && field.Time.After(latest) {
			latest = field.Time.Time
		}
	}
	return latest
}

// findLatestReadyPSP finds the latest ready manual PodSnapshotPolicy in the namespace matching the labels.
func findLatestReadyPSP(ctx context.Context, c client.Client, namespace string, podLabels map[string]string) (*unstructured.Unstructured, error) {
	pspList := &unstructured.UnstructuredList{}
	pspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicyList",
	})
	if err := c.List(ctx, pspList, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("failed to list PSPs: %w", err)
	}

	var matchingPSPs []*unstructured.Unstructured
	podLabelSet := labels.Set(podLabels)

	for i := range pspList.Items {
		psp := &pspList.Items[i]

		// 1. Verify trigger type is manual
		triggerType, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "type")
		if err != nil || !found || triggerType != "manual" {
			continue
		}

		// 2. Verify labels selector matches
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
		if !selector.Matches(podLabelSet) {
			continue
		}

		// 3. Verify status is Ready (condition Ready=True)
		conditions, found, err := unstructured.NestedSlice(psp.Object, "status", "conditions")
		if err != nil || !found {
			continue
		}
		isReady := false
		for _, condVal := range conditions {
			cond, ok := condVal.(map[string]interface{})
			if !ok {
				continue
			}
			if cond["type"] == "Ready" && cond["status"] == "True" {
				isReady = true
				break
			}
		}
		if !isReady {
			continue
		}

		matchingPSPs = append(matchingPSPs, psp)
	}

	if len(matchingPSPs) == 0 {
		return nil, nil // No matching ready policy found
	}

	// Sort by latest updated time (descending)
	slices.SortStableFunc(matchingPSPs, func(a, b *unstructured.Unstructured) int {
		return latestUpdate(b).Compare(latestUpdate(a))
	})

	return matchingPSPs[0], nil
}
