package util

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// TaintImpendingNodeTermination is applied by GKE during scheduled host maintenance / node termination.
	TaintImpendingNodeTermination = "cloud.google.com/impending-node-termination"

	// LabelActiveNodeMaintenance is applied by GKE during active host maintenance.
	LabelActiveNodeMaintenance = "cloud.google.com/active-node-maintenance"
	ValueMaintenanceOngoing    = "ONGOING"

	// PreemptionDefaultTimeout is the migration deadline for spot preemption triggers (2 minutes),
	// bounding PMJ duration so doomed migrations fail fast rather than waiting on departed nodes.
	PreemptionDefaultTimeout = 2 * time.Minute
)

// IsNodePreempting returns true if the Node exhibits any verified GKE or Kubernetes
// preemption, impending termination, or graceful shutdown signal:
// 1. GKE host maintenance taint: cloud.google.com/impending-node-termination
// 2. GKE host maintenance label: cloud.google.com/active-node-maintenance=ONGOING
// 3. Kubelet graceful node shutdown: NodeReady condition Status=False with
//    Reason "KubeletNotReady" or "NodeShuttingDown", and message containing "node is shutting down".
func IsNodePreempting(node *corev1.Node) bool {
	if node == nil {
		return false
	}

	// 1. Taints: GKE impending node termination taint
	for _, taint := range node.Spec.Taints {
		if taint.Key == TaintImpendingNodeTermination {
			return true
		}
	}

	// 2. Labels: GKE active host maintenance
	if node.Labels != nil && node.Labels[LabelActiveNodeMaintenance] == ValueMaintenanceOngoing {
		return true
	}

	// 3. Conditions: Kubelet Graceful Node Shutdown sets NodeReady to False with reason
	// KubeletNotReady or NodeShuttingDown and message indicating the node is shutting down.
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady && cond.Status == corev1.ConditionFalse {
			if cond.Reason == "KubeletNotReady" || cond.Reason == "NodeShuttingDown" {
				if strings.Contains(strings.ToLower(cond.Message), "node is shutting down") {
					return true
				}
			}
		}
	}

	return false
}

// IsPodFailedDueToNodeShutdown returns true if the pod phase is Failed and the reason, condition,
// or status message indicates it was terminated by kubelet during node graceful shutdown.
func IsPodFailedDueToNodeShutdown(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	if pod.Status.Phase != corev1.PodFailed {
		return false
	}
	if pod.Status.Reason == "NodeShutdown" || pod.Status.Reason == "Terminated" {
		return true
	}
	msg := strings.ToLower(pod.Status.Message)
	if strings.Contains(msg, "node is shutting down") || strings.Contains(msg, "node shutdown") || strings.Contains(msg, "imminent node shutdown") {
		return true
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.DisruptionTarget && cond.Reason == "TerminationByKubelet" {
			return true
		}
	}
	return false
}

// CalculatePodMemoryFootprint returns the memory footprint of a pod in bytes, and true if known.
// It first checks container and persistent sidecar init container memory requests.
// If requests are zero, it falls back to memory limits.
// If both requests and limits are zero (e.g. BestEffort QoS), it returns (0, false).
func CalculatePodMemoryFootprint(pod *corev1.Pod) (int64, bool) {
	if pod == nil {
		return 0, false
	}
	reqBytes := CalculatePodMemoryRequest(pod)
	if reqBytes > 0 {
		return reqBytes, true
	}
	var limitBytes int64
	for _, c := range pod.Spec.Containers {
		if lim := c.Resources.Limits.Memory(); lim != nil {
			limitBytes += lim.Value()
		}
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			if lim := c.Resources.Limits.Memory(); lim != nil {
				limitBytes += lim.Value()
			}
		}
	}
	if limitBytes > 0 {
		return limitBytes, true
	}
	return 0, false
}

// FindLatestReadyManualStopPSP finds the latest ready manual PodSnapshotPolicy in the namespace
// matching the labels, and verifies that its postCheckpoint behavior is set to "stop".
func FindLatestReadyManualStopPSP(ctx context.Context, c client.Reader, namespace string, podLabels map[string]string) (*unstructured.Unstructured, error) {
	pspList := &unstructured.UnstructuredList{}
	pspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicyList",
	})
	if err := c.List(ctx, pspList, client.InNamespace(namespace)); err != nil {
		if meta.IsNoMatchError(err) {
			// Tolerate missing CRDs (fail-open)
			return nil, nil
		}
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
		return LatestUpdate(b).Compare(LatestUpdate(a))
	})

	latestPSP := matchingPSPs[0]

	// 4. Validate latest policy for "stop" behavior
	postCheckpoint, found, err := unstructured.NestedString(latestPSP.Object, "spec", "triggerConfig", "postCheckpoint")
	if err == nil && found && postCheckpoint == "stop" {
		return latestPSP, nil
	}

	return nil, nil // Return nil if the latest policy is not a "stop" policy
}

// LatestUpdate extracts the most recent "Update" timestamp from managed fields of unstructured object.
// Falls back to creation time if no update operation is found.
func LatestUpdate(obj *unstructured.Unstructured) time.Time {
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

// BuildPMJLabelsAndAnnotations constructs standardized labels and annotations for a newly spawned PodMigrationJob.
func BuildPMJLabelsAndAnnotations(
	pod *corev1.Pod,
	parentName, parentKind, parentUID string,
	matchingPSP *unstructured.Unstructured,
	defaultTimeout time.Duration,
	triggerSource string,
) (map[string]string, map[string]string) {
	jobLabels := make(map[string]string)
	if parentName != "" {
		jobLabels[LabelParentName] = parentName
		jobLabels[LabelParentKind] = parentKind
		if parentUID != "" {
			jobLabels[LabelParentUID] = parentUID
		}
	}
	if pod != nil && pod.Labels != nil {
		if hash, ok := pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]; ok && hash != "" {
			jobLabels[LabelPodTemplateHash] = hash
		}
		if rev, ok := pod.Labels[appsv1.ControllerRevisionHashLabelKey]; ok && rev != "" {
			jobLabels[LabelControllerRevisionHash] = rev
		}
		if idx, ok := pod.Labels[LabelJobCompletionIndex]; ok && idx != "" {
			jobLabels[LabelJobCompletionIndex] = idx
		}
	}
	if triggerSource != "" {
		jobLabels[LabelTriggerSource] = triggerSource
	}

	var jobAnnotations map[string]string
	baseTimeout := defaultTimeout
	if baseTimeout <= 0 {
		baseTimeout = DefaultMigrationTimeout
	}
	var rawTimeout string
	if pod != nil && pod.Annotations != nil {
		rawTimeout = pod.Annotations[AnnotationMigrationTimeout]
	}
	memBytes := CalculatePodMemoryRequest(pod)
	effectiveTimeout := CalculateMigrationTimeout(rawTimeout, memBytes, baseTimeout)
	if effectiveTimeout != baseTimeout || rawTimeout != "" {
		if jobAnnotations == nil {
			jobAnnotations = make(map[string]string)
		}
		if _, ok := ParseClampedTimeout(rawTimeout); ok {
			jobAnnotations[AnnotationMigrationTimeout] = rawTimeout
		} else if effectiveTimeout != baseTimeout {
			jobAnnotations[AnnotationMigrationTimeout] = effectiveTimeout.String()
		}
	}

	if matchingPSP != nil && matchingPSP.GetName() != "" {
		if jobAnnotations == nil {
			jobAnnotations = make(map[string]string)
		}
		jobAnnotations[AnnotationPodSnapshotPolicy] = matchingPSP.GetName()
	}

	if triggerSource != "" {
		if jobAnnotations == nil {
			jobAnnotations = make(map[string]string)
		}
		jobAnnotations[AnnotationTriggerSource] = triggerSource
	}

	return jobLabels, jobAnnotations
}

// FormatBytes formats byte counts into human-readable binary representations (Gi, Mi, Ki, B).
func FormatBytes(b int64) string {
	const (
		unit = 1024
		gib  = unit * unit * unit
		mib  = unit * unit
		kib  = unit
	)
	if b >= gib {
		return fmt.Sprintf("%.2fGi", float64(b)/float64(gib))
	}
	if b >= mib {
		return fmt.Sprintf("%.2fMi", float64(b)/float64(mib))
	}
	if b >= kib {
		return fmt.Sprintf("%.2fKi", float64(b)/float64(kib))
	}
	return fmt.Sprintf("%dB", b)
}
