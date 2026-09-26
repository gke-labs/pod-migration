package util

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestIsNodePreempting(t *testing.T) {
	tests := []struct {
		name     string
		node     *corev1.Node
		expected bool
	}{
		{
			name:     "nil node",
			node:     nil,
			expected: false,
		},
		{
			name: "healthy node without taints or conditions",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "healthy-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: false,
		},
		{
			name: "node with cloud.google.com/impending-node-termination taint",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "preempting-node-taint"},
				Spec: corev1.NodeSpec{
					Taints: []corev1.Taint{
						{Key: TaintImpendingNodeTermination, Effect: corev1.TaintEffectNoSchedule},
					},
				},
			},
			expected: true,
		},
		{
			name: "node with node.kubernetes.io/out-of-service taint",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "out-of-service-node"},
				Spec: corev1.NodeSpec{
					Taints: []corev1.Taint{
						{Key: TaintOutOfService, Effect: corev1.TaintEffectNoExecute},
					},
				},
			},
			expected: true,
		},
		{
			name: "node with impending-node-termination condition True",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "cond-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: ConditionImpendingNodeTermination, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "node with Terminating condition True",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "terminating-cond-node"},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{
						{Type: ConditionTerminating, Status: corev1.ConditionTrue},
					},
				},
			},
			expected: true,
		},
		{
			name: "node with maintenance label ongoing",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "maintenance-node",
					Labels: map[string]string{
						LabelActiveNodeMaintenance: ValueMaintenanceOngoing,
					},
				},
			},
			expected: true,
		},
		{
			name: "node with preemption annotation true",
			node: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "annotated-node",
					Annotations: map[string]string{
						TaintImpendingNodeTermination: "true",
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsNodePreempting(tt.node)
			if got != tt.expected {
				t.Errorf("IsNodePreempting() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

func TestFormatBytes(t *testing.T) {
	cases := []struct {
		in       int64
		expected string
	}{
		{500, "500B"},
		{1024, "1.00Ki"},
		{10 * 1024 * 1024, "10.00Mi"},
		{15 * 1024 * 1024 * 1024, "15.00Gi"},
		{1536 * 1024 * 1024, "1.50Gi"},
	}
	for _, c := range cases {
		if got := FormatBytes(c.in); got != c.expected {
			t.Errorf("FormatBytes(%d) = %s, expected %s", c.in, got, c.expected)
		}
	}
}

func TestBuildPMJLabelsAndAnnotations(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels: map[string]string{
				appsv1.DefaultDeploymentUniqueLabelKey: "hash123",
				LabelJobCompletionIndex:                "0",
			},
			Annotations: map[string]string{
				AnnotationMigrationTimeout: "20m",
			},
		},
	}

	psp := &unstructured.Unstructured{}
	psp.SetName("psp-test-policy")

	labels, annotations := BuildPMJLabelsAndAnnotations(
		pod, "my-deploy", "Deployment", "uid-123", psp, 10*time.Minute, TriggerSourceSpotPreemption,
	)

	if labels[LabelParentName] != "my-deploy" || labels[LabelParentKind] != "Deployment" || labels[LabelParentUID] != "uid-123" {
		t.Errorf("Unexpected parent labels: %v", labels)
	}
	if labels[LabelPodTemplateHash] != "hash123" {
		t.Errorf("Expected pod template hash label, got %s", labels[LabelPodTemplateHash])
	}
	if labels[LabelTriggerSource] != TriggerSourceSpotPreemption {
		t.Errorf("Expected trigger source label %s, got %s", TriggerSourceSpotPreemption, labels[LabelTriggerSource])
	}

	if annotations[AnnotationTriggerSource] != TriggerSourceSpotPreemption {
		t.Errorf("Expected trigger source annotation %s, got %s", TriggerSourceSpotPreemption, annotations[AnnotationTriggerSource])
	}
	if annotations[AnnotationPodSnapshotPolicy] != "psp-test-policy" {
		t.Errorf("Expected PSP annotation psp-test-policy, got %s", annotations[AnnotationPodSnapshotPolicy])
	}
	if annotations[AnnotationMigrationTimeout] != "20m" {
		t.Errorf("Expected migration timeout 20m, got %s", annotations[AnnotationMigrationTimeout])
	}
}

func TestFindLatestReadyManualStopPSP(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	createPSP := func(name, triggerType, postCheckpoint, readyStatus string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotPolicy",
		})
		u.SetName(name)
		u.SetNamespace("default")
		u.Object["spec"] = map[string]interface{}{
			"triggerConfig": map[string]interface{}{
				"type":           triggerType,
				"postCheckpoint": postCheckpoint,
			},
			"selector": map[string]interface{}{
				"matchLabels": map[string]interface{}{
					"app": "redis",
				},
			},
		}
		u.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": readyStatus,
				},
			},
		}
		return u
	}

	now := time.Now()
	validPSP := createPSP("psp-valid", "manual", "stop", "True")
	validPSP.SetCreationTimestamp(metav1.NewTime(now.Add(10 * time.Second)))

	notReadyPSP := createPSP("psp-not-ready", "manual", "stop", "False")
	notReadyPSP.SetCreationTimestamp(metav1.NewTime(now))

	resumePSP := createPSP("psp-resume", "manual", "resume", "True")
	resumePSP.SetCreationTimestamp(metav1.NewTime(now.Add(-10 * time.Second)))

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(notReadyPSP, resumePSP, validPSP).
		Build()

	psp, err := FindLatestReadyManualStopPSP(context.Background(), cl, "default", map[string]string{"app": "redis"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if psp == nil || psp.GetName() != "psp-valid" {
		t.Fatalf("expected psp-valid, got %v", psp)
	}

	// Pod with non-matching labels
	pspMismatch, err := FindLatestReadyManualStopPSP(context.Background(), cl, "default", map[string]string{"app": "non-matching"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pspMismatch != nil {
		t.Fatalf("expected nil for mismatched labels, got %v", pspMismatch)
	}
}
