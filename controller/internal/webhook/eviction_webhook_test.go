package webhook

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

func TestEvictionGate(t *testing.T) {
	tests := []struct {
		name            string
		pod             *corev1.Pod
		subResource     string
		expectedAllowed bool
		expectedDenied  bool
		expectedMessage string
	}{
		{
			name: "Not an eviction request",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			subResource:     "status",
			expectedAllowed: true,
			expectedMessage: "not an eviction request",
		},
		{
			name: "Feature not enabled",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "feature not enabled",
		},
		{
			name: "Snapshot requested but not ready",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					Annotations: map[string]string{
						"pod-migration.gke.io/snapshot-requested": "true",
					},
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "bypassing eviction webhook for RWX/diskless workload",
		},
		{
			name: "Trigger snapshot",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "bypassing eviction webhook for RWX/diskless workload",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.pod).Build()

			handler := &EvictionGate{Client: fakeClient}

			req := admission.Request{}
			req.Namespace = tt.pod.Namespace
			req.Name = tt.pod.Name
			req.SubResource = tt.subResource

			resp := handler.Handle(context.Background(), req)

			if tt.expectedAllowed && !resp.Allowed {
				t.Errorf("Expected allowed, got denied: %s", resp.Result.Message)
			}

			if tt.expectedDenied && resp.Allowed {
				t.Errorf("Expected denied, got allowed")
			}

			if tt.expectedMessage != "" && resp.Result.Message != tt.expectedMessage {
				t.Errorf("Expected message %q, got %q", tt.expectedMessage, resp.Result.Message)
			}

			// Note: Trigger snapshot annotation is not verified in Approach 2 since it does not modify the pod annotation.
		})
	}
}
