package webhook

import (
	"context"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestPodGateInjector(t *testing.T) {
	tests := []struct {
		name            string
		pod             *corev1.Pod
		expectedAllowed bool
		expectedPatched bool
	}{
		{
			name: "Pod not opted in",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			expectedAllowed: true,
			expectedPatched: false,
		},
		{
			name: "Pod opted in, gate injected",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
			},
			expectedAllowed: true,
			expectedPatched: true,
		},
		{
			name: "Pod already has gate",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			expectedAllowed: true,
			expectedPatched: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			dec := admission.NewDecoder(scheme)

			cl := fake.NewClientBuilder().WithScheme(scheme).Build()

			handler := &PodGateInjector{
				Client:  cl,
				decoder: dec,
			}

			rawPod, err := json.Marshal(tt.pod)
			if err != nil {
				t.Fatalf("Failed to marshal pod: %v", err)
			}

			req := admission.Request{}
			req.Namespace = tt.pod.Namespace
			req.Name = tt.pod.Name
			req.Object = runtime.RawExtension{Raw: rawPod}

			resp := handler.Handle(context.Background(), req)

			if tt.expectedAllowed != resp.Allowed {
				t.Errorf("Expected allowed %t, got %t", tt.expectedAllowed, resp.Allowed)
			}

			hasPatches := len(resp.Patches) > 0
			if tt.expectedPatched != hasPatches {
				t.Errorf("Expected patched %t, got %t (patches: %+v)", tt.expectedPatched, hasPatches, resp.Patches)
			}

			// If patched, verify the gate was actually added
			if tt.expectedPatched {
				gateAdded := false
				for _, patch := range resp.Patches {
					if patch.Operation == "add" && patch.Path == "/spec/schedulingGates" {
						gateAdded = true
						break
					}
					// Also handle appending case if schedulingGates already exists (though empty in test)
					if patch.Operation == "add" && patch.Path == "/spec/schedulingGates/-" {
						gateAdded = true
						break
					}
				}
				if !gateAdded {
					t.Errorf("Expected patch to add scheduling gate, but patches were: %+v", resp.Patches)
				}
			}
		})
	}
}
