package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestPodReconciler(t *testing.T) {
	tests := []struct {
		name            string
		pod             *corev1.Pod
		existingObjects []client.Object
		expectedRequeue bool
		verify          func(t *testing.T, c client.Client)
	}{
		{
			name: "Feature not enabled, do nothing",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Annotations: map[string]string{
						"pod-migration.gke.io/snapshot-requested": "true",
					},
				},
			},
			expectedRequeue: false,
			verify: func(t *testing.T, c client.Client) {
				trigger := &unstructured.Unstructured{}
				trigger.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   "podsnapshot.gke.io",
					Version: "v1",
					Kind:    "PodSnapshotManualTrigger",
				})
				err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "trigger-test-pod"}, trigger)
				if err == nil {
					t.Errorf("Expected trigger to NOT be created")
				}
			},
		},
		{
			name: "Snapshot requested, create trigger",
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
			expectedRequeue: false,
			verify: func(t *testing.T, c client.Client) {
				trigger := &unstructured.Unstructured{}
				trigger.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   "podsnapshot.gke.io",
					Version: "v1",
					Kind:    "PodSnapshotManualTrigger",
				})
				err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "trigger-test-pod"}, trigger)
				if err != nil {
					t.Errorf("Expected trigger to be created, got error: %v", err)
				}

				targetPod, _, _ := unstructured.NestedString(trigger.Object, "spec", "targetPod")
				if targetPod != "test-pod" {
					t.Errorf("Expected targetPod to be test-pod, got %s", targetPod)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)

			objects := []client.Object{tt.pod}
			objects = append(objects, tt.existingObjects...)

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()

			r := &PodReconciler{Client: fakeClient, Scheme: scheme}

			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKey{Namespace: tt.pod.Namespace, Name: tt.pod.Name},
			})

			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			if res.Requeue != tt.expectedRequeue {
				t.Errorf("Expected requeue %v, got %v", tt.expectedRequeue, res.Requeue)
			}

			if tt.verify != nil {
				tt.verify(t, fakeClient)
			}
		})
	}
}
