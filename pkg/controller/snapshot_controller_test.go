package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	ctrl "sigs.k8s.io/controller-runtime"
)

func TestSnapshotReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	tests := []struct {
		name            string
		snapshotName    string
		existingObjects []client.Object
		expectedRequeue bool
		verify          func(t *testing.T, client client.Client)
	}{
		{
			name:         "Snapshot ready, delete pod and trigger",
			snapshotName: "snapshot-test-pod",
			existingObjects: []client.Object{
				&corev1.Pod{
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
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "podsnapshot.gke.io/v1",
						"kind":       "PodSnapshotManualTrigger",
						"metadata": map[string]interface{}{
							"namespace": "default",
							"name":      "trigger-test-pod",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "podsnapshot.gke.io/v1",
						"kind":       "PodSnapshot",
						"metadata": map[string]interface{}{
							"namespace": "default",
							"name":      "snapshot-test-pod",
							"annotations": map[string]interface{}{
								"podsnapshot.gke.io/origin-pod": "test-pod",
							},
						},
						"status": map[string]interface{}{
							"conditions": []interface{}{
								map[string]interface{}{
									"type":   "Ready",
									"status": "True",
								},
							},
						},
					},
				},
			},
			expectedRequeue: false,
			verify: func(t *testing.T, c client.Client) {
				// Verify pod is deleted
				pod := &corev1.Pod{}
				err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-pod"}, pod)
				if err == nil {
					t.Errorf("Expected pod to be deleted, but it still exists")
				} else if !errors.IsNotFound(err) {
					t.Errorf("Expected NotFound error for pod, got %v", err)
				}

				// Verify trigger is deleted
				trigger := &unstructured.Unstructured{}
				trigger.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   "podsnapshot.gke.io",
					Version: "v1",
					Kind:    "PodSnapshotManualTrigger",
				})
				err = c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "trigger-test-pod"}, trigger)
				if err == nil {
					t.Errorf("Expected trigger to be deleted, but it still exists")
				} else if !errors.IsNotFound(err) {
					t.Errorf("Expected NotFound error for trigger, got %v", err)
				}
			},
		},
		{
			name:         "Snapshot ready, pod missing labels, skip deletion",
			snapshotName: "snapshot-test-pod",
			existingObjects: []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-pod",
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "podsnapshot.gke.io/v1",
						"kind":       "PodSnapshotManualTrigger",
						"metadata": map[string]interface{}{
							"namespace": "default",
							"name":      "trigger-test-pod",
						},
					},
				},
				&unstructured.Unstructured{
					Object: map[string]interface{}{
						"apiVersion": "podsnapshot.gke.io/v1",
						"kind":       "PodSnapshot",
						"metadata": map[string]interface{}{
							"namespace": "default",
							"name":      "snapshot-test-pod",
							"annotations": map[string]interface{}{
								"podsnapshot.gke.io/origin-pod": "test-pod",
							},
						},
						"status": map[string]interface{}{
							"conditions": []interface{}{
								map[string]interface{}{
									"type":   "Ready",
									"status": "True",
								},
							},
						},
					},
				},
			},
			expectedRequeue: false,
			verify: func(t *testing.T, c client.Client) {
				pod := &corev1.Pod{}
				err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "test-pod"}, pod)
				if err != nil {
					t.Errorf("Expected pod to still exist, got error: %v", err)
				}

				trigger := &unstructured.Unstructured{}
				trigger.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   "podsnapshot.gke.io",
					Version: "v1",
					Kind:    "PodSnapshotManualTrigger",
				})
				err = c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: "trigger-test-pod"}, trigger)
				if err != nil {
					t.Errorf("Expected trigger to still exist, got error: %v", err)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tt.existingObjects...).Build()

			r := &SnapshotReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: client.ObjectKey{Namespace: "default", Name: tt.snapshotName},
			})

			if err != nil {
				t.Fatal(err)
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
