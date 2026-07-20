package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestDeferredEvictionReconciler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)

	// Helper to create a deferred pod
	createPod := func(name string, enabled bool, deferred bool, processed bool, node string, deleting bool) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      name,
				Labels:    make(map[string]string),
			},
			Spec: corev1.PodSpec{
				NodeName: node,
			},
		}
		if enabled {
			pod.Labels["pod-migration.gke.io/enabled"] = "true"
		}
		if processed {
			pod.Labels["pod-migration.gke.io/deferred-eviction-processed"] = "true"
		}
		if deferred {
			pod.Status.Conditions = []corev1.PodCondition{
				{
					Type:   "PodResizePending",
					Reason: "Deferred",
					Status: corev1.ConditionTrue,
				},
			}
		}
		if deleting {
			now := metav1.Now()
			pod.DeletionTimestamp = &now
			pod.Finalizers = []string{"pod-migration.gke.io/test-finalizer"}
		}
		return pod
	}

	tests := []struct {
		name                      string
		initialObjects            []client.Object
		targetPodKey              client.ObjectKey
		expectedEvictedPods       []string
		expectedDeferredPodLabels map[string]string
		expectedRequeue           bool
	}{
		{
			name: "Pod not deferred, do nothing",
			initialObjects: []client.Object{
				createPod("pod-1", true, false, false, "node-1", false),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-1"},
			expectedEvictedPods:       nil,
			expectedDeferredPodLabels: map[string]string{"pod-migration.gke.io/enabled": "true"},
			expectedRequeue:           false,
		},
		{
			name: "Pod deferred but label missing, do nothing",
			initialObjects: []client.Object{
				createPod("pod-1", false, true, false, "node-1", false),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-1"},
			expectedEvictedPods:       nil,
			expectedDeferredPodLabels: map[string]string{},
			expectedRequeue:           false,
		},
		{
			name: "Pod deferred, no other migratable pods on node (Fallback)",
			initialObjects: []client.Object{
				createPod("pod-1", true, true, false, "node-1", false),
				createPod("pod-other-node", true, false, false, "node-2", false),
				createPod("pod-not-enabled", false, false, false, "node-1", false),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-1"},
			expectedEvictedPods:       []string{"pod-1"},
			expectedDeferredPodLabels: map[string]string{"pod-migration.gke.io/enabled": "true"}, // No processed label added on fallback eviction
			expectedRequeue:           false,
		},
		{
			name: "Pod deferred, other migratable pod exists (Success Path)",
			initialObjects: []client.Object{
				createPod("pod-deferred", true, true, false, "node-1", false),
				createPod("pod-candidate", true, false, false, "node-1", false),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-deferred"},
			expectedEvictedPods:       []string{"pod-candidate"},
			expectedDeferredPodLabels: map[string]string{
				"pod-migration.gke.io/enabled":                    "true",
				"pod-migration.gke.io/deferred-eviction-processed": "true",
			},
			expectedRequeue: false,
		},
		{
			name: "Pod deferred, other migratable pod exists but is being deleted",
			initialObjects: []client.Object{
				createPod("pod-deferred", true, true, false, "node-1", false),
				createPod("pod-deleting", true, false, false, "node-1", true),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-deferred"},
			expectedEvictedPods:       []string{"pod-deferred"}, // Fallback to itself
			expectedDeferredPodLabels: map[string]string{"pod-migration.gke.io/enabled": "true"},
			expectedRequeue:           false,
		},
		{
			name: "Pod deferred but ALREADY has processed label, do nothing",
			initialObjects: []client.Object{
				createPod("pod-deferred", true, true, true, "node-1", false),
				createPod("pod-candidate", true, false, false, "node-1", false),
			},
			targetPodKey:        client.ObjectKey{Namespace: "default", Name: "pod-deferred"},
			expectedEvictedPods: nil,
			expectedDeferredPodLabels: map[string]string{
				"pod-migration.gke.io/enabled":                    "true",
				"pod-migration.gke.io/deferred-eviction-processed": "true",
			},
			expectedRequeue: false,
		},
		{
			name: "Pod is NOT deferred but HAS processed label (Cleanup Path)",
			initialObjects: []client.Object{
				createPod("pod-cleanup", true, false, true, "node-1", false),
			},
			targetPodKey:              client.ObjectKey{Namespace: "default", Name: "pod-cleanup"},
			expectedEvictedPods:       nil,
			expectedDeferredPodLabels: map[string]string{"pod-migration.gke.io/enabled": "true"}, // Processed label removed
			expectedRequeue:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var evictedPods []string

			// Build fake client with indexer and interceptor
			fb := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tt.initialObjects...).
				WithIndex(&corev1.Pod{}, "spec.nodeName", func(rawObj client.Object) []string {
					pod := rawObj.(*corev1.Pod)
					if pod.Spec.NodeName == "" {
						return nil
					}
					return []string{pod.Spec.NodeName}
				}).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, cl client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
						if subResourceName == "eviction" {
							evictedPods = append(evictedPods, obj.GetName())
						}
						return nil
					},
				})

			fakeClient := fb.Build()

			r := &DeferredEvictionReconciler{Client: fakeClient, Scheme: scheme}

			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: tt.targetPodKey,
			})

			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			if res.Requeue != tt.expectedRequeue {
				t.Errorf("Expected requeue %v, got %v", tt.expectedRequeue, res.Requeue)
			}

			// Verify evicted pods
			if len(evictedPods) != len(tt.expectedEvictedPods) {
				t.Errorf("Expected evicted pods %v, got %v", tt.expectedEvictedPods, evictedPods)
			} else {
				for i := range evictedPods {
					if evictedPods[i] != tt.expectedEvictedPods[i] {
						t.Errorf("Expected evicted pod at index %d to be %s, got %s", i, tt.expectedEvictedPods[i], evictedPods[i])
					}
				}
			}

			// Verify labels on the target deferred pod
			updatedPod := &corev1.Pod{}
			err = fakeClient.Get(context.Background(), tt.targetPodKey, updatedPod)
			if err != nil {
				t.Fatalf("Failed to get target pod after reconcile: %v", err)
			}

			// Check that all expected labels are present and have correct values
			for k, v := range tt.expectedDeferredPodLabels {
				gotV, ok := updatedPod.Labels[k]
				if !ok {
					t.Errorf("Expected label %s missing from target pod", k)
				} else if gotV != v {
					t.Errorf("Expected label %s to be %s, got %s", k, v, gotV)
				}
			}
			// Check that no extra labels are present (or at least that the processed label is gone if not expected)
			if _, expectedProcessed := tt.expectedDeferredPodLabels["pod-migration.gke.io/deferred-eviction-processed"]; !expectedProcessed {
				if _, gotProcessed := updatedPod.Labels["pod-migration.gke.io/deferred-eviction-processed"]; gotProcessed {
					t.Errorf("Unexpected pod-migration.gke.io/deferred-eviction-processed label present on target pod")
				}
			}
		})
	}
}
