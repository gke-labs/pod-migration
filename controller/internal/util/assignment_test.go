package util

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestFindUnassignedActivePMJ_BarePod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name      string
		podName   string
		existing  []runtime.Object
		expected  string
		expectErr bool
	}{
		{
			name:    "Bare pod, active PMJ, no assignment",
			podName: "my-bare-pod",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-my-bare-pod",
						Namespace: "default",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
			},
			expected: "pmj-my-bare-pod",
		},
		{
			name:    "Bare pod, active PMJ, already assigned to another pod",
			podName: "my-bare-pod",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-my-bare-pod",
						Namespace: "default",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-bare-pod-terminating",
						Namespace: "default",
						Annotations: map[string]string{
							"pod-migration.gke.io/assigned-pmj": "pmj-my-bare-pod",
						},
					},
				},
			},
			expected: "", // Should not return the PMJ because it's already assigned
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, "", "")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}
