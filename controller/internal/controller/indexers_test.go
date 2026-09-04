package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPodAssignedPMJIndex_ListsOnlyAssignedPods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	assigned := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "assigned-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "pmj-a",
			},
		},
	}
	otherPMJ := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "pmj-b",
			},
		},
	}
	unassigned := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unassigned-pod",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(assigned, otherPMJ, unassigned).
		Build()

	podList := &corev1.PodList{}
	err := cl.List(context.Background(), podList,
		client.InNamespace("default"),
		client.MatchingFields{PodAssignedPMJIndex: "pmj-a"})
	if err != nil {
		t.Fatalf("indexed list failed: %v", err)
	}

	if len(podList.Items) != 1 {
		t.Fatalf("expected exactly 1 pod indexed under pmj-a, got %d", len(podList.Items))
	}
	if podList.Items[0].Name != "assigned-pod" {
		t.Errorf("expected assigned-pod, got %s", podList.Items[0].Name)
	}
}
