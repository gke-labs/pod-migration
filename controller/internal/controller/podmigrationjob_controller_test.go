package controller

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
)

func TestPodMigrationJobReconciler_Pending(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigrationJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: podName,
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify PMJ transitioned to Snapshotting
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
	}

	// Verify PodSnapshotManualTrigger was created
	triggerName := fmt.Sprintf("trigger-%s", podName)
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, trigger)
	if err != nil {
		t.Errorf("Expected PodSnapshotManualTrigger to be created, got error: %v", err)
	} else {
		targetPod, found, err := unstructured.NestedString(trigger.Object, "spec", "targetPod")
		if err != nil || !found || targetPod != podName {
			t.Errorf("Expected PSMT spec.targetPod to be %s, got %s (found: %v, err: %v)", podName, targetPod, found, err)
		}
	}
}
