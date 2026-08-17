package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
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
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
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

func TestPodMigrationJobReconciler_Pending_WithPVs(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	pvcName := "test-pvc"
	pvName := "test-pv"
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
		Spec: corev1.PodSpec{
			Volumes: []corev1.Volume{
				{
					Name: "vol-1",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcName,
						},
					},
				},
			},
		},
	}

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigrationJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
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
		WithObjects(pod, pvc, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// Reconcile: should analyze PVs, populate PVsToDetach, create manual trigger and transition to Snapshotting
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected reconcile to request requeue")
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase to transition to %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
	}

	if len(updatedPMJ.Status.PVsToDetach) != 1 || updatedPMJ.Status.PVsToDetach[0] != pvName {
		t.Errorf("Expected PVsToDetach to contain %q, got %v", pvName, updatedPMJ.Status.PVsToDetach)
	}

	// Verify trigger IS created
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
	}
}

func TestPodMigrationJobReconciler_Pending_PodNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	pmj := &pmv1alpha1.PodMigrationJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigrationJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
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
		WithObjects(pmj). // Pod is NOT created
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue, got: %+v", res)
	}

	// Verify PMJ transitioned to Failed
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}
}

func TestPodMigrationJobReconciler_Timeout(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	tests := []struct {
		name         string
		initialPhase pmv1alpha1.PodMigrationJobPhase
	}{
		{
			name:         "Pending phase timeout",
			initialPhase: pmv1alpha1.PodMigrationJobPhasePending,
		},
		{
			name:         "Snapshotting phase timeout",
			initialPhase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
		{
			name:         "Evicting phase timeout",
			initialPhase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Mocking job creation 15 minutes ago
			creationTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))

			pmj := &pmv1alpha1.PodMigrationJob{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigrationJob",
				},
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         namespace,
					Name:              jobName,
					CreationTimestamp: creationTime,
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{
						Name: podName,
					},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase: tc.initialPhase,
				},
			}

			fakeClient := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(pmj).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				Build()

			r := &PodMigrationJobReconciler{
				Client: fakeClient,
				Scheme: scheme,
			}

			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      jobName,
				},
			})
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res.Requeue || res.RequeueAfter != 0 {
				t.Errorf("Expected no requeue, got: %+v", res)
			}

			// Verify PMJ transitioned to Failed
			updatedPMJ := &pmv1alpha1.PodMigrationJob{}
			err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
			if err != nil {
				t.Fatalf("Failed to get PMJ: %v", err)
			}
			if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
				t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
			}
			if updatedPMJ.Status.CompletionTime == nil {
				t.Errorf("Expected CompletionTime to be set")
			}
		})
	}
}

func TestPodMigrationJobReconciler_Pending_StaleTrigger(t *testing.T) {
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

	t.Run("Scenario 1: Stale Trigger Deletion", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				UID:               "current-job-uid",
				CreationTimestamp: metav1.Now(),
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

		// Create stale trigger with different owner UID
		triggerName := fmt.Sprintf("trigger-%s", podName)
		staleTrigger := &unstructured.Unstructured{}
		staleTrigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		staleTrigger.SetName(triggerName)
		staleTrigger.SetNamespace(namespace)
		staleTrigger.Object["spec"] = map[string]interface{}{
			"targetPod": podName,
		}
		staleTrigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       "old-job",
				UID:        "old-job-uid",
			},
		})

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pod, pmj, staleTrigger).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			Build()

		r := &PodMigrationJobReconciler{
			Client: fakeClient,
			Scheme: scheme,
		}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		if !res.Requeue {
			t.Errorf("Expected Requeue to be true, got %v", res.Requeue)
		}

		// Verify PMJ is still in Pending
		updatedPMJ := &pmv1alpha1.PodMigrationJob{}
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
		if err != nil {
			t.Fatalf("Failed to get PMJ: %v", err)
		}
		if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
			t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhasePending, updatedPMJ.Status.Phase)
		}

		// Verify stale trigger is deleted
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, trigger)
		if err == nil {
			t.Errorf("Expected stale trigger to be deleted, but it still exists")
		} else if !apierrors.IsNotFound(err) {
			t.Errorf("Expected NotFound error, got %v", err)
		}
	})

	t.Run("Scenario 2: Owned Trigger Kept", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				UID:               "current-job-uid",
				CreationTimestamp: metav1.Now(),
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

		// Create owned trigger with matching owner UID
		triggerName := fmt.Sprintf("trigger-%s", podName)
		ownedTrigger := &unstructured.Unstructured{}
		ownedTrigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		ownedTrigger.SetName(triggerName)
		ownedTrigger.SetNamespace(namespace)
		ownedTrigger.Object["spec"] = map[string]interface{}{
			"targetPod": podName,
		}
		isController := true
		ownedTrigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        "current-job-uid",
				Controller: &isController,
			},
		})

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pod, pmj, ownedTrigger).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			Build()

		r := &PodMigrationJobReconciler{
			Client: fakeClient,
			Scheme: scheme,
		}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		if !res.Requeue {
			t.Errorf("Expected Requeue to be true, got %v", res.Requeue)
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

		// Verify trigger still exists
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, trigger)
		if err != nil {
			t.Errorf("Expected trigger to exist, got error: %v", err)
		}
	})
}

func TestPodMigrationJobReconciler_Snapshotting(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	triggerName := "trigger-" + podName

	t.Run("Test Case 1 (Success)", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			},
		}

		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		trigger.SetName(triggerName)
		trigger.SetNamespace(namespace)
		trigger.Object["status"] = map[string]interface{}{
			"snapshotCreated": map[string]interface{}{
				"name": "my-snap",
			},
		}

		snapshot := &unstructured.Unstructured{}
		snapshot.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshot",
		})
		snapshot.SetName("my-snap")
		snapshot.SetNamespace(namespace)
		snapshot.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pmj, trigger, snapshot).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			Build()

		r := &PodMigrationJobReconciler{
			Client: fakeClient,
			Scheme: scheme,
		}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		if !res.Requeue {
			t.Errorf("Expected Requeue to be true, got %v", res.Requeue)
		}

		updatedPMJ := &pmv1alpha1.PodMigrationJob{}
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
		if err != nil {
			t.Fatalf("Failed to get PMJ: %v", err)
		}

		if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
			t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseEvicting, updatedPMJ.Status.Phase)
		}
		if updatedPMJ.Status.SnapshotRef != "my-snap" {
			t.Errorf("Expected SnapshotRef to be 'my-snap', got '%s'", updatedPMJ.Status.SnapshotRef)
		}
	})

	t.Run("Test Case 2 (Isolation)", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			},
		}

		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		trigger.SetName(triggerName)
		trigger.SetNamespace(namespace)
		trigger.Object["status"] = map[string]interface{}{
			"snapshotCreated": map[string]interface{}{
				"name": "new-snap",
			},
		}

		newSnapshot := &unstructured.Unstructured{}
		newSnapshot.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshot",
		})
		newSnapshot.SetName("new-snap")
		newSnapshot.SetNamespace(namespace)
		newSnapshot.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "False",
				},
			},
		}

		staleSnapshot := &unstructured.Unstructured{}
		staleSnapshot.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshot",
		})
		staleSnapshot.SetName("old-snap")
		staleSnapshot.SetNamespace(namespace)
		staleSnapshot.SetAnnotations(map[string]string{
			"podsnapshot.gke.io/origin-pod": podName,
		})
		staleSnapshot.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pmj, trigger, newSnapshot, staleSnapshot).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			Build()

		r := &PodMigrationJobReconciler{
			Client: fakeClient,
			Scheme: scheme,
		}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}

		if res.RequeueAfter != 1*time.Second {
			t.Errorf("Expected RequeueAfter to be 1s, got %v", res.RequeueAfter)
		}

		updatedPMJ := &pmv1alpha1.PodMigrationJob{}
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
		if err != nil {
			t.Fatalf("Failed to get PMJ: %v", err)
		}

		if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Errorf("Expected phase to remain %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
		}
	})
}


