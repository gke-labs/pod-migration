package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func newFakeClientBuilderWithEventIndex(scheme *runtime.Scheme) *fake.ClientBuilder {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Event{}, "involvedObject.uid", func(rawObj client.Object) []string {
			if event, ok := rawObj.(*corev1.Event); ok {
				return []string{string(event.InvolvedObject.UID)}
			}
			return nil
		})
}

func TestPodMigrationJobReconciler_Pending(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "12345678-abcd"
	jobName := util.FormatPMJName(podName, podUID)

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
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
			TargetPodUID: podUID,
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

	// Second reconcile: in Snapshotting phase, calls EnsureTrigger to create PSMT
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	})
	if err != nil {
		t.Fatalf("Second reconcile failed: %v", err)
	}

	// Verify PodSnapshotManualTrigger was created
	triggerName := util.FormatPSMTName(podName, podUID)
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
	podUID := "12345678-abcd"
	pvcName := "test-pvc"
	pvName := "test-pv"
	jobName := util.FormatPMJName(podName, podUID)

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
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
			TargetPodUID: podUID,
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

	// Second reconcile: in Snapshotting phase, calls EnsureTrigger to create PSMT
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	})
	if err != nil {
		t.Fatalf("Second reconcile failed: %v", err)
	}

	// Verify trigger IS created
	triggerName := util.FormatPSMTName(podName, podUID)
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
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Expected Ready condition to be set, but it was nil")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Ready condition status to be False, got %s", cond.Status)
	}
	if cond.Reason != "PodNotFound" {
		t.Errorf("Expected Ready condition reason to be 'PodNotFound', got %q", cond.Reason)
	}
}

func TestPodMigrationJobReconciler_Pending_PodUIDMismatch(t *testing.T) {
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
			UID:       "new-pod-uid",
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
			TargetPodUID: "expected-pod-uid",
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

	// Verify PMJ transitioned to Failed due to UID mismatch
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
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Expected Ready condition to be set, but it was nil")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Ready condition status to be False, got %s", cond.Status)
	}
	if cond.Reason != "PodNotFound" {
		t.Errorf("Expected Ready condition reason to be 'PodNotFound', got %q", cond.Reason)
	}
	if cond.Message != "Origin pod UID mismatch in Pending state" {
		t.Errorf("Expected Ready condition message to be 'Origin pod UID mismatch in Pending state', got %q", cond.Message)
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
			cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
			if cond == nil {
				t.Fatalf("Expected Ready condition to be set, but it was nil")
			}
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("Expected Ready condition status to be False, got %s", cond.Status)
			}
			if cond.Reason != "Timeout" {
				t.Errorf("Expected Ready condition reason to be 'Timeout', got %q", cond.Reason)
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
	podUID := "12345678-abcd"
	jobName := util.FormatPMJName(podName, podUID)

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
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
				TargetPodUID: podUID,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		// Create stale trigger owned by an older/different PMJ UID
		triggerName := util.FormatPSMTName(podName, podUID)
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
		isController := true
		staleTrigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        "stale-old-job-uid",
				Controller: &isController,
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

		// First reconcile: Pending -> Snapshotting
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
			t.Errorf("Expected reconcile to requeue after transition to Snapshotting, got res: %+v", res)
		}

		// Second reconcile: in Snapshotting phase, EnsureTrigger deletes stale trigger and requeues
		res, err = r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Second reconcile failed: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected reconcile to requeue after deleting stale trigger, got res: %+v", res)
		}

		// Verify PMJ remains in Snapshotting until stale trigger is recreated
		updatedPMJ := &pmv1alpha1.PodMigrationJob{}
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
		if err != nil {
			t.Fatalf("Failed to get PMJ: %v", err)
		}
		if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
		}

		// Verify stale trigger was deleted
		trigger := &unstructured.Unstructured{}
		trigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, trigger)
		if err == nil || !apierrors.IsNotFound(err) {
			t.Errorf("Expected stale trigger to be deleted (NotFound), got error: %v", err)
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
				TargetPodUID: podUID,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		// Create owned trigger with matching owner UID
		triggerName := util.FormatPSMTName(podName, podUID)
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
	podUID := "12345678-abcd"
	jobName := util.FormatPMJName(podName, podUID)
	triggerName := util.FormatPSMTName(podName, podUID)

	t.Run("Test Case 1 (Success)", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				UID:               "job-uid",
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
				TargetPodUID: podUID,
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
		isController := true
		trigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        pmj.UID,
				Controller: &isController,
			},
		})
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

		// Reconcile again in PhaseEvicting to execute the idempotent Cleanup
		_, err = r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{
				Namespace: namespace,
				Name:      jobName,
			},
		})
		if err != nil {
			t.Fatalf("Reconcile in Evicting phase failed: %v", err)
		}

		cleanedTrigger := &unstructured.Unstructured{}
		cleanedTrigger.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, cleanedTrigger)
		if err == nil || !apierrors.IsNotFound(err) {
			t.Errorf("Expected PSMT trigger to be cleaned up proactively upon snapshot completion, got error: %v", err)
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
				UID:               "job-uid",
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
				TargetPodUID: podUID,
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
		isController := true
		trigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        pmj.UID,
				Controller: &isController,
			},
		})
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

	t.Run("Test Case 3 (Fast-Fail on PSMT Failure)", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				UID:               "job-uid",
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
				TargetPodUID: podUID,
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
		isController := true
		trigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        pmj.UID,
				Controller: &isController,
			},
		})
		trigger.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":    "Triggered",
					"status":  "False",
					"reason":  "Failed",
					"message": "target pod not found on node",
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithObjects(pmj, trigger).
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
			t.Errorf("Expected no requeue on terminal failure, got %+v", res)
		}

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
		cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
		if cond == nil || cond.Reason != "SnapshotTriggerFailed" {
			t.Errorf("Expected Ready condition with Reason=SnapshotTriggerFailed, got %+v", cond)
		}
	})

	t.Run("Test Case 4 (Fast-Fail on PodSnapshot Failure)", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				UID:               "job-uid",
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{
					Name: podName,
				},
				TargetPodUID: podUID,
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
		isController := true
		trigger.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: "podmigration.gke.io/v1alpha1",
				Kind:       "PodMigrationJob",
				Name:       jobName,
				UID:        pmj.UID,
				Controller: &isController,
			},
		})
		trigger.Object["status"] = map[string]interface{}{
			"snapshotCreated": map[string]interface{}{
				"name": "failed-snap",
			},
		}

		snapshot := &unstructured.Unstructured{}
		snapshot.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshot",
		})
		snapshot.SetName("failed-snap")
		snapshot.SetNamespace(namespace)
		snapshot.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":    "Checkpoint",
					"status":  "False",
					"reason":  "Failed",
					"message": "runsc checkpoint: signal SIGKILL",
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
		if res.Requeue || res.RequeueAfter != 0 {
			t.Errorf("Expected no requeue on terminal failure, got %+v", res)
		}

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
		cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
		if cond == nil || cond.Reason != "SnapshotFailed" {
			t.Errorf("Expected Ready condition with Reason=SnapshotFailed, got %+v", cond)
		}
	})
}

func TestPodMigrationJobReconciler_Evicting_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "test-pv"

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
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach: []string{pvName},
		},
	}

	// Fake client setup: Pod is deleted (not added), Trigger is deleted (not added)
	// and no VolumeAttachments are active
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
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
		t.Errorf("Expected no requeue on transition to Restoring, got: %+v", res)
	}

	// Verify PMJ transitioned to Restoring
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.RestoringStartTime == nil {
		t.Errorf("Expected RestoringStartTime to be set, but got nil")
	}

	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Expected Ready condition to be set, but it was nil")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Ready condition status to be False, got %s", cond.Status)
	}
	if cond.Reason != "RestoringState" {
		t.Errorf("Expected Ready condition reason to be 'RestoringState', got %q", cond.Reason)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_StillAttached_Requeues(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"
	otherPV := "other-pv"

	pmj := &pmv1alpha1.PodMigrationJob{
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
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach: []string{pvName},
		},
	}

	vaTarget := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-target",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: "node-1",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	vaOther := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-other",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &otherPV,
			},
			NodeName: "node-2",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, vaTarget, vaOther).
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
	if res.RequeueAfter != 3*time.Second {
		t.Errorf("Expected RequeueAfter 3s, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseEvicting, updatedPMJ.Status.Phase)
	}

	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Expected Ready condition to be set, but it was nil")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Ready condition status to be False, got %s", cond.Status)
	}
	if cond.Reason != "WaitingForVolumeDetach" {
		t.Errorf("Expected Ready condition reason to be 'WaitingForVolumeDetach', got %q", cond.Reason)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_Detached_TransitionsToRestoring(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"
	otherPV := "other-pv"

	pmj := &pmv1alpha1.PodMigrationJob{
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
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach: []string{pvName},
		},
	}

	// Target PV attachment is detached (Attached: false)
	vaTarget := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-target",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: "node-1",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	// Unrelated PV is attached
	vaOther := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-other",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &otherPV,
			},
			NodeName: "node-2",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, vaTarget, vaOther).
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
		t.Errorf("Expected no requeue on transition to Restoring, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_MultiplePVs_OneStillAttached(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pv1 := "target-pv-1"
	pv2 := "target-pv-2"

	pmj := &pmv1alpha1.PodMigrationJob{
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
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach: []string{pv1, pv2},
		},
	}

	va1 := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-1",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv1,
			},
			NodeName: "node-1",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	va2 := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-2",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pv2,
			},
			NodeName: "node-2",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, va1, va2).
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
	if res.RequeueAfter != 3*time.Second {
		t.Errorf("Expected RequeueAfter 3s, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseEvicting, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_ListError_ReturnsError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"

	pmj := &pmv1alpha1.PodMigrationJob{
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
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach: []string{pvName},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, client client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*storagev1.VolumeAttachmentList); ok {
					return fmt.Errorf("simulated volume attachment list error")
				}
				return client.List(ctx, list, opts...)
			},
		}).
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
	if err == nil {
		t.Fatal("Expected error on VolumeAttachment list failure, got nil")
	}
	if !strings.Contains(err.Error(), "simulated volume attachment list error") {
		t.Errorf("Expected simulated error message, got: %v", err)
	}
}

func TestPodMigrationJobReconciler_Restoring_WaitingForConsumed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	tests := []struct {
		name   string
		status pmv1alpha1.PodMigrationJobStatus
	}{
		{
			name: "Not consumed",
			status: pmv1alpha1.PodMigrationJobStatus{
				Phase:    pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed: false,
			},
		},
		{
			name: "Consumed but empty RestoredPodUID",
			status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed:        true,
				RestoredPodUID:  "",
				RestoredPodName: "test-pod",
			},
		},
		{
			name: "Consumed but empty RestoredPodName",
			status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed:        true,
				RestoredPodUID:  "uid-123",
				RestoredPodName: "",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
					PodRef: corev1.LocalObjectReference{Name: podName},
				},
				Status: tc.status,
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
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
			})
			if err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}
			if res.RequeueAfter != 2*time.Second {
				t.Errorf("Expected RequeueAfter 2s while waiting for pod gate consumption, got: %+v", res)
			}
		})
	}
}

func TestPodMigrationJobReconciler_Restoring_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  podUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after success, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil {
		t.Fatalf("Expected Restored condition to be set")
	}
	if restoredCond.Status != metav1.ConditionTrue || restoredCond.Reason != "RestoreVerified" {
		t.Errorf("Expected Restored condition True/RestoreVerified, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_Success_DifferentReplacementPodName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	originPodName := "my-deploy-abc"
	restoredPodName := "my-deploy-xyz"
	restoredPodUID := "uid-xyz"
	jobName := "pmj-" + originPodName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      restoredPodName,
			UID:       types.UID(restoredPodUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
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
			PodRef:       corev1.LocalObjectReference{Name: originPodName},
			TargetPodUID: "origin-uid-abc",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  restoredPodUID,
			RestoredPodName: restoredPodName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after success, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil {
		t.Fatalf("Expected Restored condition to be set")
	}
	if restoredCond.Status != metav1.ConditionTrue || restoredCond.Reason != "RestoreVerified" {
		t.Errorf("Expected Restored condition True/RestoreVerified, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_ColdStartFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "fallback-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "FallbackToColdStart",
		Message: "GKE runtime skipped snapshot restore and fell back to cold start",
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  podUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, event, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil {
		t.Fatalf("Expected Restored condition to be set")
	}
	if restoredCond.Status != metav1.ConditionFalse || restoredCond.Reason != "FallbackToColdStart" {
		t.Errorf("Expected Restored condition False/FallbackToColdStart, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_IgnoresNormalEventsWithFallbackKeyword(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "cni-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Type:    corev1.EventTypeNormal,
		Reason:  "CNIFallback",
		Message: "fallback route configured",
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  podUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, event, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil {
		t.Fatalf("Expected Restored condition to be set")
	}
	if restoredCond.Status != metav1.ConditionTrue || restoredCond.Reason != "RestoreVerified" {
		t.Errorf("Expected Restored condition True/RestoreVerified, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_IgnoresStaleWarningEventsBeforeRestoringStartTime(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	event := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "stale-fallback-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Type:          corev1.EventTypeWarning,
		Reason:        "FallbackToColdStart",
		Message:       "falling back to a cold start",
		LastTimestamp: metav1.Time{Time: time.Now().Add(-10 * time.Minute)},
	}

	restoringStartTime := metav1.Time{Time: time.Now().Add(-2 * time.Minute)}
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:           true,
			RestoredPodUID:     podUID,
			RestoredPodName:    podName,
			RestoringStartTime: &restoringStartTime,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, event, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil {
		t.Fatalf("Expected Restored condition to be set")
	}
	if restoredCond.Status != metav1.ConditionTrue || restoredCond.Reason != "RestoreVerified" {
		t.Errorf("Expected Restored condition True/RestoreVerified, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_GC_DefersWhileClaimantPodStillGated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	completed := metav1.NewTime(time.Now().Add(-45 * time.Minute)) // past the 30m TTL
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			SnapshotRef:    "snapshot-1",
			CompletionTime: &completed,
		},
	}

	// A pod still gated and assigned to this PMJ has not yet received its
	// snapshot ref; deleting the PMJ now forces it into a cold start.
	gatedClaimant := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": jobName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj, gatedClaimant).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected requeue while deferring GC for gated claimant pod, got: %+v", res)
	}

	stillThere := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, stillThere); err != nil {
		t.Errorf("Expected PMJ to survive GC while claimant pod is gated, got: %v", err)
	}
}

func TestPodMigrationJobReconciler_GC_DeletesAfterTTLWhenNoClaimant(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	jobName := "pmj-test-pod"

	completed := metav1.NewTime(time.Now().Add(-45 * time.Minute)) // past the 30m TTL
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "test-pod"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &completed,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	gone := &pmv1alpha1.PodMigrationJob{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, gone)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Expected PMJ garbage collected after TTL with no gated claimant, got err=%v", err)
	}
}

// A crashlooping cold-start-fallback pod never reaches Ready; the throttled
// probe must still surface the fallback promptly instead of waiting for the
// 5-minute ceiling.
func TestPodMigrationJobReconciler_Restoring_NotReadyPodWithFallbackEventConcludesPromptly(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	crashloopingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	fallbackEvent := &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "fallback-event",
		},
		InvolvedObject: corev1.ObjectReference{
			Kind:      "Pod",
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Type:    corev1.EventTypeWarning,
		Reason:  "FallbackToColdStart",
		Message: "GKE runtime skipped snapshot restore and fell back to cold start",
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  podUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(crashloopingPod, fallbackEvent, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected prompt SucceededWithoutRestore for not-ready pod with fallback event, got %s", updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Restoring_NoEventQueriesBeforePodReady(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	notReadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  podUID,
			RestoredPodName: podName,
		},
	}

	// Restoring polls of a not-yet-Ready pod must not hit the Events API every
	// 2s tick — at 50 workers those uncached LISTs saturate the QPS budget.
	// The probe is throttled: first tick queries, back-to-back ticks do not.
	eventLists := 0
	base := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(notReadyPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()
	cl := interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.EventList); ok {
				eventLists++
			}
			return c.List(ctx, list, opts...)
		},
	})

	r := &PodMigrationJobReconciler{
		Client: cl,
		Scheme: scheme,
	}

	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName}}
	var res ctrl.Result
	var err error
	for i := 0; i < 3; i++ {
		res, err = r.Reconcile(context.Background(), req)
		if err != nil {
			t.Fatalf("Reconcile %d failed: %v", i, err)
		}
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected requeue while waiting for pod readiness, got: %+v", res)
	}
	if eventLists != 1 {
		t.Errorf("Expected exactly 1 throttled Event API query across back-to-back reconciles of a not-yet-Ready pod, got %d", eventLists)
	}
}

func TestPodMigrationJobReconciler_Restoring_Timeout_DefersWhileReplacementPodStillGated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	restoringStartTime := metav1.NewTime(time.Now().Add(-6 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			RestoringStartTime: &restoringStartTime,
			// Not consumed: the replacement pod below is still waiting on the
			// single PodGate worker to release its scheduling gate.
		},
	}

	gatedReplacement := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": jobName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj, gatedReplacement).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected requeue while deferring timeout for gated replacement pod, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase to remain %s while replacement pod is gated, got %s",
			pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}

	// Deferral must be visible to operators: an indefinitely deferred PMJ with
	// a wedged PodGate worker should be alertable from status alone.
	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Reason != "WaitingOnGateRelease" {
		t.Errorf("Expected Ready condition with Reason=WaitingOnGateRelease while deferred, got %+v", readyCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_Timeout(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	restoringStartTime := metav1.NewTime(time.Now().Add(-6 * time.Minute))
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			RestoringStartTime: &restoringStartTime,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue on timeout, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil || restoredCond.Reason != "RestoreTimeout" {
		t.Errorf("Expected Restored condition with Reason=RestoreTimeout, got %+v", restoredCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_Timeout_MeasuredFromRestoringStartTimeNotCreationTimestamp(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	restoringStartTime := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigrationJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-8 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			RestoringStartTime: &restoringStartTime,
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
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime != nil {
		t.Errorf("Expected CompletionTime to be nil")
	}
}

func TestPodMigrationJobReconciler_Restoring_UIDMismatch_Initial(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	expectedUID := "expected-uid-1234"
	differentUID := "different-uid-5678"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(differentUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
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
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s on initial UID mismatch, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Annotations[util.AnnotationMismatchSince] == "" {
		t.Errorf("Expected mismatch-since annotation to be set")
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase to remain %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Restoring_UIDMismatch_PersistedTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	expectedUID := "expected-uid-1234"
	differentUID := "different-uid-5678"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(differentUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
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
			Annotations: map[string]string{
				util.AnnotationMismatchSince: time.Now().Add(-45 * time.Second).Format(time.RFC3339),
			},
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after UID mismatch persisted >30s, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil || restoredCond.Reason != "ReplacementPodMismatch" || restoredCond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Restored condition False/ReplacementPodMismatch, got %+v", restoredCond)
	}

	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Reason != "ReplacementPodMismatch" || readyCond.Status != metav1.ConditionTrue {
		t.Errorf("Expected Ready condition True/ReplacementPodMismatch, got %+v", readyCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_UIDMismatch_MalformedAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	expectedUID := "expected-uid-1234"
	differentUID := "different-uid-5678"
	jobName := "pmj-" + podName

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(differentUID),
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: corev1.ConditionTrue,
				},
			},
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
			Annotations: map[string]string{
				util.AnnotationMismatchSince: "invalid-timestamp",
			},
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("Expected no requeue after malformed mismatch-since annotation fast-fail, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	restoredCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Restored")
	if restoredCond == nil || restoredCond.Reason != "ReplacementPodMismatch" || restoredCond.Status != metav1.ConditionFalse {
		t.Errorf("Expected Restored condition False/ReplacementPodMismatch, got %+v", restoredCond)
	}

	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Reason != "ReplacementPodMismatch" || readyCond.Status != metav1.ConditionTrue {
		t.Errorf("Expected Ready condition True/ReplacementPodMismatch, got %+v", readyCond)
	}
}

func TestPodMigrationJobReconciler_Restoring_ClearsMismatchSinceWhenUIDMatches(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	expectedUID := "uid-recovered"

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(expectedUID),
		},
		Spec: corev1.PodSpec{},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
			Annotations: map[string]string{
				util.AnnotationMismatchSince: time.Now().Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			GateReleased:    true,
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if _, exists := updatedPMJ.Annotations[util.AnnotationMismatchSince]; exists {
		t.Errorf("Expected AnnotationMismatchSince to be cleared when UID matches, but it still exists")
	}
}

func TestPodMigrationJobReconciler_Restoring_SelfHealsGateReleasedWhenPodUngated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	expectedUID := "uid-ungated"

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(expectedUID),
		},
		Spec: corev1.PodSpec{
			SchedulingGates: nil, // Gate removed
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			GateReleased:    false, // Lost or conflicted during PodGate update
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if !updatedPMJ.Status.GateReleased {
		t.Errorf("Expected GateReleased to be self-healed to true when replacement pod has no scheduling gate, got false")
	}
}

func TestPodMigrationJobReconciler_Restoring_GateReleasedRemainsFalseWhileGatePresent(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	expectedUID := "uid-gated"

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(expectedUID),
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: MigrationGateName},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      jobName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			Consumed:        true,
			GateReleased:    false,
			RestoredPodUID:  expectedUID,
			RestoredPodName: podName,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(replacementPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ)
	if err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if updatedPMJ.Status.GateReleased {
		t.Errorf("Expected GateReleased to remain false while replacement pod has scheduling gate, got true")
	}
}

func TestPodMigrationJobReconciler_Evicting_PDBSafeEvictionFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "12345678-abcd"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
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
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: podName,
			},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	evictionCalled := false
	var evictedPodName string
	var evictedNamespace string

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					evictionCalled = true
					if eviction, ok := subResource.(*policyv1.Eviction); ok {
						evictedPodName = eviction.Name
						evictedNamespace = eviction.Namespace
					}
					return nil
				}
				return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
			},
		}).
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
	if !evictionCalled {
		t.Fatalf("Expected eviction subresource to be called on timeout fallback, but was not")
	}
	if evictedPodName != podName || evictedNamespace != namespace {
		t.Errorf("Expected eviction for %s/%s, got %s/%s", namespace, podName, evictedNamespace, evictedPodName)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s, got %v", res.RequeueAfter)
	}
}

func TestPodMigrationJobReconciler_Evicting_PDBBlocked_429_Requeues(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "12345678-abcd"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
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
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: podName,
			},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	evictionAttempted := false

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					evictionAttempted = true
					return apierrors.NewTooManyRequests("Cannot evict pod due to PDB", 5)
				}
				return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
			},
		}).
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
		t.Fatalf("Reconcile failed unexpectedly on 429: %v", err)
	}
	if !evictionAttempted {
		t.Fatalf("Expected eviction subresource to be called, but was not")
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter 5s when blocked by PDB (429), got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "BlockedByPDB")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "PDBBudgetExhausted" {
		t.Errorf("Expected BlockedByPDB condition True/PDBBudgetExhausted, got: %+v", cond)
	}
}

func TestPodMigrationJobReconciler_Evicting_DeletionTimestamp_SkipsEvictionCall(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "terminating-pod"
	podUID := "uid-term-123"
	jobName := "pmj-" + podName
	now := metav1.Now()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              podName,
			UID:               types.UID(podUID),
			DeletionTimestamp: &now,
			Finalizers:        []string{"kubernetes.io/test-finalizer"},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: now,
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	evictionCalled := false
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					evictionCalled = true
				}
				return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
			},
		}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if evictionCalled {
		t.Errorf("Expected eviction subresource NOT to be called when pod has DeletionTimestamp, but was called")
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s while waiting for terminating pod, got %v", res.RequeueAfter)
	}
}

func TestPodMigrationJobReconciler_Evicting_500InternalServerError_SetsEvictionMisconfigured(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "misconfigured-pod"
	podUID := "uid-misconfig-123"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					return apierrors.NewInternalError(fmt.Errorf("multiple conflicting PDBs covering pod"))
				}
				return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
			},
		}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter 5s on 500 error, got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "EvictionMisconfigured")
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "MultiplePDBsOrInternalError" {
		t.Errorf("Expected EvictionMisconfigured condition True/MultiplePDBsOrInternalError, got: %+v", cond)
	}
}

func TestPodMigrationJobReconciler_Evicting_SuccessAfter429_ClearsBlockedByPDB(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "recovering-pod"
	podUID := "uid-recovering-123"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
			Conditions: []metav1.Condition{
				{
					Type:               "BlockedByPDB",
					Status:             metav1.ConditionTrue,
					Reason:             "PDBBudgetExhausted",
					Message:            "Prior 429",
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               "EvictionMisconfigured",
					Status:             metav1.ConditionTrue,
					Reason:             "MultiplePDBsOrInternalError",
					Message:            "Prior 500",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
				if subResourceName == "eviction" {
					return nil // Success
				}
				return c.SubResource(subResourceName).Create(ctx, obj, subResource, opts...)
			},
		}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s on eviction success, got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}
	condPDB := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "BlockedByPDB")
	if condPDB == nil || condPDB.Status != metav1.ConditionFalse || condPDB.Reason != "EvictionInitiated" {
		t.Errorf("Expected BlockedByPDB condition False/EvictionInitiated, got: %+v", condPDB)
	}
	condMisconfig := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "EvictionMisconfigured")
	if condMisconfig == nil || condMisconfig.Status != metav1.ConditionFalse || condMisconfig.Reason != "EvictionInitiated" {
		t.Errorf("Expected EvictionMisconfigured condition False/EvictionInitiated, got: %+v", condMisconfig)
	}
}

func TestPodMigrationJobReconciler_Evicting_Timeout_PDBBlocked_ConcludesSucceededWithoutRestore(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "my-pdb-pod"
	jobName := "pmj-timeout-pdb"

	originPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-my-pod",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-15 * time.Minute)},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-my-pod",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "durable-snap-xyz",
			Conditions: []metav1.Condition{
				{
					Type:               "BlockedByPDB",
					Status:             metav1.ConditionTrue,
					Reason:             "PDBBudgetExhausted",
					Message:            "Waiting for budget",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(originPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected Phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue || readyCond.Reason != "PDBEvictionTimeout" {
		t.Errorf("Expected Ready condition True/PDBEvictionTimeout, got %+v", readyCond)
	}

	// Verify origin pod was annotated to prevent re-snapshot churn
	updatedPod := &corev1.Pod{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: podName}, updatedPod); err != nil {
		t.Fatalf("Failed to get updated origin pod: %v", err)
	}
	if updatedPod.Annotations[util.AnnotationPDBEvictionTimeout] != "true" {
		t.Errorf("Expected origin pod to have annotation %s=true, got: %v", util.AnnotationPDBEvictionTimeout, updatedPod.Annotations)
	}
}

func TestPodMigrationJobReconciler_Evicting_Timeout_EvictionMisconfigured_ConcludesSucceededWithoutRestore(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "my-misconfig-pod"
	jobName := "pmj-timeout-misconfig"

	originPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-misconfig-pod",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-15 * time.Minute)},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-misconfig-pod",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "durable-snap-xyz",
			Conditions: []metav1.Condition{
				{
					Type:               "EvictionMisconfigured",
					Status:             metav1.ConditionTrue,
					Reason:             "MultiplePDBsOrInternalError",
					Message:            "Multiple conflicting PDBs",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(originPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Errorf("Expected Phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}
	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionTrue || readyCond.Reason != "EvictionMisconfiguredTimeout" {
		t.Errorf("Expected Ready condition True/EvictionMisconfiguredTimeout, got %+v", readyCond)
	}
}

func TestPodMigrationJobReconciler_Evicting_Timeout_NoBlockage_ConcludesFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "my-generic-timeout-pod"
	jobName := "pmj-timeout-generic"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-15 * time.Minute)},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-generic-pod",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "durable-snap-xyz",
			// No BlockedByPDB or EvictionMisconfigured condition
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

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected Phase %s on generic eviction timeout, got %s", pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
	}
	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionFalse || readyCond.Reason != "Timeout" {
		t.Errorf("Expected Ready condition False/Timeout, got %+v", readyCond)
	}
}

func TestPodMigrationJobReconciler_Evicting_Timeout_UIDMismatch_SkipsAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "my-pdb-pod"
	jobName := "pmj-timeout-pdb"

	// Recreated pod with a DIFFERENT UID than the PMJ's TargetPodUID
	recreatedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-recreated-new-pod",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-15 * time.Minute)},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-my-original-pod",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "durable-snap-xyz",
			Conditions: []metav1.Condition{
				{
					Type:               "BlockedByPDB",
					Status:             metav1.ConditionTrue,
					Reason:             "PDBBudgetExhausted",
					Message:            "Waiting for budget",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(recreatedPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify recreated pod was NOT annotated because UID did not match
	updatedPod := &corev1.Pod{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: podName}, updatedPod); err != nil {
		t.Fatalf("Failed to get updated pod: %v", err)
	}
	if updatedPod.Annotations != nil && updatedPod.Annotations[util.AnnotationPDBEvictionTimeout] != "" {
		t.Errorf("Expected recreated pod NOT to be annotated due to UID mismatch, got annotations: %v", updatedPod.Annotations)
	}
}

func TestPodMigrationJobReconciler_Evicting_Timeout_PodAnnotationUpdateError_ReturnsError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "my-pdb-pod"
	jobName := "pmj-timeout-pdb"

	originPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-my-pod",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Time{Time: time.Now().Add(-15 * time.Minute)},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-my-pod",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "durable-snap-xyz",
			Conditions: []metav1.Condition{
				{
					Type:               "BlockedByPDB",
					Status:             metav1.ConditionTrue,
					Reason:             "PDBBudgetExhausted",
					Message:            "Waiting for budget",
					LastTransitionTime: metav1.Now(),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(originPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*corev1.Pod); ok {
					return apierrors.NewConflict(corev1.Resource("pods"), podName, fmt.Errorf("conflict updating pod annotation"))
				}
				return c.Update(ctx, obj, opts...)
			},
		}).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err == nil {
		t.Fatalf("Expected Reconcile to return error when pod annotation update fails, but got nil")
	}

	// The annotation write must happen before the PMJ is concluded. If it fails, the
	// job has to stay Evicting so the next reconcile retries and the churn guard
	// still gets stamped; concluding first would strand the origin pod unannotated
	// and let a subsequent drain re-snapshot it. Pin that ordering.
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Errorf("Expected PMJ to remain Evicting after a failed annotation write so the reconcile retries, got %s", updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime != nil {
		t.Errorf("Expected CompletionTime to remain unset after a failed annotation write, got %v", updatedPMJ.Status.CompletionTime)
	}
}

// TestPodMigrationJobReconciler_Evicting_OriginPodRemoved_ClearsStaleBlockageConditions
// covers the case where the origin pod is removed by an external actor (typically
// `kubectl drain`'s own eviction call) after we had already recorded an eviction
// blockage. The controller's own clearing path never runs in that case, so the
// pod-gone path must retract the conditions itself; otherwise the job advances and
// terminates while still reporting BlockedByPDB=True.
func TestPodMigrationJobReconciler_Evicting_OriginPodRemoved_ClearsStaleBlockageConditions(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "drained-pod"
	podUID := "uid-drained-123"
	jobName := "pmj-" + podName

	// Note: no Pod object is seeded, modelling the origin pod already being gone.
	pmj := &pmv1alpha1.PodMigrationJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigrationJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": time.Now().Add(-35 * time.Second).Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: podName,
			},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
			Conditions: []metav1.Condition{
				{
					Type:               "BlockedByPDB",
					Status:             metav1.ConditionTrue,
					Reason:             "PDBBudgetExhausted",
					Message:            "Origin pod eviction delayed by PodDisruptionBudget; waiting for budget",
					LastTransitionTime: metav1.Now(),
				},
				{
					Type:               "EvictionMisconfigured",
					Status:             metav1.ConditionTrue,
					Reason:             "MultiplePDBsOrInternalError",
					Message:            "Origin pod eviction failed with 500 InternalServerError",
					LastTransitionTime: metav1.Now(),
				},
			},
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

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	}); err != nil {
		t.Fatalf("Reconcile failed unexpectedly when origin pod is gone: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}

	for _, condType := range []string{"BlockedByPDB", "EvictionMisconfigured"} {
		cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, condType)
		if cond == nil {
			t.Errorf("Expected %s condition to be present and retracted, but it is absent", condType)
			continue
		}
		if cond.Status != metav1.ConditionFalse || cond.Reason != "OriginPodRemoved" {
			t.Errorf("Expected %s condition False/OriginPodRemoved once the origin pod is gone, got %s/%s", condType, cond.Status, cond.Reason)
		}
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase to advance to Restoring, got %s", updatedPMJ.Status.Phase)
	}
}

// TestPodMigrationJobReconciler_Evicting_OriginPodRemoved_NoVacuousConditions asserts
// that a job which was never blocked does not gain BlockedByPDB / EvictionMisconfigured
// conditions merely because it passed through the pod-gone path.
func TestPodMigrationJobReconciler_Evicting_OriginPodRemoved_NoVacuousConditions(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "unblocked-pod"
	podUID := "uid-unblocked-456"
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
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
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

	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      jobName,
		},
	}); err != nil {
		t.Fatalf("Reconcile failed unexpectedly when origin pod is gone: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get updated PMJ: %v", err)
	}

	for _, condType := range []string{"BlockedByPDB", "EvictionMisconfigured"} {
		if cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, condType); cond != nil {
			t.Errorf("Expected no %s condition on a job that was never blocked, got %+v", condType, cond)
		}
	}
}
