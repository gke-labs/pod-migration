package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
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
		}).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue)
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

		if res.RequeueAfter != 30*time.Second {
			t.Errorf("Expected RequeueAfter to be 30s, got %v", res.RequeueAfter)
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

func TestPodMigrationJobReconciler_Pending_CapturesOriginNodeName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-origin-node-pod"
	podUID := "uid-node-test-1234"
	jobName := util.FormatPMJName(podName, podUID)
	originNode := "gke-cluster-node-alpha"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Spec: corev1.PodSpec{
			NodeName: originNode,
		},
	}

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
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if updatedPMJ.Status.OriginNodeName != originNode {
		t.Errorf("Expected OriginNodeName %q, got %q", originNode, updatedPMJ.Status.OriginNodeName)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_OriginNodeScoping_IgnoresDestinationNodeAttachment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"
	originNode := "node-origin"
	destNode := "node-destination"

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
			Phase:          pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach:    []string{pvName},
			OriginNodeName: originNode,
		},
	}

	// Origin node volume attachment is detached
	vaOrigin := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-origin",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: originNode,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	// Destination node volume attachment for the same PV is already attached
	vaDest := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-dest",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: destNode,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, vaOrigin, vaDest).
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
		t.Errorf("Expected transition to Restoring without requeue, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_OriginNodeScoping_WaitsWhenOriginNodeStillAttached(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"
	originNode := "node-origin"
	destNode := "node-destination"

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
			Phase:          pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach:    []string{pvName},
			OriginNodeName: originNode,
		},
	}

	// Origin node volume attachment is still attached
	vaOrigin := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-origin",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: originNode,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	// Destination node volume attachment is not attached
	vaDest := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-dest",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: destNode,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, vaOrigin, vaDest).
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
	if cond == nil || cond.Reason != "WaitingForVolumeDetach" {
		t.Errorf("Expected condition WaitingForVolumeDetach, got: %+v", cond)
	}
}

func TestPodMigrationJobReconciler_Evicting_BackfillsOriginNodeName_PersistsAndDoesNotDuplicate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-evicting-backfill-pod"
	podUID := "uid-backfill-1234"
	jobName := util.FormatPMJName(podName, podUID)
	originNode := "gke-node-evicting-alpha"
	now := metav1.Now()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              podName,
			UID:               types.UID(podUID),
			DeletionTimestamp: &now,
			Finalizers:        []string{"test-finalizer"},
		},
		Spec: corev1.PodSpec{
			NodeName: originNode,
		},
	}

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
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseEvicting,
			OriginNodeName: "", // In-flight job without OriginNodeName
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

	// First reconcile: backfill should persist OriginNodeName
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("First reconcile failed: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s, got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.OriginNodeName != originNode {
		t.Fatalf("Expected OriginNodeName %q, got %q", originNode, updatedPMJ.Status.OriginNodeName)
	}
	firstRV := updatedPMJ.ResourceVersion

	// Second reconcile: OriginNodeName already set, should not trigger another status write
	res2, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Second reconcile failed: %v", err)
	}
	if res2.RequeueAfter != 2*time.Second {
		t.Errorf("Expected RequeueAfter 2s, got %v", res2.RequeueAfter)
	}

	afterSecondPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, afterSecondPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if afterSecondPMJ.ResourceVersion != firstRV {
		t.Errorf("Expected ResourceVersion unchanged (%s), got %s (unexpected second write)", firstRV, afterSecondPMJ.ResourceVersion)
	}
}

func TestPodMigrationJobReconciler_Evicting_VolumeAttachment_EmptyOriginNode_FallbackWaitsForAllAttachments(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	pvName := "target-pv"
	someNode := "node-other"

	// OriginNodeName is empty (fallback behavior)
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
			Phase:          pmv1alpha1.PodMigrationJobPhaseEvicting,
			PVsToDetach:    []string{pvName},
			OriginNodeName: "",
		},
	}

	vaOther := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-other",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: someNode,
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(pmj, vaOther).
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

// --- Issue #15: cold-start fallback on a gVisor/OCI restore crash -----------
//
// Ground truth for the fixtures below was captured from a live cluster replay
// (the observation table is reproduced in the PR that introduced this feature):
//
//	restore failure : waiting=RunContainerError, terminated=StartError,
//	                  exitCode=128, restartCount=0 (NEVER increments)
//	genuine app bug : waiting=CrashLoopBackOff, terminated=Error,
//	                  exitCode=1, restartCount increments
//
// The restartCount asymmetry is why no assertion (and no production code path)
// may key off restartCount: on a restore failure it is pinned at 0 forever.

const (
	restoreCrashOCIMessage = "failed to create containerd task: failed to create shim task: " +
		"OCI runtime create failed: OCI runtime restore failed: unable to restore container: " +
		"restore failed: unknown"
	restoreCrashUnknownMessage = "failed to create containerd task: " +
		"cgroup subsystem freezer not mounted: unknown"
)

// counterValue reads the current value of a prometheus Counter without pulling
// in the testutil package (and its extra module dependencies).
func counterValue(t *testing.T, c prometheus.Counter) float64 {
	t.Helper()
	m := &dto.Metric{}
	if err := c.Write(m); err != nil {
		t.Fatalf("Failed to read counter: %v", err)
	}
	return m.GetCounter().GetValue()
}

// newRestoreCrashFixture builds a PMJ parked in Restoring that has already been
// consumed by the given replacement pod, i.e. the exact state in which a
// restore crash becomes observable.
func newRestoreCrashFixture(namespace, jobName, podName, podUID string, podStatus corev1.PodStatus) (*corev1.Pod, *pmv1alpha1.PodMigrationJob) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID(podUID),
		},
		Status: podStatus,
	}

	restoringStart := metav1.NewTime(time.Now().Add(-30 * time.Second))
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
			RestoringStartTime: &restoringStart,
			SnapshotRef:        "snapshot-ref",
			Consumed:           true,
			RestoredPodUID:     podUID,
			RestoredPodName:    podName,
		},
	}
	return pod, pmj
}

// restoreCrashPodStatus reproduces the kubelet shape where the container is
// parked in Waiting(RunContainerError) with the StartError in
// LastTerminationState.
func restoreCrashPodStatus(message string) corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "app",
				RestartCount: 0,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "RunContainerError",
						Message: message,
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "StartError",
						ExitCode: 128,
						Message:  message,
					},
				},
			},
		},
	}
}

// T1: a matched restore-crash signature triggers the destructive cold-start
// fallback: PMJ concludes Failed with RestoreCrashed=True and the replacement
// pod is deleted so its controller recreates it cold.
func TestPodMigrationJobReconciler_Restoring_RestoreCrashTriggersFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashOCIMessage))

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: recorder,
	}

	before := counterValue(t, metrics.RestoreCrashFallbackTotal)

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.CompletionTime == nil {
		t.Errorf("Expected CompletionTime to be set")
	}

	crashCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionRestoreCrashed)
	if crashCond == nil {
		t.Fatalf("Expected %s condition to be set", ConditionRestoreCrashed)
	}
	if crashCond.Status != metav1.ConditionTrue {
		t.Errorf("Expected %s condition True, got %s", ConditionRestoreCrashed, crashCond.Status)
	}
	if crashCond.Reason != ReasonRestoreCrashFallback {
		t.Errorf("Expected reason %s, got %s", ReasonRestoreCrashFallback, crashCond.Reason)
	}
	if !strings.Contains(crashCond.Message, "OCI runtime restore failed") {
		t.Errorf("Expected condition message to carry the matched signature, got %q", crashCond.Message)
	}

	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Errorf("Expected replacement pod to be deleted, got err=%v", err)
	}

	if got := counterValue(t, metrics.RestoreCrashFallbackTotal) - before; got != 1 {
		t.Errorf("Expected fallback counter to increment by 1, got %v", got)
	}
}

// T2: a genuine application bug (CrashLoopBackOff / exit 1 / restartCount>0)
// must NOT be mistaken for a restore crash — deleting the pod would mask the
// bug and spin a delete loop.
func TestPodMigrationJobReconciler_Restoring_AppCrashLoopDoesNotTriggerFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID, corev1.PodStatus{
		Phase: corev1.PodRunning,
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "app",
				RestartCount: 3,
				State: corev1.ContainerState{
					Waiting: &corev1.ContainerStateWaiting{
						Reason:  "CrashLoopBackOff",
						Message: "back-off 40s restarting failed container=app",
					},
				},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "Error",
						ExitCode: 1,
						Message:  "panic: nil map write",
					},
				},
			},
		},
	})

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase to remain %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
	if cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionRestoreCrashed); cond != nil {
		t.Errorf("Expected no %s condition, got %+v", ConditionRestoreCrashed, cond)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Errorf("Expected replacement pod to still exist, got err=%v", err)
	}
}

// T3: StartError/128 whose message matches no known signature is reported, not
// acted on: no deletion, PMJ stays Restoring, bounded by the 5m ceiling.
func TestPodMigrationJobReconciler_Restoring_UnmatchedStartErrorDoesNotDeletePod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashUnknownMessage))

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	before := counterValue(t, metrics.RestoreCrashUnmatchedTotal)
	fallbackBefore := counterValue(t, metrics.RestoreCrashFallbackTotal)

	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected a requeue so the 5m restore ceiling can conclude the PMJ, got %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected phase to remain %s, got %s", pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionUnrecognizedRestoreCrash)
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("Expected %s condition True, got %+v", ConditionUnrecognizedRestoreCrash, cond)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Errorf("Expected replacement pod to still exist, got err=%v", err)
	}
	if got := counterValue(t, metrics.RestoreCrashUnmatchedTotal) - before; got != 1 {
		t.Errorf("Expected unmatched counter to increment by 1, got %v", got)
	}
	if got := counterValue(t, metrics.RestoreCrashFallbackTotal) - fallbackBefore; got != 0 {
		t.Errorf("Expected fallback counter untouched, got +%v", got)
	}
}

// T4: the Warning event for an unmatched StartError fires exactly once; the
// condition guard must survive repeated reconciles of the same pod.
func TestPodMigrationJobReconciler_Restoring_UnmatchedStartErrorEmitsEventOnce(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashUnknownMessage))

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: recorder,
	}

	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := r.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
		}); err != nil {
			t.Fatalf("Reconcile %d failed: %v", i, err)
		}
	}

	var events []string
	for {
		select {
		case e := <-recorder.Events:
			events = append(events, e)
			continue
		default:
		}
		break
	}

	matching := 0
	for _, e := range events {
		if strings.Contains(e, ReasonUnrecognizedRestoreCrash) {
			matching++
		}
	}
	if matching != 1 {
		t.Errorf("Expected exactly 1 %s event across 3 reconciles, got %d (%v)", ReasonUnrecognizedRestoreCrash, matching, events)
	}
}

// T5a: re-adoption guard, layer 1.  After the fallback concludes the PMJ
// (Consumed=true, Phase=Failed) the successor pod must not adopt it — the
// snapshot that crashed the first pod would crash the second one too.  Each
// guard is asserted independently so a future refactor cannot silently drop
// the one that is actually load-bearing.
func TestFindUnassignedActivePMJ_DoesNotReadoptConcludedRestoreCrashPMJ(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-crashed"

	tests := []struct {
		name     string
		consumed bool
		phase    pmv1alpha1.PodMigrationJobPhase
		want     string
	}{
		{
			name:     "both guards: consumed and Failed after restore-crash fallback",
			consumed: true,
			phase:    pmv1alpha1.PodMigrationJobPhaseFailed,
			want:     "",
		},
		{
			name:     "consumed guard alone is load-bearing",
			consumed: true,
			phase:    pmv1alpha1.PodMigrationJobPhaseRestoring,
			want:     "",
		},
		{
			name:     "Failed phase guard alone is load-bearing",
			consumed: false,
			phase:    pmv1alpha1.PodMigrationJobPhaseFailed,
			want:     "",
		},
		{
			name:     "control: an unconsumed active PMJ is still adoptable",
			consumed: false,
			phase:    pmv1alpha1.PodMigrationJobPhaseRestoring,
			want:     jobName,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pmj := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: jobName},
				Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: podName}},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:    tc.phase,
					Consumed: tc.consumed,
				},
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj).Build()

			got, err := util.FindUnassignedActivePMJ(context.Background(), cl, namespace, podName, "", "", "", "", "")
			if err != nil {
				t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
			}
			if got != tc.want {
				t.Errorf("Expected %q, got %q", tc.want, got)
			}
		})
	}
}

// T5b: re-adoption guard, layer 2.  Even if a successor pod still carries the
// assigned-pmj annotation for the consumed PMJ, the gate controller must
// release it with the cold-start bypass rather than injecting the snapshot.
func TestPodGateReconciler_SuccessorPodOfConsumedPMJGetsColdStartBypass(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-crashed"

	// Both rows model states the Issue #15 fallback can actually produce. The
	// fallback persists Phase=Failed BEFORE deleting the crashed pod, and that
	// pod had necessarily been un-gated already (it started, then failed to
	// restore), so GateReleased is true by the time a successor appears.
	//
	// Issue #25's stranded-PMJ recovery in pod_gate_controller.go deliberately
	// claims the complementary state — Phase=Restoring AND !GateReleased, i.e.
	// a consumer that vanished before it was ever un-gated — and lets a
	// successor adopt the snapshot there. That state is unreachable from this
	// fallback. The two guards are therefore independent and both load-bearing:
	// do not "simplify" this by dropping either one.
	tests := []struct {
		name         string
		phase        pmv1alpha1.PodMigrationJobPhase
		gateReleased bool
	}{
		{
			name:         "PMJ concluded Failed by the restore-crash fallback",
			phase:        pmv1alpha1.PodMigrationJobPhaseFailed,
			gateReleased: true,
		},
		{
			name:         "PMJ still Restoring but its consumer was already un-gated",
			phase:        pmv1alpha1.PodMigrationJobPhaseRestoring,
			gateReleased: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			successorPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: namespace,
					Name:      "pod-successor",
					UID:       "uid-successor",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": pmjName,
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				},
			}

			pmj := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: pmjName},
				Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "pod-orig"}},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:           tc.phase,
					SnapshotRef:     "snapshot-ref",
					Consumed:        true,
					GateReleased:    tc.gateReleased,
					RestoredPodUID:  "uid-crashed-first-pod",
					RestoredPodName: "pod-crashed",
				},
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(successorPod, pmj).
				Build()

			gate := &PodGateReconciler{Client: cl, Scheme: scheme}

			ctx := context.Background()
			if _, err := gate.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: "pod-successor"},
			}); err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			updated := &corev1.Pod{}
			if err := cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-successor"}, updated); err != nil {
				t.Fatalf("Failed to get successor pod: %v", err)
			}
			if _, ok := updated.Annotations["pod-migration.gke.io/assigned-pmj"]; ok {
				t.Errorf("Expected assigned-pmj annotation to be removed, got %q", updated.Annotations["pod-migration.gke.io/assigned-pmj"])
			}
			if val, ok := updated.Annotations["podsnapshot.gke.io/ps-name"]; !ok || val != "" {
				t.Errorf("Expected podsnapshot.gke.io/ps-name to be the empty-string bypass, got %q (present: %t)", val, ok)
			}
			for _, g := range updated.Spec.SchedulingGates {
				if g.Name == MigrationGateName {
					t.Error("Expected migration scheduling gate to be removed")
				}
			}
		})
	}
}

// T7: a Delete that fails after the status write must not strand the crashed
// pod.  The fallback persists Phase=Failed BEFORE deleting, so the retry can no
// longer enter the Restoring case — without the Failed-phase completion path
// the wedged pod would survive until the PMJ was garbage collected 30 minutes
// later, which is exactly the permanent wedge Issue #15 exists to break.
func TestPodMigrationJobReconciler_RestoreCrash_DeleteRetriedAfterTransientFailure(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashOCIMessage))

	// Emulate a throttled DELETE: at 2,000-pod evacuation scale against the
	// client-go QPS budget this is an expected outcome, not an exotic one.
	deleteFails := true
	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if _, isPod := obj.(*corev1.Pod); isPod && deleteFails {
					return apierrors.NewTooManyRequests("client rate limiter", 1)
				}
				return cl.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName}}

	fallbackBefore := counterValue(t, metrics.RestoreCrashFallbackTotal)

	// Reconcile 1: the verdict is persisted, then the delete fails.
	if _, err := r.Reconcile(ctx, req); err == nil {
		t.Fatal("Expected the failed delete to be returned so the reconcile is retried")
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("Expected the verdict to be durable (phase %s), got %s",
			pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
	}
	if !meta.IsStatusConditionTrue(updatedPMJ.Status.Conditions, ConditionRestoreCrashed) {
		t.Fatalf("Expected %s condition True after the first reconcile", ConditionRestoreCrashed)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Fatalf("Expected the crashed pod to still exist after the failed delete, got err=%v", err)
	}

	// Reconcile 2 with the transient error cleared: the completion path must
	// finish the deletion even though the phase is no longer Restoring.
	deleteFails = false
	if _, err := r.Reconcile(ctx, req); err != nil {
		t.Fatalf("Retry reconcile failed: %v", err)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Errorf("Expected the crashed pod to be deleted on retry, got err=%v", err)
	}

	// The fallback is one event, not one per retry.
	if got := counterValue(t, metrics.RestoreCrashFallbackTotal) - fallbackBefore; got != 1 {
		t.Errorf("Expected the fallback counter to increment exactly once across both reconciles, got %v", got)
	}
}

// The completion path must never delete a same-named successor: that pod is
// the cold-start replacement the fallback exists to create.
func TestPodMigrationJobReconciler_RestoreCrash_CompletionPathSparesSuccessorPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName

	// Same name, different UID: the crashed instance is already gone and the
	// StatefulSet has recreated it.
	successor := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       types.UID("successor-uid-9999"),
		},
	}

	completionTime := metav1.Now()
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: jobName},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: podName}},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseFailed,
			CompletionTime:  &completionTime,
			Consumed:        true,
			RestoredPodUID:  "restored-uid-1234",
			RestoredPodName: podName,
			Conditions: []metav1.Condition{
				{
					Type:               ConditionRestoreCrashed,
					Status:             metav1.ConditionTrue,
					Reason:             ReasonRestoreCrashFallback,
					Message:            "restore crashed",
					LastTransitionTime: completionTime,
				},
			},
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(successor, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Errorf("Expected the successor pod to survive the completion path, got err=%v", err)
	}
}

// recoveredPodStatus is a pod that failed to start once and then came up: it is
// Running and Ready, but keeps the StartError/128 from the failed attempt in
// LastTerminationState forever — carrying a real gVisor signature in it.
//
// This shape was missing from the taxonomy the detector was derived from, which
// was built entirely from crashed pods.  The first implementation read
// LastTerminationState unconditionally and so DELETED pods in this state.
func recoveredPodStatus(message string) corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
		ContainerStatuses: []corev1.ContainerStatus{
			{
				Name:         "app",
				Ready:        true,
				RestartCount: 1,
				State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
				LastTerminationState: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						Reason:   "StartError",
						ExitCode: 128,
						Message:  message,
					},
				},
			},
		},
	}
}

// T7 (K1/K2): a replacement pod that hit a restore crash and then RECOVERED is
// Running and Ready, but still carries StartError/128 in LastTerminationState.
// It must survive and the PMJ must conclude Succeeded.
//
// Two independent defects made this fail: the classifier read termination
// history unconditionally, and step 5 ran before the readiness check, so a
// Ready pod could still be intercepted.  Both are exercised here — the fixture
// carries a signature that WOULD match if either guard were missing.
func TestPodMigrationJobReconciler_Restoring_RecoveredReadyPodIsNotDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		recoveredPodStatus(restoreCrashOCIMessage))

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	before := counterValue(t, metrics.RestoreCrashFallbackTotal)

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Fatalf("Expected the recovered pod to survive, got err=%v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}
	if cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionRestoreCrashed); cond != nil {
		t.Errorf("Expected no %s condition on a recovered pod, got %+v", ConditionRestoreCrashed, cond)
	}
	if got := counterValue(t, metrics.RestoreCrashFallbackTotal) - before; got != 0 {
		t.Errorf("Expected the fallback counter not to move, got +%v", got)
	}
}

// T8 (B1): a crash first OBSERVED after the 5-minute ceiling is still a crash.
//
// The ceiling sits above the pod fetch and, for a Consumed PMJ, concluded
// SucceededWithoutRestore without ever looking at the pod — recording success
// and leaving the pod wedged forever.  Any delay in first observation triggers
// it: controller restart, leader handoff, or the evacuation backlog this
// feature was filed for.
func TestPodMigrationJobReconciler_Restoring_CrashAfterCeilingStillRunsFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashOCIMessage))

	// Push the restore start past the 5-minute ceiling.
	restoringStart := metav1.NewTime(time.Now().Add(-6 * time.Minute))
	pmj.Status.RestoringStartTime = &restoringStart

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("Ceiling recorded SucceededWithoutRestore for a crashed pod; the crash was never acted on")
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseFailed, updatedPMJ.Status.Phase)
	}
	if !meta.IsStatusConditionTrue(updatedPMJ.Status.Conditions, ConditionRestoreCrashed) {
		t.Errorf("Expected %s condition True, got %+v", ConditionRestoreCrashed, updatedPMJ.Status.Conditions)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Errorf("Expected the crashed pod to be deleted by the post-ceiling fallback, got err=%v", err)
	}
}

// T9 (B2): the destructive decision is confirmed against the API server.
//
// The informer cache serves the per-reconcile classification, so it can show a
// crash on a pod that has already recovered.  Here the cache holds the crashed
// snapshot while the API server holds the recovered one: nothing may be written
// and nothing may be deleted.
func TestPodMigrationJobReconciler_Restoring_StaleCachedCrashIsNotActedOn(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	// The cache's view: crashed.
	cachedPod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashOCIMessage))

	// The API server's view: the same pod instance, recovered.
	livePod := cachedPod.DeepCopy()
	livePod.Status = recoveredPodStatus(restoreCrashOCIMessage)

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(cachedPod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()
	apiReader := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(livePod).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, APIReader: apiReader, Scheme: scheme}

	before := counterValue(t, metrics.RestoreCrashFallbackTotal)

	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter == 0 {
		t.Errorf("Expected the unconfirmed verdict to requeue rather than conclude, got %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("Expected the PMJ to stay %s on an unconfirmed verdict, got %s",
			pmv1alpha1.PodMigrationJobPhaseRestoring, updatedPMJ.Status.Phase)
	}
	if cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionRestoreCrashed); cond != nil {
		t.Errorf("Expected no %s condition to be persisted off a stale cache read, got %+v", ConditionRestoreCrashed, cond)
	}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Errorf("Expected the pod to survive an unconfirmed verdict, got err=%v", err)
	}
	if got := counterValue(t, metrics.RestoreCrashFallbackTotal) - before; got != 0 {
		t.Errorf("Expected the fallback counter not to move, got +%v", got)
	}
}

// T10 (B4): UnrecognizedRestoreCrash must be retractable.
//
// An unrecognised start failure is deliberately never acted on, so the
// container can start on a later attempt.  The condition was only ever set
// True, which permanently mislabelled a migration that went on to succeed.
func TestPodMigrationJobReconciler_Restoring_UnrecognizedConditionClearedOnSuccess(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	// A pod that hit an unrecognised start failure and has since come up Ready.
	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		recoveredPodStatus(restoreCrashUnknownMessage))
	pmj.Status.Conditions = []metav1.Condition{
		{
			Type:               ConditionUnrecognizedRestoreCrash,
			Status:             metav1.ConditionTrue,
			Reason:             ReasonUnrecognizedRestoreCrash,
			Message:            "unrecognised start failure",
			LastTransitionTime: metav1.Now(),
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Fatalf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceeded, updatedPMJ.Status.Phase)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionUnrecognizedRestoreCrash)
	if cond == nil {
		t.Fatalf("Expected %s to be retracted, not removed", ConditionUnrecognizedRestoreCrash)
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected %s to be False once the pod recovered, got %s", ConditionUnrecognizedRestoreCrash, cond.Status)
	}
}

// A job that never saw an unrecognised crash must not acquire a vacuous False
// condition, matching the hygiene clearEvictionBlockageConditions already
// enforces.
func TestPodMigrationJobReconciler_Restoring_NoVacuousUnrecognizedCondition(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	podUID := "restored-uid-1234"
	jobName := "pmj-" + podName

	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID, corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		},
	})

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{Client: fakeClient, Scheme: scheme}

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, ConditionUnrecognizedRestoreCrash); cond != nil {
		t.Errorf("Expected no %s condition on a job that never saw one, got %+v", ConditionUnrecognizedRestoreCrash, cond)
	}
}

func TestPodMigrationJobReconciler_mapSnapshotToPMJ(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pmjWithSnap := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-with-snap",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "snap-123",
		},
	}
	pmjSnapshottingDifferentSnap := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-other-snap",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef: "snap-456",
		},
	}
	pmjSnapshottingNoSnap := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-no-snap",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}
	pmjOtherNamespace := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "pmj-other-ns",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: "snap-123",
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithObjects(pmjWithSnap, pmjSnapshottingDifferentSnap, pmjSnapshottingNoSnap, pmjOtherNamespace).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	snapObj := &unstructured.Unstructured{}
	snapObj.SetNamespace("default")
	snapObj.SetName("snap-123")

	reqs := r.mapSnapshotToPMJ(context.Background(), snapObj)
	if len(reqs) != 1 {
		t.Fatalf("Expected exactly 1 request mapped to owning PMJ, got %d: %+v", len(reqs), reqs)
	}
	if reqs[0].Name != "pmj-with-snap" || reqs[0].Namespace != "default" {
		t.Errorf("Expected pmj-with-snap in default, got %+v", reqs[0])
	}
}

func TestPodMigrationJobReconciler_handleStatusError(t *testing.T) {
	r := &PodMigrationJobReconciler{}
	ctx := context.Background()

	// Conflict error -> silent requeue without error
	conflictErr := apierrors.NewConflict(schema.GroupResource{Group: "podmigration.gke.io", Resource: "podmigrationjobs"}, "my-job", fmt.Errorf("object modified"))
	res, err := r.handleStatusError(ctx, conflictErr, "Failed to update status")
	if err != nil {
		t.Errorf("Expected nil error on conflict, got %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue: true on conflict, got %+v", res)
	}

	// Non-conflict error -> returns error
	internalErr := apierrors.NewInternalError(fmt.Errorf("db connection failed"))
	res, err = r.handleStatusError(ctx, internalErr, "Failed to update status")
	if err == nil {
		t.Errorf("Expected non-nil error on internal error")
	}
	if res.Requeue {
		t.Errorf("Expected Requeue: false on non-conflict error, got %+v", res)
	}
}

type fakeSnapshotProvider struct {
	ensureTriggerResult ctrl.Result
	ensureTriggerErr    error
	checkStatusResult   *snapshot.Status
	checkStatusErr      error
	cleanupErr          error
}

func (f *fakeSnapshotProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	return f.ensureTriggerResult, f.ensureTriggerErr
}

func (f *fakeSnapshotProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	return f.checkStatusResult, f.checkStatusErr
}

func (f *fakeSnapshotProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	return f.cleanupErr
}

func TestPodMigrationJobReconciler_Snapshotting_RecordsSnapshotRefInProgress(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"
	jobName := "pmj-" + podName
	snapName := "ps-early-discovered"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "origin-uid-123",
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "origin-uid-123",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	mockProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:       snapshot.PhaseInProgress,
			SnapshotRef: snapName,
			Reason:      "Snapshotting",
			Message:     "Waiting for checkpoint",
		},
	}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("Expected RequeueAfter 30s, got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.SnapshotRef != snapName {
		t.Errorf("Expected SnapshotRef %q recorded in progress, got %q", snapName, updatedPMJ.Status.SnapshotRef)
	}

	// Verify mapSnapshotToPMJ now maps to this PMJ via the index
	snapObj := &unstructured.Unstructured{}
	snapObj.SetNamespace(namespace)
	snapObj.SetName(snapName)
	reqs := r.mapSnapshotToPMJ(context.Background(), snapObj)
	if len(reqs) != 1 || reqs[0].Name != jobName {
		t.Errorf("Expected mapped request for %q, got %+v", jobName, reqs)
	}
}

type fakeRESTMapper struct {
	meta.RESTMapper
	mappingFn func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error)
}

func (f *fakeRESTMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if f.mappingFn != nil {
		return f.mappingFn(gk, versions...)
	}
	return nil, nil
}

func TestShouldWatchCRD(t *testing.T) {
	gvk := schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshot",
	}

	tests := []struct {
		name      string
		mappingFn func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error)
		wantWatch bool
		wantErr   bool
	}{
		{
			name: "CRD registered",
			mappingFn: func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
				return &meta.RESTMapping{
					Resource: schema.GroupVersionResource{
						Group:    gvk.Group,
						Version:  gvk.Version,
						Resource: "podsnapshots",
					},
					GroupVersionKind: gvk,
				}, nil
			},
			wantWatch: true,
			wantErr:   false,
		},
		{
			name: "CRD not registered (NoKindMatchError)",
			mappingFn: func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
				return nil, &meta.NoKindMatchError{GroupKind: gk}
			},
			wantWatch: false,
			wantErr:   false,
		},
		{
			name: "CRD not registered (NoResourceMatchError)",
			mappingFn: func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
				return nil, &meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: gk.Group, Resource: gk.Kind}}
			},
			wantWatch: false,
			wantErr:   false,
		},
		{
			name: "Discovery failure (e.g. apiserver unreachable)",
			mappingFn: func(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
				return nil, fmt.Errorf("connection refused")
			},
			wantWatch: false,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mapper := &fakeRESTMapper{mappingFn: tc.mappingFn}
			watch, err := shouldWatchCRD(mapper, gvk)
			if (err != nil) != tc.wantErr {
				t.Fatalf("shouldWatchCRD() error = %v, wantErr %v", err, tc.wantErr)
			}
			if watch != tc.wantWatch {
				t.Errorf("shouldWatchCRD() = %v, want %v", watch, tc.wantWatch)
			}
		})
	}
}

func TestPodMigrationJobReconciler_StrictInvariantMode_CleansUpTriggerAndSuppressesRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "redis-strict"
	targetUID := "uid-origin-strict"
	jobName := util.FormatPMJName(podName, targetUID)
	triggerName := util.FormatPSMTName(podName, targetUID)

	// Create a PMJ in Restoring whose replacement pod has a mismatched pod-template-hash (I6 violation)
	// and an active PodSnapshotManualTrigger in the namespace.
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
			Labels: map[string]string{
				util.LabelPodTemplateHash: "hash-v1",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: targetUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snap-strict-1",
			RestoredPodName: "redis-repl-v2",
			RestoredPodUID:  "uid-repl-v2",
		},
	}

	replPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "redis-repl-v2",
			UID:       types.UID("uid-repl-v2"),
			Labels: map[string]string{
				"pod-template-hash": "hash-v2",
			},
			Annotations: map[string]string{
				"podsnapshot.gke.io/ps-name": "snap-strict-1",
			},
		},
	}

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	trigger.SetNamespace(namespace)
	trigger.SetName(triggerName)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj, replPod, trigger).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	reconciler := &PodMigrationJobReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: invariants.NewEngine(invariants.ModeStrict, recorder),
	}

	res, err := reconciler.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile returned unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("Expected strict invariant failure to suppress requeue, got %+v", res)
	}

	updated := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updated); err != nil {
		t.Fatalf("Failed to fetch updated PMJ: %v", err)
	}
	if updated.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("Expected PMJ phase Failed under strict mode I6 violation, got %s", updated.Status.Phase)
	}

	// Verify PodSnapshotManualTrigger was cleaned up by strict mode failure path
	checkTrigger := &unstructured.Unstructured{}
	checkTrigger.SetGroupVersionKind(trigger.GroupVersionKind())
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: triggerName}, checkTrigger)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected PodSnapshotManualTrigger %s to be deleted on strict mode failure, got err=%v", triggerName, err)
	}
}

func TestReconcilers_LiveInvariantEvaluation_I1_PodGate_And_PodMigration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"

	// 1. PodMigrationJobReconciler live I1 evaluation from cached NamespacePods (duplicate snapshot consumption)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "pmj-i1-live",
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "origin-i1"},
			TargetPodUID: "uid-origin-i1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:    "snap-shared-1",
			Consumed:       true,
			GateReleased:   true,
			RestoredPodUID: "uid-consumer-1",
		},
	}
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        "consumer-1",
			UID:         types.UID("uid-consumer-1"),
			Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "snap-shared-1"},
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   namespace,
			Name:        "consumer-2",
			UID:         types.UID("uid-consumer-2"),
			Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "snap-shared-1"},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj, pod1, pod2).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	recorder := record.NewFakeRecorder(10)
	pmjRec := &PodMigrationJobReconciler{
		Client:          fakeClient,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: invariants.NewEngine(invariants.ModeStrict, recorder),
	}
	if _, err := pmjRec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: "pmj-i1-live"},
	}); err != nil {
		t.Fatalf("PodMigrationJobReconciler.Reconcile failed: %v", err)
	}
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: "pmj-i1-live"}, updatedPMJ)
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("Expected live I1 duplicate snapshot consumption to fail PMJ in strict mode, got phase %s", updatedPMJ.Status.Phase)
	}

	// 2. PodGateReconciler live evaluation (I8 gated system pod assigned to an active PMJ emits Warning event)
	sysPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "pmj-coredns",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}
	sysPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      "coredns-gated",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "pmj-coredns",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
		},
	}
	gateRecorder := record.NewFakeRecorder(10)
	gateClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, func(obj client.Object) []string {
			pod, ok := obj.(*corev1.Pod)
			if !ok || pod.Annotations == nil {
				return nil
			}
			if val, exists := pod.Annotations[util.AnnotationAssignedPMJ]; exists && val != "" {
				return []string{val}
			}
			return nil
		}).
		WithObjects(sysPMJ, sysPod).
		Build()
	gateRec := &PodGateReconciler{
		Client:          gateClient,
		Scheme:          scheme,
		Recorder:        gateRecorder,
		InvariantEngine: invariants.NewEngine(invariants.ModeObserve, gateRecorder),
	}
	if _, err := gateRec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: "kube-system", Name: "coredns-gated"},
	}); err != nil {
		t.Fatalf("PodGateReconciler.Reconcile failed: %v", err)
	}
	if len(gateRecorder.Events) == 0 {
		t.Fatalf("Expected PodGateReconciler to emit InvariantViolation Warning event on gated kube-system pod")
	}

	// 3. PodMigrationReconciler live evaluation (I4 stuck deletion deferral finalizer emits Warning event)
	oldDel := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	mig := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "mig-stuck-finalizer",
			DeletionTimestamp: &oldDel,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
	}
	migRecorder := record.NewFakeRecorder(10)
	migClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(mig).
		WithInterceptorFuncs(interceptor.Funcs{
			Delete: func(ctx context.Context, client client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				return fmt.Errorf("simulated storage cleanup error")
			},
		}).
		Build()
	migRec := &PodMigrationReconciler{
		Client:          migClient,
		Scheme:          scheme,
		Recorder:        migRecorder,
		InvariantEngine: invariants.NewEngine(invariants.ModeObserve, migRecorder),
	}
	_, _ = migRec.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: "mig-stuck-finalizer"},
	})
	foundI4 := false
	for len(migRecorder.Events) > 0 {
		ev := <-migRecorder.Events
		if strings.Contains(ev, "[I4:TerminalProgress]") {
			foundI4 = true
		}
	}
	if !foundI4 {
		t.Fatalf("Expected PodMigrationReconciler to emit [I4:TerminalProgress] Warning event on stuck deletion finalizer")
	}
}

func TestPodMigrationJobReconciler_CustomTimeout_Annotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "annotated-pod"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-annotated",
		},
	}

	// Job created 15 minutes ago, but annotated with 25m timeout
	creationTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: creationTime,
			Annotations: map[string]string{
				util.AnnotationMigrationTimeout: "25m",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-annotated",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
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
	if !res.Requeue {
		t.Errorf("Expected Requeue: true, got %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSnapshotting, updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Pending_PropagatesPodTimeoutAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "annotated-pod-source"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-source",
			Annotations: map[string]string{
				util.AnnotationMigrationTimeout: "35m",
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-source",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
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
	if res.Requeue {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
		}); err != nil {
			t.Fatalf("Second Reconcile failed: %v", err)
		}
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Annotations[util.AnnotationMigrationTimeout] != "35m0s" {
		t.Errorf("Expected annotation 35m0s, got %q", updatedPMJ.Annotations[util.AnnotationMigrationTimeout])
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase to transition to Snapshotting, got %s", updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Pending_ScalesMemoryRequestTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "large-mem-pod"
	jobName := "pmj-" + podName

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-mem",
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "redis",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
				},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-mem",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
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
	if !res.Requeue {
		t.Errorf("Expected reconcile to requeue")
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Annotations[util.AnnotationMigrationTimeout] != "15m27s" {
		t.Errorf("Expected annotation 15m27s, got %q", updatedPMJ.Annotations[util.AnnotationMigrationTimeout])
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase to transition to Snapshotting, got %s", updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Pending_WithMemoryScalingAndPVC_PreservesOriginNodeAndPVsToDetach(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "redis-pod"
	jobName := "pmj-" + podName
	pvcName := "redis-pvc"
	pvName := "redis-pv"
	originNode := "worker-node-42"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-redis-123",
		},
		Spec: corev1.PodSpec{
			NodeName: originNode,
			Containers: []corev1.Container{
				{
					Name: "redis",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "redis-data",
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
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pvcName,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName: pvName,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-redis-123",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pvc, pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// Exactly one reconcile: must update timeout annotation, backfill OriginNodeName & PVsToDetach,
	// and transition to Snapshotting without clobbering unpersisted status.
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected reconcile to requeue")
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected phase Snapshotting on single reconcile, got %s", updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Annotations[util.AnnotationMigrationTimeout] != "15m27s" {
		t.Errorf("Expected annotation 15m27s, got %q", updatedPMJ.Annotations[util.AnnotationMigrationTimeout])
	}
	if updatedPMJ.Status.OriginNodeName != originNode {
		t.Errorf("Expected OriginNodeName %q, got %q", originNode, updatedPMJ.Status.OriginNodeName)
	}
	if len(updatedPMJ.Status.PVsToDetach) != 1 || updatedPMJ.Status.PVsToDetach[0] != pvName {
		t.Errorf("Expected PVsToDetach [%q], got %v", pvName, updatedPMJ.Status.PVsToDetach)
	}
}

func TestPodMigrationJobReconciler_Snapshotting_Progressing_ExtendsDeadline(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "progressing-pod"
	jobName := "pmj-" + podName
	snapName := "snap-active-upload"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-progressing",
		},
	}

	// Job was created 15 minutes ago (baseline timeout 10m has elapsed)
	creationTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: creationTime,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-progressing",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef: snapName,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	// Snapshot is actively progressing (last progress was 1 minute ago)
	mockProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:            snapshot.PhaseInProgress,
			SnapshotRef:      snapName,
			Reason:           "Snapshotting",
			Message:          "Uploading chunk 42/100",
			LastProgressTime: time.Now().Add(-1 * time.Minute),
		},
	}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 30*time.Second {
		t.Errorf("Expected RequeueAfter 30s, got %v", res.RequeueAfter)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Errorf("Expected PMJ to remain in Snapshotting, got %s", updatedPMJ.Status.Phase)
	}
}

func TestPodMigrationJobReconciler_Snapshotting_Stalled_TimesOut(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "stalled-pod"
	jobName := "pmj-" + podName
	snapName := "snap-stalled"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-stalled",
		},
	}

	// Job was created 15 minutes ago
	creationTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: creationTime,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-stalled",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef: snapName,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	// Snapshot is stalled (last progress was 12 minutes ago, exceeding 10m baseline)
	mockProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:            snapshot.PhaseInProgress,
			SnapshotRef:      snapName,
			Reason:           "Snapshotting",
			Message:          "Stuck upload",
			LastProgressTime: time.Now().Add(-12 * time.Minute),
		},
	}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected PMJ to transition to Failed, got %s", updatedPMJ.Status.Phase)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "Timeout" {
		t.Errorf("Expected Ready condition Reason=Timeout, got %+v", cond)
	}
}

func TestPodMigrationJobReconciler_Snapshotting_FastFail_CheckpointTerminalError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "fast-fail-pod"
	jobName := "pmj-" + podName
	snapName := "snap-terminal-error"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-fast-fail",
		},
	}

	// Job was just created (well within 10-minute timeout)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-fast-fail",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef: snapName,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	mockProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:       snapshot.PhaseFailed,
			SnapshotRef: snapName,
			Reason:      "SnapshotFailed",
			Message:     "GKE PodSnapshot Checkpoint failed (DeadlineExceeded): snapshot agent timed out writing checkpoint stream",
		},
	}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected PMJ to transition immediately to Failed, got %s", updatedPMJ.Status.Phase)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "SnapshotFailed" {
		t.Errorf("Expected Ready condition Reason=SnapshotFailed, got %+v", cond)
	}
}

func TestPodMigrationJobReconciler_Evicting_PostDeadlineExtension_DoesNotTimeoutImmediately(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "evicting-pod"
	jobName := "pmj-" + podName
	snapName := "snap-large-completed"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-evicting",
		},
	}

	// Job was created 25 minutes ago (snapshot upload took 25 minutes with baseline 10m timeout)
	creationTime := metav1.NewTime(time.Now().Add(-25 * time.Minute))

	// PMJ starts in PhaseSnapshotting without pre-populated evicting-since annotation
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: creationTime,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-evicting",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef: snapName,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Snapshotting",
					LastTransitionTime: creationTime,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	mockProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:            snapshot.PhaseReady,
			SnapshotRef:      snapName,
			LastProgressTime: time.Now().Add(-1 * time.Minute),
		},
	}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	// First reconcile: handles Snapshotting -> Evicting transition
	res1, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile 1 (Snapshotting -> Evicting) failed: %v", err)
	}
	if !res1.Requeue {
		t.Fatalf("Expected Reconcile 1 to requeue on transition to Evicting, got %+v", res1)
	}

	// Second reconcile: handles top-of-reconcile timeout guard and entries into Evicting phase
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile 2 (Evicting handoff) failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	// Must NOT fail immediately upon entering Evicting phase post-snapshot-extension
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Errorf("Expected PMJ to remain in Evicting, got %s", updatedPMJ.Status.Phase)
	}
	if updatedPMJ.Status.EvictingStartTime == nil {
		t.Errorf("Expected EvictingStartTime to be set, got nil")
	}
	if updatedPMJ.Annotations == nil || updatedPMJ.Annotations[util.AnnotationEvictingSince] == "" {
		t.Errorf("Expected evicting-since annotation to be stamped, got annotations=%v", updatedPMJ.Annotations)
	}
}

func TestPodMigrationJobReconciler_Evicting_UpgradeInFlightJob_SeedsEvictingStartTimeFromAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "in-flight-pod"
	jobName := "pmj-" + podName
	pastEvictingTime := time.Now().Add(-2 * time.Minute).Truncate(time.Second)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       types.UID("pod-uid-42"),
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              jobName,
			Namespace:         namespace,
			CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
			Annotations: map[string]string{
				util.AnnotationEvictingSince: pastEvictingTime.Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: podName,
			},
			TargetPodUID: "pod-uid-42",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:             pmv1alpha1.PodMigrationJobPhaseEvicting,
			EvictingStartTime: nil, // Simulating an in-flight job prior to upgrade
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Evicting",
					LastTransitionTime: metav1.NewTime(pastEvictingTime),
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj, pod).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	if updatedPMJ.Status.EvictingStartTime == nil {
		t.Fatalf("Expected EvictingStartTime to be populated from annotation, got nil")
	}
	if !updatedPMJ.Status.EvictingStartTime.Time.Equal(pastEvictingTime) {
		t.Errorf("Expected EvictingStartTime to equal %v, got %v", pastEvictingTime, updatedPMJ.Status.EvictingStartTime.Time)
	}
}

func TestPodMigrationJobReconciler_Evicting_PhaseTimeout_AfterExtendedSnapshot(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "evicting-stuck-pod"
	jobName := "pmj-" + podName
	snapName := "snap-large-completed"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-evicting-stuck",
		},
	}

	// Job was created 35 minutes ago
	creationTime := metav1.NewTime(time.Now().Add(-35 * time.Minute))
	// Transitioned to Evicting 12 minutes ago (exceeding baseline 10m eviction window)
	evictingTransitionTime := metav1.NewTime(time.Now().Add(-12 * time.Minute))

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              jobName,
			CreationTimestamp: creationTime,
			Annotations: map[string]string{
				"pod-migration.gke.io/evicting-since": evictingTransitionTime.Time.Format(time.RFC3339),
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-evicting-stuck",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef: snapName,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "Evicting",
					Message:            "Snapshot durable; waiting for origin pod eviction and volume detachment",
					LastTransitionTime: evictingTransitionTime,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	mockProvider := &fakeSnapshotProvider{}

	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, updatedPMJ); err != nil {
		t.Fatalf("Failed to get PMJ: %v", err)
	}
	// Must time out because Evicting phase exceeded its budget
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("Expected PMJ to transition to Failed, got %s", updatedPMJ.Status.Phase)
	}
	cond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if cond == nil || cond.Reason != "Timeout" {
		t.Errorf("Expected Ready condition Reason=Timeout, got %+v", cond)
	}
}
