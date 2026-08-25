package snapshot

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func setupTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

func TestGKEProvider_EnsureTrigger(t *testing.T) {
	scheme := setupTestScheme()
	client := fake.NewClientBuilder().WithScheme(scheme).Build()
	provider := NewGKEProvider(client, scheme)

	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			UID:       types.UID("job-uid-123"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "pod-uid-456",
		},
	}

	res, err := provider.EnsureTrigger(context.Background(), job, "test-pod")
	if err != nil {
		t.Fatalf("EnsureTrigger failed: %v", err)
	}
	if res.Requeue {
		t.Errorf("expected requeue=false, got true")
	}

	// Verify trigger object was created
	triggerName := util.FormatPSMTName("test-pod", "pod-uid-456")
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: triggerName}, trigger); err != nil {
		t.Fatalf("expected trigger to exist: %v", err)
	}
}

func TestGKEProvider_CheckStatus(t *testing.T) {
	scheme := setupTestScheme()
	triggerName := util.FormatPSMTName("test-pod", "pod-uid-456")
	snapshotName := "ps-snapshot-123"

	t.Run("TriggerNotFound", func(t *testing.T) {
		client := fake.NewClientBuilder().WithScheme(scheme).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseInProgress {
			t.Errorf("expected PhaseInProgress, got %v", status.Phase)
		}
	})

	t.Run("PSMT_TriggerFailed_FastFail", func(t *testing.T) {
		trigger := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshotManualTrigger",
				"metadata": map[string]interface{}{
					"name":      triggerName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Triggered",
							"status":  "False",
							"reason":  "Failed",
							"message": "target pod is not using the gvisor runtime class",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("expected PhaseFailed, got %v", status.Phase)
		}
		if status.Reason != "SnapshotTriggerFailed" {
			t.Errorf("expected Reason=SnapshotTriggerFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "not using the gvisor runtime class") {
			t.Errorf("expected message to contain error details, got %s", status.Message)
		}
	})

	t.Run("PodSnapshot_CheckpointFailed_FastFail", func(t *testing.T) {
		trigger := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshotManualTrigger",
				"metadata": map[string]interface{}{
					"name":      triggerName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"snapshotCreated": map[string]interface{}{
						"name": snapshotName,
					},
				},
			},
		}
		snapshot := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshot",
				"metadata": map[string]interface{}{
					"name":      snapshotName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Checkpoint",
							"status":  "False",
							"reason":  "Failed",
							"message": "runsc checkpoint: unhandled syscall",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshot).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("expected PhaseFailed, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "runsc checkpoint: unhandled syscall") {
			t.Errorf("expected message to contain error details, got %s", status.Message)
		}
	})

	t.Run("PodSnapshot_StorageReplicationFailed_FastFail", func(t *testing.T) {
		trigger := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshotManualTrigger",
				"metadata": map[string]interface{}{
					"name":      triggerName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"snapshotCreated": map[string]interface{}{
						"name": snapshotName,
					},
				},
			},
		}
		snapshot := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshot",
				"metadata": map[string]interface{}{
					"name":      snapshotName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "StorageReplicated",
							"status":  "False",
							"reason":  "Failed",
							"message": "GCS bucket permission denied",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshot).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("expected PhaseFailed, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
	})

	t.Run("SnapshotReady", func(t *testing.T) {
		trigger := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshotManualTrigger",
				"metadata": map[string]interface{}{
					"name":      triggerName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"snapshotCreated": map[string]interface{}{
						"name": snapshotName,
					},
				},
			},
		}
		snapshot := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshot",
				"metadata": map[string]interface{}{
					"name":      snapshotName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":   "Ready",
							"status": "True",
							"reason": "AllSnapshotsAvailable",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshot).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseReady {
			t.Errorf("expected PhaseReady, got %v", status.Phase)
		}
		if status.SnapshotRef != snapshotName {
			t.Errorf("expected snapshotRef=%s, got %s", snapshotName, status.SnapshotRef)
		}
	})

	t.Run("PodSnapshot_OrderIndependence_CheckpointSucceeded_StorageFailed", func(t *testing.T) {
		trigger := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshotManualTrigger",
				"metadata": map[string]interface{}{
					"name":      triggerName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"snapshotCreated": map[string]interface{}{
						"name": snapshotName,
					},
				},
			},
		}
		// Order 1: Checkpoint True/Succeeded at index 0, StorageReplicated False/Failed at index 1
		snapshotOrder1 := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshot",
				"metadata": map[string]interface{}{
					"name":      snapshotName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "Checkpoint",
							"status":  "True",
							"reason":  "Succeeded",
							"message": "runsc checkpoint succeeded",
						},
						map[string]interface{}{
							"type":    "StorageReplicated",
							"status":  "False",
							"reason":  "Failed",
							"message": "GCS upload failed",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshotOrder1).Build()
		provider := NewGKEProvider(client, scheme)
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("Order 1: expected PhaseFailed, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("Order 1: expected Reason=SnapshotFailed, got %s", status.Reason)
		}

		// Order 2 (Reverse): StorageReplicated False/Failed at index 0, Checkpoint True/Succeeded at index 1
		snapshotOrder2 := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "podsnapshot.gke.io/v1",
				"kind":       "PodSnapshot",
				"metadata": map[string]interface{}{
					"name":      snapshotName,
					"namespace": "default",
				},
				"status": map[string]interface{}{
					"conditions": []interface{}{
						map[string]interface{}{
							"type":    "StorageReplicated",
							"status":  "False",
							"reason":  "Failed",
							"message": "GCS upload failed",
						},
						map[string]interface{}{
							"type":    "Checkpoint",
							"status":  "True",
							"reason":  "Succeeded",
							"message": "runsc checkpoint succeeded",
						},
					},
				},
			},
		}

		client2 := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshotOrder2).Build()
		provider2 := NewGKEProvider(client2, scheme)
		status2, err := provider2.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status2.Phase != PhaseFailed {
			t.Fatalf("Order 2: expected PhaseFailed, got %v", status2.Phase)
		}
		if status2.Reason != "SnapshotFailed" {
			t.Errorf("Order 2: expected Reason=SnapshotFailed, got %s", status2.Reason)
		}
	})
}
