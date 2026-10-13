package snapshot

import (
	"context"
	"strings"
	"testing"
	"time"

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

	t.Run("SnapshotInProgress_PopulatesSnapshotRef", func(t *testing.T) {
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
							"type":   "Checkpoint",
							"status": "False",
							"reason": "InProgress",
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
		if status.Phase != PhaseInProgress {
			t.Errorf("expected PhaseInProgress, got %v", status.Phase)
		}
		if status.SnapshotRef != snapshotName {
			t.Errorf("expected SnapshotRef %q, got %q", snapshotName, status.SnapshotRef)
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

	t.Run("PodSnapshot_DeadlineExceeded_FastFail", func(t *testing.T) {
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
							"reason":  "DeadlineExceeded",
							"message": "snapshot agent timed out writing checkpoint stream",
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
			t.Fatalf("expected PhaseFailed for DeadlineExceeded, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "DeadlineExceeded") {
			t.Errorf("expected message to contain DeadlineExceeded, got %s", status.Message)
		}
	})

	t.Run("PSMT_DeadlineExceeded_FastFail", func(t *testing.T) {
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
							"reason":  "DeadlineExceeded",
							"message": "agent trigger handshake deadline exceeded",
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
			t.Fatalf("expected PhaseFailed for PSMT DeadlineExceeded, got %v", status.Phase)
		}
		if status.Reason != "SnapshotTriggerFailed" {
			t.Errorf("expected Reason=SnapshotTriggerFailed, got %s", status.Reason)
		}
	})

	t.Run("PodSnapshot_Refused_FastFail", func(t *testing.T) {
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
		// An engine that declines the pod marks both Checkpoint and Ready False with reason Refused.
		refusal := "pod has 2 containers; only single-container pods are supported"
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
							"reason":  "Refused",
							"message": refusal,
						},
						map[string]interface{}{
							"type":    "Ready",
							"status":  "False",
							"reason":  "Refused",
							"message": refusal,
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
			t.Fatalf("expected PhaseFailed for Refused, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "Refused") || !strings.Contains(status.Message, refusal) {
			t.Errorf("expected message to contain the Refused reason and the engine's message, got %s", status.Message)
		}
	})

	t.Run("PodSnapshot_ReadyFailed_TransientWhileSubconditionsInProgress", func(t *testing.T) {
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
		// Exact condition state observed in Issue #75: transient Ready=False (Failed) while
		// Checkpoint is InProgress and StorageReplicated is AwaitingCheckpoint.
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
							"type":               "Checkpoint",
							"status":             "False",
							"reason":             "InProgress",
							"lastTransitionTime": "2026-09-28T00:26:59Z",
						},
						map[string]interface{}{
							"type":               "StorageReplicated",
							"status":             "False",
							"reason":             "AwaitingCheckpoint",
							"lastTransitionTime": "2026-09-28T00:26:59Z",
						},
						map[string]interface{}{
							"type":               "Ready",
							"status":             "False",
							"reason":             "Failed",
							"message":            "Failed to take snapshot (1).",
							"lastTransitionTime": "2026-09-28T00:27:00Z",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshot).Build()
		provider := NewGKEProvider(client, scheme)
		provider.now = func() time.Time {
			return time.Date(2026, 9, 28, 0, 27, 0, 500_000_000, time.UTC)
		}
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseInProgress {
			t.Fatalf("expected PhaseInProgress for transient Ready failure while subconditions in progress, got %v (Reason: %s, Message: %s)", status.Phase, status.Reason, status.Message)
		}
		if status.Reason != "Snapshotting" {
			t.Errorf("expected Reason=Snapshotting, got %s", status.Reason)
		}
		if status.SnapshotRef != snapshotName {
			t.Errorf("expected SnapshotRef=%s, got %s", snapshotName, status.SnapshotRef)
		}
	})

	t.Run("PodSnapshot_ReadyFailed_ExpiredSuppressionWindow_FastFail", func(t *testing.T) {
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
		// Simulate a multi-minute checkpoint (started at t-150s) where Ready=False(Failed)
		// transitions at readyTransition (t=0s). Because the suppression window is anchored
		// on Ready's own lastTransitionTime, a Ready blip at t+150s of a long checkpoint gets
		// its full 60s suppression window before expiring at readyTransition + 60s + 1ns.
		readyTransition := time.Date(2026, 9, 28, 0, 29, 30, 0, time.UTC)
		checkpointStart := readyTransition.Add(-150 * time.Second)
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
							"type":               "Checkpoint",
							"status":             "False",
							"reason":             "InProgress",
							"lastTransitionTime": checkpointStart.Format(time.RFC3339),
						},
						map[string]interface{}{
							"type":               "StorageReplicated",
							"status":             "False",
							"reason":             "AwaitingCheckpoint",
							"lastTransitionTime": checkpointStart.Format(time.RFC3339),
						},
						map[string]interface{}{
							"type":               "Ready",
							"status":             "False",
							"reason":             "Failed",
							"message":            "Failed to take snapshot (1).",
							"lastTransitionTime": readyTransition.Format(time.RFC3339),
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

		// 1. At exact boundary (readyTransition + 60s, even though Checkpoint started 210s ago),
		// Ready=False(Failed) is still suppressed.
		provider.now = func() time.Time {
			return readyTransition.Add(transientReadyFailureSuppressionWindow)
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error at boundary: %v", err)
		}
		if status.Phase != PhaseInProgress {
			t.Fatalf("expected PhaseInProgress at exact 60s Ready window boundary, got %v", status.Phase)
		}

		// 2. Beyond window (readyTransition + 60s + 1ns), Ready=False(Failed) fast-fails (#95).
		provider.now = func() time.Time {
			return readyTransition.Add(transientReadyFailureSuppressionWindow + time.Nanosecond)
		}
		status, err = provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error after window expiry: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("expected PhaseFailed after 60s Ready suppression window expired, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "Ready failed (Failed): Failed to take snapshot (1).") {
			t.Errorf("unexpected failure message: %s", status.Message)
		}
	})

	t.Run("PodSnapshot_ReadyFailed_MissingLastTransitionTime_FastFail", func(t *testing.T) {
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
		baseTime := time.Date(2026, 9, 28, 0, 27, 0, 0, time.UTC)
		// Even when Checkpoint has a fresh lastTransitionTime, if Ready=False(Failed) lacks
		// a valid lastTransitionTime, CheckStatus fails closed rather than suppressing indefinitely.
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
							"type":               "Checkpoint",
							"status":             "False",
							"reason":             "InProgress",
							"lastTransitionTime": baseTime.Format(time.RFC3339),
						},
						map[string]interface{}{
							"type":    "Ready",
							"status":  "False",
							"reason":  "Failed",
							"message": "agent hung without Ready transition timestamp",
						},
					},
				},
			},
		}

		client := fake.NewClientBuilder().WithScheme(scheme).WithObjects(trigger, snapshot).Build()
		provider := NewGKEProvider(client, scheme)
		provider.now = func() time.Time {
			return baseTime.Add(time.Second)
		}
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Name: "test-job", Namespace: "default"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: "pod-uid-456"},
		}
		status, err := provider.CheckStatus(context.Background(), job, "test-pod")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if status.Phase != PhaseFailed {
			t.Fatalf("expected PhaseFailed when Ready condition lacks lastTransitionTime, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
	})

	t.Run("PodSnapshot_ReadyFailed_TerminalWhenNoSubconditionsInProgress", func(t *testing.T) {
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
		// When no sub-conditions are in progress (e.g. standalone Ready provider), Ready=False Failed is terminal.
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
							"type":    "Ready",
							"status":  "False",
							"reason":  "Failed",
							"message": "snapshot agent crashed",
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
			t.Fatalf("expected PhaseFailed for terminal Ready failure, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
	})

	t.Run("PodSnapshot_SubconditionFailed_TerminalEvenIfOtherInProgress", func(t *testing.T) {
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
		// Checkpoint failed terminally; even though StorageReplicated is AwaitingCheckpoint and Ready is Failed,
		// the sub-condition failure must immediately fast-fail.
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
							"message": "memory dump aborted",
						},
						map[string]interface{}{
							"type":    "StorageReplicated",
							"status":  "False",
							"reason":  "AwaitingCheckpoint",
							"message": "waiting for checkpoint",
						},
						map[string]interface{}{
							"type":    "Ready",
							"status":  "False",
							"reason":  "Failed",
							"message": "snapshot failed",
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
			t.Fatalf("expected PhaseFailed for Checkpoint failure, got %v", status.Phase)
		}
		if status.Reason != "SnapshotFailed" {
			t.Errorf("expected Reason=SnapshotFailed, got %s", status.Reason)
		}
		if !strings.Contains(status.Message, "Checkpoint failed") {
			t.Errorf("expected message to mention Checkpoint failure, got %s", status.Message)
		}
	})
}

func TestIsSnapshotSubconditionInProgressReason(t *testing.T) {
	tests := []struct {
		reason string
		want   bool
	}{
		{reason: "InProgress", want: true},
		{reason: "inprogress", want: true},
		{reason: "AwaitingCheckpoint", want: true},
		{reason: "awaitingcheckpoint", want: true},
		{reason: "Pending", want: true},
		{reason: "pending", want: true},
		{reason: "Failed", want: false},
		{reason: "Error", want: false},
		{reason: "Succeeded", want: false},
		{reason: "DeadlineExceeded", want: false},
		{reason: "Refused", want: false},
		{reason: "", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			if got := isSnapshotSubconditionInProgressReason(tc.reason); got != tc.want {
				t.Errorf("isSnapshotSubconditionInProgressReason(%q) = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}

func TestIsTerminalSnapshotFailureReason(t *testing.T) {
	tests := []struct {
		reason string
		want   bool
	}{
		{reason: "Failed", want: true},
		{reason: "Error", want: true},
		{reason: "DeadlineExceeded", want: true},
		{reason: "Refused", want: true},
		{reason: "refused", want: true},
		{reason: "", want: false},
		{reason: "NoError", want: false},
		{reason: "NotReady", want: false},
		{reason: "NonTerminal", want: false},
		{reason: "Pending", want: false},
		{reason: "Succeeded", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.reason, func(t *testing.T) {
			if got := isTerminalSnapshotFailureReason(tc.reason); got != tc.want {
				t.Errorf("isTerminalSnapshotFailureReason(%q) = %v, want %v", tc.reason, got, tc.want)
			}
		})
	}
}
