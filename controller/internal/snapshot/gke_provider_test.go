package snapshot

import (
	"context"
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
}
