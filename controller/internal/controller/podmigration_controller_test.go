package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPodMigrationReconciler_Reconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pm",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-bucket/my-path",
			},
		},
	}

	psscDummy := &unstructured.Unstructured{}
	psscDummy.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm).
		WithStatusSubresource(pm, psscDummy).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	req := ctrl.Request{
		NamespacedName: client.ObjectKey{
			Name:      "test-pm",
			Namespace: "default",
		},
	}

	_, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify PSSC was created and status updated
	h := sha256.New()
	h.Write([]byte("default/test-pm"))
	psscName := fmt.Sprintf("pssc-%s", hex.EncodeToString(h.Sum(nil))[:16])

	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})

	err = fakeClient.Get(context.Background(), client.ObjectKey{Name: psscName}, pssc)
	if err != nil {
		t.Fatalf("Failed to get PSSC: %v", err)
	}

	// Verify status condition
	conditions, found, err := unstructured.NestedSlice(pssc.Object, "status", "conditions")
	if err != nil {
		t.Fatalf("Failed to get PSSC conditions: %v", err)
	}
	if !found {
		t.Fatalf("PSSC conditions not found")
	}

	if len(conditions) != 1 {
		t.Fatalf("Expected 1 condition, got %d", len(conditions))
	}

	cond, ok := conditions[0].(map[string]interface{})
	if !ok {
		t.Fatalf("Condition is not a map")
	}

	if cond["type"] != "Ready" {
		t.Errorf("Expected condition type Ready, got %v", cond["type"])
	}
	if cond["status"] != "True" {
		t.Errorf("Expected condition status True, got %v", cond["status"])
	}
}
