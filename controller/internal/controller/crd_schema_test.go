package controller

import (
	"context"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestCRDSchema_PodMigrationJob_StatusFields(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest CRD schema test")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd/bases"},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("Failed to start envtest environment: %v", err)
	}
	defer func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("Failed to stop envtest environment: %v", err)
		}
	}()

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add client-go scheme: %v", err)
	}
	if err := pmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("Failed to add pmv1alpha1 scheme: %v", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("Failed to create k8s client: %v", err)
	}

	ctx := context.Background()
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pmj-schema-drift",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: "test-workload-pod",
			},
			TargetPodUID: "target-uid-12345",
		},
	}

	if err := k8sClient.Create(ctx, job); err != nil {
		t.Fatalf("Failed to create PodMigrationJob: %v", err)
	}

	// Update status fields
	job.Status.Consumed = true
	job.Status.RestoredPodUID = "uid-123"
	if err := k8sClient.Status().Update(ctx, job); err != nil {
		t.Fatalf("Failed to update PodMigrationJob status: %v", err)
	}

	// Re-fetch the object from the API server and assert status fields
	var fetched pmv1alpha1.PodMigrationJob
	if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Name}, &fetched); err != nil {
		t.Fatalf("Failed to fetch PodMigrationJob: %v", err)
	}

	if !fetched.Status.Consumed {
		t.Errorf("Expected fetched.Status.Consumed == true, got false")
	}
	if fetched.Status.RestoredPodUID != "uid-123" {
		t.Errorf("Expected fetched.Status.RestoredPodUID == %q, got %q", "uid-123", fetched.Status.RestoredPodUID)
	}
}
