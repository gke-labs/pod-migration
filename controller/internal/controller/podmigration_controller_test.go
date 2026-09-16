package controller

import (
	"context"
	"errors"
	"testing"
	"time"

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
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestPodMigrationReconciler_Reconcile_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-migration"

	config := &pmv1alpha1.PodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       configName,
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots/path",
			},
		},
	}

	psscMock := &unstructured.Unstructured{}
	psscMock.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pspMock := &unstructured.Unstructured{}
	pspMock.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithStatusSubresource(psscMock).
		WithStatusSubresource(pspMock).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Verify status condition Ready=True
	updatedConfig := &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err != nil {
		t.Fatalf("Failed to get updated config: %v", err)
	}

	cond := meta.FindStatusCondition(updatedConfig.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Ready condition not found in status")
	}
	if cond.Status != metav1.ConditionTrue {
		t.Errorf("Expected status True, got %s. Message: %s", cond.Status, cond.Message)
	}
	if cond.Reason != "Reconciled" {
		t.Errorf("Expected reason Reconciled, got %s", cond.Reason)
	}

	// Verify PodSnapshotStorageConfig was created
	pssList := &unstructured.UnstructuredList{}
	pssList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfigList",
	})
	err = fakeClient.List(context.Background(), pssList)
	if err != nil {
		t.Fatalf("Failed to list PSSCs: %v", err)
	}
	if len(pssList.Items) != 1 {
		t.Fatalf("Expected 1 PSSC, got %d", len(pssList.Items))
	}
	pssc := pssList.Items[0]

	// Verify GCS storage contents
	spec, ok := pssc.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("PSSC spec is not a map")
	}
	snapConfig, ok := spec["snapshotStorageConfig"].(map[string]interface{})
	if !ok {
		t.Fatalf("snapshotStorageConfig is not a map")
	}
	gcs, ok := snapConfig["gcs"].(map[string]interface{})
	if !ok {
		t.Fatalf("gcs config is not a map")
	}
	if gcs["bucket"] != "my-test-bucket" {
		t.Errorf("Expected bucket my-test-bucket, got %v", gcs["bucket"])
	}
	if gcs["path"] != "snapshots/path" {
		t.Errorf("Expected path snapshots/path, got %v", gcs["path"])
	}

	// Verify PodSnapshotPolicy (PSP) was created
	pspList := &unstructured.UnstructuredList{}
	pspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicyList",
	})
	err = fakeClient.List(context.Background(), pspList)
	if err != nil {
		t.Fatalf("Failed to list PSPs: %v", err)
	}
	if len(pspList.Items) != 1 {
		t.Fatalf("Expected 1 PSP, got %d", len(pspList.Items))
	}
	psp := pspList.Items[0]
	if psp.GetName() != "psp-test-migration-manual" {
		t.Errorf("Expected name psp-test-migration-manual, got %s", psp.GetName())
	}

	// Verify PSP spec selector
	pspSpec, ok := psp.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("PSP spec is not a map")
	}
	selector, ok := pspSpec["selector"].(map[string]interface{})
	if !ok {
		t.Fatalf("PSP selector is not a map")
	}
	matchExpressions, ok := selector["matchExpressions"].([]interface{})
	if !ok {
		t.Fatalf("PSP selector matchExpressions is not a slice, got %T", selector["matchExpressions"])
	}
	if len(matchExpressions) != 1 {
		t.Fatalf("Expected 1 matchExpression, got %d", len(matchExpressions))
	}
	expr, ok := matchExpressions[0].(map[string]interface{})
	if !ok {
		t.Fatalf("matchExpression is not a map")
	}
	if expr["key"] != "pod-migration.gke.io/enabled" {
		t.Errorf("Expected key pod-migration.gke.io/enabled, got %v", expr["key"])
	}
	if expr["operator"] != "In" {
		t.Errorf("Expected operator In, got %v", expr["operator"])
	}
	values, ok := expr["values"].([]interface{})
	if !ok {
		t.Fatalf("values is not []interface{}, got %T", expr["values"])
	}
	if len(values) != 1 || values[0] != "true" {
		t.Errorf("Expected values [true], got %v", values)
	}
}

func TestPodMigrationReconciler_Reconcile_BucketOnly(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-migration-bucket-only"

	config := &pmv1alpha1.PodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       configName,
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket",
			},
		},
	}

	psscMock := &unstructured.Unstructured{}
	psscMock.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pspMock := &unstructured.Unstructured{}
	pspMock.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithStatusSubresource(psscMock).
		WithStatusSubresource(pspMock).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	pssList := &unstructured.UnstructuredList{}
	pssList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfigList",
	})
	err = fakeClient.List(context.Background(), pssList)
	if err != nil {
		t.Fatalf("Failed to list PSSCs: %v", err)
	}
	pssc := pssList.Items[0]

	spec := pssc.Object["spec"].(map[string]interface{})
	snapConfig := spec["snapshotStorageConfig"].(map[string]interface{})
	gcs := snapConfig["gcs"].(map[string]interface{})
	if gcs["bucket"] != "my-test-bucket" {
		t.Errorf("Expected bucket my-test-bucket, got %v", gcs["bucket"])
	}
	if _, ok := gcs["path"]; ok {
		t.Errorf("Path field should not be populated when URI has no prefix path")
	}
}

func TestPodMigrationReconciler_Reconcile_InvalidGCSPath(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-migration-invalid"

	config := &pmv1alpha1.PodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       configName,
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "http://my-test-bucket",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err == nil {
		t.Fatalf("Expected reconcile to fail with invalid GCS path")
	}

	// Verify status condition Ready=False
	updatedConfig := &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err != nil {
		t.Fatalf("Failed to get updated config: %v", err)
	}

	cond := meta.FindStatusCondition(updatedConfig.Status.Conditions, "Ready")
	if cond == nil {
		t.Fatalf("Ready condition not found in status")
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Expected status False, got %s", cond.Status)
	}
	if cond.Reason != "InvalidStorageLocation" {
		t.Errorf("Expected reason InvalidStorageLocation, got %s", cond.Reason)
	}
}

func TestPodMigrationReconciler_Finalizer_AddedOnReconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-finalizer-add"

	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       configName,
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	updatedConfig := &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err != nil {
		t.Fatalf("Failed to get updated config: %v", err)
	}

	if !controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to be added to PodMigration, got %v", StorageCleanupFinalizer, updatedConfig.Finalizers)
	}
}

func TestPodMigrationReconciler_Finalizer_CleansUpStorageResourcesOnDelete(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "prod-ns"
	configName := "migration-to-delete"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			Generation:        1,
			DeletionTimestamp: &now,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots",
			},
		},
	}

	psscName := getPSSCName(namespace, configName)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)

	pspName := getPSPManualName(configName)
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	psp.SetName(pspName)
	psp.SetNamespace(namespace)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc, psp).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile deletion failed: %v", err)
	}

	// 1. Verify cluster-scoped PSSC was deleted
	checkPSSC := &unstructured.Unstructured{}
	checkPSSC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	err = fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC)
	if err == nil {
		t.Errorf("Expected PSSC %s to be deleted, but it still exists", psscName)
	}

	// 2. Verify namespaced PSP was deleted
	checkPSP := &unstructured.Unstructured{}
	checkPSP.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, checkPSP)
	if err == nil {
		t.Errorf("Expected PSP %s in namespace %s to be deleted, but it still exists", pspName, namespace)
	}

	// 3. Verify finalizer was removed and PodMigration was deleted by the API server
	updatedConfig := &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer %s to be removed, but still found in %v", StorageCleanupFinalizer, updatedConfig.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("Unexpected error getting updated config: %v", err)
	}
}

func TestPodMigrationReconciler_Finalizer_IdempotentWhenAlreadyDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "already-deleted"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			Generation:        1,
			DeletionTimestamp: &now,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots",
			},
		},
	}

	// Fake client without PSSC or PSP (already gone)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile should succeed when storage resources are already gone: %v", err)
	}

	// Verify finalizer removal led to deletion
	updatedConfig := &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer to be removed even if storage resources were not found")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("Unexpected error getting config: %v", err)
	}
}

func TestPodMigrationReconciler_Finalizer_RetainedOnDeleteError(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "delete-fail"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			Generation:        1,
			DeletionTimestamp: &now,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots",
			},
		},
	}

	psscName := getPSSCName(namespace, configName)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	// Interceptor that fails on Delete with an error other than NotFound
	cl := interceptor.NewClient(baseClient, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == "PodSnapshotStorageConfig" {
				return errors.New("simulated transient API error deleting storage config")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})

	r := &PodMigrationReconciler{
		Client: cl,
		Scheme: scheme,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err == nil {
		t.Fatalf("Expected reconcile to fail when delete returns non-NotFound error, got nil")
	}

	// Verify finalizer is still retained on the PodMigration object
	updatedConfig := &pmv1alpha1.PodMigration{}
	err = baseClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err != nil {
		t.Fatalf("Failed to get updated config: %v", err)
	}

	if !controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to remain on PodMigration when deletion fails, but it was removed", StorageCleanupFinalizer)
	}
}

func TestPodMigrationReconciler_Finalizer_PostponesDeleteWhenMigrationsInFlight(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "migration-with-jobs"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			Generation:        1,
			DeletionTimestamp: &now,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket/snapshots",
			},
		},
	}

	psscName := getPSSCName(namespace, configName)
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	pssc.SetName(psscName)

	pspName := getPSPManualName(configName)
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	psp.SetName(pspName)
	psp.SetNamespace(namespace)

	// In-flight migration job in the same namespace
	activeJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "active-job-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc, psp, activeJob).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// 1. First reconcile with job in Snapshotting phase
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter: 5s, got: %+v", res.RequeueAfter)
	}

	// Verify storage resources are NOT deleted
	checkPSSC := &unstructured.Unstructured{}
	checkPSSC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC); err != nil {
		t.Errorf("Expected PSSC %s to still exist while job is in-flight, got err: %v", psscName, err)
	}

	// Verify finalizer is still retained
	updatedConfig := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}
	if !controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to remain while job is in-flight", StorageCleanupFinalizer)
	}

	// 2. Transition job to terminal phase (Succeeded)
	activeJob.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	if err := fakeClient.Status().Update(context.Background(), activeJob); err != nil {
		t.Fatalf("Failed to update activeJob status: %v", err)
	}

	// 3. Second reconcile now that all jobs are terminal
	res, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed after job completed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter to be 0 after job completed, got: %+v", res.RequeueAfter)
	}

	// Verify PSSC was deleted
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC); err == nil {
		t.Errorf("Expected PSSC %s to be deleted after job completed, but it still exists", psscName)
	}

	// Verify finalizer was removed
	updatedConfig = &pmv1alpha1.PodMigration{}
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig)
	if err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer to be removed after job completed, but still present")
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("Unexpected error: %v", err)
	}
}
