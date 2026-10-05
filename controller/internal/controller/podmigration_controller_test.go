package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
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
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigration",
					Name:       configName,
				},
			},
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
	readyCond := meta.FindStatusCondition(updatedConfig.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Status != metav1.ConditionFalse || readyCond.Reason != "DeletionBlockedByInFlightMigrations" {
		t.Errorf("Expected Ready=False DeletionBlockedByInFlightMigrations condition, got: %+v", readyCond)
	}
	if readyCond != nil && readyCond.Message != "Deletion deferred: 1 migration(s) in flight" {
		t.Errorf("Expected message 'Deletion deferred: 1 migration(s) in flight', got: %s", readyCond.Message)
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

func TestPodMigrationReconciler_Finalizer_DeferralTimeoutForcesCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "timeout-migration"

	oldDeletionTime := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			Generation:        1,
			DeletionTimestamp: &oldDeletionTime,
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

	activeJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "stuck-job",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigration",
					Name:       configName,
				},
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc, activeJob).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	r := &PodMigrationReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter 0 when deferral timeout is exceeded, got: %v", res.RequeueAfter)
	}

	// Verify PSSC was deleted despite active job
	checkPSSC := &unstructured.Unstructured{}
	checkPSSC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC); err == nil {
		t.Errorf("Expected PSSC %s to be cleaned up after timeout, but it still exists", psscName)
	}

	// Verify finalizer was removed
	updatedConfig := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer to be removed after timeout expiry, but still present")
		}
	}

	// Verify warning event was emitted
	select {
	case event := <-fakeRecorder.Events:
		if !strings.Contains(event, "DeletionDeferralTimeout") {
			t.Errorf("Expected DeletionDeferralTimeout event, got: %s", event)
		}
	default:
		t.Errorf("Expected warning event to be recorded, but recorder was empty")
	}
}

func TestPodMigrationReconciler_Finalizer_UnrelatedMigrationsDoNotBlock(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "config-a"
	otherConfigName := "config-b"

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

	// Active migration job owned by config-b
	unrelatedJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "unrelated-job",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigration",
					Name:       otherConfigName,
				},
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc, unrelatedJob).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter 0 (unrelated job must not block deletion), got: %v", res.RequeueAfter)
	}

	// Verify PSSC was deleted
	checkPSSC := &unstructured.Unstructured{}
	checkPSSC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC); err == nil {
		t.Errorf("Expected PSSC %s to be deleted, but it still exists", psscName)
	}

	// Verify finalizer was removed
	updatedConfig := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer to be removed, but still present")
		}
	}
}

func TestPodMigrationReconciler_Finalizer_ConflictRetryDuringFinalizerRemoval(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "conflict-migration"

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

	conflictOnce := true
	cl := interceptor.NewClient(baseClient, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigration); ok && conflictOnce {
				conflictOnce = false
				return apierrors.NewConflict(schema.GroupResource{Group: "podmigration.gke.io", Resource: "podmigrations"}, configName, errors.New("conflict"))
			}
			return c.Update(ctx, obj, opts...)
		},
	})

	r := &PodMigrationReconciler{
		Client: cl,
		Scheme: scheme,
	}

	// First reconcile hits conflict on finalizer removal
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err == nil || !apierrors.IsConflict(err) {
		t.Fatalf("Expected conflict error on first reconcile, got: %v", err)
	}

	// Second reconcile recovers and succeeds
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Expected second reconcile to succeed, got: %v", err)
	}

	updatedConfig := &pmv1alpha1.PodMigration{}
	if err := baseClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err == nil {
		if controllerutil.ContainsFinalizer(updatedConfig, StorageCleanupFinalizer) {
			t.Errorf("Expected finalizer to be removed on retry, but still present")
		}
	}
}

func TestPodMigrationReconciler_Finalizer_RecreatedPSSCNotDeleted(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "recreated-migration"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			UID:               "old-uid-123",
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
	pssc.SetLabels(map[string]string{
		OwnerUIDLabelKey: "new-uid-456", // Recreated with new UID
	})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc).
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

	// Verify PSSC was NOT deleted because owner UID did not match
	checkPSSC := &unstructured.Unstructured{}
	checkPSSC.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, checkPSSC); err != nil {
		t.Errorf("Expected recreated PSSC %s to be preserved, but got err: %v", psscName, err)
	}
}

func TestPodMigrationReconciler_SingletonPerNamespace(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	otherNamespace := "other-ns"
	t0 := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	t1 := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	primary := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-primary",
			UID:               "uid-primary",
			Generation:        1,
			CreationTimestamp: t0,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-primary/snapshots",
			},
		},
	}

	duplicate := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-duplicate",
			UID:               "uid-duplicate",
			Generation:        1,
			CreationTimestamp: t1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-duplicate/snapshots",
			},
		},
	}

	otherNS := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         otherNamespace,
			Name:              "config-other",
			UID:               "uid-other",
			Generation:        1,
			CreationTimestamp: t1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-other/snapshots",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(primary, duplicate, otherNS).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	r := &PodMigrationReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	// 1. Reconcile primary: should succeed, add finalizer, and create PSSC + PSP
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: primary.Name},
	}); err != nil {
		t.Fatalf("Reconcile primary failed: %v", err)
	}

	gotPrimary := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: primary.Name}, gotPrimary); err != nil {
		t.Fatalf("Get primary failed: %v", err)
	}
	primaryCond := meta.FindStatusCondition(gotPrimary.Status.Conditions, "Ready")
	if primaryCond == nil || primaryCond.Status != metav1.ConditionTrue || primaryCond.Reason != "Reconciled" {
		t.Fatalf("Expected primary Ready=True (Reconciled), got: %+v", primaryCond)
	}

	// 2. Reconcile duplicate in the same namespace: should set Ready=False (DuplicatePodMigrationInNamespace),
	// emit Warning event, and NOT create PSSC/PSP or add finalizer.
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: duplicate.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile duplicate returned unexpected error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter=0 when active is not deleting, got %v", res.RequeueAfter)
	}

	gotDuplicate := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: duplicate.Name}, gotDuplicate); err != nil {
		t.Fatalf("Get duplicate failed: %v", err)
	}
	if controllerutil.ContainsFinalizer(gotDuplicate, StorageCleanupFinalizer) {
		t.Errorf("Duplicate PodMigration must not receive %s finalizer", StorageCleanupFinalizer)
	}
	dupCond := meta.FindStatusCondition(gotDuplicate.Status.Conditions, "Ready")
	if dupCond == nil || dupCond.Status != metav1.ConditionFalse || dupCond.Reason != ReasonDuplicatePodMigrationInNamespace {
		t.Fatalf("Expected duplicate Ready=False (%s), got: %+v", ReasonDuplicatePodMigrationInNamespace, dupCond)
	}
	if !strings.Contains(dupCond.Message, primary.Name) {
		t.Errorf("Expected duplicate condition message to name active %q, got: %s", primary.Name, dupCond.Message)
	}

	select {
	case ev := <-fakeRecorder.Events:
		if !strings.Contains(ev, ReasonDuplicatePodMigrationInNamespace) || !strings.Contains(ev, primary.Name) {
			t.Errorf("Unexpected event on duplicate: %s", ev)
		}
	default:
		t.Errorf("Expected %s warning event to be emitted", ReasonDuplicatePodMigrationInNamespace)
	}

	// Verify duplicate PSP does NOT exist in namespace
	dupPSP := &unstructured.Unstructured{}
	dupPSP.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      getPSPManualName(duplicate.Name),
	}, dupPSP); !apierrors.IsNotFound(err) {
		t.Errorf("Expected duplicate PSP not to be created, got err: %v", err)
	}

	// 3. Reconcile PodMigration in a different namespace: should succeed independently
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: otherNamespace, Name: otherNS.Name},
	}); err != nil {
		t.Fatalf("Reconcile other-ns failed: %v", err)
	}
	gotOther := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: otherNamespace, Name: otherNS.Name}, gotOther); err != nil {
		t.Fatalf("Get other-ns failed: %v", err)
	}
	otherCond := meta.FindStatusCondition(gotOther.Status.Conditions, "Ready")
	if otherCond == nil || otherCond.Status != metav1.ConditionTrue {
		t.Fatalf("Expected other-ns Ready=True, got: %+v", otherCond)
	}

	// 4. Delete primary and reconcile primary (finalizes and deletes primary)
	if err := fakeClient.Delete(context.Background(), gotPrimary); err != nil {
		t.Fatalf("Delete primary failed: %v", err)
	}
	// Before primary finishes reconciling deletion, duplicate should requeue while primary still holds finalizer
	// without re-emitting a second DuplicatePodMigrationInNamespace Warning event.
	resWhileDeleting, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: duplicate.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile duplicate while primary deleting failed: %v", err)
	}
	if resWhileDeleting.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter=5s while primary still holds finalizer, got %v", resWhileDeleting.RequeueAfter)
	}
	select {
	case ev := <-fakeRecorder.Events:
		t.Errorf("Expected no duplicate Warning event on 5s requeue when condition did not change, got: %s", ev)
	default:
	}

	// Finalize primary deletion
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: primary.Name},
	}); err != nil {
		t.Fatalf("Reconcile primary deletion failed: %v", err)
	}

	// 5. Reconcile duplicate again: should now promote to active (Ready=True) and create its PSP/PSSC
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: duplicate.Name},
	}); err != nil {
		t.Fatalf("Reconcile promoted duplicate failed: %v", err)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: duplicate.Name}, gotDuplicate); err != nil {
		t.Fatalf("Get promoted duplicate failed: %v", err)
	}
	promotedCond := meta.FindStatusCondition(gotDuplicate.Status.Conditions, "Ready")
	if promotedCond == nil || promotedCond.Status != metav1.ConditionTrue || promotedCond.Reason != "Reconciled" {
		t.Fatalf("Expected promoted duplicate Ready=True (Reconciled), got: %+v", promotedCond)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{
		Namespace: namespace,
		Name:      getPSPManualName(duplicate.Name),
	}, dupPSP); err != nil {
		t.Errorf("Expected promoted duplicate PSP to be created, got err: %v", err)
	}
}

func TestPodMigrationReconciler_SingletonPerNamespace_TieBreakAndCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	sameTime := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	configA := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-a",
			UID:               "uid-a",
			Generation:        1,
			CreationTimestamp: sameTime,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-a/snapshots",
			},
		},
	}

	// config-b has identical CreationTimestamp but lexicographically larger name,
	// and pre-existing finalizer + PSP/PSSC that must be cleaned up when demoted.
	configB := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-b",
			UID:               "uid-b",
			Generation:        1,
			CreationTimestamp: sameTime,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-b/snapshots",
			},
		},
	}

	psscB := &unstructured.Unstructured{}
	psscB.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	psscB.SetName(getPSSCName(namespace, configB.Name))
	psscB.SetLabels(map[string]string{OwnerUIDLabelKey: string(configB.UID)})

	pspB := &unstructured.Unstructured{}
	pspB.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspB.SetName(getPSPManualName(configB.Name))
	pspB.SetNamespace(namespace)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(configA, configB, psscB, pspB).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// Verify enqueueNamespacePeers returns config-b when triggered for config-a
	peers := r.enqueueNamespacePeers(context.Background(), configA)
	if len(peers) != 1 || peers[0].Name != configB.Name || peers[0].Namespace != namespace {
		t.Fatalf("Expected enqueueNamespacePeers to return %s/%s, got: %+v", namespace, configB.Name, peers)
	}

	// Reconcile config-b: should lose tie-breaker to config-a, clean up psscB + pspB,
	// remove its finalizer, and set Ready=False.
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configB.Name},
	}); err != nil {
		t.Fatalf("Reconcile config-b failed: %v", err)
	}

	gotB := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configB.Name}, gotB); err != nil {
		t.Fatalf("Get config-b failed: %v", err)
	}
	if controllerutil.ContainsFinalizer(gotB, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer to be removed from demoted duplicate config-b")
	}
	condB := meta.FindStatusCondition(gotB.Status.Conditions, "Ready")
	if condB == nil || condB.Status != metav1.ConditionFalse || condB.Reason != ReasonDuplicatePodMigrationInNamespace {
		t.Fatalf("Expected config-b Ready=False (%s), got: %+v", ReasonDuplicatePodMigrationInNamespace, condB)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: getPSPManualName(configB.Name)}, pspB); !apierrors.IsNotFound(err) {
		t.Errorf("Expected pspB to be cleaned up, got err: %v", err)
	}
}

func TestPodMigrationReconciler_SingletonPerNamespace_DemotionDefersCleanupWhenMigrationsInFlight(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	t0 := metav1.NewTime(time.Now().Add(-2 * time.Minute))
	t1 := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	configA := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-a",
			UID:               "uid-a",
			Generation:        1,
			CreationTimestamp: t0,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-a/snapshots",
			},
		},
	}

	// Simulate an upgraded cluster where config-b was previously active (has finalizer, PSSC, PSP)
	// and currently has an in-flight PodMigrationJob under its bucket.
	configB := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-b",
			UID:               "uid-b",
			Generation:        1,
			CreationTimestamp: t1,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-b/snapshots",
			},
		},
	}

	psscBName := getPSSCName(namespace, configB.Name)
	psscB := &unstructured.Unstructured{}
	psscB.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	psscB.SetName(psscBName)
	psscB.SetLabels(map[string]string{OwnerUIDLabelKey: string(configB.UID)})

	pspBName := getPSPManualName(configB.Name)
	pspB := &unstructured.Unstructured{}
	pspB.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspB.SetName(pspBName)
	pspB.SetNamespace(namespace)

	activeJobB := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "inflight-job-b",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigration",
					Name:       configB.Name,
					UID:        configB.UID,
				},
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(configA, configB, psscB, pspB, activeJobB).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	r := &PodMigrationReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	// 1. First reconcile of demoted config-b while activeJobB is Snapshotting:
	// must defer PSSC/PSP cleanup, keep finalizer, set Ready=False (DuplicatePodMigrationInNamespace),
	// emit Warning event once, and requeue after 5s.
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configB.Name},
	})
	if err != nil {
		t.Fatalf("First reconcile of demoted config-b failed: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter=5s while migration is in flight on demoted CR, got %v", res.RequeueAfter)
	}

	gotB := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configB.Name}, gotB); err != nil {
		t.Fatalf("Get config-b failed: %v", err)
	}
	if !controllerutil.ContainsFinalizer(gotB, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to remain on demoted config-b while migration is in flight", StorageCleanupFinalizer)
	}
	condB := meta.FindStatusCondition(gotB.Status.Conditions, "Ready")
	if condB == nil || condB.Status != metav1.ConditionFalse || condB.Reason != ReasonDuplicatePodMigrationInNamespace {
		t.Fatalf("Expected config-b Ready=False (%s), got: %+v", ReasonDuplicatePodMigrationInNamespace, condB)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscBName}, psscB); err != nil {
		t.Errorf("Expected psscB to remain while migration is in flight, got err: %v", err)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspBName}, pspB); err != nil {
		t.Errorf("Expected pspB to remain while migration is in flight, got err: %v", err)
	}
	select {
	case ev := <-fakeRecorder.Events:
		if !strings.Contains(ev, ReasonDuplicatePodMigrationInNamespace) {
			t.Errorf("Expected %s event, got: %s", ReasonDuplicatePodMigrationInNamespace, ev)
		}
	default:
		t.Errorf("Expected initial %s Warning event on demotion", ReasonDuplicatePodMigrationInNamespace)
	}

	// 2. Second reconcile tick while activeJobB is Restoring: still defers, and must NOT emit another Warning event.
	activeJobB.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	if err := fakeClient.Status().Update(context.Background(), activeJobB); err != nil {
		t.Fatalf("Update activeJobB to Restoring failed: %v", err)
	}
	res2, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configB.Name},
	})
	if err != nil {
		t.Fatalf("Second reconcile of demoted config-b failed: %v", err)
	}
	if res2.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter=5s while job is Restoring, got %v", res2.RequeueAfter)
	}
	select {
	case ev := <-fakeRecorder.Events:
		t.Errorf("Expected no duplicate Warning event on 5s deferral tick, got: %s", ev)
	default:
	}

	// 3. Transition activeJobB to terminal (Succeeded) and reconcile again:
	// should now delete psscB + pspB, remove finalizer, and stop requeuing.
	activeJobB.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	if err := fakeClient.Status().Update(context.Background(), activeJobB); err != nil {
		t.Fatalf("Update activeJobB to Succeeded failed: %v", err)
	}
	res3, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configB.Name},
	})
	if err != nil {
		t.Fatalf("Third reconcile of demoted config-b failed: %v", err)
	}
	if res3.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter=0 after in-flight migration completed, got %v", res3.RequeueAfter)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configB.Name}, gotB); err != nil {
		t.Fatalf("Get config-b after cleanup failed: %v", err)
	}
	if controllerutil.ContainsFinalizer(gotB, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to be removed after in-flight migration completed", StorageCleanupFinalizer)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscBName}, psscB); !apierrors.IsNotFound(err) {
		t.Errorf("Expected psscB to be deleted after in-flight migration completed, got err: %v", err)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspBName}, pspB); !apierrors.IsNotFound(err) {
		t.Errorf("Expected pspB to be deleted after in-flight migration completed, got err: %v", err)
	}
}

func TestPodMigrationReconciler_SingletonPerNamespace_DemotionDeferralTimeoutForcesCleanup(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	t0 := metav1.NewTime(time.Now().Add(-20 * time.Minute))
	t1 := metav1.NewTime(time.Now().Add(-19 * time.Minute))
	expiredTransition := metav1.NewTime(time.Now().Add(-15 * time.Minute))

	configA := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-a",
			UID:               "uid-a",
			Generation:        1,
			CreationTimestamp: t0,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-a/snapshots",
			},
		},
	}

	configB := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "config-b",
			UID:               "uid-b",
			Generation:        1,
			CreationTimestamp: t1,
			Finalizers:        []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://bucket-b/snapshots",
			},
		},
		Status: pmv1alpha1.PodMigrationStatus{
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             ReasonDuplicatePodMigrationInNamespace,
					Message:            `Only one PodMigration resource is allowed per namespace; active configuration is "config-a"`,
					LastTransitionTime: expiredTransition,
					ObservedGeneration: 1,
				},
			},
		},
	}

	psscBName := getPSSCName(namespace, configB.Name)
	psscB := &unstructured.Unstructured{}
	psscB.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotStorageConfig",
	})
	psscB.SetName(psscBName)
	psscB.SetLabels(map[string]string{OwnerUIDLabelKey: string(configB.UID)})

	stuckJobB := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "stuck-job-b",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "podmigration.gke.io/v1alpha1",
					Kind:       "PodMigration",
					Name:       configB.Name,
					UID:        configB.UID,
				},
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(configA, configB, psscB, stuckJobB).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	r := &PodMigrationReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configB.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile demoted config-b after deferral timeout failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter=0 when demotion deferral timeout is exceeded, got %v", res.RequeueAfter)
	}

	gotB := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configB.Name}, gotB); err != nil {
		t.Fatalf("Get config-b failed: %v", err)
	}
	if controllerutil.ContainsFinalizer(gotB, StorageCleanupFinalizer) {
		t.Errorf("Expected finalizer %s to be removed after demotion deferral timeout", StorageCleanupFinalizer)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscBName}, psscB); !apierrors.IsNotFound(err) {
		t.Errorf("Expected psscB to be deleted after demotion deferral timeout, got err: %v", err)
	}
	select {
	case ev := <-fakeRecorder.Events:
		if !strings.Contains(ev, "DeletionDeferralTimeout") {
			t.Errorf("Expected DeletionDeferralTimeout event, got: %s", ev)
		}
	default:
		t.Errorf("Expected DeletionDeferralTimeout Warning event to be emitted")
	}
}
