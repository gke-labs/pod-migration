package controller

import (
	"context"
	"errors"
	"reflect"
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
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestPodMigrationReconciler_RouteRejectUncoveringEngine(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	config := &pmv1alpha1.PodMigration{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "podmigration.gke.io/v1alpha1",
			Kind:       "PodMigration",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "test-migration",
			Generation: 1,
			Annotations: map[string]string{
				engine.AnnotationRuntimeClassEngines: "gvisor=criu",
			},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://my-test-bucket",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()

	gke := &renderingEngine{name: "gke", classes: []string{"gvisor"}}
	criu := &renderingEngine{name: "criu", classes: []string{eligibility.DefaultRuntimeClassToken, "runc"}}
	rRegistry := engine.NewRegistry("gke")
	if err := rRegistry.Register(gke); err != nil {
		t.Fatalf("Register gke: %v", err)
	}
	if err := rRegistry.Register(criu); err != nil {
		t.Fatalf("Register criu: %v", err)
	}
	if err := rRegistry.SetRoutes(engine.Routes{"gvisor": "gke"}); err != nil {
		t.Fatalf("SetRoutes: %v", err)
	}

	r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: rRegistry}
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-migration"}})
	if err != nil {
		t.Fatalf("Reconcile error: %v", err)
	}

	got := &pmv1alpha1.PodMigration{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "test-migration"}, got); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}

	cond := meta.FindStatusCondition(got.Status.Conditions, ConditionEngineRoutesValid)
	if cond == nil {
		t.Fatalf("Condition %s not found on PodMigration", ConditionEngineRoutesValid)
	}
	if cond.Status != metav1.ConditionFalse {
		t.Errorf("Condition %s status = %v, want %v", ConditionEngineRoutesValid, cond.Status, metav1.ConditionFalse)
	}
	if cond.Reason != ReasonInvalidEngineRoutes {
		t.Errorf("Condition %s reason = %v, want %v", ConditionEngineRoutesValid, cond.Reason, ReasonInvalidEngineRoutes)
	}
}

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

// The tests below cover syncResource (#102): a reconcile patches only the
// fields this controller owns on the managed PodSnapshotStorageConfig and
// PodSnapshotPolicy, and leaves metadata other components add alone.

var (
	psscGVKForTest = schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotStorageConfig"}
	pspGVKForTest  = schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotPolicy"}
)

func syncTestPodMigration(namespace, name string) *pmv1alpha1.PodMigration {
	return &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  namespace,
			Name:       name,
			UID:        types.UID("uid-" + name),
			Generation: 1,
			Finalizers: []string{StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://sync-bucket/snaps"},
		},
	}
}

func newSyncTestClient(scheme *runtime.Scheme, objs ...client.Object) client.WithWatch {
	psscMock := &unstructured.Unstructured{}
	psscMock.SetGroupVersionKind(psscGVKForTest)
	pspMock := &unstructured.Unstructured{}
	pspMock.SetGroupVersionKind(pspGVKForTest)
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithStatusSubresource(psscMock).
		WithStatusSubresource(pspMock).
		Build()
}

// wantPSSCSpec and wantPSPSpec mirror the payloads Reconcile writes for
// syncTestPodMigration.
func wantPSSCSpec() map[string]interface{} {
	return map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{
			"gcs": map[string]interface{}{"bucket": "sync-bucket", "path": "snaps"},
		},
	}
}

func wantPSPSpec(psscName string) map[string]interface{} {
	return map[string]interface{}{
		"storageConfigName": psscName,
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      "pod-migration.gke.io/enabled",
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	}
}

func reconcileSyncTest(t *testing.T, c client.Client, scheme *runtime.Scheme, namespace, name string) {
	t.Helper()
	r := &PodMigrationReconciler{Client: c, Scheme: scheme}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
	}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
}

func getManagedForTest(t *testing.T, c client.Client, gvk schema.GroupVersionKind, key types.NamespacedName) *unstructured.Unstructured {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	if err := c.Get(context.Background(), key, obj); err != nil {
		t.Fatalf("Get %s %s: %v", gvk.Kind, key, err)
	}
	return obj
}

// A reconcile that has to rewrite the managed objects must keep labels,
// annotations and finalizers that other components added, such as a snapshot
// engine's selector label.
func TestPodMigrationReconciler_Sync_PreservesForeignMetadata(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "sync-meta"
	config := syncTestPodMigration(namespace, name)
	psscName := getPSSCName(namespace, name)
	pspName := getPSPManualName(name)

	// Both objects carry foreign metadata and a drifted spec, so the reconcile
	// has to write both of them.
	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(psscGVKForTest)
	pssc.SetName(psscName)
	pssc.SetLabels(map[string]string{"pod-migrate.io/engine": "criu"})
	pssc.SetAnnotations(map[string]string{"example.com/note": "keep"})
	pssc.SetFinalizers([]string{"example.com/keep"})
	pssc.Object["spec"] = map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{
			"gcs": map[string]interface{}{"bucket": "stale-bucket"},
		},
	}

	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(pspGVKForTest)
	psp.SetNamespace(namespace)
	psp.SetName(pspName)
	psp.SetLabels(map[string]string{"pod-migrate.io/engine": "criu"})
	psp.SetAnnotations(map[string]string{"example.com/note": "keep"})
	psp.SetFinalizers([]string{"example.com/keep"})
	driftedSpec := wantPSPSpec(psscName)
	driftedSpec["triggerConfig"] = map[string]interface{}{"type": "manual", "postCheckpoint": "resume"}
	psp.Object["spec"] = driftedSpec

	c := newSyncTestClient(scheme, config, pssc, psp)
	reconcileSyncTest(t, c, scheme, namespace, name)

	for _, tc := range []struct {
		gvk      schema.GroupVersionKind
		key      types.NamespacedName
		wantSpec map[string]interface{}
	}{
		{psscGVKForTest, types.NamespacedName{Name: psscName}, wantPSSCSpec()},
		{pspGVKForTest, types.NamespacedName{Namespace: namespace, Name: pspName}, wantPSPSpec(psscName)},
	} {
		got := getManagedForTest(t, c, tc.gvk, tc.key)
		if v := got.GetLabels()["pod-migrate.io/engine"]; v != "criu" {
			t.Errorf("%s: label pod-migrate.io/engine = %q, want %q", tc.gvk.Kind, v, "criu")
		}
		if v := got.GetAnnotations()["example.com/note"]; v != "keep" {
			t.Errorf("%s: annotation example.com/note = %q, want %q", tc.gvk.Kind, v, "keep")
		}
		if !reflect.DeepEqual(got.GetFinalizers(), []string{"example.com/keep"}) {
			t.Errorf("%s: finalizers = %v, want [example.com/keep]", tc.gvk.Kind, got.GetFinalizers())
		}
		if !reflect.DeepEqual(got.Object["spec"], tc.wantSpec) {
			t.Errorf("%s: spec = %v, want %v", tc.gvk.Kind, got.Object["spec"], tc.wantSpec)
		}
	}

	gotPSSC := getManagedForTest(t, c, psscGVKForTest, types.NamespacedName{Name: psscName})
	if v := gotPSSC.GetLabels()[OwnerUIDLabelKey]; v != string(config.UID) {
		t.Errorf("PSSC label %s = %q, want %q", OwnerUIDLabelKey, v, config.UID)
	}
}

// Spec drift on the managed policy is still reverted: changed values come
// back, a removed key is restored and a key someone added is removed.
func TestPodMigrationReconciler_Sync_RevertsSpecDrift(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "sync-drift"
	config := syncTestPodMigration(namespace, name)
	psscName := getPSSCName(namespace, name)
	pspName := getPSPManualName(name)

	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(pspGVKForTest)
	psp.SetNamespace(namespace)
	psp.SetName(pspName)
	psp.Object["spec"] = map[string]interface{}{
		"storageConfigName": "some-other-config",
		"extra":             "added-elsewhere",
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "resume",
		},
		// "selector" is missing.
	}

	c := newSyncTestClient(scheme, config, psp)
	reconcileSyncTest(t, c, scheme, namespace, name)

	got := getManagedForTest(t, c, pspGVKForTest, types.NamespacedName{Namespace: namespace, Name: pspName})
	if want := wantPSPSpec(psscName); !reflect.DeepEqual(got.Object["spec"], want) {
		t.Errorf("PSP spec = %v, want %v", got.Object["spec"], want)
	}
}

// The owner-UID label on the storage config is put back if someone removed
// it, without touching the labels other components set.
func TestPodMigrationReconciler_Sync_RestoresOwnerLabel(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "sync-owner"
	config := syncTestPodMigration(namespace, name)
	psscName := getPSSCName(namespace, name)

	pssc := &unstructured.Unstructured{}
	pssc.SetGroupVersionKind(psscGVKForTest)
	pssc.SetName(psscName)
	pssc.SetLabels(map[string]string{"example.com/team": "storage"})
	pssc.Object["spec"] = wantPSSCSpec()

	c := newSyncTestClient(scheme, config, pssc)
	reconcileSyncTest(t, c, scheme, namespace, name)

	got := getManagedForTest(t, c, psscGVKForTest, types.NamespacedName{Name: psscName})
	want := map[string]string{
		"example.com/team": "storage",
		OwnerUIDLabelKey:   string(config.UID),
	}
	if !reflect.DeepEqual(got.GetLabels(), want) {
		t.Errorf("PSSC labels = %v, want %v", got.GetLabels(), want)
	}
}

// A reconcile with nothing to change must not write the managed objects, so
// engines watching them don't see resourceVersion churn.
func TestPodMigrationReconciler_Sync_NoOpDoesNotWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "sync-noop"
	config := syncTestPodMigration(namespace, name)
	psscKey := types.NamespacedName{Name: getPSSCName(namespace, name)}
	pspKey := types.NamespacedName{Namespace: namespace, Name: getPSPManualName(name)}

	base := newSyncTestClient(scheme, config)
	reconcileSyncTest(t, base, scheme, namespace, name) // creates both objects

	psscRV := getManagedForTest(t, base, psscGVKForTest, psscKey).GetResourceVersion()
	pspRV := getManagedForTest(t, base, pspGVKForTest, pspKey).GetResourceVersion()

	isManaged := func(obj client.Object) bool {
		switch obj.GetObjectKind().GroupVersionKind().Kind {
		case psscGVKForTest.Kind, pspGVKForTest.Kind:
			return true
		}
		return false
	}
	var writes []string
	c := interceptor.NewClient(base, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if isManaged(obj) {
				writes = append(writes, "Update "+obj.GetName())
			}
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if isManaged(obj) {
				writes = append(writes, "Patch "+obj.GetName())
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	reconcileSyncTest(t, c, scheme, namespace, name)

	if len(writes) != 0 {
		t.Errorf("no-op reconcile wrote managed objects: %v", writes)
	}
	if got := getManagedForTest(t, base, psscGVKForTest, psscKey).GetResourceVersion(); got != psscRV {
		t.Errorf("PSSC resourceVersion changed from %s to %s", psscRV, got)
	}
	if got := getManagedForTest(t, base, pspGVKForTest, pspKey).GetResourceVersion(); got != pspRV {
		t.Errorf("PSP resourceVersion changed from %s to %s", pspRV, got)
	}
}

// managedWriteCounter records the Update and Patch calls on the managed
// policy and storage objects. Creates aren't recorded.
func managedWriteCounter(c client.WithWatch, writes *[]string) client.WithWatch {
	isManaged := func(obj client.Object) bool {
		switch obj.GetObjectKind().GroupVersionKind().Kind {
		case psscGVKForTest.Kind, pspGVKForTest.Kind:
			return true
		}
		return false
	}
	return interceptor.NewClient(c, interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if isManaged(obj) {
				*writes = append(*writes, "Update "+obj.GetName())
			}
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if isManaged(obj) {
				*writes = append(*writes, "Patch "+obj.GetName())
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
}

// createTerminatingForTest creates a managed object with spec, held by a
// foreign finalizer, and deletes it. Like the API server, the fake client
// then only sets its deletionTimestamp.
func createTerminatingForTest(t *testing.T, c client.Client, gvk schema.GroupVersionKind, key types.NamespacedName, spec map[string]interface{}) {
	t.Helper()
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace(key.Namespace)
	obj.SetName(key.Name)
	obj.SetFinalizers([]string{"example.com/hold"})
	obj.Object["spec"] = spec
	if err := c.Create(context.Background(), obj); err != nil {
		t.Fatalf("Create %s %s: %v", gvk.Kind, key, err)
	}
	if err := c.Delete(context.Background(), obj); err != nil {
		t.Fatalf("Delete %s %s: %v", gvk.Kind, key, err)
	}
	if got := getManagedForTest(t, c, gvk, key); got.GetDeletionTimestamp() == nil {
		t.Fatalf("%s %s has no deletionTimestamp after Delete", gvk.Kind, key)
	}
}

func readyConditionForTest(t *testing.T, c client.Client, namespace, name string) *metav1.Condition {
	t.Helper()
	pm := &pmv1alpha1.PodMigration{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, pm); err != nil {
		t.Fatalf("Get PodMigration %s/%s: %v", namespace, name, err)
	}
	return meta.FindStatusCondition(pm.Status.Conditions, "Ready")
}

// terminatingTestCase is a managed object that is being deleted, and the
// other managed object of the same PodMigration.
type terminatingTestCase struct {
	kind     string
	gvk      schema.GroupVersionKind
	key      types.NamespacedName
	wantSpec map[string]interface{}
	drifted  map[string]interface{}
	otherGVK schema.GroupVersionKind
	otherKey types.NamespacedName
}

func terminatingTestCases(namespace, name string) []terminatingTestCase {
	psscKey := types.NamespacedName{Name: getPSSCName(namespace, name)}
	pspKey := types.NamespacedName{Namespace: namespace, Name: getPSPManualName(name)}
	driftedPSP := wantPSPSpec(psscKey.Name)
	driftedPSP["triggerConfig"] = map[string]interface{}{"type": "manual", "postCheckpoint": "resume"}
	return []terminatingTestCase{
		{
			kind:     "PodSnapshotStorageConfig",
			gvk:      psscGVKForTest,
			key:      psscKey,
			wantSpec: wantPSSCSpec(),
			drifted: map[string]interface{}{
				"snapshotStorageConfig": map[string]interface{}{
					"gcs": map[string]interface{}{"bucket": "stale-bucket"},
				},
			},
			otherGVK: pspGVKForTest,
			otherKey: pspKey,
		},
		{
			kind:     "PodSnapshotPolicy",
			gvk:      pspGVKForTest,
			key:      pspKey,
			wantSpec: wantPSPSpec(psscKey.Name),
			drifted:  driftedPSP,
			otherGVK: psscGVKForTest,
			otherKey: psscKey,
		},
	}
}

// A managed object that is being deleted is not patched, even with a drifted
// spec: it only waits for a foreign finalizer, and it can be recreated only
// once it's gone. While either object is terminating, the PodMigration isn't
// Ready, and the reconcile comes back to recreate the object (#144).
func TestPodMigrationReconciler_Sync_TerminatingObjectIsNotPatched(t *testing.T) {
	namespace, name := "default", "sync-terminating"
	for _, tc := range terminatingTestCases(namespace, name) {
		t.Run(tc.kind, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			base := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
			// The drifted spec gives a reconcile that ignores the
			// deletionTimestamp something to patch.
			createTerminatingForTest(t, base, tc.gvk, tc.key, tc.drifted)

			var writes []string
			r := &PodMigrationReconciler{Client: managedWriteCounter(base, &writes), Scheme: scheme}
			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: namespace, Name: name},
			})

			if err != nil {
				t.Errorf("Reconcile returned %v, want nil: a terminating object is waited for, not an error", err)
			}
			if len(writes) != 0 {
				t.Errorf("terminating %s was written: %v", tc.kind, writes)
			}
			if ready := readyConditionForTest(t, base, namespace, name); ready == nil ||
				ready.Status != metav1.ConditionFalse || ready.Reason != ReasonManagedObjectTerminating {
				t.Errorf("Ready = %+v, want False with reason %s", ready, ReasonManagedObjectTerminating)
			}
			if res.RequeueAfter <= 0 {
				t.Errorf("RequeueAfter = %v, want > 0 while the %s is terminating", res.RequeueAfter, tc.kind)
			}
			// The other object is still synced.
			getManagedForTest(t, base, tc.otherGVK, tc.otherKey)
		})
	}
}

// Once the terminating object is gone, its deletion event maps back to the
// PodMigration, and that reconcile recreates the object and reports Ready
// again. No PodMigration event is involved (#144).
func TestPodMigrationReconciler_Sync_TerminatingObjectRecreatedOnceGone(t *testing.T) {
	namespace, name := "default", "sync-recreate"
	for _, tc := range terminatingTestCases(namespace, name) {
		t.Run(tc.kind, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			c := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
			createTerminatingForTest(t, c, tc.gvk, tc.key, tc.drifted)
			r := &PodMigrationReconciler{Client: c, Scheme: scheme}
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}
			if _, err := r.Reconcile(ctx, req); err != nil {
				t.Fatalf("Reconcile while terminating: %v", err)
			}

			// The foreign finalizer is removed, so the object is gone.
			gone := getManagedForTest(t, c, tc.gvk, tc.key)
			gone.SetFinalizers(nil)
			if err := c.Update(ctx, gone); err != nil {
				t.Fatalf("removing the finalizer: %v", err)
			}
			probe := &unstructured.Unstructured{}
			probe.SetGroupVersionKind(tc.gvk)
			if err := c.Get(ctx, tc.key, probe); !apierrors.IsNotFound(err) {
				t.Fatalf("Get %s after removing its finalizer: %v, want NotFound", tc.kind, err)
			}

			// Its deletion event maps to the PodMigration.
			reqs := r.podMigrationsForManagedObject(ctx, gone)
			if !reflect.DeepEqual(reqs, []ctrl.Request{req}) {
				t.Fatalf("deleted %s maps to %v, want [%v]", tc.kind, reqs, req)
			}
			res, err := r.Reconcile(ctx, reqs[0])
			if err != nil {
				t.Fatalf("Reconcile after the deletion: %v", err)
			}
			if res.RequeueAfter != 0 {
				t.Errorf("RequeueAfter = %v, want 0 once the %s is recreated", res.RequeueAfter, tc.kind)
			}

			got := getManagedForTest(t, c, tc.gvk, tc.key)
			if got.GetDeletionTimestamp() != nil {
				t.Errorf("recreated %s has a deletionTimestamp", tc.kind)
			}
			if !reflect.DeepEqual(got.Object["spec"], tc.wantSpec) {
				t.Errorf("recreated %s spec = %v, want %v", tc.kind, got.Object["spec"], tc.wantSpec)
			}
			if ready := readyConditionForTest(t, c, namespace, name); ready == nil || ready.Status != metav1.ConditionTrue {
				t.Errorf("Ready = %+v, want True once the %s is recreated", ready, tc.kind)
			}
		})
	}
}

// A spec edit on a managed object is reverted by the reconcile that the
// object's own update event maps to. No PodMigration event is involved
// (#144).
func TestPodMigrationReconciler_Sync_SpecEditRevertedFromManagedObjectEvent(t *testing.T) {
	namespace, name := "default", "sync-edit"
	for _, tc := range terminatingTestCases(namespace, name) {
		t.Run(tc.kind, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			c := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
			reconcileSyncTest(t, c, scheme, namespace, name) // creates both objects

			edited := getManagedForTest(t, c, tc.gvk, tc.key)
			edited.Object["spec"] = tc.drifted
			if err := c.Update(ctx, edited); err != nil {
				t.Fatalf("editing the %s spec: %v", tc.kind, err)
			}

			r := &PodMigrationReconciler{Client: c, Scheme: scheme}
			reqs := r.podMigrationsForManagedObject(ctx, edited)
			want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}}}
			if !reflect.DeepEqual(reqs, want) {
				t.Fatalf("edited %s maps to %v, want %v", tc.kind, reqs, want)
			}
			if _, err := r.Reconcile(ctx, reqs[0]); err != nil {
				t.Fatalf("Reconcile after the edit: %v", err)
			}

			if got := getManagedForTest(t, c, tc.gvk, tc.key); !reflect.DeepEqual(got.Object["spec"], tc.wantSpec) {
				t.Errorf("%s spec = %v, want %v", tc.kind, got.Object["spec"], tc.wantSpec)
			}
		})
	}
}

// An event on a managed object maps to the PodMigration that renders an
// object of that kind, namespace and name. Other objects map to nothing.
func TestPodMigrationReconciler_MapsManagedObjectToPodMigration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	pm := syncTestPodMigration("default", "map-me")
	other := syncTestPodMigration("other", "map-other")
	invalid := syncTestPodMigration("invalid", "map-invalid")
	invalid.Spec.Storage.Location = "no-scheme/path"
	r := &PodMigrationReconciler{Client: newSyncTestClient(scheme, pm, other, invalid), Scheme: scheme}

	managed := func(gvk schema.GroupVersionKind, namespace, name string, labels map[string]string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetNamespace(namespace)
		obj.SetName(name)
		obj.SetLabels(labels)
		return obj
	}
	psscName := getPSSCName(pm.Namespace, pm.Name)
	wantPM := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: pm.Namespace, Name: pm.Name}}}

	for _, tc := range []struct {
		desc string
		obj  *unstructured.Unstructured
		want []ctrl.Request
	}{
		{"storage config", managed(psscGVKForTest, "", psscName, map[string]string{OwnerUIDLabelKey: string(pm.UID)}), wantPM},
		{"storage config without the owner label", managed(psscGVKForTest, "", psscName, nil), wantPM},
		{"storage config of an earlier PodMigration with the same name", managed(psscGVKForTest, "", psscName, map[string]string{OwnerUIDLabelKey: "uid-earlier"}), wantPM},
		{"policy", managed(pspGVKForTest, pm.Namespace, getPSPManualName(pm.Name), nil), wantPM},
		{"policy named for a PodMigration in another namespace", managed(pspGVKForTest, pm.Namespace, getPSPManualName(other.Name), nil), nil},
		{"unrelated policy", managed(pspGVKForTest, pm.Namespace, "psp-someone-else", nil), nil},
		{"unrelated storage config", managed(psscGVKForTest, "", "pssc-someone-else", nil), nil},
		{"policy of a PodMigration that renders nothing", managed(pspGVKForTest, invalid.Namespace, getPSPManualName(invalid.Name), nil), nil},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			if got := r.podMigrationsForManagedObject(context.Background(), tc.obj); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("maps to %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPodMigrationReconciler_Finalizer_PostponesDeleteForPolicyAnnotatedJob(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "t2-s5-policy"

	now := metav1.Now()
	config := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              configName,
			UID:               "uid-t2-s5",
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
	pssc.SetGroupVersionKind(psscGVKForTest)
	pssc.SetName(psscName)

	pspName := getPSPManualName(configName)
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(pspGVKForTest)
	psp.SetName(pspName)
	psp.SetNamespace(namespace)

	// EvictionGate and NodePreemptionReconciler record the resolved policy in
	// pod-migration.gke.io/pod-snapshot-policy without setting OwnerReferences (#185).
	activeJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "migrate-t2-s5-counter-0-abcd1234",
			Annotations: map[string]string{
				util.AnnotationPodSnapshotPolicy: pspName,
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	otherPolicyJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "migrate-other-0-ef567890",
			Annotations: map[string]string{
				util.AnnotationPodSnapshotPolicy: getPSPManualName("other-policy"),
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(config, pssc, psp, activeJob, otherPolicyJob).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
		Build()

	r := &PodMigrationReconciler{
		Client: fakeClient,
		Scheme: scheme,
	}

	// 1. Reconcile while activeJob is in Snapshotting: deletion must be deferred
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("Expected RequeueAfter 5s while policy-annotated job is in flight, got %v", res.RequeueAfter)
	}

	checkPSP := &unstructured.Unstructured{}
	checkPSP.SetGroupVersionKind(pspGVKForTest)
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, checkPSP); err != nil {
		t.Errorf("Expected PSP %s to remain while job is in flight, got err: %v", pspName, err)
	}

	updatedConfig := &pmv1alpha1.PodMigration{}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}
	readyCond := meta.FindStatusCondition(updatedConfig.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Reason != "DeletionBlockedByInFlightMigrations" || readyCond.Message != "Deletion deferred: 1 migration(s) in flight" {
		t.Errorf("Unexpected Ready condition during deferral: %+v", readyCond)
	}

	// 2. Complete activeJob (while otherPolicyJob remains in Snapshotting) -> deletion proceeds
	activeJob.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	if err := fakeClient.Status().Update(context.Background(), activeJob); err != nil {
		t.Fatalf("Failed to update activeJob status: %v", err)
	}

	res, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: configName},
	})
	if err != nil {
		t.Fatalf("Reconcile failed after job completed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter 0 once owned job completed, got %v", res.RequeueAfter)
	}
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, checkPSP); !apierrors.IsNotFound(err) {
		t.Errorf("Expected PSP %s to be deleted after job completed, got err: %v", pspName, err)
	}
}

// statusWriteCounter wraps c so that every write to a PodMigration's status
// subresource is recorded in writes.
func statusWriteCounter(c client.WithWatch, writes *[]string) client.WithWatch {
	return interceptor.NewClient(c, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigration); ok {
				*writes = append(*writes, "Update "+sub)
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigration); ok {
				*writes = append(*writes, "Patch "+sub)
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}

// A reconcile that finds the status already up to date must not write it
// (#142).
func TestPodMigrationReconciler_Status_NoOpDoesNotWrite(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "status-noop"
	key := types.NamespacedName{Namespace: namespace, Name: name}

	base := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
	reconcileSyncTest(t, base, scheme, namespace, name) // sets Ready=True

	before := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, before); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}

	var writes []string
	reconcileSyncTest(t, statusWriteCounter(base, &writes), scheme, namespace, name)

	if len(writes) != 0 {
		t.Errorf("reconcile with an unchanged status wrote it: %v", writes)
	}
	after := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, after); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	if after.ResourceVersion != before.ResourceVersion {
		t.Errorf("PodMigration resourceVersion changed from %s to %s", before.ResourceVersion, after.ResourceVersion)
	}
}

// A reconcile that reads the PodMigration from a stale cache, one that hasn't
// seen the previous reconcile's status write yet, must not fail with a
// conflict (#142).
func TestPodMigrationReconciler_Status_StaleReadDoesNotConflict(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "status-stale"
	key := types.NamespacedName{Namespace: namespace, Name: name}

	base := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
	stale := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, stale); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	reconcileSyncTest(t, base, scheme, namespace, name) // writes Ready=True

	c := interceptor.NewClient(base, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if pm, ok := obj.(*pmv1alpha1.PodMigration); ok && k == key {
				stale.DeepCopyInto(pm) // the copy from before the status write
				return nil
			}
			return cl.Get(ctx, k, obj, opts...)
		},
	})
	reconcileSyncTest(t, c, scheme, namespace, name)

	got := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, got); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition = %+v, want status True", cond)
	}
}

// A status change other than Ready is still written: adding the runtime class
// routes annotation adds the EngineRoutesValid condition while Ready stays
// True.
func TestPodMigrationReconciler_Status_RoutesConditionChangeIsWritten(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	namespace, name := "default", "status-routes"
	key := types.NamespacedName{Namespace: namespace, Name: name}

	base := newSyncTestClient(scheme, syncTestPodMigration(namespace, name))
	reconcileSyncTest(t, base, scheme, namespace, name) // sets Ready=True

	pm := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, pm); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	pm.Annotations = map[string]string{engine.AnnotationRuntimeClassEngines: "gvisor=no-such-engine"}
	if err := base.Update(context.Background(), pm); err != nil {
		t.Fatalf("Update PodMigration: %v", err)
	}
	reconcileSyncTest(t, base, scheme, namespace, name)

	got := &pmv1alpha1.PodMigration{}
	if err := base.Get(context.Background(), key, got); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition = %+v, want status True", cond)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionEngineRoutesValid); cond == nil || cond.Status != metav1.ConditionFalse {
		t.Errorf("%s condition = %+v, want status False", ConditionEngineRoutesValid, cond)
	}
}

// A change to an existing condition is written: deferring the deletion of a
// Ready PodMigration, or demoting it as a duplicate, flips its Ready=True
// condition to False in place.
func TestPodMigrationReconciler_Status_ReadyTransitionIsWritten(t *testing.T) {
	ready := metav1.Condition{
		Type:               "Ready",
		Status:             metav1.ConditionTrue,
		Reason:             "Reconciled",
		Message:            "Storage config and policy synced successfully",
		ObservedGeneration: 1,
		LastTransitionTime: metav1.Now(),
	}
	for _, tc := range []struct {
		name       string
		objects    func(namespace, name string) []client.Object
		wantReason string
	}{
		{
			name: "deletion deferred by an in-flight migration",
			objects: func(namespace, name string) []client.Object {
				pm := syncTestPodMigration(namespace, name)
				now := metav1.Now()
				pm.DeletionTimestamp = &now
				pm.Status.Conditions = []metav1.Condition{ready}
				job := &pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: namespace,
						Name:      "in-flight",
						OwnerReferences: []metav1.OwnerReference{{
							APIVersion: pmv1alpha1.GroupVersion.String(),
							Kind:       "PodMigration",
							Name:       name,
						}},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
				}
				return []client.Object{pm, job}
			},
			wantReason: "DeletionBlockedByInFlightMigrations",
		},
		{
			name: "demoted as a duplicate",
			objects: func(namespace, name string) []client.Object {
				pm := syncTestPodMigration(namespace, name)
				pm.Status.Conditions = []metav1.Condition{ready}
				// With equal creation timestamps, the smaller name is active.
				active := syncTestPodMigration(namespace, "a-"+name)
				return []client.Object{pm, active}
			},
			wantReason: ReasonDuplicatePodMigrationInNamespace,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			namespace, name := "default", "status-transition"
			key := types.NamespacedName{Namespace: namespace, Name: name}

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(tc.objects(namespace, name)...).
				WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).
				Build()
			reconcileSyncTest(t, c, scheme, namespace, name)

			got := &pmv1alpha1.PodMigration{}
			if err := c.Get(context.Background(), key, got); err != nil {
				t.Fatalf("Get PodMigration: %v", err)
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tc.wantReason {
				t.Errorf("Ready condition = %+v, want status False, reason %s", cond, tc.wantReason)
			}
		})
	}
}
