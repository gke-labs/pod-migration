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

func TestPodMigrationReconciler_Reconcile_WithExcludedPodSelectors_Success(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-migration-exclusions"

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
			ExcludedPodSelectors: []metav1.LabelSelectorRequirement{
				{
					Key:      "notebooks.kubeflow.org/workspace-name",
					Operator: metav1.LabelSelectorOpDoesNotExist,
				},
				{
					Key:      "tier",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{"cache", "batch"},
				},
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

	// Verify PodSnapshotPolicy selector has 3 matchExpressions
	pspList := &unstructured.UnstructuredList{}
	pspList.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicyList",
	})
	if err := fakeClient.List(context.Background(), pspList); err != nil {
		t.Fatalf("Failed to list PSPs: %v", err)
	}
	if len(pspList.Items) != 1 {
		t.Fatalf("Expected 1 PSP, got %d", len(pspList.Items))
	}
	psp := pspList.Items[0]

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
	if len(matchExpressions) != 3 {
		t.Fatalf("Expected 3 matchExpressions, got %d: %+v", len(matchExpressions), matchExpressions)
	}

	// 1. Opt-in expr
	expr0 := matchExpressions[0].(map[string]interface{})
	if expr0["key"] != "pod-migration.gke.io/enabled" || expr0["operator"] != "In" {
		t.Errorf("Unexpected expr0: %+v", expr0)
	}

	// 2. DoesNotExist expr
	expr1 := matchExpressions[1].(map[string]interface{})
	if expr1["key"] != "notebooks.kubeflow.org/workspace-name" || expr1["operator"] != "DoesNotExist" {
		t.Errorf("Unexpected expr1: %+v", expr1)
	}
	if _, ok := expr1["values"]; ok {
		t.Errorf("DoesNotExist expression should not have values field: %+v", expr1)
	}

	// 3. NotIn expr
	expr2 := matchExpressions[2].(map[string]interface{})
	if expr2["key"] != "tier" || expr2["operator"] != "NotIn" {
		t.Errorf("Unexpected expr2: %+v", expr2)
	}
	vals, ok := expr2["values"].([]interface{})
	if !ok || len(vals) != 2 || vals[0] != "cache" || vals[1] != "batch" {
		t.Errorf("Unexpected values in expr2: %+v", expr2)
	}
}

func TestPodMigrationReconciler_Reconcile_WithExcludedPodSelectors_Update(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	configName := "test-migration-update"

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

	// First reconcile without ExcludedPodSelectors
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("First reconcile failed: %v", err)
	}

	// Update config with ExcludedPodSelectors
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, config)
	if err != nil {
		t.Fatalf("Failed to get config: %v", err)
	}
	config.Spec.ExcludedPodSelectors = []metav1.LabelSelectorRequirement{
		{
			Key:      "notebooks.kubeflow.org/workspace-name",
			Operator: metav1.LabelSelectorOpDoesNotExist,
		},
	}
	config.Generation = 2
	if err := fakeClient.Update(context.Background(), config); err != nil {
		t.Fatalf("Failed to update config: %v", err)
	}

	// Second reconcile
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Second reconcile failed: %v", err)
	}

	// Verify PSP was updated in-place
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspName := getPSPManualName(configName)
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, psp); err != nil {
		t.Fatalf("Failed to get updated PSP: %v", err)
	}

	pspSpec := psp.Object["spec"].(map[string]interface{})
	selector := pspSpec["selector"].(map[string]interface{})
	matchExpressions := selector["matchExpressions"].([]interface{})
	if len(matchExpressions) != 2 {
		t.Fatalf("Expected 2 matchExpressions after update, got %d: %+v", len(matchExpressions), matchExpressions)
	}

	// 3. Change ExcludedPodSelectors (modify existing exclusion)
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, config)
	if err != nil {
		t.Fatalf("Failed to get config before change: %v", err)
	}
	config.Spec.ExcludedPodSelectors = []metav1.LabelSelectorRequirement{
		{
			Key:      "tier",
			Operator: metav1.LabelSelectorOpNotIn,
			Values:   []string{"cache", "batch"},
		},
	}
	config.Generation = 3
	if err := fakeClient.Update(context.Background(), config); err != nil {
		t.Fatalf("Failed to update config for change: %v", err)
	}

	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Third reconcile (change) failed: %v", err)
	}

	// Verify PSP selector was updated with changed requirement
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, psp); err != nil {
		t.Fatalf("Failed to get changed PSP: %v", err)
	}
	pspSpec = psp.Object["spec"].(map[string]interface{})
	selector = pspSpec["selector"].(map[string]interface{})
	matchExpressions = selector["matchExpressions"].([]interface{})
	if len(matchExpressions) != 2 {
		t.Fatalf("Expected 2 matchExpressions after change, got %d: %+v", len(matchExpressions), matchExpressions)
	}
	changedExpr := matchExpressions[1].(map[string]interface{})
	if changedExpr["key"] != "tier" || changedExpr["operator"] != "NotIn" {
		t.Errorf("Unexpected changedExpr: %+v", changedExpr)
	}

	// 4. Remove ExcludedPodSelectors (revert to default opt-in only)
	err = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, config)
	if err != nil {
		t.Fatalf("Failed to get config before removal: %v", err)
	}
	config.Spec.ExcludedPodSelectors = nil
	config.Generation = 4
	if err := fakeClient.Update(context.Background(), config); err != nil {
		t.Fatalf("Failed to update config for removal: %v", err)
	}

	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      configName,
		},
	})
	if err != nil {
		t.Fatalf("Fourth reconcile (removal) failed: %v", err)
	}

	// Verify PSP selector reverted to 1 matchExpression (opt-in only)
	if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, psp); err != nil {
		t.Fatalf("Failed to get reverted PSP: %v", err)
	}
	pspSpec = psp.Object["spec"].(map[string]interface{})
	selector = pspSpec["selector"].(map[string]interface{})
	matchExpressions = selector["matchExpressions"].([]interface{})
	if len(matchExpressions) != 1 {
		t.Fatalf("Expected 1 matchExpression after removal, got %d: %+v", len(matchExpressions), matchExpressions)
	}
	optInExpr := matchExpressions[0].(map[string]interface{})
	if optInExpr["key"] != "pod-migration.gke.io/enabled" || optInExpr["operator"] != "In" {
		t.Errorf("Unexpected optInExpr after removal: %+v", optInExpr)
	}
}

func TestPodMigrationReconciler_Reconcile_WithExcludedPodSelectors_ValidationErrors(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"

	tests := []struct {
		name        string
		requirement metav1.LabelSelectorRequirement
		errSubstr   string
	}{
		{
			name: "empty key",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "",
				Operator: metav1.LabelSelectorOpDoesNotExist,
			},
			errSubstr: "key must not be empty",
		},
		{
			name: "override opt-in key",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "pod-migration.gke.io/enabled",
				Operator: metav1.LabelSelectorOpDoesNotExist,
			},
			errSubstr: "cannot be overridden",
		},
		{
			name: "invalid operator",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "app",
				Operator: "InvalidOperator",
			},
			errSubstr: "invalid operator",
		},
		{
			name: "In operator rejected",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "app",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"val"},
			},
			errSubstr: "operator \"In\" is not allowed for exclusions",
		},
		{
			name: "Exists operator rejected",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "app",
				Operator: metav1.LabelSelectorOpExists,
			},
			errSubstr: "operator \"Exists\" is not allowed for exclusions",
		},
		{
			name: "NotIn operator with empty values",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "app",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   nil,
			},
			errSubstr: "requires non-empty values",
		},
		{
			name: "DoesNotExist operator with non-empty values",
			requirement: metav1.LabelSelectorRequirement{
				Key:      "app",
				Operator: metav1.LabelSelectorOpDoesNotExist,
				Values:   []string{"val"},
			},
			errSubstr: "requires empty values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configName := "invalid-selector-" + strings.ReplaceAll(tt.name, " ", "-")
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
						Location: "gs://my-test-bucket/snapshots",
					},
					ExcludedPodSelectors: []metav1.LabelSelectorRequirement{
						tt.requirement,
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

			res, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{
					Namespace: namespace,
					Name:      configName,
				},
			})
			if err != nil {
				t.Fatalf("Expected nil error from Reconcile (spec error should not requeue), got: %v", err)
			}
			if res.Requeue || res.RequeueAfter != 0 {
				t.Errorf("Expected no requeue on spec validation error, got: %+v", res)
			}

			updatedConfig := &pmv1alpha1.PodMigration{}
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: configName}, updatedConfig); err != nil {
				t.Fatalf("Failed to get updated config: %v", err)
			}

			cond := meta.FindStatusCondition(updatedConfig.Status.Conditions, "Ready")
			if cond == nil {
				t.Fatalf("Ready condition not found in status")
			}
			if cond.Status != metav1.ConditionFalse {
				t.Errorf("Expected status False, got %s", cond.Status)
			}
			if cond.Reason != "InvalidExcludedPodSelectors" {
				t.Errorf("Expected reason InvalidExcludedPodSelectors, got %s", cond.Reason)
			}
			if !strings.Contains(cond.Message, tt.errSubstr) {
				t.Errorf("Expected condition message to contain %q, got: %s", tt.errSubstr, cond.Message)
			}

			// Verify PSSC was created (storage config is not blocked by selector error)
			psscName := getPSSCName(namespace, configName)
			pssc := &unstructured.Unstructured{}
			pssc.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "podsnapshot.gke.io",
				Version: "v1",
				Kind:    "PodSnapshotStorageConfig",
			})
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Name: psscName}, pssc); err != nil {
				t.Errorf("Expected PSSC %s to be created despite invalid selector, got error: %v", psscName, err)
			}

			// Verify PSP was NOT created
			pspName := getPSPManualName(configName)
			psp := &unstructured.Unstructured{}
			psp.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   "podsnapshot.gke.io",
				Version: "v1",
				Kind:    "PodSnapshotPolicy",
			})
			if err := fakeClient.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pspName}, psp); err == nil {
				t.Errorf("Expected PSP to NOT be created on invalid selector")
			}
		})
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
