// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package controller

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestValidateCoverageRefreshInterval(t *testing.T) {
	tests := []struct {
		name     string
		interval time.Duration
		wantErr  bool
	}{
		{name: "negative", interval: -1 * time.Second, wantErr: true},
		{name: "zero disabled", interval: 0, wantErr: false},
		{name: "sub-millisecond storm", interval: 1 * time.Nanosecond, wantErr: true},
		{name: "below minimum", interval: 4 * time.Second, wantErr: true},
		{name: "minimum floor", interval: MinCoverageRefreshInterval, wantErr: false},
		{name: "default", interval: DefaultCoverageRefreshInterval, wantErr: false},
		{name: "one minute", interval: time.Minute, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateCoverageRefreshInterval(tc.interval)
			if (err != nil) != tc.wantErr {
				t.Errorf("ValidateCoverageRefreshInterval(%v) error = %v, wantErr %v", tc.interval, err, tc.wantErr)
			}
		})
	}
}

func TestCoverageRefresher_RefreshOnce_UpdatesAndClears(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	namespace := "test-refresher-ns"
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "mig-a",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/snapshots"},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-1",
			Labels: map[string]string{
				"app":                          "web",
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c1", Image: "img"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	psp.SetName("psp-web")
	psp.SetNamespace(namespace)
	psp.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
	psp.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"app": "web",
			},
		},
	}
	psp.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":   "Ready",
				"status": "True",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm, pod, psp).
		Build()

	testEng := &routedEngine{
		name:       "gke-pod-snapshots",
		apiGroup:   "podsnapshot.gke.io",
		classes:    []string{eligibility.DefaultRuntimeClassToken},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingStopped},
	}
	reg := engine.NewRegistry(testEng.Name())
	_ = reg.Register(testEng)

	refresher := &CoverageRefresher{
		Client:   fakeClient,
		Engines:  reg,
		Interval: 30 * time.Second,
	}

	// 1. Tick 1: updates metrics for active namespace
	refresher.RefreshOnce(context.Background())
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 1 {
		t.Fatalf("expected 1 covered pod after RefreshOnce, got %v", got)
	}
	if got := testutil.ToFloat64(metrics.MigratableTotalRunningPods.WithLabelValues(namespace)); got != 1 {
		t.Fatalf("expected 1 total running pod after RefreshOnce, got %v", got)
	}
	if got := testutil.ToFloat64(metrics.MigratableCoverageRatio.WithLabelValues(namespace)); got != 1.0 {
		t.Fatalf("expected coverage ratio 1.0 after RefreshOnce, got %v", got)
	}

	// 2. Reconciler deletes the PodMigration and clears namespace coverage
	if err := fakeClient.Delete(context.Background(), pm); err != nil {
		t.Fatalf("failed to delete PodMigration from fakeClient: %v", err)
	}
	metrics.ClearNamespaceCoverage(namespace)

	// Series should be 0 immediately after reconciler deletion
	if count := testutil.CollectAndCount(metrics.MigratableCoverageRatio); count != 0 {
		t.Fatalf("expected 0 series after reconciler deletion, got %d", count)
	}

	// 3. Tick 2: Refresher must NOT bring back deleted namespace gauge, and must clear any orphaned series
	refresher.RefreshOnce(context.Background())
	if count := testutil.CollectAndCount(metrics.MigratableCoverageRatio); count != 0 {
		t.Fatalf("expected 0 series after subsequent refresher tick, got %d", count)
	}
	if count := testutil.CollectAndCount(metrics.MigratableCoveredRunningPods); count != 0 {
		t.Fatalf("expected 0 covered series after subsequent refresher tick, got %d", count)
	}
}

func TestCoverageRefresher_TerminatingWithFinalizerSelection(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	namespace := "test-terminating-ns"
	now := metav1.Now()
	// Terminating PodMigration holding StorageCleanupFinalizer
	pmTerminating := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "mig-terminating",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-20 * time.Minute)),
			DeletionTimestamp: &now,
			Finalizers:        []string{util.StorageCleanupFinalizer},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/snapshots"},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-1",
			Labels: map[string]string{
				"app":                          "web",
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c1", Image: "img"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	psp.SetName("psp-web")
	psp.SetNamespace(namespace)
	psp.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
	psp.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"app": "web",
			},
		},
	}
	psp.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":   "Ready",
				"status": "True",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmTerminating, pod, psp).
		Build()

	testEng := &routedEngine{
		name:       "gke-pod-snapshots",
		apiGroup:   "podsnapshot.gke.io",
		classes:    []string{eligibility.DefaultRuntimeClassToken},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingStopped},
	}
	reg := engine.NewRegistry(testEng.Name())
	_ = reg.Register(testEng)

	refresher := &CoverageRefresher{
		Client:   fakeClient,
		Engines:  reg,
		Interval: 30 * time.Second,
	}

	// Terminating PodMigration with finalizer still counts as active owner
	refresher.RefreshOnce(context.Background())
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 1 {
		t.Fatalf("expected terminating PM with finalizer to be active and have 1 covered pod, got %v", got)
	}

	// Remove finalizer: now it is no longer an active candidate
	pmTerminating.Finalizers = nil
	if err := fakeClient.Update(context.Background(), pmTerminating); err != nil {
		t.Fatalf("failed to remove finalizer: %v", err)
	}

	refresher.RefreshOnce(context.Background())
	if count := testutil.CollectAndCount(metrics.MigratableCoverageRatio); count != 0 {
		t.Fatalf("expected 0 series once finalizer is removed, got %d", count)
	}
}

func TestCoverageMetrics_CRIU_RequiresSnapshotEngineLabel(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	namespace := "test-criu-label-ns"
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "mig-criu",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/snapshots"},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-criu",
			Labels: map[string]string{
				"app":                          "worker",
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: stringPtr("criu"),
			Containers:       []corev1.Container{{Name: "c1", Image: "img"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// PSP in CRIU group WITHOUT the required snapshot-engine=criu label
	pspWithoutLabel := &unstructured.Unstructured{}
	pspWithoutLabel.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "criu.example.com",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspWithoutLabel.SetName("psp-criu-no-label")
	pspWithoutLabel.SetNamespace(namespace)
	pspWithoutLabel.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
	pspWithoutLabel.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"app": "worker",
			},
		},
	}
	pspWithoutLabel.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":   "Ready",
				"status": "True",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm, pod, pspWithoutLabel).
		Build()

	criuEng := &routedEngine{
		name:       engine.CRIUName,
		apiGroup:   "criu.example.com",
		classes:    []string{"criu"},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingStopped},
	}
	reg := engine.NewRegistry(criuEng.Name())
	_ = reg.Register(criuEng)

	// 1. Without required snapshot-engine label on policy: covered must be 0
	if err := ComputeCoverageForNamespace(context.Background(), fakeClient, pm, reg, nil); err != nil {
		t.Fatalf("ComputeCoverageForNamespace failed: %v", err)
	}
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 0 {
		t.Fatalf("expected 0 covered pods when CRIU PSP lacks snapshot-engine label, got %v", got)
	}

	// 2. Add required label: now it matches -> covered = 1
	pspWithoutLabel.SetLabels(map[string]string{
		util.LabelSnapshotEngine: engine.CRIUName,
	})
	if err := fakeClient.Update(context.Background(), pspWithoutLabel); err != nil {
		t.Fatalf("failed to update PSP with label: %v", err)
	}
	if err := ComputeCoverageForNamespace(context.Background(), fakeClient, pm, reg, nil); err != nil {
		t.Fatalf("ComputeCoverageForNamespace failed: %v", err)
	}
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 1 {
		t.Fatalf("expected 1 covered pod after CRIU PSP has snapshot-engine label, got %v", got)
	}
}

func TestCoverageMetrics_GKE_ExcludesCRIUEngineLabel(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	namespace := "test-gke-exclude-criu-ns"
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "mig-gke",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/snapshots"},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-gke",
			Labels: map[string]string{
				"app":                          "gke-app",
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "c1", Image: "img"}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	// PSP in GKE group carrying snapshot-engine=criu label
	pspWithCRIULabel := &unstructured.Unstructured{}
	pspWithCRIULabel.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	pspWithCRIULabel.SetName("psp-gke-criu-labeled")
	pspWithCRIULabel.SetNamespace(namespace)
	pspWithCRIULabel.SetLabels(map[string]string{
		util.LabelSnapshotEngine: "criu",
	})
	pspWithCRIULabel.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-10 * time.Minute)))
	pspWithCRIULabel.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{
				"app": "gke-app",
			},
		},
	}
	pspWithCRIULabel.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":   "Ready",
				"status": "True",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm, pod, pspWithCRIULabel).
		Build()

	gkeEng := &routedEngine{
		name:       "gke-pod-snapshots",
		apiGroup:   "podsnapshot.gke.io",
		classes:    []string{eligibility.DefaultRuntimeClassToken},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingStopped},
	}
	reg := engine.NewRegistry(gkeEng.Name())
	_ = reg.Register(gkeEng)

	// 1. With snapshot-engine=criu label on GKE policy: GKE engine excludes it -> covered = 0
	if err := ComputeCoverageForNamespace(context.Background(), fakeClient, pm, reg, nil); err != nil {
		t.Fatalf("ComputeCoverageForNamespace failed: %v", err)
	}
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 0 {
		t.Fatalf("expected 0 covered pods because GKE engine excludes criu-labeled PSP, got %v", got)
	}

	// 2. Remove criu label from policy: now matches GKE engine -> covered = 1
	pspWithCRIULabel.SetLabels(nil)
	if err := fakeClient.Update(context.Background(), pspWithCRIULabel); err != nil {
		t.Fatalf("failed to update PSP: %v", err)
	}
	if err := ComputeCoverageForNamespace(context.Background(), fakeClient, pm, reg, nil); err != nil {
		t.Fatalf("ComputeCoverageForNamespace failed: %v", err)
	}
	if got := testutil.ToFloat64(metrics.MigratableCoveredRunningPods.WithLabelValues(namespace)); got != 1 {
		t.Fatalf("expected 1 covered pod after removing criu label from GKE PSP, got %v", got)
	}
}

func stringPtr(s string) *string {
	return &s
}
