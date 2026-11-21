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
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestOrphanSweep_Envtest runs the sweep against a real API server, with
// real CRDs for both engine groups, as a user bound to exactly the
// controller's generated ClusterRole. It deletes the orphans, and keeps a
// Kubeflow snapshot, one a live pod restores from, and one a job names.
func TestOrphanSweep_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest orphan sweep test")
	}
	testEnv := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd", "bases"),
			filepath.Join("..", "..", "..", "engines", "criu", "deploy", "crds.yaml"),
		},
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{gkePodSnapshotCRD()},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	scheme := relScheme()
	_ = rbacv1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	ctx := context.Background()
	controllerClient := clientForRole(ctx, t, testEnv, admin, scheme, "controller", controllerClusterRole(t))

	const ns = "sweep"
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	newSnap := func(group, name, trigger string) {
		t.Helper()
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
		u.SetNamespace(ns)
		u.SetName(name)
		u.SetLabels(map[string]string{snapshot.TriggeredByLabel: trigger})
		if err := admin.Create(ctx, u); err != nil {
			t.Fatalf("creating %s snapshot %s: %v", group, name, err)
		}
	}
	exists := func(group, name string) bool {
		t.Helper()
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
		err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, u)
		if apierrors.IsNotFound(err) {
			return false
		}
		if err != nil {
			t.Fatalf("getting snapshot: %v", err)
		}
		return true
	}

	newSnap(snapshot.DefaultAPIGroup, "orphan-gke", util.FormatPSMTName("web-0", "a1b2c3d4-0000"))
	newSnap(snapshot.CRIUAPIGroup, "orphan-criu", util.FormatPSMTName("db-0", "b1b2c3d4-0000"))
	newSnap(snapshot.DefaultAPIGroup, "kubeflow", "ws-notebook-pause")
	newSnap(snapshot.CRIUAPIGroup, "restoring", util.FormatPSMTName("api-0", "c1b2c3d4-0000"))
	newSnap(snapshot.DefaultAPIGroup, "job-held", util.FormatPSMTName("cache-0", "d1b2c3d4-0000"))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "api-0-new",
			Annotations: map[string]string{snapshot.RestoreAnnotation(snapshot.CRIUAPIGroup): "restoring"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "busybox"}}},
	}
	if err := admin.Create(ctx, pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: util.FormatPMJName("cache-0", "d1b2c3d4-0000")},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "cache-0"}, TargetPodUID: "d1b2c3d4-0000"},
	}
	if err := admin.Create(ctx, job); err != nil {
		t.Fatalf("creating job: %v", err)
	}

	s := &OrphanSnapshotSweeper{
		Client:    controllerClient,
		APIReader: controllerClient,
		Groups:    sweepGroups,
		MinAge:    DefaultOrphanSnapshotMinAge,
		Now:       func() time.Time { return time.Now().Add(2 * DefaultOrphanSnapshotMinAge) },
	}
	n, err := s.SweepOnce(ctx)
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	if n != 2 {
		t.Errorf("deleted %d, want 2", n)
	}
	for _, tc := range []struct {
		group, name string
		want        bool
	}{
		{snapshot.DefaultAPIGroup, "orphan-gke", false},
		{snapshot.CRIUAPIGroup, "orphan-criu", false},
		{snapshot.DefaultAPIGroup, "kubeflow", true},
		{snapshot.CRIUAPIGroup, "restoring", true},
		{snapshot.DefaultAPIGroup, "job-held", true},
	} {
		if got := exists(tc.group, tc.name); got != tc.want {
			t.Errorf("%s %s exists = %v, want %v", tc.group, tc.name, got, tc.want)
		}
	}

	// Younger than the minimum age: kept.
	newSnap(snapshot.DefaultAPIGroup, "young", util.FormatPSMTName("new-0", "e1b2c3d4-0000"))
	s.Now = time.Now
	if n, err := s.SweepOnce(ctx); err != nil || n != 0 || !exists(snapshot.DefaultAPIGroup, "young") {
		t.Fatalf("SweepOnce on a young snapshot: deleted %d, err %v", n, err)
	}
}
