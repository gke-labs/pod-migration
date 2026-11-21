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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// gkePodSnapshotCRD is a minimal, open-schema podsnapshots.podsnapshot.gke.io
// CRD; the managed add-on owns the real one.
func gkePodSnapshotCRD() *apiextensionsv1.CustomResourceDefinition {
	preserve := true
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "podsnapshots." + snapshot.DefaultAPIGroup},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: snapshot.DefaultAPIGroup,
			Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "podsnapshots", Singular: "podsnapshot", Kind: "PodSnapshot", ListKind: "PodSnapshotList"},
			Scope: apiextensionsv1.NamespaceScoped,
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Subresources: &apiextensionsv1.CustomResourceSubresources{Status: &apiextensionsv1.CustomResourceSubresourceStatus{}},
				Schema: &apiextensionsv1.CustomResourceValidation{OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
					Type: "object", XPreserveUnknownFields: &preserve,
				}},
			}},
		},
	}
}

// TestSnapshotRelease_Envtest runs the release against a real API server as a
// user bound to exactly the controller's generated ClusterRole: it may delete
// PodSnapshots in both engine groups, deletes the concluded job's own
// snapshot, and leaves a snapshot that another trigger created alone.
func TestSnapshotRelease_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest snapshot release test")
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

	newSnap := func(t *testing.T, ns, group, name, trigger string) {
		t.Helper()
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
		u.SetNamespace(ns)
		u.SetName(name)
		u.SetLabels(map[string]string{snapshot.TriggeredByLabel: trigger})
		if err := admin.Create(ctx, u); err != nil {
			t.Fatalf("creating %s snapshot %s: %v", group, name, err)
		}
		u.Object["status"] = map[string]interface{}{"conditions": []interface{}{
			map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded", "message": "ok",
				"lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
		}}
		if err := admin.Status().Update(ctx, u); err != nil {
			t.Fatalf("setting snapshot status: %v", err)
		}
	}
	exists := func(t *testing.T, ns, group, name string) bool {
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
	newJob := func(t *testing.T, ns, engineName, snap string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: relJobName},
			Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: relPod}, TargetPodUID: relSourceUID},
		}
		if engineName != "" {
			job.Annotations = map[string]string{engine.AnnotationEngine: engineName}
		}
		if err := admin.Create(ctx, job); err != nil {
			t.Fatalf("creating job: %v", err)
		}
		done := metav1.Now()
		job.Status = pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded, SnapshotRef: snap, CompletionTime: &done}
		if err := admin.Status().Update(ctx, job); err != nil {
			t.Fatalf("setting job status: %v", err)
		}
		return job
	}

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(controllerClient, scheme)); err != nil {
		t.Fatal(err)
	}
	criu, err := engine.NewCRIU(controllerClient, scheme, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatal(err)
	}
	if err := reg.Register(criu); err != nil {
		t.Fatal(err)
	}
	r := &PodMigrationJobReconciler{Client: controllerClient, APIReader: controllerClient, Scheme: scheme, Engines: reg}
	reconcile := func(t *testing.T, ns string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: relJobName}}); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: relJobName}, got); err != nil {
			t.Fatalf("getting job: %v", err)
		}
		return got
	}
	trigger := util.FormatPSMTName(relPod, relSourceUID)

	for _, tc := range []struct {
		ns, engine, group string
	}{
		{"release-gke", "", snapshot.DefaultAPIGroup},
		{"release-criu", engine.CRIUName, engine.CRIUAPIGroup},
	} {
		t.Run(tc.group, func(t *testing.T) {
			if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tc.ns}}); err != nil {
				t.Fatalf("creating namespace: %v", err)
			}
			newSnap(t, tc.ns, tc.group, relSnap, trigger)
			newJob(t, tc.ns, tc.engine, relSnap)
			got := reconcile(t, tc.ns)
			if exists(t, tc.ns, tc.group, relSnap) {
				t.Fatalf("snapshot in %s still exists after the job concluded Succeeded", tc.group)
			}
			if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionSnapshotReleased); cond == nil || cond.Status != metav1.ConditionTrue {
				t.Fatalf("SnapshotReleased = %+v, want True", cond)
			}
		})
	}

	t.Run("another trigger's snapshot", func(t *testing.T) {
		const ns = "release-foreign"
		if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatalf("creating namespace: %v", err)
		}
		newSnap(t, ns, snapshot.DefaultAPIGroup, relSnap, "ws-notebook-trigger")
		newJob(t, ns, "", relSnap)
		got := reconcile(t, ns)
		if !exists(t, ns, snapshot.DefaultAPIGroup, relSnap) {
			t.Fatal("deleted a snapshot that another trigger created")
		}
		if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionSnapshotReleased); cond == nil || cond.Reason != ReasonSnapshotNotOwned {
			t.Fatalf("SnapshotReleased = %+v, want %s", cond, ReasonSnapshotNotOwned)
		}
	})
}
