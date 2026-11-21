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
)

// TestDeletingSnapshot_Envtest checks #265 against a real API server, as a
// user bound to exactly the controller's generated ClusterRole, in both
// engine groups: a Ready PodSnapshot whose deletion a finalizer holds is
// seen as being deleted and never reported Ready, and a Restoring job held
// for it concludes SucceededWithoutRestore. Once the snapshot is gone, it's
// read as gone (#269), and a job held for that concludes too.
func TestDeletingSnapshot_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest deleting-snapshot test")
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

	for _, tc := range []struct {
		ns, engine, group string
	}{
		{"deleting-gke", "", snapshot.DefaultAPIGroup},
		{"deleting-criu", engine.CRIUName, engine.CRIUAPIGroup},
	} {
		t.Run(tc.group, func(t *testing.T) {
			if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: tc.ns}}); err != nil {
				t.Fatalf("creating namespace: %v", err)
			}
			snap := &unstructured.Unstructured{}
			snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(tc.group))
			snap.SetNamespace(tc.ns)
			snap.SetName(relSnap)
			snap.SetFinalizers([]string{"example.com/archive-cleanup"})
			if err := admin.Create(ctx, snap); err != nil {
				t.Fatalf("creating snapshot: %v", err)
			}
			snap.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded", "message": "ok",
					"lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
			}}
			if err := admin.Status().Update(ctx, snap); err != nil {
				t.Fatalf("setting snapshot status: %v", err)
			}
			if err := admin.Delete(ctx, snap); err != nil {
				t.Fatalf("deleting snapshot: %v", err)
			}

			deleting, err := snapshot.PodSnapshotBeingDeleted(ctx, controllerClient, tc.group, tc.ns, relSnap)
			if err != nil || !deleting {
				t.Fatalf("PodSnapshotBeingDeleted = %t, %v; want true", deleting, err)
			}
			got := &unstructured.Unstructured{}
			got.SetGroupVersionKind(snapshot.PodSnapshotGVK(tc.group))
			if err := controllerClient.Get(ctx, types.NamespacedName{Namespace: tc.ns, Name: relSnap}, got); err != nil {
				t.Fatalf("getting snapshot: %v", err)
			}
			if st := snapshot.CheckPodSnapshotStatus(got, time.Now()); st.Phase != snapshot.PhaseInProgress || st.Reason != snapshot.ReasonSnapshotDeleting {
				t.Errorf("CheckPodSnapshotStatus = %s/%s, want %s/%s", st.Phase, st.Reason, snapshot.PhaseInProgress, snapshot.ReasonSnapshotDeleting)
			}

			job := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: tc.ns, Name: relJobName},
				Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: relPod}, TargetPodUID: relSourceUID},
			}
			if tc.engine != "" {
				job.Annotations = map[string]string{engine.AnnotationEngine: tc.engine}
			}
			if err := admin.Create(ctx, job); err != nil {
				t.Fatalf("creating job: %v", err)
			}
			start := metav1.Now()
			// The pod gate's hold, recorded when it found the snapshot being
			// deleted (the gate needs the manager's field indexes, which a
			// plain client doesn't have, so it's covered by unit tests).
			job.Status = pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseRestoring, SnapshotRef: relSnap, RestoringStartTime: &start,
				Conditions: []metav1.Condition{{
					Type: ConditionSnapshotRestorable, Status: metav1.ConditionFalse, Reason: snapshot.ReasonSnapshotDeleting,
					Message: snapshotDeletingMessage(relSnap), LastTransitionTime: start,
				}},
			}
			if err := admin.Status().Update(ctx, job); err != nil {
				t.Fatalf("setting job status: %v", err)
			}
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: tc.ns, Name: relJobName}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			concluded := &pmv1alpha1.PodMigrationJob{}
			if err := admin.Get(ctx, types.NamespacedName{Namespace: tc.ns, Name: relJobName}, concluded); err != nil {
				t.Fatalf("getting job: %v", err)
			}
			if concluded.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				t.Fatalf("phase = %s, want SucceededWithoutRestore", concluded.Status.Phase)
			}
			if cond := meta.FindStatusCondition(concluded.Status.Conditions, "Restored"); cond == nil || cond.Reason != snapshot.ReasonSnapshotDeleting {
				t.Errorf("Restored = %+v, want reason %s", cond, snapshot.ReasonSnapshotDeleting)
			}

			// #269: once the finalizer is gone, so is the snapshot, and the
			// controller's role reads that as gone (not as "no such CRD").
			if err := admin.Get(ctx, types.NamespacedName{Namespace: tc.ns, Name: relSnap}, snap); err != nil {
				t.Fatalf("getting snapshot: %v", err)
			}
			snap.SetFinalizers(nil)
			if err := admin.Update(ctx, snap); err != nil {
				t.Fatalf("removing the snapshot's finalizer: %v", err)
			}
			reason, err := snapshot.PodSnapshotUnrestorable(ctx, controllerClient, tc.group, tc.ns, relSnap)
			if err != nil || reason != snapshot.ReasonSnapshotGone {
				t.Fatalf("PodSnapshotUnrestorable = %q, %v; want %s", reason, err, snapshot.ReasonSnapshotGone)
			}
			goneJob := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: tc.ns, Name: relJobName + "-gone"},
				Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: relPod}, TargetPodUID: relSourceUID},
			}
			if tc.engine != "" {
				goneJob.Annotations = map[string]string{engine.AnnotationEngine: tc.engine}
			}
			if err := admin.Create(ctx, goneJob); err != nil {
				t.Fatalf("creating job: %v", err)
			}
			goneJob.Status = pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseRestoring, SnapshotRef: relSnap, RestoringStartTime: &start,
				Conditions: []metav1.Condition{{
					Type: ConditionSnapshotRestorable, Status: metav1.ConditionFalse, Reason: snapshot.ReasonSnapshotGone,
					Message: snapshotGoneMessage(relSnap), LastTransitionTime: start,
				}},
			}
			if err := admin.Status().Update(ctx, goneJob); err != nil {
				t.Fatalf("setting job status: %v", err)
			}
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(goneJob)}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if err := admin.Get(ctx, client.ObjectKeyFromObject(goneJob), concluded); err != nil {
				t.Fatalf("getting job: %v", err)
			}
			if concluded.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
				t.Fatalf("phase = %s, want SucceededWithoutRestore", concluded.Status.Phase)
			}
			if cond := meta.FindStatusCondition(concluded.Status.Conditions, "Restored"); cond == nil || cond.Reason != snapshot.ReasonSnapshotGone {
				t.Errorf("Restored = %+v, want reason %s", cond, snapshot.ReasonSnapshotGone)
			}
		})
	}
}
