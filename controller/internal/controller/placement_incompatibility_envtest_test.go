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
	"strings"
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
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPlacementIncompatibility_Envtest checks the signals for external
// controllers (#294) against a real API server, as a user bound to exactly
// the controller's generated ClusterRole:
//
//   - a GKE Pod Snapshots and a CRIU replacement released with the engine's
//     node constraints, unschedulable past the placement deadline while a
//     node that fails them has room: the job concludes PlacementTimeout,
//     its condition and the Warning events on the job and the pod name the
//     engine and the constraint keys that failed, and the pod is deleted for
//     its owner to recreate cold;
//   - a CRIU replacement whose restore the node agent refused for the CPU
//     model: the job concludes RestoreIncompatibleNode, with Warning events
//     that name the engine, the fact and both nodes, and the replacement
//     keeps running cold.
func TestPlacementIncompatibility_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest placement incompatibility test")
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

	newReconciler := func() (*PodMigrationJobReconciler, *record.FakeRecorder) {
		recorder := record.NewFakeRecorder(50)
		return &PodMigrationJobReconciler{
			Client: controllerClient, APIReader: controllerClient, Scheme: scheme, Engines: reg, Recorder: recorder,
			RestoreStabilizationWindow: DefaultRestoreStabilizationWindow,
		}, recorder
	}
	drain := func(recorder *record.FakeRecorder) []string {
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		return events
	}
	countPrefixed := func(events []string, prefix string) int {
		n := 0
		for _, e := range events {
			if strings.HasPrefix(e, prefix) {
				n++
			}
		}
		return n
	}
	createNode := func(t *testing.T, node *corev1.Node) {
		t.Helper()
		status := node.Status
		if err := admin.Create(ctx, node); err != nil {
			t.Fatalf("creating node %s: %v", node.Name, err)
		}
		node.Status = status
		if err := admin.Status().Update(ctx, node); err != nil {
			t.Fatalf("setting node %s Ready: %v", node.Name, err)
		}
		// The API server taints a new node not-ready (TaintNodesByCondition),
		// and nothing here takes the taint off once it's Ready, as the node
		// lifecycle controller would.
		var taints []corev1.Taint
		for _, taint := range node.Spec.Taints {
			if taint.Key != corev1.TaintNodeNotReady {
				taints = append(taints, taint)
			}
		}
		node.Spec.Taints = taints
		if err := admin.Update(ctx, node); err != nil {
			t.Fatalf("untainting node %s: %v", node.Name, err)
		}
		t.Cleanup(func() { _ = admin.Delete(ctx, node) })
	}
	// createJob creates job and then writes its status.
	createJob := func(t *testing.T, job *pmv1alpha1.PodMigrationJob) {
		t.Helper()
		status := job.Status
		job.ResourceVersion = ""
		if err := admin.Create(ctx, job); err != nil {
			t.Fatalf("creating job: %v", err)
		}
		job.Status = status
		if err := admin.Status().Update(ctx, job); err != nil {
			t.Fatalf("setting job status: %v", err)
		}
	}
	// createPod creates pod and then writes its status; its UID is the
	// API server's, so it returns that.
	createPod := func(t *testing.T, pod *corev1.Pod) types.UID {
		t.Helper()
		status := pod.Status
		pod.UID = ""
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}
		pod.Status = status
		if err := admin.Status().Update(ctx, pod); err != nil {
			t.Fatalf("setting pod status: %v", err)
		}
		return pod.UID
	}
	reconcile := func(t *testing.T, r *PodMigrationJobReconciler, ns string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: placementJobName}}); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		job := &pmv1alpha1.PodMigrationJob{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: placementJobName}, job); err != nil {
			t.Fatalf("getting job: %v", err)
		}
		return job
	}
	inNamespace := func(t *testing.T, ns string) {
		t.Helper()
		if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatalf("creating namespace: %v", err)
		}
	}

	placementTimeout := func(t *testing.T, ns string, pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob, node *corev1.Node, signal string) {
		t.Helper()
		createNode(t, node)
		pod.Namespace, job.Namespace = ns, ns
		uid := createPod(t, pod)
		job.Status.RestoredPodUID = string(uid)
		createJob(t, job)

		r, recorder := newReconciler()
		concluded := reconcile(t, r, ns)
		if concluded.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Fatalf("phase = %s, want SucceededWithoutRestore", concluded.Status.Phase)
		}
		for _, condType := range []string{"Restored", "Ready"} {
			cond := meta.FindStatusCondition(concluded.Status.Conditions, condType)
			if cond == nil || cond.Reason != ReasonPlacementTimeout || !strings.HasPrefix(cond.Message, signal) {
				t.Errorf("%s = %+v, want reason %s and a message starting %q", condType, cond, ReasonPlacementTimeout, signal)
			}
		}
		events := drain(recorder)
		if n := countPrefixed(events, "Warning "+ReasonPlacementTimeout+" "+signal); n != 2 {
			t.Errorf("events = %q, want 2 %s Warnings (job and pod) starting %q", events, ReasonPlacementTimeout, signal)
		}
		// Clean cold-start recovery: the replacement, which can't be
		// scheduled with the constraints, is gone for its owner to recreate
		// without them.
		if err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: placementPodName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
			t.Errorf("getting the replacement: %v, want it deleted", err)
		}
	}

	unschedulable := []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable,
		LastTransitionTime: metav1.Now()}}
	podWithContainer := func(pod *corev1.Pod) *corev1.Pod {
		pod.Spec.Containers = []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}
		return pod
	}

	t.Run("gke-pod-snapshots placement timeout", func(t *testing.T) {
		pod := podWithContainer(stuckPod())
		pod.Status.Conditions = unschedulable
		inNamespace(t, "incompat-gke")
		placementTimeout(t, "incompat-gke", pod, releasedJob(DefaultRestorePlacementDeadline+30*time.Second), otherTypeNode(), gkePlacementSignal)
	})

	t.Run("criu placement timeout", func(t *testing.T) {
		const ns = "incompat-criu"
		// The CRIU engine reads the source node's facts from the job's
		// PodSnapshot, which the node agent records.
		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(engine.CRIUAPIGroup))
		snap.SetNamespace(ns)
		snap.SetName(placementSnapshot)
		inNamespace(t, ns)
		if err := admin.Create(ctx, snap); err != nil {
			t.Fatalf("creating snapshot: %v", err)
		}
		snap.Object["status"] = map[string]interface{}{"sourceNode": sourceNodeOf(criuSourceFacts)}
		if err := admin.Status().Update(ctx, snap); err != nil {
			t.Fatalf("setting snapshot status: %v", err)
		}
		pod := podWithContainer(criuStuckPod())
		pod.Status.Conditions = unschedulable
		job := releasedJob(DefaultRestorePlacementDeadline + 30*time.Second)
		job.Annotations[engine.AnnotationEngine] = engine.CRIUName
		placementTimeout(t, ns, pod, job, criuNode("old-kernel", olderKernel()),
			"engine=criu requirements="+criuplacement.LabelKernelVersion+": ")
	})

	t.Run("criu restore refused for the CPU model", func(t *testing.T) {
		const ns = "incompat-cpu"
		inNamespace(t, ns)
		pod := podWithContainer(incompatibleNodePod(time.Now().Add(-5 * time.Second)))
		pod.Namespace = ns
		pod.Annotations[util.AnnotationAssignedPMJ] = placementJobName
		uid := createPod(t, pod)
		job := incompatibleNodeJob()
		job.Namespace = ns
		job.Status.RestoredPodUID = string(uid)
		started := metav1.NewTime(time.Now().Add(-time.Minute))
		job.Status.RestoringStartTime = &started
		createJob(t, job)

		r, recorder := newReconciler()
		concluded := reconcile(t, r, ns)
		signal := "engine=criu fact=cpu-model node=node-b sourceNode=node-a: "
		cond := meta.FindStatusCondition(concluded.Status.Conditions, "Restored")
		if concluded.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || cond == nil ||
			cond.Status != metav1.ConditionFalse || cond.Reason != ReasonRestoreIncompatibleNode || !strings.HasPrefix(cond.Message, signal) {
			t.Fatalf("job status = %+v, want SucceededWithoutRestore with Restored False/%s starting %q", concluded.Status, ReasonRestoreIncompatibleNode, signal)
		}
		events := drain(recorder)
		if n := countPrefixed(events, "Warning "+ReasonRestoreIncompatibleNode+" "+signal); n != 2 {
			t.Errorf("events = %q, want 2 %s Warnings (job and pod) starting %q", events, ReasonRestoreIncompatibleNode, signal)
		}

		// Clean cold start: the replacement keeps running cold, through a
		// later reconcile of the concluded job.
		reconcile(t, r, ns)
		got := &corev1.Pod{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: placementPodName}, got); err != nil {
			t.Fatalf("getting the replacement: %v, want it kept", err)
		}
		if got.UID != uid || got.DeletionTimestamp != nil || !isPodReady(got) {
			t.Errorf("replacement = %+v, want the same pod, Ready and not being deleted", got.ObjectMeta)
		}
	})
}
