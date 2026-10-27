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

package psengine

// Contract conformance: replays the orchestrator's entire engine handshake
// against the real engine reconcilers, using faithful copies of the predicates
// its code applies (cited below). If either side drifts, this file goes red.
//
// Faithfulness note: the predicates below are transcribed from the
// orchestrator's source (gke-labs/pod-migration, controller/), not linked.
// The transcription is the pinned contract; bumping it against a new
// orchestrator commit is the drift check.

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// orchestratorAcceptsPSP is a faithful transcription of
// findLatestReadyManualStopPSP's per-policy checks
// (internal/webhook/eviction_webhook.go:236-325): manual trigger, selector
// match, status Ready=True, postCheckpoint=stop.
func orchestratorAcceptsPSP(psp *unstructured.Unstructured, podLabels map[string]string) bool {
	triggerType, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "type")
	if err != nil || !found || triggerType != "manual" {
		return false
	}
	selectorMap, found, err := unstructured.NestedMap(psp.Object, "spec", "selector")
	if err != nil || !found {
		return false
	}
	jsonBytes, err := json.Marshal(selectorMap)
	if err != nil {
		return false
	}
	var labelSelector metav1.LabelSelector
	if err := json.Unmarshal(jsonBytes, &labelSelector); err != nil {
		return false
	}
	selector, err := metav1.LabelSelectorAsSelector(&labelSelector)
	if err != nil || !selector.Matches(labels.Set(podLabels)) {
		return false
	}
	conditions, found, err := unstructured.NestedSlice(psp.Object, "status", "conditions")
	if err != nil || !found {
		return false
	}
	isReady := false
	for _, condVal := range conditions {
		cond, ok := condVal.(map[string]interface{})
		if !ok {
			continue
		}
		if cond["type"] == "Ready" && cond["status"] == "True" {
			isReady = true
			break
		}
	}
	if !isReady {
		return false
	}
	postCheckpoint, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "postCheckpoint")
	return err == nil && found && postCheckpoint == "stop"
}

// orchestratorSeesSnapshotReady is a faithful transcription of the PMJ
// controller's readiness poll (internal/controller/
// podmigrationjob_controller.go:294-324): loosely-typed condition maps,
// Ready=True OR Checkpoint=True/Succeeded.
func orchestratorSeesSnapshotReady(ps *unstructured.Unstructured) bool {
	snapStatus, ok := ps.Object["status"].(map[string]interface{})
	if !ok {
		return false
	}
	conditions, _ := snapStatus["conditions"].([]interface{})
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		if cond["type"] == "Ready" && cond["status"] == "True" {
			return true
		}
		if cond["type"] == "Checkpoint" && cond["status"] == "True" && cond["reason"] == "Succeeded" {
			return true
		}
	}
	return false
}

// TestConformanceFullHandshake drives the complete engine handshake:
// PSP Ready (webhook predicate) -> PSMT answered (snapshotCreated.name) ->
// node pipeline -> snapshot ready (PMJ predicate) -> ps-name restore ->
// restored annotation.
func TestConformanceFullHandshake(t *testing.T) {
	ctx := context.Background()
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		enginePSP("psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
	)

	// Phase 1 — PSP reconciler must produce a policy the eviction webhook
	// accepts for an opted-in pod (this is what makes it hold the eviction
	// and create a PMJ at all).
	psp := reconcilePSP(t, c, nil, "default", "psp-criu")
	if !orchestratorAcceptsPSP(psp, map[string]string{contract.LabelEnabled: "true"}) {
		t.Fatalf("eviction webhook predicate rejects our Ready PSP: %+v", contract.Conditions(psp))
	}
	if orchestratorAcceptsPSP(psp, map[string]string{}) {
		t.Fatal("predicate must not match unlabeled pods")
	}

	// Phase 2 — the orchestrator creates a PSMT exactly like ensureTrigger
	// (podmigrationjob_controller.go:504-558: name psmt-<pod>-<uid8>,
	// spec.targetPod).
	psmt := psmtFor("web-0")
	if err := c.Create(ctx, psmt); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, "psmt-web-0-deadbeef")

	got := contract.New(contract.GVKManualTrigger)
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "psmt-web-0-deadbeef"}, got); err != nil {
		t.Fatal(err)
	}
	snapName := contract.TriggerSnapshotCreated(got)
	if snapName == "" {
		t.Fatal("PSMT status.snapshotCreated.name not answered (the PMJ controller polls this at 1s, :247)")
	}

	// Not ready yet — Pending must NOT satisfy the PMJ predicate.
	ps := getSnapshot(t, c, snapName)
	if orchestratorSeesSnapshotReady(ps) {
		t.Fatal("pending snapshot must not read as ready to the orchestrator")
	}

	// Phase 3 — node pipeline executes the work order.
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CPUFamily: "fam-A"}
	if _, err := nr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: snapName}}); err != nil {
		t.Fatal(err)
	}
	ps = getSnapshot(t, c, snapName)
	if !orchestratorSeesSnapshotReady(ps) {
		t.Fatalf("PMJ readiness predicate rejects our Ready snapshot: %+v", contract.Conditions(ps))
	}
	if len(ops.stopped) != 1 {
		t.Fatal("postCheckpoint:stop not honored")
	}

	// Phase 4 — the pod gate controller stamps ps-name on the replacement
	// pod and removes the gate (pod_gate_controller.go:121-130); our
	// injected pod then restores on its node.
	repl := replacementPodOnNode(snapName)
	repl.Annotations[contract.AnnotationPSName] = snapName
	if err := c.Create(ctx, repl); err != nil {
		t.Fatal(err)
	}
	rops := &fakeRestoreOps{}
	rr := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: rops, Store: store, WorkDir: t.TempDir(), CPUFamily: "fam-A"}
	if _, err := rr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	final := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "web-1"}, final); err != nil {
		t.Fatal(err)
	}
	if final.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
		t.Fatalf("restore handshake incomplete: %q", final.Annotations[contract.AnnotationRestoreStatus])
	}
	if len(rops.restored) != 1 || rops.restored[0].SourcePodUID != "deadbeef-0000-1111" {
		t.Fatalf("restore inputs: %+v", rops.restored)
	}
}

// TestConformanceCPUFamilyGuard: cross-family archives are refused before
// runc, and the holder falls through cold.
func TestConformanceCPUFamilyGuard(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)

	store := newMemStore()
	// Archive recorded on a different CPU family.
	specJSON := []byte(`{"ociVersion":"1.1.0"}`)
	tar := []byte("TARBYTES")
	specSHA, _, _ := SHA256Reader(strReader(specJSON))
	tarSHA, _, _ := SHA256Reader(strReader(tar))
	mfJSON, _ := json.Marshal(Manifest{Snapshot: "ps-web-0-deadbeef", SourcePodUID: "src-uid",
		TarSHA256: tarSHA, SpecSHA256: specSHA, CPUFamily: "Intel C3 Sapphire Rapids", EngineVersion: engineVersion})
	store.objects["bkt/snaps/ps-web-0-deadbeef/manifest.json"] = mfJSON
	store.objects["bkt/snaps/ps-web-0-deadbeef/spec.json"] = specJSON
	store.objects["bkt/snaps/ps-web-0-deadbeef/checkpoint.tar"] = tar

	rops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: rops, Store: store,
		WorkDir: t.TempDir(), CPUFamily: "AMD EPYC"}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(rops.restored) != 0 {
		t.Fatal("cross-family restore must be refused")
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatal("cross-family refusal must stamp failed (cold-start fall-through)")
	}
}

func TestPreflightCRDOwnership(t *testing.T) {
	ours := func(name string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("apiextensions.k8s.io/v1")
		u.SetKind("CustomResourceDefinition")
		u.SetName(name)
		u.SetLabels(map[string]string{ComponentLabel: ComponentLabelValue})
		return u
	}
	t.Run("all ours: pass", func(t *testing.T) {
		var objs []client.Object
		for _, n := range contractCRDs {
			objs = append(objs, ours(n))
		}
		if err := VerifyCRDOwnership(context.Background(), newCRDClient(t, objs...)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("missing: fail with install hint", func(t *testing.T) {
		err := VerifyCRDOwnership(context.Background(), newCRDClient(t))
		if err == nil {
			t.Fatal("missing CRDs must fail preflight")
		}
	})
	t.Run("foreign-owned: fail loudly", func(t *testing.T) {
		var objs []client.Object
		for i, n := range contractCRDs {
			u := ours(n)
			if i == 2 {
				u.SetLabels(nil) // the addon's CRD
			}
			objs = append(objs, u)
		}
		err := VerifyCRDOwnership(context.Background(), newCRDClient(t, objs...))
		if err == nil {
			t.Fatal("foreign CRD must fail preflight")
		}
	})
	t.Run("GKE addon markers named in the refusal", func(t *testing.T) {
		var objs []client.Object
		for i, n := range contractCRDs {
			u := ours(n)
			if i == 0 {
				// The managed addon's CRDs carry addon-manager labels.
				u.SetLabels(map[string]string{"addonmanager.kubernetes.io/mode": "Reconcile"})
			}
			objs = append(objs, u)
		}
		err := VerifyCRDOwnership(context.Background(), newCRDClient(t, objs...))
		if err == nil {
			t.Fatal("addon-owned CRD must fail preflight")
		}
		if !strings.Contains(err.Error(), "GKE Pod Snapshots addon") || !strings.Contains(err.Error(), "addonmanager.kubernetes.io/mode") {
			t.Fatalf("refusal must name the addon markers (actionable error), got: %v", err)
		}
	})
	t.Run("unknown foreign owner reported honestly", func(t *testing.T) {
		var objs []client.Object
		for i, n := range contractCRDs {
			u := ours(n)
			if i == 1 {
				u.SetLabels(nil) // no markers at all
			}
			objs = append(objs, u)
		}
		err := VerifyCRDOwnership(context.Background(), newCRDClient(t, objs...))
		if err == nil || !strings.Contains(err.Error(), "owner unknown") {
			t.Fatalf("absent markers must be reported as unknown, never invented: %v", err)
		}
	})
}

func strReader(b []byte) io.Reader { return strings.NewReader(string(b)) }

// newCRDClient builds a fake client that knows the apiextensions CRD kind as
// unstructured (preflight reads CRDs unstructured).
func newCRDClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	s := testScheme(t)
	crdGVK := schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}
	s.AddKnownTypeWithName(crdGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(crdGVK.GroupVersion().WithKind("CustomResourceDefinitionList"), &unstructured.UnstructuredList{})
	return fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
}
