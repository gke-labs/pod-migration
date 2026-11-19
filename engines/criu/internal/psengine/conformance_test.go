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
	if len(ops.frozen) != 1 || len(ops.unfrozen) != 0 {
		t.Fatalf("fencing:frozen not honored: frozen=%v unfrozen=%v", ops.frozen, ops.unfrozen)
	}
	if !contract.IsSourceFrozen(ps) {
		t.Fatal("ready snapshot must record SourceFrozen=True")
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

	// Phase 5 — migrating an already-restored pod (#121) must be refused
	// by policy so the orchestrator falls back to a clean cold start
	// instead of checkpointing the holder wrapper.
	final.Labels = map[string]string{contract.LabelEnabled: "true"}
	if err := c.Update(ctx, final); err != nil {
		t.Fatal(err)
	}
	psmt2 := contract.New(contract.GVKManualTrigger)
	psmt2.SetName("psmt-web-1-repeat")
	psmt2.SetNamespace("default")
	psmt2.Object["spec"] = map[string]interface{}{"targetPod": "web-1"}
	if err := c.Create(ctx, psmt2); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, "psmt-web-1-repeat")
	snapName2 := contract.PodSnapshotName("web-1", string(final.UID))
	ps2 := getSnapshot(t, c, snapName2)
	cond2 := condition(ps2, contract.CondCheckpoint)
	if cond2 == nil || cond2.Status != metav1.ConditionFalse || cond2.Reason != contract.ReasonRefused {
		t.Fatalf("want Checkpoint=False/Refused for already-restored pod, got %+v", cond2)
	}
	if hasBucket(ps2) {
		t.Fatal("refused repeat migration must not have bucket/path work order")
	}
	if len(ops.frozen) != 1 {
		t.Fatalf("refused repeat migration must not invoke node freeze: got %d", len(ops.frozen))
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
		for _, n := range contract.CRDNames(contract.Group) {
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
		for i, n := range contract.CRDNames(contract.Group) {
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
		for i, n := range contract.CRDNames(contract.Group) {
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
		for i, n := range contract.CRDNames(contract.Group) {
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

// TestConformanceInjectedUploadFailureThawsSource verifies the C4 fencing acceptance
// invariant (#148): an injected upload failure leaves the source thawed and serving.
func TestConformanceInjectedUploadFailureThawsSource(t *testing.T) {
	ctx := context.Background()
	ps := workOrderSnapshot("ps-upload-fail")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &failingStore{memStore: newMemStore(), failSubstr: "checkpoint.tar"}
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CPUFamily: "fam-A"}

	if _, err := nr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-upload-fail"}}); err != nil {
		t.Fatal(err)
	}
	// The source was frozen before checkpoint and thawed on upload failure.
	if len(ops.frozen) != 1 || len(ops.unfrozen) != 1 {
		t.Fatalf("expected 1 freeze and 1 unfreeze on upload failure: frozen=%v unfrozen=%v", ops.frozen, ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-upload-fail")
	if contract.IsSnapshotReady(got) {
		t.Fatal("failed snapshot must not be ready")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonFailed {
		t.Fatalf("want Failed outcome, got %+v", cond)
	}
	if contract.IsSourceFrozen(got) {
		t.Fatal("failed snapshot must not record source as frozen")
	}
}

// TestConformanceRestartAfterFreezeThawsOnCancel verifies that if the agent restarts
// after freezing a source container, and the work order is cancelled, the restarted
// agent inspects the SourceFrozen record and thaws the source container before
// reporting Cancelled (#148).
func TestConformanceRestartAfterFreezeThawsOnCancel(t *testing.T) {
	ctx := context.Background()
	ps := workOrderSnapshot("ps-restart-cancel")
	_ = unstructured.SetNestedField(ps.Object, "psmt-web-0", "spec", "triggerName")
	// Simulate the recorded SourceFrozen condition from before the agent restart.
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen before restart")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CPUFamily: "fam-A"}

	// Trigger is absent in client, so cancelCheck reports workOrderCancelled.
	if _, err := nr.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-restart-cancel"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("restarted agent must thaw source on cancel: unfrozen=%v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-restart-cancel")
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonCancelled {
		t.Fatalf("want Cancelled outcome, got %+v", cond)
	}
	if contract.IsSourceFrozen(got) {
		t.Fatal("cancelled snapshot must not record source as frozen")
	}
}

// TestConformanceStartupSweepThawsUnreadySnapshots verifies that on startup,
// the agent sweeps PodSnapshots targeting its node that have SourceFrozen=True,
// thawing those that are not Ready (e.g. Failed, Cancelled, or unfinished),
// while leaving Ready snapshots frozen for eviction (#106, #148).
func TestConformanceStartupSweepThawsUnreadySnapshots(t *testing.T) {
	ctx := context.Background()

	// 1. Snapshot with Failed outcome and SourceFrozen=True (e.g. agent crashed before thawing)
	psFailed := workOrderSnapshot("ps-failed")
	_ = contract.SetSourceFrozen(psFailed, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(psFailed, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionFalse,
		Reason: contract.ReasonFailed,
	})

	// 2. Snapshot with Cancelled outcome and SourceFrozen=True
	psCancelled := workOrderSnapshot("ps-cancelled")
	_ = contract.SetSourceFrozen(psCancelled, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(psCancelled, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionFalse,
		Reason: contract.ReasonCancelled,
	})

	// 3. Snapshot with no outcome yet (interrupted mid-pipeline) and SourceFrozen=True
	psInProgress := workOrderSnapshot("ps-in-progress")
	_ = contract.SetSourceFrozen(psInProgress, true, contract.ReasonFrozen, "source frozen")

	// 4. Snapshot with Ready=True and SourceFrozen=True: must NOT be thawed (kept frozen for eviction)
	psReady := workOrderSnapshot("ps-ready")
	_ = contract.SetSourceFrozen(psReady, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(psReady, metav1.Condition{
		Type:   contract.CondReady,
		Status: metav1.ConditionTrue,
		Reason: contract.ReasonSucceeded,
	})

	// 5. Snapshot on a different node: must NOT be touched
	psOtherNode := workOrderSnapshot("ps-other-node")
	_ = unstructured.SetNestedField(psOtherNode.Object, "node-b", "spec", "nodeName")
	_ = contract.SetSourceFrozen(psOtherNode, true, contract.ReasonFrozen, "source frozen")

	// 6. Snapshot with CheckpointTimedOut: must NOT be thawed (keeps fence for eviction per C9)
	psTimedOut := workOrderSnapshot("ps-timed-out")
	_ = contract.SetSourceFrozen(psTimedOut, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(psTimedOut, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionUnknown,
		Reason: contract.ReasonCheckpointTimedOut,
	})

	c := newClient(t, psFailed, psCancelled, psInProgress, psReady, psOtherNode, psTimedOut)
	ops := &fakeNodeOps{t: t}
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CPUFamily: "fam-A"}

	if err := nr.SweepFrozenSources(ctx); err != nil {
		t.Fatalf("SweepFrozenSources failed: %v", err)
	}

	if len(ops.unfrozen) != 3 {
		t.Fatalf("expected 3 unready snapshots to be unfrozen, got %d: %v", len(ops.unfrozen), ops.unfrozen)
	}

	// Verify ps-failed, ps-cancelled, and ps-in-progress now have SourceFrozen=False
	for _, name := range []string{"ps-failed", "ps-cancelled", "ps-in-progress"} {
		snap := getSnapshot(t, c, name)
		if contract.IsSourceFrozen(snap) {
			t.Fatalf("snapshot %s must have SourceFrozen=False after startup sweep", name)
		}
	}

	// Verify ps-ready still has SourceFrozen=True
	readySnap := getSnapshot(t, c, "ps-ready")
	if !contract.IsSourceFrozen(readySnap) {
		t.Fatal("Ready snapshot must remain SourceFrozen=True after startup sweep")
	}

	// Verify ps-timed-out still has SourceFrozen=True
	timedOutSnap := getSnapshot(t, c, "ps-timed-out")
	if !contract.IsSourceFrozen(timedOutSnap) {
		t.Fatal("CheckpointTimedOut snapshot must remain SourceFrozen=True after startup sweep")
	}

	// Verify ps-other-node still has SourceFrozen=True
	otherSnap := getSnapshot(t, c, "ps-other-node")
	if !contract.IsSourceFrozen(otherSnap) {
		t.Fatal("other node snapshot must not be touched by startup sweep")
	}
}

// TestConformanceSweepSkipsInflightCheckpoint verifies that if a normal Reconcile
// is currently in-flight for a snapshot, SweepFrozenSources does not race it and
// does not thaw the source container mid-checkpoint (#106, #148).
func TestConformanceSweepSkipsInflightCheckpoint(t *testing.T) {
	ctx := context.Background()
	ps := workOrderSnapshot("ps-busy")
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen")

	c := newClient(t, ps)
	ops := &fakeNodeOps{t: t}
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CPUFamily: "fam-A"}

	// Simulate an active in-flight checkpoint reconcile for ps-busy
	key := types.NamespacedName{Namespace: "default", Name: "ps-busy"}
	nr.inflight.Store(key.String(), true)

	if err := nr.SweepFrozenSources(ctx); err != nil {
		t.Fatalf("SweepFrozenSources failed: %v", err)
	}

	if len(ops.unfrozen) != 0 {
		t.Fatalf("startup sweep must not thaw a snapshot that is currently in-flight: unfrozen=%v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-busy")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("in-flight snapshot must remain SourceFrozen=True")
	}
}

// TestConformanceCheckpointTimedOutKeepsSourceFrozen verifies that snapshots with
// CheckpointTimedOut are kept frozen by both the startup sweep and Reconcile (clause C9).
func TestConformanceCheckpointTimedOutKeepsSourceFrozen(t *testing.T) {
	ctx := context.Background()
	ps := workOrderSnapshot("ps-timed-out")
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(ps, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionUnknown,
		Reason: contract.ReasonCheckpointTimedOut,
	})

	c := newClient(t, ps)
	ops := &fakeNodeOps{t: t}
	nr := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CPUFamily: "fam-A"}

	// 1. Sweep skips CheckpointTimedOut
	if err := nr.SweepFrozenSources(ctx); err != nil {
		t.Fatalf("SweepFrozenSources failed: %v", err)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatalf("startup sweep must not thaw CheckpointTimedOut snapshot: unfrozen=%v", ops.unfrozen)
	}

	// 2. Reconcile skips CheckpointTimedOut
	key := types.NamespacedName{Namespace: "default", Name: "ps-timed-out"}
	if _, err := nr.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatalf("Reconcile must not thaw CheckpointTimedOut snapshot: unfrozen=%v", ops.unfrozen)
	}

	got := getSnapshot(t, c, "ps-timed-out")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("CheckpointTimedOut snapshot must remain SourceFrozen=True")
	}
}

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
