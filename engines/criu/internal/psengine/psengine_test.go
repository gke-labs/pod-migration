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

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"strings"

	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	for _, gvk := range []struct {
		kind string
	}{{"PodSnapshotStorageConfig"}, {"PodSnapshotPolicy"}, {"PodSnapshotManualTrigger"}, {"PodSnapshot"}, {"PodMigrationJob"}} {
		g := contract.GVKStorageConfig
		g.Kind = gvk.kind
		s.AddKnownTypeWithName(g, &unstructured.Unstructured{})
		g.Kind = gvk.kind + "List"
		s.AddKnownTypeWithName(g, &unstructured.UnstructuredList{})
	}
	s.AddKnownTypeWithName(contract.GVKPodMigrationJob, &unstructured.Unstructured{})
	gpmjList := contract.GVKPodMigrationJob
	gpmjList.Kind = "PodMigrationJobList"
	s.AddKnownTypeWithName(gpmjList, &unstructured.UnstructuredList{})
	return s
}

func pssc(name, bucket, path string) *unstructured.Unstructured {
	u := contract.New(contract.GVKStorageConfig)
	u.SetName(name)
	gcs := map[string]interface{}{"bucket": bucket}
	if path != "" {
		gcs["path"] = path
	}
	u.Object["spec"] = map[string]interface{}{"snapshotStorageConfig": map[string]interface{}{"gcs": gcs}}
	return u
}

func enginePSP(name, ns, scName string) *unstructured.Unstructured {
	u := contract.New(contract.GVKPolicy)
	u.SetName(name)
	u.SetNamespace(ns)
	u.SetLabels(map[string]string{contract.LabelEngine: contract.EngineCRIU})
	u.Object["spec"] = map[string]interface{}{
		"storageConfigName": scName,
		"selector": map[string]interface{}{
			"matchLabels": map[string]interface{}{contract.LabelEnabled: "true"},
		},
		"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
	}
	return u
}

func newClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	pmj := contract.New(contract.GVKPodMigrationJob)
	pmj.SetNamespace("default")
	pmj.SetName("test-pmj")
	unstructured.SetNestedField(pmj.Object, "web-0", "spec", "podName")
	unstructured.SetNestedField(pmj.Object, "ps-web-0-deadbeef", "status", "snapshotName")
	objs = append(objs, pmj)
	return fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(contract.New(contract.GVKPolicy), contract.New(contract.GVKManualTrigger), contract.New(contract.GVKPodSnapshot)).
		Build()
}

func reconcilePSP(t *testing.T, c client.Client, probe StorageProber, ns, name string) *unstructured.Unstructured {
	t.Helper()
	r := &PSPReconciler{Client: c, Probe: probe}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
		t.Fatal(err)
	}
	got := contract.New(contract.GVKPolicy)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
		t.Fatal(err)
	}
	return got
}

func condition(u *unstructured.Unstructured, typ string) *metav1.Condition {
	for _, c := range contract.Conditions(u) {
		if c.Type == typ {
			cc := c
			return &cc
		}
	}
	return nil
}

func TestPSPReconcilerMarksServiceablePolicyReady(t *testing.T) {
	c := newClient(t, pssc("pssc-x", "bkt", "snaps"), enginePSP("psp-criu", "default", "pssc-x"))
	got := reconcilePSP(t, c, nil, "default", "psp-criu")

	ready := condition(got, contract.CondReady)
	if ready == nil || ready.Status != metav1.ConditionTrue {
		t.Fatalf("want Ready=True, got %+v", ready)
	}
	scope := condition(got, contract.CondScopeSingleContainer)
	if scope == nil || scope.Status != metav1.ConditionTrue {
		t.Fatalf("want ScopeSingleContainer=True published, got %+v", scope)
	}
}

func TestPSPReconcilerWithholdsReady(t *testing.T) {
	t.Run("missing PSSC", func(t *testing.T) {
		c := newClient(t, enginePSP("psp-criu", "default", "pssc-missing"))
		got := reconcilePSP(t, c, nil, "default", "psp-criu")
		ready := condition(got, contract.CondReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "StorageConfigUnresolved" {
			t.Fatalf("want Ready=False/StorageConfigUnresolved, got %+v", ready)
		}
	})
	t.Run("unwritable bucket", func(t *testing.T) {
		c := newClient(t, pssc("pssc-x", "bkt", ""), enginePSP("psp-criu", "default", "pssc-x"))
		probe := func(ctx context.Context, bucket, path string) error { return errors.New("403") }
		got := reconcilePSP(t, c, probe, "default", "psp-criu")
		ready := condition(got, contract.CondReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "StorageUnwritable" {
			t.Fatalf("want Ready=False/StorageUnwritable, got %+v", ready)
		}
	})
	t.Run("non-stop policy", func(t *testing.T) {
		p := enginePSP("psp-criu", "default", "pssc-x")
		_ = unstructured.SetNestedField(p.Object, "keepRunning", "spec", "triggerConfig", "postCheckpoint")
		c := newClient(t, pssc("pssc-x", "bkt", ""), p)
		got := reconcilePSP(t, c, nil, "default", "psp-criu")
		ready := condition(got, contract.CondReady)
		if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != "UnsupportedTrigger" {
			t.Fatalf("want Ready=False/UnsupportedTrigger, got %+v", ready)
		}
	})
}

func TestPSPReconcilerIgnoresForeignPolicies(t *testing.T) {
	// A PSP without the engine label (e.g. the GKE addon's own) must never be
	// touched.
	p := enginePSP("psp-addon", "default", "pssc-x")
	p.SetLabels(nil)
	c := newClient(t, pssc("pssc-x", "bkt", ""), p)
	got := reconcilePSP(t, c, nil, "default", "psp-addon")
	if len(contract.Conditions(got)) != 0 {
		t.Fatalf("foreign PSP was touched: %+v", contract.Conditions(got))
	}
}

// --- PSMT watcher -------------------------------------------------------------

func readyEnginePSP(t *testing.T, name, ns, scName string) *unstructured.Unstructured {
	t.Helper()
	p := enginePSP(name, ns, scName)
	if err := contract.SetCondition(p, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: "Serviceable"}); err != nil {
		t.Fatal(err)
	}
	return p
}

func psmtFor(pod string) *unstructured.Unstructured {
	u := contract.New(contract.GVKManualTrigger)
	u.SetName("psmt-" + pod + "-deadbeef")
	u.SetNamespace("default")
	_ = unstructured.SetNestedField(u.Object, pod, "spec", "targetPod")
	return u
}

func sourcePod(name string, containers int, node string) *corev1.Pod {
	p := &corev1.Pod{}
	p.Name = name
	p.Namespace = "default"
	p.UID = types.UID("deadbeef-0000-1111")
	p.Labels = map[string]string{contract.LabelEnabled: "true"}
	for i := 0; i < containers; i++ {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "c" + string(rune('0'+i))})
	}
	p.Spec.NodeName = node
	return p
}

func criuNode(name string, ready bool) *corev1.Node {
	n := &corev1.Node{}
	n.Name = name
	if ready {
		n.Annotations = map[string]string{NodeReadyLabel: "true"}
	}
	return n
}

type fakeCheckpointer struct {
	got []CheckpointRequest
	err error
}

func (f *fakeCheckpointer) Checkpoint(_ context.Context, req CheckpointRequest) error {
	f.got = append(f.got, req)
	return f.err
}

func reconcilePSMT(t *testing.T, c client.Client, cp Checkpointer, name string) {
	t.Helper()
	r := &PSMTReconciler{Client: c, Checkpointer: cp}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}); err != nil {
		t.Fatal(err)
	}
}

func getSnapshot(t *testing.T, c client.Client, name string) *unstructured.Unstructured {
	t.Helper()
	ps := contract.New(contract.GVKPodSnapshot)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, ps); err != nil {
		t.Fatal(err)
	}
	return ps
}

func TestPSMTHappyPathKicksCheckpointer(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	cp := &fakeCheckpointer{}
	reconcilePSMT(t, c, cp, "psmt-web-0-deadbeef")

	// PSMT answered with the snapshot name (the field the orchestrator polls).
	psmt := contract.New(contract.GVKManualTrigger)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "psmt-web-0-deadbeef"}, psmt); err != nil {
		t.Fatal(err)
	}
	want := contract.PodSnapshotName("web-0", "deadbeef-0000-1111")
	if got := contract.TriggerSnapshotCreated(psmt); got != want {
		t.Fatalf("snapshotCreated.name = %q, want %q", got, want)
	}

	// PodSnapshot exists, engine-labeled, not (yet) ready.
	ps := getSnapshot(t, c, want)
	if ps.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
		t.Fatal("PodSnapshot must carry the engine label")
	}
	if contract.IsSnapshotReady(ps) {
		t.Fatal("snapshot must not be ready before the agent reports")
	}

	// Checkpointer got a fully-resolved order.
	if len(cp.got) != 1 {
		t.Fatalf("checkpointer calls: %d", len(cp.got))
	}
	req := cp.got[0]
	if req.Bucket != "bkt" || req.Path != "snaps" || req.NodeName != "node-a" || req.Container != "c0" || req.SnapshotName != want {
		t.Fatalf("bad request: %+v", req)
	}

	// The PodSnapshot spec is a complete work order (CR handoff contract).
	for field, wantV := range map[string]string{
		"sourcePod": "web-0", "sourcePodUID": "deadbeef-0000-1111",
		"container": "c0", "nodeName": "node-a", "bucket": "bkt", "path": "snaps",
	} {
		got, _, _ := unstructured.NestedString(ps.Object, "spec", field)
		if got != wantV {
			t.Fatalf("spec.%s = %q, want %q", field, got, wantV)
		}
	}
}

func TestPSMTHandoffWithoutCheckpointer(t *testing.T) {
	// Production mode: no direct Checkpointer — the PodSnapshot spec is the
	// handoff to the node agent's NodeSnapshotReconciler, and the controller
	// half records an honest Pending condition.
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	reconcilePSMT(t, c, nil, "psmt-web-0-deadbeef")

	ps := getSnapshot(t, c, contract.PodSnapshotName("web-0", "deadbeef-0000-1111"))
	if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b != "bkt" {
		t.Fatalf("work order bucket: %q", b)
	}
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != contract.ReasonPending {
		t.Fatalf("want Checkpoint=Unknown/Pending, got %+v", cond)
	}
	if contract.IsSnapshotReady(ps) {
		t.Fatal("pending snapshot must not be ready")
	}
}

func TestPSMTRefusesMultiContainer(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", ""),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 2, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	cp := &fakeCheckpointer{}
	reconcilePSMT(t, c, cp, "psmt-web-0-deadbeef")

	if len(cp.got) != 0 {
		t.Fatal("multi-container pod must not be checkpointed")
	}
	ps := getSnapshot(t, c, contract.PodSnapshotName("web-0", "deadbeef-0000-1111"))
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonRefused {
		t.Fatalf("want Checkpoint=False/Refused, got %+v", cond)
	}
}

func TestPSMTRefusesNonReadyNode(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", ""),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-b"),
		criuNode("node-b", false),
		psmtFor("web-0"),
	)
	cp := &fakeCheckpointer{}
	reconcilePSMT(t, c, cp, "psmt-web-0-deadbeef")
	if len(cp.got) != 0 {
		t.Fatal("non-criu-ready node must be refused")
	}
	ps := getSnapshot(t, c, contract.PodSnapshotName("web-0", "deadbeef-0000-1111"))
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonRefused {
		t.Fatalf("want Refused, got %+v", cond)
	}
}

func TestPSMTIgnoresPodsWithoutEnginePolicy(t *testing.T) {
	// Ready addon-style PSP (no engine label) matches the pod: the PSMT is
	// the addon's business, not ours — no PodSnapshot, no answer.
	p := enginePSP("psp-addon", "default", "pssc-x")
	p.SetLabels(nil)
	_ = contract.SetCondition(p, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: "AddonReady"})
	c := newClient(t,
		pssc("pssc-x", "bkt", ""),
		p,
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	cp := &fakeCheckpointer{}
	reconcilePSMT(t, c, cp, "psmt-web-0-deadbeef")

	if len(cp.got) != 0 {
		t.Fatal("must not act on foreign PSMTs")
	}
	ps := contract.New(contract.GVKPodSnapshot)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: contract.PodSnapshotName("web-0", "deadbeef-0000-1111")}, ps)
	if err == nil {
		t.Fatal("must not materialize a PodSnapshot for foreign PSMTs")
	}
}

func TestVerifyCRDOwnershipClobbered(t *testing.T) {
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	crd.SetName("podsnapshots.podsnapshot.gke.io")
	crd.SetLabels(map[string]string{
		ComponentLabel:                    ComponentLabelValue,
		"addonmanager.kubernetes.io/mode": "EnsureExists",
	})
	var objs []runtime.Object
	for _, n := range contractCRDs {
		cc := &unstructured.Unstructured{}
		cc.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
		cc.SetName(n)
		cc.SetLabels(map[string]string{ComponentLabel: ComponentLabelValue})
		if n == "podsnapshots.podsnapshot.gke.io" {
			cc.SetLabels(map[string]string{
				ComponentLabel:                    ComponentLabelValue,
				"addonmanager.kubernetes.io/mode": "EnsureExists",
			})
		}
		objs = append(objs, cc)
	}
	c := fake.NewClientBuilder().WithRuntimeObjects(objs...).Build()

	err := VerifyCRDOwnership(context.Background(), c)
	if err == nil {
		t.Fatal("expected error for clobbered CRD, got nil")
	}
	if !strings.Contains(err.Error(), "applied over an existing GKE Pod Snapshots addon CRD") {
		t.Errorf("expected clobbered CRD error, got: %v", err)
	}
}
