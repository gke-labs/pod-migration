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
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// --- restore-injection webhook (pure InjectHolder) -----------------------------

func gatedReplacementPod() *corev1.Pod {
	p := &corev1.Pod{}
	p.Name = "web-1"
	p.Namespace = "default"
	p.Annotations = map[string]string{contract.AnnotationAssignedPMJ: "pmj-web-0-deadbeef"}
	p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: contract.SchedulingGate}}
	p.Spec.Containers = []corev1.Container{{
		Name:    "app",
		Image:   "redis:7",
		Command: []string{"redis-server"},
		Args:    []string{"--appendonly", "yes"},
	}}
	return p
}

func TestInjectHolderRewiresGatedPod(t *testing.T) {
	pod := gatedReplacementPod()
	ok, reason := InjectHolder(pod, "gcr.io/x/holder:v1")
	if !ok {
		t.Fatalf("expected injection, got skip: %s", reason)
	}

	if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != seedInitName {
		t.Fatalf("seed initContainer missing: %+v", pod.Spec.InitContainers)
	}
	app := pod.Spec.Containers[0]
	wantCmd := []string{"/pmhold/holder", "wrap", "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"}
	if strings.Join(app.Command, " ") != strings.Join(wantCmd, " ") {
		t.Fatalf("wrapped command: %v", app.Command)
	}
	if app.Args != nil {
		t.Fatalf("args must be folded into the wrapper: %v", app.Args)
	}
	vols := map[string]bool{}
	for _, v := range pod.Spec.Volumes {
		vols[v.Name] = true
	}
	if !vols[holdVolume] || !vols[infoVolume] {
		t.Fatalf("volumes missing: %v", vols)
	}
	if pod.Annotations[AnnotationInjected] != "true" {
		t.Fatal("injected marker missing")
	}

	// Idempotency.
	if ok, _ := InjectHolder(pod, "gcr.io/x/holder:v1"); ok {
		t.Fatal("second injection must be a no-op")
	}
}

func TestInjectHolderSkips(t *testing.T) {
	t.Run("ungated pod", func(t *testing.T) {
		pod := gatedReplacementPod()
		pod.Spec.SchedulingGates = nil
		if ok, _ := InjectHolder(pod, "img"); ok {
			t.Fatal("ungated pod must not be touched")
		}
	})
	t.Run("no assigned pmj", func(t *testing.T) {
		pod := gatedReplacementPod()
		delete(pod.Annotations, contract.AnnotationAssignedPMJ)
		if ok, _ := InjectHolder(pod, "img"); ok {
			t.Fatal("unassigned pod must not be touched")
		}
	})
	t.Run("no explicit command", func(t *testing.T) {
		pod := gatedReplacementPod()
		pod.Spec.Containers[0].Command = nil
		ok, reason := InjectHolder(pod, "img")
		if ok {
			t.Fatal("no-command pod must be skipped (cold-start fall-through impossible)")
		}
		if !strings.Contains(reason, "no explicit command") {
			t.Fatalf("reason: %s", reason)
		}
	})
	t.Run("multi-container", func(t *testing.T) {
		pod := gatedReplacementPod()
		pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{Name: "sidecar"})
		if ok, _ := InjectHolder(pod, "img"); ok {
			t.Fatal("multi-container pod is outside v1 scope")
		}
	})
}

// --- node restore reconciler ---------------------------------------------------

type fakeRestoreOps struct {
	restored  []RestoreInputs
	failIt    bool
	extracted []string
}

func (f *fakeRestoreOps) Extract(tarPath, dstDir string) error {
	f.extracted = append(f.extracted, tarPath)
	return os.MkdirAll(dstDir, 0o700)
}

func (f *fakeRestoreOps) Restore(_ context.Context, in RestoreInputs) (int, int64, error) {
	if f.failIt {
		return 0, 0, context.DeadlineExceeded
	}
	f.restored = append(f.restored, in)
	return 4242, 250, nil
}

// readySnapshotWithArchive stages a Ready PodSnapshot + a consistent archive
// in the store.
func readySnapshotWithArchive(t *testing.T, store *memStore) *memStore {
	t.Helper()
	specJSON := []byte(`{"ociVersion":"1.1.0","annotations":{"io.kubernetes.cri.sandbox-id":"src-sb"}}`)
	tar := []byte("TARBYTES")
	specSHA, _, _ := SHA256Reader(strings.NewReader(string(specJSON)))
	tarSHA, _, _ := SHA256Reader(strings.NewReader(string(tar)))
	mf := Manifest{
		Snapshot: "ps-web-0-deadbeef", SourcePod: "web-0", SourcePodUID: "src-uid",
		Container: "app", Node: "node-a", ImageName: "redis:7",
		TarSHA256: tarSHA, TarBytes: int64(len(tar)), SpecSHA256: specSHA, EngineVersion: engineVersion,
	}
	mfJSON, _ := json.Marshal(mf)
	prefix := "snaps/ps-web-0-deadbeef/"
	store.objects["bkt/"+prefix+"manifest.json"] = mfJSON
	store.objects["bkt/"+prefix+"spec.json"] = specJSON
	store.objects["bkt/"+prefix+"checkpoint.tar"] = tar
	return store
}

func replacementPodOnNode(psName string) *corev1.Pod {
	p := &corev1.Pod{}
	p.Name = "web-1"
	p.Namespace = "default"
	p.UID = types.UID("tgt-uid")
	p.Annotations = map[string]string{
		contract.AnnotationPSName:      psName,
		AnnotationInjected:             "true",
		contract.AnnotationAssignedPMJ: "test-pmj",
	}
	p.Spec.NodeName = "node-a"
	p.Spec.Containers = []corev1.Container{{Name: "app"}}
	p.Status.Phase = corev1.PodRunning
	return p
}

func TestNodeRestoreHappyPath(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded}); err != nil {
		t.Fatal(err)
	}
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	store := readySnapshotWithArchive(t, newMemStore())
	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}

	if len(ops.restored) != 1 {
		t.Fatalf("restore calls: %d", len(ops.restored))
	}
	in := ops.restored[0]
	if in.TargetPod != "web-1" || in.SourcePodUID != "src-uid" || in.TargetPodUID != "tgt-uid" ||
		in.ImageName != "redis:7" || in.ContainerName != "app" || filepath.Base(in.CheckpointDir) != "extracted" {
		t.Fatalf("restore inputs: %+v", in)
	}

	got := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got); err != nil {
		t.Fatal(err)
	}
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
		t.Fatalf("restore status: %q", got.Annotations[contract.AnnotationRestoreStatus])
	}
}

func TestNodeRestoreFailureStampsFailed(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: &fakeRestoreOps{failIt: true},
		Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	// failed -> holder wrapper falls through to cold start
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatalf("restore status: %q", got.Annotations[contract.AnnotationRestoreStatus])
	}
}

func TestNodeRestoreRefusesUnreadySnapshot(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef") // no Ready condition
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops,
		Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.restored) != 0 {
		t.Fatal("must not restore from an unverified archive")
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatal("unready snapshot must stamp failed (cold-start fall-through)")
	}
}

func TestNodeRestoreCorruptTarDetected(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	store := readySnapshotWithArchive(t, newMemStore())
	store.objects["bkt/snaps/ps-web-0-deadbeef/checkpoint.tar"] = []byte("CORRUPTED")
	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.restored) != 0 {
		t.Fatal("sha mismatch must block the restore")
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatal("corrupt archive must stamp failed")
	}
}

func TestNodeRestoreRefusesUnassignedOrMismatchedPMJ(t *testing.T) {
	ctx := context.Background()
	psName := "ps-1"

	ps := contract.New(contract.GVKPodSnapshot)
	ps.SetNamespace("default")
	ps.SetName(psName)
	contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	unstructured.SetNestedField(ps.Object, "gs://b/ps", "spec", "path")
	unstructured.SetNestedField(ps.Object, "src-pod", "spec", "sourcePodName")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "tgt-pod",
			Annotations: map[string]string{
				contract.AnnotationPSName: psName,
				AnnotationInjected:        "true",
				// Missing PMJ annotation!
			},
		},
		Spec:   corev1.PodSpec{NodeName: "node1"},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	r := &NodeRestoreReconciler{
		Client:   fake.NewClientBuilder().WithRuntimeObjects(ps, pod).Build(),
		NodeName: "node1",
	}

	_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "tgt-pod"}})
	if err != nil {
		t.Fatalf("Reconcile should return nil (update status on error), got %v", err)
	}

	updated := &corev1.Pod{}
	r.Get(ctx, types.NamespacedName{Namespace: "default", Name: "tgt-pod"}, updated)
	if updated.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Errorf("expected RestoreStatusFailed due to missing PMJ annotation")
	}
}
