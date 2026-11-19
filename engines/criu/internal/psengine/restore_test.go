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

	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	k8syaml "sigs.k8s.io/yaml"

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
	wantCmd := []string{"/pmhold/holder", "wrap", "--ps-name-annotation=" + contract.AnnotationPSName, "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"}
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
	t.Run("hostNetwork", func(t *testing.T) {
		pod := gatedReplacementPod()
		pod.Spec.HostNetwork = true
		if ok, reason := InjectHolder(pod, "img"); ok || !strings.Contains(reason, "hostNetwork") {
			t.Fatalf("expected hostNetwork refusal, got ok=%v reason=%q", ok, reason)
		}
	})
	t.Run("non-runc runtimeClassName (gvisor)", func(t *testing.T) {
		pod := gatedReplacementPod()
		rc := "gvisor"
		pod.Spec.RuntimeClassName = &rc
		if ok, reason := InjectHolder(pod, "img"); ok || !strings.Contains(reason, "runtimeClassName") {
			t.Fatalf("expected gvisor runtimeClassName refusal, got ok=%v reason=%q", ok, reason)
		}
	})
	t.Run("explicit runc runtimeClassName is injected", func(t *testing.T) {
		pod := gatedReplacementPod()
		rc := "runc"
		pod.Spec.RuntimeClassName = &rc
		if ok, reason := InjectHolder(pod, "img"); !ok {
			t.Fatalf("expected runc pod injection, got skip: %s", reason)
		}
	})
	t.Run("sidecar initContainer", func(t *testing.T) {
		pod := gatedReplacementPod()
		always := corev1.ContainerRestartPolicyAlways
		pod.Spec.InitContainers = []corev1.Container{{Name: "mesh", RestartPolicy: &always}}
		if ok, reason := InjectHolder(pod, "img"); ok || !strings.Contains(reason, "sidecar") {
			t.Fatalf("expected sidecar initContainer refusal, got ok=%v reason=%q", ok, reason)
		}
	})
}

// --- node restore reconciler ---------------------------------------------------

type fakeRestoreOps struct {
	restored       []RestoreInputs
	cleaned        []RestoreInputs
	signaled       []RestoreInputs
	signalSig      int
	signalCount    int
	signalErr      error
	siblingRunning bool
	failIt         bool
	failCleanup    bool
	extracted      []string
	holderSignals  []int
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

func (f *fakeRestoreOps) Cleanup(_ context.Context, in RestoreInputs) error {
	if f.failCleanup {
		return errors.New("injected cleanup failure")
	}
	f.cleaned = append(f.cleaned, in)
	return nil
}

func (f *fakeRestoreOps) Signal(_ context.Context, in RestoreInputs, sig int) error {
	f.signaled = append(f.signaled, in)
	f.signalSig = sig
	f.signalCount++
	if f.signalErr != nil {
		return f.signalErr
	}
	return nil
}

func (f *fakeRestoreOps) IsSiblingRunning(_ context.Context, in RestoreInputs) (bool, error) {
	return f.siblingRunning, nil
}

func (f *fakeRestoreOps) SignalHolder(_ context.Context, in RestoreInputs, sig int) error {
	f.holderSignals = append(f.holderSignals, sig)
	return nil
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

func TestNodeRestore_TearsDownOnPodDeletion(t *testing.T) {
	// 1. Pod deletion starts, sibling container is still running and grace period active
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	future := metav1.NewTime(time.Now().Add(30 * time.Second))
	pod.DeletionTimestamp = &future
	pod.Finalizers = []string{"test.io/keep"}
	gracePeriod := int64(30)
	pod.DeletionGracePeriodSeconds = &gracePeriod

	c := newClient(t, pod)
	ops := &fakeRestoreOps{siblingRunning: true}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter != 1*time.Second {
		t.Errorf("expected RequeueAfter 1s, got %v", res.RequeueAfter)
	}
	if ops.signalSig != 15 {
		t.Errorf("expected SIGTERM (15), got %d", ops.signalSig)
	}
	if ops.signalCount != 1 {
		t.Errorf("expected signalCount 1, got %d", ops.signalCount)
	}
	if len(ops.cleaned) != 0 {
		t.Fatalf("cleanup ran before sibling exited or grace period expired")
	}

	// Reconciling again while still running should NOT re-send SIGTERM
	res2, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile 2 failed: %v", err)
	}
	if res2.RequeueAfter != 1*time.Second {
		t.Errorf("expected RequeueAfter 1s, got %v", res2.RequeueAfter)
	}
	if ops.signalCount != 1 {
		t.Errorf("SIGTERM was re-sent on subsequent requeue; expected count 1, got %d", ops.signalCount)
	}

	// 2. Sibling container exits -> teardown proceeds immediately without waiting for grace period
	ops.siblingRunning = false

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(ops.cleaned) != 1 {
		t.Fatalf("expected 1 cleanup call, got %d", len(ops.cleaned))
	}
	if ops.cleaned[0].SnapshotName != "ps-web-0-deadbeef" || ops.cleaned[0].TargetPod != "web-1" {
		t.Errorf("cleanup inputs: %+v", ops.cleaned[0])
	}
	if len(ops.holderSignals) != 1 || ops.holderSignals[0] != 2 {
		t.Errorf("expected holder to be signaled with SIGINT (2), got %v", ops.holderSignals)
	}
}

func TestNodeRestore_TearsDownOnPodDeletion_AfterCompleteRestore(t *testing.T) {
	// A successfully migrated pod has */ps-name cleared to "" by CompleteRestore,
	// and retains the durable AnnotationSnapshot and/or restore-completed annotation.
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationPSName] = "" // cleared by CompleteRestore
	pod.Annotations[contract.AnnotationSnapshot] = "ps-web-0-deadbeef"
	pod.Annotations["pod-migration.gke.io/restore-completed"] = "ps-web-0-deadbeef"
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Finalizers = []string{"test.io/keep"}

	ops := &fakeRestoreOps{siblingRunning: false}
	c := newClient(t, pod)
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(ops.cleaned) != 1 {
		t.Fatalf("expected 1 cleanup call for completed restore pod, got %d", len(ops.cleaned))
	}
	if ops.cleaned[0].SnapshotName != "ps-web-0-deadbeef" || ops.cleaned[0].TargetPod != "web-1" {
		t.Errorf("cleanup inputs: %+v", ops.cleaned[0])
	}
	if len(ops.holderSignals) != 1 || ops.holderSignals[0] != 2 {
		t.Errorf("expected holder to be signaled with SIGINT (2), got %v", ops.holderSignals)
	}
}

func TestNodeRestore_SignalTransientFailureRetries(t *testing.T) {
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	future := metav1.NewTime(time.Now().Add(30 * time.Second))
	pod.DeletionTimestamp = &future
	pod.Finalizers = []string{"test.io/keep"}
	gracePeriod := int64(30)
	pod.DeletionGracePeriodSeconds = &gracePeriod

	c := newClient(t, pod)
	ops := &fakeRestoreOps{siblingRunning: true, signalErr: errors.New("transient signal error")}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	// 1st reconcile: Signal fails
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter != 1*time.Second {
		t.Errorf("expected RequeueAfter 1s, got %v", res.RequeueAfter)
	}
	if ops.signalCount != 1 {
		t.Errorf("expected signalCount 1, got %d", ops.signalCount)
	}

	// 2nd reconcile: transient error clears, Signal should be retried!
	ops.signalErr = nil
	res2, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile 2 failed: %v", err)
	}
	if res2.RequeueAfter != 1*time.Second {
		t.Errorf("expected RequeueAfter 1s, got %v", res2.RequeueAfter)
	}
	if ops.signalCount != 2 {
		t.Errorf("expected signalCount 2 after transient failure retry, got %d", ops.signalCount)
	}

	// 3rd reconcile: Signal succeeded previously, should NOT re-signal
	res3, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile 3 failed: %v", err)
	}
	if res3.RequeueAfter != 1*time.Second {
		t.Errorf("expected RequeueAfter 1s, got %v", res3.RequeueAfter)
	}
	if ops.signalCount != 2 {
		t.Errorf("expected signalCount still 2 after successful signal, got %d", ops.signalCount)
	}
}

func TestNodeRestore_DeadlinePassedTearsDownEvenIfSiblingRunning(t *testing.T) {
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	past := metav1.NewTime(time.Now().Add(-5 * time.Second))
	pod.DeletionTimestamp = &past
	pod.Finalizers = []string{"test.io/keep"}
	gracePeriod := int64(30)
	pod.DeletionGracePeriodSeconds = &gracePeriod

	c := newClient(t, pod)
	ops := &fakeRestoreOps{siblingRunning: true}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected no requeue when deadline expired, got %v", res.RequeueAfter)
	}
	if len(ops.cleaned) != 1 {
		t.Fatalf("expected cleanup to execute when grace deadline expired even though sibling running; got %d calls", len(ops.cleaned))
	}
}

func TestNodeRestore_NilOrZeroGracePeriodTearsDownImmediately(t *testing.T) {
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	future := metav1.NewTime(time.Now().Add(30 * time.Second))
	pod.DeletionTimestamp = &future
	pod.Finalizers = []string{"test.io/keep"}
	pod.DeletionGracePeriodSeconds = nil // force delete

	c := newClient(t, pod)
	ops := &fakeRestoreOps{siblingRunning: true}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("expected no requeue for nil grace period, got %v", res.RequeueAfter)
	}
	if len(ops.cleaned) != 1 {
		t.Fatalf("expected immediate cleanup for nil grace period; got %d calls", len(ops.cleaned))
	}
}

func TestNodeRestore_CleanupFailureRetries(t *testing.T) {
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	pod.Finalizers = []string{"test.io/keep"}

	c := newClient(t, pod)
	ops := &fakeRestoreOps{failCleanup: true, siblingRunning: false}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if err == nil {
		t.Fatal("expected reconcile to fail and retry when Cleanup fails, got nil")
	}
	if !strings.Contains(err.Error(), "injected cleanup failure") {
		t.Errorf("expected injected cleanup failure error, got: %v", err)
	}
}

func TestNodeRestore_LivePodDoesNotTriggerCleanup(t *testing.T) {
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Annotations[contract.AnnotationRestoreStatus] = contract.RestoreStatusRestored
	pod.DeletionTimestamp = nil // live running pod

	c := newClient(t, pod)
	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, WorkDir: t.TempDir()}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(ops.cleaned) != 0 {
		t.Fatalf("cleanup was called on a live running pod! cleaned = %+v", ops.cleaned)
	}
}

func TestNodeRestoreFailureStampsFailed(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	rec := record.NewFakeRecorder(10)
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: &fakeRestoreOps{failIt: true},
		Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir(), Recorder: rec}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	// failed -> holder wrapper falls through to cold start
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatalf("restore status: %q", got.Annotations[contract.AnnotationRestoreStatus])
	}

	// Verify that a Warning RestoreFailed event was recorded for the replacement pod (#126).
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning RestoreFailed") || !strings.Contains(ev, "falling back to a cold start") {
			t.Fatalf("recorded event = %q, want Warning RestoreFailed with falling back to a cold start", ev)
		}
	default:
		t.Fatal("expected a Warning RestoreFailed event on restore failure, got none")
	}
}

func TestNodeRestore_CancelledContextDoesNotEmitRestoreFailed(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)
	rec := record.NewFakeRecorder(10)
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: &fakeRestoreOps{failIt: true},
		Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir(), Recorder: rec}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context to simulate agent shutdown / SIGTERM

	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled error, got res=%v, err=%v", res, err)
	}

	// Verify NO Warning RestoreFailed event was emitted on shutdown
	select {
	case ev := <-rec.Events:
		t.Fatalf("unexpected event emitted on cancelled context: %s", ev)
	default:
	}

	// Verify pod annotations were NOT modified to failed
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != "" {
		t.Fatalf("restore status should not be stamped on shutdown: %q", got.Annotations[contract.AnnotationRestoreStatus])
	}
}

func TestNodeRestore_RefusesHostNetworkPod(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	pod.Spec.HostNetwork = true
	c := newClient(t, ps, pod)
	rec := record.NewFakeRecorder(10)
	r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: &fakeRestoreOps{},
		Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir(), Recorder: rec}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
		t.Fatalf("restore status: %q, want %s", got.Annotations[contract.AnnotationRestoreStatus], contract.RestoreStatusFailed)
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, "Warning RestoreFailed") || !strings.Contains(ev, "hostNetwork") {
			t.Fatalf("recorded event = %q, want Warning RestoreFailed with hostNetwork", ev)
		}
	default:
		t.Fatal("expected a Warning RestoreFailed event on hostNetwork refusal, got none")
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
	psName := "ps-web-0-deadbeef"

	makePS := func() *unstructured.Unstructured {
		ps := workOrderSnapshot(psName)
		_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
		return ps
	}

	validPMJ := func(pmj *unstructured.Unstructured) {
		_ = unstructured.SetNestedField(pmj.Object, "web-0", "spec", "podRef", "name")
		_ = unstructured.SetNestedField(pmj.Object, "Restoring", "status", "phase")
		_ = unstructured.SetNestedField(pmj.Object, psName, "status", "snapshotRef")
		_ = unstructured.SetNestedField(pmj.Object, "tgt-uid", "status", "restoredPodUID")
		_ = unstructured.SetNestedField(pmj.Object, "web-1", "status", "restoredPodName")
	}

	cases := []struct {
		name      string
		pmjMutate func(pmj *unstructured.Unstructured)
		noPMJAnno bool
	}{
		{
			name:      "missing assigned-pmj annotation",
			noPMJAnno: true,
		},
		{
			name: "mismatched spec.podRef.name",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "other-pod", "spec", "podRef", "name")
			},
		},
		{
			name: "disallowed status.phase Failed",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "Failed", "status", "phase")
			},
		},
		{
			name: "disallowed status.phase SucceededWithoutRestore",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "SucceededWithoutRestore", "status", "phase")
			},
		},
		{
			name: "disallowed status.phase Evicting",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "Evicting", "status", "phase")
			},
		},
		{
			name: "empty status.snapshotRef",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "", "status", "snapshotRef")
			},
		},
		{
			name: "mismatched status.snapshotRef",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "ps-other-snapshot", "status", "snapshotRef")
			},
		},
		{
			name: "empty status.restoredPodUID",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "", "status", "restoredPodUID")
			},
		},
		{
			name: "mismatched status.restoredPodUID",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "some-other-uid", "status", "restoredPodUID")
			},
		},
		{
			name: "mismatched status.restoredPodName",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				validPMJ(pmj)
				_ = unstructured.SetNestedField(pmj.Object, "some-other-pod", "status", "restoredPodName")
			},
		},
		{
			name: "legacy spec.podName and status.snapshotName rejected",
			pmjMutate: func(pmj *unstructured.Unstructured) {
				_ = unstructured.SetNestedField(pmj.Object, "web-0", "spec", "podName")
				_ = unstructured.SetNestedField(pmj.Object, psName, "status", "snapshotName")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ps := makePS()
			pod := replacementPodOnNode(psName)
			if tc.noPMJAnno {
				delete(pod.Annotations, contract.AnnotationAssignedPMJ)
			}
			pmj := contract.New(contract.GVKPodMigrationJob)
			pmj.SetNamespace("default")
			pmj.SetName("test-pmj")
			if tc.pmjMutate != nil {
				tc.pmjMutate(pmj)
			}

			c := fake.NewClientBuilder().
				WithScheme(testScheme(t)).
				WithObjects(ps, pod, pmj).
				Build()
			ops := &fakeRestoreOps{}
			r := &NodeRestoreReconciler{
				Client:   c,
				NodeName: "node-a",
				Ops:      ops,
				Store:    readySnapshotWithArchive(t, newMemStore()),
				WorkDir:  t.TempDir(),
			}

			_, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}})
			if err != nil {
				t.Fatalf("Reconcile should return nil (update status on error), got %v", err)
			}
			if len(ops.restored) != 0 {
				t.Fatalf("expected 0 restores for %s, got %d", tc.name, len(ops.restored))
			}

			updated := &corev1.Pod{}
			_ = r.Get(ctx, types.NamespacedName{Namespace: "default", Name: "web-1"}, updated)
			if updated.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
				t.Errorf("expected RestoreStatusFailed for %s, got %q", tc.name, updated.Annotations[contract.AnnotationRestoreStatus])
			}
		})
	}
}

func TestNodeRestoreAcceptsSucceededPhaseWhenBoundToThisPod(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded}); err != nil {
		t.Fatal(err)
	}
	pod := replacementPodOnNode("ps-web-0-deadbeef")
	c := newClient(t, ps, pod)

	// Simulate the no-readiness-probe race: the PMJ controller already marked
	// the PMJ Succeeded on the same pod update while bound to this pod's UID.
	pmj := contract.New(contract.GVKPodMigrationJob)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "test-pmj"}, pmj); err != nil {
		t.Fatal(err)
	}
	_ = unstructured.SetNestedField(pmj.Object, "Succeeded", "status", "phase")
	if err := c.Status().Update(context.Background(), pmj); err != nil {
		// unstructured status update fallback
		_ = c.Update(context.Background(), pmj)
	}

	ops := &fakeRestoreOps{}
	r := &NodeRestoreReconciler{
		Client:   c,
		NodeName: "node-a",
		Ops:      ops,
		Store:    readySnapshotWithArchive(t, newMemStore()),
		WorkDir:  t.TempDir(),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.restored) != 1 {
		t.Fatalf("expected 1 restore when PMJ is Succeeded and bound to this pod UID, got %d", len(ops.restored))
	}
	got := &corev1.Pod{}
	_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
	if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusRestored {
		t.Fatalf("restore status = %q, want %q", got.Annotations[contract.AnnotationRestoreStatus], contract.RestoreStatusRestored)
	}
}

// TestDeployCRDsUseDefaultGroup pins deploy/crds.yaml to contract.DefaultGroup
// (#135), so reverting the manifest to podsnapshot.gke.io fails the suite.
func TestDeployCRDsUseDefaultGroup(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "crds.yaml"))
	if err != nil {
		t.Fatalf("read crds.yaml: %v", err)
	}
	type crd struct {
		Kind     string `json:"kind"`
		Metadata struct {
			Name string `json:"name"`
		} `json:"metadata"`
		Spec struct {
			Group string `json:"group"`
		} `json:"spec"`
	}
	seen := 0
	for _, doc := range strings.Split(string(raw), "\n---") {
		doc = strings.TrimSpace(doc)
		if doc == "" {
			continue
		}
		var c crd
		if err := k8syaml.Unmarshal([]byte(doc), &c); err != nil {
			t.Fatalf("unmarshal crds.yaml document: %v", err)
		}
		if c.Kind != "CustomResourceDefinition" {
			continue
		}
		seen++
		if c.Spec.Group != contract.DefaultGroup {
			t.Errorf("CRD %s has spec.group %q, want %q", c.Metadata.Name, c.Spec.Group, contract.DefaultGroup)
		}
		if !strings.HasSuffix(c.Metadata.Name, "."+contract.DefaultGroup) {
			t.Errorf("CRD name %q does not end in .%s", c.Metadata.Name, contract.DefaultGroup)
		}
	}
	if seen != 4 {
		t.Errorf("crds.yaml defines %d CRDs, want 4", seen)
	}
}

func TestDeployManifestRBACMatchesContractGVKs(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatalf("read deploy.yaml: %v", err)
	}
	if strings.Contains(string(raw), "pod-migration.gke.io") && strings.Contains(string(raw), `apiGroups: ["pod-migration.gke.io"]`) {
		t.Fatalf("deploy.yaml uses hyphenated apiGroup pod-migration.gke.io; real group is %q", contract.GVKPodMigrationJob.Group)
	}

	var agentRole, controllerRole *rbacv1.ClusterRole
	for _, doc := range strings.Split(string(raw), "\n---") {
		doc = strings.TrimSpace(doc)
		if doc == "" || !strings.Contains(doc, "kind: ClusterRole\n") {
			continue
		}
		var cr rbacv1.ClusterRole
		if err := k8syaml.Unmarshal([]byte(doc), &cr); err != nil {
			t.Fatalf("unmarshal ClusterRole from deploy.yaml: %v", err)
		}
		if cr.Kind != "ClusterRole" {
			continue
		}
		switch cr.Name {
		case "pod-migrate-psengine-agent":
			cp := cr
			agentRole = &cp
		case "pod-migrate-psengine-controller":
			cp := cr
			controllerRole = &cp
		}
	}
	if agentRole == nil {
		t.Fatal("ClusterRole pod-migrate-psengine-agent not found in deploy.yaml")
	}
	if controllerRole == nil {
		t.Fatal("ClusterRole pod-migrate-psengine-controller not found in deploy.yaml")
	}

	hasRule := func(cr *rbacv1.ClusterRole, group, resource string, wantVerbs ...string) bool {
		for _, rule := range cr.Rules {
			groupMatch := false
			for _, g := range rule.APIGroups {
				if g == group {
					groupMatch = true
					break
				}
			}
			resMatch := false
			for _, r := range rule.Resources {
				if r == resource {
					resMatch = true
					break
				}
			}
			if !groupMatch || !resMatch {
				continue
			}
			verbSet := map[string]bool{}
			for _, v := range rule.Verbs {
				verbSet[v] = true
			}
			allVerbs := true
			for _, wv := range wantVerbs {
				if !verbSet[wv] {
					allVerbs = false
					break
				}
			}
			if allVerbs {
				return true
			}
		}
		return false
	}

	if !hasRule(agentRole, contract.GVKPodMigrationJob.Group, "podmigrationjobs", "get", "list", "watch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing get/list/watch on %s/podmigrationjobs", contract.GVKPodMigrationJob.Group)
	}
	if !hasRule(agentRole, contract.GVKPodSnapshot.Group, "podsnapshots", "get", "list", "watch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing get/list/watch on %s/podsnapshots", contract.GVKPodSnapshot.Group)
	}
	// The agent reads the work order's trigger live before the checkpoint and
	// before the source stop (C9, #106).
	if !hasRule(agentRole, contract.GVKManualTrigger.Group, "podsnapshotmanualtriggers", "get") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing get on %s/podsnapshotmanualtriggers", contract.GVKManualTrigger.Group)
	}
	if !hasRule(agentRole, contract.GVKPodSnapshot.Group, "podsnapshots/status", "get", "update", "patch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing get/update/patch on %s/podsnapshots/status", contract.GVKPodSnapshot.Group)
	}
	if !hasRule(agentRole, "", "pods", "get", "list", "watch", "update", "patch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing update/patch on core/pods")
	}
	if !hasRule(agentRole, "", "nodes", "get", "patch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing get/patch on core/nodes")
	}
	if hasRule(agentRole, "", "nodes", "list") || hasRule(agentRole, "", "nodes", "watch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole must not grant list/watch on core/nodes (#227)")
	}
	if !hasRule(agentRole, "", "nodes/checkpoint", "create") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing create on core/nodes/checkpoint")
	}
	if !hasRule(agentRole, "", "events", "create", "patch") {
		t.Errorf("pod-migrate-psengine-agent ClusterRole missing create/patch on core/events")
	}
	if !hasRule(controllerRole, contract.GVKStorageConfig.Group, "podsnapshotstorageconfigs", "get", "list", "watch") {
		t.Errorf("pod-migrate-psengine-controller ClusterRole missing get/list/watch on %s/podsnapshotstorageconfigs", contract.GVKStorageConfig.Group)
	}
	if !hasRule(controllerRole, contract.GVKPodMigrationJob.Group, "podmigrationjobs", "get") {
		t.Errorf("pod-migrate-psengine-controller ClusterRole missing get on %s/podmigrationjobs", contract.GVKPodMigrationJob.Group)
	}
	if hasRule(controllerRole, contract.GVKPodMigrationJob.Group, "podmigrationjobs", "list") || hasRule(controllerRole, contract.GVKPodMigrationJob.Group, "podmigrationjobs", "watch") {
		t.Errorf("pod-migrate-psengine-controller ClusterRole must grant only get (not list/watch) on %s/podmigrationjobs", contract.GVKPodMigrationJob.Group)
	}

	rnBytes, err := os.ReadFile("restore_node.go")
	if err != nil {
		t.Fatalf("read restore_node.go: %v", err)
	}
	wantMarker := "+kubebuilder:rbac:groups=" + contract.GVKPodMigrationJob.Group + ",resources=podmigrationjobs"
	if !strings.Contains(string(rnBytes), wantMarker) {
		t.Fatalf("restore_node.go missing %q marker", wantMarker)
	}

	rwBytes, err := os.ReadFile("restore_webhook.go")
	if err != nil {
		t.Fatalf("read restore_webhook.go: %v", err)
	}
	wantWebhookMarker := "+kubebuilder:rbac:groups=" + contract.GVKPodMigrationJob.Group + ",resources=podmigrationjobs,verbs=get"
	if !strings.Contains(string(rwBytes), wantWebhookMarker) || strings.Contains(string(rwBytes), "resources=podmigrationjobs,verbs=get;list") {
		t.Fatalf("restore_webhook.go missing narrowed %q marker", wantWebhookMarker)
	}
}

type trackingStore struct {
	*memStore
	gets []string
}

func (s *trackingStore) Get(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	s.gets = append(s.gets, object)
	return s.memStore.Get(ctx, bucket, object)
}

func TestNodeRestore_RejectsOversizedMetadata(t *testing.T) {
	const prefix = "snaps/ps-web-0-deadbeef/"

	t.Run("manifest exceeding 4 MiB fails before fetching spec.json", func(t *testing.T) {
		ps := workOrderSnapshot("ps-web-0-deadbeef")
		_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
		pod := replacementPodOnNode("ps-web-0-deadbeef")
		c := newClient(t, ps, pod)

		store := &trackingStore{memStore: newMemStore()}
		validManifest := Manifest{
			Snapshot: "ps-web-0-deadbeef", SourcePod: "web-0", SourcePodUID: "src-uid",
			Container: "app", Node: "node-a", ImageName: "redis:7",
			TarSHA256: "dummy", TarBytes: 8, SpecSHA256: "dummy", EngineVersion: engineVersion,
		}
		rawMF, _ := json.Marshal(validManifest)
		// Pad with valid JSON whitespace up to 4 MiB + 1
		oversizedManifest := bytes.Repeat([]byte(" "), (4<<20)+1)
		copy(oversizedManifest, rawMF)

		store.objects["bkt/"+prefix+"manifest.json"] = oversizedManifest
		store.objects["bkt/"+prefix+"spec.json"] = []byte(`{"ociVersion":"1.1.0"}`)
		store.objects["bkt/"+prefix+"checkpoint.tar"] = []byte("TARBYTES")

		ops := &fakeRestoreOps{}
		r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, WorkDir: t.TempDir()}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
			t.Fatal(err)
		}

		if len(ops.restored) != 0 {
			t.Fatalf("restore called %d times, want 0", len(ops.restored))
		}
		got := &corev1.Pod{}
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
		if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
			t.Fatalf("restore status: %q, want %q", got.Annotations[contract.AnnotationRestoreStatus], contract.RestoreStatusFailed)
		}

		for _, fetched := range store.gets {
			if strings.Contains(fetched, "spec.json") {
				t.Fatalf("spec.json was fetched (%s); oversized manifest must abort before fetching spec", fetched)
			}
			if strings.Contains(fetched, "checkpoint.tar") {
				t.Fatalf("checkpoint.tar was fetched (%s); oversized manifest must abort before fetching tar", fetched)
			}
		}
	})

	t.Run("spec.json exceeding 4 MiB fails before fetching checkpoint.tar", func(t *testing.T) {
		ps := workOrderSnapshot("ps-web-0-deadbeef")
		_ = contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded})
		pod := replacementPodOnNode("ps-web-0-deadbeef")
		c := newClient(t, ps, pod)

		store := &trackingStore{memStore: newMemStore()}
		oversizedSpec := make([]byte, (4<<20)+1)
		specSHA, _, _ := SHA256Reader(bytes.NewReader(oversizedSpec))
		tar := []byte("TARBYTES")
		tarSHA, _, _ := SHA256Reader(bytes.NewReader(tar))

		mf := Manifest{
			Snapshot: "ps-web-0-deadbeef", SourcePod: "web-0", SourcePodUID: "src-uid",
			Container: "app", Node: "node-a", ImageName: "redis:7",
			TarSHA256: tarSHA, TarBytes: int64(len(tar)), SpecSHA256: specSHA, EngineVersion: engineVersion,
		}
		mfJSON, _ := json.Marshal(mf)
		store.objects["bkt/"+prefix+"manifest.json"] = mfJSON
		store.objects["bkt/"+prefix+"spec.json"] = oversizedSpec
		store.objects["bkt/"+prefix+"checkpoint.tar"] = tar

		ops := &fakeRestoreOps{}
		r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, WorkDir: t.TempDir()}

		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-1"}}); err != nil {
			t.Fatal(err)
		}

		if len(ops.restored) != 0 {
			t.Fatalf("restore called %d times, want 0", len(ops.restored))
		}
		got := &corev1.Pod{}
		_ = c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-1"}, got)
		if got.Annotations[contract.AnnotationRestoreStatus] != contract.RestoreStatusFailed {
			t.Fatalf("restore status: %q, want %q", got.Annotations[contract.AnnotationRestoreStatus], contract.RestoreStatusFailed)
		}

		for _, fetched := range store.gets {
			if strings.Contains(fetched, "checkpoint.tar") {
				t.Fatalf("checkpoint.tar was fetched (%s); oversized spec.json must abort before fetching tar", fetched)
			}
		}
	})
}
