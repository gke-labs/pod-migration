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
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

type fakeNodeOps struct {
	tarContent string
	failDump   bool
	// checkpointErr, when set, is returned by Checkpoint (after afterCheckpoint).
	checkpointErr error
	freezeErr     error
	unfreezeErr   error
	stopped       []string
	frozen        []string
	unfrozen      []string
	events        []string
	t             *testing.T

	beforeFreeze func()
	// afterCheckpoint runs once the checkpoint is taken, before it returns.
	afterCheckpoint func()
	// beforeStop runs when AfterCommit is called.
	beforeStop func()
	// blockCheckpoint, when set, is closed once the checkpoint starts; the
	// call then blocks until releaseCheckpoint is closed or its context is
	// cancelled (or 10s pass).
	blockCheckpoint        chan struct{}
	releaseCheckpoint      chan struct{}
	checkpointCtxCancelled bool
}

func (f *fakeNodeOps) Checkpoint(ctx context.Context, ns, pod, container string) (string, error) {
	f.events = append(f.events, "checkpoint")
	if f.blockCheckpoint != nil {
		close(f.blockCheckpoint)
		select {
		case <-f.releaseCheckpoint:
		case <-ctx.Done():
			f.checkpointCtxCancelled = true
			return "", ctx.Err()
		case <-time.After(10 * time.Second):
			return "", errors.New("checkpoint was never released")
		}
	}
	if f.afterCheckpoint != nil {
		defer f.afterCheckpoint()
	}
	if f.checkpointErr != nil {
		return "", f.checkpointErr
	}
	if f.failDump {
		return "", errors.New("criu dump failed: fsnotify handle")
	}
	dir := f.t.TempDir()
	p := filepath.Join(dir, "checkpoint.tar")
	if err := os.WriteFile(p, []byte(f.tarContent), 0o600); err != nil {
		return "", err
	}
	return p, nil
}

func (f *fakeNodeOps) Freeze(_ context.Context, ns, pod, container string) error {
	if f.beforeFreeze != nil {
		f.beforeFreeze()
	}
	f.events = append(f.events, "freeze")
	f.frozen = append(f.frozen, ns+"/"+pod+"/"+container)
	return f.freezeErr
}

func (f *fakeNodeOps) Unfreeze(_ context.Context, ns, pod, container string) error {
	f.events = append(f.events, "unfreeze")
	f.unfrozen = append(f.unfrozen, ns+"/"+pod+"/"+container)
	return f.unfreezeErr
}

func (f *fakeNodeOps) AfterCommit() {
	if f.beforeStop != nil {
		f.beforeStop()
	}
	f.events = append(f.events, "commit")
	f.stopped = append(f.stopped, "committed")
}

func (f *fakeNodeOps) SourceInfo(_ context.Context, ns, pod, container string) ([]byte, string, error) {
	f.events = append(f.events, "sourceinfo")
	return []byte(`{"ociVersion":"1.1.0"}`), "docker.io/library/redis:7", nil
}

// memStore is an in-memory ObjectStore recording write order.
type memStore struct {
	objects map[string][]byte
	order   []string
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) Put(_ context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	m.objects[bucket+"/"+object] = data
	m.order = append(m.order, object)
	return &ObjectInfo{Bytes: int64(len(data)), MD5: md5b64(data)}, nil
}

func (m *memStore) Get(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	data, ok := m.objects[bucket+"/"+object]
	if !ok {
		return nil, errors.New("not found: " + object)
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (m *memStore) Delete(_ context.Context, bucket, object string) error {
	delete(m.objects, bucket+"/"+object)
	return nil
}

// failingStore fails Put on objects whose name contains failSubstr —
// exercises the upload-failure path (archive must not go Ready and the source
// container must NOT be stopped).
type failingStore struct {
	*memStore
	failSubstr string
	onFail     func() // runs before the failure is returned
}

func (f *failingStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	if strings.Contains(object, f.failSubstr) {
		if f.onFail != nil {
			f.onFail()
		}
		return nil, errors.New("gcs put: HTTP 503 backend unavailable")
	}
	return f.memStore.Put(ctx, bucket, object, r)
}

func workOrderSnapshot(name string) *unstructured.Unstructured {
	ps := contract.New(contract.GVKPodSnapshot)
	ps.SetName(name)
	ps.SetNamespace("default")
	ps.SetLabels(map[string]string{contract.LabelEngine: contract.EngineCRIU})
	ps.Object["spec"] = map[string]interface{}{
		"sourcePod": "web-0", "sourcePodUID": "deadbeef-0000-1111",
		"container": "app", "nodeName": "node-a",
		"bucket": "bkt", "path": "snaps",
	}
	return ps
}

func TestNodePipelineHappyPath(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CPUFamily: "Intel(R) Xeon(R) test"}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}

	// Archive layout + write order: manifest LAST (upload-complete marker).
	want := []string{"snaps/ps-web-0-deadbeef/spec.json", "snaps/ps-web-0-deadbeef/checkpoint.tar", "snaps/ps-web-0-deadbeef/manifest.json"}
	if len(store.order) != 3 || store.order[0] != want[0] || store.order[1] != want[1] || store.order[2] != want[2] {
		t.Fatalf("write order: %v", store.order)
	}
	if string(store.objects["bkt/snaps/ps-web-0-deadbeef/checkpoint.tar"]) != "TARBYTES" {
		t.Fatal("tar content mismatch")
	}

	// Freeze BEFORE checkpoint, keep frozen (no unfreeze), exactly once.
	if got := strings.Join(ops.events, ","); got != "freeze,checkpoint,sourceinfo,commit" {
		t.Fatalf("event order: %s", got)
	}
	if len(ops.frozen) != 1 || len(ops.unfrozen) != 0 {
		t.Fatalf("fencing state: frozen=%v unfrozen=%v", ops.frozen, ops.unfrozen)
	}

	// PodSnapshot Ready — the condition shape the orchestrator polls.
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSnapshotReady(got) {
		t.Fatalf("snapshot not ready: %+v", contract.Conditions(got))
	}
	if !contract.IsSourceFrozen(got) {
		t.Fatal("successful snapshot must record SourceFrozen=True")
	}
}

func TestNodePipelineFailureLeavesSourceRunning(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{failDump: true, t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("failed checkpoint must thaw the container (source keeps serving): unfrozen=%v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSnapshotReady(got) {
		t.Fatal("failed snapshot must not be ready")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonFailed || !strings.Contains(cond.Message, "criu dump failed") {
		t.Fatalf("want Failed with dump error, got %+v", cond)
	}
}

func TestNodePipelineUploadFailureLeavesSourceRunning(t *testing.T) {
	// The dump succeeded but GCS is down: the snapshot must go Failed (never
	// Ready), and — critically — the container must NOT be stopped, because
	// nothing durable exists to restore from.
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &failingStore{memStore: newMemStore(), failSubstr: "checkpoint.tar"}
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("upload failure must thaw the container: unfrozen=%v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSnapshotReady(got) {
		t.Fatal("snapshot must not be ready after a failed upload")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonFailed || !strings.Contains(cond.Message, "upload checkpoint.tar") {
		t.Fatalf("want Failed with upload error, got %+v", cond)
	}
	// The manifest (completion marker) must never exist for a failed upload.
	if _, ok := store.objects["bkt/snaps/ps-web-0-deadbeef/manifest.json"]; ok {
		t.Fatal("manifest must not be written when the tar upload failed")
	}
}

func TestNodePipelineIgnoresOtherNodesAndForeignSnapshots(t *testing.T) {
	other := workOrderSnapshot("ps-other-node")
	_ = unstructured.SetNestedField(other.Object, "node-b", "spec", "nodeName")
	foreign := workOrderSnapshot("ps-foreign")
	foreign.SetLabels(nil)
	c := newClient(t, other, foreign)
	ops := &fakeNodeOps{t: t}
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore()}

	for _, name := range []string{"ps-other-node", "ps-foreign"} {
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: name}}); err != nil {
			t.Fatal(err)
		}
	}
	if len(ops.events) != 0 {
		t.Fatalf("must not act on other nodes / foreign snapshots: %v", ops.events)
	}
}

func md5b64(data []byte) string {
	sum := md5.Sum(data) //nolint:gosec
	return base64.StdEncoding.EncodeToString(sum[:])
}

// TestPipeline_RecordSourceFrozenBeforePause pins that SourceFrozen=True is
// written to the API server BEFORE containerd task pause is executed (#148).
func TestPipeline_RecordSourceFrozenBeforePause(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()

	pauseObservedFrozen := false
	ops.beforeFreeze = func() {
		// When Freeze is called, SourceFrozen=True must ALREADY be recorded on the CR.
		current := getSnapshot(t, c, "ps-web-0-deadbeef")
		if contract.IsSourceFrozen(current) {
			pauseObservedFrozen = true
		}
	}

	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if !pauseObservedFrozen {
		t.Fatal("SourceFrozen=True must be recorded on the snapshot BEFORE task Pause is invoked")
	}
}

// TestPipeline_FailedThawLeavesSourceFrozenAndRetries pins that if Unfreeze
// returns an error on pipeline failure, the outcome is NOT settled as Failed,
// SourceFrozen remains True, and Reconcile returns an error so it retries (#148).
func TestPipeline_FailedThawLeavesSourceFrozenAndRetries(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{
		failDump:    true,
		unfreezeErr: errors.New("containerd unfreeze connection error"),
		t:           t,
	}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err == nil {
		t.Fatal("expected error on failed unfreeze, got nil")
	}
	if res.RequeueAfter == 0 {
		t.Fatal("expected requeue on failed unfreeze")
	}

	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("failed unfreeze must leave SourceFrozen=True so controller keeps fence")
	}
	// Checkpoint condition must NOT be settled as Failed when unfreeze failed.
	cond := condition(got, contract.CondCheckpoint)
	if cond != nil && cond.Reason == contract.ReasonFailed {
		t.Fatalf("checkpoint must not settle with ReasonFailed when thaw failed, got %+v", cond)
	}
}

// TestPipeline_FailedFreezeThawsSource pins that if Freeze fails, Unfreeze is
// called to clean up any partial state (#148).
func TestPipeline_FailedFreezeThawsSource(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{
		freezeErr: errors.New("containerd pause failed"),
		t:         t,
	}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) == 0 {
		t.Fatal("failed freeze must attempt to unfreeze container")
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got) {
		t.Fatal("failed freeze with successful unfreeze must clear SourceFrozen")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonFailed {
		t.Fatalf("want Failed outcome, got %+v", cond)
	}
}

// TestPipeline_ShutdownCallsUnfreeze pins that agent shutdown (context cancelled)
// thaws the source container so clients aren't hung while the agent restarts (#148).
func TestPipeline_ShutdownCallsUnfreeze(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ctx, cancel := context.WithCancel(context.Background())
	ops := &fakeNodeOps{
		checkpointErr: context.Canceled,
		t:             t,
	}
	cancel()
	store := newMemStore()

	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got: %v", err)
	}
	if len(ops.unfrozen) == 0 {
		t.Fatal("agent shutdown must call Unfreeze on source container")
	}
}

// TestPipeline_ErrFinishedThawsSourceIfNotReady pins that if an outcome is
// written by someone else (e.g. Refused/Failed) mid-pipeline, the agent thaws
// the frozen source container instead of abandoning it frozen (#148).
func TestPipeline_ErrFinishedThawsSourceIfNotReady(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()

	// Simulate someone writing Refused outcome mid-pipeline after freeze.
	ops.afterCheckpoint = func() {
		snap := getSnapshot(t, c, "ps-web-0-deadbeef")
		_ = contract.SetCondition(snap, metav1.Condition{
			Type:    contract.CondCheckpoint,
			Status:  metav1.ConditionFalse,
			Reason:  contract.ReasonRefused,
			Message: "node demoted mid-run",
		})
		_ = c.Status().Update(context.Background(), snap)
	}

	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("errFinished on unready snapshot must thaw source: unfrozen=%v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got) {
		t.Fatal("SourceFrozen must be cleared to false when thawed on errFinished")
	}
}
