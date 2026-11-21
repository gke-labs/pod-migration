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
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec
	"encoding/base64"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

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
	unfrozenUIDs  []string
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

func (f *fakeNodeOps) Freeze(_ context.Context, ns, pod, podUID, container string) error {
	if f.beforeFreeze != nil {
		f.beforeFreeze()
	}
	f.events = append(f.events, "freeze")
	f.frozen = append(f.frozen, ns+"/"+pod+"/"+container)
	return f.freezeErr
}

func (f *fakeNodeOps) Unfreeze(_ context.Context, ns, pod, podUID, container string) error {
	f.events = append(f.events, "unfreeze")
	f.unfrozen = append(f.unfrozen, ns+"/"+pod+"/"+container)
	f.unfrozenUIDs = append(f.unfrozenUIDs, podUID)
	return f.unfreezeErr
}

func (f *fakeNodeOps) AfterCommit() {
	if f.beforeStop != nil {
		f.beforeStop()
	}
	f.events = append(f.events, "commit")
	f.stopped = append(f.stopped, "committed")
}

func (f *fakeNodeOps) SourceInfo(_ context.Context, ns, pod, podUID, container string) ([]byte, string, error) {
	f.events = append(f.events, "sourceinfo")
	return []byte(`{"ociVersion":"1.1.0"}`), "docker.io/library/redis:7", nil
}

// memStore is an in-memory ObjectStore recording write order.
type memStore struct {
	mu           sync.Mutex
	objects      map[string][]byte
	order        []string
	composeCalls int
	rangeCalls   int
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) Put(_ context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	m.objects[bucket+"/"+object] = data
	m.order = append(m.order, object)
	m.mu.Unlock()
	return &ObjectInfo{Bytes: int64(len(data)), MD5: md5b64(data)}, nil
}

func (m *memStore) Get(_ context.Context, bucket, object string) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[bucket+"/"+object]
	if !ok {
		return nil, errors.New("not found: " + object)
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

func (m *memStore) Delete(_ context.Context, bucket, object string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, bucket+"/"+object)
	return nil
}

func (m *memStore) Compose(_ context.Context, bucket, destination string, sources []string) (*ObjectInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.composeCalls++
	var combined []byte
	for _, src := range sources {
		data, ok := m.objects[bucket+"/"+src]
		if !ok {
			return nil, errors.New("not found: " + src)
		}
		combined = append(combined, data...)
	}
	m.objects[bucket+"/"+destination] = combined
	m.order = append(m.order, destination)
	return &ObjectInfo{Bytes: int64(len(combined)), MD5: md5b64(combined)}, nil
}

func (m *memStore) GetRange(_ context.Context, bucket, object string, start, end int64) (io.ReadCloser, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rangeCalls++
	data, ok := m.objects[bucket+"/"+object]
	if !ok {
		return nil, errors.New("not found: " + object)
	}
	if start > int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	if end < 0 || end >= int64(len(data)) {
		end = int64(len(data)) - 1
	}
	return io.NopCloser(bytes.NewReader(data[start : end+1])), nil
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

	// A subsequent reconcile of a Ready snapshot must be a no-op (keeps SourceFrozen=True, does not thaw).
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if gotEv := strings.Join(ops.events, ","); gotEv != "freeze,checkpoint,sourceinfo,commit" {
		t.Fatalf("subsequent reconcile added events: %s", gotEv)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatalf("subsequent reconcile must not unfreeze Ready snapshot: %v", ops.unfrozen)
	}
	got = getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("subsequent reconcile must preserve SourceFrozen=True")
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
	if err != nil {
		t.Fatalf("expected nil error on failed unfreeze to preserve fixed 2s requeue, got: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("expected 2s requeue on failed unfreeze, got %v", res.RequeueAfter)
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

	// Retry pass: unfreeze succeeds. The retry must ONLY thaw the container and settle,
	// NOT re-run the checkpoint pipeline (no re-freeze, no re-checkpoint) (#148).
	ops.unfreezeErr = nil
	ops.events = nil
	res2, err2 := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err2 != nil {
		t.Fatalf("unexpected error on retry: %v", err2)
	}
	if res2.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue on successful retry: %v", res2)
	}
	if len(ops.events) != 1 || ops.events[0] != "unfreeze" {
		t.Fatalf("failed-thaw retry must only unfreeze, never re-run checkpoint pipeline: events=%v", ops.events)
	}
	gotAfterRetry := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(gotAfterRetry) {
		t.Fatal("successful thaw on retry must clear SourceFrozen")
	}
	condAfterRetry := condition(gotAfterRetry, contract.CondCheckpoint)
	if condAfterRetry == nil || condAfterRetry.Reason != contract.ReasonFailed {
		t.Fatalf("want Failed outcome after retry thaw, got %+v", condAfterRetry)
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

// TestPipeline_ErrFinishedFailedThawRetriesAndThaws pins that if an outcome was written
// elsewhere (e.g. Refused) and Unfreeze fails, SourceFrozen remains True, Reconcile
// requeues, and the retry pass thaws without re-running checkpointing (#148).
func TestPipeline_ErrFinishedFailedThawRetriesAndThaws(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{
		tarContent:  "TARBYTES",
		unfreezeErr: errors.New("containerd unfreeze failed"),
		t:           t,
	}
	store := newMemStore()

	// Controller writes Refused outcome mid-pipeline after freeze.
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
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("expected nil error on failed unfreeze for errFinished to preserve 2s requeue, got: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("expected 2s requeue on failed unfreeze for errFinished, got %v", res.RequeueAfter)
	}

	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("SourceFrozen must remain True when thaw fails on errFinished")
	}

	// Retry pass: unfreeze succeeds.
	ops.unfreezeErr = nil
	ops.events = nil
	res2, err2 := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err2 != nil {
		t.Fatalf("unexpected error on retry pass: %v", err2)
	}
	if res2.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue on successful retry: %v", res2)
	}
	if len(ops.events) != 1 || ops.events[0] != "unfreeze" {
		t.Fatalf("retry pass on errFinished must only unfreeze, got events: %v", ops.events)
	}
	got2 := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got2) {
		t.Fatal("SourceFrozen must be cleared to false after successful thaw on retry")
	}
	cond2 := condition(got2, contract.CondCheckpoint)
	if cond2 == nil || cond2.Reason != contract.ReasonRefused {
		t.Fatalf("existing Refused outcome must be preserved, got %+v", cond2)
	}
}

type failOnNthGetReader struct {
	client.Reader
	calls      int
	failOnCall int
}

func (f *failOnNthGetReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	f.calls++
	if f.failOnCall > 0 && f.calls == f.failOnCall {
		return errors.New("apiserver unavailable on live get")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

type injectOutcomeOnSnapshotGetReader struct {
	client.Reader
	targetKey     types.NamespacedName
	outcomeReason string
	snapshotGets  int
	injectOnGet   int
}

func (r *injectOutcomeOnSnapshotGetReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if err := r.Reader.Get(ctx, key, obj, opts...); err != nil {
		return err
	}
	if key == r.targetKey {
		if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PodSnapshot" {
			r.snapshotGets++
			if r.injectOnGet > 0 && r.snapshotGets >= r.injectOnGet && r.outcomeReason != "" {
				_ = contract.SetCondition(u, metav1.Condition{
					Type:   contract.CondCheckpoint,
					Status: metav1.ConditionFalse,
					Reason: r.outcomeReason,
				})
			}
		}
	}
	return nil
}

// TestPipeline_ErrFinishedLiveGetErrorRequeues pins that if live reading of the
// PodSnapshot fails during errFinished, Reconcile requeues and does not fail open (does not thaw).
func TestPipeline_ErrFinishedLiveGetErrorRequeues(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()

	reader := &failOnNthGetReader{Reader: c}
	ops.afterCheckpoint = func() {
		snap := getSnapshot(t, c, "ps-web-0-deadbeef")
		_ = contract.SetCondition(snap, metav1.Condition{
			Type:   contract.CondCheckpoint,
			Status: metav1.ConditionFalse,
			Reason: contract.ReasonRefused,
		})
		_ = c.Status().Update(context.Background(), snap)
		// Next read is pre-commit cancelCheck (must succeed to return errFinished).
		// The read after that is liveReader().Get in errFinished handling: make it fail.
		reader.failOnCall = reader.calls + 2
	}

	r := &NodeSnapshotReconciler{Client: c, APIReader: reader, NodeName: "node-a", Ops: ops, Store: store}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("expected nil error on failing live read to preserve 2s requeue, got: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("expected 2s requeue on failing live read, got %v", res.RequeueAfter)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatal("live read error on errFinished must not fail open (must not thaw)")
	}
}

// TestPipeline_ErrFinishedReadySnapshotKeptFrozen pins that if errFinished reveals
// the snapshot is Ready=True, the source is kept frozen for eviction and Unfreeze is not called.
func TestPipeline_ErrFinishedReadySnapshotKeptFrozen(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()

	// Controller or another worker committed Ready mid-run
	ops.afterCheckpoint = func() {
		snap := getSnapshot(t, c, "ps-web-0-deadbeef")
		_ = contract.SetCondition(snap, metav1.Condition{
			Type:   contract.CondReady,
			Status: metav1.ConditionTrue,
			Reason: contract.ReasonSucceeded,
		})
		_ = c.Status().Update(context.Background(), snap)
	}

	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatal("Ready snapshot on errFinished must NOT be unfrozen")
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("Ready snapshot must remain SourceFrozen=True")
	}
}

// TestPipeline_FailedFreezeFailedThawLeavesSourceFrozenAndRequeues pins that if
// Freeze fails and Unfreeze also fails, SourceFrozen stays True, Reconcile requeues,
// and the subsequent retry thaws without re-attempting Freeze.
func TestPipeline_FailedFreezeFailedThawLeavesSourceFrozenAndRequeues(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{
		freezeErr:   errors.New("containerd pause failed"),
		unfreezeErr: errors.New("containerd unfreeze failed"),
		t:           t,
	}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("expected nil error on failed unfreeze after failed freeze to preserve 2s requeue, got: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("expected 2s requeue on failed unfreeze after failed freeze, got %v", res.RequeueAfter)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("SourceFrozen must remain True when thaw fails after failed freeze")
	}

	// Retry pass: unfreeze succeeds.
	ops.unfreezeErr = nil
	ops.events = nil
	res2, err2 := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err2 != nil {
		t.Fatalf("unexpected error on retry pass: %v", err2)
	}
	if res2.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue on successful retry: %v", res2)
	}
	if len(ops.events) != 1 || ops.events[0] != "unfreeze" {
		t.Fatalf("retry after failed freeze must only unfreeze, got events: %v", ops.events)
	}
	got2 := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got2) {
		t.Fatal("successful thaw on retry must clear SourceFrozen")
	}
	cond2 := condition(got2, contract.CondCheckpoint)
	if cond2 == nil || cond2.Reason != contract.ReasonFailed {
		t.Fatalf("want Failed outcome, got %+v", cond2)
	}
}

// TestPipeline_ThawedSourceClearsSourceFrozenWithExistingOutcome pins that if an outcome
// was already written (e.g. Refused) while SourceFrozen=True, thawing clears SourceFrozen
// without overwriting the outcome (fixes Point 5).
func TestPipeline_ThawedSourceClearsSourceFrozenWithExistingOutcome(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen")
	_ = contract.SetCondition(ps, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionFalse,
		Reason: contract.ReasonRefused,
	})
	c := newClient(t, ps)
	ops := &fakeNodeOps{t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("expected 1 unfreeze, got: %v", ops.unfrozen)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got) {
		t.Fatal("SourceFrozen must be cleared to false after thaw")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond == nil || cond.Reason != contract.ReasonRefused {
		t.Fatalf("existing Refused outcome must be preserved, got %+v", cond)
	}
}

// TestPipeline_FailedThawOnCancelLeavesSourceFrozenAndRequeues pins that if cancel
// occurs and Unfreeze fails, SourceFrozen stays True, Reconcile requeues, and the outcome
// is not settled as Cancelled until thaw succeeds.
func TestPipeline_FailedThawOnCancelLeavesSourceFrozenAndRequeues(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen before cancel")
	_ = contract.SetCondition(ps, metav1.Condition{
		Type:   contract.CondCheckpoint,
		Status: metav1.ConditionUnknown,
		Reason: contract.ReasonCancelRequested,
	})
	c := newClient(t, ps)
	ops := &fakeNodeOps{
		unfreezeErr: errors.New("containerd unfreeze failed"),
		t:           t,
	}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("expected nil error on failed unfreeze during cancel to preserve 2s requeue, got: %v", err)
	}
	if res.RequeueAfter != 2*time.Second {
		t.Fatalf("expected 2s requeue on failed unfreeze during cancel, got %v", res.RequeueAfter)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSourceFrozen(got) {
		t.Fatal("failed unfreeze on cancel must leave SourceFrozen=True")
	}
	cond := condition(got, contract.CondCheckpoint)
	if cond != nil && cond.Reason == contract.ReasonCancelled {
		t.Fatal("checkpoint must not settle as Cancelled while thaw is failing")
	}

	// Retry pass: unfreeze succeeds
	ops.unfreezeErr = nil
	ops.events = nil
	res2, err2 := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err2 != nil {
		t.Fatalf("unexpected error on retry: %v", err2)
	}
	if res2.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue: %v", res2)
	}
	if len(ops.events) != 1 || ops.events[0] != "unfreeze" {
		t.Fatalf("retry on cancel must only unfreeze, got events: %v", ops.events)
	}
	got2 := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(got2) {
		t.Fatal("SourceFrozen must be cleared to false on successful cancel thaw")
	}
	cond2 := condition(got2, contract.CondCheckpoint)
	if cond2 == nil || cond2.Reason != contract.ReasonCancelled {
		t.Fatalf("want Cancelled outcome, got %+v", cond2)
	}
}

// TestPipeline_ThawPassesSourcePodUID pins that Unfreeze receives the exact
// spec.SourcePodUID on leftover thaw, cancel, and failure paths.
func TestPipeline_ThawPassesSourcePodUID(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	_ = unstructured.SetNestedField(ps.Object, "uid-source-pod-12345", "spec", "sourcePodUID")
	_ = contract.SetSourceFrozen(ps, true, contract.ReasonFrozen, "source frozen")
	c := newClient(t, ps)
	ops := &fakeNodeOps{t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store}

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}); err != nil {
		t.Fatal(err)
	}
	if len(ops.unfrozenUIDs) != 1 || ops.unfrozenUIDs[0] != "uid-source-pod-12345" {
		t.Fatalf("Unfreeze must be called with sourcePodUID 'uid-source-pod-12345', got %v", ops.unfrozenUIDs)
	}
}

type fakeManagerWithAdd struct {
	ctrl.Manager
	added  []manager.Runnable
	addErr error
}

func (m *fakeManagerWithAdd) Add(r manager.Runnable) error {
	m.added = append(m.added, r)
	return m.addErr
}

// TestSetupWithManagerRegistersStartupSweep pins that SetupWithManager registers
// the startup sweep Runnable with the manager.
func TestSetupWithManagerRegistersStartupSweep(t *testing.T) {
	r := &NodeSnapshotReconciler{NodeName: "node-a"}
	mgr := &fakeManagerWithAdd{addErr: errors.New("registration failed")}
	err := r.SetupWithManager(mgr)
	if err == nil || !strings.Contains(err.Error(), "registration failed") {
		t.Fatalf("expected error from mgr.Add, got: %v", err)
	}
	if len(mgr.added) != 1 {
		t.Fatalf("expected 1 runnable added to manager, got %d", len(mgr.added))
	}
}

// TestPipeline_StaleCacheDoesNotThawJustCheckpointedReadySource pins that when
// cached r.Get returns a pre-Ready snapshot with SourceFrozen=True (cache lagging
// status update), the liveReader re-read under r.inflight discovers Ready=True
// and skips without unfreezing the source container.
func TestPipeline_StaleCacheDoesNotThawJustCheckpointedReadySource(t *testing.T) {
	// Live object in APIReader is Ready=True and SourceFrozen=True
	liveSnap := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetSourceFrozen(liveSnap, true, contract.ReasonFrozen, "source container frozen")
	_ = contract.SetCondition(liveSnap, metav1.Condition{
		Type:   contract.CondReady,
		Status: metav1.ConditionTrue,
		Reason: contract.ReasonSucceeded,
	})
	liveClient := newClient(t, liveSnap)

	// Cached object has SourceFrozen=True but no Ready condition (lagging cache)
	staleSnap := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetSourceFrozen(staleSnap, true, contract.ReasonFrozen, "source container frozen")

	cachedClient := &staleSnapshotClient{
		Client: liveClient,
		key:    types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"},
		stale:  staleSnap,
	}
	ops := &fakeNodeOps{t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{
		Client:    cachedClient,
		APIReader: liveClient,
		NodeName:  "node-a",
		Ops:       ops,
		Store:     store,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("unexpected error on reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue: %v", res)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatalf("stale cache must not cause Ready snapshot source to be unfrozen: %v", ops.unfrozen)
	}
	got := getSnapshot(t, liveClient, "ps-web-0-deadbeef")
	if !contract.IsSnapshotReady(got) {
		t.Fatal("snapshot must remain Ready")
	}
	if !contract.IsSourceFrozen(got) {
		t.Fatal("snapshot must remain SourceFrozen=True")
	}
}

// TestPipeline_OutcomeBeforeRecordSourceFrozenStopsBeforeFreeze pins that if an outcome
// (e.g. Refused from demoting node) lands before recordSourceFrozen(true), recordSourceFrozen
// fails closed with errFinished, causing run() to return early without calling Ops.Freeze,
// without calling Ops.Unfreeze, and without writing SourceFrozen condition.
func TestPipeline_OutcomeBeforeRecordSourceFrozenStopsBeforeFreeze(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	c := newClient(t, ps)
	ops := &fakeNodeOps{t: t}
	store := newMemStore()

	// Inject Refused on the liveReader Get call inside recordSourceFrozen(true)
	// (snapshotGet #3: #1 is initial livePS read, #2 is cancelCheck).
	// Before that, the snapshot has no outcome so Reconcile enters run().
	reader := &injectOutcomeOnSnapshotGetReader{
		Reader:        c,
		targetKey:     types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"},
		outcomeReason: contract.ReasonRefused,
		injectOnGet:   3,
	}

	r := &NodeSnapshotReconciler{Client: c, APIReader: reader, NodeName: "node-a", Ops: ops, Store: store}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue: %v", res)
	}
	if len(ops.frozen) != 0 {
		t.Fatalf("Freeze must not be called when snapshot already has outcome before recordSourceFrozen: %v", ops.frozen)
	}
	if len(ops.unfrozen) != 0 {
		t.Fatalf("Unfreeze must not be called when snapshot was never frozen: %v", ops.unfrozen)
	}
	finalSnap := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSourceFrozen(finalSnap) {
		t.Fatal("snapshot must not be marked SourceFrozen=True")
	}
	for _, cond := range contract.Conditions(finalSnap) {
		if cond.Type == contract.CondSourceFrozen {
			t.Fatalf("snapshot must not carry SourceFrozen condition when never frozen: %v", cond)
		}
	}
}

// TestPipeline_NotFoundOnLiveGetInLeftoverThawsDirectly pins that when cached ps
// has SourceFrozen=True but liveReader returns NotFound (snapshot was deleted while frozen),
// Reconcile thaws the source container directly and returns without error.
func TestPipeline_NotFoundOnLiveGetInLeftoverThawsDirectly(t *testing.T) {
	cachedSnap := workOrderSnapshot("ps-web-0-deadbeef")
	_ = contract.SetSourceFrozen(cachedSnap, true, contract.ReasonFrozen, "source container frozen")

	// Empty live client (snapshot does not exist in API server)
	liveClient := newClient(t)
	cachedClient := &staleSnapshotClient{
		Client: liveClient,
		key:    types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"},
		stale:  cachedSnap,
	}
	ops := &fakeNodeOps{t: t}
	store := newMemStore()
	r := &NodeSnapshotReconciler{
		Client:    cachedClient,
		APIReader: liveClient,
		NodeName:  "node-a",
		Ops:       ops,
		Store:     store,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue: %v", res)
	}
	if len(ops.unfrozen) != 1 {
		t.Fatalf("expected 1 unfreeze when snapshot deleted while frozen, got: %v", ops.unfrozen)
	}
}
