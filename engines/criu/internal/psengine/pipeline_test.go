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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

type fakeNodeOps struct {
	tarContent string
	failDump   bool
	stopped    []string
	events     []string
	t          *testing.T
}

func (f *fakeNodeOps) Checkpoint(_ context.Context, ns, pod, container string) (string, error) {
	f.events = append(f.events, "checkpoint")
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

func (f *fakeNodeOps) SourceInfo(_ context.Context, ns, pod, container string) ([]byte, string, error) {
	f.events = append(f.events, "sourceinfo")
	return []byte(`{"ociVersion":"1.1.0"}`), "docker.io/library/redis:7", nil
}

func (f *fakeNodeOps) StopContainer(_ context.Context, ns, pod, container string) error {
	f.events = append(f.events, "stop")
	f.stopped = append(f.stopped, ns+"/"+pod+"/"+container)
	return nil
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
}

func (f *failingStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	if strings.Contains(object, f.failSubstr) {
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

	// Stop AFTER upload (event order), exactly once.
	if got := strings.Join(ops.events, ","); got != "checkpoint,sourceinfo,stop" {
		t.Fatalf("event order: %s", got)
	}

	// PodSnapshot Ready — the condition shape the orchestrator polls.
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSnapshotReady(got) {
		t.Fatalf("snapshot not ready: %+v", contract.Conditions(got))
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
	if len(ops.stopped) != 0 {
		t.Fatal("failed checkpoint must NOT stop the container (source keeps serving)")
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
	if len(ops.stopped) != 0 {
		t.Fatal("upload failure must NOT stop the container")
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
