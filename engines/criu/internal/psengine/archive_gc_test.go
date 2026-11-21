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
	"errors"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	k8syaml "sigs.k8s.io/yaml"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const (
	gcNS     = "default"
	gcSnap   = "ps-web-0-abcdef12"
	gcBucket = "ckpt-bucket"
	gcPath   = "migrations"
)

// gcStore is an in-memory object store whose deletes can fail.
type gcStore struct {
	*memStore
	deleteErr error
	deletes   []string
}

func (s *gcStore) Delete(ctx context.Context, bucket, object string) error {
	s.deletes = append(s.deletes, bucket+"/"+object)
	if s.deleteErr != nil {
		return s.deleteErr
	}
	return s.memStore.Delete(ctx, bucket, object)
}

// gcStoreWithArchive returns a store holding a complete archive of gcSnap.
func gcStoreWithArchive() *gcStore {
	s := &gcStore{memStore: newMemStore()}
	for _, name := range ArchiveObjects {
		s.objects[gcBucket+"/"+path.Join(ArchivePrefix(gcPath, gcSnap), name)] = []byte("data")
	}
	// An object of another snapshot in the same bucket must survive.
	s.objects[gcBucket+"/"+path.Join(ArchivePrefix(gcPath, "ps-other-00000000"), "checkpoint.tar")] = []byte("other")
	return s
}

func (s *gcStore) archiveLeft() []string {
	var left []string
	for key := range s.objects {
		if strings.HasPrefix(key, gcBucket+"/"+ArchivePrefix(gcPath, gcSnap)+"/") {
			left = append(left, key)
		}
	}
	return left
}

// gcSnapshot returns a CRIU work order with a bucket, Ready, with the
// engine's archive finalizer.
func gcSnapshot(withFinalizer bool, ready bool) *unstructured.Unstructured {
	ps := contract.New(contract.GVKPodSnapshot)
	ps.SetNamespace(gcNS)
	ps.SetName(gcSnap)
	ps.SetLabels(map[string]string{contract.LabelEngine: contract.EngineCRIU})
	if withFinalizer {
		ps.SetFinalizers([]string{ArchiveFinalizer})
	}
	ps.Object["spec"] = map[string]interface{}{
		"sourcePod": "web-0", "sourcePodUID": "abcdef12-0000", "container": "app", "nodeName": "node-a",
		"bucket": gcBucket, "path": gcPath,
	}
	if ready {
		ps.Object["status"] = map[string]interface{}{"conditions": []interface{}{
			map[string]interface{}{"type": contract.CondCheckpoint, "status": "True", "reason": contract.ReasonSucceeded, "message": "ok", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
			map[string]interface{}{"type": contract.CondReady, "status": "True", "reason": contract.ReasonSucceeded, "message": "ok", "lastTransitionTime": time.Now().UTC().Format(time.RFC3339)},
		}}
	}
	return ps
}

func gcReconcile(t *testing.T, r *ArchiveGCReconciler) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: gcNS, Name: gcSnap}})
}

func gcGet(t *testing.T, c client.Client) (*unstructured.Unstructured, bool) {
	t.Helper()
	ps := contract.New(contract.GVKPodSnapshot)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: gcNS, Name: gcSnap}, ps)
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatalf("getting PodSnapshot: %v", err)
	}
	return ps, true
}

func gcDelete(t *testing.T, c client.Client) {
	t.Helper()
	ps := contract.New(contract.GVKPodSnapshot)
	ps.SetNamespace(gcNS)
	ps.SetName(gcSnap)
	if err := c.Delete(context.Background(), ps); err != nil {
		t.Fatalf("deleting PodSnapshot: %v", err)
	}
}

// The engine puts its finalizer on its PodSnapshots, including ones created
// before the upgrade.
func TestArchiveGC_AddsFinalizer(t *testing.T) {
	c := newClient(t, gcSnapshot(false, true))
	r := &ArchiveGCReconciler{Client: c, Store: gcStoreWithArchive()}
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ps, _ := gcGet(t, c)
	if !controllerutil.ContainsFinalizer(ps, ArchiveFinalizer) {
		t.Fatalf("finalizers = %v, want %s", ps.GetFinalizers(), ArchiveFinalizer)
	}
}

// Another engine's PodSnapshots are left alone.
func TestArchiveGC_IgnoresOtherEngines(t *testing.T) {
	ps := gcSnapshot(false, true)
	ps.SetLabels(nil)
	c := newClient(t, ps)
	r := &ArchiveGCReconciler{Client: c, Store: gcStoreWithArchive()}
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got, _ := gcGet(t, c)
	if len(got.GetFinalizers()) != 0 {
		t.Fatalf("finalizers = %v on another engine's PodSnapshot, want none", got.GetFinalizers())
	}
}

// Deleting the PodSnapshot deletes its archive, and only then releases it.
func TestArchiveGC_DeletesArchiveThenReleases(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	store := gcStoreWithArchive()
	rec := record.NewFakeRecorder(10)
	r := &ArchiveGCReconciler{Client: c, Store: store, Recorder: rec}
	gcDelete(t, c)
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if left := store.archiveLeft(); len(left) != 0 {
		t.Fatalf("archive objects left after release: %v", left)
	}
	if _, ok := store.objects[gcBucket+"/"+path.Join(ArchivePrefix(gcPath, "ps-other-00000000"), "checkpoint.tar")]; !ok {
		t.Fatal("deleted another snapshot's archive")
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists after its archive was deleted, want the finalizer removed")
	}
	// The manifest marks an archive complete, so it goes first.
	if len(store.deletes) == 0 || !strings.HasSuffix(store.deletes[0], "/manifest.json") {
		t.Fatalf("deletes = %v, want manifest.json first", store.deletes)
	}
}

// A missing archive is success.
func TestArchiveGC_MissingArchiveIsSuccess(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	r := &ArchiveGCReconciler{Client: c, Store: &gcStore{memStore: newMemStore()}}
	gcDelete(t, c)
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists, want it released when the archive is already gone")
	}
}

// A work order without a bucket has no archive.
func TestArchiveGC_NoBucketReleasesRightAway(t *testing.T) {
	ps := gcSnapshot(true, false)
	_ = unstructured.SetNestedField(ps.Object, "", "spec", "bucket")
	c := newClient(t, ps)
	store := &gcStore{memStore: newMemStore()}
	r := &ArchiveGCReconciler{Client: c, Store: store}
	gcDelete(t, c)
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("bucketless PodSnapshot still exists")
	}
	if len(store.deletes) != 0 {
		t.Fatalf("deletes = %v for a work order without a bucket, want none", store.deletes)
	}
}

// A failed delete keeps the finalizer, is retried, and is reported.
func TestArchiveGC_DeleteFailureRetriesAndKeepsFinalizer(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	store := gcStoreWithArchive()
	store.deleteErr = errors.New("gcs delete: HTTP 403")
	rec := record.NewFakeRecorder(10)
	r := &ArchiveGCReconciler{Client: c, Store: store, Recorder: rec}
	gcDelete(t, c)
	if _, err := gcReconcile(t, r); err == nil {
		t.Fatal("Reconcile = nil, want the delete error so it's retried with backoff")
	}
	ps, ok := gcGet(t, c)
	if !ok || !controllerutil.ContainsFinalizer(ps, ArchiveFinalizer) {
		t.Fatal("finalizer removed although the archive wasn't deleted")
	}
	select {
	case ev := <-rec.Events:
		if !strings.Contains(ev, corev1.EventTypeWarning) || !strings.Contains(ev, ReasonArchiveDeleteFailed) {
			t.Fatalf("event = %q, want a Warning %s", ev, ReasonArchiveDeleteFailed)
		}
	default:
		t.Fatal("no event for the failed delete")
	}

	store.deleteErr = nil
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("retry Reconcile: %v", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists after the retry deleted the archive")
	}
}

// A delete that keeps failing gives up after a bound, so a namespace delete
// or an uninstall never wedges on the finalizer; the leak is reported.
func TestArchiveGC_GivesUpAfterBound(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	store := gcStoreWithArchive()
	store.deleteErr = errors.New("gcs delete: HTTP 403")
	rec := record.NewFakeRecorder(10)
	r := &ArchiveGCReconciler{Client: c, Store: store, Recorder: rec, GiveUpAfter: time.Minute}
	gcDelete(t, c)
	// Even long after the delete was requested (the engine controller was
	// down), the first failure doesn't give up: the bound runs from it.
	start := time.Now().Add(time.Hour)
	r.now = func() time.Time { return start }
	if _, err := gcReconcile(t, r); err == nil {
		t.Fatal("first failed attempt gave up, want a retry")
	}
	if _, ok := gcGet(t, c); !ok {
		t.Fatal("PodSnapshot released on the first failure")
	}
	r.now = func() time.Time { return start.Add(2 * time.Minute) }
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists past the give-up bound")
	}
	found := false
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, ReasonArchiveDeleteAbandoned) {
			found = true
		}
	}
	if !found {
		t.Fatalf("no %s event for the leaked archive", ReasonArchiveDeleteAbandoned)
	}
}

// A work order without an outcome may still be uploading: wait for the node
// agent to record one (deletion cancels it), so the upload can't recreate
// objects after the delete. Bounded: past SettleWait it goes ahead.
func TestArchiveGC_WaitsForUnfinishedWorkOrder(t *testing.T) {
	c := newClient(t, gcSnapshot(true, false))
	store := gcStoreWithArchive()
	r := &ArchiveGCReconciler{Client: c, Store: store, SettleWait: time.Minute}
	gcDelete(t, c)
	res, err := gcReconcile(t, r)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v, want a recheck while the work order is unfinished", res.RequeueAfter)
	}
	if len(store.deletes) != 0 {
		t.Fatalf("deleted %v while the node agent may still be uploading", store.deletes)
	}
	if _, ok := gcGet(t, c); !ok {
		t.Fatal("PodSnapshot released while the work order is unfinished")
	}

	r.now = func() time.Time { return time.Now().Add(2 * time.Minute) }
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile past SettleWait: %v", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists past SettleWait")
	}
	if left := store.archiveLeft(); len(left) != 0 {
		t.Fatalf("archive objects left: %v", left)
	}
}

// Every object the node agent uploads for a snapshot is deleted when the
// PodSnapshot goes away: run the real pipeline, then the GC.
func TestArchiveGC_DeletesEverythingThePipelineUploads(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	ps.SetFinalizers([]string{ArchiveFinalizer})
	c := newClient(t, ps)
	store := newMemStore()
	pipe := &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: store}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}
	if _, err := pipe.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) == 0 {
		t.Fatal("test setup: the pipeline uploaded nothing")
	}
	if err := c.Delete(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	r := &ArchiveGCReconciler{Client: c, Store: &gcStore{memStore: store}}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(store.objects) != 0 {
		t.Fatalf("objects left after the GC: %v (add them to ArchiveObjects)", store.objects)
	}
}

// Composite upload parts left behind by source node agent cleanup failure
// must be swept by ArchiveGC.
func TestArchiveGC_SweepsCompositeParts(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	store := newMemStore()
	prefix := ArchivePrefix(gcPath, gcSnap)
	store.objects[gcBucket+"/"+path.Join(prefix, "manifest.json")] = []byte("{}")
	store.objects[gcBucket+"/"+path.Join(prefix, "checkpoint.tar")] = []byte("tar-data")
	store.objects[gcBucket+"/"+path.Join(prefix, "checkpoint.tar.part-000")] = []byte("part-0")
	store.objects[gcBucket+"/"+path.Join(prefix, "checkpoint.tar.part-001")] = []byte("part-1")
	store.objects[gcBucket+"/"+path.Join(prefix, "checkpoint.tar.part-002")] = []byte("part-2")

	gcDelete(t, c)
	r := &ArchiveGCReconciler{Client: c, Store: &gcStore{memStore: store}}
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	for obj := range store.objects {
		if strings.Contains(obj, "checkpoint.tar.part-") {
			t.Fatalf("composite part object left behind by ArchiveGC: %s", obj)
		}
	}
	if len(store.objects) != 0 {
		t.Fatalf("objects left after ArchiveGC: %v", store.objects)
	}
}

// The engine controller adds the finalizer when it creates a work order, so a
// PodSnapshot deleted before the GC reconciler first sees it still has its
// archive cleaned up.
func TestPSMT_CreatesWorkOrderWithArchiveFinalizer(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	reconcilePSMT(t, c, nil, "psmt-web-0-deadbeef")
	ps := getSnapshot(t, c, contract.PodSnapshotName("web-0", "deadbeef-0000-1111"))
	if !controllerutil.ContainsFinalizer(ps, ArchiveFinalizer) {
		t.Fatalf("work order finalizers = %v, want %s", ps.GetFinalizers(), ArchiveFinalizer)
	}
}

// A PodSnapshot that's being deleted (its archive is going away) is never
// handed to a trigger re-created for the same pod instance: the trigger
// waits for it to be gone, and then gets a new work order.
func TestPSMT_DoesNotAnswerWithDeletingSnapshot(t *testing.T) {
	name := contract.PodSnapshotName("web-0", "deadbeef-0000-1111")
	old := contract.New(contract.GVKPodSnapshot)
	old.SetNamespace("default")
	old.SetName(name)
	old.SetLabels(map[string]string{contract.LabelEngine: contract.EngineCRIU})
	old.SetFinalizers([]string{ArchiveFinalizer})
	old.Object["spec"] = map[string]interface{}{"sourcePod": "web-0", "sourcePodUID": "deadbeef-0000-1111", "container": "c0", "nodeName": "node-a", "bucket": "bkt", "path": "snaps"}
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
		old,
	)
	if err := c.Delete(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	r := &PSMTReconciler{Client: c}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "psmt-web-0-deadbeef"}})
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v, want a wait for the deleting snapshot", res.RequeueAfter)
	}
	psmt := contract.New(contract.GVKManualTrigger)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "psmt-web-0-deadbeef"}, psmt); err != nil {
		t.Fatal(err)
	}
	if got := contract.TriggerSnapshotCreated(psmt); got != "" {
		t.Fatalf("trigger answered with %q, a snapshot being deleted", got)
	}
}

// The engine controller's ClusterRole lets it add and remove the finalizer.
func TestDeployManifestGrantsArchiveFinalizer(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range strings.Split(string(raw), "\n---") {
		var cr rbacv1.ClusterRole
		if err := k8syaml.Unmarshal([]byte(doc), &cr); err != nil || cr.Kind != "ClusterRole" || cr.Name != "pod-migrate-psengine-controller" {
			continue
		}
		verbs := map[string]bool{}
		for _, rule := range cr.Rules {
			for _, g := range rule.APIGroups {
				for _, res := range rule.Resources {
					if g == contract.GVKPodSnapshot.Group && res == "podsnapshots" {
						for _, v := range rule.Verbs {
							verbs[v] = true
						}
					}
				}
			}
		}
		if !verbs["update"] || !verbs["patch"] {
			t.Fatalf("pod-migrate-psengine-controller verbs on podsnapshots = %v, want update and patch for the archive finalizer", verbs)
		}
		if !verbs["delete"] {
			t.Fatalf("pod-migrate-psengine-controller verbs on podsnapshots = %v, want delete for a work order an earlier trigger left (#262)", verbs)
		}
		return
	}
	t.Fatal("ClusterRole pod-migrate-psengine-controller not found in deploy.yaml")
}

// The finalizer patch checks the resourceVersion: a stale copy never drops a
// finalizer another writer added since.
func TestArchiveGC_FinalizerPatchKeepsOtherWriters(t *testing.T) {
	c := newClient(t, gcSnapshot(true, true))
	stale, _ := gcGet(t, c)
	fresh, _ := gcGet(t, c)
	fresh.SetFinalizers(append(fresh.GetFinalizers(), "example.com/other"))
	if err := c.Update(context.Background(), fresh); err != nil {
		t.Fatal(err)
	}
	r := &ArchiveGCReconciler{Client: c, Store: gcStoreWithArchive()}
	if err := r.release(context.Background(), stale); !apierrors.IsConflict(err) {
		t.Fatalf("release with a stale copy = %v, want a conflict", err)
	}
	got, _ := gcGet(t, c)
	if !controllerutil.ContainsFinalizer(got, "example.com/other") {
		t.Fatalf("finalizers = %v, another writer's finalizer was dropped", got.GetFinalizers())
	}
}

// A finalizer patch that fails on the give-up path (a conflict) must not
// restart the give-up clock or record the abandon twice.
func TestArchiveGC_GiveUpReleaseConflictKeepsClock(t *testing.T) {
	failPatch := 1
	c := newClientWithInterceptor(t, interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if obj.GetDeletionTimestamp() != nil && failPatch > 0 {
				failPatch--
				return apierrors.NewConflict(schema.GroupResource{Resource: "podsnapshots"}, obj.GetName(), errors.New("stale"))
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}, gcSnapshot(true, true))
	store := gcStoreWithArchive()
	store.deleteErr = errors.New("gcs delete: HTTP 403")
	rec := record.NewFakeRecorder(20)
	r := &ArchiveGCReconciler{Client: c, Store: store, Recorder: rec, GiveUpAfter: time.Minute}
	gcDelete(t, c)
	start := time.Now()
	r.now = func() time.Time { return start }
	if _, err := gcReconcile(t, r); err == nil {
		t.Fatal("first failed attempt gave up, want a retry")
	}
	// Past the bound: the release hits a conflict.
	r.now = func() time.Time { return start.Add(2 * time.Minute) }
	if _, err := gcReconcile(t, r); !apierrors.IsConflict(err) {
		t.Fatalf("Reconcile = %v, want the release conflict", err)
	}
	// The immediate retry still sees the original first failure and releases.
	if _, err := gcReconcile(t, r); err != nil {
		t.Fatalf("retry after the conflict: %v (the give-up clock restarted?)", err)
	}
	if _, ok := gcGet(t, c); ok {
		t.Fatal("PodSnapshot still exists after the retry past the give-up bound")
	}
	abandoned := 0
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; strings.Contains(ev, ReasonArchiveDeleteAbandoned) {
			abandoned++
		}
	}
	if abandoned != 1 {
		t.Fatalf("%d %s events, want exactly 1", abandoned, ReasonArchiveDeleteAbandoned)
	}
}
