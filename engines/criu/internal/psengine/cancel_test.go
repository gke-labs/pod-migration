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

// C9 cancel (#106): deleting the PodSnapshotManualTrigger means start nothing
// new against the source and stop what can still be stopped. The irreversible
// step is the source stop, so it must never run once the trigger is gone.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const (
	cancelTrigger  = "psmt-web-0-deadbeef"
	cancelSnapshot = "ps-web-0-deadbeef"
)

var cancelSnapshotKey = types.NamespacedName{Namespace: "default", Name: cancelSnapshot}

// cancelFixture runs the controller half against a live trigger, so the
// PodSnapshot work order exists exactly as production creates it.
func cancelFixture(t *testing.T) client.Client {
	t.Helper()
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtFor("web-0"),
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	return c
}

// deleteTriggerErr is what the orchestrator does when it gives up on a job in
// Snapshotting. With controllerUp, the controller half then reconciles the
// deletion; without it, only the node agent can notice.
func deleteTriggerErr(c client.Client, controllerUp bool) error {
	psmt := contract.New(contract.GVKManualTrigger)
	psmt.SetNamespace("default")
	psmt.SetName(cancelTrigger)
	if err := c.Delete(context.Background(), psmt); err != nil {
		return err
	}
	if !controllerUp {
		return nil
	}
	_, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}})
	return err
}

func deleteTrigger(t *testing.T, c client.Client, controllerUp bool) {
	t.Helper()
	if err := deleteTriggerErr(c, controllerUp); err != nil {
		t.Fatal(err)
	}
}

func runAgent(t *testing.T, r *NodeSnapshotReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: cancelSnapshotKey}); err != nil {
		t.Fatal(err)
	}
}

// requireCancelRequested: the controller half's request is not terminal, so
// the orchestrator keeps waiting until the agent confirms.
func requireCancelRequested(t *testing.T, c client.Client) {
	t.Helper()
	ps := getSnapshot(t, c, cancelSnapshot)
	if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
		t.Fatalf("a cancel request must not be terminal: %+v", contract.Conditions(ps))
	}
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != contract.ReasonCancelRequested {
		t.Fatalf("want Checkpoint=Unknown/%s, got %+v", contract.ReasonCancelRequested, cond)
	}
	if ready := condition(ps, contract.CondReady); ready != nil {
		t.Fatalf("a cancel request must not touch Ready: %+v", ready)
	}
}

func requireCancelled(t *testing.T, c client.Client) *metav1.Condition {
	t.Helper()
	ps := getSnapshot(t, c, cancelSnapshot)
	if contract.IsSnapshotReady(ps) {
		t.Fatalf("cancelled snapshot must not be ready: %+v", contract.Conditions(ps))
	}
	for _, typ := range []string{contract.CondCheckpoint, contract.CondReady} {
		cond := condition(ps, typ)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonCancelled {
			t.Fatalf("want %s=False/%s, got %+v", typ, contract.ReasonCancelled, cond)
		}
	}
	return condition(ps, contract.CondCheckpoint)
}

// hookStore runs onPut before storing the named object.
type hookStore struct {
	*memStore
	object string
	onPut  func()
}

func (h *hookStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	if h.onPut != nil && strings.HasSuffix(object, h.object) {
		h.onPut()
	}
	return h.memStore.Put(ctx, bucket, object, r)
}

// podSnapshotToTrigger only enqueues the trigger of an unfinished CRIU work
// order that hasn't been asked to cancel yet.
func TestPodSnapshotToTriggerSkipsSettledAndForeign(t *testing.T) {
	cond := func(typ string, status metav1.ConditionStatus, reason string) func(*unstructured.Unstructured) {
		return func(ps *unstructured.Unstructured) {
			if err := contract.SetCondition(ps, metav1.Condition{Type: typ, Status: status, Reason: reason}); err != nil {
				t.Fatal(err)
			}
		}
	}
	cases := map[string]func(*unstructured.Unstructured){
		"Ready":           cond(contract.CondReady, metav1.ConditionTrue, contract.ReasonSucceeded),
		"Failed":          cond(contract.CondCheckpoint, metav1.ConditionFalse, contract.ReasonFailed),
		"Refused":         cond(contract.CondCheckpoint, metav1.ConditionFalse, contract.ReasonRefused),
		"Cancelled":       cond(contract.CondCheckpoint, metav1.ConditionFalse, contract.ReasonCancelled),
		"CancelRequested": cond(contract.CondCheckpoint, metav1.ConditionUnknown, contract.ReasonCancelRequested),
		"another engine":  func(ps *unstructured.Unstructured) { ps.SetLabels(map[string]string{contract.LabelEngine: "gke"}) },
		"no engine label": func(ps *unstructured.Unstructured) { ps.SetLabels(nil) },
		"no trigger name": func(ps *unstructured.Unstructured) {
			unstructured.RemoveNestedField(ps.Object, "spec", specTriggerName)
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ps := workOrderSnapshot(cancelSnapshot)
			_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
			if reqs := podSnapshotToTrigger(context.Background(), ps); len(reqs) != 1 {
				t.Fatalf("test setup: the unmodified work order must enqueue its trigger, got %v", reqs)
			}
			mutate(ps)
			if reqs := podSnapshotToTrigger(context.Background(), ps); len(reqs) != 0 {
				t.Fatalf("want no request, got %v", reqs)
			}
		})
	}
}

func TestPSMTRecordsTriggerOnWorkOrder(t *testing.T) {
	c := cancelFixture(t)
	ps := getSnapshot(t, c, cancelSnapshot)
	if got := snapshotTrigger(ps); got != cancelTrigger {
		t.Fatalf("work order trigger = %q, want %q", got, cancelTrigger)
	}
	reqs := podSnapshotToTrigger(context.Background(), ps)
	if len(reqs) != 1 || reqs[0].NamespacedName != (types.NamespacedName{Namespace: "default", Name: cancelTrigger}) {
		t.Fatalf("a PodSnapshot must enqueue its trigger, got %v", reqs)
	}
}

// (a) The trigger is deleted before the node agent starts: no checkpoint, no
// stop, and the agent confirms the controller half's request as Cancelled.
func TestCancelTriggerDeletedBeforeAgentStarts(t *testing.T) {
	c := cancelFixture(t)
	deleteTrigger(t, c, true)
	requireCancelRequested(t, c)

	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store})

	if len(ops.events) != 0 {
		t.Fatalf("a cancelled work order must not start: %v", ops.events)
	}
	if len(store.order) != 0 {
		t.Fatalf("a cancelled work order must not upload: %v", store.order)
	}
	if cond := requireCancelled(t, c); !strings.Contains(cond.Message, "not touched") {
		t.Fatalf("the cancel message must say the source was not touched, got %q", cond.Message)
	}
}

// (a, controller down) The engine controller never saw the deletion: the
// agent's own live read of the trigger before it starts refuses the work.
func TestCancelAgentSeesDeletedTriggerBeforeStart(t *testing.T) {
	c := cancelFixture(t)
	deleteTrigger(t, c, false)

	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore()})

	if len(ops.events) != 0 {
		t.Fatalf("a work order whose trigger is gone must not start: %v", ops.events)
	}
	requireCancelled(t, c)
}

// (b) The trigger is deleted after the checkpoint and the upload, right
// before the stop: the source must not be stopped, and the outcome is
// Cancelled, not Succeeded.
func TestCancelAfterUploadSkipsStop(t *testing.T) {
	for name, controllerUp := range map[string]bool{"controller half up": true, "controller half down": false} {
		t.Run(name, func(t *testing.T) {
			c := cancelFixture(t)
			ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
			store := &hookStore{memStore: newMemStore(), object: "manifest.json", onPut: func() { deleteTrigger(t, c, controllerUp) }}
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

			if len(ops.stopped) != 0 {
				t.Fatalf("source stopped after its trigger was deleted: events %v", ops.events)
			}
			cond := requireCancelled(t, c)
			if !strings.Contains(cond.Message, "not stopped") {
				t.Fatalf("the cancel message must say the source was left running, got %q", cond.Message)
			}
		})
	}
}

// The commit point: a trigger deleted once the stop has begun is not seen.
// The source is stopped and Ready=True wins, even over the controller half's
// CancelRequested, so the orchestrator reads a completed checkpoint, which is
// the truth.
func TestTriggerDeletedDuringStopStaysReady(t *testing.T) {
	for name, controllerUp := range map[string]bool{"controller half up": true, "controller half down": false} {
		t.Run(name, func(t *testing.T) {
			c := cancelFixture(t)
			ops := &fakeNodeOps{tarContent: "TARBYTES", t: t, beforeStop: func() { deleteTrigger(t, c, controllerUp) }}
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CancelPollInterval: time.Millisecond})
			if len(ops.stopped) != 1 {
				t.Fatalf("want the committed stop: events %v", ops.events)
			}
			if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) {
				t.Fatalf("want Ready=True after the committed stop, got %+v", contract.Conditions(ps))
			}
		})
	}
}

// (b, earlier) The trigger is deleted once the checkpoint is taken: nothing
// is uploaded and the source is not stopped.
func TestCancelAfterCheckpointSkipsUploadAndStop(t *testing.T) {
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t, afterCheckpoint: func() { deleteTrigger(t, c, true) }}
	store := newMemStore()
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

	if len(ops.stopped) != 0 {
		t.Fatalf("source stopped after its trigger was deleted: events %v", ops.events)
	}
	if len(store.order) != 0 {
		t.Fatalf("nothing should be uploaded after the cancel: %v", store.order)
	}
	requireCancelled(t, c)
}

// A cancel during the kubelet checkpoint call does not cancel the call:
// cancelling it wouldn't end the freeze (#106). The agent waits for the call
// to return, so Cancelled is only written once the source runs again, then
// uploads nothing and doesn't stop the source.
func TestCancelDuringCheckpointWaitsForTheCall(t *testing.T) {
	c := cancelFixture(t)
	started, release := make(chan struct{}), make(chan struct{})
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t, blockCheckpoint: started, releaseCheckpoint: release}
	store := newMemStore()
	errs := make(chan error, 1)
	go func() {
		<-started
		err := deleteTriggerErr(c, true)
		// While the call is still running the snapshot must not look settled.
		if err == nil {
			ps := contract.New(contract.GVKPodSnapshot)
			if err = c.Get(context.Background(), cancelSnapshotKey, ps); err == nil && hasTerminalCheckpoint(ps) {
				err = errors.New("terminal outcome written while the checkpoint call was still running")
			}
		}
		// Give the 5ms poller time to see the request before the call returns,
		// and check again: the poller must not have written an outcome either.
		time.Sleep(50 * time.Millisecond)
		if err == nil {
			ps := contract.New(contract.GVKPodSnapshot)
			if err = c.Get(context.Background(), cancelSnapshotKey, ps); err == nil && hasTerminalCheckpoint(ps) {
				err = errors.New("terminal outcome written after the poller ticked, while the checkpoint call was still running")
			}
		}
		close(release)
		errs <- err
	}()
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: 5 * time.Millisecond})
	if err := <-errs; err != nil {
		t.Fatal(err)
	}

	if ops.checkpointCtxCancelled {
		t.Fatal("the kubelet checkpoint call must not be cancelled")
	}
	if len(ops.stopped) != 0 || len(store.order) != 0 {
		t.Fatalf("after a cancel during the checkpoint: events %v, uploads %v", ops.events, store.order)
	}
	requireCancelled(t, c)
}

// blockingStore blocks the Put of the named object until its context ends.
type blockingStore struct {
	*memStore
	object    string
	started   chan struct{}
	cancelled bool
}

func (b *blockingStore) Put(ctx context.Context, bucket, object string, r io.Reader) (*ObjectInfo, error) {
	if strings.HasSuffix(object, b.object) {
		close(b.started)
		select {
		case <-ctx.Done():
			b.cancelled = true
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return nil, errors.New("upload was never cancelled")
		}
	}
	return b.memStore.Put(ctx, bucket, object, r)
}

// A cancel during the upload aborts the upload.
func TestCancelInterruptsUpload(t *testing.T) {
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &blockingStore{memStore: newMemStore(), object: "checkpoint.tar", started: make(chan struct{})}
	errs := make(chan error, 1)
	go func() {
		<-store.started
		errs <- deleteTriggerErr(c, true)
	}()
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: 5 * time.Millisecond})
	if err := <-errs; err != nil {
		t.Fatal(err)
	}

	if !store.cancelled {
		t.Fatal("the in-flight upload was not cancelled")
	}
	if _, ok := store.objects["bkt/snaps/"+cancelSnapshot+"/manifest.json"]; ok {
		t.Fatal("no manifest may be written after the cancel")
	}
	if len(ops.stopped) != 0 {
		t.Fatalf("source stopped after its trigger was deleted: events %v", ops.events)
	}
	requireCancelled(t, c)
}

// (c) The trigger still exists: the source is frozen before the checkpoint
// and kept frozen after the upload, and the snapshot goes Ready.
func TestCancelNormalPathStillStops(t *testing.T) {
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Millisecond})

	if got := strings.Join(ops.events, ","); got != "freeze,checkpoint,sourceinfo,commit" {
		t.Fatalf("event order: %s", got)
	}
	if len(store.order) != 3 {
		t.Fatalf("write order: %v", store.order)
	}
	if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) {
		t.Fatalf("snapshot not ready: %+v", contract.Conditions(ps))
	}
}

// A trigger created again under the same name before the agent acts takes
// the work order back: the request is replaced by Pending and the pipeline
// runs normally.
func TestRecreatedTriggerRevokesCancelRequest(t *testing.T) {
	c := cancelFixture(t)
	deleteTrigger(t, c, true)
	requireCancelRequested(t, c)
	if err := c.Create(context.Background(), psmtFor("web-0")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	if cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint); cond == nil || cond.Reason != contract.ReasonPending {
		t.Fatalf("want the request replaced by Pending, got %+v", cond)
	}
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore()})
	if len(ops.stopped) != 1 || !contract.IsSnapshotReady(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatalf("want the normal path for the new trigger: events %v", ops.events)
	}
}

// A trigger that is being deleted (finalizer pending) cancels like a deleted one.
func TestPSMTBeingDeletedRequestsCancel(t *testing.T) {
	c := cancelFixture(t)
	psmt := contract.New(contract.GVKManualTrigger)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: cancelTrigger}, psmt); err != nil {
		t.Fatal(err)
	}
	psmt.SetFinalizers([]string{"example.com/hold"})
	if err := c.Update(context.Background(), psmt); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), psmt); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	requireCancelRequested(t, c)
}

// Deleting the trigger after the snapshot is Ready is the normal Evicting
// cleanup: the snapshot must stay Ready, since the replacement restores it.
func TestPSMTDeletionKeepsReadySnapshot(t *testing.T) {
	c := cancelFixture(t)
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: newMemStore()})
	deleteTrigger(t, c, true)
	if ps := getSnapshot(t, c, cancelSnapshot); !contract.IsSnapshotReady(ps) {
		t.Fatalf("a Ready snapshot must survive its trigger's deletion: %+v", contract.Conditions(ps))
	}
}

// A deleted trigger cancels only its own work order; a work order from an
// older engine (no trigger name) is left alone.
func TestPSMTDeletionIgnoresOtherWorkOrders(t *testing.T) {
	c := cancelFixture(t)
	other := workOrderSnapshot("ps-other-0-cafebabe")
	_ = unstructured.SetNestedField(other.Object, "psmt-other-0-cafebabe", "spec", specTriggerName)
	legacy := workOrderSnapshot("ps-legacy-0-cafebabe")
	for _, ps := range []*unstructured.Unstructured{other, legacy} {
		if err := c.Create(context.Background(), ps); err != nil {
			t.Fatal(err)
		}
	}
	deleteTrigger(t, c, true)
	requireCancelRequested(t, c)
	for _, name := range []string{"ps-other-0-cafebabe", "ps-legacy-0-cafebabe"} {
		if cond := condition(getSnapshot(t, c, name), contract.CondCheckpoint); cond != nil {
			t.Fatalf("%s was touched: %+v", name, cond)
		}
	}
}

// A failed pre-stop read must not stop the source: if the agent can't confirm
// the trigger still wants the stop, it leaves the source running.
func TestCancelCheckErrorBeforeStopLeavesSourceRunning(t *testing.T) {
	fastPreStopBackoff(t)
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	reader := &flakyReader{Reader: c}
	store := &hookStore{memStore: newMemStore(), object: "manifest.json", onPut: func() { reader.fail = true }}
	runAgent(t, &NodeSnapshotReconciler{Client: c, APIReader: reader, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})

	if len(ops.stopped) != 0 {
		t.Fatalf("source stopped although the trigger check failed: events %v", ops.events)
	}
	ps := getSnapshot(t, c, cancelSnapshot)
	if contract.IsSnapshotReady(ps) {
		t.Fatal("snapshot must not be ready when the source was not stopped")
	}
	if cond := condition(ps, contract.CondCheckpoint); cond == nil || cond.Reason != contract.ReasonFailed {
		t.Fatalf("want Failed, got %+v", cond)
	}
}

type flakyReader struct {
	client.Reader
	fail bool
}

func (f *flakyReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if f.fail {
		return errors.New("apiserver unavailable")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

// staleSnapshotClient serves a fixed, stale copy of one PodSnapshot from Get,
// the way a lagging informer cache would; everything else, including the
// status writes, goes to the real client.
type staleSnapshotClient struct {
	client.Client
	key   types.NamespacedName
	stale *unstructured.Unstructured
}

func (s *staleSnapshotClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && key == s.key && u.GetKind() == contract.GVKPodSnapshot.Kind {
		s.stale.DeepCopyInto(u)
		return nil
	}
	return s.Client.Get(ctx, key, obj, opts...)
}

func outcomeCount(reason string) float64 {
	return testutil.ToFloat64(checkpointOutcomes.WithLabelValues(reason))
}

// A reconcile that reads a stale Pending copy after the snapshot went Ready
// must neither work it again nor change its outcome. With the trigger
// deleted (the normal Evicting cleanup) it must not turn the completed
// snapshot into Cancelled: the source is already stopped. With the trigger
// still there, it must not checkpoint and stop the source a second time.
func TestStaleReadAfterReadyDoesNotRerunOrCancel(t *testing.T) {
	for name, deleted := range map[string]bool{"trigger deleted": true, "trigger present": false} {
		t.Run(name, func(t *testing.T) {
			c := cancelFixture(t)
			stale := getSnapshot(t, c, cancelSnapshot).DeepCopy()
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: newMemStore()})
			if deleted {
				deleteTrigger(t, c, true)
			}
			want := contract.Conditions(getSnapshot(t, c, cancelSnapshot))
			before := outcomeCount(contract.ReasonCancelled)

			ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
			runAgent(t, &NodeSnapshotReconciler{
				Client:    &staleSnapshotClient{Client: c, key: cancelSnapshotKey, stale: stale},
				APIReader: c, NodeName: "node-a", Ops: ops, Store: newMemStore(),
			})

			if len(ops.events) != 0 {
				t.Fatalf("a finished snapshot must not be worked again: %v", ops.events)
			}
			if got := contract.Conditions(getSnapshot(t, c, cancelSnapshot)); fmt.Sprint(got) != fmt.Sprint(want) {
				t.Fatalf("a Ready snapshot was overwritten:\n got %+v\nwant %+v", got, want)
			}
			if got := outcomeCount(contract.ReasonCancelled); got != before {
				t.Fatalf("a skipped outcome was counted: Cancelled %v -> %v", before, got)
			}
		})
	}
}

// If the snapshot gets an outcome from elsewhere while the agent uploads, the
// live read before the stop sees it and the source is not stopped.
func TestOutcomeBeforeStopSkipsStop(t *testing.T) {
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &hookStore{memStore: newMemStore(), object: "manifest.json", onPut: func() {
		if err := (&PSMTReconciler{Client: c}).setSnapshotTerminal(context.Background(), "default", cancelSnapshot, contract.ReasonFailed, "elsewhere"); err != nil {
			t.Fatal(err)
		}
	}}
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour})
	if len(ops.stopped) != 0 {
		t.Fatalf("source stopped although the snapshot already had an outcome: events %v", ops.events)
	}
	if cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint); cond == nil || cond.Message != "elsewhere" {
		t.Fatalf("the existing outcome must stand, got %+v", cond)
	}
}

// finish never overwrites an outcome already recorded (matching
// patchSnapshotConditions), and doesn't count a write it skipped.
func TestFinishKeepsExistingOutcome(t *testing.T) {
	outcomes := map[string]func(t *testing.T, c client.Client){
		"Ready": func(t *testing.T, c client.Client) {
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: newMemStore()})
		},
		contract.ReasonRefused: func(t *testing.T, c client.Client) {
			if err := (&PSMTReconciler{Client: c}).setSnapshotTerminal(context.Background(), "default", cancelSnapshot, contract.ReasonRefused, "policy"); err != nil {
				t.Fatal(err)
			}
		},
		contract.ReasonCancelled: func(t *testing.T, c client.Client) {
			deleteTrigger(t, c, true)
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{t: t}, Store: newMemStore()})
		},
	}
	for name, setup := range outcomes {
		for _, reason := range []string{contract.ReasonCancelled, contract.ReasonFailed, contract.ReasonSucceeded} {
			t.Run(name+" then "+reason, func(t *testing.T) {
				c := cancelFixture(t)
				setup(t, c)
				want := contract.Conditions(getSnapshot(t, c, cancelSnapshot))
				before := outcomeCount(reason)
				r := &NodeSnapshotReconciler{Client: c, NodeName: "node-a"}
				if err := r.finish(context.Background(), ctrl.Request{NamespacedName: cancelSnapshotKey}, reason, "late"); err != nil {
					t.Fatal(err)
				}
				got := contract.Conditions(getSnapshot(t, c, cancelSnapshot))
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Fatalf("outcome overwritten:\n got %+v\nwant %+v", got, want)
				}
				if after := outcomeCount(reason); after != before {
					t.Fatalf("a skipped outcome was counted: %s %v -> %v", reason, before, after)
				}
			})
		}
	}
}

// A work order without a bucket belongs to the controller half (a refusal or
// a storage failure it is about to record): no node agent writes an outcome
// for it, so the controller's Refused isn't pre-empted by an agent's Failed.
func TestAgentLeavesBucketlessWorkOrderToController(t *testing.T) {
	ps := workOrderSnapshot(cancelSnapshot)
	unstructured.RemoveNestedField(ps.Object, "spec", "bucket")
	unstructured.RemoveNestedField(ps.Object, "spec", "path")
	c := newClient(t, ps)
	for _, node := range []string{"node-a", "node-b"} {
		ops := &fakeNodeOps{t: t}
		runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: node, Ops: ops, Store: newMemStore()})
		if len(ops.events) != 0 {
			t.Fatalf("agent on %s acted on a work order without a bucket: %v", node, ops.events)
		}
	}
	if conds := contract.Conditions(getSnapshot(t, c, cancelSnapshot)); len(conds) != 0 {
		t.Fatalf("an agent wrote an outcome for a controller-owned work order: %+v", conds)
	}
	if err := (&PSMTReconciler{Client: c}).setSnapshotTerminal(context.Background(), "default", cancelSnapshot, contract.ReasonRefused, "policy"); err != nil {
		t.Fatal(err)
	}
	if cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint); cond == nil || cond.Reason != contract.ReasonRefused {
		t.Fatalf("want the controller's Refused, got %+v", cond)
	}
}

// The work order carries the label GKE Pod Snapshots puts on its snapshots,
// valued with the trigger's name, so the orchestrator's CheckpointState finds
// an unfinished CRIU snapshot after the trigger is gone even when the job
// never recorded status.snapshotRef. A trigger name too long for a label
// value gets no label rather than a create the API server rejects.
func TestPSMTLabelsWorkOrderWithTrigger(t *testing.T) {
	c := cancelFixture(t)
	if got := getSnapshot(t, c, cancelSnapshot).GetLabels()[contract.LabelTriggeredBy]; got != cancelTrigger {
		t.Fatalf("%s = %q, want %q", contract.LabelTriggeredBy, got, cancelTrigger)
	}

	long := strings.Repeat("p", 50) // psmt-<50>-<uid8> is 64 characters
	pod := sourcePod(long, 1, "node-a")
	trigger := psmtFor(long)
	if len(trigger.GetName()) <= 63 {
		t.Fatalf("test setup: trigger name %q fits in a label value", trigger.GetName())
	}
	c = newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		pod, criuNode("node-a", true), trigger,
	)
	reconcilePSMT(t, c, nil, trigger.GetName())
	ps := getSnapshot(t, c, contract.PodSnapshotName(long, string(pod.UID)))
	if v, ok := ps.GetLabels()[contract.LabelTriggeredBy]; ok {
		t.Fatalf("an invalid label value was set: %q", v)
	}
	if snapshotTrigger(ps) != trigger.GetName() {
		t.Fatalf("the work order must still record its trigger, got %q", snapshotTrigger(ps))
	}
}

// scriptedReader runs onGet for every PodSnapshot Get once armed; a non-nil
// error fails the Get. calls counts the armed PodSnapshot Gets: one per
// pre-stop attempt, then one for finish's read.
type scriptedReader struct {
	client.Reader
	armed bool
	calls int
	onGet func(ctx context.Context, call int) error
}

func (s *scriptedReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && s.armed && u.GetKind() == contract.GVKPodSnapshot.Kind {
		s.calls++
		if err := s.onGet(ctx, s.calls); err != nil {
			return err
		}
	}
	return s.Reader.Get(ctx, key, obj, opts...)
}

// fastPreStopBackoff keeps the production step count and growth but shrinks
// the waits, so the retry tests run quickly.
func fastPreStopBackoff(t *testing.T) {
	t.Helper()
	saved, savedTimeout := preCommitCheckBackoff, preCommitCheckTimeout
	preCommitCheckBackoff.Duration = time.Millisecond
	preCommitCheckTimeout = 50 * time.Millisecond
	t.Cleanup(func() { preCommitCheckBackoff, preCommitCheckTimeout = saved, savedTimeout })
}

func runWithPreStopReader(t *testing.T, ctx context.Context, onGet func(context.Context, int) error) (client.Client, *fakeNodeOps, *scriptedReader) {
	t.Helper()
	c := cancelFixture(t)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	reader := &scriptedReader{Reader: c, onGet: onGet}
	store := &hookStore{memStore: newMemStore(), object: "manifest.json", onPut: func() { reader.armed = true }}
	_, _ = (&NodeSnapshotReconciler{Client: c, APIReader: reader, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour}).
		Reconcile(ctx, ctrl.Request{NamespacedName: cancelSnapshotKey})
	return c, ops, reader
}

// The pre-commit budget rides out a few seconds of API server trouble before
// it fails closed, and bounds each attempt.
func TestPreCommitCheckBudget(t *testing.T) {
	var total time.Duration
	d := preCommitCheckBackoff.Duration
	for i := 1; i < preCommitCheckBackoff.Steps; i++ {
		total += d
		d = time.Duration(float64(d) * preCommitCheckBackoff.Factor)
	}
	if total < 3*time.Second || total > 4*time.Second {
		t.Fatalf("pre-commit retry waits %v in total, want 3-4s", total)
	}
	if preCommitCheckTimeout <= 0 || preCommitCheckTimeout > 3*time.Second {
		t.Fatalf("per-attempt timeout %v, want a bound of a few seconds", preCommitCheckTimeout)
	}
}

// Three failed reads in a row, then a good one: the source is stopped and the
// snapshot goes Ready, instead of a durable archive being marked Failed.
func TestPreStopCheckRidesOutBriefOutage(t *testing.T) {
	fastPreStopBackoff(t)
	c, ops, reader := runWithPreStopReader(t, context.Background(), func(_ context.Context, call int) error {
		if call <= 3 {
			return errors.New("apiserver unavailable")
		}
		return nil
	})
	if len(ops.stopped) != 1 || !contract.IsSnapshotReady(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatalf("want the stop after the outage: events %v, attempts %d, conditions %+v",
			ops.events, reader.calls, contract.Conditions(getSnapshot(t, c, cancelSnapshot)))
	}
}

// Each attempt has its own deadline, so one hung request can't use up the
// budget.
func TestPreStopCheckBoundsEachAttempt(t *testing.T) {
	fastPreStopBackoff(t)
	timedOut := false
	c, ops, _ := runWithPreStopReader(t, context.Background(), func(ctx context.Context, call int) error {
		if call > 1 {
			return nil
		}
		select {
		case <-ctx.Done():
			timedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
			return ctx.Err()
		case <-time.After(2 * time.Second):
			return errors.New("the attempt had no deadline")
		}
	})
	if !timedOut {
		t.Fatal("the hung attempt was not cut off by a per-attempt deadline")
	}
	if len(ops.stopped) != 1 || !contract.IsSnapshotReady(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatalf("want the stop once the hung attempt timed out: events %v", ops.events)
	}
}

// Forbidden won't fix itself, and a cancelled parent means the agent is
// shutting down: neither is retried, and the source is not stopped.
func TestPreStopCheckDoesNotRetryHopelessErrors(t *testing.T) {
	fastPreStopBackoff(t)
	t.Run("forbidden", func(t *testing.T) {
		_, ops, reader := runWithPreStopReader(t, context.Background(), func(context.Context, int) error {
			return apierrors.NewForbidden(schema.GroupResource{Group: contract.Group, Resource: "podsnapshots"}, cancelSnapshot, errors.New("rbac"))
		})
		if len(ops.stopped) != 0 || reader.calls != 2 {
			t.Fatalf("want one attempt (plus finish's read) and no stop: reads %d, events %v", reader.calls, ops.events)
		}
	})
	t.Run("parent cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		_, ops, reader := runWithPreStopReader(t, ctx, func(context.Context, int) error {
			cancel()
			return errors.New("apiserver unavailable")
		})
		// A done parent is a shutdown: no finish, so no read for it either.
		if len(ops.stopped) != 0 || reader.calls != 1 {
			t.Fatalf("want one attempt and no stop: reads %d, events %v", reader.calls, ops.events)
		}
	})
}

// timeoutErr is what the node agent's kubelet client returns when the
// checkpoint call ran out of time.
type timeoutErr struct{}

func (timeoutErr) Error() string { return "kubelet checkpoint: deadline exceeded" }
func (timeoutErr) Timeout() bool { return true }

// A checkpoint call that timed out may have left the dump running with the
// source frozen (#106). The agent records Checkpoint=Unknown with the reason
// CheckpointTimedOut, which is not terminal: a terminal Failed would let the
// orchestrator's checkpoint-settle wait unfence a source that may be frozen,
// while a snapshot that never settles keeps the fence and cold-evicts it. A
// pending cancel isn't confirmed as Cancelled either.
func TestCheckpointTimeoutLeavesSnapshotUnsettled(t *testing.T) {
	for name, cancelPending := range map[string]bool{"no cancel": false, "cancel pending": true} {
		t.Run(name, func(t *testing.T) {
			c := cancelFixture(t)
			ops := &fakeNodeOps{t: t, checkpointErr: fmt.Errorf("wrapped: %w", timeoutErr{})}
			if cancelPending {
				ops.afterCheckpoint = func() { deleteTrigger(t, c, true) }
			}
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: newMemStore(), CancelPollInterval: time.Hour})
			if len(ops.unfrozen) != 0 {
				t.Fatalf("source container was unfrozen after a timed-out checkpoint: %v", ops.events)
			}
			if !contract.IsSourceFrozen(getSnapshot(t, c, cancelSnapshot)) {
				t.Fatalf("source container must remain SourceFrozen=True after timed-out checkpoint")
			}
			requireTimedOut(t, c)

			// Nothing works the snapshot again: not the agent ...
			again := &fakeNodeOps{tarContent: "TARBYTES", t: t}
			runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: again, Store: newMemStore()})
			if len(again.events) != 0 {
				t.Fatalf("a timed-out snapshot was worked again: %v", again.events)
			}
			// ... nor the controller half, re-reconciling a live trigger
			// (Pending) or a deleted one (CancelRequested).
			if !cancelPending {
				reconcilePSMT(t, c, nil, cancelTrigger)
				requireTimedOut(t, c)
				deleteTrigger(t, c, true)
			}
			requireTimedOut(t, c)
			if reqs := podSnapshotToTrigger(context.Background(), getSnapshot(t, c, cancelSnapshot)); len(reqs) != 0 {
				t.Fatalf("a timed-out snapshot must not enqueue its trigger: %v", reqs)
			}
		})
	}
}

func requireTimedOut(t *testing.T, c client.Client) {
	t.Helper()
	ps := getSnapshot(t, c, cancelSnapshot)
	if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
		t.Fatalf("a timed-out checkpoint must not be terminal: %+v", contract.Conditions(ps))
	}
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != contract.ReasonCheckpointTimedOut || !strings.Contains(cond.Message, "frozen") {
		t.Fatalf("want Checkpoint=Unknown/%s saying the source may be frozen, got %+v", contract.ReasonCheckpointTimedOut, cond)
	}
	if ready := condition(ps, contract.CondReady); ready != nil {
		t.Fatalf("a timed-out checkpoint must not set Ready: %+v", ready)
	}
}

// A first pass that withholds the bucket (no storage yet, or the node can't
// be read) and then fails to record that outcome leaves a bucketless
// PodSnapshot behind. If the retry finds the trigger eligible, the work order
// can't be handed to an agent (agents ignore bucketless orders), so it must
// end Failed, as main did, rather than wait as Pending until the job's
// deadline.
func TestPSMTRetryFailsBucketlessWorkOrder(t *testing.T) {
	cases := map[string]struct {
		missing client.Object // absent on the first pass, present on the retry
	}{
		"storage unavailable on the first pass": {missing: pssc("pssc-x", "bkt", "snaps")},
		"node unreadable on the first pass":     {missing: criuNode("node-a", true)},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			objs := []client.Object{
				pssc("pssc-x", "bkt", "snaps"),
				readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
				sourcePod("web-0", 1, "node-a"),
				criuNode("node-a", true),
				psmtFor("web-0"),
			}
			var first []client.Object
			for _, o := range objs {
				if o.GetName() != tc.missing.GetName() || o.GetObjectKind().GroupVersionKind().Kind != tc.missing.GetObjectKind().GroupVersionKind().Kind {
					first = append(first, o)
				}
			}
			failOnce := true
			c := newClientWithInterceptor(t, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind && failOnce {
						failOnce = false
						return errors.New("apiserver unavailable")
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			}, first...)
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}}
			if _, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), req); err == nil {
				t.Fatal("test setup: the first pass should fail to record its outcome")
			}
			if b, _, _ := unstructured.NestedString(getSnapshot(t, c, cancelSnapshot).Object, "spec", "bucket"); b != "" {
				t.Fatalf("test setup: the first pass should withhold the bucket, got %q", b)
			}
			if err := c.Create(context.Background(), tc.missing); err != nil {
				t.Fatal(err)
			}
			if _, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), req); err != nil {
				t.Fatal(err)
			}
			ps := getSnapshot(t, c, cancelSnapshot)
			cond := condition(ps, contract.CondCheckpoint)
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonFailed {
				t.Fatalf("want Checkpoint=False/Failed, got %+v", cond)
			}
			if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b != "" {
				t.Fatalf("the work order must not be changed after creation, got bucket %q", b)
			}
		})
	}
}

// bucketlessWorkOrder is a PodSnapshot the engine controller created for a
// refusal or a storage failure, before it recorded that outcome.
func bucketlessWorkOrder() *unstructured.Unstructured {
	ps := workOrderSnapshot(cancelSnapshot)
	unstructured.RemoveNestedField(ps.Object, "spec", "bucket")
	unstructured.RemoveNestedField(ps.Object, "spec", "path")
	_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
	return ps
}

// A trigger deleted before the engine controller recorded its refusal or
// storage failure leaves a bucketless PodSnapshot. No node agent will ever
// act on it, so a CancelRequested would never be confirmed, and the
// orchestrator would keep the fence on a source that was never touched. The
// engine controller records Cancelled itself, also over a request an earlier
// pass already wrote.
func TestCancelWorkOrdersConfirmsBucketlessOrder(t *testing.T) {
	for name, requested := range map[string]bool{"no request yet": false, "request already written": true} {
		t.Run(name, func(t *testing.T) {
			ps := bucketlessWorkOrder()
			if requested {
				if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonCancelRequested}); err != nil {
					t.Fatal(err)
				}
			}
			c := newClient(t, ps)
			reconcilePSMT(t, c, nil, cancelTrigger) // the trigger is gone
			cond := requireCancelled(t, c)
			if !strings.Contains(cond.Message, "not touched") {
				t.Fatalf("the cancel message must say the source was not touched, got %q", cond.Message)
			}
			if reqs := podSnapshotToTrigger(context.Background(), getSnapshot(t, c, cancelSnapshot)); len(reqs) != 0 {
				t.Fatalf("a confirmed cancel must not enqueue its trigger: %v", reqs)
			}
		})
	}
	t.Run("the watch re-enqueues a stuck request", func(t *testing.T) {
		ps := bucketlessWorkOrder()
		if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonCancelRequested}); err != nil {
			t.Fatal(err)
		}
		if reqs := podSnapshotToTrigger(context.Background(), ps); len(reqs) != 1 {
			t.Fatalf("a bucketless order with an unconfirmable request must enqueue its trigger, got %v", reqs)
		}
	})
}

// A PodSnapshot deleted between the List and the status write is no longer
// a work order to cancel.
func TestCancelWorkOrdersIgnoresVanishedSnapshot(t *testing.T) {
	ps := workOrderSnapshot(cancelSnapshot)
	_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
	c := newClientWithInterceptor(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind {
				return apierrors.NewNotFound(schema.GroupResource{Group: contract.Group, Resource: "podsnapshots"}, key.Name)
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}, ps)
	if _, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}}); err != nil {
		t.Fatalf("a vanished PodSnapshot must not fail the cancel: %v", err)
	}
}

// The engine controller writes a PodSnapshot's status only when its
// conditions change, and counts an outcome only when it wrote it: the PSMT
// is re-enqueued by its own status write and by the PodSnapshot watch.
func TestPSMTWritesAndCountsOutcomesOnce(t *testing.T) {
	writes := 0
	c := newClientWithInterceptor(t, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind {
				writes++
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}, workOrderSnapshot(cancelSnapshot))
	r := &PSMTReconciler{Client: c}
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := r.setSnapshotPending(ctx, "default", cancelSnapshot, "awaiting node agent on node-a"); err != nil {
			t.Fatal(err)
		}
	}
	if writes != 1 {
		t.Fatalf("an unchanged Pending must not be written again: %d writes", writes)
	}

	before := outcomeCount(contract.ReasonRefused)
	for i := 0; i < 2; i++ {
		if err := r.setSnapshotTerminal(ctx, "default", cancelSnapshot, contract.ReasonRefused, "policy"); err != nil {
			t.Fatal(err)
		}
	}
	if got := outcomeCount(contract.ReasonRefused) - before; got != 1 {
		t.Fatalf("Refused counted %v times for one outcome", got)
	}
	if writes != 2 {
		t.Fatalf("want one more write for the outcome, got %d writes", writes)
	}
}

// The agent's context ending (shutdown) is not a pipeline outcome: nothing is
// written at shutdown time, leaving leftover SourceFrozen state for the restarted
// agent to thaw and fail (#148).
func TestAgentShutdownWritesNoOutcome(t *testing.T) {
	c := cancelFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := &failingStore{memStore: newMemStore(), failSubstr: "checkpoint.tar"}
	store.onFail = cancel
	_, _ = (&NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store, CancelPollInterval: time.Hour}).
		Reconcile(ctx, ctrl.Request{NamespacedName: cancelSnapshotKey})
	if ps := getSnapshot(t, c, cancelSnapshot); hasOutcome(ps) {
		t.Fatalf("an outcome was written on shutdown: %+v", contract.Conditions(ps))
	}
}

// --- A trigger re-created for the same pod instance -------------------------
//
// The orchestrator names the trigger after the pod and its UID, and the
// engine names the work order the same way, so a later job for the same pod
// instance creates a trigger with the same name. A work order that an earlier
// instance of the trigger left with an outcome must not answer it: a Ready one
// holds the source's state at that earlier checkpoint, and a Failed, Cancelled
// or Refused one says nothing about this attempt.

// psmtWithUID is psmtFor with the UID the API server gives each trigger, so a
// trigger created again under the same name is a different instance.
func psmtWithUID(pod, uid string) *unstructured.Unstructured {
	u := psmtFor(pod)
	u.SetUID(types.UID(uid))
	return u
}

// staleFixture runs a first trigger for web-0 to an outcome on the node
// agent, then deletes it, as the orchestrator does once the job moves on.
func staleFixture(t *testing.T, ops *fakeNodeOps, store ObjectStore) client.Client {
	t.Helper()
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtWithUID("web-0", "trigger-1"),
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store})
	deleteTrigger(t, c, true)
	return c
}

func triggerAnswer(t *testing.T, c client.Client) string {
	t.Helper()
	psmt := contract.New(contract.GVKManualTrigger)
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: cancelTrigger}, psmt); err != nil {
		t.Fatal(err)
	}
	return contract.TriggerSnapshotCreated(psmt)
}

// requireNotAnsweredWithEarlier: the new trigger isn't answered, and the
// earlier work order is being deleted so the trigger can get its own.
func requireNotAnsweredWithEarlier(t *testing.T, c client.Client) {
	t.Helper()
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	res, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}})
	if err != nil {
		t.Fatal(err)
	}
	if got := triggerAnswer(t, c); got != "" {
		t.Fatalf("the re-created trigger was answered with %q, the work order an earlier trigger left with %+v",
			got, contract.Conditions(getSnapshot(t, c, got)))
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("RequeueAfter = %v, want a wait for the earlier work order to be gone", res.RequeueAfter)
	}
	if ps := getSnapshot(t, c, cancelSnapshot); ps.GetDeletionTimestamp() == nil {
		t.Fatalf("the earlier work order wasn't deleted: %+v", contract.Conditions(ps))
	}
}

// A later job for the same pod instance gets a new checkpoint, not the Ready
// one an earlier job's trigger took. The source may have run since (a
// container restart while frozen), so restoring the earlier checkpoint would
// lose what it did.
func TestRecreatedTriggerGetsNewCheckpointNotEarlierReady(t *testing.T) {
	store := newMemStore()
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, store)
	if !contract.IsSnapshotReady(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatal("test setup: the first checkpoint isn't Ready")
	}

	requireNotAnsweredWithEarlier(t, c)

	// The engine deletes the archive, then lets the work order go.
	if _, err := (&ArchiveGCReconciler{Client: c, Store: store}).Reconcile(context.Background(), ctrl.Request{NamespacedName: cancelSnapshotKey}); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	ps := getSnapshot(t, c, cancelSnapshot)
	if hasOutcome(ps) || ps.GetDeletionTimestamp() != nil {
		t.Fatalf("want a new work order for the new trigger, got %+v", contract.Conditions(ps))
	}
	if got := workOrderString(ps, specTriggerUID); got != "trigger-2" {
		t.Fatalf("work order %s = %q, want trigger-2", specTriggerUID, got)
	}
	if got := triggerAnswer(t, c); got != cancelSnapshot {
		t.Fatalf("trigger answer = %q, want %q", got, cancelSnapshot)
	}

	ops := &fakeNodeOps{tarContent: "SECOND", t: t}
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: store})
	if !contract.IsSnapshotReady(getSnapshot(t, c, cancelSnapshot)) || len(ops.events) == 0 || ops.events[0] != "freeze" {
		t.Fatalf("want a new checkpoint for the new trigger: events %v, conditions %+v", ops.events, contract.Conditions(getSnapshot(t, c, cancelSnapshot)))
	}
}

// An earlier trigger's failure isn't this trigger's outcome either.
func TestRecreatedTriggerDoesNotInheritEarlierFailure(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{failDump: true, t: t}, newMemStore())
	if !hasTerminalCheckpoint(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatalf("test setup: want a failed first checkpoint, got %+v", contract.Conditions(getSnapshot(t, c, cancelSnapshot)))
	}
	requireNotAnsweredWithEarlier(t, c)
}

// The trigger that created a work order keeps being answered with it.
func TestSameTriggerKeepsItsReadyWorkOrder(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtWithUID("web-0", "trigger-1"),
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "T", t: t}, Store: newMemStore()})
	reconcilePSMT(t, c, nil, cancelTrigger)
	ps := getSnapshot(t, c, cancelSnapshot)
	if !contract.IsSnapshotReady(ps) || ps.GetDeletionTimestamp() != nil || triggerAnswer(t, c) != cancelSnapshot {
		t.Fatalf("the trigger's own Ready work order must stand: answer %q, conditions %+v", triggerAnswer(t, c), contract.Conditions(ps))
	}
}

// A work order created before the engine recorded the trigger's UID is
// judged by age: one created before the trigger can't be its own.
func TestRecreatedTriggerSkipsLegacyWorkOrderOlderThanIt(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	for _, tc := range []struct {
		name      string
		createdAt time.Time
		stale     bool
	}{
		{"created before the trigger", now.Add(-time.Hour), true},
		{"created after the trigger", now.Add(time.Second), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := workOrderSnapshot(cancelSnapshot)
			_ = unstructured.SetNestedField(ps.Object, cancelTrigger, "spec", specTriggerName)
			ps.SetFinalizers([]string{ArchiveFinalizer})
			ps.SetCreationTimestamp(metav1.NewTime(tc.createdAt))
			ps.Object["status"] = map[string]interface{}{"conditions": []interface{}{
				map[string]interface{}{"type": contract.CondCheckpoint, "status": "True", "reason": contract.ReasonSucceeded, "message": "ok", "lastTransitionTime": now.UTC().Format(time.RFC3339)},
				map[string]interface{}{"type": contract.CondReady, "status": "True", "reason": contract.ReasonSucceeded, "message": "ok", "lastTransitionTime": now.UTC().Format(time.RFC3339)},
			}}
			trigger := psmtWithUID("web-0", "trigger-2")
			trigger.SetCreationTimestamp(metav1.NewTime(now))
			c := newClient(t,
				pssc("pssc-x", "bkt", "snaps"),
				readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
				sourcePod("web-0", 1, "node-a"),
				criuNode("node-a", true),
				trigger, ps,
			)
			reconcilePSMT(t, c, nil, cancelTrigger)
			deleting := getSnapshot(t, c, cancelSnapshot).GetDeletionTimestamp() != nil
			answered := triggerAnswer(t, c) == cancelSnapshot
			if deleting != tc.stale || answered == tc.stale {
				t.Fatalf("deleting=%v answered=%v, want stale=%v", deleting, answered, tc.stale)
			}
		})
	}
}

// A work order that answers another trigger (another name) is left alone:
// that trigger may still be using it.
func TestRecreatedTriggerLeavesOtherTriggersWorkOrder(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	ps := getSnapshot(t, c, cancelSnapshot)
	_ = unstructured.SetNestedField(ps.Object, "psmt-someone-else", "spec", specTriggerName)
	if err := c.Update(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	if getSnapshot(t, c, cancelSnapshot).GetDeletionTimestamp() != nil {
		t.Fatal("deleted the work order of another trigger")
	}
}

// A trigger created again while the earlier one's work order is still in
// flight takes the work order over, so its Ready outcome is the new
// trigger's and stays.
func TestRecreatedTriggerTakesOverUnfinishedWorkOrder(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtWithUID("web-0", "trigger-1"),
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	deleteTrigger(t, c, true)
	requireCancelRequested(t, c)
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	if got := workOrderString(getSnapshot(t, c, cancelSnapshot), specTriggerUID); got != "trigger-2" {
		t.Fatalf("work order %s = %q, want trigger-2 after the take-over", specTriggerUID, got)
	}
	runAgent(t, &NodeSnapshotReconciler{Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "T", t: t}, Store: newMemStore()})
	reconcilePSMT(t, c, nil, cancelTrigger)
	ps := getSnapshot(t, c, cancelSnapshot)
	if !contract.IsSnapshotReady(ps) || ps.GetDeletionTimestamp() != nil || triggerAnswer(t, c) != cancelSnapshot {
		t.Fatalf("the taken-over work order must stand once Ready: deleting=%v answer %q, conditions %+v",
			ps.GetDeletionTimestamp() != nil, triggerAnswer(t, c), contract.Conditions(ps))
	}
}

// A checkpoint that timed out never settles, and the orchestrator keeps its
// source fenced and evicts it cold. A trigger created again leaves it alone:
// a second kubelet checkpoint could run while the first dump still is.
func TestRecreatedTriggerLeavesTimedOutWorkOrder(t *testing.T) {
	c := newClient(t,
		pssc("pssc-x", "bkt", "snaps"),
		readyEnginePSP(t, "psp-criu", "default", "pssc-x"),
		sourcePod("web-0", 1, "node-a"),
		criuNode("node-a", true),
		psmtWithUID("web-0", "trigger-1"),
	)
	reconcilePSMT(t, c, nil, cancelTrigger)
	ps := getSnapshot(t, c, cancelSnapshot)
	if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonCheckpointTimedOut, Message: "timed out"}); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), ps); err != nil {
		t.Fatal(err)
	}
	deleteTrigger(t, c, true)
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	ps = getSnapshot(t, c, cancelSnapshot)
	if ps.GetDeletionTimestamp() != nil || workOrderString(ps, specTriggerUID) != "trigger-1" || !checkpointTimedOut(ps) {
		t.Fatalf("the timed-out work order must be left as it is: deleting=%v %s=%q conditions %+v",
			ps.GetDeletionTimestamp() != nil, specTriggerUID, workOrderString(ps, specTriggerUID), contract.Conditions(ps))
	}
}
