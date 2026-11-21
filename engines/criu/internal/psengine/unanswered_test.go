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

// A trigger the engine leaves unanswered on purpose (an earlier trigger's
// work order is still in use, #262, or is being deleted, #260) records why on
// the trigger as an event, once per reason, so an operator can see why its
// job timed out.

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

var cancelTriggerReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}}

// drainEvents returns the events recorded so far.
func drainEvents(rec *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func reconcileTimes(t *testing.T, r *PSMTReconciler, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := r.Reconcile(context.Background(), cancelTriggerReq); err != nil {
			t.Fatal(err)
		}
	}
}

func requireOneEvent(t *testing.T, events []string, prefix string, contains ...string) {
	t.Helper()
	if len(events) != 1 {
		t.Fatalf("want exactly one event, got %d: %q", len(events), events)
	}
	if !strings.HasPrefix(events[0], prefix) {
		t.Fatalf("event %q, want prefix %q", events[0], prefix)
	}
	for _, s := range contains {
		if !strings.Contains(events[0], s) {
			t.Fatalf("event %q doesn't name %q", events[0], s)
		}
	}
}

func createJobUsing(t *testing.T, c client.Client, name string) {
	t.Helper()
	pmj := contract.New(contract.GVKPodMigrationJob)
	pmj.SetNamespace("default")
	pmj.SetName(name)
	pmj.SetUID(types.UID(name + "-uid"))
	_ = unstructured.SetNestedField(pmj.Object, "Restoring", "status", "phase")
	_ = unstructured.SetNestedField(pmj.Object, cancelSnapshot, "status", "snapshotRef")
	if err := c.Create(context.Background(), pmj); err != nil {
		t.Fatal(err)
	}
}

// The trigger left unanswered because a job uses an earlier trigger's work
// order gets one Warning naming the work order and the job, not one per
// requeue. When something else uses it instead, it gets a new one.
func TestUnansweredTriggerRecordsWhoUsesTheWorkOrder(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	createJobUsing(t, c, "other-pmj")
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(100)
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: rec}

	reconcileTimes(t, r, 3)
	if got := triggerAnswer(t, c); got != "" {
		t.Fatalf("test setup: the trigger was answered with %q", got)
	}
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot, "PodMigrationJob other-pmj")

	// The job lets it go, and a pod that's restoring from it takes its place.
	pmj := contract.New(contract.GVKPodMigrationJob)
	pmj.SetNamespace("default")
	pmj.SetName("other-pmj")
	if err := c.Delete(context.Background(), pmj); err != nil {
		t.Fatal(err)
	}
	pod := sourcePod("web-1", 1, "node-b")
	pod.UID = "replacement-uid"
	pod.Annotations = map[string]string{contract.AnnotationPSName: cancelSnapshot}
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	reconcileTimes(t, r, 3)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot, "pod web-1")
}

// A trigger created again under the same name is a new trigger: it gets its
// own event, even though the work order and its user haven't changed.
func TestUnansweredTriggerRecordsAgainForANewTriggerInstance(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	createJobUsing(t, c, "other-pmj")
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(100)
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: rec}
	reconcileTimes(t, r, 2)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot)

	deleteTrigger(t, c, false)
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-3")); err != nil {
		t.Fatal(err)
	}
	reconcileTimes(t, r, 2)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot)
}

// While the work order is being deleted (#260) the trigger records that once,
// though the wait looks again every 2 s.
func TestTriggerWaitingForDeletedWorkOrderRecordsItOnce(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(100)
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: rec}

	// The first pass deletes the earlier trigger's work order; the archive
	// finalizer holds it while the later passes wait.
	reconcileTimes(t, r, 4)
	if ps := getSnapshot(t, c, cancelSnapshot); ps.GetDeletionTimestamp() == nil {
		t.Fatal("test setup: the earlier work order wasn't deleted")
	}
	if got := triggerAnswer(t, c); got != "" {
		t.Fatalf("test setup: the trigger was answered with %q", got)
	}
	requireOneEvent(t, drainEvents(rec), "Normal "+EventReasonWaitingForWorkOrderDeletion, cancelSnapshot)
}

// Each wait in turn is recorded once, an answered trigger records nothing,
// and its waits are forgotten.
func TestAnsweredTriggerRecordsNothingAndForgetsTheWait(t *testing.T) {
	store := newMemStore()
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, store)
	createJobUsing(t, c, "other-pmj")
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	rec := record.NewFakeRecorder(100)
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: rec}
	reconcileTimes(t, r, 2)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot)

	// The job lets the work order go: it's deleted, archive first.
	pmj := contract.New(contract.GVKPodMigrationJob)
	pmj.SetNamespace("default")
	pmj.SetName("other-pmj")
	if err := c.Delete(context.Background(), pmj); err != nil {
		t.Fatal(err)
	}
	reconcileTimes(t, r, 3)
	requireOneEvent(t, drainEvents(rec), "Normal "+EventReasonWaitingForWorkOrderDeletion, cancelSnapshot)

	if _, err := (&ArchiveGCReconciler{Client: c, Store: store}).Reconcile(context.Background(), ctrl.Request{NamespacedName: cancelSnapshotKey}); err != nil {
		t.Fatal(err)
	}
	reconcileTimes(t, r, 2)
	if got := triggerAnswer(t, c); got != cancelSnapshot {
		t.Fatalf("test setup: trigger answer = %q, want %q", got, cancelSnapshot)
	}
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("an answered trigger recorded %q", events)
	}
	if n := r.recordedWaits(); n != 0 {
		t.Fatalf("%d waits still remembered after the trigger was answered", n)
	}
}

// Deleting the trigger forgets its wait.
func TestDeletedTriggerForgetsItsWait(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	createJobUsing(t, c, "other-pmj")
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: record.NewFakeRecorder(100)}
	reconcileTimes(t, r, 1)
	if n := r.recordedWaits(); n != 1 {
		t.Fatalf("test setup: %d waits remembered, want 1", n)
	}
	deleteTrigger(t, c, false)
	reconcileTimes(t, r, 1)
	if n := r.recordedWaits(); n != 0 {
		t.Fatalf("%d waits still remembered after the trigger was deleted", n)
	}
}

// A wait that goes on is recorded again every waitEventInterval, so it
// outlives the Event TTL and a recent event shows it's still going.
func TestUnansweredTriggerRecordsAgainAfterTheInterval(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	createJobUsing(t, c, "other-pmj")
	if err := c.Create(context.Background(), psmtWithUID("web-0", "trigger-2")); err != nil {
		t.Fatal(err)
	}
	clock := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	rec := record.NewFakeRecorder(100)
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: rec, now: func() time.Time { return clock }}
	reconcileTimes(t, r, 1)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot)

	clock = clock.Add(waitEventInterval - time.Second)
	reconcileTimes(t, r, 1)
	if events := drainEvents(rec); len(events) != 0 {
		t.Fatalf("recorded again before the interval: %q", events)
	}
	clock = clock.Add(time.Second)
	reconcileTimes(t, r, 1)
	requireOneEvent(t, drainEvents(rec), "Warning "+EventReasonWorkOrderInUse, cancelSnapshot)
}

// A trigger that's being deleted (held by a finalizer) forgets its wait too.
func TestTerminatingTriggerForgetsItsWait(t *testing.T) {
	c := staleFixture(t, &fakeNodeOps{tarContent: "FIRST", t: t}, newMemStore())
	createJobUsing(t, c, "other-pmj")
	trigger := psmtWithUID("web-0", "trigger-2")
	trigger.SetFinalizers([]string{"example.com/hold"})
	if err := c.Create(context.Background(), trigger); err != nil {
		t.Fatal(err)
	}
	r := &PSMTReconciler{Client: c, APIReader: c, Recorder: record.NewFakeRecorder(100)}
	reconcileTimes(t, r, 1)
	if n := r.recordedWaits(); n != 1 {
		t.Fatalf("test setup: %d waits remembered, want 1", n)
	}
	deleteTrigger(t, c, false)
	psmt := contract.New(contract.GVKManualTrigger)
	if err := c.Get(context.Background(), cancelTriggerReq.NamespacedName, psmt); err != nil || psmt.GetDeletionTimestamp() == nil {
		t.Fatalf("test setup: want the trigger terminating, err %v", err)
	}
	reconcileTimes(t, r, 1)
	if n := r.recordedWaits(); n != 0 {
		t.Fatalf("%d waits still remembered after the trigger began deleting", n)
	}
}
