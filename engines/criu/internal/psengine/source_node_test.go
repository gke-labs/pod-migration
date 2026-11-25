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
	"reflect"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

var testSourceFacts = contract.SourceNodeFacts{
	CPUModel:          "Intel(R) Xeon(R) test",
	CPUFeatures:       []string{"avx2", "fpu", "sse4_2"},
	KernelVersion:     "6.6.72+",
	CRIUVersion:       "4.1",
	RuncVersion:       "1.2.5",
	ContainerdVersion: "2.0.4",
}

var testSnapshotReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "ps-web-0-deadbeef"}}

// A successful checkpoint records the source node's facts twice: in the
// archive manifest (travels with the checkpoint) and on the PodSnapshot
// status (readable by the orchestrator without GCS access), in the same
// status write that marks it Ready.
func TestNodePipelineRecordsSourceNodeFacts(t *testing.T) {
	c := newClient(t, jobWorkOrderObjects("ps-web-0-deadbeef")...)
	ops := &fakeNodeOps{tarContent: "TARBYTES", t: t}
	store := newMemStore()
	var gathered int
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID,
		Client: c, NodeName: "node-a", Ops: ops, Store: store,
		SourceNodeFacts: func(context.Context) contract.SourceNodeFacts {
			gathered++
			// Gathered before the source is frozen, so it adds nothing to the freeze.
			if len(ops.events) != 0 {
				t.Errorf("facts gathered after %v; want before the freeze", ops.events)
			}
			return testSourceFacts
		},
	}
	if _, err := r.Reconcile(context.Background(), testSnapshotReq); err != nil {
		t.Fatal(err)
	}
	if gathered != 1 {
		t.Fatalf("facts gathered %d times, want 1", gathered)
	}

	mf := openManifest(t, store, "bkt/snaps/ps-web-0-deadbeef/manifest.json")
	if mf.SourceNode == nil || !reflect.DeepEqual(*mf.SourceNode, testSourceFacts) {
		t.Fatalf("manifest sourceNode = %+v, want %+v", mf.SourceNode, testSourceFacts)
	}

	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if !contract.IsSnapshotReady(got) {
		t.Fatalf("snapshot not ready: %+v", contract.Conditions(got))
	}
	facts, ok := contract.GetSourceNodeFacts(got)
	if !ok || !reflect.DeepEqual(facts, testSourceFacts) {
		t.Fatalf("status.sourceNode = %+v (found=%v), want %+v", facts, ok, testSourceFacts)
	}
}

// No facts probe (or nothing known): the checkpoint still succeeds, and
// neither the manifest nor the status carries an empty sourceNode.
func TestNodePipelineWithoutSourceNodeFacts(t *testing.T) {
	for name, fn := range map[string]func(context.Context) contract.SourceNodeFacts{
		"nil probe":     nil,
		"nothing known": func(context.Context) contract.SourceNodeFacts { return contract.SourceNodeFacts{} },
	} {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, jobWorkOrderObjects("ps-web-0-deadbeef")...)
			store := newMemStore()
			r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID,
				Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: store,
				SourceNodeFacts: fn,
			}
			if _, err := r.Reconcile(context.Background(), testSnapshotReq); err != nil {
				t.Fatal(err)
			}
			got := getSnapshot(t, c, "ps-web-0-deadbeef")
			if !contract.IsSnapshotReady(got) {
				t.Fatalf("missing facts must not fail the checkpoint: %+v", contract.Conditions(got))
			}
			if _, ok := contract.GetSourceNodeFacts(got); ok {
				t.Fatal("status.sourceNode written with nothing known")
			}
			// The manifest is the signed envelope's payload.
			payload, _, err := testArchiveKeyring.Open(store.objects["bkt/snaps/ps-web-0-deadbeef/manifest.json"])
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(payload), "sourceNode") {
				t.Fatal("manifest carries an empty sourceNode")
			}
		})
	}
}

// A failed checkpoint doesn't publish facts: there is nothing to restore.
func TestNodePipelineFailureRecordsNoSourceNodeFacts(t *testing.T) {
	c := newClient(t, workOrderSnapshot("ps-web-0-deadbeef"))
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID,
		Client: c, NodeName: "node-a", Ops: &fakeNodeOps{failDump: true, t: t}, Store: newMemStore(),
		SourceNodeFacts: func(context.Context) contract.SourceNodeFacts { return testSourceFacts },
	}
	if _, err := r.Reconcile(context.Background(), testSnapshotReq); err != nil {
		t.Fatal(err)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSnapshotReady(got) {
		t.Fatal("failed snapshot must not be ready")
	}
	if _, ok := contract.GetSourceNodeFacts(got); ok {
		t.Fatal("failed snapshot must not carry status.sourceNode")
	}
}

// The facts land in the status write that first marks the snapshot Ready,
// never in a later one: a reader that sees Ready also sees status.sourceNode.
func TestNodePipelineSourceNodeFactsInReadyWrite(t *testing.T) {
	var readyWrites, readyWithFacts int
	c := newClientWithInterceptor(t, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && sub == "status" && contract.IsSnapshotReady(u) {
				readyWrites++
				if f, ok := contract.GetSourceNodeFacts(u); ok && reflect.DeepEqual(f, testSourceFacts) {
					readyWithFacts++
				}
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}, jobWorkOrderObjects("ps-web-0-deadbeef")...)
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID,
		Client: c, NodeName: "node-a", Ops: &fakeNodeOps{tarContent: "TARBYTES", t: t}, Store: newMemStore(),
		SourceNodeFacts: func(context.Context) contract.SourceNodeFacts { return testSourceFacts },
	}
	if _, err := r.Reconcile(context.Background(), testSnapshotReq); err != nil {
		t.Fatal(err)
	}
	if readyWrites != 1 || readyWithFacts != 1 {
		t.Fatalf("Ready status writes = %d, with facts = %d; want 1 and 1", readyWrites, readyWithFacts)
	}
}

// finishWith never writes facts over an outcome already recorded.
func TestFinishWithLeavesExistingOutcome(t *testing.T) {
	ps := workOrderSnapshot("ps-web-0-deadbeef")
	if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionFalse, Reason: contract.ReasonRefused}); err != nil {
		t.Fatal(err)
	}
	c := newClient(t, ps)
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a"}
	if err := r.finishWith(context.Background(), testSnapshotReq, contract.ReasonSucceeded, "late",
		func(u *unstructured.Unstructured) error { return contract.SetSourceNodeFacts(u, testSourceFacts) }); err != nil {
		t.Fatal(err)
	}
	got := getSnapshot(t, c, "ps-web-0-deadbeef")
	if contract.IsSnapshotReady(got) {
		t.Fatal("existing Refused outcome overwritten with Ready")
	}
	if _, ok := contract.GetSourceNodeFacts(got); ok {
		t.Fatal("facts written onto a snapshot that already had an outcome")
	}
}

// Facts are best effort: a setStatus error never holds back the outcome.
func TestFinishWithSetStatusErrorStillWritesOutcome(t *testing.T) {
	c := newClient(t, workOrderSnapshot("ps-web-0-deadbeef"))
	r := &NodeSnapshotReconciler{Signer: testArchiveSigner, ClusterUID: testClusterUID, Client: c, NodeName: "node-a"}
	if err := r.finishWith(context.Background(), testSnapshotReq, contract.ReasonSucceeded, "ok",
		func(*unstructured.Unstructured) error { return errors.New("boom") }); err != nil {
		t.Fatal(err)
	}
	if !contract.IsSnapshotReady(getSnapshot(t, c, "ps-web-0-deadbeef")) {
		t.Fatal("a setStatus error must not block Ready")
	}
}
