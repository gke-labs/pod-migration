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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// Eligibility is decided once, when the work order is created (#218). The
// trigger is re-reconciled every 30 s while the node agent runs the order;
// those passes must not re-judge it, or a transient error (or a pod or node
// that has changed since) writes Refused or Failed onto a checkpoint the
// agent is already executing.

var eligibilityTriggerReq = ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: cancelTrigger}}

// eligibilityObjects is an eligible trigger whose policy selects the pod by
// app=web, so the pod can lose its opt-in label and still be routed here.
func eligibilityObjects(t *testing.T) []client.Object {
	t.Helper()
	psp := readyEnginePSP(t, "psp-criu", "default", "pssc-x")
	_ = unstructured.SetNestedField(psp.Object, map[string]interface{}{"app": "web"}, "spec", "selector", "matchLabels")
	pod := sourcePod("web-0", 1, "node-a")
	pod.Labels["app"] = "web"
	return []client.Object{
		pssc("pssc-x", "bkt", "snaps"),
		psp,
		pod,
		criuNode("node-a", true),
		psmtFor("web-0"),
	}
}

// eligibilityPod, replaceNode and withoutStorageConfig find fixture objects
// by kind and name, so the subtests don't depend on eligibilityObjects' order.
func eligibilityPod(objs []client.Object) *corev1.Pod {
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok {
			return p
		}
	}
	panic("eligibilityObjects has no pod")
}

func replaceNode(objs []client.Object, node *corev1.Node) []client.Object {
	for i, o := range objs {
		if _, ok := o.(*corev1.Node); ok && o.GetName() == node.Name {
			objs[i] = node
		}
	}
	return objs
}

func withoutStorageConfig(objs []client.Object) []client.Object {
	var out []client.Object
	for _, o := range objs {
		if u, ok := o.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKStorageConfig.Kind {
			continue
		}
		out = append(out, o)
	}
	return out
}

func requirePendingWorkOrder(t *testing.T, c client.Client) {
	t.Helper()
	ps := getSnapshot(t, c, cancelSnapshot)
	if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b != "bkt" {
		t.Fatalf("work order bucket = %q, want bkt", b)
	}
	if hasOutcome(ps) {
		t.Fatalf("a running work order must not get an outcome from the controller: %+v", contract.Conditions(ps))
	}
	cond := condition(ps, contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionUnknown || cond.Reason != contract.ReasonPending {
		t.Fatalf("want Checkpoint=Unknown/Pending, got %+v", cond)
	}
}

func TestPSMTLaterPassDoesNotRejudgeRunningWorkOrder(t *testing.T) {
	errAPI := errors.New("apiserver unavailable")
	cases := map[string]struct {
		// getErr fails Gets on the later pass only.
		getErr func(obj client.Object) error
		// change mutates the cluster between the passes.
		change func(t *testing.T, c client.Client)
	}{
		"node Get fails": {
			getErr: func(obj client.Object) error {
				if _, ok := obj.(*corev1.Node); ok {
					return errAPI
				}
				return nil
			},
		},
		"storage config Get fails": {
			getErr: func(obj client.Object) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKStorageConfig.Kind {
					return errAPI
				}
				return nil
			},
		},
		"storage config deleted": {
			change: func(t *testing.T, c client.Client) {
				if err := c.Delete(context.Background(), pssc("pssc-x", "bkt", "snaps")); err != nil {
					t.Fatal(err)
				}
			},
		},
		"pod no longer opted in": {
			change: func(t *testing.T, c client.Client) {
				pod := &corev1.Pod{}
				if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-0"}, pod); err != nil {
					t.Fatal(err)
				}
				delete(pod.Labels, contract.LabelEnabled)
				if err := c.Update(context.Background(), pod); err != nil {
					t.Fatal(err)
				}
			},
		},
		"node no longer criu-ready": {
			change: func(t *testing.T, c client.Client) {
				node := &corev1.Node{}
				if err := c.Get(context.Background(), types.NamespacedName{Name: "node-a"}, node); err != nil {
					t.Fatal(err)
				}
				node.Annotations = nil
				if err := c.Update(context.Background(), node); err != nil {
					t.Fatal(err)
				}
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			later := false
			c := newClientWithInterceptor(t, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if later && tc.getErr != nil {
						if err := tc.getErr(obj); err != nil {
							return err
						}
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			}, eligibilityObjects(t)...)
			reconcilePSMT(t, c, nil, cancelTrigger)
			requirePendingWorkOrder(t, c) // the first pass judged it eligible

			if tc.change != nil {
				tc.change(t, c)
			}
			later = true
			res, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), eligibilityTriggerReq)
			requirePendingWorkOrder(t, c)
			if err != nil {
				t.Fatalf("a later pass must not fail over eligibility it no longer decides: %v", err)
			}
			if res.RequeueAfter != 30*time.Second {
				t.Fatalf("a later pass must keep requeueing while the agent works, got %+v", res)
			}
		})
	}
}

// In direct-call mode a later pass hands the checkpointer the work order's
// own storage, not a fresh lookup.
func TestPSMTLaterPassUsesWorkOrderStorage(t *testing.T) {
	c := newClient(t, eligibilityObjects(t)...)
	cp := &fakeCheckpointer{}
	reconcilePSMT(t, c, cp, cancelTrigger)
	if err := c.Delete(context.Background(), pssc("pssc-x", "bkt", "snaps")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, cp, cancelTrigger)
	if hasOutcome(getSnapshot(t, c, cancelSnapshot)) {
		t.Fatalf("a later pass must not record an outcome: %+v", contract.Conditions(getSnapshot(t, c, cancelSnapshot)))
	}
	if len(cp.got) != 2 {
		t.Fatalf("checkpointer calls: %d, want 2", len(cp.got))
	}
	if got := cp.got[1]; got.Bucket != "bkt" || got.Path != "snaps" {
		t.Fatalf("later pass request = %+v, want the work order's bkt/snaps", got)
	}
}

// The first pass still records its verdict: a refusal is Refused, a storage
// failure is Failed, and neither work order gets a bucket.
func TestPSMTFirstPassOutcomesUnchanged(t *testing.T) {
	errAPI := errors.New("apiserver unavailable")
	cases := map[string]struct {
		mutate     func(objs []client.Object) []client.Object
		getErr     func(obj client.Object) error
		wantReason string
		wantMsg    string
	}{
		"pod not opted in": {
			mutate: func(objs []client.Object) []client.Object {
				delete(eligibilityPod(objs).Labels, contract.LabelEnabled)
				return objs
			},
			wantReason: contract.ReasonRefused, wantMsg: "not opted in",
		},
		"node not criu-ready": {
			mutate: func(objs []client.Object) []client.Object {
				return replaceNode(objs, criuNode("node-a", false))
			},
			wantReason: contract.ReasonRefused, wantMsg: "not criu-ready",
		},
		"node Get fails": {
			getErr: func(obj client.Object) error {
				if _, ok := obj.(*corev1.Node); ok {
					return errAPI
				}
				return nil
			},
			wantReason: contract.ReasonRefused, wantMsg: "cannot resolve node",
		},
		"storage config missing": {
			mutate: func(objs []client.Object) []client.Object {
				return withoutStorageConfig(objs)
			},
			wantReason: contract.ReasonFailed, wantMsg: "resolving storage",
		},
		"storage config Get fails": {
			getErr: func(obj client.Object) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKStorageConfig.Kind {
					return errAPI
				}
				return nil
			},
			wantReason: contract.ReasonFailed, wantMsg: "resolving storage",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			objs := eligibilityObjects(t)
			if tc.mutate != nil {
				objs = tc.mutate(objs)
			}
			c := newClientWithInterceptor(t, interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if tc.getErr != nil {
						if err := tc.getErr(obj); err != nil {
							return err
						}
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			}, objs...)
			cp := &fakeCheckpointer{}
			reconcilePSMT(t, c, cp, cancelTrigger)
			if len(cp.got) != 0 {
				t.Fatalf("an ineligible trigger must not be checkpointed: %+v", cp.got)
			}
			ps := getSnapshot(t, c, cancelSnapshot)
			if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b != "" {
				t.Fatalf("an ineligible work order must not get a bucket, got %q", b)
			}
			for _, typ := range []string{contract.CondCheckpoint, contract.CondReady} {
				cond := condition(ps, typ)
				if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tc.wantReason || !strings.Contains(cond.Message, tc.wantMsg) {
					t.Fatalf("want %s=False/%s (%q), got %+v", typ, tc.wantReason, tc.wantMsg, cond)
				}
			}
		})
	}
}

// A bucketless work order was never handed to an agent, so a later pass still
// judges it: a first pass whose Refused write failed is retried as Refused
// (the source was never touched), not turned into Failed.
func TestPSMTRetryKeepsRefusalOnBucketlessWorkOrder(t *testing.T) {
	objs := eligibilityObjects(t)
	delete(eligibilityPod(objs).Labels, contract.LabelEnabled)
	failOnce := true
	c := newClientWithInterceptor(t, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind && failOnce {
				failOnce = false
				return errors.New("apiserver unavailable")
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}, objs...)
	if _, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), eligibilityTriggerReq); err == nil {
		t.Fatal("test setup: the first pass should fail to record its refusal")
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	cond := condition(getSnapshot(t, c, cancelSnapshot), contract.CondCheckpoint)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != contract.ReasonRefused {
		t.Fatalf("want the retried Checkpoint=False/Refused, got %+v", cond)
	}
}

// A trigger recreated over a cancel request takes the running work order back
// as Pending, even when eligibility would now fail: the order was judged
// when it was created, and the agent may still be working it.
func TestRecreatedTriggerRevokesCancelWithoutRejudging(t *testing.T) {
	c := newClient(t, eligibilityObjects(t)...)
	reconcilePSMT(t, c, nil, cancelTrigger)
	deleteTrigger(t, c, true)
	requireCancelRequested(t, c)

	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "web-0"}, pod); err != nil {
		t.Fatal(err)
	}
	delete(pod.Labels, contract.LabelEnabled)
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), psmtFor("web-0")); err != nil {
		t.Fatal(err)
	}
	reconcilePSMT(t, c, nil, cancelTrigger)
	requirePendingWorkOrder(t, c)
}

// A failed read of the work order is returned, not judged: an empty
// PodSnapshot would look bucketless and get Failed over a running order.
func TestPSMTLaterPassPodSnapshotGetFails(t *testing.T) {
	failOnce := false
	c := newClientWithInterceptor(t, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind && failOnce {
				failOnce = false
				return errors.New("apiserver unavailable")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	}, eligibilityObjects(t)...)
	reconcilePSMT(t, c, nil, cancelTrigger)
	requirePendingWorkOrder(t, c)

	failOnce = true
	if _, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), eligibilityTriggerReq); err == nil {
		t.Fatal("a failed PodSnapshot read must be returned, not judged")
	}
	requirePendingWorkOrder(t, c)
}

// If another writer creates the work order between this pass's read and its
// Create, the pass requeues instead of recording its own verdict, which was
// formed without seeing that order.
func TestPSMTCreateAlreadyExistsRequeuesWithoutVerdict(t *testing.T) {
	objs := eligibilityObjects(t)
	delete(eligibilityPod(objs).Labels, contract.LabelEnabled) // this pass would refuse
	racer := workOrderSnapshot(cancelSnapshot)                 // a bucketed order, created by the racing writer
	_ = unstructured.SetNestedField(racer.Object, cancelTrigger, "spec", specTriggerName)
	c := newClientWithInterceptor(t, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == contract.GVKPodSnapshot.Kind {
				if err := cl.Create(ctx, racer.DeepCopy()); err != nil {
					t.Fatalf("test setup: racing create: %v", err)
				}
			}
			return cl.Create(ctx, obj, opts...)
		},
	}, objs...)
	res, err := (&PSMTReconciler{Client: c}).Reconcile(context.Background(), eligibilityTriggerReq)
	if err != nil {
		t.Fatal(err)
	}
	ps := getSnapshot(t, c, cancelSnapshot)
	if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b == "" {
		t.Fatal("test setup: the racing writer's bucketed order should have won")
	}
	if hasOutcome(ps) {
		t.Fatalf("a lost create race must not write a verdict onto the winner's order: %+v", contract.Conditions(ps))
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("a lost create race must requeue, got %+v", res)
	}
}
