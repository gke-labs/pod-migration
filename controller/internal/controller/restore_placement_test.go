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

package controller

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	placementNamespace = "default"
	placementPodName   = "replacement"
	placementPodUID    = "replacement-uid"
	placementJobName   = "pmj-placement"
	placementSnapshot  = "snapshot-1"
	gvisorLabel        = "sandbox.gke.io/runtime"
)

// instanceTypeIn is the constraint GKE Pod Snapshots states for a job whose
// source node was of machine type t.
func instanceTypeIn(t string) corev1.NodeSelectorRequirement {
	return corev1.NodeSelectorRequirement{
		Key:      corev1.LabelInstanceTypeStable,
		Operator: corev1.NodeSelectorOpIn,
		Values:   []string{t},
	}
}

// placementNode returns a Ready gVisor node of machine type instanceType,
// changed by mutate.
func placementNode(name, instanceType string, mutate ...func(*corev1.Node)) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				corev1.LabelInstanceTypeStable: instanceType,
				gvisorLabel:                    "gvisor",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{{Key: gvisorLabel, Value: "gvisor", Effect: corev1.TaintEffectNoSchedule}},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
	for _, m := range mutate {
		m(node)
	}
	return node
}

// placementPod returns a gated gVisor replacement assigned to the placement
// job. Like a pod of the gvisor runtime class, it selects and tolerates
// gVisor nodes.
func placementPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   placementNamespace,
			Name:        placementPodName,
			UID:         placementPodUID,
			Annotations: map[string]string{util.AnnotationAssignedPMJ: placementJobName},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			NodeSelector:    map[string]string{gvisorLabel: "gvisor"},
			Tolerations:     []corev1.Toleration{{Key: gvisorLabel, Operator: corev1.TolerationOpEqual, Value: "gvisor", Effect: corev1.TaintEffectNoSchedule}},
		},
	}
}

// placementJob returns a Restoring job with a durable snapshot, whose source
// node was an n2-standard-4.
func placementJob() *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   placementNamespace,
			Name:        placementJobName,
			Annotations: map[string]string{},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "source"},
			TargetPodUID: "source-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                    pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:              placementSnapshot,
			OriginNodeInstanceType:   "n2-standard-4",
			OriginNodeKubeletVersion: "v1.37.1-gke.2097000",
		},
	}
}

func noCompatibleNodeCondition() metav1.Condition {
	return metav1.Condition{
		Type:    ConditionRestorePlacement,
		Status:  metav1.ConditionFalse,
		Reason:  ReasonNoCompatibleNode,
		Message: "no node",
	}
}

type placementRun struct {
	result ctrl.Result
	err    error
	pod    *corev1.Pod
	job    *pmv1alpha1.PodMigrationJob
	events []string
}

// reconcilePlacement reconciles the placement pod once, with the given
// objects, RestorePlacement and engines (nil: GKE Pod Snapshots).
func reconcilePlacement(t *testing.T, restorePlacement bool, engines *engine.Registry, objs ...client.Object) placementRun {
	t.Helper()
	return reconcilePlacementWith(t, restorePlacement, engines, interceptor.Funcs{}, objs...)
}

// reconcilePlacementWith is reconcilePlacement with a client that funcs
// intercept.
func reconcilePlacementWith(t *testing.T, restorePlacement bool, engines *engine.Registry, funcs interceptor.Funcs, objs ...client.Object) placementRun {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	recorder := record.NewFakeRecorder(20)
	r := &PodGateReconciler{
		Client:           cl,
		Scheme:           scheme,
		Recorder:         recorder,
		Engines:          engines,
		RestorePlacement: restorePlacement,
	}
	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}})

	run := placementRun{result: res, err: err, pod: &corev1.Pod{}, job: &pmv1alpha1.PodMigrationJob{}}
	if getErr := cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, run.pod); getErr != nil {
		t.Fatalf("getting the pod: %v", getErr)
	}
	if getErr := cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}, run.job); getErr != nil {
		t.Fatalf("getting the job: %v", getErr)
	}
	for len(recorder.Events) > 0 {
		run.events = append(run.events, <-recorder.Events)
	}
	return run
}

// requiredTerms returns pod's required node affinity terms.
func requiredTerms(pod *corev1.Pod) []corev1.NodeSelectorTerm {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return nil
	}
	return pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
}

// hasEventWith reports whether one of events contains every substring.
func hasEventWith(events []string, substrings ...string) bool {
	for _, e := range events {
		found := true
		for _, s := range substrings {
			if !strings.Contains(e, s) {
				found = false
				break
			}
		}
		if found {
			return true
		}
	}
	return false
}

func TestPodGateReconciler_RestorePlacement(t *testing.T) {
	wantTerms := []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}}}

	t.Run("a node fits: releases with the restore order and the constraint", func(t *testing.T) {
		run := reconcilePlacement(t, true, nil, placementPod(), placementJob(),
			placementNode("n2-a", "n2-standard-4"), placementNode("e2-a", "e2-medium"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) {
			t.Error("pod is still gated, want it released")
		}
		if got := run.pod.Annotations["podsnapshot.gke.io/ps-name"]; got != placementSnapshot {
			t.Errorf("ps-name = %q, want %q", got, placementSnapshot)
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, wantTerms)
		}
		if !run.job.Status.Consumed || !run.job.Status.GateReleased || run.job.Status.RestoredPodUID != placementPodUID {
			t.Errorf("job status = %+v, want consumed by the pod with the gate released", run.job.Status)
		}
		if !hasEventWith(run.events, "MigrationRestoreReleased", "n2-standard-4") {
			t.Errorf("events = %q, want MigrationRestoreReleased naming the constraint", run.events)
		}
	})

	t.Run("no node fits: holds the pod and records NoCompatibleNode", func(t *testing.T) {
		notReady := placementNode("n2-down", "n2-standard-4", func(n *corev1.Node) {
			n.Status.Conditions[0].Status = corev1.ConditionFalse
		})
		cordoned := placementNode("n2-cordoned", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true })
		run := reconcilePlacement(t, true, nil, placementPod(), placementJob(),
			notReady, cordoned, placementNode("e2-a", "e2-medium"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.result.RequeueAfter != 30*time.Second {
			t.Errorf("result = %+v, want a 30s backstop requeue", run.result)
		}
		if !podHasMigrationGate(run.pod) {
			t.Error("pod was released, want it held")
		}
		if _, ok := run.pod.Annotations["podsnapshot.gke.io/ps-name"]; ok {
			t.Errorf("pod has a restore order %q, want none", run.pod.Annotations["podsnapshot.gke.io/ps-name"])
		}
		if terms := requiredTerms(run.pod); terms != nil {
			t.Errorf("pod has required node affinity %+v, want none", terms)
		}
		if run.job.Status.Consumed || run.job.Status.GateReleased {
			t.Errorf("job status = %+v, want neither consumed nor released", run.job.Status)
		}
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonNoCompatibleNode ||
			!strings.Contains(cond.Message, "node.kubernetes.io/instance-type In [n2-standard-4]") {
			t.Errorf("RestorePlacement condition = %+v, want False, %s, naming the constraint", cond, ReasonNoCompatibleNode)
		}
		if !hasEventWith(run.events, "Warning", ReasonNoCompatibleNode) {
			t.Errorf("events = %q, want a %s Warning on the pod", run.events, ReasonNoCompatibleNode)
		}
	})

	t.Run("NoCompatibleNode recorded: keeps holding, even once a node fits", func(t *testing.T) {
		job := placementJob()
		job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
		run := reconcilePlacement(t, true, nil, placementPod(), job, placementNode("n2-a", "n2-standard-4"))
		if run.err != nil || run.result.RequeueAfter != 30*time.Second {
			t.Fatalf("Reconcile() = %+v, %v; want a 30s backstop requeue", run.result, run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed {
			t.Errorf("pod gated %t, job consumed %t; want the pod held and the job unconsumed", podHasMigrationGate(run.pod), run.job.Status.Consumed)
		}
	})

	t.Run("the job concluded NoCompatibleNode: releases cold without the constraint", func(t *testing.T) {
		job := placementJob()
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
		run := reconcilePlacement(t, true, nil, placementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) {
			t.Error("pod is still gated, want it released")
		}
		if got, ok := run.pod.Annotations["podsnapshot.gke.io/ps-name"]; !ok || got != "" {
			t.Errorf("ps-name = %q (set %t), want the cold-start bypass", got, ok)
		}
		if terms := requiredTerms(run.pod); terms != nil {
			t.Errorf("pod has required node affinity %+v, want none", terms)
		}
		if !run.job.Status.Consumed || run.job.Status.RestoredPodUID != placementPodUID {
			t.Errorf("job status = %+v, want consumed by the pod", run.job.Status)
		}
	})

	// otherPodsSucceededJob returns a job that concluded Succeeded with the
	// gate never recorded as released, consumed by another pod.
	otherPodsSucceededJob := func() *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "other-uid"
		job.Status.RestoredPodName = "other-replacement"
		return job
	}
	releasedColdWithoutCheck := func(t *testing.T, run placementRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) {
			t.Fatal("pod is still gated, want it released cold")
		}
		if got, ok := run.pod.Annotations["podsnapshot.gke.io/ps-name"]; !ok || got != "" {
			t.Errorf("ps-name = %q (set %t), want the cold-start bypass", got, ok)
		}
		if terms := requiredTerms(run.pod); terms != nil {
			t.Errorf("pod has required node affinity %+v, want none", terms)
		}
	}

	t.Run("a job that isn't Restoring isn't checked", func(t *testing.T) {
		// Only a Restoring job concludes on a hold, so a hold now would
		// keep the pod gated for good.
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
		}
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), otherPodsSucceededJob(), placementNode("e2-a", "e2-medium"))
		releasedColdWithoutCheck(t, run)
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
		if eng.calls != 0 {
			t.Errorf("Placement() called %d times, want 0", eng.calls)
		}
	})

	t.Run("NoCompatibleNode recorded on a job that isn't Restoring: doesn't hold", func(t *testing.T) {
		job := otherPodsSucceededJob()
		job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
		releasedColdWithoutCheck(t, reconcilePlacement(t, true, nil, placementPod(), job, placementNode("e2-a", "e2-medium")))
	})

	t.Run("a stale view of the job can't record a hold", func(t *testing.T) {
		// The job concluded after the pod gate read it. The hold's patch
		// replaces the conditions list as a whole, so it must not land on
		// the newer job; the pod gate decides again on the current one.
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		key := types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}
		var stale *pmv1alpha1.PodMigrationJob
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithObjects(placementPod(), placementJob(), placementNode("e2-a", "e2-medium")).
			WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && stale != nil && k == key {
					stale.DeepCopyInto(job)
					return nil
				}
				return c.Get(ctx, k, obj, opts...)
			}}).
			Build()
		ctx := context.Background()
		read := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, key, read); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		concluded := read.DeepCopy()
		concluded.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		meta.SetStatusCondition(&concluded.Status.Conditions, metav1.Condition{Type: "Restored", Status: metav1.ConditionFalse, Reason: "RestoreTimeout", Message: "timed out"})
		if err := cl.Status().Update(ctx, concluded); err != nil {
			t.Fatalf("concluding the job: %v", err)
		}
		stale = read

		r := &PodGateReconciler{Client: cl, Scheme: scheme, Recorder: record.NewFakeRecorder(20), RestorePlacement: true}
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}})
		stale = nil
		if err != nil || res.RequeueAfter <= 0 {
			t.Fatalf("Reconcile() = %+v, %v; want a requeue without an error", res, err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, key, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none on the newer job", cond)
		}
		if meta.FindStatusCondition(got.Status.Conditions, "Restored") == nil {
			t.Errorf("conditions = %+v, want the newer job's Restored condition kept", got.Status.Conditions)
		}
		pod := &corev1.Pod{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, pod); err != nil {
			t.Fatalf("getting the pod: %v", err)
		}
		if !podHasMigrationGate(pod) {
			t.Error("pod was released, want it gated until the pod gate decides again")
		}
	})

	t.Run("RestorePlacement off: releases without a constraint", func(t *testing.T) {
		run := reconcilePlacement(t, false, nil, placementPod(), placementJob())
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || run.pod.Annotations["podsnapshot.gke.io/ps-name"] != placementSnapshot {
			t.Errorf("pod gated %t, ps-name %q; want it released with the restore order", podHasMigrationGate(run.pod), run.pod.Annotations["podsnapshot.gke.io/ps-name"])
		}
		if terms := requiredTerms(run.pod); terms != nil {
			t.Errorf("pod has required node affinity %+v, want none", terms)
		}
		if !hasEventWith(run.events, "Removed scheduling gate gke.io/pod-migration-gate and injected snapshot restore annotation") {
			t.Errorf("events = %q, want the release event without a constraint", run.events)
		}
		if hasEventWith(run.events, "without node constraints") {
			t.Errorf("events = %q, want the release event as before placement", run.events)
		}
	})

	t.Run("machine type unknown: releases without a constraint or a check", func(t *testing.T) {
		// The source node couldn't be read while the job was Pending, so
		// nothing was recorded, and no node is known to fit either.
		job := placementJob()
		job.Status.OriginNodeInstanceType = ""
		job.Status.OriginNodeKubeletVersion = ""
		run := reconcilePlacement(t, true, nil, placementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
		if !hasEventWith(run.events, "MigrationRestoreReleased", "without node constraints") {
			t.Errorf("events = %q, want the release event to say there are no node constraints", run.events)
		}
	})

	t.Run("gate release already recorded: releases with the constraint without checking", func(t *testing.T) {
		// A pod update that failed after GateReleased was recorded is
		// retried as is, even if no node fits any more.
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = placementPodUID
		job.Status.RestoredPodName = placementPodName
		job.Status.GateReleased = true
		run := reconcilePlacement(t, true, nil, placementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) {
			t.Error("pod is still gated, want it released")
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, wantTerms)
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
	})

	t.Run("the job's engine states the constraints", func(t *testing.T) {
		custom := corev1.NodeSelectorRequirement{Key: "example.com/cpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"icelake"}}
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{custom},
		}
		node := placementNode("icelake", "n2-standard-4", func(n *corev1.Node) { n.Labels["example.com/cpu"] = "icelake" })
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), placementJob(), node)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		want := []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{custom}}}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, want) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, want)
		}
		if eng.calls != 1 {
			t.Errorf("Placement() called %d times, want 1", eng.calls)
		}
	})

	t.Run("an engine without the capability adds no constraint", func(t *testing.T) {
		eng := &orderingEngine{routedEngine{name: "ordering", provider: &fakeSnapshotProvider{}}}
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), placementJob())
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
	})

	t.Run("this pod already consumed the job: releases with the constraint without checking", func(t *testing.T) {
		// The Consumed write followed a check that passed, and then the
		// GateReleased patch failed. Holding now would conclude a consumed
		// job with the pod still gated.
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = placementPodUID
		job.Status.RestoredPodName = placementPodName
		run := reconcilePlacement(t, true, nil, placementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || !run.job.Status.GateReleased {
			t.Errorf("pod gated %t, GateReleased %t; want it released", podHasMigrationGate(run.pod), run.job.Status.GateReleased)
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, wantTerms)
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
	})

	strandedJob := func() *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "dead-uid"
		job.Status.RestoredPodName = "dead-replacement"
		return job
	}

	t.Run("a dead pod consumed the job and no node fits: holds, and clears the dead pod's record", func(t *testing.T) {
		// The dead pod's gate was never released, so it never restored.
		// With its record cleared, the job concludes unconsumed, so I3
		// doesn't report this pod while it waits for the cold release.
		run := reconcilePlacement(t, true, nil, placementPod(), strandedJob(), placementNode("e2-a", "e2-medium"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if !podHasMigrationGate(run.pod) {
			t.Error("pod was released, want it held")
		}
		if got := run.job.Status; got.Consumed || got.RestoredPodUID != "" || got.RestoredPodName != "" || got.GateReleased {
			t.Errorf("job status = %+v, want the dead pod's record cleared, and the held pod not adopting the job", got)
		}
		if _, held := noCompatibleNode(run.job); !held {
			t.Errorf("conditions = %+v, want NoCompatibleNode", run.job.Status.Conditions)
		}
	})

	t.Run("a dead pod consumed the job and a node fits: adopts and releases with the constraint", func(t *testing.T) {
		// The constraint it releases with is the one it checked: the
		// engine is asked once, before the adoption.
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
		}
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), strandedJob(), placementNode("n2-a", "n2-standard-4"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || run.job.Status.RestoredPodUID != placementPodUID || !run.job.Status.GateReleased {
			t.Errorf("pod gated %t, job status %+v; want the pod to adopt the job and be released", podHasMigrationGate(run.pod), run.job.Status)
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, wantTerms)
		}
		if eng.calls != 1 {
			t.Errorf("Placement() called %d times, want 1", eng.calls)
		}
	})

	t.Run("the job names an engine that isn't registered: fails without holding", func(t *testing.T) {
		eng := &placementEngine{orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}}}
		job := placementJob()
		job.Annotations[engine.AnnotationEngine] = "missing"
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), job)
		if !errors.Is(run.err, engine.ErrEngineNotRegistered) {
			t.Fatalf("Reconcile() error = %v, want ErrEngineNotRegistered", run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed || len(run.job.Status.Conditions) != 0 {
			t.Errorf("pod gated %t, job status %+v; want the pod gated and the job untouched", podHasMigrationGate(run.pod), run.job.Status)
		}
		if eng.calls != 0 {
			t.Errorf("Placement() called %d times, want 0", eng.calls)
		}
	})

	t.Run("another pod's released job with an unregistered engine: releases this pod cold", func(t *testing.T) {
		// No engine is asked for constraints for a pod that can only be
		// released cold, so a missing engine can't keep it gated.
		eng := &placementEngine{orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}}}
		job := placementJob()
		job.Annotations[engine.AnnotationEngine] = "missing"
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "other-uid"
		job.Status.RestoredPodName = "other-replacement"
		job.Status.GateReleased = true
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released cold without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
		if eng.calls != 0 {
			t.Errorf("Placement() called %d times, want 0", eng.calls)
		}
	})

	t.Run("Placement fails: the pod stays gated and nothing is recorded", func(t *testing.T) {
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			err:            errors.New("constraints unavailable"),
		}
		run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), placementJob())
		if run.err == nil || !strings.Contains(run.err.Error(), "constraints unavailable") {
			t.Fatalf("Reconcile() error = %v, want the Placement error", run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed || len(run.job.Status.Conditions) != 0 {
			t.Errorf("pod gated %t, job status %+v; want the pod gated and the job untouched", podHasMigrationGate(run.pod), run.job.Status)
		}
	})

	t.Run("listing nodes fails: the pod stays gated and nothing is recorded", func(t *testing.T) {
		funcs := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				return errors.New("node cache unavailable")
			}
			return c.List(ctx, list, opts...)
		}}
		run := reconcilePlacementWith(t, true, nil, funcs, placementPod(), placementJob(), placementNode("n2-a", "n2-standard-4"))
		if run.err == nil || !strings.Contains(run.err.Error(), "node cache unavailable") {
			t.Fatalf("Reconcile() error = %v, want the List error", run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed || len(run.job.Status.Conditions) != 0 {
			t.Errorf("pod gated %t, job status %+v; want the pod gated and the job untouched", podHasMigrationGate(run.pod), run.job.Status)
		}
	})

	t.Run("NoCompatibleNode recorded and placement turned off since: keeps holding", func(t *testing.T) {
		// The job reconciler concludes the job on the hold either way, so
		// the pod waits for the cold release rather than being released
		// with the restore order meanwhile.
		job := placementJob()
		job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
		run := reconcilePlacement(t, false, nil, placementPod(), job, placementNode("n2-a", "n2-standard-4"))
		if run.err != nil || run.result.RequeueAfter != 30*time.Second {
			t.Fatalf("Reconcile() = %+v, %v; want a 30s backstop requeue", run.result, run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed || run.job.Status.GateReleased {
			t.Errorf("pod gated %t, job status %+v; want the pod held and the job neither consumed nor released", podHasMigrationGate(run.pod), run.job.Status)
		}
	})

	t.Run("the engine states constraints that aren't valid label requirements: releases without them", func(t *testing.T) {
		// The API server would reject the pod update that adds them, and a
		// retry would fail the same way, or the scheduler could never match
		// them (Gt with a non-number), so the pod gets none, as for an
		// engine that states none.
		invalid := map[string]corev1.NodeSelectorRequirement{
			"unknown operator":         {Key: "example.com/cpu", Operator: "Near", Values: []string{"icelake"}},
			"In without values":        {Key: "example.com/cpu", Operator: corev1.NodeSelectorOpIn},
			"Gt with a non-number":     {Key: "example.com/cores", Operator: corev1.NodeSelectorOpGt, Values: []string{"many"}},
			"a key that isn't a label": {Key: "not a key", Operator: corev1.NodeSelectorOpExists},
		}
		releasedWithout := func(t *testing.T, run placementRun, eng *placementEngine) {
			t.Helper()
			if run.err != nil {
				t.Fatalf("Reconcile() error = %v", run.err)
			}
			if podHasMigrationGate(run.pod) || !run.job.Status.GateReleased || run.pod.Annotations[routedRestoreAnnotation] != placementSnapshot {
				t.Errorf("pod gated %t, restore order %q, job status %+v; want it released with the engine's restore order", podHasMigrationGate(run.pod), run.pod.Annotations[routedRestoreAnnotation], run.job.Status)
			}
			if terms := requiredTerms(run.pod); terms != nil {
				t.Errorf("pod has required node affinity %+v, want none", terms)
			}
			if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
				t.Errorf("RestorePlacement condition = %+v, want none", cond)
			}
			if eng.calls != 1 {
				t.Errorf("Placement() called %d times, want 1", eng.calls)
			}
		}
		for name, req := range invalid {
			t.Run(name, func(t *testing.T) {
				eng := &placementEngine{
					orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
					reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4"), req},
				}
				run := reconcilePlacement(t, true, registryOf(t, eng), placementPod(), placementJob(), placementNode("n2-a", "n2-standard-4"))
				releasedWithout(t, run, eng)
				if !hasEventWith(run.events, "MigrationRestoreReleased", "without node constraints") {
					t.Errorf("events = %q, want the release event to say there are no node constraints", run.events)
				}
			})
		}
		t.Run("on a retry of a release under way", func(t *testing.T) {
			eng := &placementEngine{
				orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
				reqs:           []corev1.NodeSelectorRequirement{invalid["unknown operator"]},
			}
			job := placementJob()
			job.Status.Consumed = true
			job.Status.RestoredPodUID = placementPodUID
			job.Status.RestoredPodName = placementPodName
			releasedWithout(t, reconcilePlacement(t, true, registryOf(t, eng), placementPod(), job), eng)
		})
	})

	t.Run("the recorded consumer is still terminating: waits before checking", func(t *testing.T) {
		// No node fits, so a check would hold the pod and conclude the job
		// cold. The consumer that is going away comes first: once it's
		// gone, this pod can adopt the job.
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "terminating-uid"
		job.Status.RestoredPodName = "terminating-replacement"
		now := metav1.Now()
		terminating := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace:         placementNamespace,
			Name:              "terminating-replacement",
			UID:               "terminating-uid",
			DeletionTimestamp: &now,
			Finalizers:        []string{"example.com/hold"},
		}}
		run := reconcilePlacement(t, true, nil, placementPod(), job, terminating, placementNode("e2-a", "e2-medium"))
		if run.err != nil || run.result.RequeueAfter != 5*time.Second {
			t.Fatalf("Reconcile() = %+v, %v; want a 5s requeue to wait for the consumer", run.result, run.err)
		}
		if !podHasMigrationGate(run.pod) {
			t.Error("pod was released, want it gated")
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none while the consumer is going away", cond)
		}
		if run.job.Status.RestoredPodUID != "terminating-uid" {
			t.Errorf("RestoredPodUID = %q, want the terminating consumer's", run.job.Status.RestoredPodUID)
		}
	})

	t.Run("a stale view on a retry can't release the pod onto a concluded job", func(t *testing.T) {
		// This pod consumed the job, and the GateReleased write failed; on
		// the retry the Consumed write is skipped. Meanwhile the job
		// concluded without a restore. The GateReleased patch checks the
		// resourceVersion, so the stale view can't release the pod with a
		// restore order onto a job that reports a cold start. And the
		// failed patch leaves the job in memory as it was read.
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		key := types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}
		consumed := placementJob()
		consumed.Status.Consumed = true
		consumed.Status.RestoredPodUID = placementPodUID
		consumed.Status.RestoredPodName = placementPodName
		var stale, patched *pmv1alpha1.PodMigrationJob
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithObjects(placementPod(), consumed, placementNode("n2-a", "n2-standard-4")).
			WithInterceptorFuncs(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && stale != nil && k == key {
						stale.DeepCopyInto(job)
						return nil
					}
					return c.Get(ctx, k, obj, opts...)
				},
				SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
					if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && subResource == "status" {
						patched = job
					}
					return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
				},
			}).
			Build()
		ctx := context.Background()
		read := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, key, read); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		concluded := read.DeepCopy()
		concluded.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		if err := cl.Status().Update(ctx, concluded); err != nil {
			t.Fatalf("concluding the job: %v", err)
		}
		stale = read

		r := &PodGateReconciler{Client: cl, Scheme: scheme, Recorder: record.NewFakeRecorder(20), RestorePlacement: true}
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}})
		stale = nil
		if !apierrors.IsConflict(err) {
			t.Fatalf("Reconcile() error = %v, want a conflict", err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, key, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		if got.Status.GateReleased {
			t.Errorf("job status = %+v, want no gate release recorded on the concluded job", got.Status)
		}
		if patched == nil || patched.Status.GateReleased {
			t.Errorf("job in memory after the failed patch = %+v, want it as it was read, without GateReleased", patched)
		}
		pod := &corev1.Pod{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, pod); err != nil {
			t.Fatalf("getting the pod: %v", err)
		}
		if !podHasMigrationGate(pod) {
			t.Error("pod was released, want it gated")
		}
	})
}

// placementEngine is an orderingEngine that states fixed placement
// constraints and counts the calls.
type placementEngine struct {
	orderingEngine
	reqs  []corev1.NodeSelectorRequirement
	err   error
	calls int
}

func (e *placementEngine) Placement(context.Context, *pmv1alpha1.PodMigrationJob) ([]corev1.NodeSelectorRequirement, error) {
	e.calls++
	return e.reqs, e.err
}

func TestPodMigrationJobReconciler_NoCompatibleNode(t *testing.T) {
	reconcileJob := func(t *testing.T, job *pmv1alpha1.PodMigrationJob) restoringRun {
		t.Helper()
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		c := newFakeClientBuilderWithEventIndex(scheme).
			WithObjects(placementPod(), job).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			Build()
		recorder := record.NewFakeRecorder(20)
		r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, Recorder: recorder, RestoreStabilizationWindow: DefaultRestoreStabilizationWindow}
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}})
		got := &pmv1alpha1.PodMigrationJob{}
		if getErr := c.Get(context.Background(), types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}, got); getErr != nil {
			t.Fatalf("getting the job: %v", getErr)
		}
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		return restoringRun{result: res, err: err, job: got, events: events}
	}
	heldJob := func(restoringFor time.Duration) *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		start := metav1.NewTime(time.Now().Add(-restoringFor))
		job.Status.RestoringStartTime = &start
		job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
		return job
	}
	concludedNoCompatibleNode := func(t *testing.T, run restoringRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || run.job.Status.CompletionTime == nil {
			t.Fatalf("phase = %s, completion %v; want SucceededWithoutRestore", run.job.Status.Phase, run.job.Status.CompletionTime)
		}
		for _, condType := range []string{"Restored", "Ready"} {
			cond := meta.FindStatusCondition(run.job.Status.Conditions, condType)
			if cond == nil || cond.Reason != ReasonNoCompatibleNode || cond.Message != "no node" {
				t.Errorf("%s condition = %+v, want reason %s with the gate's message", condType, cond, ReasonNoCompatibleNode)
			}
		}
		if !hasEventWith(run.events, "Warning", ReasonNoCompatibleNode, "no node") {
			t.Errorf("events = %q, want a %s Warning on the job", run.events, ReasonNoCompatibleNode)
		}
	}

	t.Run("held replacement concludes SucceededWithoutRestore", func(t *testing.T) {
		before := outcomeCount("succeeded_without_restore")
		verdictsBefore := verdictCount(engine.GKEPodSnapshotsName, "no_compatible_node")
		concludedNoCompatibleNode(t, reconcileJob(t, heldJob(10*time.Second)))
		if got := outcomeCount("succeeded_without_restore") - before; got != 1 {
			t.Errorf("succeeded_without_restore outcomes recorded = %v, want 1", got)
		}
		if got := verdictCount(engine.GKEPodSnapshotsName, "no_compatible_node") - verdictsBefore; got != 1 {
			t.Errorf("no_compatible_node verdicts recorded = %v, want 1", got)
		}
	})

	t.Run("past the restore ceiling it still concludes NoCompatibleNode", func(t *testing.T) {
		concludedNoCompatibleNode(t, reconcileJob(t, heldJob(10*time.Minute)))
	})

	t.Run("a replacement released with a restore order isn't concluded", func(t *testing.T) {
		job := heldJob(10 * time.Second)
		job.Status.GateReleased = true
		run := reconcileJob(t, job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase = %s, want Restoring", run.job.Status.Phase)
		}
	})

	t.Run("a stale view of the hold can't conclude a job released since", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		key := types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}
		var stale *pmv1alpha1.PodMigrationJob
		c := newFakeClientBuilderWithEventIndex(scheme).
			WithObjects(placementPod(), heldJob(10*time.Second)).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithInterceptorFuncs(interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if job, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && stale != nil && k == key {
					stale.DeepCopyInto(job)
					return nil
				}
				return cl.Get(ctx, k, obj, opts...)
			}}).
			Build()
		ctx := context.Background()
		held := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, key, held); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		// The gate has since released the replacement with a restore order,
		// but the reconciler still reads the hold.
		released := held.DeepCopy()
		released.Status.Consumed = true
		released.Status.RestoredPodUID = placementPodUID
		released.Status.RestoredPodName = placementPodName
		released.Status.GateReleased = true
		if err := c.Status().Update(ctx, released); err != nil {
			t.Fatalf("releasing the replacement: %v", err)
		}
		stale = held

		r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(20)}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile() error = %v", err)
		}
		stale = nil
		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !got.Status.GateReleased {
			t.Errorf("job status = %+v, want it still Restoring with the gate released", got.Status)
		}
	})

	t.Run("a conclusion that conflicts isn't judged by the invariant checks", func(t *testing.T) {
		// The job records a consumer, and the next replacement is held, so
		// the checks would report a conclusion they wrongly saw: a
		// concluded, consumed job with a gated replacement is an I3
		// violation. (The pod gate's hold clears a dead consumer's record;
		// here the record only makes a wrong view visible.) When the
		// conclusion's patch conflicts, nothing is written, so the checks
		// after the reconcile must see the job still Restoring.
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		job := heldJob(10 * time.Second)
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "dead-uid"
		job.Status.RestoredPodName = "dead-replacement"
		c := newFakeClientBuilderWithEventIndex(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithObjects(placementPod(), job).
			WithInterceptorFuncs(interceptor.Funcs{SubResourcePatch: func(ctx context.Context, cl client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && subResource == "status" {
					return apierrors.NewConflict(pmv1alpha1.GroupVersion.WithResource("podmigrationjobs").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
				}
				return cl.SubResource(subResource).Patch(ctx, obj, patch, opts...)
			}}).
			Build()
		recorder := record.NewFakeRecorder(50)
		r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, Recorder: recorder, InvariantEngine: invariants.NewEngine(invariants.ModeObserve, recorder)}
		ctx := context.Background()
		key := types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatalf("Reconcile() error = %v, want the conflict handled", err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase = %s, want Restoring", got.Status.Phase)
		}
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		for _, v := range violations(events) {
			t.Errorf("invariant violation: %s", v)
		}
	})
}

func TestAddRequiredNodeAffinity(t *testing.T) {
	req := instanceTypeIn("n2-standard-4")
	zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	zoneB := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}
	byName := corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-1"}}
	preferred := []corev1.PreferredSchedulingTerm{{Weight: 1, Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{zoneB}}}}
	required := func(terms ...corev1.NodeSelectorTerm) *corev1.Affinity {
		return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
		}}
	}
	exprs := func(reqs ...corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: reqs}
	}

	cases := []struct {
		name string
		in   *corev1.Affinity
		reqs []corev1.NodeSelectorRequirement
		want *corev1.Affinity
	}{
		{
			name: "no affinity: one term",
			reqs: []corev1.NodeSelectorRequirement{req},
			want: required(exprs(req)),
		},
		{
			name: "only preferred terms: they stay",
			in:   &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: preferred}},
			reqs: []corev1.NodeSelectorRequirement{req},
			want: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution:  &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{exprs(req)}},
				PreferredDuringSchedulingIgnoredDuringExecution: preferred,
			}},
		},
		{
			name: "every term gets it",
			in:   required(exprs(zoneA), exprs(zoneB), corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{byName}}),
			reqs: []corev1.NodeSelectorRequirement{req},
			want: required(exprs(zoneA, req), exprs(zoneB, req),
				corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{req}, MatchFields: []corev1.NodeSelectorRequirement{byName}}),
		},
		{
			name: "a term that has it doesn't get it twice",
			in:   required(exprs(req, zoneA), exprs(zoneB)),
			reqs: []corev1.NodeSelectorRequirement{req},
			want: required(exprs(req, zoneA), exprs(zoneB, req)),
		},
		{
			name: "an empty term stays empty",
			in:   required(corev1.NodeSelectorTerm{}, exprs(zoneA)),
			reqs: []corev1.NodeSelectorRequirement{req},
			want: required(corev1.NodeSelectorTerm{}, exprs(zoneA, req)),
		},
		{
			name: "no requirements: nothing changes",
			in:   nil,
			reqs: nil,
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: tc.in.DeepCopy(), NodeSelector: map[string]string{gvisorLabel: "gvisor"}}}
			addRequiredNodeAffinity(pod, tc.reqs)
			if !reflect.DeepEqual(pod.Spec.Affinity, tc.want) {
				t.Errorf("affinity = %+v, want %+v", pod.Spec.Affinity, tc.want)
			}
			if !reflect.DeepEqual(pod.Spec.NodeSelector, map[string]string{gvisorLabel: "gvisor"}) {
				t.Errorf("node selector = %v, want it unchanged", pod.Spec.NodeSelector)
			}
		})
	}

	t.Run("the requirements aren't shared with the pod", func(t *testing.T) {
		reqs := []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}
		pod := &corev1.Pod{}
		addRequiredNodeAffinity(pod, reqs)
		reqs[0].Values[0] = "changed"
		if got := requiredTerms(pod)[0].MatchExpressions[0].Values[0]; got != "n2-standard-4" {
			t.Errorf("pod's requirement value = %q after changing the input, want n2-standard-4", got)
		}
	})
}

func TestNodeFits(t *testing.T) {
	pod := placementPod()
	addRequiredNodeAffinity(pod, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")})
	now := metav1.Now()
	tolerating := func(toleration corev1.Toleration) *corev1.Pod {
		p := pod.DeepCopy()
		p.Spec.Tolerations = append(p.Spec.Tolerations, toleration)
		return p
	}
	tierTaint := func(n *corev1.Node) {
		n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "example.com/tier", Value: "5", Effect: corev1.TaintEffectNoSchedule})
	}

	cases := []struct {
		name string
		node *corev1.Node
		pod  *corev1.Pod
		want bool
	}{
		{name: "Ready node of the type", node: placementNode("n", "n2-standard-4"), want: true},
		{name: "another machine type", node: placementNode("n", "n2-standard-8")},
		{name: "not Ready", node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionUnknown })},
		{name: "no Ready condition", node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { n.Status.Conditions = nil })},
		{name: "being deleted", node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { n.DeletionTimestamp = &now })},
		{name: "cordoned", node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true })},
		{
			name: "cordoned, and the pod tolerates that",
			node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true }),
			pod:  tolerating(corev1.Toleration{Key: corev1.TaintNodeUnschedulable, Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}),
			want: true,
		},
		{name: "not a gVisor node, which the pod selects", node: placementNode("n", "n2-standard-4", func(n *corev1.Node) { delete(n.Labels, gvisorLabel) })},
		{
			name: "a NoSchedule taint the pod doesn't tolerate",
			node: placementNode("n", "n2-standard-4", func(n *corev1.Node) {
				n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule})
			}),
		},
		{
			name: "a NoExecute taint the pod doesn't tolerate",
			node: placementNode("n", "n2-standard-4", func(n *corev1.Node) {
				n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoExecute})
			}),
		},
		{
			name: "a PreferNoSchedule taint doesn't block",
			node: placementNode("n", "n2-standard-4", func(n *corev1.Node) {
				n.Spec.Taints = append(n.Spec.Taints, corev1.Taint{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectPreferNoSchedule})
			}),
			want: true,
		},
		{
			// As the scheduler matches them with the
			// TaintTolerationComparisonOperators feature gate.
			name: "a NoSchedule taint the pod tolerates with Gt",
			node: placementNode("n", "n2-standard-4", tierTaint),
			pod:  tolerating(corev1.Toleration{Key: "example.com/tier", Operator: corev1.TolerationOpGt, Value: "3", Effect: corev1.TaintEffectNoSchedule}),
			want: true,
		},
		{
			name: "a NoSchedule taint above what the pod tolerates with Lt",
			node: placementNode("n", "n2-standard-4", tierTaint),
			pod:  tolerating(corev1.Toleration{Key: "example.com/tier", Operator: corev1.TolerationOpLt, Value: "3", Effect: corev1.TaintEffectNoSchedule}),
		},
		{
			// The API server accepts Gt with a value that isn't a number,
			// but the scheduler can't parse it, so the term matches no
			// node. Match reports that as an error with false.
			name: "a required term that the scheduler can't parse",
			node: placementNode("n", "n2-standard-4"),
			pod: func() *corev1.Pod {
				p := pod.DeepCopy()
				term := &p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0]
				term.MatchExpressions = append(term.MatchExpressions, corev1.NodeSelectorRequirement{Key: "example.com/tier", Operator: corev1.NodeSelectorOpGt, Values: []string{"abc"}})
				return p
			}(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := pod
			if tc.pod != nil {
				p = tc.pod
			}
			if got := nodeFits(context.Background(), tc.node, p, nodeaffinity.GetRequiredNodeAffinity(p)); got != tc.want {
				t.Errorf("nodeFits() = %t, want %t", got, tc.want)
			}
		})
	}
}

// TestRestorePlacement_KeepsValidConstraints pins the valid side of the
// check that drops an engine's invalid constraints: a valid requirement with
// any operator that node affinity accepts must reach the pod.
func TestRestorePlacement_KeepsValidConstraints(t *testing.T) {
	valid := []corev1.NodeSelectorRequirement{
		instanceTypeIn("n2-standard-4"),
		{Key: "example.com/cpu", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"icelake"}},
		{Key: "example.com/gpu", Operator: corev1.NodeSelectorOpExists},
		{Key: "example.com/spot", Operator: corev1.NodeSelectorOpDoesNotExist},
		{Key: "example.com/cores", Operator: corev1.NodeSelectorOpGt, Values: []string{"4"}},
		{Key: "example.com/cores", Operator: corev1.NodeSelectorOpLt, Values: []string{"64"}},
	}
	for _, req := range valid {
		t.Run(string(req.Operator), func(t *testing.T) {
			eng := &placementEngine{
				orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
				reqs:           []corev1.NodeSelectorRequirement{req},
			}
			r := &PodGateReconciler{RestorePlacement: true, Engines: registryOf(t, eng)}
			got, err := r.restorePlacement(context.Background(), placementJob())
			if err != nil || !reflect.DeepEqual(got, []corev1.NodeSelectorRequirement{req}) {
				t.Errorf("restorePlacement() = %+v, %v; want %+v kept", got, err, req)
			}
		})
	}
}

func TestLabelSelectorFor(t *testing.T) {
	selector, ok := labelSelectorFor([]corev1.NodeSelectorRequirement{
		instanceTypeIn("n2-standard-4"),
		{Key: gvisorLabel, Operator: corev1.NodeSelectorOpExists},
	})
	if !ok {
		t.Fatal("labelSelectorFor() = false, want a selector")
	}
	matching := map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4", gvisorLabel: "gvisor"}
	if !selector.Matches(labels.Set(matching)) {
		t.Errorf("selector %q doesn't match %v", selector, matching)
	}
	other := map[string]string{corev1.LabelInstanceTypeStable: "e2-medium", gvisorLabel: "gvisor"}
	if selector.Matches(labels.Set(other)) {
		t.Errorf("selector %q matches %v", selector, other)
	}
	if _, ok := labelSelectorFor([]corev1.NodeSelectorRequirement{{Key: "k", Operator: "Near", Values: []string{"v"}}}); ok {
		t.Error("labelSelectorFor() with an unknown operator = true, want false")
	}
	if _, ok := labelSelectorFor([]corev1.NodeSelectorRequirement{{Key: "k", Operator: corev1.NodeSelectorOpGt, Values: []string{"not-a-number"}}}); ok {
		t.Error("labelSelectorFor() with an invalid Gt value = true, want false")
	}
}

func TestDescribeRequirements(t *testing.T) {
	got := describeRequirements([]corev1.NodeSelectorRequirement{
		instanceTypeIn("n2-standard-4"),
		{Key: gvisorLabel, Operator: corev1.NodeSelectorOpExists},
		{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"a", "b"}},
	})
	want := "node.kubernetes.io/instance-type In [n2-standard-4], sandbox.gke.io/runtime Exists, topology.kubernetes.io/zone NotIn [a, b]"
	if got != want {
		t.Errorf("describeRequirements() = %q, want %q", got, want)
	}
}

// placementFlow drives the pod gate and the job reconciler over one fake
// client, with the invariants in observe mode, so a test can reconcile them
// in turn and check which invariants fired.
type placementFlow struct {
	t        *testing.T
	cl       client.Client
	recorder *record.FakeRecorder
	gate     *PodGateReconciler
	jobs     *PodMigrationJobReconciler
}

func newPlacementFlow(t *testing.T, job *pmv1alpha1.PodMigrationJob, nodes ...client.Object) *placementFlow {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	start := metav1.NewTime(time.Now().Add(-10 * time.Second))
	job.Status.RestoringStartTime = &start
	cl := newFakeClientBuilderWithEventIndex(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(append([]client.Object{placementPod(), job}, nodes...)...).
		Build()
	recorder := record.NewFakeRecorder(100)
	checks := invariants.NewEngine(invariants.ModeObserve, recorder)
	return &placementFlow{
		t:        t,
		cl:       cl,
		recorder: recorder,
		gate:     &PodGateReconciler{Client: cl, Scheme: scheme, Recorder: recorder, InvariantEngine: checks, RestorePlacement: true},
		jobs:     &PodMigrationJobReconciler{Client: cl, Scheme: scheme, Recorder: recorder, InvariantEngine: checks},
	}
}

// reconcileGate reconciles the replacement with the pod gate.
func (f *placementFlow) reconcileGate(step string) {
	f.t.Helper()
	if _, err := f.gate.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}}); err != nil {
		f.t.Fatalf("%s: pod gate Reconcile() error = %v", step, err)
	}
}

// reconcileJob reconciles the job with the job reconciler.
func (f *placementFlow) reconcileJob(step string) {
	f.t.Helper()
	if _, err := f.jobs.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}}); err != nil {
		f.t.Fatalf("%s: job Reconcile() error = %v", step, err)
	}
}

func (f *placementFlow) pod() *corev1.Pod {
	f.t.Helper()
	pod := &corev1.Pod{}
	if err := f.cl.Get(context.Background(), types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, pod); err != nil {
		f.t.Fatalf("getting the pod: %v", err)
	}
	return pod
}

func (f *placementFlow) job() *pmv1alpha1.PodMigrationJob {
	f.t.Helper()
	job := &pmv1alpha1.PodMigrationJob{}
	if err := f.cl.Get(context.Background(), types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}, job); err != nil {
		f.t.Fatalf("getting the job: %v", err)
	}
	return job
}

// events drains the recorded events.
func (f *placementFlow) events() []string {
	var events []string
	for len(f.recorder.Events) > 0 {
		events = append(events, <-f.recorder.Events)
	}
	return events
}

// violations returns the invariant violations among events.
func violations(events []string) []string {
	var found []string
	for _, e := range events {
		if strings.Contains(e, invariants.EventReasonInvariantViolation) {
			found = append(found, e)
		}
	}
	return found
}

// checkReleasedCold checks that the flow ended with the replacement released
// cold, without the constraint, and the job concluded and consumed.
func (f *placementFlow) checkReleasedCold() {
	f.t.Helper()
	pod := f.pod()
	if podHasMigrationGate(pod) {
		f.t.Fatal("pod is still gated, want it released")
	}
	if got, ok := pod.Annotations["podsnapshot.gke.io/ps-name"]; !ok || got != "" {
		f.t.Errorf("ps-name = %q (set %t), want the cold-start bypass", got, ok)
	}
	if terms := requiredTerms(pod); terms != nil {
		f.t.Errorf("pod has required node affinity %+v, want none", terms)
	}
	if job := f.job(); job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || !job.Status.Consumed {
		f.t.Errorf("job status = %+v, want SucceededWithoutRestore and consumed", job.Status)
	}
}

// TestRestorePlacement_NoCompatibleNodeFlow runs the whole flow with no node
// that can restore the checkpoint, through both reconcilers with the
// invariants on: the pod gate holds the replacement, the job concludes
// without a restore, and the pod gate releases the replacement cold. No
// invariant may fire on the way, I2 in particular.
func TestRestorePlacement_NoCompatibleNodeFlow(t *testing.T) {
	f := newPlacementFlow(t, placementJob(), placementNode("e2-a", "e2-medium"))

	f.reconcileGate("pod gate holds")
	if !podHasMigrationGate(f.pod()) {
		t.Fatal("pod was released, want it held")
	}
	f.reconcileJob("job concludes")
	if phase := f.job().Status.Phase; phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("phase = %s, want SucceededWithoutRestore", phase)
	}
	f.reconcileGate("pod gate releases cold")
	f.reconcileJob("job settles")
	f.checkReleasedCold()

	events := f.events()
	for _, v := range violations(events) {
		t.Errorf("invariant violation: %s", v)
	}
	if !hasEventWith(events, "Warning", ReasonNoCompatibleNode) {
		t.Errorf("events = %q, want %s Warnings", events, ReasonNoCompatibleNode)
	}

	// The checks above would see I2: had the job concluded Succeeded with
	// the replacement started cold, it fires.
	succeeded := f.job()
	succeeded.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	if err := f.cl.Status().Update(context.Background(), succeeded); err != nil {
		t.Fatalf("marking the job Succeeded: %v", err)
	}
	f.reconcileJob("job reconciled as Succeeded")
	if !hasEventWith(violations(f.events()), "[I2:") {
		t.Error("no I2 violation for a Succeeded job whose replacement started cold; the invariant checks aren't running")
	}
}

// TestRestorePlacement_StrandedJobHoldFlow runs the flow for a job that a
// replacement consumed and then died before its gate was released, when no
// node can restore the checkpoint for the next replacement. The hold clears
// the dead pod's record, so the job concludes unconsumed while the next
// replacement is still gated, and no invariant fires at any step: a job that
// concluded consumed with a gated pod would be an I3 violation. The cold
// release then records the pod that started cold, as in the flow without a
// dead consumer.
func TestRestorePlacement_StrandedJobHoldFlow(t *testing.T) {
	job := placementJob()
	job.Status.Consumed = true
	job.Status.RestoredPodUID = "dead-uid"
	job.Status.RestoredPodName = "dead-replacement"
	f := newPlacementFlow(t, job, placementNode("e2-a", "e2-medium"))

	f.reconcileGate("pod gate holds")
	f.reconcileJob("job concludes")
	if got := f.job().Status; got.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || got.Consumed {
		t.Fatalf("job status = %+v, want SucceededWithoutRestore and unconsumed", got)
	}
	for _, v := range violations(f.events()) {
		t.Errorf("invariant violation while the pod is held: %s", v)
	}

	f.reconcileGate("pod gate releases cold")
	f.reconcileJob("job settles")
	f.checkReleasedCold()
	for _, v := range violations(f.events()) {
		t.Errorf("invariant violation after the release: %s", v)
	}
	if got := f.job().Status; got.RestoredPodUID != placementPodUID || got.RestoredPodName != placementPodName {
		t.Errorf("job records pod %s (%s), want the pod that started cold", got.RestoredPodName, got.RestoredPodUID)
	}
}
