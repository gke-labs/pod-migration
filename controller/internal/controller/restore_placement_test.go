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
	"fmt"
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

// ownedPlacementPod is placementPod owned by a StatefulSet, which would
// recreate it if it were deleted.
func ownedPlacementPod() *corev1.Pod {
	pod := placementPod()
	controller := true
	pod.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1",
		Kind:       "StatefulSet",
		Name:       "redis",
		UID:        "statefulset-uid",
		Controller: &controller,
	}}
	return pod
}

// placementPodWith is placementPod changed by mutate.
func placementPodWith(mutate ...func(*corev1.Pod)) *corev1.Pod {
	pod := placementPod()
	for _, m := range mutate {
		m(pod)
	}
	return pod
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
		WithObjects(withReadySnapshots(objs, engine.DefaultAPIGroup, "engine.example.com", engine.CRIUAPIGroup)...).
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
		run := reconcilePlacement(t, true, nil, ownedPlacementPod(), placementJob(),
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
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if !restorePlacementRecorded(run.job) || !strings.Contains(cond.Message, "node.kubernetes.io/instance-type In [n2-standard-4]") || strings.Contains(cond.Message, "no node met") {
			t.Errorf("RestorePlacement condition = %+v, want True (%s) naming the constraint", cond, ReasonNodeConstraintsAdded)
		}
		if !hasEventWith(run.events, "MigrationRestoreReleased", "n2-standard-4") || hasEventWith(run.events, "no node meets it yet") {
			t.Errorf("events = %q, want MigrationRestoreReleased naming the constraint", run.events)
		}
	})

	t.Run("no node fits an owned pod: releases it with the constraint to wait for one", func(t *testing.T) {
		// A node of the type can still come up, for example one that the
		// cluster autoscaler adds once it sees the pod. If none does, the
		// job reconciler deletes the pod after the placement deadline for
		// its StatefulSet to recreate, if a node of another type could take
		// it.
		run := reconcilePlacement(t, true, nil, ownedPlacementPod(), placementJob(),
			placementNode("n2-cordoned", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true }),
			placementNode("e2-a", "e2-medium"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || run.pod.Annotations["podsnapshot.gke.io/ps-name"] != placementSnapshot {
			t.Errorf("pod gated %t, ps-name %q; want it released with the restore order", podHasMigrationGate(run.pod), run.pod.Annotations["podsnapshot.gke.io/ps-name"])
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, wantTerms)
		}
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if !run.job.Status.GateReleased || !restorePlacementRecorded(run.job) || !strings.Contains(cond.Message, "no node met it at the release") {
			t.Errorf("GateReleased %t, RestorePlacement condition %+v; want the release recorded, with no node met", run.job.Status.GateReleased, cond)
		}
		if _, held := noCompatibleNode(run.job); held || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("job status = %+v, want it still Restoring and not held", run.job.Status)
		}
		if !hasEventWith(run.events, "MigrationRestoreReleased", "n2-standard-4", "no node meets it yet") {
			t.Errorf("events = %q, want the release event to say no node meets the constraint yet", run.events)
		}
	})

	t.Run("a pod that no ReplicaSet or StatefulSet recreates: released without the constraint, and the engine isn't asked", func(t *testing.T) {
		// Nothing would recreate it without the constraint if it got stuck
		// with it, and a node that passes the check can still turn it away,
		// for lack of room, for one. So it's placed as with placement off,
		// whether or not a node meets the constraint. On a node of another
		// type, the restore-crash fallback handles a failed restore, as
		// with placement off.
		pods := map[string]func() *corev1.Pod{
			"without a controller": placementPod,
			"a Job's":              func() *corev1.Pod { return placementPodWith(controlledBy("batch/v1", "Job")) },
			"an operator's":        func() *corev1.Pod { return placementPodWith(controlledBy("ray.io/v1", "RayCluster")) },
		}
		nodeSets := map[string]func() []client.Object{
			"a node meets the constraint": func() []client.Object {
				return []client.Object{placementNode("n2-a", "n2-standard-4"), placementNode("e2-a", "e2-medium")}
			},
			"no node meets it": func() []client.Object { return []client.Object{placementNode("e2-a", "e2-medium")} },
		}
		for podName, pod := range pods {
			for nodesName, nodes := range nodeSets {
				t.Run(podName+", "+nodesName, func(t *testing.T) {
					eng := &placementEngine{
						orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
						reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
					}
					run := reconcilePlacement(t, true, registryOf(t, eng), append([]client.Object{pod(), placementJob()}, nodes()...)...)
					if run.err != nil {
						t.Fatalf("Reconcile() error = %v", run.err)
					}
					if podHasMigrationGate(run.pod) || !run.job.Status.GateReleased || run.pod.Annotations[routedRestoreAnnotation] != placementSnapshot {
						t.Errorf("pod gated %t, restore order %q, job status %+v; want it released with the restore order", podHasMigrationGate(run.pod), run.pod.Annotations[routedRestoreAnnotation], run.job.Status)
					}
					if terms := requiredTerms(run.pod); terms != nil {
						t.Errorf("pod has required node affinity %+v, want none", terms)
					}
					if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
						t.Errorf("RestorePlacement condition = %+v, want none", cond)
					}
					if eng.calls != 0 {
						t.Errorf("Placement() called %d times, want 0", eng.calls)
					}
					if !hasEventWith(run.events, "MigrationRestoreReleased", "without adding node constraints") {
						t.Errorf("events = %q, want the release event to say no node constraints were added", run.events)
					}
				})
			}
		}
	})

	t.Run("the pod's own node affinity already requires the constraint: releases it without adding or checking", func(t *testing.T) {
		// Deleting it at the placement deadline couldn't get it scheduled
		// anywhere else: a pod recreated in its place would have the same
		// affinity. It would otherwise be released with a record, and the
		// deadline would delete it.
		for name, pod := range map[string]*corev1.Pod{"owned": ownedPlacementPod()} {
			t.Run(name, func(t *testing.T) {
				addRequiredNodeAffinity(pod, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")})
				run := reconcilePlacement(t, true, nil, pod, placementJob(), placementNode("e2-a", "e2-medium"))
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if podHasMigrationGate(run.pod) || run.pod.Annotations["podsnapshot.gke.io/ps-name"] != placementSnapshot {
					t.Errorf("pod gated %t, ps-name %q; want it released with the restore order", podHasMigrationGate(run.pod), run.pod.Annotations["podsnapshot.gke.io/ps-name"])
				}
				if got := requiredTerms(run.pod); !reflect.DeepEqual(got, wantTerms) {
					t.Errorf("required node affinity terms = %+v, want its own, %+v", got, wantTerms)
				}
				if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
					t.Errorf("RestorePlacement condition = %+v, want none", cond)
				}
				if !hasEventWith(run.events, "MigrationRestoreReleased", "without adding node constraints") {
					t.Errorf("events = %q, want the release event to say no node constraints were added", run.events)
				}
			})
		}
	})

	t.Run("one of the pod's terms lacks the constraint: adds it there and checks", func(t *testing.T) {
		zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
		pod := ownedPlacementPod()
		pod.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA}},
			},
		}}}
		// The e2 node meets the pod's second term, but not with the
		// constraint added to it.
		run := reconcilePlacement(t, true, nil, pod, placementJob(), placementNode("e2-a", "e2-medium", func(n *corev1.Node) {
			n.Labels[corev1.LabelTopologyZone] = "a"
		}))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		want := []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA, instanceTypeIn("n2-standard-4")}},
		}
		if got := requiredTerms(run.pod); podHasMigrationGate(run.pod) || !reflect.DeepEqual(got, want) {
			t.Errorf("pod gated %t, required node affinity terms %+v; want it released with %+v", podHasMigrationGate(run.pod), got, want)
		}
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if !restorePlacementRecorded(run.job) || !strings.Contains(cond.Message, "no node met it at the release") {
			t.Errorf("RestorePlacement condition = %+v, want the release recorded, with no node met", cond)
		}
	})

	t.Run("the pod's node selector already pins the machine type: releases it without adding or checking", func(t *testing.T) {
		for name, pinned := range map[string]*corev1.Pod{"owned": ownedPlacementPod()} {
			t.Run(name, func(t *testing.T) {
				pinned.Spec.NodeSelector[corev1.LabelInstanceTypeStable] = "n2-standard-4"
				run := reconcilePlacement(t, true, nil, pinned, placementJob(), placementNode("e2-a", "e2-medium"))
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
					t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
				}
				if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
					t.Errorf("RestorePlacement condition = %+v, want none", cond)
				}
			})
		}
	})

	t.Run("a hold that an earlier version recorded: keeps holding, even once a node fits", func(t *testing.T) {
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
		// Only a Restoring job gets constraints, because only then does the
		// job reconciler's placement deadline back them: a pod released
		// with them on a job in another phase could stay stuck. Another
		// pod's job is released cold before the check, so this one isn't
		// consumed: a new release in another phase goes ahead as without
		// placement.
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
		}
		job := placementJob()
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), job, placementNode("e2-a", "e2-medium"))
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
		if eng.calls != 0 {
			t.Errorf("Placement() called %d times, want 0", eng.calls)
		}
	})

	t.Run("another pod's job that isn't Restoring: released cold without a check", func(t *testing.T) {
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
		}
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), otherPodsSucceededJob(), placementNode("e2-a", "e2-medium"))
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

	t.Run("RestorePlacement off: releases without a constraint", func(t *testing.T) {
		run := reconcilePlacement(t, false, nil, ownedPlacementPod(), placementJob())
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
		if hasEventWith(run.events, "node constraints") {
			t.Errorf("events = %q, want the release event as before placement", run.events)
		}
	})

	t.Run("machine type unknown: releases without a constraint or a check", func(t *testing.T) {
		// The source node couldn't be read while the job was Pending, so
		// nothing was recorded, and no node is known to fit either.
		job := placementJob()
		job.Status.OriginNodeInstanceType = ""
		job.Status.OriginNodeKubeletVersion = ""
		run := reconcilePlacement(t, true, nil, ownedPlacementPod(), job)
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement); cond != nil {
			t.Errorf("RestorePlacement condition = %+v, want none", cond)
		}
		if !hasEventWith(run.events, "MigrationRestoreReleased", "without adding node constraints") {
			t.Errorf("events = %q, want the release event to say there are no node constraints", run.events)
		}
	})

	// failNodeList fails the nth list of nodes, counting from 1.
	failNodeList := func(n int) interceptor.Funcs {
		lists := 0
		return interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				if lists++; lists == n {
					return errors.New("cache not synced")
				}
			}
			return c.List(ctx, list, opts...)
		}}
	}
	// underWayJob returns the placement job as an earlier attempt to release
	// the pod left it: consumed by the pod, and with GateReleased if
	// gateReleased.
	underWayJob := func(gateReleased bool) *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = placementPodUID
		job.Status.RestoredPodName = placementPodName
		job.Status.GateReleased = gateReleased
		return job
	}
	// releasedWith checks that the pod was released, with terms in its
	// required node affinity, and the job records whether that was with
	// constraints.
	releasedWith := func(t *testing.T, run placementRun, terms []corev1.NodeSelectorTerm) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || !run.job.Status.GateReleased {
			t.Errorf("pod gated %t, GateReleased %t; want it released", podHasMigrationGate(run.pod), run.job.Status.GateReleased)
		}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, terms) {
			t.Errorf("required node affinity terms = %+v, want %+v", got, terms)
		}
		// A release with the constraint is recorded now, so the placement
		// deadline counts from it.
		if recorded := restorePlacementRecorded(run.job); recorded != (terms != nil) {
			t.Errorf("RestorePlacement recorded %t, conditions %+v; want %t", recorded, run.job.Status.Conditions, terms != nil)
		}
	}

	t.Run("a release under way adds what the first attempt did, without a new check", func(t *testing.T) {
		// A failed GateReleased patch (Consumed recorded) or a failed pod
		// update (GateReleased recorded) is retried. An owned pod gets the
		// constraint whatever the nodes look like now, and a pod that
		// nothing recreates gets none. The nodes aren't listed: a list that
		// fails doesn't keep the pod gated.
		for _, gateReleased := range []bool{false, true} {
			t.Run(fmt.Sprintf("GateReleased %t", gateReleased), func(t *testing.T) {
				t.Run("owned: released with the constraint, which no node meets", func(t *testing.T) {
					releasedWith(t, reconcilePlacementWith(t, true, nil, failNodeList(1), ownedPlacementPod(), underWayJob(gateReleased), placementNode("e2-a", "e2-medium")), wantTerms)
				})
				t.Run("nothing recreates it: released without the constraint, even if a node meets it", func(t *testing.T) {
					releasedWith(t, reconcilePlacementWith(t, true, nil, failNodeList(1), placementPod(), underWayJob(gateReleased), placementNode("n2-a", "n2-standard-4")), nil)
				})
			})
		}
	})

	t.Run("a retry that adds no constraints removes an earlier attempt's record", func(t *testing.T) {
		// The record must match the pod, or the placement deadline would act
		// on a pod without the constraints.
		recordedJob := func() *pmv1alpha1.PodMigrationJob {
			job := underWayJob(true)
			recordRestorePlacement(job, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}, false)
			return job
		}
		t.Run("nothing recreates the pod", func(t *testing.T) {
			// An earlier version may have released it with them.
			releasedWith(t, reconcilePlacement(t, true, nil, placementPod(), recordedJob(), placementNode("n2-a", "n2-standard-4")), nil)
		})
		t.Run("placement turned off since", func(t *testing.T) {
			releasedWith(t, reconcilePlacement(t, false, nil, ownedPlacementPod(), recordedJob(), placementNode("e2-a", "e2-medium")), nil)
		})
	})

	t.Run("the job's engine states the constraints", func(t *testing.T) {
		custom := corev1.NodeSelectorRequirement{Key: "example.com/cpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"icelake"}}
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{custom},
		}
		node := placementNode("icelake", "n2-standard-4", func(n *corev1.Node) { n.Labels["example.com/cpu"] = "icelake" })
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), placementJob(), node)
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
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), placementJob())
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) || requiredTerms(run.pod) != nil {
			t.Errorf("pod gated %t, terms %+v; want it released without a constraint", podHasMigrationGate(run.pod), requiredTerms(run.pod))
		}
	})

	strandedJob := func() *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		job.Status.Consumed = true
		job.Status.RestoredPodUID = "dead-uid"
		job.Status.RestoredPodName = "dead-replacement"
		return job
	}

	t.Run("a dead pod consumed the job and no node fits: adopts it and releases as for a new release", func(t *testing.T) {
		// The dead pod's gate was never released, so it never restored.
		// The new pod adopts the job: an owned one is released with the
		// constraint to wait for a node, and one that nothing recreates
		// without it.
		for name, tc := range map[string]struct {
			pod   *corev1.Pod
			terms []corev1.NodeSelectorTerm
		}{
			"owned":                {ownedPlacementPod(), wantTerms},
			"without a controller": {placementPod(), nil},
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcilePlacement(t, true, nil, tc.pod, strandedJob(), placementNode("e2-a", "e2-medium"))
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if got := run.job.Status; podHasMigrationGate(run.pod) || !got.Consumed || got.RestoredPodUID != placementPodUID || got.RestoredPodName != placementPodName || !got.GateReleased {
					t.Errorf("pod gated %t, job status %+v; want the pod to adopt the job and be released", podHasMigrationGate(run.pod), got)
				}
				if got := requiredTerms(run.pod); !reflect.DeepEqual(got, tc.terms) {
					t.Errorf("required node affinity terms = %+v, want %+v", got, tc.terms)
				}
				if _, held := noCompatibleNode(run.job); held {
					t.Errorf("conditions = %+v, want no hold", run.job.Status.Conditions)
				}
			})
		}
	})

	t.Run("a dead pod consumed the job and a node fits: adopts and releases with the constraint", func(t *testing.T) {
		// The constraint it releases with is the one it checked: the
		// engine is asked once, before the adoption.
		eng := &placementEngine{
			orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
			reqs:           []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
		}
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), strandedJob(), placementNode("n2-a", "n2-standard-4"))
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
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), job)
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
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), job)
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
		run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), placementJob())
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
		run := reconcilePlacementWith(t, true, nil, funcs, ownedPlacementPod(), placementJob(), placementNode("n2-a", "n2-standard-4"))
		if run.err == nil || !strings.Contains(run.err.Error(), "node cache unavailable") {
			t.Fatalf("Reconcile() error = %v, want the List error", run.err)
		}
		if !podHasMigrationGate(run.pod) || run.job.Status.Consumed || len(run.job.Status.Conditions) != 0 {
			t.Errorf("pod gated %t, job status %+v; want the pod gated and the job untouched", podHasMigrationGate(run.pod), run.job.Status)
		}
	})

	t.Run("the recorded consumer is still terminating: waits before checking", func(t *testing.T) {
		// The consumer that is going away comes first: once it's gone, this
		// pod can adopt the job. Nothing is asked or recorded meanwhile.
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
		run := reconcilePlacement(t, true, nil, ownedPlacementPod(), job, terminating, placementNode("e2-a", "e2-medium"))
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

	t.Run("a hold that an earlier version recorded, and placement turned off since: keeps holding", func(t *testing.T) {
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
				run := reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), placementJob(), placementNode("n2-a", "n2-standard-4"))
				releasedWithout(t, run, eng)
				if !hasEventWith(run.events, "MigrationRestoreReleased", "without adding node constraints") {
					t.Errorf("events = %q, want the release event to say no node constraints were added", run.events)
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
			releasedWithout(t, reconcilePlacement(t, true, registryOf(t, eng), ownedPlacementPod(), job), eng)
		})
	})

	t.Run("a stale view on a retry can't release the pod onto a concluded job", func(t *testing.T) {
		// This pod consumed the job, and the GateReleased write failed; on
		// the retry the Consumed write is skipped. Meanwhile the job
		// concluded without a restore. The GateReleased patch checks the
		// resourceVersion, so the stale view can't release the pod with a
		// restore order onto a job that reports a cold start. The conflict
		// is expected on a retry, so it's requeued quietly. And the failed
		// patch leaves the job in memory as it was read.
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
			WithObjects(ownedPlacementPod(), consumed, placementNode("n2-a", "n2-standard-4")).
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
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}})
		stale = nil
		if err != nil || res.RequeueAfter <= 0 {
			t.Fatalf("Reconcile() = %+v, %v; want a requeue without an error", res, err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, key, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		if got.Status.GateReleased || restorePlacementRecorded(got) {
			t.Errorf("job status = %+v, want no gate release recorded on the concluded job", got.Status)
		}
		if patched == nil || patched.Status.GateReleased || restorePlacementRecorded(patched) {
			t.Errorf("job in memory after the failed patch = %+v, want it as it was read, without GateReleased or a placement record", patched)
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

func TestSomeNodeFits(t *testing.T) {
	reqs := []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}
	someNodeFitsWith := func(t *testing.T, funcs interceptor.Funcs, nodes ...client.Object) bool {
		t.Helper()
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodes...).WithInterceptorFuncs(funcs).Build()
		r := &PodGateReconciler{Client: cl, Scheme: scheme}
		fits, err := r.someNodeFits(context.Background(), placementPod(), reqs)
		if err != nil {
			t.Fatalf("someNodeFits() error = %v", err)
		}
		return fits
	}

	t.Run("narrows the node list by the constraint's labels", func(t *testing.T) {
		var selector string
		funcs := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			listOpts := &client.ListOptions{}
			listOpts.ApplyOptions(opts)
			if listOpts.LabelSelector != nil {
				selector = listOpts.LabelSelector.String()
			}
			return c.List(ctx, list, opts...)
		}}
		if !someNodeFitsWith(t, funcs, placementNode("n2-a", "n2-standard-4")) {
			t.Error("someNodeFits() = false, want true")
		}
		if selector != "node.kubernetes.io/instance-type in (n2-standard-4)" {
			t.Errorf("node list label selector = %q, want the constraint", selector)
		}
	})

	t.Run("checks the constraint on a list it couldn't narrow", func(t *testing.T) {
		// An engine's constraint may not convert to a label selector; then
		// every node is listed, and the node check must reject the wrong
		// machine type itself.
		ignoreSelector := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return c.List(ctx, list)
		}}
		if someNodeFitsWith(t, ignoreSelector, placementNode("n2x8-a", "n2-standard-8")) {
			t.Error("someNodeFits() = true for a node of another machine type, want false")
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

// requiredAffinity returns an affinity that requires terms.
func requiredAffinity(terms ...corev1.NodeSelectorTerm) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: terms},
	}}
}

// termOf returns a node selector term of reqs.
func termOf(reqs ...corev1.NodeSelectorRequirement) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{MatchExpressions: reqs}
}

// TestWithoutRequirements pins that withoutRequirements undoes
// addRequiredNodeAffinity, so the backstop judges the pod as its owner
// recreates it.
func TestWithoutRequirements(t *testing.T) {
	req := instanceTypeIn("n2-standard-4")
	reqs := []corev1.NodeSelectorRequirement{req}
	zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	zoneB := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}
	byName := corev1.NodeSelectorRequirement{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"node-1"}}
	preferred := []corev1.PreferredSchedulingTerm{{Weight: 1, Preference: termOf(zoneB)}}

	for _, tc := range []struct {
		name string
		in   *corev1.Affinity
	}{
		{name: "no affinity"},
		{name: "only preferred terms", in: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{PreferredDuringSchedulingIgnoredDuringExecution: preferred}}},
		{name: "pod affinity only", in: &corev1.Affinity{PodAffinity: &corev1.PodAffinity{}}},
		{name: "terms of its own", in: requiredAffinity(termOf(zoneA), termOf(zoneB), corev1.NodeSelectorTerm{MatchFields: []corev1.NodeSelectorRequirement{byName}})},
		{name: "an empty term of its own", in: requiredAffinity(corev1.NodeSelectorTerm{}, termOf(zoneA))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: tc.in.DeepCopy()}}
			placed := pod.DeepCopy()
			addRequiredNodeAffinity(placed, reqs)
			before := placed.DeepCopy()
			got := withoutRequirements(placed, reqs)
			if !reflect.DeepEqual(requiredTerms(got), requiredTerms(pod)) {
				t.Errorf("required terms = %+v, want the pod's own, %+v", requiredTerms(got), requiredTerms(pod))
			}
			if !reflect.DeepEqual(placed, before) {
				t.Errorf("the input changed to %+v", placed.Spec.Affinity)
			}
		})
	}

	t.Run("a term of its own that has the requirement loses it too", func(t *testing.T) {
		// restorePlacement adds nothing when every term has it, so this
		// only happens when the pod's terms differ in it.
		placed := &corev1.Pod{Spec: corev1.PodSpec{Affinity: requiredAffinity(termOf(req, zoneA), termOf(zoneB))}}
		addRequiredNodeAffinity(placed, reqs)
		want := []corev1.NodeSelectorTerm{termOf(zoneA), termOf(zoneB)}
		if got := requiredTerms(withoutRequirements(placed, reqs)); !reflect.DeepEqual(got, want) {
			t.Errorf("required terms = %+v, want %+v", got, want)
		}
	})
}

func TestNarrows(t *testing.T) {
	req := instanceTypeIn("n2-standard-4")
	zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	for _, tc := range []struct {
		name     string
		affinity *corev1.Affinity
		want     bool
	}{
		{name: "no affinity", want: true},
		{name: "a term without it", affinity: requiredAffinity(termOf(zoneA)), want: true},
		{name: "another value of the key", affinity: requiredAffinity(termOf(instanceTypeIn("n2-standard-8"))), want: true},
		{name: "one of two terms lacks it", affinity: requiredAffinity(termOf(req), termOf(zoneA)), want: true},
		{name: "its only term has it", affinity: requiredAffinity(termOf(req))},
		{name: "every term has it, among others", affinity: requiredAffinity(termOf(zoneA, req), termOf(req))},
	} {
		pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: tc.affinity}}
		if got := narrows(pod, []corev1.NodeSelectorRequirement{req}); got != tc.want {
			t.Errorf("%s: narrows() = %t, want %t", tc.name, got, tc.want)
		}
	}

	for _, tc := range []struct {
		name         string
		nodeSelector map[string]string
		reqs         []corev1.NodeSelectorRequirement
		want         bool
	}{
		{name: "a node selector that pins the type", nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"}, reqs: []corev1.NodeSelectorRequirement{req}},
		{name: "a node selector with another type", nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-8"}, reqs: []corev1.NodeSelectorRequirement{req}, want: true},
		{name: "a node selector without the key", nodeSelector: map[string]string{gvisorLabel: "gvisor"}, reqs: []corev1.NodeSelectorRequirement{req}, want: true},
		{
			name:         "NotIn, pinned to another value",
			nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"},
			reqs:         []corev1.NodeSelectorRequirement{{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"e2-medium"}}},
		},
		{
			name:         "NotIn, pinned to a listed value",
			nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "e2-medium"},
			reqs:         []corev1.NodeSelectorRequirement{{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"e2-medium"}}},
			want:         true,
		},
		{
			name:         "Exists, pinned",
			nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"},
			reqs:         []corev1.NodeSelectorRequirement{{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpExists}},
		},
		{
			name:         "one of two requirements pinned",
			nodeSelector: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"},
			reqs:         []corev1.NodeSelectorRequirement{req, zoneA},
			want:         true,
		},
	} {
		pod := &corev1.Pod{Spec: corev1.PodSpec{NodeSelector: tc.nodeSelector}}
		if got := narrows(pod, tc.reqs); got != tc.want {
			t.Errorf("%s: narrows() = %t, want %t", tc.name, got, tc.want)
		}
	}
}

func TestRecreatedByOwner(t *testing.T) {
	notController := ownedPlacementPod()
	notController.OwnerReferences[0].Controller = nil
	for name, tc := range map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"a StatefulSet":                      {pod: ownedPlacementPod(), want: true},
		"a ReplicaSet":                       {pod: placementPodWith(controlledBy("apps/v1", "ReplicaSet")), want: true},
		"no owner":                           {pod: placementPod()},
		"a Job":                              {pod: placementPodWith(controlledBy("batch/v1", "Job"))},
		"a DaemonSet":                        {pod: placementPodWith(controlledBy("apps/v1", "DaemonSet"))},
		"a ReplicaSet of another API group":  {pod: placementPodWith(controlledBy("example.com/v1", "ReplicaSet"))},
		"an owner that isn't its controller": {pod: notController},
	} {
		if got := recreatedByOwner(tc.pod); got != tc.want {
			t.Errorf("%s: recreatedByOwner() = %t, want %t", name, got, tc.want)
		}
	}
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
			got, err := r.restorePlacement(context.Background(), placementJob(), ownedPlacementPod())
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
	return newPlacementFlowFor(t, placementPod(), job, nodes...)
}

// newPlacementFlowFor is newPlacementFlow with pod as the replacement.
func newPlacementFlowFor(t *testing.T, pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob, nodes ...client.Object) *placementFlow {
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
		WithObjects(withReadySnapshots(append([]client.Object{pod, job}, nodes...))...).
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

// TestRestorePlacement_RecordedHoldFlow runs the flow for a job that an
// earlier version of the pod gate held because no node could restore the
// checkpoint, through both reconcilers with the invariants on: the pod gate
// keeps holding the replacement, the job concludes without a restore, as
// recorded, and the pod gate releases the replacement cold. No invariant
// may fire on the way, I2 in particular.
func TestRestorePlacement_RecordedHoldFlow(t *testing.T) {
	job := placementJob()
	job.Status.Conditions = []metav1.Condition{noCompatibleNodeCondition()}
	f := newPlacementFlow(t, job, placementNode("n2-a", "n2-standard-4"))

	f.reconcileGate("pod gate keeps holding")
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
		t.Errorf("events = %q, want a %s Warning", events, ReasonNoCompatibleNode)
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

// TestRestorePlacement_StrandedJobFlow runs the flow for a job that a
// replacement consumed and then died before its gate was released, when no
// node meets the constraint, for a next replacement that nothing recreates.
// The pod gate doesn't hold it: it adopts the job and is released with the
// restore order and without the constraint, as with placement off. No
// invariant fires at any step; a hold that concluded the job while the pod
// was still gated could have been an I3 violation.
func TestRestorePlacement_StrandedJobFlow(t *testing.T) {
	job := placementJob()
	job.Status.Consumed = true
	job.Status.RestoredPodUID = "dead-uid"
	job.Status.RestoredPodName = "dead-replacement"
	f := newPlacementFlow(t, job, placementNode("e2-a", "e2-medium"))

	f.reconcileGate("pod gate adopts the job and releases the pod")
	pod := f.pod()
	if podHasMigrationGate(pod) || pod.Annotations["podsnapshot.gke.io/ps-name"] != placementSnapshot || requiredTerms(pod) != nil {
		t.Fatalf("pod gated %t, ps-name %q, terms %+v; want it released with the restore order and without the constraint",
			podHasMigrationGate(pod), pod.Annotations["podsnapshot.gke.io/ps-name"], requiredTerms(pod))
	}
	f.reconcileJob("job keeps restoring")
	if got := f.job().Status; got.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || got.RestoredPodUID != placementPodUID || !got.GateReleased {
		t.Errorf("job status = %+v, want it Restoring, released, and recording the new pod", got)
	}
	for _, v := range violations(f.events()) {
		t.Errorf("invariant violation: %s", v)
	}
}

// TestRestorePlacement_PlacementTimeoutFlow runs the whole flow for an owned
// replacement when no node meets the constraint, through both reconcilers
// with the invariants on: the pod gate releases the pod with the constraint
// so that a node can still come up, the scheduler finds none, and after the
// placement deadline the job concludes without a restore and deletes the
// pod. Its owner recreates it, and the new pod, which no job claims, starts
// cold. No invariant may fire on the way. The invariants are checked when a
// reconcile returns, so this flow can't see the order inside the one that
// concludes the job and deletes the pod. That order, the conclusion first so
// that I10 never sees a Restoring job without its replacement, is pinned by
// the status conflict cases of TestPodMigrationJobReconciler_PlacementTimeout,
// which find the pod still there when the conclusion fails.
func TestRestorePlacement_PlacementTimeoutFlow(t *testing.T) {
	f := newPlacementFlowFor(t, ownedPlacementPod(), placementJob(), placementNode("e2-a", "e2-medium"))
	ctx := context.Background()

	f.reconcileGate("pod gate releases with the constraint")
	pod := f.pod()
	if podHasMigrationGate(pod) || requiredTerms(pod) == nil {
		t.Fatalf("pod gated %t, terms %+v; want it released with the constraint", podHasMigrationGate(pod), requiredTerms(pod))
	}
	f.reconcileJob("job waits for the pod")
	if phase := f.job().Status.Phase; phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Fatalf("phase = %s, want Restoring before the deadline", phase)
	}

	// The scheduler finds no node, and the deadline passes. The job has been
	// Restoring for long enough that I10 would report it if its replacement
	// went missing while it's still Restoring.
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	if err := f.cl.Status().Update(ctx, pod); err != nil {
		t.Fatalf("marking the pod unschedulable: %v", err)
	}
	job := f.job()
	released := metav1.NewTime(time.Now().Add(-DefaultRestorePlacementDeadline - time.Minute))
	job.Status.RestoringStartTime = &released
	for i := range job.Status.Conditions {
		if job.Status.Conditions[i].Type == ConditionRestorePlacement {
			job.Status.Conditions[i].LastTransitionTime = released
		}
	}
	if err := f.cl.Status().Update(ctx, job); err != nil {
		t.Fatalf("moving the release back past the deadline: %v", err)
	}

	f.reconcileJob("job concludes and deletes the pod")
	job = f.job()
	cond := meta.FindStatusCondition(job.Status.Conditions, "Restored")
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || cond == nil || cond.Reason != ReasonPlacementTimeout {
		t.Fatalf("job status = %+v, want SucceededWithoutRestore with reason %s", job.Status, ReasonPlacementTimeout)
	}
	if err := f.cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("getting the pod: %v, want it deleted", err)
	}

	// The StatefulSet recreates the pod. No job claims it, so the webhook
	// neither gates nor assigns it, and it starts cold.
	recreated := ownedPlacementPod()
	recreated.UID = "recreated-uid"
	recreated.Annotations = nil
	recreated.Spec.SchedulingGates = nil
	if err := f.cl.Create(ctx, recreated); err != nil {
		t.Fatalf("recreating the pod: %v", err)
	}
	f.reconcileJob("job settles")
	if err := f.cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, &corev1.Pod{}); err != nil {
		t.Fatalf("getting the recreated pod: %v, want it kept", err)
	}

	events := f.events()
	for _, v := range violations(events) {
		t.Errorf("invariant violation: %s", v)
	}
	if !hasEventWith(events, "Warning", ReasonPlacementTimeout) {
		t.Errorf("events = %q, want %s Warnings", events, ReasonPlacementTimeout)
	}
}
