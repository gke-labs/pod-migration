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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

// releasedJob returns a Restoring job whose replacement the pod gate
// released with the machine-type constraint releasedAgo.
func releasedJob(releasedAgo time.Duration) *pmv1alpha1.PodMigrationJob {
	job := placementJob()
	released := metav1.NewTime(time.Now().Add(-releasedAgo))
	job.Status.RestoringStartTime = &released
	job.Status.Consumed = true
	job.Status.RestoredPodUID = placementPodUID
	job.Status.RestoredPodName = placementPodName
	job.Status.GateReleased = true
	job.Status.Conditions = []metav1.Condition{{
		Type:               ConditionRestorePlacement,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonNodeConstraintsAdded,
		Message:            "Released with required node affinity node.kubernetes.io/instance-type In [n2-standard-4]; no node met it at the release",
		LastTransitionTime: released,
	}}
	return job
}

// stuckPod returns the owned replacement as the pod gate released it, which
// the scheduler couldn't place, changed by mutate.
func stuckPod(mutate ...func(*corev1.Pod)) *corev1.Pod {
	pod := ownedPlacementPod()
	pod.Spec.SchedulingGates = nil
	pod.Annotations["podsnapshot.gke.io/ps-name"] = placementSnapshot
	addRequiredNodeAffinity(pod, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")})
	pod.Status.Phase = corev1.PodPending
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	for _, m := range mutate {
		m(pod)
	}
	return pod
}

// controlledBy makes kind of apiVersion the pod's controller.
func controlledBy(apiVersion, kind string) func(*corev1.Pod) {
	return func(p *corev1.Pod) {
		controller := true
		p.OwnerReferences = []metav1.OwnerReference{{APIVersion: apiVersion, Kind: kind, Name: "owner", UID: "owner-uid", Controller: &controller}}
	}
}

// otherTypeNode returns a Ready gVisor node of another machine type than the
// source node's: the stuck pod can't run on it with the constraint, but
// could without it.
func otherTypeNode() *corev1.Node {
	return placementNode("e2-a", "e2-medium")
}

type timeoutRun struct {
	result    ctrl.Result
	err       error
	job       *pmv1alpha1.PodMigrationJob
	jobExists bool
	pod       *corev1.Pod
	podExists bool
	events    []string
	deletes   []*client.DeleteOptions
}

type timeoutSetup struct {
	funcs     interceptor.Funcs
	apiReader client.Reader
	deadline  time.Duration
	// nodes are the cluster's nodes; nil means otherTypeNode only.
	nodes []client.Object
	// beforeDelete runs before a delete reaches the client.
	beforeDelete func(ctx context.Context, c client.WithWatch, obj client.Object)
	// engines serve the job; nil means GKE Pod Snapshots.
	engines *engine.Registry
	// deleteErr, if set, fails every delete.
	deleteErr error
	// podDeleteErr, if set, fails every pod delete.
	podDeleteErr error
}

// reconcileTimeout reconciles the placement job once with the job
// reconciler, with job and pod in the client.
func reconcileTimeout(t *testing.T, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, setup timeoutSetup) timeoutRun {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	run := timeoutRun{}
	nodes := setup.nodes
	if nodes == nil {
		nodes = []client.Object{otherTypeNode()}
	}
	funcs := setup.funcs
	funcs.Delete = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		deleteOpts := &client.DeleteOptions{}
		deleteOpts.ApplyOptions(opts)
		run.deletes = append(run.deletes, deleteOpts)
		if setup.beforeDelete != nil {
			setup.beforeDelete(ctx, c, obj)
		}
		if setup.deleteErr != nil {
			return setup.deleteErr
		}
		if _, ok := obj.(*corev1.Pod); ok && setup.podDeleteErr != nil {
			return setup.podDeleteErr
		}
		return c.Delete(ctx, obj, opts...)
	}
	c := newFakeClientBuilderWithEventIndex(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(append([]client.Object{pod, job}, nodes...)...).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(funcs).
		Build()
	recorder := record.NewFakeRecorder(20)
	r := &PodMigrationJobReconciler{
		Client:                     c,
		APIReader:                  setup.apiReader,
		Scheme:                     scheme,
		Recorder:                   recorder,
		RestoreStabilizationWindow: DefaultRestoreStabilizationWindow,
		RestorePlacementDeadline:   setup.deadline,
		Engines:                    setup.engines,
	}
	ctx := context.Background()
	run.result, run.err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}})
	run.job = &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementJobName}, run.job); err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting the job: %v", err)
	} else {
		run.jobExists = err == nil
	}
	run.pod = &corev1.Pod{}
	err := c.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, run.pod)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting the pod: %v", err)
	}
	run.podExists = err == nil
	for len(recorder.Events) > 0 {
		run.events = append(run.events, <-recorder.Events)
	}
	return run
}

// liveClient returns a client that serves objs, standing in for the API
// server behind a stale cache.
func liveClient(objs ...client.Object) client.Client {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	return newFakeClientBuilderWithEventIndex(scheme).WithObjects(objs...).Build()
}

// failingReader fails every read, like an API server that can't be reached.
type failingReader struct{ client.Reader }

func (failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("connection refused")
}

// countingReader counts the pod reads that reach Reader.
type countingReader struct {
	client.Reader
	gets int
}

func (c *countingReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		c.gets++
	}
	return c.Reader.Get(ctx, key, obj, opts...)
}

// writeBeforeJobStatusPatch returns interceptors that, on the job's first
// status patch, first write change to the job: a write by another
// reconciler that the patching reconcile didn't see. A patch that checks
// the resourceVersion then conflicts.
func writeBeforeJobStatusPatch(t *testing.T, change func(*pmv1alpha1.PodMigrationJob)) interceptor.Funcs {
	written := false
	return interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
		if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && subResource == "status" && !written {
			written = true
			current := &pmv1alpha1.PodMigrationJob{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), current); err != nil {
				t.Fatalf("getting the job: %v", err)
			}
			change(current)
			if err := c.Status().Update(ctx, current); err != nil {
				t.Fatalf("writing the job: %v", err)
			}
		}
		return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
	}}
}

// gkePlacementSignal is the start of a PlacementTimeout message for a GKE
// Pod Snapshots replacement that a node of another machine type could take
// (#294).
const gkePlacementSignal = "engine=gke-pod-snapshots requirements=node.kubernetes.io/instance-type: "

// signalled checks that the job's Restored and Ready conditions and the
// PlacementTimeout Warning events, on the job and on the pod, start with
// signal (#294), and that they still carry the human text.
func signalled(t *testing.T, run timeoutRun, signal string) {
	t.Helper()
	for _, condType := range []string{"Restored", "Ready"} {
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, condType); cond == nil || !strings.HasPrefix(cond.Message, signal) ||
			!strings.Contains(cond.Message, "released with the restore's node constraints") {
			t.Errorf("%s condition = %+v, want its message to start with %q", condType, cond, signal)
		}
	}
	n := 0
	for _, e := range run.events {
		if strings.HasPrefix(e, "Warning "+ReasonPlacementTimeout+" "+signal) {
			n++
		}
	}
	if n != 2 {
		t.Errorf("events = %q, want 2 %s Warnings (job and pod) starting with %q", run.events, ReasonPlacementTimeout, signal)
	}
}

func TestPodMigrationJobReconciler_PlacementTimeout(t *testing.T) {
	past := DefaultRestorePlacementDeadline + 30*time.Second

	concluded := func(t *testing.T, run timeoutRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || run.job.Status.CompletionTime == nil {
			t.Fatalf("phase = %s, completion %v; want SucceededWithoutRestore", run.job.Status.Phase, run.job.Status.CompletionTime)
		}
		for _, condType := range []string{"Restored", "Ready"} {
			if cond := meta.FindStatusCondition(run.job.Status.Conditions, condType); cond == nil || cond.Reason != ReasonPlacementTimeout {
				t.Errorf("%s condition = %+v, want reason %s", condType, cond, ReasonPlacementTimeout)
			}
		}
		if run.podExists {
			t.Error("replacement still exists, want it deleted for its owner to recreate")
		}
		if !hasEventWith(run.events, "Warning", ReasonPlacementTimeout) {
			t.Errorf("events = %q, want %s Warnings", run.events, ReasonPlacementTimeout)
		}
	}
	untouched := func(t *testing.T, run timeoutRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase = %s, want Restoring", run.job.Status.Phase)
		}
		if !run.podExists || len(run.deletes) != 0 {
			t.Errorf("pod exists %t, deletes %d; want the pod kept", run.podExists, len(run.deletes))
		}
	}

	t.Run("an owned replacement unscheduled past the deadline: concludes and deletes it", func(t *testing.T) {
		verdicts := verdictCount(engine.GKEPodSnapshotsName, "placement_timeout")
		outcomes := outcomeCount("succeeded_without_restore")
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{})
		concluded(t, run)
		signalled(t, run, gkePlacementSignal)
		if got := verdictCount(engine.GKEPodSnapshotsName, "placement_timeout") - verdicts; got != 1 {
			t.Errorf("placement_timeout verdicts recorded = %v, want 1", got)
		}
		if got := outcomeCount("succeeded_without_restore") - outcomes; got != 1 {
			t.Errorf("succeeded_without_restore outcomes recorded = %v, want 1", got)
		}
		// The UID and resourceVersion preconditions keep the delete off a
		// successor, and off a pod that was scheduled after the read.
		if len(run.deletes) != 1 || run.deletes[0].Preconditions == nil ||
			run.deletes[0].Preconditions.UID == nil || *run.deletes[0].Preconditions.UID != placementPodUID ||
			run.deletes[0].Preconditions.ResourceVersion == nil || *run.deletes[0].Preconditions.ResourceVersion == "" {
			t.Errorf("deletes = %+v, want one with UID and resourceVersion preconditions", run.deletes)
		}
		// The RestorePlacement record stays, next to the conclusion.
		if !restorePlacementRecorded(run.job) {
			t.Errorf("conditions = %+v, want the RestorePlacement record kept", run.job.Status.Conditions)
		}
	})

	t.Run("a ReplicaSet's replacement: concludes and deletes it", func(t *testing.T) {
		concluded(t, reconcileTimeout(t, releasedJob(past), stuckPod(controlledBy("apps/v1", "ReplicaSet")), timeoutSetup{}))
	})

	t.Run("before the deadline: waits", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(time.Minute), stuckPod(), timeoutSetup{}))
	})

	t.Run("the deadline is configurable", func(t *testing.T) {
		concluded(t, reconcileTimeout(t, releasedJob(45*time.Second), stuckPod(), timeoutSetup{deadline: 30 * time.Second}))
	})

	t.Run("released late and first seen past the restore ceiling: concludes before the ceiling would", func(t *testing.T) {
		// The release came 5m after the job started restoring, so the
		// deadline hasn't passed, but the ceiling has: the ceiling would
		// conclude RestoreTimeout and leave the pod stuck with the
		// constraint.
		job := releasedJob(time.Minute)
		started := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		job.Status.RestoringStartTime = &started
		run := reconcileTimeout(t, job, stuckPod(), timeoutSetup{})
		concluded(t, run)
		if !hasEventWith(run.events, "before the restore ceiling") {
			t.Errorf("events = %q, want them to say the pod wasn't scheduled before the restore ceiling", run.events)
		}
	})

	t.Run("only nodes that meet the constraint: deleting couldn't help, so it waits", func(t *testing.T) {
		// A pool of one machine type that is full until the cluster
		// autoscaler adds a node: the recreated pod would wait for the same
		// node, and lose the restore.
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{
			nodes: []client.Object{placementNode("n2-a", "n2-standard-4"), placementNode("n2-b", "n2-standard-4")},
		}))
	})

	t.Run("past the restore ceiling, with no node outside the constraint: recreated anyway", func(t *testing.T) {
		// The job gives up on the restore at the ceiling. No node of the
		// type may ever come, for example after a spot reclaim, and without
		// the constraint the cluster autoscaler can add any node for the
		// recreated pod, such as one in a fallback pool that scales from
		// zero.
		job := releasedJob(4 * time.Minute)
		started := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		job.Status.RestoringStartTime = &started
		run := reconcileTimeout(t, job, stuckPod(), timeoutSetup{
			nodes: []client.Object{placementNode("n2-cordoned", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true })},
		})
		concluded(t, run)
		if !hasEventWith(run.events, "before the restore ceiling") {
			t.Errorf("events = %q, want them to say the pod wasn't scheduled before the restore ceiling", run.events)
		}
		// No node would take the pod without the constraint either, so the
		// message names the one constraint that the pod gate added.
		signalled(t, run, gkePlacementSignal)
	})

	t.Run("between 5m and the restore ceiling with the stabilization window: not past it yet", func(t *testing.T) {
		job := releasedJob(time.Minute)
		started := metav1.NewTime(time.Now().Add(-5*time.Minute - 15*time.Second))
		job.Status.RestoringStartTime = &started
		untouched(t, reconcileTimeout(t, job, stuckPod(), timeoutSetup{
			nodes: []client.Object{placementNode("n2-cordoned", "n2-standard-4", func(n *corev1.Node) { n.Spec.Unschedulable = true })},
		}))
	})

	t.Run("a live read that fails is retried, not taken as a pod that isn't stuck", func(t *testing.T) {
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{apiReader: failingReader{}})
		if run.err == nil {
			t.Error("Reconcile() error = nil, want the live read error")
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !run.podExists || len(run.deletes) != 0 {
			t.Errorf("phase = %s, pod exists %t, deletes %d; want nothing changed", run.job.Status.Phase, run.podExists, len(run.deletes))
		}
	})

	t.Run("lists only the nodes that could take the pod without the constraint", func(t *testing.T) {
		// In a pool of one machine type, that's no node at all.
		var selector string
		funcs := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				listOpts := &client.ListOptions{}
				listOpts.ApplyOptions(opts)
				if listOpts.LabelSelector != nil {
					selector = listOpts.LabelSelector.String()
				}
			}
			return c.List(ctx, list, opts...)
		}}
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{
			funcs: funcs,
			nodes: []client.Object{placementNode("n2-a", "n2-standard-4")},
		}))
		if want := "node.kubernetes.io/instance-type notin (n2-standard-4),sandbox.gke.io/runtime=gvisor"; selector != want {
			t.Errorf("node list label selector = %q, want %q", selector, want)
		}
	})

	t.Run("constraints that a label selector can't narrow by: a node that meets them still isn't a way out", func(t *testing.T) {
		// Two requirements: every node is listed, and the loop must skip
		// the ones that meet them.
		zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
		reqs := []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4"), zoneA}
		eng := &placementEngine{orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}}, reqs: reqs}
		pod := stuckPod(func(p *corev1.Pod) {
			p.Spec.Affinity = nil
			addRequiredNodeAffinity(p, reqs)
		})
		inside := placementNode("n2-a", "n2-standard-4", func(n *corev1.Node) { n.Labels[corev1.LabelTopologyZone] = "a" })
		untouched(t, reconcileTimeout(t, releasedJob(past), pod, timeoutSetup{engines: registryOf(t, eng), nodes: []client.Object{inside}}))
		outside := placementNode("e2-b", "e2-medium", func(n *corev1.Node) { n.Labels[corev1.LabelTopologyZone] = "b" })
		concluded(t, reconcileTimeout(t, releasedJob(past), pod, timeoutSetup{engines: registryOf(t, eng), nodes: []client.Object{inside, outside}}))
	})

	t.Run("a requirement the pod's own template has is kept when estimating the recreated pod", func(t *testing.T) {
		// The CRIU engine requires kubernetes.io/arch In [amd64]. A pod
		// whose template requires the same must keep it when the deadline
		// check strips the engine's constraints, or an idle arm64 node
		// counts as a way out and the pod is deleted at the deadline
		// instead of waiting for the ceiling.
		amd64 := corev1.NodeSelectorRequirement{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}}
		vendor := corev1.NodeSelectorRequirement{Key: "criu.pod-migrate.io/cpu-vendor", Operator: corev1.NodeSelectorOpIn, Values: []string{"GenuineIntel", "unknown"}}
		reqs := []corev1.NodeSelectorRequirement{amd64, vendor}
		eng := &placementEngine{orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}}, reqs: reqs}
		pod := stuckPod(func(p *corev1.Pod) {
			p.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{amd64}}},
			}}}
			addRequiredNodeAffinity(p, reqs)
		})
		arch := func(a string) func(*corev1.Node) {
			return func(n *corev1.Node) { n.Labels[corev1.LabelArchStable] = a }
		}
		idleARM := placementNode("arm-a", "t2a-standard-4", arch("arm64"))
		fullIntel := placementNode("n2-a", "n2-standard-4", arch("amd64"), func(n *corev1.Node) { n.Labels[vendor.Key] = "GenuineIntel" })
		untouched(t, reconcileTimeout(t, releasedJob(past), pod, timeoutSetup{engines: registryOf(t, eng), nodes: []client.Object{idleARM, fullIntel}}))
		// An amd64 node of another vendor is a way out: the template
		// allows it, only the engine's vendor requirement doesn't.
		amd := placementNode("c2d-a", "c2d-standard-4", arch("amd64"), func(n *corev1.Node) { n.Labels[vendor.Key] = "AuthenticAMD" })
		concluded(t, reconcileTimeout(t, releasedJob(past), pod, timeoutSetup{engines: registryOf(t, eng), nodes: []client.Object{idleARM, fullIntel, amd}}))
	})

	t.Run("a node list that fails is retried, not taken as no node", func(t *testing.T) {
		failing := interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.NodeList); ok {
				return errors.New("cache not synced")
			}
			return c.List(ctx, list, opts...)
		}}
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{funcs: failing})
		if run.err == nil {
			t.Error("Reconcile() error = nil, want the node list error")
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !run.podExists || len(run.deletes) != 0 {
			t.Errorf("phase = %s, pod exists %t, deletes %d; want nothing changed", run.job.Status.Phase, run.podExists, len(run.deletes))
		}
	})

	t.Run("a node of another type the pod couldn't run on anyway doesn't count", func(t *testing.T) {
		for name, node := range map[string]*corev1.Node{
			"not a gVisor node": placementNode("e2-a", "e2-medium", func(n *corev1.Node) {
				delete(n.Labels, gvisorLabel)
				n.Spec.Taints = nil
			}),
			"cordoned":  placementNode("e2-a", "e2-medium", func(n *corev1.Node) { n.Spec.Unschedulable = true }),
			"not Ready": placementNode("e2-a", "e2-medium", func(n *corev1.Node) { n.Status.Conditions[0].Status = corev1.ConditionFalse }),
		} {
			t.Run(name, func(t *testing.T) {
				untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{nodes: []client.Object{node}}))
			})
		}
	})

	t.Run("the engine states no constraint for the job: waits", func(t *testing.T) {
		job := releasedJob(past)
		job.Status.OriginNodeInstanceType = ""
		untouched(t, reconcileTimeout(t, job, stuckPod(), timeoutSetup{}))
	})

	t.Run("a scheduled replacement isn't touched", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) {
			p.Spec.NodeName = "n2-a"
			p.Status.Conditions[0].Status = corev1.ConditionTrue
			p.Status.Conditions[0].Reason = ""
		}), timeoutSetup{}))
	})

	t.Run("a replacement the scheduler hasn't tried yet isn't touched", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) { p.Status.Conditions = nil }), timeoutSetup{}))
	})

	t.Run("a replacement that another scheduling gate holds isn't touched", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) {
			p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/queue"}}
			p.Status.Conditions[0].Reason = corev1.PodReasonSchedulingGated
		}), timeoutSetup{}))
	})

	t.Run("a replacement the scheduler nominated a node for, to preempt there, isn't touched", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" }), timeoutSetup{}))
	})

	t.Run("a replacement without an owner is left to the restore ceiling", func(t *testing.T) {
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) { p.OwnerReferences = nil }), timeoutSetup{}))
	})

	t.Run("a Job's replacement isn't deleted", func(t *testing.T) {
		// The Job controller would count the deleted pod as failed toward
		// the Job's backoffLimit.
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(controlledBy("batch/v1", "Job")), timeoutSetup{}))
	})

	t.Run("a release without constraints isn't backstopped", func(t *testing.T) {
		job := releasedJob(past)
		job.Status.Conditions = nil
		untouched(t, reconcileTimeout(t, job, stuckPod(), timeoutSetup{}))
	})

	t.Run("the cache shows it unscheduled, but live it isn't: not touched", func(t *testing.T) {
		for name, live := range map[string]*corev1.Pod{
			"bound":                        stuckPod(func(p *corev1.Pod) { p.Spec.NodeName = "n2-a" }),
			"a successor of the same name": stuckPod(func(p *corev1.Pod) { p.UID = "successor-uid" }),
			"orphaned":                     stuckPod(func(p *corev1.Pod) { p.OwnerReferences = nil }),
			"being deleted": stuckPod(func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				p.Finalizers = []string{"example.com/hold"}
			}),
			"nominated a node": stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" }),
		} {
			t.Run(name, func(t *testing.T) {
				untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{apiReader: liveClient(live)}))
			})
		}
	})

	t.Run("a conclusion that conflicts deletes nothing", func(t *testing.T) {
		conflict := interceptor.Funcs{SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && subResource == "status" {
				return apierrors.NewConflict(pmv1alpha1.GroupVersion.WithResource("podmigrationjobs").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
			}
			return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
		}}
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{funcs: conflict}))
	})

	t.Run("a stale view of the job can't conclude it", func(t *testing.T) {
		// The job changed after this reconcile read it. The conclusion
		// checks the resourceVersion, so it conflicts, and nothing is
		// deleted.
		changed := writeBeforeJobStatusPatch(t, func(j *pmv1alpha1.PodMigrationJob) { j.Status.SnapshotRef = "snapshot-2" })
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{funcs: changed}))
	})

	t.Run("a pod scheduled between the read and the delete is kept, and the job says so", func(t *testing.T) {
		bind := func(ctx context.Context, c client.WithWatch, obj client.Object) {
			pod := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			pod.Spec.NodeName = "n2-a"
			if err := c.Update(ctx, pod); err != nil {
				t.Fatalf("binding the pod: %v", err)
			}
		}
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{beforeDelete: bind})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if !run.podExists || run.pod.Spec.NodeName != "n2-a" {
			t.Fatalf("pod exists %t on node %q, want the scheduled pod kept", run.podExists, run.pod.Spec.NodeName)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Errorf("phase = %s, want SucceededWithoutRestore", run.job.Status.Phase)
		}
		if !hasEventWith(run.events, "Warning "+ReasonPlacementTimeout+" "+gkePlacementSignal+"Replacement pod replacement was scheduled to node n2-a after all") {
			t.Errorf("events = %q, want a Warning that the pod was scheduled after all and kept", run.events)
		}
	})

	t.Run("a pod being deleted isn't read live", func(t *testing.T) {
		// A finalizer can hold a deleted pod for a while; it isn't checked
		// on the API server on every poll.
		live := &countingReader{Reader: liveClient(stuckPod())}
		untouched(t, reconcileTimeout(t, releasedJob(past), stuckPod(func(p *corev1.Pod) {
			now := metav1.Now()
			p.DeletionTimestamp = &now
			p.Finalizers = []string{"example.com/hold"}
		}), timeoutSetup{apiReader: live}))
		if live.gets != 0 {
			t.Errorf("live reads = %d, want none", live.gets)
		}
	})

	t.Run("a pod that changed but isn't scheduled when the delete conflicts: looked at again soon", func(t *testing.T) {
		touch := func(ctx context.Context, c client.WithWatch, obj client.Object) {
			pod := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			pod.Labels = map[string]string{"touched": "true"}
			if err := c.Update(ctx, pod); err != nil {
				t.Fatalf("updating the pod: %v", err)
			}
		}
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{beforeDelete: touch})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v, want the conflict handled by a requeue", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || !run.podExists {
			t.Errorf("phase = %s, pod exists %t; want the job concluded and the pod left for the retry", run.job.Status.Phase, run.podExists)
		}
		if run.result.RequeueAfter != placementRecheckInterval {
			t.Errorf("result = %+v, want a requeue after %s for the retry on a fresh read", run.result, placementRecheckInterval)
		}
	})

	t.Run("a pod nominated a node between the read and the delete: kept for now, and the job says so", func(t *testing.T) {
		// The scheduler is preempting pods on a node that meets the
		// constraint to make room for it, so it may still restore. A
		// nominated node can fall through, so the job looks again soon.
		nominate := func(ctx context.Context, c client.WithWatch, obj client.Object) {
			pod := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			pod.Status.NominatedNodeName = "n2-a"
			if err := c.Status().Update(ctx, pod); err != nil {
				t.Fatalf("nominating a node for the pod: %v", err)
			}
		}
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{beforeDelete: nominate})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if !run.podExists || run.pod.Status.NominatedNodeName != "n2-a" {
			t.Fatalf("pod exists %t, nominated %q; want the nominated pod kept", run.podExists, run.pod.Status.NominatedNodeName)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Errorf("phase = %s, want SucceededWithoutRestore", run.job.Status.Phase)
		}
		if run.result.RequeueAfter != placementRecheckInterval {
			t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
		}
		if !hasEventWith(run.events, "Warning "+ReasonPlacementTimeout+" "+gkePlacementSignal+"Replacement pod replacement was nominated to node n2-a") {
			t.Errorf("events = %q, want a Warning that the nominated pod is kept for now", run.events)
		}
	})

	t.Run("a delete that fails at the deadline is retried soon, without failing the reconcile", func(t *testing.T) {
		run := reconcileTimeout(t, releasedJob(past), stuckPod(), timeoutSetup{podDeleteErr: apierrors.NewInternalError(errors.New("etcd timeout"))})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v, want it logged and retried", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || !run.podExists {
			t.Errorf("phase = %s, pod exists %t; want the job concluded and the pod left for the retry", run.job.Status.Phase, run.podExists)
		}
		if run.result.RequeueAfter != placementRecheckInterval {
			t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
		}
	})

	t.Run("a delete that failed is retried once the job has concluded", func(t *testing.T) {
		// The conclusion is written before the delete; if the controller
		// stopped in between, the next reconcile deletes the pod.
		job := releasedJob(past)
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: "Restored", Status: metav1.ConditionFalse, Reason: ReasonPlacementTimeout, Message: "timed out"})
		run := reconcileTimeout(t, job, stuckPod(), timeoutSetup{})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.podExists {
			t.Error("replacement still exists, want it deleted")
		}
	})

	// timedOutJob returns the placement job as concluded PlacementTimeout,
	// completedAgo, changed by mutate.
	timedOutJob := func(completedAgo time.Duration, mutate ...func(*pmv1alpha1.PodMigrationJob)) *pmv1alpha1.PodMigrationJob {
		job := releasedJob(past)
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		completed := metav1.NewTime(time.Now().Add(-completedAgo))
		job.Status.CompletionTime = &completed
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: "Restored", Status: metav1.ConditionFalse, Reason: ReasonPlacementTimeout, Message: "timed out"})
		for _, m := range mutate {
			m(job)
		}
		return job
	}

	t.Run("the retry leaves a pod that was scheduled, orphaned or replaced since, and doesn't look again", func(t *testing.T) {
		// A scheduled pod is kept quietly: a Warning that it's kept was
		// recorded when the delete found it bound, and every later
		// reconcile until garbage collection would repeat it.
		for name, pod := range map[string]*corev1.Pod{
			"scheduled":   stuckPod(func(p *corev1.Pod) { p.Spec.NodeName = "n2-a" }),
			"orphaned":    stuckPod(func(p *corev1.Pod) { p.OwnerReferences = nil }),
			"a successor": stuckPod(func(p *corev1.Pod) { p.UID = "successor-uid" }),
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, timedOutJob(time.Minute), pod, timeoutSetup{})
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if !run.podExists || len(run.deletes) != 0 {
					t.Errorf("pod exists %t, deletes %d; want the pod kept", run.podExists, len(run.deletes))
				}
				if run.result.RequeueAfter <= placementRecheckInterval {
					t.Errorf("result = %+v, want the usual requeue for garbage collection", run.result)
				}
				if len(run.events) != 0 {
					t.Errorf("events = %q, want none", run.events)
				}
			})
		}
	})

	t.Run("the retry leaves a pod that the scheduler hasn't found unschedulable, and looks again soon", func(t *testing.T) {
		// A nominated node is where the scheduler is preempting pods to
		// make room, and it can fall through. Another scheduling gate can
		// go away. Either way the pod may become unschedulable again, and
		// then the retry deletes it.
		for name, pod := range map[string]*corev1.Pod{
			"nominated a node": stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" }),
			"held by another scheduling gate": stuckPod(func(p *corev1.Pod) {
				p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example.com/queue"}}
				p.Status.Conditions[0].Reason = corev1.PodReasonSchedulingGated
			}),
			"not tried": stuckPod(func(p *corev1.Pod) { p.Status.Conditions = nil }),
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, timedOutJob(time.Minute), pod, timeoutSetup{})
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if !run.podExists || len(run.deletes) != 0 {
					t.Errorf("pod exists %t, deletes %d; want the pod kept", run.podExists, len(run.deletes))
				}
				if run.result.RequeueAfter != placementRecheckInterval {
					t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
				}
			})
		}
	})

	t.Run("a retry whose live read fails looks again soon, without failing the reconcile", func(t *testing.T) {
		run := reconcileTimeout(t, timedOutJob(time.Minute), stuckPod(), timeoutSetup{apiReader: failingReader{}})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v, want it logged and retried", run.err)
		}
		if !run.podExists || len(run.deletes) != 0 {
			t.Errorf("pod exists %t, deletes %d; want the pod kept for the next look", run.podExists, len(run.deletes))
		}
		if run.result.RequeueAfter != placementRecheckInterval {
			t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
		}
	})

	t.Run("a retry that keeps failing doesn't hold the source fence or garbage collection", func(t *testing.T) {
		// For example, an admission policy forbids the delete. The job
		// keeps retrying until it's garbage collected, 30 minutes after it
		// concluded, but its other cleanup goes ahead.
		forbidden := apierrors.NewForbidden(corev1.Resource("pods"), placementPodName, errors.New("denied by an admission policy"))
		withFinalizer := func(j *pmv1alpha1.PodMigrationJob) { j.Finalizers = []string{SourceFenceFinalizer} }
		t.Run("before garbage collection", func(t *testing.T) {
			run := reconcileTimeout(t, timedOutJob(time.Minute, withFinalizer), stuckPod(), timeoutSetup{podDeleteErr: forbidden})
			if run.err != nil {
				t.Fatalf("Reconcile() error = %v, want it logged and retried", run.err)
			}
			if !run.podExists || len(run.deletes) != 1 {
				t.Errorf("pod exists %t, deletes %d; want one failed delete", run.podExists, len(run.deletes))
			}
			if run.result.RequeueAfter != placementRecheckInterval {
				t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
			}
			if len(run.job.Finalizers) != 0 {
				t.Errorf("finalizers = %v, want the source fence finalizer removed", run.job.Finalizers)
			}
		})
		t.Run("past garbage collection", func(t *testing.T) {
			run := reconcileTimeout(t, timedOutJob(31*time.Minute, withFinalizer), stuckPod(), timeoutSetup{podDeleteErr: forbidden})
			if run.err != nil {
				t.Fatalf("Reconcile() error = %v", run.err)
			}
			if run.jobExists {
				t.Error("job still exists, want it garbage collected 30 minutes after it concluded")
			}
		})
	})
}

// TestPodMigrationJobReconciler_RestoreCeilingStaleView pins that the
// restore ceiling's writes check the resourceVersion. On a stale view of a
// job that the pod gate has since consumed and released with the
// constraint, a merge patch would replace the conditions list and drop the
// RestorePlacement record, and the replacement would then stay stuck with
// the constraint: nothing would recreate it.
func TestPodMigrationJobReconciler_RestoreCeilingStaleView(t *testing.T) {
	staleJob := func() *pmv1alpha1.PodMigrationJob {
		job := placementJob()
		started := metav1.NewTime(time.Now().Add(-6 * time.Minute))
		job.Status.RestoringStartTime = &started
		return job
	}
	// released is the pod gate's write that the stale view missed.
	released := func(j *pmv1alpha1.PodMigrationJob) {
		j.Status.Consumed = true
		j.Status.RestoredPodUID = placementPodUID
		j.Status.RestoredPodName = placementPodName
		j.Status.GateReleased = true
		recordRestorePlacement(j, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}, true)
	}
	kept := func(t *testing.T, run timeoutRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !restorePlacementRecorded(run.job) {
			t.Errorf("phase = %s, conditions %+v; want Restoring with the RestorePlacement record", run.job.Status.Phase, run.job.Status.Conditions)
		}
	}

	t.Run("RestoreTimeout", func(t *testing.T) {
		// The pod informer already shows the pod released, so there's no
		// gated claimant, and the ceiling concludes.
		kept(t, reconcileTimeout(t, staleJob(), stuckPod(), timeoutSetup{funcs: writeBeforeJobStatusPatch(t, released)}))
	})

	t.Run("WaitingOnGateRelease", func(t *testing.T) {
		kept(t, reconcileTimeout(t, staleJob(), ownedPlacementPod(), timeoutSetup{funcs: writeBeforeJobStatusPatch(t, released)}))
	})
}

// TestPodMigrationJobReconciler_RestoreTimeoutRecheck pins that a job that
// concluded RestoreTimeout keeps looking at a replacement released with the
// restore's node constraints that isn't scheduled: the scheduler hadn't tried
// it yet at the restore ceiling, or a node it nominated fell through. Nothing
// else looks at the pod once the job has concluded.
// failFirstPodGet returns interceptors that fail the first read of the
// replacement pod, like a cache that isn't synced yet.
func failFirstPodGet() interceptor.Funcs {
	failed := false
	return interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if _, ok := obj.(*corev1.Pod); ok && key.Name == placementPodName && !failed {
			failed = true
			return errors.New("cache not synced")
		}
		return c.Get(ctx, key, obj, opts...)
	}}
}

func TestPodMigrationJobReconciler_RestoreTimeoutRecheck(t *testing.T) {
	timedOut := func(mutate ...func(*pmv1alpha1.PodMigrationJob)) *pmv1alpha1.PodMigrationJob {
		job := releasedJob(DefaultRestorePlacementDeadline + 3*time.Minute)
		completed := metav1.NewTime(time.Now().Add(-time.Minute))
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
		job.Status.CompletionTime = &completed
		meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{Type: "Restored", Status: metav1.ConditionFalse, Reason: reasonRestoreTimeout, Message: "timed out"})
		for _, m := range mutate {
			m(job)
		}
		return job
	}

	t.Run("unschedulable now: deleted, and the job keeps its conclusion", func(t *testing.T) {
		run := reconcileTimeout(t, timedOut(), stuckPod(), timeoutSetup{})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.podExists {
			t.Error("replacement still exists, want it deleted for its owner to recreate")
		}
		if len(run.deletes) != 1 || run.deletes[0].Preconditions == nil || run.deletes[0].Preconditions.ResourceVersion == nil {
			t.Errorf("deletes = %+v, want one with preconditions", run.deletes)
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored"); cond == nil || cond.Reason != reasonRestoreTimeout {
			t.Errorf("Restored condition = %+v, want reason %s kept", cond, reasonRestoreTimeout)
		}
		if !hasEventWith(run.events, "Warning", ReasonPlacementTimeout, "after the job concluded RestoreTimeout") {
			t.Errorf("events = %q, want a %s Warning", run.events, ReasonPlacementTimeout)
		}
		// The events name the engine and the constraint (#294), on the job
		// and on the pod.
		n := 0
		for _, e := range run.events {
			if strings.HasPrefix(e, "Warning "+ReasonPlacementTimeout+" "+gkePlacementSignal) {
				n++
			}
		}
		if n != 2 {
			t.Errorf("events = %q, want 2 starting with %q", run.events, gkePlacementSignal)
		}
	})

	t.Run("the cache shows it unschedulable, but live it isn't: kept", func(t *testing.T) {
		for name, live := range map[string]*corev1.Pod{
			"bound":                        stuckPod(func(p *corev1.Pod) { p.Spec.NodeName = "n2-a" }),
			"a successor of the same name": stuckPod(func(p *corev1.Pod) { p.UID = "successor-uid" }),
			"orphaned":                     stuckPod(func(p *corev1.Pod) { p.OwnerReferences = nil }),
			"being deleted": stuckPod(func(p *corev1.Pod) {
				now := metav1.Now()
				p.DeletionTimestamp = &now
				p.Finalizers = []string{"example.com/hold"}
			}),
			"nominated a node": stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" }),
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, timedOut(), stuckPod(), timeoutSetup{apiReader: liveClient(live)})
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if !run.podExists || len(run.deletes) != 0 {
					t.Errorf("pod exists %t, deletes %d; want it kept", run.podExists, len(run.deletes))
				}
				// The cache will catch up: look again soon.
				if run.result.RequeueAfter != placementRecheckInterval {
					t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
				}
			})
		}
	})

	t.Run("released cold after the job concluded, without the constraints: left alone", func(t *testing.T) {
		// The pod gate releases a pod whose job has concluded with the
		// cold-start bypass and no constraints, even if an earlier attempt
		// recorded a release with them.
		run := reconcileTimeout(t, timedOut(), stuckPod(func(p *corev1.Pod) {
			p.Spec.Affinity = nil
			p.Annotations["podsnapshot.gke.io/ps-name"] = ""
		}), timeoutSetup{})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if !run.podExists || len(run.deletes) != 0 {
			t.Errorf("pod exists %t, deletes %d; want it kept", run.podExists, len(run.deletes))
		}
		if run.result.RequeueAfter <= placementRecheckInterval {
			t.Errorf("result = %+v, want the usual requeue for garbage collection", run.result)
		}
	})

	t.Run("a check that fails is retried soon, and doesn't fail the reconcile", func(t *testing.T) {
		conflict := apierrors.NewConflict(corev1.Resource("pods"), placementPodName, errors.New("the object has been modified"))
		touch := func(ctx context.Context, c client.WithWatch, obj client.Object) {
			pod := &corev1.Pod{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), pod); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			pod.Labels = map[string]string{"touched": "true"}
			if err := c.Update(ctx, pod); err != nil {
				t.Fatalf("updating the pod: %v", err)
			}
		}
		for name, setup := range map[string]timeoutSetup{
			"a live read that fails":       {apiReader: failingReader{}},
			"a cache read that fails":      {funcs: failFirstPodGet()},
			"a delete that fails":          {deleteErr: apierrors.NewInternalError(errors.New("etcd timeout"))},
			"a delete that conflicts":      {deleteErr: conflict},
			"a pod that changed meanwhile": {beforeDelete: touch},
			"an engine that can't state the constraints": {engines: registryOf(t, &placementEngine{
				orderingEngine: orderingEngine{routedEngine{name: "placing", provider: &fakeSnapshotProvider{}}},
				err:            errors.New("backend unavailable"),
			})},
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, timedOut(), stuckPod(), setup)
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v, want it logged and retried", run.err)
				}
				if !run.podExists {
					t.Error("replacement was deleted, want it kept for the next look")
				}
				if run.result.RequeueAfter != placementRecheckInterval {
					t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
				}
			})
		}
	})

	t.Run("a check that keeps failing doesn't hold garbage collection", func(t *testing.T) {
		job := timedOut(func(j *pmv1alpha1.PodMigrationJob) {
			completed := metav1.NewTime(time.Now().Add(-31 * time.Minute))
			j.Status.CompletionTime = &completed
		})
		run := reconcileTimeout(t, job, stuckPod(), timeoutSetup{apiReader: failingReader{}})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.jobExists {
			t.Error("job still exists, want it garbage collected 30 minutes after it concluded")
		}
	})

	t.Run("not tried yet, or nominated a node: looked at again soon", func(t *testing.T) {
		for name, pod := range map[string]*corev1.Pod{
			"not tried yet":    stuckPod(func(p *corev1.Pod) { p.Status.Conditions = nil }),
			"nominated a node": stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" }),
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, timedOut(), pod, timeoutSetup{})
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if !run.podExists || len(run.deletes) != 0 {
					t.Errorf("pod exists %t, deletes %d; want it kept", run.podExists, len(run.deletes))
				}
				if run.result.RequeueAfter != placementRecheckInterval {
					t.Errorf("result = %+v, want a requeue after %s", run.result, placementRecheckInterval)
				}
			})
		}
	})

	t.Run("scheduled, a Job's, or without the record: left alone", func(t *testing.T) {
		for name, tc := range map[string]struct {
			job *pmv1alpha1.PodMigrationJob
			pod *corev1.Pod
		}{
			"scheduled": {job: timedOut(), pod: stuckPod(func(p *corev1.Pod) { p.Spec.NodeName = "n2-a" })},
			"a Job's":   {job: timedOut(), pod: stuckPod(controlledBy("batch/v1", "Job"))},
			"without the record": {job: timedOut(func(j *pmv1alpha1.PodMigrationJob) {
				meta.RemoveStatusCondition(&j.Status.Conditions, ConditionRestorePlacement)
			}), pod: stuckPod()},
		} {
			t.Run(name, func(t *testing.T) {
				run := reconcileTimeout(t, tc.job, tc.pod, timeoutSetup{})
				if run.err != nil {
					t.Fatalf("Reconcile() error = %v", run.err)
				}
				if !run.podExists || len(run.deletes) != 0 {
					t.Errorf("pod exists %t, deletes %d; want it kept", run.podExists, len(run.deletes))
				}
				if run.result.RequeueAfter <= placementRecheckInterval {
					t.Errorf("result = %+v, want the usual requeue for garbage collection", run.result)
				}
			})
		}
	})
}

func TestUnschedulable(t *testing.T) {
	cases := map[string]struct {
		pod  *corev1.Pod
		want bool
	}{
		"tried and no node":           {pod: stuckPod(), want: true},
		"bound":                       {pod: stuckPod(func(p *corev1.Pod) { p.Spec.NodeName = "n2-a" })},
		"nominated a node":            {pod: stuckPod(func(p *corev1.Pod) { p.Status.NominatedNodeName = "n2-a" })},
		"held by another gate":        {pod: stuckPod(func(p *corev1.Pod) { p.Status.Conditions[0].Reason = corev1.PodReasonSchedulingGated })},
		"not tried yet":               {pod: stuckPod(func(p *corev1.Pod) { p.Status.Conditions = nil })},
		"scheduled per its condition": {pod: stuckPod(func(p *corev1.Pod) { p.Status.Conditions[0].Status = corev1.ConditionTrue })},
	}
	for name, tc := range cases {
		if got := unschedulable(tc.pod); got != tc.want {
			t.Errorf("%s: unschedulable() = %t, want %t", name, got, tc.want)
		}
	}
}

func TestWayOutSelector(t *testing.T) {
	gvisor := map[string]string{gvisorLabel: "gvisor"}
	zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	for _, tc := range []struct {
		name         string
		nodeSelector map[string]string
		reqs         []corev1.NodeSelectorRequirement
		want         string
	}{
		{name: "GKE Pod Snapshots' constraint", nodeSelector: gvisor, reqs: []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")},
			want: "node.kubernetes.io/instance-type notin (n2-standard-4),sandbox.gke.io/runtime=gvisor"},
		{name: "NotIn", reqs: []corev1.NodeSelectorRequirement{{Key: "k", Operator: corev1.NodeSelectorOpNotIn, Values: []string{"v"}}}, want: "k in (v)"},
		{name: "Exists", reqs: []corev1.NodeSelectorRequirement{{Key: "k", Operator: corev1.NodeSelectorOpExists}}, want: "!k"},
		{name: "DoesNotExist", reqs: []corev1.NodeSelectorRequirement{{Key: "k", Operator: corev1.NodeSelectorOpDoesNotExist}}, want: "k"},
		{name: "Gt can't be negated", nodeSelector: gvisor, reqs: []corev1.NodeSelectorRequirement{{Key: "k", Operator: corev1.NodeSelectorOpGt, Values: []string{"1"}}},
			want: "sandbox.gke.io/runtime=gvisor"},
		{name: "two requirements can't be negated", nodeSelector: gvisor, reqs: []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4"), zoneA},
			want: "sandbox.gke.io/runtime=gvisor"},
	} {
		got, ok := wayOutSelector(&corev1.Pod{Spec: corev1.PodSpec{NodeSelector: tc.nodeSelector}}, tc.reqs)
		if !ok || got.String() != tc.want {
			t.Errorf("%s: wayOutSelector() = %v, %t; want %q", tc.name, got, ok, tc.want)
		}
	}
	if got, ok := wayOutSelector(&corev1.Pod{}, []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4"), zoneA}); ok {
		t.Errorf("wayOutSelector() = %v, true; want no narrowing for two requirements and no node selector", got)
	}
}

func TestValidateRestorePlacementDeadline(t *testing.T) {
	cases := []struct {
		deadline, stabilization time.Duration
		ok                      bool
	}{
		{deadline: DefaultRestorePlacementDeadline, stabilization: DefaultRestoreStabilizationWindow, ok: true},
		{deadline: DefaultRestorePlacementDeadline, stabilization: 0, ok: true},
		{deadline: 4*time.Minute + 30*time.Second, stabilization: 30 * time.Second, ok: true},
		{deadline: 4*time.Minute + 31*time.Second, stabilization: 30 * time.Second},
		{deadline: 4 * time.Minute, ok: true},
		{deadline: 4*time.Minute + time.Second},
		{deadline: 6 * time.Minute, stabilization: MaxRestoreStabilizationWindow, ok: true},
		{deadline: 6*time.Minute + time.Second, stabilization: MaxRestoreStabilizationWindow},
		{deadline: 0, stabilization: 30 * time.Second},
		{deadline: -time.Second, stabilization: 30 * time.Second},
	}
	for _, tc := range cases {
		err := ValidateRestorePlacementDeadline(tc.deadline, tc.stabilization)
		if (err == nil) != tc.ok {
			t.Errorf("ValidateRestorePlacementDeadline(%v, %v) = %v, want ok %t", tc.deadline, tc.stabilization, err, tc.ok)
		}
	}
}
