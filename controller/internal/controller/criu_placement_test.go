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

// The CRIU engine's restore placement (#257) through the pod gate and the
// job reconciler: the same release, deadline and cold-start fallback as GKE
// Pod Snapshots' (#14), with the CRIU compatibility rule's constraints.

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// criuSourceFacts are the source node facts the CRIU node agent recorded in
// the placement job's snapshot.
var criuSourceFacts = criuplacement.Facts{
	Arch:              "amd64",
	CPUVendor:         "GenuineIntel",
	CPUModel:          "Intel(R) Xeon(R) CPU @ 2.20GHz",
	CPUFeatures:       []string{"avx", "avx2", "fpu", "hypervisor", "sse", "sse2"},
	KernelVersion:     "6.6.72+",
	CRIUVersion:       "4.1",
	RuncVersion:       "1.2.5",
	ContainerdVersion: "2.0.4",
	PlacementLabels:   criuplacement.PlacementLabelsVersion,
}

// criuPlacementRegistry returns a registry whose default engine is CRIU,
// reading the placement job's snapshot, with status.sourceNode set to
// sourceNode (none if nil), from its own client.
func criuPlacementRegistry(t *testing.T, sourceNode map[string]interface{}) *engine.Registry {
	t.Helper()
	scheme := runtime.NewScheme()
	ps := &unstructured.Unstructured{}
	ps.SetGroupVersionKind(snapshot.PodSnapshotGVK(engine.CRIUAPIGroup))
	ps.SetNamespace(placementNamespace)
	ps.SetName(placementSnapshot)
	if sourceNode != nil {
		ps.Object["status"] = map[string]interface{}{"sourceNode": sourceNode}
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(ps).Build()
	criu, err := engine.NewCRIU(cl, scheme, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatal(err)
	}
	return registryOf(t, criu)
}

func sourceNodeOf(f criuplacement.Facts) map[string]interface{} {
	features := make([]interface{}, len(f.CPUFeatures))
	for i, s := range f.CPUFeatures {
		features[i] = s
	}
	return map[string]interface{}{
		"arch": f.Arch, "cpuVendor": f.CPUVendor, "cpuModel": f.CPUModel, "cpuFeatures": features,
		"kernelVersion": f.KernelVersion, "criuVersion": f.CRIUVersion, "runcVersion": f.RuncVersion,
		"containerdVersion": f.ContainerdVersion, "placementLabels": f.PlacementLabels,
	}
}

// criuNodeLabels are the labels the CRIU node agent publishes for a node
// with the facts f (engines/criu/internal/psengine/contract), restricted to
// the flags these tests use.
// sourceModelLabel is the cpu-model label value of the source node.
func sourceModelLabel() string {
	for _, req := range criuplacement.Requirements(criuSourceFacts) {
		if req.Key == criuplacement.LabelCPUModel {
			return req.Values[0]
		}
	}
	return ""
}

func criuNodeLabels(f criuplacement.Facts) map[string]string {
	code := func(v string) string {
		parts := strings.SplitN(strings.TrimRight(v, "+"), ".", 3)
		n := int64(0)
		for i, scale := range []int64{1_000_000, 1_000, 1} {
			if i < len(parts) {
				p, _ := strconv.ParseInt(parts[i], 10, 64)
				n += p * scale
			}
		}
		return strconv.FormatInt(n, 10)
	}
	labels := map[string]string{
		corev1.LabelArchStable:               f.Arch,
		criuplacement.LabelCPUVendor:         f.CPUVendor,
		criuplacement.LabelCPUModel:          sourceModelLabel(), // same model
		criuplacement.LabelXState:            "avx",
		criuplacement.LabelKernelVersion:     code(f.KernelVersion),
		criuplacement.LabelCRIUVersion:       code(f.CRIUVersion),
		criuplacement.LabelRuncVersion:       code(f.RuncVersion),
		criuplacement.LabelContainerdVersion: code(f.ContainerdVersion),
	}
	for _, flag := range []string{"avx", "avx2", "fpu", "sse", "sse2"} {
		has := false
		for _, have := range f.CPUFeatures {
			has = has || have == flag
		}
		if !has {
			labels[criuplacement.MissingCPUFlagLabelPrefix+flag] = "true"
		}
	}
	return labels
}

// criuNode returns a Ready node named name whose CRIU agent published the
// labels for facts f.
func criuNode(name string, f criuplacement.Facts) *corev1.Node {
	return placementNode(name, "n2-standard-4", func(n *corev1.Node) {
		for k, v := range criuNodeLabels(f) {
			n.Labels[k] = v
		}
	})
}

func olderKernel() criuplacement.Facts {
	f := criuSourceFacts
	f.KernelVersion = "6.1.100+"
	return f
}

func withoutAVX2() criuplacement.Facts {
	f := criuSourceFacts
	f.CPUFeatures = []string{"avx", "fpu", "sse", "sse2"}
	return f
}

func newerEverything() criuplacement.Facts {
	f := criuSourceFacts
	f.KernelVersion, f.CRIUVersion, f.RuncVersion, f.ContainerdVersion = "6.12.8+", "4.2", "1.3.0", "2.1.1"
	f.CPUFeatures = append([]string{"gfni", "sha_ni"}, f.CPUFeatures...)
	return f
}

func TestPodGateReconciler_CRIURestorePlacement(t *testing.T) {
	wantReqs := criuplacement.Requirements(criuSourceFacts)
	if len(wantReqs) < 8 {
		t.Fatalf("Requirements = %v, want vendor, model, xstate, four versions and the flags", wantReqs)
	}
	restoreAnnotation := engine.RestoreAnnotation(engine.CRIUAPIGroup)

	released := func(t *testing.T, run placementRun) {
		t.Helper()
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if podHasMigrationGate(run.pod) {
			t.Fatal("pod is still gated, want it released")
		}
		if got := run.pod.Annotations[restoreAnnotation]; got != placementSnapshot {
			t.Errorf("%s = %q, want %q", restoreAnnotation, got, placementSnapshot)
		}
	}

	t.Run("a compatible node exists: released with the CRIU constraints", func(t *testing.T) {
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)), ownedPlacementPod(), placementJob(),
			criuNode("old-kernel", olderKernel()), criuNode("newer", newerEverything()))
		released(t, run)
		want := []corev1.NodeSelectorTerm{{MatchExpressions: wantReqs}}
		if got := requiredTerms(run.pod); !reflect.DeepEqual(got, want) {
			t.Errorf("terms = %+v\nwant %+v", got, want)
		}
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if cond == nil || cond.Reason != ReasonNodeConstraintsAdded || strings.Contains(cond.Message, "no node met it") {
			t.Errorf("RestorePlacement = %+v, want constraints added with a node that met them", cond)
		}
	})

	t.Run("no compatible node: released with the constraints to wait for the deadline", func(t *testing.T) {
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)), ownedPlacementPod(), placementJob(),
			criuNode("old-kernel", olderKernel()), criuNode("no-avx2", withoutAVX2()))
		released(t, run)
		if got := requiredTerms(run.pod); len(got) != 1 || !reflect.DeepEqual(got[0].MatchExpressions, wantReqs) {
			t.Errorf("terms = %+v, want the CRIU constraints", got)
		}
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if cond == nil || cond.Reason != ReasonNodeConstraintsAdded || !strings.Contains(cond.Message, "no node met it") {
			t.Errorf("RestorePlacement = %+v, want constraints added with no node meeting them", cond)
		}
	})

	t.Run("a node with unknown facts counts as compatible", func(t *testing.T) {
		unknown := placementNode("unknown", "n2-standard-4", func(n *corev1.Node) {
			n.Labels[corev1.LabelArchStable] = "amd64"
			for _, key := range []string{criuplacement.LabelCPUVendor, criuplacement.LabelCPUModel, criuplacement.LabelXState} {
				n.Labels[key] = criuplacement.LabelValueUnknown
			}
			for _, key := range []string{criuplacement.LabelKernelVersion, criuplacement.LabelCRIUVersion, criuplacement.LabelRuncVersion, criuplacement.LabelContainerdVersion} {
				n.Labels[key] = "1000000000"
			}
		})
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)), ownedPlacementPod(), placementJob(), unknown)
		released(t, run)
		cond := meta.FindStatusCondition(run.job.Status.Conditions, ConditionRestorePlacement)
		if cond == nil || strings.Contains(cond.Message, "no node met it") {
			t.Errorf("RestorePlacement = %+v, want the unknown node to meet the constraints", cond)
		}
	})

	t.Run("a snapshot from an agent before the label format: no constraints", func(t *testing.T) {
		old := criuSourceFacts
		old.PlacementLabels = ""
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, sourceNodeOf(old)), ownedPlacementPod(), placementJob(), criuNode("old-kernel", olderKernel()))
		released(t, run)
		if got := requiredTerms(run.pod); got != nil {
			t.Errorf("terms = %+v, want none", got)
		}
	})

	t.Run("a snapshot without source facts: no constraints", func(t *testing.T) {
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, nil), ownedPlacementPod(), placementJob(), criuNode("old-kernel", olderKernel()))
		released(t, run)
		if got := requiredTerms(run.pod); got != nil {
			t.Errorf("terms = %+v, want none", got)
		}
	})

	t.Run("placement off: no constraints", func(t *testing.T) {
		run := reconcilePlacement(t, false, criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)), ownedPlacementPod(), placementJob(), criuNode("old-kernel", olderKernel()))
		released(t, run)
		if got := requiredTerms(run.pod); got != nil {
			t.Errorf("terms = %+v, want none", got)
		}
	})

	t.Run("a Job's replacement: no constraints", func(t *testing.T) {
		pod := placementPodWith(controlledBy("batch/v1", "Job"))
		run := reconcilePlacement(t, true, criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)), pod, placementJob(), criuNode("old-kernel", olderKernel()))
		released(t, run)
		if got := requiredTerms(run.pod); got != nil {
			t.Errorf("terms = %+v, want none", got)
		}
	})
}

// criuStuckPod is the owned replacement released with the CRIU constraints,
// which the scheduler couldn't place.
func criuStuckPod() *corev1.Pod {
	pod := ownedPlacementPod()
	pod.Spec.SchedulingGates = nil
	pod.Annotations[engine.RestoreAnnotation(engine.CRIUAPIGroup)] = placementSnapshot
	addRequiredNodeAffinity(pod, criuplacement.Requirements(criuSourceFacts))
	pod.Status.Phase = corev1.PodPending
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	return pod
}

func TestPodMigrationJobReconciler_CRIUPlacementTimeout(t *testing.T) {
	past := DefaultRestorePlacementDeadline + 30*time.Second

	t.Run("an incompatible node could take it: concludes and deletes it to start cold", func(t *testing.T) {
		run := reconcileTimeout(t, releasedJob(past), criuStuckPod(), timeoutSetup{
			engines: criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)),
			nodes:   []client.Object{criuNode("old-kernel", olderKernel())},
		})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Fatalf("phase = %s, want SucceededWithoutRestore", run.job.Status.Phase)
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored"); cond == nil || cond.Reason != ReasonPlacementTimeout {
			t.Errorf("Restored = %+v, want %s", cond, ReasonPlacementTimeout)
		}
		if run.podExists {
			t.Error("replacement still exists, want it deleted for its owner to recreate cold")
		}
	})

	t.Run("only compatible nodes, all full: deleting couldn't help, so it waits", func(t *testing.T) {
		run := reconcileTimeout(t, releasedJob(past), criuStuckPod(), timeoutSetup{
			engines: criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)),
			nodes:   []client.Object{criuNode("newer", newerEverything())},
		})
		if run.err != nil {
			t.Fatalf("Reconcile() error = %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !run.podExists {
			t.Errorf("phase %s, pod exists %t; want it left to wait", run.job.Status.Phase, run.podExists)
		}
	})

	t.Run("before the deadline: waits", func(t *testing.T) {
		run := reconcileTimeout(t, releasedJob(time.Minute), criuStuckPod(), timeoutSetup{
			engines: criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts)),
			nodes:   []client.Object{criuNode("old-kernel", olderKernel())},
		})
		if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || !run.podExists {
			t.Errorf("err %v, phase %s, pod exists %t; want it left to wait", run.err, run.job.Status.Phase, run.podExists)
		}
	})
}
