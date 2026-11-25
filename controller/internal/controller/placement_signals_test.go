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
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

// criuRequirementKeys returns the keys of the CRIU constraints for
// criuSourceFacts, in the engine's order.
func criuRequirementKeys() []string {
	var keys []string
	for _, req := range criuplacement.Requirements(criuSourceFacts) {
		keys = append(keys, req.Key)
	}
	return keys
}

// TestFailedPlacementKeys pins which constraint keys a PlacementTimeout
// message names (#294): the constraints the pod gate added that no node
// meets among the nodes that would take the pod without them, checked as
// the deadline checks them; all the added ones when that doesn't single
// any out.
func TestFailedPlacementKeys(t *testing.T) {
	notReady := func(n *corev1.Node) {
		n.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}
	}
	arm64 := func(n *corev1.Node) { n.Labels[corev1.LabelArchStable] = "arm64" }
	kernelKey := []string{criuplacement.LabelKernelVersion}
	allCRIU := criuRequirementKeys()
	instanceType := []string{corev1.LabelInstanceTypeStable}

	for _, tc := range []struct {
		name    string
		engines func(t *testing.T) *engine.Registry
		pod     *corev1.Pod
		nodes   []client.Object
		want    []string
	}{{
		name:  "GKE Pod Snapshots, a node of another machine type: the instance type",
		pod:   stuckPod(),
		nodes: []client.Object{otherTypeNode()},
		want:  instanceType,
	}, {
		name: "GKE Pod Snapshots, no node at all: the added constraint",
		pod:  stuckPod(),
		want: instanceType,
	}, {
		name:  "CRIU, a node with an older kernel: the kernel version",
		pod:   criuStuckPod(),
		nodes: []client.Object{criuNode("old-kernel", olderKernel())},
		want:  kernelKey,
	}, {
		name:  "CRIU, a node without avx2: that flag",
		pod:   criuStuckPod(),
		nodes: []client.Object{criuNode("no-avx2", withoutAVX2())},
		want:  []string{criuplacement.MissingCPUFlagLabelPrefix + "avx2"},
	}, {
		name:  "CRIU, a node with an older kernel and without avx2: both",
		pod:   criuStuckPod(),
		nodes: []client.Object{criuNode("old", func() criuplacement.Facts { f := withoutAVX2(); f.KernelVersion = "6.1.100+"; return f }())},
		want:  []string{criuplacement.LabelKernelVersion, criuplacement.MissingCPUFlagLabelPrefix + "avx2"},
	}, {
		name: "CRIU, each constraint met by some node, only not together: all added",
		pod:  criuStuckPod(),
		nodes: []client.Object{
			criuNode("old-kernel", olderKernel()),
			criuNode("no-avx2", withoutAVX2()),
		},
		want: allCRIU,
	}, {
		name:  "CRIU, the only other node isn't Ready: all added",
		pod:   criuStuckPod(),
		nodes: []client.Object{criuNode("old-kernel", olderKernel(), notReady)},
		want:  allCRIU,
	}, {
		name:  "CRIU, a node that only meets the requirements: all added",
		pod:   criuStuckPod(),
		nodes: []client.Object{criuNode("newer", newerEverything())},
		want:  allCRIU,
	}, {
		// The pod's own template requires amd64, so the gate didn't add the
		// CRIU engine's arch constraint: an arm64 node can't take the pod
		// either way, and the arch isn't named.
		name:  "CRIU, the template's own arch requirement isn't the restore's",
		pod:   templateArchPod(),
		nodes: []client.Object{criuNode("arm", newerEverything(), arm64), criuNode("old-kernel", olderKernel())},
		want:  kernelKey,
	}, {
		// When all the added constraints are named, the template's own
		// arch requirement isn't among them.
		name:  "CRIU, the template's own arch requirement isn't named among all the added ones",
		pod:   templateArchPod(),
		nodes: []client.Object{criuNode("old-kernel", olderKernel()), criuNode("no-avx2", withoutAVX2())},
		want:  allCRIU[1:],
	}} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tc.nodes...).Build()
			r := &PodMigrationJobReconciler{Client: cl, Scheme: scheme}
			if strings.Contains(tc.name, "CRIU") {
				r.Engines = criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts))
			}
			if got := r.failedPlacementKeys(context.Background(), releasedJob(0), tc.pod); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("failedPlacementKeys() = %v\nwant %v", got, tc.want)
			}
		})
	}

	t.Run("the engine states no constraints any more: the ones the pod records", func(t *testing.T) {
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(criuNode("old-kernel", olderKernel())).Build()
		// The snapshot has no source facts, so CRIU states none.
		r := &PodMigrationJobReconciler{Client: cl, Scheme: scheme, Engines: criuPlacementRegistry(t, nil)}
		if got := r.failedPlacementKeys(context.Background(), releasedJob(0), criuStuckPod()); !reflect.DeepEqual(got, allCRIU) {
			t.Errorf("failedPlacementKeys() = %v\nwant %v", got, allCRIU)
		}
	})
}

// templateArchPod is criuStuckPod with a template that already requires
// amd64, so the pod gate didn't add the CRIU engine's arch constraint.
func templateArchPod() *corev1.Pod {
	pod := ownedPlacementPod()
	pod.Spec.SchedulingGates = nil
	pod.Annotations[engine.RestoreAnnotation(engine.CRIUAPIGroup)] = placementSnapshot
	pod.Spec.Affinity = requiredAffinity(termOf(corev1.NodeSelectorRequirement{
		Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"},
	}))
	addRequiredNodeAffinity(pod, criuplacement.Requirements(criuSourceFacts))
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	return pod
}

func TestCompactRequirementKeys(t *testing.T) {
	flags := func(n int) []string {
		var keys []string
		for _, f := range criuplacement.InstructionFlags[:n] {
			keys = append(keys, criuplacement.MissingCPUFlagLabelPrefix+f)
		}
		return keys
	}
	few := append([]string{corev1.LabelArchStable}, flags(maxListedCPUFlagKeys)...)
	if got := compactRequirementKeys(few); !reflect.DeepEqual(got, few) {
		t.Errorf("compactRequirementKeys(%d flags) = %v, want them listed", maxListedCPUFlagKeys, got)
	}
	many := append(append([]string{corev1.LabelArchStable}, flags(maxListedCPUFlagKeys+1)...), criuplacement.LabelKernelVersion)
	want := []string{corev1.LabelArchStable, criuplacement.MissingCPUFlagLabelPrefix + "*", criuplacement.LabelKernelVersion}
	if got := compactRequirementKeys(many); !reflect.DeepEqual(got, want) {
		t.Errorf("compactRequirementKeys(%d flags) = %v, want %v", maxListedCPUFlagKeys+1, got, want)
	}
	dup := []string{corev1.LabelArchStable, corev1.LabelArchStable}
	if got := compactRequirementKeys(dup); !reflect.DeepEqual(got, []string{corev1.LabelArchStable}) {
		t.Errorf("compactRequirementKeys(%v) = %v, want one", dup, got)
	}
}

func TestPlacementSignal(t *testing.T) {
	signal := placementSignal("criu", []string{criuplacement.LabelKernelVersion, criuplacement.MissingCPUFlagLabelPrefix + "avx2"})
	want := "engine=criu requirements=criu.pod-migrate.io/kernel-version-code,missing-cpu.criu.pod-migrate.io/avx2"
	if signal != want {
		t.Fatalf("placementSignal() = %q, want %q", signal, want)
	}
	message := withSignal(signal, "Replacement pod p wasn't scheduled")
	if message != want+": Replacement pod p wasn't scheduled" {
		t.Errorf("withSignal() = %q", message)
	}
	if got, ok := placementSignalOf(message); !ok || got != signal {
		t.Errorf("placementSignalOf(%q) = %q, %t; want %q", message, got, ok, signal)
	}
	if got := placementSignal("gke-pod-snapshots", nil); got != "engine=gke-pod-snapshots requirements=" {
		t.Errorf("placementSignal(no keys) = %q", got)
	}
	if got, ok := placementSignalOf("engine=gke-pod-snapshots requirements=: text"); !ok || got != "engine=gke-pod-snapshots requirements=" {
		t.Errorf("placementSignalOf(no keys) = %q, %t", got, ok)
	}
	for _, old := range []string{"", "Replacement pod p, released with the restore's node constraints", "engine=criu: text"} {
		if got, ok := placementSignalOf(old); ok {
			t.Errorf("placementSignalOf(%q) = %q, want none", old, got)
		}
	}
}
