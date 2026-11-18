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
	"os"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestRestorePlacement_Envtest checks against a real API server that the pod
// gate can add the restore's node constraints in the update that removes the
// gate, and that they can't be added once the gate is gone (#14).
func TestRestorePlacement_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest placement test")
	}
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}
	ctx := context.Background()

	const ns = "placement-test"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	newGated := func(name string, affinity *corev1.Affinity) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec: corev1.PodSpec{
				Containers:      []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}},
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				NodeSelector:    map[string]string{gvisorLabel: "gvisor"},
				Affinity:        affinity,
			},
		}
		if err := c.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}
		return pod
	}
	get := func(name string) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, pod); err != nil {
			t.Fatalf("getting pod: %v", err)
		}
		return pod
	}
	gate := &PodGateReconciler{}
	reqs := []corev1.NodeSelectorRequirement{instanceTypeIn("n2-standard-4")}
	zoneA := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"a"}}
	zoneB := corev1.NodeSelectorRequirement{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{"b"}}

	t.Run("one update orders the restore, adds the constraint and removes the gate", func(t *testing.T) {
		pod := newGated("no-affinity", nil)
		pod.Annotations = map[string]string{"podsnapshot.gke.io/ps-name": "snapshot-1"}
		addRequiredNodeAffinity(pod, reqs)
		gate.removeGate(pod)
		if err := c.Update(ctx, pod); err != nil {
			t.Fatalf("releasing the gate with the constraint: %v", err)
		}
		got := get(pod.Name)
		want := []corev1.NodeSelectorTerm{{MatchExpressions: reqs}}
		if terms := requiredTerms(got); !reflect.DeepEqual(terms, want) || len(got.Spec.SchedulingGates) != 0 {
			t.Errorf("terms %+v, gates %v; want %+v and no gate", terms, got.Spec.SchedulingGates, want)
		}

		// Once the gate is gone, Kubernetes refuses any change to the
		// required node affinity, so constraints can't be added or dropped
		// after release.
		addRequiredNodeAffinity(got, []corev1.NodeSelectorRequirement{zoneA})
		if err := c.Update(ctx, got); !apierrors.IsInvalid(err) {
			t.Errorf("changing the node affinity of a released pod: error %v, want Invalid", err)
		}
	})

	t.Run("existing terms each get the constraint", func(t *testing.T) {
		pod := newGated("with-terms", &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
				{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA}},
				{MatchExpressions: []corev1.NodeSelectorRequirement{zoneB}},
			}},
		}})
		addRequiredNodeAffinity(pod, reqs)
		gate.removeGate(pod)
		if err := c.Update(ctx, pod); err != nil {
			t.Fatalf("releasing the gate with the constraint: %v", err)
		}
		want := []corev1.NodeSelectorTerm{
			{MatchExpressions: []corev1.NodeSelectorRequirement{zoneA, reqs[0]}},
			{MatchExpressions: []corev1.NodeSelectorRequirement{zoneB, reqs[0]}},
		}
		if terms := requiredTerms(get(pod.Name)); !reflect.DeepEqual(terms, want) {
			t.Errorf("terms = %+v, want %+v", terms, want)
		}
	})
}
