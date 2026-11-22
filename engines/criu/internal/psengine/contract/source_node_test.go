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

package contract

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestSourceNodeFactsRoundTrip(t *testing.T) {
	ps := New(GVKPodSnapshot)
	if _, ok := GetSourceNodeFacts(ps); ok {
		t.Fatal("a fresh PodSnapshot must report no source node facts")
	}
	want := SourceNodeFacts{
		CPUModel:          "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures:       []string{"avx2", "fpu", "sse4_2"},
		KernelVersion:     "6.6.72+",
		CRIUVersion:       "4.1",
		RuncVersion:       "1.2.5",
		ContainerdVersion: "2.0.4",
	}
	if err := SetSourceNodeFacts(ps, want); err != nil {
		t.Fatal(err)
	}
	// The orchestrator-facing field path, pinned.
	if v, _, _ := unstructured.NestedString(ps.Object, "status", "sourceNode", "kernelVersion"); v != "6.6.72+" {
		t.Fatalf("status.sourceNode.kernelVersion = %q", v)
	}
	got, ok := GetSourceNodeFacts(ps)
	if !ok {
		t.Fatal("facts written but not found")
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip: got %+v, want %+v", got, want)
	}
}

func TestSourceNodeFactsPartialOmitsUnknown(t *testing.T) {
	ps := New(GVKPodSnapshot)
	if err := SetSourceNodeFacts(ps, SourceNodeFacts{KernelVersion: "6.6.72+"}); err != nil {
		t.Fatal(err)
	}
	sn, _ := ps.Object["status"].(map[string]interface{})["sourceNode"].(map[string]interface{})
	if len(sn) != 1 || sn["kernelVersion"] != "6.6.72+" {
		t.Fatalf("unknown facts must be omitted, not written empty: %v", sn)
	}
}

func TestSourceNodeFactsEmptyWritesNothing(t *testing.T) {
	ps := New(GVKPodSnapshot)
	if err := SetSourceNodeFacts(ps, SourceNodeFacts{}); err != nil {
		t.Fatal(err)
	}
	if _, found := ps.Object["status"]; found {
		t.Fatalf("empty facts must not create status.sourceNode: %v", ps.Object)
	}
	if !(SourceNodeFacts{}).IsEmpty() || (SourceNodeFacts{RuncVersion: "1.2.5"}).IsEmpty() {
		t.Fatal("IsEmpty is wrong")
	}
	if (SourceNodeFacts{Arch: "amd64"}).IsEmpty() {
		t.Fatal("IsEmpty ignores Arch, a fact")
	}
	if !(SourceNodeFacts{PlacementLabels: PlacementLabelsVersion}).IsEmpty() {
		t.Fatal("IsEmpty counts PlacementLabels, which isn't a fact")
	}
}

func TestSourceNodeFactsKeepsConditions(t *testing.T) {
	ps := New(GVKPodSnapshot)
	if err := SetCondition(ps, metav1.Condition{Type: CondReady, Status: metav1.ConditionTrue, Reason: ReasonSucceeded}); err != nil {
		t.Fatal(err)
	}
	if err := SetSourceNodeFacts(ps, SourceNodeFacts{CRIUVersion: "4.1"}); err != nil {
		t.Fatal(err)
	}
	if !IsSnapshotReady(ps) {
		t.Fatal("writing facts must not disturb status.conditions")
	}
}
