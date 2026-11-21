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
	"encoding/json"
	"flag"
	"os"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

var updateGolden = flag.Bool("update", false, "rewrite testdata/node_placement_labels.json from the facts in it")

const goldenPath = "testdata/node_placement_labels.json"

// placementCase is one entry of the golden file, which the controller's
// placement tests read too (controller/internal/criuplacement).
type placementCase struct {
	Name   string            `json:"name"`
	Facts  SourceNodeFacts   `json:"facts"`
	Labels map[string]string `json:"labels"`
}

// TestNodePlacementLabelsGolden pins the label format against the golden
// file. Run with -update after an intended format change, and bump
// PlacementLabelsVersion if old controllers would misread the new labels.
func TestNodePlacementLabelsGolden(t *testing.T) {
	b, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var cases []placementCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatal("golden file has no cases")
	}
	for i := range cases {
		got := NodePlacementLabels(cases[i].Facts)
		if *updateGolden {
			cases[i].Labels = got
			continue
		}
		if !reflect.DeepEqual(got, cases[i].Labels) {
			t.Errorf("%s: labels\n got %v\nwant %v", cases[i].Name, got, cases[i].Labels)
		}
	}
	if *updateGolden {
		out, err := json.MarshalIndent(cases, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNodePlacementLabelsAreValid(t *testing.T) {
	facts := SourceNodeFacts{
		CPUVendor: "GenuineIntel", CPUModel: "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures: []string{"fpu"}, KernelVersion: "6.6.72+",
	}
	for key, value := range NodePlacementLabels(facts) {
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			t.Errorf("label key %q: %v", key, errs)
		}
		if errs := validation.IsValidLabelValue(value); len(errs) > 0 {
			t.Errorf("label %q value %q: %v", key, value, errs)
		}
		if !IsPlacementLabel(key) {
			t.Errorf("label %q isn't recognized as a placement label, so it would never be removed", key)
		}
	}
}

func TestNodePlacementLabelsMissingFlags(t *testing.T) {
	all := slices.Clone(InstructionFlags)
	t.Run("an x86 node with every flag has no missing-flag label", func(t *testing.T) {
		labels := NodePlacementLabels(SourceNodeFacts{CPUVendor: "GenuineIntel", CPUFeatures: all})
		for key := range labels {
			if strings.HasPrefix(key, MissingCPUFlagLabelPrefix) {
				t.Errorf("unexpected %s", key)
			}
		}
	})
	t.Run("an x86 node lacking avx2 says so, and only for instruction flags", func(t *testing.T) {
		features := slices.DeleteFunc(slices.Clone(all), func(f string) bool { return f == "avx2" })
		labels := NodePlacementLabels(SourceNodeFacts{CPUVendor: "AuthenticAMD", CPUFeatures: features})
		var missing []string
		for key := range labels {
			if flag, ok := strings.CutPrefix(key, MissingCPUFlagLabelPrefix); ok {
				missing = append(missing, flag)
			}
		}
		if !reflect.DeepEqual(missing, []string{"avx2"}) {
			t.Errorf("missing = %v, want [avx2]", missing)
		}
	})
	t.Run("unknown features: no missing-flag labels and an unknown xstate", func(t *testing.T) {
		labels := NodePlacementLabels(SourceNodeFacts{CPUVendor: "GenuineIntel"})
		if labels[LabelXState] != LabelValueUnknown {
			t.Errorf("xstate = %q, want unknown", labels[LabelXState])
		}
		for key := range labels {
			if strings.HasPrefix(key, MissingCPUFlagLabelPrefix) {
				t.Errorf("unexpected %s", key)
			}
		}
	})
	t.Run("a non-x86 node: no missing-flag labels and an unknown xstate", func(t *testing.T) {
		labels := NodePlacementLabels(SourceNodeFacts{CPUFeatures: []string{"fp", "asimd", "aes"}})
		if labels[LabelXState] != LabelValueUnknown || labels[LabelCPUVendor] != LabelValueUnknown {
			t.Errorf("xstate %q vendor %q, want unknown", labels[LabelXState], labels[LabelCPUVendor])
		}
		for key := range labels {
			if strings.HasPrefix(key, MissingCPUFlagLabelPrefix) {
				t.Errorf("unexpected %s", key)
			}
		}
	})
}

func TestNodePlacementLabelsUnknownFacts(t *testing.T) {
	labels := NodePlacementLabels(SourceNodeFacts{CPUVendor: "  Shanghai  ", KernelVersion: "garbage"})
	want := map[string]string{
		LabelCPUVendor:         LabelValueUnknown, // not a valid label value
		LabelCPUModel:          LabelValueUnknown,
		LabelXState:            LabelValueUnknown,
		LabelKernelVersion:     VersionCodeUnknown,
		LabelCRIUVersion:       VersionCodeUnknown,
		LabelRuncVersion:       VersionCodeUnknown,
		LabelContainerdVersion: VersionCodeUnknown,
	}
	if !reflect.DeepEqual(labels, want) {
		t.Errorf("labels = %v, want %v", labels, want)
	}
	unknown, _ := strconv.ParseInt(VersionCodeUnknown, 10, 64)
	if max, _ := VersionCode("999.999.999"); unknown <= max {
		t.Errorf("VersionCodeUnknown %d must exceed every real code (max %d)", unknown, max)
	}
}

func TestVersionCode(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{"6.6.72+", 6_006_072, true},
		{"5.15.0-1057-gke", 5_015_000, true},
		{"v2.0.1", 2_000_001, true},
		{"4.1", 4_001_000, true},
		{"3.19", 3_019_000, true},
		{"1.2.0-rc.1", 1_002_000, true},
		{"7", 7_000_000, true},
		{" 1.7.24\n", 1_007_024, true},
		{"", 0, false},
		{"unknown", 0, false},
		{"1.1000.0", 0, false},
	} {
		got, ok := VersionCode(tc.in)
		if got != tc.want || ok != tc.ok {
			t.Errorf("VersionCode(%q) = %d, %t; want %d, %t", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestXStateValue(t *testing.T) {
	if got := XStateValue([]string{"sse", "avx512f", "avx", "ospke"}); got != "avx.avx512f.ospke" {
		t.Errorf("XStateValue = %q", got)
	}
	if got := XStateValue([]string{"sse"}); got != XStateNone {
		t.Errorf("XStateValue = %q, want none", got)
	}
}

func TestInstructionFlagsSortedAndUnique(t *testing.T) {
	if !sort.StringsAreSorted(InstructionFlags) || !sort.StringsAreSorted(XStateFlags) {
		t.Error("flag lists must stay sorted, so the controller's copy is easy to compare")
	}
	if len(slices.Compact(slices.Clone(InstructionFlags))) != len(InstructionFlags) {
		t.Error("InstructionFlags has duplicates")
	}
	// Mitigation and virtualization flags aren't compared.
	for _, f := range []string{"pti", "md_clear", "hypervisor", "ibrs", "ssbd", "constant_tsc", "vmx"} {
		if slices.Contains(InstructionFlags, f) {
			t.Errorf("%s must not be compared", f)
		}
	}
}

func TestNodeFactsAnnotations(t *testing.T) {
	a := NodeFactsAnnotations(SourceNodeFacts{CPUVendor: "GenuineIntel", CPUFeatures: []string{"avx", "fpu"}, KernelVersion: "6.6.72+"})
	if a[AnnotationNodeCPUFeatures] != "avx fpu" {
		t.Errorf("cpu features annotation = %q", a[AnnotationNodeCPUFeatures])
	}
	if a[AnnotationNodeFacts] != `{"cpuVendor":"GenuineIntel","kernelVersion":"6.6.72+"}` {
		t.Errorf("facts annotation = %q", a[AnnotationNodeFacts])
	}
	if _, ok := NodeFactsAnnotations(SourceNodeFacts{})[AnnotationNodeCPUFeatures]; ok {
		t.Error("unknown features must not publish an empty features annotation")
	}
}
