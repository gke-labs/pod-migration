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

package criuplacement

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
)

// intel returns facts of a Cascade Lake-like Intel node, changed by mutate.
func intel(mutate ...func(*Facts)) Facts {
	f := Facts{
		Arch:      "amd64",
		CPUVendor: "GenuineIntel",
		CPUModel:  "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures: strings.Fields("fpu tsc cx8 sep cmov clflush mmx fxsr sse sse2 syscall rdtscp rep_good nopl pni " +
			"pclmulqdq ssse3 fma cx16 sse4_1 sse4_2 movbe popcnt aes xsave avx f16c rdrand abm 3dnowprefetch fsgsbase " +
			"bmi1 hle avx2 bmi2 erms rtm mpx avx512f avx512dq rdseed adx clflushopt avx512cd avx512bw avx512vl xsaveopt " +
			"xsavec xgetbv1 xsaves avx512_vnni pku ospke hypervisor md_clear ibrs ssbd arch_capabilities constant_tsc"),
		KernelVersion:     "6.6.72+",
		CRIUVersion:       "4.1",
		RuncVersion:       "1.2.5",
		ContainerdVersion: "2.0.4",
		PlacementLabels:   PlacementLabelsVersion,
	}
	f.CPUFeatures = slices.Clone(f.CPUFeatures)
	for _, m := range mutate {
		m(&f)
	}
	return f
}

func without(flags ...string) func(*Facts) {
	return func(f *Facts) {
		f.CPUFeatures = slices.DeleteFunc(f.CPUFeatures, func(s string) bool { return slices.Contains(flags, s) })
	}
}

func with(flags ...string) func(*Facts) {
	return func(f *Facts) { f.CPUFeatures = append(f.CPUFeatures, flags...) }
}

func TestCompatible(t *testing.T) {
	for _, tc := range []struct {
		name   string
		source Facts
		target Facts
		want   bool
		reason string
	}{
		{"identical", intel(), intel(), true, ""},
		{"target is a superset", intel(), intel(with("gfni", "vaes", "sha_ni")), true, ""},
		{"target lacks an instruction flag", intel(), intel(without("avx512_vnni")), false, "avx512_vnni"},
		{"target lacks TSX", intel(), intel(without("hle", "rtm")), false, "hle rtm"},
		{"source flag the target lacks isn't compared: mitigation", intel(), intel(without("md_clear", "ibrs", "ssbd", "arch_capabilities")), true, ""},
		{"source flag the target lacks isn't compared: virtualization", intel(), intel(without("hypervisor", "constant_tsc")), true, ""},
		{"target lacks fma, an extension CRIU's set predates", intel(), intel(without("fma")), false, "fma"},
		{"architecture mismatch", Facts{Arch: "arm64"}, Facts{Arch: "amd64"}, false, "architecture"},
		{"unknown target architecture is compatible", intel(), intel(func(f *Facts) { f.Arch = "" }), true, ""},
		{"target has more XSAVE state", intel(), intel(with("amx_tile")), false, "XSAVE"},
		{"target has less XSAVE state", intel(with("amx_tile")), intel(), false, "XSAVE"},
		{"vendor mismatch", intel(), intel(func(f *Facts) { f.CPUVendor = "AuthenticAMD" }), false, "vendor"},
		{"model mismatch", intel(), intel(func(f *Facts) { f.CPUModel = "Intel(R) Xeon(R) Platinum 8481C CPU @ 2.70GHz" }), false, "model"},
		{"older kernel refused", intel(), intel(func(f *Facts) { f.KernelVersion = "6.1.100+" }), false, "kernel"},
		{"older CRIU refused", intel(), intel(func(f *Facts) { f.CRIUVersion = "3.19" }), false, "CRIU"},
		{"older runc refused", intel(), intel(func(f *Facts) { f.RuncVersion = "1.1.12" }), false, "runc"},
		{"older containerd refused", intel(), intel(func(f *Facts) { f.ContainerdVersion = "1.7.24" }), false, "containerd"},
		{"newer versions allowed", intel(), intel(func(f *Facts) {
			f.KernelVersion, f.CRIUVersion, f.RuncVersion, f.ContainerdVersion = "6.12.1", "4.2", "1.3.0", "v2.1.0"
		}), true, ""},
		{"same version, other suffix", intel(), intel(func(f *Facts) { f.KernelVersion = "6.6.72-gke.1" }), true, ""},
		{"unknown target facts are compatible", intel(), Facts{}, true, ""},
		{"unknown source facts are compatible", Facts{}, intel(func(f *Facts) { f.CPUVendor = "AuthenticAMD"; f.KernelVersion = "1.0" }), true, ""},
		{"unknown target features are compatible", intel(), intel(func(f *Facts) { f.CPUFeatures = nil }), true, ""},
		{"unparseable target version is compatible", intel(), intel(func(f *Facts) { f.KernelVersion = "weird" }), true, ""},
		{"non-x86 features aren't compared", Facts{CPUFeatures: []string{"fp", "asimd", "aes"}}, Facts{CPUFeatures: []string{"fp"}}, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Compatible(tc.source, tc.target)
			if got.Compatible != tc.want {
				t.Fatalf("Compatible = %+v, want %t", got, tc.want)
			}
			for _, word := range strings.Fields(tc.reason) {
				if !strings.Contains(strings.Join(got.Reasons, "; "), word) {
					t.Errorf("reasons %q don't mention %q", got.Reasons, word)
				}
			}
		})
	}
}

func TestUnknown(t *testing.T) {
	if got := Unknown(intel()); len(got) != 0 {
		t.Errorf("Unknown(all known) = %v", got)
	}
	got := Unknown(Facts{CPUFeatures: []string{"fp"}, KernelVersion: "6.6", RuncVersion: "garbage"})
	want := []string{"architecture", "CPU vendor", "CPU model", "x86 CPU features", "CRIU version", "runc version", "containerd version"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Unknown = %v, want %v", got, want)
	}
}

func TestRequirements(t *testing.T) {
	t.Run("an agent that doesn't publish the label format: none", func(t *testing.T) {
		if got := Requirements(intel(func(f *Facts) { f.PlacementLabels = "" })); got != nil {
			t.Errorf("Requirements = %v, want none", got)
		}
		if got := Requirements(intel(func(f *Facts) { f.PlacementLabels = "v2" })); got != nil {
			t.Errorf("Requirements(v2) = %v, want none", got)
		}
	})
	t.Run("every fact unknown: none", func(t *testing.T) {
		if got := Requirements(Facts{PlacementLabels: PlacementLabelsVersion}); len(got) != 0 {
			t.Errorf("Requirements = %v, want none", got)
		}
	})
	t.Run("deterministic, valid, and ordered fixed facts first", func(t *testing.T) {
		a, b := Requirements(intel()), Requirements(intel())
		if !reflect.DeepEqual(a, b) {
			t.Fatal("Requirements isn't deterministic")
		}
		wantFirst := []corev1.NodeSelectorRequirement{
			{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{"amd64"}},
			{Key: LabelCPUVendor, Operator: corev1.NodeSelectorOpIn, Values: []string{"GenuineIntel", LabelValueUnknown}},
			{Key: LabelCPUModel, Operator: corev1.NodeSelectorOpIn, Values: []string{cpuModelHash("Intel(R) Xeon(R) CPU @ 2.20GHz"), LabelValueUnknown}},
			{Key: LabelXState, Operator: corev1.NodeSelectorOpIn, Values: []string{"avx.avx512f.ospke", LabelValueUnknown}},
			{Key: LabelKernelVersion, Operator: corev1.NodeSelectorOpGt, Values: []string{"6006071"}},
			{Key: LabelCRIUVersion, Operator: corev1.NodeSelectorOpGt, Values: []string{"4000999"}},
			{Key: LabelRuncVersion, Operator: corev1.NodeSelectorOpGt, Values: []string{"1002004"}},
			{Key: LabelContainerdVersion, Operator: corev1.NodeSelectorOpGt, Values: []string{"2000003"}},
		}
		if !reflect.DeepEqual(a[:len(wantFirst)], wantFirst) {
			t.Errorf("first requirements = %v\nwant %v", a[:len(wantFirst)], wantFirst)
		}
		for _, req := range a[len(wantFirst):] {
			flag, ok := strings.CutPrefix(req.Key, MissingCPUFlagLabelPrefix)
			if !ok || req.Operator != corev1.NodeSelectorOpDoesNotExist || !slices.Contains(InstructionFlags, flag) {
				t.Errorf("unexpected requirement %v", req)
			}
			if flag == "md_clear" || flag == "hypervisor" {
				t.Errorf("filtered flag %s is required", flag)
			}
		}
		for _, req := range a {
			if errs := validation.IsQualifiedName(req.Key); len(errs) > 0 {
				t.Errorf("key %q: %v", req.Key, errs)
			}
			for _, v := range req.Values {
				if errs := validation.IsValidLabelValue(v); len(errs) > 0 {
					t.Errorf("value %q: %v", v, errs)
				}
			}
		}
	})
	t.Run("version 0.0.0 needs no requirement", func(t *testing.T) {
		for _, req := range Requirements(Facts{PlacementLabels: PlacementLabelsVersion, RuncVersion: "0.0.0"}) {
			t.Errorf("unexpected requirement %v", req)
		}
	})
	t.Run("an invalid vendor label value is unknown", func(t *testing.T) {
		for _, req := range Requirements(Facts{PlacementLabels: PlacementLabelsVersion, CPUVendor: "  Shanghai  "}) {
			t.Errorf("unexpected requirement %v", req)
		}
	})
}

// goldenCase is an entry of the CRIU node agent's golden file: a node's
// facts, and the labels the agent publishes for them.
type goldenCase struct {
	Name   string            `json:"name"`
	Facts  Facts             `json:"facts"`
	Labels map[string]string `json:"labels"`
}

const goldenFile = "../../../engines/criu/internal/psengine/contract/testdata/node_placement_labels.json"

func readGolden(t *testing.T) []goldenCase {
	t.Helper()
	b, err := os.ReadFile(filepath.FromSlash(goldenFile))
	if err != nil {
		t.Fatalf("reading the agent's golden label file: %v", err)
	}
	var cases []goldenCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	if len(cases) < 10 {
		t.Fatalf("golden file has %d cases, want more", len(cases))
	}
	return cases
}

// withKubeletArch returns a copy of c's labels with the kubernetes.io/arch
// label that the kubelet, not the agent, sets on every node.
func withKubeletArch(c goldenCase) map[string]string {
	labels := map[string]string{corev1.LabelArchStable: c.Facts.Arch}
	for k, v := range c.Labels {
		labels[k] = v
	}
	return labels
}

// TestRequirementsMatchTheRuleOnAgentLabels checks, for every pair of nodes
// in the agent's golden file, that a node whose labels the agent published
// meets the requirements for a checkpoint from the other exactly when the
// rule says it's compatible. That ties the requirements, the rule and the
// agent's label format together.
func TestRequirementsMatchTheRuleOnAgentLabels(t *testing.T) {
	cases := readGolden(t)
	compatible, incompatible := 0, 0
	for _, source := range cases {
		reqs := Requirements(source.Facts)
		pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}},
			},
		}}}}
		affinity := nodeaffinity.GetRequiredNodeAffinity(pod)
		for _, target := range cases {
			node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: target.Name, Labels: withKubeletArch(target)}}
			matches, err := affinity.Match(node)
			if err != nil {
				t.Fatalf("%s -> %s: %v", source.Name, target.Name, err)
			}
			verdict := Compatible(source.Facts, target.Facts)
			if len(reqs) == 0 {
				// No requirements: every node matches, but the rule may
				// still say no for facts that labels can't carry. Only an
				// all-unknown source is in the golden file this way.
				continue
			}
			if matches != verdict.Compatible {
				t.Errorf("%s -> %s: labels match %t, rule says %+v", source.Name, target.Name, matches, verdict)
			}
			if verdict.Compatible {
				compatible++
			} else {
				incompatible++
			}
		}
	}
	if compatible < 10 || incompatible < 10 {
		t.Errorf("only %d compatible and %d incompatible pairs: the golden file should exercise both", compatible, incompatible)
	}
}

// TestRequirementsOnAgentLabelsOneFactOff takes the agent's labels for the
// golden "intel" node and changes one of them at a time: the requirements
// for a checkpoint from that node must refuse each change, and accept the
// unknown value.
func TestRequirementsOnAgentLabelsOneFactOff(t *testing.T) {
	var base goldenCase
	for _, c := range readGolden(t) {
		if c.Name == "intel" {
			base = c
		}
	}
	if base.Labels == nil {
		t.Fatal("no intel case in the golden file")
	}
	reqs := Requirements(base.Facts)
	pod := &corev1.Pod{Spec: corev1.PodSpec{Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}}},
	}}}}
	affinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	match := func(change func(map[string]string)) bool {
		labels := withKubeletArch(base)
		change(labels)
		ok, err := affinity.Match(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Labels: labels}})
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !match(func(map[string]string) {}) {
		t.Fatal("the source node itself doesn't meet its requirements")
	}
	for _, key := range []string{LabelCPUVendor, LabelCPUModel, LabelXState} {
		if match(func(l map[string]string) { l[key] = "other" }) {
			t.Errorf("%s=other meets the requirements", key)
		}
		if !match(func(l map[string]string) { l[key] = LabelValueUnknown }) {
			t.Errorf("%s=unknown doesn't meet the requirements", key)
		}
	}
	for _, key := range []string{LabelKernelVersion, LabelCRIUVersion, LabelRuncVersion, LabelContainerdVersion} {
		if match(func(l map[string]string) { l[key] = "1000" }) {
			t.Errorf("an older %s meets the requirements", key)
		}
		if !match(func(l map[string]string) { l[key] = "1000000000" }) {
			t.Errorf("an unknown %s doesn't meet the requirements", key)
		}
	}
	if match(func(l map[string]string) { l[corev1.LabelArchStable] = "arm64" }) {
		t.Error("an arm64 node meets the requirements of an amd64 checkpoint")
	}
	if match(func(l map[string]string) { l[MissingCPUFlagLabelPrefix+"avx2"] = "true" }) {
		t.Error("a node lacking avx2 meets the requirements")
	}
	if !match(func(l map[string]string) { l[MissingCPUFlagLabelPrefix+"sha_ni"] = "true" }) {
		t.Error("a node lacking a flag the source didn't have doesn't meet the requirements")
	}
}

// TestFlagListsMatchTheAgent compares the flag lists with the agent's copy,
// since the modules can't share code.
func TestFlagListsMatchTheAgent(t *testing.T) {
	b, err := os.ReadFile(filepath.FromSlash("../../../engines/criu/internal/psengine/contract/node_placement.go"))
	if err != nil {
		t.Fatalf("reading the agent's label format: %v", err)
	}
	src := string(b)
	for name, ours := range map[string][]string{"InstructionFlags": InstructionFlags, "XStateFlags": XStateFlags} {
		m := regexp.MustCompile(`(?s)var ` + name + ` = \[\]string\{(.*?)\}`).FindStringSubmatch(src)
		if m == nil {
			t.Fatalf("%s not found in the agent's source", name)
		}
		var theirs []string
		for _, q := range regexp.MustCompile(`"([^"]+)"`).FindAllStringSubmatch(m[1], -1) {
			theirs = append(theirs, q[1])
		}
		if !reflect.DeepEqual(ours, theirs) {
			t.Errorf("%s differs from the agent's:\n ours %v\ntheirs %v", name, ours, theirs)
		}
	}
	for name, ours := range map[string]string{
		"PlacementLabelsVersion": PlacementLabelsVersion, "LabelValueUnknown": LabelValueUnknown,
		"LabelPrefix": labelPrefix, "MissingCPUFlagLabelPrefix": MissingCPUFlagLabelPrefix,
	} {
		if !regexp.MustCompile(name + `\s*=\s*"` + regexp.QuoteMeta(ours) + `"`).MatchString(src) {
			t.Errorf("%s = %q isn't the agent's", name, ours)
		}
	}
}

func TestSourceFacts(t *testing.T) {
	ps := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"sourceNode": map[string]interface{}{
			"cpuVendor": "GenuineIntel", "cpuFeatures": []interface{}{"avx", "fpu"}, "kernelVersion": "6.6.72+", "placementLabels": "v1",
		}},
	}}
	got, ok := SourceFacts(ps)
	want := Facts{CPUVendor: "GenuineIntel", CPUFeatures: []string{"avx", "fpu"}, KernelVersion: "6.6.72+", PlacementLabels: "v1"}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Errorf("SourceFacts = %+v, %t; want %+v", got, ok, want)
	}
	if _, ok := SourceFacts(&unstructured.Unstructured{Object: map[string]interface{}{}}); ok {
		t.Error("no status.sourceNode: want ok false")
	}
	bad := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"sourceNode": "x"}}}
	if _, ok := SourceFacts(bad); ok {
		t.Error("unreadable status.sourceNode: want ok false")
	}
}
