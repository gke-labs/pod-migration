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

// Package criuplacement is the compatibility rule for restoring a CRIU
// checkpoint on another node (#257), and its encoding as node label
// requirements for the CRIU engine's restore placement (C8).
//
// A target node can restore a checkpoint taken on a source node when, for
// every fact known on both sides:
//
//   - the architecture is the same;
//   - the CPU vendor is the same;
//   - the CPU model name is the same: the CRIU node agent refuses a restore
//     across model names (its CPU family guard), so placement does too;
//   - on x86, the target has every instruction set flag of the source that
//     CRIU compares (InstructionFlags), and exactly the source's XSAVE state
//     flags (XStateFlags), since CRIU requires the same XSAVE area size;
//   - the kernel, CRIU, runc and containerd versions are the same or newer.
//
// A fact unknown on either side never excludes a node.
//
// The CRIU node agent publishes each node's facts as labels
// (engines/criu/internal/psengine/contract/node_placement.go), so the rule
// can be stated as node label requirements that depend only on the source's
// facts: the same requirements every time for a job, as PlacementAdvisor
// requires.
package criuplacement

import (
	"encoding/json"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Facts are a node's facts, as the CRIU node agent records them in a
// PodSnapshot's status.sourceNode for the source node. An empty field is
// unknown. The JSON names are the agent's.
type Facts struct {
	// Arch is the node's architecture, as in the kubernetes.io/arch label.
	Arch              string   `json:"arch,omitempty"`
	CPUVendor         string   `json:"cpuVendor,omitempty"`
	CPUModel          string   `json:"cpuModel,omitempty"`
	CPUFeatures       []string `json:"cpuFeatures,omitempty"`
	KernelVersion     string   `json:"kernelVersion,omitempty"`
	CRIUVersion       string   `json:"criuVersion,omitempty"`
	RuncVersion       string   `json:"runcVersion,omitempty"`
	ContainerdVersion string   `json:"containerdVersion,omitempty"`
	// PlacementLabels is the node label format that the agent which took
	// the checkpoint publishes (PlacementLabelsVersion).
	PlacementLabels string `json:"placementLabels,omitempty"`
}

// SourceFacts reads the source node's facts from a CRIU PodSnapshot's
// status.sourceNode. ok is false when it's absent or unreadable.
func SourceFacts(ps *unstructured.Unstructured) (Facts, bool) {
	m, found, err := unstructured.NestedMap(ps.Object, "status", "sourceNode")
	if err != nil || !found {
		return Facts{}, false
	}
	b, err := json.Marshal(m)
	if err != nil {
		return Facts{}, false
	}
	var f Facts
	if err := json.Unmarshal(b, &f); err != nil {
		return Facts{}, false
	}
	return f, true
}

// x86Vendors are the vendors whose CPU features the rule compares: the ones
// CRIU's x86 cpuinfo support knows.
var x86Vendors = []string{"AuthenticAMD", "GenuineIntel"}

func isX86(f Facts) bool { return slices.Contains(x86Vendors, f.CPUVendor) }

// x86FeaturesKnown reports whether f's x86 CPU features are known.
func x86FeaturesKnown(f Facts) bool { return isX86(f) && len(f.CPUFeatures) > 0 }

// Verdict is the rule's answer for one source and target.
type Verdict struct {
	Compatible bool
	// Reasons say why the target isn't compatible.
	Reasons []string
}

// versionFacts are the versions the rule compares, by name.
func versionFacts(f Facts) []struct{ name, value string } {
	return []struct{ name, value string }{
		{"kernel", f.KernelVersion},
		{"CRIU", f.CRIUVersion},
		{"runc", f.RuncVersion},
		{"containerd", f.ContainerdVersion},
	}
}

// Compatible applies the rule to the facts of a source node and a target
// node (CPUFeatures the target's intersection across processors).
func Compatible(source, target Facts) Verdict {
	var reasons []string
	if s, t := labelValue(source.Arch), labelValue(target.Arch); s != "" && t != "" && s != t {
		reasons = append(reasons, fmt.Sprintf("architecture %s, not %s", t, s))
	}
	if s, t := labelVendor(source.CPUVendor), labelVendor(target.CPUVendor); s != "" && t != "" && s != t {
		reasons = append(reasons, fmt.Sprintf("CPU vendor %s, not %s", t, s))
	}
	if source.CPUModel != "" && target.CPUModel != "" && source.CPUModel != target.CPUModel {
		reasons = append(reasons, fmt.Sprintf("CPU model %q, not %q", target.CPUModel, source.CPUModel))
	}
	if x86FeaturesKnown(source) && x86FeaturesKnown(target) {
		var missing []string
		for _, flag := range InstructionFlags {
			if slices.Contains(source.CPUFeatures, flag) && !slices.Contains(target.CPUFeatures, flag) {
				missing = append(missing, flag)
			}
		}
		if len(missing) > 0 {
			reasons = append(reasons, fmt.Sprintf("CPU lacks %v", missing))
		}
		if s, t := xstateValue(source.CPUFeatures), xstateValue(target.CPUFeatures); s != t {
			reasons = append(reasons, fmt.Sprintf("XSAVE state %s, not %s", t, s))
		}
	}
	targetVersions := versionFacts(target)
	for i, sv := range versionFacts(source) {
		s, sok := versionCode(sv.value)
		t, tok := versionCode(targetVersions[i].value)
		if sok && tok && t < s {
			reasons = append(reasons, fmt.Sprintf("%s %s is older than %s", sv.name, targetVersions[i].value, sv.value))
		}
	}
	return Verdict{Compatible: len(reasons) == 0, Reasons: reasons}
}

// Unknown names the source facts that the rule can't use, so a node isn't
// checked for them: for the log, when a restore is placed.
func Unknown(source Facts) []string {
	var unknown []string
	if labelValue(source.Arch) == "" {
		unknown = append(unknown, "architecture")
	}
	if labelVendor(source.CPUVendor) == "" {
		unknown = append(unknown, "CPU vendor")
	}
	if source.CPUModel == "" {
		unknown = append(unknown, "CPU model")
	}
	if !x86FeaturesKnown(source) {
		unknown = append(unknown, "x86 CPU features")
	}
	for _, v := range versionFacts(source) {
		if _, ok := versionCode(v.value); !ok {
			unknown = append(unknown, v.name+" version")
		}
	}
	return unknown
}
