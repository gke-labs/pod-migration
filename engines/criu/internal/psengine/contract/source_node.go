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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// SourceNodeFacts are the facts about the node a CRIU checkpoint was taken
// on that decide which nodes can restore it (#257): the CPU, the kernel, and
// the CRIU, runc and containerd versions. The node agent records them when it
// takes the checkpoint, in the archive manifest and in PodSnapshot
// status.sourceNode. An empty field means the fact is unknown, never that the
// node lacks it.
//
// The controller reads them from status.sourceNode to place the restore
// (C8): see node_placement.go and controller/internal/criuplacement.
type SourceNodeFacts struct {
	// Arch is the node's architecture as Go names it (amd64, arm64): the
	// agent's own, which runs natively on the node. It matches the
	// kubernetes.io/arch label the kubelet sets.
	Arch string `json:"arch,omitempty"`
	// CPUVendor is the "vendor_id" line of /proc/cpuinfo (x86), such as
	// GenuineIntel. Absent on arm64.
	CPUVendor string `json:"cpuVendor,omitempty"`
	// CPUModel is the "model name" line of /proc/cpuinfo, the value the
	// restore node's CPU family guard compares (restore_node.go). Absent on
	// arm64, whose cpuinfo has no such line.
	CPUModel string `json:"cpuModel,omitempty"`
	// CPUFeatures are the CPU feature flags, the "flags" (x86) or "Features"
	// (arm64) lines of /proc/cpuinfo: the union across processors, sorted.
	// The union is right for a source (what the app may have used); a
	// target's features would be the intersection. The list is raw: on x86
	// it also carries kernel and mitigation flags (pti, md_clear,
	// hypervisor, ...) that a compatibility rule has to filter out.
	CPUFeatures []string `json:"cpuFeatures,omitempty"`
	// KernelVersion is the kernel release (uname -r).
	KernelVersion string `json:"kernelVersion,omitempty"`
	// CRIUVersion is the version in the installed CRIU bundle's VERSION
	// file, written from that binary's `criu --version` when the agent image
	// is built. The binary itself isn't run.
	CRIUVersion string `json:"criuVersion,omitempty"`
	// RuncVersion is what `runc --version` reports in the host mount
	// namespace, on the PATH the agent's restores run runc with. That may
	// not be the runc containerd's config names.
	RuncVersion string `json:"runcVersion,omitempty"`
	// ContainerdVersion is the version containerd reports, without a
	// leading "v".
	ContainerdVersion string `json:"containerdVersion,omitempty"`
	// PlacementLabels is the node label format (PlacementLabelsVersion)
	// that the agent which took the checkpoint publishes on its own node.
	// The controller only places a restore by those labels when it's set.
	PlacementLabels string `json:"placementLabels,omitempty"`
}

// IsEmpty reports whether no fact is known. PlacementLabels isn't a fact.
func (f SourceNodeFacts) IsEmpty() bool {
	return f.Arch == "" && f.CPUVendor == "" && f.CPUModel == "" && len(f.CPUFeatures) == 0 && f.KernelVersion == "" &&
		f.CRIUVersion == "" && f.RuncVersion == "" && f.ContainerdVersion == ""
}

// SetSourceNodeFacts writes f to status.sourceNode on a PodSnapshot. Unknown
// facts are omitted. Empty facts write nothing.
func SetSourceNodeFacts(ps *unstructured.Unstructured, f SourceNodeFacts) error {
	if f.IsEmpty() {
		return nil
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	return unstructured.SetNestedMap(ps.Object, m, "status", "sourceNode")
}

// GetSourceNodeFacts reads status.sourceNode from a PodSnapshot. ok is false
// when the field is absent or unreadable.
func GetSourceNodeFacts(ps *unstructured.Unstructured) (SourceNodeFacts, bool) {
	m, found, err := unstructured.NestedMap(ps.Object, "status", "sourceNode")
	if err != nil || !found {
		return SourceNodeFacts{}, false
	}
	b, err := json.Marshal(m)
	if err != nil {
		return SourceNodeFacts{}, false
	}
	var f SourceNodeFacts
	if err := json.Unmarshal(b, &f); err != nil {
		return SourceNodeFacts{}, false
	}
	return f, true
}
