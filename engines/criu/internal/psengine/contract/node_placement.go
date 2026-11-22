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

// Restore placement (#257), the node agent's half. The agent publishes its
// own node's facts on the Node: labels that the controller's placement
// (controller/internal/criuplacement) turns into a replacement pod's required
// node affinity, and annotations for people. The label format here and the
// requirements there must agree; testdata/node_placement_labels.json pins
// the format, and tests in both modules read it.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// PlacementLabelsVersion is the label format below. An agent that
	// publishes it stamps it in SourceNodeFacts.PlacementLabels, and the
	// controller only places a restore by labels when the checkpoint's agent
	// did, so a controller upgraded before the agents doesn't constrain
	// restores to labels that no node carries yet.
	PlacementLabelsVersion = "v1"

	// LabelPrefix prefixes the agent's fact labels.
	LabelPrefix = "criu.pod-migrate.io/"
	// LabelCPUVendor is the x86 vendor_id, or LabelValueUnknown.
	LabelCPUVendor = LabelPrefix + "cpu-vendor"
	// LabelCPUModel is a hash of the "model name" line (CPUModelHash), or
	// LabelValueUnknown. It mirrors the restore agent's CPU family guard.
	LabelCPUModel = LabelPrefix + "cpu-model"
	// LabelXState is the x86 XSAVE state flags the node has (XStateValue),
	// or LabelValueUnknown.
	LabelXState = LabelPrefix + "xstate"
	// LabelKernelVersion and the other version labels are integer version
	// codes (VersionCode), or VersionCodeUnknown.
	LabelKernelVersion     = LabelPrefix + "kernel-version-code"
	LabelCRIUVersion       = LabelPrefix + "criu-version-code"
	LabelRuncVersion       = LabelPrefix + "runc-version-code"
	LabelContainerdVersion = LabelPrefix + "containerd-version-code"
	// MissingCPUFlagLabelPrefix prefixes one label per x86 instruction flag
	// (InstructionFlags) that the node lacks, with value "true".
	MissingCPUFlagLabelPrefix = "missing-cpu.criu.pod-migrate.io/"

	// LabelValueUnknown marks a fact the agent couldn't read. The controller
	// accepts it wherever it requires a value, so an unknown fact never
	// excludes a node.
	LabelValueUnknown = "unknown"
	// VersionCodeUnknown is the version code of an unknown version: higher
	// than any real code, so it meets every "same or newer" requirement.
	VersionCodeUnknown = "1000000000"
	// XStateNone is the xstate label value of an x86 node with none of the
	// XStateFlags.
	XStateNone = "none"

	// AnnotationNodeFacts is the node's facts as JSON, without the CPU
	// features, for people.
	AnnotationNodeFacts = "pod-migrate.io/criu-node-facts"
	// AnnotationNodeCPUFeatures is the node's CPU features, space-separated:
	// the intersection across its processors.
	AnnotationNodeCPUFeatures = "pod-migrate.io/criu-node-cpu-features"
)

// InstructionFlags are the x86 /proc/cpuinfo flags whose absence on a target
// excludes it for a checkpoint whose source had them. They're CRIU's own
// "ins" CPU capability set (x86_ins_capability_mask in criu/arch/x86/cpu.c:
// the user-visible instruction set extensions), plus xsaves from CRIU's
// "fpu" check, under their Linux names. OSXSAVE is in CRIU's set too, but
// Linux doesn't show it. CRIU's set predates some user-visible extensions, so
// these are added: fma, avx512ifma, avx512_bf16, avx512_fp16, avx_vnni,
// amx_bf16, amx_int8, movdiri, movdir64b, serialize, cldemote and waitpkg.
// Every other flag, such as the kernel's mitigation
// flags (pti, md_clear, ...), hypervisor and other virtualization flags, and
// power management flags, isn't compared, as in CRIU. Changing the list
// changes the label format: keep it in step with the controller's copy.
var InstructionFlags = []string{
	"3dnow", "3dnowext", "3dnowprefetch", "abm", "adx", "aes", "amx_bf16",
	"amx_int8", "avx", "avx2", "avx512_4fmaps", "avx512_4vnniw",
	"avx512_bf16", "avx512_bitalg", "avx512_fp16", "avx512_vbmi2",
	"avx512_vnni", "avx512_vpopcntdq", "avx512bw", "avx512cd", "avx512dq",
	"avx512er", "avx512f", "avx512ifma", "avx512pf", "avx512vbmi", "avx512vl",
	"avx_vnni", "bmi1", "bmi2", "cldemote", "clflush", "clflushopt", "clzero",
	"cmov", "cx16", "cx8", "erms", "f16c", "fma", "fma4", "fpu", "fsgsbase",
	"fxsr", "gfni", "hle", "misalignsse", "mmx", "mmxext", "monitor", "movbe",
	"movdir64b", "movdiri", "mpx", "nopl", "pclmulqdq", "pni", "popcnt",
	"rdpid", "rdrand", "rdseed", "rdtscp", "rep_good", "rtm", "sep",
	"serialize", "sha_ni", "sse", "sse2", "sse4_1", "sse4_2", "sse4a",
	"ssse3", "syscall", "tbm", "tme", "tsc", "vaes", "vpclmulqdq", "waitpkg",
	"xgetbv1", "xop", "xsave", "xsavec", "xsaveopt", "xsaves",
}

// XStateFlags are the x86 flags that stand for user XSAVE state components:
// AVX, AVX-512, protection keys and AMX. CRIU's "fpu" check requires the
// same XSAVE area size on both nodes, since glibc caches it at startup, so a
// target must have exactly the source's set, not a superset.
var XStateFlags = []string{"amx_tile", "avx", "avx512f", "ospke"}

// x86Vendors are the CPU vendors CRIU's x86 cpuinfo support knows.
var x86Vendors = []string{"AuthenticAMD", "GenuineIntel"}

// IsX86Vendor reports whether vendor is one whose flags the x86 rule
// compares.
func IsX86Vendor(vendor string) bool { return slices.Contains(x86Vendors, vendor) }

// CPUModelHash is the label value for a CPU model name: the first 16 hex
// digits of its SHA-256, since model names aren't valid label values.
func CPUModelHash(model string) string {
	sum := sha256.Sum256([]byte(model))
	return hex.EncodeToString(sum[:])[:16]
}

// XStateValue is the xstate label value for x86 features: the XStateFlags
// among them, sorted and dot-separated, or XStateNone.
func XStateValue(features []string) string {
	var have []string
	for _, f := range XStateFlags {
		if slices.Contains(features, f) {
			have = append(have, f)
		}
	}
	if len(have) == 0 {
		return XStateNone
	}
	return strings.Join(have, ".")
}

var leadingVersion = regexp.MustCompile(`^v?([0-9]+)(?:\.([0-9]+))?(?:\.([0-9]+))?`)

// VersionCode parses the leading major[.minor[.patch]] of a version, such as
// "6.1.100+", "5.15.0-1057-gke" or "v2.0.1", into major*1e6 + minor*1e3 +
// patch, which a node affinity Gt requirement compares. Anything after the
// patch is ignored. ok is false for a version without a leading number, or
// with a part over 999.
func VersionCode(v string) (code int64, ok bool) {
	m := leadingVersion.FindStringSubmatch(strings.TrimSpace(v))
	if m == nil {
		return 0, false
	}
	for i, scale := range []int64{1_000_000, 1_000, 1} {
		part := m[i+1]
		if part == "" {
			continue
		}
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || n > 999 {
			return 0, false
		}
		code += n * scale
	}
	return code, true
}

// NodePlacementLabels returns the labels that publish f, a node's own facts
// (CPUFeatures the intersection across its processors), for restore
// placement. Every fixed label is always present, LabelValueUnknown or
// VersionCodeUnknown for a fact the agent couldn't read. The missing-flag
// labels are only there for an x86 node whose features are known.
func NodePlacementLabels(f SourceNodeFacts) map[string]string {
	labels := map[string]string{
		LabelCPUVendor: LabelValueUnknown,
		LabelCPUModel:  LabelValueUnknown,
		LabelXState:    LabelValueUnknown,
	}
	if f.CPUVendor != "" && len(validation.IsValidLabelValue(f.CPUVendor)) == 0 {
		labels[LabelCPUVendor] = f.CPUVendor
	}
	if f.CPUModel != "" {
		labels[LabelCPUModel] = CPUModelHash(f.CPUModel)
	}
	for key, v := range map[string]string{
		LabelKernelVersion:     f.KernelVersion,
		LabelCRIUVersion:       f.CRIUVersion,
		LabelRuncVersion:       f.RuncVersion,
		LabelContainerdVersion: f.ContainerdVersion,
	} {
		labels[key] = VersionCodeUnknown
		if code, ok := VersionCode(v); ok {
			labels[key] = strconv.FormatInt(code, 10)
		}
	}
	if IsX86Vendor(f.CPUVendor) && len(f.CPUFeatures) > 0 {
		labels[LabelXState] = XStateValue(f.CPUFeatures)
		for _, flag := range InstructionFlags {
			if !slices.Contains(f.CPUFeatures, flag) {
				labels[MissingCPUFlagLabelPrefix+flag] = "true"
			}
		}
	}
	return labels
}

// IsPlacementLabel reports whether key is a label that NodePlacementLabels
// may publish, so the agent can drop the ones it no longer publishes.
func IsPlacementLabel(key string) bool {
	return strings.HasPrefix(key, LabelPrefix) || strings.HasPrefix(key, MissingCPUFlagLabelPrefix)
}

// NodeFactsAnnotations returns the annotations that show f, a node's own
// facts, to people. Unknown facts are left out of the JSON, and the CPU
// features annotation is only there when they're known.
func NodeFactsAnnotations(f SourceNodeFacts) map[string]string {
	withoutFeatures := f
	withoutFeatures.CPUFeatures = nil
	b, err := json.Marshal(withoutFeatures)
	if err != nil {
		b = []byte("{}")
	}
	annotations := map[string]string{AnnotationNodeFacts: string(b)}
	if len(f.CPUFeatures) > 0 {
		annotations[AnnotationNodeCPUFeatures] = strings.Join(f.CPUFeatures, " ")
	}
	return annotations
}
