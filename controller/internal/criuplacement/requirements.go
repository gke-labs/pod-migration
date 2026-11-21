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
	"crypto/sha256"
	"encoding/hex"
	"regexp"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// The node label format, the CRIU node agent's
// (engines/criu/internal/psengine/contract/node_placement.go). Both sides
// must agree; the golden file there pins it, and this package's tests read
// it.
const (
	// PlacementLabelsVersion is the label format this package reads.
	PlacementLabelsVersion = "v1"

	labelPrefix            = "criu.pod-migrate.io/"
	LabelCPUVendor         = labelPrefix + "cpu-vendor"
	LabelCPUModel          = labelPrefix + "cpu-model"
	LabelXState            = labelPrefix + "xstate"
	LabelKernelVersion     = labelPrefix + "kernel-version-code"
	LabelCRIUVersion       = labelPrefix + "criu-version-code"
	LabelRuncVersion       = labelPrefix + "runc-version-code"
	LabelContainerdVersion = labelPrefix + "containerd-version-code"
	// MissingCPUFlagLabelPrefix prefixes the label of an InstructionFlags
	// flag that a node lacks.
	MissingCPUFlagLabelPrefix = "missing-cpu.criu.pod-migrate.io/"
	// LabelValueUnknown is the value of a fact the agent couldn't read,
	// which every requirement on that label accepts. An unknown version is
	// published as a code higher than any real one, which meets every Gt.
	LabelValueUnknown = "unknown"

	xstateNone = "none"
)

// InstructionFlags are the x86 /proc/cpuinfo flags whose absence on a target
// excludes it for a source that had them: CRIU's "ins" CPU capability set
// (x86_ins_capability_mask in criu/arch/x86/cpu.c) under the Linux names,
// without OSXSAVE, which Linux doesn't show, plus xsaves from CRIU's "fpu"
// check, plus user-visible extensions that CRIU's set predates (fma,
// avx512ifma, avx512_bf16, avx512_fp16, avx_vnni, amx_bf16, amx_int8,
// movdiri, movdir64b, serialize, cldemote, waitpkg). Other flags, such as mitigation (pti, md_clear, ...),
// virtualization (hypervisor, vmx, ...) and timer or power management flags,
// aren't compared, as in CRIU. It must match the agent's list.
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

// XStateFlags are the x86 flags that stand for user XSAVE state components
// (AVX, AVX-512, protection keys, AMX). A target must have exactly the
// source's set: CRIU's "fpu" check requires the same XSAVE area size, which
// glibc caches at startup. It must match the agent's list.
var XStateFlags = []string{"amx_tile", "avx", "avx512f", "ospke"}

func xstateValue(features []string) string {
	var have []string
	for _, f := range XStateFlags {
		if slices.Contains(features, f) {
			have = append(have, f)
		}
	}
	if len(have) == 0 {
		return xstateNone
	}
	return strings.Join(have, ".")
}

func cpuModelHash(model string) string {
	sum := sha256.Sum256([]byte(model))
	return hex.EncodeToString(sum[:])[:16]
}

var leadingVersion = regexp.MustCompile(`^v?([0-9]+)(?:\.([0-9]+))?(?:\.([0-9]+))?`)

// versionCode parses the leading major[.minor[.patch]] of a version into
// major*1e6 + minor*1e3 + patch, as the agent does. ok is false for a
// version without a leading number, or with a part over 999: unknown.
func versionCode(v string) (code int64, ok bool) {
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

// labelVendor is the source vendor as a label value, or "" when it's
// unknown or isn't a valid label value, which the agent publishes as
// unknown too.
func labelVendor(vendor string) string { return labelValue(vendor) }

// labelValue is v, or "" when it isn't a valid, non-empty label value.
func labelValue(v string) string {
	if v == "" || len(validation.IsValidLabelValue(v)) > 0 {
		return ""
	}
	return v
}

// Requirements returns the node label requirements that a node must meet,
// all of them, to restore a checkpoint whose source node had the facts
// source, in a fixed order. A node meets them exactly when Compatible says
// so for its facts, given the labels the agent publishes for them. None for
// a checkpoint taken by an agent that doesn't publish this label format:
// nodes may not carry the labels yet.
func Requirements(source Facts) []corev1.NodeSelectorRequirement {
	if source.PlacementLabels != PlacementLabelsVersion {
		return nil
	}
	var reqs []corev1.NodeSelectorRequirement
	in := func(key, value string) {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key: key, Operator: corev1.NodeSelectorOpIn, Values: []string{value, LabelValueUnknown},
		})
	}
	// The kubelet sets kubernetes.io/arch on every node, so it needs no
	// unknown value.
	if arch := labelValue(source.Arch); arch != "" {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{arch},
		})
	}
	if vendor := labelVendor(source.CPUVendor); vendor != "" {
		in(LabelCPUVendor, vendor)
	}
	if source.CPUModel != "" {
		in(LabelCPUModel, cpuModelHash(source.CPUModel))
	}
	if x86FeaturesKnown(source) {
		in(LabelXState, xstateValue(source.CPUFeatures))
	}
	for _, v := range []struct{ key, value string }{
		{LabelKernelVersion, source.KernelVersion},
		{LabelCRIUVersion, source.CRIUVersion},
		{LabelRuncVersion, source.RuncVersion},
		{LabelContainerdVersion, source.ContainerdVersion},
	} {
		// Gt is strict, so "same or newer" is Gt code-1. A code of 0
		// (0.0.0) is met by every version: no requirement, and no "-1",
		// which isn't a valid label value.
		if code, ok := versionCode(v.value); ok && code > 0 {
			reqs = append(reqs, corev1.NodeSelectorRequirement{
				Key: v.key, Operator: corev1.NodeSelectorOpGt, Values: []string{strconv.FormatInt(code-1, 10)},
			})
		}
	}
	if x86FeaturesKnown(source) {
		for _, flag := range InstructionFlags {
			if slices.Contains(source.CPUFeatures, flag) {
				reqs = append(reqs, corev1.NodeSelectorRequirement{
					Key: MissingCPUFlagLabelPrefix + flag, Operator: corev1.NodeSelectorOpDoesNotExist,
				})
			}
		}
	}
	return reqs
}
