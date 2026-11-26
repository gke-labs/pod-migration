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

package node

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// NodeAnnotationCRIUReady records whether the host kernel passed the CRIU check.
	NodeAnnotationCRIUReady = "pod-migrate.io/criu-ready"
	// NodeAnnotationCRIUVersion records the installed CRIU binary version on the node.
	NodeAnnotationCRIUVersion = "pod-migrate.io/criu-version"
	// NodeAnnotationCRIUReason records why the node is unready for CRIU operations,
	// or what optional CRIU features are missing on an otherwise ready node.
	NodeAnnotationCRIUReason = "pod-migrate.io/criu-reason"
	// NodeAnnotationCRIURemediation records actionable remediation instructions for operators.
	NodeAnnotationCRIURemediation = "pod-migrate.io/criu-remediation"

	// ReasonContainerdCachedCRIU indicates containerd cached a failed CRIU lookup.
	ReasonContainerdCachedCRIU = "containerd cached failed CRIU lookup; restart containerd to clear"
	// RemediationRestartContainerd provides actionable guidance to clear ReasonContainerdCachedCRIU.
	RemediationRestartContainerd = "restart containerd (systemctl restart containerd)"

	// imageBundleDir is where the agent image carries the self-contained CRIU
	// bundle (built by build/agent/Dockerfile from a criu >= 4.2.1 distro).
	imageBundleDir = "/opt/criu-bundle"

	// hostPathBinDir is a host directory on containerd's/kubelet's PATH
	// (hostPath mount). The kubelet checkpoint API fails with "criu: executable
	// file not found in $PATH" unless the wrapper is visible there.
	hostPathBinDir = "/usr/local/sbin"

	// nodeAPITimeout bounds individual Node GET/PATCH calls so a slow API server
	// cannot stall PSEngineCheckpoint or RunProbeLoop indefinitely.
	nodeAPITimeout = 10 * time.Second
)

// installCRIUBundle copies the bundle from the image onto the node's writable
// path (stock GKE ships no CRIU; COS needs a writable location) and installs
// the wrapper on the host PATH.
func installCRIUBundle() (version string, err error) {
	return installCRIUBundleDirs(imageBundleDir, criuBundleDir, criuConfDir, hostPathBinDir)
}

func installCRIUBundleDirs(srcBundleDir, dstBundleDir, dstConfDir, dstBinDir string) (version string, err error) {
	entries, err := os.ReadDir(srcBundleDir)
	if err != nil {
		return "", fmt.Errorf("agent image carries no criu bundle: %w", err)
	}
	if err := os.MkdirAll(dstBundleDir, 0o755); err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		src := filepath.Join(srcBundleDir, e.Name())
		dst := filepath.Join(dstBundleDir, e.Name())
		if err := copyFile(src, dst, 0o755); err != nil {
			return "", fmt.Errorf("install %s: %w", e.Name(), err)
		}
	}
	v, err := os.ReadFile(filepath.Join(dstBundleDir, "VERSION"))
	if err != nil {
		return "", fmt.Errorf("bundle has no VERSION: %w", err)
	}
	// CRIU options for every runc-driven CRIU invocation on this node — CRIU
	// reads /etc/criu/runc.conf for dump AND restore. tcp-close and
	// skip-in-flight abandon established and half-open connections so clients
	// reconnect cleanly; file-validation buildid verifies binary identity on
	// restore. network-lock iptables pins the lock method, whose default the
	// CRIU package chooses at build time, to the one cleanNetnsLock removes
	// after a failed restore (#126). Written at bootstrap because the kubelet
	// checkpoint dump happens before any agent-driven restore.
	if err := os.MkdirAll(dstConfDir, 0o755); err != nil {
		return "", err
	}
	if err := writeFileAtomic(filepath.Join(dstConfDir, "runc.conf"),
		[]byte("tcp-close\nskip-in-flight\nfile-validation buildid\nfile-locks\nnetwork-lock iptables\n"), 0o644); err != nil {
		return "", fmt.Errorf("write criu runc.conf: %w", err)
	}
	// Put the wrapper on the host PATH so containerd/runc (and therefore the
	// kubelet checkpoint API) can exec it.
	wrapper, err := os.ReadFile(filepath.Join(dstBundleDir, "criu"))
	if err != nil {
		return "", err
	}
	if err := writeFileAtomic(filepath.Join(dstBinDir, "criu"), wrapper, 0o755); err != nil {
		return "", fmt.Errorf("install criu wrapper on host PATH: %w", err)
	}

	return strings.TrimSpace(string(v)), nil
}

// knownOptionalFeatures lists optional CRIU kernel features that are checked by
// `criu check --all` but are not required for standard container checkpoint and restore.
// A node lacking only these optional features reaches criu-ready=true, with the missing
// features recorded in pod-migrate.io/criu-reason.
var knownOptionalFeatures = []string{
	"pagemap_scan_guard_pages",
	"timer_cr_ids",
}

type featurePattern struct {
	name  string
	regex *regexp.Regexp
}

var knownFeaturePatterns = func() []featurePattern {
	knownFeatures := []string{
		"uffd_noncoop",
		"uffd",
		"cgroupns",
		"aio_remap",
		"seccomp_suspend",
		"seccomp_filters",
		"mem_dirty_track",
		"posix_timers",
		"timerfd",
		"tun_ns",
		"tun",
		"userns",
		"fdinfo_lock",
		"tcp_half_closed",
		"tcp_window",
		"compat_cr",
		"can_map_vdso",
		"sk_unix_file",
		"sk_ns",
		"net_diag_raw",
		"link_nsid",
		"nsid",
		"kcmp_epoll",
		"timens",
		"external_net_ns",
		"clone3_set_tid",
		"newifindex",
		"network_lock_nftables",
		"nftables",
		"has_ipt_legacy",
		"pidfd_store",
		"ns_pid",
		"apparmor_stacking",
		"sockopt_buf_lock",
		"memfd_hugetlb",
		"move_mount_set_group",
		"openat2",
		"get_rseq_conf",
		"ipv6_freebind",
		"pagemap_scan",
		"overlayfs_maps",
		"autofs",
	}
	patterns := make([]featurePattern, len(knownFeatures))
	for i, feat := range knownFeatures {
		patterns[i] = featurePattern{
			name:  feat,
			regex: regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(feat) + `\b`),
		}
	}
	return patterns
}()

// verifyCRIU runs `criu check --all` on the host (host kernel context) via
// nsenter and returns the capability verdict, reason, and raw output.
// PODMIG_CRIU_CHECK=core relaxes the gate to plain `criu check` (core C/R features only)
// for kernels where `--all` reports optional/experimental feature warnings while core dump/restore works.
// When `--all` reports missing features, if only known optional features are missing
// (e.g. pagemap_scan_guard_pages, timer_cr_ids), verifyCRIU returns ok=true and records the missing
// features in reason so they can be published on the node.
func verifyCRIU(ctx context.Context, execHost func(context.Context, []string, string, ...string) (string, error)) (ok bool, reason string, raw string) {
	checkMode := os.Getenv("PODMIG_CRIU_CHECK")
	args := []string{"check", "--all"}
	if checkMode == "core" {
		args = []string{"check"}
	}
	out, err := execHost(ctx, nil, criuBundleDir+"/criu", args...)
	raw = strings.TrimSpace(out)
	if err != nil && raw == "" {
		return false, "criu check failed: " + err.Error(), "(err: " + err.Error() + ")"
	}
	ok, reason = parseCRIUCheckOutput(raw, checkMode)
	return ok, reason, raw
}

// parseCRIUCheckOutput evaluates the stdout/stderr from `criu check` or `criu check --all`.
// When checkMode is "core", it only checks for the core "Looks good" confirmation.
// When checkMode is "all" (or empty default), it distinguishes between:
//   - Clean pass: "Looks good." with no feature warnings -> ok=true, reason=""
//   - Optional-only missing features: "Looks good but some kernel features are missing" with only known
//     optional features missing -> ok=true, reason="missing optional CRIU features: ..."
//   - Required missing features: "Looks good but some kernel features are missing" with features the engine needs missing -> ok=false, reason="criu check --all missing required features: ..."
//   - Core check failure: does not contain "Looks good" or contains "Does not look good" -> ok=false, reason="criu check --all failed"
func parseCRIUCheckOutput(raw string, checkMode string) (bool, string) {
	if checkMode == "core" {
		if strings.Contains(raw, "Looks good") && !strings.Contains(raw, "Does not look good") {
			return true, ""
		}
		return false, "criu check failed"
	}

	if !strings.Contains(raw, "Looks good") || strings.Contains(raw, "Does not look good") {
		return false, "criu check --all failed"
	}

	if !strings.Contains(raw, "Looks good but some kernel features are missing") {
		return true, ""
	}

	var missingOptional []string
	var missingRequired []string
	seenOptional := make(map[string]bool)
	seenRequired := make(map[string]bool)

	lines := strings.Split(raw, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.Contains(line, "Looks good") ||
			strings.Contains(line, "which, depending on your process tree") ||
			strings.Contains(line, "dump or restore failure.") ||
			strings.HasPrefix(line, "(err: ") ||
			strings.HasPrefix(line, "Version:") ||
			strings.HasPrefix(line, "Running on") ||
			strings.HasPrefix(line, "File ") {
			continue
		}

		lower := strings.ToLower(line)
		isWarning := strings.Contains(lower, "warn") ||
			strings.Contains(lower, "error") ||
			strings.Contains(lower, "not supported") ||
			strings.Contains(lower, "missing") ||
			strings.Contains(lower, "doesn't support") ||
			strings.Contains(lower, "does not support") ||
			strings.Contains(lower, "is off") ||
			strings.Contains(lower, "failed")

		if !isWarning {
			continue
		}

		if strings.Contains(lower, "pagemap_scan_guard_pages") ||
			strings.Contains(lower, "page_is_guard") ||
			strings.Contains(lower, "madv_guard") {
			if !seenOptional["pagemap_scan_guard_pages"] {
				seenOptional["pagemap_scan_guard_pages"] = true
				missingOptional = append(missingOptional, "pagemap_scan_guard_pages")
			}
			continue
		}

		if strings.Contains(lower, "timer_cr_ids") ||
			strings.Contains(lower, "timer_create_restore_ids") {
			if !seenOptional["timer_cr_ids"] {
				seenOptional["timer_cr_ids"] = true
				missingOptional = append(missingOptional, "timer_cr_ids")
			}
			continue
		}

		feat := extractFeatureName(line)
		if !seenRequired[feat] {
			seenRequired[feat] = true
			missingRequired = append(missingRequired, feat)
		}
	}

	if len(missingRequired) > 0 {
		sort.Strings(missingRequired)
		return false, "criu check --all missing required features: " + strings.Join(missingRequired, ", ")
	}

	if len(missingOptional) > 0 {
		sort.Strings(missingOptional)
		return true, "missing optional CRIU features: " + strings.Join(missingOptional, ", ")
	}

	return false, "criu check --all missing unspecified features"
}

// extractFeatureName extracts a canonical or clean feature identifier from a CRIU check warning line.
func extractFeatureName(line string) string {
	for _, p := range knownFeaturePatterns {
		if p.regex.MatchString(line) {
			return p.name
		}
	}

	cleaned := line
	if idx := strings.Index(cleaned, "): "); idx != -1 {
		cleaned = strings.TrimSpace(cleaned[idx+3:])
	} else if idx := strings.Index(cleaned, ": "); idx != -1 {
		cleaned = strings.TrimSpace(cleaned[idx+2:])
	}
	return cleaned
}

// annotateNode publishes the node's CRIU capability verdict, version, and optional reason/remediation.
func annotateNode(ctx context.Context, c client.Client, nodeName, version, reason, remediation string, ready bool) error {
	apiCtx, cancel := context.WithTimeout(ctx, nodeAPITimeout)
	defer cancel()

	var node corev1.Node
	if err := c.Get(apiCtx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return err
	}
	patch := client.MergeFrom(node.DeepCopy())
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[NodeAnnotationCRIUReady] = fmt.Sprintf("%t", ready)
	if version != "" {
		node.Annotations[NodeAnnotationCRIUVersion] = version
	}
	if ready {
		if reason != "" {
			node.Annotations[NodeAnnotationCRIUReason] = reason
		} else {
			delete(node.Annotations, NodeAnnotationCRIUReason)
		}
		delete(node.Annotations, NodeAnnotationCRIURemediation)
	} else {
		if reason != "" {
			node.Annotations[NodeAnnotationCRIUReason] = reason
		} else {
			delete(node.Annotations, NodeAnnotationCRIUReason)
		}
		if remediation != "" {
			node.Annotations[NodeAnnotationCRIURemediation] = remediation
		} else {
			delete(node.Annotations, NodeAnnotationCRIURemediation)
		}
	}
	return c.Patch(apiCtx, &node, patch)
}

// copyFile installs src at dst atomically via temp-file + rename so an
// executing binary never fails with ETXTBSY on re-bootstrap.
func copyFile(src, dst string, mode os.FileMode) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return writeFileAtomic(dst, data, mode)
}

// writeFileAtomic replaces dst via temp-file + rename, skipping identical
// content (safe against ETXTBSY on executing binaries; idempotent).
func writeFileAtomic(dst string, data []byte, mode os.FileMode) error {
	if old, err := os.ReadFile(dst); err == nil && bytes.Equal(old, data) {
		return os.Chmod(dst, mode)
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".tmp*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op after successful rename
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(mode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, dst)
}

func CheckContainerdVersion(runtimeVersion string) error {
	v := strings.TrimPrefix(runtimeVersion, "containerd://")
	parts := strings.Split(v, ".")
	if len(parts) < 3 {
		return fmt.Errorf("unrecognized containerd version format: %s", runtimeVersion)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	patch, err3 := strconv.Atoi(strings.Split(parts[2], "-")[0])
	if err1 != nil || err2 != nil || err3 != nil {
		return fmt.Errorf("failed to parse containerd version: %s", runtimeVersion)
	}

	if major < 2 {
		return fmt.Errorf("containerd major version < 2 is not supported")
	}
	if major == 2 {
		if minor == 1 {
			return fmt.Errorf("containerd version %s is vulnerable to GHSA-p7v4; containerd >= 2.2.7 or >= 2.3.4 or 2.0.x required", runtimeVersion)
		}
		if minor == 2 && patch < 7 {
			return fmt.Errorf("containerd version %s is vulnerable to GHSA-p7v4; containerd >= 2.2.7 or >= 2.3.4 or 2.0.x required", runtimeVersion)
		}
		if minor == 3 && patch < 4 {
			return fmt.Errorf("containerd version %s is vulnerable to GHSA-p7v4; containerd >= 2.2.7 or >= 2.3.4 or 2.0.x required", runtimeVersion)
		}
	}
	return nil
}
