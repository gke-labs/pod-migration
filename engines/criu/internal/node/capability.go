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

	// nodeAPITimeout bounds individual Node GET/PATCH calls so a slow API server
	// cannot stall PSEngineCheckpoint or RunProbeLoop indefinitely.
	nodeAPITimeout = 10 * time.Second
)

// installCRIUBundle copies the bundle from the image onto the host (stock GKE
// ships no CRIU), in layout l, and puts the criu wrapper where containerd and
// runc (and therefore the kubelet checkpoint API, which fails with "criu:
// executable file not found in $PATH" otherwise) find it.
func installCRIUBundle(ctx context.Context, l criuLayout, execHost func(context.Context, []string, string, ...string) (string, error)) (version string, err error) {
	return installCRIUBundleLayout(ctx, imageBundleDir, hostRootDir, criuConfDir, l, execHost)
}

// installCRIUBundleDirs copies the bundle's files from srcBundleDir to
// dstBundleDir, writes runc.conf to dstConfDir, and writes wrapper as criu
// in both dstBundleDir and dstBinDir. The directories are as the agent sees
// them.
func installCRIUBundleDirs(srcBundleDir, dstBundleDir, dstConfDir, dstBinDir string, wrapper []byte) (version string, err error) {
	entries, err := os.ReadDir(srcBundleDir)
	if err != nil {
		return "", fmt.Errorf("agent image carries no criu bundle: %w", err)
	}
	if err := os.MkdirAll(dstBundleDir, 0o755); err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == "criu" {
			continue
		}
		src := filepath.Join(srcBundleDir, e.Name())
		dst := filepath.Join(dstBundleDir, e.Name())
		if err := copyFile(src, dst, 0o755); err != nil {
			return "", fmt.Errorf("install %s: %w", e.Name(), err)
		}
	}
	// The wrapper names the bundle's host directory, which depends on the
	// layout, so the agent writes its own rather than the image's.
	if err := writeFileAtomic(filepath.Join(dstBundleDir, "criu"), wrapper, 0o755); err != nil {
		return "", fmt.Errorf("install criu wrapper: %w", err)
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
	if err := writeFileAtomic(filepath.Join(dstBinDir, "criu"), wrapper, 0o755); err != nil {
		return "", fmt.Errorf("install criu wrapper on host PATH: %w", err)
	}

	return strings.TrimSpace(string(v)), nil
}

// knownOptionalFeatures lists optional CRIU kernel features that are checked by
// `criu check --feature <name>` but are not required for standard container checkpoint and restore.
// A node lacking only these optional features reaches criu-ready=true, with the missing
// features recorded in pod-migrate.io/criu-reason.
var knownOptionalFeatures = []string{
	"pagemap_scan_guard_pages",
	"timer_cr_ids",
}

// requiredFeatures lists the Category-2 CRIU kernel features that the engine
// depends on beyond the core CRIU checks. Each feature is probed via
// `criu check --feature <name>`.
//
//   - cgroupns: namespace isolation for container checkpoint/restore
//   - fdinfo_lock: file-locks option in runc.conf
//   - mem_dirty_track: memory dirty tracking
//   - seccomp_filters: RuntimeDefault seccomp profile dump/restore
//   - seccomp_suspend: RuntimeDefault seccomp profile dump/restore
//   - uffd: userfaultfd support
var requiredFeatures = []string{
	"cgroupns",
	"fdinfo_lock",
	"mem_dirty_track",
	"seccomp_filters",
	"seccomp_suspend",
	"uffd",
}

// verifyCRIU probes the host kernel CRIU capabilities using bundleDir's criu binary
// in the host context via nsenter.
//
// Gating is authoritative and exit-code based:
//
//  1. Core mode: when PODMIG_CRIU_CHECK=core, gates strictly on exit 0 and "Looks good"
//     from `criu check`.
//  2. Default fast path: runs `criu check --all`. If clean (exit status 0, "Looks good.",
//     and no missing features warning), the node is immediately marked ready (single invocation).
//  3. Default fallback: when `criu check --all` reports missing features or exits non-zero:
//     - Core check: if core kernel checks fail (output lacks "Looks good" or contains "Does not look good"),
//     the node is unconditionally unready (criu-ready=false).
//     - Required features: probes each engine dependency (cgroupns, fdinfo_lock, mem_dirty_track,
//     seccomp_filters, seccomp_suspend, uffd) via `criu check --feature <name>`. If any required
//     probe exits non-zero, the node is marked unready (criu-ready=false).
//     - Optional features: probes known optional features (timer_cr_ids, pagemap_scan_guard_pages).
//     For pagemap_scan_guard_pages, it is treated as optional ONLY when the kernel lacks
//     MADV_GUARD_INSTALL. On kernels with MADV_GUARD_INSTALL (6.13+) without PAGE_IS_GUARD, CRIU
//     warns that dumps will fail if processes use guard pages; in that case, it is treated as
//     required and marks the node unready.
//     - If core and all required features pass, the node is marked ready (criu-ready=true).
//     Any missing optional features are recorded in pod-migrate.io/criu-reason.
func verifyCRIU(ctx context.Context, execHost func(context.Context, []string, string, ...string) (string, error), bundleDir string) (ok bool, reason string, raw string) {
	checkMode := os.Getenv("PODMIG_CRIU_CHECK")
	if checkMode == "core" {
		out, err := execHost(ctx, nil, bundleDir+"/criu", "check")
		raw = strings.TrimSpace(out)
		if err != nil && raw == "" {
			return false, "criu check failed: " + err.Error(), "(err: " + err.Error() + ")"
		}
		if err == nil && strings.Contains(raw, "Looks good") && !strings.Contains(raw, "Does not look good") {
			return true, "", raw
		}
		return false, "criu check failed", raw
	}

	// 1. Fast path: run `criu check --all`. On fully capable kernels (all core and category-2
	// features supported), CRIU outputs "Looks good." with exit status 0 in a single invocation.
	out, err := execHost(ctx, nil, bundleDir+"/criu", "check", "--all")
	raw = strings.TrimSpace(out)
	if err != nil && raw == "" {
		return false, "criu check failed: " + err.Error(), "(err: " + err.Error() + ")"
	}
	if err == nil && strings.Contains(raw, "Looks good") && !strings.Contains(raw, "Looks good but some kernel features are missing") && !strings.Contains(raw, "Does not look good") {
		return true, "", raw
	}

	// 2. Core check: verify category-1 features.
	// In CRIU 4.2.1 (cr-check.c), CHECK_GOOD ("Looks good.") and CHECK_MAYBE ("Looks good but some kernel features...")
	// are only emitted after every Category-1 (core) check has passed. Thus, presence of "Looks good" in the
	// `criu check --all` output proves core checks passed without needing a redundant second `criu check` subprocess.
	if !strings.Contains(raw, "Looks good") || strings.Contains(raw, "Does not look good") {
		return false, "criu check failed", raw
	}

	// 3. Category-2 required features: probe each feature required by the engine via
	// `criu check --feature <name>`. Gated authoritatively by exit code.
	var missingRequired []string
	for _, feat := range requiredFeatures {
		_, featErr := execHost(ctx, nil, bundleDir+"/criu", "check", "--feature", feat)
		if featErr != nil {
			missingRequired = append(missingRequired, feat)
		}
	}
	if len(missingRequired) > 0 {
		sort.Strings(missingRequired)
		return false, "criu check missing required features: " + strings.Join(missingRequired, ", "), raw
	}

	// 4. Optional features: probe known optional features via `criu check --feature <name>`.
	// For pagemap_scan_guard_pages, it is treated as optional ONLY when the kernel lacks MADV_GUARD_INSTALL.
	// On kernels with MADV_GUARD_INSTALL (6.13+) without PAGE_IS_GUARD, CRIU warns that dumps will fail
	// if processes use guard pages; in that case, it is treated as required and marks the node unready.
	var missingOptional []string
	for _, feat := range knownOptionalFeatures {
		featOut, featErr := execHost(ctx, nil, bundleDir+"/criu", "check", "--feature", feat)
		if featErr != nil {
			if feat == "pagemap_scan_guard_pages" {
				if strings.Contains(featOut, "MADV_GUARD_INSTALL") || strings.Contains(featOut, "PAGE_IS_GUARD") {
					return false, "criu check missing required features: pagemap_scan_guard_pages", featOut
				}
			}
			missingOptional = append(missingOptional, feat)
		}
	}

	// All required features are satisfied. If any optional features are missing, record them
	// in reason so they are published in pod-migrate.io/criu-reason.
	if len(missingOptional) > 0 {
		sort.Strings(missingOptional)
		return true, "missing optional CRIU features: " + strings.Join(missingOptional, ", "), raw
	}

	return true, "", raw
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
