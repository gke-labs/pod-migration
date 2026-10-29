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
	"strconv"

	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// NodeAnnotationCRIUReady records whether the host kernel passed the CRIU check.
	NodeAnnotationCRIUReady = "pod-migrate.io/criu-ready"
	// NodeAnnotationCRIUVersion records the installed CRIU binary version on the node.
	NodeAnnotationCRIUVersion = "pod-migrate.io/criu-version"

	// imageBundleDir is where the agent image carries the self-contained CRIU
	// bundle (built by build/agent/Dockerfile from a criu >= 3.19 distro).
	imageBundleDir = "/opt/criu-bundle"

	// hostPathBinDir is a host directory on containerd's/kubelet's PATH
	// (hostPath mount). The kubelet checkpoint API fails with "criu: executable
	// file not found in $PATH" unless the wrapper is visible there.
	hostPathBinDir = "/usr/local/sbin"
)

// installCRIUBundle copies the bundle from the image onto the node's writable
// path (stock GKE ships no CRIU; COS needs a writable location) and installs
// the wrapper on the host PATH.
func installCRIUBundle() (version string, err error) {
	entries, err := os.ReadDir(imageBundleDir)
	if err != nil {
		return "", fmt.Errorf("agent image carries no criu bundle: %w", err)
	}
	if err := os.MkdirAll(criuBundleDir, 0o755); err != nil {
		return "", err
	}
	for _, e := range entries {
		src := filepath.Join(imageBundleDir, e.Name())
		dst := filepath.Join(criuBundleDir, e.Name())
		if err := copyFile(src, dst, 0o755); err != nil {
			return "", fmt.Errorf("install %s: %w", e.Name(), err)
		}
	}
	v, err := os.ReadFile(filepath.Join(criuBundleDir, "VERSION"))
	if err != nil {
		return "", fmt.Errorf("bundle has no VERSION: %w", err)
	}
	// CRIU options for every runc-driven CRIU invocation on this node — CRIU
	// reads /etc/criu/runc.conf for dump AND restore. tcp-close and
	// skip-in-flight abandon established and half-open connections so clients
	// reconnect cleanly; file-validation buildid verifies binary identity on
	// restore. Written at bootstrap because the kubelet checkpoint dump happens
	// before any agent-driven restore.
	if err := os.MkdirAll(criuConfDir, 0o755); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(criuConfDir, "runc.conf"),
		[]byte("tcp-close\nskip-in-flight\nfile-validation buildid\nfile-locks\n"), 0o644); err != nil {
		return "", fmt.Errorf("write criu runc.conf: %w", err)
	}
	// Put the wrapper on the host PATH so containerd/runc (and therefore the
	// kubelet checkpoint API) can exec it.
	wrapper, err := os.ReadFile(filepath.Join(criuBundleDir, "criu"))
	if err != nil {
		return "", err
	}
	if err := writeFileAtomic(filepath.Join(hostPathBinDir, "criu"), wrapper, 0o755); err != nil {
		return "", fmt.Errorf("install criu wrapper on host PATH: %w", err)
	}

	// Provide a seccomp profile to safely bypass io_uring on modern datastores.
	seccompDir := "/var/lib/kubelet/seccomp"
	_ = os.MkdirAll(seccompDir, 0o755)
	profile := `{"defaultAction":"SCMP_ACT_ALLOW","syscalls":[{"names":["io_uring_setup"],"action":"SCMP_ACT_ERRNO","errnoRet":38}]}`
	if err := os.WriteFile(filepath.Join(seccompDir, "disable-iouring.json"), []byte(profile), 0o644); err != nil {
		return "", fmt.Errorf("write seccomp profile: %w", err)
	}

	return strings.TrimSpace(string(v)), nil
}

// verifyCRIU runs `criu check --all` on the host (host kernel context) via
// nsenter and returns the raw output. PODMIG_CRIU_CHECK=core relaxes the gate
// to plain `criu check` (core C/R features only) for kernels where `--all`
// reports optional/experimental feature warnings while core dump/restore works.
func verifyCRIU(ctx context.Context, execHost func(context.Context, []string, string, ...string) (string, error)) (ok bool, raw string) {
	args := []string{"check", "--all"}
	if os.Getenv("PODMIG_CRIU_CHECK") == "core" {
		args = []string{"check"}
	}
	out, err := execHost(ctx, nil, criuBundleDir+"/criu", args...)
	raw = strings.TrimSpace(out)
	if err != nil {
		return false, raw + " (err: " + err.Error() + ")"
	}
	return strings.Contains(raw, "Looks good"), raw
}

// annotateNode publishes the node's CRIU capability verdict and version.
func annotateNode(ctx context.Context, c client.Client, nodeName, version string, ready bool) error {
	var node corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return err
	}
	patch := client.MergeFrom(node.DeepCopy())
	if node.Annotations == nil {
		node.Annotations = map[string]string{}
	}
	node.Annotations[NodeAnnotationCRIUReady] = fmt.Sprintf("%t", ready)
	node.Annotations[NodeAnnotationCRIUVersion] = version
	return c.Patch(ctx, &node, patch)
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
