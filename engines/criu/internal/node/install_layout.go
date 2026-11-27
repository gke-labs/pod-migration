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

// Where the agent installs CRIU on the host (#109).
//
// containerd 2.x has no criu_path option: runc, run by the containerd shim,
// finds criu on the shim's PATH, which is containerd's own PATH (with
// /opt/containerd/bin first on GKE's COS). On Ubuntu and kind, /usr/local/sbin
// is on that PATH and writable, so the wrapper goes there and the bundle under
// /var/lib/pod-migrate, as before.
//
// COS mounts its root, /usr and /opt included, read-only, and /var and /home
// noexec; containerd's PATH there is
// /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin, with no
// writable directory on it, and /home/kubernetes/bin, the one writable host
// path that isn't noexec, isn't on it. So on COS the agent installs the
// bundle under /home/kubernetes/bin, puts the wrapper in a directory there,
// and binds that directory over /usr/local, which COS ships empty. containerd
// then finds /usr/local/sbin/criu as it does on Ubuntu.

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// hostRootDir is the host's root directory as the agent sees it: the root of
// the host's init, which the agent's hostPID makes PID 1. The agent installs
// CRIU through it, so its DaemonSet mounts no host directory that is
// read-only on some node images.
const hostRootDir = "/proc/1/root"

// criuLayout says where on the host the agent installs CRIU. All paths are
// host paths.
type criuLayout struct {
	// name is logged at bootstrap.
	name string
	// bundleDir holds criu.bin, its libraries and loader, VERSION and the
	// wrapper. It must be on a mount that allows exec.
	bundleDir string
	// wrapperDir is where the agent writes the criu wrapper for containerd
	// to find.
	wrapperDir string
	// usrLocalBind, when set, is a host directory the agent binds over
	// /usr/local, so wrapperDir, under it, is /usr/local/sbin on the host.
	// Changing it strands nodes bound to the old one: the new agent finds
	// /usr/local not empty and refuses it until the node reboots.
	usrLocalBind string
}

var (
	// defaultLayout is the install on every node but COS: Ubuntu, kind.
	defaultLayout = criuLayout{
		name:       "default",
		bundleDir:  criuBundleDir,
		wrapperDir: "/usr/local/sbin",
	}
	// cosLayout is the install on Container-Optimized OS.
	cosLayout = criuLayout{
		name:         "cos",
		bundleDir:    "/home/kubernetes/bin/pod-migrate/criu-bundle",
		wrapperDir:   "/home/kubernetes/bin/pod-migrate/usr-local/sbin",
		usrLocalBind: "/home/kubernetes/bin/pod-migrate/usr-local",
	}
)

// detectCRIULayout picks the layout for the host whose root is root: COS when
// its /etc/os-release says ID=cos, otherwise the default.
func detectCRIULayout(root string) criuLayout {
	// In root, so an absolute symlink (/etc/os-release -> /usr/lib/os-release)
	// resolves on the host rather than in the agent's image.
	b, err := ReadFileInRoot(root, "/etc/os-release")
	if err != nil {
		return defaultLayout
	}
	sc := bufio.NewScanner(bytes.NewReader(b))
	for sc.Scan() {
		if v, ok := strings.CutPrefix(strings.TrimSpace(sc.Text()), "ID="); ok {
			if strings.Trim(v, `"'`) == "cos" {
				return cosLayout
			}
			return defaultLayout
		}
	}
	return defaultLayout
}

// renderCRIUWrapper is the criu wrapper for a bundle in bundleDir: it runs
// criu.bin with the bundle's own loader and libraries. For the default layout
// it is the wrapper build/agent/Dockerfile puts in the image's bundle.
func renderCRIUWrapper(bundleDir string) string {
	return fmt.Sprintf("#!/bin/sh\nexec %[1]s/ld-linux-x86-64.so.2 --library-path %[1]s %[1]s/criu.bin \"$@\"\n", bundleDir)
}

// installCRIUBundleLayout installs the image's bundle (srcBundleDir) on the
// host whose root the agent sees at hostRoot, in layout l, writes runc.conf
// to dstConfDir, and, for a layout that binds over /usr/local, makes sure the
// bind is in place (execHost runs the mount in the host's namespaces).
func installCRIUBundleLayout(ctx context.Context, srcBundleDir, hostRoot, dstConfDir string, l criuLayout,
	execHost func(context.Context, []string, string, ...string) (string, error)) (string, error) {
	bundleDir, err := hostDir(hostRoot, l.bundleDir)
	if err != nil {
		return "", fmt.Errorf("criu bundle directory %s: %w", l.bundleDir, err)
	}
	wrapperDir, err := hostDir(hostRoot, l.wrapperDir)
	if err != nil {
		return "", fmt.Errorf("criu wrapper directory %s: %w", l.wrapperDir, err)
	}
	version, err := installCRIUBundleDirs(srcBundleDir, bundleDir, dstConfDir, wrapperDir, []byte(renderCRIUWrapper(l.bundleDir)))
	if err != nil {
		return "", err
	}
	if l.usrLocalBind != "" {
		if err := ensureUsrLocalBind(ctx, hostRoot, l.usrLocalBind, execHost); err != nil {
			return "", err
		}
	}
	return version, nil
}

// hostDir creates the host directory p and returns it as the agent sees it,
// resolving symlinks on the host inside hostRoot. The install paths have no
// absolute symlinks on the supported images; if one did, MkdirAll would
// follow it into the agent's own filesystem and the resolve would fail.
func hostDir(hostRoot, p string) (string, error) {
	if err := os.MkdirAll(filepath.Join(hostRoot, p), 0o755); err != nil {
		return "", err
	}
	return resolvePathInRoot(hostRoot, p)
}

// ensureUsrLocalBind binds the host directory src over the host's
// /usr/local, unless it is bound there already. It refuses a /usr/local with
// anything in it, which the bind would hide: COS ships it empty.
func ensureUsrLocalBind(ctx context.Context, hostRoot, src string,
	execHost func(context.Context, []string, string, ...string) (string, error)) error {
	bound, err := usrLocalIs(hostRoot, src)
	if err != nil || bound {
		return err
	}
	usrLocal, err := resolvePathInRoot(hostRoot, "/usr/local")
	if err != nil {
		return fmt.Errorf("host /usr/local: %w", err)
	}
	entries, err := os.ReadDir(usrLocal)
	if err != nil {
		return fmt.Errorf("host /usr/local: %w", err)
	}
	if len(entries) > 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return fmt.Errorf("host /usr/local isn't empty (%s): refusing to bind %s over it for criu", strings.Join(names, ", "), src)
	}
	if execHost == nil {
		return fmt.Errorf("host command runner is not configured")
	}
	if out, err := execHost(ctx, nil, "mount", "--bind", src, "/usr/local"); err != nil {
		return fmt.Errorf("bind %s over /usr/local: %w (%s)", src, err, strings.TrimSpace(out))
	}
	bound, err = usrLocalIs(hostRoot, src)
	if err != nil {
		return err
	}
	if !bound {
		return fmt.Errorf("bound %s over /usr/local, but the host's /usr/local is still another directory", src)
	}
	return nil
}

// usrLocalIs reports whether the host's /usr/local is the host directory src.
func usrLocalIs(hostRoot, src string) (bool, error) {
	srcPath, err := resolvePathInRoot(hostRoot, src)
	if err != nil {
		return false, fmt.Errorf("host %s: %w", src, err)
	}
	srcInfo, err := os.Stat(srcPath)
	if err != nil {
		return false, fmt.Errorf("host %s: %w", src, err)
	}
	usrLocal, err := resolvePathInRoot(hostRoot, "/usr/local")
	if err != nil {
		return false, fmt.Errorf("host /usr/local: %w", err)
	}
	cur, err := os.Stat(usrLocal)
	if err != nil {
		return false, fmt.Errorf("host /usr/local: %w", err)
	}
	return os.SameFile(srcInfo, cur), nil
}
