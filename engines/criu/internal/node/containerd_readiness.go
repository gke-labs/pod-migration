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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
)

const (
	// MinContainerdCRIUVersion is the minimum CRIU version required by containerd (3.16.0).
	// Represented as major*10000 + minor*100 + patch = 31600, matching go-criu's PodCriuVersion.
	MinContainerdCRIUVersion = 31600

	// ReasonCRIUDisabledByConfig indicates containerd has enable_criu = false.
	ReasonCRIUDisabledByConfig = "criu support is disabled in containerd configuration (enable_criu = false)"
	// RemediationEnableCRIUConfig provides guidance to enable CRIU in containerd config.
	RemediationEnableCRIUConfig = "enable CRIU in containerd configuration (set enable_criu = true) and restart containerd"

	// ReasonCRIUNotFoundOnContainerdPath indicates containerd cannot resolve criu on its PATH or shim PATH.
	ReasonCRIUNotFoundOnContainerdPath = "criu binary not found in containerd PATH or shim PATH"
	// RemediationInstallCRIUOnContainerdPath provides guidance to make criu resolvable by containerd.
	RemediationInstallCRIUOnContainerdPath = "install criu wrapper on containerd PATH or configure shim PATH, and restart containerd"

	// RemediationUpgradeCRIUVersion provides guidance to update an outdated CRIU binary.
	RemediationUpgradeCRIUVersion = "upgrade CRIU binary to >= 3.16.0 and restart containerd"

	maxSymlinks = 40
)

var (
	criuVersionRegex = regexp.MustCompile(`(?:^|[Vv]ersion:\s*|\s)(\d+)\.(\d+)(?:\.(\d+))?`)
)

// ContainerdReadinessInspector encapsulates environment queries needed to check
// whether containerd can resolve and execute CRIU without poisoning its lookup cache.
type ContainerdReadinessInspector struct {
	GetPID             func(sockPath string) (int, error)
	ReadEnviron        func(pid int) ([]string, error)
	ReadCmdline        func(pid int) ([]string, error)
	ReadConfig         func(pid int) ([]byte, error)
	StatContainerdFile func(pid int, path string) (os.FileInfo, error)
	GetCRIUVersion     func(ctx context.Context, criuPath string) (string, error)
}

// resolvePathInRoot walks targetPath segment-by-segment inside root, correctly following symlinks
// relative to root rather than the calling process's root. Absolute symlinks (e.g. -> /opt/criu/bin/criu)
// are re-anchored to root, relative symlinks are resolved against the symlink's directory,
// and '..' components are clamped so they cannot escape root.
func resolvePathInRoot(root, targetPath string) (string, error) {
	cleanRoot := filepath.Clean(root)
	if !filepath.IsAbs(targetPath) {
		return "", fmt.Errorf("target path must be absolute: %q", targetPath)
	}

	cleanTarget := filepath.Clean(targetPath)
	curr := cleanRoot
	symlinkCount := 0

	parts := strings.Split(strings.TrimPrefix(cleanTarget, "/"), "/")

	for len(parts) > 0 {
		seg := parts[0]
		parts = parts[1:]
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			if curr != cleanRoot {
				curr = filepath.Dir(curr)
			}
			continue
		}

		next := filepath.Join(curr, seg)
		fi, err := os.Lstat(next)
		if err != nil {
			return "", err
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			symlinkCount++
			if symlinkCount > maxSymlinks {
				return "", fmt.Errorf("too many levels of symbolic links while resolving %s in %s", targetPath, cleanRoot)
			}
			link, err := os.Readlink(next)
			if err != nil {
				return "", err
			}
			if filepath.IsAbs(link) {
				curr = cleanRoot
				newParts := strings.Split(strings.TrimPrefix(filepath.Clean(link), "/"), "/")
				parts = append(newParts, parts...)
			} else {
				newParts := strings.Split(filepath.Clean(link), "/")
				parts = append(newParts, parts...)
			}
			continue
		}

		curr = next
	}

	return curr, nil
}

// StatFileInRoot resolves targetPath starting from root, following symlinks inside root.
func StatFileInRoot(root, targetPath string) (os.FileInfo, error) {
	resolved, err := resolvePathInRoot(root, targetPath)
	if err != nil {
		return nil, err
	}
	return os.Stat(resolved)
}

// StatFileInProcRoot resolves targetPath within /proc/<pid>/root using StatFileInRoot.
func StatFileInProcRoot(pid int, targetPath string) (os.FileInfo, error) {
	procRoot := fmt.Sprintf("/proc/%d/root", pid)
	return StatFileInRoot(procRoot, targetPath)
}

// ReadFileInRoot reads a file starting from root, resolving symlinks inside root.
func ReadFileInRoot(root, targetPath string) ([]byte, error) {
	resolved, err := resolvePathInRoot(root, targetPath)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(resolved)
}

// ReadFileInProcRoot reads a file from /proc/<pid>/root using ReadFileInRoot.
func ReadFileInProcRoot(pid int, targetPath string) ([]byte, error) {
	procRoot := fmt.Sprintf("/proc/%d/root", pid)
	return ReadFileInRoot(procRoot, targetPath)
}

// parseEnviron splits a NUL-delimited /proc/<pid>/environ byte slice into key=value strings.
func parseEnviron(raw []byte) []string {
	parts := bytes.Split(raw, []byte{0})
	env := make([]string, 0, len(parts))
	for _, p := range parts {
		if len(p) > 0 {
			env = append(env, string(p))
		}
	}
	return env
}

// parseCmdline splits a NUL-delimited /proc/<pid>/cmdline byte slice into argument strings.
func parseCmdline(raw []byte) []string {
	parts := bytes.Split(raw, []byte{0})
	args := make([]string, 0, len(parts))
	for _, p := range parts {
		if len(p) > 0 {
			args = append(args, string(p))
		}
	}
	return args
}

// readContainerdConfig discovers the containerd configuration path via cmdline args or default
// and reads it through root, failing closed on read errors.
func readContainerdConfig(root string, pid int, readCmdline func(int) ([]string, error)) ([]byte, error) {
	configPath := "/etc/containerd/config.toml"
	if readCmdline != nil {
		args, err := readCmdline(pid)
		if err != nil {
			if !os.IsNotExist(err) {
				// Fail closed if cmdline cannot be read
				return nil, fmt.Errorf("read containerd cmdline (pid %d): %w", pid, err)
			}
		} else {
			for i := 0; i < len(args); i++ {
				arg := args[i]
				if arg == "-c" || arg == "--config" || arg == "-config" {
					if i+1 < len(args) && len(args[i+1]) > 0 {
						configPath = args[i+1]
						break
					}
				} else if p, ok := strings.CutPrefix(arg, "--config="); ok {
					configPath = p
					break
				} else if p, ok := strings.CutPrefix(arg, "-config="); ok {
					configPath = p
					break
				} else if p, ok := strings.CutPrefix(arg, "-c="); ok {
					configPath = p
					break
				}
			}
		}
	}

	// Stat and read through containerd's own mount namespace
	data, err := ReadFileInRoot(root, configPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No custom config; containerd uses built-in defaults
			return nil, nil
		}
		// Fail closed on EACCES or any other read failure
		return nil, fmt.Errorf("read containerd config %s from host proc: %w", configPath, err)
	}
	return data, nil
}

// NewDefaultContainerdInspector returns the production inspector using Linux host interfaces.
func NewDefaultContainerdInspector(execHost func(context.Context, []string, string, ...string) (string, error)) *ContainerdReadinessInspector {
	insp := &ContainerdReadinessInspector{}
	insp.GetPID = func(sockPath string) (int, error) {
		pid, err := getSocketPeerPID(sockPath)
		if err != nil {
			return 0, err
		}
		if pid <= 1 {
			return 0, fmt.Errorf("invalid containerd peer PID %d", pid)
		}
		return pid, nil
	}
	insp.ReadEnviron = func(pid int) ([]string, error) {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
		if err != nil {
			return nil, err
		}
		return parseEnviron(raw), nil
	}
	insp.ReadCmdline = func(pid int) ([]string, error) {
		raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if err != nil {
			return nil, err
		}
		return parseCmdline(raw), nil
	}
	insp.ReadConfig = func(pid int) ([]byte, error) {
		procRoot := fmt.Sprintf("/proc/%d/root", pid)
		return readContainerdConfig(procRoot, pid, insp.ReadCmdline)
	}
	insp.StatContainerdFile = func(pid int, path string) (os.FileInfo, error) {
		return StatFileInProcRoot(pid, path)
	}
	insp.GetCRIUVersion = func(ctx context.Context, criuPath string) (string, error) {
		if execHost == nil {
			return "", errors.New("host command executor is not configured")
		}
		out, err := execHost(ctx, nil, criuPath, "--version")
		if err != nil {
			return "", fmt.Errorf("exec host %s --version: %w (%s)", criuPath, err, strings.TrimSpace(out))
		}
		return out, nil
	}
	return insp
}

// parseContainerdConfig extracts enable_criu and shimPath from containerd's config.toml using TOML decoding.
// Following containerd's own CRI service implementation:
// - enable_criu defaults to true if omitted; if set to false, CRIU is disabled.
// - shimPath is extracted from the last "PATH=..." entry in the [plugins."io.containerd.shim.v1.manager"] env array.
func parseContainerdConfig(data []byte) (enableCRIU *bool, shimPath string, err error) {
	if len(data) == 0 {
		return nil, "", nil
	}

	var root map[string]any
	if err := toml.Unmarshal(data, &root); err != nil {
		return nil, "", fmt.Errorf("unmarshal containerd toml: %w", err)
	}

	var foundCRIUs []bool
	if plugins, ok := root["plugins"].(map[string]any); ok {
		for key, val := range plugins {
			// CRI plugins in containerd v1 ("io.containerd.grpc.v1.cri") and v2 ("io.containerd.cri.v1.runtime") or "cri"
			if key == "io.containerd.grpc.v1.cri" || key == "io.containerd.cri.v1.runtime" || key == "cri" {
				if pluginMap, ok := val.(map[string]any); ok {
					if ec, exists := pluginMap["enable_criu"]; exists {
						if b, ok := ec.(bool); ok {
							foundCRIUs = append(foundCRIUs, b)
						}
					}
				}
			}
			// Shim manager in containerd: [plugins."io.containerd.shim.v1.manager"] env = ["PATH=..."]
			if key == "io.containerd.shim.v1.manager" {
				if shimMap, ok := val.(map[string]any); ok {
					if envVal, exists := shimMap["env"]; exists {
						if envSlice, ok := envVal.([]any); ok {
							for i := len(envSlice) - 1; i >= 0; i-- {
								if envStr, ok := envSlice[i].(string); ok {
									if path, ok := strings.CutPrefix(envStr, "PATH="); ok {
										shimPath = path
										break
									}
								}
							}
						}
					}
				}
			}
		}
	}

	if len(foundCRIUs) > 0 {
		first := foundCRIUs[0]
		for _, b := range foundCRIUs[1:] {
			if b != first {
				return nil, "", errors.New("conflicting enable_criu settings across containerd CRI plugins")
			}
		}
		enableCRIU = &first
	}

	return enableCRIU, shimPath, nil
}

// resolveCRIUPath implements containerd's resolveCriuPath lookup rules:
//   - If shimPath is set, it checks only absolute directories in shimPath for an executable criu binary.
//     It does NOT fall back to containerd's process PATH if shimPath is configured.
//   - If shimPath is empty, it checks each directory in containerd's process PATH.
//   - Relative or empty PATH components are skipped (matching containerd / exec.LookPath ErrDot).
func resolveCRIUPath(shimPath, containerdPATH string, statFn func(string) (os.FileInfo, error)) (string, error) {
	if statFn == nil {
		return "", errors.New("statFn is nil")
	}

	checkDirs := func(pathList string) (string, error) {
		for _, dir := range filepath.SplitList(pathList) {
			if !filepath.IsAbs(dir) || dir == "" || dir == "." {
				continue
			}
			target := filepath.Join(dir, "criu")
			fi, err := statFn(target)
			if err == nil && fi.Mode().IsRegular() && fi.Mode()&0o111 != 0 {
				return target, nil
			}
		}
		return "", nil
	}

	if shimPath != "" {
		return checkDirs(shimPath)
	}

	if containerdPATH != "" {
		return checkDirs(containerdPATH)
	}

	return "", nil
}

// parseCRIUVersionCode parses a version string like "4.0", "Version: 4.0", "3.16.1", "3.19", "4.0-2"
// into an integer format major*10000 + minor*100 + patch, matching go-criu's representation.
func parseCRIUVersionCode(verStr string) (int, error) {
	m := criuVersionRegex.FindStringSubmatch(verStr)
	if len(m) < 3 {
		return 0, fmt.Errorf("unrecognized criu version format: %q", verStr)
	}
	major, err1 := strconv.Atoi(m[1])
	minor, err2 := strconv.Atoi(m[2])
	if err1 != nil || err2 != nil {
		return 0, fmt.Errorf("invalid criu version numbers in %q", verStr)
	}
	patch := 0
	if len(m) > 3 && m[3] != "" {
		patch, _ = strconv.Atoi(m[3])
	}
	return major*10000 + minor*100 + patch, nil
}

// CheckContainerdReadiness evaluates whether containerd is ready to perform checkpoint/restore operations.
// It verifies that:
// 1. containerd configuration does not disable CRIU (enable_criu != false).
// 2. containerd can resolve an executable CRIU binary according to its lookup rules (shim PATH or process PATH).
// 3. the resolved CRIU binary meets the minimum version required by containerd (>= 3.16.0).
func CheckContainerdReadiness(ctx context.Context, inspector *ContainerdReadinessInspector, sockPath string) (ok bool, reason, remediation string, err error) {
	if inspector == nil {
		return false, "", "", errors.New("inspector is nil")
	}
	if inspector.GetPID == nil {
		return false, "", "", errors.New("inspector GetPID is nil")
	}
	if inspector.ReadEnviron == nil {
		return false, "", "", errors.New("inspector ReadEnviron is nil")
	}
	if inspector.ReadConfig == nil {
		return false, "", "", errors.New("inspector ReadConfig is nil")
	}
	if inspector.StatContainerdFile == nil {
		return false, "", "", errors.New("inspector StatContainerdFile is nil")
	}
	if inspector.GetCRIUVersion == nil {
		return false, "", "", errors.New("inspector GetCRIUVersion is nil")
	}

	pid, err := inspector.GetPID(sockPath)
	if err != nil {
		return false, "", "", fmt.Errorf("containerd process lookup: %w", err)
	}

	env, err := inspector.ReadEnviron(pid)
	if err != nil {
		return false, "", "", fmt.Errorf("read containerd environ (pid %d): %w", pid, err)
	}

	containerdPATH := ""
	for _, e := range env {
		if path, ok := strings.CutPrefix(e, "PATH="); ok {
			containerdPATH = path
			break
		}
	}

	cfgBytes, err := inspector.ReadConfig(pid)
	if err != nil {
		return false, truncate("containerd config error: "+err.Error(), 200), "fix containerd configuration and restart containerd", nil
	}

	enableCRIU, shimPath, err := parseContainerdConfig(cfgBytes)
	if err != nil {
		return false, truncate("containerd config error: "+err.Error(), 200), "fix containerd configuration and restart containerd", nil
	}
	if enableCRIU != nil && !*enableCRIU {
		return false, ReasonCRIUDisabledByConfig, RemediationEnableCRIUConfig, nil
	}

	statFn := func(path string) (os.FileInfo, error) {
		return inspector.StatContainerdFile(pid, path)
	}

	criuPath, err := resolveCRIUPath(shimPath, containerdPATH, statFn)
	if err != nil {
		return false, "", "", fmt.Errorf("resolve criu path: %w", err)
	}
	if criuPath == "" {
		return false, ReasonCRIUNotFoundOnContainerdPath, RemediationInstallCRIUOnContainerdPath, nil
	}

	verStr, err := inspector.GetCRIUVersion(ctx, criuPath)
	if err != nil {
		return false, "", "", fmt.Errorf("execute criu version (%s): %w", criuPath, err)
	}

	code, err := parseCRIUVersionCode(verStr)
	if err != nil {
		return false, truncate(fmt.Sprintf("failed to parse criu version (%s): %v", strings.TrimSpace(verStr), err), 200), "verify CRIU binary version format and restart containerd", nil
	}

	if code < MinContainerdCRIUVersion {
		return false, fmt.Sprintf("criu version %s on containerd PATH is older than minimum supported version 3.16.0", strings.TrimSpace(verStr)), RemediationUpgradeCRIUVersion, nil
	}

	return true, "", "", nil
}
