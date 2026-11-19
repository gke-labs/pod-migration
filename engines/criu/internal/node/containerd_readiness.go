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
	RemediationEnableCRIUConfig = "enable CRIU in containerd configuration (set enable_criu = true)"

	// ReasonCRIUNotFoundOnContainerdPath indicates containerd cannot resolve criu on its PATH or shim PATH.
	ReasonCRIUNotFoundOnContainerdPath = "criu binary not found in containerd PATH or shim PATH"
	// RemediationInstallCRIUOnContainerdPath provides guidance to make criu resolvable by containerd.
	RemediationInstallCRIUOnContainerdPath = "install criu wrapper on containerd PATH or configure shim PATH"

	maxSymlinks = 40
)

var (
	criuVersionRegex = regexp.MustCompile(`(\d+)\.(\d+)(?:\.(\d+))?`)
)

// ContainerdReadinessInspector encapsulates environment queries needed to check
// whether containerd can resolve and execute CRIU without poisoning its lookup cache.
type ContainerdReadinessInspector struct {
	GetPID             func(sockPath string) (int, error)
	ReadEnviron        func(pid int) ([]string, error)
	ReadCmdline        func(pid int) ([]string, error)
	ReadConfig         func(pid int) ([]byte, error)
	StatContainerdFile func(pid int, path string) (os.FileInfo, error)
	GetCRIUVersion     func(criuPath string) (string, error)
}

// StatFileInRoot resolves targetPath starting from root, correctly following symlinks
// relative to root rather than the calling process's root. Absolute symlinks (e.g. -> /opt/criu/bin/criu)
// are re-anchored to root, relative symlinks are resolved against the symlink's directory,
// and '..' components are clamped so they cannot escape root.
func StatFileInRoot(root, targetPath string) (os.FileInfo, error) {
	if !filepath.IsAbs(targetPath) {
		return nil, fmt.Errorf("target path must be absolute: %q", targetPath)
	}

	cleanTarget := filepath.Clean(targetPath)
	curr := root
	symlinkCount := 0

	parts := strings.Split(strings.TrimPrefix(cleanTarget, "/"), "/")

	for len(parts) > 0 {
		seg := parts[0]
		parts = parts[1:]
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			if curr != root {
				curr = filepath.Dir(curr)
			}
			continue
		}

		next := filepath.Join(curr, seg)
		fi, err := os.Lstat(next)
		if err != nil {
			return nil, err
		}

		if fi.Mode()&os.ModeSymlink != 0 {
			symlinkCount++
			if symlinkCount > maxSymlinks {
				return nil, fmt.Errorf("too many levels of symbolic links while resolving %s in %s", targetPath, root)
			}
			link, err := os.Readlink(next)
			if err != nil {
				return nil, err
			}
			if filepath.IsAbs(link) {
				curr = root
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

	return os.Stat(curr)
}

// StatFileInProcRoot resolves targetPath within /proc/<pid>/root using StatFileInRoot.
func StatFileInProcRoot(pid int, targetPath string) (os.FileInfo, error) {
	procRoot := fmt.Sprintf("/proc/%d/root", pid)
	return StatFileInRoot(procRoot, targetPath)
}

// ReadFileInProcRoot reads a file from /proc/<pid>/root, resolving symlinks relative to that root.
func ReadFileInProcRoot(pid int, targetPath string) ([]byte, error) {
	procRoot := fmt.Sprintf("/proc/%d/root", pid)
	if !filepath.IsAbs(targetPath) {
		return nil, fmt.Errorf("target path must be absolute: %q", targetPath)
	}
	cleanTarget := filepath.Clean(targetPath)
	curr := procRoot
	symlinkCount := 0
	parts := strings.Split(strings.TrimPrefix(cleanTarget, "/"), "/")

	for len(parts) > 0 {
		seg := parts[0]
		parts = parts[1:]
		if seg == "" || seg == "." {
			continue
		}
		if seg == ".." {
			if curr != procRoot {
				curr = filepath.Dir(curr)
			}
			continue
		}
		next := filepath.Join(curr, seg)
		fi, err := os.Lstat(next)
		if err != nil {
			return nil, err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			symlinkCount++
			if symlinkCount > maxSymlinks {
				return nil, fmt.Errorf("too many levels of symbolic links while reading %s in %s", targetPath, procRoot)
			}
			link, err := os.Readlink(next)
			if err != nil {
				return nil, err
			}
			if filepath.IsAbs(link) {
				curr = procRoot
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

	return os.ReadFile(curr)
}

// NewDefaultContainerdInspector returns the production inspector using Linux host interfaces.
func NewDefaultContainerdInspector(execHost func(context.Context, []string, string, ...string) (string, error)) *ContainerdReadinessInspector {
	return &ContainerdReadinessInspector{
		GetPID: func(sockPath string) (int, error) {
			pid, err := getSocketPeerPID(sockPath)
			if err == nil && pid > 1 {
				return pid, nil
			}
			// If pid == 1 (e.g. systemd socket activation) or peer lookup failed,
			// fallback to reading containerd.pid on the host filesystem.
			pidData, errPid := os.ReadFile("/run/containerd/containerd.pid")
			if errPid == nil {
				if parsed, errParse := strconv.Atoi(strings.TrimSpace(string(pidData))); errParse == nil && parsed > 1 {
					return parsed, nil
				}
			}
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("could not determine containerd PID from socket (pid=%d) or pidfile", pid)
		},
		ReadEnviron: func(pid int) ([]string, error) {
			raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
			if err != nil {
				return nil, err
			}
			parts := bytes.Split(raw, []byte{0})
			env := make([]string, 0, len(parts))
			for _, p := range parts {
				if len(p) > 0 {
					env = append(env, string(p))
				}
			}
			return env, nil
		},
		ReadCmdline: func(pid int) ([]string, error) {
			raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
			if err != nil {
				return nil, err
			}
			parts := bytes.Split(raw, []byte{0})
			args := make([]string, 0, len(parts))
			for _, p := range parts {
				if len(p) > 0 {
					args = append(args, string(p))
				}
			}
			return args, nil
		},
		ReadConfig: func(pid int) ([]byte, error) {
			configPath := "/etc/containerd/config.toml"
			// Check cmdline for custom -c or --config argument
			if cmdlineRaw, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid)); err == nil {
				args := bytes.Split(cmdlineRaw, []byte{0})
				for i := 0; i < len(args); i++ {
					arg := string(args[i])
					if arg == "-c" || arg == "--config" {
						if i+1 < len(args) && len(args[i+1]) > 0 {
							configPath = string(args[i+1])
							break
						}
					} else if strings.HasPrefix(arg, "--config=") {
						configPath = strings.TrimPrefix(arg, "--config=")
						break
					}
				}
			}

			// Stat and read through containerd's own mount namespace (/proc/<pid>/root)
			data, err := ReadFileInProcRoot(pid, configPath)
			if err != nil {
				if os.IsNotExist(err) {
					// No custom config; containerd uses built-in defaults
					return nil, nil
				}
				// Fail closed on EACCES or any other read failure
				return nil, fmt.Errorf("read containerd config %s from host proc: %w", configPath, err)
			}
			return data, nil
		},
		StatContainerdFile: func(pid int, path string) (os.FileInfo, error) {
			return StatFileInProcRoot(pid, path)
		},
		GetCRIUVersion: func(criuPath string) (string, error) {
			if execHost == nil {
				return "", errors.New("host command executor is not configured")
			}
			out, err := execHost(context.Background(), nil, criuPath, "--version")
			if err != nil {
				return "", fmt.Errorf("exec host %s --version: %w (%s)", criuPath, err, strings.TrimSpace(out))
			}
			return out, nil
		},
	}
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

	if plugins, ok := root["plugins"].(map[string]any); ok {
		for key, val := range plugins {
			// CRI plugins in containerd v1 ("io.containerd.grpc.v1.cri") and v2 ("io.containerd.cri.v1.runtime")
			if key == "io.containerd.grpc.v1.cri" || key == "io.containerd.cri.v1.runtime" || key == "cri" || strings.Contains(key, "cri") {
				if pluginMap, ok := val.(map[string]any); ok {
					if ec, exists := pluginMap["enable_criu"]; exists {
						if b, ok := ec.(bool); ok {
							enableCRIU = &b
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
									if path, ok := cutPrefix(envStr, "PATH="); ok {
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

	return enableCRIU, shimPath, nil
}

func cutPrefix(s, prefix string) (string, bool) {
	if strings.HasPrefix(s, prefix) {
		return strings.TrimPrefix(s, prefix), true
	}
	return s, false
}

// resolveCRIUPath implements containerd's resolveCriuPath lookup rules:
//   - If shimPath is set, it checks only absolute directories in shimPath for an executable criu binary.
//     It does NOT fall back to containerd's process PATH if shimPath is configured.
//   - If shimPath is empty, it checks each directory in containerd's process PATH.
//   - Relative or empty PATH components are skipped (matching containerd / exec.LookPath ErrDot).
func resolveCRIUPath(shimPath, containerdPATH string, statFn func(string) (os.FileInfo, error)) (string, error) {
	if statFn == nil {
		statFn = os.Stat
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
func CheckContainerdReadiness(inspector *ContainerdReadinessInspector, sockPath string) (ok bool, reason, remediation string, err error) {
	if inspector == nil {
		return false, "", "", errors.New("inspector is nil")
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
		if path, ok := cutPrefix(e, "PATH="); ok {
			containerdPATH = path
			break
		}
	}

	cfgBytes, err := inspector.ReadConfig(pid)
	if err != nil {
		return false, "", "", fmt.Errorf("read containerd config: %w", err)
	}

	enableCRIU, shimPath, err := parseContainerdConfig(cfgBytes)
	if err != nil {
		return false, "", "", fmt.Errorf("parse containerd config: %w", err)
	}
	if enableCRIU != nil && !*enableCRIU {
		return false, ReasonCRIUDisabledByConfig, RemediationEnableCRIUConfig, nil
	}

	statFn := func(path string) (os.FileInfo, error) {
		if inspector.StatContainerdFile != nil {
			return inspector.StatContainerdFile(pid, path)
		}
		return os.Stat(path)
	}

	criuPath, err := resolveCRIUPath(shimPath, containerdPATH, statFn)
	if err != nil {
		return false, "", "", fmt.Errorf("resolve criu path: %w", err)
	}
	if criuPath == "" {
		return false, ReasonCRIUNotFoundOnContainerdPath, RemediationInstallCRIUOnContainerdPath, nil
	}

	verStr, err := inspector.GetCRIUVersion(criuPath)
	if err != nil {
		return false, fmt.Sprintf("failed to retrieve criu version from %s: %v", criuPath, err), "verify CRIU binary functionality", nil
	}

	code, err := parseCRIUVersionCode(verStr)
	if err != nil {
		return false, fmt.Sprintf("failed to parse criu version (%s): %v", strings.TrimSpace(verStr), err), "verify CRIU binary version format", nil
	}

	if code < MinContainerdCRIUVersion {
		return false, fmt.Sprintf("criu version %s on containerd PATH is older than minimum supported version 3.16.0", strings.TrimSpace(verStr)), "upgrade CRIU binary to >= 3.16.0", nil
	}

	return true, "", "", nil
}
