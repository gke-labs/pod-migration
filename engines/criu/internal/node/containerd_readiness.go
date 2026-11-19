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
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
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
)

var (
	criuVersionRegex = regexp.MustCompile(`(\d+)\.(\d+)(?:\.(\d+))?`)
	enableCRIURegex  = regexp.MustCompile(`(?m)^\s*enable_criu\s*=\s*(false|true)\b`)
	envArrayRegex    = regexp.MustCompile(`(?s)env\s*=\s*\[(.*?)\]`)
	quotedStrRegex   = regexp.MustCompile(`"([^"\\]*(?:\\.[^"\\]*)*)"|'([^'\\]*(?:\\.[^'\\]*)*)'`)
)

// ContainerdReadinessInspector encapsulates environment queries needed to check
// whether containerd can resolve and execute CRIU without poisoning its lookup cache.
type ContainerdReadinessInspector struct {
	GetPID         func(sockPath string) (int, error)
	ReadEnviron    func(pid int) ([]string, error)
	ReadConfig     func(pid int) ([]byte, error)
	StatFile       func(path string) (os.FileInfo, error)
	GetCRIUVersion func(criuPath string) (string, error)
}

// NewDefaultContainerdInspector returns the production inspector using Linux host interfaces.
func NewDefaultContainerdInspector(execHost func(context.Context, []string, string, ...string) (string, error)) *ContainerdReadinessInspector {
	return &ContainerdReadinessInspector{
		GetPID: func(sockPath string) (int, error) {
			pid, err := getSocketPeerPID(sockPath)
			if err == nil && pid > 0 {
				return pid, nil
			}
			// Fallback: try reading containerd.pid
			pidData, errPid := os.ReadFile("/run/containerd/containerd.pid")
			if errPid == nil {
				if parsed, errParse := strconv.Atoi(strings.TrimSpace(string(pidData))); errParse == nil && parsed > 0 {
					return parsed, nil
				}
			}
			if err != nil {
				return 0, err
			}
			return 0, fmt.Errorf("could not determine containerd PID from socket or pidfile")
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
		ReadConfig: func(pid int) ([]byte, error) {
			// Check /proc/<pid>/root/etc/containerd/config.toml first
			procPath := fmt.Sprintf("/proc/%d/root/etc/containerd/config.toml", pid)
			if data, err := os.ReadFile(procPath); err == nil {
				return data, nil
			}
			// Fallback to /etc/containerd/config.toml
			if data, err := os.ReadFile("/etc/containerd/config.toml"); err == nil {
				return data, nil
			}
			return nil, nil // No custom config; containerd uses defaults
		},
		StatFile: os.Stat,
		GetCRIUVersion: func(criuPath string) (string, error) {
			if execHost != nil {
				out, err := execHost(context.Background(), nil, criuPath, "--version")
				if err == nil {
					return out, nil
				}
			}
			cmd := exec.Command(criuPath, "--version")
			out, err := cmd.CombinedOutput()
			if err != nil {
				return "", fmt.Errorf("exec %s --version: %w (%s)", criuPath, err, strings.TrimSpace(string(out)))
			}
			return string(out), nil
		},
	}
}

// parseContainerdConfig extracts enable_criu and shimPath from containerd's config.toml.
// Following containerd's own CRI service implementation:
// - enable_criu defaults to true if omitted; if set to false, CRIU is disabled.
// - shimPath is extracted from the last "PATH=..." entry in the shim manager env array, if configured.
func parseContainerdConfig(data []byte) (enableCRIU *bool, shimPath string) {
	if len(data) == 0 {
		return nil, ""
	}

	// 1. Check enable_criu
	matches := enableCRIURegex.FindAllSubmatch(data, -1)
	if len(matches) > 0 {
		// Take the last match in the file
		last := matches[len(matches)-1]
		val := string(last[1]) == "true"
		enableCRIU = &val
	}

	// 2. Check shim manager env array for PATH=
	envMatches := envArrayRegex.FindAllSubmatch(data, -1)
	for _, em := range envMatches {
		arrayBody := em[1]
		strMatches := quotedStrRegex.FindAllSubmatch(arrayBody, -1)
		for i := len(strMatches) - 1; i >= 0; i-- {
			val := string(strMatches[i][1])
			if val == "" {
				val = string(strMatches[i][2])
			}
			if path, ok := cutPrefix(val, "PATH="); ok {
				shimPath = path
				break
			}
		}
	}

	return enableCRIU, shimPath
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
func resolveCRIUPath(shimPath, containerdPATH string, statFn func(string) (os.FileInfo, error)) (string, error) {
	if statFn == nil {
		statFn = os.Stat
	}

	if shimPath != "" {
		for _, dir := range filepath.SplitList(shimPath) {
			if !filepath.IsAbs(dir) {
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

	if containerdPATH != "" {
		for _, dir := range filepath.SplitList(containerdPATH) {
			target := filepath.Join(dir, "criu")
			fi, err := statFn(target)
			if err == nil && fi.Mode().IsRegular() && fi.Mode()&0o111 != 0 {
				return target, nil
			}
		}
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
	if err != nil && !os.IsNotExist(err) {
		return false, "", "", fmt.Errorf("read containerd config: %w", err)
	}

	enableCRIU, shimPath := parseContainerdConfig(cfgBytes)
	if enableCRIU != nil && !*enableCRIU {
		return false, ReasonCRIUDisabledByConfig, RemediationEnableCRIUConfig, nil
	}

	criuPath, err := resolveCRIUPath(shimPath, containerdPATH, inspector.StatFile)
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
