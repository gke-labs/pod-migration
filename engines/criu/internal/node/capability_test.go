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
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInstallCRIUBundle_SkipsSubdirectories(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()
	confDir := t.TempDir()
	binDir := t.TempDir()

	// Populate required bundle files.
	if err := os.WriteFile(filepath.Join(srcDir, "criu"), []byte("#!/bin/sh\necho criu\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "VERSION"), []byte("3.19\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "libdummy.so"), []byte("dummy-so-content"), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create a subdirectory inside srcDir (e.g. /bundle/licenses/) containing files.
	licensesDir := filepath.Join(srcDir, "licenses")
	if err := os.MkdirAll(licensesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(licensesDir, "criu.copyright"), []byte("GPL-2.0"), 0o644); err != nil {
		t.Fatal(err)
	}

	// installCRIUBundleDirs must skip licenses/ without erroring on copyFile.
	ver, err := installCRIUBundleDirs(srcDir, dstDir, confDir, binDir, []byte("#!/bin/sh\n"))
	if err != nil {
		t.Fatalf("installCRIUBundleDirs failed: %v", err)
	}
	if ver != "3.19" {
		t.Errorf("expected version 3.19, got %q", ver)
	}

	// Verify dstDir contains files and not the skipped directory.
	if _, err := os.Stat(filepath.Join(dstDir, "criu")); err != nil {
		t.Errorf("dstDir missing criu wrapper: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "libdummy.so")); err != nil {
		t.Errorf("dstDir missing libdummy.so: %v", err)
	}
	if conf, err := os.ReadFile(filepath.Join(confDir, "runc.conf")); err != nil {
		t.Errorf("confDir missing runc.conf: %v", err)
	} else if !strings.Contains(string(conf), "network-lock iptables\n") {
		// cleanNetnsLock only removes iptables locks (#126).
		t.Errorf("runc.conf must pin network-lock iptables, got %q", conf)
	}
	if _, err := os.Stat(filepath.Join(binDir, "criu")); err != nil {
		t.Errorf("binDir missing wrapper: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "licenses")); !os.IsNotExist(err) {
		t.Errorf("expected licenses directory to be skipped, but found in dstDir")
	}
}

func TestCheckContainerdVersion(t *testing.T) {
	tests := []struct {
		v       string
		wantErr bool
	}{
		{"containerd://2.0.2", false},
		{"containerd://2.1.0", true},
		{"containerd://2.2.6", true},
		{"containerd://2.2.7", false},
		{"containerd://2.3.1", true},
		{"containerd://2.3.4", false},
		{"containerd://3.0.0", false},
		{"1.6.20", true},
	}
	for _, tt := range tests {
		err := CheckContainerdVersion(tt.v)
		if (err != nil) != tt.wantErr {
			t.Errorf("CheckContainerdVersion(%q) err = %v, wantErr %v", tt.v, err, tt.wantErr)
		}
	}
}

func TestVerifyCRIU(t *testing.T) {
	ctx := context.Background()

	raw612 := `Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`

	tests := []struct {
		name       string
		checkMode  string
		execHost   func(context.Context, []string, string, ...string) (string, error)
		wantOK     bool
		wantReason string
	}{
		{
			name: "clean pass with core and all features supported",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) >= 1 && args[0] == "check" && (len(args) == 1 || args[1] == "--all") {
					return "Looks good.\n", nil
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     true,
			wantReason: "",
		},
		{
			name: "GKE 6.12 kernel: missing optional pagemap_scan_guard_pages and timer_cr_ids",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					switch args[2] {
					case "timer_cr_ids":
						// On Linux 6.12, check_timer_cr_ids returns -1 silently (pr_debug only)
						return "", errors.New("exit status 1")
					case "pagemap_scan_guard_pages":
						// On Linux 6.12 lacking MADV_GUARD_INSTALL, returns -1 silently without warning
						return "", errors.New("exit status 1")
					default:
						return args[2] + " is supported\n", nil
					}
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     true,
			wantReason: "missing optional CRIU features: pagemap_scan_guard_pages, timer_cr_ids",
		},
		{
			name: "single optional feature missing: timer_cr_ids",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					if args[2] == "timer_cr_ids" {
						return "", errors.New("exit status 1")
					}
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     true,
			wantReason: "missing optional CRIU features: timer_cr_ids",
		},
		{
			name: "guard pages on 6.13+ kernel with MADV_GUARD_INSTALL but lacking PAGE_IS_GUARD fails closed",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					if args[2] == "pagemap_scan_guard_pages" {
						// 6.13+ warning: madvise(MADV_GUARD_INSTALL) exists, so dump will fail!
						warn := "Warn (criu/kerndat.c:1851): ioctl(PAGEMAP_SCAN) doesn't support PAGE_IS_GUARD flag. CRIU dump will fail if dumped processes use madvise(MADV_GUARD_INSTALL).\n"
						return warn, errors.New("exit status 1")
					}
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     false,
			wantReason: "criu check missing required features: pagemap_scan_guard_pages",
		},
		{
			name: "required feature missing: uffd",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					if args[2] == "uffd" {
						return "", errors.New("exit status 1")
					}
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     false,
			wantReason: "criu check missing required features: uffd",
		},
		{
			name: "multiple required features missing: cgroupns and uffd",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					if args[2] == "cgroupns" || args[2] == "uffd" {
						return "", errors.New("exit status 1")
					}
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     false,
			wantReason: "criu check missing required features: cgroupns, uffd",
		},
		{
			name: "mixed missing required and optional features: fails closed and names required",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 2 && args[0] == "check" && args[1] == "--all" {
					return raw612, errors.New("exit status 1")
				}
				if len(args) == 3 && args[0] == "check" && args[1] == "--feature" {
					if args[2] == "uffd" || args[2] == "timer_cr_ids" {
						return "", errors.New("exit status 1")
					}
					return args[2] + " is supported\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     false,
			wantReason: "criu check missing required features: uffd",
		},
		{
			name: "core check failure: does not look good",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				return "Does not look good.\n", errors.New("exit status 1")
			},
			wantOK:     false,
			wantReason: "criu check --all failed",
		},
		{
			name: "core check binary exec error",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				return "", errors.New("binary not found")
			},
			wantOK:     false,
			wantReason: "criu check failed: binary not found",
		},
		{
			name:      "checkMode core passes with core check even if features missing",
			checkMode: "core",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				if len(args) == 1 && args[0] == "check" {
					return "Looks good.\n", nil
				}
				return "", errors.New("unexpected args")
			},
			wantOK:     true,
			wantReason: "",
		},
		{
			name:      "checkMode core fails if core check fails",
			checkMode: "core",
			execHost: func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
				return "Does not look good.\n", errors.New("exit status 1")
			},
			wantOK:     false,
			wantReason: "criu check failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PODMIG_CRIU_CHECK", tc.checkMode)
			gotOK, gotReason, _ := verifyCRIU(ctx, tc.execHost, criuBundleDir)
			if gotOK != tc.wantOK {
				t.Errorf("verifyCRIU() ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotReason != tc.wantReason {
				t.Errorf("verifyCRIU() reason = %q, want %q", gotReason, tc.wantReason)
			}
		})
	}
}
