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
	"reflect"
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
	ver, err := installCRIUBundleDirs(srcDir, dstDir, confDir, binDir)
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

func TestParseCRIUCheckOutput(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		checkMode  string
		wantOK     bool
		wantReason string
	}{
		{
			name: "clean pass with Looks good",
			raw: `Version: 4.2.1
Running on Linux 6.6.0
File /run/criu.kdat does not exist
Looks good.
`,
			checkMode:  "all",
			wantOK:     true,
			wantReason: "",
		},
		{
			name: "GKE 6.12 kernel: missing pagemap_scan_guard_pages and timer_cr_ids (only optional features)",
			raw: `(00.000042) Version: 4.2.1
(00.000055) Running on Linux 6.12.113+
Warn  (criu/kerndat.c:1904): ioctl(PAGEMAP_SCAN) doesn't support PAGE_IS_GUARD flag. CRIU dump will fail if dumped processes use madvise(MADV_GUARD_INSTALL). Please, consider updating your kernel.
Warn  (criu/cr-check.c:1740): timer_cr_ids feature is not supported
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     true,
			wantReason: "missing optional CRIU features: pagemap_scan_guard_pages, timer_cr_ids",
		},
		{
			name: "single optional feature missing: pagemap_scan_guard_pages",
			raw: `Warn  (criu/kerndat.c:1904): ioctl(PAGEMAP_SCAN) doesn't support PAGE_IS_GUARD flag. CRIU dump will fail if dumped processes use madvise(MADV_GUARD_INSTALL). Please, consider updating your kernel.
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     true,
			wantReason: "missing optional CRIU features: pagemap_scan_guard_pages",
		},
		{
			name: "single optional feature missing: timer_cr_ids",
			raw: `Warn: feature timer_cr_ids is not supported
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     true,
			wantReason: "missing optional CRIU features: timer_cr_ids",
		},
		{
			name: "required feature missing: uffd",
			raw: `Warn  (criu/cr-check.c:1232): UFFD is not supported
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all missing required features: uffd",
		},
		{
			name: "required feature missing: cgroupns",
			raw: `Warn  (criu/cr-check.c:1040): cgroupns not supported. This is not fatal.
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all missing required features: cgroupns",
		},
		{
			name: "mixed optional and required features missing: rejects readiness and names required",
			raw: `Warn  (criu/kerndat.c:1904): ioctl(PAGEMAP_SCAN) doesn't support PAGE_IS_GUARD flag. CRIU dump will fail if dumped processes use madvise(MADV_GUARD_INSTALL). Please, consider updating your kernel.
Warn: timer_cr_ids feature is not supported
Warn  (criu/cr-check.c:1232): UFFD is not supported
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all missing required features: uffd",
		},
		{
			name: "unrecognized warning when some features missing: fails closed",
			raw: `Warn: unknown_kernel_capability_xyz failed
Looks good but some kernel features are missing
which, depending on your process tree, may cause
dump or restore failure.
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all missing required features: unknown_kernel_capability_xyz failed",
		},
		{
			name: "core check failure: does not look good",
			raw: `Error (criu/crtools.c:260): Could not initialize kernel features detection.
Does not look good.
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all failed",
		},
		{
			name: "core check failure: lacks CONFIG_CHECKPOINT_RESTORE",
			raw: `Error: kernel lacks CONFIG_CHECKPOINT_RESTORE
`,
			checkMode:  "all",
			wantOK:     false,
			wantReason: "criu check --all failed",
		},
		{
			name: "checkMode core with Looks good passes cleanly",
			raw: `Looks good.
`,
			checkMode:  "core",
			wantOK:     true,
			wantReason: "",
		},
		{
			name: "checkMode core with failure fails",
			raw: `Does not look good.
`,
			checkMode:  "core",
			wantOK:     false,
			wantReason: "criu check failed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotOK, gotReason := parseCRIUCheckOutput(tc.raw, tc.checkMode)
			if gotOK != tc.wantOK {
				t.Errorf("parseCRIUCheckOutput() ok = %v, want %v", gotOK, tc.wantOK)
			}
			if gotReason != tc.wantReason {
				t.Errorf("parseCRIUCheckOutput() reason = %q, want %q", gotReason, tc.wantReason)
			}
		})
	}
}

func TestVerifyCRIU_ExecAndModes(t *testing.T) {
	ctx := context.Background()

	t.Run("default check mode invokes check --all", func(t *testing.T) {
		t.Setenv("PODMIG_CRIU_CHECK", "")
		var capturedArgs []string
		execHost := func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
			capturedArgs = args
			return "Looks good.\n", nil
		}

		ok, reason, raw := verifyCRIU(ctx, execHost)
		if !ok || reason != "" || !strings.Contains(raw, "Looks good") {
			t.Fatalf("unexpected verifyCRIU result: ok=%v reason=%q raw=%q", ok, reason, raw)
		}
		if want := []string{"check", "--all"}; !reflect.DeepEqual(capturedArgs, want) {
			t.Fatalf("expected args %v, got %v", want, capturedArgs)
		}
	})

	t.Run("PODMIG_CRIU_CHECK=core invokes check without --all", func(t *testing.T) {
		t.Setenv("PODMIG_CRIU_CHECK", "core")
		var capturedArgs []string
		execHost := func(_ context.Context, _ []string, _ string, args ...string) (string, error) {
			capturedArgs = args
			return "Looks good.\n", nil
		}

		ok, reason, _ := verifyCRIU(ctx, execHost)
		if !ok || reason != "" {
			t.Fatalf("unexpected verifyCRIU result: ok=%v reason=%q", ok, reason)
		}
		if want := []string{"check"}; !reflect.DeepEqual(capturedArgs, want) {
			t.Fatalf("expected args %v, got %v", want, capturedArgs)
		}
	})

	t.Run("execHost error with empty raw returns error failure", func(t *testing.T) {
		t.Setenv("PODMIG_CRIU_CHECK", "")
		execHost := func(_ context.Context, _ []string, _ string, _ ...string) (string, error) {
			return "", errors.New("binary not executable")
		}

		ok, reason, _ := verifyCRIU(ctx, execHost)
		if ok {
			t.Fatal("expected ok=false on exec error")
		}
		if !strings.Contains(reason, "binary not executable") {
			t.Fatalf("expected error reason, got %q", reason)
		}
	})
}
