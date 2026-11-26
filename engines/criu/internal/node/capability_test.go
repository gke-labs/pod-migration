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
