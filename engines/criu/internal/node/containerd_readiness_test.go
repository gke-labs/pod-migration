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
	"io/fs"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeFileInfo struct {
	name string
	mode fs.FileMode
}

func (f *fakeFileInfo) Name() string       { return f.name }
func (f *fakeFileInfo) Size() int64        { return 1024 }
func (f *fakeFileInfo) Mode() fs.FileMode  { return f.mode }
func (f *fakeFileInfo) ModTime() time.Time { return time.Now() }
func (f *fakeFileInfo) IsDir() bool        { return f.mode.IsDir() }
func (f *fakeFileInfo) Sys() any           { return nil }

func TestParseCRIUVersionCode(t *testing.T) {
	tests := []struct {
		input   string
		want    int
		wantErr bool
	}{
		{input: "4.0", want: 40000},
		{input: "Version: 4.0", want: 40000},
		{input: "Version: 4.0-2", want: 40000},
		{input: "Version: 3.19", want: 31900},
		{input: "3.16.0", want: 31600},
		{input: "3.16.1", want: 31601},
		{input: "3.15.2", want: 31502},
		{input: "criu 4.0.0", want: 40000},
		{input: "invalid", wantErr: true},
		{input: "", wantErr: true},
	}

	for _, tc := range tests {
		got, err := parseCRIUVersionCode(tc.input)
		if tc.wantErr {
			if err == nil {
				t.Errorf("parseCRIUVersionCode(%q) expected error, got %d", tc.input, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseCRIUVersionCode(%q) unexpected error: %v", tc.input, err)
			continue
		}
		if got != tc.want {
			t.Errorf("parseCRIUVersionCode(%q) = %d, want %d", tc.input, got, tc.want)
		}
	}
}

func TestParseContainerdConfig(t *testing.T) {
	t.Run("default empty config", func(t *testing.T) {
		enableCRIU, shimPath, err := parseContainerdConfig(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enableCRIU != nil {
			t.Errorf("expected enableCRIU nil, got %v", *enableCRIU)
		}
		if shimPath != "" {
			t.Errorf("expected empty shimPath, got %q", shimPath)
		}
	})

	t.Run("enable_criu = false inside cri v1 plugin", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = false
`)
		enableCRIU, _, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enableCRIU == nil || *enableCRIU != false {
			t.Fatalf("expected enableCRIU == false, got %v", enableCRIU)
		}
	})

	t.Run("enable_criu = false inside cri v2 runtime plugin", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.cri.v1.runtime"]
  enable_criu = false
`)
		enableCRIU, _, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enableCRIU == nil || *enableCRIU != false {
			t.Fatalf("expected enableCRIU == false, got %v", enableCRIU)
		}
	})

	t.Run("enable_criu = true inside cri plugin", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = true
`)
		enableCRIU, _, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enableCRIU == nil || *enableCRIU != true {
			t.Fatalf("expected enableCRIU == true, got %v", enableCRIU)
		}
	})

	t.Run("conflicting enable_criu between cri plugins returns error", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = true

[plugins."io.containerd.cri.v1.runtime"]
  enable_criu = false
`)
		_, _, err := parseContainerdConfig(cfg)
		if err == nil {
			t.Fatal("expected error on conflicting enable_criu settings across cri plugins")
		}
		if !strings.Contains(err.Error(), "conflicting enable_criu") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("unrelated plugin with cri in name does not affect CRIU detection", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.crimson.v1"]
  enable_criu = false

[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = true
`)
		enableCRIU, _, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if enableCRIU == nil || *enableCRIU != true {
			t.Fatalf("expected enableCRIU == true, got %v", enableCRIU)
		}
	})

	t.Run("shim manager env path extracted", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.shim.v1.manager"]
  env = ["FOO=BAR", "PATH=/opt/containerd/bin:/usr/local/sbin", "BAZ=QUX"]
`)
		_, shimPath, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if shimPath != "/opt/containerd/bin:/usr/local/sbin" {
			t.Fatalf("unexpected shimPath: %q", shimPath)
		}
	})

	t.Run("unrelated plugin env without shim manager produces empty path deterministically", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.other.plugin"]
  env = ["PATH=/should/not/match"]
`)
		_, shimPath, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if shimPath != "" {
			t.Fatalf("expected empty shimPath when shim.v1.manager is absent, got %q", shimPath)
		}
	})

	t.Run("shim manager with no env ignores unrelated plugins with env", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.other.plugin1"]
  env = ["PATH=/bad/path/1"]

[plugins."io.containerd.shim.v1.manager"]

[plugins."io.containerd.other.plugin2"]
  env = ["PATH=/bad/path/2"]
`)
		_, shimPath, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if shimPath != "" {
			t.Fatalf("expected empty shimPath when shim.v1.manager has no env, got %q", shimPath)
		}
	})

	t.Run("unrelated plugin env does not override shim manager", func(t *testing.T) {
		cfg := []byte(`
version = 2
[plugins."io.containerd.other.plugin"]
  env = ["PATH=/should/not/match"]

[plugins."io.containerd.shim.v1.manager"]
  env = ["PATH=/correct/shim/path"]
`)
		_, shimPath, err := parseContainerdConfig(cfg)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if shimPath != "/correct/shim/path" {
			t.Fatalf("unexpected shimPath: %q", shimPath)
		}
	})

	t.Run("malformed toml returns error", func(t *testing.T) {
		cfg := []byte(`this is not toml [ [ invalid`)
		_, _, err := parseContainerdConfig(cfg)
		if err == nil {
			t.Fatal("expected error on malformed toml")
		}
	})
}

func TestReadContainerdConfig_RealRoot(t *testing.T) {
	fakeRoot := t.TempDir()
	defaultConfig := []byte("version = 2\ndefault = true\n")
	customConfig := []byte("version = 2\ncustom = true\n")

	if err := os.MkdirAll(filepath.Join(fakeRoot, "etc", "containerd"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeRoot, "etc", "containerd", "config.toml"), defaultConfig, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(fakeRoot, "etc", "containerd", "custom.toml"), customConfig, 0o644); err != nil {
		t.Fatal(err)
	}

	t.Run("default config path when no args", func(t *testing.T) {
		data, err := readContainerdConfig(fakeRoot, 1001, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(data, defaultConfig) {
			t.Fatalf("got %q, want %q", string(data), string(defaultConfig))
		}
	})

	cmdlineTests := []struct {
		name string
		args []string
	}{
		{name: "-c custom", args: []string{"containerd", "-c", "/etc/containerd/custom.toml"}},
		{name: "-c=custom", args: []string{"containerd", "-c=/etc/containerd/custom.toml"}},
		{name: "-config custom", args: []string{"containerd", "-config", "/etc/containerd/custom.toml"}},
		{name: "-config=custom", args: []string{"containerd", "-config=/etc/containerd/custom.toml"}},
		{name: "--config custom", args: []string{"containerd", "--config", "/etc/containerd/custom.toml"}},
		{name: "--config=custom", args: []string{"containerd", "--config=/etc/containerd/custom.toml"}},
	}
	for _, tc := range cmdlineTests {
		t.Run(tc.name, func(t *testing.T) {
			data, err := readContainerdConfig(fakeRoot, 1001, func(int) ([]string, error) {
				return tc.args, nil
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(data, customConfig) {
				t.Fatalf("got %q, want %q", string(data), string(customConfig))
			}
		})
	}

	t.Run("cmdline error fails closed", func(t *testing.T) {
		_, err := readContainerdConfig(fakeRoot, 1001, func(int) ([]string, error) {
			return nil, errors.New("denied by proc")
		})
		if err == nil {
			t.Fatal("expected error on cmdline read error")
		}
		if !strings.Contains(err.Error(), "denied by proc") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("absent default config returns nil, nil", func(t *testing.T) {
		emptyRoot := t.TempDir()
		data, err := readContainerdConfig(emptyRoot, 1001, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if data != nil {
			t.Fatalf("expected nil data for absent config, got %q", string(data))
		}
	})

	t.Run("unreadable config file fails closed", func(t *testing.T) {
		unreadableDir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(unreadableDir, "etc", "containerd"), 0o755); err != nil {
			t.Fatal(err)
		}
		unreadableFile := filepath.Join(unreadableDir, "etc", "containerd", "config.toml")
		if err := os.WriteFile(unreadableFile, []byte("secret"), 0o000); err != nil {
			t.Fatal(err)
		}
		_, err := readContainerdConfig(unreadableDir, 1001, nil)
		if err == nil {
			t.Fatal("expected error for unreadable config file")
		}
	})

	t.Run("symlink escape outside root fails closed without reading target", func(t *testing.T) {
		escapeRoot := t.TempDir()
		outsideDir := t.TempDir()
		outsideConfig := filepath.Join(outsideDir, "outside_config.toml")
		if err := os.WriteFile(outsideConfig, []byte("forbidden-outside-data"), 0o644); err != nil {
			t.Fatal(err)
		}

		if err := os.MkdirAll(filepath.Join(escapeRoot, "etc", "containerd"), 0o755); err != nil {
			t.Fatal(err)
		}
		// Symlink pointing to outsideConfig
		if err := os.Symlink(outsideConfig, filepath.Join(escapeRoot, "etc", "containerd", "config.toml")); err != nil {
			t.Fatal(err)
		}

		data, err := readContainerdConfig(escapeRoot, 1001, nil)
		if string(data) == "forbidden-outside-data" {
			t.Fatal("readContainerdConfig read outside file via symlink escape!")
		}
		_ = err
	})
}

func TestResolveCRIUPath(t *testing.T) {
	files := map[string]fs.FileMode{
		"/usr/local/sbin/criu": 0o755,
		"/opt/bin/criu":        0o755,
		"/nonexec/criu":        0o644,
		"relative/bin/criu":    0o755,
		"criu":                 0o755,
	}
	statFn := func(path string) (os.FileInfo, error) {
		mode, ok := files[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return &fakeFileInfo{name: filepath.Base(path), mode: mode}, nil
	}

	t.Run("nil statFn returns error", func(t *testing.T) {
		_, err := resolveCRIUPath("", "/usr/local/sbin", nil)
		if err == nil {
			t.Fatal("expected error for nil statFn")
		}
	})

	t.Run("daemon PATH lookup when shimPath is empty", func(t *testing.T) {
		got, err := resolveCRIUPath("", "/bin:/usr/bin:/usr/local/sbin", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "/usr/local/sbin/criu" {
			t.Fatalf("expected /usr/local/sbin/criu, got %q", got)
		}
	})

	t.Run("shimPath configured searches only shimPath", func(t *testing.T) {
		got, err := resolveCRIUPath("/opt/bin", "/usr/local/sbin", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "/opt/bin/criu" {
			t.Fatalf("expected /opt/bin/criu, got %q", got)
		}
	})

	t.Run("shimPath missing criu does not fall back to daemon PATH", func(t *testing.T) {
		got, err := resolveCRIUPath("/missing/dir", "/usr/local/sbin", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Fatalf("expected empty resolution, got %q", got)
		}
	})

	t.Run("relative dirs in shimPath or containerd PATH are skipped", func(t *testing.T) {
		got, err := resolveCRIUPath("relative/bin:.", "", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Fatalf("expected empty resolution, got %q", got)
		}

		gotPath, err := resolveCRIUPath("", "relative/bin::.", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if gotPath != "" {
			t.Fatalf("expected empty resolution, got %q", gotPath)
		}
	})

	t.Run("non-executable file is rejected", func(t *testing.T) {
		got, err := resolveCRIUPath("/nonexec", "", statFn)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "" {
			t.Fatalf("expected empty resolution for non-executable file, got %q", got)
		}
	})

	t.Run("path lookup requires proc root to succeed", func(t *testing.T) {
		fakeRoot := t.TempDir()
		isolatedDir := filepath.Join(fakeRoot, "isolated", "path", "bin")
		if err := os.MkdirAll(isolatedDir, 0o755); err != nil {
			t.Fatal(err)
		}
		realCriu := filepath.Join(isolatedDir, "criu")
		if err := os.WriteFile(realCriu, []byte("#!/bin/sh\n"), 0o755); err != nil {
			t.Fatal(err)
		}

		// /isolated/path/bin/criu does NOT exist on the host filesystem
		if _, err := os.Stat("/isolated/path/bin/criu"); err == nil {
			t.Skip("host collision: /isolated/path/bin/criu exists on host")
		}

		// 1. PATH lookup with StatFileInRoot succeeds because it resolves inside fakeRoot
		statFnRoot := func(path string) (os.FileInfo, error) {
			return StatFileInRoot(fakeRoot, path)
		}
		got, err := resolveCRIUPath("", "/isolated/path/bin", statFnRoot)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != "/isolated/path/bin/criu" {
			t.Fatalf("resolveCRIUPath with proc root stat = %q, want /isolated/path/bin/criu", got)
		}

		// 2. PATH lookup directly on host fs fails (proving that host fs lookup cannot find it)
		gotHost, errHost := resolveCRIUPath("", "/isolated/path/bin", os.Stat)
		if errHost != nil {
			t.Fatalf("unexpected error: %v", errHost)
		}
		if gotHost != "" {
			t.Fatalf("expected host PATH lookup without proc root to fail, got %q", gotHost)
		}
	})
}

func TestStatFileInRoot_SymlinkResolution(t *testing.T) {
	root := t.TempDir()

	optBin := filepath.Join(root, "opt", "criu", "bin")
	if err := os.MkdirAll(optBin, 0o755); err != nil {
		t.Fatal(err)
	}
	realCriu := filepath.Join(optBin, "criu")
	if err := os.WriteFile(realCriu, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	usrBin := filepath.Join(root, "usr", "bin")
	if err := os.MkdirAll(usrBin, 0o755); err != nil {
		t.Fatal(err)
	}
	// Absolute symlink inside root pointing to /opt/criu/bin/criu
	if err := os.Symlink("/opt/criu/bin/criu", filepath.Join(usrBin, "criu")); err != nil {
		t.Fatal(err)
	}

	usrLocalBin := filepath.Join(root, "usr", "local", "bin")
	if err := os.MkdirAll(usrLocalBin, 0o755); err != nil {
		t.Fatal(err)
	}
	// Relative symlink: /usr/local/bin/criu -> ../../bin/criu
	if err := os.Symlink("../../bin/criu", filepath.Join(usrLocalBin, "criu")); err != nil {
		t.Fatal(err)
	}

	loopDir := filepath.Join(root, "loop")
	if err := os.MkdirAll(loopDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/loop/b", filepath.Join(loopDir, "a")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("/loop/a", filepath.Join(loopDir, "b")); err != nil {
		t.Fatal(err)
	}

	t.Run("resolves absolute symlink within root", func(t *testing.T) {
		fi, err := StatFileInRoot(root, "/usr/bin/criu")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fi.Mode()&0o111 == 0 {
			t.Fatal("expected executable mode")
		}
	})

	t.Run("resolves relative symlink within root", func(t *testing.T) {
		fi, err := StatFileInRoot(root, "/usr/local/bin/criu")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fi.Mode()&0o111 == 0 {
			t.Fatal("expected executable mode")
		}
	})

	t.Run("clean root handles trailing slashes and clamped ..", func(t *testing.T) {
		rootWithSlash := root + "/"
		fi, err := StatFileInRoot(rootWithSlash, "/usr/local/bin/criu")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fi.Mode()&0o111 == 0 {
			t.Fatal("expected executable mode")
		}

		// Symlink attempting to escape via /../..
		escapeLink := filepath.Join(root, "escape_link")
		if err := os.Symlink("/../../../../opt/criu/bin/criu", escapeLink); err != nil {
			t.Fatal(err)
		}
		fiEsc, err := StatFileInRoot(rootWithSlash, "/escape_link")
		if err != nil {
			t.Fatalf("unexpected error resolving clamped link: %v", err)
		}
		if fiEsc.Mode()&0o111 == 0 {
			t.Fatal("expected executable mode on clamped link")
		}
	})

	t.Run("symlink escape target outside root fails to resolve", func(t *testing.T) {
		outsideDir := t.TempDir()
		canary := filepath.Join(outsideDir, "secret_outside.txt")
		if err := os.WriteFile(canary, []byte("super-secret"), 0o644); err != nil {
			t.Fatal(err)
		}

		// Absolute symlink pointing directly to canary on the host filesystem
		absEscape := filepath.Join(root, "abs_escape")
		if err := os.Symlink(canary, absEscape); err != nil {
			t.Fatal(err)
		}
		// StatFileInRoot must re-anchor canary to root (root + canary), which does not exist!
		// It must NOT read outsideDir/secret_outside.txt!
		if _, err := StatFileInRoot(root, "/abs_escape"); err == nil {
			t.Fatal("expected StatFileInRoot to fail for absolute symlink targeting file outside root")
		}
		if _, err := ReadFileInRoot(root, "/abs_escape"); err == nil {
			t.Fatal("expected ReadFileInRoot to fail for absolute symlink targeting file outside root")
		}

		// Relative symlink with enough ".." to escape root
		relEscape := filepath.Join(root, "rel_escape")
		relTarget := "../../../../../../../../../.." + canary
		if err := os.Symlink(relTarget, relEscape); err != nil {
			t.Fatal(err)
		}
		// Clamping .. to root means this can never escape to outsideDir!
		if _, err := StatFileInRoot(root, "/rel_escape"); err == nil {
			t.Fatal("expected StatFileInRoot to fail for relative symlink escaping root")
		}
		if _, err := ReadFileInRoot(root, "/rel_escape"); err == nil {
			t.Fatal("expected ReadFileInRoot to fail for relative symlink escaping root")
		}
	})

	t.Run("resolvePathInRoot clamps .. at root boundary", func(t *testing.T) {
		clampedFile := filepath.Join(root, "clamped_file")
		if err := os.WriteFile(clampedFile, []byte("ok"), 0o644); err != nil {
			t.Fatal(err)
		}
		resolved, err := resolvePathInRoot(root, "/../../../../clamped_file")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := clampedFile
		if resolved != want {
			t.Fatalf("resolvePathInRoot = %q, want %q", resolved, want)
		}
	})

	t.Run("detects symlink loop", func(t *testing.T) {
		_, err := StatFileInRoot(root, "/loop/a")
		if err == nil {
			t.Fatal("expected error on symlink loop")
		}
		if !strings.Contains(err.Error(), "too many levels of symbolic links") {
			t.Fatalf("unexpected error message: %v", err)
		}
	})

	t.Run("non-existent file returns error", func(t *testing.T) {
		_, err := StatFileInRoot(root, "/missing/file")
		if err == nil {
			t.Fatal("expected error on missing file")
		}
	})
}

func TestReadFileInRoot_Direct(t *testing.T) {
	root := t.TempDir()
	content := []byte("configuration data for test")
	target := filepath.Join(root, "etc", "test.conf")
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, content, 0o644); err != nil {
		t.Fatal(err)
	}

	// Symlink to target
	sym := filepath.Join(root, "link.conf")
	if err := os.Symlink("/etc/test.conf", sym); err != nil {
		t.Fatal(err)
	}

	data, err := ReadFileInRoot(root, "/link.conf")
	if err != nil {
		t.Fatalf("ReadFileInRoot failed: %v", err)
	}
	if !bytes.Equal(data, content) {
		t.Fatalf("got %q, want %q", string(data), string(content))
	}
}

// fakeContainerdEnv sets up an in-memory containerd environment for testing.
type fakeContainerdEnv struct {
	pid            int
	environ        []string
	configData     []byte
	files          map[string]fs.FileMode
	criuVersionOut string
}

func (f *fakeContainerdEnv) toInspector() *ContainerdReadinessInspector {
	return &ContainerdReadinessInspector{
		GetPID: func(sockPath string) (int, error) {
			if f.pid <= 0 {
				return 0, errors.New("containerd process not found")
			}
			return f.pid, nil
		},
		ReadEnviron: func(pid int) ([]string, error) {
			if pid != f.pid {
				return nil, errors.New("no such process")
			}
			return f.environ, nil
		},
		ReadCmdline: func(pid int) ([]string, error) {
			return []string{"containerd"}, nil
		},
		ReadConfig: func(pid int) ([]byte, error) {
			return f.configData, nil
		},
		StatContainerdFile: func(pid int, path string) (os.FileInfo, error) {
			mode, ok := f.files[path]
			if !ok {
				return nil, os.ErrNotExist
			}
			return &fakeFileInfo{name: filepath.Base(path), mode: mode}, nil
		},
		GetCRIUVersion: func(ctx context.Context, criuPath string) (string, error) {
			if f.criuVersionOut == "" {
				return "", errors.New("criu exec failed")
			}
			return f.criuVersionOut, nil
		},
	}
}

func TestCheckContainerdReadiness_FakeContainerd(t *testing.T) {
	ctx := context.Background()

	t.Run("clean ready environment", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0\n",
		}
		ok, reason, remediation, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatalf("expected ready=true, got ok=false (reason=%q, remediation=%q)", reason, remediation)
		}
	})

	t.Run("criu missing from containerd PATH", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/bin:/bin",
			},
			files:          map[string]fs.FileMode{},
			criuVersionOut: "Version: 4.0",
		}
		ok, reason, remediation, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false when criu missing from PATH")
		}
		if reason != ReasonCRIUNotFoundOnContainerdPath {
			t.Fatalf("expected reason %q, got %q", ReasonCRIUNotFoundOnContainerdPath, reason)
		}
		if remediation != RemediationInstallCRIUOnContainerdPath {
			t.Fatalf("expected remediation %q, got %q", RemediationInstallCRIUOnContainerdPath, remediation)
		}
	})

	t.Run("COS repro from #130: criu installed only in /home/kubernetes/bin not on containerd PATH", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
			},
			files: map[string]fs.FileMode{
				"/home/kubernetes/bin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		ok, reason, _, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false for COS repro with criu outside containerd PATH")
		}
		if reason != ReasonCRIUNotFoundOnContainerdPath {
			t.Fatalf("expected reason %q, got %q", ReasonCRIUNotFoundOnContainerdPath, reason)
		}
	})

	t.Run("enable_criu = false in containerd config", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin:/usr/bin",
			},
			configData: []byte(`
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = false
`),
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		ok, reason, remediation, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false when enable_criu = false")
		}
		if reason != ReasonCRIUDisabledByConfig {
			t.Fatalf("expected reason %q, got %q", ReasonCRIUDisabledByConfig, reason)
		}
		if remediation != RemediationEnableCRIUConfig {
			t.Fatalf("expected remediation %q, got %q", RemediationEnableCRIUConfig, remediation)
		}
	})

	t.Run("shim PATH configured with CRIU", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/bin",
			},
			configData: []byte(`
version = 2
[plugins."io.containerd.shim.v1.manager"]
  env = ["PATH=/opt/containerd/bin"]
`),
			files: map[string]fs.FileMode{
				"/opt/containerd/bin/criu": 0o755,
				"/usr/bin/criu":            0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		ok, reason, _, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !ok {
			t.Fatalf("expected ok=true with shim PATH, got reason=%q", reason)
		}
	})

	t.Run("shim PATH configured without CRIU fails even if daemon PATH has CRIU", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			configData: []byte(`
version = 2
[plugins."io.containerd.shim.v1.manager"]
  env = ["PATH=/opt/containerd/bin"]
`),
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755, // on daemon PATH, but not in shim PATH!
			},
			criuVersionOut: "Version: 4.0",
		}
		ok, reason, _, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false when shim PATH lacks CRIU")
		}
		if reason != ReasonCRIUNotFoundOnContainerdPath {
			t.Fatalf("expected reason %q, got %q", ReasonCRIUNotFoundOnContainerdPath, reason)
		}
	})

	t.Run("outdated CRIU version rejected", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 3.15.2\n",
		}
		ok, reason, remediation, err := CheckContainerdReadiness(ctx, env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false for outdated CRIU version")
		}
		if !strings.Contains(reason, "older than minimum supported version 3.16.0") {
			t.Fatalf("unexpected reason: %q", reason)
		}
		if remediation != RemediationUpgradeCRIUVersion {
			t.Fatalf("expected remediation %q, got %q", RemediationUpgradeCRIUVersion, remediation)
		}
	})

	t.Run("nil StatContainerdFile hook returns error", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files:          map[string]fs.FileMode{},
			criuVersionOut: "Version: 4.0",
		}
		insp := env.toInspector()
		insp.StatContainerdFile = nil
		_, _, _, err := CheckContainerdReadiness(ctx, insp, "/run/containerd/containerd.sock")
		if err == nil {
			t.Fatal("expected error when StatContainerdFile is nil")
		}
	})
}

func TestParseEnviron_ExactEntries(t *testing.T) {
	raw := []byte("PATH=/usr/bin:/bin\x00FOO=BAR=BAZ\x00EMPTY=\x00EQUALS==\x00TRAILING\x00\x00")
	got := parseEnviron(raw)
	want := []string{
		"PATH=/usr/bin:/bin",
		"FOO=BAR=BAZ",
		"EMPTY=",
		"EQUALS==",
		"TRAILING",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("parseEnviron() = %#v, want %#v", got, want)
	}

	rawCmd := []byte("containerd\x00--config\x00/etc/containerd/custom.toml\x00\x00")
	gotCmd := parseCmdline(rawCmd)
	wantCmd := []string{"containerd", "--config", "/etc/containerd/custom.toml"}
	if !reflect.DeepEqual(gotCmd, wantCmd) {
		t.Fatalf("parseCmdline() = %#v, want %#v", gotCmd, wantCmd)
	}
}

func TestGetCRIUVersion_NoLocalExecFallback(t *testing.T) {
	tempDir := t.TempDir()
	dummyCRIU := filepath.Join(tempDir, "criu")
	if err := os.WriteFile(dummyCRIU, []byte("#!/bin/sh\necho 'Version: 4.0'\n"), 0o755); err != nil {
		t.Fatal(err)
	}

	insp := NewDefaultContainerdInspector(func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		return "", errors.New("host exec denied")
	})

	_, err := insp.GetCRIUVersion(context.Background(), dummyCRIU)
	if err == nil {
		t.Fatal("expected error when execHost fails")
	}
	if !strings.Contains(err.Error(), "host exec denied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRealProcInspection(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("skipping linux /proc tests on non-linux OS")
	}

	tempDir := t.TempDir()
	sockPath := filepath.Join(tempDir, "test.sock")
	l, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen unix: %v", err)
	}
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err == nil {
			defer conn.Close()
			buf := make([]byte, 1)
			_, _ = conn.Read(buf)
		}
	}()

	pid, err := getSocketPeerPID(sockPath)
	if err != nil {
		t.Fatalf("getSocketPeerPID: %v", err)
	}
	if pid != os.Getpid() {
		t.Fatalf("expected pid %d, got %d", os.Getpid(), pid)
	}

	insp := NewDefaultContainerdInspector(nil)
	env, err := insp.ReadEnviron(os.Getpid())
	if err != nil {
		t.Fatalf("ReadEnviron(%d): %v", os.Getpid(), err)
	}
	if len(env) == 0 {
		t.Fatal("expected non-empty environ")
	}

	// Verify exact PATH entry matches os.Getenv("PATH")
	var foundPath string
	for _, e := range env {
		if p, ok := strings.CutPrefix(e, "PATH="); ok {
			foundPath = p
			break
		}
	}
	if foundPath == "" || foundPath != os.Getenv("PATH") {
		t.Fatalf("insp.ReadEnviron did not match os.Getenv(PATH): got %q, want %q", foundPath, os.Getenv("PATH"))
	}

	cmdline, err := insp.ReadCmdline(os.Getpid())
	if err != nil {
		t.Fatalf("ReadCmdline(%d): %v", os.Getpid(), err)
	}
	if len(cmdline) == 0 {
		t.Fatal("expected non-empty cmdline")
	}

	fi, err := insp.StatContainerdFile(os.Getpid(), "/proc/self/exe")
	if err != nil {
		t.Fatalf("StatContainerdFile(/proc/self/exe): %v", err)
	}
	if fi.Mode().IsRegular() == false && fi.Mode()&fs.ModeSymlink == 0 {
		t.Fatalf("unexpected file mode: %v", fi.Mode())
	}

	// Verify ReadFileInProcRoot on real /proc
	cmdlineRaw, err := ReadFileInProcRoot(os.Getpid(), "/proc/self/cmdline")
	if err != nil {
		t.Fatalf("ReadFileInProcRoot failed: %v", err)
	}
	if len(cmdlineRaw) == 0 {
		t.Fatal("expected non-empty cmdline from ReadFileInProcRoot")
	}
}

func fakeReadyInspector() *ContainerdReadinessInspector {
	return &ContainerdReadinessInspector{
		GetPID: func(sockPath string) (int, error) {
			return 1001, nil
		},
		ReadEnviron: func(pid int) ([]string, error) {
			return []string{"PATH=/usr/local/sbin:/usr/bin"}, nil
		},
		ReadCmdline: func(pid int) ([]string, error) {
			return []string{"containerd"}, nil
		},
		ReadConfig: func(pid int) ([]byte, error) {
			return nil, nil
		},
		StatContainerdFile: func(pid int, path string) (os.FileInfo, error) {
			if path == "/usr/local/sbin/criu" {
				return &fakeFileInfo{name: "criu", mode: 0o755}, nil
			}
			return nil, os.ErrNotExist
		},
		GetCRIUVersion: func(ctx context.Context, criuPath string) (string, error) {
			return "Version: 4.0\n", nil
		},
	}
}

func TestNode_Bootstrap_WithContainerdReadiness(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	setupNode := func(env *fakeContainerdEnv) (*Node, *corev1.Node, *int32) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-node",
			},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{
					ContainerRuntimeVersion: "containerd://2.2.7",
				},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

		var installCalls int32
		nd := &Node{
			Client:     c,
			NodeName:   "test-node",
			SocketPath: "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) {
				return 12345, nil
			},
			installBundleFn: func() (string, error) {
				atomic.AddInt32(&installCalls, 1)
				return "4.0", nil
			},
			Inspector: env.toInspector(),
			execHostFn: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
				return "Looks good.", nil
			},
		}
		return nd, nodeObj, &installCalls
	}

	t.Run("bootstrap marks unready with reason when enable_criu = false and stays quiet until restart", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			configData: []byte(`
version = 2
[plugins."io.containerd.grpc.v1.cri"]
  enable_criu = false
`),
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		nd, _, installCalls := setupNode(env)

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap error: %v", err)
		}

		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false")
		}

		var cur corev1.Node
		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Errorf("expected criu-ready: false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		if cur.Annotations[NodeAnnotationCRIUReason] != ReasonCRIUDisabledByConfig {
			t.Errorf("expected reason %q, got %q", ReasonCRIUDisabledByConfig, cur.Annotations[NodeAnnotationCRIUReason])
		}
		if cur.Annotations[NodeAnnotationCRIURemediation] != RemediationEnableCRIUConfig {
			t.Errorf("expected remediation %q, got %q", RemediationEnableCRIUConfig, cur.Annotations[NodeAnnotationCRIURemediation])
		}

		// Subsequent ProbeReadiness on unchanged socket inode 12345 stays quiet (commits inode)
		reProbed, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness error: %v", err)
		}
		if reProbed {
			t.Fatal("expected ProbeReadiness to stay unready")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls == 1, got %d", atomic.LoadInt32(installCalls))
		}
	})

	t.Run("bootstrap marks ready: true when containerd readiness passes", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		nd, _, _ := setupNode(env)

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap error: %v", err)
		}

		if !nd.IsReady() {
			t.Fatal("expected nd.IsReady() == true")
		}

		var cur corev1.Node
		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
			t.Errorf("expected criu-ready: true, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		if cur.Annotations[NodeAnnotationCRIUReason] != "" {
			t.Errorf("expected empty reason, got %q", cur.Annotations[NodeAnnotationCRIUReason])
		}
	})

	t.Run("transient containerd readiness error leaves socket inode uncommitted so ProbeReadiness retries", func(t *testing.T) {
		var failDial atomic.Bool
		failDial.Store(true)

		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		insp := env.toInspector()
		insp.GetPID = func(sockPath string) (int, error) {
			if failDial.Load() {
				return 0, errors.New("socket temporarily disconnected during containerd restart")
			}
			return 1001, nil
		}

		nd, _, _ := setupNode(env)
		nd.Inspector = insp

		// Bootstrap hits transient inspection error: returns error and annotates node unready
		err := nd.Bootstrap(ctx)
		if err == nil {
			t.Fatal("expected Bootstrap error on transient lookup failure")
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false")
		}

		var cur corev1.Node
		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Errorf("expected criu-ready: false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		if !strings.Contains(cur.Annotations[NodeAnnotationCRIUReason], "containerd readiness probe error") {
			t.Errorf("expected reason with containerd readiness probe error, got %q", cur.Annotations[NodeAnnotationCRIUReason])
		}

		// Now transient dial failure clears without containerd socket inode changing
		failDial.Store(false)

		// ProbeReadiness should re-probe because inode was not committed!
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness error: %v", err)
		}
		if !ready || !nd.IsReady() {
			t.Fatal("expected ProbeReadiness to restore node readiness on transient resolution")
		}

		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
			t.Errorf("expected criu-ready: true, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
	})

	t.Run("transient dial retry is cheap: retries readiness check without bundle reinstall or criu check", func(t *testing.T) {
		var failDial atomic.Bool
		failDial.Store(true)

		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		insp := env.toInspector()
		insp.GetPID = func(sockPath string) (int, error) {
			if failDial.Load() {
				return 0, errors.New("socket temporarily disconnected during containerd restart")
			}
			return 1001, nil
		}

		nd, _, installCalls := setupNode(env)
		var checkCalls int32
		nd.execHostFn = func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			atomic.AddInt32(&checkCalls, 1)
			return "Looks good.", nil
		}
		nd.Inspector = insp

		// Tick 1: Bootstrap hits transient dial error
		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected Bootstrap error on dial failure")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls == 1, got %d", atomic.LoadInt32(installCalls))
		}
		if atomic.LoadInt32(&checkCalls) != 1 {
			t.Fatalf("expected checkCalls == 1, got %d", atomic.LoadInt32(&checkCalls))
		}

		// Ticks 2..7: 6 ProbeReadiness ticks fail dial without re-installing bundle or re-running criu check
		for tick := 2; tick <= 7; tick++ {
			ready, err := nd.ProbeReadiness(ctx)
			if ready || err == nil {
				t.Fatalf("tick %d: expected ProbeReadiness to fail with dial error, got ready=%v err=%v", tick, ready, err)
			}
			if atomic.LoadInt32(installCalls) != 1 {
				t.Fatalf("tick %d: expected installCalls to remain 1, got %d", tick, atomic.LoadInt32(installCalls))
			}
			if atomic.LoadInt32(&checkCalls) != 1 {
				t.Fatalf("tick %d: expected checkCalls to remain 1, got %d", tick, atomic.LoadInt32(&checkCalls))
			}
		}

		// Tick 8: containerd starts up, dial succeeds
		failDial.Store(false)
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil || !ready {
			t.Fatalf("tick 8: expected ProbeReadiness recovery, got ready=%v err=%v", ready, err)
		}
		if !nd.IsReady() {
			t.Fatal("expected nd.IsReady() == true after recovery")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls to remain 1 after recovery, got %d", atomic.LoadInt32(installCalls))
		}
		if atomic.LoadInt32(&checkCalls) != 1 {
			t.Fatalf("expected checkCalls to remain 1 after recovery, got %d", atomic.LoadInt32(&checkCalls))
		}
	})

	t.Run("deterministic config error commits socket inode and goes quiet until restart", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			configData: []byte(`this is invalid toml [[[`),
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		nd, _, installCalls := setupNode(env)

		// Bootstrap completes with deterministic config error: commits inode
		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("unexpected Bootstrap error: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected unready on config error")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls == 1, got %d", atomic.LoadInt32(installCalls))
		}

		var cur corev1.Node
		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Errorf("expected criu-ready: false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		if !strings.Contains(cur.Annotations[NodeAnnotationCRIUReason], "containerd config error") {
			t.Errorf("expected reason with containerd config error, got %q", cur.Annotations[NodeAnnotationCRIUReason])
		}

		// Subsequent 6 ProbeReadiness ticks on unchanged inode must stay quiet (0 extra installs)
		for tick := 2; tick <= 7; tick++ {
			reProbed, err := nd.ProbeReadiness(ctx)
			if err != nil {
				t.Fatalf("tick %d: ProbeReadiness error: %v", tick, err)
			}
			if reProbed {
				t.Fatalf("tick %d: expected ProbeReadiness to stay unready", tick)
			}
			if atomic.LoadInt32(installCalls) != 1 {
				t.Fatalf("tick %d: expected installCalls to remain 1, got %d", tick, atomic.LoadInt32(installCalls))
			}
		}
	})

	t.Run("criu --version exec error is transient and retries instead of permanently committing", func(t *testing.T) {
		var failExec atomic.Bool
		failExec.Store(true)

		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		insp := env.toInspector()
		insp.GetCRIUVersion = func(ctx context.Context, criuPath string) (string, error) {
			if failExec.Load() {
				return "", errors.New("transient host exec failure")
			}
			return "Version: 4.0", nil
		}

		nd, _, installCalls := setupNode(env)
		nd.Inspector = insp

		// Bootstrap fails with transient exec error
		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected Bootstrap error on exec failure")
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false")
		}

		// Once exec succeeds, ProbeReadiness retries and recovers without containerd restart
		failExec.Store(false)
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil || !ready {
			t.Fatalf("expected ProbeReadiness recovery on exec success, got ready=%v err=%v", ready, err)
		}
		if !nd.IsReady() {
			t.Fatal("expected nd.IsReady() == true")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls to remain 1 after cheap retry, got %d", atomic.LoadInt32(installCalls))
		}
	})

	t.Run("kernel failure commits socket inode and goes quiet until restart", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			files: map[string]fs.FileMode{
				"/usr/local/sbin/criu": 0o755,
			},
			criuVersionOut: "Version: 4.0",
		}
		nd, _, installCalls := setupNode(env)
		// Mock host kernel check failing
		nd.execHostFn = func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Error (check.c:123): Kernel lacks support", errors.New("check failed")
		}

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap error: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected unready on kernel check failure")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls == 1, got %d", atomic.LoadInt32(installCalls))
		}

		// ProbeReadiness on unchanged socket inode 12345 must stay quiet (return false, nil without re-running)
		reProbed, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness error: %v", err)
		}
		if reProbed {
			t.Fatal("expected ProbeReadiness to stay unready and quiet")
		}
		if atomic.LoadInt32(installCalls) != 1 {
			t.Fatalf("expected installCalls to remain 1, got %d", atomic.LoadInt32(installCalls))
		}
	})
}
