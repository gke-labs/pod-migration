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
	"os"
	"path/filepath"
	"strings"
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

func TestResolveCRIUPath(t *testing.T) {
	files := map[string]fs.FileMode{
		"/usr/local/sbin/criu": 0o755,
		"/opt/bin/criu":        0o755,
		"/nonexec/criu":        0o644,
	}
	statFn := func(path string) (os.FileInfo, error) {
		mode, ok := files[path]
		if !ok {
			return nil, os.ErrNotExist
		}
		return &fakeFileInfo{name: filepath.Base(path), mode: mode}, nil
	}

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
		GetCRIUVersion: func(criuPath string) (string, error) {
			if f.criuVersionOut == "" {
				return "", errors.New("criu exec failed")
			}
			return f.criuVersionOut, nil
		},
	}
}

func TestCheckContainerdReadiness_FakeContainerd(t *testing.T) {
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
		ok, reason, remediation, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, remediation, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, _, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, remediation, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, _, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, _, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
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
		ok, reason, remediation, err := CheckContainerdReadiness(env.toInspector(), "/run/containerd/containerd.sock")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ok {
			t.Fatal("expected ok=false for outdated CRIU version")
		}
		if !strings.Contains(reason, "older than minimum supported version 3.16.0") {
			t.Fatalf("unexpected reason: %q", reason)
		}
		if remediation != "upgrade CRIU binary to >= 3.16.0" {
			t.Fatalf("unexpected remediation: %q", remediation)
		}
	})
}

func TestEnvironNULSeparation(t *testing.T) {
	raw := []byte("PATH=/bin\x00FOO=BAR\x00")
	parts := bytes.Split(raw, []byte{0})
	var env []string
	for _, p := range parts {
		if len(p) > 0 {
			env = append(env, string(p))
		}
	}
	if len(env) != 2 || env[0] != "PATH=/bin" || env[1] != "FOO=BAR" {
		t.Fatalf("unexpected env parsing: %v", env)
	}
}

func TestGetCRIUVersion_NoLocalExecFallback(t *testing.T) {
	insp := NewDefaultContainerdInspector(func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		return "", errors.New("host exec denied")
	})

	_, err := insp.GetCRIUVersion("/usr/bin/criu")
	if err == nil {
		t.Fatal("expected error when execHost fails")
	}
	if !strings.Contains(err.Error(), "host exec denied") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNode_Bootstrap_WithContainerdReadiness(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	setupNode := func(env *fakeContainerdEnv) (*Node, *corev1.Node) {
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

		nd := &Node{
			Client:     c,
			NodeName:   "test-node",
			SocketPath: "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) {
				return 12345, nil
			},
			installBundleFn: func() (string, error) {
				return "4.0", nil
			},
			Inspector: env.toInspector(),
			Restorer: &restorer{
				execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
					// Mock host kernel check passing
					return "Looks good.", nil
				},
			},
		}
		return nd, nodeObj
	}

	t.Run("bootstrap marks unready with reason when enable_criu = false", func(t *testing.T) {
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
		nd, _ := setupNode(env)

		// Call real Bootstrap!
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
		nd, _ := setupNode(env)

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

	t.Run("readiness check failure leaves socket inode uncommitted so ProbeReadiness retries without restart", func(t *testing.T) {
		env := &fakeContainerdEnv{
			pid: 1001,
			environ: []string{
				"PATH=/usr/local/sbin",
			},
			// Initially criu is missing from PATH
			files:          map[string]fs.FileMode{},
			criuVersionOut: "Version: 4.0",
		}
		nd, _ := setupNode(env)

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap error: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false initially")
		}

		// Now administrator installs criu on PATH without restarting containerd (socket inode unchanged 12345)
		env.files["/usr/local/sbin/criu"] = 0o755

		// ProbeReadiness should re-probe and transition to ready!
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness error: %v", err)
		}
		if !ready || !nd.IsReady() {
			t.Fatal("expected ProbeReadiness to restore node readiness without containerd restart")
		}

		var cur corev1.Node
		if err := nd.Client.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
			t.Errorf("expected criu-ready: true, got %q", cur.Annotations[NodeAnnotationCRIUReady])
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
		nd, _ := setupNode(env)
		// Mock host kernel check failing
		nd.Restorer.execHost = func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Error (check.c:123): Kernel lacks support", errors.New("check failed")
		}

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap error: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected unready on kernel check failure")
		}

		// ProbeReadiness on unchanged socket inode 12345 must stay quiet (return false, nil without re-running)
		reProbed, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness error: %v", err)
		}
		if reProbed {
			t.Fatal("expected ProbeReadiness to stay unready and quiet")
		}
	})
}
