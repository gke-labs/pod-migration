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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func init() {
	if os.Getenv("GO_WANT_CGROUP_NSENTER_HELPER_PROCESS") == "1" {
		args := os.Args[1:]
		if p := os.Getenv("CGROUP_HELPER_ARGS_OUT"); p != "" {
			_ = os.WriteFile(p, []byte(strings.Join(args, " ")), 0o644)
		}
		os.Exit(0)
	}
}

type fakeContainer struct {
	containerd.Container
	id      string
	labels  map[string]string
	spec    *specs.Spec
	info    containers.Container
	task    containerd.Task
	taskErr error
}

func (f *fakeContainer) ID() string { return f.id }
func (f *fakeContainer) Labels(context.Context) (map[string]string, error) {
	return f.labels, nil
}
func (f *fakeContainer) Spec(context.Context) (*specs.Spec, error) {
	return f.spec, nil
}
func (f *fakeContainer) Info(context.Context, ...containerd.InfoOpts) (containers.Container, error) {
	return f.info, nil
}
func (f *fakeContainer) Task(context.Context, cio.Attach) (containerd.Task, error) {
	return f.task, f.taskErr
}

type fakeTask struct {
	containerd.Task
	pid uint32
}

func (t *fakeTask) Pid() uint32 { return t.pid }

func TestFindContainer_UIDFilter(t *testing.T) {
	spec := &specs.Spec{
		Annotations: map[string]string{
			"io.kubernetes.cri.sandbox-id": "sb-123",
		},
	}
	info := containers.Container{Image: "test-image:v1"}

	cOld := &fakeContainer{
		id: "container-old",
		labels: map[string]string{
			"io.kubernetes.pod.namespace":  "test-ns",
			"io.kubernetes.pod.name":       "test-pod",
			"io.kubernetes.container.name": "app",
			"io.cri-containerd.kind":       "container",
			"io.kubernetes.pod.uid":        "old-uid-111",
		},
		spec: spec,
		info: info,
		task: &fakeTask{pid: 1001},
	}

	cTarget := &fakeContainer{
		id: "container-target",
		labels: map[string]string{
			"io.kubernetes.pod.namespace":  "test-ns",
			"io.kubernetes.pod.name":       "test-pod",
			"io.kubernetes.container.name": "app",
			"io.cri-containerd.kind":       "container",
			"io.kubernetes.pod.uid":        "target-uid-222",
		},
		spec: spec,
		info: info,
		task: &fakeTask{pid: 1002},
	}

	r := &restorer{
		listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
			return []containerd.Container{cOld, cTarget}, nil
		},
	}

	ref, err := r.findContainer(context.Background(), "test-ns", "test-pod", "target-uid-222", "app")
	if err != nil {
		t.Fatalf("findContainer failed: %v", err)
	}
	if ref.ID != "container-target" {
		t.Fatalf("findContainer returned ID %q, want container-target", ref.ID)
	}
	if ref.Pid != 1002 {
		t.Fatalf("findContainer returned Pid %d, want 1002", ref.Pid)
	}
}

func TestFindContainer_PidZeroContinuation(t *testing.T) {
	spec := &specs.Spec{
		Annotations: map[string]string{
			"io.kubernetes.cri.sandbox-id": "sb-123",
		},
	}
	info := containers.Container{Image: "test-image:v1"}

	cStopped := &fakeContainer{
		id: "container-stopped",
		labels: map[string]string{
			"io.kubernetes.pod.namespace":  "test-ns",
			"io.kubernetes.pod.name":       "test-pod",
			"io.kubernetes.container.name": "app",
			"io.cri-containerd.kind":       "container",
			"io.kubernetes.pod.uid":        "target-uid-222",
		},
		spec: spec,
		info: info,
		task: &fakeTask{pid: 0},
	}

	cRunning := &fakeContainer{
		id: "container-running",
		labels: map[string]string{
			"io.kubernetes.pod.namespace":  "test-ns",
			"io.kubernetes.pod.name":       "test-pod",
			"io.kubernetes.container.name": "app",
			"io.cri-containerd.kind":       "container",
			"io.kubernetes.pod.uid":        "target-uid-222",
		},
		spec: spec,
		info: info,
		task: &fakeTask{pid: 2002},
	}

	r := &restorer{
		listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
			return []containerd.Container{cStopped, cRunning}, nil
		},
	}

	ref, err := r.findContainer(context.Background(), "test-ns", "test-pod", "target-uid-222", "app")
	if err != nil {
		t.Fatalf("findContainer failed: %v", err)
	}
	if ref.ID != "container-running" {
		t.Fatalf("findContainer returned ID %q, want container-running (skipped pid=0 stopped container)", ref.ID)
	}
	if ref.Pid != 2002 {
		t.Fatalf("findContainer returned Pid %d, want 2002", ref.Pid)
	}
}

func TestBuildAdaptParams_ProducesAbsoluteCgroupsPath(t *testing.T) {
	in := restoreInputs{
		MigrationName: "mig-123",
		SrcSpec: &specs.Spec{
			Annotations: map[string]string{
				"io.kubernetes.cri.sandbox-id": "src-sb",
			},
		},
	}
	holder := &containerRef{
		Pid:       3456,
		SandboxID: "tgt-sb",
		ImageName: "registry.example.com/app:v1",
		Spec:      sampleTargetSpec(),
	}

	testCases := []struct {
		name     string
		podSlice string
		want     string
	}{
		{
			name:     "already absolute pod slice",
			podSlice: "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice",
			want:     "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice/pm-mig-123",
		},
		{
			name:     "relative pod slice normalized to absolute",
			podSlice: "kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice",
			want:     "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice/pm-mig-123",
		},
		{
			name:     "messy path cleaned to absolute",
			podSlice: "///kubepods.slice//kubepods-pod123.slice/..//kubepods-pod123.slice",
			want:     "/kubepods.slice/kubepods-pod123.slice/pm-mig-123",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			params, img, err := buildAdaptParams(in, holder, "/rootfs", tc.podSlice, "pm-mig-123", "/anon", nil)
			if err != nil {
				t.Fatalf("buildAdaptParams failed: %v", err)
			}
			if img != "registry.example.com/app:v1" {
				t.Errorf("imageName = %q, want registry.example.com/app:v1", img)
			}
			if params.CgroupsPath != tc.want {
				t.Errorf("CgroupsPath = %q, want %q", params.CgroupsPath, tc.want)
			}
			if !strings.HasPrefix(params.CgroupsPath, "/") {
				t.Errorf("CgroupsPath must be absolute, got %q", params.CgroupsPath)
			}
		})
	}
}

func TestExecRestore_EntersHostCgroupNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	argsOut := filepath.Join(tmpDir, "args.txt")
	helperScript := filepath.Join(tmpDir, "mock-nsenter.sh")
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_CGROUP_NSENTER_HELPER_PROCESS=1\nexport CGROUP_HELPER_ARGS_OUT=%q\nexec %q \"$@\"\n", argsOut, os.Args[0])
	if err := os.WriteFile(helperScript, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	r := &restorer{nsenterBin: helperScript}
	logPath := filepath.Join(tmpDir, "runc.log")
	ctx := context.Background()
	_, err := r.execRestore(ctx, nil, logPath, "runc", "restore", "--bundle", "/bundle")
	if err != nil {
		t.Fatalf("execRestore failed: %v", err)
	}

	b, err := os.ReadFile(argsOut)
	if err != nil {
		t.Fatalf("failed to read recorded args: %v", err)
	}
	recorded := string(b)
	for _, flag := range []string{"-t 1", "-m", "-p", "-C", "-c"} {
		if !strings.Contains(recorded, flag) {
			t.Errorf("execRestore missing flag %q; got args: %s", flag, recorded)
		}
	}
	if !strings.Contains(recorded, "-- runc restore --bundle /bundle") {
		t.Errorf("execRestore missing command payload; got args: %s", recorded)
	}
}

func TestNsenterExec_EntersHostCgroupNamespace(t *testing.T) {
	tmpDir := t.TempDir()
	argsOut := filepath.Join(tmpDir, "args.txt")
	helperScript := filepath.Join(tmpDir, "mock-nsenter.sh")
	script := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_CGROUP_NSENTER_HELPER_PROCESS=1\nexport CGROUP_HELPER_ARGS_OUT=%q\nexec %q \"$@\"\n", argsOut, os.Args[0])
	if err := os.WriteFile(helperScript, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	r := &restorer{nsenterBin: helperScript}
	ctx := context.Background()
	_, err := r.nsenterExec(ctx, nil, "runc", "--root", "/run/runc", "delete", "-f", "pm-test")
	if err != nil {
		t.Fatalf("nsenterExec failed: %v", err)
	}

	b, err := os.ReadFile(argsOut)
	if err != nil {
		t.Fatalf("failed to read recorded args: %v", err)
	}
	recorded := string(b)
	for _, flag := range []string{"-t 1", "-m", "-p", "-C", "-c"} {
		if !strings.Contains(recorded, flag) {
			t.Errorf("nsenterExec missing flag %q; got args: %s", flag, recorded)
		}
	}
	if !strings.Contains(recorded, "-- runc --root /run/runc delete -f pm-test") {
		t.Errorf("nsenterExec missing command payload; got args: %s", recorded)
	}
}
