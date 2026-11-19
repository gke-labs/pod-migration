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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	"github.com/containerd/errdefs"
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
	id        string
	labels    map[string]string
	labelsErr error
	spec      *specs.Spec
	specErr   error
	info      containers.Container
	infoErr   error
	task      containerd.Task
	taskErr   error
}

func (f *fakeContainer) ID() string { return f.id }
func (f *fakeContainer) Labels(context.Context) (map[string]string, error) {
	return f.labels, f.labelsErr
}
func (f *fakeContainer) Spec(context.Context) (*specs.Spec, error) {
	return f.spec, f.specErr
}
func (f *fakeContainer) Info(context.Context, ...containerd.InfoOpts) (containers.Container, error) {
	return f.info, f.infoErr
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

func TestFindContainer_SkipsNotFoundAcrossLabelsSpecInfo(t *testing.T) {
	spec := &specs.Spec{
		Annotations: map[string]string{
			"io.kubernetes.cri.sandbox-id": "sb-123",
		},
	}
	info := containers.Container{Image: "test-image:v1"}

	targetLabels := map[string]string{
		"io.kubernetes.pod.namespace":  "test-ns",
		"io.kubernetes.pod.name":       "test-pod",
		"io.kubernetes.container.name": "app",
		"io.cri-containerd.kind":       "container",
		"io.kubernetes.pod.uid":        "target-uid-222",
	}

	cLabelsNotFound := &fakeContainer{
		id:        "c-labels-not-found",
		labelsErr: errdefs.ErrNotFound,
	}
	cSpecNotFound := &fakeContainer{
		id:      "c-spec-not-found",
		labels:  targetLabels,
		specErr: errdefs.ErrNotFound,
	}
	cInfoNotFound := &fakeContainer{
		id:      "c-info-not-found",
		labels:  targetLabels,
		spec:    spec,
		infoErr: errdefs.ErrNotFound,
	}
	cTarget := &fakeContainer{
		id:     "c-target",
		labels: targetLabels,
		spec:   spec,
		info:   info,
		task:   &fakeTask{pid: 3003},
	}

	r := &restorer{
		listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
			return []containerd.Container{cLabelsNotFound, cSpecNotFound, cInfoNotFound, cTarget}, nil
		},
	}

	ref, err := r.findContainer(context.Background(), "test-ns", "test-pod", "target-uid-222", "app")
	if err != nil {
		t.Fatalf("findContainer failed: %v", err)
	}
	if ref.ID != "c-target" {
		t.Fatalf("findContainer returned ID %q, want c-target", ref.ID)
	}
	if ref.Pid != 3003 {
		t.Fatalf("findContainer returned Pid %d, want 3003", ref.Pid)
	}
}

func TestFindContainer_PropagatesRPCErrorAcrossLabelsSpecInfo(t *testing.T) {
	spec := &specs.Spec{
		Annotations: map[string]string{
			"io.kubernetes.cri.sandbox-id": "sb-123",
		},
	}
	targetLabels := map[string]string{
		"io.kubernetes.pod.namespace":  "test-ns",
		"io.kubernetes.pod.name":       "test-pod",
		"io.kubernetes.container.name": "app",
		"io.cri-containerd.kind":       "container",
		"io.kubernetes.pod.uid":        "target-uid-222",
	}

	tests := []struct {
		name       string
		container  *fakeContainer
		wantErrSub string
	}{
		{
			name: "labels transport error propagates",
			container: &fakeContainer{
				id:        "c-labels-err",
				labelsErr: errors.New("daemon transport disconnected"),
			},
			wantErrSub: "containerd labels for c-labels-err: daemon transport disconnected",
		},
		{
			name: "spec rpc error propagates",
			container: &fakeContainer{
				id:      "c-spec-err",
				labels:  targetLabels,
				specErr: errors.New("rpc timeout getting spec"),
			},
			wantErrSub: "containerd spec for c-spec-err: rpc timeout getting spec",
		},
		{
			name: "info rpc error propagates",
			container: &fakeContainer{
				id:      "c-info-err",
				labels:  targetLabels,
				spec:    spec,
				infoErr: errors.New("rpc internal error getting info"),
			},
			wantErrSub: "containerd info for c-info-err: rpc internal error getting info",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &restorer{
				listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
					return []containerd.Container{tt.container}, nil
				},
			}
			_, err := r.findContainer(context.Background(), "test-ns", "test-pod", "target-uid-222", "app")
			if err == nil {
				t.Fatal("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tt.wantErrSub) {
				t.Fatalf("expected error containing %q, got: %v", tt.wantErrSub, err)
			}
			if errors.Is(err, ErrContainerNotFound) {
				t.Fatalf("rpc transport error must not be masked as ErrContainerNotFound: %v", err)
			}
		})
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
			name:     "nested absolute pod slice",
			podSlice: "/kubepods.slice/kubepods-pod123.slice",
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
	for _, flag := range []string{"-t 1", "-m", "-p", "-C"} {
		if !strings.Contains(recorded, flag) {
			t.Errorf("execRestore missing flag %q; got args: %s", flag, recorded)
		}
	}
	if strings.Contains(recorded, " -c ") || strings.HasSuffix(recorded, " -c") {
		t.Errorf("execRestore must not pass -c flag (unsupported on Ubuntu 24.04 util-linux 2.39): %s", recorded)
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
	for _, flag := range []string{"-t 1", "-m", "-p", "-C"} {
		if !strings.Contains(recorded, flag) {
			t.Errorf("nsenterExec missing flag %q; got args: %s", flag, recorded)
		}
	}
	if strings.Contains(recorded, " -c ") || strings.HasSuffix(recorded, " -c") {
		t.Errorf("nsenterExec must not pass -c flag (unsupported on Ubuntu 24.04 util-linux 2.39): %s", recorded)
	}
	if !strings.Contains(recorded, "-- runc --root /run/runc delete -f pm-test") {
		t.Errorf("nsenterExec missing command payload; got args: %s", recorded)
	}
}

func TestBuildAdaptParams_RejectsInvalidPodSlice(t *testing.T) {
	in := restoreInputs{
		MigrationName: "mig-root",
		SrcSpec: &specs.Spec{
			Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "src-sb"},
		},
	}
	holder := &containerRef{
		Pid:       1234,
		SandboxID: "tgt-sb",
		ImageName: "app:v1",
		Spec:      sampleTargetSpec(),
	}

	invalidSlices := []string{
		"",
		"/",
		"///",
		"relative.slice",
		"relative/pod.slice",
		"../escape",
		"/kubepods/../escape",
		"///kubepods.slice//kubepods-pod123.slice/..//kubepods-pod123.slice",
	}
	for _, invalid := range invalidSlices {
		if _, _, err := buildAdaptParams(in, holder, "/rootfs", invalid, "pm-mig-root", "/anon", nil); err == nil {
			t.Errorf("buildAdaptParams accepted invalid podSlice %q", invalid)
		}
	}
}

func TestReadProcCgroup_ExecHost(t *testing.T) {
	var calledName string
	var calledArgs []string
	expectedOutput := "0::/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod123.slice/cri-containerd-holder.scope\n"

	r := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			calledName = name
			calledArgs = args
			return expectedOutput, nil
		},
	}

	ctx := context.Background()
	out, err := r.readProcCgroup(ctx, 4567)
	if err != nil {
		t.Fatalf("readProcCgroup failed: %v", err)
	}
	if string(out) != expectedOutput {
		t.Errorf("readProcCgroup returned %q, want %q", string(out), expectedOutput)
	}
	if calledName != "cat" {
		t.Errorf("execHost command = %q, want cat", calledName)
	}
	if len(calledArgs) != 1 || calledArgs[0] != "/proc/4567/cgroup" {
		t.Errorf("execHost args = %v, want [/proc/4567/cgroup]", calledArgs)
	}
}

func TestReadProcCgroup_HostFailureFailsWithoutFallback(t *testing.T) {
	r := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "", fmt.Errorf("simulated host cat failure")
		},
	}

	ctx := context.Background()
	_, err := r.readProcCgroup(ctx, 4567)
	if err == nil {
		t.Fatal("expected error on execHost failure, got nil")
	}
	if !strings.Contains(err.Error(), "simulated host cat failure") {
		t.Errorf("error %q does not contain expected failure reason", err.Error())
	}
}

func TestReadProcCgroup_PrivateCgroupnsTraversalViewRejectedByParser(t *testing.T) {
	// Private cgroupns view emits ../ traversal segments relative to agent cgroup root
	privateCgroupnsOutput := "0::/../../kubepods-burstable-podT.slice/cri-containerd-H.scope\n"

	r := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return privateCgroupnsOutput, nil
		},
	}

	ctx := context.Background()
	out, err := r.readProcCgroup(ctx, 9999)
	if err != nil {
		t.Fatalf("unexpected readProcCgroup error: %v", err)
	}

	_, parseErr := PodSliceFromProcCgroup(string(out))
	if parseErr == nil {
		t.Fatal("PodSliceFromProcCgroup accepted private-cgroupns output with traversal segments")
	}
	if !strings.Contains(parseErr.Error(), "traversal") {
		t.Errorf("parseErr %q does not mention traversal", parseErr.Error())
	}
}

func TestAdaptSpec_RejectsRelativeCgroupsPath(t *testing.T) {
	s := sampleSpec()
	p := params()
	p.CgroupsPath = "relative.slice/pm-mig1" // missing leading slash
	if _, err := AdaptSpec(s, p); err == nil {
		t.Error("AdaptSpec accepted relative CgroupsPath without leading slash")
	}
}
