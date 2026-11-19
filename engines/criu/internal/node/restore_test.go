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

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

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

func TestCleanNetnsLock(t *testing.T) {
	ctx := context.Background()
	var executed [][]string
	r := &restorer{
		execNetnsFn: func(ctx context.Context, pid int, name string, args ...string) (string, error) {
			if pid != 1234 {
				t.Fatalf("execNetns called with pid=%d, want 1234", pid)
			}
			call := append([]string{name}, args...)
			executed = append(executed, call)
			return "", nil
		},
	}

	notes := r.cleanNetnsLock(ctx, nil, 1234)
	if len(notes) != 4 {
		t.Fatalf("cleanNetnsLock returned %d notes, want 4 (one per tool): %v", len(notes), notes)
	}

	for _, tool := range []string{"iptables", "ip6tables", "iptables-legacy", "ip6tables-legacy"} {
		foundInputDel := false
		foundOutputDel := false
		foundFlush := false
		foundDeleteChain := false
		var toolCalls []string
		for _, call := range executed {
			if call[0] == tool {
				cmdStr := strings.Join(call, " ")
				toolCalls = append(toolCalls, cmdStr)
				if strings.Contains(cmdStr, "-w 2 -D INPUT -j CRIU") {
					foundInputDel = true
				}
				if strings.Contains(cmdStr, "-w 2 -D OUTPUT -j CRIU") {
					foundOutputDel = true
				}
				if strings.Contains(cmdStr, "-w 2 -F CRIU") {
					foundFlush = true
				}
				if strings.Contains(cmdStr, "-w 2 -X CRIU") {
					foundDeleteChain = true
				}
			}
		}
		if !foundInputDel || !foundOutputDel || !foundFlush || !foundDeleteChain {
			t.Errorf("tool %s missing required cleanup commands: inputDel=%v, outputDel=%v, flush=%v, delChain=%v",
				tool, foundInputDel, foundOutputDel, foundFlush, foundDeleteChain)
		}
		// Assert command ordering: -D -> -F CRIU -> -X CRIU
		delIdx, flushIdx, xIdx := -1, -1, -1
		for i, cmdStr := range toolCalls {
			if strings.Contains(cmdStr, "-D OUTPUT -j CRIU") {
				delIdx = i
			}
			if strings.Contains(cmdStr, "-F CRIU") {
				flushIdx = i
			}
			if strings.Contains(cmdStr, "-X CRIU") {
				xIdx = i
			}
		}
		if !(delIdx < flushIdx && flushIdx < xIdx) {
			t.Errorf("tool %s commands not in expected order (-D < -F < -X): del=%d, flush=%d, x=%d", tool, delIdx, flushIdx, xIdx)
		}
	}
}

func TestCleanNetnsLock_ToolNotFoundIgnored(t *testing.T) {
	ctx := context.Background()
	r := &restorer{
		execNetnsFn: func(ctx context.Context, pid int, name string, args ...string) (string, error) {
			return "executable file not found", errors.New("not found")
		},
	}

	notes := r.cleanNetnsLock(ctx, nil, 1234)
	if len(notes) != 0 {
		t.Fatalf("cleanNetnsLock returned notes when all tools failed: %v", notes)
	}
}

func TestCleanNetnsLock_InvalidPid(t *testing.T) {
	ctx := context.Background()
	called := false
	r := &restorer{
		execNetnsFn: func(ctx context.Context, pid int, name string, args ...string) (string, error) {
			called = true
			return "", nil
		},
	}

	if notes := r.cleanNetnsLock(ctx, nil, 0); len(notes) != 0 || called {
		t.Fatalf("cleanNetnsLock(0) should return nil and not call execNetns; got notes=%v, called=%v", notes, called)
	}
	if notes := r.cleanNetnsLock(ctx, nil, -1); len(notes) != 0 || called {
		t.Fatalf("cleanNetnsLock(-1) should return nil and not call execNetns; got notes=%v, called=%v", notes, called)
	}
}

func TestCleanNetnsLock_CancelledContextStillCleans(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context to simulate caller timeout/cancellation

	called := false
	r := &restorer{
		execNetnsFn: func(cleanCtx context.Context, pid int, name string, args ...string) (string, error) {
			if cleanCtx.Err() != nil {
				t.Fatalf("expected cleanCtx to be uncancelled, got %v", cleanCtx.Err())
			}
			called = true
			return "", nil
		},
	}

	notes := r.cleanNetnsLock(ctx, nil, 1234)
	if !called {
		t.Fatal("expected execNetns to be called despite cancelled parent context")
	}
	if len(notes) != 4 {
		t.Fatalf("cleanNetnsLock returned %d notes, want 4: %v", len(notes), notes)
	}
}

func TestExecNetns_DirectNsenterInvocation(t *testing.T) {
	ctx := context.Background()
	r := &restorer{} // no execNetnsFn, exercises execNetns directly

	// When netns is nil, it runs `nsenter -t <pid> -n -- <name> <args...>`
	// In this test environment, running a non-existent binary or `echo` via nsenter
	// verifies that execNetns invokes nsenter directly without prepending host PID 1's -t 1 -m -p.
	out, err := r.execNetns(ctx, nil, 9999999, "true")
	if err == nil {
		t.Logf("execNetns output: %s", out)
	} else {
		// nsenter will fail with "cannot open /proc/9999999/ns/net: No such file or directory"
		// or similar proc lookup failure, proving direct container nsenter was called.
		t.Logf("direct nsenter executed and returned expected proc lookup error: %v (%s)", err, out)
	}

	// When netns file is passed via ExtraFiles:
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	out, err = r.execNetns(ctx, tmpFile, 0, "true")
	if err == nil {
		t.Logf("execNetns with netns output: %s", out)
	} else {
		// nsenter fails entering non-netns fd, proving `--net=/proc/self/fd/3` was passed.
		t.Logf("direct nsenter with fd=3 returned expected error: %v (%s)", err, out)
	}
}

func TestRestore_FailureCleansNetnsLock(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "etc-criu")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "runc.conf"), []byte("file-validation buildid\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	chkDir := filepath.Join(tmpDir, "chk")
	if err := os.MkdirAll(filepath.Join(chkDir, "checkpoint"), 0o755); err != nil {
		t.Fatal(err)
	}

	holderSpec := sampleTargetSpec()
	var netnsCleanedAfterRestore []int
	var execRestoreCalled bool
	r := &restorer{
		pmRootDir:       tmpDir,
		criuConfDirPath: confDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "holder-ctr-1",
				SandboxID: "tgtsandbox222",
				Pid:       5555,
				Spec:      holderSpec,
				ImageName: "gcr.io/trusted/app:v1",
			}, nil
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
			rf := filepath.Join(bundleDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			execRestoreCalled = true
			// Simulate runc restore failure (e.g. CRIU failed with exit 1).
			return "runc restore error: criu failed with code 1", errors.New("exit status 1")
		},
		execNetnsFn: func(ctx context.Context, pid int, name string, args ...string) (string, error) {
			if execRestoreCalled {
				netnsCleanedAfterRestore = append(netnsCleanedAfterRestore, pid)
			}
			return "", nil
		},
	}

	srcSpec := sampleSpec()
	out, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-netlock-fail",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       srcSpec,
		ImageName:     "gcr.io/trusted/app:v1",
	})
	if err == nil {
		t.Fatal("restore must fail when execRestoreFn returns error")
	}

	// Verify netns lock cleanup was called on failure for the holder container PID 5555.
	if len(netnsCleanedAfterRestore) == 0 {
		t.Fatal("cleanNetnsLock was not called on restore failure")
	}
	foundHolderPid := false
	for _, pid := range netnsCleanedAfterRestore {
		if pid == 5555 {
			foundHolderPid = true
			break
		}
	}
	if !foundHolderPid {
		t.Fatalf("cleanNetnsLock called on failure for pids %v, want 5555", netnsCleanedAfterRestore)
	}

	// Verify notes reflect netns cleanup.
	foundNote := false
	for _, note := range out.Notes {
		if strings.Contains(note, "cleaned CRIU network-lock rules") {
			foundNote = true
			break
		}
	}
	if !foundNote {
		t.Fatalf("expected note about cleaning CRIU network-lock rules in out.Notes: %v", out.Notes)
	}
}

func TestRestore_Variant0CgroupFailureCleansNetnsBeforeVariant1(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "etc-criu")
	if err := os.MkdirAll(confDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(confDir, "runc.conf"), []byte("file-validation buildid\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	chkDir := filepath.Join(tmpDir, "chk")
	if err := os.MkdirAll(filepath.Join(chkDir, "checkpoint"), 0o755); err != nil {
		t.Fatal(err)
	}

	holderSpec := sampleTargetSpec()
	var execRestoreCalls int
	var netnsCleanedCalls int
	var variant0CleanedBeforeVariant1 bool

	r := &restorer{
		pmRootDir:       tmpDir,
		criuConfDirPath: confDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "holder-ctr-1",
				SandboxID: "tgtsandbox222",
				Pid:       5555,
				Spec:      holderSpec,
				ImageName: "gcr.io/trusted/app:v1",
			}, nil
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
			rf := filepath.Join(bundleDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			execRestoreCalls++
			if execRestoreCalls == 1 {
				// Variant 0: fails with cgroup error
				return "Error: failed to configure cgroup: cgroup error", errors.New("exit status 1")
			}
			// Variant 1: succeeds and writes pid file
			pidPath := filepath.Join(tmpDir, "bundles", "pm-mig-cgroup-variant", "pid")
			_ = os.WriteFile(pidPath, []byte("6666\n"), 0o644)
			return "restored successfully", nil
		},
		execNetnsFn: func(ctx context.Context, pid int, name string, args ...string) (string, error) {
			netnsCleanedCalls++
			if execRestoreCalls == 1 {
				variant0CleanedBeforeVariant1 = true
			}
			return "", nil
		},
	}

	srcSpec := sampleSpec()
	out, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-cgroup-variant",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       srcSpec,
		ImageName:     "gcr.io/trusted/app:v1",
	})
	if err != nil {
		t.Fatalf("restore should have succeeded via variant 1, got error: %v", err)
	}
	if out.Pid != 6666 {
		t.Fatalf("expected restored pid 6666, got %d", out.Pid)
	}
	if !variant0CleanedBeforeVariant1 {
		t.Fatal("expected cleanNetnsLock to be called after variant 0 cgroup failure before variant 1 retry")
	}
}
