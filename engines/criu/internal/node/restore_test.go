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
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/containers"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
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
	if os.Getenv("GO_WANT_NETNS_HELPER_PROCESS") == "1" {
		fd3 := os.NewFile(uintptr(3), "netns-fd")
		if fd3 == nil {
			fmt.Fprintln(os.Stderr, "fd 3 is nil")
			os.Exit(2)
		}
		if _, err := fd3.Stat(); err != nil {
			fmt.Fprintf(os.Stderr, "fd 3 stat error: %v\n", err)
			os.Exit(3)
		}
		args := os.Args[1:]
		for i, a := range args {
			if a == "-t" && i+1 < len(args) && args[i+1] == "1" {
				fmt.Fprintln(os.Stderr, "unexpected host PID 1 targeting in netns helper")
				os.Exit(4)
			}
			if a == "-m" || a == "-p" {
				fmt.Fprintf(os.Stderr, "unexpected mount/pid namespace flag %q in netns helper\n", a)
				os.Exit(5)
			}
		}
		if len(args) < 6 || args[0] != "--net=/proc/self/fd/3" || args[1] != "--" ||
			args[2] != "iptables" || args[3] != "-w" || args[4] != "2" || args[5] != "-S" {
			fmt.Fprintf(os.Stderr, "unexpected netns helper args: %v\n", args)
			os.Exit(6)
		}
		fmt.Fprintf(os.Stdout, "netns helper success: args=%v\n", args)
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

func TestCleanNetnsLock(t *testing.T) {
	ctx := context.Background()
	var executed [][]string
	deleteCounts := make(map[string]int)
	r := &restorer{
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if netns == nil {
				t.Fatal("execNetns called with nil netns")
			}
			call := append([]string{name}, args...)
			executed = append(executed, call)
			// For -D rules, succeed on first attempt per tool+chain and fail on second,
			// exercising the 1-rule delete + break path.
			if len(args) >= 4 && args[2] == "-D" {
				key := fmt.Sprintf("%s:%s", name, args[3])
				deleteCounts[key]++
				if deleteCounts[key] > 1 {
					return "iptables: No chain/target/match by that name.", errors.New("exit status 1")
				}
			}
			return "", nil
		},
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	notes := r.cleanNetnsLock(ctx, tmpFile)
	if len(notes) != 4 {
		t.Fatalf("cleanNetnsLock returned %d notes, want 4 (one per tool): %v", len(notes), notes)
	}

	for _, tool := range []string{"iptables", "ip6tables", "iptables-legacy", "ip6tables-legacy"} {
		foundProbe := false
		foundInputDel := false
		foundOutputDel := false
		foundFlush := false
		foundDeleteChain := false
		var toolCalls []string
		for _, call := range executed {
			if call[0] == tool {
				cmdStr := strings.Join(call, " ")
				toolCalls = append(toolCalls, cmdStr)
				if strings.Contains(cmdStr, "-w 2 -S CRIU") {
					foundProbe = true
				}
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
		if !foundProbe || !foundInputDel || !foundOutputDel || !foundFlush || !foundDeleteChain {
			t.Errorf("tool %s missing required cleanup commands: probe=%v, inputDel=%v, outputDel=%v, flush=%v, delChain=%v",
				tool, foundProbe, foundInputDel, foundOutputDel, foundFlush, foundDeleteChain)
		}
		// Assert command ordering: -S CRIU -> -D -> -F CRIU -> -X CRIU
		probeIdx, delIdx, flushIdx, xIdx := -1, -1, -1, -1
		for i, cmdStr := range toolCalls {
			if strings.Contains(cmdStr, "-w 2 -S CRIU") {
				probeIdx = i
			}
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
		if !(probeIdx < delIdx && delIdx < flushIdx && flushIdx < xIdx) {
			t.Errorf("tool %s commands not in expected order (-S < -D < -F < -X): probe=%d, del=%d, flush=%d, x=%d", tool, probeIdx, delIdx, flushIdx, xIdx)
		}
	}
}

func TestCleanNetnsLock_ToolNotFoundIgnored(t *testing.T) {
	ctx := context.Background()
	r := &restorer{
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			return "executable file not found", errors.New("not found")
		},
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	notes := r.cleanNetnsLock(ctx, tmpFile)
	if len(notes) != 0 {
		t.Fatalf("cleanNetnsLock returned notes when all tools failed: %v", notes)
	}
}

func TestCleanNetnsLock_NilNetns(t *testing.T) {
	ctx := context.Background()
	called := false
	r := &restorer{
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			called = true
			return "", nil
		},
	}

	notes := r.cleanNetnsLock(ctx, nil)
	if called {
		t.Fatal("expected execNetnsFn not called when netns is nil")
	}
	if len(notes) != 1 || !strings.Contains(notes[0], "skipped: netns fd is nil") {
		t.Fatalf("expected skipped note on nil netns, got %v", notes)
	}
}

func TestCleanNetnsLock_CancelledContextStillCleans(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel context to simulate caller timeout/cancellation

	called := false
	r := &restorer{
		execNetnsFn: func(cleanCtx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if cleanCtx.Err() != nil {
				t.Fatalf("expected cleanCtx to be uncancelled, got %v", cleanCtx.Err())
			}
			called = true
			return "", nil
		},
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	notes := r.cleanNetnsLock(ctx, tmpFile)
	if !called {
		t.Fatal("expected execNetns to be called despite cancelled parent context")
	}
	if len(notes) != 4 {
		t.Fatalf("cleanNetnsLock returned %d notes, want 4: %v", len(notes), notes)
	}
}

func TestCleanNetnsLock_ProbesChainExistsFirst(t *testing.T) {
	ctx := context.Background()
	var executedTools []string
	r := &restorer{
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if len(args) >= 3 && args[2] == "-S" {
				if name == "iptables-legacy" || name == "ip6tables-legacy" {
					return "iptables: No chain/target/match by that name", errors.New("exit status 1")
				}
				return "-N CRIU\n", nil
			}
			executedTools = append(executedTools, name)
			return "", nil
		},
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	notes := r.cleanNetnsLock(ctx, tmpFile)
	for _, tool := range executedTools {
		if tool == "iptables-legacy" || tool == "ip6tables-legacy" {
			t.Fatalf("legacy tool %s executed commands despite missing CRIU chain", tool)
		}
	}
	if len(notes) != 2 {
		t.Fatalf("expected 2 cleanup notes (iptables & ip6tables), got %d: %v", len(notes), notes)
	}
}

func TestCleanNetnsLock_RecordsErrorNotesOnFailedFlushOrDelete(t *testing.T) {
	ctx := context.Background()
	r := &restorer{
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if len(args) >= 3 && args[2] == "-S" {
				return "-N CRIU\n", nil
			}
			if len(args) >= 3 && args[2] == "-F" {
				return "permission denied", errors.New("exit status 1")
			}
			if len(args) >= 3 && args[2] == "-X" {
				return "resource busy", errors.New("exit status 1")
			}
			return "", nil
		},
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	notes := r.cleanNetnsLock(ctx, tmpFile)
	foundFlushErr := false
	foundXErr := false
	for _, n := range notes {
		if strings.Contains(n, "-F CRIU failed") {
			foundFlushErr = true
		}
		if strings.Contains(n, "-X CRIU failed") {
			foundXErr = true
		}
	}
	if !foundFlushErr || !foundXErr {
		t.Fatalf("expected error notes for failed -F and -X, got: %v", notes)
	}
}

func TestExecNetns_DirectNsenterInvocation(t *testing.T) {
	ctx := context.Background()

	// 1. Refusal when netns is nil
	r := &restorer{}
	if _, err := r.execNetns(ctx, nil, "iptables", "-L"); err == nil {
		t.Fatal("expected error when netns is nil, got nil")
	}

	// 2. Subprocess helper proves that:
	// - ExtraFiles[0] lands on fd 3 and is a valid open file descriptor
	// - `--net=/proc/self/fd/3` is passed
	// - No `-t 1 -m -p` host namespace escape is present
	tmpFile, err := os.CreateTemp(t.TempDir(), "netns-test")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpFile.Close()

	helperScript := filepath.Join(t.TempDir(), "nsenter-mock.sh")
	scriptContent := fmt.Sprintf("#!/bin/sh\nexport GO_WANT_NETNS_HELPER_PROCESS=1\nexec %q \"$@\"\n", os.Args[0])
	if err := os.WriteFile(helperScript, []byte(scriptContent), 0o755); err != nil {
		t.Fatal(err)
	}

	r.nsenterBin = helperScript
	out, err := r.execNetns(ctx, tmpFile, "iptables", "-w", "2", "-S", "CRIU")
	if err != nil {
		t.Fatalf("execNetns helper process failed: %v (%s)", err, out)
	}
	if !strings.Contains(out, "netns helper success") {
		t.Fatalf("expected netns helper success, got: %s", out)
	}
}

func TestOpenPodNetns_HostNetworkPodRefused(t *testing.T) {
	selfPath := fmt.Sprintf("/proc/%d/ns/net", os.Getpid())
	r := &restorer{
		hostNetnsPath: selfPath,
	}
	f, err := r.openPodNetns(os.Getpid())
	if err == nil || f != nil {
		t.Fatalf("expected openPodNetns to refuse host network PID, got f=%v err=%v", f, err)
	}
	if !strings.Contains(err.Error(), "shares host network namespace") {
		t.Fatalf("expected host netns refusal error, got %v", err)
	}

	// Non-existent PID returns open error
	f, err = r.openPodNetns(99999999)
	if err == nil || f != nil {
		t.Fatalf("expected error on non-existent PID, got f=%v err=%v", f, err)
	}
	// PID <= 0 returns nil, nil
	f, err = r.openPodNetns(0)
	if err != nil || f != nil {
		t.Fatalf("expected nil, nil on PID 0, got f=%v err=%v", f, err)
	}
}

func TestRestore_OpenPodNetnsErrorAbortsBeforeExecRestore(t *testing.T) {
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
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return nil, errors.New("target pod PID shares host network namespace; refusing restore to protect host")
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			execRestoreCalled = true
			return "", nil
		},
	}

	srcSpec := sampleSpec()
	_, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-netns-err",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       srcSpec,
	})
	if err == nil {
		t.Fatal("expected restore to abort with error when openPodNetns fails, got nil")
	}
	if !strings.Contains(err.Error(), "open pod netns") {
		t.Fatalf("expected 'open pod netns' in error, got: %v", err)
	}
	if execRestoreCalled {
		t.Fatal("execRestore must not be called when openPodNetns fails")
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
	var netnsCleanedTools []string
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
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			if holderPid != 5555 {
				t.Errorf("openPodNetns called for pid %d, want 5555", holderPid)
			}
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string, labels map[string]string) (string, error) {
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
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if netns == nil {
				t.Errorf("execNetns received nil netns file")
			}
			if execRestoreCalled {
				netnsCleanedTools = append(netnsCleanedTools, name)
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

	// Verify netns lock cleanup was called on failure.
	if len(netnsCleanedTools) == 0 {
		t.Fatal("cleanNetnsLock was not called on restore failure")
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
	// Verify error summary contains netfilter cleanup summary near the front
	if !strings.Contains(err.Error(), "cleaned CRIU network-lock rules with iptables") {
		t.Fatalf("expected error string to contain netns cleanup summary near front: %v", err)
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
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			if holderPid != 5555 {
				t.Errorf("openPodNetns called for pid %d, want 5555", holderPid)
			}
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string, labels map[string]string) (string, error) {
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
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if netns == nil {
				t.Errorf("execNetns received nil netns")
			}
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

func TestRestore_PreRestoreCleansStaleNetnsLock(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "etc-criu")
	_ = os.MkdirAll(confDir, 0o755)
	_ = os.WriteFile(filepath.Join(confDir, "runc.conf"), []byte("file-validation buildid\n"), 0o644)
	chkDir := filepath.Join(tmpDir, "chk")
	_ = os.MkdirAll(filepath.Join(chkDir, "checkpoint"), 0o755)

	holderSpec := sampleTargetSpec()
	var preRestoreCleaned bool
	var execRestoreCalls int

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
			return []byte("0::/kubepods.slice/kubepods-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string, labels map[string]string) (string, error) {
			rf := filepath.Join(bundleDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			if execRestoreCalls == 0 {
				preRestoreCleaned = true
			}
			return "", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			execRestoreCalls++
			pidPath := filepath.Join(tmpDir, "bundles", "pm-mig-prerestore-clean", "pid")
			_ = os.WriteFile(pidPath, []byte("7777\n"), 0o644)
			return "restored successfully", nil
		},
	}

	srcSpec := sampleSpec()
	out, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-prerestore-clean",
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
		t.Fatalf("restore failed: %v", err)
	}
	if !preRestoreCleaned {
		t.Fatal("expected cleanNetnsLock to run pre-restore before execRestore")
	}
	foundNote := false
	for _, n := range out.Notes {
		if strings.Contains(n, "cleaned CRIU network-lock rules") {
			foundNote = true
			break
		}
	}
	if !foundNote {
		t.Fatalf("expected pre-restore cleanup note in out.Notes: %v", out.Notes)
	}
}

func TestRestore_MissingPidFileCleansNetnsLock(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "etc-criu")
	_ = os.MkdirAll(confDir, 0o755)
	_ = os.WriteFile(filepath.Join(confDir, "runc.conf"), []byte("file-validation buildid\n"), 0o644)
	chkDir := filepath.Join(tmpDir, "chk")
	_ = os.MkdirAll(filepath.Join(chkDir, "checkpoint"), 0o755)

	holderSpec := sampleTargetSpec()
	var netnsCleanedOnMissingPid bool

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
			return []byte("0::/kubepods.slice/kubepods-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string, labels map[string]string) (string, error) {
			rf := filepath.Join(bundleDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			// runc restore exits 0 but does not write pidFile
			return "restored", nil
		},
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
			netnsCleanedOnMissingPid = true
			return "", nil
		},
	}

	srcSpec := sampleSpec()
	out, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-missing-pid",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       srcSpec,
		ImageName:     "gcr.io/trusted/app:v1",
	})
	if err == nil || !strings.Contains(err.Error(), "pid-file missing") {
		t.Fatalf("expected pid-file missing error, got err=%v", err)
	}
	if !netnsCleanedOnMissingPid {
		t.Fatal("expected cleanNetnsLock to be called when pid-file is missing")
	}
	if !strings.Contains(err.Error(), "cleaned CRIU network-lock rules") {
		t.Fatalf("expected netns cleanup summary in error string, got %v", err)
	}
	_ = out
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

func TestCleanup_OrderAndTeardown(t *testing.T) {
	tmpDir := t.TempDir()
	id := "pm-test-cleanup"
	bundleDir := filepath.Join(tmpDir, "bundles", id)
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}

	var calls []string
	r := &restorer{
		pmRootDir: tmpDir,
		runcDeleteFn: func(ctx context.Context, cid string) error {
			calls = append(calls, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			calls = append(calls, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			calls = append(calls, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			calls = append(calls, "lease-delete:"+lid)
			return nil
		},
	}

	notes, err := r.cleanup(context.Background(), "test-cleanup", "", "", false)
	if err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if len(notes) > 0 {
		t.Fatalf("unexpected cleanup notes: %v", notes)
	}

	wantOrder := []string{
		"runc-delete:" + id,
		"unmount:" + bundleDir,
		"snapshot-remove:" + id,
		"lease-delete:lease-" + id,
	}
	if len(calls) != len(wantOrder) {
		t.Fatalf("calls = %v, want %v", calls, wantOrder)
	}
	for i := range wantOrder {
		if calls[i] != wantOrder[i] {
			t.Errorf("step %d = %q, want %q", i, calls[i], wantOrder[i])
		}
	}

	// Bundle directory should be removed.
	if _, err := os.Stat(bundleDir); !os.IsNotExist(err) {
		t.Errorf("bundle directory still exists after cleanup: %v", err)
	}
}

func TestCleanup_StopsOnFirstFailure(t *testing.T) {
	tmpDir := t.TempDir()
	id := "pm-test-fail-order"
	bundleDir := filepath.Join(tmpDir, "bundles", id)
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}

	// 1. runc delete failure must stop immediately and NOT unmount or delete snapshot/lease
	unmounted := false
	snapRemoved := false
	leaseDeleted := false
	r1 := &restorer{
		pmRootDir: tmpDir,
		runcDeleteFn: func(ctx context.Context, cid string) error {
			return errors.New("runc delete in D-state")
		},
		unmountTreeFn: func(dir string) error {
			unmounted = true
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			snapRemoved = true
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			leaseDeleted = true
			return nil
		},
	}
	err := r1.Cleanup(context.Background(), "test-fail-order")
	if err == nil {
		t.Fatal("expected cleanup to return error on runc delete failure, got nil")
	}
	if unmounted || snapRemoved || leaseDeleted {
		t.Fatalf("subsequent steps executed after runc delete failed: unmounted=%v, snapRemoved=%v, leaseDeleted=%v",
			unmounted, snapRemoved, leaseDeleted)
	}

	// 2. unmount failure must stop immediately and NOT remove snapshot or lease
	snapRemoved2 := false
	leaseDeleted2 := false
	r2 := &restorer{
		pmRootDir: tmpDir,
		runcDeleteFn: func(ctx context.Context, cid string) error {
			return nil
		},
		unmountTreeFn: func(dir string) error {
			return errors.New("mount busy")
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			snapRemoved2 = true
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			leaseDeleted2 = true
			return nil
		},
	}
	err2 := r2.Cleanup(context.Background(), "test-fail-order")
	if err2 == nil {
		t.Fatal("expected cleanup to return error on unmount failure, got nil")
	}
	if snapRemoved2 || leaseDeleted2 {
		t.Fatalf("subsequent steps executed after unmount failed: snapRemoved=%v, leaseDeleted=%v",
			snapRemoved2, leaseDeleted2)
	}
}

func TestCleanup_RealRuncDeleteAndKillArgv(t *testing.T) {
	var executedCommands [][]string
	r := &restorer{
		pmRootDir: t.TempDir(),
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			executedCommands = append(executedCommands, append([]string{name}, args...))
			return "", nil
		},
	}

	// Test Cleanup argv
	if err := r.Cleanup(context.Background(), "test-cmd"); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if len(executedCommands) != 1 {
		t.Fatalf("expected 1 execHost call, got %d", len(executedCommands))
	}
	wantDel := []string{"runc", "--root", "/run/pod-migrate/runc", "delete", "-f", "pm-test-cmd"}
	if strings.Join(executedCommands[0], " ") != strings.Join(wantDel, " ") {
		t.Errorf("runc delete argv = %v, want %v", executedCommands[0], wantDel)
	}

	// Test Signal argv
	if err := r.Signal(context.Background(), "test-cmd", 15); err != nil {
		t.Fatalf("signal failed: %v", err)
	}
	if len(executedCommands) != 2 {
		t.Fatalf("expected 2 execHost calls, got %d", len(executedCommands))
	}
	wantKill := []string{"runc", "--root", "/run/pod-migrate/runc", "kill", "pm-test-cmd", "15"}
	if strings.Join(executedCommands[1], " ") != strings.Join(wantKill, " ") {
		t.Errorf("runc kill argv = %v, want %v", executedCommands[1], wantKill)
	}
}

func TestRestore_DoubleRestoreTeardownBeforePrepareRootfs(t *testing.T) {
	tmpDir := t.TempDir()
	id := "pm-double-restore"
	bundleDir := filepath.Join(tmpDir, "bundles", id)
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	criuConf := filepath.Join(tmpDir, "criu")
	if err := os.MkdirAll(criuConf, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(criuConf, "runc.conf"), []byte("file-validation buildid\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var events []string
	r := &restorer{
		pmRootDir:       tmpDir,
		criuConfDirPath: criuConf,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "c-holder",
				Pid:       1234,
				Spec:      sampleTargetSpec(),
				ImageName: "redis:7",
			}, nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-pod123.slice/c1.scope\n"), nil
		},
		runcDeleteFn: func(ctx context.Context, cid string) error {
			events = append(events, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			events = append(events, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			events = append(events, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			events = append(events, "lease-delete:"+lid)
			return nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bDir, diffTar string, labels map[string]string) (string, error) {
			events = append(events, "prepare-rootfs:"+key)
			if labels["pod-migrate.io/pod-uid"] != "pod-uid-456" {
				t.Errorf("expected pod-uid label, got %v", labels)
			}
			rf := filepath.Join(bDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			for i := 0; i < len(args)-1; i++ {
				if args[i] == "--pid-file" {
					_ = os.WriteFile(args[i+1], []byte("5678\n"), 0o644)
				}
			}
			events = append(events, "exec-restore")
			return "", nil
		},
	}

	in := restoreInputs{
		MigrationName: "double-restore",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		TargetPodUID:  "pod-uid-456",
		ContainerName: "app",
		CheckpointDir: tmpDir,
		SrcSpec:       sampleSpec(),
	}

	out, err := r.restore(context.Background(), in)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	if out.Pid != 5678 {
		t.Errorf("pid = %d, want 5678", out.Pid)
	}

	// Verify that teardown events occurred BEFORE prepare-rootfs
	foundPrep := false
	for _, ev := range events {
		if ev == "prepare-rootfs:"+id {
			foundPrep = true
			break
		}
		if !strings.HasPrefix(ev, "runc-delete:") && !strings.HasPrefix(ev, "unmount:") &&
			!strings.HasPrefix(ev, "snapshot-remove:") && !strings.HasPrefix(ev, "lease-delete:") {
			t.Errorf("unexpected pre-restore event: %q", ev)
		}
	}
	if !foundPrep {
		t.Fatal("prepare-rootfs event not found")
	}
}

func TestRestore_FailureTeardownCleansContainerMountsSnapshot(t *testing.T) {
	tmpDir := t.TempDir()
	id := "pm-fail-restore"

	var cleaned []string
	r := &restorer{
		pmRootDir: tmpDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "c-holder",
				Pid:       1234,
				Spec:      &specs.Spec{Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "sb1"}},
				ImageName: "redis:7",
			}, nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-pod123.slice/c1.scope\n"), nil
		},
		runcDeleteFn: func(ctx context.Context, cid string) error {
			cleaned = append(cleaned, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			cleaned = append(cleaned, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			cleaned = append(cleaned, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			cleaned = append(cleaned, "lease-delete:"+lid)
			return nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bDir, diffTar string, labels map[string]string) (string, error) {
			rf := filepath.Join(bDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			return "Error: failed to restore memory pages", errors.New("runc exit status 1")
		},
	}

	in := restoreInputs{
		MigrationName: "fail-restore",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		TargetPodUID:  "pod-uid-456",
		ContainerName: "app",
		CheckpointDir: tmpDir,
		SrcSpec: &specs.Spec{
			Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "src-sb"},
		},
	}

	_, err := r.restore(context.Background(), in)
	if err == nil {
		t.Fatal("expected restore to fail, got nil")
	}

	// Verify that teardown ran on restore failure
	hasRuncDel := false
	hasUnmount := false
	hasSnapRem := false
	hasLeaseDel := false
	for _, cl := range cleaned {
		if cl == "runc-delete:"+id {
			hasRuncDel = true
		}
		if cl == "unmount:"+filepath.Join(tmpDir, "bundles", id) {
			hasUnmount = true
		}
		if cl == "snapshot-remove:"+id {
			hasSnapRem = true
		}
		if cl == "lease-delete:lease-"+id {
			hasLeaseDel = true
		}
	}
	if !hasRuncDel || !hasUnmount || !hasSnapRem || !hasLeaseDel {
		t.Fatalf("missing failure teardown actions; cleaned = %v", cleaned)
	}
}

func TestSweepOrphans_CleansOrphanLeasesAndBundles(t *testing.T) {
	tmpDir := t.TempDir()

	// Stage an orphaned bundle dir
	bundleOrphan := filepath.Join(tmpDir, "bundles", "pm-psengine-orphan-snap")
	if err := os.MkdirAll(bundleOrphan, 0o755); err != nil {
		t.Fatal(err)
	}
	// Stage a living bundle dir
	bundleLiving := filepath.Join(tmpDir, "bundles", "pm-psengine-living-snap")
	if err := os.MkdirAll(bundleLiving, 0o755); err != nil {
		t.Fatal(err)
	}
	// Stage an orphaned bundle dir created after sweepStart (must be skipped)
	bundleAfterSweep := filepath.Join(tmpDir, "bundles", "pm-psengine-after-sweep-snap")
	if err := os.MkdirAll(bundleAfterSweep, 0o755); err != nil {
		t.Fatal(err)
	}
	futureTime := time.Now().Add(10 * time.Minute)
	if err := os.Chtimes(bundleAfterSweep, futureTime, futureTime); err != nil {
		t.Fatal(err)
	}

	var cleaned []string
	deletedLeaseIDs := make(map[string]bool)
	r := &restorer{
		pmRootDir: tmpDir,
		runcDeleteFn: func(ctx context.Context, cid string) error {
			cleaned = append(cleaned, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			cleaned = append(cleaned, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			cleaned = append(cleaned, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			deletedLeaseIDs[lid] = true
			cleaned = append(cleaned, "lease-delete:"+lid)
			return nil
		},
		listLeasesFn: func(ctx context.Context) ([]leases.Lease, error) {
			return []leases.Lease{
				{
					ID: "lease-pm-psengine-orphan-lease",
					Labels: map[string]string{
						"pod-migrate.io/pod-uid": "dead-pod-uid",
					},
				},
				{
					ID: "lease-pm-psengine-living-lease",
					Labels: map[string]string{
						"pod-migrate.io/pod-uid": "live-pod-uid",
					},
				},
				// Lease created after sweepStart belongs to a restore started after the sweep began (must be skipped)
				{
					ID:        "lease-pm-psengine-after-sweep-lease",
					CreatedAt: futureTime,
					Labels: map[string]string{
						"pod-migrate.io/pod-uid": "dead-pod-uid",
					},
				},
				// Lease without lease-pm- prefix must be ignored (unpinned check)
				{
					ID: "lease-other-service-dead",
					Labels: map[string]string{
						"pod-migrate.io/pod-uid": "dead-pod-uid",
					},
				},
				// Lease with snapshot-name label
				{
					ID: "lease-pm-psengine-snap-orphan",
					Labels: map[string]string{
						"pod-migrate.io/snapshot-name": "dead-snap",
					},
				},
				{
					ID: "lease-pm-psengine-snap-living",
					Labels: map[string]string{
						"pod-migrate.io/snapshot-name": "living-snap",
					},
				},
			}, nil
		},
	}

	livingUIDs := map[string]bool{"live-pod-uid": true}
	livingSnaps := map[string]bool{"living-snap": true}

	sweepStart := time.Now()
	swept, err := r.SweepOrphans(context.Background(), livingUIDs, livingSnaps, sweepStart)
	if err != nil {
		t.Fatalf("SweepOrphans failed: %v", err)
	}

	// Should sweep orphan-lease, orphan-snap, and snap-orphan, but NOT living-lease, living-snap, snap-living, or other-service
	sweptSet := make(map[string]bool)
	for _, s := range swept {
		sweptSet[s] = true
	}
	if !sweptSet["psengine-orphan-lease"] {
		t.Errorf("orphan lease was not swept; swept = %v", swept)
	}
	if !sweptSet["psengine-orphan-snap"] {
		t.Errorf("orphan bundle was not swept; swept = %v", swept)
	}
	if !sweptSet["psengine-snap-orphan"] {
		t.Errorf("snapshot-name orphan lease was not swept; swept = %v", swept)
	}
	if sweptSet["psengine-living-lease"] {
		t.Errorf("living lease was incorrectly swept; swept = %v", swept)
	}
	if sweptSet["psengine-living-snap"] {
		t.Errorf("living bundle was incorrectly swept; swept = %v", swept)
	}
	if sweptSet["psengine-after-sweep-lease"] || deletedLeaseIDs["lease-pm-psengine-after-sweep-lease"] {
		t.Errorf("after-sweep lease was incorrectly swept; swept = %v, deleted = %v", swept, deletedLeaseIDs)
	}
	if sweptSet["psengine-after-sweep-snap"] {
		t.Errorf("after-sweep bundle was incorrectly swept; swept = %v", swept)
	}
	if sweptSet["psengine-snap-living"] {
		t.Errorf("living snapshot-name lease was incorrectly swept; swept = %v", swept)
	}
	if sweptSet["other-service-dead"] || sweptSet["lease-other-service-dead"] || deletedLeaseIDs["lease-other-service-dead"] {
		t.Errorf("non-pm lease was incorrectly swept; swept = %v, deleted = %v", swept, deletedLeaseIDs)
	}
}

func TestSweepOrphans_SkipsInflightRestores(t *testing.T) {
	tmpDir := t.TempDir()

	// Stage an orphaned bundle dir for an in-flight migration
	bundleInflight := filepath.Join(tmpDir, "bundles", "pm-psengine-inflight-mig")
	if err := os.MkdirAll(bundleInflight, 0o755); err != nil {
		t.Fatal(err)
	}

	var cleaned []string
	r := &restorer{
		pmRootDir: tmpDir,
		runcDeleteFn: func(ctx context.Context, cid string) error {
			cleaned = append(cleaned, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			cleaned = append(cleaned, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			cleaned = append(cleaned, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			cleaned = append(cleaned, "lease-delete:"+lid)
			return nil
		},
		listLeasesFn: func(ctx context.Context) ([]leases.Lease, error) {
			return []leases.Lease{
				{
					ID: "lease-pm-psengine-inflight-mig",
					Labels: map[string]string{
						"pod-migrate.io/pod-uid": "inflight-pod-uid",
					},
				},
			}, nil
		},
	}

	// Mark in flight
	r.markInflight("psengine-inflight-mig", "pm-psengine-inflight-mig", "inflight-pod-uid")

	// Neither pod UID nor snapshot is in living sets (simulating restore starting before pod is visible or during sweep)
	livingUIDs := map[string]bool{}
	livingSnaps := map[string]bool{}

	swept, err := r.SweepOrphans(context.Background(), livingUIDs, livingSnaps, time.Time{})
	if err != nil {
		t.Fatalf("SweepOrphans failed: %v", err)
	}

	if len(swept) > 0 {
		t.Fatalf("in-flight restore was swept! swept = %v", swept)
	}
	if len(cleaned) > 0 {
		t.Fatalf("cleanup called on in-flight restore! cleaned = %v", cleaned)
	}
}

func TestRestore_FailureInsidePrepareRootfsCleansUp(t *testing.T) {
	tmpDir := t.TempDir()
	id := "pm-fail-prep"

	prepCalled := false
	var cleaned []string
	var failureCleaned []string
	r := &restorer{
		pmRootDir: tmpDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "c-holder",
				Pid:       1234,
				Spec:      &specs.Spec{Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "sb1"}},
				ImageName: "redis:7",
			}, nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-pod123.slice/c1.scope\n"), nil
		},
		runcDeleteFn: func(ctx context.Context, cid string) error {
			if prepCalled {
				failureCleaned = append(failureCleaned, "runc-delete:"+cid)
			}
			cleaned = append(cleaned, "runc-delete:"+cid)
			return nil
		},
		unmountTreeFn: func(dir string) error {
			if prepCalled {
				failureCleaned = append(failureCleaned, "unmount:"+dir)
			}
			cleaned = append(cleaned, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, key string) error {
			if prepCalled {
				failureCleaned = append(failureCleaned, "snapshot-remove:"+key)
			}
			cleaned = append(cleaned, "snapshot-remove:"+key)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			if prepCalled {
				failureCleaned = append(failureCleaned, "lease-delete:"+lid)
			}
			cleaned = append(cleaned, "lease-delete:"+lid)
			return nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bDir, diffTar string, labels map[string]string) (string, error) {
			prepCalled = true
			return "", errors.New("containerd prepare snapshot failed: OOM")
		},
	}

	in := restoreInputs{
		MigrationName: "fail-prep",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		TargetPodUID:  "pod-uid-456",
		ContainerName: "app",
		CheckpointDir: tmpDir,
		SrcSpec: &specs.Spec{
			Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "src-sb"},
		},
	}

	_, err := r.restore(context.Background(), in)
	if err == nil {
		t.Fatal("expected restore to fail when prepareRootfs fails, got nil")
	}

	// Verify that teardown ran on restore failure after prepareRootfs failed
	if len(failureCleaned) == 0 {
		t.Fatalf("expected failure cleanup defer to execute after prepareRootfs failed, got empty failureCleaned")
	}
	hasUnmount := false
	hasSnapRem := false
	hasLeaseDel := false
	for _, cl := range failureCleaned {
		if cl == "unmount:"+filepath.Join(tmpDir, "bundles", id) {
			hasUnmount = true
		}
		if cl == "snapshot-remove:"+id {
			hasSnapRem = true
		}
		if cl == "lease-delete:lease-"+id {
			hasLeaseDel = true
		}
	}
	if !hasUnmount || !hasSnapRem || !hasLeaseDel {
		t.Fatalf("missing failure teardown actions when prepFn fails; failureCleaned = %v", failureCleaned)
	}
}

func TestPrepareRootfs_RollbackOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	bundleDir := filepath.Join(tmpDir, "bundles", "pm-test-prep-snap")
	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		t.Fatal(err)
	}
	key := "test-prep-snap"

	var cleaned []string
	r := &restorer{
		pmRootDir: tmpDir,
		getImageChainIDFn: func(ctx context.Context, imageName string) (string, error) {
			return "sha256:parent123", nil
		},
		createLeaseFn: func(ctx context.Context, id string, labels map[string]string) error {
			return nil
		},
		prepareSnapshotFn: func(ctx context.Context, k, parent string) ([]mount.Mount, error) {
			return []mount.Mount{{Type: "overlay", Source: "overlay"}}, nil
		},
		mountAllFn: func(mounts []mount.Mount, target string) error {
			return errors.New("mount failed: permission denied")
		},
		unmountTreeFn: func(dir string) error {
			cleaned = append(cleaned, "unmount:"+dir)
			return nil
		},
		removeSnapshotFn: func(ctx context.Context, k string) error {
			cleaned = append(cleaned, "snapshot-remove:"+k)
			return nil
		},
		deleteLeaseFn: func(ctx context.Context, lid string) error {
			cleaned = append(cleaned, "lease-delete:"+lid)
			return nil
		},
	}

	_, err := r.prepareRootfs(context.Background(), "redis:7", key, bundleDir, "", map[string]string{"pod-migrate.io/pod-uid": "uid123"})
	if err == nil {
		t.Fatal("expected prepareRootfs to fail on mount error, got nil")
	}

	// Verify that the internal rollback defer inside prepareRootfs executed:
	// 1. Snapshot removed
	// 2. Lease deleted
	// 3. Bundle dir removed
	hasSnapRem := false
	hasLeaseDel := false
	for _, cl := range cleaned {
		if cl == "snapshot-remove:"+key {
			hasSnapRem = true
		}
		if cl == "lease-delete:lease-"+key {
			hasLeaseDel = true
		}
	}
	if !hasSnapRem {
		t.Errorf("internal prepareRootfs rollback did not remove snapshot: %v", cleaned)
	}
	if !hasLeaseDel {
		t.Errorf("internal prepareRootfs rollback did not delete lease: %v", cleaned)
	}
	if _, err := os.Stat(bundleDir); !os.IsNotExist(err) {
		t.Errorf("internal prepareRootfs rollback did not remove bundleDir: %v", err)
	}
}

func TestRestore_InflightTracking(t *testing.T) {
	tmpDir := t.TempDir()
	migName := "inflight-test"
	id := "pm-" + migName
	podUID := "pod-uid-inflight"

	var observedInflightDuringRestore bool
	var r *restorer
	r = &restorer{
		pmRootDir: tmpDir,
		findContainerFn: func(ctx context.Context, podNS, podName, pUID, containerName string) (*containerRef, error) {
			return &containerRef{
				ID:        "c-holder",
				Pid:       1234,
				Spec:      &specs.Spec{Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "sb1"}},
				ImageName: "redis:7",
			}, nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) {
			return os.CreateTemp(t.TempDir(), "netns-holder")
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/c1.scope\n"), nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bDir, diffTar string, labels map[string]string) (string, error) {
			// Check inflight status while restore is running
			if r.isInflight(migName) && r.isInflight(id) && r.isInflight(podUID) {
				observedInflightDuringRestore = true
			}
			return "", errors.New("abort after checking inflight")
		},
	}

	in := restoreInputs{
		MigrationName: migName,
		PodNamespace:  "default",
		TargetPod:     "web-1",
		TargetPodUID:  podUID,
		ContainerName: "app",
		CheckpointDir: tmpDir,
		SrcSpec: &specs.Spec{
			Annotations: map[string]string{"io.kubernetes.cri.sandbox-id": "src-sb"},
		},
	}

	_, _ = r.restore(context.Background(), in)

	if !observedInflightDuringRestore {
		t.Errorf("expected r.isInflight to be true during restore")
	}
	// After restore completes/fails, inflight keys must be cleared
	if r.isInflight(migName) || r.isInflight(id) || r.isInflight(podUID) {
		t.Errorf("inflight keys were not cleared after restore: mig=%v, id=%v, uid=%v",
			r.isInflight(migName), r.isInflight(id), r.isInflight(podUID))
	}
}

func TestCleanup_RuncDoesNotExistTolerance(t *testing.T) {
	tmpDir := t.TempDir()
	r := &restorer{
		pmRootDir: tmpDir,
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "container does not exist", errors.New("exit status 1")
		},
	}

	// runc delete -f returning "does not exist" should be tolerated
	notes, err := r.cleanup(context.Background(), "no-such-container", "", "", false)
	if err != nil {
		t.Errorf("expected cleanup to tolerate missing runc container, got: %v", err)
	}
	_ = notes

	// Signal returning "does not exist" should also be tolerated
	if err := r.Signal(context.Background(), "no-such-container", 15); err != nil {
		t.Errorf("expected Signal to tolerate missing runc container, got: %v", err)
	}
}

func TestRestorer_IsRunning(t *testing.T) {
	rRunning := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return `{"status": "running", "pid": 1234}`, nil
		},
	}
	running, err := rRunning.IsRunning(context.Background(), "running-app")
	if err != nil || !running {
		t.Errorf("expected running=true, nil; got %v, %v", running, err)
	}

	rStopped := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return `{"status": "stopped", "pid": 0}`, nil
		},
	}
	stopped, err := rStopped.IsRunning(context.Background(), "stopped-app")
	if err != nil || stopped {
		t.Errorf("expected running=false, nil; got %v, %v", stopped, err)
	}

	rMissing := &restorer{
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "container does not exist", errors.New("exit status 1")
		},
	}
	missing, err := rMissing.IsRunning(context.Background(), "missing-app")
	if err != nil || missing {
		t.Errorf("expected running=false, nil; got %v, %v", missing, err)
	}
}
