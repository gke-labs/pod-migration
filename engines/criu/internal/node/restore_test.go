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

func init() {
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
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
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
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
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
