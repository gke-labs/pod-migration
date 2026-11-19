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
	"testing"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
)

type fakeGlueTask struct {
	containerd.Task
	pid            uint32
	status         containerd.ProcessStatus
	statusErr      error
	pauseCalled    bool
	pauseErr       error
	resumeCalled   bool
	resumeErr      error
	resumeDeadline time.Time
}

func (t *fakeGlueTask) Pid() uint32 { return t.pid }

func (t *fakeGlueTask) Status(context.Context) (containerd.Status, error) {
	return containerd.Status{Status: t.status}, t.statusErr
}

func (t *fakeGlueTask) Pause(context.Context) error {
	t.pauseCalled = true
	return t.pauseErr
}

func (t *fakeGlueTask) Resume(ctx context.Context) error {
	t.resumeCalled = true
	if d, ok := ctx.Deadline(); ok {
		t.resumeDeadline = d
	}
	return t.resumeErr
}

func TestPSEngineFreezeContainer_PausesTaskAndIdempotent(t *testing.T) {
	ctx := context.Background()

	// 1. Task already paused: does not call Pause again (idempotent).
	taskPaused := &fakeGlueTask{pid: 1234, status: containerd.Paused}
	cPaused := &fakeContainer{
		id:   "c-paused",
		task: taskPaused,
	}
	var observedFreezeUID string
	nodePaused := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				observedFreezeUID = podUID
				return &containerRef{ID: "c-paused", Pid: 1234}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cPaused}, nil
			},
		},
	}
	if err := nodePaused.PSEngineFreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("unexpected error on already paused task: %v", err)
	}
	if observedFreezeUID != "pod-uid-42" {
		t.Fatalf("expected podUID 'pod-uid-42', got %q", observedFreezeUID)
	}
	if taskPaused.pauseCalled {
		t.Fatal("pause must not be called when task is already paused")
	}

	// 2. Task running: calls Pause.
	taskRunning := &fakeGlueTask{pid: 5678, status: containerd.Running}
	cRunning := &fakeContainer{
		id:   "c-running",
		task: taskRunning,
	}
	nodeRunning := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return &containerRef{ID: "c-running", Pid: 5678}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cRunning}, nil
			},
		},
	}
	if err := nodeRunning.PSEngineFreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("unexpected error on running task: %v", err)
	}
	if !taskRunning.pauseCalled {
		t.Fatal("pause must be called when task is running")
	}
}

func TestPSEngineUnfreezeContainer_ContainerdUnreachableReturnsError(t *testing.T) {
	ctx := context.Background()

	// When containerd returns connection refused during findContainer, Unfreeze must return an error (not nil!).
	nodeUnreachable := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return nil, errors.New("containerd list: connection refused")
			},
		},
	}
	err := nodeUnreachable.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr")
	if err == nil {
		t.Fatal("expected error when containerd is unreachable, got nil")
	}
}

func TestPSEngineUnfreezeContainer_NotFoundReturnsNil(t *testing.T) {
	ctx := context.Background()

	// When container is genuinely not found, Unfreeze treats it as already gone and returns nil.
	nodeNotFound := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return nil, ErrContainerNotFound
			},
		},
	}
	if err := nodeNotFound.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("expected nil when container not found, got: %v", err)
	}

	// Also when findContainer returns errdefs.ErrNotFound.
	nodeErrdefsNotFound := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return nil, errdefs.ErrNotFound
			},
		},
	}
	if err := nodeErrdefsNotFound.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("expected nil when container is errdefs.ErrNotFound, got: %v", err)
	}
}

func TestPSEngineUnfreezeContainer_TaskStatusRunningOrStoppedReturnsNil(t *testing.T) {
	ctx := context.Background()

	// Running task does not need Resume.
	taskRunning := &fakeGlueTask{pid: 100, status: containerd.Running}
	cRunning := &fakeContainer{id: "c-run", task: taskRunning}
	nodeRunning := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return &containerRef{ID: "c-run", Pid: 100}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cRunning}, nil
			},
		},
	}
	if err := nodeRunning.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if taskRunning.resumeCalled {
		t.Fatal("resume must not be called when task is already running")
	}

	// Stopped task does not need Resume.
	taskStopped := &fakeGlueTask{pid: 101, status: containerd.Stopped}
	cStopped := &fakeContainer{id: "c-stop", task: taskStopped}
	nodeStopped := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return &containerRef{ID: "c-stop", Pid: 101}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cStopped}, nil
			},
		},
	}
	if err := nodeStopped.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if taskStopped.resumeCalled {
		t.Fatal("resume must not be called when task is stopped")
	}
}

func TestPSEngineUnfreezeContainer_PausedTaskResumesWithBoundedContext(t *testing.T) {
	// Cancel the caller's context beforehand to prove Resume uses a fresh bounded context,
	// not the cancelled caller context.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	taskPaused := &fakeGlueTask{pid: 200, status: containerd.Paused}
	cPaused := &fakeContainer{id: "c-paused", task: taskPaused}
	var observedUnfreezeUID string
	nodePaused := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				if err := ctx.Err(); err != nil {
					t.Fatalf("findContainer called with error context: %v", err)
				}
				if dl, ok := ctx.Deadline(); !ok || dl.IsZero() {
					t.Fatal("findContainer must be called with a bounded context with a deadline")
				}
				observedUnfreezeUID = podUID
				return &containerRef{ID: "c-paused", Pid: 200}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cPaused}, nil
			},
		},
	}

	before := time.Now()
	if err := nodePaused.PSEngineUnfreezeContainer(canceledCtx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observedUnfreezeUID != "pod-uid-42" {
		t.Fatalf("expected podUID 'pod-uid-42', got %q", observedUnfreezeUID)
	}
	if !taskPaused.resumeCalled {
		t.Fatal("resume must be called when task is paused")
	}
	if taskPaused.resumeDeadline.IsZero() {
		t.Fatal("resume must use a context with deadline")
	}
	remaining := time.Until(taskPaused.resumeDeadline)
	if remaining <= 0 || remaining > 16*time.Second {
		t.Fatalf("expected ~15s timeout on resume context, got remaining %v (since %v)", remaining, before)
	}
}

func TestPSEngineUnfreezeContainer_TaskLookupErrors(t *testing.T) {
	ctx := context.Background()

	// Task lookup error that is errdefs.ErrNotFound returns nil (task already gone).
	cTaskNotFound := &fakeContainer{id: "c-tnf", taskErr: errdefs.ErrNotFound}
	nodeTaskNotFound := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return &containerRef{ID: "c-tnf", Pid: 300}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cTaskNotFound}, nil
			},
		},
	}
	if err := nodeTaskNotFound.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err != nil {
		t.Fatalf("expected nil when task is ErrNotFound, got: %v", err)
	}

	// Task lookup error that is NOT not-found returns an error.
	cTaskErr := &fakeContainer{id: "c-terr", taskErr: errors.New("rpc error: connection reset")}
	nodeTaskErr := &Node{
		Restorer: &restorer{
			findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
				return &containerRef{ID: "c-terr", Pid: 301}, nil
			},
			listContainersFn: func(ctx context.Context) ([]containerd.Container, error) {
				return []containerd.Container{cTaskErr}, nil
			},
		},
	}
	if err := nodeTaskErr.PSEngineUnfreezeContainer(ctx, "ns", "pod", "pod-uid-42", "ctr"); err == nil {
		t.Fatal("expected error on non-notfound task lookup error, got nil")
	}
}
