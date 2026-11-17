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
