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

// psengine glue: exported adapters over the node worker's mechanism layer
// (kubelet checkpoint client, containerd access, sandbox-sibling restorer) for
// the CRIU snapshot engine (internal/psengine). Kept thin on purpose — the
// engine's pipeline logic lives in psengine and is unit-tested there; this
// file only exposes the node-touching verbs.

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// PSEngineCheckpoint drives the kubelet ContainerCheckpoint API and validates
// the resulting tar.
func (r *Node) PSEngineCheckpoint(ctx context.Context, ns, pod, container string) (string, error) {
	res, err := r.Kubelet.checkpoint(ctx, ns, pod, container)
	if err != nil {
		return "", err
	}
	if err := validateCheckpointTarFile(res.TarPath); err != nil {
		return "", fmt.Errorf("tar invalid: %w", err)
	}
	return res.TarPath, nil
}

// PSEngineSourceInfo captures the source container's OCI runtime spec and
// image name — they travel with the archive (the restore node has no source
// container to inspect).
func (r *Node) PSEngineSourceInfo(ctx context.Context, ns, pod, container string) ([]byte, string, error) {
	ref, err := r.Restorer.findContainer(ctx, ns, pod, container)
	if err != nil {
		return nil, "", err
	}
	if ref.Spec == nil {
		return nil, "", fmt.Errorf("source container %s/%s/%s has no OCI spec", ns, pod, container)
	}
	specJSON, err := json.Marshal(ref.Spec)
	if err != nil {
		return nil, "", err
	}
	return specJSON, ref.ImageName, nil
}

// PSEngineStopContainer enforces postCheckpoint:stop after the verified
// upload: SIGKILL the container's task via containerd (the kubelet API
// checkpoints without stopping).
func (r *Node) PSEngineStopContainer(ctx context.Context, ns, pod, container string) error {
	ref, err := r.Restorer.findContainer(ctx, ns, pod, container)
	if err != nil {
		return err
	}
	cs, err := r.Restorer.cclient.Containers(ctx)
	if err != nil {
		return fmt.Errorf("containerd list: %w", err)
	}
	for _, c := range cs {
		if c.ID() != ref.ID {
			continue
		}
		task, err := c.Task(ctx, nil)
		if err != nil {
			return fmt.Errorf("task lookup for %s: %w", ref.ID, err)
		}
		exitCh, err := task.Wait(ctx)
		if err != nil {
			return fmt.Errorf("task wait setup: %w", err)
		}
		if err := task.Kill(ctx, syscall.SIGKILL, containerd.WithKillAll); err != nil {
			return fmt.Errorf("task kill: %w", err)
		}
		select {
		case <-exitCh:
			return nil
		case <-time.After(15 * time.Second):
			return fmt.Errorf("container %s did not exit within 15s of SIGKILL", ref.ID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("container %s not found in containerd", ref.ID)
}

// PSEngineExtract unpacks a kubelet checkpoint tar with the node worker's safe
// untar and validates the extracted entries.
func (r *Node) PSEngineExtract(tarPath, dstDir string) error {
	entries, err := extractCheckpoint(tarPath, dstDir)
	if err != nil {
		return err
	}
	return ValidateCheckpointTar(entries)
}

// PSEngineRestoreInputs mirrors psengine.RestoreInputs (kept separate to
// avoid a node->psengine import; cmd/agent adapts between them).
type PSEngineRestoreInputs struct {
	SnapshotName  string
	PodNamespace  string
	TargetPod     string
	ContainerName string
	SourcePodUID  string
	TargetPodUID  string
	CheckpointDir string
	SpecJSON      []byte
	ImageName     string
}

// PSEngineRestore performs the sandbox-sibling runc restore into the target
// pod (the holder keeps the sandbox alive; readiness passes only when the
// restored process serves).
func (r *Node) PSEngineRestore(ctx context.Context, in PSEngineRestoreInputs) (int, int64, error) {
	var srcSpec specs.Spec
	if err := json.Unmarshal(in.SpecJSON, &srcSpec); err != nil {
		return 0, 0, fmt.Errorf("source spec decode: %w", err)
	}
	out, err := r.Restorer.restore(ctx, restoreInputs{
		MigrationName: "psengine-" + in.SnapshotName,
		PodNamespace:  in.PodNamespace,
		TargetPod:     in.TargetPod,
		ContainerName: in.ContainerName,
		SourcePodUID:  in.SourcePodUID,
		TargetPodUID:  in.TargetPodUID,
		CheckpointDir: in.CheckpointDir,
		SrcSpec:       &srcSpec,
		ImageName:     in.ImageName,
	})
	if err != nil {
		msg := err.Error()
		if out != nil && len(out.Notes) > 0 {
			msg += " | " + truncate(joinNotes(out.Notes), 1500)
		}
		return 0, 0, fmt.Errorf("%s", msg)
	}
	return out.Pid, out.DurationMs, nil
}

// PSEngineCPUFamily reports the node's CPU model for the archive manifest
// (restore-side compatibility guard). Empty when unknown — absent, never
// invented.
func PSEngineCPUFamily() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
