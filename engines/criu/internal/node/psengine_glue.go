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
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// PSEngineCheckpoint drives the kubelet ContainerCheckpoint API and validates
// the resulting tar.
func (r *Node) PSEngineCheckpoint(ctx context.Context, ns, pod, container string) (string, error) {
	res, err := r.Kubelet.checkpoint(ctx, ns, pod, container)
	if err != nil {
		if errors.Is(err, ErrContainerdCachedCRIU) {
			if demoteErr := r.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); demoteErr != nil {
				logf.FromContext(ctx).Error(demoteErr, "failed to annotate node unready after containerd cached CRIU failure", "node", r.NodeName)
			}
		}
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
func (r *Node) PSEngineSourceInfo(ctx context.Context, ns, pod, podUID, container string) ([]byte, string, error) {
	ref, err := r.Restorer.findContainer(ctx, ns, pod, podUID, container)
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

// PSEngineFreezeContainer freezes (pauses) the source container task via
// containerd before the checkpoint (C4 Frozen fencing mode, #148).
func (r *Node) PSEngineFreezeContainer(ctx context.Context, ns, pod, podUID, container string) error {
	ref, err := r.Restorer.findContainer(ctx, ns, pod, podUID, container)
	if err != nil {
		return err
	}
	cs, err := r.Restorer.listContainers(ctx)
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
		st, err := task.Status(ctx)
		if err == nil && st.Status == containerd.Paused {
			return nil
		}
		if err := task.Pause(ctx); err != nil {
			return fmt.Errorf("task pause: %w", err)
		}
		return nil
	}
	return fmt.Errorf("container %s not found in containerd", ref.ID)
}

// PSEngineUnfreezeContainer thaws (resumes) the source container task via
// containerd. Resume uses a fresh context, not caller ctx, so caller
// cancellation or deadlines never leave the task frozen (#106, #148).
func (r *Node) PSEngineUnfreezeContainer(ctx context.Context, ns, pod, podUID, container string) error {
	// Resume with a fresh, bounded context so a cancelled or timed-out
	// caller context doesn't fail the resume call (#106, #148).
	resumeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ref, err := r.Restorer.findContainer(resumeCtx, ns, pod, podUID, container)
	if err != nil {
		if errors.Is(err, ErrContainerNotFound) || errdefs.IsNotFound(err) {
			// If the container is already gone, it doesn't need to be unfrozen.
			return nil
		}
		return fmt.Errorf("find container for unfreeze: %w", err)
	}

	cs, err := r.Restorer.listContainers(resumeCtx)
	if err != nil {
		return fmt.Errorf("containerd list: %w", err)
	}
	for _, c := range cs {
		if c.ID() != ref.ID {
			continue
		}
		task, err := c.Task(resumeCtx, nil)
		if err != nil {
			if errdefs.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("task lookup for %s: %w", ref.ID, err)
		}
		st, err := task.Status(resumeCtx)
		if err == nil && (st.Status == containerd.Running || st.Status == containerd.Stopped) {
			return nil
		}
		if err != nil && errdefs.IsNotFound(err) {
			return nil
		}
		if err := task.Resume(resumeCtx); err != nil {
			if errdefs.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("task resume: %w", err)
		}
		return nil
	}
	return nil
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
