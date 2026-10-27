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
	"fmt"
	"path"
	"regexp"
	"strings"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// anonVolRe matches a containerd container-scoped anonymous image-volume bind
// source (a Dockerfile `VOLUME`, e.g. redis:7-alpine's /data):
//
//	/var/lib/containerd/.../containers/<containerID>/volumes/<volID>
//
// The <containerID> is the source container's — it does not exist on the target,
// so a straight restore fails the mount. In warm migration the authoritative
// state is the checkpointed memory; the anonymous volume is ephemeral scratch,
// so it is re-created fresh on the target.
var anonVolRe = regexp.MustCompile(`/containers/[^/]+/volumes/([^/]+)`)

// AdaptParams carries everything needed to turn the source container's OCI
// runtime spec into a bundle spec for the sibling container restored inside
// the target pod's sandbox.
type AdaptParams struct {
	// RootfsPath is the absolute host path of the prepared rootfs
	// (image snapshot + rootfs-diff applied).
	RootfsPath string
	// HolderPid is the host pid of the target pod's holder container — its
	// net/ipc/uts namespaces are the target sandbox namespaces.
	HolderPid int
	// CgroupsPath is the cgroupfs-relative path for the restored scope,
	// derived from the holder's /proc/<pid>/cgroup pod slice.
	CgroupsPath string
	// SourcePodUID / TargetPodUID remap kubelet-projected mount sources
	// (/var/lib/kubelet/pods/<uid>/...).
	SourcePodUID string
	TargetPodUID string
	// SourceSandboxID / TargetSandboxID remap containerd sandbox-scoped mount
	// sources (etc-hosts, resolv.conf, shm under .../sandboxes/<id>/...).
	SourceSandboxID string
	TargetSandboxID string
	// ExtraRewrites are literal old->new substring pairs applied to mount
	// sources after the UID/sandbox swaps — e.g. the per-pod random suffix of
	// the projected SA-token volume (kube-api-access-XXXXX), which the UID
	// remap cannot fix.
	ExtraRewrites [][2]string
	// AnonVolumeDir is the target-side directory under which fresh scratch dirs
	// are minted for anonymous image volumes (Dockerfile VOLUME). Empty leaves
	// such mounts untouched. The caller creates the rewritten dirs on disk
	// before restore.
	AnonVolumeDir string
	// TargetSpec is the OCI spec of the destination container created by the kubelet.
	TargetSpec *specs.Spec
}

// AdaptSpec rewrites spec in place. It returns the list of rewrites performed
// and an error if a mount source still references the source pod after
// remapping (a restore would then silently read source-pod state — refuse
// instead).
func AdaptSpec(spec *specs.Spec, p AdaptParams) ([]string, error) {
	var notes []string

	if spec.Root == nil || spec.Linux == nil {
		return nil, fmt.Errorf("source OCI spec lacks root/linux sections — not a CRI container spec")
	}
	spec.Root.Path = p.RootfsPath
	spec.Root.Readonly = false

	// Namespaces: join the target sandbox's net/ipc/uts; keep pid+mount
	// private (CRIU restores them from the images).
	nsPath := func(kind string) string { return fmt.Sprintf("/proc/%d/ns/%s", p.HolderPid, kind) }
	var out []specs.LinuxNamespace
	for _, ns := range spec.Linux.Namespaces {
		switch ns.Type {
		case specs.NetworkNamespace:
			ns.Path = nsPath("net")
		case specs.IPCNamespace:
			ns.Path = nsPath("ipc")
		case specs.UTSNamespace:
			ns.Path = nsPath("uts")
		case specs.PIDNamespace, specs.MountNamespace:
			ns.Path = ""
		}
		out = append(out, ns)
	}
	spec.Linux.Namespaces = out
	notes = append(notes, fmt.Sprintf("joined sandbox namespaces of pid %d (net/ipc/uts)", p.HolderPid))

	spec.Linux.CgroupsPath = p.CgroupsPath
	notes = append(notes, "cgroupsPath="+p.CgroupsPath)

	spec.Hooks = nil
	notes = append(notes, "scrubbed OCI runtime hooks")

	var targetMounts map[string]specs.Mount
	if p.TargetSpec != nil {
		if spec.Process != nil && p.TargetSpec.Process != nil {
			spec.Process.Capabilities = p.TargetSpec.Process.Capabilities
			spec.Process.User = p.TargetSpec.Process.User
			spec.Process.NoNewPrivileges = p.TargetSpec.Process.NoNewPrivileges
			spec.Process.ApparmorProfile = p.TargetSpec.Process.ApparmorProfile
			spec.Process.SelinuxLabel = p.TargetSpec.Process.SelinuxLabel
			spec.Process.OOMScoreAdj = p.TargetSpec.Process.OOMScoreAdj
			spec.Process.Rlimits = p.TargetSpec.Process.Rlimits
		}
		if p.TargetSpec.Linux != nil {
			spec.Linux.Seccomp = p.TargetSpec.Linux.Seccomp
			spec.Linux.MaskedPaths = p.TargetSpec.Linux.MaskedPaths
			spec.Linux.ReadonlyPaths = p.TargetSpec.Linux.ReadonlyPaths
			spec.Linux.Devices = p.TargetSpec.Linux.Devices
			spec.Linux.Resources = p.TargetSpec.Linux.Resources
			spec.Linux.Sysctl = p.TargetSpec.Linux.Sysctl
		}

		targetMounts = make(map[string]specs.Mount)
		for _, tm := range p.TargetSpec.Mounts {
			if tm.Destination == "/pmhold" || tm.Destination == "/pminfo" {
				continue
			}
			targetMounts[tm.Destination] = tm
		}
	}

	// Remap external bind-mount sources source-pod -> target-pod.
	for i := range spec.Mounts {
		m := &spec.Mounts[i]
		orig := m.Source
		if p.SourcePodUID != "" && p.TargetPodUID != "" {
			m.Source = strings.ReplaceAll(m.Source, p.SourcePodUID, p.TargetPodUID)
		}
		if p.SourceSandboxID != "" && p.TargetSandboxID != "" {
			m.Source = strings.ReplaceAll(m.Source, p.SourceSandboxID, p.TargetSandboxID)
		}
		for _, rw := range p.ExtraRewrites {
			if rw[0] != "" {
				m.Source = strings.ReplaceAll(m.Source, rw[0], rw[1])
			}
		}
		// Anonymous image volume (Dockerfile VOLUME): the source is the source
		// container's volumes dir, absent on the target. Re-point it at fresh
		// target scratch when AnonVolumeDir is set.
		if p.TargetSpec != nil {
			if p.AnonVolumeDir != "" && anonVolRe.MatchString(m.Source) {
				mm := anonVolRe.FindStringSubmatch(m.Source)
				m.Source = path.Join(p.AnonVolumeDir, mm[1])
			} else if tgtMount, ok := targetMounts[m.Destination]; ok {
				m.Source = tgtMount.Source
				m.Type = tgtMount.Type
				m.Options = tgtMount.Options
			} else {
				return notes, fmt.Errorf("mount %s (source %s) is not permitted by destination container spec", m.Destination, m.Source)
			}
		} else {
			if p.AnonVolumeDir != "" {
				if mm := anonVolRe.FindStringSubmatch(m.Source); mm != nil {
					m.Source = path.Join(p.AnonVolumeDir, mm[1])
				}
			}
			if m.Type == "bind" {
				if !(strings.HasPrefix(m.Source, "/var/lib/kubelet/pods/") || strings.HasPrefix(m.Source, "/var/lib/containerd/") || (p.AnonVolumeDir != "" && strings.HasPrefix(m.Source, p.AnonVolumeDir))) {
					return notes, fmt.Errorf("mount %s (source %s) is not permitted in legacy mode", m.Destination, m.Source)
				}
			}
		}

		// Container-scoped paths (termination-log lives under .../containers/
		// <container-name>/<attempt>) survive the UID swap unchanged in name.
		if m.Source != orig {
			notes = append(notes, fmt.Sprintf("mount %s: %s -> %s", m.Destination, orig, m.Source))
		}
		if p.SourcePodUID != "" && strings.Contains(m.Source, p.SourcePodUID) {
			return notes, fmt.Errorf("mount %s still references source pod uid after remap: %s", m.Destination, m.Source)
		}
	}

	// The restored process must not be re-hooked to containerd's shim
	// lifecycle; scrub CRI bookkeeping that no longer holds.
	if spec.Annotations != nil {
		delete(spec.Annotations, "io.kubernetes.cri.sandbox-id")
		delete(spec.Annotations, "io.kubernetes.cri.sandbox-name")
		delete(spec.Annotations, "io.kubernetes.cri.sandbox-uid")
	}
	return notes, nil
}

// PodSliceFromProcCgroup derives the cgroupfs-relative path of the pod slice
// from a holder container's /proc/<pid>/cgroup line (cgroup v2: "0::<path>").
// The holder's own scope (last component) is stripped so the restored scope
// becomes a sibling under the same pod slice.
func PodSliceFromProcCgroup(procCgroup string) (string, error) {
	for _, line := range strings.Split(strings.TrimSpace(procCgroup), "\n") {
		rest, ok := strings.CutPrefix(line, "0::")
		if !ok {
			continue
		}
		p := strings.TrimPrefix(strings.TrimSpace(rest), "/")
		if p == "" {
			return "", fmt.Errorf("holder is in the root cgroup — cannot derive pod slice")
		}
		return path.Dir(p), nil
	}
	return "", fmt.Errorf("no cgroup v2 (0::) line in %q", procCgroup)
}

// ValidateCheckpointTar checks the entry list of a checkpoint tar.
// It accepts either a kubelet checkpoint tar (spec.dump + config.dump +
// checkpoint/*.img) or a raw runc delta tar (*.img at root).
func ValidateCheckpointTar(entries []string) error {
	var hasSpec, hasConfig, hasKubeletPages, hasKubeletPstree bool
	var hasRawPages, hasRawPstree bool

	for _, e := range entries {
		switch {
		case e == "spec.dump":
			hasSpec = true
		case e == "config.dump":
			hasConfig = true
		case strings.HasPrefix(e, "checkpoint/pages-"):
			hasKubeletPages = true
		case e == "checkpoint/pstree.img":
			hasKubeletPstree = true
		case strings.HasPrefix(e, "pages-"):
			hasRawPages = true
		case e == "pstree.img":
			hasRawPstree = true
		}
	}

	// Kubelet checkpoint API format
	if hasSpec || hasConfig || hasKubeletPages || hasKubeletPstree {
		var missing []string
		if !hasSpec {
			missing = append(missing, "spec.dump")
		}
		if !hasConfig {
			missing = append(missing, "config.dump")
		}
		if !hasKubeletPages {
			missing = append(missing, "checkpoint/pages-*.img")
		}
		if !hasKubeletPstree {
			missing = append(missing, "checkpoint/pstree.img")
		}
		if len(missing) > 0 {
			return fmt.Errorf("checkpoint tar invalid, missing: %s", strings.Join(missing, ", "))
		}
		return nil
	}

	// Raw runc delta format
	var missing []string
	if !hasRawPages {
		missing = append(missing, "pages-*.img")
	}
	if !hasRawPstree {
		missing = append(missing, "pstree.img")
	}
	if len(missing) > 0 {
		return fmt.Errorf("checkpoint tar invalid, missing: %s", strings.Join(missing, ", "))
	}
	return nil
}
