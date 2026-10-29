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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/leases"
	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/pkg/archive"
	"github.com/containerd/containerd/v2/pkg/archive/compression"
	"github.com/opencontainers/image-spec/identity"
	specs "github.com/opencontainers/runtime-spec/specs-go"
)

// Host paths. /var/lib/pod-migrate is a Bidirectional hostPath mount at the
// same path inside the agent, so paths written into config.json are valid in
// both mount namespaces; overlay mounts made by the agent propagate to the
// host where runc (exec'd via nsenter into host namespaces) consumes them.
const (
	pmRoot        = "/var/lib/pod-migrate"
	criuBundleDir = pmRoot + "/criu-bundle"
	runcStateRoot = "/run/pod-migrate/runc"
	criuConfDir   = "/etc/criu" // hostPath mount; runc's criu reads runc.conf here
)

// restorer drives the sandbox-sibling container restore.
type restorer struct {
	cclient *containerd.Client
	// execHost runs a command in the host namespaces (nsenter -t 1 -m -p).
	// Swappable in tests.
	execHost func(ctx context.Context, env []string, name string, args ...string) (string, error)
}

func newRestorer(c *containerd.Client) *restorer {
	return &restorer{cclient: c, execHost: nsenterExec}
}

// nsenterExec executes cmd in the host mount+pid namespaces (agent has
// hostPID, so target pid 1 is the host init). Output goes through a file, not
// pipes: `runc restore --detach` leaves the restored process holding the
// inherited stderr, so CombinedOutput would block forever after a successful
// restore. With *os.File stdio, Wait returns on runc exit.
func nsenterExec(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	full := append([]string{"-t", "1", "-m", "-p", "--", name}, args...)
	cmd := exec.CommandContext(cctx, "nsenter", full...)
	cmd.Env = append(os.Environ(), env...)
	f, err := os.CreateTemp("", "pm-exec-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	cmd.Stdout, cmd.Stderr = f, f
	runErr := cmd.Run()
	b, _ := os.ReadFile(f.Name())
	if cctx.Err() != nil {
		return string(b), fmt.Errorf("timed out after 120s: %w", cctx.Err())
	}
	return string(b), runErr
}

func (r *restorer) execRestore(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	// runc restore holds inherited stderr, so we redirect to a file.
	// We keep this file so it acts as a container-stdout log for the restored process.
	full := append([]string{"-t", "1", "-m", "-p", "--", name}, args...)
	cmd := exec.CommandContext(cctx, "nsenter", full...)
	cmd.Env = append(os.Environ(), env...)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return "", err
	}
	defer f.Close()
	cmd.Stdout, cmd.Stderr = f, f
	runErr := cmd.Run()
	b, _ := os.ReadFile(logPath)
	if cctx.Err() != nil {
		return string(b), fmt.Errorf("timed out after 120s: %w", cctx.Err())
	}
	return string(b), runErr
}

// containerRef identifies a CRI container/sandbox pairing in containerd.
type containerRef struct {
	ID        string
	SandboxID string
	Pid       int
	Spec      *specs.Spec
	ImageName string
}

// findContainer locates the CRI container for pod/container by containerd
// labels (namespace k8s.io is set on the client).
func (r *restorer) findContainer(ctx context.Context, podNS, podName, containerName string) (*containerRef, error) {
	cs, err := r.cclient.Containers(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerd list: %w", err)
	}
	for _, c := range cs {
		labels, err := c.Labels(ctx)
		if err != nil {
			continue
		}
		if labels["io.kubernetes.pod.namespace"] != podNS ||
			labels["io.kubernetes.pod.name"] != podName ||
			labels["io.kubernetes.container.name"] != containerName ||
			labels["io.cri-containerd.kind"] != "container" {
			continue
		}
		ref := &containerRef{ID: c.ID()}
		if spec, err := c.Spec(ctx); err == nil {
			ref.Spec = spec
			ref.SandboxID = spec.Annotations["io.kubernetes.cri.sandbox-id"]
		}
		if info, err := c.Info(ctx); err == nil {
			ref.ImageName = info.Image
		}
		if task, err := c.Task(ctx, nil); err == nil {
			ref.Pid = int(task.Pid())
		}
		return ref, nil
	}
	return nil, fmt.Errorf("container %s/%s/%s not found in containerd", podNS, podName, containerName)
}

// prepareRootfs makes a fresh writable snapshot of the app image and applies
// the checkpoint's rootfs-diff on top. Returns the mounted rootfs path.
func (r *restorer) prepareRootfs(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
	img, err := r.cclient.GetImage(ctx, imageName)
	if err != nil {
		return "", fmt.Errorf("get image %s: %w", imageName, err)
	}
	diffIDs, err := img.RootFS(ctx)
	if err != nil {
		return "", err
	}
	parent := identity.ChainID(diffIDs).String()
	sn := r.cclient.SnapshotService("overlayfs")
	_ = sn.Remove(ctx, key) // idempotent retry: a failed prior attempt may have left the key

	// Create a lease to protect the snapshot from containerd GC.
	lm := r.cclient.LeasesService()
	_ = lm.Delete(ctx, leases.Lease{ID: "lease-" + key})
	lease, err := lm.Create(ctx, leases.WithID("lease-"+key))
	if err != nil {
		return "", fmt.Errorf("create containerd lease: %w", err)
	}
	ctx = leases.WithLease(ctx, lease.ID)

	mounts, err := sn.Prepare(ctx, key, parent)
	if err != nil {
		return "", fmt.Errorf("snapshot prepare: %w", err)
	}
	rootfs := filepath.Join(bundleDir, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		return "", err
	}
	if err := mount.All(mounts, rootfs); err != nil {
		return "", fmt.Errorf("mount rootfs: %w", err)
	}
	if _, err := os.Stat(rootfsDiffTar); err == nil {
		f, err := os.Open(rootfsDiffTar)
		if err != nil {
			return rootfs, err
		}
		defer f.Close()
		// containerd 2.x writes rootfs-diff.tar gzip-compressed despite the
		// name; DecompressStream auto-detects plain/gz/zstd.
		dec, err := compression.DecompressStream(f)
		if err != nil {
			return rootfs, fmt.Errorf("rootfs-diff decompress: %w", err)
		}
		defer dec.Close()
		if _, err := archive.Apply(ctx, rootfs, dec); err != nil {
			return rootfs, fmt.Errorf("apply rootfs-diff: %w", err)
		}
	}
	// Scratch images ship no /tmp — CRIU's pivot-root bookkeeping needs a
	// writable temporary directory inside the new root.
	for _, d := range []string{"tmp", "run"} {
		if err := os.MkdirAll(filepath.Join(rootfs, d), 0o1777); err != nil {
			return rootfs, err
		}
	}
	return rootfs, nil
}

type restoreInputs struct {
	MigrationName string
	PodNamespace  string
	TargetPod     string
	ContainerName string
	SourcePodUID  string
	TargetPodUID  string
	CheckpointDir string // extracted kubelet tar (contains checkpoint/, rootfs-diff.tar)
	PreCopy       bool
	SrcSpec       *specs.Spec
	ImageName     string
}

type restoreOutcome struct {
	Pid                int
	DurationMs         int64
	CriuVersion        string
	FileValidationUsed bool
	TimeNsDistinct     *bool // nil = not determinable
	Notes              []string
}

// restore performs the full sibling-restore into the target pod's sandbox.
func (r *restorer) restore(ctx context.Context, in restoreInputs) (*restoreOutcome, error) {
	start := time.Now()
	out := &restoreOutcome{}

	if in.SrcSpec == nil {
		return out, fmt.Errorf("no source OCI spec in the bundle")
	}
	srcSandboxID := in.SrcSpec.Annotations["io.kubernetes.cri.sandbox-id"]
	// The target pod's app container runs the seeded holder binary and keeps
	// the source container's name. The kubelet may report the pod Running a
	// beat before containerd's task pid is queryable — wait for it briefly
	// rather than failing the restore on a startup race.
	var holder *containerRef
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		holder, err = r.findContainer(ctx, in.PodNamespace, in.TargetPod, in.ContainerName)
		if err == nil && holder.Pid != 0 {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return out, fmt.Errorf("target holder container not found within 30s: %w", err)
			}
			return out, fmt.Errorf("target holder task pid not up within 30s — sandbox slow to start")
		}
		time.Sleep(250 * time.Millisecond)
	}

	// Pod cgroup slice of the target, from the holder's own cgroup.
	procCg, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", holder.Pid))
	if err != nil {
		return out, fmt.Errorf("read holder cgroup: %w", err)
	}
	podSlice, err := PodSliceFromProcCgroup(string(procCg))
	if err != nil {
		return out, err
	}

	id := "pm-" + in.MigrationName
	bundleDir := filepath.Join(pmRoot, "bundles", id)

	// Clean up any leftover runc container from a previous failed restore
	// attempt.
	if delOut, delErr := r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id); delErr != nil {
		out.Notes = append(out.Notes, fmt.Sprintf("stale-container pre-delete failed (restore will likely conflict): %s: %s",
			delErr, truncate(delOut, 200)))
	}

	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return out, err
	}

	imgName := holder.ImageName
	if imgName == "" {
		imgName = in.ImageName
	}
	rootfs, err := r.prepareRootfs(ctx, imgName, id, bundleDir, filepath.Join(in.CheckpointDir, "rootfs-diff.tar"))
	if err != nil {
		return out, err
	}

	// The projected SA-token volume dir carries a per-pod random suffix
	// (kube-api-access-XXXXX) that survives the UID swap — map source name to
	// the target's actual dir (globbed from the target pod's volumes).
	rewrites := projectedVolumeRewrites(in.SrcSpec, in.TargetPodUID)

	anonVolDir := filepath.Join(bundleDir, "anon-volumes")
	spec := *in.SrcSpec // shallow copy; AdaptSpec rewrites in place
	notes, err := AdaptSpec(&spec, AdaptParams{
		RootfsPath:      rootfs,
		HolderPid:       holder.Pid,
		CgroupsPath:     podSlice + "/" + id,
		SourcePodUID:    in.SourcePodUID,
		TargetPodUID:    in.TargetPodUID,
		SourceSandboxID: srcSandboxID,
		TargetSandboxID: holder.SandboxID,
		ExtraRewrites:   rewrites,
		AnonVolumeDir:   anonVolDir,
	})
	out.Notes = append(out.Notes, notes...)
	if err != nil {
		return out, err
	}
	// Create fresh scratch dirs for any anonymous image volume AdaptSpec
	// re-pointed here (warm state is in memory; the volume is ephemeral).
	for i := range spec.Mounts {
		if src := spec.Mounts[i].Source; strings.HasPrefix(src, anonVolDir) {
			if err := os.MkdirAll(src, 0o755); err != nil {
				return out, fmt.Errorf("create anon-volume scratch %s: %w", src, err)
			}
		}
	}
	cfg, err := json.MarshalIndent(&spec, "", "  ")
	if err != nil {
		return out, err
	}
	if err := os.WriteFile(filepath.Join(bundleDir, "config.json"), cfg, 0o644); err != nil {
		return out, err
	}

	// CRIU options (file-validation, tcp-close, skip-in-flight) come from
	// /etc/criu/runc.conf, written once at bootstrap.
	if b, err := os.ReadFile(filepath.Join(criuConfDir, "runc.conf")); err != nil || !strings.Contains(string(b), "file-validation") {
		return out, fmt.Errorf("criu runc.conf missing or lacks file-validation (bootstrap owns it): %v", err)
	}
	out.FileValidationUsed = true

	if v, err := r.execHost(ctx, nil, criuBundleDir+"/criu", "--version"); err == nil {
		out.CriuVersion = strings.TrimSpace(strings.TrimPrefix(strings.Split(v, "\n")[0], "Version: "))
	}

	imagePath := filepath.Join(in.CheckpointDir, "checkpoint")
	if in.PreCopy {
		imagePath = in.CheckpointDir
	}
	pidFile := filepath.Join(bundleDir, "pid")
	env := []string{"PATH=" + criuBundleDir + ":/usr/sbin:/usr/bin:/sbin:/bin:/home/kubernetes/bin"}
	baseArgs := []string{
		"--root", runcStateRoot, "restore",
		"--bundle", bundleDir,
		"--image-path", imagePath,
		"--work-path", bundleDir,
		"--pid-file", pidFile,
		"--detach",
	}
	// runc's default "soft" mode creates the pm-<id> cgroup from config.json and
	// has CRIU remap into it — required for workloads holding cgroup fds (e.g.
	// Go >= 1.25 keeps /sys/fs/cgroup/cpu.max open for GOMAXPROCS). "ignore" is
	// kept as a fallback for cgroup-config restore breakage.
	variants := [][]string{
		append(append([]string{}, baseArgs...), id), // runc default: soft
		append(append([]string{}, baseArgs...), "--manage-cgroups-mode", "ignore", id),
	}
	var lastOut string
	var lastErr error
	logPath := filepath.Join(bundleDir, "runc-stdout.log")
	for vi, args := range variants {
		lastOut, lastErr = r.execRestore(ctx, env, logPath, "runc", args...)
		if lastErr == nil {
			break
		}
		// Preserve the full evidence per variant.
		saveFailureArtifact(id, fmt.Sprintf("variant%d-runc.out", vi), lastOut)
		if lg, err2 := os.ReadFile(filepath.Join(bundleDir, "restore.log")); err2 == nil {
			saveFailureArtifact(id, fmt.Sprintf("variant%d-restore.log", vi), string(lg))
		}
		out.Notes = append(out.Notes, fmt.Sprintf("variant %d failed: %s", vi, truncate(criuErrorLines(lastOut), 500)))
		if !strings.Contains(lastOut, "cgroup") {
			break // not cgroup-shaped — the fallback mode cannot help
		}
		_, _ = r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id)
	}
	if lastErr != nil {
		sig := criuErrorLines(lastOut)
		if tail, err2 := tailFile(filepath.Join(bundleDir, "restore.log"), 200); err2 == nil {
			if s := criuErrorLines(tail); s != "" {
				sig += "\n" + s
			}
		}
		if sig == "" {
			sig = truncate(lastOut, 800)
		}
		// Failure teardown: clean up the bundle and unmount the prepared rootfs
		// overlay so abandoned restores don't leak mounts on the host node.
		if notes := r.cleanup(ctx, in.MigrationName, "", "", false); len(notes) > 0 {
			out.Notes = append(out.Notes, notes...)
		}
		return out, fmt.Errorf("runc restore failed: %w: %s", lastErr, truncate(sig, 1200))
	}

	pidB, err := os.ReadFile(pidFile)
	if err != nil {
		return out, fmt.Errorf("restored but pid-file missing: %w", err)
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(string(pidB)))
	out.Pid = pid
	out.DurationMs = time.Since(start).Milliseconds()

	if hostNs, err1 := os.Readlink("/proc/1/ns/time"); err1 == nil {
		if procNs, err2 := os.Readlink(fmt.Sprintf("/proc/%d/ns/time", pid)); err2 == nil {
			d := procNs != hostNs
			out.TimeNsDistinct = &d
		}
	}
	return out, nil
}

// cleanup tears down restore artifacts. keepContainer=true leaves the restored
// process + rootfs alive and only GCs checkpoint material.
func (r *restorer) cleanup(ctx context.Context, migrationName, checkpointDir, tarPath string, keepContainer bool) []string {
	var notes []string
	id := "pm-" + migrationName
	bundleDir := filepath.Join(pmRoot, "bundles", id)
	if !keepContainer {
		if out, err := r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id); err != nil {
			notes = append(notes, "runc delete: "+truncate(out, 200))
		}
		if err := unmountAll(filepath.Join(bundleDir, "rootfs")); err != nil {
			notes = append(notes, "unmount rootfs: "+err.Error())
		} else {
			if err := r.cclient.SnapshotService("overlayfs").Remove(ctx, id); err != nil {
				notes = append(notes, "snapshot remove: "+err.Error())
			}
			lm := r.cclient.LeasesService()
			if err := lm.Delete(ctx, leases.Lease{ID: "lease-" + id}); err != nil {
				notes = append(notes, "lease remove: "+err.Error())
			}
		}
		if err := os.RemoveAll(bundleDir); err != nil {
			notes = append(notes, "rm bundle: "+err.Error())
		}
	}
	if checkpointDir != "" {
		if err := os.RemoveAll(checkpointDir); err != nil {
			notes = append(notes, "rm checkpoint dir: "+err.Error())
		}
	}
	if tarPath != "" {
		if err := os.Remove(tarPath); err != nil && !os.IsNotExist(err) {
			notes = append(notes, "rm tar: "+err.Error())
		}
	}
	return notes
}

// projectedVolumeRewrites pairs source-pod kubelet paths whose names are NOT
// derivable from the UID swap with the target pod's actual ones (globbed
// under /var/lib/kubelet/pods/<target-uid>):
//   - the projected SA-token volume dir (kube-api-access-XXXXX, random suffix)
//   - the per-container termination-log file (containers/<name>/<hash> — the
//     target's container is the holder, with its own hash)
//
// Honest failure mode: no match -> no rewrite -> AdaptSpec's lingering-source
// check refuses the restore with a precise error.
func projectedVolumeRewrites(srcSpec *specs.Spec, targetPodUID string) [][2]string {
	var rewrites [][2]string
	if srcSpec == nil {
		return nil
	}
	tgtRoot := "/var/lib/kubelet/pods/" + targetPodUID
	for _, m := range srcSpec.Mounts {
		if i := strings.Index(m.Source, "volumes/kubernetes.io~projected/"); i >= 0 {
			srcName := strings.SplitN(m.Source[i+len("volumes/kubernetes.io~projected/"):], "/", 2)[0]
			matches, _ := filepath.Glob(tgtRoot + "/volumes/kubernetes.io~projected/*")
			for _, tgt := range matches {
				if tgtName := filepath.Base(tgt); tgtName != srcName {
					rewrites = append(rewrites, [2]string{srcName, tgtName})
				}
			}
		}
		if i := strings.Index(m.Source, "/containers/"); i >= 0 && strings.Contains(m.Source, "/var/lib/kubelet/pods/") {
			srcRel := "containers/" + m.Source[i+len("/containers/"):] // containers/<name>/<hash>
			ctrName := strings.SplitN(strings.TrimPrefix(srcRel, "containers/"), "/", 2)[0]
			// Prefer the same-named container on the target (the target pod
			// keeps the source container's name; an init container also has
			// an entry here, so a bare */* glob would be ambiguous).
			matches, _ := filepath.Glob(tgtRoot + "/containers/" + ctrName + "/*")
			if len(matches) == 0 {
				matches, _ = filepath.Glob(tgtRoot + "/containers/*/*")
			}
			if len(matches) > 0 {
				tgtRel := "containers/" + strings.TrimPrefix(matches[0], tgtRoot+"/containers/")
				if tgtRel != srcRel {
					rewrites = append(rewrites, [2]string{srcRel, tgtRel})
				}
			}
		}
		if i := strings.Index(m.Source, "volume-subpaths/"); i >= 0 {
			// format: volume-subpaths/<vol-name>/<ctr-name>/<idx>
			parts := strings.Split(m.Source[i+len("volume-subpaths/"):], "/")
			if len(parts) >= 3 {
				volName := parts[0]
				ctrName := parts[1]
				srcIdx := parts[2]
				matches, _ := filepath.Glob(tgtRoot + "/volume-subpaths/" + volName + "/" + ctrName + "/*")
				if len(matches) > 0 {
					tgtIdx := filepath.Base(matches[0])
					if tgtIdx != srcIdx {
						srcPath := "volume-subpaths/" + volName + "/" + ctrName + "/" + srcIdx
						tgtPath := "volume-subpaths/" + volName + "/" + ctrName + "/" + tgtIdx
						rewrites = append(rewrites, [2]string{srcPath, tgtPath})
					}
				}
			}
		}
	}
	return rewrites
}

// saveFailureArtifact keeps restore failure evidence under
// /var/lib/pod-migrate/failed/<id>/ (bounded: overwritten per migration id).
func saveFailureArtifact(id, name, content string) {
	dir := filepath.Join(pmRoot, "failed", id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644)
}

func unmountAll(target string) error {
	if _, err := os.Stat(target); os.IsNotExist(err) {
		return nil
	}
	return mount.UnmountAll(target, 0)
}

// criuErrorLines extracts criu/runc error lines from noisy combined output.
func criuErrorLines(s string) string {
	var errs []string
	for _, line := range strings.Split(s, "\n") {
		if strings.Contains(line, "Error (") || strings.Contains(line, "level=error") ||
			strings.Contains(line, "Error:") {
			errs = append(errs, strings.TrimSpace(line))
		}
	}
	if len(errs) > 15 {
		errs = errs[len(errs)-15:]
	}
	return strings.Join(errs, "\n")
}

func tailFile(path string, lines int) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	all := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n"), nil
}
