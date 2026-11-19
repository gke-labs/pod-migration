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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
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
	// execHost runs a command in the host namespaces (nsenter -t 1 -m -p -C).
	// Swappable in tests.
	execHost         func(ctx context.Context, env []string, name string, args ...string) (string, error)
	nsenterBin       string
	openPodNetnsFn   func(holderPid int) (*os.File, error)
	execNetnsFn      func(ctx context.Context, netns *os.File, name string, args ...string) (string, error)
	execRestoreFn    func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error)
	findContainerFn  func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error)
	listContainersFn func(ctx context.Context) ([]containerd.Container, error)
	prepareRootfsFn  func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error)
	readProcCgroupFn func(pid int) ([]byte, error)
	pmRootDir        string
	criuConfDirPath  string
	hostNetnsPath    string
}

func newRestorer(c *containerd.Client) *restorer {
	r := &restorer{
		cclient:       c,
		nsenterBin:    "nsenter",
		hostNetnsPath: "/proc/1/ns/net",
	}
	r.execHost = r.nsenterExec
	return r
}

// nsenterExec executes cmd in the host mount, pid, and cgroup namespaces (agent
// has hostPID, so target pid 1 is the host init). Entering host cgroup
// namespace (-C) ensures commands like runc restore access the full host cgroup
// hierarchy and place restored containers under the target pod slice.
// Output goes through a file, not pipes: `runc restore --detach` leaves the
// restored process holding the inherited stderr, so CombinedOutput would block
// forever after a successful restore. With *os.File stdio, Wait returns on
// runc exit.
func (r *restorer) nsenterExec(ctx context.Context, env []string, name string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	bin := "nsenter"
	if r != nil && r.nsenterBin != "" {
		bin = r.nsenterBin
	}
	full := append([]string{"-t", "1", "-m", "-p", "-C", "--", name}, args...)
	cmd := exec.CommandContext(cctx, bin, full...)
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

// readProcCgroup reads the cgroup v2 membership of a target process from the
// host cgroup namespace (via r.execHost). It fails closed with no fallback to
// the agent's private cgroup namespace, preventing ../ path traversal escapes.
func (r *restorer) readProcCgroup(ctx context.Context, pid int) ([]byte, error) {
	if r.readProcCgroupFn != nil {
		return r.readProcCgroupFn(pid)
	}
	if r.execHost == nil {
		return nil, fmt.Errorf("cannot read host cgroup: execHost is nil")
	}
	out, err := r.execHost(ctx, nil, "cat", fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return nil, fmt.Errorf("read host cgroup for pid %d: %w", pid, err)
	}
	return []byte(out), nil
}

func nsenterExec(ctx context.Context, env []string, name string, args ...string) (string, error) {
	return (&restorer{}).nsenterExec(ctx, env, name, args...)
}

func (r *restorer) execRestore(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	bin := "nsenter"
	if r != nil && r.nsenterBin != "" {
		bin = r.nsenterBin
	}
	// runc restore holds inherited stderr, so we redirect to a file.
	// We keep this file so it acts as a container-stdout log for the restored process.
	full := append([]string{"-t", "1", "-m", "-p", "-C", "--", name}, args...)
	cmd := exec.CommandContext(cctx, bin, full...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process != nil && cmd.Process.Pid > 0 {
			if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
				return err
			}
		}
		return nil
	}
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

func (r *restorer) rootDir() string {
	if r != nil && r.pmRootDir != "" {
		return r.pmRootDir
	}
	return pmRoot
}

// openPodNetns opens the network namespace for the holder container PID.
// It verifies that the target pod does not share the host network namespace (hostNetwork: true pods
// are out of scope for sandbox netfilter locking), and opens /proc/<pid>/ns/net.
// Returns (nil, nil) if holderPid <= 0.
// If the PID shares the host netns or opening fails, it returns an error and nil *os.File.
func (r *restorer) openPodNetns(holderPid int) (*os.File, error) {
	if holderPid <= 0 {
		return nil, nil
	}
	if r != nil && r.openPodNetnsFn != nil {
		return r.openPodNetnsFn(holderPid)
	}
	f, err := os.Open(fmt.Sprintf("/proc/%d/ns/net", holderPid))
	if err != nil {
		return nil, fmt.Errorf("open /proc/%d/ns/net: %w", holderPid, err)
	}

	hostPath := "/proc/1/ns/net"
	if r != nil && r.hostNetnsPath != "" {
		hostPath = r.hostNetnsPath
	}
	hostFi, err := os.Stat(hostPath)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat host network namespace %s: %w", hostPath, err)
	}
	podFi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stat pod network namespace: %w", err)
	}
	if os.SameFile(hostFi, podFi) {
		f.Close()
		return nil, fmt.Errorf("target pod PID %d shares host network namespace; refusing restore to protect host", holderPid)
	}
	return f, nil
}

func (r *restorer) execNetns(ctx context.Context, netns *os.File, name string, args ...string) (string, error) {
	if netns == nil {
		return "", errors.New("cannot enter netns: netns fd is nil")
	}
	if r != nil && r.execNetnsFn != nil {
		return r.execNetnsFn(ctx, netns, name, args...)
	}
	nsenterBin := "nsenter"
	if r != nil && r.nsenterBin != "" {
		nsenterBin = r.nsenterBin
	}
	nsArgs := append([]string{"--net=/proc/self/fd/3", "--", name}, args...)
	cmd := exec.CommandContext(ctx, nsenterBin, nsArgs...)
	cmd.ExtraFiles = []*os.File{netns}
	b, err := cmd.CombinedOutput()
	return string(b), err
}

// cleanNetnsLock removes CRIU network-lock iptables rules from the target pod's
// network namespace so that a cold fallback start is not left with locked network (#126).
func (r *restorer) cleanNetnsLock(ctx context.Context, netns *os.File) []string {
	if netns == nil {
		return []string{"cleanNetnsLock skipped: netns fd is nil"}
	}
	cleanCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()

	var notes []string
	tools := []string{"iptables", "ip6tables", "iptables-legacy", "ip6tables-legacy"}
	for _, tool := range tools {
		if cleanCtx.Err() != nil {
			notes = append(notes, "cleanNetnsLock timed out after 15s")
			break
		}
		// Probe whether CRIU chain exists first to avoid blind legacy module/table creation on nft hosts.
		if _, err := r.execNetns(cleanCtx, netns, tool, "-w", "2", "-S", "CRIU"); err != nil {
			continue
		}

		cleaned := false
		// Delete any jumps to CRIU in INPUT and OUTPUT.
		for _, chain := range []string{"INPUT", "OUTPUT"} {
			for i := 0; i < 10; i++ {
				_, err := r.execNetns(cleanCtx, netns, tool, "-w", "2", "-D", chain, "-j", "CRIU")
				if err != nil {
					break
				}
				cleaned = true
			}
		}
		// Flush and delete the CRIU chain.
		if out, err := r.execNetns(cleanCtx, netns, tool, "-w", "2", "-F", "CRIU"); err == nil {
			cleaned = true
		} else {
			notes = append(notes, fmt.Sprintf("%s -F CRIU failed: %v (%s)", tool, err, truncate(out, 100)))
		}
		if out, err := r.execNetns(cleanCtx, netns, tool, "-w", "2", "-X", "CRIU"); err == nil {
			cleaned = true
		} else {
			notes = append(notes, fmt.Sprintf("%s -X CRIU failed: %v (%s)", tool, err, truncate(out, 100)))
		}
		if cleaned {
			notes = append(notes, fmt.Sprintf("cleaned CRIU network-lock rules with %s", tool))
		}
	}
	return notes
}

// containerRef identifies a CRI container/sandbox pairing in containerd.
type containerRef struct {
	ID        string
	SandboxID string
	Pid       int
	Spec      *specs.Spec
	ImageName string
}

func (r *restorer) listContainers(ctx context.Context) ([]containerd.Container, error) {
	if r.listContainersFn != nil {
		return r.listContainersFn(ctx)
	}
	if r.cclient == nil {
		return nil, fmt.Errorf("containerd client not initialized")
	}
	return r.cclient.Containers(ctx)
}

// ErrContainerNotFound is returned by findContainer when no container matches the target pod/container labels.
var ErrContainerNotFound = errors.New("container not found in containerd")

// findContainer locates the CRI container for pod/container by containerd
// labels (namespace k8s.io is set on the client). When podUID is non-empty,
// containers from earlier pods with the same name are skipped. If multiple
// containers match (e.g. a stopped attempt with Pid == 0 alongside a running
// one), prefers a container with a live task Pid.
func (r *restorer) findContainer(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
	if r.findContainerFn != nil {
		return r.findContainerFn(ctx, podNS, podName, podUID, containerName)
	}
	cs, err := r.listContainers(ctx)
	if err != nil {
		return nil, fmt.Errorf("containerd list: %w", err)
	}
	var fallback *containerRef
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
		if podUID != "" && labels["io.kubernetes.pod.uid"] != podUID {
			continue
		}
		ref := &containerRef{ID: c.ID()}
		spec, err := c.Spec(ctx)
		if err != nil {
			return nil, fmt.Errorf("containerd spec for %s: %w", c.ID(), err)
		}
		ref.Spec = spec
		ref.SandboxID = spec.Annotations["io.kubernetes.cri.sandbox-id"]
		info, err := c.Info(ctx)
		if err != nil {
			return nil, fmt.Errorf("containerd info for %s: %w", c.ID(), err)
		}
		ref.ImageName = info.Image
		if task, err := c.Task(ctx, nil); err == nil {
			ref.Pid = int(task.Pid())
		}
		if ref.Pid != 0 && ref.Spec != nil && ref.ImageName != "" {
			return ref, nil
		}
		fallback = ref
	}
	if fallback != nil {
		return fallback, nil
	}
	return nil, fmt.Errorf("container %s/%s/%s (uid=%s): %w", podNS, podName, containerName, podUID, ErrContainerNotFound)
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
	// The target pod's app container runs the seeded holder binary and keeps
	// the source container's name. The kubelet may report the pod Running a
	// beat before containerd's task pid is queryable — wait for it briefly
	// rather than failing the restore on a startup race.
	var holder *containerRef
	deadline := time.Now().Add(30 * time.Second)
	findFn := r.findContainer
	if r.findContainerFn != nil {
		findFn = r.findContainerFn
	}
	for {
		var err error
		holder, err = findFn(ctx, in.PodNamespace, in.TargetPod, in.TargetPodUID, in.ContainerName)
		if err == nil && holder.Pid != 0 && holder.Spec != nil && holder.ImageName != "" {
			break
		}
		if time.Now().After(deadline) {
			if err != nil {
				return out, fmt.Errorf("target holder container not found within 30s: %w", err)
			}
			if holder.Spec == nil || holder.ImageName == "" {
				return out, fmt.Errorf("target holder container metadata incomplete (spec=%v, image=%q)", holder.Spec != nil, holder.ImageName)
			}
			return out, fmt.Errorf("target holder task pid not up within 30s — sandbox slow to start")
		}
		time.Sleep(250 * time.Millisecond)
	}

	var netnsFile *os.File
	if holder.Pid > 0 {
		var netnsErr error
		netnsFile, netnsErr = r.openPodNetns(holder.Pid)
		if netnsErr != nil {
			return out, fmt.Errorf("open pod netns: %w", netnsErr)
		}
		if netnsFile != nil {
			defer netnsFile.Close()
		}
	}

	// Pod cgroup slice of the target, from the holder's own cgroup.
	// Read within the host cgroup namespace (r.execHost) so the cgroup path is
	// anchored to the host root and not distorted by the agent's private
	// cgroup namespace (which emits relative ../ paths under cgroup_namespaces(7)).
	procCg, err := r.readProcCgroup(ctx, holder.Pid)
	if err != nil {
		return out, fmt.Errorf("read holder cgroup: %w", err)
	}
	podSlice, err := PodSliceFromProcCgroup(string(procCg))
	if err != nil {
		return out, err
	}

	rootDir := r.rootDir()
	id := "pm-" + in.MigrationName
	bundleDir := filepath.Join(rootDir, "bundles", id)

	// Clean up any leftover runc container and stale network locks from a
	// previous failed restore attempt.
	if delOut, delErr := r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id); delErr != nil {
		out.Notes = append(out.Notes, fmt.Sprintf("stale-container pre-delete failed (restore will likely conflict): %s: %s",
			delErr, truncate(delOut, 200)))
	}
	if netnsFile != nil {
		if netNotes := r.cleanNetnsLock(ctx, netnsFile); len(netNotes) > 0 {
			out.Notes = append(out.Notes, netNotes...)
		}
	}

	if err := os.MkdirAll(bundleDir, 0o755); err != nil {
		return out, err
	}

	rootfsPath := filepath.Join(bundleDir, "rootfs")
	anonVolDir := filepath.Join(bundleDir, "anon-volumes")
	// The projected SA-token volume dir carries a per-pod random suffix
	// (kube-api-access-XXXXX) that survives the UID swap — map source name to
	// the target's actual dir (globbed from the target pod's volumes).
	rewrites := projectedVolumeRewrites(in.SrcSpec, in.TargetPodUID)
	adaptParams, imgName, err := buildAdaptParams(in, holder, rootfsPath, podSlice, id, anonVolDir, rewrites)
	if err != nil {
		return out, err
	}

	prepFn := r.prepareRootfs
	if r.prepareRootfsFn != nil {
		prepFn = r.prepareRootfsFn
	}
	rootfs, err := prepFn(ctx, imgName, id, bundleDir, filepath.Join(in.CheckpointDir, "rootfs-diff.tar"))
	if err != nil {
		return out, err
	}
	adaptParams.RootfsPath = rootfs

	spec := *in.SrcSpec // shallow copy; AdaptSpec rewrites in place
	notes, err := AdaptSpec(&spec, adaptParams)
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
	confDir := criuConfDir
	if r.criuConfDirPath != "" {
		confDir = r.criuConfDirPath
	}
	if b, err := os.ReadFile(filepath.Join(confDir, "runc.conf")); err != nil || !strings.Contains(string(b), "file-validation") {
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
	execRest := r.execRestore
	if r.execRestoreFn != nil {
		execRest = r.execRestoreFn
	}
	for vi, args := range variants {
		lastOut, lastErr = execRest(ctx, env, logPath, "runc", args...)
		if lastErr == nil {
			break
		}
		// Preserve the full evidence per variant.
		r.saveFailureArtifact(id, fmt.Sprintf("variant%d-runc.out", vi), lastOut)
		if lg, err2 := os.ReadFile(filepath.Join(bundleDir, "restore.log")); err2 == nil {
			r.saveFailureArtifact(id, fmt.Sprintf("variant%d-restore.log", vi), string(lg))
		}
		out.Notes = append(out.Notes, fmt.Sprintf("variant %d failed: %s", vi, truncate(criuErrorLines(lastOut), 500)))
		if vi == len(variants)-1 || ctx.Err() != nil || !strings.Contains(lastOut, "cgroup") {
			break // not cgroup-shaped or final variant — cannot retry
		}
		_, _ = r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id)
		if netnsFile != nil {
			if netNotes := r.cleanNetnsLock(ctx, netnsFile); len(netNotes) > 0 {
				out.Notes = append(out.Notes, netNotes...)
			}
		}
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
		// Clean up any CRIU network-lock iptables rules in the target pod's netns
		// so that the holder wrapper's cold fallback has network access (#126).
		var netNotes []string
		if netnsFile != nil {
			netNotes = r.cleanNetnsLock(ctx, netnsFile)
			if len(netNotes) > 0 {
				out.Notes = append(netNotes, out.Notes...)
			}
		}
		netSummary := ""
		if len(netNotes) > 0 {
			netSummary = " [" + strings.Join(netNotes, "; ") + "]"
		}
		return out, fmt.Errorf("runc restore failed%s: %w: %s", netSummary, lastErr, truncate(sig, 800))
	}

	pidB, err := os.ReadFile(pidFile)
	if err != nil {
		if notes := r.cleanup(ctx, in.MigrationName, "", "", false); len(notes) > 0 {
			out.Notes = append(out.Notes, notes...)
		}
		var netNotes []string
		if netnsFile != nil {
			netNotes = r.cleanNetnsLock(ctx, netnsFile)
			if len(netNotes) > 0 {
				out.Notes = append(netNotes, out.Notes...)
			}
		}
		netSummary := ""
		if len(netNotes) > 0 {
			netSummary = " [" + strings.Join(netNotes, "; ") + "]"
		}
		return out, fmt.Errorf("restored but pid-file missing%s: %w", netSummary, err)
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

// buildAdaptParams validates the destination holder container reference and
// constructs AdaptParams anchored to the destination container's OCI spec and
// image name (never falling back to untrusted archive metadata).
func buildAdaptParams(in restoreInputs, holder *containerRef, rootfsPath, podSlice, id, anonVolDir string, rewrites [][2]string) (AdaptParams, string, error) {
	if in.SrcSpec == nil {
		return AdaptParams{}, "", fmt.Errorf("no source OCI spec in the bundle")
	}
	if holder == nil || holder.Spec == nil {
		return AdaptParams{}, "", fmt.Errorf("target holder container has no OCI spec; refusing unhardened restore")
	}
	if holder.ImageName == "" {
		return AdaptParams{}, "", fmt.Errorf("target holder container has no image name; refusing to fall back to archive image")
	}
	if !strings.HasPrefix(podSlice, "/") || strings.Contains(podSlice, "..") {
		return AdaptParams{}, "", fmt.Errorf("podSlice must be an absolute path without traversal: %q", podSlice)
	}
	cleanSlice := path.Clean(podSlice)
	if cleanSlice == "/" || cleanSlice == "." {
		return AdaptParams{}, "", fmt.Errorf("podSlice cannot be empty or root cgroup")
	}
	srcSandboxID := in.SrcSpec.Annotations["io.kubernetes.cri.sandbox-id"]
	cgroupsPath := path.Clean(cleanSlice + "/" + id)
	return AdaptParams{
		RootfsPath:      rootfsPath,
		HolderPid:       holder.Pid,
		CgroupsPath:     cgroupsPath,
		SourcePodUID:    in.SourcePodUID,
		TargetPodUID:    in.TargetPodUID,
		SourceSandboxID: srcSandboxID,
		TargetSandboxID: holder.SandboxID,
		ExtraRewrites:   rewrites,
		AnonVolumeDir:   anonVolDir,
		TargetSpec:      holder.Spec,
	}, holder.ImageName, nil
}

// cleanup tears down restore artifacts. keepContainer=true leaves the restored
// process + rootfs alive and only GCs checkpoint material.
func (r *restorer) cleanup(ctx context.Context, migrationName, checkpointDir, tarPath string, keepContainer bool) []string {
	var notes []string
	id := "pm-" + migrationName
	bundleDir := filepath.Join(r.rootDir(), "bundles", id)
	if !keepContainer {
		if out, err := r.execHost(ctx, nil, "runc", "--root", runcStateRoot, "delete", "-f", id); err != nil {
			notes = append(notes, "runc delete: "+truncate(out, 200))
		}
		if err := unmountAll(filepath.Join(bundleDir, "rootfs")); err != nil {
			notes = append(notes, "unmount rootfs: "+err.Error())
		} else if r.cclient != nil {
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
// <rootDir>/failed/<id>/ (bounded: overwritten per migration id).
func (r *restorer) saveFailureArtifact(id, name, content string) {
	dir := filepath.Join(r.rootDir(), "failed", id)
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
