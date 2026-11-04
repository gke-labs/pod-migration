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
	"os"
	"path/filepath"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
)

func sampleSpec() *specs.Spec {
	return &specs.Spec{
		Root: &specs.Root{Path: "rootfs"},
		Process: &specs.Process{
			Args: []string{"/app"},
		},
		Annotations: map[string]string{
			"io.kubernetes.cri.sandbox-id":   "srcsandbox111",
			"io.kubernetes.cri.sandbox-name": "counter-src",
		},
		Mounts: []specs.Mount{
			{Destination: "/etc/hosts", Type: "bind",
				Source: "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/srcsandbox111/etc-hosts"},
			{Destination: "/var/run/secrets/kubernetes.io/serviceaccount", Type: "bind",
				Source: "/var/lib/kubelet/pods/src-uid/volumes/kubernetes.io~projected/kube-api-access-x/"},
			{Destination: "/dev/termination-log", Type: "bind",
				Source: "/var/lib/kubelet/pods/src-uid/containers/counter/abc123"},
			{Destination: "/proc", Type: "proc", Source: "proc"},
		},
		Linux: &specs.Linux{
			CgroupsPath: "kubepods-burstable-podsrc.slice:cri-containerd:oldid",
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace, Path: "/proc/1111/ns/ipc"},
				{Type: specs.UTSNamespace, Path: "/proc/1111/ns/uts"},
				{Type: specs.MountNamespace},
				{Type: specs.NetworkNamespace, Path: "/proc/1111/ns/net"},
			},
		},
	}
}

func sampleTargetSpec() *specs.Spec {
	return &specs.Spec{
		Root: &specs.Root{Path: "rootfs"},
		Process: &specs.Process{
			Args:            []string{"/pmhold/holder", "wrap", "/pminfo/annotations", "--", "/app"},
			Capabilities:    &specs.LinuxCapabilities{Bounding: []string{}},
			User:            specs.User{UID: 1000, GID: 1000},
			NoNewPrivileges: true,
		},
		Mounts: []specs.Mount{
			{Destination: "/etc/hosts", Type: "bind",
				Source: "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/tgtsandbox222/etc-hosts"},
			{Destination: "/var/run/secrets/kubernetes.io/serviceaccount", Type: "bind",
				Source: "/var/lib/kubelet/pods/tgt-uid/volumes/kubernetes.io~projected/kube-api-access-x"},
			{Destination: "/dev/termination-log", Type: "bind",
				Source: "/var/lib/kubelet/pods/tgt-uid/containers/counter/abc123"},
			{Destination: "/proc", Type: "proc", Source: "proc"},
			{Destination: "/pmhold", Type: "bind", Source: "/var/lib/kubelet/pods/tgt-uid/volumes/kubernetes.io~empty-dir/pmhold"},
			{Destination: "/pminfo", Type: "bind", Source: "/var/lib/kubelet/pods/tgt-uid/volumes/kubernetes.io~downward-api/pminfo"},
		},
		Linux: &specs.Linux{
			Seccomp:     &specs.LinuxSeccomp{DefaultAction: specs.ActErrno},
			MaskedPaths: []string{"/proc/kcore"},
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.PIDNamespace},
				{Type: specs.IPCNamespace, Path: "/proc/2222/ns/ipc"},
				{Type: specs.UTSNamespace, Path: "/proc/2222/ns/uts"},
				{Type: specs.MountNamespace},
				{Type: specs.NetworkNamespace, Path: "/proc/2222/ns/net"},
			},
		},
	}
}

func params() AdaptParams {
	return AdaptParams{
		RootfsPath:      "/var/lib/pod-migrate/bundles/pm-mig1/rootfs",
		HolderPid:       2222,
		CgroupsPath:     "kubepods.slice/kubepods-pod-tgt.slice/pm-mig1",
		SourcePodUID:    "src-uid",
		TargetPodUID:    "tgt-uid",
		SourceSandboxID: "srcsandbox111",
		TargetSandboxID: "tgtsandbox222",
		TargetSpec:      sampleTargetSpec(),
	}
}

func TestAdaptSpecJoinsTargetSandboxNamespaces(t *testing.T) {
	s := sampleSpec()
	if _, err := AdaptSpec(s, params()); err != nil {
		t.Fatal(err)
	}
	got := map[specs.LinuxNamespaceType]string{}
	for _, ns := range s.Linux.Namespaces {
		got[ns.Type] = ns.Path
	}
	for _, kind := range []struct {
		t    specs.LinuxNamespaceType
		want string
	}{
		{specs.NetworkNamespace, "/proc/2222/ns/net"},
		{specs.IPCNamespace, "/proc/2222/ns/ipc"},
		{specs.UTSNamespace, "/proc/2222/ns/uts"},
		{specs.PIDNamespace, ""},   // private: criu restores it
		{specs.MountNamespace, ""}, // private: criu restores it
	} {
		if got[kind.t] != kind.want {
			t.Errorf("ns %s path = %q, want %q", kind.t, got[kind.t], kind.want)
		}
	}
}

func TestAdaptSpecRemapsMountsAndCgroup(t *testing.T) {
	s := sampleSpec()
	notes, err := AdaptSpec(s, params())
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Mounts {
		if strings.Contains(m.Source, "src-uid") || strings.Contains(m.Source, "srcsandbox111") {
			t.Errorf("mount %s still references source: %s", m.Destination, m.Source)
		}
	}
	if s.Mounts[0].Source != "/var/lib/containerd/io.containerd.grpc.v1.cri/sandboxes/tgtsandbox222/etc-hosts" {
		t.Errorf("etc-hosts remap = %s", s.Mounts[0].Source)
	}
	if s.Linux.CgroupsPath != "kubepods.slice/kubepods-pod-tgt.slice/pm-mig1" {
		t.Errorf("cgroupsPath = %s", s.Linux.CgroupsPath)
	}
	if s.Root.Path != "/var/lib/pod-migrate/bundles/pm-mig1/rootfs" {
		t.Errorf("root = %s", s.Root.Path)
	}
	if len(notes) == 0 {
		t.Error("no evidence notes returned")
	}
	if _, ok := s.Annotations["io.kubernetes.cri.sandbox-id"]; ok {
		t.Error("stale cri sandbox-id annotation survived")
	}
}

// An anonymous image volume (Dockerfile VOLUME) is bind-mounted from the source
// container's volumes dir; on the target that path is absent -> restore mount
// fails. With AnonVolumeDir set, AdaptSpec re-points it at fresh target scratch.
func TestAdaptSpecRepointsAnonymousImageVolume(t *testing.T) {
	s := sampleSpec()
	s.Mounts = append(s.Mounts, specs.Mount{
		Destination: "/data", Type: "bind",
		Source: "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/7cb5e8acf3c8/volumes/aad7845112f5",
	})
	p := params()
	p.AnonVolumeDir = "/var/lib/pod-migrate/bundles/pm-mig1/anon-volumes"
	if _, err := AdaptSpec(s, p); err != nil {
		t.Fatal(err)
	}
	var data *specs.Mount
	for i := range s.Mounts {
		if s.Mounts[i].Destination == "/data" {
			data = &s.Mounts[i]
		}
	}
	want := "/var/lib/pod-migrate/bundles/pm-mig1/anon-volumes/aad7845112f5"
	if data == nil || data.Source != want {
		t.Errorf("/data anon volume source = %v, want %s", data, want)
	}
}

// Without AnonVolumeDir an anonymous volume mount not in TargetSpec is rejected.
func TestAdaptSpecRejectsAnonVolumeWhenDirUnset(t *testing.T) {
	s := sampleSpec()
	orig := "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/abc/volumes/xyz"
	s.Mounts = append(s.Mounts, specs.Mount{Destination: "/data", Type: "bind", Source: orig})
	if _, err := AdaptSpec(s, params()); err == nil { // AnonVolumeDir empty
		t.Fatal("expected unmapped anon volume to be rejected when AnonVolumeDir is empty")
	}
}

func TestAdaptSpecRefusesLingeringSourceRef(t *testing.T) {
	s := sampleSpec()
	p := params()
	// Simulate a target mount that still carries the source pod UID literal.
	p.TargetSpec.Mounts[1].Source = "/var/lib/kubelet/pods/src-uid/volumes/kubernetes.io~projected/kube-api-access-x"
	p.SourcePodUID = "src-uid"
	if _, err := AdaptSpec(s, p); err == nil {
		t.Fatal("AdaptSpec accepted a spec still referencing the source pod")
	}
}

func TestAdaptSpecNilSections(t *testing.T) {
	if _, err := AdaptSpec(&specs.Spec{}, params()); err == nil {
		t.Fatal("nil root/linux accepted")
	}
	p := params()
	p.TargetSpec = nil
	if _, err := AdaptSpec(sampleSpec(), p); err == nil {
		t.Fatal("nil TargetSpec accepted")
	}
}

func TestPodSliceFromProcCgroup(t *testing.T) {
	in := "0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice/cri-containerd-holderabc.scope\n"
	got, err := PodSliceFromProcCgroup(in)
	if err != nil {
		t.Fatal(err)
	}
	want := "kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice"
	if got != want {
		t.Errorf("pod slice = %q, want %q", got, want)
	}
}

func TestPodSliceFromProcCgroupRejectsRootAndV1(t *testing.T) {
	if _, err := PodSliceFromProcCgroup("0::/\n"); err == nil {
		t.Error("root cgroup accepted")
	}
	if _, err := PodSliceFromProcCgroup("12:cpuset:/kubepods/x\n"); err == nil {
		t.Error("cgroup v1-only content accepted")
	}
}

func TestValidateCheckpointTar(t *testing.T) {
	good := []string{"spec.dump", "config.dump", "checkpoint/pstree.img", "checkpoint/pages-1.img",
		"checkpoint/core-1.img", "rootfs-diff.tar"}
	if err := ValidateCheckpointTar(good); err != nil {
		t.Errorf("valid set rejected: %v", err)
	}
	for _, missing := range []string{"spec.dump", "config.dump", "checkpoint/pstree.img", "checkpoint/pages-1.img"} {
		var entries []string
		for _, e := range good {
			if e != missing {
				entries = append(entries, e)
			}
		}
		if err := ValidateCheckpointTar(entries); err == nil {
			t.Errorf("missing %s not detected", missing)
		}
	}
}

func TestAdaptSpecStripsHooks(t *testing.T) {
	s := sampleSpec()
	s.Hooks = &specs.Hooks{
		Prestart: []specs.Hook{{Path: "/bin/true"}},
	}
	if _, err := AdaptSpec(s, params()); err != nil {
		t.Fatal(err)
	}
	if s.Hooks != nil {
		t.Errorf("Hooks not stripped to nil")
	}
}

func TestAdaptSpecWithTargetSpec(t *testing.T) {
	s := sampleSpec()
	s.Process.Capabilities = &specs.LinuxCapabilities{
		Bounding: []string{"CAP_SYS_ADMIN"},
	}
	s.Process.User = specs.User{UID: 0}

	s.Mounts = append(s.Mounts, specs.Mount{
		Destination: "/host", Type: "bind", Source: "/",
	})

	p := params()
	p.TargetSpec = &specs.Spec{
		Process: &specs.Process{
			Capabilities:    &specs.LinuxCapabilities{Bounding: []string{}},
			User:            specs.User{UID: 1000},
			NoNewPrivileges: true,
		},
		Linux: &specs.Linux{
			Seccomp:     &specs.LinuxSeccomp{DefaultAction: specs.ActKill},
			MaskedPaths: []string{"/proc/kcore"},
		},
		Mounts: []specs.Mount{
			{Destination: "/etc/hosts", Type: "bind", Source: "/target/etc-hosts"},
			{Destination: "/var/run/secrets/kubernetes.io/serviceaccount", Type: "bind", Source: "/target/sa"},
			{Destination: "/dev/termination-log", Type: "bind", Source: "/target/term"},
			{Destination: "/proc", Type: "proc", Source: "proc"},
		},
	}

	_, err := AdaptSpec(s, p)
	if err == nil {
		t.Fatal("expected error rejecting /host mount, got nil")
	}
	if !strings.Contains(err.Error(), "not permitted by destination container spec") {
		t.Errorf("expected destination container spec error, got: %v", err)
	}

	// Remove the offending mount and try again
	s.Mounts = s.Mounts[:len(s.Mounts)-1]
	_, err = AdaptSpec(s, p)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if s.Process.User.UID != 1000 {
		t.Errorf("User UID not copied, got %d", s.Process.User.UID)
	}
	if s.Process.NoNewPrivileges != true {
		t.Errorf("NoNewPrivileges not copied")
	}
	if len(s.Process.Capabilities.Bounding) != 0 {
		t.Errorf("Capabilities not copied")
	}
	if s.Linux.Seccomp == nil || s.Linux.Seccomp.DefaultAction != specs.ActKill {
		t.Errorf("Seccomp not copied")
	}
	if len(s.Linux.MaskedPaths) != 1 || s.Linux.MaskedPaths[0] != "/proc/kcore" {
		t.Errorf("MaskedPaths not copied")
	}
}

func TestAdaptSpecRejectsDotDotAnonVolumeAndNilTargetSpec(t *testing.T) {
	s := sampleSpec()
	s.Mounts = append(s.Mounts, specs.Mount{
		Destination: "/data", Type: "bind",
		Source: "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/7cb5e8acf3c8/volumes/..",
	})
	p := params()
	p.AnonVolumeDir = "/var/lib/pod-migrate/bundles/pm-mig1/anon-volumes"
	if _, err := AdaptSpec(s, p); err == nil {
		t.Fatal("expected error rejecting .. anonymous volume name, got nil")
	}
}

func TestRestoreEndToEndEnforcesTargetSpec(t *testing.T) {
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

	var preparedImage string
	holderSpec := sampleTargetSpec()
	r := &restorer{
		pmRootDir:       tmpDir,
		criuConfDirPath: confDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			if podUID != "tgt-uid" {
				t.Fatalf("findContainerFn called with podUID=%q, want tgt-uid", podUID)
			}
			return &containerRef{
				ID:        "holder-ctr-1",
				SandboxID: "tgtsandbox222",
				Pid:       2222,
				Spec:      holderSpec,
				ImageName: "gcr.io/trusted/app:v1",
			}, nil
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string) (string, error) {
			preparedImage = imageName
			rf := filepath.Join(bundleDir, "rootfs")
			_ = os.MkdirAll(rf, 0o755)
			return rf, nil
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			return "Version: 3.19", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			for i := 0; i < len(args)-1; i++ {
				if args[i] == "--pid-file" {
					_ = os.WriteFile(args[i+1], []byte("4321\n"), 0o644)
				}
			}
			return "", nil
		},
	}

	// 1. Archive attempts privilege escalation (UID 0, CAP_SYS_ADMIN, hooks,
	// host UserNamespace) AND an unauthorized /host bind mount -> restore() must refuse!
	badArchiveSpec := sampleSpec()
	badArchiveSpec.Process.User = specs.User{UID: 0}
	badArchiveSpec.Process.Capabilities = &specs.LinuxCapabilities{Bounding: []string{"CAP_SYS_ADMIN"}}
	badArchiveSpec.Hooks = &specs.Hooks{Prestart: []specs.Hook{{Path: "/bin/sh"}}}
	badArchiveSpec.Mounts = append(badArchiveSpec.Mounts, specs.Mount{
		Destination: "/host", Type: "bind", Source: "/var/lib/kubelet/pods/../../",
	})
	_, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-bad",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       badArchiveSpec,
		ImageName:     "evil.example.com/attacker:latest",
	})
	if err == nil || !strings.Contains(err.Error(), "not permitted by destination container spec") {
		t.Fatalf("expected restore() to reject unauthorized /host mount, got: %v", err)
	}

	// 2. With valid mounts, restore() succeeds, uses holder.ImageName (ignoring
	// archive ImageName), and writes config.json enforcing holder.Spec's UID 1000,
	// empty capabilities, Seccomp, stripped hooks, and canonical namespaces.
	goodArchiveSpec := sampleSpec()
	goodArchiveSpec.Process.User = specs.User{UID: 0}
	goodArchiveSpec.Process.Capabilities = &specs.LinuxCapabilities{Bounding: []string{"CAP_SYS_ADMIN"}}
	goodArchiveSpec.Hooks = &specs.Hooks{Prestart: []specs.Hook{{Path: "/bin/sh"}}}
	goodArchiveSpec.Linux.Namespaces = []specs.LinuxNamespace{
		{Type: specs.UserNamespace, Path: "/proc/1/ns/user"},
	}

	out, err := r.restore(ctx, restoreInputs{
		MigrationName: "mig-good",
		PodNamespace:  "default",
		TargetPod:     "web-1",
		ContainerName: "app",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		CheckpointDir: chkDir,
		SrcSpec:       goodArchiveSpec,
		ImageName:     "evil.example.com/attacker:latest",
	})
	if err != nil {
		t.Fatalf("restore() failed: %v", err)
	}
	if out.Pid != 4321 {
		t.Fatalf("out.Pid = %d, want 4321", out.Pid)
	}
	if preparedImage != "gcr.io/trusted/app:v1" {
		t.Fatalf("preparedImage = %q, want holder image gcr.io/trusted/app:v1", preparedImage)
	}

	cfgBytes, err := os.ReadFile(filepath.Join(tmpDir, "bundles", "pm-mig-good", "config.json"))
	if err != nil {
		t.Fatalf("read written config.json: %v", err)
	}
	var written specs.Spec
	if err := json.Unmarshal(cfgBytes, &written); err != nil {
		t.Fatalf("decode written config.json: %v", err)
	}
	if written.Process.User.UID != 1000 {
		t.Errorf("written UID = %d, want 1000 from holder.Spec", written.Process.User.UID)
	}
	if len(written.Process.Capabilities.Bounding) != 0 {
		t.Errorf("written Bounding capabilities = %v, want empty from holder.Spec", written.Process.Capabilities.Bounding)
	}
	if !written.Process.NoNewPrivileges {
		t.Errorf("written NoNewPrivileges = false, want true")
	}
	if written.Hooks != nil {
		t.Errorf("written Hooks = %+v, want nil", written.Hooks)
	}
	if written.Linux.Seccomp == nil || written.Linux.Seccomp.DefaultAction != specs.ActErrno {
		t.Errorf("written Seccomp = %+v, want ActErrno from holder.Spec", written.Linux.Seccomp)
	}
	for _, ns := range written.Linux.Namespaces {
		if ns.Type == specs.UserNamespace {
			t.Errorf("archive UserNamespace survived in written config.json")
		}
	}
}

func TestBuildAdaptParamsWiresTargetSpecAndRejectsMissingHolderMetadata(t *testing.T) {
	srcSpec := sampleSpec()
	targetSpec := sampleSpec()
	in := restoreInputs{
		MigrationName: "mig1",
		SourcePodUID:  "src-uid",
		TargetPodUID:  "tgt-uid",
		SrcSpec:       srcSpec,
		ImageName:     "evil.example.com/attacker:latest",
	}

	t.Run("missing holder spec rejected", func(t *testing.T) {
		_, _, err := buildAdaptParams(in, &containerRef{Pid: 2222, ImageName: "redis:7"}, "/rootfs", "slice", "pm-mig1", "/anon", nil)
		if err == nil || !strings.Contains(err.Error(), "no OCI spec") {
			t.Fatalf("expected missing OCI spec error, got %v", err)
		}
	})

	t.Run("missing holder image rejected instead of falling back to archive image", func(t *testing.T) {
		_, _, err := buildAdaptParams(in, &containerRef{Pid: 2222, Spec: targetSpec}, "/rootfs", "slice", "pm-mig1", "/anon", nil)
		if err == nil || !strings.Contains(err.Error(), "no image name") {
			t.Fatalf("expected missing image name error, got %v", err)
		}
	})

	t.Run("wires TargetSpec and uses holder image", func(t *testing.T) {
		holder := &containerRef{
			Pid:       2222,
			SandboxID: "tgtsandbox222",
			Spec:      targetSpec,
			ImageName: "redis:7-alpine",
		}
		ap, img, err := buildAdaptParams(in, holder, "/rootfs", "slice", "pm-mig1", "/anon", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if ap.TargetSpec != targetSpec {
			t.Fatalf("TargetSpec was not wired from holder.Spec")
		}
		if img != "redis:7-alpine" {
			t.Fatalf("image = %q, want holder image redis:7-alpine", img)
		}
	})
}

func TestAdaptSpecEnforcesCanonicalNamespacesAndTargetSecurityPosture(t *testing.T) {
	// Archive attempts to omit PID/Mount namespaces (to inherit host ns),
	// inject a host UserNamespace path, and omit Process section.
	s := sampleSpec()
	s.Process = nil
	s.Linux.Namespaces = []specs.LinuxNamespace{
		{Type: specs.UserNamespace, Path: "/proc/1/ns/user"},
		{Type: specs.NetworkNamespace, Path: "/proc/1/ns/net"},
	}
	// Archive attempts to shadow /etc/hosts using an anon-volume-shaped source.
	s.Mounts[0].Source = "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/7cb5e8acf3c8/volumes/aad7845112f5"
	s.Mounts[0].Options = []string{"rw", "suid", "dev"}

	p := params()
	p.AnonVolumeDir = "/var/lib/pod-migrate/bundles/pm-mig1/anon-volumes"
	p.TargetSpec = &specs.Spec{
		Process: &specs.Process{
			Capabilities:    &specs.LinuxCapabilities{Bounding: []string{"CAP_NET_BIND_SERVICE"}},
			User:            specs.User{UID: 1000, GID: 1000},
			NoNewPrivileges: true,
		},
		Linux: &specs.Linux{
			Seccomp:     &specs.LinuxSeccomp{DefaultAction: specs.ActErrno},
			MaskedPaths: []string{"/proc/kcore"},
			Namespaces: []specs.LinuxNamespace{
				{Type: specs.CgroupNamespace},
			},
		},
		Mounts: []specs.Mount{
			{Destination: "/etc/hosts", Type: "bind", Source: "/target/etc-hosts", Options: []string{"rbind", "ro"}},
			{Destination: "/var/run/secrets/kubernetes.io/serviceaccount", Type: "bind", Source: "/target/sa", Options: []string{"rbind", "ro"}},
			{Destination: "/dev/termination-log", Type: "bind", Source: "/target/term", Options: []string{"rbind", "rw"}},
			{Destination: "/proc", Type: "proc", Source: "proc"},
		},
	}

	if _, err := AdaptSpec(s, p); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Process must be populated from TargetSpec even when archive had nil Process.
	if s.Process == nil || s.Process.User.UID != 1000 || !s.Process.NoNewPrivileges {
		t.Fatalf("Process not populated from TargetSpec: %+v", s.Process)
	}

	// Namespaces must contain private PID/Mount/Cgroup and holder net/ipc/uts, and no UserNamespace.
	nsMap := map[specs.LinuxNamespaceType]string{}
	for _, ns := range s.Linux.Namespaces {
		if ns.Type == specs.UserNamespace {
			t.Fatalf("archive UserNamespace was not stripped")
		}
		nsMap[ns.Type] = ns.Path
	}
	if _, ok := nsMap[specs.PIDNamespace]; !ok || nsMap[specs.PIDNamespace] != "" {
		t.Fatalf("PIDNamespace missing or not private: %v", nsMap)
	}
	if _, ok := nsMap[specs.MountNamespace]; !ok || nsMap[specs.MountNamespace] != "" {
		t.Fatalf("MountNamespace missing or not private: %v", nsMap)
	}

	// /etc/hosts is a reserved system path and in targetMounts: anonVolRe must NOT shadow /etc/hosts!
	// Wait: in targetMounts, /etc/hosts has Source "/target/etc-hosts", while archive had anonVolRe source.
	// Wait — let's check what s.Mounts[0] got!
	if s.Mounts[0].Destination == "/etc/hosts" && strings.HasPrefix(s.Mounts[0].Source, p.AnonVolumeDir) {
		t.Fatalf("anonVolRe was allowed to shadow /etc/hosts: %+v", s.Mounts[0])
	}
}

func TestAdaptSpecDropsInjectedHolderMountsForSecondHopMigration(t *testing.T) {
	// A pod that was once a gated replacement pod carries /pmhold and /pminfo
	// mounts injected by the webhook. When it is migrated again, its archive
	// spec includes those mounts; AdaptSpec must drop them rather than failing.
	s := sampleSpec()
	s.Mounts = append(s.Mounts,
		specs.Mount{Destination: "/pmhold", Type: "bind", Source: "/var/lib/kubelet/pods/src-uid/volumes/kubernetes.io~empty-dir/pmhold"},
		specs.Mount{Destination: "/pminfo", Type: "bind", Source: "/var/lib/kubelet/pods/src-uid/volumes/kubernetes.io~downward-api/pminfo"},
	)
	if _, err := AdaptSpec(s, params()); err != nil {
		t.Fatalf("expected second-hop restore to drop /pmhold and /pminfo, got error: %v", err)
	}
	for _, m := range s.Mounts {
		if m.Destination == "/pmhold" || m.Destination == "/pminfo" {
			t.Errorf("expected %s mount to be dropped from adapted spec, still present: %+v", m.Destination, m)
		}
	}
}
