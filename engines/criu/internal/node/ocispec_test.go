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

func params() AdaptParams {
	return AdaptParams{
		RootfsPath:      "/var/lib/pod-migrate/bundles/pm-mig1/rootfs",
		HolderPid:       2222,
		CgroupsPath:     "kubepods.slice/kubepods-pod-tgt.slice/pm-mig1",
		SourcePodUID:    "src-uid",
		TargetPodUID:    "tgt-uid",
		SourceSandboxID: "srcsandbox111",
		TargetSandboxID: "tgtsandbox222",
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

// Without AnonVolumeDir the anon-volume mount is left untouched.
func TestAdaptSpecLeavesAnonVolumeWhenDirUnset(t *testing.T) {
	s := sampleSpec()
	orig := "/var/lib/containerd/io.containerd.grpc.v1.cri/containers/abc/volumes/xyz"
	s.Mounts = append(s.Mounts, specs.Mount{Destination: "/data", Type: "bind", Source: orig})
	if _, err := AdaptSpec(s, params()); err != nil { // AnonVolumeDir empty
		t.Fatal(err)
	}
	for i := range s.Mounts {
		if s.Mounts[i].Destination == "/data" && s.Mounts[i].Source != orig {
			t.Errorf("anon volume rewritten despite empty AnonVolumeDir: %s", s.Mounts[i].Source)
		}
	}
}

func TestAdaptSpecRefusesLingeringSourceRef(t *testing.T) {
	s := sampleSpec()
	p := params()
	p.TargetPodUID = "" // remap disabled -> source refs survive -> must refuse
	p.SourcePodUID = ""
	p.SourceSandboxID = ""
	// Simulate a weird mount that still carries the uid literal.
	p.SourcePodUID = "src-uid"
	if _, err := AdaptSpec(s, p); err == nil {
		t.Fatal("AdaptSpec accepted a spec still referencing the source pod")
	}
}

func TestAdaptSpecNilSections(t *testing.T) {
	if _, err := AdaptSpec(&specs.Spec{}, params()); err == nil {
		t.Fatal("nil root/linux accepted")
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

func TestAdaptSpecLegacyRejectsBadBindMount(t *testing.T) {
	s := sampleSpec()
	s.Mounts = append(s.Mounts, specs.Mount{
		Destination: "/host", Type: "bind", Source: "/",
	})
	p := params()
	_, err := AdaptSpec(s, p)
	if err == nil {
		t.Fatal("expected error rejecting / bind mount in legacy mode, got nil")
	}
	if !strings.Contains(err.Error(), "not permitted in legacy mode") {
		t.Errorf("expected legacy mode error, got: %v", err)
	}
}
