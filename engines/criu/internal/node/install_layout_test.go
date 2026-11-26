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

// Where the agent installs CRIU on the host (#109): the default layout
// (Ubuntu, kind) and the COS layout.

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"
)

// imageWrapper is the wrapper build/agent/Dockerfile writes into the bundle,
// byte for byte: the default layout must install exactly this.
const imageWrapper = "#!/bin/sh\nexec /var/lib/pod-migrate/criu-bundle/ld-linux-x86-64.so.2 --library-path /var/lib/pod-migrate/criu-bundle /var/lib/pod-migrate/criu-bundle/criu.bin \"$@\"\n"

func writeFile(t *testing.T, path, content string, mode os.FileMode) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), mode); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// fakeImageBundle is an agent image's /opt/criu-bundle.
func fakeImageBundle(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	writeFile(t, filepath.Join(src, "criu"), imageWrapper, 0o755)
	writeFile(t, filepath.Join(src, "criu.bin"), "ELF", 0o755)
	writeFile(t, filepath.Join(src, "ld-linux-x86-64.so.2"), "LD", 0o755)
	writeFile(t, filepath.Join(src, "VERSION"), "Version: 4.2.1\n", 0o644)
	writeFile(t, filepath.Join(src, "licenses", "criu.copyright"), "GPL-2.0", 0o644)
	return src
}

func TestDetectCRIULayout(t *testing.T) {
	for _, tc := range []struct {
		name      string
		osRelease string // "" leaves /etc/os-release out
		want      criuLayout
	}{
		{"COS", "NAME=\"Container-Optimized OS\"\nID=cos\nPRETTY_NAME=\"Container-Optimized OS from Google\"\n", cosLayout},
		{"COS quoted ID", "NAME=\"Container-Optimized OS\"\nID=\"cos\"\n", cosLayout},
		{"Ubuntu", "NAME=\"Ubuntu\"\nID=ubuntu\nID_LIKE=debian\n", defaultLayout},
		{"Debian (kind)", "PRETTY_NAME=\"Debian GNU/Linux 12 (bookworm)\"\nID=debian\n", defaultLayout},
		{"ID_LIKE alone isn't COS", "ID=other\nID_LIKE=cos\n", defaultLayout},
		{"no os-release", "", defaultLayout},
		{"COS behind an absolute symlink", "link:ID=cos\n", cosLayout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if content, ok := strings.CutPrefix(tc.osRelease, "link:"); ok {
				// /etc/os-release -> /usr/lib/os-release, resolved in root.
				writeFile(t, filepath.Join(root, "usr", "lib", "os-release"), content, 0o644)
				if err := os.MkdirAll(filepath.Join(root, "etc"), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink("/usr/lib/os-release", filepath.Join(root, "etc", "os-release")); err != nil {
					t.Fatal(err)
				}
			} else if tc.osRelease != "" {
				writeFile(t, filepath.Join(root, "etc", "os-release"), tc.osRelease, 0o644)
			}
			if got := detectCRIULayout(root); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("detectCRIULayout = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// The default layout's wrapper is the image's, so Ubuntu nodes get the same
// /usr/local/sbin/criu as before #109.
func TestRenderCRIUWrapper_DefaultLayoutMatchesImageWrapper(t *testing.T) {
	if got := renderCRIUWrapper(defaultLayout.bundleDir); got != imageWrapper {
		t.Errorf("wrapper for the default layout:\n%q\nwant the image's:\n%q", got, imageWrapper)
	}
	cos := renderCRIUWrapper(cosLayout.bundleDir)
	if strings.Contains(cos, "/var/lib/pod-migrate") || !strings.Contains(cos, "/home/kubernetes/bin/pod-migrate/criu-bundle/criu.bin") {
		t.Errorf("COS wrapper must exec the bundle under /home/kubernetes/bin (COS mounts /var noexec): %q", cos)
	}
}

// Ubuntu: the bundle under /var/lib/pod-migrate and the wrapper in
// /usr/local/sbin, as before, and no mount.
func TestInstallCRIUBundle_DefaultLayout(t *testing.T) {
	src := fakeImageBundle(t)
	root := t.TempDir()
	confDir := t.TempDir()
	execHost := func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		t.Fatalf("default layout must not run host commands, ran %s %v", name, args)
		return "", nil
	}

	ver, err := installCRIUBundleLayout(context.Background(), src, root, confDir, defaultLayout, execHost)
	if err != nil {
		t.Fatalf("install: %v", err)
	}
	if ver != "Version: 4.2.1" {
		t.Errorf("version = %q", ver)
	}
	for _, f := range []string{"criu.bin", "ld-linux-x86-64.so.2", "VERSION"} {
		if _, err := os.Stat(filepath.Join(root, "var/lib/pod-migrate/criu-bundle", f)); err != nil {
			t.Errorf("bundle file %s: %v", f, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "var/lib/pod-migrate/criu-bundle", "licenses")); !os.IsNotExist(err) {
		t.Errorf("licenses/ must not be installed: %v", err)
	}
	if got := readFile(t, filepath.Join(root, "usr/local/sbin/criu")); got != imageWrapper {
		t.Errorf("/usr/local/sbin/criu = %q, want the image's wrapper", got)
	}
	if fi, err := os.Stat(filepath.Join(root, "usr/local/sbin/criu")); err != nil || fi.Mode().Perm() != 0o755 {
		t.Errorf("/usr/local/sbin/criu mode: %v %v", fi, err)
	}
	if got := readFile(t, filepath.Join(root, "var/lib/pod-migrate/criu-bundle/criu")); got != imageWrapper {
		t.Errorf("bundle criu = %q", got)
	}
	if !strings.Contains(readFile(t, filepath.Join(confDir, "runc.conf")), "file-validation buildid\n") {
		t.Error("runc.conf not written")
	}
	if _, err := os.Stat(filepath.Join(root, "home")); !os.IsNotExist(err) {
		t.Errorf("default layout must not touch /home: %v", err)
	}
}

// cosHost is a COS host root: /usr/local exists and is empty, and
// /home/kubernetes/bin exists.
func cosHost(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, d := range []string{"usr/local", "home/kubernetes/bin"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

// bindByLink stands in for `mount --bind src /usr/local` in a test host root:
// it replaces root/usr/local with a link to src, which the agent resolves
// inside root, as it would the bind mount.
func bindByLink(t *testing.T, root string, calls *[][]string) func(context.Context, []string, string, ...string) (string, error) {
	return func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		*calls = append(*calls, append([]string{name}, args...))
		if name != "mount" || len(args) != 3 || args[0] != "--bind" {
			return "", errors.New("unexpected host command")
		}
		dst := filepath.Join(root, args[2])
		if err := os.Remove(dst); err != nil {
			return "", err
		}
		return "", os.Symlink(args[1], dst)
	}
}

// COS: the bundle under /home/kubernetes/bin (the only writable host path that
// isn't mounted noexec), and the wrapper on containerd's PATH through a
// directory under /home/kubernetes/bin bound over /usr/local, which COS ships
// empty and read-only.
func TestInstallCRIUBundle_COSLayout(t *testing.T) {
	src := fakeImageBundle(t)
	root := cosHost(t)
	var calls [][]string

	if _, err := installCRIUBundleLayout(context.Background(), src, root, t.TempDir(), cosLayout, bindByLink(t, root, &calls)); err != nil {
		t.Fatalf("install: %v", err)
	}
	bundle := filepath.Join(root, "home/kubernetes/bin/pod-migrate/criu-bundle")
	for _, f := range []string{"criu.bin", "ld-linux-x86-64.so.2", "VERSION", "criu"} {
		if _, err := os.Stat(filepath.Join(bundle, f)); err != nil {
			t.Errorf("bundle file %s: %v", f, err)
		}
	}
	want := renderCRIUWrapper("/home/kubernetes/bin/pod-migrate/criu-bundle")
	if got := readFile(t, filepath.Join(bundle, "criu")); got != want {
		t.Errorf("bundle criu = %q, want %q", got, want)
	}
	if got := readFile(t, filepath.Join(root, "home/kubernetes/bin/pod-migrate/usr-local/sbin/criu")); got != want {
		t.Errorf("wrapper = %q, want %q", got, want)
	}
	wantCalls := [][]string{{"mount", "--bind", "/home/kubernetes/bin/pod-migrate/usr-local", "/usr/local"}}
	if !reflect.DeepEqual(calls, wantCalls) {
		t.Fatalf("host commands = %v, want %v", calls, wantCalls)
	}
	// containerd and the shims look criu up on PATHs that start with
	// /usr/local/sbin (or /opt/containerd/bin, then /usr/local/sbin).
	viaBind, err := resolvePathInRoot(root, "/usr/local/sbin/criu")
	if err != nil {
		t.Fatalf("/usr/local/sbin/criu through the bind: %v", err)
	}
	if got := readFile(t, viaBind); got != want {
		t.Errorf("/usr/local/sbin/criu through the bind = %q, want %q", got, want)
	}
	if _, err := os.Stat(filepath.Join(root, "var")); !os.IsNotExist(err) {
		t.Errorf("COS layout must not install under /var (noexec): %v", err)
	}

	// A restarted agent finds its bind in place and doesn't mount again.
	calls = nil
	if _, err := installCRIUBundleLayout(context.Background(), src, root, t.TempDir(), cosLayout, bindByLink(t, root, &calls)); err != nil {
		t.Fatalf("second install: %v", err)
	}
	if len(calls) != 0 {
		t.Errorf("second install ran %v, want no mount", calls)
	}
}

// A /usr/local with something in it isn't COS's empty one: binding over it
// would hide it, so the install fails instead.
func TestInstallCRIUBundle_COSLayout_RefusesNonEmptyUsrLocal(t *testing.T) {
	root := cosHost(t)
	writeFile(t, filepath.Join(root, "usr/local/bin/tool"), "x", 0o755)
	var calls [][]string

	_, err := installCRIUBundleLayout(context.Background(), fakeImageBundle(t), root, t.TempDir(), cosLayout, bindByLink(t, root, &calls))
	// The README quotes this prefix in the node's criu-reason.
	if err == nil || !strings.HasPrefix(err.Error(), "host /usr/local isn't empty") || !strings.Contains(err.Error(), "bin") {
		t.Fatalf("err = %v, want a refusal naming /usr/local and its entry", err)
	}
	if len(calls) != 0 {
		t.Errorf("ran %v, want no mount", calls)
	}
}

func TestInstallCRIUBundle_COSLayout_MountFailure(t *testing.T) {
	root := cosHost(t)
	execHost := func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		return "mount: permission denied", errors.New("exit status 32")
	}
	_, err := installCRIUBundleLayout(context.Background(), fakeImageBundle(t), root, t.TempDir(), cosLayout, execHost)
	if err == nil || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("err = %v, want the mount failure with its output", err)
	}
}

// A mount that exits 0 but leaves /usr/local as it was fails the install.
func TestInstallCRIUBundle_COSLayout_BindNotInPlace(t *testing.T) {
	root := cosHost(t)
	execHost := func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		return "", nil
	}
	_, err := installCRIUBundleLayout(context.Background(), fakeImageBundle(t), root, t.TempDir(), cosLayout, execHost)
	if err == nil || !strings.Contains(err.Error(), "/usr/local") {
		t.Fatalf("err = %v, want the bind reported missing", err)
	}
}

// The kernel check runs the bundle the layout installed.
func TestVerifyCRIU_RunsLayoutBundle(t *testing.T) {
	var ran string
	execHost := func(ctx context.Context, env []string, name string, args ...string) (string, error) {
		ran = name
		return "Looks good.", nil
	}
	if ok, _, _ := verifyCRIU(context.Background(), execHost, cosLayout.bundleDir); !ok {
		t.Fatal("verifyCRIU = false")
	}
	if ran != "/home/kubernetes/bin/pod-migrate/criu-bundle/criu" {
		t.Errorf("ran %q", ran)
	}
}

// The agent's DaemonSet runs on COS nodes and mounts no host directory that
// COS has read-only: it installs through the host's root (/proc/1/root),
// which takes hostPID.
func TestDeployManifest_AgentRunsOnCOS(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var ds map[string]any
	for _, doc := range strings.Split(string(raw), "\n---\n") {
		var obj map[string]any
		if err := yaml.Unmarshal([]byte(doc), &obj); err != nil {
			t.Fatalf("parse deploy.yaml: %v", err)
		}
		if obj["kind"] == "DaemonSet" {
			if md, _ := obj["metadata"].(map[string]any); md["name"] == "pod-migrate-agent" {
				ds = obj
			}
		}
	}
	if ds == nil {
		t.Fatal("DaemonSet pod-migrate-agent not in deploy.yaml")
	}
	pod := ds["spec"].(map[string]any)["template"].(map[string]any)["spec"].(map[string]any)
	if pod["hostPID"] != true {
		t.Error("the agent installs through /proc/1/root, which needs hostPID: true")
	}
	affinity, _ := yaml.Marshal(pod["affinity"])
	if strings.Contains(string(affinity), "gke-os-distribution") {
		t.Errorf("the agent must not exclude COS nodes:\n%s", affinity)
	}
	for _, v := range pod["volumes"].([]any) {
		hp, _ := v.(map[string]any)["hostPath"].(map[string]any)
		if p, _ := hp["path"].(string); p == "/usr" || strings.HasPrefix(p, "/usr/") {
			t.Errorf("hostPath %s: COS's /usr is read-only, and a DirectoryOrCreate there keeps the agent from starting", p)
		}
	}
}

// The source node facts read the CRIU version from the agent's own image,
// which every layout installs from: on COS nothing is installed under
// /var/lib/pod-migrate, which the agent mounts.
func TestFactsProbe_CRIUVersionFileIndependentOfLayout(t *testing.T) {
	for _, l := range []criuLayout{defaultLayout, cosLayout} {
		got := (&Node{layout: l}).newFactsProbe().criuVersionFile
		if want := filepath.Join(imageBundleDir, "VERSION"); got != want {
			t.Errorf("layout %s: criuVersionFile = %q, want %q", l.name, got, want)
		}
	}
}

// The agent's restore runs runc with the layout's bundle first on PATH, and
// records that bundle's version.
func TestRestore_UsesLayoutBundle(t *testing.T) {
	tmpDir := t.TempDir()
	confDir := filepath.Join(tmpDir, "etc-criu")
	writeFile(t, filepath.Join(confDir, "runc.conf"), "file-validation buildid\n", 0o644)
	chkDir := filepath.Join(tmpDir, "chk")
	if err := os.MkdirAll(filepath.Join(chkDir, "checkpoint"), 0o755); err != nil {
		t.Fatal(err)
	}
	var versionRan string
	var restoreEnv []string
	r := &restorer{
		pmRootDir:       tmpDir,
		criuConfDirPath: confDir,
		bundleDir:       cosLayout.bundleDir,
		findContainerFn: func(ctx context.Context, podNS, podName, podUID, containerName string) (*containerRef, error) {
			return &containerRef{ID: "holder-ctr-1", SandboxID: "tgtsandbox222", Pid: 5555, Spec: sampleTargetSpec(), ImageName: "gcr.io/trusted/app:v1"}, nil
		},
		readProcCgroupFn: func(pid int) ([]byte, error) {
			return []byte("0::/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod123.slice/cri-containerd-holder.scope\n"), nil
		},
		openPodNetnsFn: func(holderPid int) (*os.File, error) { return os.CreateTemp(t.TempDir(), "netns-holder") },
		prepareRootfsFn: func(ctx context.Context, imageName, key, bundleDir, rootfsDiffTar string, labels map[string]string) (string, error) {
			rf := filepath.Join(bundleDir, "rootfs")
			return rf, os.MkdirAll(rf, 0o755)
		},
		execHost: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			if len(args) == 1 && args[0] == "--version" {
				versionRan = name
			}
			return "Version: 4.2.1", nil
		},
		execRestoreFn: func(ctx context.Context, env []string, logPath string, name string, args ...string) (string, error) {
			restoreEnv = env
			return "stop here", errors.New("exit status 1")
		},
		execNetnsFn: func(ctx context.Context, netns *os.File, name string, args ...string) (string, error) { return "", nil },
	}
	_, _ = r.restore(context.Background(), restoreInputs{
		MigrationName: "mig-cos", PodNamespace: "default", TargetPod: "web-1", ContainerName: "app",
		SourcePodUID: "src-uid", TargetPodUID: "tgt-uid", CheckpointDir: chkDir, SrcSpec: sampleSpec(),
		ImageName: "gcr.io/trusted/app:v1",
	})
	if versionRan != "/home/kubernetes/bin/pod-migrate/criu-bundle/criu" {
		t.Errorf("criu --version ran %q", versionRan)
	}
	want := []string{"PATH=/home/kubernetes/bin/pod-migrate/criu-bundle:" + hostRuncPATH}
	if !reflect.DeepEqual(restoreEnv, want) {
		t.Errorf("runc restore env = %v, want %v", restoreEnv, want)
	}
}

// useLayout points the restores at the layout's bundle.
func TestUseLayout_WiresRestorer(t *testing.T) {
	n := &Node{Restorer: &restorer{}}
	n.useLayout(cosLayout)
	if n.criuLayout() != cosLayout {
		t.Errorf("layout = %+v", n.criuLayout())
	}
	if got := n.Restorer.criuBundle(); got != cosLayout.bundleDir {
		t.Errorf("restorer bundle = %q, want %q", got, cosLayout.bundleDir)
	}
	if n.installBundleFn == nil {
		t.Error("useLayout must set the install")
	}
}

// Bootstrap runs the kernel check with the node's layout's bundle.
func TestBootstrap_KernelCheckUsesLayoutBundle(t *testing.T) {
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "cos-node"},
		Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"}},
	}
	var ran []string
	nd := &Node{
		Client:          fake.NewClientBuilder().WithScheme(clientgoscheme.Scheme).WithObjects(nodeObj).Build(),
		NodeName:        "cos-node",
		SocketPath:      "/run/containerd/containerd.sock",
		Inspector:       fakeReadyInspector(),
		layout:          cosLayout,
		statSocketFn:    func(string) (uint64, error) { return 7, nil },
		installBundleFn: func() (string, error) { return "Version: 4.2.1", nil },
		execHostFn: func(ctx context.Context, env []string, name string, args ...string) (string, error) {
			ran = append(ran, name)
			return "Looks good.", nil
		},
	}
	if err := nd.Bootstrap(context.Background()); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if len(ran) == 0 || ran[0] != "/home/kubernetes/bin/pod-migrate/criu-bundle/criu" {
		t.Errorf("host commands = %v, want the COS bundle's criu first", ran)
	}
	if !nd.IsReady() {
		t.Error("node not ready")
	}
}
