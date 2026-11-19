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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme: %v", err)
	}
	return s
}

func TestNode_DemoteAndRecoverOnContainerdRestart(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				NodeAnnotationCRIUReady:   "true",
				NodeAnnotationCRIUVersion: "v3.19",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "containerd.sock")
	if err := os.WriteFile(sockPath, []byte("sock1"), 0o600); err != nil {
		t.Fatalf("write sock1: %v", err)
	}

	nd := &Node{
		Client:       c,
		NodeName:     "test-node",
		SocketPath:   sockPath,
		statSocketFn: getSocketInode,
	}

	bootstrapped := 0
	nd.bootstrapFn = func(ctx context.Context) error {
		bootstrapped++
		nd.mu.Lock()
		nd.ready = true
		nd.version = "v3.19"
		nd.mu.Unlock()
		return annotateNode(ctx, nd.Client, nd.NodeName, "v3.19", "", "", true)
	}

	// 1. Initial bootstrap
	if err := nd.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if !nd.IsReady() {
		t.Fatal("expected nd.IsReady() == true")
	}
	if bootstrapped != 1 {
		t.Fatalf("expected bootstrapped == 1, got %d", bootstrapped)
	}

	var cur corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
		t.Fatalf("expected criu-ready=true, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	if cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
		t.Fatalf("expected criu-version=v3.19, got %q", cur.Annotations[NodeAnnotationCRIUVersion])
	}

	// 2. Demote the node on cached failure
	if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err != nil {
		t.Fatalf("DemoteNode: %v", err)
	}
	if nd.IsReady() {
		t.Fatal("expected nd.IsReady() == false after demotion")
	}

	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected criu-ready=false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	// Version must NOT be erased or blanked!
	if cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
		t.Fatalf("expected criu-version preserved as v3.19, got %q", cur.Annotations[NodeAnnotationCRIUVersion])
	}
	if cur.Annotations[NodeAnnotationCRIUReason] != ReasonContainerdCachedCRIU {
		t.Fatalf("expected criu-reason=%q, got %q", ReasonContainerdCachedCRIU, cur.Annotations[NodeAnnotationCRIUReason])
	}
	if cur.Annotations[NodeAnnotationCRIURemediation] != RemediationRestartContainerd {
		t.Fatalf("expected criu-remediation=%q, got %q", RemediationRestartContainerd, cur.Annotations[NodeAnnotationCRIURemediation])
	}

	// 3. Probe before containerd restart: socket inode is unchanged, node remains unready
	recovered, err := nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness error: %v", err)
	}
	if recovered {
		t.Fatal("expected recovered=false when containerd has not restarted")
	}
	if nd.IsReady() {
		t.Fatal("expected node to remain unready")
	}
	if bootstrapped != 1 {
		t.Fatalf("expected bootstrapped still 1, got %d", bootstrapped)
	}

	// 4. Simulate containerd restart: remove socket and create new one (new inode)
	if err := os.Remove(sockPath); err != nil {
		t.Fatalf("remove sock: %v", err)
	}
	// Consume the freed inode with a dummy file to ensure the new socket receives a distinct inode
	dummy := filepath.Join(tmpDir, "dummy")
	if err := os.WriteFile(dummy, []byte("dummy"), 0o600); err != nil {
		t.Fatalf("write dummy: %v", err)
	}
	if err := os.WriteFile(sockPath, []byte("sock2"), 0o600); err != nil {
		t.Fatalf("write sock2: %v", err)
	}

	// 5. Probe after containerd restart: new inode detected, triggers re-bootstrap!
	recovered, err = nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness after restart error: %v", err)
	}
	if !recovered {
		t.Fatal("expected recovered=true after containerd restart")
	}
	if !nd.IsReady() {
		t.Fatal("expected node to be ready after restart recovery")
	}
	if bootstrapped != 2 {
		t.Fatalf("expected bootstrapped == 2, got %d", bootstrapped)
	}

	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node after recovery: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
		t.Fatalf("expected criu-ready=true after recovery, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	if cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
		t.Fatalf("expected criu-version=v3.19, got %q", cur.Annotations[NodeAnnotationCRIUVersion])
	}
	if reason, exists := cur.Annotations[NodeAnnotationCRIUReason]; exists {
		t.Fatalf("expected criu-reason deleted after recovery, got %q", reason)
	}
	if remediation, exists := cur.Annotations[NodeAnnotationCRIURemediation]; exists {
		t.Fatalf("expected criu-remediation deleted after recovery, got %q", remediation)
	}
}

func TestPSEngineCheckpoint_DemotesNodeOnCachedFailure(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				NodeAnnotationCRIUReady:   "true",
				NodeAnnotationCRIUVersion: "v3.19",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"checkpoint failed: criu binary not found in shim path or system PATH"}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	nd := &Node{
		Client:   c,
		NodeName: "test-node",
		version:  "v3.19",
		ready:    true,
		Kubelet: &kubeletCheckpoint{
			TokenPath: tokenFile,
			Endpoint:  srv.URL,
			Client:    srv.Client(),
		},
	}

	_, err := nd.PSEngineCheckpoint(ctx, "default", "pod-1", "container-1")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU, got: %v", err)
	}

	// Assert the node was demoted via the client patch!
	var cur corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected criu-ready=false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	if cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
		t.Fatalf("expected criu-version=v3.19 preserved, got %q", cur.Annotations[NodeAnnotationCRIUVersion])
	}
	if cur.Annotations[NodeAnnotationCRIUReason] != ReasonContainerdCachedCRIU {
		t.Fatalf("expected reason %q, got %q", ReasonContainerdCachedCRIU, cur.Annotations[NodeAnnotationCRIUReason])
	}
	if cur.Annotations[NodeAnnotationCRIURemediation] != RemediationRestartContainerd {
		t.Fatalf("expected remediation %q, got %q", RemediationRestartContainerd, cur.Annotations[NodeAnnotationCRIURemediation])
	}
}

type failPatchClient struct {
	client.Client
}

func (f *failPatchClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	return errors.New("simulated patch failure")
}

type errSink struct {
	errCalls atomic.Int32
}

func (s *errSink) Init(logr.RuntimeInfo)          {}
func (s *errSink) Enabled(int) bool               { return true }
func (s *errSink) Info(int, string, ...any)       {}
func (s *errSink) WithValues(...any) logr.LogSink { return s }
func (s *errSink) WithName(string) logr.LogSink   { return s }
func (s *errSink) Error(error, string, ...any)    { s.errCalls.Add(1) }

func TestPSEngineCheckpoint_DemoteErrorLogged(t *testing.T) {
	sink := &errSink{}
	ctx := logf.IntoContext(context.Background(), logr.New(sink))
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
	c := &failPatchClient{Client: baseClient}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"checkpoint failed: criu binary not found in shim path or system PATH"}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	nd := &Node{
		Client:   c,
		NodeName: "test-node",
		Kubelet: &kubeletCheckpoint{
			TokenPath: tokenFile,
			Endpoint:  srv.URL,
			Client:    srv.Client(),
		},
	}

	// Should not panic, logs the demotion patch failure, and returns ErrContainerdCachedCRIU.
	_, err := nd.PSEngineCheckpoint(ctx, "default", "pod-1", "container-1")
	if err == nil || !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU, got: %v", err)
	}
	if sink.errCalls.Load() != 1 {
		t.Fatalf("expected demotion patch error to be logged once, got %d", sink.errCalls.Load())
	}
}

func TestPSEngineCheckpoint_Ordinary500DoesNotDemoteNode(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				NodeAnnotationCRIUReady:   "true",
				NodeAnnotationCRIUVersion: "v3.19",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"checkpoint failed: container not running"}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	nd := &Node{
		Client:   c,
		NodeName: "test-node",
		version:  "v3.19",
		ready:    true,
		Kubelet: &kubeletCheckpoint{
			TokenPath: tokenFile,
			Endpoint:  srv.URL,
			Client:    srv.Client(),
		},
	}

	if _, err := nd.PSEngineCheckpoint(ctx, "default", "pod-1", "container-1"); err == nil {
		t.Fatal("expected checkpoint error, got nil")
	}
	if !nd.IsReady() {
		t.Fatal("ordinary kubelet 500 must leave nd.IsReady() == true")
	}
	var cur corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
		t.Fatalf("expected criu-ready=true preserved, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
}

type flakyNodeClient struct {
	client.Client
	getCalls   atomic.Int32
	patchCalls atomic.Int32
	failGets   int32
	failPatch  int32
}

func (f *flakyNodeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if n := f.getCalls.Add(1); n <= f.failGets {
		return errors.New("transient node get timeout")
	}
	return f.Client.Get(ctx, key, obj, opts...)
}

func (f *flakyNodeClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if n := f.patchCalls.Add(1); n <= f.failPatch {
		return errors.New("transient node patch 503")
	}
	return f.Client.Patch(ctx, obj, patch, opts...)
}

func TestNode_DeterministicFailuresDoNotReprobeWithoutRestart(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	t.Run("bundle install failure marks node false once and stays quiet", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-node",
				Annotations: map[string]string{NodeAnnotationCRIUReady: "true"},
			},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
		fc := &flakyNodeClient{Client: base}

		installCalls := 0
		nd := &Node{
			Client:     fc,
			NodeName:   "test-node",
			SocketPath: "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) {
				return 4242, nil
			},
			installBundleFn: func() (string, error) {
				installCalls++
				return "", errors.New("disk full")
			},
		}

		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected install error, got nil")
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false")
		}

		var cur corev1.Node
		if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Fatalf("expected stale criu-ready=true cleared to false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		getsAfterBootstrap := fc.getCalls.Load()
		patchesAfterBootstrap := fc.patchCalls.Load()

		for i := 1; i <= 5; i++ {
			ready, err := nd.ProbeReadiness(ctx)
			if err != nil {
				t.Fatalf("probe %d: unexpected error: %v", i, err)
			}
			if ready {
				t.Fatalf("probe %d: expected ready=false", i)
			}
		}
		if installCalls != 1 {
			t.Fatalf("expected 1 bundle install call across 5 ticks, got %d", installCalls)
		}
		if fc.getCalls.Load() != getsAfterBootstrap || fc.patchCalls.Load() != patchesAfterBootstrap {
			t.Fatalf("expected 0 extra API calls across 5 ticks, got extra gets=%d patches=%d",
				fc.getCalls.Load()-getsAfterBootstrap, fc.patchCalls.Load()-patchesAfterBootstrap)
		}
	})
}

func TestNode_TransientAPIErrorsRetryAndConvergeWithoutContainerdRestart(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	t.Run("transient GET error on recovery retries on next tick", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
		fc := &flakyNodeClient{Client: base, failGets: 1}

		nd := &Node{
			Client:          fc,
			NodeName:        "test-node",
			SocketPath:      "/run/containerd/containerd.sock",
			statSocketFn:    func(string) (uint64, error) { return 7777, nil },
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				return "Looks good.\n", nil
			},
		}

		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected initial transient GET error")
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false after failed GET")
		}

		// Without restarting containerd (same inode 7777), the next ProbeReadiness tick retries and recovers!
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness retry failed: %v", err)
		}
		if !ready || !nd.IsReady() {
			t.Fatal("expected node to recover to ready=true on retry tick")
		}
	})

	t.Run("transient PATCH error after verifyCRIU passes leaves ready=false and retries cleanly", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-node",
				Annotations: map[string]string{NodeAnnotationCRIUReady: "false"},
			},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
		fc := &flakyNodeClient{Client: base, failPatch: 1}

		nd := &Node{
			Client:          fc,
			NodeName:        "test-node",
			SocketPath:      "/run/containerd/containerd.sock",
			lastSocketInode: 1000, // old pre-restart inode
			statSocketFn:    func(string) (uint64, error) { return 2000, nil },
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				return "Looks good.\n", nil
			},
		}

		// First post-restart probe hits transient PATCH 503.
		ready, err := nd.ProbeReadiness(ctx)
		if err == nil {
			t.Fatal("expected transient PATCH error")
		}
		if ready || nd.IsReady() {
			t.Fatal("expected ready=false when annotateNode PATCH fails")
		}

		// Second tick (still inode 2000) must NOT exit early at if isReady; it retries and writes criu-ready=true.
		ready, err = nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("second ProbeReadiness failed: %v", err)
		}
		if !ready || !nd.IsReady() {
			t.Fatal("expected ready=true on second ProbeReadiness tick")
		}
		var cur corev1.Node
		if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "true" {
			t.Fatalf("expected criu-ready=true on API server, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
	})
}

func TestNode_DemotePatchFailureRetriedAndPoisonedInodeBlocked(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				NodeAnnotationCRIUReady:   "true",
				NodeAnnotationCRIUVersion: "v3.19",
			},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
	fc := &flakyNodeClient{Client: base, failPatch: 1}

	currentIno := uint64(9001)
	nd := &Node{
		Client:          fc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		ready:           true,
		version:         "v3.19",
		lastSocketInode: 8000, // containerd restarted while ready, then got poisoned on 9001
		statSocketFn:    func(string) (uint64, error) { return currentIno, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
	}

	// DemoteNode fails its first PATCH transiently.
	if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err == nil {
		t.Fatal("expected initial DemoteNode PATCH error")
	}
	if nd.IsReady() {
		t.Fatal("expected nd.IsReady() == false immediately in memory")
	}

	// Next ProbeReadiness tick retries the pending demotion PATCH and does NOT re-promote poisoned inode 9001.
	ready, err := nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness demotion retry failed: %v", err)
	}
	if ready || nd.IsReady() {
		t.Fatal("poisoned inode 9001 must remain unready")
	}
	var cur corev1.Node
	if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected criu-ready=false persisted on retry, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}

	// Even a direct Bootstrap call on poisoned inode 9001 must not overwrite criu-ready back to true.
	if err := nd.Bootstrap(ctx); !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU on poisoned inode, got %v", err)
	}
	if nd.IsReady() {
		t.Fatal("poisoned inode must not become ready before containerd restarts")
	}
}

func TestNode_UnsupportedContainerdVersionDoesNotReprobeWithoutRestart(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	// containerd 1.6 is unsupported
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{
				ContainerRuntimeVersion: "containerd://1.6.28",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	nd := &Node{
		Client:     c,
		NodeName:   "test-node",
		SocketPath: "/run/containerd/containerd.sock",
		statSocketFn: func(string) (uint64, error) {
			return 5555, nil
		},
	}

	// Initial bootstrap fails with unsupported containerd version
	if err := nd.Bootstrap(ctx); err == nil {
		t.Fatal("expected unsupported containerd version error, got nil")
	}
	if nd.IsReady() {
		t.Fatal("expected nd.IsReady() == false")
	}

	// Inode was recorded before the early return!
	nd.mu.Lock()
	ino := nd.lastSocketInode
	nd.mu.Unlock()
	if ino != 5555 {
		t.Fatalf("expected lastSocketInode=5555, got %d", ino)
	}

	// Subsequent probes must not re-probe
	for i := 1; i <= 5; i++ {
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("probe %d: unexpected error: %v", i, err)
		}
		if ready {
			t.Fatalf("probe %d: expected ready=false", i)
		}
	}
}

func TestNode_ProbeReadinessReturnsFalseWhenVerifyCRIUFalse(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	currentInode := uint64(100)
	nd := &Node{
		Client:     c,
		NodeName:   "test-node",
		SocketPath: "/run/containerd/containerd.sock",
		statSocketFn: func(string) (uint64, error) {
			return currentInode, nil
		},
	}

	// Bootstrap marks node unready (verifyCRIU was false)
	nd.bootstrapFn = func(ctx context.Context) error {
		nd.mu.Lock()
		nd.ready = false
		nd.mu.Unlock()
		return nil
	}

	if err := nd.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if nd.IsReady() {
		t.Fatal("expected nd.IsReady() == false")
	}

	// Simulate containerd restart: new inode
	currentInode = 200

	// ProbeReadiness triggers re-bootstrap, but CRIU is not verified ok.
	// It must return ready=false, not true!
	ready, err := nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness: %v", err)
	}
	if ready {
		t.Fatal("expected ProbeReadiness to return false when verifyCRIU was false")
	}
}
