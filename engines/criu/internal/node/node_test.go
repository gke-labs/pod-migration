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
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

func newTestScheme(t *testing.T) *k8sruntime.Scheme {
	s := k8sruntime.NewScheme()
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
				NodeAnnotationCRIUReady:       "false",
				NodeAnnotationCRIUReason:      "stale-reason",
				NodeAnnotationCRIURemediation: "stale-remediation",
			},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	tmpDir := t.TempDir()
	sockPath := filepath.Join(tmpDir, "containerd.sock")
	if err := os.WriteFile(sockPath, []byte("sock1"), 0o600); err != nil {
		t.Fatalf("write sock1: %v", err)
	}

	bootstrapped := 0
	nd := &Node{
		Client:       c,
		NodeName:     "test-node",
		SocketPath:   sockPath,
		statSocketFn: getSocketInode,
		installBundleFn: func() (string, error) {
			bootstrapped++
			return "v3.19", nil
		},
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
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
	if reason, exists := cur.Annotations[NodeAnnotationCRIUReason]; exists {
		t.Fatalf("expected stale criu-reason deleted on initial promote, got %q", reason)
	}
	if remediation, exists := cur.Annotations[NodeAnnotationCRIURemediation]; exists {
		t.Fatalf("expected stale criu-remediation deleted on initial promote, got %q", remediation)
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

	t.Run("verifyCRIU false marks node false once and stays quiet", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
		fc := &flakyNodeClient{Client: base}

		execCalls := 0
		nd := &Node{
			Client:          fc,
			NodeName:        "test-node",
			SocketPath:      "/run/containerd/containerd.sock",
			statSocketFn:    func(string) (uint64, error) { return 4343, nil },
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				execCalls++
				return "Error: kernel lacks CONFIG_CHECKPOINT_RESTORE\n", nil
			},
		}

		if err := nd.Bootstrap(ctx); err != nil {
			t.Fatalf("Bootstrap with verifyCRIU=false should return nil after annotating false: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false")
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
		if execCalls != 1 || fc.getCalls.Load() != getsAfterBootstrap || fc.patchCalls.Load() != patchesAfterBootstrap {
			t.Fatalf("expected 0 extra exec/API calls across 5 ticks, got exec=%d extraGets=%d extraPatches=%d",
				execCalls, fc.getCalls.Load()-getsAfterBootstrap, fc.patchCalls.Load()-patchesAfterBootstrap)
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
	execCalls := 0
	nd := &Node{
		Client:          fc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		ready:           true,
		version:         "v3.19",
		lastSocketInode: 8000, // containerd restarted while ready, then got poisoned on 9001
		statSocketFn:    func(string) (uint64, error) { return atomic.LoadUint64(&currentIno), nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			execCalls++
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

	// Direct Bootstrap on already-poisoned inode 9001 must short-circuit before verifyCRIU exec.
	if err := nd.Bootstrap(ctx); !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU on poisoned inode, got %v", err)
	}
	if execCalls != 0 {
		t.Fatalf("expected verifyCRIU skipped on already-poisoned inode, got %d exec calls", execCalls)
	}
	if nd.IsReady() {
		t.Fatal("poisoned inode must not become ready before containerd restarts")
	}

	// Once containerd restarts onto a fresh inode (9002), ProbeReadiness recovers cleanly.
	atomic.StoreUint64(&currentIno, 9002)
	ready, err = nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness after containerd restart to 9002 failed: %v", err)
	}
	if !ready || !nd.IsReady() || execCalls != 1 {
		t.Fatalf("expected node to recover to ready=true with 1 exec on 9002, got ready=%v execCalls=%d", ready, execCalls)
	}
}

func TestNode_ConcurrentBootstrapCannotOverwriteDemotion(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-node",
			Annotations: map[string]string{NodeAnnotationCRIURemediation: "stale-remediation"},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	execStarted := make(chan struct{})
	allowExecFinish := make(chan struct{})

	var nd *Node
	nd = &Node{
		Client:          c,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 3030, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			close(execStarted)
			<-allowExecFinish
			return "Looks good.\n", nil
		},
	}

	bootDone := make(chan error, 1)
	go func() {
		bootDone <- nd.Bootstrap(ctx)
	}()

	<-execStarted
	// DemoteNode runs while Bootstrap is in flight on the same socket inode.
	if err := nd.DemoteNode(ctx, "custom-demotion", ""); err != nil {
		t.Fatalf("DemoteNode: %v", err)
	}
	close(allowExecFinish)

	if err := <-bootDone; err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if nd.IsReady() {
		t.Fatal("concurrent DemoteNode must not be overwritten back to ready=true")
	}
	var cur corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected criu-ready=false on API server, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	if _, exists := cur.Annotations[NodeAnnotationCRIURemediation]; exists {
		t.Fatalf("expected empty remediation deleted, got %q", cur.Annotations[NodeAnnotationCRIURemediation])
	}
}

type hookPatchClient struct {
	client.Client
	onPatch func(call int32, obj client.Object) error
	calls   atomic.Int32
}

func (h *hookPatchClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	c := h.calls.Add(1)
	if h.onPatch != nil {
		if err := h.onPatch(c, obj); err != nil {
			return err
		}
	}
	return h.Client.Patch(ctx, obj, patch, opts...)
}

func TestNode_MidAnnotateNodeDemotePreservesUnreadyAndRetries(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test-node",
			Annotations: map[string]string{NodeAnnotationCRIURemediation: "stale-remediation"},
		},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	patchEntered := make(chan struct{})
	allowPatchFinish := make(chan struct{})
	demoteQueued := make(chan struct{})

	var nd *Node
	demoteErrCh := make(chan error, 1)

	hc := &hookPatchClient{
		Client: base,
		onPatch: func(call int32, _ client.Object) error {
			switch call {
			case 1:
				// Bootstrap's publish(ready=true) is mid-Patch while DemoteNode arrives!
				close(patchEntered)
				<-allowPatchFinish
				return nil
			case 2:
				// DemoteNode's first PATCH fails transiently to verify pendingDemote survives publish!
				return errors.New("simulated transient demote patch error")
			default:
				return nil
			}
		},
	}

	nd = &Node{
		Client:          hc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 5050, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
	}

	bootErrCh := make(chan error, 1)
	go func() {
		bootErrCh <- nd.Bootstrap(ctx)
	}()

	<-patchEntered
	go func() {
		close(demoteQueued)
		demoteErrCh <- nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd)
	}()
	<-demoteQueued
	// Wait until demoteSeq increments under n.mu before releasing publish's Patch.
	for {
		nd.mu.Lock()
		seq := nd.demoteSeq
		nd.mu.Unlock()
		if seq == 1 {
			break
		}
		runtime.Gosched()
	}
	close(allowPatchFinish)

	if err := <-bootErrCh; !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected Bootstrap to return ErrContainerdCachedCRIU on mid-patch demotion, got %v", err)
	}
	if err := <-demoteErrCh; err == nil {
		t.Fatal("expected simulated transient demote patch error on call 2")
	}
	if nd.IsReady() {
		t.Fatal("nd.IsReady() must remain false when DemoteNode ran during publish's annotateNode")
	}

	// Next ProbeReadiness tick retries the failed pendingDemote PATCH and writes criu-ready=false!
	ready, err := nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness retry failed: %v", err)
	}
	if ready || nd.IsReady() {
		t.Fatal("expected node to remain unready on poisoned inode 5050")
	}
	var cur corev1.Node
	if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected criu-ready=false repaired on API server, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
	if cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
		t.Fatalf("expected criu-version=v3.19 preserved on demotion, got %q", cur.Annotations[NodeAnnotationCRIUVersion])
	}
}

func TestNode_CrossRestartDemotionAndZeroInodeDemotionNotClobbered(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	t.Run("containerd restarts 100->200 during Bootstrap and 200 is demoted", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

		var currIno atomic.Uint64
		currIno.Store(100)

		execStarted := make(chan struct{})
		allowExecFinish := make(chan struct{})

		nd := &Node{
			Client:          c,
			NodeName:        "test-node",
			SocketPath:      "/run/containerd/containerd.sock",
			statSocketFn:    func(string) (uint64, error) { return currIno.Load(), nil },
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				close(execStarted)
				<-allowExecFinish
				return "Looks good.\n", nil
			},
		}

		bootDone := make(chan error, 1)
		go func() {
			bootDone <- nd.Bootstrap(ctx)
		}()

		<-execStarted
		// containerd restarts to inode 200, and a checkpoint on 200 gets poisoned!
		currIno.Store(200)
		if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err != nil {
			t.Fatalf("DemoteNode(200): %v", err)
		}
		close(allowExecFinish)

		if err := <-bootDone; err == nil {
			t.Fatal("expected stale Bootstrap(100) to abort when socket moved to 200")
		}
		if nd.IsReady() {
			t.Fatal("stale Bootstrap(100) must not promote poisoned inode 200")
		}
		var cur corev1.Node
		if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Fatalf("expected criu-ready=false preserved for poisoned 200, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
	})

	t.Run("DemoteNode with ino==0 concurrent with Bootstrap(v.ino!=0)", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

		var failStat atomic.Bool
		execStarted := make(chan struct{})
		allowExecFinish := make(chan struct{})

		nd := &Node{
			Client:     c,
			NodeName:   "test-node",
			SocketPath: "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) {
				if failStat.Load() {
					return 0, errors.New("socket temporarily unavailable")
				}
				return 6060, nil
			},
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				close(execStarted)
				<-allowExecFinish
				return "Looks good.\n", nil
			},
		}

		bootDone := make(chan error, 1)
		go func() {
			bootDone <- nd.Bootstrap(ctx)
		}()

		<-execStarted
		failStat.Store(true)
		if err := nd.DemoteNode(ctx, "manual-demote", "investigate"); err != nil {
			t.Fatalf("DemoteNode(ino=0): %v", err)
		}
		failStat.Store(false)
		close(allowExecFinish)

		if err := <-bootDone; err != nil {
			t.Fatalf("Bootstrap: %v", err)
		}
		if nd.IsReady() {
			t.Fatal("DemoteNode with ino==0 must not be overwritten by concurrent Bootstrap")
		}
		var cur corev1.Node
		if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Fatalf("expected criu-ready=false on API server, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil || ready || nd.IsReady() {
			t.Fatalf("expected ProbeReadiness to remain unready without re-promoting, got ready=%v err=%v", ready, err)
		}
	})

	t.Run("mid-Patch DemoteNode(ino==0) on post-restart Bootstrap updates lastSocketInode", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

		patchEntered := make(chan struct{})
		allowPatchFinish := make(chan struct{})
		hc := &hookPatchClient{
			Client: base,
			onPatch: func(call int32, _ client.Object) error {
				if call == 1 {
					close(patchEntered)
					<-allowPatchFinish
				}
				return nil
			},
		}

		var failStat atomic.Bool
		nd := &Node{
			Client:          hc,
			NodeName:        "test-node",
			SocketPath:      "/run/containerd/containerd.sock",
			lastSocketInode: 1000, // prior pre-restart socket inode
			statSocketFn: func(string) (uint64, error) {
				if failStat.Load() {
					return 0, errors.New("socket temporarily unavailable")
				}
				return 6060, nil
			},
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				return "Looks good.\n", nil
			},
		}

		bootDone := make(chan error, 1)
		go func() {
			bootDone <- nd.Bootstrap(ctx)
		}()

		<-patchEntered
		failStat.Store(true)
		demoteDone := make(chan error, 1)
		go func() {
			demoteDone <- nd.DemoteNode(ctx, "manual-demote", "investigate")
		}()
		for {
			nd.mu.Lock()
			seq := nd.demoteSeq
			nd.mu.Unlock()
			if seq == 1 {
				break
			}
			runtime.Gosched()
		}
		failStat.Store(false)
		close(allowPatchFinish)

		if err := <-bootDone; err != nil {
			t.Fatalf("Bootstrap: %v", err)
		}
		if err := <-demoteDone; err != nil {
			t.Fatalf("DemoteNode(ino=0): %v", err)
		}
		if nd.IsReady() {
			t.Fatal("expected nd.IsReady() == false after mid-Patch DemoteNode(ino=0)")
		}

		// Subsequent ProbeReadiness tick on unchanged inode 6060 must NOT re-promote the demoted node!
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("ProbeReadiness: %v", err)
		}
		if ready || nd.IsReady() {
			t.Fatal("ProbeReadiness must not re-promote demoted node on unchanged socket 6060")
		}
		var cur corev1.Node
		if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Fatalf("expected criu-ready=false preserved on API server, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}
	})
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
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
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
		installBundleFn: func() (string, error) {
			return "v3.19", nil
		},
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Error: kernel lacks CONFIG_CHECKPOINT_RESTORE\n", nil
		},
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

func TestNode_PersistentPatchFailureRetriesOnlyAnnotation(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	t.Run("healthy verifyCRIU=true retries only annotateNode across 7 failed PATCHes", func(t *testing.T) {
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
		fc := &flakyNodeClient{Client: base, failPatch: 7}

		installCalls := 0
		execCalls := 0
		nd := &Node{
			Client:       fc,
			NodeName:     "test-node",
			SocketPath:   "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) { return 4400, nil },
			installBundleFn: func() (string, error) {
				installCalls++
				return "v3.19", nil
			},
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				execCalls++
				return "Looks good.\n", nil
			},
		}

		// Tick 1: Bootstrap computes verdict once, PATCH #1 fails.
		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected PATCH #1 failure")
		}
		if nd.IsReady() {
			t.Fatal("expected ready=false while PATCH is failing")
		}

		// Ticks 2..7: 6 ProbeReadiness ticks also fail PATCH (#2..#7), without re-running bundle install or criu check.
		for i := 2; i <= 7; i++ {
			ready, err := nd.ProbeReadiness(ctx)
			if err == nil {
				t.Fatalf("tick %d: expected PATCH failure", i)
			}
			if ready || nd.IsReady() {
				t.Fatalf("tick %d: expected ready=false", i)
			}
		}
		if installCalls != 1 {
			t.Fatalf("expected 1 bundle install across 7 failed PATCH ticks, got %d", installCalls)
		}
		if execCalls != 1 {
			t.Fatalf("expected 1 criu check exec across 7 failed PATCH ticks, got %d", execCalls)
		}
		// 1 containerd-version Get + 7 annotateNode Gets = 8 Gets (not 14).
		if got := fc.getCalls.Load(); got != 8 {
			t.Fatalf("expected 8 Gets (1 version + 7 annotateNode), got %d", got)
		}
		if got := fc.patchCalls.Load(); got != 7 {
			t.Fatalf("expected 7 Patches, got %d", got)
		}

		// Tick 8: PATCH #8 succeeds and commits criu-ready=true.
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil {
			t.Fatalf("tick 8 ProbeReadiness failed: %v", err)
		}
		if !ready || !nd.IsReady() {
			t.Fatal("expected ready=true once PATCH #8 succeeds")
		}
		if installCalls != 1 || execCalls != 1 {
			t.Fatalf("expected still 1 install and 1 exec after recovery, got install=%d exec=%d", installCalls, execCalls)
		}

		// Tick 9: node is ready; 0 extra API calls.
		getsAfterReady := fc.getCalls.Load()
		patchesAfterReady := fc.patchCalls.Load()
		ready, err = nd.ProbeReadiness(ctx)
		if err != nil || !ready {
			t.Fatalf("tick 9 ProbeReadiness: ready=%v err=%v", ready, err)
		}
		if fc.getCalls.Load() != getsAfterReady || fc.patchCalls.Load() != patchesAfterReady {
			t.Fatalf("expected 0 extra API calls once ready")
		}
	})

	t.Run("unsupported containerd retries only annotateNode and stays quiet once persisted", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name:        "test-node",
				Annotations: map[string]string{NodeAnnotationCRIUReady: "true"},
			},
			Status: corev1.NodeStatus{
				NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://1.6.28"},
			},
		}
		base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
		fc := &flakyNodeClient{Client: base, failPatch: 7}

		nd := &Node{
			Client:       fc,
			NodeName:     "test-node",
			SocketPath:   "/run/containerd/containerd.sock",
			statSocketFn: func(string) (uint64, error) { return 4500, nil },
		}

		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected unsupported containerd + PATCH #1 failure")
		}
		for i := 2; i <= 7; i++ {
			ready, err := nd.ProbeReadiness(ctx)
			if err == nil || ready {
				t.Fatalf("tick %d: expected PATCH failure and ready=false, got ready=%v err=%v", i, ready, err)
			}
		}
		// 1 initial version Get + 7 annotateNode Gets = 8 Gets (not 14).
		if got := fc.getCalls.Load(); got != 8 {
			t.Fatalf("expected 8 Gets across 7 failed PATCH ticks, got %d", got)
		}
		if got := fc.patchCalls.Load(); got != 7 {
			t.Fatalf("expected 7 Patches, got %d", got)
		}

		// Tick 8: PATCH #8 lands criu-ready=false and clears pendingVerdict.
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil || ready {
			t.Fatalf("tick 8: expected ready=false err=nil once unready annotation lands, got ready=%v err=%v", ready, err)
		}
		var cur corev1.Node
		if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
			t.Fatalf("expected criu-ready=false persisted, got %q", cur.Annotations[NodeAnnotationCRIUReady])
		}

		// Subsequent ticks stay completely quiet.
		getsAfterPersist := fc.getCalls.Load()
		patchesAfterPersist := fc.patchCalls.Load()
		for i := 0; i < 3; i++ {
			if ready, err := nd.ProbeReadiness(ctx); err != nil || ready {
				t.Fatalf("quiet probe %d: ready=%v err=%v", i, ready, err)
			}
		}
		if fc.getCalls.Load() != getsAfterPersist || fc.patchCalls.Load() != patchesAfterPersist {
			t.Fatalf("expected 0 API calls after deterministic unready verdict persisted")
		}
	})

	t.Run("DemoteNode clears buffered pendingVerdict so next probe does not re-promote", func(t *testing.T) {
		nodeObj := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
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
			statSocketFn:    func(string) (uint64, error) { return 4600, nil },
			installBundleFn: func() (string, error) { return "v3.19", nil },
			execHostFn: func(context.Context, []string, string, ...string) (string, error) {
				return "Looks good.\n", nil
			},
		}

		if err := nd.Bootstrap(ctx); err == nil {
			t.Fatal("expected Bootstrap PATCH #1 error")
		}
		if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err != nil {
			t.Fatalf("DemoteNode: %v", err)
		}
		ready, err := nd.ProbeReadiness(ctx)
		if err != nil || ready || nd.IsReady() {
			t.Fatalf("expected demoted node to stay unready on next probe, got ready=%v err=%v", ready, err)
		}
		var cur corev1.Node
		if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
			t.Fatalf("get node: %v", err)
		}
		if cur.Annotations[NodeAnnotationCRIUReady] != "false" || cur.Annotations[NodeAnnotationCRIUVersion] != "v3.19" {
			t.Fatalf("expected criu-ready=false and criu-version=v3.19, got %+v", cur.Annotations)
		}
	})
}

type serializingCheckClient struct {
	client.Client
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	patchCalls  atomic.Int32
	patch1In    chan struct{}
	patch2In    chan struct{}
}

func (s *serializingCheckClient) trackMax(cur int32) {
	for {
		old := s.maxInFlight.Load()
		if cur <= old || s.maxInFlight.CompareAndSwap(old, cur) {
			return
		}
	}
}

func (s *serializingCheckClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	cur := s.inFlight.Add(1)
	s.trackMax(cur)
	defer s.inFlight.Add(-1)

	call := s.patchCalls.Add(1)
	if call == 1 {
		close(s.patch1In)
		// Give an unserialized DemoteNode a window to overtake Patch #1 on the API server.
		select {
		case <-s.patch2In:
		case <-time.After(40 * time.Millisecond):
		}
	} else if call == 2 {
		close(s.patch2In)
	}
	return s.Client.Patch(ctx, obj, patch, opts...)
}

func TestNode_AnnotateMuSerializesOverlappingPatchCalls(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
	sc := &serializingCheckClient{
		Client:   base,
		patch1In: make(chan struct{}),
		patch2In: make(chan struct{}),
	}

	nd := &Node{
		Client:          sc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 5151, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
	}

	bootDone := make(chan error, 1)
	go func() {
		bootDone <- nd.Bootstrap(ctx)
	}()

	<-sc.patch1In
	demoteDone := make(chan error, 1)
	go func() {
		demoteDone <- nd.DemoteNode(ctx, "manual-demote", "investigate")
	}()

	if err := <-demoteDone; err != nil {
		t.Fatalf("DemoteNode: %v", err)
	}
	if err := <-bootDone; err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if got := sc.maxInFlight.Load(); got != 1 {
		t.Fatalf("expected annotateMu to serialize Patch calls (maxInFlight=1), got %d", got)
	}
	var cur corev1.Node
	if err := base.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if cur.Annotations[NodeAnnotationCRIUReady] != "false" {
		t.Fatalf("expected DemoteNode Patch to land second and leave criu-ready=false, got %q", cur.Annotations[NodeAnnotationCRIUReady])
	}
}

func TestNode_MidVerifyCRIUPoisonReturnsCachedCRIUError(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	execStarted := make(chan struct{})
	allowExecFinish := make(chan struct{})
	nd := &Node{
		Client:          c,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 3131, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			close(execStarted)
			<-allowExecFinish
			return "Looks good.\n", nil
		},
	}

	bootDone := make(chan error, 1)
	go func() {
		bootDone <- nd.Bootstrap(ctx)
	}()

	<-execStarted
	if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err != nil {
		t.Fatalf("DemoteNode: %v", err)
	}
	close(allowExecFinish)

	if err := <-bootDone; !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected Bootstrap to surface ErrContainerdCachedCRIU when socket poisoned mid-verifyCRIU, got %v", err)
	}
}

type deadlineCheckingClient struct {
	client.Client
	t          *testing.T
	getCalls   atomic.Int32
	patchCalls atomic.Int32
}

func (d *deadlineCheckingClient) assertBoundedDeadline(ctx context.Context, op string) {
	d.t.Helper()
	dl, ok := ctx.Deadline()
	if !ok {
		d.t.Errorf("%s called without context deadline; expected <= %v", op, nodeAPITimeout)
		return
	}
	rem := time.Until(dl)
	if rem <= 0 || rem > nodeAPITimeout+time.Second {
		d.t.Errorf("%s deadline remaining %v outside (0, %v]", op, rem, nodeAPITimeout)
	}
}

func (d *deadlineCheckingClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	d.getCalls.Add(1)
	d.assertBoundedDeadline(ctx, "Get")
	return d.Client.Get(ctx, key, obj, opts...)
}

func (d *deadlineCheckingClient) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	d.patchCalls.Add(1)
	d.assertBoundedDeadline(ctx, "Patch")
	return d.Client.Patch(ctx, obj, patch, opts...)
}

func TestNode_APICallsCarryBoundedTimeout(t *testing.T) {
	ctx := context.Background() // intentionally unbounded parent context
	scheme := newTestScheme(t)

	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
	dc := &deadlineCheckingClient{Client: base, t: t}

	nd := &Node{
		Client:          dc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 6262, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
	}

	if err := nd.Bootstrap(ctx); err != nil {
		t.Fatalf("Bootstrap: %v", err)
	}
	if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err != nil {
		t.Fatalf("DemoteNode: %v", err)
	}
	if dc.getCalls.Load() != 3 || dc.patchCalls.Load() != 2 {
		t.Fatalf("expected 3 Gets and 2 Patches checked for deadlines, got gets=%d patches=%d",
			dc.getCalls.Load(), dc.patchCalls.Load())
	}
}

func TestPSEngineCheckpoint_SamplesSocketInodeBeforeKubeletCall(t *testing.T) {
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
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	var currentIno atomic.Uint64
	currentIno.Store(7001)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// containerd restarts onto fresh inode 7002 while kubelet is returning the failure from 7001.
		currentIno.Store(7002)
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message":"checkpoint failed: criu binary not found in shim path or system PATH"}`))
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("fake-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	nd := &Node{
		Client:          c,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		version:         "v3.19",
		ready:           true,
		lastSocketInode: 7001,
		statSocketFn:    func(string) (uint64, error) { return currentIno.Load(), nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			return "Looks good.\n", nil
		},
		Kubelet: &kubeletCheckpoint{
			TokenPath: tokenFile,
			Endpoint:  srv.URL,
			Client:    srv.Client(),
		},
	}

	if _, err := nd.PSEngineCheckpoint(ctx, "default", "pod-1", "container-1"); !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU, got %v", err)
	}

	// Because preIno=7001 was sampled before kubelet.checkpoint, fresh inode 7002 is NOT poisoned
	// and recovers on the next ProbeReadiness tick.
	ready, err := nd.ProbeReadiness(ctx)
	if err != nil {
		t.Fatalf("ProbeReadiness on fresh inode 7002 failed: %v", err)
	}
	if !ready || !nd.IsReady() {
		t.Fatal("expected fresh post-checkpoint inode 7002 to recover to ready=true")
	}
}

func TestNode_CancelledBootstrapDoesNotCacheSyntheticVerdict(t *testing.T) {
	scheme := newTestScheme(t)
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	cancelCtx, cancel := context.WithCancel(context.Background())
	execCalls := 0
	nd := &Node{
		Client:          c,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    func(string) (uint64, error) { return 8181, nil },
		installBundleFn: func() (string, error) { return "v3.19", nil },
		execHostFn: func(ctx context.Context, _ []string, _ string, _ ...string) (string, error) {
			execCalls++
			if execCalls == 1 {
				cancel()
				return "", ctx.Err()
			}
			return "Looks good.\n", nil
		},
	}

	if err := nd.Bootstrap(cancelCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled from cancelled Bootstrap, got %v", err)
	}
	nd.mu.Lock()
	pv := nd.pendingVerdict
	nd.mu.Unlock()
	if pv != nil {
		t.Fatalf("cancelled Bootstrap must not cache synthetic pendingVerdict: %+v", pv)
	}

	ready, err := nd.ProbeReadiness(context.Background())
	if err != nil || !ready || !nd.IsReady() {
		t.Fatalf("expected clean ProbeReadiness recovery after cancelled Bootstrap, got ready=%v err=%v", ready, err)
	}
}

func TestNode_StalePreRestartPendingDemoteDoesNotBlockNewInodePendingVerdict(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "test-node"},
		Status: corev1.NodeStatus{
			NodeInfo: corev1.NodeSystemInfo{ContainerRuntimeVersion: "containerd://2.2.7"},
		},
	}
	base := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()
	// Fail Patch #1 (DemoteNode on 1000) and Patch #2 (Bootstrap on 2000); Patch #3 succeeds.
	fc := &flakyNodeClient{Client: base, failPatch: 2}

	var currentIno atomic.Uint64
	currentIno.Store(1000)
	installCalls := 0
	execCalls := 0
	nd := &Node{
		Client:          fc,
		NodeName:        "test-node",
		SocketPath:      "/run/containerd/containerd.sock",
		ready:           true,
		version:         "v3.19",
		lastSocketInode: 1000,
		statSocketFn:    func(string) (uint64, error) { return currentIno.Load(), nil },
		installBundleFn: func() (string, error) {
			installCalls++
			return "v3.19", nil
		},
		execHostFn: func(context.Context, []string, string, ...string) (string, error) {
			execCalls++
			return "Looks good.\n", nil
		},
	}

	if err := nd.DemoteNode(ctx, ReasonContainerdCachedCRIU, RemediationRestartContainerd); err == nil {
		t.Fatal("expected Patch #1 failure on DemoteNode(1000)")
	}
	// containerd restarts onto fresh inode 2000 while pendingDemote from 1000 is still true.
	currentIno.Store(2000)
	if ready, err := nd.ProbeReadiness(ctx); err == nil || ready {
		t.Fatalf("expected Patch #2 failure on Bootstrap(2000), got ready=%v err=%v", ready, err)
	}
	// Next tick on 2000 must publish 2000's buffered ready=true verdict without re-running install/exec
	// and without writing 1000's stale pendingDemote!
	ready, err := nd.ProbeReadiness(ctx)
	if err != nil || !ready || !nd.IsReady() {
		t.Fatalf("expected 2000 pendingVerdict to converge to ready=true, got ready=%v err=%v", ready, err)
	}
	if installCalls != 1 || execCalls != 1 {
		t.Fatalf("expected 1 install and 1 exec on 2000, got install=%d exec=%d", installCalls, execCalls)
	}
}

func TestAnnotateNode_DeletesEmptyReasonWhenUnready(t *testing.T) {
	ctx := context.Background()
	scheme := newTestScheme(t)
	nodeObj := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node",
			Annotations: map[string]string{
				NodeAnnotationCRIUReason:      "stale-reason",
				NodeAnnotationCRIURemediation: "stale-remediation",
			},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(nodeObj).Build()

	if err := annotateNode(ctx, c, "test-node", "v3.19", "", "new-remediation", false); err != nil {
		t.Fatalf("annotateNode: %v", err)
	}
	var cur corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "test-node"}, &cur); err != nil {
		t.Fatalf("get node: %v", err)
	}
	if _, exists := cur.Annotations[NodeAnnotationCRIUReason]; exists {
		t.Fatalf("expected empty criu-reason deleted on unready update, got %q", cur.Annotations[NodeAnnotationCRIUReason])
	}
	if got := cur.Annotations[NodeAnnotationCRIURemediation]; got != "new-remediation" {
		t.Fatalf("expected criu-remediation=new-remediation, got %q", got)
	}
}
