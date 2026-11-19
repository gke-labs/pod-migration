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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
		if ino, err := nd.statSocketFn(nd.SocketPath); err == nil {
			nd.lastSocketInode = ino
		}
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

func TestPSEngineCheckpoint_DemoteErrorLogged(t *testing.T) {
	ctx := context.Background()
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

	// Should not panic, and returns ErrContainerdCachedCRIU
	_, err := nd.PSEngineCheckpoint(ctx, "default", "pod-1", "container-1")
	if err == nil || !errors.Is(err, ErrContainerdCachedCRIU) {
		t.Fatalf("expected ErrContainerdCachedCRIU, got: %v", err)
	}
}
