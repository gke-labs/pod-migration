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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/pod-migration/engines/criu/internal/node"
)

func TestAgentManagerOptions_DisablesNodeInformerUnderGetPatchRBAC(t *testing.T) {
	var listOrWatchCalls atomic.Int32
	var getCalls atomic.Int32
	var patchCalls atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v1/nodes":
			listOrWatchCalls.Add(1)
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(&metav1.Status{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Status"},
				Status:   metav1.StatusFailure,
				Reason:   metav1.StatusReasonForbidden,
				Code:     http.StatusForbidden,
				Message:  "nodes is forbidden: agent ClusterRole grants only get and patch",
			})
		case r.URL.Path == "/api/v1/nodes/test-node" && r.Method == http.MethodGet:
			getCalls.Add(1)
			_ = json.NewEncoder(w).Encode(&corev1.Node{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
				ObjectMeta: metav1.ObjectMeta{Name: "test-node", ResourceVersion: "1"},
			})
		case r.URL.Path == "/api/v1/nodes/test-node" && r.Method == http.MethodPatch:
			patchCalls.Add(1)
			_ = json.NewEncoder(w).Encode(&corev1.Node{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"},
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-node",
					ResourceVersion: "2",
					Annotations:     map[string]string{"pod-migrate.io/criu-ready": "true"},
				},
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	opts := agentManagerOptions("0")
	opts.MapperProvider = func(_ *rest.Config, _ *http.Client) (meta.RESTMapper, error) {
		m := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion})
		m.Add(corev1.SchemeGroupVersion.WithKind("Node"), meta.RESTScopeRoot)
		return m, nil
	}

	mgr, err := ctrl.NewManager(&rest.Config{Host: srv.URL}, opts)
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	mgrErr := make(chan error, 1)
	go func() {
		mgrErr <- mgr.Start(ctx)
	}()
	defer func() {
		cancel()
		select {
		case err := <-mgrErr:
			if err != nil && err != context.Canceled {
				t.Errorf("mgr.Start returned unexpected error: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("mgr.Start did not exit within 2s after context cancel")
		}
	}()

	if !mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("WaitForCacheSync failed")
	}

	var n corev1.Node
	if err := mgr.GetClient().Get(ctx, types.NamespacedName{Name: "test-node"}, &n); err != nil {
		t.Fatalf("Get(*corev1.Node) under get/patch-only RBAC failed: %v", err)
	}
	patch := client.MergeFrom(n.DeepCopy())
	n.Annotations = map[string]string{"pod-migrate.io/criu-ready": "true"}
	if err := mgr.GetClient().Patch(ctx, &n, patch); err != nil {
		t.Fatalf("Patch(*corev1.Node) under get/patch-only RBAC failed: %v", err)
	}

	if got := listOrWatchCalls.Load(); got != 0 {
		t.Fatalf("expected 0 Node list/watch calls with DisableFor(&corev1.Node{}), got %d", got)
	}
	if getCalls.Load() != 1 || patchCalls.Load() != 1 {
		t.Fatalf("expected 1 Get and 1 Patch call, got get=%d patch=%d", getCalls.Load(), patchCalls.Load())
	}
}

// fakeNodeWorker records what runNodeWorker starts.
type fakeNodeWorker struct {
	factsLoop chan [2]time.Duration
}

func (f *fakeNodeWorker) Bootstrap(context.Context) error { return errors.New("bootstrap failed") }
func (f *fakeNodeWorker) RunNodeFactsLoop(_ context.Context, interval, retry time.Duration) {
	f.factsLoop <- [2]time.Duration{interval, retry}
}
func (f *fakeNodeWorker) SweepOrphanRestores(context.Context) error         { return nil }
func (f *fakeNodeWorker) RunProbeLoop(ctx context.Context, _ time.Duration) { <-ctx.Done() }

// TestRunNodeWorkerStartsNodeFactsLoop pins that the agent publishes its
// node's facts, even after a failed bootstrap: without the labels, every
// constrained CRIU restore waits out the restore ceiling (#257).
func TestRunNodeWorkerStartsNodeFactsLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := &fakeNodeWorker{factsLoop: make(chan [2]time.Duration, 1)}
	done := make(chan struct{})
	go func() {
		runNodeWorker(ctx, w)
		close(done)
	}()
	select {
	case got := <-w.factsLoop:
		if want := [2]time.Duration{node.DefaultNodeFactsInterval, node.DefaultNodeFactsRetry}; got != want {
			t.Errorf("RunNodeFactsLoop(interval, retry) = %v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("runNodeWorker didn't start the node facts loop")
	}
	cancel()
	<-done
}
