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

package util

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMetadataPreemptionWatcher_CheckOnce(t *testing.T) {
	mux := http.NewServeMux()
	var (
		mu          sync.Mutex
		preempted   = "FALSE"
		maintenance = "NONE"
	)

	mux.HandleFunc("/computeMetadata/v1/instance/preempted", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing metadata flavor", http.StatusForbidden)
			return
		}
		mu.Lock()
		val := preempted
		mu.Unlock()
		_, _ = w.Write([]byte(val))
	})

	mux.HandleFunc("/computeMetadata/v1/instance/maintenance-event", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			http.Error(w, "missing metadata flavor", http.StatusForbidden)
			return
		}
		mu.Lock()
		val := maintenance
		mu.Unlock()
		_, _ = w.Write([]byte(val))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	watcher := &MetadataPreemptionWatcher{
		NodeName:       "node-test",
		MetadataURL:    server.URL + "/computeMetadata/v1/instance/preempted",
		MaintenanceURL: server.URL + "/computeMetadata/v1/instance/maintenance-event",
		HTTPClient:     server.Client(),
	}

	ctx := context.Background()

	// 1. Initial healthy state
	active, notice, err := watcher.CheckOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if active || notice != nil {
		t.Fatalf("expected inactive, got active=%v, notice=%+v", active, notice)
	}

	// 2. Preempted signal active
	mu.Lock()
	preempted = "TRUE"
	mu.Unlock()

	active, notice, err = watcher.CheckOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !active || notice == nil {
		t.Fatalf("expected active preemption notice, got active=%v, notice=%+v", active, notice)
	}
	if notice.SignalType != SignalGCEPreempted {
		t.Fatalf("expected SignalGCEPreempted, got %v", notice.SignalType)
	}

	// 3. Maintenance event signal
	mu.Lock()
	preempted = "FALSE"
	maintenance = "TERMINATE_ON_HOST_MAINTENANCE"
	mu.Unlock()

	active, notice, err = watcher.CheckOnce(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !active || notice == nil {
		t.Fatalf("expected active maintenance notice, got active=%v, notice=%+v", active, notice)
	}
	if notice.SignalType != SignalGCEMaintenanceEvent {
		t.Fatalf("expected SignalGCEMaintenanceEvent, got %v", notice.SignalType)
	}
}

func TestMetadataPreemptionWatcher_StartLoop(t *testing.T) {
	mux := http.NewServeMux()
	var (
		mu        sync.Mutex
		preempted = "FALSE"
	)

	mux.HandleFunc("/computeMetadata/v1/instance/preempted", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		val := preempted
		mu.Unlock()
		_, _ = w.Write([]byte(val))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	noticedCh := make(chan PreemptionNotice, 1)
	watcher := &MetadataPreemptionWatcher{
		NodeName:     "node-worker-1",
		MetadataURL:  server.URL + "/computeMetadata/v1/instance/preempted",
		HTTPClient:   server.Client(),
		PollInterval: 20 * time.Millisecond,
		Notifier: func(ctx context.Context, notice PreemptionNotice) error {
			noticedCh <- notice
			return nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- watcher.Start(ctx)
	}()

	// Verify no premature trigger
	select {
	case n := <-noticedCh:
		t.Fatalf("unexpected notice before signal: %+v", n)
	case <-time.After(50 * time.Millisecond):
	}

	// Emit preemption signal
	signalTime := time.Now()
	mu.Lock()
	preempted = "TRUE"
	mu.Unlock()

	// Wait for notification
	select {
	case notice := <-noticedCh:
		latency := time.Since(signalTime)
		if latency > 1*time.Second {
			t.Fatalf("preemption notice exceeded 1s deadline: %v", latency)
		}
		if notice.NodeName != "node-worker-1" {
			t.Fatalf("expected node-worker-1, got %s", notice.NodeName)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for preemption notice dispatch")
	}
}

func TestNewNodePatcherNotifier(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-target",
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).Build()
	notifier := NewNodePatcherNotifier(cl, "node-target")

	notice := PreemptionNotice{
		SignalType: SignalGCEPreempted,
		ReceivedAt: time.Now(),
		NodeName:   "node-target",
		Message:    "Preemption triggered",
	}

	ctx := context.Background()
	if err := notifier(ctx, notice); err != nil {
		t.Fatalf("notifier failed: %v", err)
	}

	updated := &corev1.Node{}
	if err := cl.Get(ctx, client.ObjectKey{Name: "node-target"}, updated); err != nil {
		t.Fatalf("failed to get updated node: %v", err)
	}

	// 1. Label
	if updated.Labels[LabelNodePreempting] != ValueNodePreempting {
		t.Fatalf("expected LabelNodePreempting=true, got %v", updated.Labels)
	}

	// 2. Taint
	hasTaint := false
	for _, taint := range updated.Spec.Taints {
		if taint.Key == TaintPodMigrationPreempting {
			hasTaint = true
			break
		}
	}
	if !hasTaint {
		t.Fatalf("expected TaintPodMigrationPreempting on node, got: %v", updated.Spec.Taints)
	}

	// 3. Condition
	hasCond := false
	for _, cond := range updated.Status.Conditions {
		if cond.Type == ConditionPreemptionImminent && cond.Status == corev1.ConditionTrue {
			hasCond = true
			break
		}
	}
	if !hasCond {
		t.Fatalf("expected ConditionPreemptionImminent on node status, got: %v", updated.Status.Conditions)
	}

	// 4. Verify IsNodePreempting returns true
	if !IsNodePreempting(updated) {
		t.Fatalf("IsNodePreempting(updated) must return true")
	}
}
