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

package controller

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
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestSubSecondPreemptionNoticeToJobInitiation verifies the sub-second preemption reaction requirement
// from Issue #248: checkpoint initiation begins within 1 second of host preemption notice.
func TestSubSecondPreemptionNoticeToJobInitiation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	var (
		mu        sync.Mutex
		preempted = "FALSE"
	)

	mux := http.NewServeMux()
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

	metadataServer := httptest.NewServer(mux)
	defer metadataServer.Close()

	nodeName := "spot-node-subsecond"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}

	pod := createTestPod("stateful-redis", "default", nodeName, "4Gi", "uid-redis-1")
	psp := createTestPSP("psp-default", "default")

	recorder := record.NewFakeRecorder(20)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(node, pod, psp).
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		APIReader:               cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
		SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
	}

	// 1. Initial Reconcile before preemption signal: must be a no-op (node is healthy)
	ctx := context.Background()
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue for healthy node: %v", res)
	}

	// Verify no PMJ exists
	var pmjList pmv1alpha1.PodMigrationJobList
	if err := cl.List(ctx, &pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}
	if len(pmjList.Items) != 0 {
		t.Fatalf("expected 0 PMJs, got %d", len(pmjList.Items))
	}

	// 2. Set up MetadataPreemptionWatcher with NodePatcherNotifier
	notifierTriggered := make(chan time.Time, 1)
	baseNotifier := util.NewNodePatcherNotifier(cl, nodeName)
	wrappedNotifier := func(ctx context.Context, notice util.PreemptionNotice) error {
		err := baseNotifier(ctx, notice)
		now := time.Now()
		notifierTriggered <- now
		return err
	}

	watcher := &util.MetadataPreemptionWatcher{
		NodeName:     nodeName,
		MetadataURL:  metadataServer.URL + "/computeMetadata/v1/instance/preempted",
		HTTPClient:   metadataServer.Client(),
		PollInterval: 25 * time.Millisecond,
		Notifier:     wrappedNotifier,
	}

	watcherCtx, cancelWatcher := context.WithCancel(context.Background())
	defer cancelWatcher()
	go func() {
		_ = watcher.Start(watcherCtx)
	}()

	// 3. Emit host preemption notice
	noticeTimestamp := time.Now()
	mu.Lock()
	preempted = "TRUE"
	mu.Unlock()

	// Wait for watcher to detect and patch the node
	var patchTimestamp time.Time
	select {
	case patchTimestamp = <-notifierTriggered:
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for metadata watcher to detect preemption notice within 1s")
	}

	// 4. Drive Reconcile upon node update (as controller-runtime Informer does)
	checkNode := &corev1.Node{}
	_ = cl.Get(ctx, client.ObjectKey{Name: nodeName}, checkNode)
	t.Logf("checkNode labels=%v taints=%v conds=%v isPreempting=%v", checkNode.Labels, checkNode.Spec.Taints, checkNode.Status.Conditions, util.IsNodePreempting(checkNode))

	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: nodeName}})
	if err != nil {
		t.Fatalf("failed to reconcile preempting node: %v", err)
	}

	for len(recorder.Events) > 0 {
		t.Logf("Event: %s", <-recorder.Events)
	}

	totalLatency := time.Since(noticeTimestamp)
	t.Logf("Measured preemption notice-to-job initiation latency: %v (detection+patch: %v)",
		totalLatency, patchTimestamp.Sub(noticeTimestamp))

	// Hard requirement from Issue #248: checkpoint initiation begins within 1 second of host notice.
	if totalLatency >= 1*time.Second {
		t.Fatalf("checkpoint initiation exceeded 1s deadline: total latency %v", totalLatency)
	}

	// 5. Verify PodMigrationJob was created with proper trigger source and budget constraints
	if err := cl.List(ctx, &pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}
	if len(pmjList.Items) != 1 {
		t.Fatalf("expected exactly 1 PMJ created, got %d", len(pmjList.Items))
	}

	pmj := pmjList.Items[0]
	if pmj.Labels[util.LabelTriggerSource] != util.TriggerSourceSpotPreemption {
		t.Fatalf("expected LabelTriggerSource=%q, got %q", util.TriggerSourceSpotPreemption, pmj.Labels[util.LabelTriggerSource])
	}
	if pmj.Annotations[util.AnnotationTriggerSource] != util.TriggerSourceSpotPreemption {
		t.Fatalf("expected AnnotationTriggerSource=%q, got %q", util.TriggerSourceSpotPreemption, pmj.Annotations[util.AnnotationTriggerSource])
	}
	if pmj.Annotations[util.AnnotationMigrationTimeout] != util.PreemptionDefaultTimeout.String() {
		t.Fatalf("expected 2m preemption deadline timeout %q, got %q",
			util.PreemptionDefaultTimeout.String(), pmj.Annotations[util.AnnotationMigrationTimeout])
	}

	// 6. Verify Node has low-latency preemption indicators
	updatedNode := &corev1.Node{}
	if err := cl.Get(ctx, client.ObjectKey{Name: nodeName}, updatedNode); err != nil {
		t.Fatalf("failed to get node: %v", err)
	}
	if !util.IsNodePreempting(updatedNode) {
		t.Fatalf("expected IsNodePreempting(updatedNode) == true")
	}
	if updatedNode.Labels[util.LabelNodePreempting] != util.ValueNodePreempting {
		t.Fatalf("expected LabelNodePreempting=true on node")
	}
}
