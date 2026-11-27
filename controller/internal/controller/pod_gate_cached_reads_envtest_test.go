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
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodGateReconcile_ReadsJobSnapshotFromCache_Envtest checks, against a
// real API server and the manager's real cache, that the pod gate reads the
// assigned job's PodSnapshot for I4 from the cache, and that I4 then no longer
// flags a job whose snapshot is still making progress (#231). Job b started
// Snapshotting 15 minutes ago, past its 10-minute timeout, and its snapshot
// made progress 10 seconds ago.
func TestPodGateReconcile_ReadsJobSnapshotFromCache_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest pod gate cached read test")
	}
	env, _ := startCachedReadsEnv(t, buildDynamicPodSnapshotCRDs())
	ctx := context.Background()
	ns := createTestNamespace(t, ctx, env.admin, "gate-cache")

	env.createJob(t, ns, "b", "", pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	env.createSnapshotObj(t, snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup), ns, "snap-b", time.Now().Add(-10*time.Second))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "web-0-repl", Annotations: map[string]string{util.AnnotationAssignedPMJ: "b"}},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			Containers:      []corev1.Container{{Name: "app", Image: "app"}},
		},
	}
	if err := env.admin.Create(ctx, pod); err != nil {
		t.Fatalf("creating the gated pod: %v", err)
	}

	jobs := &PodMigrationJobReconciler{Client: env.mgr.GetClient(), APIReader: env.mgr.GetAPIReader(), Scheme: env.mgr.GetScheme()}
	if err := jobs.SetupWithManager(env.mgr, ctrlcontroller.Options{}); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	env.startCache(t, jobs)
	env.waitForCachedJobs(t, ns, 1)
	cacheReader := &countingCacheReader{Reader: jobs.invariantReader}
	jobs.invariantReader = cacheReader

	recorder := record.NewFakeRecorder(50)
	engine := invariants.NewEngine(invariants.ModeObserve, recorder)
	gate := &PodGateReconciler{Client: env.mgr.GetClient(), APIReader: env.mgr.GetAPIReader(), Scheme: env.mgr.GetScheme(), Recorder: recorder, InvariantEngine: engine}
	reconcileGate := func(t *testing.T) []string {
		t.Helper()
		engine.ResetDeduplication()
		for len(recorder.Events) > 0 {
			<-recorder.Events
		}
		env.counter.reset()
		rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		res, err := gate.Reconcile(rctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: pod.Name}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.RequeueAfter != 30*time.Second {
			t.Fatalf("result = %+v, want the pod held while job b is Snapshotting", res)
		}
		var i4 []string
		for len(recorder.Events) > 0 {
			if e := <-recorder.Events; strings.Contains(e, "[I4:") {
				i4 = append(i4, e)
			}
		}
		return i4
	}

	t.Run("before: no progress, so I4 flags the job", func(t *testing.T) {
		gate.ReadForInvariants = nil
		if i4 := reconcileGate(t); len(i4) == 0 {
			t.Fatal("no I4 event: the false positive this test guards against didn't show")
		}
		if n, by := env.counter.snapshot(); n != 0 {
			t.Errorf("live trigger/PodSnapshot GETs = %d (%v), want 0", n, by)
		}
	})

	t.Run("a live read would be counted", func(t *testing.T) {
		gate.ReadForInvariants = func(ctx context.Context, key types.NamespacedName, obj *unstructured.Unstructured) error {
			return env.mgr.GetAPIReader().Get(ctx, key, obj)
		}
		reconcileGate(t)
		if n, by := env.counter.snapshot(); n != 1 {
			t.Fatalf("live trigger/PodSnapshot GETs = %d (%v), want 1", n, by)
		}
	})

	t.Run("after: the job's PodSnapshot comes from the cache", func(t *testing.T) {
		gate.ReadForInvariants = jobs.ReadForInvariants
		cacheReader.reads = 0
		if i4 := reconcileGate(t); len(i4) != 0 {
			t.Errorf("I4 events = %q, want none for a job whose snapshot is making progress", i4)
		}
		if n, by := env.counter.snapshot(); n != 0 {
			t.Errorf("live trigger/PodSnapshot GETs = %d (%v), want 0", n, by)
		}
		if cacheReader.reads != 1 {
			t.Errorf("cached PodSnapshot reads = %d, want 1", cacheReader.reads)
		}
	})
}
