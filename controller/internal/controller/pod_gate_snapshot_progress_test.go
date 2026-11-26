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
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodGate_I4SeesTheAssignedJobsSnapshotProgress (#231): the pod gate
// evaluates I4 for the job its gated pod is assigned to. A Snapshotting job
// past its timeout is fine while its snapshot keeps making progress, but the
// pod gate passed no progress, so I4 flagged it. It now reads the job's
// PodSnapshot, only when I4 needs it, and still flags a stalled job.
func TestPodGate_I4SeesTheAssignedJobsSnapshotProgress(t *testing.T) {
	for _, tc := range []struct {
		name         string
		snapshotting time.Duration // how long ago pmj-b started Snapshotting
		progressAgo  time.Duration // when snap-b last made progress
		noSnapshot   bool          // snap-b doesn't exist
		noReader     bool          // ReadForInvariants isn't set
		criu         bool          // pmj-b is served by the CRIU engine
		failed       bool          // snap-b reports a failed checkpoint
		notWatched   bool          // the reader doesn't serve snap-b's kind
		wantI4       bool
		wantReads    int
	}{
		{name: "progressing past its timeout", snapshotting: 12 * time.Minute, progressAgo: 10 * time.Second, wantReads: 1},
		{name: "stalled past its timeout", snapshotting: 12 * time.Minute, progressAgo: 12 * time.Minute, wantI4: true, wantReads: 1},
		{name: "within its timeout", snapshotting: 5 * time.Minute, progressAgo: 10 * time.Second},
		{name: "past the hard cap", snapshotting: util.MaxMigrationTimeout + time.Minute, progressAgo: 10 * time.Second, wantI4: true},
		{name: "snapshot not found", snapshotting: 12 * time.Minute, noSnapshot: true, wantI4: true, wantReads: 1},
		{name: "no reader", snapshotting: 12 * time.Minute, progressAgo: 10 * time.Second, noReader: true, wantI4: true},
		{name: "CRIU job, snapshot in the CRIU group", snapshotting: 12 * time.Minute, progressAgo: 10 * time.Second, criu: true, wantReads: 1},
		{name: "snapshot failed", snapshotting: 12 * time.Minute, progressAgo: 10 * time.Second, failed: true, wantI4: true, wantReads: 1},
		{name: "kind not watched", snapshotting: 12 * time.Minute, progressAgo: 10 * time.Second, notWatched: true, wantI4: true, wantReads: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			now := time.Now()
			started := metav1.NewTime(now.Add(-tc.snapshotting))
			group, jobAnns := snapshot.DefaultAPIGroup, map[string]string(nil)
			if tc.criu {
				group, jobAnns = engine.CRIUAPIGroup, map[string]string{engine.AnnotationEngine: engine.CRIUName}
			}
			objs := []client.Object{
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "web-0-repl", UID: "uid-repl",
						Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-b"}},
					Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}}},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-b", UID: "uid-pmj-b", CreationTimestamp: metav1.NewTime(started.Add(-time.Minute)), Annotations: jobAnns},
					Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "web-0"}, TargetPodUID: "uid-src"},
					Status:     pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting, SnapshotRef: "snap-b", SnapshottingStartTime: &started},
				},
			}
			if !tc.noSnapshot {
				reason := "InProgress"
				if tc.failed {
					reason = "Failed"
				}
				snap := &unstructured.Unstructured{Object: map[string]interface{}{"status": map[string]interface{}{"conditions": []interface{}{
					map[string]interface{}{"type": "Checkpoint", "status": "False", "reason": reason,
						"lastTransitionTime": now.Add(-tc.progressAgo).UTC().Format(time.RFC3339)},
				}}}}
				snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
				snap.SetNamespace("default")
				snap.SetName("snap-b")
				objs = append(objs, snap)
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(objs...).Build()
			recorder := record.NewFakeRecorder(20)
			r := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme, Recorder: recorder,
				InvariantEngine: invariants.NewEngine(invariants.ModeObserve, recorder)}
			if tc.criu {
				r.Engines = buildSideBySideControllerRegistry(t, cl, scheme)
			}
			reads := 0
			if !tc.noReader {
				r.ReadForInvariants = func(ctx context.Context, key types.NamespacedName, obj *unstructured.Unstructured) error {
					reads++
					if tc.notWatched {
						return &meta.NoKindMatchError{GroupKind: obj.GroupVersionKind().GroupKind(), SearchedVersions: []string{"v1"}}
					}
					return cl.Get(ctx, key, obj)
				}
			}

			res, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "web-0-repl"}})
			if err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if res.RequeueAfter != 30*time.Second {
				t.Errorf("result = %+v, want the pod held for another 30s", res)
			}
			var i4 []string
			for len(recorder.Events) > 0 {
				if e := <-recorder.Events; strings.Contains(e, "[I4:") {
					i4 = append(i4, e)
				}
			}
			if got := len(i4) > 0; got != tc.wantI4 {
				t.Errorf("I4 events = %q, want a violation: %t", i4, tc.wantI4)
			}
			if reads != tc.wantReads {
				t.Errorf("PodSnapshot reads = %d, want %d", reads, tc.wantReads)
			}
		})
	}
}
