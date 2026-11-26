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
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// snapshotKindReads counts Gets of PodSnapshotManualTriggers and PodSnapshots,
// by kind.
type snapshotKindReads struct {
	mu    sync.Mutex
	byGVK map[schema.GroupVersionKind]int
}

func (c *snapshotKindReads) record(obj client.Object) {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return
	}
	switch u.GetKind() {
	case "PodSnapshotManualTrigger", "PodSnapshot":
	default:
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byGVK == nil {
		c.byGVK = map[schema.GroupVersionKind]int{}
	}
	c.byGVK[u.GroupVersionKind()]++
}

func (c *snapshotKindReads) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, v := range c.byGVK {
		n += v
	}
	return n
}

// countingClient builds a fake client holding objs whose Gets of triggers and
// PodSnapshots are counted in reads.
func countingClient(scheme *runtime.Scheme, reads *snapshotKindReads, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				reads.record(obj)
				return c.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
}

const cachedReadsNS = "cached-reads"

func cachedReadsTrigger(podName, uid string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.ManualTriggerGVK(snapshot.DefaultAPIGroup))
	u.SetNamespace(cachedReadsNS)
	u.SetName(util.FormatPSMTName(podName, uid))
	return u
}

func cachedReadsSnapshot(name string, progress time.Time) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup))
	u.SetNamespace(cachedReadsNS)
	u.SetName(name)
	u.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":               "Checkpoint",
				"status":             "False",
				"reason":             "InProgress",
				"lastTransitionTime": progress.UTC().Format(time.RFC3339),
			},
		},
	}
	return u
}

// cachedReadsFixture is a namespace with three jobs: the reconciled job A,
// Restoring with its trigger and PodSnapshot, and peers B and C, both still
// Snapshotting 15 minutes in, past their 10-minute timeout, with fresh
// checkpoint progress on their PodSnapshots. Evaluating A reads A's trigger,
// A's PodSnapshot and both peers' PodSnapshots: four reads.
func cachedReadsFixture() (primary *pmv1alpha1.PodMigrationJob, objs []client.Object) {
	now := time.Now()
	started := metav1.NewTime(now.Add(-15 * time.Minute))
	restoring := metav1.NewTime(now.Add(-5 * time.Second))
	primary = &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: cachedReadsNS, Name: "pmj-a", CreationTimestamp: started},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "pod-a"}, TargetPodUID: "uid-a"},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshottingStartTime: &started,
			RestoringStartTime:    &restoring,
			SnapshotRef:           "snap-a",
		},
	}
	objs = []client.Object{primary, cachedReadsTrigger("pod-a", "uid-a"), cachedReadsSnapshot("snap-a", now.Add(-time.Minute))}
	for _, name := range []string{"b", "c"} {
		peer := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: cachedReadsNS, Name: "pmj-" + name, CreationTimestamp: started},
			Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "pod-" + name}, TargetPodUID: "uid-" + name},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
				SnapshottingStartTime: &started,
				SnapshotRef:           "snap-" + name,
			},
		}
		objs = append(objs, peer, cachedReadsSnapshot("snap-"+name, now.Add(-10*time.Second)))
	}
	return primary, objs
}

func cachedReadsScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := pmv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func gkeSnapshotGVKs() map[schema.GroupVersionKind]bool {
	return map[schema.GroupVersionKind]bool{
		snapshot.ManualTriggerGVK(snapshot.DefaultAPIGroup): true,
		snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup):   true,
	}
}

// TestEvaluateInvariants_StrictReadsTriggerAndSnapshotsFromCache checks that,
// in strict mode, evaluating a job reads its trigger, its PodSnapshot and its
// peers' PodSnapshots from the informer cache and never from the API server
// (#199, #231 item 3). The live client holds the same objects, so a read that
// went live would succeed and be counted.
func TestEvaluateInvariants_StrictReadsTriggerAndSnapshotsFromCache(t *testing.T) {
	scheme := cachedReadsScheme(t)

	run := func(t *testing.T, useCache bool) (live, cached int, captured *invariants.ReconcileSnapshot) {
		t.Helper()
		primary, objs := cachedReadsFixture()
		liveReads, cacheReads := &snapshotKindReads{}, &snapshotKindReads{}
		liveClient := countingClient(scheme, liveReads, objs...)
		capture := &snapshotCaptureRule{}
		eng := invariants.NewEngine(invariants.ModeStrict, nil)
		eng.Rules = append(append([]invariants.Rule{}, invariants.DefaultRules...), capture)
		r := &PodMigrationJobReconciler{
			Client:          liveClient,
			APIReader:       liveClient,
			Scheme:          scheme,
			InvariantEngine: eng,
		}
		if useCache {
			r.invariantReader = countingClient(scheme, cacheReads, objs...)
			r.invariantCachedGVKs = gkeSnapshotGVKs()
		}
		var res ctrl.Result
		var recErr error
		r.evaluateInvariantsWithSnapshot(context.Background(), primary, nil, &res, &recErr)
		if recErr != nil {
			t.Fatalf("evaluation set a reconcile error: %v", recErr)
		}
		if capture.captured == nil {
			t.Fatal("evaluation never reached the invariant engine")
		}
		return liveReads.total(), cacheReads.total(), capture.captured
	}

	t.Run("without the cache every read is live", func(t *testing.T) {
		live, _, _ := run(t, false)
		if live != 4 {
			t.Fatalf("live trigger/PodSnapshot reads = %d, want 4 (trigger, own snapshot, 2 peers)", live)
		}
	})

	t.Run("with the cache no read is live", func(t *testing.T) {
		live, cached, s := run(t, true)
		if live != 0 {
			t.Errorf("live trigger/PodSnapshot reads = %d, want 0", live)
		}
		if cached != 4 {
			t.Errorf("cached trigger/PodSnapshot reads = %d, want 4", cached)
		}
		if len(s.PrimarySnapshotConditions) == 0 {
			t.Error("PrimarySnapshotConditions empty: the job's PodSnapshot wasn't read from the cache")
		}
		for _, peer := range []string{"pmj-b", "pmj-c"} {
			if _, ok := s.SnapshotProgress[cachedReadsNS+"/"+peer]; !ok {
				t.Errorf("SnapshotProgress has no entry for %s: its PodSnapshot wasn't read from the cache", peer)
			}
		}
		if s.HasOrphanedTrigger {
			t.Error("HasOrphanedTrigger = true for an active job")
		}
	})
}

// blockingReader stands in for a cache read that starts an informer that
// never syncs, as for a kind whose CRD was installed after startup in an API
// group the controller can't list: Get waits until the context is done.
type blockingReader struct {
	mu    sync.Mutex
	calls int
}

func (b *blockingReader) Get(ctx context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (b *blockingReader) List(ctx context.Context, _ client.ObjectList, _ ...client.ListOption) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestEvaluateInvariants_UnwatchedKindsAreNotRead checks kinds that
// SetupWithManager didn't watch, as in a cluster without the snapshot CRDs
// (for example a kind cluster running only CRIU, without GKE Pod Snapshots):
// evaluation reads none of them, from the cache or the API server. The cache
// here blocks every read, as one would whose informer can't sync, so reading
// an unwatched kind from it would hang the evaluation.
func TestEvaluateInvariants_UnwatchedKindsAreNotRead(t *testing.T) {
	scheme := cachedReadsScheme(t)
	primary, objs := cachedReadsFixture()
	liveReads := &snapshotKindReads{}
	liveClient := countingClient(scheme, liveReads, objs...)
	blocking := &blockingReader{}
	capture := &snapshotCaptureRule{}
	eng := invariants.NewEngine(invariants.ModeStrict, nil)
	eng.Rules = append(append([]invariants.Rule{}, invariants.DefaultRules...), capture)
	r := &PodMigrationJobReconciler{
		Client:              liveClient,
		APIReader:           liveClient,
		Scheme:              scheme,
		InvariantEngine:     eng,
		invariantReader:     blocking,
		invariantCachedGVKs: map[schema.GroupVersionKind]bool{},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		var res ctrl.Result
		var recErr error
		r.evaluateInvariantsWithSnapshot(ctx, primary, nil, &res, &recErr)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("evaluation is stuck reading a kind the controller doesn't watch")
	}
	if blocking.calls != 0 {
		t.Errorf("cache reads of unwatched kinds = %d, want 0", blocking.calls)
	}
	if n := liveReads.total(); n != 0 {
		t.Errorf("live trigger/PodSnapshot reads = %d, want 0", n)
	}
	if capture.captured == nil {
		t.Fatal("evaluation never reached the invariant engine")
	}
	if capture.captured.HasOrphanedTrigger || len(capture.captured.PrimarySnapshotConditions) != 0 {
		t.Errorf("snapshot state reported for kinds that weren't read: orphaned=%v conditions=%v",
			capture.captured.HasOrphanedTrigger, capture.captured.PrimarySnapshotConditions)
	}
}

// TestEvaluateInvariants_I5ConfirmsCachedTriggerLive checks the one live read
// evaluation keeps: when the cache shows a concluded job's trigger undeleted,
// I5 asks the API server, because the reconcile that failed the job deleted
// the trigger moments earlier and the cache may not have seen it yet.
func TestEvaluateInvariants_I5ConfirmsCachedTriggerLive(t *testing.T) {
	scheme := cachedReadsScheme(t)
	failedJob := func() *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: cachedReadsNS, Name: "pmj-failed"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "pod-f"}, TargetPodUID: "uid-f"},
			Status:     pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseFailed},
		}
	}
	deleting := func() client.Object {
		u := cachedReadsTrigger("pod-f", "uid-f")
		now := metav1.Now()
		u.SetDeletionTimestamp(&now)
		u.SetFinalizers([]string{"test/hold"})
		return u
	}

	cases := []struct {
		name         string
		cacheTrigger bool
		liveTrigger  func() client.Object
		liveErr      error
		wantOrphan   bool
		wantLive     int
	}{
		{name: "cache stale after the delete", cacheTrigger: true, liveTrigger: nil, wantOrphan: false, wantLive: 1},
		{name: "trigger really leaked", cacheTrigger: true, liveTrigger: func() client.Object { return cachedReadsTrigger("pod-f", "uid-f") }, wantOrphan: true, wantLive: 1},
		{name: "trigger being deleted", cacheTrigger: true, liveTrigger: deleting, wantOrphan: false, wantLive: 1},
		{name: "no trigger in the cache", cacheTrigger: false, liveTrigger: func() client.Object { return cachedReadsTrigger("pod-f", "uid-f") }, wantOrphan: false, wantLive: 0},
		{name: "live read refused: the cache's answer stands", cacheTrigger: true,
			liveErr:    apierrors.NewForbidden(schema.GroupResource{Group: snapshot.DefaultAPIGroup, Resource: "podsnapshotmanualtriggers"}, "psmt", errors.New("denied")),
			wantOrphan: true, wantLive: 1},
		{name: "CRD deleted since startup", cacheTrigger: true,
			liveErr:    &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: snapshot.DefaultAPIGroup, Kind: "PodSnapshotManualTrigger"}},
			wantOrphan: false, wantLive: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := failedJob()
			liveObjs := []client.Object{job}
			if tc.liveTrigger != nil {
				liveObjs = append(liveObjs, tc.liveTrigger())
			}
			cacheObjs := []client.Object{job.DeepCopy()}
			if tc.cacheTrigger {
				cacheObjs = append(cacheObjs, cachedReadsTrigger("pod-f", "uid-f"))
			}
			liveReads := &snapshotKindReads{}
			var liveClient client.Client = countingClient(scheme, liveReads, liveObjs...)
			if tc.liveErr != nil {
				liveClient = fake.NewClientBuilder().WithScheme(scheme).WithObjects(liveObjs...).
					WithInterceptorFuncs(interceptor.Funcs{
						Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
							if _, ok := obj.(*unstructured.Unstructured); ok {
								liveReads.record(obj)
								return tc.liveErr
							}
							return c.Get(ctx, key, obj, opts...)
						},
					}).Build()
			}
			capture := &snapshotCaptureRule{}
			eng := invariants.NewEngine(invariants.ModeStrict, nil)
			eng.Rules = append(append([]invariants.Rule{}, invariants.DefaultRules...), capture)
			r := &PodMigrationJobReconciler{
				Client:              liveClient,
				APIReader:           liveClient,
				Scheme:              scheme,
				InvariantEngine:     eng,
				invariantReader:     countingClient(scheme, &snapshotKindReads{}, cacheObjs...),
				invariantCachedGVKs: gkeSnapshotGVKs(),
			}
			var res ctrl.Result
			var recErr error
			r.evaluateInvariantsWithSnapshot(context.Background(), job, nil, &res, &recErr)
			if capture.captured == nil {
				t.Fatal("evaluation never reached the invariant engine")
			}
			if got := capture.captured.HasOrphanedTrigger; got != tc.wantOrphan {
				t.Errorf("HasOrphanedTrigger = %v, want %v", got, tc.wantOrphan)
			}
			if got := liveReads.total(); got != tc.wantLive {
				t.Errorf("live trigger reads = %d, want %d", got, tc.wantLive)
			}
		})
	}
}
