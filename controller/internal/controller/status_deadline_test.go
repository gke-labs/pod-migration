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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func deadlineTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

// TestStatusDeadline_Pending_BaselineAndOverrides verifies that in Pending phase,
// status.deadline matches the time the reconciler would time out the job:
// baseline default, per-pod annotation override, Spot preemption (2m), and size-scaled memory requests.
func TestStatusDeadline_Pending_BaselineAndOverrides(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	t.Run("default baseline timeout (10m)", func(t *testing.T) {
		createdAt := metav1.NewTime(time.Now().Add(-30 * time.Second))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "test-pod",
				UID:       "pod-uid-1",
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-test-1",
				CreationTimestamp: createdAt,
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
				TargetPodUID: "pod-uid-1",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pod, pmj).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		}

		// Reconcile uninitialized job -> enters Pending
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-test-1"}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-test-1"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
			t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhasePending)
		}
		if got.Status.Deadline == nil {
			t.Fatalf("status.deadline is nil, want non-nil")
		}
		wantDeadline := got.CreationTimestamp.Time.Add(util.DefaultMigrationTimeout)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
		}
	})

	t.Run("spot preemption job (2m timeout)", func(t *testing.T) {
		createdAt := metav1.NewTime(time.Now().Add(-10 * time.Second))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "spot-pod",
				UID:       "spot-uid",
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-spot",
				CreationTimestamp: createdAt,
				Annotations: map[string]string{
					util.AnnotationMigrationTimeout: "2m",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "spot-pod"},
				TargetPodUID: "spot-uid",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pod, pmj).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		}

		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-spot"}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-spot"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if got.Status.Deadline == nil {
			t.Fatalf("status.deadline is nil, want non-nil")
		}
		wantDeadline := got.CreationTimestamp.Time.Add(2 * time.Minute)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
		}
	})

	t.Run("per-pod override annotation (25m)", func(t *testing.T) {
		createdAt := metav1.NewTime(time.Now().Add(-5 * time.Second))
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "custom-pod",
				UID:       "custom-uid",
				Annotations: map[string]string{
					util.AnnotationMigrationTimeout: "25m",
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-custom",
				CreationTimestamp: createdAt,
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "custom-pod"},
				TargetPodUID: "custom-uid",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pod, pmj).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		}

		// Reconcile 1: sets Phase=Pending
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-custom"}}); err != nil {
			t.Fatalf("Reconcile 1: %v", err)
		}
		// Reconcile 2: inspects pod, propagates 25m timeout, transitions to Snapshotting
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-custom"}}); err != nil {
			t.Fatalf("Reconcile 2: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-custom"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
		}
		if got.Status.Deadline == nil {
			t.Fatalf("status.deadline is nil, want non-nil")
		}
		wantDeadline := got.Status.SnapshottingStartTime.Time.Add(25 * time.Minute)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
		}
	})

	t.Run("size-scaled memory requests", func(t *testing.T) {
		createdAt := metav1.NewTime(time.Now().Add(-5 * time.Second))
		// 500 MiB memory request adds 10s (500MiB / 50MiB/s = 10s)
		memQty := resource.MustParse("500Mi")
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "mem-pod",
				UID:       "mem-uid",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name: "app",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{corev1.ResourceMemory: memQty},
						},
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-mem",
				CreationTimestamp: createdAt,
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "mem-pod"},
				TargetPodUID: "mem-uid",
			},
		}

		c := fake.NewClientBuilder().WithScheme(scheme).
			WithObjects(pod, pmj).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{
			Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
		}

		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-mem"}}); err != nil {
			t.Fatalf("Reconcile 1: %v", err)
		}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-mem"}}); err != nil {
			t.Fatalf("Reconcile 2: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-mem"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		wantTimeout := util.DefaultMigrationTimeout + 10*time.Second
		wantDeadline := got.Status.SnapshottingStartTime.Time.Add(wantTimeout)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
		}
	})
}

// TestStatusDeadline_Transitions_SnapshottingAndEvicting verifies that status.deadline
// moves on each phase transition:
// - runs from creation in Pending
// - moves to SnapshottingStartTime in Snapshotting
// - moves to EvictingStartTime in Evicting
func TestStatusDeadline_Transitions_SnapshottingAndEvicting(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	createdAt := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	snapStart := metav1.NewTime(time.Now().Add(-2 * time.Minute))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "source-pod",
			UID:       "source-uid",
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-multi-phase",
			CreationTimestamp: createdAt,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "source-pod"},
			TargetPodUID: "source-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshottingStartTime: &snapStart,
			SnapshotRef:           "snapshot-123",
		},
	}

	mockSnapProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:       snapshot.PhaseReady,
			SnapshotRef: "snapshot-123",
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	r := &PodMigrationJobReconciler{
		Client:           c,
		Scheme:           scheme,
		Recorder:         record.NewFakeRecorder(10),
		SnapshotProvider: mockSnapProvider,
	}

	// Reconcile: Snapshotting -> Evicting (mock provider reports PhaseReady)
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-multi-phase"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !res.Requeue {
		t.Errorf("expected Requeue: true on transition to Evicting")
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-multi-phase"}, got); err != nil {
		t.Fatalf("get PMJ: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseEvicting)
	}
	if got.Status.EvictingStartTime == nil {
		t.Fatalf("EvictingStartTime is nil")
	}
	if got.Status.Deadline == nil {
		t.Fatalf("status.deadline is nil, want non-nil")
	}

	// In Evicting, deadline must run from EvictingStartTime
	wantDeadline := got.Status.EvictingStartTime.Time.Add(util.DefaultMigrationTimeout)
	if !got.Status.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
	}
}

// TestStatusDeadline_Evicting_MaxMigrationTimeoutHardCap verifies that in Evicting phase,
// the deadline is strictly capped by MaxMigrationTimeout (2h) from SnapshottingStartTime,
// matching the reconciler's hard cap enforcement.
func TestStatusDeadline_Evicting_MaxMigrationTimeoutHardCap(t *testing.T) {
	scheme := deadlineTestScheme()

	// Snapshotting started 1h50m ago (extended via active progress).
	// Now entering Evicting with a 20m timeout.
	// 1h50m + 20m = 2h10m, which exceeds the 2h MaxMigrationTimeout hard cap!
	snapStart := metav1.NewTime(time.Now().Add(-110 * time.Minute))
	evictStart := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-hard-cap",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-120 * time.Minute)),
			Annotations: map[string]string{
				util.AnnotationMigrationTimeout: "20m",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "app-pod"},
			TargetPodUID: "app-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshottingStartTime: &snapStart,
			EvictingStartTime:     &evictStart,
			SnapshotRef:           "snapshot-cap-test",
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	r := &PodMigrationJobReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
	}

	// Calculate deadline
	deadline := r.calculateDeadline(pmj)
	if deadline == nil {
		t.Fatalf("calculateDeadline returned nil")
	}

	// Hard cap: snapStart + 2h
	wantHardCap := snapStart.Time.Add(util.MaxMigrationTimeout)
	uncappedDeadline := evictStart.Time.Add(20 * time.Minute)

	if !uncappedDeadline.After(wantHardCap) {
		t.Fatalf("test setup sanity error: uncappedDeadline %v should be after hardCap %v", uncappedDeadline, wantHardCap)
	}

	if !deadline.Time.Equal(wantHardCap) {
		t.Errorf("deadline = %v, want hard cap %v (uncapped would be %v)", deadline.Time, wantHardCap, uncappedDeadline)
	}
}

// TestStatusDeadline_TerminalPhase_Retained verifies that when a job transitions
// to a terminal phase (Failed or Succeeded), status.deadline is preserved and not cleared.
func TestStatusDeadline_TerminalPhase_Retained(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	createdAt := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "failing-pod",
			UID:       "failing-uid",
		},
	}
	// Expired job in Pending (15m old > 10m default timeout)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-timeout",
			CreationTimestamp: createdAt,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "failing-pod"},
			TargetPodUID: "failing-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:    pmv1alpha1.PodMigrationJobPhasePending,
			Deadline: &metav1.Time{Time: createdAt.Time.Add(10 * time.Minute)},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	r := &PodMigrationJobReconciler{
		Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10),
	}

	// Reconcile: times out -> transitions to Failed
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-timeout"}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-timeout"}, got); err != nil {
		t.Fatalf("get PMJ: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseFailed)
	}
	if got.Status.Deadline == nil {
		t.Fatalf("status.deadline was cleared on terminal phase, want it retained")
	}
	wantDeadline := got.CreationTimestamp.Time.Add(10 * time.Minute)
	if !got.Status.Deadline.Time.Equal(wantDeadline) {
		t.Errorf("status.deadline = %v, want %v", got.Status.Deadline.Time, wantDeadline)
	}

	// Also verify calculateDeadline preserves existing deadline on Restoring and Succeeded
	got.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	restoringDeadline := r.calculateDeadline(got)
	if restoringDeadline == nil || !restoringDeadline.Time.Equal(wantDeadline) {
		t.Errorf("calculateDeadline in Restoring = %v, want %v", restoringDeadline, wantDeadline)
	}

	got.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	succeededDeadline := r.calculateDeadline(got)
	if succeededDeadline == nil || !succeededDeadline.Time.Equal(wantDeadline) {
		t.Errorf("calculateDeadline in Succeeded = %v, want %v", succeededDeadline, wantDeadline)
	}
}

// TestStatusDeadline_ZeroExtraStatusWrites verifies that status.deadline rides the
// existing status writes that move the job on, with zero extra status writes.
func TestStatusDeadline_ZeroExtraStatusWrites(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			UID:       "test-pod-uid",
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-no-extra-writes",
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
			TargetPodUID: "test-pod-uid",
		},
	}

	var statusWrites int
	funcs := interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && sub == "status" {
				statusWrites++
			}
			return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && sub == "status" {
				statusWrites++
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(funcs).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}

	key := types.NamespacedName{Namespace: "default", Name: "pmj-no-extra-writes"}

	// Pass 1: Empty status -> initializes Pending with deadline (exactly 1 status write)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 1: %v", err)
	}
	if statusWrites != 1 {
		t.Errorf("status writes after initialization = %d, want 1", statusWrites)
	}

	// Pass 2: Pending -> transitions to Snapshotting with new deadline (exactly 1 status write)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 2: %v", err)
	}
	if statusWrites != 2 {
		t.Errorf("status writes after Snapshotting transition = %d, want 2", statusWrites)
	}

	// Pass 3: In Snapshotting with mock provider returning Ready ->
	// fences source (1 write) and transitions to Evicting (1 write) -> exactly 4 status writes
	r.SnapshotProvider = &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:       snapshot.PhaseReady,
			SnapshotRef: "snap-ready",
		},
	}
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 3: %v", err)
	}
	if statusWrites != 4 {
		t.Errorf("status writes after Evicting transition = %d, want 4", statusWrites)
	}

	// Pass 4: In Evicting, stamps evicting-since annotation and requeues (0 status writes)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 4: %v", err)
	}
	if statusWrites != 4 {
		t.Errorf("status writes after Evicting evicting-since stamp = %d, want 4 (no status writes)", statusWrites)
	}

	// Pass 5: In Evicting, initiates eviction and clears eviction blockage conditions (1 status write)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 5: %v", err)
	}
	if statusWrites != 5 {
		t.Errorf("status writes after clearing eviction blockage conditions = %d, want 5", statusWrites)
	}

	// Pass 6: Origin pod is gone, all PVs detached -> transitions Evicting to Restoring (1 status write)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 6: %v", err)
	}
	if statusWrites != 6 {
		t.Errorf("status writes after Restoring transition = %d, want 6", statusWrites)
	}

	// Pass 7: In Restoring waiting for replacement pod -> exactly 0 new status writes
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 7: %v", err)
	}
	if statusWrites != 6 {
		t.Errorf("status writes while waiting in Restoring = %d, want 6 (no extra writes)", statusWrites)
	}

	// Verify final job has deadline set
	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ: %v", err)
	}
	if got.Status.Deadline == nil {
		t.Fatalf("final PMJ status.deadline is nil, want non-nil")
	}
}
