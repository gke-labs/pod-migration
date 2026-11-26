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

type stubDeadlineBudgetChecker struct {
	reason string
	err    error
}

func (s *stubDeadlineBudgetChecker) Check(context.Context, *pmv1alpha1.PodMigrationJob, string) (string, error) {
	return s.reason, s.err
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

	t.Run("per-pod override annotation (25m) pinned in budget-blocked pending", func(t *testing.T) {
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
			BudgetChecker: &stubDeadlineBudgetChecker{
				reason: reasonBudgetExhausted,
				err:    errors.New("budget exhausted"),
			},
		}

		// Reconcile 1: sets Phase=Pending (with default 10m baseline deadline)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-custom"}}); err != nil {
			t.Fatalf("Reconcile 1: %v", err)
		}
		// Reconcile 2: inspects pod, discovers 25m override, recomputes deadline, budget check blocks and keeps job in Pending
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-custom"}}); err != nil {
			t.Fatalf("Reconcile 2: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-custom"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
			t.Fatalf("phase = %s, want %s (job should remain in Pending while budget-blocked)", got.Status.Phase, pmv1alpha1.PodMigrationJobPhasePending)
		}
		if got.Status.Deadline == nil {
			t.Fatalf("status.deadline is nil, want non-nil")
		}
		wantDeadline := got.CreationTimestamp.Time.Add(25 * time.Minute)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v (difference: %v)", got.Status.Deadline.Time, wantDeadline, got.Status.Deadline.Time.Sub(got.CreationTimestamp.Time))
		}
	})

	t.Run("size-scaled memory requests pinned in budget-blocked pending (5000Mi -> 11m40s)", func(t *testing.T) {
		createdAt := metav1.NewTime(time.Now().Add(-5 * time.Second))
		// 5000 MiB memory request adds 100s = 1m40s (5000MiB / 50MiB/s = 100s), giving 11m40s total
		memQty := resource.MustParse("5000Mi")
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
			BudgetChecker: &stubDeadlineBudgetChecker{
				reason: reasonBudgetExhausted,
				err:    errors.New("budget exhausted"),
			},
		}

		// Reconcile 1: sets Phase=Pending (with default 10m baseline deadline)
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-mem"}}); err != nil {
			t.Fatalf("Reconcile 1: %v", err)
		}
		// Reconcile 2: inspects pod, discovers 5000Mi memory, scales timeout to 11m40s, recomputes deadline, budget check blocks and keeps job in Pending
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-mem"}}); err != nil {
			t.Fatalf("Reconcile 2: %v", err)
		}

		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-mem"}, got); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
			t.Fatalf("phase = %s, want %s (job should remain in Pending while budget-blocked)", got.Status.Phase, pmv1alpha1.PodMigrationJobPhasePending)
		}
		if got.Status.Deadline == nil {
			t.Fatalf("status.deadline is nil, want non-nil")
		}
		wantTimeout := util.DefaultMigrationTimeout + 100*time.Second // 11m40s
		wantDeadline := got.CreationTimestamp.Time.Add(wantTimeout)
		if !got.Status.Deadline.Time.Equal(wantDeadline) {
			t.Errorf("status.deadline = %v, want %v (deadline - created = %v, want 11m40s)",
				got.Status.Deadline.Time, wantDeadline, got.Status.Deadline.Time.Sub(got.CreationTimestamp.Time))
		}
	})
}

// TestStatusDeadline_Transitions_SnapshottingEvictingAndRestoring verifies that status.deadline
// moves on each phase transition through the real Reconcile:
// - Pending -> Snapshotting: moves to SST + MaxMigrationTimeout (2h upper bound for progressing snapshots)
// - Snapshotting -> Evicting: moves to EvictingStartTime + timeout (capped by MaxMigrationTimeout from SST)
// - Evicting -> Restoring: moves to RestoringStartTime + restoreCeiling()
// - Restoring pod gate consumption: updates to new RST + restoreCeiling() when pod gate resets RST
func TestStatusDeadline_Transitions_SnapshottingEvictingAndRestoring(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	createdAt := metav1.NewTime(time.Now().Add(-2 * time.Minute))

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
			Phase:    pmv1alpha1.PodMigrationJobPhasePending,
			Deadline: &metav1.Time{Time: createdAt.Time.Add(util.DefaultMigrationTimeout)},
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

	key := types.NamespacedName{Namespace: "default", Name: "pmj-multi-phase"}

	// 1. Reconcile Pending -> Snapshotting: verifies Reconcile writes SST + MaxMigrationTimeout (2h)
	// and pins the deadline update at the Pending -> Snapshotting transition (:1527).
	res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("Reconcile to Snapshotting: %v", err)
	}
	if !res.Requeue {
		t.Errorf("expected Requeue: true on transition to Snapshotting")
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ after Snapshotting transition: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	}
	if got.Status.SnapshottingStartTime == nil {
		t.Fatalf("SnapshottingStartTime is nil")
	}
	if got.Status.Deadline == nil {
		t.Fatalf("Snapshotting deadline is nil")
	}
	wantSnapDeadline := got.Status.SnapshottingStartTime.Time.Add(util.MaxMigrationTimeout)
	if !got.Status.Deadline.Time.Equal(wantSnapDeadline) {
		t.Errorf("Snapshotting deadline = %v, want SST + MaxMigrationTimeout %v (was pending deadline kept?)", got.Status.Deadline.Time, wantSnapDeadline)
	}

	// 2. Reconcile: Snapshotting -> Evicting (mock provider reports PhaseReady)
	res, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("Reconcile to Evicting: %v", err)
	}
	if !res.Requeue {
		t.Errorf("expected Requeue: true on transition to Evicting")
	}

	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ after Evicting transition: %v", err)
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

	// In Evicting, deadline runs from EvictingStartTime + timeout
	wantEvictDeadline := got.Status.EvictingStartTime.Time.Add(util.DefaultMigrationTimeout)
	if !got.Status.Deadline.Time.Equal(wantEvictDeadline) {
		t.Errorf("status.deadline in Evicting = %v, want %v", got.Status.Deadline.Time, wantEvictDeadline)
	}

	// 3. Delete origin pod and reconcile to transition Evicting -> Restoring
	if err := c.Delete(ctx, pod); err != nil {
		t.Fatalf("deleting source pod: %v", err)
	}
	// Reconcile: source pod is gone, PVs detached -> transitions to Restoring
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile to Restoring: %v", err)
	}

	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ after Restoring transition: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseRestoring)
	}
	if got.Status.RestoringStartTime == nil {
		t.Fatalf("RestoringStartTime is nil")
	}
	if got.Status.Deadline == nil {
		t.Fatalf("status.deadline in Restoring is nil, want non-nil")
	}

	// In Restoring, deadline must be stamped with RestoringStartTime + restoreCeiling()
	wantRestoreDeadline := got.Status.RestoringStartTime.Time.Add(r.restoreCeiling())
	if !got.Status.Deadline.Time.Equal(wantRestoreDeadline) {
		t.Errorf("status.deadline in Restoring = %v, want %v", got.Status.Deadline.Time, wantRestoreDeadline)
	}

	// 4. PodGate consumption in Restoring: pod gate consumes job 3 minutes later and resets
	// RestoringStartTime = now. The reconciler must advance status.deadline to new RST + restoreCeiling().
	gateRST := metav1.NewTime(got.Status.RestoringStartTime.Time.Add(3 * time.Minute))
	got.Status.RestoringStartTime = &gateRST
	got.Status.Consumed = true
	got.Status.RestoredPodUID = "restored-pod-uid"
	got.Status.RestoredPodName = "restored-pod-name"
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatalf("simulating pod gate consumption update: %v", err)
	}

	// Next Reconcile pass in Restoring must detect the new RST and patch status.deadline
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile after pod gate consumption: %v", err)
	}

	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ after pod gate consumption reconcile: %v", err)
	}
	wantConsumedDeadline := gateRST.Time.Add(r.restoreCeiling())
	if got.Status.Deadline == nil || !got.Status.Deadline.Time.Equal(wantConsumedDeadline) {
		t.Errorf("status.deadline after pod gate consumption = %v, want %v", got.Status.Deadline, wantConsumedDeadline)
	}
}

// TestStatusDeadline_Evicting_MaxMigrationTimeoutHardCap_ThroughReconcile verifies that
// during transition to Evicting phase driven by Reconcile, the deadline is strictly capped
// by MaxMigrationTimeout (2h) from SnapshottingStartTime, matching enforcement.
func TestStatusDeadline_Evicting_MaxMigrationTimeoutHardCap_ThroughReconcile(t *testing.T) {
	ctx := context.Background()
	scheme := deadlineTestScheme()

	// Snapshotting started 110m ago.
	// Entering Evicting with a 20m timeout.
	// 110m + 20m = 130m > 120m (2h MaxMigrationTimeout hard cap).
	snapStart := metav1.NewTime(time.Now().Add(-110 * time.Minute).Truncate(time.Second))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "cap-pod",
			UID:       "cap-uid",
		},
	}
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
			PodRef:       corev1.LocalObjectReference{Name: "cap-pod"},
			TargetPodUID: "cap-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshottingStartTime: &snapStart,
			SnapshotRef:           "snapshot-cap-test",
		},
	}

	mockSnapProvider := &fakeSnapshotProvider{
		checkStatusResult: &snapshot.Status{
			Phase:       snapshot.PhaseReady,
			SnapshotRef: "snapshot-cap-test",
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

	// Reconcile: drives transition from Snapshotting to Evicting
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-hard-cap"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-hard-cap"}, got); err != nil {
		t.Fatalf("get PMJ: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
		t.Fatalf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseEvicting)
	}
	if got.Status.Deadline == nil {
		t.Fatalf("status.deadline is nil, want non-nil")
	}

	// Hard cap: snapStart + 2h
	wantHardCap := snapStart.Time.Add(util.MaxMigrationTimeout)
	if !got.Status.Deadline.Time.Equal(wantHardCap) {
		t.Errorf("deadline = %v, want hard cap %v", got.Status.Deadline.Time, wantHardCap)
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

	// Also verify calculateDeadline preserves existing deadline on terminal phases
	got.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	succeededDeadline := r.calculateDeadline(got)
	if succeededDeadline == nil || !succeededDeadline.Time.Equal(wantDeadline) {
		t.Errorf("calculateDeadline in Succeeded = %v, want %v", succeededDeadline, wantDeadline)
	}

	got.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
	swrDeadline := r.calculateDeadline(got)
	if swrDeadline == nil || !swrDeadline.Time.Equal(wantDeadline) {
		t.Errorf("calculateDeadline in SucceededWithoutRestore = %v, want %v", swrDeadline, wantDeadline)
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
	gotSnap := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, key, gotSnap); err != nil {
		t.Fatalf("get PMJ after Snapshotting transition: %v", err)
	}
	wantSnapDeadline := gotSnap.Status.SnapshottingStartTime.Time.Add(util.MaxMigrationTimeout)
	if gotSnap.Status.Deadline == nil || !gotSnap.Status.Deadline.Time.Equal(wantSnapDeadline) {
		t.Errorf("status.deadline after Snapshotting transition = %v, want %v", gotSnap.Status.Deadline, wantSnapDeadline)
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
	wantFinalDeadline := got.Status.RestoringStartTime.Time.Add(r.restoreCeiling())
	if !got.Status.Deadline.Time.Equal(wantFinalDeadline) {
		t.Errorf("final PMJ status.deadline = %v, want %v", got.Status.Deadline.Time, wantFinalDeadline)
	}

	// Pass 8: PodGate consumes job and resets RestoringStartTime 3 minutes later ->
	// Reconcile updates status.deadline to new RST + restoreCeiling() with exactly 1 status write
	gateRST := metav1.NewTime(got.Status.RestoringStartTime.Time.Add(3 * time.Minute))
	got.Status.RestoringStartTime = &gateRST
	got.Status.Consumed = true
	got.Status.RestoredPodUID = "consumer-pod-uid"
	got.Status.RestoredPodName = "consumer-pod-name"
	if err := c.Status().Update(ctx, got); err != nil {
		t.Fatalf("simulating pod gate consumption update: %v", err)
	}
	consumptionUpdateWrites := statusWrites

	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 8: %v", err)
	}
	if statusWrites != consumptionUpdateWrites+1 {
		t.Errorf("status writes after pod gate consumption update = %d, want %d (exactly 1 patch)", statusWrites, consumptionUpdateWrites+1)
	}

	if err := c.Get(ctx, key, got); err != nil {
		t.Fatalf("get PMJ after consumption reconcile: %v", err)
	}
	wantUpdatedDeadline := gateRST.Time.Add(r.restoreCeiling())
	if got.Status.Deadline == nil || !got.Status.Deadline.Time.Equal(wantUpdatedDeadline) {
		t.Errorf("status.deadline after consumption update = %v, want %v", got.Status.Deadline, wantUpdatedDeadline)
	}

	// Pass 9: Subsequent reconcile in Restoring -> exactly 0 new status writes (no churn)
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile 9: %v", err)
	}
	if statusWrites != consumptionUpdateWrites+1 {
		t.Errorf("status writes on subsequent reconcile = %d, want %d (zero extra writes / no churn)", statusWrites, consumptionUpdateWrites+1)
	}
}
