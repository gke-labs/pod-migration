package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestPodGateReconciler_Reconcile(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name          string
		pod           *corev1.Pod
		pmj           *pmv1alpha1.PodMigrationJob
		apiReaderPMJ  *pmv1alpha1.PodMigrationJob // PMJ visible only via APIReader (simulates informer cache lag)
		expectHasGate bool
		expectRequeue bool
		// expectColdStartBypass asserts the release scrubbed the assignment:
		// ps-name key present and empty, assigned-pmj annotation removed.
		expectColdStartBypass bool
	}{
		{
			name: "Pod without scheduling gate is ignored",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{},
				},
			},
			expectHasGate: false,
		},
		{
			name: "Gated pod without PMJ assignment gets gate released with cold-start bypass (bare pod)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			expectHasGate:         false,
			expectColdStartBypass: true,
		},
		{
			name: "Terminating gated pod is left alone",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-pod-terminating",
					Namespace:         "default",
					DeletionTimestamp: &metav1.Time{Time: metav1.Now().Time},
					Finalizers:        []string{"test.gke.io/keep"},
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			expectHasGate: true,
		},
		{
			name: "Gated pod with Restoring PMJ but empty SnapshotRef releases with cold-start bypass",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-no-snapref",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-no-snapref",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-no-snapref",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod-no-snapref"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
					SnapshotRef: "",
				},
			},
			expectHasGate:         false,
			expectColdStartBypass: true,
		},
		{
			name: "Gated pod with in-progress PMJ does not release gate",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
				},
			},
			expectHasGate: true,
		},
		{
			name: "Gated pod with succeeded PMJ gets gate released & snapshot ref injected",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseSucceeded,
					SnapshotRef: "some-snapshot-name",
				},
			},
			expectHasGate: false,
		},
		{
			name: "Gated pod with failed PMJ gets gate released (fail-open)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase: pmv1alpha1.PodMigrationJobPhaseFailed,
				},
			},
			expectHasGate:         false,
			expectColdStartBypass: true,
		},
		{
			name: "Gated pod with SucceededWithoutRestore PMJ gets gate released without snapshot injection",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
					SnapshotRef: "some-snapshot-name",
				},
			},
			expectHasGate:         false,
			expectColdStartBypass: true,
		},
		{
			name: "Gated pod with missing parent ReplicaSet (NotFound) still releases gate if PMJ succeeds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-rs-missing",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "missing-rs",
							UID:        "missing-rs-uid",
						},
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod-rs-missing"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseSucceeded,
					SnapshotRef: "some-snapshot-name",
				},
			},
			expectHasGate: false,
		},
		{
			name: "Gated pod with Restoring PMJ gets gate released & snapshot ref injected",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
					SnapshotRef: "some-snapshot-name",
				},
			},
			expectHasGate: false,
		},
		{
			name: "Gated pod with Evicting PMJ does not release gate until Restoring",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-test-pod",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-test-pod",
					Namespace: "default",
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef: corev1.LocalObjectReference{Name: "test-pod"},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:       pmv1alpha1.PodMigrationJobPhaseEvicting,
					SnapshotRef: "some-snapshot-name",
					PVsToDetach: []string{},
				},
			},
			expectHasGate: true,
		},
		{
			name: "Gated pod whose PMJ is deleted (confirmed via APIReader) releases gate with cold-start bypass",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-deleted-pmj",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "nonexistent-pmj",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj:                   nil,
			apiReaderPMJ:          nil,
			expectHasGate:         false,
			expectColdStartBypass: true,
		},
		{
			name: "Gated pod whose PMJ is missing from cache but live per APIReader keeps gate and requeues",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod-cache-lag",
					Namespace: "default",
					Annotations: map[string]string{
						"pod-migration.gke.io/assigned-pmj": "pmj-lagging",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			pmj: nil,
			apiReaderPMJ: &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pmj-lagging",
					Namespace: "default",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
				},
			},
			expectHasGate: true,
			expectRequeue: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			var initObjs []runtime.Object
			initObjs = append(initObjs, tt.pod)
			if tt.pmj != nil {
				initObjs = append(initObjs, tt.pmj)
			}

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithRuntimeObjects(initObjs...).
				Build()

			// The APIReader sees everything the cache sees, plus any PMJ the
			// test declares as not-yet-synced into the informer cache.
			apiObjs := append([]runtime.Object{}, initObjs...)
			if tt.apiReaderPMJ != nil {
				apiObjs = append(apiObjs, tt.apiReaderPMJ)
			}
			apiReader := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithRuntimeObjects(apiObjs...).
				Build()

			r := &PodGateReconciler{
				Client:    cl,
				APIReader: apiReader,
				Scheme:    scheme,
			}

			res, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: tt.pod.Namespace,
					Name:      tt.pod.Name,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.expectRequeue && res.RequeueAfter == 0 {
				t.Errorf("expected a RequeueAfter, got: %+v", res)
			}

			updatedPod := &corev1.Pod{}
			err = cl.Get(ctx, types.NamespacedName{Namespace: tt.pod.Namespace, Name: tt.pod.Name}, updatedPod)
			if err != nil {
				t.Fatalf("failed to fetch updated pod: %v", err)
			}

			hasGate := false
			for _, gate := range updatedPod.Spec.SchedulingGates {
				if gate.Name == "gke.io/pod-migration-gate" {
					hasGate = true
					break
				}
			}

			if hasGate != tt.expectHasGate {
				t.Errorf("expected scheduling gate presence: %t, got: %t", tt.expectHasGate, hasGate)
			}

			if !tt.expectHasGate && tt.pmj != nil && (tt.pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded || tt.pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseRestoring) {
				snapName := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]
				if snapName != tt.pmj.Status.SnapshotRef {
					t.Errorf("expected snapshot annotation %q, got %q", tt.pmj.Status.SnapshotRef, snapName)
				}
			}

			if tt.expectColdStartBypass {
				psName, ok := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]
				if !ok {
					t.Errorf("expected explicit empty ps-name cold-start bypass annotation, key is absent")
				} else if psName != "" {
					t.Errorf("expected empty ps-name bypass, got %q", psName)
				}
				if leftover, ok := updatedPod.Annotations["pod-migration.gke.io/assigned-pmj"]; ok {
					t.Errorf("expected assigned-pmj annotation removed on release, still present: %q", leftover)
				}
			}
		})
	}
}

// The 2s poll was a requeue storm at scale: 2,000 gated pods on a single
// serialized worker.  PMJ watch events are the fast path for gate release;
// the wait requeue is only a resync backstop and must stay coarse.
func TestPodGateReconciler_ActivePMJWaitUsesBackstopRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "waiting-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "pmj-active",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
		},
	}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "pmj-active", Namespace: "default"},
		Status:     pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithRuntimeObjects(pod, pmj).
		Build()
	r := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme}

	res, err := r.Reconcile(context.TODO(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "waiting-pod"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter < 30*time.Second {
		t.Errorf("expected coarse backstop requeue (>= 30s), got %v", res.RequeueAfter)
	}
}

func TestPodGateReconciler_mapPMJToPods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-original-pod"
	originalPodName := "original-pod"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{
				Name: originalPodName,
			},
		},
	}

	replacementPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "replacement-pod-1",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
	}

	replacementPod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "replacement-pod-2",
		},
	}

	siblingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "sibling-pod",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pmj, replacementPod1, replacementPod2, siblingPod).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	requests := r.mapPMJToPods(context.Background(), pmj)

	// We expect 3 requests: original-pod, replacement-pod-1, sibling-pod
	expectedNames := map[string]bool{
		originalPodName:     true,
		"replacement-pod-1": true,
		"sibling-pod":       true,
	}

	if len(requests) != 3 {
		t.Fatalf("Expected 3 requests, got %d: %v", len(requests), requests)
	}

	for _, req := range requests {
		if req.Namespace != namespace {
			t.Errorf("Expected namespace %q, got %q", namespace, req.Namespace)
		}
		if !expectedNames[req.Name] {
			t.Errorf("Unexpected reconcile request for pod %q", req.Name)
		}
		delete(expectedNames, req.Name)
	}

	if len(expectedNames) > 0 {
		t.Errorf("Failed to receive reconcile requests for expected pods: %v", expectedNames)
	}
}

func TestPodGateReconciler_Reconcile_Collision(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-test"

	winnerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "a-winner",
			UID:       "uid-winner",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	loserPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "b-loser",
			UID:       "uid-loser",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "origin-pod-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(winnerPod, loserPod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "b-loser",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling loser: %v", err)
	}

	updatedLoser := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "b-loser"}, updatedLoser)
	if err != nil {
		t.Fatalf("failed to fetch updated loser pod: %v", err)
	}

	hasGate := false
	for _, gate := range updatedLoser.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			hasGate = true
			break
		}
	}
	if hasGate {
		t.Error("expected loser pod to have scheduling gate removed")
	}

	if _, assigned := updatedLoser.Annotations["pod-migration.gke.io/assigned-pmj"]; assigned {
		t.Error("expected loser pod to have PMJ assignment cleared")
	}

	bypass, hasBypass := updatedLoser.Annotations["podsnapshot.gke.io/ps-name"]
	if !hasBypass || bypass != "" {
		t.Errorf("expected restore-bypass annotation, got %q (present: %t)", bypass, hasBypass)
	}

	updatedWinner := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "a-winner"}, updatedWinner)
	if err != nil {
		t.Fatalf("failed to fetch winner pod: %v", err)
	}

	hasGate = false
	for _, gate := range updatedWinner.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			hasGate = true
			break
		}
	}
	if !hasGate {
		t.Error("expected winner pod to keep scheduling gate")
	}

	if assigned := updatedWinner.Annotations["pod-migration.gke.io/assigned-pmj"]; assigned != pmjName {
		t.Errorf("expected winner pod to keep PMJ assignment, got %q", assigned)
	}

	if _, hasBypass := updatedWinner.Annotations["podsnapshot.gke.io/ps-name"]; hasBypass {
		t.Error("expected winner pod to NOT have restore-bypass annotation")
	}
}

func TestPodGateReconciler_Reconcile_Collision_WinnerAlreadyUngated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-test"

	// Winner pod already had its gate released during an earlier reconcile and is running
	winnerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "a-winner",
			UID:       "uid-winner",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
				"podsnapshot.gke.io/ps-name":        "snapshot-ref",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: nil, // Gate already released
		},
	}

	// Loser pod is reconciled next
	loserPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "b-loser",
			UID:       "uid-loser",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "origin-pod-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSucceeded,
			SnapshotRef: "snapshot-ref",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(winnerPod, loserPod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "b-loser",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling loser: %v", err)
	}

	updatedLoser := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "b-loser"}, updatedLoser)
	if err != nil {
		t.Fatalf("failed to fetch updated loser pod: %v", err)
	}

	// Loser pod must have its gate removed, assignment cleared, and restore-bypass injected
	for _, gate := range updatedLoser.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			t.Error("expected loser pod to have scheduling gate removed")
		}
	}

	if _, assigned := updatedLoser.Annotations["pod-migration.gke.io/assigned-pmj"]; assigned {
		t.Error("expected loser pod to have PMJ assignment cleared")
	}

	bypass, hasBypass := updatedLoser.Annotations["podsnapshot.gke.io/ps-name"]
	if !hasBypass || bypass != "" {
		t.Errorf("expected restore-bypass annotation \"\", got %q (present: %t)", bypass, hasBypass)
	}
}

func TestPodGateReconciler_Reconcile_Collision_Deployment_AlternativePMJFound(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-deploy-1"
	altPmjName := "pmj-deploy-2"

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "test-rs",
			UID:       "rs-uid",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "test-deployment",
					UID:        "deploy-uid",
				},
			},
		},
	}

	winnerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "a-winner",
			UID:               "uid-winner",
			CreationTimestamp: metav1.Now(),
			Labels: map[string]string{
				appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1",
				"pod-migration.gke.io/enabled":         "true",
			},
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "test-rs",
					UID:        "rs-uid",
				},
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	loserPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "b-loser",
			UID:               "uid-loser",
			CreationTimestamp: metav1.Now(),
			Labels: map[string]string{
				appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1",
				"pod-migration.gke.io/enabled":         "true",
			},
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "test-rs",
					UID:        "rs-uid",
				},
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
			Labels: map[string]string{
				"pod-migration.gke.io/parent-name":       "test-deployment",
				"pod-migration.gke.io/parent-kind":       "Deployment",
				"pod-migration.gke.io/pod-template-hash": "hash-v1",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "origin-uid-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      altPmjName,
			Labels: map[string]string{
				"pod-migration.gke.io/parent-name":       "test-deployment",
				"pod-migration.gke.io/parent-kind":       "Deployment",
				"pod-migration.gke.io/pod-template-hash": "hash-v1",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "origin-uid-2",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(rs, winnerPod, loserPod, pmj1, pmj2).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "b-loser",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling loser: %v", err)
	}

	updatedLoser := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "b-loser"}, updatedLoser)
	if err != nil {
		t.Fatalf("failed to fetch updated loser pod: %v", err)
	}

	if assigned := updatedLoser.Annotations["pod-migration.gke.io/assigned-pmj"]; assigned != altPmjName {
		t.Errorf("expected loser pod to be reassigned to %q, got %q", altPmjName, assigned)
	}
}

func TestPodGateReconciler_Reconcile_AlreadyConsumedPMJ_ReleasesGateWithColdStartBypass(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-consumed"

	loserPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-loser",
			UID:       "uid-loser",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			SnapshotRef:    "snapshot-ref",
			Consumed:       true,
			RestoredPodUID: "uid-winner",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(loserPod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	_, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-loser",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-loser"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated pod: %v", err)
	}

	for _, gate := range updatedPod.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			t.Error("expected scheduling gate to be removed")
		}
	}

	if val, ok := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; !ok || val != "" {
		t.Errorf("expected podsnapshot.gke.io/ps-name annotation to be %q, got %q (present: %t)", "", val, ok)
	}

	if val, ok := updatedPod.Annotations["pod-migration.gke.io/assigned-pmj"]; ok {
		t.Errorf("expected pod-migration.gke.io/assigned-pmj annotation to be deleted, got %q", val)
	}
}

func TestPodGateReconciler_Reconcile_RecordsRestoredPodNameAndUID(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-test"
	podName := "test-replacement-pod"
	podUID := types.UID("uid-replacement-123")

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       podUID,
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "orig-pod"},
			TargetPodUID: "orig-pod-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef: "snapshot-123",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      podName,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on successful un-gate, got %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if !updatedPMJ.Status.Consumed {
		t.Errorf("expected PMJ Consumed to be true, got false")
	}
	if updatedPMJ.Status.RestoredPodUID != string(podUID) {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", string(podUID), updatedPMJ.Status.RestoredPodUID)
	}
	if updatedPMJ.Status.RestoredPodName != podName {
		t.Errorf("expected PMJ RestoredPodName == %q, got %q", podName, updatedPMJ.Status.RestoredPodName)
	}
	if !updatedPMJ.Status.GateReleased {
		t.Errorf("expected PMJ GateReleased to be true after gate release, got false")
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_RecoversAndAdopts(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// PMJ was previously marked Consumed by a pod that has since died/disappeared.
	// RestoringStartTime was recorded for that dead pod and must be reset on adoption.
	oldStartTime := metav1.NewTime(time.Now().Add(-4 * time.Minute))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:        "snapshot-valid-123",
			Consumed:           true,
			RestoringStartTime: &oldStartTime,
			RestoredPodUID:     "uid-dead-consumer",
			RestoredPodName:    "pod-dead-consumer",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj). // dead consumer pod is NOT in objects
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on successful adoption, got %+v", res)
	}

	// Verify candidate pod was ungated and adopted the snapshot
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	for _, gate := range updatedPod.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			t.Error("expected scheduling gate to be removed from candidate pod")
		}
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "snapshot-valid-123" {
		t.Errorf("expected ps-name annotation %q, got %q", "snapshot-valid-123", val)
	}

	// Verify PMJ was recovered and adopted by candidate pod
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if !updatedPMJ.Status.Consumed {
		t.Errorf("expected PMJ Consumed to be true, got false")
	}
	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
	if updatedPMJ.Status.RestoredPodName != "pod-candidate" {
		t.Errorf("expected PMJ RestoredPodName == %q, got %q", "pod-candidate", updatedPMJ.Status.RestoredPodName)
	}
	if updatedPMJ.Status.RestoringStartTime == nil {
		t.Errorf("expected PMJ RestoringStartTime to be set, got nil")
	} else if updatedPMJ.Status.RestoringStartTime.Equal(&oldStartTime) {
		t.Errorf("expected PMJ RestoringStartTime to be reset from %v, but was unchanged", oldStartTime)
	}
	if !updatedPMJ.Status.GateReleased {
		t.Errorf("expected PMJ GateReleased to be true after un-gate, got false")
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_TerminatingConsumerDoesNotRecover(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded-terminating"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Prior consumer exists (terminating or not, it still occupies the pod resource).
	// Candidate pod must not double-restore while consumer exists.
	now := metav1.Now()
	terminatingConsumer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "pod-terminating",
			UID:               "uid-terminating",
			DeletionTimestamp: &now,
			Finalizers:        []string{"test.gke.io/protect"},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-term-123",
			Consumed:        true,
			RestoredPodUID:  "uid-terminating",
			RestoredPodName: "pod-terminating",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, terminatingConsumer, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected RequeueAfter: 5s while consumer is terminating, got %v", res.RequeueAfter)
	}

	// Candidate pod must remain gated in Pending while consumer is still terminating
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}

	if len(updatedPod.Spec.SchedulingGates) == 0 {
		t.Errorf("expected candidate pod to remain gated while consumer terminates, but gate was removed")
	}

	// PMJ remains consumed by terminating consumer
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-terminating" {
		t.Errorf("expected PMJ RestoredPodUID to remain %q, got %q", "uid-terminating", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_CacheLagConsumerStillOnAPIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-cache-lag"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	liveConsumer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-live",
			UID:       "uid-live",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-lag-123",
			Consumed:        true,
			RestoredPodUID:  "uid-live",
			RestoredPodName: "pod-live",
		},
	}

	// Fake informer cache is missing pod-live (simulating cache lag)
	cacheClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	// Direct API reader has pod-live
	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(liveConsumer).
		Build()

	r := &PodGateReconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Scheme:    scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected RequeueAfter: 5s, got %v", res.RequeueAfter)
	}

	// Because APIReader confirmed pod-live is still on the API server, candidate pod
	// remains gated waiting for the consumer
	updatedPod := &corev1.Pod{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if len(updatedPod.Spec.SchedulingGates) == 0 {
		t.Errorf("expected candidate pod to remain gated, but gate was removed")
	}

	// PMJ remains consumed by uid-live
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-live" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-live", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_ConfirmedDeletedOnAPIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-confirmed-deleted"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-confirmed-123",
			Consumed:        true,
			RestoredPodUID:  "uid-dead",
			RestoredPodName: "pod-dead",
		},
	}

	cacheClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		Build() // empty: pod-dead confirmed NotFound on API server

	r := &PodGateReconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Scheme:    scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on adoption, got %+v", res)
	}

	// Verify candidate pod adopted the snapshot
	updatedPod := &corev1.Pod{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "snapshot-confirmed-123" {
		t.Errorf("expected ps-name annotation %q, got %q", "snapshot-confirmed-123", val)
	}

	// Verify PMJ was recovered and adopted by candidate pod
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_AdoptsNewCandidateUIDAndReleasesGate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded-adopt"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-clear-123",
			Consumed:        true,
			RestoredPodUID:  "uid-dead",
			RestoredPodName: "pod-dead",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result, got %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
	if !updatedPMJ.Status.GateReleased {
		t.Errorf("expected PMJ GateReleased == true after gate removal, got false")
	}
}

func TestPodGateReconciler_Reconcile_SucceededPMJ_NeverReRestored(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-succeeded-single-use"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// PMJ succeeded in the past; consumer pod died later.
	// Must NOT be resurrected.
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
			SnapshotRef:     "snapshot-succeeded-123",
			Consumed:        true,
			RestoredPodUID:  "uid-dead",
			RestoredPodName: "pod-dead",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj). // consumer pod does not exist
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on cold-start bypass, got %+v", res)
	}

	// Candidate pod must be released with cold-start bypass!
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "" {
		t.Errorf("expected ps-name annotation %q (bypass), got %q", "", val)
	}

	// PMJ remains Succeeded and untouched
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-dead" {
		t.Errorf("expected PMJ RestoredPodUID to remain %q, got %q", "uid-dead", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_UIDMismatchRecovers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded-mismatch"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Pod with same name exists, but UID is different (recreated pod instance)
	recreatedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-prior",
			UID:       "uid-different-instance",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-mismatch-123",
			Consumed:        true,
			RestoredPodUID:  "uid-original-instance",
			RestoredPodName: "pod-prior",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, recreatedPod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on adoption, got %+v", res)
	}

	// Verify candidate pod was ungated and adopted the snapshot
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "snapshot-mismatch-123" {
		t.Errorf("expected ps-name annotation %q, got %q", "snapshot-mismatch-123", val)
	}

	// Verify PMJ was recovered and adopted by candidate pod
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_EmptyRestoredPodNameRecovers(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded-noname"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Legacy status: RestoredPodName is empty, RestoredPodUID is set to non-existent pod
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:    "snapshot-noname-123",
			Consumed:       true,
			RestoredPodUID: "uid-dead-no-name",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on adoption, got %+v", res)
	}

	// Verify candidate pod was ungated and adopted the snapshot
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "snapshot-noname-123" {
		t.Errorf("expected ps-name annotation %q, got %q", "snapshot-noname-123", val)
	}

	// Verify PMJ was recovered and adopted by candidate pod
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_EmptyRestoredPodName_ConsumerStillExists(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stranded-noname-exists"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Consumer pod still exists, carries assigned-pmj annotation, but PMJ has empty RestoredPodName
	existingConsumerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-consumer-legacy",
			UID:       "uid-consumer-still-alive",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:    "snapshot-noname-alive-123",
			Consumed:       true,
			RestoredPodUID: "uid-consumer-still-alive",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, existingConsumerPod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected RequeueAfter: 5s while consumer pod still exists via index, got %v", res.RequeueAfter)
	}

	// Candidate pod must remain gated
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}
	if len(updatedPod.Spec.SchedulingGates) == 0 {
		t.Errorf("expected candidate pod to remain gated, but gate was removed")
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_GateAlreadyReleasedDoesNotRecover(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-gate-already-released"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Consumer pod died, BUT its scheduling gate was already released before it died.
	// Single-use isolation must prevent re-restoring the snapshot into a second pod.
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-gate-released-123",
			Consumed:        true,
			GateReleased:    true, // Gate was already released on consumer pod
			RestoredPodUID:  "uid-dead-after-ungate",
			RestoredPodName: "pod-dead-after-ungate",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on cold-start bypass, got %+v", res)
	}

	// Candidate pod must release with cold-start bypass (never re-restores snapshot)
	updatedPod := &corev1.Pod{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch updated candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "" {
		t.Errorf("expected ps-name annotation %q (cold-start bypass), got %q", "", val)
	}

	// PMJ remains unchanged
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cl.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-dead-after-ungate" {
		t.Errorf("expected PMJ RestoredPodUID to remain %q, got %q", "uid-dead-after-ungate", updatedPMJ.Status.RestoredPodUID)
	}
}

func TestPodGateReconciler_Reconcile_StrandedPMJ_StaleCacheHitConfirmedDeletedOnAPIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-stale-cache-hit"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Stale cache has pod-dead, but API server confirms it has already been deleted
	staleConsumerInCache := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-dead",
			UID:       "uid-dead",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-stale-cache-123",
			Consumed:        true,
			GateReleased:    false,
			RestoredPodUID:  "uid-dead",
			RestoredPodName: "pod-dead",
		},
	}

	cacheClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj, staleConsumerInCache).
		Build()

	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		Build() // empty: pod-dead is confirmed NotFound on API server

	r := &PodGateReconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Scheme:    scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result on adoption, got %+v", res)
	}

	// Verify candidate pod was un-gated and adopted the snapshot despite the stale cache hit
	updatedPod := &corev1.Pod{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedPod)
	if err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}

	if val := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]; val != "snapshot-stale-cache-123" {
		t.Errorf("expected ps-name %q, got %q", "snapshot-stale-cache-123", val)
	}

	// Verify PMJ adopted by candidate
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	err = cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ)
	if err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}

	if updatedPMJ.Status.RestoredPodUID != "uid-candidate" {
		t.Errorf("expected PMJ RestoredPodUID == %q, got %q", "uid-candidate", updatedPMJ.Status.RestoredPodUID)
	}
	if !updatedPMJ.Status.GateReleased {
		t.Errorf("expected PMJ GateReleased == true, got false")
	}
}

func TestPodGate_GateReleased_StatusPatchFailure_PodRemainsGated(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-patch-failure"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-retry-123",
			Consumed:        true,
			GateReleased:    false,
			RestoredPodUID:  "uid-dead",
			RestoredPodName: "pod-dead",
		},
	}

	baseClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	failPatchOnce := true
	cl := interceptor.NewClient(baseClient, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, subResourceName string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if subResourceName == "status" && failPatchOnce {
				failPatchOnce = false
				return apierrors.NewConflict(schema.GroupResource{Group: "podmigration.gke.io", Resource: "podmigrationjobs"}, pmjName, errors.New("conflict on status patch"))
			}
			return c.SubResource(subResourceName).Patch(ctx, obj, patch, opts...)
		},
	})

	r := &PodGateReconciler{
		Client: cl,
		Scheme: scheme,
	}

	ctx := context.Background()
	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	}

	// 1. First reconcile: status patch fails. Reconcile must return the error and fail closed:
	// the candidate pod MUST still be gated so it cannot restore.
	_, err := r.Reconcile(ctx, req)
	if err == nil {
		t.Fatalf("expected error on status patch failure, got nil")
	}

	podCheck := &corev1.Pod{}
	if err := baseClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, podCheck); err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}
	if !podHasMigrationGate(podCheck) {
		t.Fatalf("fail-closed violation: expected candidate pod to retain scheduling gate when status patch fails, but gate was removed")
	}

	// 2. Second reconcile (retry): patch succeeds.
	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("unexpected error on retry reconcile: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("expected empty reconcile.Result after successful retry, got %+v", res)
	}

	// Verify GateReleased was persisted and candidate pod is now un-gated
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := baseClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ); err != nil {
		t.Fatalf("failed to fetch updated PMJ: %v", err)
	}
	if !updatedPMJ.Status.GateReleased {
		t.Errorf("expected GateReleased == true after retry, got false")
	}

	if err := baseClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, podCheck); err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}
	if podHasMigrationGate(podCheck) {
		t.Errorf("expected candidate pod to have scheduling gate removed after successful reconcile, but gate is still present")
	}
}

func TestPodGate_EmptyRestoredPodName_UsesLiveReader(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-empty-name-live-reader"

	candidatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-candidate",
			UID:       "uid-candidate",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
		},
	}

	// Living consumer pod that exists on the API server, but NOT yet in the cache
	liveConsumerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pod-consumer-live",
			UID:       "uid-consumer-live",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": pmjName,
			},
		},
	}

	// Legacy PMJ with empty RestoredPodName
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "pod-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:     "snapshot-live-read-123",
			Consumed:        true,
			GateReleased:    false,
			RestoredPodUID:  "uid-consumer-live",
			RestoredPodName: "", // Empty: triggers live namespace list fallback
		},
	}

	// Cache only has candidatePod and pmj (liveConsumerPod is absent due to cache lag)
	cacheClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(candidatePod, pmj).
		Build()

	// Live APIReader has liveConsumerPod
	apiReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(liveConsumerPod).
		Build()

	r := &PodGateReconciler{
		Client:    cacheClient,
		APIReader: apiReader,
		Scheme:    scheme,
	}

	ctx := context.Background()
	res, err := r.Reconcile(ctx, reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      "pod-candidate",
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Reconciler should see consumer still exists via live APIReader, and requeue without recovering
	if res.RequeueAfter != 5*time.Second {
		t.Errorf("expected RequeueAfter: 5s while consumer is still alive on live API, got %+v", res)
	}

	// Candidate pod must NOT be un-gated
	updatedCandidate := &corev1.Pod{}
	if err := cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "pod-candidate"}, updatedCandidate); err != nil {
		t.Fatalf("failed to fetch candidate pod: %v", err)
	}
	if !podHasMigrationGate(updatedCandidate) {
		t.Errorf("candidate pod must remain gated while consumer exists on live API server")
	}

	// PMJ RestoredPodUID must remain unchanged
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cacheClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pmjName}, updatedPMJ); err != nil {
		t.Fatalf("failed to fetch PMJ: %v", err)
	}
	if updatedPMJ.Status.RestoredPodUID != "uid-consumer-live" {
		t.Errorf("PMJ RestoredPodUID must not be overwritten, got %q", updatedPMJ.Status.RestoredPodUID)
	}
}
