package controller

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
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
		expectHasGate bool
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
			name: "Gated pod without PMJ assignment gets gate released immediately (bare pod)",
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
			expectHasGate: false,
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
			expectHasGate: false,
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			var initObjs []runtime.Object
			initObjs = append(initObjs, tt.pod)
			if tt.pmj != nil {
				initObjs = append(initObjs, tt.pmj)
			}

			cl := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(initObjs...).Build()
			r := &PodGateReconciler{
				Client: cl,
				Scheme: scheme,
			}

			_, err := r.Reconcile(ctx, reconcile.Request{
				NamespacedName: types.NamespacedName{
					Namespace: tt.pod.Namespace,
					Name:      tt.pod.Name,
				},
			})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
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

			if !tt.expectHasGate && tt.pmj != nil && tt.pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseSucceeded {
				snapName := updatedPod.Annotations["podsnapshot.gke.io/ps-name"]
				if snapName != tt.pmj.Status.SnapshotRef {
					t.Errorf("expected snapshot annotation %q, got %q", tt.pmj.Status.SnapshotRef, snapName)
				}
			}
		})
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
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
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

