package util

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestFindUnassignedActivePMJ_BarePod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name      string
		podName   string
		existing  []runtime.Object
		expected  string
		expectErr bool
	}{
		{
			name:    "Bare pod, active PMJ, no assignment",
			podName: "my-bare-pod",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      FormatPMJName("my-bare-pod", "12345678"),
						Namespace: "default",
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "my-bare-pod",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
			},
			expected: FormatPMJName("my-bare-pod", "12345678"),
		},
		{
			name:    "Bare pod, active PMJ, already assigned to another pod",
			podName: "my-bare-pod",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      FormatPMJName("my-bare-pod", "12345678"),
						Namespace: "default",
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "my-bare-pod",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "my-bare-pod-terminating",
						Namespace: "default",
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
						Annotations: map[string]string{
							"pod-migration.gke.io/assigned-pmj": FormatPMJName("my-bare-pod", "12345678"),
						},
					},
				},
			},
			expected: "", // Should not return the PMJ because it's already assigned
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, "", "", "", "", "")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_DeploymentRevisionIsolation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name            string
		podName         string
		parentName      string
		parentKind      string
		podTemplateHash string
		existing        []runtime.Object
		expected        string
		expectErr       bool
	}{
		{
			name:            "Deployment pod matches PMJ with identical pod-template-hash",
			podName:         "deploy-pod-v1-abc",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-v1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "deploy-pod-v1-orig",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
			},
			expected: "pmj-deploy-pod-v1",
		},
		{
			name:            "Deployment pod with different pod-template-hash (rolling update new revision) does not match old revision PMJ",
			podName:         "deploy-pod-v2-xyz",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v2",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-v1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "deploy-pod-v1-orig",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
			},
			expected: "", // Revision isolation prevents hijacking
		},
		{
			name:            "Deployment pod with multiple PMJs selects correct PMJ matching its pod-template-hash",
			podName:         "deploy-pod-v2-xyz",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v2",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-v1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "deploy-pod-v1-orig",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-v2",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v2",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "deploy-pod-v2-orig",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
			},
			expected: "pmj-deploy-pod-v2",
		},
		{
			name:            "Deployment pod with matching hash PMJ already assigned returns empty",
			podName:         "deploy-pod-v1-new",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-v1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "deploy-pod-v1-orig",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deploy-pod-v1-assigned",
						Namespace: "default",
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
						Annotations: map[string]string{
							AnnotationAssignedPMJ: "pmj-deploy-pod-v1",
						},
					},
				},
			},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_BatchIndexedJobs(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name               string
		podName            string
		parentName         string
		parentKind         string
		jobCompletionIndex string
		existing           []runtime.Object
		expected           string
		expectErr          bool
	}{
		{
			name:               "Indexed Job pod index 0 matches PMJ for index 0",
			podName:            "indexed-job-0-abc",
			parentName:         "my-indexed-job",
			parentKind:         "Job",
			jobCompletionIndex: "0",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-idx0",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:         "my-indexed-job",
							LabelParentKind:         "Job",
							LabelJobCompletionIndex: "0",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "indexed-job-0-orig"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-idx1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:         "my-indexed-job",
							LabelParentKind:         "Job",
							LabelJobCompletionIndex: "1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "indexed-job-1-orig"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expected: "pmj-job-idx0",
		},
		{
			name:               "Indexed Job pod index 1 matches PMJ for index 1",
			podName:            "indexed-job-1-xyz",
			parentName:         "my-indexed-job",
			parentKind:         "Job",
			jobCompletionIndex: "1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-idx0",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:         "my-indexed-job",
							LabelParentKind:         "Job",
							LabelJobCompletionIndex: "0",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "indexed-job-0-orig"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-idx1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:         "my-indexed-job",
							LabelParentKind:         "Job",
							LabelJobCompletionIndex: "1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "indexed-job-1-orig"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expected: "pmj-job-idx1",
		},
		{
			name:               "Indexed Job pod index 2 does NOT match existing PMJs for index 0 or 1",
			podName:            "indexed-job-2-new",
			parentName:         "my-indexed-job",
			parentKind:         "Job",
			jobCompletionIndex: "2",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-idx0",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:         "my-indexed-job",
							LabelParentKind:         "Job",
							LabelJobCompletionIndex: "0",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "indexed-job-0-orig"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expected: "",
		},
		{
			name:               "Non-indexed parallel Job matches active PMJ without index constraint",
			podName:            "parallel-job-pod-2",
			parentName:         "my-parallel-job",
			parentKind:         "Job",
			jobCompletionIndex: "",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-parallel-job-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "my-parallel-job",
							LabelParentKind: "Job",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "parallel-job-pod-1"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expected: "pmj-parallel-job-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, tc.parentName, tc.parentKind, "", "", tc.jobCompletionIndex)
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_GenerationalWorkloadIsolation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name       string
		podName    string
		parentName string
		parentKind string
		parentUID  string
		existing   []runtime.Object
		expected   string
		expectErr  bool
	}{
		{
			name:       "StatefulSet pod matches PMJ with identical parent UID (same generation)",
			podName:    "redis-0",
			parentName: "redis",
			parentKind: "StatefulSet",
			parentUID:  "uid-generation-1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-redis-0-gen1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "redis",
							LabelParentKind: "StatefulSet",
							LabelParentUID:  "uid-generation-1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "redis-0"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
					},
				},
			},
			expected: "pmj-redis-0-gen1",
		},
		{
			name:       "Redeployed StatefulSet pod with new parent UID does NOT match PMJ from old deleted generation",
			podName:    "redis-0",
			parentName: "redis",
			parentKind: "StatefulSet",
			parentUID:  "uid-generation-2", // New generation
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-redis-0-gen1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "redis",
							LabelParentKind: "StatefulSet",
							LabelParentUID:  "uid-generation-1", // Old generation
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "redis-0"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
					},
				},
			},
			expected: "", // Stale state resurrection prevented!
		},
		{
			name:       "Deployment pod with new parent UID does NOT match PMJ from old deleted generation",
			podName:    "web-xyz",
			parentName: "web-deploy",
			parentKind: "Deployment",
			parentUID:  "uid-deploy-gen2",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-web-gen1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "web-deploy",
							LabelParentKind: "Deployment",
							LabelParentUID:  "uid-deploy-gen1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "web-abc"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
					},
				},
			},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_SingleUseConsumedGuard(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name       string
		podName    string
		parentName string
		parentKind string
		parentUID  string
		existing   []runtime.Object
		expected   string
		expectErr  bool
	}{
		{
			name:       "Unconsumed Restoring PMJ is eligible for adoption",
			podName:    "redis-0",
			parentName: "redis",
			parentKind: "StatefulSet",
			parentUID:  "uid-123",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-redis-0",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "redis",
							LabelParentKind: "StatefulSet",
							LabelParentUID:  "uid-123",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "redis-0"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase:    pmv1alpha1.PodMigrationJobPhaseRestoring,
						Consumed: false,
					},
				},
			},
			expected: "pmj-redis-0",
		},
		{
			name:       "Already consumed PMJ is NOT eligible for adoption (crash/re-spawn guard)",
			podName:    "redis-0",
			parentName: "redis",
			parentKind: "StatefulSet",
			parentUID:  "uid-123",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-redis-0",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "redis",
							LabelParentKind: "StatefulSet",
							LabelParentUID:  "uid-123",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{Name: "redis-0"},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
						Consumed:       true, // Already consumed by prior replacement pod
						RestoredPodUID: "prior-pod-uid",
					},
				},
			},
			expected: "", // Single-use consumption guard prevents double restore!
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "")
			if (err != nil) != tc.expectErr {
				t.Fatalf("expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_EvictingScaleUpIsolation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	tests := []struct {
		name            string
		podName         string
		parentName      string
		parentKind      string
		podTemplateHash string
		existing        []runtime.Object
		expected        string
	}{
		{
			name:            "Evicting PMJ with origin pod still alive does NOT match concurrent scale-up pod",
			podName:         "deploy-pod-scaleup-2",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "deploy-pod-1"},
						TargetPodUID: "origin-uid-111",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
					},
				},
				// Origin pod still exists (e.g. waiting for PDB budget or drain)
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "deploy-pod-1",
						Namespace: "default",
						UID:       "origin-uid-111",
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
					},
				},
			},
			expected: "", // Scale-up pod must NOT adopt the PMJ while origin pod is alive!
		},
		{
			name:            "Evicting PMJ with origin pod terminating (has DeletionTimestamp) matches replacement pod",
			podName:         "deploy-pod-replacement-1",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "deploy-pod-1"},
						TargetPodUID: "origin-uid-111",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
					},
				},
				// Origin pod is terminating in grace period (has DeletionTimestamp)
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "deploy-pod-1",
						Namespace:         "default",
						UID:               "origin-uid-111",
						DeletionTimestamp: &metav1.Time{Time: time.Now()},
						Finalizers:        []string{"kubernetes.io/test-finalizer"},
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
					},
				},
			},
			expected: "pmj-deploy-pod-1",
		},
		{
			name:            "Evicting PMJ with origin pod deleted matches replacement pod",
			podName:         "deploy-pod-replacement-1",
			parentName:      "my-deploy",
			parentKind:      "Deployment",
			podTemplateHash: "hash-v1",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-deploy-pod-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName:      "my-deploy",
							LabelParentKind:      "Deployment",
							LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "deploy-pod-1"},
						TargetPodUID: "origin-uid-111",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
					},
				},
				// Origin pod is gone (deleted)
			},
			expected: "pmj-deploy-pod-1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tc.existing...).Build()
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, c, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("expected: %q, got: %q", tc.expected, result)
			}
		})
	}
}

func TestResolveParentWorkload_StandaloneReplicaSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)

	namespace := "default"
	rsUID := "rs-uid-999"
	rsName := "standalone-rs"

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: namespace,
			UID:       types.UID(rsUID),
			// No owner reference to a Deployment -> standalone ReplicaSet
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "standalone-rs-pod",
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       rsName,
					UID:        types.UID(rsUID),
				},
			},
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, pod).Build()
	ctx := context.Background()

	parentName, parentKind, parentUID, err := ResolveParentWorkload(ctx, c, pod)
	if err != nil {
		t.Fatalf("ResolveParentWorkload failed: %v", err)
	}

	if parentName != rsName {
		t.Errorf("expected parentName %q, got %q", rsName, parentName)
	}
	if parentKind != "ReplicaSet" {
		t.Errorf("expected parentKind %q, got %q", "ReplicaSet", parentKind)
	}
	if parentUID != rsUID {
		t.Errorf("expected parentUID %q, got %q", rsUID, parentUID)
	}
}

func TestDistilledPodSpecDigest(t *testing.T) {
	baseSpec := &corev1.PodSpec{
		NodeName: "gke-node-01",
		NodeSelector: map[string]string{
			"cloud.google.com/gke-nodepool": "default-pool",
		},
		Tolerations: []corev1.Toleration{
			{
				Key:      "example-key",
				Operator: corev1.TolerationOpExists,
			},
		},
		SchedulingGates: []corev1.PodSchedulingGate{
			{
				Name: "gke.io/pod-migration-gate",
			},
		},
		InitContainers: []corev1.Container{
			{
				Name:  "init-setup",
				Image: "busybox:latest",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("100m"),
					},
				},
			},
		},
		Containers: []corev1.Container{
			{
				Name:  "app",
				Image: "redis:7.0",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1000m"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			},
		},
	}

	digest1, err := DistilledPodSpecDigest(baseSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}
	if len(digest1) != 64 {
		t.Fatalf("expected 64-char sha256 hex digest, got %q (len %d)", digest1, len(digest1))
	}

	// Determinism check: same spec produces identical digest
	digest2, err := DistilledPodSpecDigest(baseSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}
	if digest1 != digest2 {
		t.Errorf("expected deterministic digest, got %q vs %q", digest1, digest2)
	}

	// VPA mutated spec: altered CPU/memory requests and limits, empty nodeName/selector/tolerations/gates
	vpaSpec := &corev1.PodSpec{
		NodeName:        "",
		NodeSelector:    nil,
		Tolerations:     nil,
		SchedulingGates: nil,
		InitContainers: []corev1.Container{
			{
				Name:  "init-setup",
				Image: "busybox:latest",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU: resource.MustParse("250m"),
					},
				},
			},
		},
		Containers: []corev1.Container{
			{
				Name:  "app",
				Image: "redis:7.0",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1500m"),
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("2000m"),
						corev1.ResourceMemory: resource.MustParse("8Gi"),
					},
				},
			},
		},
	}

	vpaDigest, err := DistilledPodSpecDigest(vpaSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}
	if vpaDigest != digest1 {
		t.Errorf("expected VPA mutated spec to produce identical distilled digest %q, got %q", digest1, vpaDigest)
	}

	// Genuinely different spec (image changed) -> must produce different digest
	diffSpec := vpaSpec.DeepCopy()
	diffSpec.Containers[0].Image = "redis:7.2"
	diffDigest, err := DistilledPodSpecDigest(diffSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}
	if diffDigest == digest1 {
		t.Errorf("expected different image to produce different digest, got same %q", diffDigest)
	}

	// ServiceAccount projected volume random suffix normalization test
	saSpec1 := baseSpec.DeepCopy()
	saSpec1.Volumes = append(saSpec1.Volumes, corev1.Volume{
		Name: "kube-api-access-abc12",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{},
		},
	})
	saSpec1.Containers[0].VolumeMounts = append(saSpec1.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "kube-api-access-abc12",
		MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
		ReadOnly:  true,
	})

	saSpec2 := baseSpec.DeepCopy()
	saSpec2.Volumes = append(saSpec2.Volumes, corev1.Volume{
		Name: "kube-api-access-xyz89",
		VolumeSource: corev1.VolumeSource{
			Projected: &corev1.ProjectedVolumeSource{},
		},
	})
	saSpec2.Containers[0].VolumeMounts = append(saSpec2.Containers[0].VolumeMounts, corev1.VolumeMount{
		Name:      "kube-api-access-xyz89",
		MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
		ReadOnly:  true,
	})

	saDigest1, err := DistilledPodSpecDigest(saSpec1)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed for saSpec1: %v", err)
	}
	saDigest2, err := DistilledPodSpecDigest(saSpec2)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed for saSpec2: %v", err)
	}
	if saDigest1 != saDigest2 {
		t.Errorf("expected SA projected volume suffix to normalize to identical digest, got %q vs %q", saDigest1, saDigest2)
	}

	// Ephemeral debug container normalization test
	ephemSpec := baseSpec.DeepCopy()
	ephemSpec.EphemeralContainers = []corev1.EphemeralContainer{
		{
			EphemeralContainerCommon: corev1.EphemeralContainerCommon{
				Name:  "debugger",
				Image: "busybox:latest",
			},
		},
	}
	ephemDigest, err := DistilledPodSpecDigest(ephemSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed for ephemSpec: %v", err)
	}
	if ephemDigest != digest1 {
		t.Errorf("expected ephemeral debug containers to be cleared, got %q vs %q", ephemDigest, digest1)
	}

	// Nil spec returns empty string
	nilDigest, err := DistilledPodSpecDigest(nil)
	if err != nil || nilDigest != "" {
		t.Errorf("expected empty string and nil error for nil spec, got %q, %v", nilDigest, err)
	}
}

func TestFindUnassignedActivePMJ_VPADistilledSpecMatching(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	originSpec := &corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "web",
				Image: "nginx:1.25",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("256Mi"),
					},
				},
			},
		},
	}
	originDigest, err := DistilledPodSpecDigest(originSpec)
	if err != nil {
		t.Fatalf("failed to compute origin digest: %v", err)
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-deploy-origin",
			Namespace: "default",
			Labels: map[string]string{
				LabelParentName:          "web-deploy",
				LabelParentKind:          "Deployment",
				LabelParentUID:           "deploy-uid-111",
				LabelPodTemplateHash:     "hash-v1-original",
				LabelDistilledSpecDigest: originDigest[:63],
			},
			Annotations: map[string]string{
				AnnotationDistilledSpecDigest: originDigest,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-deploy-orig"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj).Build()
	ctx := context.Background()

	// 1. Same template hash matches directly
	got1, err := FindUnassignedActivePMJ(ctx, c, "default", "web-deploy-new", "web-deploy", "Deployment", "deploy-uid-111", "hash-v1-original", "")
	if err != nil || got1 != pmj.Name {
		t.Errorf("exact template hash match expected %q, got %q, err=%v", pmj.Name, got1, err)
	}

	// 2. VPA mutated replacement pod: pod-template-hash differs (hash-vpa-scaled), but distilledDigest matches
	got2, err := FindUnassignedActivePMJ(ctx, c, "default", "web-deploy-new", "web-deploy", "Deployment", "deploy-uid-111", "hash-vpa-scaled", "", originDigest)
	if err != nil || got2 != pmj.Name {
		t.Errorf("VPA mutated pod with matching distilledDigest expected %q, got %q, err=%v", pmj.Name, got2, err)
	}

	// 3. VPA mutated replacement pod WITHOUT passing distilledDigest: rejected (returns empty string)
	got3, err := FindUnassignedActivePMJ(ctx, c, "default", "web-deploy-new", "web-deploy", "Deployment", "deploy-uid-111", "hash-vpa-scaled", "")
	if err != nil || got3 != "" {
		t.Errorf("differing template hash without distilledDigest expected empty string, got %q, err=%v", got3, err)
	}

	// 4. Differing template hash with completely different distilledDigest (e.g. image rollout): rejected
	differentSpec := &corev1.PodSpec{
		Containers: []corev1.Container{
			{Name: "web", Image: "nginx:1.26"},
		},
	}
	diffDigest, _ := DistilledPodSpecDigest(differentSpec)
	got4, err := FindUnassignedActivePMJ(ctx, c, "default", "web-deploy-new", "web-deploy", "Deployment", "deploy-uid-111", "hash-vpa-scaled", "", diffDigest)
	if err != nil || got4 != "" {
		t.Errorf("differing template hash with mismatched distilledDigest expected empty string, got %q, err=%v", got4, err)
	}

	// 5. Candidate priority: exact template hash match takes precedence over distilled spec fallback match
	pmjExact := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-deploy-exact",
			Namespace: "default",
			Labels: map[string]string{
				LabelParentName:          "web-deploy",
				LabelParentKind:          "Deployment",
				LabelParentUID:           "deploy-uid-111",
				LabelPodTemplateHash:     "hash-target-revision",
				LabelDistilledSpecDigest: originDigest[:63],
			},
			Annotations: map[string]string{
				AnnotationDistilledSpecDigest: originDigest,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-deploy-exact"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}
	cPriority := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj, pmjExact).Build()
	got5, err := FindUnassignedActivePMJ(ctx, cPriority, "default", "web-deploy-new", "web-deploy", "Deployment", "deploy-uid-111", "hash-target-revision", "", originDigest)
	if err != nil || got5 != pmjExact.Name {
		t.Errorf("candidate priority expected exact match %q over distilled fallback %q, got %q, err=%v", pmjExact.Name, pmj.Name, got5, err)
	}
}
