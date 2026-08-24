package util

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
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
			name:       "Unconsumed Succeeded PMJ is eligible for adoption",
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
						Phase:    pmv1alpha1.PodMigrationJobPhaseSucceeded,
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
						Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
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
