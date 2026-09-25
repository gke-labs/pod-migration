package util

import (
	"context"
	"testing"
	"time"

	"fmt"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func newTestClientIndexed(scheme *runtime.Scheme, objs ...runtime.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithRuntimeObjects(objs...).
		Build()
}

func newTestClientLive(scheme *runtime.Scheme, objs ...runtime.Object) client.WithWatch {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithRuntimeObjects(objs...).
		Build()
}

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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, "", "", "", "", "", true)
			if (err != nil) != tc.expectErr {
				t.Fatalf("indexed path expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, "", "", "", "", "", false)
			if (errLive != nil) != tc.expectErr {
				t.Fatalf("live path expected error: %v, got: %v", tc.expectErr, errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "", true)
			if (err != nil) != tc.expectErr {
				t.Fatalf("indexed path expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "", false)
			if (errLive != nil) != tc.expectErr {
				t.Fatalf("live path expected error: %v, got: %v", tc.expectErr, errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, tc.parentName, tc.parentKind, "", "", tc.jobCompletionIndex, true)
			if (err != nil) != tc.expectErr {
				t.Fatalf("indexed path expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, tc.parentName, tc.parentKind, "", "", tc.jobCompletionIndex, false)
			if (errLive != nil) != tc.expectErr {
				t.Fatalf("live path expected error: %v, got: %v", tc.expectErr, errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "", true)
			if (err != nil) != tc.expectErr {
				t.Fatalf("indexed path expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "", false)
			if (errLive != nil) != tc.expectErr {
				t.Fatalf("live path expected error: %v, got: %v", tc.expectErr, errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "", true)
			if (err != nil) != tc.expectErr {
				t.Fatalf("indexed path expected error: %v, got: %v", tc.expectErr, err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, tc.parentName, tc.parentKind, tc.parentUID, "", "", false)
			if (errLive != nil) != tc.expectErr {
				t.Fatalf("live path expected error: %v, got: %v", tc.expectErr, errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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
			cIndexed := newTestClientIndexed(scheme, tc.existing...)
			cLive := newTestClientLive(scheme, tc.existing...)
			ctx := context.Background()

			result, err := FindUnassignedActivePMJ(ctx, cIndexed, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "", true)
			if err != nil {
				t.Fatalf("indexed path unexpected error: %v", err)
			}
			if result != tc.expected {
				t.Errorf("indexed path expected: %q, got: %q", tc.expected, result)
			}

			resultLive, errLive := FindUnassignedActivePMJ(ctx, cLive, "default", tc.podName, tc.parentName, tc.parentKind, "", tc.podTemplateHash, "", false)
			if errLive != nil {
				t.Fatalf("live path unexpected error: %v", errLive)
			}
			if resultLive != tc.expected {
				t.Errorf("live path expected: %q, got: %q", tc.expected, resultLive)
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

func TestFormatParentKey(t *testing.T) {
	tests := []struct {
		name       string
		parentName string
		parentKind string
		podName    string
		expected   string
	}{
		{
			name:       "Deployment parent",
			parentName: "web-deploy",
			parentKind: "Deployment",
			podName:    "web-deploy-xxx",
			expected:   "web-deploy/Deployment",
		},
		{
			name:       "StatefulSet parent",
			parentName: "db-sts",
			parentKind: "StatefulSet",
			podName:    "db-sts-0",
			expected:   "db-sts/StatefulSet",
		},
		{
			name:       "Job parent",
			parentName: "batch-job",
			parentKind: "Job",
			podName:    "batch-job-1",
			expected:   "batch-job/Job",
		},
		{
			name:       "Bare pod (empty parent)",
			parentName: "",
			parentKind: "",
			podName:    "my-bare-pod",
			expected:   "my-bare-pod/Pod",
		},
		{
			name:       "All empty",
			parentName: "",
			parentKind: "",
			podName:    "",
			expected:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := FormatParentKey(tt.parentName, tt.parentKind, tt.podName)
			if got != tt.expected {
				t.Errorf("FormatParentKey(%q, %q, %q) = %q, expected %q", tt.parentName, tt.parentKind, tt.podName, got, tt.expected)
			}
		})
	}
}

func TestPMJParentKeyIndexValue_Extraction(t *testing.T) {
	// Workload PMJ
	workloadPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pmj-w",
			Labels: map[string]string{
				LabelParentName: "deploy-1",
				LabelParentKind: "Deployment",
			},
		},
	}
	keys := PMJParentKeyIndexValue(workloadPMJ)
	if len(keys) != 1 || keys[0] != "deploy-1/Deployment" {
		t.Errorf("expected [deploy-1/Deployment], got %v", keys)
	}

	// Bare pod PMJ
	barePMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pmj-b",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "bare-1"},
		},
	}
	bareKeys := PMJParentKeyIndexValue(barePMJ)
	if len(bareKeys) != 1 || bareKeys[0] != "bare-1/Pod" {
		t.Errorf("expected [bare-1/Pod], got %v", bareKeys)
	}

	// Nil / empty checks
	if keys := PMJParentKeyIndexValue(nil); keys != nil {
		t.Errorf("expected nil for nil job, got %v", keys)
	}
	emptyPMJ := &pmv1alpha1.PodMigrationJob{}
	if keys := PMJParentKeyIndexValue(emptyPMJ); keys != nil {
		t.Errorf("expected nil for empty job, got %v", keys)
	}
}

func TestFindUnassignedActivePMJ_IndexedMultiWorkload(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "prod"

	// Setup 3 workloads in the same namespace:
	// 1. Deployment "frontend" (hash-v1) with active PMJ "pmj-frontend-1"
	// 2. Deployment "backend" (hash-b1) with active PMJ "pmj-backend-1" already assigned to a pod
	// 3. StatefulSet "database" with active PMJ "pmj-database-0"
	// 4. Bare pod "standalone" with active PMJ "pmj-standalone"

	frontendPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-frontend-1",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName:      "frontend",
				LabelParentKind:      "Deployment",
				LabelPodTemplateHash: "hash-v1",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "frontend-origin-1"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	backendPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-backend-1",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName:      "backend",
				LabelParentKind:      "Deployment",
				LabelPodTemplateHash: "hash-b1",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "backend-origin-1"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	backendAssignedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "backend-replacement-1",
			Namespace: namespace,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				AnnotationAssignedPMJ: "pmj-backend-1",
			},
		},
	}

	databasePMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-database-0",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: "database",
				LabelParentKind: "StatefulSet",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "database-0"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	// Database origin pod is terminating
	databaseOriginPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "database-0",
			Namespace:         namespace,
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Finalizers:        []string{"test-finalizer"},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
	}

	barePMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-standalone",
			Namespace: namespace,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "standalone"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	// Build indexed client
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(frontendPMJ, backendPMJ, backendAssignedPod, databasePMJ, databaseOriginPod, barePMJ).
		Build()

	ctx := context.Background()

	// 1. Frontend query should find pmj-frontend-1
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "frontend-replacement-1", "frontend", "Deployment", "", "hash-v1", "", true)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ for frontend failed: %v", err)
	}
	if got != "pmj-frontend-1" {
		t.Errorf("expected pmj-frontend-1, got %q", got)
	}

	// 2. Backend query should return "" because pmj-backend-1 is already assigned to backend-replacement-1
	gotBackend, err := FindUnassignedActivePMJ(ctx, cl, namespace, "backend-replacement-2", "backend", "Deployment", "", "hash-b1", "", true)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ for backend failed: %v", err)
	}
	if gotBackend != "" {
		t.Errorf("expected empty string for backend (already assigned), got %q", gotBackend)
	}

	// 3. Database query for database-0 should match pmj-database-0
	gotDB, err := FindUnassignedActivePMJ(ctx, cl, namespace, "database-0", "database", "StatefulSet", "", "", "", true)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ for database-0 failed: %v", err)
	}
	if gotDB != "pmj-database-0" {
		t.Errorf("expected pmj-database-0, got %q", gotDB)
	}

	// 4. Database query for database-1 should NOT match pmj-database-0 (wrong pod name)
	gotDB1, err := FindUnassignedActivePMJ(ctx, cl, namespace, "database-1", "database", "StatefulSet", "", "", "", true)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ for database-1 failed: %v", err)
	}
	if gotDB1 != "" {
		t.Errorf("expected empty string for database-1, got %q", gotDB1)
	}

	// 5. Bare pod query for "standalone" should match pmj-standalone
	gotBare, err := FindUnassignedActivePMJ(ctx, cl, namespace, "standalone", "", "", "", "", "", true)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ for standalone failed: %v", err)
	}
	if gotBare != "pmj-standalone" {
		t.Errorf("expected pmj-standalone, got %q", gotBare)
	}

	// 6. Test unindexed fallback produces identical results
	unindexedClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(frontendPMJ, backendPMJ, backendAssignedPod, databasePMJ, databaseOriginPod, barePMJ).
		Build()

	gotFallback, err := FindUnassignedActivePMJ(ctx, unindexedClient, namespace, "frontend-replacement-1", "frontend", "Deployment", "", "hash-v1", "", false)
	if err != nil {
		t.Fatalf("unindexed fallback for frontend failed: %v", err)
	}
	if gotFallback != "pmj-frontend-1" {
		t.Errorf("expected pmj-frontend-1 on unindexed fallback, got %q", gotFallback)
	}
}

func TestFindUnassignedActivePMJ_TerminatingClaimantCountsAsAssigned(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-terminating-claimant"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pmjName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: "web",
				LabelParentKind: "Deployment",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-origin-pod"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Pod A is assigned to pmjName and is terminating (DeletionTimestamp != nil)
	terminatingPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-replacement-a",
			Namespace:         namespace,
			DeletionTimestamp: &metav1.Time{Time: time.Now()},
			Finalizers:        []string{"test-finalizer"},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				AnnotationAssignedPMJ: pmjName,
			},
		},
	}

	cIndexed := newTestClientIndexed(scheme, pmj, terminatingPod)
	cLive := newTestClientLive(scheme, pmj, terminatingPod)
	ctx := context.Background()

	// New pod B arrives for the same Deployment
	gotIndexed, err := FindUnassignedActivePMJ(ctx, cIndexed, namespace, "web-replacement-b", "web", "Deployment", "", "", "", true)
	if err != nil {
		t.Fatalf("indexed lookup failed: %v", err)
	}
	if gotIndexed != "" {
		t.Errorf("expected empty string (PMJ already assigned to terminating claimant), got %q", gotIndexed)
	}

	gotLive, err := FindUnassignedActivePMJ(ctx, cLive, namespace, "web-replacement-b", "web", "Deployment", "", "", "", false)
	if err != nil {
		t.Fatalf("live lookup failed: %v", err)
	}
	if gotLive != "" {
		t.Errorf("expected empty string on live fallback (PMJ already assigned to terminating claimant), got %q", gotLive)
	}
}

func TestFindUnassignedActivePMJ_OriginPodTransientReadError_SkipsCandidate(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	pmjName := "pmj-evicting-error"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pmjName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: "web",
				LabelParentKind: "Deployment",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-origin-pod"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	// Intercept Get on Pod to simulate transient apiserver error
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Pod); ok && key.Name == "web-origin-pod" {
					return fmt.Errorf("simulated 500 transient apiserver read error")
				}
				return client.Get(ctx, key, obj, opts...)
			},
		}).
		Build()

	ctx := context.Background()
	// Should skip the candidate on transient read error rather than failing admission!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "web-replacement-1", "web", "Deployment", "", "", "", true)
	if err != nil {
		t.Fatalf("expected transient error to be skipped, got error: %v", err)
	}
	if got != "" {
		t.Errorf("expected candidate to be skipped, got %q", got)
	}
}
