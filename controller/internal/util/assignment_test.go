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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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
		{
			name:       "Snapshotting PMJ with Job-owned source pod Failed matches replacement pod",
			podName:    "job-pod-replacement-1",
			parentName: "my-job",
			parentKind: "Job",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-pod-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "my-job",
							LabelParentKind: "Job",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "job-pod-1"},
						TargetPodUID: "origin-job-uid",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "job-pod-1",
						Namespace: "default",
						UID:       "origin-job-uid",
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
					},
					Status: corev1.PodStatus{
						Phase:  corev1.PodFailed,
						Reason: "EvictedByMigration",
					},
				},
			},
			expected: "pmj-job-pod-1",
		},
		{
			name:       "Evicting PMJ with Job-owned source pod Failed matches replacement pod",
			podName:    "job-pod-replacement-1",
			parentName: "my-job",
			parentKind: "Job",
			existing: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "pmj-job-pod-1",
						Namespace: "default",
						Labels: map[string]string{
							LabelParentName: "my-job",
							LabelParentKind: "Job",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "job-pod-1"},
						TargetPodUID: "origin-job-uid",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "job-pod-1",
						Namespace: "default",
						UID:       "origin-job-uid",
						Labels: map[string]string{
							"pod-migration.gke.io/enabled": "true",
						},
					},
					Status: corev1.PodStatus{
						Phase:  corev1.PodFailed,
						Reason: "EvictedByMigration",
					},
				},
			},
			expected: "pmj-job-pod-1",
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
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
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

func TestResolveParentWorkload_OperatorOwnedPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	namespace := "default"
	clusterUID := "ray-uid-1234"
	clusterName := "my-raycluster"
	isController := true
	isNotController := false

	tests := []struct {
		name            string
		ownerReferences []metav1.OwnerReference
		expectedParent  string
		expectedKind    string
		expectedUID     string
	}{
		{
			name: "RayCluster controller owner",
			ownerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayCluster",
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
			expectedParent: clusterName,
			expectedKind:   "RayCluster",
			expectedUID:    clusterUID,
		},
		{
			name: "Non-controller owner followed by controller owner",
			ownerReferences: []metav1.OwnerReference{
				{
					APIVersion: "custom.io/v1",
					Kind:       "SomeGroup",
					Name:       "group-1",
					UID:        "group-uid-1",
					Controller: &isNotController,
				},
				{
					APIVersion: "ray.io/v1",
					Kind:       "RayCluster",
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
			expectedParent: clusterName,
			expectedKind:   "RayCluster",
			expectedUID:    clusterUID,
		},
		{
			name: "Bare pod with only non-controller owner",
			ownerReferences: []metav1.OwnerReference{
				{
					APIVersion: "custom.io/v1",
					Kind:       "SomeGroup",
					Name:       "group-1",
					UID:        "group-uid-1",
					Controller: &isNotController,
				},
			},
			expectedParent: "",
			expectedKind:   "",
			expectedUID:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "test-pod",
					Namespace:       namespace,
					OwnerReferences: tc.ownerReferences,
				},
			}
			cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
			parentName, parentKind, parentUID, err := ResolveParentWorkload(context.Background(), cl, pod)
			if err != nil {
				t.Fatalf("ResolveParentWorkload failed: %v", err)
			}
			if parentName != tc.expectedParent || parentKind != tc.expectedKind || parentUID != tc.expectedUID {
				t.Errorf("expected (%q, %q, %q), got (%q, %q, %q)",
					tc.expectedParent, tc.expectedKind, tc.expectedUID, parentName, parentKind, parentUID)
			}
		})
	}
}

func TestResolveOperatorReplicaKey(t *testing.T) {
	tests := []struct {
		name        string
		pod         *corev1.Pod
		parentKind  string
		expectedKey string
		expectedOK  bool
	}{
		{
			name: "RayCluster head pod with group and node-type",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/group":     "headgroup",
						"ray.io/node-type": "head",
					},
				},
			},
			parentKind:  "RayCluster",
			expectedKey: "ray.io/group=headgroup,ray.io/node-type=head",
			expectedOK:  true,
		},
		{
			name: "RayCluster worker pod with group only",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/group": "workergroup",
					},
				},
			},
			parentKind:  "RayCluster",
			expectedKey: "ray.io/group=workergroup",
			expectedOK:  true,
		},
		{
			name: "RayCluster missing group",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"ray.io/node-type": "head",
					},
				},
			},
			parentKind:  "RayCluster",
			expectedKey: "",
			expectedOK:  false,
		},
		{
			name: "Operator pod with pod-template-hash",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						appsv1.DefaultDeploymentUniqueLabelKey: "7b4c9f",
					},
				},
			},
			parentKind:  "CustomWorkload",
			expectedKey: "pod-template-hash=7b4c9f",
			expectedOK:  true,
		},
		{
			name: "Explicit replica identity annotation takes precedence",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						AnnotationReplicaIdentityKey: "custom-identity-0",
					},
				},
			},
			parentKind:  "CustomWorkload",
			expectedKey: "custom-identity-0",
			expectedOK:  true,
		},
		{
			name: "Stable-name operator pod without generateName",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:         "pytorch-worker-0",
					GenerateName: "",
				},
			},
			parentKind:  "PyTorchJob",
			expectedKey: "pod-name=pytorch-worker-0",
			expectedOK:  true,
		},
		{
			name: "Generated-name operator pod without keys returns false",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:         "custom-worker-xyz",
					GenerateName: "custom-worker-",
					Labels: map[string]string{
						"app": "foo",
					},
				},
			},
			parentKind:  "UnknownOperator",
			expectedKey: "",
			expectedOK:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotKey, gotOK := ResolveOperatorReplicaKey(tc.pod, tc.parentKind)
			if gotOK != tc.expectedOK || gotKey != tc.expectedKey {
				t.Errorf("ResolveOperatorReplicaKey() = (%q, %v), want (%q, %v)",
					gotKey, gotOK, tc.expectedKey, tc.expectedOK)
			}
		})
	}
}

func TestFindUnassignedActivePMJ_RayCluster_HeadPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	headReplicaKey := "ray.io/group=headgroup,ray.io/node-type=head"

	// Pre-existing PMJ for the head pod
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-head-origin",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: headReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-origin"},
			TargetPodUID: "origin-pod-uid-999",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cIndexed := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	cLive := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj).
		Build()

	ctx := context.Background()

	// 1. Indexed lookup for newly created replacement head pod
	got, err := FindUnassignedActivePMJ(ctx, cIndexed, namespace, "sample-ray-head-new-generated", clusterName, clusterKind, clusterUID, "", "", true, headReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ indexed failed: %v", err)
	}
	if got != pmj.Name {
		t.Fatalf("expected indexed assignment to %q, got %q", pmj.Name, got)
	}

	// 2. Unindexed live reader fallback for replacement head pod
	gotLive, errLive := FindUnassignedActivePMJ(ctx, cLive, namespace, "sample-ray-head-new-generated", clusterName, clusterKind, clusterUID, "", "", false, headReplicaKey)
	if errLive != nil {
		t.Fatalf("FindUnassignedActivePMJ live failed: %v", errLive)
	}
	if gotLive != pmj.Name {
		t.Fatalf("expected live fallback assignment to %q, got %q", pmj.Name, gotLive)
	}
}

func TestFindUnassignedActivePMJ_RayCluster_AmbiguousWorkerGroup_Refused(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	workerReplicaKey := "ray.io/group=workergroup,ray.io/node-type=worker"
	isController := true

	// PMJ for worker-1
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker-1",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: workerReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-1"},
			TargetPodUID: "worker-1-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Sibling worker-2 is alive and running in the cluster with the same replica key!
	siblingWorker2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample-ray-worker-2",
			Namespace: namespace,
			UID:       "worker-2-uid",
			Labels: map[string]string{
				"ray.io/group":     "workergroup",
				"ray.io/node-type": "worker",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj, siblingWorker2).
		Build()

	ctx := context.Background()

	// A replacement pod arrives. Because siblingWorker2 shares the same replica key, it is ambiguous!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "sample-ray-worker-new", clusterName, clusterKind, clusterUID, "", "", true, workerReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected assignment to be REFUSED due to ambiguous sibling, but got %q", got)
	}
}

func TestFindUnassignedActivePMJ_Operator_MultipleCandidatePMJs_Refused(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	workerReplicaKey := "ray.io/group=workergroup,ray.io/node-type=worker"

	// Two in-flight PMJs for the same parent with the exact same replica identity key!
	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker-1",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: workerReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-1"},
			TargetPodUID: "worker-1-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker-2",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: workerReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-2"},
			TargetPodUID: "worker-2-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj1, pmj2).
		Build()

	ctx := context.Background()

	// A replacement pod arrives. Because two active PMJs match the same replica key, it must refuse!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "sample-ray-worker-new", clusterName, clusterKind, clusterUID, "", "", true, workerReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected assignment to be REFUSED due to multiple candidate PMJs, but got %q", got)
	}
}

func TestFindUnassignedActivePMJ_IncarnationProtection_Operator(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	oldClusterUID := "ray-uid-old"
	newClusterUID := "ray-uid-new"
	headReplicaKey := "ray.io/group=headgroup,ray.io/node-type=head"

	// PMJ from deleted previous incarnation of RayCluster
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-old-incarnation",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  oldClusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: headReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head"},
			TargetPodUID: "old-pod-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	ctx := context.Background()

	// Pod created under new RayCluster UID must NOT match the old PMJ!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "sample-ray-head-new", clusterName, clusterKind, newClusterUID, "", "", true, headReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected old incarnation PMJ to be rejected, but matched %q", got)
	}
}

func TestFindUnassignedActivePMJ_RayCluster_DifferentReplicaKey_Rejection(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	workerReplicaKey := "ray.io/group=workergroup,ray.io/node-type=worker"
	headReplicaKey := "ray.io/group=headgroup,ray.io/node-type=head"

	// Active PMJ under RayCluster is for a worker pod
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: workerReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-0"},
			TargetPodUID: "worker-0-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	ctx := context.Background()

	// A replacement head pod arrives with headReplicaKey -> must NOT match the worker's PMJ!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "sample-ray-head-new", clusterName, clusterKind, clusterUID, "", "", true, headReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected PMJ with different replica key to be rejected, but matched %q", got)
	}
}

func TestFindUnassignedActivePMJ_InFlightUpgradeFallback_StableNameOperatorPod(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "pytorch-worker-0"
	parentName := "pytorch-job-1"
	parentKind := "PyTorchJob"
	parentUID := "pytorch-uid-111"
	replicaKey := "pod-name=" + podName

	// PMJ created by pre-upgrade controller: bare-pod format (empty parent labels, indexed under <podName>/Pod)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-pytorch-worker-0-preupgrade",
			Namespace: namespace,
			Labels:    map[string]string{
				// Empty parent labels, as created by pre-upgrade controller
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "old-worker-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cIndexed := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	cLive := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj).
		Build()

	ctx := context.Background()

	// 1. Indexed lookup: replacement pod with parent PyTorchJob should fall back to bare-pod PMJ
	got, err := FindUnassignedActivePMJ(ctx, cIndexed, namespace, podName, parentName, parentKind, parentUID, "", "", true, replicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ indexed failed: %v", err)
	}
	if got != pmj.Name {
		t.Fatalf("expected indexed in-flight upgrade fallback to match %q, got %q", pmj.Name, got)
	}

	// 2. Unindexed live reader fallback: replacement pod should also match
	gotLive, errLive := FindUnassignedActivePMJ(ctx, cLive, namespace, podName, parentName, parentKind, parentUID, "", "", false, replicaKey)
	if errLive != nil {
		t.Fatalf("FindUnassignedActivePMJ live failed: %v", errLive)
	}
	if gotLive != pmj.Name {
		t.Fatalf("expected live fallback to match %q, got %q", pmj.Name, gotLive)
	}
}

func TestFindUnassignedActivePMJ_Unindexed_LiveSiblingAmbiguity(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	workerReplicaKey := "ray.io/group=workergroup,ray.io/node-type=worker"
	isController := true

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker-1",
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: workerReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-1"},
			TargetPodUID: "worker-1-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Sibling worker-2 is running with the same replica key
	siblingWorker2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "sample-ray-worker-2",
			Namespace: namespace,
			UID:       "worker-2-uid",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				"ray.io/group":                 "workergroup",
				"ray.io/node-type":             "worker",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj, siblingWorker2).
		Build()

	ctx := context.Background()

	// Calling FindUnassignedActivePMJ with indexed=false when an ambiguous sibling exists must refuse!
	got, err := FindUnassignedActivePMJ(ctx, cl, namespace, "sample-ray-worker-new", clusterName, clusterKind, clusterUID, "", "", false, workerReplicaKey)
	if err != nil {
		t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
	}
	if got != "" {
		t.Fatalf("expected unindexed assignment to be REFUSED due to ambiguous live sibling, got %q", got)
	}
}

func TestResolveCollision_OperatorOwned_LoserIdentified(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	headReplicaKey := "ray.io/group=headgroup,ray.io/node-type=head"
	isController := true
	pmjName := "pmj-sample-ray-head"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pmjName,
			Namespace: namespace,
			Labels: map[string]string{
				LabelParentName: clusterName,
				LabelParentKind: clusterKind,
				LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				AnnotationReplicaIdentityKey: headReplicaKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-origin"},
			TargetPodUID: "head-origin-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	now := time.Now()
	// Pod 1 was created earlier (winner)
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sample-ray-head-repl-1",
			Namespace:         namespace,
			UID:               "pod1-uid",
			CreationTimestamp: metav1.NewTime(now.Add(-10 * time.Second)),
			Labels: map[string]string{
				"ray.io/group":     "headgroup",
				"ray.io/node-type": "head",
			},
			Annotations: map[string]string{
				AnnotationAssignedPMJ: pmjName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
	}

	// Pod 2 was created later (loser)
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sample-ray-head-repl-2",
			Namespace:         namespace,
			UID:               "pod2-uid",
			CreationTimestamp: metav1.NewTime(now.Add(-5 * time.Second)),
			Labels: map[string]string{
				"ray.io/group":     "headgroup",
				"ray.io/node-type": "head",
			},
			Annotations: map[string]string{
				AnnotationAssignedPMJ: pmjName,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndexKey, PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndexKey, PodAssignedPMJIndexValue).
		WithObjects(pmj, pod1, pod2).
		Build()

	ctx := context.Background()

	// 1. Resolve collision for winner (pod1) -> unchanged
	correctedWinner, changedWinner, errWinner := ResolveCollision(ctx, cl, pod1, pmjName, clusterName, clusterKind, clusterUID)
	if errWinner != nil {
		t.Fatalf("ResolveCollision for winner failed: %v", errWinner)
	}
	if changedWinner {
		t.Errorf("expected winner to remain unchanged, but changed was true")
	}
	if correctedWinner != pmjName {
		t.Errorf("expected winner to keep PMJ %q, got %q", pmjName, correctedWinner)
	}

	// 2. Resolve collision for loser (pod2) -> loser identified, cleared (changed=true, corrected="")
	correctedLoser, changedLoser, errLoser := ResolveCollision(ctx, cl, pod2, pmjName, clusterName, clusterKind, clusterUID)
	if errLoser != nil {
		t.Fatalf("ResolveCollision for loser failed: %v", errLoser)
	}
	if !changedLoser {
		t.Errorf("expected loser changed=true")
	}
	if correctedLoser != "" {
		t.Errorf("expected loser to have PMJ assignment cleared (empty), got %q", correctedLoser)
	}
}
