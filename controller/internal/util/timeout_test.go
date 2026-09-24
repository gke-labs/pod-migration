package util

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestCalculatePodMemoryRequest(t *testing.T) {
	if got := CalculatePodMemoryRequest(nil); got != 0 {
		t.Errorf("Expected 0 for nil pod, got %d", got)
	}

	podNoRequests := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "c1"},
			},
		},
	}
	if got := CalculatePodMemoryRequest(podNoRequests); got != 0 {
		t.Errorf("Expected 0 for pod without memory requests, got %d", got)
	}

	alwaysRestart := corev1.ContainerRestartPolicyAlways
	podWithRequests := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "redis"},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name: "init-sysctl-sequential",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("512Mi"),
						},
					},
				},
				{
					Name:          "init-sidecar",
					RestartPolicy: &alwaysRestart,
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("128Mi"),
						},
					},
				},
			},
			Containers: []corev1.Container{
				{
					Name: "redis-app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("16Gi"),
						},
					},
				},
				{
					Name: "sidecar-metrics",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceMemory: resource.MustParse("256Mi"),
						},
					},
				},
			},
		},
	}

	q16Gi := resource.MustParse("16Gi")
	q128Mi := resource.MustParse("128Mi")
	q256Mi := resource.MustParse("256Mi")

	// init-sysctl-sequential is excluded because it exits before pod runs; init-sidecar (Always) is included
	expectedBytes := q16Gi.Value() + q128Mi.Value() + q256Mi.Value()

	if got := CalculatePodMemoryRequest(podWithRequests); got != expectedBytes {
		t.Errorf("Expected %d bytes, got %d", expectedBytes, got)
	}
}

func TestCalculateMigrationTimeout(t *testing.T) {
	q16Gi := resource.MustParse("16Gi")
	q32Gi := resource.MustParse("32Gi")
	q40Gi := resource.MustParse("40Gi")
	q500Gi := resource.MustParse("500Gi")

	tests := []struct {
		name             string
		annotatedTimeout string
		memBytes         int64
		baseTimeout      time.Duration
		expected         time.Duration
	}{
		{
			name:             "Defaults with no inputs",
			annotatedTimeout: "",
			memBytes:         0,
			baseTimeout:      0,
			expected:         DefaultMigrationTimeout,
		},
		{
			name:             "Custom base timeout preserved when no annotation or memory request",
			annotatedTimeout: "",
			memBytes:         0,
			baseTimeout:      15 * time.Minute,
			expected:         15 * time.Minute,
		},
		{
			name:             "Annotation overrides base and memory",
			annotatedTimeout: "25m",
			memBytes:         q32Gi.Value(),
			baseTimeout:      10 * time.Minute,
			expected:         25 * time.Minute,
		},
		{
			name:             "Annotation below minimum is clamped to MinMigrationTimeout",
			annotatedTimeout: "10s",
			memBytes:         0,
			baseTimeout:      10 * time.Minute,
			expected:         MinMigrationTimeout,
		},
		{
			name:             "Annotation above maximum is clamped to MaxMigrationTimeout",
			annotatedTimeout: "5h",
			memBytes:         0,
			baseTimeout:      10 * time.Minute,
			expected:         MaxMigrationTimeout,
		},
		{
			name:             "Invalid annotation falls back to memory scaling",
			annotatedTimeout: "not-a-duration",
			memBytes:         q16Gi.Value(), // 16Gi / 50MiB/s = 327s (~5m27s)
			baseTimeout:      10 * time.Minute,
			expected:         10*time.Minute + time.Duration(q16Gi.Value()/DefaultThroughputBytesPerSec)*time.Second,
		},
		{
			name:             "Memory request scales deadline beyond base timeout",
			annotatedTimeout: "",
			memBytes:         q40Gi.Value(), // 40Gi / 50MiB/s = 819s (~13m39s)
			baseTimeout:      10 * time.Minute,
			expected:         10*time.Minute + time.Duration(q40Gi.Value()/DefaultThroughputBytesPerSec)*time.Second,
		},
		{
			name:             "Extremely large memory request is capped at MaxMigrationTimeout",
			annotatedTimeout: "",
			memBytes:         q500Gi.Value(),
			baseTimeout:      10 * time.Minute,
			expected:         MaxMigrationTimeout,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := CalculateMigrationTimeout(tc.annotatedTimeout, tc.memBytes, tc.baseTimeout)
			if got != tc.expected {
				t.Errorf("CalculateMigrationTimeout(%q, %d, %v) = %v; want %v",
					tc.annotatedTimeout, tc.memBytes, tc.baseTimeout, got, tc.expected)
			}
		})
	}
}
