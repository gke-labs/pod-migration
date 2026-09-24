package util

import (
	"time"

	corev1 "k8s.io/api/core/v1"
)

const (
	// AnnotationMigrationTimeout allows workloads to override the default migration timeout.
	// Valid values are parseable duration strings (e.g. "15m", "30m", "1h").
	AnnotationMigrationTimeout = "pod-migration.gke.io/timeout"

	// DefaultMigrationTimeout is the baseline timeout for active migrations (Pending, Snapshotting, Evicting).
	DefaultMigrationTimeout = 10 * time.Minute

	// MinMigrationTimeout is the minimum allowable migration timeout.
	MinMigrationTimeout = 1 * time.Minute

	// MaxMigrationTimeout is the maximum allowable migration timeout ceiling.
	MaxMigrationTimeout = 2 * time.Hour

	// DefaultThroughputBytesPerSec is the baseline checkpoint upload throughput (50 MiB/s)
	// used for size-aware migration timeout estimation.
	DefaultThroughputBytesPerSec = int64(50 * 1024 * 1024)
)

// CalculatePodMemoryRequest sums container and persistent sidecar init container memory requests in bytes for a pod.
// Sequential init containers that run to completion before pod startup are not running at snapshot time.
func CalculatePodMemoryRequest(pod *corev1.Pod) int64 {
	if pod == nil {
		return 0
	}
	var totalBytes int64
	for _, c := range pod.Spec.Containers {
		if req := c.Resources.Requests.Memory(); req != nil {
			totalBytes += req.Value()
		}
	}
	for _, c := range pod.Spec.InitContainers {
		if c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			if req := c.Resources.Requests.Memory(); req != nil {
				totalBytes += req.Value()
			}
		}
	}
	return totalBytes
}

// CalculateMigrationTimeout computes the effective migration timeout based on explicit annotation,
// pod memory request scaling, and controller baseline timeout.
func CalculateMigrationTimeout(annotatedTimeout string, memBytes int64, baseTimeout time.Duration) time.Duration {
	if baseTimeout <= 0 {
		baseTimeout = DefaultMigrationTimeout
	}

	if annotatedTimeout != "" {
		if d, err := time.ParseDuration(annotatedTimeout); err == nil && d > 0 {
			if d < MinMigrationTimeout {
				return MinMigrationTimeout
			}
			if d > MaxMigrationTimeout {
				return MaxMigrationTimeout
			}
			return d
		}
	}

	if memBytes > 0 {
		additionalSec := memBytes / DefaultThroughputBytesPerSec
		calculated := baseTimeout + time.Duration(additionalSec)*time.Second
		if calculated > MaxMigrationTimeout {
			return MaxMigrationTimeout
		}
		return calculated
	}

	return baseTimeout
}
