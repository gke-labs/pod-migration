// Package metrics holds the Prometheus instrumentation for the pod-migration
// controller.  Collectors are registered with the controller-runtime registry
// in init(), so the manager's existing /metrics endpoint serves them with no
// additional wiring beyond importing this package.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// RestoreCrashFallbackTotal counts cold-start fallbacks triggered because
	// a replacement pod crashed with a recognised gVisor/OCI restore-failure
	// signature.  Counter, not Gauge: each fallback is a discrete destructive
	// action (the replacement pod is deleted) and the rate of those actions is
	// what operators alert on.
	RestoreCrashFallbackTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_migration_restore_crash_fallback_total",
		Help: "Total number of cold-start fallbacks triggered by a matched gVisor/OCI restore crash signature.",
	})

	// RestoreCrashUnmatchedTotal counts StartError/exit-128 container states on
	// a restoring pod whose message matched no known signature.  These take no
	// destructive action; a rising counter means the signature set has drifted
	// from what the runtime actually emits and needs extending.
	RestoreCrashUnmatchedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_migration_restore_crash_unmatched_total",
		Help: "Total number of StartError/128 restore-pod crashes with no matching restore-failure signature.",
	})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		RestoreCrashFallbackTotal,
		RestoreCrashUnmatchedTotal,
	)
}
