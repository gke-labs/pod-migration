// Package metrics holds the Prometheus instrumentation for the pod-migration
// controller.  Collectors are registered with the controller-runtime registry
// in init(), so the manager's existing /metrics endpoint serves them with no
// additional wiring beyond importing this package.
package metrics

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
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

	// OutcomesTotal counts completed pod migration jobs by outcome label.
	// Outcomes: succeeded | failed | timeout | fallback | succeeded_without_restore
	OutcomesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_outcomes_total",
		Help: "Total number of completed pod migration jobs by outcome.",
	}, []string{"outcome"})

	// PhaseDurationSeconds tracks duration of pod migration phases in seconds.
	// Phases: pending | snapshotting | evicting | restoring
	PhaseDurationSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pod_migration_phase_duration_seconds",
		Help:    "Duration of pod migration phases in seconds.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"phase"})

	// ActiveMigrations tracks the number of currently active pod migrations.
	ActiveMigrations = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pod_migration_active",
		Help: "Number of currently active pod migrations.",
	})

	// InvariantViolationsTotal counts detected correctness invariant violations by invariant ID (I1-I9).
	InvariantViolationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_invariant_violations_total",
		Help: "Total number of detected correctness invariant violations by invariant ID (I1-I9).",
	}, []string{"invariant"})

	activeJobsMu sync.Mutex
	activeJobs   = make(map[string]struct{})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		RestoreCrashFallbackTotal,
		RestoreCrashUnmatchedTotal,
		OutcomesTotal,
		PhaseDurationSeconds,
		ActiveMigrations,
		InvariantViolationsTotal,
	)
}

// RecordInvariantViolation increments the invariant violation counter for the given invariant ID (e.g. "I1").
func RecordInvariantViolation(invariant string) {
	if invariant != "" {
		InvariantViolationsTotal.WithLabelValues(invariant).Inc()
	}
}

// MarkPMJActive adds a PMJ key to the active tracking set and updates ActiveMigrations.
func MarkPMJActive(key string) {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	if _, ok := activeJobs[key]; !ok {
		activeJobs[key] = struct{}{}
		ActiveMigrations.Set(float64(len(activeJobs)))
	}
}

// MarkPMJInactive removes a PMJ key from the active tracking set and updates ActiveMigrations.
func MarkPMJInactive(key string) {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	if _, ok := activeJobs[key]; ok {
		delete(activeJobs, key)
		ActiveMigrations.Set(float64(len(activeJobs)))
	}
}

// RecordPhaseDuration records the observed duration for a given migration phase.
func RecordPhaseDuration(phase string, durationSeconds float64) {
	if durationSeconds >= 0 {
		PhaseDurationSeconds.WithLabelValues(phase).Observe(durationSeconds)
	}
}

// RecordOutcome increments the counter for a completed migration outcome.
func RecordOutcome(outcome string) {
	OutcomesTotal.WithLabelValues(outcome).Inc()
}

// OutcomeFor derives the Prometheus outcome label for a terminal phase and reason.
func OutcomeFor(phase pmv1alpha1.PodMigrationJobPhase, reason string) string {
	switch phase {
	case pmv1alpha1.PodMigrationJobPhaseSucceeded:
		return "succeeded"
	case pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore:
		if strings.Contains(strings.ToLower(reason), "timeout") {
			return "timeout"
		}
		if reason == "FallbackToColdStart" || strings.Contains(strings.ToLower(reason), "fallback") {
			return "fallback"
		}
		return "succeeded_without_restore"
	default:
		if strings.Contains(strings.ToLower(reason), "timeout") {
			return "timeout"
		}
		return "failed"
	}
}

// ResetActiveJobsForTest resets active migration tracking state for unit tests.
func ResetActiveJobsForTest() {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	activeJobs = make(map[string]struct{})
	ActiveMigrations.Set(0)
}
