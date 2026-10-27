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
	// Outcomes: succeeded | failed | timeout | budget_timeout | fallback | succeeded_without_restore | engine_not_registered | restore_lost
	// (budget_timeout: timed out waiting in Pending for the PodDisruptionBudget, with nothing checkpointed;
	// engine_not_registered: the job names a snapshot engine that the controller doesn't register;
	// restore_lost: a restored replacement's container restarted before the restore was verified)
	OutcomesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_outcomes_total",
		Help: "Total number of completed pod migration jobs by outcome.",
	}, []string{"outcome"})

	// RestoreVerdictsTotal counts restore verdicts on replacement pods (C6),
	// by engine and verdict, so a restore the engine confirmed can be told
	// apart from one that was inferred.
	// Verdicts: confirmed | inferred | cold_start | failed | lost | timeout
	RestoreVerdictsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_restore_verdicts_total",
		Help: "Total number of restore verdicts on replacement pods, by engine and verdict.",
	}, []string{"engine", "verdict"})

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

	// SpotPreemptionTriggeredTotal counts pods for which spot preemption migration was initiated.
	SpotPreemptionTriggeredTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "pod_migration_spot_preemption_triggered_total",
		Help: "Total number of pod migrations initiated due to spot VM preemption.",
	})

	// SpotPreemptionSkippedTotal counts pods skipped on spot preemption by reason.
	// Reasons: budget_exceeded | no_policy | unknown_memory | unfenced_engine
	SpotPreemptionSkippedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_spot_preemption_skipped_total",
		Help: "Total number of pods skipped for spot preemption migration by reason.",
	}, []string{"reason"})

	// BudgetChecksTotal counts the dry-run evictions that check a source's
	// disruption budget before its checkpoint (#42), by result.
	// Results: allowed | budget_exhausted | in_flight | check_error
	BudgetChecksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_budget_checks_total",
		Help: "Total number of disruption budget checks (dry-run evictions) before a checkpoint, by result.",
	}, []string{"result"})

	// FenceWebhookRequestsTotal counts fence webhook requests by action.
	// Actions: fenced | unchanged | not_fenced | skipped | decode_error
	FenceWebhookRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_fence_webhook_requests_total",
		Help: "Total number of pod status updates seen by the fence webhook, by action.",
	}, []string{"action"})

	// SourceFenceTotal counts the controller's fence actions on migration
	// sources (#131), by action.
	// Actions: fenced | unfenced | kept | refenced
	// (fenced: before the checkpoint; unfenced: a failed migration returned
	// the source to service; kept: a failed migration kept it fenced and
	// evicts it; refenced: the source stopped after it was unfenced)
	SourceFenceTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_source_fence_total",
		Help: "Total number of fence actions on migration sources, by action.",
	}, []string{"action"})

	// SourceFencedSeconds tracks how long a migration source was fenced, by
	// outcome.
	// Outcomes: migrated | unfenced
	// (migrated: until the migration moved on to restoring, with the source
	// gone; unfenced: until a failed migration returned it to service)
	SourceFencedSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pod_migration_source_fenced_seconds",
		Help:    "Time a migration source spent fenced, by outcome.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"outcome"})

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
		SpotPreemptionTriggeredTotal,
		SpotPreemptionSkippedTotal,
		BudgetChecksTotal,
		FenceWebhookRequestsTotal,
		SourceFenceTotal,
		SourceFencedSeconds,
		RestoreVerdictsTotal,
	)
}

// RecordSourceFence increments the source fence counter for the given action.
func RecordSourceFence(action string) {
	if action != "" {
		SourceFenceTotal.WithLabelValues(action).Inc()
	}
}

// RecordSourceFencedDuration records how long a source was fenced.
func RecordSourceFencedDuration(outcome string, durationSeconds float64) {
	if outcome != "" && durationSeconds >= 0 {
		SourceFencedSeconds.WithLabelValues(outcome).Observe(durationSeconds)
	}
}

// RecordRestoreVerdict increments the restore verdict counter for engine and
// verdict.
func RecordRestoreVerdict(engine, verdict string) {
	if verdict != "" {
		RestoreVerdictsTotal.WithLabelValues(engine, verdict).Inc()
	}
}

// RecordBudgetCheck increments the disruption budget check counter for the given result.
func RecordBudgetCheck(result string) {
	if result != "" {
		BudgetChecksTotal.WithLabelValues(result).Inc()
	}
}

// RecordFenceWebhookRequest increments the fence webhook request counter for the given action.
func RecordFenceWebhookRequest(action string) {
	if action != "" {
		FenceWebhookRequestsTotal.WithLabelValues(action).Inc()
	}
}

// RecordSpotPreemptionTriggered increments the spot preemption triggered counter.
func RecordSpotPreemptionTriggered() {
	SpotPreemptionTriggeredTotal.Inc()
}

// RecordSpotPreemptionSkipped increments the spot preemption skipped counter for the given reason.
func RecordSpotPreemptionSkipped(reason string) {
	if reason != "" {
		SpotPreemptionSkippedTotal.WithLabelValues(reason).Inc()
	}
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
		if reason == "RestoreLost" {
			return "restore_lost"
		}
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
