package controller

import (
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

var (
	pmjOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pod_migration_outcomes_total",
		Help: "Total number of completed pod migration jobs by outcome.",
	}, []string{"outcome"}) // succeeded|failed|timeout|fallback|succeeded_without_restore

	pmjPhaseSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pod_migration_phase_duration_seconds",
		Help:    "Duration of pod migration phases in seconds.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{"phase"})

	pmjActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "pod_migration_active",
		Help: "Number of currently active pod migrations.",
	})

	activeJobsMu sync.Mutex
	activeJobs   = make(map[string]struct{})
)

func init() {
	metrics.Registry.MustRegister(pmjOutcomes, pmjPhaseSeconds, pmjActive)
}

func markPMJActive(key string) {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	if _, ok := activeJobs[key]; !ok {
		activeJobs[key] = struct{}{}
		pmjActive.Set(float64(len(activeJobs)))
	}
}

func markPMJInactive(key string) {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	if _, ok := activeJobs[key]; ok {
		delete(activeJobs, key)
		pmjActive.Set(float64(len(activeJobs)))
	}
}

func recordPhaseDuration(phase string, durationSeconds float64) {
	if durationSeconds >= 0 {
		pmjPhaseSeconds.WithLabelValues(phase).Observe(durationSeconds)
	}
}

func recordOutcome(outcome string) {
	pmjOutcomes.WithLabelValues(outcome).Inc()
}

func outcomeFor(phase pmv1alpha1.PodMigrationJobPhase, reason string) string {
	switch phase {
	case pmv1alpha1.PodMigrationJobPhaseSucceeded:
		return "succeeded"
	case pmv1alpha1.PodMigrationJobPhaseFailed:
		if strings.Contains(strings.ToLower(reason), "timeout") {
			return "timeout"
		}
		return "failed"
	case pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore:
		if strings.Contains(strings.ToLower(reason), "timeout") {
			return "timeout"
		}
		if reason == "FallbackToColdStart" || strings.Contains(strings.ToLower(reason), "fallback") {
			return "fallback"
		}
		return "succeeded_without_restore"
	default:
		return strings.ToLower(string(phase))
	}
}

func resetActiveJobsForTest() {
	activeJobsMu.Lock()
	defer activeJobsMu.Unlock()
	activeJobs = make(map[string]struct{})
	pmjActive.Set(0)
}
