// Package invariants implements the stateless, zero-API-call Correctness Guard Rail
// evaluator for GKE Live Pod Migration (LPM).
package invariants

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// Mode controls runtime enforcement when an invariant violation is detected.
type Mode string

const (
	// ModeDisabled (default) turns off invariant evaluation completely.
	ModeDisabled Mode = "disabled"
	// ModeObserve records Prometheus metrics, emits Kubernetes Warning Events
	// (Reason: InvariantViolation), and logs structured ERROR entries without
	// interrupting active migrations.
	ModeObserve Mode = "observe"
	// ModeStrict (enabled in per-merge CI T1/T2 and nightly T3) performs all observe
	// actions and immediately transitions offending PodMigrationJobs to PhaseFailed.
	ModeStrict Mode = "strict"
)

// ParseMode validates and normalizes the --invariant-mode CLI flag.
func ParseMode(raw string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(raw))) {
	case "", ModeDisabled:
		return ModeDisabled, nil
	case ModeObserve:
		return ModeObserve, nil
	case ModeStrict:
		return ModeStrict, nil
	default:
		return "", fmt.Errorf("invalid invariant mode %q (expected disabled, observe, or strict)", raw)
	}
}

// ReconcileSnapshot holds the point-in-time, in-memory state evaluated by pure
// invariant predicates (I1-I9+) at the end of a reconcile cycle without issuing
// any extra Kubernetes API server requests.
type ReconcileSnapshot struct {
	Now        time.Time
	Reconciler string

	// Primary objects under reconciliation in the current step (any may be nil).
	PrimaryPMJ       *pmv1alpha1.PodMigrationJob
	PrimaryPod       *corev1.Pod
	PrimaryMigration *pmv1alpha1.PodMigration

	// Optional contextual slices populated from the informer cache for PMJ reconciles.
	NamespacePMJs []pmv1alpha1.PodMigrationJob
	NamespacePods []corev1.Pod

	// Explicit state signals captured during the reconcile step.
	PodListFailed                bool
	PMJListFailed                bool
	HasOrphanedTrigger           bool
	RestoreCrashSignatureMatched bool

	// PrimarySnapshotConditions holds the status.conditions of the active PodSnapshot
	// (when present in the informer cache) so cross-CRD invariants can inspect
	// Checkpoint / StorageReplicated / Ready sub-conditions without extra API calls.
	PrimarySnapshotConditions []metav1.Condition
}

// Violation represents a single detected violation of an invariant (I1-I9).
type Violation struct {
	InvariantID         string
	InvariantName       string
	Reason              string
	Message             string
	Namespace           string
	PMJName             string
	PodName             string
	ConsecutiveRequired int           // Optional per-violation consecutive samples required before strict abort (>0)
	DurationRequired    time.Duration // Optional per-violation minimum duration required before strict abort (>0)
}

// Rule is a pure, stateless predicate over a ReconcileSnapshot.
type Rule interface {
	ID() string
	Name() string
	Evaluate(s *ReconcileSnapshot) []Violation
}

// DebouncedRule is an optional interface implemented by Invariant Rules
// that require multiple consecutive reconcile samples and a minimum elapsed
// duration detecting a violation before escalating to strict mode abort.
type DebouncedRule interface {
	Rule
	ConsecutiveSamples() int
	MinimumDuration() time.Duration
}

type debouncedRule struct {
	Rule
	consecutive     int
	minimumDuration time.Duration
}

func (r debouncedRule) ConsecutiveSamples() int {
	return r.consecutive
}

func (r debouncedRule) MinimumDuration() time.Duration {
	return r.minimumDuration
}

// NewDebouncedRule wraps an existing Rule to require N consecutive reconcile samples
// and a minimum duration since first observation before strict mode triggers failure.
// If consecutive < 1, it defaults to 1. If minDuration < 0, it defaults to 0.
func NewDebouncedRule(r Rule, consecutive int, minDuration time.Duration) DebouncedRule {
	if consecutive < 1 {
		consecutive = 1
	}
	if minDuration < 0 {
		minDuration = 0
	}
	return debouncedRule{
		Rule:            r,
		consecutive:     consecutive,
		minimumDuration: minDuration,
	}
}

type funcRule struct {
	id   string
	name string
	fn   func(s *ReconcileSnapshot) []Violation
}

func (r funcRule) ID() string                                { return r.id }
func (r funcRule) Name() string                              { return r.name }
func (r funcRule) Evaluate(s *ReconcileSnapshot) []Violation { return r.fn(s) }
