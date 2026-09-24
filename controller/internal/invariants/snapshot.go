// Package invariants implements the stateless, zero-API-call Correctness Guard Rail
// evaluator for GKE Live Pod Migration (LPM).
package invariants

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// Mode controls runtime enforcement when an invariant violation is detected.
type Mode string

const (
	// ModeDisabled turns off invariant evaluation completely.
	ModeDisabled Mode = "disabled"
	// ModeObserve (default in production) records Prometheus metrics, emits Kubernetes
	// Warning Events (Reason: InvariantViolation), and logs structured ERROR entries
	// without interrupting active migrations.
	ModeObserve Mode = "observe"
	// ModeStrict (enabled in per-merge CI T1/T2 and nightly T3) performs all observe
	// actions and immediately transitions offending PodMigrationJobs to PhaseFailed.
	ModeStrict Mode = "strict"
)

// ParseMode validates and normalizes the --invariant-mode CLI flag.
func ParseMode(raw string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(raw))) {
	case "", ModeObserve:
		return ModeObserve, nil
	case ModeDisabled:
		return ModeDisabled, nil
	case ModeStrict:
		return ModeStrict, nil
	default:
		return "", fmt.Errorf("invalid invariant mode %q (expected disabled, observe, or strict)", raw)
	}
}

// ReconcileSnapshot holds the point-in-time, in-memory state evaluated by pure
// invariant predicates (I1-I9) at the end of a reconcile cycle without issuing
// any extra Kubernetes API server requests.
type ReconcileSnapshot struct {
	Now        time.Time
	Reconciler string

	// Primary objects under reconciliation in the current step (any may be nil).
	PrimaryPMJ       *pmv1alpha1.PodMigrationJob
	PrimaryPod       *corev1.Pod
	PrimaryMigration *pmv1alpha1.PodMigration

	// Optional contextual slices populated when already available in memory or in tests.
	NamespacePMJs []pmv1alpha1.PodMigrationJob
	NamespacePods []corev1.Pod

	// Explicit state signals captured during the reconcile step.
	HasOrphanedTrigger           bool
	UsedBareDeleteBeforeDeadline bool
	RestoreCrashSignatureMatched bool
}

// Violation represents a single detected violation of an invariant (I1-I9).
type Violation struct {
	InvariantID   string
	InvariantName string
	Reason        string
	Message       string
	Namespace     string
	PMJName       string
	PodName       string
}

// Rule is a pure, stateless predicate over a ReconcileSnapshot.
type Rule interface {
	ID() string
	Name() string
	Evaluate(s *ReconcileSnapshot) []Violation
}

type funcRule struct {
	id   string
	name string
	fn   func(s *ReconcileSnapshot) []Violation
}

func (r funcRule) ID() string                             { return r.id }
func (r funcRule) Name() string                           { return r.name }
func (r funcRule) Evaluate(s *ReconcileSnapshot) []Violation { return r.fn(s) }
