package invariants

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

const (
	// EventReasonInvariantViolation is the Kubernetes Warning Event reason emitted
	// whenever an invariant (I1-I9) is violated in observe or strict mode.
	EventReasonInvariantViolation = "InvariantViolation"
)

// Engine evaluates a set of stateless invariant Rules against ReconcileSnapshots
// and records violations via Prometheus metrics, Kubernetes Events, and structured logs.
type Engine struct {
	Mode     Mode
	Rules    []Rule
	Recorder record.EventRecorder
}

// NewEngine constructs an invariant Engine with DefaultRules.
func NewEngine(mode Mode, recorder record.EventRecorder) *Engine {
	if mode == "" {
		mode = ModeObserve
	}
	return &Engine{
		Mode:     mode,
		Rules:    DefaultRules,
		Recorder: recorder,
	}
}

// Evaluate runs all registered invariant rules against s.
// It returns all detected violations and a boolean indicating whether strict mode
// requires failing the active migration job immediately.
func (e *Engine) Evaluate(ctx context.Context, s *ReconcileSnapshot) ([]Violation, bool) {
	if e == nil || e.Mode == ModeDisabled || s == nil {
		return nil, false
	}

	rules := e.Rules
	if len(rules) == 0 {
		rules = DefaultRules
	}

	var violations []Violation
	for _, rule := range rules {
		vs := rule.Evaluate(s)
		if len(vs) > 0 {
			violations = append(violations, vs...)
		}
	}

	if len(violations) == 0 {
		return nil, false
	}

	logger := log.FromContext(ctx).WithName("invariants")
	for _, v := range violations {
		metrics.RecordInvariantViolation(v.InvariantID)

		logger.Error(nil, "Invariant violation detected",
			"invariant", v.InvariantID,
			"name", v.InvariantName,
			"reason", v.Reason,
			"mode", string(e.Mode),
			"reconciler", s.Reconciler,
			"namespace", v.Namespace,
			"pmj", v.PMJName,
			"pod", v.PodName,
			"detail", v.Message,
		)

		if e.Recorder != nil {
			msg := fmt.Sprintf("[%s:%s] %s (%s)", v.InvariantID, v.InvariantName, v.Message, v.Reason)
			if s.PrimaryPMJ != nil {
				e.Recorder.Event(s.PrimaryPMJ, corev1.EventTypeWarning, EventReasonInvariantViolation, msg)
			}
			if s.PrimaryPod != nil {
				e.Recorder.Event(s.PrimaryPod, corev1.EventTypeWarning, EventReasonInvariantViolation, msg)
			}
			if s.PrimaryPMJ == nil && s.PrimaryPod == nil && s.PrimaryMigration != nil {
				e.Recorder.Event(s.PrimaryMigration, corev1.EventTypeWarning, EventReasonInvariantViolation, msg)
			}
		}
	}

	return violations, e.Mode == ModeStrict
}
