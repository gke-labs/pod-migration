package invariants

import (
	"context"
	"fmt"
	"sync"

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
// It deduplicates repeated observations of the same violation per reconcile subject so
// active-phase requeue loops (2-5s) do not spam etcd events or inflate violation counters.
type Engine struct {
	Mode     Mode
	Rules    []Rule
	Recorder record.EventRecorder

	mu               sync.Mutex
	activeViolations map[string]map[string]string // scopeKey -> violationKey -> message
}

// NewEngine constructs an invariant Engine with DefaultRules.
func NewEngine(mode Mode, recorder record.EventRecorder) *Engine {
	if mode == "" {
		mode = ModeObserve
	}
	return &Engine{
		Mode:             mode,
		Rules:            DefaultRules,
		Recorder:         recorder,
		activeViolations: make(map[string]map[string]string),
	}
}

// ResetDeduplication clears the in-memory violation deduplication state.
func (e *Engine) ResetDeduplication() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeViolations = make(map[string]map[string]string)
}

func snapshotScopeKey(s *ReconcileSnapshot) string {
	if s == nil {
		return ""
	}
	if s.PrimaryPMJ != nil && s.PrimaryPMJ.Name != "" {
		return fmt.Sprintf("%s/pmj/%s/%s", s.Reconciler, s.PrimaryPMJ.Namespace, s.PrimaryPMJ.Name)
	}
	if s.PrimaryPod != nil && s.PrimaryPod.Name != "" {
		return fmt.Sprintf("%s/pod/%s/%s", s.Reconciler, s.PrimaryPod.Namespace, s.PrimaryPod.Name)
	}
	if s.PrimaryMigration != nil && s.PrimaryMigration.Name != "" {
		return fmt.Sprintf("%s/mig/%s/%s", s.Reconciler, s.PrimaryMigration.Namespace, s.PrimaryMigration.Name)
	}
	return fmt.Sprintf("%s/global", s.Reconciler)
}

func violationDedupKey(v Violation) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", v.InvariantID, v.Reason, v.Namespace, v.PMJName, v.PodName)
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

	scopeKey := snapshotScopeKey(s)

	e.mu.Lock()
	if e.activeViolations == nil {
		e.activeViolations = make(map[string]map[string]string)
	}
	if len(violations) == 0 {
		delete(e.activeViolations, scopeKey)
		e.mu.Unlock()
		return nil, false
	}

	prevSet := e.activeViolations[scopeKey]
	currSet := make(map[string]string, len(violations))
	var newlyObserved []Violation
	for _, v := range violations {
		vKey := violationDedupKey(v)
		currSet[vKey] = v.Message
		if prevMsg, seen := prevSet[vKey]; !seen || prevMsg != v.Message {
			newlyObserved = append(newlyObserved, v)
		}
	}
	e.activeViolations[scopeKey] = currSet
	e.mu.Unlock()

	logger := log.FromContext(ctx).WithName("invariants")
	for _, v := range newlyObserved {
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
