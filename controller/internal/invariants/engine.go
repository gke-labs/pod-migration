package invariants

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

const (
	// EventReasonInvariantViolation is the Kubernetes Warning Event reason emitted
	// whenever an invariant is violated in observe or strict mode.
	EventReasonInvariantViolation = "InvariantViolation"

	// maxActiveViolationScopes caps the size of the in-memory deduplication map.
	maxActiveViolationScopes = 4096
)

// Engine evaluates a set of stateless invariant Rules against ReconcileSnapshots
// and records violations via Prometheus metrics, Kubernetes Events, and structured logs.
// It deduplicates repeated observations of the same violation per reconcile subject so
// active-phase requeue loops (2-5s) do not spam etcd events or inflate violation counters.
// It also tracks consecutive violation samples per subject for debounced absence-style rules.
type Engine struct {
	Mode     Mode
	Rules    []Rule
	Recorder record.EventRecorder

	mu                   sync.Mutex
	activeViolations     map[string]map[string]string    // scopeKey -> violationKey -> message
	consecutiveCounts    map[string]map[string]int       // subjectKey -> invariantID -> consecutive count
	consecutiveFirstSeen map[string]map[string]time.Time // subjectKey -> invariantID -> first seen timestamp
	consecutiveEscalated map[string]map[string]bool      // subjectKey -> invariantID -> whether escalation event was emitted
}

// NewEngine constructs an invariant Engine with DefaultRules.
func NewEngine(mode Mode, recorder record.EventRecorder) *Engine {
	if mode == "" {
		mode = ModeDisabled
	}
	return &Engine{
		Mode:                 mode,
		Rules:                DefaultRules,
		Recorder:             recorder,
		activeViolations:     make(map[string]map[string]string),
		consecutiveCounts:    make(map[string]map[string]int),
		consecutiveFirstSeen: make(map[string]map[string]time.Time),
		consecutiveEscalated: make(map[string]map[string]bool),
	}
}

// Enabled reports whether the engine is active (observe or strict mode).
// Reconcilers check Enabled() before building a ReconcileSnapshot or listing
// namespace objects from the informer cache.
func (e *Engine) Enabled() bool {
	return e != nil && e.Mode != "" && e.Mode != ModeDisabled
}

// ForgetObject removes any cached deduplication and debounce state for a deleted object.
func (e *Engine) ForgetObject(reconciler, kind, namespace, name string) {
	if e == nil {
		return
	}
	scopeKey := fmt.Sprintf("%s/%s/%s/%s", reconciler, kind, namespace, name)
	prefix := scopeKey + "/"
	e.mu.Lock()
	delete(e.activeViolations, scopeKey)
	for k := range e.consecutiveCounts {
		if k == scopeKey || strings.HasPrefix(k, prefix) {
			delete(e.consecutiveCounts, k)
		}
	}
	for k := range e.consecutiveFirstSeen {
		if k == scopeKey || strings.HasPrefix(k, prefix) {
			delete(e.consecutiveFirstSeen, k)
		}
	}
	for k := range e.consecutiveEscalated {
		if k == scopeKey || strings.HasPrefix(k, prefix) {
			delete(e.consecutiveEscalated, k)
		}
	}
	e.mu.Unlock()
}

// ResetDeduplication clears the in-memory violation deduplication and debounce state.
func (e *Engine) ResetDeduplication() {
	if e == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.activeViolations = make(map[string]map[string]string)
	e.consecutiveCounts = make(map[string]map[string]int)
	e.consecutiveFirstSeen = make(map[string]map[string]time.Time)
	e.consecutiveEscalated = make(map[string]map[string]bool)
}

// ConsecutiveCount returns the consecutive violation sample count for a subject key and invariant ID.
// Used primarily for verification and testing.
func (e *Engine) ConsecutiveCount(subjectKey, invariantID string) int {
	if e == nil {
		return 0
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.consecutiveCounts == nil {
		return 0
	}
	return e.consecutiveCounts[subjectKey][invariantID]
}

// FirstSeen returns the first-seen timestamp for a subject key and invariant ID.
// Used primarily for verification and testing.
func (e *Engine) FirstSeen(subjectKey, invariantID string) time.Time {
	if e == nil {
		return time.Time{}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.consecutiveFirstSeen == nil {
		return time.Time{}
	}
	return e.consecutiveFirstSeen[subjectKey][invariantID]
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

func debounceSubjectKey(s *ReconcileSnapshot) string {
	if s == nil {
		return ""
	}
	scopeKey := snapshotScopeKey(s)
	var uid string
	if s.PrimaryPMJ != nil && s.PrimaryPMJ.UID != "" {
		uid = string(s.PrimaryPMJ.UID)
	} else if s.PrimaryPod != nil && s.PrimaryPod.UID != "" {
		uid = string(s.PrimaryPod.UID)
	} else if s.PrimaryMigration != nil && s.PrimaryMigration.UID != "" {
		uid = string(s.PrimaryMigration.UID)
	}
	if uid != "" {
		return fmt.Sprintf("%s/%s", scopeKey, uid)
	}
	return scopeKey
}

func isPMJTerminal(pmj *pmv1alpha1.PodMigrationJob) bool {
	if pmj == nil {
		return false
	}
	switch pmj.Status.Phase {
	case pmv1alpha1.PodMigrationJobPhaseSucceeded,
		pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
		pmv1alpha1.PodMigrationJobPhaseFailed:
		return true
	default:
		return false
	}
}

func violationDedupKey(v Violation) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", v.InvariantID, v.Reason, v.Namespace, v.PMJName, v.PodName)
}

func violationThreshold(v Violation, r Rule) (int, time.Duration) {
	count := 1
	var minDuration time.Duration

	if dr, ok := r.(DebouncedRule); ok {
		if c := dr.ConsecutiveSamples(); c > 0 {
			count = c
		}
		if d := dr.MinimumDuration(); d > 0 {
			minDuration = d
		}
	}

	// Per-violation overrides take precedence if set.
	if v.ConsecutiveRequired > 0 {
		count = v.ConsecutiveRequired
	}
	if v.DurationRequired > 0 {
		minDuration = v.DurationRequired
	}

	return count, minDuration
}

func isDegraded(s *ReconcileSnapshot) bool {
	return s != nil && (s.PodListFailed || s.PMJListFailed)
}

// Evaluate runs all registered invariant rules against s.
// It returns all detected violations and a boolean indicating whether strict mode
// requires failing the active migration job immediately.
func (e *Engine) Evaluate(ctx context.Context, s *ReconcileSnapshot) ([]Violation, bool) {
	if !e.Enabled() || s == nil {
		return nil, false
	}

	rules := e.Rules
	if len(rules) == 0 {
		rules = DefaultRules
	}

	var violations []Violation
	ruleByID := make(map[string]Rule, len(rules))
	for _, rule := range rules {
		ruleByID[rule.ID()] = rule
		vs := rule.Evaluate(s)
		if len(vs) > 0 {
			violations = append(violations, vs...)
		}
	}

	scopeKey := snapshotScopeKey(s)
	subjKey := debounceSubjectKey(s)
	degraded := isDegraded(s)
	now := s.Now
	if now.IsZero() {
		now = time.Now()
	}

	e.mu.Lock()
	if e.activeViolations == nil {
		e.activeViolations = make(map[string]map[string]string)
	}
	if e.consecutiveCounts == nil {
		e.consecutiveCounts = make(map[string]map[string]int)
	}
	if e.consecutiveFirstSeen == nil {
		e.consecutiveFirstSeen = make(map[string]map[string]time.Time)
	}
	if e.consecutiveEscalated == nil {
		e.consecutiveEscalated = make(map[string]map[string]bool)
	}

	// Terminal PMJ: clear debounce state for this subject so terminal jobs do not persist counts.
	if s.PrimaryPMJ != nil && isPMJTerminal(s.PrimaryPMJ) {
		delete(e.consecutiveCounts, subjKey)
		delete(e.consecutiveFirstSeen, subjKey)
		delete(e.consecutiveEscalated, subjKey)
	}

	if len(violations) == 0 {
		delete(e.activeViolations, scopeKey)
		// Only clear debounce counters if this sample was not degraded by informer list errors.
		// A flapping list failure must not reset the debounce persistence of an active wedge.
		if !degraded {
			delete(e.consecutiveCounts, subjKey)
			delete(e.consecutiveFirstSeen, subjKey)
			delete(e.consecutiveEscalated, subjKey)
		}
		e.mu.Unlock()
		return nil, false
	}

	if _, exists := e.activeViolations[scopeKey]; !exists && len(e.activeViolations) >= maxActiveViolationScopes {
		for k := range e.activeViolations {
			delete(e.activeViolations, k)
			break
		}
	}
	if _, exists := e.consecutiveCounts[subjKey]; !exists && len(e.consecutiveCounts) >= maxActiveViolationScopes {
		for k := range e.consecutiveCounts {
			delete(e.consecutiveCounts, k)
			delete(e.consecutiveFirstSeen, k)
			delete(e.consecutiveEscalated, k)
			break
		}
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

	// Debounce persistence tracking:
	// Update consecutive counters for active invariants on non-terminal subjects.
	// Invariants that evaluated clean on this sample are reset to 0, unless the sample is degraded.
	activeInvIDs := make(map[string]bool, len(violations))
	for _, v := range violations {
		activeInvIDs[v.InvariantID] = true
	}

	subjCounts := e.consecutiveCounts[subjKey]
	if subjCounts == nil {
		subjCounts = make(map[string]int)
		e.consecutiveCounts[subjKey] = subjCounts
	}
	subjFirstSeen := e.consecutiveFirstSeen[subjKey]
	if subjFirstSeen == nil {
		subjFirstSeen = make(map[string]time.Time)
		e.consecutiveFirstSeen[subjKey] = subjFirstSeen
	}
	subjEscalated := e.consecutiveEscalated[subjKey]
	if subjEscalated == nil {
		subjEscalated = make(map[string]bool)
		e.consecutiveEscalated[subjKey] = subjEscalated
	}

	if !degraded {
		for invID := range subjCounts {
			if !activeInvIDs[invID] {
				delete(subjCounts, invID)
				delete(subjFirstSeen, invID)
				delete(subjEscalated, invID)
			}
		}
	}

	shouldFail := false
	countedThisReconcile := make(map[string]int, len(violations))
	elapsedThisReconcile := make(map[string]time.Duration, len(violations))
	var escalatingViolations []Violation

	for _, v := range violations {
		thresholdCount, minDuration := violationThreshold(v, ruleByID[v.InvariantID])
		if thresholdCount <= 1 && minDuration <= 0 {
			// Immediate invariant: requires no debounce.
			if e.Mode == ModeStrict {
				shouldFail = true
			}
			continue
		}

		// Debounced invariant: do not track or escalate if PMJ is already terminal.
		if s.PrimaryPMJ != nil && isPMJTerminal(s.PrimaryPMJ) {
			continue
		}

		if !degraded {
			if _, seen := countedThisReconcile[v.InvariantID]; !seen {
				if subjFirstSeen[v.InvariantID].IsZero() {
					subjFirstSeen[v.InvariantID] = now
				}
				subjCounts[v.InvariantID]++
				countedThisReconcile[v.InvariantID] = subjCounts[v.InvariantID]
				elapsedThisReconcile[v.InvariantID] = now.Sub(subjFirstSeen[v.InvariantID])
			}
		} else {
			// Inconclusive degraded sample: neither increment nor reset debounce counters.
			if _, seen := countedThisReconcile[v.InvariantID]; !seen {
				countedThisReconcile[v.InvariantID] = subjCounts[v.InvariantID]
				if !subjFirstSeen[v.InvariantID].IsZero() {
					elapsedThisReconcile[v.InvariantID] = now.Sub(subjFirstSeen[v.InvariantID])
				}
			}
		}

		count := countedThisReconcile[v.InvariantID]
		elapsed := elapsedThisReconcile[v.InvariantID]
		thresholdMet := count >= thresholdCount && (minDuration <= 0 || elapsed >= minDuration)

		if thresholdMet {
			if e.Mode == ModeStrict {
				shouldFail = true
			}
			if !subjEscalated[v.InvariantID] {
				subjEscalated[v.InvariantID] = true
				escalatingViolations = append(escalatingViolations, v)
			}
		}
	}
	e.mu.Unlock()

	logger := log.FromContext(ctx).WithName("invariants")
	for _, v := range newlyObserved {
		metrics.RecordInvariantViolation(v.InvariantID)

		count := countedThisReconcile[v.InvariantID]
		thresholdCount, minDuration := violationThreshold(v, ruleByID[v.InvariantID])

		logger.Error(nil, "Invariant violation detected",
			"invariant", v.InvariantID,
			"name", v.InvariantName,
			"reason", v.Reason,
			"mode", string(e.Mode),
			"consecutive", count,
			"thresholdCount", thresholdCount,
			"minDuration", minDuration.String(),
			"reconciler", s.Reconciler,
			"namespace", v.Namespace,
			"pmj", v.PMJName,
			"pod", v.PodName,
			"detail", v.Message,
		)

		if e.Recorder != nil {
			msg := fmt.Sprintf("[%s:%s] %s (%s)", v.InvariantID, v.InvariantName, v.Message, v.Reason)
			if thresholdCount > 1 || minDuration > 0 {
				if minDuration > 0 {
					msg = fmt.Sprintf("[%s:%s] %s (%s) [sample %d/%d, min %s]",
						v.InvariantID, v.InvariantName, v.Message, v.Reason, count, thresholdCount, minDuration)
				} else {
					msg = fmt.Sprintf("[%s:%s] %s (%s) [sample %d/%d]",
						v.InvariantID, v.InvariantName, v.Message, v.Reason, count, thresholdCount)
				}
			}
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

	for _, v := range escalatingViolations {
		count := countedThisReconcile[v.InvariantID]
		elapsed := elapsedThisReconcile[v.InvariantID]
		thresholdCount, minDuration := violationThreshold(v, ruleByID[v.InvariantID])

		if e.Mode == ModeStrict {
			logger.Error(nil, "Invariant violation escalated to strict mode abort",
				"invariant", v.InvariantID,
				"name", v.InvariantName,
				"reason", v.Reason,
				"consecutive", count,
				"elapsed", elapsed.String(),
				"thresholdCount", thresholdCount,
				"minDuration", minDuration.String(),
				"reconciler", s.Reconciler,
				"namespace", v.Namespace,
				"pmj", v.PMJName,
				"pod", v.PodName,
			)
			if e.Recorder != nil {
				msg := fmt.Sprintf("[%s:%s] Invariant violation persisted across %d consecutive samples (elapsed %s); escalating to strict abort: %s",
					v.InvariantID, v.InvariantName, count, elapsed.Round(time.Millisecond), v.Message)
				if s.PrimaryPMJ != nil {
					e.Recorder.Event(s.PrimaryPMJ, corev1.EventTypeWarning, EventReasonInvariantViolation, msg)
				}
			}
		} else {
			logger.Info("Invariant violation reached debounce threshold (would escalate to strict mode abort)",
				"invariant", v.InvariantID,
				"name", v.InvariantName,
				"reason", v.Reason,
				"consecutive", count,
				"elapsed", elapsed.String(),
				"thresholdCount", thresholdCount,
				"minDuration", minDuration.String(),
				"reconciler", s.Reconciler,
				"namespace", v.Namespace,
				"pmj", v.PMJName,
				"pod", v.PodName,
			)
			if e.Recorder != nil {
				msg := fmt.Sprintf("[%s:%s] Invariant violation persisted across %d consecutive samples (elapsed %s); would escalate to strict abort in strict mode: %s",
					v.InvariantID, v.InvariantName, count, elapsed.Round(time.Millisecond), v.Message)
				if s.PrimaryPMJ != nil {
					e.Recorder.Event(s.PrimaryPMJ, corev1.EventTypeWarning, EventReasonInvariantViolation, msg)
				}
			}
		}
	}

	return violations, shouldFail
}
