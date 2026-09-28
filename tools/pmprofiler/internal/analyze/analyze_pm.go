package analyze

// Native reconstruction for this operator's PodMigration CRs (pod-migrate.io).
// Unlike the PMJ path (which reconstructs timelines from watch receipt times),
// PodMigration carries MEASURED timings in status — startedAt/completedAt,
// unavailableMs, phaseMs, agent.warmStateVerified — so the analysis is a
// direct read of object-carried truth. Signals that are absent stay absent
// (negative durations), never invented.

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Additional outcome for migrations the operator declined at precheck.
const OutcomeRefused = "refused"

// pmState is the latest observed object per PodMigration name.
type pmState struct {
	obj     *unstructured.Unstructured
	lastTS  time.Time
	created time.Time
}

func ingestPM(pms map[string]*pmState, u *unstructured.Unstructured, ts time.Time) {
	name := u.GetName()
	s, ok := pms[name]
	if !ok {
		s = &pmState{created: u.GetCreationTimestamp().Time}
		pms[name] = s
	}
	s.obj = u
	s.lastTS = ts
}

func nestedStr(u *unstructured.Unstructured, path ...string) string {
	v, _, _ := unstructured.NestedString(u.Object, path...)
	return v
}

func nestedI64(u *unstructured.Unstructured, path ...string) (int64, bool) {
	// ndjson round-trips numbers as float64; live client objects carry int64.
	if v, ok, _ := unstructured.NestedInt64(u.Object, path...); ok {
		return v, ok
	}
	v, ok, _ := unstructured.NestedFloat64(u.Object, path...)
	return int64(v), ok
}

func secsBetween(a, b string) float64 {
	ta, tb := parseTime(a), parseTime(b)
	if ta.IsZero() || tb.IsZero() {
		return -1
	}
	return tb.Sub(ta).Seconds()
}

// reconstructPM maps one PodMigration onto the report's Migration shape.
func reconstructPM(name string, s *pmState, lastTS time.Time, wedge time.Duration) Migration {
	u := s.obj
	phase := nestedStr(u, "status", "phase")
	started := nestedStr(u, "status", "timing", "startedAt")
	completed := nestedStr(u, "status", "timing", "completedAt")

	m := Migration{
		PMJ:        name, // report field name kept for chart compatibility
		Pod:        nestedStr(u, "spec", "podName"),
		SrcNode:    nestedStr(u, "status", "sourceNode"),
		DstNode:    nestedStr(u, "status", "targetNode"),
		T0:         started,
		Phase:      phase,
		SnapReadyS: -1, EvictedS: -1, E2ES: -1, DowntimeS: -1, GateHoldS: -1,
	}
	if m.T0 == "" {
		m.T0 = fmtTime(s.created)
		started = m.T0
	}

	// Checkpoint-ready ≙ snapshot-ready; detach ≙ evicted-from-traffic.
	m.TSnapReady = nestedStr(u, "status", "timing", "checkpointedAt")
	m.SnapReadyS = secsBetween(started, m.TSnapReady)
	m.TEvicting = nestedStr(u, "status", "timing", "windowStartedAt")
	m.EvictedS = secsBetween(started, m.TEvicting)
	m.TDstReady = nestedStr(u, "status", "timing", "windowEndedAt")
	m.TTerminal = completed
	m.E2ES = secsBetween(started, completed)
	if ms, ok := nestedI64(u, "status", "timing", "unavailableMs"); ok {
		m.DowntimeS = float64(ms) / 1000
	}
	if b, ok := nestedI64(u, "status", "agent", "checkpointTarBytes"); ok {
		m.SnapshotBytes = b
	}
	if wv, ok, _ := unstructured.NestedBool(u.Object, "status", "agent", "warmStateVerified"); ok {
		m.Restored = wv
		m.RestoreSignal = fmt.Sprintf("agent.warmStateVerified:%t", wv)
	} else {
		m.RestoreSignal = "absent"
	}

	switch phase {
	case "Succeeded":
		// A succeeded warm migration IS a restore (the process moved); the
		// Restored flag above additionally records agent-verified warm state.
		m.Outcome = OutcomeRestored
	case "Failed", "Aborted":
		m.Outcome = OutcomeFailed
	case "Rejected":
		m.Outcome = OutcomeRefused
	default:
		if !s.lastTS.IsZero() && lastTS.Sub(s.created) > wedge {
			m.Outcome = OutcomeWedged
		} else {
			m.Outcome = OutcomeInFlight
		}
	}
	if msg := nestedStr(u, "status", "message"); msg != "" && m.Outcome != OutcomeRestored {
		m.Warnings = append(m.Warnings, msg)
	}
	return m
}
