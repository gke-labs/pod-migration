package analyze

import (
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func pmObj(phase string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"metadata": map[string]any{"name": "bench-p1"},
		"spec":     map[string]any{"podName": "p1"},
		"status": map[string]any{
			"phase":      phase,
			"sourceNode": "n1", "targetNode": "n2",
			"timing": map[string]any{
				"startedAt":       "2026-08-21T10:00:00Z",
				"windowStartedAt": "2026-08-21T10:00:02Z",
				"checkpointedAt":  "2026-08-21T10:00:03Z",
				"windowEndedAt":   "2026-08-21T10:00:04Z",
				"completedAt":     "2026-08-21T10:00:05Z",
				"unavailableMs":   int64(1800),
			},
			"agent": map[string]any{
				"warmStateVerified":  true,
				"checkpointTarBytes": int64(123456),
			},
		},
	}}
}

func TestReconstructPMSucceeded(t *testing.T) {
	s := &pmState{obj: pmObj("Succeeded"), lastTS: time.Now()}
	m := reconstructPM("bench-p1", s, time.Now(), 5*time.Minute)
	if m.Outcome != OutcomeRestored || !m.Restored {
		t.Fatalf("outcome=%q restored=%v", m.Outcome, m.Restored)
	}
	if m.E2ES != 5 || m.DowntimeS != 1.8 || m.SnapReadyS != 3 || m.EvictedS != 2 {
		t.Errorf("durations e2e=%v down=%v snap=%v evict=%v", m.E2ES, m.DowntimeS, m.SnapReadyS, m.EvictedS)
	}
	if m.SnapshotBytes != 123456 || m.SrcNode != "n1" || m.DstNode != "n2" {
		t.Errorf("fields: %+v", m)
	}
}

func TestReconstructPMRefusedAndFailed(t *testing.T) {
	if m := reconstructPM("x", &pmState{obj: pmObj("Rejected")}, time.Now(), time.Minute); m.Outcome != OutcomeRefused {
		t.Errorf("rejected -> %q", m.Outcome)
	}
	if m := reconstructPM("x", &pmState{obj: pmObj("Failed")}, time.Now(), time.Minute); m.Outcome != OutcomeFailed {
		t.Errorf("failed -> %q", m.Outcome)
	}
}
