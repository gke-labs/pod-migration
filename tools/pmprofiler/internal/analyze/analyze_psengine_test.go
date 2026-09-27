package analyze

// psengine-native reconstruction (HR-2 coverage for the criu-snapshot-engine
// flow, docs/proposals/criu-engine-plugin.html): engine identity from the
// PodSnapshot's label, checkpoint+upload duration from its condition LTT,
// restore outcome from the engine's explicit annotation — the ONLY accepted
// restore signal (generic snapshot-related keys like ps-name sit on source
// and cold-started pods alike and prove nothing).

import (
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writePSEngineRun(t *testing.T) string {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const snapGVR = "podsnapshots.v1.podsnapshot.gke.io"
	const podGVR = "pods.v1"

	// criu-engine PodSnapshot: our work-order spec shape (spec.sourcePod),
	// engine label, created +2s, Ready (verified upload) +14s → 12s
	// checkpoint+upload.
	criuSnap := func(withReady bool) map[string]any {
		o := map[string]any{
			"metadata": map[string]any{
				"name": "ps-web-0-deadbeef", "creationTimestamp": ts(2 * time.Second),
				"labels": map[string]any{"pod-migrate.io/engine": "criu"},
			},
			"spec": map[string]any{"sourcePod": "web-0", "nodeName": "node-a", "bucket": "bkt"},
		}
		if withReady {
			o["status"] = map[string]any{"conditions": []any{
				map[string]any{"type": "Checkpoint", "status": "True", "reason": "Succeeded",
					"lastTransitionTime": ts(14 * time.Second)},
				map[string]any{"type": "Ready", "status": "True", "reason": "Succeeded",
					"lastTransitionTime": ts(14 * time.Second)},
			}}
		}
		return o
	}

	// Restored migration.
	rec(t, f, -time.Minute, "list", podGVR, podObj("web-0", "uid-src", -time.Minute, nil))
	rec(t, f, 0, "add", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Pending", ""))
	rec(t, f, 2*time.Second, "add", snapGVR, criuSnap(false))
	rec(t, f, 14*time.Second, "update", snapGVR, criuSnap(true))
	rec(t, f, 16*time.Second, "update", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Evicting", "ps-web-0-deadbeef"))
	rec(t, f, 20*time.Second, "delete", podGVR, podObj("web-0", "uid-src", -time.Minute, nil))
	rec(t, f, 22*time.Second, "add", podGVR, podObj("web-0", "uid-dst", 22*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{
			"podsnapshot.gke.io/ps-name":      "ps-web-0-deadbeef",
			"pod-migrate.io/psengine-restore": "restored",
		}
	}))
	rec(t, f, 31*time.Second, "update", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Succeeded", "ps-web-0-deadbeef"))

	// Failed restore → cold-start fall-through: the ps-name annotation is
	// present (it never implies a restore) and the engine stamped failed —
	// the explicit value decides.
	rec(t, f, 0, "list", podGVR, podObj("cold-0", "uid-c-src", -time.Minute, nil))
	rec(t, f, 40*time.Second, "add", pmjGVR, pmjObj("pmj-cold-0", "cold-0", "uid-c-src", "Pending", ""))
	rec(t, f, 60*time.Second, "add", podGVR, podObj("cold-0", "uid-c-dst", 60*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{
			"podsnapshot.gke.io/ps-name":      "ps-cold-0-cafebabe",
			"pod-migrate.io/psengine-restore": "failed",
		}
	}))
	rec(t, f, 70*time.Second, "update", pmjGVR, pmjObj("pmj-cold-0", "cold-0", "uid-c-src", "Succeeded", ""))
	return dir
}

func TestAnalyzePSEngineRows(t *testing.T) {
	dir := writePSEngineRun(t)
	run, err := Analyze(Options{RunDir: dir, Scenario: "psengine", WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	byPMJ := map[string]Migration{}
	for _, m := range run.Migrations {
		byPMJ[m.PMJ] = m
	}

	m := byPMJ["pmj-web-0"]
	if m.Engine != "criu" {
		t.Errorf("engine = %q, want criu", m.Engine)
	}
	if math.Abs(m.CheckpointUploadS-12.0) > 0.5 {
		t.Errorf("checkpointUploadS = %v, want ~12 (PodSnapshot creation → Ready LTT)", m.CheckpointUploadS)
	}
	if m.RestoreOutcome != "restored" || !m.Restored || m.Outcome != OutcomeRestored {
		t.Errorf("restored row wrong: outcome=%s restoreOutcome=%q restored=%v", m.Outcome, m.RestoreOutcome, m.Restored)
	}
	if m.RestoreSignal != "psengine-restore:restored" {
		t.Errorf("restoreSignal = %q, want psengine-restore:restored", m.RestoreSignal)
	}

	c := byPMJ["pmj-cold-0"]
	if c.RestoreOutcome != "failed" {
		t.Errorf("cold restoreOutcome = %q, want failed", c.RestoreOutcome)
	}
	if c.Restored || c.Outcome != OutcomeColdStart {
		t.Errorf("explicit failed outcome must yield cold-start, got outcome=%s restored=%v", c.Outcome, c.Restored)
	}
	// No engine label on the cold PMJ's (absent) snapshot: engine must be
	// absent, never invented.
	if c.Engine != "" {
		t.Errorf("cold engine = %q, want absent", c.Engine)
	}
}
