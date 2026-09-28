// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDetectAndVerifyPrematureSnapshotFailedBlindSpot(t *testing.T) {
	runDir := t.TempDir()
	outDir := t.TempDir()

	// Write a clean green trace for GREEN replay verification.
	greenTrace := filepath.Join(t.TempDir(), "clean_records.ndjson")
	cleanNDJSON := strings.Join([]string{
		`{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-clean-0","namespace":"default"},"spec":{"podRef":{"name":"app-0"}},"status":{"phase":"Snapshotting","snapshotName":"ps-clean-0"}}}`,
		`{"ts":"2026-09-28T12:00:02Z","type":"add","gvr":"podsnapshots.v1alpha1.podsnapshot.gke.io","obj":{"metadata":{"name":"ps-clean-0","namespace":"default"},"spec":{"podName":"app-0"},"status":{"conditions":[{"type":"Checkpoint","status":"False","reason":"InProgress"},{"type":"Ready","status":"False","reason":"InProgress"}]}}}`,
		`{"ts":"2026-09-28T12:00:05Z","type":"update","gvr":"podsnapshots.v1alpha1.podsnapshot.gke.io","obj":{"metadata":{"name":"ps-clean-0","namespace":"default"},"spec":{"podName":"app-0"},"status":{"conditions":[{"type":"Checkpoint","status":"True","reason":"Succeeded"},{"type":"StorageReplicated","status":"True","reason":"Succeeded"},{"type":"Ready","status":"True","reason":"AllSnapshotsAvailable"}]}}}`,
		`{"ts":"2026-09-28T12:00:10Z","type":"update","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-clean-0","namespace":"default"},"spec":{"podRef":{"name":"app-0"}},"status":{"phase":"Succeeded","snapshotName":"ps-clean-0","restoredPodName":"app-0-dst"}}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(greenTrace, []byte(cleanNDJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write a blind-spot run.json + records.ndjson reproducing Issue #75:
	// PMJ transitions to Failed (SnapshotFailed) while PodSnapshot Checkpoint=InProgress,
	// and invariantViolations == 0.
	runJSON := `{
  "scenario": "T3-S2 burst drain blind spot (#75)",
  "outcomes": {"failed": 1},
  "invariantViolations": 0,
  "migrations": [
    {
      "app": "t3-counter",
      "srcPod": "t3-counter-0",
      "pmj": "pmj-race-0",
      "snapshot": "ps-race-0",
      "outcome": "failed",
      "finalPhase": "Failed",
      "t0": "2026-09-28T00:26:58Z",
      "tEnd": "2026-09-28T00:27:00Z"
    }
  ]
}`
	recordsNDJSON := strings.Join([]string{
		`{"ts":"2026-09-28T00:26:58Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-race-0","namespace":"default"},"spec":{"podRef":{"name":"t3-counter-0"}},"status":{"phase":"Snapshotting","snapshotName":"ps-race-0"}}}`,
		`{"ts":"2026-09-28T00:26:59Z","type":"add","gvr":"podsnapshots.v1alpha1.podsnapshot.gke.io","obj":{"metadata":{"name":"ps-race-0","namespace":"default"},"spec":{"podName":"t3-counter-0"},"status":{"conditions":[{"type":"Checkpoint","status":"False","reason":"InProgress"},{"type":"StorageReplicated","status":"False","reason":"AwaitingCheckpoint"},{"type":"Ready","status":"False","reason":"Failed","message":"Failed to take snapshot (1)."}]}}}`,
		`{"ts":"2026-09-28T00:27:00Z","type":"update","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-race-0","namespace":"default"},"spec":{"podRef":{"name":"t3-counter-0"}},"status":{"phase":"Failed","reason":"SnapshotFailed","message":"PodSnapshot ps-race-0 condition Ready failed: Failed to take snapshot (1).","snapshotName":"ps-race-0"}}}`,
	}, "\n") + "\n"

	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(runJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "records.ndjson"), []byte(recordsNDJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	findings, err := DetectBlindSpots(runDir, "I10", false)
	if err != nil {
		t.Fatalf("DetectBlindSpots failed: %v", err)
	}
	if len(findings) != 1 {
		t.Fatalf("expected 1 blind spot finding, got %d", len(findings))
	}
	if findings[0].Template != TemplatePrematureSnapshotFailed {
		t.Fatalf("expected template %q, got %q", TemplatePrematureSnapshotFailed, findings[0].Template)
	}

	proof, err := SynthesizeAndVerify(findings[0], outDir, []string{greenTrace})
	if err != nil {
		t.Fatalf("SynthesizeAndVerify failed: %v", err)
	}
	if !proof.RedPassed || len(proof.RedViolations) != 1 {
		t.Fatalf("expected RED proof to pass with 1 violation, got passed=%v violations=%v", proof.RedPassed, proof.RedViolations)
	}
	if !proof.GreenPassed || len(proof.GreenViolations) != 0 {
		t.Fatalf("expected GREEN proof to pass with 0 violations, got passed=%v violations=%v", proof.GreenPassed, proof.GreenViolations)
	}
	if proof.GreenTraceStepsChecked != 2 {
		t.Fatalf("expected 2 PMJ trace steps checked in clean_records.ndjson, got %d", proof.GreenTraceStepsChecked)
	}
	for _, p := range []string{proof.SnapshotFixturePath, proof.RuleFilePath, proof.TestFilePath, proof.PRBodyPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected generated artifact %s to exist: %v", p, err)
		}
	}
}

func TestDetectBlindSpotsSkipsAlreadyCaughtAndHealthyRuns(t *testing.T) {
	runDir := t.TempDir()
	// 1. Run where I1-I9 already recorded invariantViolations > 0: not a blind spot.
	caughtJSON := `{
  "scenario": "already-caught",
  "outcomes": {"failed": 1},
  "invariantViolations": 1,
  "migrations": [{"app": "a", "pmj": "pmj-1", "outcome": "failed"}]
}`
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(caughtJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(runDir, "records.ndjson"), []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	findings, err := DetectBlindSpots(runDir, "I10", false)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 blind spots when invariantViolations > 0, got %d", len(findings))
	}

	// 2. Run with cold-start when allowColdStart=true: not a blind spot.
	expectedColdJSON := `{
  "scenario": "expected-cold-start",
  "outcomes": {"cold-start": 1},
  "invariantViolations": 0,
  "migrations": [{"app": "a", "pmj": "pmj-1", "outcome": "cold-start"}]
}`
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(expectedColdJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	findings, err = DetectBlindSpots(runDir, "I10", true)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(findings) != 0 {
		t.Fatalf("expected 0 blind spots when allowColdStart=true, got %d", len(findings))
	}
}

func TestParseTriggerCommentAndCustomScaffoldHonesty(t *testing.T) {
	// 1. Trigger C (/extract-invariant) for wedged restoring orphan: passes RED & GREEN.
	f1, err := ParseTriggerCommentOrPR("I10", "/extract-invariant I11 wedged-restoring-orphan PMJ stuck in Restoring after replacement pod deleted", "")
	if err != nil {
		t.Fatalf("ParseTriggerCommentOrPR failed: %v", err)
	}
	if f1.InvariantID != "I11" || f1.Template != TemplateWedgedRestoringOrphan {
		t.Fatalf("unexpected finding: id=%s template=%s", f1.InvariantID, f1.Template)
	}
	p1, err := SynthesizeAndVerify(f1, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("SynthesizeAndVerify failed: %v", err)
	}
	if !p1.RedPassed || !p1.GreenPassed {
		t.Fatalf("expected wedged-restoring-orphan template to pass RED and GREEN, got red=%v green=%v", p1.RedPassed, p1.GreenPassed)
	}

	// 2. Custom novel directive: must set RequiresAuthorBody=true and RedPassed=false
	// (never falsely claiming RED/GREEN passed on an unauthored scaffold).
	f2, err := ParseTriggerCommentOrPR("I12", "/extract-invariant I12 custom-novel-check some brand new cross-resource invariant", "")
	if err != nil {
		t.Fatalf("ParseTriggerCommentOrPR custom failed: %v", err)
	}
	if f2.Template != TemplateCustomScaffold {
		t.Fatalf("expected TemplateCustomScaffold, got %s", f2.Template)
	}
	p2, err := SynthesizeAndVerify(f2, t.TempDir(), nil)
	if err != nil {
		t.Fatalf("SynthesizeAndVerify custom failed: %v", err)
	}
	if !p2.RequiresAuthorBody || p2.RedPassed || p2.GreenPassed {
		t.Fatalf("expected custom scaffold to require author body and report redPassed=false greenPassed=false, got %+v", p2)
	}
}
