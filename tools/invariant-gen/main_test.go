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

func TestDetectAndVerifyWedgedRestoringOrphanWithCompiledGoTest(t *testing.T) {
	runDir := t.TempDir()
	outDir := t.TempDir()

	// Write a clean green trace for GREEN replay verification.
	greenTrace := filepath.Join(t.TempDir(), "clean_records.ndjson")
	cleanNDJSON := strings.Join([]string{
		`{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"pods.v1.","obj":{"metadata":{"name":"app-0","namespace":"default","uid":"uid-src-0"},"spec":{"nodeName":"node-a"},"status":{"phase":"Running"}}}`,
		`{"ts":"2026-09-28T12:00:02Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-clean-0","namespace":"default"},"spec":{"podRef":{"name":"app-0"}},"status":{"phase":"Snapshotting","snapshotRef":"ps-clean-0"}}}`,
		`{"ts":"2026-09-28T12:00:05Z","type":"add","gvr":"pods.v1.","obj":{"metadata":{"name":"app-0-dst","namespace":"default","uid":"uid-dst-0","annotations":{"gke.io/pod-snapshot-restore-name":"ps-clean-0"}},"spec":{"nodeName":"node-b"},"status":{"phase":"Running"}}}`,
		`{"ts":"2026-09-28T12:00:10Z","type":"update","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-clean-0","namespace":"default"},"spec":{"podRef":{"name":"app-0"}},"status":{"phase":"Succeeded","snapshotRef":"ps-clean-0","restoredPodName":"app-0-dst"}}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(greenTrace, []byte(cleanNDJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	// Write a blind-spot run.json using pmprofiler's exact schema:
	// invariantViolations is map[string]float64 ({}), totalInvariantViolations is 0,
	// and MigrationRecord uses pod, snapshotName, phase, tTerminal.
	runJSON := `{
  "scenario": "synthetic-self-test: wedged-restoring-orphan",
  "outcomes": {"stalled": 1},
  "invariantViolations": {},
  "totalInvariantViolations": 0,
  "migrations": [
    {
      "app": "t3-counter",
      "pod": "t3-counter-0",
      "pmj": "pmj-wedge-0",
      "snapshotName": "ps-wedge-0",
      "outcome": "stalled",
      "phase": "Restoring",
      "t0": "2026-09-28T00:26:58Z",
      "tTerminal": "2026-09-28T00:29:58Z"
    }
  ]
}`
	recordsNDJSON := strings.Join([]string{
		`{"ts":"2026-09-28T00:26:58Z","type":"add","gvr":"pods.v1.","obj":{"metadata":{"name":"t3-counter-0","namespace":"default","uid":"uid-src-wedge"},"spec":{"nodeName":"node-a"},"status":{"phase":"Running"}}}`,
		`{"ts":"2026-09-28T00:27:05Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-wedge-0","namespace":"default"},"spec":{"podRef":{"name":"t3-counter-0"}},"status":{"phase":"Restoring","snapshotRef":"ps-wedge-0","restoredPodName":"t3-counter-0-dst","restoringStartTime":"2026-09-28T00:27:05Z"}}}`,
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
	if findings[0].Template != TemplateWedgedRestoringOrphan {
		t.Fatalf("expected template %q, got %q", TemplateWedgedRestoringOrphan, findings[0].Template)
	}
	if findings[0].SourcePod != "t3-counter-0" || findings[0].Snapshot.PrimaryPMJ == nil || findings[0].Snapshot.PrimaryPMJ.Status.SnapshotRef != "ps-wedge-0" {
		t.Fatalf("expected pmprofiler field mapping (pod/snapshotName) to populate SourcePod and PrimaryPMJ.Status.SnapshotRef, got sourcePod=%q primaryPMJ=%+v",
			findings[0].SourcePod, findings[0].Snapshot.PrimaryPMJ)
	}

	proof, err := SynthesizeAndVerify(findings[0], outDir, "", []string{greenTrace})
	if err != nil {
		t.Fatalf("SynthesizeAndVerify failed: %v", err)
	}
	if !proof.CompiledAndTested {
		t.Fatalf("expected compiled go test verification to run, redOut=%s greenOut=%s", proof.RedTestOutput, proof.GreenTestOutput)
	}
	if !proof.RedPassed {
		t.Fatalf("expected RED proof to pass via go test, got redOut:\n%s", proof.RedTestOutput)
	}
	if !proof.GreenPassed {
		t.Fatalf("expected GREEN proof to pass via go test, got greenOut:\n%s", proof.GreenTestOutput)
	}
	if proof.GreenTraceStepsChecked != 2 {
		t.Fatalf("expected 2 PMJ trace steps checked in clean_records.ndjson, got %d", proof.GreenTraceStepsChecked)
	}
	if !strings.Contains(proof.RedTestOutput, "PASS: TestI10_RestoringReplacementLiveness_RedProof") ||
		!strings.Contains(proof.GreenTestOutput, "PASS: TestI10_RestoringReplacementLiveness_GreenProof") {
		t.Fatalf("expected compiled go test output to include RedProof and GreenProof PASS lines, got:\nRED:\n%s\nGREEN:\n%s",
			proof.RedTestOutput, proof.GreenTestOutput)
	}
	for _, p := range []string{proof.SnapshotFixturePath, proof.GreenFixturesPath, proof.RuleFilePath, proof.TestFilePath, proof.PRBodyPath} {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected generated artifact %s to exist: %v", p, err)
		}
	}
}

func TestDetectAndVerifyEvictingStallAndColdStartTemplates(t *testing.T) {
	for _, tc := range []struct {
		name     string
		invID    string
		runJSON  string
		ndjson   string
		template TemplateClass
	}{
		{
			name:  "no-replacement-evicting-stall",
			invID: "I11",
			runJSON: `{
  "scenario": "synthetic-self-test: evicting-stall",
  "outcomes": {"stalled": 1},
  "invariantViolations": {},
  "totalInvariantViolations": 0,
  "migrations": [{"app": "zk", "pod": "zk-0", "pmj": "pmj-evict-0", "outcome": "stalled", "phase": "Evicting"}]
}`,
			ndjson:   `{"ts":"2026-09-28T01:00:00Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-evict-0","namespace":"default"},"spec":{"podRef":{"name":"zk-0"}},"status":{"phase":"Evicting","snapshotRef":"ps-evict-0","evictingStartTime":"2026-09-28T01:00:00Z"}}}` + "\n",
			template: TemplateNoReplacementEvictingStall,
		},
		{
			name:  "unintended-cold-start-active-pmj",
			invID: "I12",
			runJSON: `{
  "scenario": "synthetic-self-test: unintended-cold-start",
  "outcomes": {"cold-start": 1},
  "invariantViolations": {},
  "totalInvariantViolations": 0,
  "migrations": [{"app": "pg", "pod": "pg-0", "pmj": "pmj-cold-0", "snapshotName": "ps-cold-0", "outcome": "cold-start", "phase": "Running"}]
}`,
			ndjson: strings.Join([]string{
				`{"ts":"2026-09-28T01:00:00Z","type":"add","gvr":"pods.v1.","obj":{"metadata":{"name":"pg-0-new","namespace":"default","uid":"uid-cold-new","annotations":{"gke.io/pod-snapshot-restore-name":""}},"spec":{"nodeName":"node-b"},"status":{"phase":"Running","conditions":[{"type":"Ready","status":"True"}]}}}`,
				`{"ts":"2026-09-28T01:00:01Z","type":"add","gvr":"podmigrationjobs.v1alpha1.podmigration.gke.io","obj":{"metadata":{"name":"pmj-cold-0","namespace":"default"},"spec":{"podRef":{"name":"pg-0"}},"status":{"phase":"Restoring","snapshotRef":"ps-cold-0","restoredPodName":"pg-0-new"}}}`,
			}, "\n") + "\n",
			template: TemplateUnintendedColdStartActivePMJ,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runDir := t.TempDir()
			if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(tc.runJSON), 0o644); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(runDir, "records.ndjson"), []byte(tc.ndjson), 0o644); err != nil {
				t.Fatal(err)
			}
			findings, err := DetectBlindSpots(runDir, tc.invID, false)
			if err != nil {
				t.Fatalf("DetectBlindSpots failed: %v", err)
			}
			if len(findings) != 1 || findings[0].Template != tc.template {
				t.Fatalf("expected 1 finding with template %s, got %+v", tc.template, findings)
			}
			proof, err := SynthesizeAndVerify(findings[0], t.TempDir(), "", nil)
			if err != nil {
				t.Fatalf("SynthesizeAndVerify failed: %v", err)
			}
			if !proof.CompiledAndTested || !proof.RedPassed || !proof.GreenPassed {
				t.Fatalf("expected compiled go test RED and GREEN to pass for %s, got compiled=%v red=%v green=%v\nRED:\n%s\nGREEN:\n%s",
					tc.name, proof.CompiledAndTested, proof.RedPassed, proof.GreenPassed, proof.RedTestOutput, proof.GreenTestOutput)
			}
		})
	}
}

func TestDetectBlindSpotsSkipsAlreadyCaughtAndHealthyRuns(t *testing.T) {
	runDir := t.TempDir()
	// 1. Run where I1-I9 already recorded invariantViolations map (real pmprofiler schema): not a blind spot.
	caughtMapJSON := `{
  "scenario": "already-caught-map",
  "outcomes": {"failed": 1},
  "invariantViolations": {"I3": 1},
  "totalInvariantViolations": 1,
  "migrations": [{"app": "a", "pmj": "pmj-1", "outcome": "failed"}]
}`
	if err := os.WriteFile(filepath.Join(runDir, "run.json"), []byte(caughtMapJSON), 0o644); err != nil {
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
		t.Fatalf("expected 0 blind spots when invariantViolations map has entries, got %d", len(findings))
	}

	// 2. Run with cold-start when allowColdStart=true: not a blind spot.
	expectedColdJSON := `{
  "scenario": "expected-cold-start",
  "outcomes": {"cold-start": 1},
  "invariantViolations": {},
  "totalInvariantViolations": 0,
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
	// 1. Trigger C (/extract-invariant) for wedged restoring orphan: passes compiled go test RED & GREEN.
	f1, err := ParseTriggerCommentOrPR("I10", "/extract-invariant I11 wedged-restoring-orphan PMJ stuck in Restoring after replacement pod deleted", "")
	if err != nil {
		t.Fatalf("ParseTriggerCommentOrPR failed: %v", err)
	}
	if f1.InvariantID != "I11" || f1.Template != TemplateWedgedRestoringOrphan {
		t.Fatalf("unexpected finding: id=%s template=%s", f1.InvariantID, f1.Template)
	}
	p1, err := SynthesizeAndVerify(f1, t.TempDir(), "", nil)
	if err != nil {
		t.Fatalf("SynthesizeAndVerify failed: %v", err)
	}
	if !p1.CompiledAndTested || !p1.RedPassed || !p1.GreenPassed {
		t.Fatalf("expected wedged-restoring-orphan template to pass compiled go test RED and GREEN, got compiled=%v red=%v green=%v",
			p1.CompiledAndTested, p1.RedPassed, p1.GreenPassed)
	}

	// 2. Custom novel directive (or premature-snapshot-failed before ReconcileSnapshot carries PodSnapshot):
	// must set RequiresAuthorBody=true and fail the compiled RED proof (RedPassed=false, GreenPassed=false).
	f2, err := ParseTriggerCommentOrPR("I12", "/extract-invariant I12 custom-novel-check some brand new cross-resource invariant", "")
	if err != nil {
		t.Fatalf("ParseTriggerCommentOrPR custom failed: %v", err)
	}
	if f2.Template != TemplateCustomScaffold {
		t.Fatalf("expected TemplateCustomScaffold, got %s", f2.Template)
	}
	p2, err := SynthesizeAndVerify(f2, t.TempDir(), "", nil)
	if err != nil {
		t.Fatalf("SynthesizeAndVerify custom failed: %v", err)
	}
	if !p2.RequiresAuthorBody || !p2.CompiledAndTested || p2.RedPassed || p2.GreenPassed {
		t.Fatalf("expected custom scaffold to compile and run go test, require author body, and report redPassed=false greenPassed=false, got %+v", p2)
	}
}
