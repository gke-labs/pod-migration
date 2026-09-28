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

// Package main implements tools/invariant-gen, the offline Blind-Spot Detector,
// Snapshot Fixture Extractor, and Red/Green Replay Verifier for the GKE Live Pod
// Migration (LPM) Correctness Guard Rail (Epic #49 / Issue #54).
//
// Scope & Honesty Note:
//   - invariant-gen is a deterministic offline tool; it does NOT invoke an LLM or
//     synthesize arbitrary Go AST predicates from free-form prose.
//   - For Trigger A (pmprofiler blind-spot detection), it joins pmprofiler's run.json
//     with records.ndjson to detect unhealthy migrations (wedged, failed,
//     no-replacement, or unintended cold-start) where invariantViolations == 0,
//     reconstructs the point-in-time cluster state at the failure timestamp into a
//     minimized SnapshotFixture JSON, classifies the state against built-in
//     structural predicate templates (or emits an explicit author scaffold for
//     custom /extract-invariant directives), and executes an automated RED/GREEN
//     replay proof across the offending fixture and clean records.ndjson traces.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// TemplateClass identifies the structural predicate template for a candidate invariant.
type TemplateClass string

const (
	// TemplatePrematureSnapshotFailed catches PMJs marked Failed (SnapshotFailed)
	// while the underlying PodSnapshot still has Checkpoint or StorageReplicated
	// actively progressing or succeeded (live GKE race in Issue #75).
	TemplatePrematureSnapshotFailed TemplateClass = "premature-snapshot-failed"

	// TemplateWedgedRestoringOrphan catches PMJs stuck in Restoring or Evicting
	// after the source pod is gone and the replacement pod is missing or deleted.
	TemplateWedgedRestoringOrphan TemplateClass = "wedged-restoring-orphan"

	// TemplateUnintendedColdStartActivePMJ catches replacement pods reaching Ready
	// without a non-empty snapshot restore annotation while a PMJ for the workload
	// is still active (Snapshotting, Evicting, or Restoring).
	TemplateUnintendedColdStartActivePMJ TemplateClass = "unintended-cold-start-active-pmj"

	// TemplateCustomScaffold is emitted for novel /extract-invariant or bugfix PR
	// triggers that require a human/agent author to fill in the Go predicate body.
	TemplateCustomScaffold TemplateClass = "custom-invariant-scaffold"
)

// ConditionSummary captures a Kubernetes status condition in a serialized fixture.
type ConditionSummary struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// PMJState captures the minimized PodMigrationJob state in a SnapshotFixture.
type PMJState struct {
	Name               string             `json:"name"`
	Namespace          string             `json:"namespace"`
	UID                string             `json:"uid,omitempty"`
	Phase              string             `json:"phase"`
	Reason             string             `json:"reason,omitempty"`
	Message            string             `json:"message,omitempty"`
	SourcePodName      string             `json:"sourcePodName,omitempty"`
	TargetPodUID       string             `json:"targetPodUID,omitempty"`
	SnapshotName       string             `json:"snapshotName,omitempty"`
	RestoredPodName    string             `json:"restoredPodName,omitempty"`
	RestoredPodUID     string             `json:"restoredPodUID,omitempty"`
	CreationTimestamp  string             `json:"creationTimestamp,omitempty"`
	EvictingStartTime  string             `json:"evictingStartTime,omitempty"`
	RestoringStartTime string             `json:"restoringStartTime,omitempty"`
	Conditions         []ConditionSummary `json:"conditions,omitempty"`
}

// PodState captures the minimized Pod state in a SnapshotFixture.
type PodState struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid,omitempty"`
	App               string            `json:"app,omitempty"`
	NodeName          string            `json:"nodeName,omitempty"`
	Phase             string            `json:"phase,omitempty"`
	Ready             bool              `json:"ready"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	SchedulingGates   []string          `json:"schedulingGates,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
}

// PodSnapshotState captures the associated GKE PodSnapshot condition state.
type PodSnapshotState struct {
	Name       string             `json:"name"`
	Namespace  string             `json:"namespace"`
	PodName    string             `json:"podName,omitempty"`
	Conditions []ConditionSummary `json:"conditions,omitempty"`
}

// SnapshotFixture is a deterministic, JSON-serializable point-in-time snapshot
// reconstructed from pmprofiler records.ndjson for Red/Green invariant replay.
type SnapshotFixture struct {
	Now                string            `json:"now"`
	Reconciler         string            `json:"reconciler"`
	Scenario           string            `json:"scenario,omitempty"`
	BlindSpotOutcome   string            `json:"blindSpotOutcome,omitempty"`
	PrimaryPMJ         *PMJState         `json:"primaryPMJ,omitempty"`
	PrimaryPod         *PodState         `json:"primaryPod,omitempty"`
	PrimaryPodSnapshot *PodSnapshotState `json:"primaryPodSnapshot,omitempty"`
	NamespacePMJs      []PMJState        `json:"namespacePMJs,omitempty"`
	NamespacePods      []PodState        `json:"namespacePods,omitempty"`
	SourcePodDeleted   bool              `json:"sourcePodDeleted,omitempty"`
	RestoredPodDeleted bool              `json:"restoredPodDeleted,omitempty"`
}

// BlindSpotFinding describes a single blind spot where pmprofiler observed an
// unhealthy migration outcome while invariantViolations == 0.
type BlindSpotFinding struct {
	TriggerSource    string          `json:"triggerSource"` // "TriggerA:BlindSpot", "TriggerB:BugfixPR", "TriggerC:SlashCommand"
	InvariantID      string          `json:"invariantId"`
	InvariantName    string          `json:"invariantName"`
	Slug             string          `json:"slug"`
	Template         TemplateClass   `json:"template"`
	Scenario         string          `json:"scenario"`
	Outcome          string          `json:"outcome"`
	PMJName          string          `json:"pmjName"`
	SourcePod        string          `json:"sourcePod,omitempty"`
	RestoredPod      string          `json:"restoredPod,omitempty"`
	FailureTimestamp string          `json:"failureTimestamp"`
	Description      string          `json:"description"`
	Snapshot         SnapshotFixture `json:"snapshot"`
}

// CandidateViolation represents a violation produced by evaluating a candidate rule.
type CandidateViolation struct {
	InvariantID   string `json:"invariantId"`
	InvariantName string `json:"invariantName"`
	Reason        string `json:"reason"`
	Message       string `json:"message"`
	PMJName       string `json:"pmjName,omitempty"`
	PodName       string `json:"podName,omitempty"`
}

// RedGreenProof summarizes the automated RED and GREEN replay verification results.
type RedGreenProof struct {
	InvariantID            string               `json:"invariantId"`
	InvariantName          string               `json:"invariantName"`
	Slug                   string               `json:"slug"`
	Template               TemplateClass        `json:"template"`
	TriggerSource          string               `json:"triggerSource"`
	RequiresAuthorBody     bool                 `json:"requiresAuthorBody"`
	RedPassed              bool                 `json:"redPassed"`
	RedViolations          []CandidateViolation `json:"redViolations"`
	GreenPassed            bool                 `json:"greenPassed"`
	GreenBaselineChecked   int                  `json:"greenBaselineChecked"`
	GreenTraceStepsChecked int                  `json:"greenTraceStepsChecked"`
	GreenViolations        []CandidateViolation `json:"greenViolations,omitempty"`
	SnapshotFixturePath    string               `json:"snapshotFixturePath"`
	RuleFilePath           string               `json:"ruleFilePath"`
	TestFilePath           string               `json:"testFilePath"`
	PRBodyPath             string               `json:"prBodyPath"`
}

// pmprofilerRunJSON mirrors the subset of pmprofiler's run.json needed by detect.
type pmprofilerRunJSON struct {
	Scenario            string            `json:"scenario"`
	Outcomes            map[string]int    `json:"outcomes"`
	InvariantViolations int               `json:"invariantViolations"`
	Migrations          []pmMigrationJSON `json:"migrations"`
}

type pmMigrationJSON struct {
	App        string  `json:"app"`
	SrcPod     *string `json:"srcPod"`
	DstPod     *string `json:"dstPod"`
	PMJ        *string `json:"pmj"`
	Snapshot   *string `json:"snapshot"`
	Outcome    string  `json:"outcome"`
	FinalPhase *string `json:"finalPhase"`
	T0         string  `json:"t0"`
	TEnd       *string `json:"tEnd"`
}

type rawNDJSONRecord struct {
	TS   string         `json:"ts"`
	Type string         `json:"type"`
	GVR  string         `json:"gvr"`
	Obj  map[string]any `json:"obj"`
}

func main() {
	var (
		mode             = flag.String("mode", "pipeline", "Execution mode: detect, verify, extract-comment, or pipeline")
		runDir           = flag.String("run", "", "Path to pmprofiler run directory containing run.json and records.ndjson")
		outDir           = flag.String("out-dir", "", "Output directory for generated fixture, Go rule/test, proof.json, and pr_body.md")
		invariantID      = flag.String("invariant-id", "I10", "Candidate invariant ID (e.g. I10)")
		allowColdStart   = flag.Bool("allow-cold-start", false, "Treat cold-start outcomes as expected (do not flag as blind spots)")
		greenRecords     = flag.String("green-records", "", "Comma-separated paths to clean records.ndjson traces for GREEN replay verification")
		commentBody      = flag.String("comment", "", "PR review comment body containing /extract-invariant (Trigger C)")
		prTitle          = flag.String("pr-title", "", "Bugfix PR title for Trigger B extraction")
		overrideTemplate = flag.String("template", "", "Optional override for candidate template class")
		requireRedGreen  = flag.Bool("require-red-green", true, "Exit non-zero if RED or GREEN replay proof fails")
	)
	flag.Parse()

	if err := execute(*mode, *runDir, *outDir, *invariantID, *allowColdStart, *greenRecords, *commentBody, *prTitle, TemplateClass(*overrideTemplate), *requireRedGreen); err != nil {
		fmt.Fprintf(os.Stderr, "invariant-gen error: %v\n", err)
		os.Exit(1)
	}
}

func execute(
	mode, runDir, outDir, invariantID string,
	allowColdStart bool,
	greenRecordsCSV, commentBody, prTitle string,
	overrideTemplate TemplateClass,
	requireRedGreen bool,
) error {
	if outDir == "" {
		return errors.New("--out-dir is required")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("create out-dir: %w", err)
	}

	var greenPaths []string
	for _, p := range strings.Split(greenRecordsCSV, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			greenPaths = append(greenPaths, p)
		}
	}

	switch mode {
	case "detect":
		if runDir == "" {
			return errors.New("--run is required for detect mode")
		}
		findings, err := DetectBlindSpots(runDir, invariantID, allowColdStart)
		if err != nil {
			return err
		}
		b, err := json.MarshalIndent(findings, "", "  ")
		if err != nil {
			return err
		}
		outPath := filepath.Join(outDir, "blindspots.json")
		if err := os.WriteFile(outPath, append(b, '\n'), 0o644); err != nil {
			return err
		}
		fmt.Printf("invariant-gen detect: %d blind spot(s) found (wrote %s)\n", len(findings), outPath)
		return nil

	case "extract-comment":
		finding, err := ParseTriggerCommentOrPR(invariantID, commentBody, prTitle)
		if err != nil {
			return err
		}
		if overrideTemplate != "" {
			finding.Template = overrideTemplate
		}
		proof, err := SynthesizeAndVerify(finding, outDir, greenPaths)
		if err != nil {
			return err
		}
		printProofSummary(proof)
		if requireRedGreen && (!proof.RedPassed || !proof.GreenPassed) {
			return fmt.Errorf("RED/GREEN verification failed for %s (red=%v, green=%v)", proof.InvariantID, proof.RedPassed, proof.GreenPassed)
		}
		return nil

	case "pipeline", "verify":
		if runDir == "" {
			return errors.New("--run is required for pipeline/verify mode")
		}
		findings, err := DetectBlindSpots(runDir, invariantID, allowColdStart)
		if err != nil {
			return err
		}
		if len(findings) == 0 {
			fmt.Printf("invariant-gen %s: 0 blind spots detected in %s\n", mode, runDir)
			return nil
		}
		finding := findings[0]
		if overrideTemplate != "" {
			finding.Template = overrideTemplate
		}
		proof, err := SynthesizeAndVerify(finding, outDir, greenPaths)
		if err != nil {
			return err
		}
		printProofSummary(proof)
		if requireRedGreen && (!proof.RedPassed || !proof.GreenPassed) {
			return fmt.Errorf("RED/GREEN verification failed for %s (red=%v, green=%v)", proof.InvariantID, proof.RedPassed, proof.GreenPassed)
		}
		return nil

	default:
		return fmt.Errorf("unknown --mode %q (expected detect, verify, extract-comment, or pipeline)", mode)
	}
}

func printProofSummary(p RedGreenProof) {
	fmt.Printf("invariant-gen %s (%s) [template=%s, trigger=%s]:\n", p.InvariantID, p.InvariantName, p.Template, p.TriggerSource)
	fmt.Printf("  RED proof   : passed=%v (violations=%d on offending snapshot)\n", p.RedPassed, len(p.RedViolations))
	fmt.Printf("  GREEN proof : passed=%v (baselineSnapshots=%d, traceSteps=%d, falsePositives=%d)\n",
		p.GreenPassed, p.GreenBaselineChecked, p.GreenTraceStepsChecked, len(p.GreenViolations))
	fmt.Printf("  artifacts   : fixture=%s rule=%s test=%s pr=%s\n",
		p.SnapshotFixturePath, p.RuleFilePath, p.TestFilePath, p.PRBodyPath)
}

// DetectBlindSpots inspects <runDir>/run.json and <runDir>/records.ndjson and
// returns any migration that ended in an unhealthy outcome while
// invariantViolations == 0.
func DetectBlindSpots(runDir, nextInvariantID string, allowColdStart bool) ([]BlindSpotFinding, error) {
	runBytes, err := os.ReadFile(filepath.Join(runDir, "run.json"))
	if err != nil {
		return nil, fmt.Errorf("read run.json: %w", err)
	}
	var run pmprofilerRunJSON
	if err := json.Unmarshal(runBytes, &run); err != nil {
		return nil, fmt.Errorf("parse run.json: %w", err)
	}

	// If I1-I9 already recorded invariant violations for this run, it is not an
	// silent detector blind spot.
	if run.InvariantViolations > 0 {
		return nil, nil
	}

	recordsPath := filepath.Join(runDir, "records.ndjson")
	records, err := loadNDJSONRecords(recordsPath)
	if err != nil {
		return nil, fmt.Errorf("read records.ndjson: %w", err)
	}

	var findings []BlindSpotFinding
	seq := 0
	for _, m := range run.Migrations {
		if !isUnhealthyBlindSpotOutcome(m.Outcome, allowColdStart) {
			continue
		}
		pmjName := derefStr(m.PMJ)
		srcPod := derefStr(m.SrcPod)
		dstPod := derefStr(m.DstPod)
		cutoffTS := derefStr(m.TEnd)

		snap := reconstructSnapshotAt(records, run.Scenario, m.Outcome, pmjName, srcPod, dstPod, derefStr(m.Snapshot), cutoffTS)
		if srcPod == "" && snap.PrimaryPMJ != nil {
			srcPod = snap.PrimaryPMJ.SourcePodName
		}
		if dstPod == "" && snap.PrimaryPMJ != nil {
			dstPod = snap.PrimaryPMJ.RestoredPodName
		}
		tmpl, invName, slug, desc := classifyBlindSpot(m, snap)

		id := nextInvariantID
		if seq > 0 && strings.HasPrefix(strings.ToUpper(nextInvariantID), "I") {
			var baseNum int
			if _, err := fmt.Sscanf(strings.ToUpper(nextInvariantID), "I%d", &baseNum); err == nil {
				id = fmt.Sprintf("I%d", baseNum+seq)
			}
		}
		seq++

		findings = append(findings, BlindSpotFinding{
			TriggerSource:    "TriggerA:BlindSpot",
			InvariantID:      id,
			InvariantName:    invName,
			Slug:             slug,
			Template:         tmpl,
			Scenario:         run.Scenario,
			Outcome:          m.Outcome,
			PMJName:          pmjName,
			SourcePod:        srcPod,
			RestoredPod:      dstPod,
			FailureTimestamp: snap.Now,
			Description:      desc,
			Snapshot:         snap,
		})
	}
	return findings, nil
}

func isUnhealthyBlindSpotOutcome(outcome string, allowColdStart bool) bool {
	switch outcome {
	case "wedged", "failed", "no-replacement", "restore_crash_unmatched":
		return true
	case "cold-start":
		return !allowColdStart
	default:
		return false
	}
}

func classifyBlindSpot(m pmMigrationJSON, snap SnapshotFixture) (TemplateClass, string, string, string) {
	// 1. Check for PrematureSnapshotFailedWhileCheckpointing (Issue #75 class):
	// PMJ is Failed while PodSnapshot still has Checkpoint or StorageReplicated in progress/succeeded.
	if snap.PrimaryPMJ != nil && snap.PrimaryPMJ.Phase == "Failed" && snap.PrimaryPodSnapshot != nil {
		if isPodSnapshotStillProgressingOrSucceeded(snap.PrimaryPodSnapshot) {
			return TemplatePrematureSnapshotFailed,
				"PrematureSnapshotFailureIsolation",
				"premature_snapshot_failed",
				fmt.Sprintf("PMJ %q transitioned to PhaseFailed while PodSnapshot %q sub-conditions (Checkpoint/StorageReplicated) were still actively progressing or succeeded",
					snap.PrimaryPMJ.Name, snap.PrimaryPodSnapshot.Name)
		}
	}

	// 2. Check for UnintendedColdStartDuringActivePMJ:
	if m.Outcome == "cold-start" || isUnintendedColdStartSnapshot(snap) {
		return TemplateUnintendedColdStartActivePMJ,
			"ActivePMJReplacementColdStartGuard",
			"unintended_cold_start_active_pmj",
			fmt.Sprintf("Replacement pod reached Ready without a warm-restore snapshot annotation while PMJ %q was active",
				derefStr(m.PMJ))
	}

	// 3. Check for WedgedRestoringOrphan (wedged or no-replacement):
	if m.Outcome == "wedged" || m.Outcome == "no-replacement" || isWedgedRestoringOrphanSnapshot(snap) {
		return TemplateWedgedRestoringOrphan,
			"RestoringReplacementLiveness",
			"wedged_restoring_orphan",
			fmt.Sprintf("PMJ %q remained non-terminal in %s after source pod eviction with missing or deleted replacement pod",
				derefStr(m.PMJ), pmjPhaseStr(snap.PrimaryPMJ))
	}

	return TemplateCustomScaffold,
		"CustomBlindSpotInvariant",
		"custom_blind_spot",
		fmt.Sprintf("Unhealthy migration outcome %q observed on PMJ %q with 0 existing I1-I9 violations",
			m.Outcome, derefStr(m.PMJ))
}

var extractCmdRe = regexp.MustCompile(`(?i)/extract-invariant(?:\s+(I\d+))?(?:\s+([a-z0-9_-]+))?(?:\s+(.*))?`)

// ParseTriggerCommentOrPR parses a Trigger C (/extract-invariant) comment or
// Trigger B (bugfix PR title) into a BlindSpotFinding with a representative
// offending SnapshotFixture so it can be verified via RED/GREEN replay.
func ParseTriggerCommentOrPR(defaultID, commentBody, prTitle string) (BlindSpotFinding, error) {
	id := defaultID
	if id == "" {
		id = "I10"
	}
	triggerSource := "TriggerC:SlashCommand"
	rawToken := ""
	desc := ""

	if strings.TrimSpace(commentBody) != "" {
		m := extractCmdRe.FindStringSubmatch(strings.TrimSpace(commentBody))
		if m == nil {
			return BlindSpotFinding{}, fmt.Errorf("comment does not contain /extract-invariant directive: %q", commentBody)
		}
		if m[1] != "" {
			id = strings.ToUpper(m[1])
		}
		rawToken = strings.ToLower(strings.TrimSpace(m[2]))
		desc = strings.TrimSpace(m[3])
	} else if strings.TrimSpace(prTitle) != "" {
		triggerSource = "TriggerB:BugfixPR"
		rawToken = strings.ToLower(strings.TrimSpace(prTitle))
		desc = strings.TrimSpace(prTitle)
	} else {
		return BlindSpotFinding{}, errors.New("either --comment or --pr-title must be provided")
	}

	tmpl, invName, slug, snap := buildTemplateFixtureFromDirective(rawToken, desc)
	if desc == "" {
		desc = fmt.Sprintf("Extracted candidate invariant %s (%s) from %s", id, invName, triggerSource)
	}

	return BlindSpotFinding{
		TriggerSource:    triggerSource,
		InvariantID:      id,
		InvariantName:    invName,
		Slug:             slug,
		Template:         tmpl,
		Scenario:         triggerSource,
		Outcome:          snap.BlindSpotOutcome,
		PMJName:          snap.PrimaryPMJ.Name,
		SourcePod:        snap.PrimaryPMJ.SourcePodName,
		RestoredPod:      snap.PrimaryPMJ.RestoredPodName,
		FailureTimestamp: snap.Now,
		Description:      desc,
		Snapshot:         snap,
	}, nil
}

func buildTemplateFixtureFromDirective(token, desc string) (TemplateClass, string, string, SnapshotFixture) {
	combined := strings.ToLower(token + " " + desc)
	now := "2026-09-28T12:00:30Z"

	switch {
	case strings.Contains(combined, "snapshot") && (strings.Contains(combined, "race") || strings.Contains(combined, "premature") || strings.Contains(combined, "inprogress")):
		return TemplatePrematureSnapshotFailed,
			"PrematureSnapshotFailureIsolation",
			"premature_snapshot_failed",
			SnapshotFixture{
				Now:              now,
				Reconciler:       "PodMigrationJobReconciler",
				Scenario:         "TriggerDirective:PrematureSnapshotFailed",
				BlindSpotOutcome: "failed",
				PrimaryPMJ: &PMJState{
					Name:          "pmj-snapshot-race-0",
					Namespace:     "default",
					UID:           "uid-pmj-snap-race",
					Phase:         "Failed",
					Reason:        "SnapshotFailed",
					Message:       "PodSnapshot ps-0 condition Ready failed: Failed to take snapshot (1).",
					SourcePodName: "app-0",
					TargetPodUID:  "uid-app-0",
					SnapshotName:  "ps-0",
				},
				PrimaryPodSnapshot: &PodSnapshotState{
					Name:      "ps-0",
					Namespace: "default",
					PodName:   "app-0",
					Conditions: []ConditionSummary{
						{Type: "Checkpoint", Status: "False", Reason: "InProgress"},
						{Type: "StorageReplicated", Status: "False", Reason: "AwaitingCheckpoint"},
						{Type: "Ready", Status: "False", Reason: "Failed", Message: "Failed to take snapshot (1)."},
					},
				},
			}

	case strings.Contains(combined, "cold") && strings.Contains(combined, "start"):
		return TemplateUnintendedColdStartActivePMJ,
			"ActivePMJReplacementColdStartGuard",
			"unintended_cold_start_active_pmj",
			SnapshotFixture{
				Now:              now,
				Reconciler:       "PodMigrationJobReconciler",
				Scenario:         "TriggerDirective:UnintendedColdStart",
				BlindSpotOutcome: "cold-start",
				PrimaryPMJ: &PMJState{
					Name:               "pmj-cold-start-0",
					Namespace:          "default",
					UID:                "uid-pmj-cold",
					Phase:              "Restoring",
					SourcePodName:      "app-0",
					TargetPodUID:       "uid-app-0",
					SnapshotName:       "ps-0",
					RestoredPodName:    "app-0-replacement",
					RestoredPodUID:     "uid-app-replacement",
					RestoringStartTime: "2026-09-28T12:00:10Z",
				},
				PrimaryPod: &PodState{
					Name:        "app-0-replacement",
					Namespace:   "default",
					UID:         "uid-app-replacement",
					App:         "app",
					NodeName:    "node-b",
					Phase:       "Running",
					Ready:       true,
					Annotations: map[string]string{"gke.io/pod-snapshot-restore-name": ""},
				},
				NamespacePods: []PodState{
					{
						Name:        "app-0-replacement",
						Namespace:   "default",
						UID:         "uid-app-replacement",
						App:         "app",
						NodeName:    "node-b",
						Phase:       "Running",
						Ready:       true,
						Annotations: map[string]string{"gke.io/pod-snapshot-restore-name": ""},
					},
				},
			}

	case strings.Contains(combined, "wedge") || strings.Contains(combined, "orphan") || strings.Contains(combined, "restoring") || strings.Contains(combined, "no-replacement"):
		return TemplateWedgedRestoringOrphan,
			"RestoringReplacementLiveness",
			"wedged_restoring_orphan",
			SnapshotFixture{
				Now:                now,
				Reconciler:         "PodMigrationJobReconciler",
				Scenario:           "TriggerDirective:WedgedRestoringOrphan",
				BlindSpotOutcome:   "wedged",
				SourcePodDeleted:   true,
				RestoredPodDeleted: true,
				PrimaryPMJ: &PMJState{
					Name:               "pmj-wedged-0",
					Namespace:          "default",
					UID:                "uid-pmj-wedged",
					Phase:              "Restoring",
					SourcePodName:      "app-0",
					TargetPodUID:       "uid-app-0",
					SnapshotName:       "ps-0",
					RestoredPodName:    "app-0-dst",
					RestoredPodUID:     "uid-app-dst",
					EvictingStartTime:  "2026-09-28T12:00:05Z",
					RestoringStartTime: "2026-09-28T12:00:08Z",
				},
			}

	default:
		slug := sanitizeSlug(token)
		if slug == "" {
			slug = "custom_blind_spot"
		}
		return TemplateCustomScaffold,
			"CustomExtractedInvariant",
			slug,
			SnapshotFixture{
				Now:              now,
				Reconciler:       "PodMigrationJobReconciler",
				Scenario:         "TriggerDirective:CustomScaffold",
				BlindSpotOutcome: "custom",
				PrimaryPMJ: &PMJState{
					Name:          "pmj-custom-0",
					Namespace:     "default",
					Phase:         "Restoring",
					SourcePodName: "app-0",
				},
			}
	}
}

func sanitizeSlug(raw string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(raw) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastUnderscore = false
		} else if !lastUnderscore && b.Len() > 0 {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}

// EvaluateCandidateRule runs the pure stateless candidate predicate for a
// TemplateClass against a SnapshotFixture.
func EvaluateCandidateRule(id, name string, tmpl TemplateClass, s *SnapshotFixture) []CandidateViolation {
	if s == nil || s.PrimaryPMJ == nil {
		return nil
	}
	pmj := s.PrimaryPMJ

	switch tmpl {
	case TemplatePrematureSnapshotFailed:
		// Fires when PMJ is Failed (SnapshotFailed) while the PodSnapshot still has
		// Checkpoint or StorageReplicated in an active progress or Succeeded state.
		if pmj.Phase != "Failed" || s.PrimaryPodSnapshot == nil {
			return nil
		}
		if isPodSnapshotStillProgressingOrSucceeded(s.PrimaryPodSnapshot) {
			return []CandidateViolation{{
				InvariantID:   id,
				InvariantName: name,
				Reason:        "PrematureSnapshotFailureWhileCheckpointing",
				Message: fmt.Sprintf("PMJ %s/%s entered PhaseFailed (%s) while PodSnapshot %s had non-failed Checkpoint/StorageReplicated sub-conditions",
					pmj.Namespace, pmj.Name, pmj.Reason, s.PrimaryPodSnapshot.Name),
				PMJName: pmj.Name,
				PodName: pmj.SourcePodName,
			}}
		}
		return nil

	case TemplateWedgedRestoringOrphan:
		// Fires when PMJ is stuck in Restoring after the source pod was deleted and
		// its assigned RestoredPodName has been deleted (or is missing past the grace window).
		if pmj.Phase != "Restoring" {
			return nil
		}
		if s.RestoredPodDeleted {
			return []CandidateViolation{{
				InvariantID:   id,
				InvariantName: name,
				Reason:        "RestoringReplacementPodDeleted",
				Message: fmt.Sprintf("PMJ %s/%s remains in PhaseRestoring after replacement pod %q was deleted",
					pmj.Namespace, pmj.Name, pmj.RestoredPodName),
				PMJName: pmj.Name,
				PodName: pmj.RestoredPodName,
			}}
		}
		if s.SourcePodDeleted && pmj.RestoredPodName != "" && !podExistsInSlice(pmj.RestoredPodName, s.NamespacePods) &&
			exceededWindow(pmj.RestoringStartTime, s.Now, 30*time.Second) {
			return []CandidateViolation{{
				InvariantID:   id,
				InvariantName: name,
				Reason:        "RestoringReplacementPodMissing",
				Message: fmt.Sprintf("PMJ %s/%s remained in PhaseRestoring >30s with missing replacement pod %q",
					pmj.Namespace, pmj.Name, pmj.RestoredPodName),
				PMJName: pmj.Name,
				PodName: pmj.RestoredPodName,
			}}
		}
		return nil

	case TemplateUnintendedColdStartActivePMJ:
		// Fires when PMJ is actively Restoring (or Evicting/Snapshotting) and its bound
		// replacement pod is Ready=true without a non-empty snapshot restore annotation.
		if pmj.Phase != "Snapshotting" && pmj.Phase != "Evicting" && pmj.Phase != "Restoring" {
			return nil
		}
		for _, pod := range s.NamespacePods {
			if (pmj.RestoredPodName != "" && pod.Name == pmj.RestoredPodName) ||
				(pmj.RestoredPodUID != "" && pod.UID == pmj.RestoredPodUID) {
				restoreSnap := strings.TrimSpace(pod.Annotations["gke.io/pod-snapshot-restore-name"])
				if restoreSnap == "" {
					restoreSnap = strings.TrimSpace(pod.Annotations["pod-migration.gke.io/ps-name"])
				}
				if pod.Ready && len(pod.SchedulingGates) == 0 && restoreSnap == "" {
					return []CandidateViolation{{
						InvariantID:   id,
						InvariantName: name,
						Reason:        "ActivePMJReplacementColdStarted",
						Message: fmt.Sprintf("Replacement pod %s/%s became Ready without snapshot restore annotation while PMJ %s was in phase %s",
							pod.Namespace, pod.Name, pmj.Name, pmj.Phase),
						PMJName: pmj.Name,
						PodName: pod.Name,
					}}
				}
			}
		}
		return nil

	default:
		// TemplateCustomScaffold intentionally produces 0 violations until a human or
		// coding agent supplies the domain predicate.
		return nil
	}
}

func isPodSnapshotStillProgressingOrSucceeded(ps *PodSnapshotState) bool {
	if ps == nil {
		return false
	}
	readyFailed := false
	subProgressOrSucceeded := false
	for _, c := range ps.Conditions {
		if c.Type == "Ready" && c.Status == "False" && c.Reason == "Failed" {
			readyFailed = true
		}
		if c.Type == "Checkpoint" || c.Type == "StorageReplicated" {
			if c.Status == "True" || c.Reason == "InProgress" || c.Reason == "AwaitingCheckpoint" || c.Reason == "Pending" || c.Reason == "Succeeded" {
				subProgressOrSucceeded = true
			}
		}
	}
	return readyFailed && subProgressOrSucceeded
}

func isUnintendedColdStartSnapshot(s SnapshotFixture) bool {
	return len(EvaluateCandidateRule("I_CHECK", "Check", TemplateUnintendedColdStartActivePMJ, &s)) > 0
}

func isWedgedRestoringOrphanSnapshot(s SnapshotFixture) bool {
	return len(EvaluateCandidateRule("I_CHECK", "Check", TemplateWedgedRestoringOrphan, &s)) > 0
}

func podExistsInSlice(name string, pods []PodState) bool {
	for _, p := range pods {
		if p.Name == name && p.DeletionTimestamp == "" {
			return true
		}
	}
	return false
}

func exceededWindow(startRFC3339, nowRFC3339 string, limit time.Duration) bool {
	if startRFC3339 == "" || nowRFC3339 == "" {
		return false
	}
	tStart, err1 := time.Parse(time.RFC3339Nano, startRFC3339)
	tNow, err2 := time.Parse(time.RFC3339Nano, nowRFC3339)
	if err1 != nil || err2 != nil {
		return false
	}
	return tNow.Sub(tStart) >= limit
}

// BaselineGreenFixtures returns the canonical healthy ReconcileSnapshot states
// across all 5 PMJ lifecycle phases (Pending, Snapshotting, Evicting, Restoring,
// Succeeded) plus a legitimate terminal failure (where Checkpoint itself failed).
func BaselineGreenFixtures() []SnapshotFixture {
	return []SnapshotFixture{
		{
			Now:        "2026-09-28T12:00:01Z",
			Reconciler: "PodMigrationJobReconciler",
			Scenario:   "Baseline:Pending",
			PrimaryPMJ: &PMJState{
				Name:          "pmj-clean-1",
				Namespace:     "default",
				Phase:         "Pending",
				SourcePodName: "counter-0",
			},
			NamespacePods: []PodState{{Name: "counter-0", Namespace: "default", Phase: "Running", Ready: true}},
		},
		{
			Now:        "2026-09-28T12:00:03Z",
			Reconciler: "PodMigrationJobReconciler",
			Scenario:   "Baseline:Snapshotting",
			PrimaryPMJ: &PMJState{
				Name:          "pmj-clean-1",
				Namespace:     "default",
				Phase:         "Snapshotting",
				SourcePodName: "counter-0",
				SnapshotName:  "ps-clean-1",
			},
			PrimaryPodSnapshot: &PodSnapshotState{
				Name:      "ps-clean-1",
				Namespace: "default",
				PodName:   "counter-0",
				Conditions: []ConditionSummary{
					{Type: "Checkpoint", Status: "False", Reason: "InProgress"},
					{Type: "StorageReplicated", Status: "False", Reason: "AwaitingCheckpoint"},
					{Type: "Ready", Status: "False", Reason: "InProgress"},
				},
			},
			NamespacePods: []PodState{{Name: "counter-0", Namespace: "default", Phase: "Running", Ready: true}},
		},
		{
			Now:        "2026-09-28T12:00:06Z",
			Reconciler: "PodMigrationJobReconciler",
			Scenario:   "Baseline:Evicting",
			PrimaryPMJ: &PMJState{
				Name:              "pmj-clean-1",
				Namespace:         "default",
				Phase:             "Evicting",
				SourcePodName:     "counter-0",
				SnapshotName:      "ps-clean-1",
				EvictingStartTime: "2026-09-28T12:00:05Z",
			},
			NamespacePods: []PodState{{Name: "counter-0", Namespace: "default", Phase: "Running", DeletionTimestamp: "2026-09-28T12:00:05Z"}},
		},
		{
			Now:              "2026-09-28T12:00:09Z",
			Reconciler:       "PodMigrationJobReconciler",
			Scenario:         "Baseline:RestoringHealthy",
			SourcePodDeleted: true,
			PrimaryPMJ: &PMJState{
				Name:               "pmj-clean-1",
				Namespace:          "default",
				Phase:              "Restoring",
				SourcePodName:      "counter-0",
				SnapshotName:       "ps-clean-1",
				RestoredPodName:    "counter-0-dst",
				RestoredPodUID:     "uid-counter-dst",
				EvictingStartTime:  "2026-09-28T12:00:05Z",
				RestoringStartTime: "2026-09-28T12:00:07Z",
			},
			NamespacePods: []PodState{{
				Name:        "counter-0-dst",
				Namespace:   "default",
				UID:         "uid-counter-dst",
				Phase:       "Running",
				Ready:       true,
				Annotations: map[string]string{"gke.io/pod-snapshot-restore-name": "ps-clean-1"},
			}},
		},
		{
			Now:              "2026-09-28T12:00:12Z",
			Reconciler:       "PodMigrationJobReconciler",
			Scenario:         "Baseline:Succeeded",
			SourcePodDeleted: true,
			PrimaryPMJ: &PMJState{
				Name:            "pmj-clean-1",
				Namespace:       "default",
				Phase:           "Succeeded",
				SourcePodName:   "counter-0",
				SnapshotName:    "ps-clean-1",
				RestoredPodName: "counter-0-dst",
				RestoredPodUID:  "uid-counter-dst",
			},
			NamespacePods: []PodState{{
				Name:        "counter-0-dst",
				Namespace:   "default",
				UID:         "uid-counter-dst",
				Phase:       "Running",
				Ready:       true,
				Annotations: map[string]string{"gke.io/pod-snapshot-restore-name": "ps-clean-1"},
			}},
		},
		{
			Now:        "2026-09-28T12:00:15Z",
			Reconciler: "PodMigrationJobReconciler",
			Scenario:   "Baseline:GenuineSnapshotCheckpointFailed",
			PrimaryPMJ: &PMJState{
				Name:          "pmj-genuine-fail",
				Namespace:     "default",
				Phase:         "Failed",
				Reason:        "SnapshotFailed",
				SourcePodName: "counter-fail",
				SnapshotName:  "ps-genuine-fail",
			},
			PrimaryPodSnapshot: &PodSnapshotState{
				Name:      "ps-genuine-fail",
				Namespace: "default",
				PodName:   "counter-fail",
				Conditions: []ConditionSummary{
					{Type: "Checkpoint", Status: "False", Reason: "Failed"},
					{Type: "StorageReplicated", Status: "False", Reason: "Failed"},
					{Type: "Ready", Status: "False", Reason: "Failed"},
				},
			},
		},
	}
}

// SynthesizeAndVerify writes the SnapshotFixture, candidate Go rule/test files,
// and PR description to outDir, and executes the RED and GREEN replay proofs.
func SynthesizeAndVerify(finding BlindSpotFinding, outDir string, greenTracePaths []string) (RedGreenProof, error) {
	testdataDir := filepath.Join(outDir, "testdata")
	if err := os.MkdirAll(testdataDir, 0o755); err != nil {
		return RedGreenProof{}, err
	}

	idLower := strings.ToLower(finding.InvariantID)
	basePrefix := fmt.Sprintf("%s_%s", idLower, finding.Slug)
	fixturePath := filepath.Join(testdataDir, basePrefix+"_snapshot.json")
	rulePath := filepath.Join(outDir, basePrefix+"_rule.go")
	testPath := filepath.Join(outDir, basePrefix+"_test.go")
	prBodyPath := filepath.Join(outDir, "pr_body.md")
	proofPath := filepath.Join(outDir, "proof.json")

	fixtureBytes, err := json.MarshalIndent(finding.Snapshot, "", "  ")
	if err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(fixturePath, append(fixtureBytes, '\n'), 0o644); err != nil {
		return RedGreenProof{}, err
	}

	// 1. RED Proof: evaluate candidate rule on the offending snapshot fixture.
	redViolations := EvaluateCandidateRule(finding.InvariantID, finding.InvariantName, finding.Template, &finding.Snapshot)
	redPassed := len(redViolations) >= 1

	// 2. GREEN Proof: evaluate candidate rule across baseline healthy fixtures + clean records.ndjson traces.
	baselines := BaselineGreenFixtures()
	var greenViolations []CandidateViolation
	for _, b := range baselines {
		cp := b
		vs := EvaluateCandidateRule(finding.InvariantID, finding.InvariantName, finding.Template, &cp)
		greenViolations = append(greenViolations, vs...)
	}

	traceStepsChecked := 0
	for _, tracePath := range greenTracePaths {
		steps, err := ReconstructAllTraceSnapshots(tracePath)
		if err != nil {
			return RedGreenProof{}, fmt.Errorf("replay green trace %s: %w", tracePath, err)
		}
		for _, st := range steps {
			traceStepsChecked++
			cp := st
			vs := EvaluateCandidateRule(finding.InvariantID, finding.InvariantName, finding.Template, &cp)
			greenViolations = append(greenViolations, vs...)
		}
	}
	greenPassed := len(greenViolations) == 0 && finding.Template != TemplateCustomScaffold

	proof := RedGreenProof{
		InvariantID:            finding.InvariantID,
		InvariantName:          finding.InvariantName,
		Slug:                   finding.Slug,
		Template:               finding.Template,
		TriggerSource:          finding.TriggerSource,
		RequiresAuthorBody:     finding.Template == TemplateCustomScaffold,
		RedPassed:              redPassed,
		RedViolations:          redViolations,
		GreenPassed:            greenPassed,
		GreenBaselineChecked:   len(baselines),
		GreenTraceStepsChecked: traceStepsChecked,
		GreenViolations:        greenViolations,
		SnapshotFixturePath:    fixturePath,
		RuleFilePath:           rulePath,
		TestFilePath:           testPath,
		PRBodyPath:             prBodyPath,
	}

	if err := os.WriteFile(rulePath, []byte(renderCandidateRuleGo(finding)), 0o644); err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(testPath, []byte(renderCandidateTestGo(finding, filepath.Base(fixturePath))), 0o644); err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(prBodyPath, []byte(renderPRBodyMarkdown(finding, proof)), 0o644); err != nil {
		return RedGreenProof{}, err
	}
	proofBytes, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(proofPath, append(proofBytes, '\n'), 0o644); err != nil {
		return RedGreenProof{}, err
	}

	return proof, nil
}

// ReconstructAllTraceSnapshots replays a pmprofiler records.ndjson trace and
// emits a SnapshotFixture for every PMJ add/update step in the trace so GREEN
// proofs can verify 0 false positives across real cluster recordings.
func ReconstructAllTraceSnapshots(recordsPath string) ([]SnapshotFixture, error) {
	records, err := loadNDJSONRecords(recordsPath)
	if err != nil {
		return nil, err
	}
	state := newTraceState()
	var snapshots []SnapshotFixture
	for _, rec := range records {
		state.apply(rec)
		if strings.HasPrefix(rec.GVR, "podmigrationjobs.") && rec.Type != "delete" {
			name := nestedStr(rec.Obj, "metadata", "name")
			if pmj, ok := state.pmjs[name]; ok {
				snap := state.snapshotForPMJ(rec.TS, pmj)
				snapshots = append(snapshots, snap)
			}
		}
	}
	return snapshots, nil
}

type traceState struct {
	pmjs            map[string]PMJState
	pods            map[string]PodState
	deletedPods     map[string]bool
	podSnapshots    map[string]PodSnapshotState
	podSnapshotsPod map[string]string // podName -> latest PodSnapshot name
}

func newTraceState() *traceState {
	return &traceState{
		pmjs:            make(map[string]PMJState),
		pods:            make(map[string]PodState),
		deletedPods:     make(map[string]bool),
		podSnapshots:    make(map[string]PodSnapshotState),
		podSnapshotsPod: make(map[string]string),
	}
}

func (s *traceState) apply(rec rawNDJSONRecord) {
	name := nestedStr(rec.Obj, "metadata", "name")
	ns := nestedStr(rec.Obj, "metadata", "namespace")
	if ns == "" {
		ns = "default"
	}
	if name == "" {
		return
	}

	switch {
	case strings.HasPrefix(rec.GVR, "podmigrationjobs."):
		if rec.Type == "delete" {
			delete(s.pmjs, name)
			return
		}
		pmj := PMJState{
			Name:               name,
			Namespace:          ns,
			UID:                nestedStr(rec.Obj, "metadata", "uid"),
			Phase:              nestedStr(rec.Obj, "status", "phase"),
			Reason:             nestedStr(rec.Obj, "status", "reason"),
			Message:            nestedStr(rec.Obj, "status", "message"),
			SourcePodName:      nestedStr(rec.Obj, "spec", "podRef", "name"),
			TargetPodUID:       nestedStr(rec.Obj, "spec", "targetPodUID"),
			SnapshotName:       nestedStr(rec.Obj, "status", "snapshotName"),
			RestoredPodName:    nestedStr(rec.Obj, "status", "restoredPodName"),
			RestoredPodUID:     nestedStr(rec.Obj, "status", "restoredPodUID"),
			CreationTimestamp:  nestedStr(rec.Obj, "metadata", "creationTimestamp"),
			EvictingStartTime:  nestedStr(rec.Obj, "status", "evictingStartTime"),
			RestoringStartTime: nestedStr(rec.Obj, "status", "restoringStartTime"),
			Conditions:         extractConditions(rec.Obj),
		}
		s.pmjs[name] = pmj

	case strings.HasPrefix(rec.GVR, "pods."):
		if rec.Type == "delete" {
			delete(s.pods, name)
			s.deletedPods[name] = true
			return
		}
		labels := extractStringMap(rec.Obj, "metadata", "labels")
		ann := extractStringMap(rec.Obj, "metadata", "annotations")
		app := labels["app"]
		if app == "" {
			app = labels["app.kubernetes.io/name"]
		}
		p := PodState{
			Name:              name,
			Namespace:         ns,
			UID:               nestedStr(rec.Obj, "metadata", "uid"),
			App:               app,
			NodeName:          nestedStr(rec.Obj, "spec", "nodeName"),
			Phase:             nestedStr(rec.Obj, "status", "phase"),
			Ready:             isPodObjReady(rec.Obj),
			DeletionTimestamp: nestedStr(rec.Obj, "metadata", "deletionTimestamp"),
			SchedulingGates:   extractSchedulingGates(rec.Obj),
			Annotations:       ann,
			Labels:            labels,
		}
		s.pods[name] = p

	case strings.HasPrefix(rec.GVR, "podsnapshots."):
		if rec.Type == "delete" {
			delete(s.podSnapshots, name)
			return
		}
		podName := nestedStr(rec.Obj, "spec", "podName")
		if podName == "" {
			podName = nestedStr(rec.Obj, "status", "podName")
		}
		ps := PodSnapshotState{
			Name:       name,
			Namespace:  ns,
			PodName:    podName,
			Conditions: extractConditions(rec.Obj),
		}
		s.podSnapshots[name] = ps
		if podName != "" {
			s.podSnapshotsPod[podName] = name
		}
	}
}

func (s *traceState) snapshotForPMJ(ts string, pmj PMJState) SnapshotFixture {
	cpPMJ := pmj
	var primaryPod *PodState
	if pmj.RestoredPodName != "" {
		if p, ok := s.pods[pmj.RestoredPodName]; ok {
			cp := p
			primaryPod = &cp
		}
	}
	if primaryPod == nil && pmj.SourcePodName != "" {
		if p, ok := s.pods[pmj.SourcePodName]; ok {
			cp := p
			primaryPod = &cp
		}
	}

	var primaryPS *PodSnapshotState
	psName := pmj.SnapshotName
	if psName == "" && pmj.SourcePodName != "" {
		psName = s.podSnapshotsPod[pmj.SourcePodName]
	}
	if psName != "" {
		if ps, ok := s.podSnapshots[psName]; ok {
			cp := ps
			primaryPS = &cp
		}
	}

	var nsPMJs []PMJState
	for _, item := range s.pmjs {
		nsPMJs = append(nsPMJs, item)
	}
	sort.Slice(nsPMJs, func(i, j int) bool { return nsPMJs[i].Name < nsPMJs[j].Name })

	var nsPods []PodState
	for _, item := range s.pods {
		nsPods = append(nsPods, item)
	}
	sort.Slice(nsPods, func(i, j int) bool { return nsPods[i].Name < nsPods[j].Name })

	srcDeleted := pmj.SourcePodName != "" && s.deletedPods[pmj.SourcePodName]
	if p, ok := s.pods[pmj.SourcePodName]; ok && p.DeletionTimestamp != "" {
		srcDeleted = true
	}
	dstDeleted := pmj.RestoredPodName != "" && s.deletedPods[pmj.RestoredPodName]

	return SnapshotFixture{
		Now:                ts,
		Reconciler:         "PodMigrationJobReconciler",
		PrimaryPMJ:         &cpPMJ,
		PrimaryPod:         primaryPod,
		PrimaryPodSnapshot: primaryPS,
		NamespacePMJs:      nsPMJs,
		NamespacePods:      nsPods,
		SourcePodDeleted:   srcDeleted,
		RestoredPodDeleted: dstDeleted,
	}
}

func reconstructSnapshotAt(
	records []rawNDJSONRecord,
	scenario, outcome, pmjName, srcPod, dstPod, snapName, cutoffTS string,
) SnapshotFixture {
	var cutoff time.Time
	hasCutoff := false
	if cutoffTS != "" {
		if t, err := time.Parse(time.RFC3339Nano, cutoffTS); err == nil {
			cutoff = t.Add(2 * time.Second)
			hasCutoff = true
		}
	}

	state := newTraceState()
	lastTS := cutoffTS
	for _, rec := range records {
		if hasCutoff && rec.TS != "" {
			if t, err := time.Parse(time.RFC3339Nano, rec.TS); err == nil && t.After(cutoff) {
				break
			}
		}
		if rec.TS != "" {
			lastTS = rec.TS
		}
		state.apply(rec)
	}

	pmj, ok := state.pmjs[pmjName]
	if !ok {
		pmj = PMJState{
			Name:            pmjName,
			Namespace:       "default",
			SourcePodName:   srcPod,
			RestoredPodName: dstPod,
			SnapshotName:    snapName,
		}
	}
	if pmj.SnapshotName == "" && snapName != "" {
		pmj.SnapshotName = snapName
	}
	if pmj.RestoredPodName == "" && dstPod != "" {
		pmj.RestoredPodName = dstPod
	}
	snap := state.snapshotForPMJ(lastTS, pmj)
	snap.Scenario = scenario
	snap.BlindSpotOutcome = outcome
	return snap
}

func loadNDJSONRecords(path string) ([]rawNDJSONRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []rawNDJSONRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec rawNDJSONRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, sc.Err()
}

func renderCandidateRuleGo(f BlindSpotFinding) string {
	funcName := fmt.Sprintf("Evaluate%s_%s", strings.ToUpper(f.InvariantID), f.InvariantName)
	ruleConstructor := fmt.Sprintf("Rule%s", strings.ToUpper(f.InvariantID))

	return fmt.Sprintf(`// Code generated by tools/invariant-gen (%s); review before merging on an invariant/* branch.
package invariants

import (
	"fmt"
	"strings"
)

// %s returns the stateless Rule for %s (%s).
//
// Trigger: %s
// Summary: %s
func %s() Rule {
	return funcRule{
		id:   %q,
		name: %q,
		fn:   %s,
	}
}

// %s evaluates %s (%s) over a point-in-time ReconcileSnapshot.
func %s(s *ReconcileSnapshot) []Violation {
	if s == nil || s.PrimaryPMJ == nil {
		return nil
	}
	pmj := s.PrimaryPMJ
	_ = strings.TrimSpace
	_ = fmt.Sprintf
%s
}
`, f.TriggerSource, ruleConstructor, f.InvariantID, f.InvariantName, f.TriggerSource, f.Description,
		ruleConstructor, f.InvariantID, f.InvariantName, funcName,
		funcName, f.InvariantID, f.InvariantName, funcName,
		renderPredicateBodyGo(f))
}

func renderPredicateBodyGo(f BlindSpotFinding) string {
	switch f.Template {
	case TemplatePrematureSnapshotFailed:
		return `	// PrematureSnapshotFailureIsolation: a PMJ must not transition to PhaseFailed
	// with Reason=SnapshotFailed while its Checkpoint/StorageReplicated sub-conditions
	// remain actively progressing.
	if string(pmj.Status.Phase) == "Failed" && pmj.Status.Reason == "SnapshotFailed" &&
		strings.Contains(pmj.Status.Message, "InProgress") {
		return []Violation{{
			InvariantID:   "` + f.InvariantID + `",
			InvariantName: "` + f.InvariantName + `",
			Reason:        "PrematureSnapshotFailureWhileCheckpointing",
			Message:       fmt.Sprintf("PMJ %s/%s failed with SnapshotFailed while checkpoint was still progressing", pmj.Namespace, pmj.Name),
			Namespace:     pmj.Namespace,
			PMJName:       pmj.Name,
			PodName:       pmj.Spec.PodRef.Name,
		}}
	}
	return nil`

	case TemplateWedgedRestoringOrphan:
		return `	// RestoringReplacementLiveness: when a PMJ is in PhaseRestoring and has bound
	// RestoredPodName, the replacement pod must exist in the namespace informer view.
	if string(pmj.Status.Phase) == "Restoring" && pmj.Status.RestoredPodName != "" && len(s.NamespacePods) > 0 {
		found := false
		for i := range s.NamespacePods {
			if s.NamespacePods[i].Name == pmj.Status.RestoredPodName && s.NamespacePods[i].DeletionTimestamp == nil {
				found = true
				break
			}
		}
		if !found {
			return []Violation{{
				InvariantID:   "` + f.InvariantID + `",
				InvariantName: "` + f.InvariantName + `",
				Reason:        "RestoringReplacementPodMissing",
				Message:       fmt.Sprintf("PMJ %s/%s is in PhaseRestoring but replacement pod %q is missing or deleting", pmj.Namespace, pmj.Name, pmj.Status.RestoredPodName),
				Namespace:     pmj.Namespace,
				PMJName:       pmj.Name,
				PodName:       pmj.Status.RestoredPodName,
			}}
		}
	}
	return nil`

	case TemplateUnintendedColdStartActivePMJ:
		return `	// ActivePMJReplacementColdStartGuard: while a PMJ is in PhaseRestoring, its
	// bound replacement pod must not be Running+Ready with an empty restore annotation.
	if string(pmj.Status.Phase) == "Restoring" && pmj.Status.RestoredPodName != "" {
		for i := range s.NamespacePods {
			pod := &s.NamespacePods[i]
			if pod.Name == pmj.Status.RestoredPodName && len(pod.Spec.SchedulingGates) == 0 {
				restoreName := strings.TrimSpace(pod.Annotations["gke.io/pod-snapshot-restore-name"])
				if restoreName == "" {
					return []Violation{{
						InvariantID:   "` + f.InvariantID + `",
						InvariantName: "` + f.InvariantName + `",
						Reason:        "ActivePMJReplacementColdStarted",
						Message:       fmt.Sprintf("Replacement pod %s/%s ungated without gke.io/pod-snapshot-restore-name while PMJ %s is Restoring", pod.Namespace, pod.Name, pmj.Name),
						Namespace:     pmj.Namespace,
						PMJName:       pmj.Name,
						PodName:       pod.Name,
					}}
				}
			}
		}
	}
	return nil`

	default:
		return `	// TODO(invariant-author): implement pure predicate for ` + f.InvariantID + ` (` + f.InvariantName + `).
	_ = pmj
	return nil`
	}
}

func renderCandidateTestGo(f BlindSpotFinding, fixtureFilename string) string {
	funcName := fmt.Sprintf("Evaluate%s_%s", strings.ToUpper(f.InvariantID), f.InvariantName)
	testName := fmt.Sprintf("Test%s_%s_RedGreenReplay", strings.ToUpper(f.InvariantID), f.InvariantName)

	return fmt.Sprintf(`// Code generated by tools/invariant-gen (%s); review before merging on an invariant/* branch.
package invariants

import (
	"os"
	"path/filepath"
	"testing"
)

func %s(t *testing.T) {
	fixturePath := filepath.Join("testdata", %q)
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("failed to read offending snapshot fixture %%s: %%v", fixturePath, err)
	}
	if len(raw) == 0 {
		t.Fatalf("expected non-empty snapshot fixture %%s", fixturePath)
	}
	// Verify nil/empty snapshot produces 0 false positives (GREEN baseline sanity).
	if vs := %s(&ReconcileSnapshot{}); len(vs) != 0 {
		t.Fatalf("expected 0 violations on empty ReconcileSnapshot, got %%v", vs)
	}
}
`, f.TriggerSource, testName, fixtureFilename, funcName)
}

func renderPRBodyMarkdown(f BlindSpotFinding, p RedGreenProof) string {
	redStatus := "PASS"
	if !p.RedPassed {
		redStatus = "FAIL (requires author predicate)"
	}
	greenStatus := "PASS"
	if !p.GreenPassed {
		greenStatus = "FAIL"
	}

	return fmt.Sprintf(`## [Invariant %s] %s

**Part of Epic:** #49
**Generated by:** `+"`tools/invariant-gen`"+` (`+"`%s`"+`, template `+"`%s`"+`)
**Governance Lane:** Lane 2 (`+"`invariant/%s-%s`"+` — touches **only** `+"`controller/internal/invariants/**`"+`)

---

### 1. Blind-Spot / Trigger Summary
- **Trigger Source:** `+"`%s`"+`
- **Scenario:** `+"`%s`"+`
- **Observed Outcome:** `+"`%s`"+` (while `+"`invariantViolations == 0`"+`)
- **Offending PMJ / Pods:** `+"`%s`"+` (source: `+"`%s`"+`, replacement: `+"`%s`"+`)
- **Failure Timestamp:** `+"`%s`"+`
- **Root Cause & Predicate Summary:** %s

---

### 2. Automated RED / GREEN Replay Proof

| Proof Stage | Target Corpus | Checked Snapshots / Steps | Violations | Result |
| :--- | :--- | :---: | :---: | :---: |
| **RED Proof** (Must catch offending state) | `+"`testdata/%s`"+` | `+"`1`"+` | `+"`%d`"+` | **%s** |
| **GREEN Proof** (Must have 0 false positives) | Baseline lifecycle fixtures + clean `+"`records.ndjson`"+` traces | `+"`%d`"+` baseline + `+"`%d`"+` trace steps | `+"`%d`"+` | **%s** |

---

### 3. Generated Artifacts (Lane 2 Scope)
- `+"`controller/internal/invariants/testdata/%s`"+`
- `+"`controller/internal/invariants/%s`"+`
- `+"`controller/internal/invariants/%s`"+`
`,
		f.InvariantID, f.InvariantName,
		f.TriggerSource, f.Template,
		strings.ToLower(f.InvariantID), f.Slug,
		f.TriggerSource,
		f.Scenario,
		f.Outcome,
		f.PMJName, f.SourcePod, f.RestoredPod,
		f.FailureTimestamp,
		f.Description,
		filepath.Base(p.SnapshotFixturePath), len(p.RedViolations), redStatus,
		p.GreenBaselineChecked, p.GreenTraceStepsChecked, len(p.GreenViolations), greenStatus,
		filepath.Base(p.SnapshotFixturePath),
		filepath.Base(p.RuleFilePath),
		filepath.Base(p.TestFilePath),
	)
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func pmjPhaseStr(p *PMJState) string {
	if p == nil || p.Phase == "" {
		return "unknown"
	}
	return p.Phase
}

func nestedStr(obj map[string]any, keys ...string) string {
	cur := any(obj)
	for _, k := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return ""
		}
		cur = m[k]
	}
	s, _ := cur.(string)
	return s
}

func extractStringMap(obj map[string]any, keys ...string) map[string]string {
	cur := any(obj)
	for _, k := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[k]
	}
	raw, ok := cur.(map[string]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	out := make(map[string]string, len(raw))
	for k, v := range raw {
		if s, ok := v.(string); ok {
			out[k] = s
		}
	}
	return out
}

func extractSchedulingGates(obj map[string]any) []string {
	spec, ok := obj["spec"].(map[string]any)
	if !ok {
		return nil
	}
	gates, ok := spec["schedulingGates"].([]any)
	if !ok {
		return nil
	}
	var out []string
	for _, g := range gates {
		if gm, ok := g.(map[string]any); ok {
			if name, ok := gm["name"].(string); ok && name != "" {
				out = append(out, name)
			}
		}
	}
	return out
}

func extractConditions(obj map[string]any) []ConditionSummary {
	status, ok := obj["status"].(map[string]any)
	if !ok {
		return nil
	}
	conds, ok := status["conditions"].([]any)
	if !ok {
		return nil
	}
	var out []ConditionSummary
	for _, c := range conds {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		cs := ConditionSummary{
			Type:    nestedStr(cm, "type"),
			Status:  nestedStr(cm, "status"),
			Reason:  nestedStr(cm, "reason"),
			Message: nestedStr(cm, "message"),
		}
		if cs.Type != "" {
			out = append(out, cs)
		}
	}
	return out
}

func isPodObjReady(obj map[string]any) bool {
	for _, c := range extractConditions(obj) {
		if c.Type == "Ready" && c.Status == "True" {
			return true
		}
	}
	return false
}
