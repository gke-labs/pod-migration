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
// ReconcileSnapshot Fixture Extractor, and Go-Test Red/Green Replay Verifier for
// the GKE Live Pod Migration (LPM) Correctness Guard Rail (Part of Issue #54 / Epic #49).
//
// Scope & Honesty Note:
//   - invariant-gen is a deterministic offline CLI; it does NOT invoke an LLM,
//     synthesize arbitrary Go AST predicates from free-form prose, or automatically
//     push branches / open GitHub PRs (the bot PR-creation half of #54 remains open).
//   - Every emitted candidate rule (i<N>_<slug>_rule.go), test (i<N>_<slug>_test.go),
//     and ReconcileSnapshot fixture (testdata/i<N>_<slug>_snapshot.json) is staged
//     into a temporary copy of controller/internal/invariants and compiled + executed
//     via `go test` against the real controller/api/v1alpha1 and invariants types.
//     RedPassed and GreenPassed reflect the actual `go test` outcome on the emitted code.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"
)

// TemplateClass identifies the structural predicate template for a candidate invariant.
type TemplateClass string

const (
	// TemplateWedgedRestoringOrphan catches PMJs stuck in PhaseRestoring >=30s
	// after the bound RestoredPodName is missing or deleting in NamespacePods.
	TemplateWedgedRestoringOrphan TemplateClass = "wedged-restoring-orphan"

	// TemplateNoReplacementEvictingStall catches PMJs stuck in PhaseEvicting >=30s
	// after the source pod is gone from NamespacePods with no RestoredPodName bound.
	TemplateNoReplacementEvictingStall TemplateClass = "no-replacement-evicting-stall"

	// TemplateUnintendedColdStartActivePMJ catches replacement pods reaching Ready
	// with no scheduling gates and an empty snapshot restore annotation while the
	// PMJ is still in PhaseRestoring.
	TemplateUnintendedColdStartActivePMJ TemplateClass = "unintended-cold-start-active-pmj"

	// TemplatePrematureSnapshotFailed catches PMJs that transitioned to PhaseFailed
	// with Reason=SnapshotFailed while PrimarySnapshotConditions shows Ready=False
	// alongside an in-progress Checkpoint or StorageReplicated sub-condition.
	TemplatePrematureSnapshotFailed TemplateClass = "premature-snapshot-failed"

	// TemplateCustomScaffold is emitted when a blind spot or novel
	// /extract-invariant directive requires a human/agent author to supply the
	// predicate body.
	TemplateCustomScaffold TemplateClass = "custom-invariant-scaffold"
)

// K8sObjectMeta matches the JSON serialization of metav1.ObjectMeta fields used
// by invariants.ReconcileSnapshot.
type K8sObjectMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace,omitempty"`
	UID               string            `json:"uid,omitempty"`
	CreationTimestamp string            `json:"creationTimestamp,omitempty"`
	DeletionTimestamp string            `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
}

// K8sCondition matches metav1.Condition / corev1.PodCondition JSON fields.
type K8sCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

// K8sLocalObjectRef matches corev1.LocalObjectReference.
type K8sLocalObjectRef struct {
	Name string `json:"name"`
}

// K8sPMJSpec matches pmv1alpha1.PodMigrationJobSpec.
type K8sPMJSpec struct {
	PodRef       K8sLocalObjectRef `json:"podRef"`
	TargetPodUID string            `json:"targetPodUID,omitempty"`
}

// K8sPMJStatus matches pmv1alpha1.PodMigrationJobStatus exactly.
type K8sPMJStatus struct {
	Phase                 string         `json:"phase,omitempty"`
	SnapshotRef           string         `json:"snapshotRef,omitempty"`
	OriginNodeName        string         `json:"originNodeName,omitempty"`
	SnapshottingStartTime string         `json:"snapshottingStartTime,omitempty"`
	EvictingStartTime     string         `json:"evictingStartTime,omitempty"`
	RestoringStartTime    string         `json:"restoringStartTime,omitempty"`
	CompletionTime        string         `json:"completionTime,omitempty"`
	Consumed              bool           `json:"consumed,omitempty"`
	RestoredPodUID        string         `json:"restoredPodUID,omitempty"`
	RestoredPodName       string         `json:"restoredPodName,omitempty"`
	GateReleased          bool           `json:"gateReleased,omitempty"`
	Conditions            []K8sCondition `json:"conditions,omitempty"`
}

// K8sPMJ matches pmv1alpha1.PodMigrationJob JSON layout for ReconcileSnapshot.
type K8sPMJ struct {
	Metadata K8sObjectMeta `json:"metadata"`
	Spec     K8sPMJSpec    `json:"spec"`
	Status   K8sPMJStatus  `json:"status"`
}

// K8sSchedulingGate matches corev1.PodSchedulingGate.
type K8sSchedulingGate struct {
	Name string `json:"name"`
}

// K8sPodSpec matches the subset of corev1.PodSpec used by invariants.
type K8sPodSpec struct {
	NodeName        string              `json:"nodeName,omitempty"`
	SchedulingGates []K8sSchedulingGate `json:"schedulingGates,omitempty"`
}

// K8sPodStatus matches the subset of corev1.PodStatus used by invariants.
type K8sPodStatus struct {
	Phase      string         `json:"phase,omitempty"`
	Conditions []K8sCondition `json:"conditions,omitempty"`
}

// K8sPod matches corev1.Pod JSON layout for ReconcileSnapshot.
type K8sPod struct {
	Metadata K8sObjectMeta `json:"metadata"`
	Spec     K8sPodSpec    `json:"spec"`
	Status   K8sPodStatus  `json:"status"`
}

// ReconcileSnapshotJSON is wire-compatible with invariants.ReconcileSnapshot
// in controller/internal/invariants/snapshot.go so generated fixtures unmarshal
// directly into invariants.ReconcileSnapshot during `go test`.
type ReconcileSnapshotJSON struct {
	Now                          string         `json:"Now"`
	Reconciler                   string         `json:"Reconciler"`
	PrimaryPMJ                   *K8sPMJ        `json:"PrimaryPMJ,omitempty"`
	PrimaryPod                   *K8sPod        `json:"PrimaryPod,omitempty"`
	NamespacePMJs                []K8sPMJ       `json:"NamespacePMJs,omitempty"`
	NamespacePods                []K8sPod       `json:"NamespacePods,omitempty"`
	PodListFailed                bool           `json:"PodListFailed,omitempty"`
	PMJListFailed                bool           `json:"PMJListFailed,omitempty"`
	HasOrphanedTrigger           bool           `json:"HasOrphanedTrigger,omitempty"`
	RestoreCrashSignatureMatched bool           `json:"RestoreCrashSignatureMatched,omitempty"`
	PrimarySnapshotConditions    []K8sCondition `json:"PrimarySnapshotConditions,omitempty"`
}

// BlindSpotFinding describes a single blind spot where pmprofiler observed an
// unhealthy migration outcome while invariantViolations == 0.
type BlindSpotFinding struct {
	TriggerSource    string                `json:"triggerSource"`
	InvariantID      string                `json:"invariantId"`
	InvariantName    string                `json:"invariantName"`
	Slug             string                `json:"slug"`
	Template         TemplateClass         `json:"template"`
	Scenario         string                `json:"scenario"`
	Outcome          string                `json:"outcome"`
	PMJName          string                `json:"pmjName"`
	SourcePod        string                `json:"sourcePod,omitempty"`
	RestoredPod      string                `json:"restoredPod,omitempty"`
	FailureTimestamp string                `json:"failureTimestamp"`
	Description      string                `json:"description"`
	Snapshot         ReconcileSnapshotJSON `json:"snapshot"`
}

// RedGreenProof summarizes the compiled `go test` RED and GREEN replay results.
type RedGreenProof struct {
	InvariantID            string        `json:"invariantId"`
	InvariantName          string        `json:"invariantName"`
	Slug                   string        `json:"slug"`
	Template               TemplateClass `json:"template"`
	TriggerSource          string        `json:"triggerSource"`
	RequiresAuthorBody     bool          `json:"requiresAuthorBody"`
	CompiledAndTested      bool          `json:"compiledAndTested"`
	RedPassed              bool          `json:"redPassed"`
	RedTestOutput          string        `json:"redTestOutput,omitempty"`
	GreenPassed            bool          `json:"greenPassed"`
	GreenBaselineChecked   int           `json:"greenBaselineChecked"`
	GreenTraceStepsChecked int           `json:"greenTraceStepsChecked"`
	GreenTestOutput        string        `json:"greenTestOutput,omitempty"`
	SnapshotFixturePath    string        `json:"snapshotFixturePath"`
	GreenFixturesPath      string        `json:"greenFixturesPath"`
	RuleFilePath           string        `json:"ruleFilePath"`
	TestFilePath           string        `json:"testFilePath"`
	PRBodyPath             string        `json:"prBodyPath"`
}

// pmprofilerRunJSON supports both pmprofiler's canonical Run schema
// (TotalInvariantViolations float64 + InvariantViolations map[string]float64)
// and compact integer representations.
type pmprofilerRunJSON struct {
	Scenario                 string            `json:"scenario"`
	Outcomes                 map[string]int    `json:"outcomes"`
	TotalInvariantViolations float64           `json:"totalInvariantViolations"`
	InvariantViolations      any               `json:"invariantViolations,omitempty"`
	Migrations               []pmMigrationJSON `json:"migrations"`
}

// pmMigrationJSON supports both pmprofiler's canonical MigrationRecord tags
// (pod, snapshotName, phase, tTerminal) and legacy aliases (srcPod, snapshot,
// finalPhase, tEnd).
type pmMigrationJSON struct {
	App          string  `json:"app"`
	Pod          *string `json:"pod"`
	SrcPod       *string `json:"srcPod"`
	DstPod       *string `json:"dstPod"`
	PMJ          *string `json:"pmj"`
	Snapshot     *string `json:"snapshot"`
	SnapshotName *string `json:"snapshotName"`
	Outcome      string  `json:"outcome"`
	Phase        *string `json:"phase"`
	FinalPhase   *string `json:"finalPhase"`
	T0           string  `json:"t0"`
	TEnd         *string `json:"tEnd"`
	TTerminal    *string `json:"tTerminal"`
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
		controllerDir    = flag.String("controller-dir", "", "Optional path to controller/ directory for compiling and running emitted rules via go test")
		invariantID      = flag.String("invariant-id", "I10", "Candidate invariant ID (e.g. I10)")
		allowColdStart   = flag.Bool("allow-cold-start", false, "Treat cold-start outcomes as expected (do not flag as blind spots)")
		greenRecords     = flag.String("green-records", "", "Comma-separated paths to clean records.ndjson traces for GREEN replay verification")
		commentBody      = flag.String("comment", "", "PR review comment body containing /extract-invariant (Trigger C)")
		prTitle          = flag.String("pr-title", "", "Bugfix PR title for Trigger B extraction")
		overrideTemplate = flag.String("template", "", "Optional override for candidate template class")
		requireRedGreen  = flag.Bool("require-red-green", true, "Exit non-zero if compiled go test RED or GREEN proof fails")
	)
	flag.Parse()

	if err := execute(*mode, *runDir, *outDir, *controllerDir, *invariantID, *allowColdStart, *greenRecords, *commentBody, *prTitle, TemplateClass(*overrideTemplate), *requireRedGreen); err != nil {
		fmt.Fprintf(os.Stderr, "invariant-gen error: %v\n", err)
		os.Exit(1)
	}
}

func resolveControllerDir(explicit string) (string, error) {
	if explicit != "" {
		abs, err := filepath.Abs(explicit)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(filepath.Join(abs, "internal", "invariants", "snapshot.go")); err == nil {
			return abs, nil
		}
		return "", fmt.Errorf("controller-dir %s does not contain internal/invariants/snapshot.go", abs)
	}
	if _, thisFile, _, ok := runtime.Caller(0); ok {
		candidate := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "controller"))
		if _, err := os.Stat(filepath.Join(candidate, "internal", "invariants", "snapshot.go")); err == nil {
			return candidate, nil
		}
	}
	cwd, err := os.Getwd()
	if err == nil {
		for _, rel := range []string{"controller", "../controller", "../../controller"} {
			candidate := filepath.Clean(filepath.Join(cwd, rel))
			if _, err := os.Stat(filepath.Join(candidate, "internal", "invariants", "snapshot.go")); err == nil {
				return candidate, nil
			}
		}
	}
	return "", errors.New("could not locate controller/ directory containing internal/invariants/snapshot.go")
}

func execute(
	mode, runDir, outDir, controllerDir, invariantID string,
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
		ctrlDir, err := resolveControllerDir(controllerDir)
		if err != nil {
			return err
		}
		finding, err := ParseTriggerCommentOrPR(invariantID, commentBody, prTitle)
		if err != nil {
			return err
		}
		if overrideTemplate != "" {
			finding.Template = overrideTemplate
		}
		proof, err := SynthesizeAndVerify(finding, outDir, ctrlDir, greenPaths)
		if err != nil {
			return err
		}
		printProofSummary(proof)
		if requireRedGreen && (!proof.RedPassed || !proof.GreenPassed) {
			return fmt.Errorf("compiled go test RED/GREEN verification failed for %s (red=%v, green=%v)", proof.InvariantID, proof.RedPassed, proof.GreenPassed)
		}
		return nil

	case "pipeline", "verify":
		if runDir == "" {
			return errors.New("--run is required for pipeline/verify mode")
		}
		ctrlDir, err := resolveControllerDir(controllerDir)
		if err != nil {
			return err
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
		proof, err := SynthesizeAndVerify(finding, outDir, ctrlDir, greenPaths)
		if err != nil {
			return err
		}
		printProofSummary(proof)
		if requireRedGreen && (!proof.RedPassed || !proof.GreenPassed) {
			return fmt.Errorf("compiled go test RED/GREEN verification failed for %s (red=%v, green=%v)", proof.InvariantID, proof.RedPassed, proof.GreenPassed)
		}
		return nil

	default:
		return fmt.Errorf("unknown --mode %q (expected detect, verify, extract-comment, or pipeline)", mode)
	}
}

func printProofSummary(p RedGreenProof) {
	fmt.Printf("invariant-gen %s (%s) [template=%s, trigger=%s, compiled=%v]:\n",
		p.InvariantID, p.InvariantName, p.Template, p.TriggerSource, p.CompiledAndTested)
	fmt.Printf("  RED proof   : passed=%v (executed via `go test` on emitted rule & fixture)\n", p.RedPassed)
	fmt.Printf("  GREEN proof : passed=%v (baselineSnapshots=%d, traceSteps=%d via `go test`)\n",
		p.GreenPassed, p.GreenBaselineChecked, p.GreenTraceStepsChecked)
	fmt.Printf("  artifacts   : fixture=%s rule=%s test=%s pr=%s\n",
		p.SnapshotFixturePath, p.RuleFilePath, p.TestFilePath, p.PRBodyPath)
}

func hasRecordedInvariantViolations(run pmprofilerRunJSON) bool {
	if run.TotalInvariantViolations > 0 {
		return true
	}
	switch v := run.InvariantViolations.(type) {
	case float64:
		return v > 0
	case int:
		return v > 0
	case map[string]any:
		for _, rawVal := range v {
			if f, ok := rawVal.(float64); ok && f > 0 {
				return true
			}
		}
	}
	return false
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

	if hasRecordedInvariantViolations(run) {
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
		srcPod := firstNonEmpty(derefStr(m.SrcPod), derefStr(m.Pod))
		dstPod := derefStr(m.DstPod)
		snapName := firstNonEmpty(derefStr(m.Snapshot), derefStr(m.SnapshotName))
		cutoffTS := firstNonEmpty(derefStr(m.TEnd), derefStr(m.TTerminal))

		snap := reconstructSnapshotAt(records, pmjName, srcPod, dstPod, snapName, cutoffTS)
		if srcPod == "" && snap.PrimaryPMJ != nil {
			srcPod = snap.PrimaryPMJ.Spec.PodRef.Name
		}
		if dstPod == "" && snap.PrimaryPMJ != nil {
			dstPod = snap.PrimaryPMJ.Status.RestoredPodName
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
	case "wedged", "stalled", "failed", "no-replacement", "restore_crash_unmatched":
		return true
	case "cold-start":
		return !allowColdStart
	default:
		return false
	}
}

func classifyBlindSpot(m pmMigrationJSON, snap ReconcileSnapshotJSON) (TemplateClass, string, string, string) {
	if snap.PrimaryPMJ != nil {
		pmj := snap.PrimaryPMJ
		// 1. UnintendedColdStartActivePMJ:
		if m.Outcome == "cold-start" && pmj.Status.Phase == "Restoring" && pmj.Status.RestoredPodName != "" {
			return TemplateUnintendedColdStartActivePMJ,
				"ActivePMJReplacementColdStartGuard",
				"unintended_cold_start_active_pmj",
				fmt.Sprintf("Replacement pod %q reached Ready without a snapshot restore annotation while PMJ %q was in PhaseRestoring",
					pmj.Status.RestoredPodName, pmj.Metadata.Name)
		}
		// 2. WedgedRestoringOrphan:
		if pmj.Status.Phase == "Restoring" && pmj.Status.RestoredPodName != "" && !activePodInSlice(pmj.Status.RestoredPodName, snap.NamespacePods) &&
			exceededWindow(pmj.Status.RestoringStartTime, snap.Now, 30*time.Second) {
			return TemplateWedgedRestoringOrphan,
				"RestoringReplacementLiveness",
				"wedged_restoring_orphan",
				fmt.Sprintf("PMJ %q remained in PhaseRestoring >=30s after bound replacement pod %q was deleted or missing from NamespacePods",
					pmj.Metadata.Name, pmj.Status.RestoredPodName)
		}
		// 3. NoReplacementEvictingStall:
		if pmj.Status.Phase == "Evicting" && pmj.Status.RestoredPodName == "" && !activePodInSlice(pmj.Spec.PodRef.Name, snap.NamespacePods) &&
			exceededWindow(pmj.Status.EvictingStartTime, snap.Now, 30*time.Second) {
			return TemplateNoReplacementEvictingStall,
				"EvictingNoReplacementLiveness",
				"no_replacement_evicting_stall",
				fmt.Sprintf("PMJ %q remained in PhaseEvicting >=30s after source pod %q was deleted with no replacement pod bound",
					pmj.Metadata.Name, pmj.Spec.PodRef.Name)
		}
		// 4. PrematureSnapshotFailed:
		if pmj.Status.Phase == "Failed" && hasPrematureSnapshotFailure(pmj.Status.Conditions, snap.PrimarySnapshotConditions) {
			return TemplatePrematureSnapshotFailed,
				"SnapshotSubconditionConsistency",
				"premature_snapshot_failed",
				fmt.Sprintf("PMJ %q transitioned to PhaseFailed (SnapshotFailed) while PodSnapshot %q still had an in-progress sub-condition",
					pmj.Metadata.Name, pmj.Status.SnapshotRef)
		}
	}

	return TemplateCustomScaffold,
		"CustomExtractedInvariant",
		"custom_blind_spot",
		fmt.Sprintf("Unhealthy migration outcome %q observed on PMJ %q with 0 existing I1-I9 violations (requires author predicate over ReconcileSnapshot)",
			m.Outcome, derefStr(m.PMJ))
}

func hasPrematureSnapshotFailure(pmjConds []K8sCondition, snapConds []K8sCondition) bool {
	if len(snapConds) == 0 {
		return false
	}
	pmjSnapshotFailed := false
	for _, c := range pmjConds {
		if c.Reason == "SnapshotFailed" {
			pmjSnapshotFailed = true
			break
		}
	}
	if !pmjSnapshotFailed {
		return false
	}
	subInProgress := false
	for _, c := range snapConds {
		if (c.Type == "Checkpoint" || c.Type == "StorageReplicated") && c.Status == "False" && isSnapshotSubconditionInProgress(c.Reason) {
			subInProgress = true
			break
		}
	}
	return subInProgress
}

func isSnapshotSubconditionInProgress(reason string) bool {
	switch strings.ToLower(strings.TrimSpace(reason)) {
	case "inprogress", "pending", "running", "uploading", "triggering", "replicating":
		return true
	default:
		return false
	}
}

var extractCmdRe = regexp.MustCompile(`(?i)/extract-invariant(?:\s+(I\d+))?(?:\s+([a-z0-9_-]+))?(?:\s+(.*))?`)

// ParseTriggerCommentOrPR parses a Trigger C (/extract-invariant) comment or
// Trigger B (bugfix PR title) into a BlindSpotFinding with a representative
// offending ReconcileSnapshotJSON fixture.
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

	tmpl, invName, slug, outcome, snap := buildTemplateFixtureFromDirective(rawToken, desc)
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
		Outcome:          outcome,
		PMJName:          snap.PrimaryPMJ.Metadata.Name,
		SourcePod:        snap.PrimaryPMJ.Spec.PodRef.Name,
		RestoredPod:      snap.PrimaryPMJ.Status.RestoredPodName,
		FailureTimestamp: snap.Now,
		Description:      desc,
		Snapshot:         snap,
	}, nil
}

func buildTemplateFixtureFromDirective(token, desc string) (TemplateClass, string, string, string, ReconcileSnapshotJSON) {
	combined := strings.ToLower(token + " " + desc)
	now := "2026-09-28T12:01:00Z"

	switch {
	case strings.Contains(combined, "cold") && strings.Contains(combined, "start"):
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{Name: "pmj-cold-start-0", Namespace: "default", UID: "uid-pmj-cold"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "app-0"}, TargetPodUID: "uid-app-0"},
			Status: K8sPMJStatus{
				Phase:              "Restoring",
				SnapshotRef:        "ps-0",
				RestoredPodName:    "app-0-replacement",
				RestoredPodUID:     "uid-app-replacement",
				RestoringStartTime: "2026-09-28T12:00:10Z",
			},
		}
		pod := K8sPod{
			Metadata: K8sObjectMeta{
				Name:        "app-0-replacement",
				Namespace:   "default",
				UID:         "uid-app-replacement",
				Annotations: map[string]string{"podsnapshot.gke.io/ps-name": ""},
			},
			Spec: K8sPodSpec{NodeName: "node-b"},
			Status: K8sPodStatus{
				Phase:      "Running",
				Conditions: []K8sCondition{{Type: "Ready", Status: "True"}},
			},
		}
		return TemplateUnintendedColdStartActivePMJ,
			"ActivePMJReplacementColdStartGuard",
			"unintended_cold_start_active_pmj",
			"cold-start",
			ReconcileSnapshotJSON{
				Now:           now,
				Reconciler:    "PodMigrationJobReconciler",
				PrimaryPMJ:    &pmj,
				PrimaryPod:    &pod,
				NamespacePMJs: []K8sPMJ{pmj},
				NamespacePods: []K8sPod{pod},
			}

	case strings.Contains(combined, "no-replacement") || (strings.Contains(combined, "evict") && strings.Contains(combined, "stall")):
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{Name: "pmj-evict-stall-0", Namespace: "default", UID: "uid-pmj-evict"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "app-0"}, TargetPodUID: "uid-app-0"},
			Status: K8sPMJStatus{
				Phase:             "Evicting",
				SnapshotRef:       "ps-0",
				EvictingStartTime: "2026-09-28T12:00:10Z",
			},
		}
		return TemplateNoReplacementEvictingStall,
			"EvictingNoReplacementLiveness",
			"no_replacement_evicting_stall",
			"no-replacement",
			ReconcileSnapshotJSON{
				Now:           now,
				Reconciler:    "PodMigrationJobReconciler",
				PrimaryPMJ:    &pmj,
				NamespacePMJs: []K8sPMJ{pmj},
			}

	case strings.Contains(combined, "wedge") || strings.Contains(combined, "orphan") || strings.Contains(combined, "restoring"):
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{Name: "pmj-wedged-0", Namespace: "default", UID: "uid-pmj-wedged"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "app-0"}, TargetPodUID: "uid-app-0"},
			Status: K8sPMJStatus{
				Phase:              "Restoring",
				SnapshotRef:        "ps-0",
				RestoredPodName:    "app-0-dst",
				RestoredPodUID:     "uid-app-dst",
				EvictingStartTime:  "2026-09-28T12:00:05Z",
				RestoringStartTime: "2026-09-28T12:00:10Z",
			},
		}
		return TemplateWedgedRestoringOrphan,
			"RestoringReplacementLiveness",
			"wedged_restoring_orphan",
			"wedged",
			ReconcileSnapshotJSON{
				Now:           now,
				Reconciler:    "PodMigrationJobReconciler",
				PrimaryPMJ:    &pmj,
				NamespacePMJs: []K8sPMJ{pmj},
			}

	case strings.Contains(combined, "premature") || strings.Contains(combined, "subcondition") || (strings.Contains(combined, "snapshot") && strings.Contains(combined, "failed")):
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{Name: "pmj-snap-race-0", Namespace: "default", UID: "uid-pmj-snap-race"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "app-0"}, TargetPodUID: "uid-app-0"},
			Status: K8sPMJStatus{
				Phase:       "Failed",
				SnapshotRef: "ps-race-0",
				Conditions: []K8sCondition{
					{Type: "Ready", Status: "False", Reason: "SnapshotFailed", Message: "GKE PodSnapshot Ready failed (Failed)"},
				},
			},
		}
		snapConds := []K8sCondition{
			{Type: "Ready", Status: "False", Reason: "Failed"},
			{Type: "Checkpoint", Status: "False", Reason: "InProgress"},
		}
		return TemplatePrematureSnapshotFailed,
			"SnapshotSubconditionConsistency",
			"premature_snapshot_failed",
			"failed",
			ReconcileSnapshotJSON{
				Now:                       now,
				Reconciler:                "PodMigrationJobReconciler",
				PrimaryPMJ:                &pmj,
				NamespacePMJs:             []K8sPMJ{pmj},
				PrimarySnapshotConditions: snapConds,
			}

	default:
		slug := sanitizeSlug(token)
		if slug == "" {
			slug = "custom_blind_spot"
		}
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{Name: "pmj-custom-0", Namespace: "default", UID: "uid-pmj-custom"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "app-0"}},
			Status:   K8sPMJStatus{Phase: "Restoring"},
		}
		return TemplateCustomScaffold,
			"CustomExtractedInvariant",
			slug,
			"custom",
			ReconcileSnapshotJSON{
				Now:           now,
				Reconciler:    "PodMigrationJobReconciler",
				PrimaryPMJ:    &pmj,
				NamespacePMJs: []K8sPMJ{pmj},
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

func activePodInSlice(name string, pods []K8sPod) bool {
	for _, p := range pods {
		if p.Metadata.Name == name && p.Metadata.DeletionTimestamp == "" {
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

// BaselineGreenSnapshots returns canonical healthy ReconcileSnapshot states
// across all 5 PMJ lifecycle phases (Pending, Snapshotting, Evicting, Restoring,
// Succeeded), exercising the podsnapshot.gke.io/ps-name annotation.
func BaselineGreenSnapshots() []ReconcileSnapshotJSON {
	srcPod := K8sPod{
		Metadata: K8sObjectMeta{Name: "counter-0", Namespace: "default", UID: "uid-src"},
		Spec:     K8sPodSpec{NodeName: "node-a"},
		Status:   K8sPodStatus{Phase: "Running", Conditions: []K8sCondition{{Type: "Ready", Status: "True"}}},
	}
	deletingSrcPod := srcPod
	deletingSrcPod.Metadata.DeletionTimestamp = "2026-09-28T12:00:05Z"

	dstPodGated := K8sPod{
		Metadata: K8sObjectMeta{
			Name:        "counter-0-dst",
			Namespace:   "default",
			UID:         "uid-dst",
			Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "ps-clean-1"},
		},
		Spec:   K8sPodSpec{SchedulingGates: []K8sSchedulingGate{{Name: "pod-migration.gke.io/restoring"}}},
		Status: K8sPodStatus{Phase: "Pending"},
	}
	dstPodReady := K8sPod{
		Metadata: K8sObjectMeta{
			Name:        "counter-0-dst",
			Namespace:   "default",
			UID:         "uid-dst",
			Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "ps-clean-1"},
		},
		Spec:   K8sPodSpec{NodeName: "node-b"},
		Status: K8sPodStatus{Phase: "Running", Conditions: []K8sCondition{{Type: "Ready", Status: "True"}}},
	}

	pmjPending := K8sPMJ{
		Metadata: K8sObjectMeta{Name: "pmj-clean-1", Namespace: "default"},
		Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "counter-0"}},
		Status:   K8sPMJStatus{Phase: "Pending"},
	}
	pmjSnap := K8sPMJ{
		Metadata: K8sObjectMeta{Name: "pmj-clean-1", Namespace: "default"},
		Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "counter-0"}},
		Status:   K8sPMJStatus{Phase: "Snapshotting", SnapshotRef: "ps-clean-1", SnapshottingStartTime: "2026-09-28T12:00:02Z"},
	}
	pmjEvicting := K8sPMJ{
		Metadata: K8sObjectMeta{Name: "pmj-clean-1", Namespace: "default"},
		Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "counter-0"}},
		Status:   K8sPMJStatus{Phase: "Evicting", SnapshotRef: "ps-clean-1", EvictingStartTime: "2026-09-28T12:00:05Z"},
	}
	pmjRestoringGated := K8sPMJ{
		Metadata: K8sObjectMeta{Name: "pmj-clean-1", Namespace: "default"},
		Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: "counter-0"}},
		Status: K8sPMJStatus{
			Phase:              "Restoring",
			SnapshotRef:        "ps-clean-1",
			RestoredPodName:    "counter-0-dst",
			RestoredPodUID:     "uid-dst",
			EvictingStartTime:  "2026-09-28T12:00:05Z",
			RestoringStartTime: "2026-09-28T12:00:07Z",
		},
	}
	pmjRestoringReady := pmjRestoringGated
	pmjSucceeded := pmjRestoringGated
	pmjSucceeded.Status.Phase = "Succeeded"

	return []ReconcileSnapshotJSON{
		{Now: "2026-09-28T12:00:01Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjPending, PrimaryPod: &srcPod, NamespacePMJs: []K8sPMJ{pmjPending}, NamespacePods: []K8sPod{srcPod}},
		{Now: "2026-09-28T12:00:03Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjSnap, PrimaryPod: &srcPod, NamespacePMJs: []K8sPMJ{pmjSnap}, NamespacePods: []K8sPod{srcPod}},
		{Now: "2026-09-28T12:00:06Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjEvicting, PrimaryPod: &deletingSrcPod, NamespacePMJs: []K8sPMJ{pmjEvicting}, NamespacePods: []K8sPod{deletingSrcPod}},
		{Now: "2026-09-28T12:00:08Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjRestoringGated, PrimaryPod: &dstPodGated, NamespacePMJs: []K8sPMJ{pmjRestoringGated}, NamespacePods: []K8sPod{dstPodGated}},
		{Now: "2026-09-28T12:00:10Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjRestoringReady, PrimaryPod: &dstPodReady, NamespacePMJs: []K8sPMJ{pmjRestoringReady}, NamespacePods: []K8sPod{dstPodReady}},
		{Now: "2026-09-28T12:00:12Z", Reconciler: "PodMigrationJobReconciler", PrimaryPMJ: &pmjSucceeded, PrimaryPod: &dstPodReady, NamespacePMJs: []K8sPMJ{pmjSucceeded}, NamespacePods: []K8sPod{dstPodReady}},
	}
}

// SynthesizeAndVerify writes the ReconcileSnapshot fixtures, candidate Go rule
// and test files, and PR body to outDir, then stages them into a temporary copy
// of controller/internal/invariants and runs `go test` to execute the RED and
// GREEN replay proofs on the actual emitted Go code.
func SynthesizeAndVerify(
	finding BlindSpotFinding,
	outDir, controllerDir string,
	greenTracePaths []string,
) (RedGreenProof, error) {
	ctrlDir, err := resolveControllerDir(controllerDir)
	if err != nil {
		return RedGreenProof{}, err
	}
	testdataDir := filepath.Join(outDir, "testdata")
	if err := os.MkdirAll(testdataDir, 0o755); err != nil {
		return RedGreenProof{}, err
	}

	idLower := strings.ToLower(finding.InvariantID)
	basePrefix := fmt.Sprintf("%s_%s", idLower, finding.Slug)
	fixtureFilename := basePrefix + "_snapshot.json"
	greenFilename := basePrefix + "_green_snapshots.json"
	fixturePath := filepath.Join(testdataDir, fixtureFilename)
	greenFixturesPath := filepath.Join(testdataDir, greenFilename)
	rulePath := filepath.Join(outDir, basePrefix+"_rule.go")
	testPath := filepath.Join(outDir, basePrefix+"_test.go")
	prBodyPath := filepath.Join(outDir, "pr_body.md")
	proofPath := filepath.Join(outDir, "proof.json")

	// 1. Write RED ReconcileSnapshot fixture.
	fixtureBytes, err := json.MarshalIndent(finding.Snapshot, "", "  ")
	if err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(fixturePath, append(fixtureBytes, '\n'), 0o644); err != nil {
		return RedGreenProof{}, err
	}

	// 2. Build GREEN ReconcileSnapshot corpus (baseline lifecycle snapshots + reconstructed trace steps).
	baselines := BaselineGreenSnapshots()
	greenCorpus := append([]ReconcileSnapshotJSON(nil), baselines...)
	traceStepsChecked := 0
	for _, tracePath := range greenTracePaths {
		steps, err := ReconstructAllTraceSnapshots(tracePath)
		if err != nil {
			return RedGreenProof{}, fmt.Errorf("replay green trace %s: %w", tracePath, err)
		}
		traceStepsChecked += len(steps)
		greenCorpus = append(greenCorpus, steps...)
	}
	greenBytes, err := json.MarshalIndent(greenCorpus, "", "  ")
	if err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(greenFixturesPath, append(greenBytes, '\n'), 0o644); err != nil {
		return RedGreenProof{}, err
	}

	// 3. Emit candidate Go rule and Go test files.
	ruleGo := renderCandidateRuleGo(finding)
	testGo := renderCandidateTestGo(finding, fixtureFilename, greenFilename)
	if err := os.WriteFile(rulePath, []byte(ruleGo), 0o644); err != nil {
		return RedGreenProof{}, err
	}
	if err := os.WriteFile(testPath, []byte(testGo), 0o644); err != nil {
		return RedGreenProof{}, err
	}

	// 4. Compile and run the emitted rule + test + fixtures inside a temporary
	// copy of controller/internal/invariants via `go test`.
	redPassed, redOut, greenPassed, greenOut, compiled, err := runCompiledGoTestProof(
		ctrlDir, finding, fixtureFilename, greenFilename,
		filepath.Base(rulePath), filepath.Base(testPath),
		fixtureBytes, greenBytes, []byte(ruleGo), []byte(testGo),
	)
	if err != nil {
		return RedGreenProof{}, err
	}
	// A rule that fails RED (e.g., an unauthored custom scaffold returning nil)
	// cannot claim a meaningful GREEN pass until its RED proof passes.
	if !redPassed {
		greenPassed = false
	}

	proof := RedGreenProof{
		InvariantID:            finding.InvariantID,
		InvariantName:          finding.InvariantName,
		Slug:                   finding.Slug,
		Template:               finding.Template,
		TriggerSource:          finding.TriggerSource,
		RequiresAuthorBody:     finding.Template == TemplateCustomScaffold,
		CompiledAndTested:      compiled,
		RedPassed:              redPassed,
		RedTestOutput:          strings.TrimSpace(redOut),
		GreenPassed:            greenPassed,
		GreenBaselineChecked:   len(baselines),
		GreenTraceStepsChecked: traceStepsChecked,
		GreenTestOutput:        strings.TrimSpace(greenOut),
		SnapshotFixturePath:    fixturePath,
		GreenFixturesPath:      greenFixturesPath,
		RuleFilePath:           rulePath,
		TestFilePath:           testPath,
		PRBodyPath:             prBodyPath,
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

func runCompiledGoTestProof(
	controllerDir string,
	finding BlindSpotFinding,
	fixtureFilename, greenFilename, ruleFilename, testFilename string,
	fixtureBytes, greenBytes, ruleBytes, testBytes []byte,
) (redPassed bool, redOut string, greenPassed bool, greenOut string, compiled bool, err error) {
	internalDir := filepath.Join(controllerDir, "internal")
	srcInvDir := filepath.Join(internalDir, "invariants")

	stageDir, err := os.MkdirTemp(internalDir, "invverify")
	if err != nil {
		return false, "", false, "", false, fmt.Errorf("create temp stage dir in %s: %w", internalDir, err)
	}
	defer os.RemoveAll(stageDir)

	entries, err := os.ReadDir(srcInvDir)
	if err != nil {
		return false, "", false, "", false, fmt.Errorf("read %s: %w", srcInvDir, err)
	}
	for _, ent := range entries {
		if ent.IsDir() || !strings.HasSuffix(ent.Name(), ".go") {
			continue
		}
		b, readErr := os.ReadFile(filepath.Join(srcInvDir, ent.Name()))
		if readErr != nil {
			return false, "", false, "", false, readErr
		}
		if writeErr := os.WriteFile(filepath.Join(stageDir, ent.Name()), b, 0o644); writeErr != nil {
			return false, "", false, "", false, writeErr
		}
	}

	stageTestdata := filepath.Join(stageDir, "testdata")
	if err := os.MkdirAll(stageTestdata, 0o755); err != nil {
		return false, "", false, "", false, err
	}
	if err := os.WriteFile(filepath.Join(stageTestdata, fixtureFilename), fixtureBytes, 0o644); err != nil {
		return false, "", false, "", false, err
	}
	if err := os.WriteFile(filepath.Join(stageTestdata, greenFilename), greenBytes, 0o644); err != nil {
		return false, "", false, "", false, err
	}
	if err := os.WriteFile(filepath.Join(stageDir, ruleFilename), ruleBytes, 0o644); err != nil {
		return false, "", false, "", false, err
	}
	if err := os.WriteFile(filepath.Join(stageDir, testFilename), testBytes, 0o644); err != nil {
		return false, "", false, "", false, err
	}

	relPkg := "./internal/" + filepath.Base(stageDir)
	redFuncName := fmt.Sprintf("Test%s_%s_RedProof", strings.ToUpper(finding.InvariantID), finding.InvariantName)
	greenFuncName := fmt.Sprintf("Test%s_%s_GreenProof", strings.ToUpper(finding.InvariantID), finding.InvariantName)
	redTestPattern := "^" + redFuncName + "$"
	greenTestPattern := "^" + greenFuncName + "$"

	redCtx, redCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer redCancel()
	redCmd := exec.CommandContext(redCtx, "go", "test", "-v", "-count=1", "-run", redTestPattern, relPkg)
	redCmd.Dir = controllerDir
	redBytes, redErr := redCmd.CombinedOutput()
	redOut = string(redBytes)
	redPassed = redErr == nil &&
		strings.Contains(redOut, "--- PASS: "+redFuncName) &&
		!strings.Contains(redOut, "[no tests to run]")

	greenCtx, greenCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer greenCancel()
	greenCmd := exec.CommandContext(greenCtx, "go", "test", "-v", "-count=1", "-run", greenTestPattern, relPkg)
	greenCmd.Dir = controllerDir
	greenOutBytes, greenErr := greenCmd.CombinedOutput()
	greenOut = string(greenOutBytes)
	greenPassed = greenErr == nil &&
		strings.Contains(greenOut, "--- PASS: "+greenFuncName) &&
		!strings.Contains(greenOut, "[no tests to run]")

	compiled = !strings.Contains(redOut, "[build failed]") && !strings.Contains(greenOut, "[build failed]")
	return redPassed, redOut, greenPassed, greenOut, compiled, nil
}

// ReconstructAllTraceSnapshots replays a pmprofiler records.ndjson trace and
// emits a ReconcileSnapshotJSON for every PMJ add/update step in the trace so
// GREEN proofs can verify 0 false positives across real cluster recordings.
func ReconstructAllTraceSnapshots(recordsPath string) ([]ReconcileSnapshotJSON, error) {
	records, err := loadNDJSONRecords(recordsPath)
	if err != nil {
		return nil, err
	}
	state := newTraceState()
	var snapshots []ReconcileSnapshotJSON
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
	pmjs      map[string]K8sPMJ
	pods      map[string]K8sPod
	snapshots map[string][]K8sCondition
}

func newTraceState() *traceState {
	return &traceState{
		pmjs:      make(map[string]K8sPMJ),
		pods:      make(map[string]K8sPod),
		snapshots: make(map[string][]K8sCondition),
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
		snapRef := firstNonEmpty(nestedStr(rec.Obj, "status", "snapshotRef"), nestedStr(rec.Obj, "status", "snapshotName"))
		pmj := K8sPMJ{
			Metadata: K8sObjectMeta{
				Name:              name,
				Namespace:         ns,
				UID:               nestedStr(rec.Obj, "metadata", "uid"),
				CreationTimestamp: nestedStr(rec.Obj, "metadata", "creationTimestamp"),
				DeletionTimestamp: nestedStr(rec.Obj, "metadata", "deletionTimestamp"),
				Labels:            extractStringMap(rec.Obj, "metadata", "labels"),
				Annotations:       extractStringMap(rec.Obj, "metadata", "annotations"),
			},
			Spec: K8sPMJSpec{
				PodRef:       K8sLocalObjectRef{Name: nestedStr(rec.Obj, "spec", "podRef", "name")},
				TargetPodUID: nestedStr(rec.Obj, "spec", "targetPodUID"),
			},
			Status: K8sPMJStatus{
				Phase:                 nestedStr(rec.Obj, "status", "phase"),
				SnapshotRef:           snapRef,
				OriginNodeName:        nestedStr(rec.Obj, "status", "originNodeName"),
				SnapshottingStartTime: nestedStr(rec.Obj, "status", "snapshottingStartTime"),
				EvictingStartTime:     firstNonEmpty(nestedStr(rec.Obj, "status", "evictingStartTime"), defaultPhaseStart(nestedStr(rec.Obj, "status", "phase"), "Evicting", rec.TS)),
				RestoringStartTime:    firstNonEmpty(nestedStr(rec.Obj, "status", "restoringStartTime"), defaultPhaseStart(nestedStr(rec.Obj, "status", "phase"), "Restoring", rec.TS)),
				CompletionTime:        nestedStr(rec.Obj, "status", "completionTime"),
				RestoredPodName:       nestedStr(rec.Obj, "status", "restoredPodName"),
				RestoredPodUID:        nestedStr(rec.Obj, "status", "restoredPodUID"),
				Conditions:            extractConditions(rec.Obj),
			},
		}
		s.pmjs[name] = pmj

	case strings.HasPrefix(rec.GVR, "pods."):
		if rec.Type == "delete" {
			delete(s.pods, name)
			return
		}
		p := K8sPod{
			Metadata: K8sObjectMeta{
				Name:              name,
				Namespace:         ns,
				UID:               nestedStr(rec.Obj, "metadata", "uid"),
				CreationTimestamp: nestedStr(rec.Obj, "metadata", "creationTimestamp"),
				DeletionTimestamp: nestedStr(rec.Obj, "metadata", "deletionTimestamp"),
				Labels:            extractStringMap(rec.Obj, "metadata", "labels"),
				Annotations:       extractStringMap(rec.Obj, "metadata", "annotations"),
			},
			Spec: K8sPodSpec{
				NodeName:        nestedStr(rec.Obj, "spec", "nodeName"),
				SchedulingGates: extractSchedulingGates(rec.Obj),
			},
			Status: K8sPodStatus{
				Phase:      nestedStr(rec.Obj, "status", "phase"),
				Conditions: extractConditions(rec.Obj),
			},
		}
		s.pods[name] = p

	case strings.HasPrefix(rec.GVR, "podsnapshots."):
		if rec.Type == "delete" {
			delete(s.snapshots, name)
			return
		}
		s.snapshots[name] = extractConditions(rec.Obj)
	}
}

func defaultPhaseStart(actualPhase, targetPhase, ts string) string {
	if actualPhase == targetPhase {
		return ts
	}
	return ""
}

func (s *traceState) snapshotForPMJ(ts string, pmj K8sPMJ) ReconcileSnapshotJSON {
	cpPMJ := pmj
	var primaryPod *K8sPod
	if pmj.Status.RestoredPodName != "" {
		if p, ok := s.pods[pmj.Status.RestoredPodName]; ok {
			cp := p
			primaryPod = &cp
		}
	}
	if primaryPod == nil && pmj.Spec.PodRef.Name != "" {
		if p, ok := s.pods[pmj.Spec.PodRef.Name]; ok {
			cp := p
			primaryPod = &cp
		}
	}

	var nsPMJs []K8sPMJ
	for _, item := range s.pmjs {
		nsPMJs = append(nsPMJs, item)
	}
	sort.Slice(nsPMJs, func(i, j int) bool { return nsPMJs[i].Metadata.Name < nsPMJs[j].Metadata.Name })

	var nsPods []K8sPod
	for _, item := range s.pods {
		nsPods = append(nsPods, item)
	}
	sort.Slice(nsPods, func(i, j int) bool { return nsPods[i].Metadata.Name < nsPods[i].Metadata.Name })

	var snapConds []K8sCondition
	if pmj.Status.SnapshotRef != "" {
		if conds, ok := s.snapshots[pmj.Status.SnapshotRef]; ok && len(conds) > 0 {
			snapConds = append([]K8sCondition(nil), conds...)
		}
	}

	if ts == "" {
		ts = "2026-09-28T12:00:00Z"
	}
	return ReconcileSnapshotJSON{
		Now:                       ts,
		Reconciler:                "PodMigrationJobReconciler",
		PrimaryPMJ:                &cpPMJ,
		PrimaryPod:                primaryPod,
		NamespacePMJs:             nsPMJs,
		NamespacePods:             nsPods,
		PrimarySnapshotConditions: snapConds,
	}
}

func reconstructSnapshotAt(
	records []rawNDJSONRecord,
	pmjName, srcPod, dstPod, snapName, cutoffTS string,
) ReconcileSnapshotJSON {
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
	if cutoffTS != "" {
		if tCut, err1 := time.Parse(time.RFC3339Nano, cutoffTS); err1 == nil {
			if tLast, err2 := time.Parse(time.RFC3339Nano, lastTS); err2 != nil || tCut.After(tLast) {
				lastTS = cutoffTS
			}
		}
	}

	pmj, ok := state.pmjs[pmjName]
	if !ok {
		pmj = K8sPMJ{
			Metadata: K8sObjectMeta{Name: pmjName, Namespace: "default"},
			Spec:     K8sPMJSpec{PodRef: K8sLocalObjectRef{Name: srcPod}},
			Status:   K8sPMJStatus{SnapshotRef: snapName, RestoredPodName: dstPod},
		}
	}
	if pmj.Status.SnapshotRef == "" && snapName != "" {
		pmj.Status.SnapshotRef = snapName
	}
	if pmj.Status.RestoredPodName == "" && dstPod != "" {
		pmj.Status.RestoredPodName = dstPod
	}
	if cutoffTS == "" && (pmj.Status.Phase == "Restoring" || pmj.Status.Phase == "Evicting") && lastTS != "" {
		if tLast, err := time.Parse(time.RFC3339Nano, lastTS); err == nil {
			lastTS = tLast.Add(60 * time.Second).UTC().Format(time.RFC3339)
		}
	}
	return state.snapshotForPMJ(lastTS, pmj)
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
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
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
	_ = fmt.Sprintf
	_ = strings.TrimSpace
	_ = time.Second
	_ = corev1.ConditionTrue
	_ = metav1.ConditionFalse
	_ = pmv1alpha1.PodMigrationJobPhaseRestoring
%s
}
`, f.TriggerSource, ruleConstructor, f.InvariantID, f.InvariantName, f.TriggerSource, f.Description,
		ruleConstructor, f.InvariantID, f.InvariantName, funcName,
		funcName, f.InvariantID, f.InvariantName, funcName,
		renderPredicateBodyGo(f))
}

func renderPredicateBodyGo(f BlindSpotFinding) string {
	switch f.Template {
	case TemplateWedgedRestoringOrphan:
		return `	// RestoringReplacementLiveness: when a PMJ has remained in PhaseRestoring
	// for >=30s with a bound RestoredPodName and !s.PodListFailed, that replacement
	// pod must still exist and be non-deleting in the namespace (including single-pod
	// namespaces where NamespacePods is empty after source eviction).
	if !s.PodListFailed &&
		(s.Reconciler == "" || s.Reconciler == "PodMigrationJobReconciler") &&
		pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseRestoring &&
		pmj.Status.RestoredPodName != "" &&
		pmj.Status.RestoringStartTime != nil &&
		!s.Now.IsZero() &&
		s.Now.Sub(pmj.Status.RestoringStartTime.Time) >= 30*time.Second {
		found := false
		for i := range s.NamespacePods {
			pod := &s.NamespacePods[i]
			if (pod.Namespace == "" || pmj.Namespace == "" || pod.Namespace == pmj.Namespace) &&
				pod.Name == pmj.Status.RestoredPodName && pod.DeletionTimestamp == nil {
				found = true
				break
			}
		}
		if !found && s.PrimaryPod != nil &&
			(s.PrimaryPod.Namespace == "" || pmj.Namespace == "" || s.PrimaryPod.Namespace == pmj.Namespace) &&
			s.PrimaryPod.Name == pmj.Status.RestoredPodName && s.PrimaryPod.DeletionTimestamp == nil {
			found = true
		}
		if !found {
			return []Violation{{
				InvariantID:   "` + f.InvariantID + `",
				InvariantName: "` + f.InvariantName + `",
				Reason:        "RestoringReplacementPodMissing",
				Message:       fmt.Sprintf("PMJ %s/%s remained in PhaseRestoring >=30s while replacement pod %q is missing or deleting", pmj.Namespace, pmj.Name, pmj.Status.RestoredPodName),
				Namespace:     pmj.Namespace,
				PMJName:       pmj.Name,
				PodName:       pmj.Status.RestoredPodName,
			}}
		}
	}
	return nil`

	case TemplateNoReplacementEvictingStall:
		return `	// EvictingNoReplacementLiveness: when a PMJ has remained in PhaseEvicting
	// for >=30s with no RestoredPodName bound, !s.PodListFailed, and the source
	// pod is already gone or deleting (including single-pod namespaces where
	// NamespacePods is empty after source eviction), the migration is stalled.
	if !s.PodListFailed &&
		(s.Reconciler == "" || s.Reconciler == "PodMigrationJobReconciler") &&
		pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseEvicting &&
		pmj.Status.RestoredPodName == "" &&
		pmj.Status.EvictingStartTime != nil &&
		!s.Now.IsZero() &&
		s.Now.Sub(pmj.Status.EvictingStartTime.Time) >= 30*time.Second {
		srcActive := false
		for i := range s.NamespacePods {
			pod := &s.NamespacePods[i]
			if (pod.Namespace == "" || pmj.Namespace == "" || pod.Namespace == pmj.Namespace) &&
				pod.Name == pmj.Spec.PodRef.Name && pod.DeletionTimestamp == nil {
				srcActive = true
				break
			}
		}
		if !srcActive && s.PrimaryPod != nil &&
			(s.PrimaryPod.Namespace == "" || pmj.Namespace == "" || s.PrimaryPod.Namespace == pmj.Namespace) &&
			s.PrimaryPod.Name == pmj.Spec.PodRef.Name && s.PrimaryPod.DeletionTimestamp == nil {
			srcActive = true
		}
		if !srcActive {
			return []Violation{{
				InvariantID:   "` + f.InvariantID + `",
				InvariantName: "` + f.InvariantName + `",
				Reason:        "EvictingSourceGoneWithoutReplacement",
				Message:       fmt.Sprintf("PMJ %s/%s remained in PhaseEvicting >=30s after source pod %q disappeared with no replacement pod", pmj.Namespace, pmj.Name, pmj.Spec.PodRef.Name),
				Namespace:     pmj.Namespace,
				PMJName:       pmj.Name,
				PodName:       pmj.Spec.PodRef.Name,
			}}
		}
	}
	return nil`

	case TemplateUnintendedColdStartActivePMJ:
		return `	// ActivePMJReplacementColdStartGuard: while a PMJ is in PhaseRestoring, its
	// bound replacement pod must not become Ready with an empty podsnapshot.gke.io/ps-name annotation.
	if pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseRestoring && pmj.Status.RestoredPodName != "" {
		for i := range s.NamespacePods {
			pod := &s.NamespacePods[i]
			if (pod.Namespace != "" && pmj.Namespace != "" && pod.Namespace != pmj.Namespace) ||
				pod.Name != pmj.Status.RestoredPodName || len(pod.Spec.SchedulingGates) > 0 {
				continue
			}
			ready := false
			for _, c := range pod.Status.Conditions {
				if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
					ready = true
					break
				}
			}
			restoreSnap := strings.TrimSpace(pod.Annotations["podsnapshot.gke.io/ps-name"])
			if ready && restoreSnap == "" {
				return []Violation{{
					InvariantID:   "` + f.InvariantID + `",
					InvariantName: "` + f.InvariantName + `",
					Reason:        "ActivePMJReplacementColdStarted",
					Message:       fmt.Sprintf("Replacement pod %s/%s became Ready without podsnapshot.gke.io/ps-name annotation while PMJ %s was in PhaseRestoring", pod.Namespace, pod.Name, pmj.Name),
					Namespace:     pmj.Namespace,
					PMJName:       pmj.Name,
					PodName:       pod.Name,
				}}
			}
		}
	}
	return nil`

	case TemplatePrematureSnapshotFailed:
		return `	// SnapshotSubconditionConsistency: a PMJ must not transition to PhaseFailed
	// with Reason=SnapshotFailed while PrimarySnapshotConditions reports an
	// active in-progress Checkpoint or StorageReplicated sub-condition (#75, #88).
	if pmj.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed && len(s.PrimarySnapshotConditions) > 0 {
		snapshotFailed := false
		for _, c := range pmj.Status.Conditions {
			if c.Reason == "SnapshotFailed" {
				snapshotFailed = true
				break
			}
		}
		if snapshotFailed {
			for _, sc := range s.PrimarySnapshotConditions {
				if (sc.Type == "Checkpoint" || sc.Type == "StorageReplicated") && sc.Status == metav1.ConditionFalse {
					r := strings.ToLower(strings.TrimSpace(sc.Reason))
					if r == "inprogress" || r == "pending" || r == "running" || r == "uploading" || r == "triggering" || r == "replicating" {
						return []Violation{{
							InvariantID:   "` + f.InvariantID + `",
							InvariantName: "` + f.InvariantName + `",
							Reason:        "PrematureSnapshotFailedWhileSubconditionInProgress",
							Message:       fmt.Sprintf("PMJ %s/%s failed with SnapshotFailed while PodSnapshot %q sub-condition %s=%s (Reason=%s) was still in progress", pmj.Namespace, pmj.Name, pmj.Status.SnapshotRef, sc.Type, sc.Status, sc.Reason),
							Namespace:     pmj.Namespace,
							PMJName:       pmj.Name,
							PodName:       pmj.Spec.PodRef.Name,
						}}
					}
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

func renderCandidateTestGo(f BlindSpotFinding, fixtureFilename, greenFilename string) string {
	ruleConstructor := fmt.Sprintf("Rule%s", strings.ToUpper(f.InvariantID))
	redTestName := fmt.Sprintf("Test%s_%s_RedProof", strings.ToUpper(f.InvariantID), f.InvariantName)
	greenTestName := fmt.Sprintf("Test%s_%s_GreenProof", strings.ToUpper(f.InvariantID), f.InvariantName)

	return fmt.Sprintf(`// Code generated by tools/invariant-gen (%s); review before merging on an invariant/* branch.
package invariants

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func %s(t *testing.T) {
	fixturePath := filepath.Join("testdata", %q)
	raw, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read RED snapshot fixture %%s: %%v", fixturePath, err)
	}
	var snap ReconcileSnapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatalf("unmarshal RED snapshot fixture %%s into ReconcileSnapshot: %%v", fixturePath, err)
	}
	rule := %s()
	vs := rule.Evaluate(&snap)
	if len(vs) == 0 {
		t.Fatalf("RED proof failed: expected >= 1 violation from %%s (%%s) on offending fixture %%s, got 0", rule.ID(), rule.Name(), fixturePath)
	}
	if vs[0].InvariantID != %q {
		t.Fatalf("expected violation InvariantID=%s, got %%q", vs[0].InvariantID)
	}
}

func %s(t *testing.T) {
	greenPath := filepath.Join("testdata", %q)
	raw, err := os.ReadFile(greenPath)
	if err != nil {
		t.Fatalf("read GREEN snapshot corpus %%s: %%v", greenPath, err)
	}
	var snaps []ReconcileSnapshot
	if err := json.Unmarshal(raw, &snaps); err != nil {
		t.Fatalf("unmarshal GREEN snapshot corpus %%s: %%v", greenPath, err)
	}
	if len(snaps) == 0 {
		t.Fatalf("expected non-empty GREEN snapshot corpus in %%s", greenPath)
	}
	rule := %s()
	for i := range snaps {
		if vs := rule.Evaluate(&snaps[i]); len(vs) != 0 {
			t.Fatalf("GREEN proof failed at step %%d (now=%%s): unexpected false-positive violation %%+v", i, snaps[i].Now.Format("2006-01-02T15:04:05Z07:00"), vs)
		}
	}
}
`, f.TriggerSource,
		redTestName, fixtureFilename, ruleConstructor, f.InvariantID, f.InvariantID,
		greenTestName, greenFilename, ruleConstructor)
}

func renderPRBodyMarkdown(f BlindSpotFinding, p RedGreenProof) string {
	redStatus := "PASS (`go test` verified)"
	if !p.RedPassed {
		redStatus = "FAIL (requires author predicate)"
	}
	greenStatus := "PASS (`go test` verified)"
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

### 2. Compiled `+"`go test`"+` RED / GREEN Replay Proof

| Proof Stage | Target Corpus | Checked Snapshots / Steps | Compiled & Tested | Result |
| :--- | :--- | :---: | :---: | :---: |
| **RED Proof** (`+"`Test%s_%s_RedProof`"+`) | `+"`testdata/%s`"+` (`+"`ReconcileSnapshot`"+`) | `+"`1`"+` | `+"`%v`"+` | **%s** |
| **GREEN Proof** (`+"`Test%s_%s_GreenProof`"+`) | `+"`testdata/%s`"+` (baseline + clean `+"`records.ndjson`"+` steps) | `+"`%d`"+` baseline + `+"`%d`"+` trace steps | `+"`%v`"+` | **%s** |

---

### 3. Generated Artifacts (Lane 2 Scope)
- `+"`controller/internal/invariants/testdata/%s`"+`
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
		strings.ToUpper(f.InvariantID), f.InvariantName, filepath.Base(p.SnapshotFixturePath), p.CompiledAndTested, redStatus,
		strings.ToUpper(f.InvariantID), f.InvariantName, filepath.Base(p.GreenFixturesPath), p.GreenBaselineChecked, p.GreenTraceStepsChecked, p.CompiledAndTested, greenStatus,
		filepath.Base(p.SnapshotFixturePath),
		filepath.Base(p.GreenFixturesPath),
		filepath.Base(p.RuleFilePath),
		filepath.Base(p.TestFilePath),
	)
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func derefStr(p *string) string {
	if p == nil {
		return ""
	}
	return *p
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

func extractSchedulingGates(obj map[string]any) []K8sSchedulingGate {
	spec, ok := obj["spec"].(map[string]any)
	if !ok {
		return nil
	}
	gates, ok := spec["schedulingGates"].([]any)
	if !ok {
		return nil
	}
	var out []K8sSchedulingGate
	for _, g := range gates {
		if gm, ok := g.(map[string]any); ok {
			if name, ok := gm["name"].(string); ok && name != "" {
				out = append(out, K8sSchedulingGate{Name: name})
			}
		}
	}
	return out
}

func extractConditions(obj map[string]any) []K8sCondition {
	status, ok := obj["status"].(map[string]any)
	if !ok {
		return nil
	}
	conds, ok := status["conditions"].([]any)
	if !ok {
		return nil
	}
	var out []K8sCondition
	for _, c := range conds {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		cs := K8sCondition{
			Type:               nestedStr(cm, "type"),
			Status:             nestedStr(cm, "status"),
			Reason:             nestedStr(cm, "reason"),
			Message:            nestedStr(cm, "message"),
			LastTransitionTime: nestedStr(cm, "lastTransitionTime"),
		}
		if cs.Type != "" {
			out = append(out, cs)
		}
	}
	return out
}
