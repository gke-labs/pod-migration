// SPDX-License-Identifier: Apache-2.0
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

// Package conformance provides the canonical snapshot engine conformance suite
// (docs/design/engine-contract.md #81, #108).
//
// It audits an Engine implementation against contract clauses C1–C9 to verify
// that it provides the identity, eligibility, checkpoint outcomes, source fencing,
// restore orders, outcome signals, failure classification, placement, and cleanup
// semantics required by the pod migration controller. Clauses C10 (Security) and
// C11 (Observability) are tracked separately and not audited by this suite.
package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClauseID represents a formal clause identifier in the snapshot engine contract.
type ClauseID string

const (
	ClauseC1Identity             ClauseID = "C1"
	ClauseC2Eligibility          ClauseID = "C2"
	ClauseC3CheckpointOutcomes   ClauseID = "C3"
	ClauseC4SourceFencing        ClauseID = "C4"
	ClauseC5RestoreOrder         ClauseID = "C5"
	ClauseC6RestoreOutcomeSignal ClauseID = "C6"
	ClauseC7FailureClassify      ClauseID = "C7"
	ClauseC8Placement            ClauseID = "C8"
	ClauseC9CancellationCleanup  ClauseID = "C9"
	ClauseC10Security            ClauseID = "C10"
	ClauseC11Observability       ClauseID = "C11"
)

// ClauseStatus represents the outcome of an evaluated conformance clause.
type ClauseStatus string

const (
	StatusPassed     ClauseStatus = "PASSED"
	StatusFailed     ClauseStatus = "FAILED"
	StatusSkipped    ClauseStatus = "SKIPPED"
	StatusNotCovered ClauseStatus = "NOT_COVERED"
)

// SubtestResult records the outcome of an individual assertion within a clause.
type SubtestResult struct {
	Name    string
	Passed  bool
	Message string
}

// ClauseResult records the audit outcome of a single engine contract clause.
type ClauseResult struct {
	ID       ClauseID
	Title    string
	Required bool
	Status   ClauseStatus
	Details  string
	Subtests []SubtestResult
}

// Report holds the complete conformance evaluation report for an Engine.
type Report struct {
	EngineName string
	APIGroup   string
	TestedAt   time.Time
	Clauses    []ClauseResult
	Passed     bool
}

// Summary returns a concise human-readable summary of the conformance report.
func (r *Report) Summary() string {
	passedCount := 0
	failedCount := 0
	skippedCount := 0
	notCoveredCount := 0
	for _, c := range r.Clauses {
		switch c.Status {
		case StatusPassed:
			passedCount++
		case StatusFailed:
			failedCount++
		case StatusSkipped:
			skippedCount++
		case StatusNotCovered:
			notCoveredCount++
		}
	}
	verdict := "PASSED"
	if !r.Passed {
		verdict = "FAILED"
	}
	if notCoveredCount > 0 {
		return fmt.Sprintf("Engine Conformance for %q (%s): %s (%d passed, %d failed, %d skipped, %d not covered)",
			r.EngineName, r.APIGroup, verdict, passedCount, failedCount, skippedCount, notCoveredCount)
	}
	return fmt.Sprintf("Engine Conformance for %q (%s): %s (%d passed, %d failed, %d skipped)",
		r.EngineName, r.APIGroup, verdict, passedCount, failedCount, skippedCount)
}

// Markdown formats the report as a GitHub-compatible markdown document with
// badges, summary tables, and detailed clause breakdowns suitable for PRs or
// release notes.
func (r *Report) Markdown() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("## Snapshot Engine Conformance Report: `%s`\n\n", r.EngineName))
	sb.WriteString(fmt.Sprintf("- **API Group:** `%s`\n", r.APIGroup))
	sb.WriteString(fmt.Sprintf("- **Evaluated At:** `%s`\n", r.TestedAt.UTC().Format(time.RFC3339)))
	if r.Passed {
		sb.WriteString("- **Overall Status:** `[PASS]`\n\n")
	} else {
		sb.WriteString("- **Overall Status:** `[FAIL]`\n\n")
	}

	sb.WriteString("| Clause | Title | Status | Scope | Details |\n")
	sb.WriteString("| :--- | :--- | :--- | :--- | :--- |\n")
	for _, c := range r.Clauses {
		scope := "Mandatory"
		if !c.Required {
			scope = "Optional"
		}
		if c.Status == StatusNotCovered {
			scope = "Not Covered"
		}
		statusBadge := fmt.Sprintf("[%s]", c.Status)
		sb.WriteString(fmt.Sprintf("| **%s** | %s | `%s` | %s | %s |\n",
			c.ID, c.Title, statusBadge, scope, c.Details))
	}
	sb.WriteString("\n### Clause Audit Details\n\n")
	for _, c := range r.Clauses {
		sb.WriteString(fmt.Sprintf("#### Clause %s: %s (`%s`)\n\n", c.ID, c.Title, c.Status))
		sb.WriteString(fmt.Sprintf("%s\n\n", c.Details))
		if len(c.Subtests) > 0 {
			for _, st := range c.Subtests {
				icon := "PASS"
				if !st.Passed {
					icon = "FAIL"
				}
				if st.Message != "" {
					sb.WriteString(fmt.Sprintf("- `[%s]` **%s**: %s\n", icon, st.Name, st.Message))
				} else {
					sb.WriteString(fmt.Sprintf("- `[%s]` **%s**\n", icon, st.Name))
				}
			}
			sb.WriteString("\n")
		}
	}
	return sb.String()
}

func defaultScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

// Runner executes the engine contract conformance suite against an Engine.
type Runner struct {
	Scheme *runtime.Scheme
	Client client.Client
}

// NewRunner creates a conformance Runner with default scheme configuration.
func NewRunner() *Runner {
	return &Runner{Scheme: defaultScheme()}
}

func getClient(eng engine.Engine, prov snapshot.Provider, r *Runner) client.Client {
	type clientHolder interface {
		Client() client.Client
	}
	type engineUnwrapper interface {
		Unwrap() engine.Engine
	}
	type providerUnwrapper interface {
		Unwrap() snapshot.Provider
	}

	currProv := prov
	for currProv != nil {
		if ch, ok := currProv.(clientHolder); ok && ch.Client() != nil {
			return ch.Client()
		}
		if uw, ok := currProv.(providerUnwrapper); ok {
			currProv = uw.Unwrap()
		} else {
			break
		}
	}

	currEng := eng
	for currEng != nil {
		if ch, ok := currEng.(clientHolder); ok && ch.Client() != nil {
			return ch.Client()
		}
		if uw, ok := currEng.(engineUnwrapper); ok {
			currEng = uw.Unwrap()
		} else {
			break
		}
	}

	if r != nil && r.Client != nil {
		return r.Client
	}
	return nil
}

// Evaluate audits eng against contract clauses C1–C11 without testing.T dependencies.
// It returns a Report reflecting full clause outcomes.
func (r *Runner) Evaluate(eng engine.Engine) *Report {
	if eng == nil {
		return &Report{Passed: false}
	}

	rep := &Report{
		EngineName: eng.Name(),
		APIGroup:   eng.APIGroup(),
		TestedAt:   time.Now(),
		Passed:     true,
	}

	clauses := []func(engine.Engine) ClauseResult{
		r.auditC1Identity,
		r.auditC2Eligibility,
		r.auditC3CheckpointOutcomes,
		r.auditC4SourceFencing,
		r.auditC5RestoreOrder,
		r.auditC6RestoreOutcomeSignal,
		r.auditC7FailureClassification,
		r.auditC8PlacementConstraints,
		r.auditC9CancellationAndCleanup,
		r.auditC10Security,
		r.auditC11Observability,
	}

	for _, audit := range clauses {
		cr := audit(eng)
		if cr.Status == StatusFailed {
			rep.Passed = false
		}
		rep.Clauses = append(rep.Clauses, cr)
	}

	return rep
}

// Run executes the conformance suite against eng, logging any failing assertions to t
// and returning a structured Report.
func (r *Runner) Run(t *testing.T, eng engine.Engine) *Report {
	t.Helper()
	if eng == nil {
		t.Fatal("engine under conformance test is nil")
	}

	rep := r.Evaluate(eng)
	for _, cr := range rep.Clauses {
		for _, st := range cr.Subtests {
			if !st.Passed {
				t.Errorf("[%s] %s: %s", cr.ID, st.Name, st.Message)
			}
		}
	}
	return rep
}

// auditC1Identity audits Clause C1: Identity and targeting.
func (r *Runner) auditC1Identity(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC1Identity,
		Title:    "Identity and targeting",
		Required: true,
		Status:   StatusPassed,
	}

	// 1. Non-empty name
	name := eng.Name()
	if name == "" {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Name non-empty", Passed: false, Message: "Name() returned empty string"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Name non-empty", Passed: true, Message: fmt.Sprintf("name=%q", name)})
	}

	// 2. Valid API group
	group := eng.APIGroup()
	if err := engine.ValidateAPIGroup(group); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "APIGroup valid", Passed: false, Message: fmt.Sprintf("ValidateAPIGroup(%q) failed: %v", group, err)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "APIGroup valid", Passed: true, Message: fmt.Sprintf("group=%q", group)})
	}

	// 3. Restore annotation matches APIGroup
	ann := eng.RestoreAnnotation()
	expectedAnn := engine.RestoreAnnotation(group)
	if ann != expectedAnn {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreAnnotation matches group", Passed: false, Message: fmt.Sprintf("RestoreAnnotation() = %q, want %q", ann, expectedAnn)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreAnnotation matches group", Passed: true, Message: fmt.Sprintf("annotation=%q", ann)})
	}

	// 4. Runtime classes non-empty
	rClasses := eng.RuntimeClasses()
	if len(rClasses) == 0 {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RuntimeClasses non-empty", Passed: false, Message: "RuntimeClasses() returned empty slice"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RuntimeClasses non-empty", Passed: true, Message: fmt.Sprintf("classes=%v", rClasses)})
	}

	// 5. NamespaceResources: fails closed on nil PM
	if _, err := eng.NamespaceResources(nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources nil guard", Passed: false, Message: "expected error when pm == nil"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources nil guard", Passed: true})
	}

	// 6. NamespaceResources: valid GCS location
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pm",
			Namespace: "default",
			UID:       types.UID("pm-uid-1234"),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{
				Location: "gs://conformance-bucket/snapshots",
			},
		},
	}
	objs, err := eng.NamespaceResources(pm)
	if err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources valid GCS", Passed: false, Message: fmt.Sprintf("NamespaceResources error: %v", err)})
	} else if len(objs) == 0 {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources non-empty", Passed: false, Message: "rendered zero resources"})
	} else {
		hasOwnerLabel := false
		for _, obj := range objs {
			if obj.GetLabels()[engine.OwnerUIDLabel] == string(pm.UID) {
				hasOwnerLabel = true
				break
			}
		}
		if !hasOwnerLabel {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources OwnerUIDLabel", Passed: false, Message: "missing OwnerUIDLabel matching PodMigration UID"})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources valid GCS", Passed: true, Message: fmt.Sprintf("%d object(s) rendered with owner label", len(objs))})
		}
	}

	// 7. NamespaceResources: invalid location returns *StorageLocationError (#233)
	invalidPM := pm.DeepCopy()
	invalidPM.Spec.Storage.Location = "invalid://bucket/path"
	_, err = eng.NamespaceResources(invalidPM)
	if err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources rejects invalid location", Passed: false, Message: "accepted invalid location without error"})
	} else {
		var locErr *engine.StorageLocationError
		if !errors.As(err, &locErr) {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources StorageLocationError", Passed: false, Message: fmt.Sprintf("expected *engine.StorageLocationError via errors.As, got %T: %v", err, err)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources rejects invalid location", Passed: true, Message: fmt.Sprintf("rejected with *StorageLocationError: %v", locErr)})
		}
	}

	if cr.Status == StatusPassed {
		cr.Details = fmt.Sprintf("Name %q, Group %q, Annotation %q, and %d runtime class(es) verified.", name, group, ann, len(rClasses))
	} else {
		cr.Details = "Contract C1 violations detected in identity or resource rendering."
	}
	return cr
}

// auditC2Eligibility audits Clause C2: Eligibility and policy resolution.
func (r *Runner) auditC2Eligibility(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC2Eligibility,
		Title:    "Eligibility and policy resolution",
		Required: true,
		Status:   StatusPassed,
	}

	// 1. Eligible fails closed on nil pod
	verdict := eng.Eligible(nil, nil)
	if verdict.OK || verdict.Reason == "" {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Eligible nil pod guard", Passed: false, Message: fmt.Sprintf("verdict=%+v; want OK=false with Reason", verdict)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Eligible nil pod guard", Passed: true, Message: fmt.Sprintf("refusal reason: %q", verdict.Reason)})
	}

	// 2. ResolvePolicy fails on nil pod
	ctx := context.Background()
	if _, err := eng.ResolvePolicy(ctx, nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy nil pod guard", Passed: false, Message: "expected error when pod == nil"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy nil pod guard", Passed: true})
	}

	// 3. ResolvePolicy returns nil, nil when no policy exists in cache (MigrationSkippedNoPolicy)
	dummyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: "default",
			Labels:    map[string]string{"app": "demo"},
		},
	}
	polRef, err := eng.ResolvePolicy(ctx, dummyPod)
	if err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy empty cache", Passed: false, Message: fmt.Sprintf("unexpected error on empty cache: %v", err)})
	} else if polRef != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy empty cache", Passed: false, Message: fmt.Sprintf("returned non-nil PolicyRef %+v on empty cache", polRef)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy empty cache", Passed: true, Message: "returned nil PolicyRef (allows clean cold fallback)"})
	}

	// 4. Engine-specific refusal invariants (CRIU C2)
	if eng.Name() == engine.CRIUName {
		readyNode := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: "criu-ready-node",
				Annotations: map[string]string{
					engine.CRIUNodeReadyAnnotation: "true",
				},
			},
		}

		// a. hostNetwork pod refusal (#229)
		hostNetPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "hostnet-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "c1", Command: []string{"sh"}},
				},
				HostNetwork: true,
				NodeName:    "criu-ready-node",
			},
		}
		vHost := eng.Eligible(hostNetPod, readyNode)
		if vHost.OK || !strings.Contains(strings.ToLower(vHost.Reason), "hostnetwork") {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU hostNetwork refusal", Passed: false, Message: fmt.Sprintf("verdict=%+v; want OK=false with Reason mentioning hostNetwork", vHost)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU hostNetwork refusal", Passed: true, Message: fmt.Sprintf("refusal reason: %q", vHost.Reason)})
		}

		// b. multi-container pod refusal
		multiPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "multicont-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "c1", Command: []string{"sh"}},
					{Name: "c2", Command: []string{"sh"}},
				},
				NodeName: "criu-ready-node",
			},
		}
		vMulti := eng.Eligible(multiPod, readyNode)
		if vMulti.OK || !strings.Contains(strings.ToLower(vMulti.Reason), "container") {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU multi-container refusal", Passed: false, Message: fmt.Sprintf("verdict=%+v; want OK=false with Reason mentioning container", vMulti)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU multi-container refusal", Passed: true, Message: fmt.Sprintf("refusal reason: %q", vMulti.Reason)})
		}

		// c. nil node refusal
		singlePod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: "single-pod", Namespace: "default"},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "c1", Command: []string{"sh"}},
				},
			},
		}
		vNilNode := eng.Eligible(singlePod, nil)
		if vNilNode.OK || vNilNode.Reason == "" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU nil node refusal", Passed: false, Message: fmt.Sprintf("verdict=%+v; want OK=false with non-empty Reason", vNilNode)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CRIU nil node refusal", Passed: true, Message: fmt.Sprintf("refusal reason: %q", vNilNode.Reason)})
		}
	}

	if cr.Status == StatusPassed {
		if eng.Name() == engine.CRIUName {
			cr.Details = "Nil pod guard, policy cache miss defaults, and CRIU refusal invariants (hostNetwork, multi-container, nil node) verified."
		} else {
			cr.Details = "Nil pod guard and policy resolution cache miss defaults verified."
		}
	} else {
		cr.Details = "Contract C2 eligibility/policy resolution violations detected."
	}
	return cr
}

// auditC3CheckpointOutcomes audits Clause C3: Checkpoint outcomes.
func (r *Runner) auditC3CheckpointOutcomes(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC3CheckpointOutcomes,
		Title:    "Checkpoint outcomes",
		Required: true,
		Status:   StatusPassed,
	}

	prov := eng.Snapshots()
	if prov == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Snapshots provider non-nil", Passed: false, Message: "Snapshots() returned nil"})
		cr.Details = "Engine does not supply a snapshot provider."
		return cr
	}
	cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Snapshots provider non-nil", Passed: true})

	// Check provider group and annotation alignment if provider exposes them
	type apiGroupGetter interface{ APIGroup() string }
	type groupGetter interface{ Group() string }
	type restoreAnnotationGetter interface{ RestoreAnnotation() string }

	var provGroup string
	if agg, ok := prov.(apiGroupGetter); ok {
		provGroup = agg.APIGroup()
	} else if gg, ok := prov.(groupGetter); ok {
		provGroup = gg.Group()
	}

	if provGroup != "" {
		if provGroup != eng.APIGroup() {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider group alignment", Passed: false, Message: fmt.Sprintf("Provider group %q != Engine APIGroup %q", provGroup, eng.APIGroup())})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider group alignment", Passed: true, Message: fmt.Sprintf("group=%q", provGroup)})
		}
	}

	if rg, ok := prov.(restoreAnnotationGetter); ok {
		if got := rg.RestoreAnnotation(); got != eng.RestoreAnnotation() {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider restore annotation alignment", Passed: false, Message: fmt.Sprintf("Provider RestoreAnnotation %q != Engine RestoreAnnotation %q", got, eng.RestoreAnnotation())})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider restore annotation alignment", Passed: true, Message: fmt.Sprintf("annotation=%q", got)})
		}
	}

	ctx := context.Background()
	cl := getClient(eng, prov, r)

	// If no client is available, verify CheckStatus on absent trigger as a baseline.
	if cl == nil {
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-job-c3",
				Namespace: "default",
				UID:       types.UID("job-c3-uid"),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				TargetPodUID: "target-c3-uid",
			},
		}
		st, err := prov.CheckStatus(ctx, job, "web-absent")
		if err != nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus absent trigger", Passed: false, Message: fmt.Sprintf("CheckStatus returned error on absent trigger: %v", err)})
		} else if st == nil || st.Phase != snapshot.PhaseInProgress {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus absent trigger", Passed: false, Message: fmt.Sprintf("expected PhaseInProgress on absent trigger, got: %+v", st)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus absent trigger", Passed: true, Message: fmt.Sprintf("returns InProgress (phase=%s, reason=%q)", st.Phase, st.Reason)})
		}
		if cr.Status == StatusPassed {
			cr.Details = fmt.Sprintf("Snapshot provider configured and aligned with API group %q.", eng.APIGroup())
		} else {
			cr.Details = "Contract C3 snapshot provider violations detected."
		}
		return cr
	}

	// When a client is available, drive CheckStatus over the canonical PodSnapshot lifecycle fixtures.
	type fixtureCase struct {
		name         string
		subtestName  string
		podName      string
		targetUID    string
		snapName     string
		conditions   []map[string]interface{}
		finalizers   []string
		deleting     bool
		omitSnapshot bool // trigger exists, but snapshot is gone (#269)
		wantPhase    snapshot.Phase
		wantReason   string
		wantFound    *bool
	}

	tBool := true
	fBool := false
	nowTime := time.Now().UTC()
	recentStr := nowTime.Format(time.RFC3339)

	cases := []fixtureCase{
		{
			name:        "transient_ready_false",
			subtestName: "CheckStatus transient Ready=False is retryable (#75/#95)",
			podName:     "c3-transient",
			targetUID:   "uid-transient",
			snapName:    "snap-transient",
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "False", "reason": "InProgress", "lastTransitionTime": recentStr},
				{"type": "StorageReplicated", "status": "False", "reason": "AwaitingCheckpoint", "lastTransitionTime": recentStr},
				{"type": "Ready", "status": "False", "reason": "Failed", "message": "transient race", "lastTransitionTime": recentStr},
			},
			wantPhase: snapshot.PhaseInProgress,
			wantFound: &tBool,
		},
		{
			name:        "refused",
			subtestName: "CheckStatus Refused is terminal",
			podName:     "c3-refused",
			targetUID:   "uid-refused",
			snapName:    "snap-refused",
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "False", "reason": "Refused", "message": "unsupported pod shape"},
				{"type": "Ready", "status": "False", "reason": "Refused", "message": "unsupported pod shape"},
			},
			wantPhase: snapshot.PhaseRefused,
		},
		{
			name:        "cancel_requested",
			subtestName: "CheckStatus CancelRequested is retryable",
			podName:     "c3-cancel",
			targetUID:   "uid-cancel",
			snapName:    "snap-cancel",
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "Unknown", "reason": "CancelRequested", "lastTransitionTime": recentStr},
			},
			wantPhase: snapshot.PhaseInProgress,
			wantFound: &tBool,
		},
		{
			name:        "checkpoint_timed_out",
			subtestName: "CheckStatus CheckpointTimedOut is retryable",
			podName:     "c3-timeout",
			targetUID:   "uid-timeout",
			snapName:    "snap-timeout",
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "Unknown", "reason": "CheckpointTimedOut", "lastTransitionTime": recentStr},
			},
			wantPhase: snapshot.PhaseInProgress,
			wantFound: &tBool,
		},
		{
			name:        "being_deleted",
			subtestName: "CheckStatus snapshot being deleted is retryable (#265)",
			podName:     "c3-deleting",
			targetUID:   "uid-deleting",
			snapName:    "snap-deleting",
			finalizers:  []string{"criu.podmigration.gke.io/archive-cleanup"},
			deleting:    true,
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "True", "reason": "Succeeded"},
				{"type": "Ready", "status": "True", "reason": "AllSnapshotsAvailable"},
			},
			wantPhase:  snapshot.PhaseInProgress,
			wantReason: snapshot.ReasonSnapshotDeleting,
			wantFound:  &tBool,
		},
		{
			name:         "snapshot_gone",
			subtestName:  "CheckStatus snapshot gone is retryable with Found=false (#269)",
			podName:      "c3-gone",
			targetUID:    "uid-gone",
			snapName:     "snap-gone",
			omitSnapshot: true,
			wantPhase:    snapshot.PhaseInProgress,
			wantFound:    &fBool,
		},
		{
			name:        "failed_terminal",
			subtestName: "CheckStatus Failed is terminal",
			podName:     "c3-failed",
			targetUID:   "uid-failed",
			snapName:    "snap-failed",
			conditions: []map[string]interface{}{
				{"type": "Checkpoint", "status": "False", "reason": "Failed", "message": "checkpointing container failed"},
			},
			wantPhase: snapshot.PhaseFailed,
		},
	}

	triggerGVK := snapshot.ManualTriggerGVK(eng.APIGroup())
	snapshotGVK := snapshot.PodSnapshotGVK(eng.APIGroup())
	jobNamespace := "default"

	for _, tc := range cases {
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("job-%s", tc.name),
				Namespace: jobNamespace,
				UID:       types.UID(fmt.Sprintf("job-%s-uid", tc.name)),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				TargetPodUID: tc.targetUID,
			},
		}
		triggerName := util.FormatPSMTName(tc.podName, tc.targetUID)

		// Create trigger pointing to tc.snapName
		trig := &unstructured.Unstructured{}
		trig.SetGroupVersionKind(triggerGVK)
		trig.SetName(triggerName)
		trig.SetNamespace(jobNamespace)
		_ = unstructured.SetNestedField(trig.Object, tc.snapName, "status", "snapshotCreated", "name")
		if err := cl.Create(ctx, trig); err != nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("failed to create trigger: %v", err)})
			continue
		}

		var snap *unstructured.Unstructured
		if !tc.omitSnapshot {
			snap = &unstructured.Unstructured{}
			snap.SetGroupVersionKind(snapshotGVK)
			snap.SetName(tc.snapName)
			snap.SetNamespace(jobNamespace)
			if len(tc.finalizers) > 0 {
				snap.SetFinalizers(tc.finalizers)
			}
			if tc.deleting {
				delTime := metav1.Now()
				snap.SetDeletionTimestamp(&delTime)
			}
			if len(tc.conditions) > 0 {
				condSlice := make([]interface{}, len(tc.conditions))
				for i, c := range tc.conditions {
					condSlice[i] = c
				}
				_ = unstructured.SetNestedSlice(snap.Object, condSlice, "status", "conditions")
			}
			if err := cl.Create(ctx, snap); err != nil {
				cr.Status = StatusFailed
				cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("failed to create snapshot: %v", err)})
				_ = cl.Delete(ctx, trig)
				continue
			}
			if tc.deleting {
				// Deleting an object with finalizers in fake.Client sets metadata.deletionTimestamp
				_ = cl.Delete(ctx, snap)
			}
		}

		st, err := prov.CheckStatus(ctx, job, tc.podName)

		// Clean up created objects immediately
		_ = cl.Delete(ctx, trig)
		if snap != nil {
			currentSnap := &unstructured.Unstructured{}
			currentSnap.SetGroupVersionKind(snapshotGVK)
			if err := cl.Get(ctx, types.NamespacedName{Namespace: jobNamespace, Name: tc.snapName}, currentSnap); err == nil {
				if len(currentSnap.GetFinalizers()) > 0 {
					currentSnap.SetFinalizers(nil)
					_ = cl.Update(ctx, currentSnap)
				}
				_ = cl.Delete(ctx, currentSnap)
			}
		}

		if err != nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("CheckStatus returned error: %v", err)})
		} else if st == nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: "CheckStatus returned nil status"})
		} else if st.Phase != tc.wantPhase {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("got Phase=%s (reason=%q), want Phase=%s", st.Phase, st.Reason, tc.wantPhase)})
		} else if tc.wantReason != "" && st.Reason != tc.wantReason {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("got Reason=%q, want Reason=%q", st.Reason, tc.wantReason)})
		} else if tc.wantFound != nil && st.Found != *tc.wantFound {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: false, Message: fmt.Sprintf("got Found=%v, want Found=%v", st.Found, *tc.wantFound)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: tc.subtestName, Passed: true, Message: fmt.Sprintf("phase=%s, reason=%q, found=%v", st.Phase, st.Reason, st.Found)})
		}
	}

	if cr.Status == StatusPassed {
		cr.Details = fmt.Sprintf("Snapshot provider aligned with API group %q; terminal and retryable outcomes verified across lifecycle fixtures (#75, #95, #265, #269).", eng.APIGroup())
	} else {
		cr.Details = "Contract C3 snapshot provider or CheckStatus outcome classification violations detected."
	}
	return cr
}

// auditC4SourceFencing audits Clause C4: Source fencing.
func (r *Runner) auditC4SourceFencing(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC4SourceFencing,
		Title:    "Source fencing",
		Required: true,
		Status:   StatusPassed,
	}

	guarantees := eng.Guarantees()
	fencing := guarantees.Fencing

	validModes := map[engine.FencingMode]bool{
		engine.FencingFrozen:  true,
		engine.FencingStopped: true,
		engine.FencingNone:    true,
	}
	if !validModes[fencing] {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingMode declaration valid", Passed: false, Message: fmt.Sprintf("invalid FencingMode %q", fencing)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingMode declaration valid", Passed: true, Message: fmt.Sprintf("mode=%s", fencing)})
	}

	// Test AllowsFencing behavior
	normalPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-normal"}}
	optedInPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "pod-opted-in",
			Annotations: map[string]string{engine.AnnotationAllowUnfenced: "true"},
		},
	}

	allowedNormal, reasonNormal := engine.AllowsFencing(normalPod, eng)
	allowedOptedIn, _ := engine.AllowsFencing(optedInPod, eng)

	if fencing == engine.FencingNone {
		if allowedNormal {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone refuses un-annotated pod", Passed: false, Message: "allowed un-annotated pod under FencingNone"})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone refuses un-annotated pod", Passed: true, Message: fmt.Sprintf("refused with reason: %s", reasonNormal)})
		}
		if !allowedOptedIn {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone accepts opted-in pod", Passed: false, Message: "refused opted-in pod under FencingNone"})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone accepts opted-in pod", Passed: true})
		}
	} else {
		if !allowedNormal {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Fenced engine accepts pod", Passed: false, Message: fmt.Sprintf("refused pod under mode %s: %s", fencing, reasonNormal)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Fenced engine accepts pod", Passed: true, Message: fmt.Sprintf("accepted under mode %s", fencing)})
		}
	}

	if cr.Status == StatusPassed {
		cr.Details = fmt.Sprintf("Declared fencing mode: %s. AllowsFencing declaration verified.", fencing)
	} else {
		cr.Details = "Contract C4 source fencing declaration violations detected."
	}
	return cr
}

// auditC5RestoreOrder audits Clause C5: Restore order.
func (r *Runner) auditC5RestoreOrder(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC5RestoreOrder,
		Title:    "Restore order",
		Required: true,
		Status:   StatusPassed,
	}

	orderer, isOrderer := eng.(engine.RestoreOrderer)
	if !isOrderer {
		cr.Status = StatusSkipped
		cr.Details = "Engine does not implement optional RestoreOrderer capability; default annotation used."
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreOrderer optional capability", Passed: true, Message: "Not implemented (using default RestoreAnnotation)"})
		return cr
	}

	job := &pmv1alpha1.PodMigrationJob{
		Status: pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "ps-snap-1234"},
	}

	// 1. Guard checks on nil inputs
	if err := orderer.OrderRestore(nil, job); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil pod guard", Passed: false, Message: "accepted nil pod without error"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil pod guard", Passed: true})
	}

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-0"}}
	if err := orderer.OrderRestore(pod, nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil job guard", Passed: false, Message: "accepted nil job without error"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil job guard", Passed: true})
	}

	emptyJob := &pmv1alpha1.PodMigrationJob{}
	if err := orderer.OrderRestore(pod, emptyJob); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore empty snapshotRef guard", Passed: false, Message: "accepted job with empty SnapshotRef"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore empty snapshotRef guard", Passed: true})
	}

	if err := orderer.OrderColdStartBypass(nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass nil pod guard", Passed: false, Message: "accepted nil pod without error"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass nil pod guard", Passed: true})
	}

	// 2. OrderRestore stamps order under RestoreAnnotation key
	orderPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-ordered"}}
	if err := orderer.OrderRestore(orderPod, job); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore valid", Passed: false, Message: fmt.Sprintf("OrderRestore error: %v", err)})
	} else {
		annKey := eng.RestoreAnnotation()
		stampedVal, stampedOk := orderPod.Annotations[annKey]
		if !stampedOk || stampedVal != "ps-snap-1234" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore key alignment", Passed: false, Message: fmt.Sprintf("pod.Annotations[%q] = %q (ok=%v), want %q (#135)", annKey, stampedVal, stampedOk, "ps-snap-1234")})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore key alignment", Passed: true, Message: fmt.Sprintf("stamped annotation %s=%q", annKey, stampedVal)})
		}

		snapRef, bypass, present := orderer.InspectRestoreOrder(orderPod)
		if !present || bypass || snapRef != "ps-snap-1234" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder after OrderRestore", Passed: false, Message: fmt.Sprintf("got (snapRef=%q, bypass=%v, present=%v), want (\"ps-snap-1234\", false, true)", snapRef, bypass, present)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder after OrderRestore", Passed: true, Message: "verified (snapRef, bypass=false, present=true)"})
		}
	}

	// 3. OrderColdStartBypass replaces an existing restore order
	if err := orderer.OrderColdStartBypass(orderPod); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass overwrites order", Passed: false, Message: fmt.Sprintf("OrderColdStartBypass error: %v", err)})
	} else {
		snapRef, bypass, present := orderer.InspectRestoreOrder(orderPod)
		if !present || !bypass || snapRef != "" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass overwrites order", Passed: false, Message: fmt.Sprintf("got (snapRef=%q, bypass=%v, present=%v), want (\"\", true, true)", snapRef, bypass, present)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass overwrites order", Passed: true, Message: "bypass cleanly replaces existing restore order"})
		}
	}

	// 4. OrderRestore re-orders over existing bypass
	job2 := &pmv1alpha1.PodMigrationJob{
		Status: pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "ps-snap-5678"},
	}
	if err := orderer.OrderRestore(orderPod, job2); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore re-orders over bypass", Passed: false, Message: fmt.Sprintf("OrderRestore error: %v", err)})
	} else {
		snapRef, bypass, present := orderer.InspectRestoreOrder(orderPod)
		if !present || bypass || snapRef != "ps-snap-5678" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore re-orders over bypass", Passed: false, Message: fmt.Sprintf("got (snapRef=%q, bypass=%v, present=%v), want (\"ps-snap-5678\", false, true)", snapRef, bypass, present)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore re-orders over bypass", Passed: true, Message: "re-ordered snapshotRef successfully"})
		}
	}

	// 5. Pristine unannotated pod inspect
	pristinePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-pristine"}}
	snapRef, bypass, present := orderer.InspectRestoreOrder(pristinePod)
	if present {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder pristine pod", Passed: false, Message: fmt.Sprintf("present=true on pristine unannotated pod (%q, %v)", snapRef, bypass)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder pristine pod", Passed: true, Message: "present=false on unannotated pod"})
	}

	if cr.Status == StatusPassed {
		if eng.Guarantees().RestoreOnce {
			cr.Details = fmt.Sprintf("Restore order annotation round-trip verified (key=%q); declares RestoreOnce=true.", eng.RestoreAnnotation())
		} else {
			cr.Details = fmt.Sprintf("Restore order annotation round-trip verified (key=%q); at-most-once restore not declared (RestoreOnce=false).", eng.RestoreAnnotation())
		}
	} else {
		cr.Details = "Contract C5 restore order annotation or validation violations detected."
	}
	return cr
}

// auditC6RestoreOutcomeSignal audits Clause C6: Restore outcome signal.
func (r *Runner) auditC6RestoreOutcomeSignal(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC6RestoreOutcomeSignal,
		Title:    "Restore outcome signal",
		Required: false,
		Status:   StatusPassed,
	}

	verifier, isVerifier := eng.(engine.RestoreVerifier)
	if !isVerifier {
		cr.Status = StatusSkipped
		cr.Details = "Engine does not implement optional RestoreVerifier capability; controller infers restore outcome from pod readiness."
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreVerifier capability", Passed: true, Message: "Not implemented (inferred from pod readiness)"})
		return cr
	}

	ctx := context.Background()
	job := &pmv1alpha1.PodMigrationJob{
		Status: pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "ps-snap-1"},
	}

	// 1. Nil pod guard
	if _, err := verifier.VerifyRestore(ctx, job, nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore nil pod guard", Passed: false, Message: "accepted nil pod without error"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore nil pod guard", Passed: true})
	}

	// 2. InProgress check on unready replacement (!Confirmed)
	unreadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "repl-unready", Namespace: "default"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	verdict, err := verifier.VerifyRestore(ctx, job, unreadyPod)
	if err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: false, Message: fmt.Sprintf("error: %v", err)})
	} else if verdict.Outcome != engine.RestoreInProgress {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: false, Message: fmt.Sprintf("got outcome %s, want RestoreInProgress", verdict.Outcome)})
	} else if verdict.Confirmed {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: false, Message: "unready pod reported Confirmed=true without positive restore evidence"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: true, Message: "returns InProgress with Confirmed=false"})
	}

	// 3. Ready pod with NO restore evidence must never report Confirmed=true
	readyPodNoSignal := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "repl-ready-no-signal", Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	verdictReady, errReady := verifier.VerifyRestore(ctx, job, readyPodNoSignal)
	if errReady != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore ready pod without signal", Passed: false, Message: fmt.Sprintf("error: %v", errReady)})
	} else if verdictReady.Confirmed {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore ready pod without signal", Passed: false, Message: "reported Confirmed=true for ready pod with no positive restore evidence"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore ready pod without signal", Passed: true, Message: fmt.Sprintf("Confirmed=false maintained without positive signal (outcome=%s)", verdictReady.Outcome)})
	}

	// 4. Failure signal check: pod with restore failure signal must yield ColdStart
	cl := getClient(eng, nil, r)
	failedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "repl-failed-signal",
			Namespace: "default",
			UID:       types.UID("repl-failed-signal-uid"),
			Annotations: map[string]string{
				"pod-migrate.io/psengine-restore": "failed",
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
	var warningEvt *corev1.Event
	if cl != nil {
		warningEvt = &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "repl-failed-signal-warning",
				Namespace: failedPod.Namespace,
			},
			InvolvedObject: corev1.ObjectReference{
				UID:       failedPod.UID,
				Name:      failedPod.Name,
				Namespace: failedPod.Namespace,
			},
			Type:          corev1.EventTypeWarning,
			Reason:        "FailedRestore",
			Message:       "failed to restore snapshot; falling back to a cold start",
			LastTimestamp: metav1.Now(),
		}
		_ = cl.Create(ctx, warningEvt)
	}

	verdictFail, errFail := verifier.VerifyRestore(ctx, job, failedPod)
	if warningEvt != nil && cl != nil {
		_ = cl.Delete(ctx, warningEvt)
	}

	if errFail != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore failure signal yields ColdStart", Passed: false, Message: fmt.Sprintf("error: %v", errFail)})
	} else if verdictFail.Outcome != engine.ColdStart {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore failure signal yields ColdStart", Passed: false, Message: fmt.Sprintf("got outcome %s, want ColdStart", verdictFail.Outcome)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore failure signal yields ColdStart", Passed: true, Message: fmt.Sprintf("ColdStart confirmed on failure signal (reason: %q)", verdictFail.Reason)})
	}

	if cr.Status == StatusPassed {
		cr.Details = "RestoreVerifier outcome signals, unconfirmed ready pod requirements, and ColdStart failure signals verified."
	} else {
		cr.Details = "Contract C6 restore outcome signal or ColdStart failure classification violations detected."
	}
	return cr
}

// auditC7FailureClassification audits Clause C7: Restore failure classification.
func (r *Runner) auditC7FailureClassification(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC7FailureClassify,
		Title:    "Restore failure classification",
		Required: true,
		Status:   StatusPassed,
	}

	failures := eng.RestoreFailures()
	if len(failures) == 0 {
		cr.Details = "Engine declares no container start-failure signatures (runc/default safe)."
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreFailures declared", Passed: true, Message: "empty classifier slice (safe)"})
		return cr
	}

	engClasses := make(map[string]bool)
	for _, c := range eng.RuntimeClasses() {
		engClasses[c] = true
	}

	for _, rf := range failures {
		if rf == nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Classifier non-nil", Passed: false, Message: "nil classifier in RestoreFailures()"})
			continue
		}
		rfName := rf.Name()
		if rfName == "" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Classifier name non-empty", Passed: false, Message: "classifier returned empty Name()"})
		}

		// A classifier must implement restore.RuntimeClassScoped (#132)
		scoped, ok := rf.(restore.RuntimeClassScoped)
		if !ok {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{
				Name:    "Classifier RuntimeClassScoped",
				Passed:  false,
				Message: fmt.Sprintf("classifier %q does not implement restore.RuntimeClassScoped; unscoped classifiers match across runtimes (#132)", rfName),
			})
			continue
		}
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Classifier RuntimeClassScoped", Passed: true, Message: fmt.Sprintf("%q implements RuntimeClassScoped", rfName)})

		// A classifier must cover at least one runtime class served by the engine
		coversOwn := false
		for _, ownClass := range eng.RuntimeClasses() {
			if scoped.CoversRuntimeClass(ownClass) {
				coversOwn = true
				break
			}
		}
		if !coversOwn {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{
				Name:    "Classifier runtime class coverage",
				Passed:  false,
				Message: fmt.Sprintf("classifier %q covers none of engine %s own runtime classes %v", rfName, eng.Name(), eng.RuntimeClasses()),
			})
			continue
		}
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Classifier runtime class coverage", Passed: true, Message: fmt.Sprintf("%q covers served runtime class(es)", rfName)})

		// Probe standard classes, engine's own classes, DefaultRuntimeClassToken, and an unsupported sentinel
		checkClasses := []string{
			eligibility.DefaultRuntimeClassToken,
			"unsupported-sentinel-runtime-class",
		}
		for _, stdClass := range []string{"gvisor", "runc"} {
			if !engClasses[stdClass] {
				checkClasses = append(checkClasses, stdClass)
			}
		}

		for _, checkClass := range checkClasses {
			if scoped.CoversRuntimeClass(checkClass) && !engClasses[checkClass] {
				cr.Status = StatusFailed
				cr.Subtests = append(cr.Subtests, SubtestResult{
					Name:    "Classifier runtime class scope",
					Passed:  false,
					Message: fmt.Sprintf("classifier %q covers runtime class %q not served by engine %v (#132)", rfName, checkClass, eng.RuntimeClasses()),
				})
			}
		}
	}

	if cr.Status == StatusPassed {
		cr.Details = fmt.Sprintf("%d restore failure classifier(s) verified within engine runtime scope.", len(failures))
	} else {
		cr.Details = "Restore failure classifiers exceed engine runtime classes (breaches cross-runtime safety #132)."
	}
	return cr
}

// auditC8PlacementConstraints audits Clause C8: Placement constraints.
func (r *Runner) auditC8PlacementConstraints(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC8Placement,
		Title:    "Placement constraints",
		Required: false,
		Status:   StatusPassed,
	}

	advisor, isAdvisor := eng.(engine.PlacementAdvisor)
	if !isAdvisor {
		cr.Status = StatusSkipped
		cr.Details = "Engine does not implement optional PlacementAdvisor capability; replacement can schedule on any node."
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "PlacementAdvisor capability", Passed: true, Message: "Not implemented (replacement unconstrained)"})
		return cr
	}

	ctx := context.Background()
	jobWithMachine := &pmv1alpha1.PodMigrationJob{
		Status: pmv1alpha1.PodMigrationJobStatus{
			OriginNodeInstanceType: "n2-standard-4",
		},
	}
	jobBefore := jobWithMachine.DeepCopy()

	// 1. Determinism and job immutability test: repeated calls must return identical requirements
	reqs1, err1 := advisor.Placement(ctx, jobWithMachine)
	reqs2, err2 := advisor.Placement(ctx, jobWithMachine)
	if err1 != nil || err2 != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement execution", Passed: false, Message: fmt.Sprintf("Placement returned error: %v / %v", err1, err2)})
	} else if !reflect.DeepEqual(jobBefore, jobWithMachine) {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement job immutability", Passed: false, Message: "Placement mutated input PodMigrationJob"})
	} else if !reflect.DeepEqual(reqs1, reqs2) {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement determinism", Passed: false, Message: fmt.Sprintf("non-deterministic return values: %v vs %v", reqs1, reqs2)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement determinism", Passed: true, Message: fmt.Sprintf("returned %d requirement(s) identically across calls", len(reqs1))})
	}

	// 2. Validate operators, keys, and values match selection.Operator requirements (#14)
	operators := map[corev1.NodeSelectorOperator]selection.Operator{
		corev1.NodeSelectorOpIn:           selection.In,
		corev1.NodeSelectorOpNotIn:        selection.NotIn,
		corev1.NodeSelectorOpExists:       selection.Exists,
		corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist,
		corev1.NodeSelectorOpGt:           selection.GreaterThan,
		corev1.NodeSelectorOpLt:           selection.LessThan,
	}

	for i, req := range reqs1 {
		if errs := validation.IsQualifiedName(req.Key); len(errs) > 0 {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Requirement %d key valid", i), Passed: false, Message: fmt.Sprintf("invalid label key %q: %v", req.Key, errs)})
		}
		op, ok := operators[req.Operator]
		if !ok {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Requirement %d operator valid", i), Passed: false, Message: fmt.Sprintf("invalid operator %q", req.Operator)})
		} else if _, err := labels.NewRequirement(req.Key, op, req.Values); err != nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Requirement %d value validity", i), Passed: false, Message: fmt.Sprintf("invalid requirement (%s %s %v): %v", req.Key, req.Operator, req.Values, err)})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Requirement %d valid", i), Passed: true, Message: fmt.Sprintf("%s %s %v", req.Key, req.Operator, req.Values)})
		}
	}

	// 3. Clean fallback when machine type is empty/unknown
	emptyJob := &pmv1alpha1.PodMigrationJob{}
	reqsEmpty, errEmpty := advisor.Placement(ctx, emptyJob)
	if errEmpty != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: false, Message: fmt.Sprintf("returned error on empty facts: %v", errEmpty)})
	} else if len(reqsEmpty) > 0 {
		for i, req := range reqsEmpty {
			op, ok := operators[req.Operator]
			if !ok {
				cr.Status = StatusFailed
				cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Static requirement %d operator", i), Passed: false, Message: fmt.Sprintf("unknown operator %q", req.Operator)})
			} else if _, err := labels.NewRequirement(req.Key, op, req.Values); err != nil {
				cr.Status = StatusFailed
				cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Static requirement %d validity", i), Passed: false, Message: fmt.Sprintf("invalid requirement: %v", err)})
			}
		}
		if cr.Status != StatusFailed {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: true, Message: fmt.Sprintf("%d static requirement(s)", len(reqsEmpty))})
		}
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: true, Message: "returns 0 requirements on unknown source facts"})
	}

	if cr.Status == StatusPassed {
		cr.Details = "PlacementAdvisor capability verified for deterministic label requirements."
	} else {
		cr.Details = "Contract C8 placement constraint violations detected."
	}
	return cr
}

// auditC9CancellationAndCleanup audits Clause C9: Cancellation and cleanup.
func (r *Runner) auditC9CancellationAndCleanup(eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC9CancellationCleanup,
		Title:    "Cancellation and cleanup",
		Required: true,
		Status:   StatusPassed,
	}

	prov := eng.Snapshots()
	if prov == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Snapshots provider cleanup", Passed: false, Message: "provider is nil"})
		cr.Details = "Cannot test cleanup: provider is nil."
		return cr
	}

	ctx := context.Background()
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job-cleanup",
			Namespace: "default",
			UID:       types.UID("job-cleanup-uid"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			TargetPodUID: "target-cleanup-uid",
		},
	}
	podName := "web-cleanup-0"

	// 1. Create a trigger first
	if _, err := prov.EnsureTrigger(ctx, job, podName); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Trigger creation before cleanup", Passed: false, Message: fmt.Sprintf("EnsureTrigger error: %v", err)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Trigger creation before cleanup", Passed: true})
	}

	// 2. CheckStatus should see the trigger exists (PhaseInProgress and TriggerUnanswered == true)
	st, err := prov.CheckStatus(ctx, job, podName)
	if err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus on created trigger", Passed: false, Message: fmt.Sprintf("CheckStatus error: %v", err)})
	} else if st == nil || st.Phase != snapshot.PhaseInProgress || !st.TriggerUnanswered {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus on created trigger", Passed: false, Message: fmt.Sprintf("unexpected status: %+v (expected PhaseInProgress and TriggerUnanswered=true)", st)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus on created trigger", Passed: true, Message: fmt.Sprintf("phase=%s, unanswered=%v", st.Phase, st.TriggerUnanswered)})
	}

	// 3. First Cleanup deletes the trigger
	if err := prov.Cleanup(ctx, job, podName); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup trigger", Passed: false, Message: fmt.Sprintf("Cleanup returned error: %v", err)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup trigger", Passed: true})
	}

	// 4. Verify trigger is gone (CheckStatus no longer reports TriggerUnanswered: true)
	stAfter, errAfter := prov.CheckStatus(ctx, job, podName)
	if errAfter != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckStatus after cleanup", Passed: false, Message: fmt.Sprintf("CheckStatus error: %v", errAfter)})
	} else if stAfter != nil && stAfter.TriggerUnanswered {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup removes trigger", Passed: false, Message: "trigger still reported as existing/unanswered after Cleanup"})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup removes trigger", Passed: true, Message: "trigger successfully removed"})
	}

	// 5. CheckpointState if CheckpointReporter is implemented
	if reporter, ok := prov.(snapshot.CheckpointReporter); ok {
		state, err := reporter.CheckpointState(ctx, job, podName)
		if err != nil {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckpointState after cleanup", Passed: false, Message: fmt.Sprintf("CheckpointState error: %v", err)})
		} else if !state.Settled {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckpointState settled after cleanup", Passed: false, Message: "state.Settled = false after trigger cleanup"})
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "CheckpointState settled after cleanup", Passed: true, Message: fmt.Sprintf("Settled=%v, Completed=%v", state.Settled, state.Completed)})
		}
	}

	// 6. Second Cleanup is idempotent
	if err := prov.Cleanup(ctx, job, podName); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup idempotency", Passed: false, Message: fmt.Sprintf("idempotent Cleanup returned error: %v", err)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup idempotency", Passed: true, Message: "idempotent on already deleted trigger"})
	}

	if cr.Status == StatusPassed {
		cr.Details = "Snapshot provider trigger creation, deletion, idempotency, and settled state confirmed."
	} else {
		cr.Details = "Contract C9 cancellation and cleanup violations detected."
	}
	return cr
}

// auditC10Security records the coverage state of Clause C10: Security.
func (r *Runner) auditC10Security(eng engine.Engine) ClauseResult {
	return ClauseResult{
		ID:       ClauseC10Security,
		Title:    "Security isolation and privilege boundaries",
		Required: false,
		Status:   StatusNotCovered,
		Details:  "Not checked by this adapter suite; tracked in #129.",
	}
}

// auditC11Observability records the coverage state of Clause C11: Observability.
func (r *Runner) auditC11Observability(eng engine.Engine) ClauseResult {
	return ClauseResult{
		ID:       ClauseC11Observability,
		Title:    "Telemetry and metric fidelity",
		Required: false,
		Status:   StatusNotCovered,
		Details:  "Not checked by this adapter suite; observability and telemetry fidelity tracked in controller telemetry suites.",
	}
}
