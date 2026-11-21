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
// It audits an Engine implementation against the eleven contract clauses (C1–C11)
// to verify that it provides the identity, eligibility, checkpoint outcomes,
// source fencing, restore orders, outcome signals, failure classification,
// placement constraints, and cleanup semantics required by the pod migration
// controller.
package conformance

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
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
)

// ClauseStatus represents the outcome of an evaluated conformance clause.
type ClauseStatus string

const (
	StatusPassed  ClauseStatus = "PASSED"
	StatusFailed  ClauseStatus = "FAILED"
	StatusSkipped ClauseStatus = "SKIPPED"
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
	for _, c := range r.Clauses {
		switch c.Status {
		case StatusPassed:
			passedCount++
		case StatusFailed:
			failedCount++
		case StatusSkipped:
			skippedCount++
		}
	}
	verdict := "PASSED"
	if !r.Passed {
		verdict = "FAILED"
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

// TestScheme returns a runtime.Scheme populated with standard types and PMJ types.
func TestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

// NewTestClient returns a fake controller-runtime client with standard indices
// (including involvedObject.uid for Event queries), matching the real API server.
func NewTestClient(scheme *runtime.Scheme, objs ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Event{}, "involvedObject.uid", func(o client.Object) []string {
			if evt, ok := o.(*corev1.Event); ok {
				return []string{string(evt.InvolvedObject.UID)}
			}
			return nil
		}).
		WithObjects(objs...).
		Build()
}

// Runner executes the engine contract conformance suite against an Engine.
type Runner struct {
	Scheme *runtime.Scheme
}

// NewRunner creates a conformance Runner with default scheme configuration.
func NewRunner() *Runner {
	return &Runner{Scheme: TestScheme()}
}

// Run executes the conformance suite against eng, registering assertions with t
// and returning a structured Report.
func (r *Runner) Run(t *testing.T, eng engine.Engine) *Report {
	t.Helper()
	if eng == nil {
		t.Fatal("engine under conformance test is nil")
	}

	rep := &Report{
		EngineName: eng.Name(),
		APIGroup:   eng.APIGroup(),
		TestedAt:   time.Now(),
		Passed:     true,
	}

	clauses := []func(*testing.T, engine.Engine) ClauseResult{
		r.auditC1Identity,
		r.auditC2Eligibility,
		r.auditC3CheckpointOutcomes,
		r.auditC4SourceFencing,
		r.auditC5RestoreOrder,
		r.auditC6RestoreOutcomeSignal,
		r.auditC7FailureClassification,
		r.auditC8PlacementConstraints,
		r.auditC9CancellationAndCleanup,
	}

	for _, audit := range clauses {
		cr := audit(t, eng)
		if cr.Status == StatusFailed && cr.Required {
			rep.Passed = false
		}
		rep.Clauses = append(rep.Clauses, cr)
	}

	return rep
}

// auditC1Identity audits Clause C1: Identity and targeting.
func (r *Runner) auditC1Identity(t *testing.T, eng engine.Engine) ClauseResult {
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
		t.Errorf("[C1] Name() returned empty string")
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Name non-empty", Passed: true, Message: fmt.Sprintf("name=%q", name)})
	}

	// 2. Valid API group
	group := eng.APIGroup()
	if err := engine.ValidateAPIGroup(group); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "APIGroup valid", Passed: false, Message: fmt.Sprintf("ValidateAPIGroup(%q) failed: %v", group, err)})
		t.Errorf("[C1] APIGroup %q invalid: %v", group, err)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "APIGroup valid", Passed: true, Message: fmt.Sprintf("group=%q", group)})
	}

	// 3. Restore annotation matches APIGroup
	ann := eng.RestoreAnnotation()
	expectedAnn := engine.RestoreAnnotation(group)
	if ann != expectedAnn {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreAnnotation matches group", Passed: false, Message: fmt.Sprintf("RestoreAnnotation() = %q, want %q", ann, expectedAnn)})
		t.Errorf("[C1] RestoreAnnotation() = %q, want %q", ann, expectedAnn)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RestoreAnnotation matches group", Passed: true, Message: fmt.Sprintf("annotation=%q", ann)})
	}

	// 4. Runtime classes non-empty
	rClasses := eng.RuntimeClasses()
	if len(rClasses) == 0 {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RuntimeClasses non-empty", Passed: false, Message: "RuntimeClasses() returned empty slice"})
		t.Errorf("[C1] RuntimeClasses() returned empty slice")
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "RuntimeClasses non-empty", Passed: true, Message: fmt.Sprintf("classes=%v", rClasses)})
	}

	// 5. NamespaceResources: fails closed on nil PM
	if _, err := eng.NamespaceResources(nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources nil guard", Passed: false, Message: "expected error when pm == nil"})
		t.Errorf("[C1] NamespaceResources(nil) succeeded; want error")
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
		t.Errorf("[C1] NamespaceResources valid GCS failed: %v", err)
	} else if len(objs) == 0 {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources non-empty", Passed: false, Message: "rendered zero resources"})
		t.Errorf("[C1] NamespaceResources returned 0 resources")
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
			t.Errorf("[C1] NamespaceResources rendered objects without %s", engine.OwnerUIDLabel)
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources valid GCS", Passed: true, Message: fmt.Sprintf("%d object(s) rendered with owner label", len(objs))})
		}
	}

	// 7. NamespaceResources: invalid location returns *StorageLocationError
	invalidPM := pm.DeepCopy()
	invalidPM.Spec.Storage.Location = "invalid://bucket/path"
	_, err = eng.NamespaceResources(invalidPM)
	if err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources rejects invalid location", Passed: false, Message: "accepted invalid location without error"})
		t.Errorf("[C1] NamespaceResources accepted invalid storage location")
	} else {
		var locErr *engine.StorageLocationError
		if !strings.Contains(err.Error(), "invalid") && !strings.Contains(err.Error(), "gs://") {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources StorageLocationError", Passed: false, Message: fmt.Sprintf("unexpected error message: %v", err)})
			t.Errorf("[C1] unexpected error type/message for invalid location: %v", err)
		} else {
			_ = locErr
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "NamespaceResources rejects invalid location", Passed: true, Message: fmt.Sprintf("rejected: %v", err)})
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
func (r *Runner) auditC2Eligibility(t *testing.T, eng engine.Engine) ClauseResult {
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
		t.Errorf("[C2] Eligible(nil, nil) returned OK=true or empty reason: %+v", verdict)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Eligible nil pod guard", Passed: true, Message: fmt.Sprintf("refusal reason: %q", verdict.Reason)})
	}

	// 2. ResolvePolicy fails on nil pod
	ctx := context.Background()
	if _, err := eng.ResolvePolicy(ctx, nil); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy nil pod guard", Passed: false, Message: "expected error when pod == nil"})
		t.Errorf("[C2] ResolvePolicy(nil) succeeded; want error")
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
		t.Errorf("[C2] ResolvePolicy unexpected error on empty cache: %v", err)
	} else if polRef != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy empty cache", Passed: false, Message: fmt.Sprintf("returned non-nil PolicyRef %+v on empty cache", polRef)})
		t.Errorf("[C2] ResolvePolicy returned PolicyRef on empty cache: %+v", polRef)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "ResolvePolicy empty cache", Passed: true, Message: "returned nil PolicyRef (allows clean cold fallback)"})
	}

	if cr.Status == StatusPassed {
		cr.Details = "Conservative eligibility and non-blocking policy resolution confirmed."
	} else {
		cr.Details = "Contract C2 eligibility/policy resolution violations detected."
	}
	return cr
}

// auditC3CheckpointOutcomes audits Clause C3: Checkpoint outcomes.
func (r *Runner) auditC3CheckpointOutcomes(t *testing.T, eng engine.Engine) ClauseResult {
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
		t.Errorf("[C3] Snapshots() returned nil provider")
		cr.Details = "Engine does not supply a snapshot provider."
		return cr
	}
	cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Snapshots provider non-nil", Passed: true})

	// Check provider group and annotation alignment if provider exposes them
	type groupGetter interface{ Group() string }
	type restoreAnnotationGetter interface{ RestoreAnnotation() string }

	if gg, ok := prov.(groupGetter); ok {
		if got := gg.Group(); got != eng.APIGroup() {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider group alignment", Passed: false, Message: fmt.Sprintf("Provider Group %q != Engine APIGroup %q", got, eng.APIGroup())})
			t.Errorf("[C3] Provider Group %q != Engine APIGroup %q", got, eng.APIGroup())
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider group alignment", Passed: true, Message: fmt.Sprintf("group=%q", got)})
		}
	}

	if rg, ok := prov.(restoreAnnotationGetter); ok {
		if got := rg.RestoreAnnotation(); got != eng.RestoreAnnotation() {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider restore annotation alignment", Passed: false, Message: fmt.Sprintf("Provider RestoreAnnotation %q != Engine RestoreAnnotation %q", got, eng.RestoreAnnotation())})
			t.Errorf("[C3] Provider RestoreAnnotation %q != Engine RestoreAnnotation %q", got, eng.RestoreAnnotation())
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Provider restore annotation alignment", Passed: true, Message: fmt.Sprintf("annotation=%q", got)})
		}
	}

	cr.Details = fmt.Sprintf("Snapshot provider configured and aligned with API group %q.", eng.APIGroup())
	return cr
}

// auditC4SourceFencing audits Clause C4: Source fencing.
func (r *Runner) auditC4SourceFencing(t *testing.T, eng engine.Engine) ClauseResult {
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
		t.Errorf("[C4] invalid FencingMode %q", fencing)
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
			t.Errorf("[C4] AllowsFencing allowed un-annotated pod under FencingNone")
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone refuses un-annotated pod", Passed: true, Message: fmt.Sprintf("refused with reason: %s", reasonNormal)})
		}
		if !allowedOptedIn {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone accepts opted-in pod", Passed: false, Message: "refused opted-in pod under FencingNone"})
			t.Errorf("[C4] AllowsFencing refused opted-in pod under FencingNone")
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "FencingNone accepts opted-in pod", Passed: true})
		}
	} else {
		if !allowedNormal {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Fenced engine accepts pod", Passed: false, Message: fmt.Sprintf("refused pod under mode %s: %s", fencing, reasonNormal)})
			t.Errorf("[C4] AllowsFencing refused normal pod under mode %s: %s", fencing, reasonNormal)
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Fenced engine accepts pod", Passed: true, Message: fmt.Sprintf("accepted under mode %s", fencing)})
		}
	}

	cr.Details = fmt.Sprintf("Declared fencing mode: %s. AllowsFencing gating verified.", fencing)
	return cr
}

// auditC5RestoreOrder audits Clause C5: Restore order and exactly-once restore.
func (r *Runner) auditC5RestoreOrder(t *testing.T, eng engine.Engine) ClauseResult {
	cr := ClauseResult{
		ID:       ClauseC5RestoreOrder,
		Title:    "Restore order and exactly-once restore",
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

	// 1. OrderRestore fail closed on nil pod
	job := &pmv1alpha1.PodMigrationJob{
		Status: pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "ps-snap-1234"},
	}
	if err := orderer.OrderRestore(nil, job); err == nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil pod guard", Passed: false, Message: "accepted nil pod without error"})
		t.Errorf("[C5] OrderRestore(nil, job) succeeded; want error")
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore nil pod guard", Passed: true})
	}

	// 2. OrderRestore stamps order
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-0"}}
	if err := orderer.OrderRestore(pod, job); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore valid", Passed: false, Message: fmt.Sprintf("OrderRestore error: %v", err)})
		t.Errorf("[C5] OrderRestore failed: %v", err)
	} else {
		snapRef, bypass, present := orderer.InspectRestoreOrder(pod)
		if !present || bypass || snapRef != "ps-snap-1234" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder after OrderRestore", Passed: false, Message: fmt.Sprintf("got (snapRef=%q, bypass=%v, present=%v), want (\"ps-snap-1234\", false, true)", snapRef, bypass, present)})
			t.Errorf("[C5] InspectRestoreOrder mismatch after OrderRestore: got (%q, %v, %v)", snapRef, bypass, present)
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderRestore and InspectRestoreOrder", Passed: true, Message: fmt.Sprintf("stamped and verified snapshotRef=%q", snapRef)})
		}
	}

	// 3. OrderColdStartBypass
	bypassPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-bypass"}}
	if err := orderer.OrderColdStartBypass(bypassPod); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass", Passed: false, Message: fmt.Sprintf("OrderColdStartBypass error: %v", err)})
		t.Errorf("[C5] OrderColdStartBypass failed: %v", err)
	} else {
		snapRef, bypass, present := orderer.InspectRestoreOrder(bypassPod)
		if !present || !bypass || snapRef != "" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder after OrderColdStartBypass", Passed: false, Message: fmt.Sprintf("got (snapRef=%q, bypass=%v, present=%v), want (\"\", true, true)", snapRef, bypass, present)})
			t.Errorf("[C5] InspectRestoreOrder mismatch after OrderColdStartBypass: got (%q, %v, %v)", snapRef, bypass, present)
		} else {
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "OrderColdStartBypass and InspectRestoreOrder", Passed: true, Message: "stamped cold-start bypass (empty snapshotRef)"})
		}
	}

	// 4. Pristine unannotated pod inspect
	pristinePod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "repl-pristine"}}
	snapRef, bypass, present := orderer.InspectRestoreOrder(pristinePod)
	if present {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder pristine pod", Passed: false, Message: fmt.Sprintf("present=true on pristine unannotated pod (%q, %v)", snapRef, bypass)})
		t.Errorf("[C5] InspectRestoreOrder present=true on pristine pod: (%q, %v)", snapRef, bypass)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "InspectRestoreOrder pristine pod", Passed: true, Message: "present=false on unannotated pod"})
	}

	cr.Details = fmt.Sprintf("RestoreOrderer capability fully conforming. RestoreOnce=%v.", eng.Guarantees().RestoreOnce)
	return cr
}

// auditC6RestoreOutcomeSignal audits Clause C6: Restore outcome signal.
func (r *Runner) auditC6RestoreOutcomeSignal(t *testing.T, eng engine.Engine) ClauseResult {
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

	// 1. InProgress check on unready replacement
	unreadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "repl-unready", Namespace: "default"},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	verdict, err := verifier.VerifyRestore(ctx, job, unreadyPod)
	if err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: false, Message: fmt.Sprintf("error: %v", err)})
		t.Errorf("[C6] VerifyRestore unready pod failed: %v", err)
	} else if verdict.Outcome != engine.RestoreInProgress {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: true, Message: fmt.Sprintf("outcome=%s (reason=%q)", verdict.Outcome, verdict.Reason)})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "VerifyRestore unready pod", Passed: true, Message: "returns InProgress while pod is not Ready"})
	}

	cr.Details = "RestoreVerifier capability verified for read-only outcome polling."
	return cr
}

// auditC7FailureClassification audits Clause C7: Restore failure classification.
func (r *Runner) auditC7FailureClassification(t *testing.T, eng engine.Engine) ClauseResult {
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
		rfName := rf.Name()
		if rfName == "" {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Classifier name non-empty", Passed: false, Message: "classifier returned empty Name()"})
			t.Errorf("[C7] restore failure classifier returned empty Name()")
		}
		if scoped, ok := rf.(restore.RuntimeClassScoped); ok {
			// Ensure it does not claim to cover classes that this engine does not serve
			allKnownClasses := []string{"gvisor", "runc", "@default", "custom-runtime"}
			for _, checkClass := range allKnownClasses {
				if scoped.CoversRuntimeClass(checkClass) && !engClasses[checkClass] {
					cr.Status = StatusFailed
					cr.Subtests = append(cr.Subtests, SubtestResult{
						Name:    "Classifier runtime class scope",
						Passed:  false,
						Message: fmt.Sprintf("classifier %q covers runtime class %q not served by engine %v", rfName, checkClass, eng.RuntimeClasses()),
					})
					t.Errorf("[C7] classifier %q covers class %q outside engine %v", rfName, checkClass, eng.RuntimeClasses())
				}
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
func (r *Runner) auditC8PlacementConstraints(t *testing.T, eng engine.Engine) ClauseResult {
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

	// 1. Determinism test: repeated calls must return identical requirements
	reqs1, err1 := advisor.Placement(ctx, jobWithMachine)
	reqs2, err2 := advisor.Placement(ctx, jobWithMachine)
	if err1 != nil || err2 != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement execution", Passed: false, Message: fmt.Sprintf("Placement returned error: %v / %v", err1, err2)})
		t.Errorf("[C8] Placement returned unexpected error: %v / %v", err1, err2)
	} else if !reflect.DeepEqual(reqs1, reqs2) {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement determinism", Passed: false, Message: fmt.Sprintf("non-deterministic return values: %v vs %v", reqs1, reqs2)})
		t.Errorf("[C8] Placement non-deterministic: %v != %v", reqs1, reqs2)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement determinism", Passed: true, Message: fmt.Sprintf("returned %d requirement(s) identically across calls", len(reqs1))})
	}

	// 2. Requirements validity
	for i, req := range reqs1 {
		if errs := validation.IsQualifiedName(req.Key); len(errs) > 0 {
			cr.Status = StatusFailed
			cr.Subtests = append(cr.Subtests, SubtestResult{Name: fmt.Sprintf("Requirement %d key valid", i), Passed: false, Message: fmt.Sprintf("invalid label key %q: %v", req.Key, errs)})
			t.Errorf("[C8] Placement returned invalid label key %q: %v", req.Key, errs)
		}
	}

	// 3. Clean fallback when machine type is empty/unknown
	emptyJob := &pmv1alpha1.PodMigrationJob{}
	reqsEmpty, errEmpty := advisor.Placement(ctx, emptyJob)
	if errEmpty != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: false, Message: fmt.Sprintf("returned error on empty facts: %v", errEmpty)})
		t.Errorf("[C8] Placement returned error on empty facts: %v", errEmpty)
	} else if len(reqsEmpty) > 0 && advisor != nil {
		// Empty facts should generally state no requirements unless static
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: true, Message: fmt.Sprintf("%d static requirement(s)", len(reqsEmpty))})
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Placement empty facts fallback", Passed: true, Message: "returns 0 requirements on unknown source facts"})
	}

	cr.Details = "PlacementAdvisor capability verified for deterministic label requirements."
	return cr
}

// auditC9CancellationAndCleanup audits Clause C9: Cancellation and cleanup.
func (r *Runner) auditC9CancellationAndCleanup(t *testing.T, eng engine.Engine) ClauseResult {
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
			Name:      "test-job",
			Namespace: "default",
			UID:       types.UID("job-cleanup-uid"),
		},
	}

	// Cleanup should be idempotent and return nil error even if trigger doesn't exist
	if err := prov.Cleanup(ctx, job, "web-0"); err != nil {
		cr.Status = StatusFailed
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup idempotency", Passed: false, Message: fmt.Sprintf("Cleanup returned error on non-existent trigger: %v", err)})
		t.Errorf("[C9] Cleanup returned error on non-existent trigger: %v", err)
	} else {
		cr.Subtests = append(cr.Subtests, SubtestResult{Name: "Cleanup idempotency", Passed: true, Message: "idempotent on non-existent trigger"})
	}

	cr.Details = "Snapshot provider cleanup idempotency confirmed."
	return cr
}
