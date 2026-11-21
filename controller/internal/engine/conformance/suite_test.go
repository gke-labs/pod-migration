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

package conformance

import (
	"context"
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// testScheme returns a runtime.Scheme populated with standard types and PMJ types.
func testScheme() *runtime.Scheme {
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

func TestEngineConformance_GKEPodSnapshots(t *testing.T) {
	scheme := testScheme()
	cl := NewTestClient(scheme)
	gkeEngine := engine.NewGKEPodSnapshots(cl, scheme)

	runner := NewRunner()
	report := runner.Run(t, gkeEngine)

	t.Logf("\n%s", report.Markdown())

	if !report.Passed {
		t.Fatalf("GKEPodSnapshots failed conformance:\n%s", report.Markdown())
	}

	summary := report.Summary()
	if !strings.Contains(summary, "PASSED") {
		t.Errorf("Summary %q does not contain PASSED", summary)
	}

	md := report.Markdown()
	if !strings.Contains(md, "Snapshot Engine Conformance Report: `gke-pod-snapshots`") {
		t.Errorf("Markdown header missing: %s", md)
	}
	if !strings.Contains(md, "[PASS]") {
		t.Errorf("Markdown missing [PASS]: %s", md)
	}
}

func TestEngineConformance_CRIU(t *testing.T) {
	scheme := testScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	criuEngine, err := engine.NewCRIU(cl, scheme, engine.DefaultCRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}

	runner := NewRunner()
	report := runner.Run(t, criuEngine)

	t.Logf("\n%s", report.Markdown())

	if !report.Passed {
		t.Fatalf("CRIU failed conformance:\n%s", report.Markdown())
	}

	summary := report.Summary()
	if !strings.Contains(summary, "PASSED") {
		t.Errorf("Summary %q does not contain PASSED", summary)
	}

	md := report.Markdown()
	if !strings.Contains(md, "Snapshot Engine Conformance Report: `criu`") {
		t.Errorf("Markdown header missing: %s", md)
	}
	if !strings.Contains(md, "[PASS]") {
		t.Errorf("Markdown missing [PASS]: %s", md)
	}
}

// faultyEngine is a mock engine configurable to sabotage specific contract clauses.
type faultyEngine struct {
	name              string
	apiGroup          string
	restoreAnnotation string
	runtimeClasses    []string
	guarantees        engine.Guarantees
	provider          snapshot.Provider
	failures          []restore.Engine
	namespaceErr      error
	untypedLocErr     bool
	eligibleOK        bool
	admitInvalid      bool

	// RestoreOrderer behavior
	orderWrongKey bool
	bypassNoop    bool

	// RestoreVerifier behavior
	verifyVerdict engine.RestoreVerdict
	verifyErr     error

	// PlacementAdvisor behavior
	placementReqs    []corev1.NodeSelectorRequirement
	nonDeterministic bool
	callCount        int
}

func (f *faultyEngine) Name() string              { return f.name }
func (f *faultyEngine) APIGroup() string          { return f.apiGroup }
func (f *faultyEngine) RestoreAnnotation() string { return f.restoreAnnotation }
func (f *faultyEngine) RuntimeClasses() []string  { return f.runtimeClasses }
func (f *faultyEngine) NamespaceResources(pm *pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	if pm == nil {
		return nil, errors.New("nil pm")
	}
	if f.namespaceErr != nil {
		return nil, f.namespaceErr
	}
	if !strings.HasPrefix(pm.Spec.Storage.Location, "gs://") {
		if f.untypedLocErr {
			return nil, errors.New("invalid storage location without typed error")
		}
		return nil, &engine.StorageLocationError{
			Location: pm.Spec.Storage.Location,
			Message:  "unsupported storage location: must begin with gs://",
		}
	}
	return []*unstructured.Unstructured{
		{
			Object: map[string]interface{}{
				"apiVersion": f.apiGroup + "/v1",
				"kind":       "PodSnapshotPolicy",
				"metadata": map[string]interface{}{
					"name": "psp-fake",
					"labels": map[string]interface{}{
						engine.OwnerUIDLabel: string(pm.UID),
					},
				},
			},
		},
	}, nil
}
func (f *faultyEngine) Eligible(pod *corev1.Pod, node *corev1.Node) engine.Verdict {
	if pod == nil {
		if f.eligibleOK {
			return engine.Verdict{OK: true}
		}
		return engine.Verdict{OK: false, Reason: "nil pod"}
	}
	if f.name == "criu" && !f.admitInvalid {
		if pod.Spec.HostNetwork {
			return engine.Verdict{OK: false, Reason: "CRIU engine does not support hostNetwork pods"}
		}
		if len(pod.Spec.Containers) > 1 {
			return engine.Verdict{OK: false, Reason: "CRIU engine requires a single-container pod"}
		}
		if node == nil {
			return engine.Verdict{OK: false, Reason: "node is nil"}
		}
	}
	if f.admitInvalid {
		return engine.Verdict{OK: true}
	}
	return engine.Verdict{OK: true}
}
func (f *faultyEngine) ResolvePolicy(ctx context.Context, pod *corev1.Pod) (*engine.PolicyRef, error) {
	if pod == nil {
		return nil, errors.New("nil pod")
	}
	return nil, nil
}
func (f *faultyEngine) Snapshots() snapshot.Provider      { return f.provider }
func (f *faultyEngine) RestoreFailures() []restore.Engine { return f.failures }
func (f *faultyEngine) Guarantees() engine.Guarantees     { return f.guarantees }
func (f *faultyEngine) Client() client.Client {
	if ch, ok := f.provider.(interface{ Client() client.Client }); ok {
		return ch.Client()
	}
	return nil
}

// RestoreOrderer implementation
func (f *faultyEngine) OrderRestore(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) error {
	if pod == nil || job == nil || job.Status.SnapshotRef == "" {
		return errors.New("invalid arguments")
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	key := f.restoreAnnotation
	if f.orderWrongKey {
		key = "wrong-foreign-group.io/ps-name"
	}
	pod.Annotations[key] = job.Status.SnapshotRef
	return nil
}
func (f *faultyEngine) OrderColdStartBypass(pod *corev1.Pod) error {
	if pod == nil {
		return errors.New("nil pod")
	}
	if f.bypassNoop {
		return nil // Leaves existing restore order in place!
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[f.restoreAnnotation] = ""
	return nil
}
func (f *faultyEngine) InspectRestoreOrder(pod *corev1.Pod) (snapshotRef string, bypass bool, present bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false, false
	}
	key := f.restoreAnnotation
	if f.orderWrongKey {
		key = "wrong-foreign-group.io/ps-name"
	}
	val, ok := pod.Annotations[key]
	if !ok {
		return "", false, false
	}
	if val == "" {
		return "", true, true
	}
	return val, false, true
}

// RestoreVerifier implementation
func (f *faultyEngine) VerifyRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (engine.RestoreVerdict, error) {
	if pod == nil {
		return engine.RestoreVerdict{}, errors.New("nil pod")
	}
	if f.verifyErr != nil {
		return engine.RestoreVerdict{}, f.verifyErr
	}
	if f.verifyVerdict != (engine.RestoreVerdict{}) {
		return f.verifyVerdict, nil
	}
	if pod.Annotations != nil && strings.TrimSpace(pod.Annotations["pod-migrate.io/psengine-restore"]) == "failed" {
		return engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "restore failed signal"}, nil
	}
	if pod.Status.Phase != corev1.PodRunning {
		return engine.RestoreVerdict{Outcome: engine.RestoreInProgress, Confirmed: false}, nil
	}
	return engine.RestoreVerdict{Outcome: engine.Restored, Confirmed: false}, nil
}

// PlacementAdvisor implementation
func (f *faultyEngine) Placement(ctx context.Context, job *pmv1alpha1.PodMigrationJob) ([]corev1.NodeSelectorRequirement, error) {
	f.callCount++
	if f.nonDeterministic && f.callCount%2 == 0 {
		return []corev1.NodeSelectorRequirement{
			{Key: "node.kubernetes.io/instance-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"n2-standard-4"}},
		}, nil
	}
	if len(f.placementReqs) > 0 {
		return f.placementReqs, nil
	}
	return []corev1.NodeSelectorRequirement{
		{Key: "node.kubernetes.io/instance-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"c2-standard-8"}},
	}, nil
}

// leakingClassifier implements restore.Engine and restore.RuntimeClassScoped, but
// deliberately leaks coverage to a runtime class not served by the engine.
type leakingClassifier struct{}

func (leakingClassifier) Name() string { return "leaker" }
func (leakingClassifier) MatchesRestoreFailure(msg string) (string, bool) {
	return "", false
}
func (leakingClassifier) CoversRuntimeClass(rc string) bool {
	return rc == "gvisor" // leaks gvisor coverage when engine only serves runc!
}

// unscopedClassifier implements restore.Engine but NOT restore.RuntimeClassScoped (#132).
type unscopedClassifier struct{}

func (unscopedClassifier) Name() string { return "unscoped" }
func (unscopedClassifier) MatchesRestoreFailure(msg string) (string, bool) {
	return "", false
}

// foreignClassClassifier implements restore.Engine and restore.RuntimeClassScoped, but
// only covers a foreign runtime class ("gvisor-v2") that the engine does not serve.
type foreignClassClassifier struct{}

func (foreignClassClassifier) Name() string { return "foreign" }
func (foreignClassClassifier) MatchesRestoreFailure(msg string) (string, bool) {
	return "", false
}
func (foreignClassClassifier) CoversRuntimeClass(rc string) bool {
	return rc == "gvisor-v2"
}

// neverTerminalProvider wraps a valid provider but converts any terminal Failed/Refused
// phase into PhaseInProgress.
type neverTerminalProvider struct {
	snapshot.Provider
}

func (p *neverTerminalProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	st, err := p.Provider.CheckStatus(ctx, job, podName)
	if err != nil || st == nil {
		return st, err
	}
	if st.Phase == snapshot.PhaseFailed || st.Phase == snapshot.PhaseRefused {
		cp := *st
		cp.Phase = snapshot.PhaseInProgress
		return &cp, nil
	}
	return st, nil
}
func (p *neverTerminalProvider) Unwrap() snapshot.Provider { return p.Provider }
func (p *neverTerminalProvider) Client() client.Client {
	if ch, ok := p.Provider.(interface{ Client() client.Client }); ok {
		return ch.Client()
	}
	return nil
}

// prematureFailedProvider wraps a valid provider but fails on transient Ready=False.
type prematureFailedProvider struct {
	snapshot.Provider
}

func (p *prematureFailedProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	st, err := p.Provider.CheckStatus(ctx, job, podName)
	if err != nil || st == nil {
		return st, err
	}
	if podName == "c3-transient" {
		cp := *st
		cp.Phase = snapshot.PhaseFailed
		return &cp, nil
	}
	return st, nil
}
func (p *prematureFailedProvider) Unwrap() snapshot.Provider { return p.Provider }
func (p *prematureFailedProvider) Client() client.Client {
	if ch, ok := p.Provider.(interface{ Client() client.Client }); ok {
		return ch.Client()
	}
	return nil
}

// noopCleanupProvider wraps a valid provider but makes Cleanup a no-op.
type noopCleanupProvider struct {
	snapshot.Provider
}

func (p *noopCleanupProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	return nil // No-op: never deletes the trigger!
}
func (p *noopCleanupProvider) Unwrap() snapshot.Provider { return p.Provider }
func (p *noopCleanupProvider) Client() client.Client {
	if ch, ok := p.Provider.(interface{ Client() client.Client }); ok {
		return ch.Client()
	}
	return nil
}

func findClause(rep *Report, id ClauseID) *ClauseResult {
	for i := range rep.Clauses {
		if rep.Clauses[i].ID == id {
			return &rep.Clauses[i]
		}
	}
	return nil
}

func TestConformance_SabotageDetections(t *testing.T) {
	scheme := testScheme()
	cl := NewTestClient(scheme)
	validProvider, _ := snapshot.NewGKEProviderForGroup(cl, scheme, engine.DefaultCRIUAPIGroup)

	runner := NewRunner()

	t.Run("BaselineConformingEngine", func(t *testing.T) {
		cl := NewTestClient(scheme)
		prov, err := snapshot.NewGKEProviderForGroup(cl, scheme, engine.DefaultCRIUAPIGroup)
		if err != nil {
			t.Fatalf("failed to create provider: %v", err)
		}
		conforming := &faultyEngine{
			name:              "conforming-mock",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.RestoreAnnotation(engine.DefaultCRIUAPIGroup),
			runtimeClasses:    []string{"runc"},
			provider:          prov,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped, RestoreOnce: true},
		}
		rep := runner.Evaluate(conforming)
		if !rep.Passed {
			t.Fatalf("Expected baseline conforming engine to pass all clauses, got failed report:\n%s", rep.Markdown())
		}
		for _, cr := range rep.Clauses {
			if cr.Status == StatusFailed {
				t.Errorf("Expected clause %s not to fail, got details: %s", cr.ID, cr.Details)
			}
		}
	})

	t.Run("Sabotage_C1_InvalidAPIGroup", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-1",
			apiGroup:          "invalid_group_no_dot",
			restoreAnnotation: "invalid_group_no_dot/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on invalid API group")
		}
		cr := findClause(rep, ClauseC1Identity)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C1 FAILED, got %+v", cr)
		}
	})

	t.Run("Sabotage_C1_EmptyName", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on empty name")
		}
		cr := findClause(rep, ClauseC1Identity)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C1 FAILED, got %+v", cr)
		}
	})

	t.Run("Sabotage_C1_UntypedStorageLocationError", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-storage-err",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			untypedLocErr:     true,
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on untyped storage location error")
		}
		cr := findClause(rep, ClauseC1Identity)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C1 FAILED for untyped storage error, got %+v", cr)
		}
	})

	t.Run("Sabotage_C2_EligibleNilPodLeak", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-2",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			eligibleOK:        true, // Faulty: approves nil pod!
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on eligible nil pod leak")
		}
		cr := findClause(rep, ClauseC2Eligibility)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C2 FAILED, got %+v", cr)
		}
	})

	t.Run("Sabotage_C3_NilProvider", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-3",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          nil, // Faulty: nil provider!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on nil provider")
		}
		cr := findClause(rep, ClauseC3CheckpointOutcomes)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C3 FAILED, got %+v", cr)
		}
	})

	t.Run("Sabotage_C4_InvalidFencingMode", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-4",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: "UnrecognizedMode"}, // Faulty!
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on invalid FencingMode")
		}
		cr := findClause(rep, ClauseC4SourceFencing)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C4 FAILED, got %+v", cr)
		}
	})

	t.Run("Sabotage_C5_CrossGroupKeyMismatch", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-5-key",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			orderWrongKey:     true, // Stamping wrong annotation key (#135)!
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on cross-group key mismatch")
		}
		cr := findClause(rep, ClauseC5RestoreOrder)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C5 FAILED on wrong annotation key, got %+v", cr)
		}
	})

	t.Run("Sabotage_C5_BypassLeavesOrderInPlace", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-5-bypass",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			bypassNoop:        true, // Faulty: OrderColdStartBypass does not clear existing order!
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false when bypass leaves order in place")
		}
		cr := findClause(rep, ClauseC5RestoreOrder)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C5 FAILED when bypass leaves order in place, got %+v", cr)
		}
	})

	t.Run("Sabotage_C6_UnreadyPodConfirmedTrue", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-6",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			verifyVerdict: engine.RestoreVerdict{
				Outcome:   engine.Restored,
				Confirmed: true, // Faulty: returns Confirmed=true for pending unready pod!
			},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false when unready pod returns Confirmed=true")
		}
		cr := findClause(rep, ClauseC6RestoreOutcomeSignal)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C6 FAILED on unready pod with Confirmed=true, got %+v", cr)
		}
	})

	t.Run("Sabotage_C7_UnscopedClassifier", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-7-unscoped",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			failures:          []restore.Engine{unscopedClassifier{}}, // Not RuntimeClassScoped!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on unscoped classifier")
		}
		cr := findClause(rep, ClauseC7FailureClassify)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C7 FAILED on unscoped classifier, got %+v", cr)
		}
	})

	t.Run("Sabotage_C7_CrossRuntimeClassifierLeak", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-7-leak",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"}, // only runc!
			provider:          validProvider,
			failures:          []restore.Engine{leakingClassifier{}}, // leaks gvisor!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on leaking classifier")
		}
		cr := findClause(rep, ClauseC7FailureClassify)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C7 FAILED on leaking classifier, got %+v", cr)
		}
	})

	t.Run("Sabotage_C8_NonDeterministicPlacement", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-8-det",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			nonDeterministic:  true, // Faulty: oscillates requirements!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on non-deterministic requirements")
		}
		cr := findClause(rep, ClauseC8Placement)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C8 FAILED on non-deterministic placement, got %+v", cr)
		}
	})

	t.Run("Sabotage_C8_OperatorWithNoValues", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-8-values",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			placementReqs: []corev1.NodeSelectorRequirement{
				{Key: "node.kubernetes.io/instance-type", Operator: corev1.NodeSelectorOpIn, Values: []string{}}, // Empty values for In!
			},
			guarantees: engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on requirement with no values")
		}
		cr := findClause(rep, ClauseC8Placement)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C8 FAILED on In with no values, got %+v", cr)
		}
	})

	t.Run("Sabotage_C9_NoopCleanup", func(t *testing.T) {
		testCl := NewTestClient(scheme)
		prov, _ := snapshot.NewGKEProviderForGroup(testCl, scheme, engine.DefaultCRIUAPIGroup)
		noopProv := &noopCleanupProvider{Provider: prov}

		faulty := &faultyEngine{
			name:              "faulty-9-cleanup",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          noopProv, // Cleanup does not delete trigger!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on no-op Cleanup")
		}
		cr := findClause(rep, ClauseC9CancellationCleanup)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C9 FAILED on no-op Cleanup, got %+v", cr)
		}
	})

	t.Run("Sabotage_C3_NeverTerminal", func(t *testing.T) {
		testCl := NewTestClient(scheme)
		prov, _ := snapshot.NewGKEProviderForGroup(testCl, scheme, engine.DefaultCRIUAPIGroup)
		faultyProv := &neverTerminalProvider{Provider: prov}

		faulty := &faultyEngine{
			name:              "faulty-3-never-terminal",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          faultyProv,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on never-terminal CheckStatus")
		}
		cr := findClause(rep, ClauseC3CheckpointOutcomes)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C3 FAILED on never-terminal CheckStatus, got %+v", cr)
		}
	})

	t.Run("Sabotage_C3_TransientReadyFailed", func(t *testing.T) {
		testCl := NewTestClient(scheme)
		prov, _ := snapshot.NewGKEProviderForGroup(testCl, scheme, engine.DefaultCRIUAPIGroup)
		faultyProv := &prematureFailedProvider{Provider: prov}

		faulty := &faultyEngine{
			name:              "faulty-3-transient-fail",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          faultyProv,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on premature failed CheckStatus")
		}
		cr := findClause(rep, ClauseC3CheckpointOutcomes)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C3 FAILED on premature failed CheckStatus, got %+v", cr)
		}
	})

	t.Run("Sabotage_C6_IgnoresFailureSignal", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-6-ignores-signal",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			verifyVerdict:     engine.RestoreVerdict{Outcome: engine.Restored, Confirmed: false}, // Always returns Restored, ignoring failure signal!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false when verifier ignores failure signals")
		}
		cr := findClause(rep, ClauseC6RestoreOutcomeSignal)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C6 FAILED when ignoring failure signal, got %+v", cr)
		}
	})

	t.Run("Sabotage_C2_CRIUAdmitsHostNetwork", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "criu",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			admitInvalid:      true, // Admits hostNetwork!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false when CRIU admits hostNetwork")
		}
		cr := findClause(rep, ClauseC2Eligibility)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C2 FAILED on CRIU admitting hostNetwork, got %+v", cr)
		}
	})

	t.Run("Sabotage_C7_ForeignClassOnly", func(t *testing.T) {
		faulty := &faultyEngine{
			name:              "faulty-7-foreign",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},                           // only runc!
			failures:          []restore.Engine{foreignClassClassifier{}}, // covers gvisor-v2!
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		rep := runner.Evaluate(faulty)
		if rep.Passed {
			t.Errorf("Expected Report.Passed = false on classifier covering only foreign runtime class")
		}
		cr := findClause(rep, ClauseC7FailureClassify)
		if cr == nil || cr.Status != StatusFailed {
			t.Errorf("Expected Clause C7 FAILED on foreign runtime class classifier, got %+v", cr)
		}
	})
}
