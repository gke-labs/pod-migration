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
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

func TestEngineConformance_GKEPodSnapshots(t *testing.T) {
	scheme := TestScheme()
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
	scheme := TestScheme()
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
	eligibleOK        bool
	nonDeterministic  bool
	callCount         int
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
		return engine.Verdict{OK: f.eligibleOK, Reason: "nil pod"}
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

// Placement implements PlacementAdvisor with intentional non-determinism when requested.
func (f *faultyEngine) Placement(ctx context.Context, job *pmv1alpha1.PodMigrationJob) ([]corev1.NodeSelectorRequirement, error) {
	f.callCount++
	if f.nonDeterministic && f.callCount%2 == 0 {
		return []corev1.NodeSelectorRequirement{
			{Key: "node.kubernetes.io/instance-type", Operator: corev1.NodeSelectorOpIn, Values: []string{"n2-standard-4"}},
		}, nil
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

func TestConformance_SabotageDetections(t *testing.T) {
	scheme := TestScheme()
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	validProvider, _ := snapshot.NewGKEProviderForGroup(cl, scheme, engine.DefaultCRIUAPIGroup)

	runner := NewRunner()

	t.Run("Sabotage_C1_InvalidAPIGroup", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-1",
			apiGroup:          "invalid_group_no_dot",
			restoreAnnotation: "invalid_group_no_dot/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		cr := runner.auditC1Identity(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC1Identity to fail on invalid API group, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C1_EmptyName", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		cr := runner.auditC1Identity(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC1Identity to fail on empty name, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C2_EligibleNilPodLeak", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-2",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
			eligibleOK:        true, // Faulty: approves nil pod!
		}
		cr := runner.auditC2Eligibility(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC2Eligibility to fail when nil pod returns OK=true, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C3_NilProvider", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-3",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          nil, // Faulty: nil provider!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		cr := runner.auditC3CheckpointOutcomes(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC3CheckpointOutcomes to fail on nil provider, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C4_InvalidFencingMode", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-4",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			guarantees:        engine.Guarantees{Fencing: "UnrecognizedMode"}, // Faulty!
		}
		cr := runner.auditC4SourceFencing(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC4SourceFencing to fail on invalid FencingMode, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C7_CrossRuntimeClassifierLeak", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-7",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"}, // only runc!
			provider:          validProvider,
			failures:          []restore.Engine{leakingClassifier{}}, // leaks gvisor!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		cr := runner.auditC7FailureClassification(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC7FailureClassification to fail on leaking classifier, got status %s", cr.Status)
		}
	})

	t.Run("Sabotage_C8_NonDeterministicPlacement", func(t *testing.T) {
		fakeSub := &testing.T{}
		faulty := &faultyEngine{
			name:              "faulty-8",
			apiGroup:          engine.DefaultCRIUAPIGroup,
			restoreAnnotation: engine.DefaultCRIUAPIGroup + "/ps-name",
			runtimeClasses:    []string{"runc"},
			provider:          validProvider,
			nonDeterministic:  true, // Faulty: oscillates requirements!
			guarantees:        engine.Guarantees{Fencing: engine.FencingStopped},
		}
		cr := runner.auditC8PlacementConstraints(fakeSub, faulty)
		if cr.Status != StatusFailed {
			t.Errorf("Expected auditC8PlacementConstraints to fail on non-deterministic requirements, got status %s", cr.Status)
		}
	})
}
