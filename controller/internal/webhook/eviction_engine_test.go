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

package webhook

import (
	"context"
	"errors"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// evictionTestEngine is a snapshot engine whose eligibility verdict and
// policy are whatever the test configured.
type evictionTestEngine struct {
	classes   []string
	verdict   engine.Verdict
	policy    *engine.PolicyRef
	policyErr error
}

func (e *evictionTestEngine) Name() string             { return "eviction-test-engine" }
func (e *evictionTestEngine) RuntimeClasses() []string { return e.classes }
func (e *evictionTestEngine) NamespaceResources(*pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	return nil, nil
}
func (e *evictionTestEngine) Eligible(*corev1.Pod, *corev1.Node) engine.Verdict { return e.verdict }
func (e *evictionTestEngine) ResolvePolicy(context.Context, *corev1.Pod) (*engine.PolicyRef, error) {
	return e.policy, e.policyErr
}
func (e *evictionTestEngine) Snapshots() snapshot.Provider      { return nil }
func (e *evictionTestEngine) RestoreFailures() []restore.Engine { return nil }
func (e *evictionTestEngine) Guarantees() engine.Guarantees     { return engine.Guarantees{} }

func TestEvictionGate_UsesThePodEngine(t *testing.T) {
	gvisor, runc := "gvisor", "runc"
	tests := []struct {
		name              string
		registry          func(t *testing.T) *engine.Registry
		runtimeClass      string
		wantAllowed       bool
		wantCode          int32
		wantMessage       string
		wantPolicy        string // "" means no job
		wantNoPolicyEvent bool
	}{
		{
			name:         "the engine refuses the pod",
			registry:     registryWithEvictionTestEngine(&evictionTestEngine{verdict: engine.Verdict{Reason: "the engine can't migrate this pod"}}),
			runtimeClass: gvisor,
			wantAllowed:  true,
			wantMessage:  "the engine can't migrate this pod",
		},
		{
			name: "the engine's policy is recorded on the job",
			registry: registryWithEvictionTestEngine(&evictionTestEngine{
				verdict: engine.Verdict{OK: true},
				policy:  &engine.PolicyRef{Group: "engine.example.com", Kind: "Policy", Name: "engine-policy"},
			}),
			runtimeClass: gvisor,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "migration job spawned",
			wantPolicy:   "engine-policy",
		},
		{
			name: "the engine's eligibility replaces the default runtime-class allow-list",
			registry: registryWithEvictionTestEngine(&evictionTestEngine{
				verdict: engine.Verdict{OK: true},
				policy:  &engine.PolicyRef{Name: "engine-policy"},
			}),
			runtimeClass: runc,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "migration job spawned",
			wantPolicy:   "engine-policy",
		},
		{
			name:              "the engine has no policy for the pod",
			registry:          registryWithEvictionTestEngine(&evictionTestEngine{verdict: engine.Verdict{OK: true}}),
			runtimeClass:      gvisor,
			wantAllowed:       true,
			wantMessage:       "skipping migration: no valid manual+stop policy found",
			wantNoPolicyEvent: true,
		},
		{
			name: "the engine's policy lookup fails",
			registry: registryWithEvictionTestEngine(&evictionTestEngine{
				verdict:   engine.Verdict{OK: true},
				policyErr: errors.New("cache not synced"),
			}),
			runtimeClass: gvisor,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "transient error resolving snapshot policy, retrying",
		},
		{
			name: "no engine serves the pod",
			registry: func(t *testing.T) *engine.Registry {
				// The default engine isn't registered, and no engine declares runc.
				r := engine.NewRegistry("missing")
				if err := r.Register(&evictionTestEngine{classes: []string{"gvisor"}}); err != nil {
					t.Fatalf("Register: %v", err)
				}
				return r
			},
			runtimeClass: runc,
			wantAllowed:  true,
			wantMessage:  "No snapshot engine serves runtime class runc, skipping migration",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			class := tt.runtimeClass
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					UID:       "test-uid-12345",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{RuntimeClassName: &class},
			}
			// No PodSnapshotPolicy: only the engine can supply a policy.
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
			recorder := record.NewFakeRecorder(10)
			gate := &EvictionGate{Client: c, APIReader: c, Recorder: recorder, Engines: tt.registry(t)}

			req := admission.Request{}
			req.Namespace, req.Name, req.SubResource = "default", "test-pod", "eviction"
			resp := gate.Handle(context.Background(), req)

			if resp.Allowed != tt.wantAllowed {
				t.Errorf("Allowed = %t, want %t", resp.Allowed, tt.wantAllowed)
			}
			gotMessage, gotCode := "", int32(0)
			if resp.Result != nil {
				gotMessage, gotCode = resp.Result.Message, resp.Result.Code
			}
			if gotMessage != tt.wantMessage {
				t.Errorf("message = %q, want %q", gotMessage, tt.wantMessage)
			}
			if tt.wantCode != 0 && gotCode != tt.wantCode {
				t.Errorf("code = %d, want %d", gotCode, tt.wantCode)
			}

			jobs := &pmv1alpha1.PodMigrationJobList{}
			if err := c.List(context.Background(), jobs); err != nil {
				t.Fatalf("listing jobs: %v", err)
			}
			switch {
			case tt.wantPolicy == "" && len(jobs.Items) != 0:
				t.Errorf("created %d job(s), want none", len(jobs.Items))
			case tt.wantPolicy != "" && len(jobs.Items) != 1:
				t.Errorf("created %d job(s), want 1", len(jobs.Items))
			case tt.wantPolicy != "":
				if got := jobs.Items[0].Annotations[util.AnnotationPodSnapshotPolicy]; got != tt.wantPolicy {
					t.Errorf("job policy annotation = %q, want %q", got, tt.wantPolicy)
				}
			}

			gotNoPolicyEvent := false
			for len(recorder.Events) > 0 {
				if ev := <-recorder.Events; ev == "Warning MigrationSkippedNoPolicy Pod is opted into live migration, but no matching Ready manual+stop PodSnapshotPolicy was found; allowing cold eviction" {
					gotNoPolicyEvent = true
				}
			}
			if gotNoPolicyEvent != tt.wantNoPolicyEvent {
				t.Errorf("MigrationSkippedNoPolicy event = %t, want %t", gotNoPolicyEvent, tt.wantNoPolicyEvent)
			}
		})
	}
}

func registryWithEvictionTestEngine(e *evictionTestEngine) func(t *testing.T) *engine.Registry {
	return func(t *testing.T) *engine.Registry {
		t.Helper()
		r := engine.NewRegistry(e.Name())
		if err := r.Register(e); err != nil {
			t.Fatalf("Register: %v", err)
		}
		return r
	}
}
