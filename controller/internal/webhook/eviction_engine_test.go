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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// evictionTestEngine is a snapshot engine whose eligibility verdict and
// policy are whatever the test configured.
type evictionTestEngine struct {
	name       string // "" is eviction-test-engine
	classes    []string
	verdict    engine.Verdict
	policy     *engine.PolicyRef
	policyErr  error
	guarantees *engine.Guarantees
}

func (e *evictionTestEngine) Name() string {
	if e.name == "" {
		return "eviction-test-engine"
	}
	return e.name
}
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
func (e *evictionTestEngine) Guarantees() engine.Guarantees {
	if e.guarantees != nil {
		return *e.guarantees
	}
	return engine.Guarantees{Fencing: engine.FencingFrozen, RestoreOnce: true}
}

func TestEvictionGate_UsesThePodEngine(t *testing.T) {
	const gvisor, runc = "gvisor", "runc"
	migrating := func() *evictionTestEngine {
		return &evictionTestEngine{
			verdict: engine.Verdict{OK: true},
			policy:  &engine.PolicyRef{Group: "engine.example.com", Kind: "Policy", Name: "engine-policy"},
		}
	}
	other := &evictionTestEngine{
		name:    "other-engine",
		classes: []string{"gvisor"},
		verdict: engine.Verdict{OK: true},
		policy:  &engine.PolicyRef{Name: "other-policy"},
	}
	tests := []struct {
		name     string
		registry func(t *testing.T) *engine.Registry
		// allowList is the operator allow-list; "" leaves it nil: gVisor only.
		allowList string
		// podMigrationRoutes, when set, adds a PodMigration with these
		// routes to the namespace.
		podMigrationRoutes string
		// jobPhase, when set, adds the pod's job in that phase.
		jobPhase          pmv1alpha1.PodMigrationJobPhase
		runtimeClass      string
		wantAllowed       bool
		wantCode          int32
		wantMessage       string
		wantPolicy        string // "" means no new job
		wantEngine        string
		wantNoPolicyEvent bool
	}{
		{
			name:         "the engine refuses the pod",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine", &evictionTestEngine{verdict: engine.Verdict{Reason: "the engine can't migrate this pod"}}),
			runtimeClass: gvisor,
			wantAllowed:  true,
			wantMessage:  "the engine can't migrate this pod",
		},
		{
			name:         "the engine's policy and name are recorded on the job",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine", migrating()),
			runtimeClass: gvisor,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "migration job spawned",
			wantPolicy:   "engine-policy",
			wantEngine:   "eviction-test-engine",
		},
		{
			name:         "the allow-list applies before the engine",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine,runc=eviction-test-engine", migrating()),
			runtimeClass: runc,
			wantAllowed:  true,
			wantMessage:  "Pod runtime class runc is not migratable (allowed: gvisor), skipping migration",
		},
		{
			name:         "an allow-listed class routed to the engine",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine,runc=eviction-test-engine", migrating()),
			allowList:    "gvisor,runc",
			runtimeClass: runc,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "migration job spawned",
			wantPolicy:   "engine-policy",
			wantEngine:   "eviction-test-engine",
		},
		{
			name:         "no engine serves the pod's runtime class",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine", migrating()),
			allowList:    "gvisor,runc",
			runtimeClass: runc,
			wantAllowed:  true,
			wantMessage:  "No snapshot engine serves runtime class runc, skipping migration",
		},
		{
			name:               "the namespace's PodMigration routes the class to another engine",
			registry:           evictionTestRegistry("gvisor=eviction-test-engine", migrating(), other),
			podMigrationRoutes: "gvisor=other-engine",
			runtimeClass:       gvisor,
			wantCode:           http.StatusTooManyRequests,
			wantMessage:        "migration job spawned",
			wantPolicy:         "other-policy",
			wantEngine:         "other-engine",
		},
		{
			name:               "a PodMigration route to an unregistered engine is ignored",
			registry:           evictionTestRegistry("gvisor=eviction-test-engine", migrating(), other),
			podMigrationRoutes: "gvisor=missing",
			runtimeClass:       gvisor,
			wantCode:           http.StatusTooManyRequests,
			wantMessage:        "migration job spawned",
			wantPolicy:         "engine-policy",
			wantEngine:         "eviction-test-engine",
		},
		{
			// The engine is picked after the job checks: a job in flight is
			// answered for whatever the routes say now.
			name:         "a job in flight, and an engine that refuses the pod now",
			registry:     evictionTestRegistry("gvisor=eviction-test-engine", &evictionTestEngine{verdict: engine.Verdict{Reason: "the engine can't migrate this pod"}}),
			jobPhase:     pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			runtimeClass: gvisor,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "migration job in progress: status Snapshotting",
		},
		{
			name:              "the engine has no policy for the pod",
			registry:          evictionTestRegistry("gvisor=eviction-test-engine", &evictionTestEngine{verdict: engine.Verdict{OK: true}}),
			runtimeClass:      gvisor,
			wantAllowed:       true,
			wantMessage:       "skipping migration: no valid manual+stop policy found",
			wantNoPolicyEvent: true,
		},
		{
			name: "the engine's policy lookup fails",
			registry: evictionTestRegistry("gvisor=eviction-test-engine", &evictionTestEngine{
				verdict:   engine.Verdict{OK: true},
				policyErr: errors.New("cache not synced"),
			}),
			runtimeClass: gvisor,
			wantCode:     http.StatusTooManyRequests,
			wantMessage:  "transient error resolving snapshot policy, retrying",
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
			objects := []client.Object{pod}
			if tt.podMigrationRoutes != "" {
				objects = append(objects, &pmv1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "migration",
					Annotations: map[string]string{engine.AnnotationRuntimeClassEngines: tt.podMigrationRoutes},
				}})
			}
			if tt.jobPhase != "" {
				objects = append(objects, &pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: util.FormatPMJName(pod.Name, string(pod.UID))},
					Status:     pmv1alpha1.PodMigrationJobStatus{Phase: tt.jobPhase},
				})
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
			var allowList *eligibility.RuntimeClassPolicy
			if tt.allowList != "" {
				var err error
				if allowList, err = eligibility.ParseRuntimeClassPolicy(tt.allowList); err != nil {
					t.Fatalf("ParseRuntimeClassPolicy(%q): %v", tt.allowList, err)
				}
			}
			recorder := record.NewFakeRecorder(10)
			gate := &EvictionGate{Client: c, APIReader: c, Recorder: recorder, Eligibility: allowList, Engines: tt.registry(t)}

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
			wantJobs := 0
			if tt.jobPhase != "" || tt.wantPolicy != "" {
				wantJobs = 1
			}
			switch {
			case len(jobs.Items) != wantJobs:
				t.Errorf("found %d job(s), want %d", len(jobs.Items), wantJobs)
			case tt.wantPolicy != "":
				if got := jobs.Items[0].Annotations[util.AnnotationPodSnapshotPolicy]; got != tt.wantPolicy {
					t.Errorf("job policy annotation = %q, want %q", got, tt.wantPolicy)
				}
				if got := jobs.Items[0].Annotations[engine.AnnotationEngine]; got != tt.wantEngine {
					t.Errorf("job engine annotation = %q, want %q", got, tt.wantEngine)
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

// A PodMigration that can't be read could route the pod to another engine:
// the evictor retries.
func TestEvictionGate_PodMigrationLookupFails(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	class := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "test-pod",
			UID:       "test-uid-12345",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{RuntimeClassName: &class},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if _, ok := list.(*pmv1alpha1.PodMigrationList); ok {
					return errors.New("cache not synced")
				}
				return cl.List(ctx, list, opts...)
			},
		}).Build()
	registry := evictionTestRegistry("gvisor=eviction-test-engine", &evictionTestEngine{
		verdict: engine.Verdict{OK: true},
		policy:  &engine.PolicyRef{Name: "engine-policy"},
	})(t)
	gate := &EvictionGate{Client: c, APIReader: c, Engines: registry}

	req := admission.Request{}
	req.Namespace, req.Name, req.SubResource = "default", "test-pod", "eviction"
	resp := gate.Handle(context.Background(), req)

	if resp.Allowed || resp.Result == nil || resp.Result.Code != http.StatusTooManyRequests ||
		resp.Result.Message != "transient error reading the namespace's PodMigration, retrying" {
		t.Errorf("response = allowed %t, %+v; want a 429 for the PodMigration lookup", resp.Allowed, resp.Result)
	}
	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := c.List(context.Background(), jobs); err != nil {
		t.Fatalf("listing jobs: %v", err)
	}
	if len(jobs.Items) != 0 {
		t.Errorf("created %d job(s), want none", len(jobs.Items))
	}
}

// evictionTestRegistry registers engines, the first one as the default, and
// sets routes, in --runtime-class-engines syntax, as the cluster routes.
func evictionTestRegistry(routes string, engines ...*evictionTestEngine) func(t *testing.T) *engine.Registry {
	return func(t *testing.T) *engine.Registry {
		t.Helper()
		r := engine.NewRegistry(engines[0].Name())
		for _, e := range engines {
			if err := r.Register(e); err != nil {
				t.Fatalf("Register(%s): %v", e.Name(), err)
			}
		}
		parsed, err := engine.ParseRoutes(routes)
		if err != nil {
			t.Fatalf("ParseRoutes(%q): %v", routes, err)
		}
		if err := r.SetRoutes(parsed); err != nil {
			t.Fatalf("SetRoutes(%q): %v", routes, err)
		}
		return r
	}
}

func TestEvictionGate_UnfencedEngineRequiresPodOptIn(t *testing.T) {
	const gvisor = "gvisor"
	const refusalReason = `Snapshot engine "eviction-test-engine" does not fence the source pod after checkpoint (fencing=None) and pod does not carry pod-migration.gke.io/allow-unfenced=true`

	engineWithFencing := func(mode engine.FencingMode) *evictionTestEngine {
		return &evictionTestEngine{
			verdict:    engine.Verdict{OK: true},
			policy:     &engine.PolicyRef{Name: "engine-policy"},
			guarantees: &engine.Guarantees{Fencing: mode},
		}
	}

	tests := []struct {
		name        string
		fencing     engine.FencingMode
		annotations map[string]string
		wantAllowed bool
		wantCode    int32
		wantMessage string
		wantJob     bool
		wantEvent   string
	}{
		{
			name:        "FencingNone without allow-unfenced annotation is refused and emits Warning event",
			fencing:     engine.FencingNone,
			wantAllowed: true,
			wantMessage: "skipping migration: " + refusalReason,
			wantJob:     false,
			wantEvent:   "Warning MigrationSkippedUnfencedEngine " + refusalReason + "; allowing cold eviction",
		},
		{
			name:        "unset Fencing without allow-unfenced annotation is refused and emits Warning event",
			fencing:     "",
			wantAllowed: true,
			wantMessage: "skipping migration: " + refusalReason,
			wantJob:     false,
			wantEvent:   "Warning MigrationSkippedUnfencedEngine " + refusalReason + "; allowing cold eviction",
		},
		{
			name:        "FencingNone with allow-unfenced=true proceeds",
			fencing:     engine.FencingNone,
			annotations: map[string]string{engine.AnnotationAllowUnfenced: "true"},
			wantAllowed: false,
			wantCode:    http.StatusTooManyRequests,
			wantMessage: "migration job spawned",
			wantJob:     true,
		},
		{
			name:        "FencingFrozen without annotation proceeds",
			fencing:     engine.FencingFrozen,
			wantAllowed: false,
			wantCode:    http.StatusTooManyRequests,
			wantMessage: "migration job spawned",
			wantJob:     true,
		},
		{
			name:        "FencingStopped without annotation proceeds",
			fencing:     engine.FencingStopped,
			wantAllowed: false,
			wantCode:    http.StatusTooManyRequests,
			wantMessage: "migration job spawned",
			wantJob:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			class := gvisor
			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "test-pod",
					UID:         "test-uid-12345",
					Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: tt.annotations,
				},
				Spec: corev1.PodSpec{RuntimeClassName: &class},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
			recorder := record.NewFakeRecorder(10)
			reg := evictionTestRegistry("gvisor=eviction-test-engine", engineWithFencing(tt.fencing))(t)
			gate := &EvictionGate{Client: c, APIReader: c, Recorder: recorder, Engines: reg}

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
			wantCount := 0
			if tt.wantJob {
				wantCount = 1
			}
			if len(jobs.Items) != wantCount {
				t.Errorf("found %d job(s), want %d", len(jobs.Items), wantCount)
			}

			var gotEvent string
			select {
			case gotEvent = <-recorder.Events:
			default:
			}
			if gotEvent != tt.wantEvent {
				t.Errorf("event = %q, want %q", gotEvent, tt.wantEvent)
			}
		})
	}
}
