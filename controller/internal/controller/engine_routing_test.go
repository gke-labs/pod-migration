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

package controller

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// routedEngine is a snapshot engine for routing tests: each method returns
// what the test configured.
type routedEngine struct {
	name       string
	classes    []string
	verdict    engine.Verdict
	policy     *engine.PolicyRef
	policyErr  error
	provider   snapshot.Provider
	failures   []restore.Engine
	guarantees *engine.Guarantees
}

func (e *routedEngine) Name() string             { return e.name }
func (e *routedEngine) RuntimeClasses() []string { return e.classes }
func (e *routedEngine) NamespaceResources(*pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	return nil, nil
}
func (e *routedEngine) Eligible(*corev1.Pod, *corev1.Node) engine.Verdict { return e.verdict }
func (e *routedEngine) ResolvePolicy(context.Context, *corev1.Pod) (*engine.PolicyRef, error) {
	return e.policy, e.policyErr
}
func (e *routedEngine) Snapshots() snapshot.Provider      { return e.provider }
func (e *routedEngine) RestoreFailures() []restore.Engine { return e.failures }
func (e *routedEngine) Guarantees() engine.Guarantees {
	if e.guarantees != nil {
		return *e.guarantees
	}
	return engine.Guarantees{Fencing: engine.FencingFrozen, RestoreOnce: true}
}

// routedRestoreAnnotation is the restore order of orderingEngine.
const routedRestoreAnnotation = "engine.example.com/restore-from"

// orderingEngine is a routedEngine with restore orders of its own.
type orderingEngine struct{ routedEngine }

func (e *orderingEngine) OrderRestore(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) error {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[routedRestoreAnnotation] = job.Status.SnapshotRef
	return nil
}

func (e *orderingEngine) OrderColdStartBypass(pod *corev1.Pod) error {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[routedRestoreAnnotation] = "none"
	return nil
}

func (e *orderingEngine) InspectRestoreOrder(pod *corev1.Pod) (string, bool, bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false, false
	}
	val, exists := pod.Annotations[routedRestoreAnnotation]
	if !exists {
		return "", false, false
	}
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || trimmed == "none" {
		return "", true, true
	}
	return trimmed, false, true
}

// namedClassifier is a restore-failure classifier that only has a name, to
// tell classifier sets apart.
type namedClassifier string

func (n namedClassifier) Name() string                                { return string(n) }
func (n namedClassifier) MatchesRestoreFailure(string) (string, bool) { return "", false }

// registryOf registers engines, the first one as the default. It sets no
// routes, so the engines' declared runtime classes decide.
func registryOf(t *testing.T, engines ...engine.Engine) *engine.Registry {
	t.Helper()
	r := engine.NewRegistry(engines[0].Name())
	for _, e := range engines {
		if err := r.Register(e); err != nil {
			t.Fatalf("Register(%s): %v", e.Name(), err)
		}
	}
	return r
}

// podMigrationWithRoutes returns a PodMigration in namespace whose
// runtime-class routes annotation is routes.
func podMigrationWithRoutes(namespace, routes string) *pmv1alpha1.PodMigration {
	return &pmv1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{
		Namespace:   namespace,
		Name:        "migration",
		Annotations: map[string]string{engine.AnnotationRuntimeClassEngines: routes},
	}}
}

func TestPodMigrationJobReconciler_SnapshotProviderComesFromTheJobEngine(t *testing.T) {
	job := &pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{Name: "pmj", Namespace: "default"}}
	provider := &fakeSnapshotProvider{}
	registry := registryOf(t, &routedEngine{name: "routed", provider: provider})

	t.Run("the job engine's provider, unwrapped", func(t *testing.T) {
		// Unwrapped, so optional provider capabilities stay discoverable by
		// type assertion.
		r := &PodMigrationJobReconciler{Engines: registry}
		if got := r.getSnapshotProvider(job); got != snapshot.Provider(provider) {
			t.Fatalf("getSnapshotProvider() = %#v, want the engine's provider %#v", got, provider)
		}
	})
	t.Run("the SnapshotProvider override wins", func(t *testing.T) {
		override := &fakeSnapshotProvider{}
		r := &PodMigrationJobReconciler{Engines: registry, SnapshotProvider: override}
		if got := r.getSnapshotProvider(job); got != snapshot.Provider(override) {
			t.Fatalf("getSnapshotProvider() = %#v, want the override %#v", got, override)
		}
	})
	t.Run("without a registry, GKE Pod Snapshots", func(t *testing.T) {
		r := &PodMigrationJobReconciler{}
		if got, ok := r.getSnapshotProvider(job).(*snapshot.GKEProvider); !ok {
			t.Fatalf("getSnapshotProvider() = %T, want *snapshot.GKEProvider", got)
		}
	})
	t.Run("a registry without a default engine falls back to GKE Pod Snapshots", func(t *testing.T) {
		r := &PodMigrationJobReconciler{Engines: engine.NewRegistry("missing")}
		if got, ok := r.getSnapshotProvider(job).(*snapshot.GKEProvider); !ok {
			t.Fatalf("getSnapshotProvider() = %T, want *snapshot.GKEProvider", got)
		}
	})
}

func TestPodMigrationJobReconciler_RestoreEnginesComeFromTheJobEngine(t *testing.T) {
	job := &pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{Name: "pmj", Namespace: "default"}}
	failures := []restore.Engine{namedClassifier("routed")}
	registry := registryOf(t, &routedEngine{name: "routed", failures: failures})

	if got := (&PodMigrationJobReconciler{Engines: registry}).restoreEngines(job); !reflect.DeepEqual(got, failures) {
		t.Errorf("restoreEngines() with a registry = %v, want the engine's %v", got, failures)
	}
	override := []restore.Engine{namedClassifier("override")}
	if got := (&PodMigrationJobReconciler{Engines: registry, RestoreEngines: override}).restoreEngines(job); !reflect.DeepEqual(got, override) {
		t.Errorf("restoreEngines() with an override = %v, want the override %v", got, override)
	}
	if got := (&PodMigrationJobReconciler{}).restoreEngines(job); !reflect.DeepEqual(got, restore.DefaultEngines()) {
		t.Errorf("restoreEngines() without a registry = %v, want restore.DefaultEngines() %v", got, restore.DefaultEngines())
	}
}

// A Snapshotting job reads its checkpoint status from its engine's provider.
func TestPodMigrationJobReconciler_SnapshottingUsesTheJobEngine(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-pod", UID: "origin-uid-123"}}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-test-pod", CreationTimestamp: metav1.Now()},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
			TargetPodUID: "origin-uid-123",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	provider := &fakeSnapshotProvider{checkStatusResult: &snapshot.Status{
		Phase:       snapshot.PhaseInProgress,
		SnapshotRef: "snapshot-from-the-engine",
		Reason:      "Snapshotting",
	}}
	r := &PodMigrationJobReconciler{
		Client:  c,
		Scheme:  scheme,
		Engines: registryOf(t, &routedEngine{name: "routed", provider: provider}),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}, got); err != nil {
		t.Fatalf("getting the job: %v", err)
	}
	if got.Status.SnapshotRef != "snapshot-from-the-engine" {
		t.Errorf("SnapshotRef = %q, want the one the engine's provider reported", got.Status.SnapshotRef)
	}
}

// A job that names an engine reads its checkpoint status from that engine's
// provider, not the default engine's.
func TestPodMigrationJobReconciler_SnapshottingUsesTheEngineTheJobNames(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-pod", UID: "origin-uid-123"}}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "pmj-test-pod", CreationTimestamp: metav1.Now(),
			Annotations: map[string]string{engine.AnnotationEngine: "named"},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
			TargetPodUID: "origin-uid-123",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	reporting := func(ref string) *fakeSnapshotProvider {
		return &fakeSnapshotProvider{checkStatusResult: &snapshot.Status{Phase: snapshot.PhaseInProgress, SnapshotRef: ref, Reason: "Snapshotting"}}
	}
	r := &PodMigrationJobReconciler{
		Client: c,
		Scheme: scheme,
		Engines: registryOf(t,
			&routedEngine{name: "default", provider: reporting("snapshot-from-the-default-engine")},
			&routedEngine{name: "named", provider: reporting("snapshot-from-the-named-engine")}),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}, got); err != nil {
		t.Fatalf("getting the job: %v", err)
	}
	if got.Status.SnapshotRef != "snapshot-from-the-named-engine" {
		t.Errorf("SnapshotRef = %q, want the one the named engine's provider reported", got.Status.SnapshotRef)
	}
}

// countingProvider is a snapshot provider that counts its calls.
type countingProvider struct{ calls int }

func (p *countingProvider) EnsureTrigger(context.Context, *pmv1alpha1.PodMigrationJob, string) (ctrl.Result, error) {
	p.calls++
	return ctrl.Result{}, nil
}
func (p *countingProvider) CheckStatus(context.Context, *pmv1alpha1.PodMigrationJob, string) (*snapshot.Status, error) {
	p.calls++
	return nil, nil
}
func (p *countingProvider) Cleanup(context.Context, *pmv1alpha1.PodMigrationJob, string) error {
	p.calls++
	return nil
}

func TestPodMigrationJobReconciler_FailsAJobWhoseEngineIsNotRegistered(t *testing.T) {
	newJob := func(phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default", Name: "pmj-test-pod", CreationTimestamp: metav1.Now(),
				Annotations: map[string]string{engine.AnnotationEngine: "missing"},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
				TargetPodUID: "origin-uid-123",
			},
			Status: pmv1alpha1.PodMigrationJobStatus{Phase: phase},
		}
	}
	reconcileJob := func(t *testing.T, job *pmv1alpha1.PodMigrationJob) (*pmv1alpha1.PodMigrationJob, ctrl.Result, *countingProvider, []string) {
		t.Helper()
		scheme := runtime.NewScheme()
		_ = corev1.AddToScheme(scheme)
		_ = pmv1alpha1.AddToScheme(scheme)
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-pod", UID: "origin-uid-123"}}
		c := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithObjects(pod, job).
			Build()
		provider := &countingProvider{}
		recorder := record.NewFakeRecorder(10)
		r := &PodMigrationJobReconciler{
			Client:   c,
			Scheme:   scheme,
			Recorder: recorder,
			Engines:  registryOf(t, &routedEngine{name: "routed", provider: provider}),
		}
		res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job.Name}})
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: job.Name}, got); err != nil {
			t.Fatalf("getting the job: %v", err)
		}
		var events []string
		for len(recorder.Events) > 0 {
			events = append(events, <-recorder.Events)
		}
		return got, res, provider, events
	}

	const wantMessage = `Snapshot engine "missing" is not registered in this controller (registered: routed); failing the migration`
	// Pending: before the checkpoint. Snapshotting: the controller restarted
	// without the engine that took the checkpoint.
	for _, phase := range []pmv1alpha1.PodMigrationJobPhase{pmv1alpha1.PodMigrationJobPhasePending, pmv1alpha1.PodMigrationJobPhaseSnapshotting} {
		t.Run(string(phase), func(t *testing.T) {
			before := testutil.ToFloat64(metrics.OutcomesTotal.WithLabelValues("engine_not_registered"))
			got, res, provider, events := reconcileJob(t, newJob(phase))

			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
				t.Errorf("phase = %s, want Failed", got.Status.Phase)
			}
			if got.Status.CompletionTime == nil {
				t.Errorf("CompletionTime isn't set")
			}
			cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonEngineNotRegistered || cond.Message != wantMessage {
				t.Errorf("Ready condition = %+v, want False/%s with message %q", cond, ReasonEngineNotRegistered, wantMessage)
			}
			if !res.IsZero() {
				t.Errorf("Reconcile result = %+v, want no requeue", res)
			}
			if provider.calls != 0 {
				t.Errorf("the default engine's provider was called %d time(s), want none", provider.calls)
			}
			if want := []string{"Warning MigrationFailed " + wantMessage}; !reflect.DeepEqual(events, want) {
				t.Errorf("events = %q, want %q", events, want)
			}
			if got := testutil.ToFloat64(metrics.OutcomesTotal.WithLabelValues("engine_not_registered")) - before; got != 1 {
				t.Errorf("engine_not_registered outcomes recorded = %v, want 1", got)
			}
		})
	}

	t.Run("a concluded job is left alone", func(t *testing.T) {
		job := newJob(pmv1alpha1.PodMigrationJobPhaseSucceeded)
		now := metav1.Now()
		job.Status.CompletionTime = &now
		got, _, _, _ := reconcileJob(t, job)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			t.Errorf("phase = %s, want Succeeded", got.Status.Phase)
		}
		if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond != nil && cond.Reason == ReasonEngineNotRegistered {
			t.Errorf("Ready condition = %+v, want no %s", cond, ReasonEngineNotRegistered)
		}
	})
}

func TestNodePreemptionReconciler_UsesThePodEngine(t *testing.T) {
	runc := "runc"
	gvisorEngine := func(e routedEngine) *routedEngine {
		e.name, e.classes = "routed", []string{"gvisor"}
		return &e
	}
	other := &routedEngine{name: "other", classes: []string{"gvisor"}, verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "other-policy"}}
	tests := []struct {
		name               string
		engines            []engine.Engine // the first is the default; no routes
		allowList          string          // "" leaves RuntimeClassPolicy nil: gVisor only
		podMigrationRoutes string          // set: the namespace has a PodMigration with these routes
		runtimeClass       *string         // nil keeps the test pod's gvisor
		wantPolicy         string          // "" means no job
		wantEngine         string
		wantNoPolicy       bool
		wantNoEvents       bool
	}{
		{
			name:         "the engine refuses the pod",
			engines:      []engine.Engine{gvisorEngine(routedEngine{verdict: engine.Verdict{Reason: "not for this engine"}})},
			wantNoEvents: true,
		},
		{
			name:       "the engine's policy and name are recorded on the job",
			engines:    []engine.Engine{gvisorEngine(routedEngine{verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Group: "engine.example.com", Kind: "Policy", Name: "engine-policy"}})},
			wantPolicy: "engine-policy",
			wantEngine: "routed",
		},
		{
			name:         "the engine has no policy for the pod",
			engines:      []engine.Engine{gvisorEngine(routedEngine{verdict: engine.Verdict{OK: true}})},
			wantNoPolicy: true,
		},
		{
			name:         "the allow-list applies before the engine",
			engines:      []engine.Engine{&routedEngine{name: "routed", classes: []string{"gvisor", "runc"}, verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "engine-policy"}}},
			runtimeClass: &runc,
			wantNoEvents: true,
		},
		{
			name:         "an allow-listed class the engine serves",
			engines:      []engine.Engine{&routedEngine{name: "routed", classes: []string{"gvisor", "runc"}, verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "engine-policy"}}},
			allowList:    "gvisor,runc",
			runtimeClass: &runc,
			wantPolicy:   "engine-policy",
			wantEngine:   "routed",
		},
		{
			name:         "no engine serves the pod's runtime class",
			engines:      []engine.Engine{gvisorEngine(routedEngine{verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "engine-policy"}})},
			allowList:    "gvisor,runc",
			runtimeClass: &runc,
			wantNoEvents: true,
		},
		{
			name:               "the namespace's PodMigration routes the class to another engine",
			engines:            []engine.Engine{gvisorEngine(routedEngine{verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "engine-policy"}}), other},
			podMigrationRoutes: "gvisor=other",
			wantPolicy:         "other-policy",
			wantEngine:         "other",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			node := createTestNode("spot-node-engine", true, "taint-gke")
			pod := createTestPod("pod-engine", "default", "spot-node-engine", "1Gi", "uid-engine")
			if tt.runtimeClass != nil {
				pod.Spec.RuntimeClassName = tt.runtimeClass
			}
			objects := []client.Object{node, pod} // no PodSnapshotPolicy: only the engine can supply a policy
			if tt.podMigrationRoutes != "" {
				objects = append(objects, podMigrationWithRoutes("default", tt.podMigrationRoutes))
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
				WithObjects(objects...).
				Build()
			var allowList *eligibility.RuntimeClassPolicy
			if tt.allowList != "" {
				var err error
				if allowList, err = eligibility.ParseRuntimeClassPolicy(tt.allowList); err != nil {
					t.Fatalf("ParseRuntimeClassPolicy(%q): %v", tt.allowList, err)
				}
			}
			recorder := record.NewFakeRecorder(10)
			r := &NodePreemptionReconciler{
				Client:                  c,
				Scheme:                  scheme,
				Recorder:                recorder,
				DefaultMigrationTimeout: 10 * time.Minute,
				SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
				RuntimeClassPolicy:      allowList,
				Engines:                 registryOf(t, tt.engines...),
			}
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "spot-node-engine"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			jobs := &pmv1alpha1.PodMigrationJobList{}
			if err := c.List(context.Background(), jobs); err != nil {
				t.Fatalf("listing jobs: %v", err)
			}
			if tt.wantPolicy == "" {
				if len(jobs.Items) != 0 {
					t.Fatalf("created %d job(s), want none", len(jobs.Items))
				}
			} else {
				if len(jobs.Items) != 1 {
					t.Fatalf("created %d job(s), want 1", len(jobs.Items))
				}
				if got := jobs.Items[0].Annotations[util.AnnotationPodSnapshotPolicy]; got != tt.wantPolicy {
					t.Errorf("job policy annotation = %q, want %q", got, tt.wantPolicy)
				}
				if got := jobs.Items[0].Annotations[engine.AnnotationEngine]; got != tt.wantEngine {
					t.Errorf("job engine annotation = %q, want %q", got, tt.wantEngine)
				}
			}

			var events []string
			for len(recorder.Events) > 0 {
				events = append(events, <-recorder.Events)
			}
			sawNoPolicy := false
			for _, ev := range events {
				if strings.Contains(ev, "MigrationSkippedNoPolicy") {
					sawNoPolicy = true
				}
			}
			if sawNoPolicy != tt.wantNoPolicy {
				t.Errorf("MigrationSkippedNoPolicy event seen = %t, want %t (events %v)", sawNoPolicy, tt.wantNoPolicy, events)
			}
			if tt.wantNoEvents && len(events) > 0 {
				t.Errorf("events = %v, want none for a pod the engine refuses", events)
			}
		})
	}
}

func TestPodGateReconciler_RestoreOrdersComeFromTheEngine(t *testing.T) {
	newPod := func(assignedPMJ string) *corev1.Pod {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "replacement", UID: "uid-replacement"},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			},
		}
		if assignedPMJ != "" {
			pod.Annotations = map[string]string{util.AnnotationAssignedPMJ: assignedPMJ}
		}
		return pod
	}
	restoringJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-test"},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "orig-pod"},
			TargetPodUID: "orig-pod-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef: "snapshot-123",
		},
	}
	// Both declare @default, the class of the test pod, so without routes
	// the default engine serves it.
	orderer := &orderingEngine{routedEngine{name: "ordering", classes: []string{"@default"}}}
	plain := &routedEngine{name: "plain", classes: []string{"@default"}}
	jobNaming := func(engineName string) *pmv1alpha1.PodMigrationJob {
		job := restoringJob.DeepCopy()
		job.Annotations = map[string]string{engine.AnnotationEngine: engineName}
		return job
	}

	tests := []struct {
		name               string
		engines            []engine.Engine // the first is the default
		job                *pmv1alpha1.PodMigrationJob
		podMigrationRoutes string // set: the namespace has a PodMigration with these routes
		pod                *corev1.Pod
		wantAnnotations    map[string]string
		wantErr            bool // and the pod keeps its gate
	}{
		{
			name:            "restore order from the job's engine",
			engines:         []engine.Engine{orderer},
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{routedRestoreAnnotation: "snapshot-123", util.AnnotationAssignedPMJ: "pmj-test"},
		},
		{
			name:            "restore order from the engine the job names",
			engines:         []engine.Engine{plain, orderer},
			job:             jobNaming("ordering"),
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{routedRestoreAnnotation: "snapshot-123", util.AnnotationAssignedPMJ: "pmj-test"},
		},
		{
			// Another engine's restore order would restore the wrong
			// snapshot; the job reconciler fails the job.
			name:            "no restore order for a job that names an unregistered engine",
			engines:         []engine.Engine{orderer},
			job:             jobNaming("missing"),
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-test"},
			wantErr:         true,
		},
		{
			name:            "cold-start bypass from the pod's engine",
			engines:         []engine.Engine{orderer},
			pod:             newPod(""),
			wantAnnotations: map[string]string{routedRestoreAnnotation: "none"},
		},
		{
			name:               "cold-start bypass from the engine the namespace's PodMigration routes the pod to",
			engines:            []engine.Engine{plain, orderer},
			podMigrationRoutes: "@default=ordering",
			pod:                newPod(""),
			wantAnnotations:    map[string]string{routedRestoreAnnotation: "none"},
		},
		{
			name:            "an engine without restore orders gets GKE Pod Snapshots' restore order",
			engines:         []engine.Engine{plain},
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{"podsnapshot.gke.io/ps-name": "snapshot-123", util.AnnotationAssignedPMJ: "pmj-test"},
		},
		{
			name:            "an engine without restore orders gets GKE Pod Snapshots' cold-start bypass",
			engines:         []engine.Engine{plain},
			pod:             newPod(""),
			wantAnnotations: map[string]string{"podsnapshot.gke.io/ps-name": ""},
		},
		{
			name:    "cold-start bypass preserves job engine bypass on route change (Failed job)",
			engines: []engine.Engine{plain, orderer},
			job: func() *pmv1alpha1.PodMigrationJob {
				j := jobNaming("plain")
				j.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
				j.Status.SnapshotRef = "snapshot-123"
				return j
			}(),
			podMigrationRoutes: "@default=ordering",
			pod:                newPod("pmj-test"),
			wantAnnotations:    map[string]string{routedRestoreAnnotation: "none", "podsnapshot.gke.io/ps-name": ""},
		},
		{
			name:    "cold-start bypass preserves job engine bypass on route change (SucceededWithoutRestore job)",
			engines: []engine.Engine{plain, orderer},
			job: func() *pmv1alpha1.PodMigrationJob {
				j := jobNaming("plain")
				j.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
				return j
			}(),
			podMigrationRoutes: "@default=ordering",
			pod:                newPod("pmj-test"),
			wantAnnotations:    map[string]string{routedRestoreAnnotation: "none", "podsnapshot.gke.io/ps-name": ""},
		},
		{
			name:    "cold-start bypass preserves job engine bypass on route change (already-consumed job)",
			engines: []engine.Engine{plain, orderer},
			job: func() *pmv1alpha1.PodMigrationJob {
				j := jobNaming("plain")
				j.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
				j.Status.SnapshotRef = "snapshot-123"
				j.Status.Consumed = true
				j.Status.GateReleased = true
				j.Status.RestoredPodUID = "other-pod-uid"
				return j
			}(),
			podMigrationRoutes: "@default=ordering",
			pod:                newPod("pmj-test"),
			wantAnnotations:    map[string]string{routedRestoreAnnotation: "none", "podsnapshot.gke.io/ps-name": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			job := tt.job
			if job == nil {
				job = restoringJob.DeepCopy()
			}
			objects := []client.Object{tt.pod, job}
			if tt.podMigrationRoutes != "" {
				objects = append(objects, podMigrationWithRoutes("default", tt.podMigrationRoutes))
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(objects...).
				Build()

			r := &PodGateReconciler{Client: c, Scheme: scheme, Engines: registryOf(t, tt.engines...)}
			_, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "replacement"}})
			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("Reconcile error = %v, want error: %t", err, tt.wantErr)
			}

			got := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "replacement"}, got); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			if !reflect.DeepEqual(got.Annotations, tt.wantAnnotations) {
				t.Errorf("annotations = %v, want %v", got.Annotations, tt.wantAnnotations)
			}
			if gated := podHasMigrationGate(got); gated != tt.wantErr {
				t.Errorf("the pod's migration gate is there: %t, want %t", gated, tt.wantErr)
			}
		})
	}
}

func TestNodePreemptionReconciler_UnfencedEngineRequiresPodOptIn(t *testing.T) {
	const refusalReason = `Snapshot engine "routed" does not fence the source pod after checkpoint (fencing=None) and pod does not carry pod-migration.gke.io/allow-unfenced=true`

	engineWithFencing := func(mode engine.FencingMode) *routedEngine {
		return &routedEngine{
			name:       "routed",
			classes:    []string{"gvisor"},
			verdict:    engine.Verdict{OK: true},
			policy:     &engine.PolicyRef{Name: "engine-policy"},
			guarantees: &engine.Guarantees{Fencing: mode},
		}
	}

	tests := []struct {
		name            string
		fencing         engine.FencingMode
		annotations     map[string]string
		wantJob         bool
		wantUnfencedEv  bool
		wantSkippedDiff float64
	}{
		{
			name:            "FencingNone without allow-unfenced annotation is refused and emits Warning event",
			fencing:         engine.FencingNone,
			wantJob:         false,
			wantUnfencedEv:  true,
			wantSkippedDiff: 1,
		},
		{
			name:            "FencingNone with allow-unfenced=true proceeds",
			fencing:         engine.FencingNone,
			annotations:     map[string]string{engine.AnnotationAllowUnfenced: "true"},
			wantJob:         true,
			wantUnfencedEv:  false,
			wantSkippedDiff: 0,
		},
		{
			name:            "FencingFrozen without annotation proceeds",
			fencing:         engine.FencingFrozen,
			wantJob:         true,
			wantUnfencedEv:  false,
			wantSkippedDiff: 0,
		},
		{
			name:            "FencingStopped without annotation proceeds",
			fencing:         engine.FencingStopped,
			wantJob:         true,
			wantUnfencedEv:  false,
			wantSkippedDiff: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			node := createTestNode("spot-node-fencing", true, "taint-gke")
			pod := createTestPod("pod-fencing", "default", "spot-node-fencing", "1Gi", "uid-fencing")
			pod.Annotations = tt.annotations

			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
				WithObjects(node, pod).
				Build()
			recorder := record.NewFakeRecorder(10)
			r := &NodePreemptionReconciler{
				Client:                  c,
				Scheme:                  scheme,
				Recorder:                recorder,
				DefaultMigrationTimeout: 10 * time.Minute,
				SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
				Engines:                 registryOf(t, engineWithFencing(tt.fencing)),
			}

			beforeSkipped := testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("unfenced_engine"))
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: "spot-node-fencing"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
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
				t.Errorf("created %d job(s), want %d", len(jobs.Items), wantCount)
			}

			var events []string
			for len(recorder.Events) > 0 {
				events = append(events, <-recorder.Events)
			}
			wantEvent := "Warning MigrationSkippedUnfencedEngine " + refusalReason + "; skipping spot preemption migration"
			sawUnfenced := false
			for _, ev := range events {
				if ev == wantEvent {
					sawUnfenced = true
				}
			}
			if sawUnfenced != tt.wantUnfencedEv {
				t.Errorf("saw MigrationSkippedUnfencedEngine event = %t, want %t (events: %v)", sawUnfenced, tt.wantUnfencedEv, events)
			}
			if gotDiff := testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("unfenced_engine")) - beforeSkipped; gotDiff != tt.wantSkippedDiff {
				t.Errorf("SpotPreemptionSkippedTotal[unfenced_engine] diff = %v, want %v", gotDiff, tt.wantSkippedDiff)
			}
		})
	}
}

func TestPodMigrationJobReconciler_SnapshottingPhaseRefused(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-pod", UID: "origin-uid-123"}}
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-refused", CreationTimestamp: metav1.Now()},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "test-pod"},
			TargetPodUID: "origin-uid-123",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pod, pmj).
		Build()

	provider := &fakeSnapshotProvider{checkStatusResult: &snapshot.Status{
		Phase:   snapshot.PhaseRefused,
		Message: "pod has 2 containers; only single-container pods are supported",
	}}
	recorder := record.NewFakeRecorder(10)
	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
		Engines:  registryOf(t, &routedEngine{name: "routed", provider: provider}),
	}
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-refused"}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pmj-refused"}, got); err != nil {
		t.Fatalf("getting the job: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("phase = %s, want Failed", got.Status.Phase)
	}
	cond := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != "SnapshotRefused" {
		t.Errorf("Ready condition = %+v, want False/SnapshotRefused", cond)
	}
}
