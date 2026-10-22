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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// routedEngine is a snapshot engine for routing tests: each method returns
// what the test configured.
type routedEngine struct {
	name      string
	classes   []string
	verdict   engine.Verdict
	policy    *engine.PolicyRef
	policyErr error
	provider  snapshot.Provider
	failures  []restore.Engine
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

// namedClassifier is a restore-failure classifier that only has a name, to
// tell classifier sets apart.
type namedClassifier string

func (n namedClassifier) Name() string                                { return string(n) }
func (n namedClassifier) MatchesRestoreFailure(string) (string, bool) { return "", false }

// registryOf registers engines, the first one as the default.
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

func TestNodePreemptionReconciler_UsesThePodEngine(t *testing.T) {
	runc := "runc"
	tests := []struct {
		name         string
		engine       *routedEngine
		runtimeClass *string // nil keeps the test pod's gvisor
		wantPolicy   string  // "" means no job
		wantNoPolicy bool
		wantNoEvents bool
	}{
		{
			name:         "the engine refuses the pod",
			engine:       &routedEngine{name: "routed", verdict: engine.Verdict{Reason: "not for this engine"}},
			wantNoEvents: true,
		},
		{
			name:       "the engine's policy is recorded on the job",
			engine:     &routedEngine{name: "routed", verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Group: "engine.example.com", Kind: "Policy", Name: "engine-policy"}},
			wantPolicy: "engine-policy",
		},
		{
			name:         "the engine has no policy for the pod",
			engine:       &routedEngine{name: "routed", verdict: engine.Verdict{OK: true}},
			wantNoPolicy: true,
		},
		{
			name:         "the engine's eligibility replaces the default runtime-class allow-list",
			engine:       &routedEngine{name: "routed", verdict: engine.Verdict{OK: true}, policy: &engine.PolicyRef{Name: "engine-policy"}},
			runtimeClass: &runc,
			wantPolicy:   "engine-policy",
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
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
				WithObjects(node, pod). // no PodSnapshotPolicy: only the engine can supply a policy
				Build()
			recorder := record.NewFakeRecorder(10)
			r := &NodePreemptionReconciler{
				Client:                  c,
				Scheme:                  scheme,
				Recorder:                recorder,
				DefaultMigrationTimeout: 10 * time.Minute,
				SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
				Engines:                 registryOf(t, tt.engine),
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
	orderer := &orderingEngine{routedEngine{name: "ordering"}}
	plain := &routedEngine{name: "plain"}

	tests := []struct {
		name            string
		engine          engine.Engine
		pod             *corev1.Pod
		wantAnnotations map[string]string
	}{
		{
			name:            "restore order from the job's engine",
			engine:          orderer,
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{routedRestoreAnnotation: "snapshot-123", util.AnnotationAssignedPMJ: "pmj-test"},
		},
		{
			name:            "cold-start bypass from the pod's engine",
			engine:          orderer,
			pod:             newPod(""),
			wantAnnotations: map[string]string{routedRestoreAnnotation: "none"},
		},
		{
			name:            "an engine without restore orders gets GKE Pod Snapshots' restore order",
			engine:          plain,
			pod:             newPod("pmj-test"),
			wantAnnotations: map[string]string{"podsnapshot.gke.io/ps-name": "snapshot-123", util.AnnotationAssignedPMJ: "pmj-test"},
		},
		{
			name:            "an engine without restore orders gets GKE Pod Snapshots' cold-start bypass",
			engine:          plain,
			pod:             newPod(""),
			wantAnnotations: map[string]string{"podsnapshot.gke.io/ps-name": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(tt.pod, restoringJob.DeepCopy()).
				Build()

			r := &PodGateReconciler{Client: c, Scheme: scheme, Engines: registryOf(t, tt.engine)}
			if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "replacement"}}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got := &corev1.Pod{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "replacement"}, got); err != nil {
				t.Fatalf("getting the pod: %v", err)
			}
			if !reflect.DeepEqual(got.Annotations, tt.wantAnnotations) {
				t.Errorf("annotations = %v, want %v", got.Annotations, tt.wantAnnotations)
			}
			if podHasMigrationGate(got) {
				t.Errorf("the migration gate is still on the pod")
			}
		})
	}
}
