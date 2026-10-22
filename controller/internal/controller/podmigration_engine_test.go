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
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// renderingEngine is an engine whose NamespaceResources returns fixed
// objects, or a fixed error.
type renderingEngine struct {
	name      string   // "" is rendering-engine
	classes   []string // nil is @default
	resources []*unstructured.Unstructured
	err       error
}

func (e *renderingEngine) Name() string {
	if e.name == "" {
		return "rendering-engine"
	}
	return e.name
}
func (e *renderingEngine) RuntimeClasses() []string {
	if e.classes == nil {
		return []string{"@default"}
	}
	return e.classes
}
func (e *renderingEngine) NamespaceResources(*pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	if e.err != nil {
		return nil, e.err
	}
	out := make([]*unstructured.Unstructured, 0, len(e.resources))
	for _, r := range e.resources {
		out = append(out, r.DeepCopy())
	}
	return out, nil
}
func (e *renderingEngine) Eligible(*corev1.Pod, *corev1.Node) engine.Verdict {
	return engine.Verdict{OK: true}
}
func (e *renderingEngine) ResolvePolicy(context.Context, *corev1.Pod) (*engine.PolicyRef, error) {
	return nil, nil
}
func (e *renderingEngine) Snapshots() snapshot.Provider      { return nil }
func (e *renderingEngine) RestoreFailures() []restore.Engine { return nil }
func (e *renderingEngine) Guarantees() engine.Guarantees     { return engine.Guarantees{} }

func engineTestPodMigration(location string) *pmv1alpha1.PodMigration {
	return &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  "default",
			Name:       "test-migration",
			UID:        types.UID("pm-uid"),
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: location},
		},
	}
}

func reconcileEngineTestPodMigration(t *testing.T, r *PodMigrationReconciler, config *pmv1alpha1.PodMigration) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: config.Namespace, Name: config.Name},
	})
	return err
}

func readyCondition(t *testing.T, c client.Client, config *pmv1alpha1.PodMigration) *metav1.Condition {
	t.Helper()
	got := &pmv1alpha1.PodMigration{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(config), got); err != nil {
		t.Fatalf("getting the PodMigration: %v", err)
	}
	return meta.FindStatusCondition(got.Status.Conditions, "Ready")
}

func gkeObjectExists(t *testing.T, c client.Client, kind, namespace, name string) bool {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: kind})
	err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, u)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting %s %s: %v", kind, name, err)
	}
	return err == nil
}

// Without a registry, the reconciler syncs exactly the GKE Pod Snapshots
// engine's objects, as it rendered them itself before the engine package.
func TestPodMigrationReconciler_WithoutEnginesSyncsGKEPodSnapshotsResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	config := engineTestPodMigration("gs://my-test-bucket/snapshots/path")
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()

	if err := reconcileEngineTestPodMigration(t, &PodMigrationReconciler{Client: c, Scheme: scheme}, config); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	want, err := engine.NewGKEPodSnapshots(nil, nil).NamespaceResources(config)
	if err != nil {
		t.Fatalf("NamespaceResources: %v", err)
	}
	for _, w := range want {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(w.GroupVersionKind())
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("getting %s %s: %v", w.GetKind(), w.GetName(), err)
		}
		if !reflect.DeepEqual(got.Object["spec"], w.Object["spec"]) {
			t.Errorf("%s spec = %v, want %v", w.GetKind(), got.Object["spec"], w.Object["spec"])
		}
		if !reflect.DeepEqual(got.GetLabels(), w.GetLabels()) {
			t.Errorf("%s labels = %v, want %v", w.GetKind(), got.GetLabels(), w.GetLabels())
		}
	}
	if cond := readyCondition(t, c, config); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("Ready condition = %+v, want True", cond)
	}
}

func TestPodMigrationReconciler_SyncsTheDefaultEngineResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	config := engineTestPodMigration("not-a-gcs-location")

	storage := &unstructured.Unstructured{}
	storage.SetGroupVersionKind(schema.GroupVersionKind{Group: "engine.example.com", Version: "v1", Kind: "StorageConfig"})
	storage.SetName("storage-for-test-migration")
	storage.Object["spec"] = map[string]interface{}{"location": "not-a-gcs-location"}
	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(schema.GroupVersionKind{Group: "engine.example.com", Version: "v1", Kind: "Policy"})
	policy.SetName("policy-for-test-migration")
	policy.SetNamespace("default")
	policy.Object["spec"] = map[string]interface{}{"storage": "storage-for-test-migration"}

	var created []string
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok {
					created = append(created, u.GetKind())
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).Build()

	registry := engine.NewRegistry("rendering-engine")
	if err := registry.Register(&renderingEngine{resources: []*unstructured.Unstructured{storage, policy}}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
	if err := reconcileEngineTestPodMigration(t, r, config); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if want := []string{"StorageConfig", "Policy"}; !reflect.DeepEqual(created, want) {
		t.Errorf("created %v, want %v in that order", created, want)
	}
	for _, w := range []*unstructured.Unstructured{storage, policy} {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(w.GroupVersionKind())
		if err := c.Get(context.Background(), client.ObjectKeyFromObject(w), got); err != nil {
			t.Fatalf("getting %s: %v", w.GetKind(), err)
		}
		if !reflect.DeepEqual(got.Object["spec"], w.Object["spec"]) {
			t.Errorf("%s spec = %v, want %v", w.GetKind(), got.Object["spec"], w.Object["spec"])
		}
	}
	// The GKE Pod Snapshots objects aren't rendered: the location isn't even
	// a GCS one, and the engine accepted it.
	if gkeObjectExists(t, c, "PodSnapshotStorageConfig", "", getPSSCName("default", "test-migration")) ||
		gkeObjectExists(t, c, "PodSnapshotPolicy", "default", getPSPManualName("test-migration")) {
		t.Errorf("GKE Pod Snapshots objects were created for a PodMigration served by another engine")
	}
	if cond := readyCondition(t, c, config); cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != "Reconciled" {
		t.Errorf("Ready condition = %+v, want True/Reconciled", cond)
	}
}

func TestPodMigrationReconciler_EngineRenderErrors(t *testing.T) {
	tests := []struct {
		name       string
		registry   func(t *testing.T) *engine.Registry
		wantErr    string
		wantReason string // "" means no Ready condition is set
	}{
		{
			name: "storage location the engine can't use",
			registry: func(t *testing.T) *engine.Registry {
				r := engine.NewRegistry("rendering-engine")
				if err := r.Register(&renderingEngine{err: &engine.StorageLocationError{
					Location: "gs://my-test-bucket", Message: "this engine can't use gs://my-test-bucket",
				}}); err != nil {
					t.Fatalf("Register: %v", err)
				}
				return r
			},
			wantErr:    "this engine can't use gs://my-test-bucket",
			wantReason: "InvalidStorageLocation",
		},
		{
			name: "other render error",
			registry: func(t *testing.T) *engine.Registry {
				r := engine.NewRegistry("rendering-engine")
				if err := r.Register(&renderingEngine{err: errors.New("render failed")}); err != nil {
					t.Fatalf("Register: %v", err)
				}
				return r
			},
			wantErr: "render failed",
		},
		{
			name:     "default engine not registered",
			registry: func(*testing.T) *engine.Registry { return engine.NewRegistry("missing") },
			wantErr:  `default engine "missing" is not registered`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			config := engineTestPodMigration("gs://my-test-bucket")
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
				WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()

			r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: tt.registry(t)}
			err := reconcileEngineTestPodMigration(t, r, config)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("Reconcile error = %v, want %q", err, tt.wantErr)
			}

			cond := readyCondition(t, c, config)
			switch {
			case tt.wantReason == "" && cond != nil:
				t.Errorf("Ready condition = %+v, want none", cond)
			case tt.wantReason != "" && (cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != tt.wantReason || cond.Message != tt.wantErr):
				t.Errorf("Ready condition = %+v, want False/%s with message %q", cond, tt.wantReason, tt.wantErr)
			}
			if gkeObjectExists(t, c, "PodSnapshotStorageConfig", "", getPSSCName("default", "test-migration")) ||
				gkeObjectExists(t, c, "PodSnapshotPolicy", "default", getPSPManualName("test-migration")) {
				t.Errorf("objects were synced despite the render error")
			}
		})
	}
}

// engineObject returns an engine.example.com/v1 object of kind; namespace ""
// makes it cluster-scoped.
func engineObject(kind, namespace, name string, labels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: "engine.example.com", Version: "v1", Kind: kind})
	u.SetNamespace(namespace)
	u.SetName(name)
	u.SetLabels(labels)
	u.Object["spec"] = map[string]interface{}{"owner": name}
	return u
}

func objectExists(t *testing.T, c client.Client, obj *unstructured.Unstructured) bool {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(obj.GroupVersionKind())
	err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), u)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting %s %s: %v", obj.GetKind(), obj.GetName(), err)
	}
	return err == nil
}

func mustRoutedRegistry(t *testing.T, routes string, engines ...engine.Engine) *engine.Registry {
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

// The reconciler syncs the objects of every engine that serves a runtime
// class in the namespace: the cluster routes' engines and those of the
// PodMigration's routes, but not an engine that nothing routes to.
func TestPodMigrationReconciler_SyncsTheResourcesOfEveryRoutedEngine(t *testing.T) {
	objA := engineObject("Policy", "default", "policy-a", nil)
	objB := engineObject("Policy", "default", "policy-b", nil)
	objUnused := engineObject("Policy", "default", "policy-unused", nil)
	tests := []struct {
		name               string
		podMigrationRoutes string
		wantSynced         []*unstructured.Unstructured
		wantNotSynced      []*unstructured.Unstructured
		wantCondition      *metav1.Condition // nil: no EngineRoutesValid condition
	}{
		{
			name:          "cluster routes",
			wantSynced:    []*unstructured.Unstructured{objA},
			wantNotSynced: []*unstructured.Unstructured{objB, objUnused},
		},
		{
			name:               "the PodMigration routes another class",
			podMigrationRoutes: "runc=b",
			wantSynced:         []*unstructured.Unstructured{objA, objB},
			wantNotSynced:      []*unstructured.Unstructured{objUnused},
			wantCondition: &metav1.Condition{
				Type: ConditionEngineRoutesValid, Status: metav1.ConditionTrue,
				Reason: ReasonEngineRoutesValid, Message: "Runtime class routes: runc=b",
			},
		},
		{
			name:               "the PodMigration routes the class elsewhere",
			podMigrationRoutes: "gvisor=b",
			wantSynced:         []*unstructured.Unstructured{objB},
			wantNotSynced:      []*unstructured.Unstructured{objA, objUnused},
			wantCondition: &metav1.Condition{
				Type: ConditionEngineRoutesValid, Status: metav1.ConditionTrue,
				Reason: ReasonEngineRoutesValid, Message: "Runtime class routes: gvisor=b",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			config := engineTestPodMigration("gs://my-test-bucket")
			if tt.podMigrationRoutes != "" {
				config.Annotations = map[string]string{engine.AnnotationRuntimeClassEngines: tt.podMigrationRoutes}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
				WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()
			registry := mustRoutedRegistry(t, "gvisor=a",
				&renderingEngine{name: "a", classes: []string{"gvisor"}, resources: []*unstructured.Unstructured{objA}},
				&renderingEngine{name: "b", classes: []string{"runc"}, resources: []*unstructured.Unstructured{objB}},
				&renderingEngine{name: "unused", classes: []string{"kata"}, resources: []*unstructured.Unstructured{objUnused}})

			r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
			if err := reconcileEngineTestPodMigration(t, r, config); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			for _, obj := range tt.wantSynced {
				if !objectExists(t, c, obj) {
					t.Errorf("%s wasn't synced", obj.GetName())
				}
			}
			for _, obj := range tt.wantNotSynced {
				if objectExists(t, c, obj) {
					t.Errorf("%s was synced, for an engine that serves no class in the namespace", obj.GetName())
				}
			}
			got := &pmv1alpha1.PodMigration{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(config), got); err != nil {
				t.Fatalf("getting the PodMigration: %v", err)
			}
			if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond == nil || cond.Status != metav1.ConditionTrue {
				t.Errorf("Ready condition = %+v, want True", cond)
			}
			assertEngineRoutesCondition(t, got, tt.wantCondition)
		})
	}
}

func assertEngineRoutesCondition(t *testing.T, pm *pmv1alpha1.PodMigration, want *metav1.Condition) {
	t.Helper()
	got := meta.FindStatusCondition(pm.Status.Conditions, ConditionEngineRoutesValid)
	switch {
	case want == nil && got != nil:
		t.Errorf("%s condition = %+v, want none", ConditionEngineRoutesValid, got)
	case want != nil && got == nil:
		t.Errorf("no %s condition, want %+v", ConditionEngineRoutesValid, want)
	case want != nil && (got.Status != want.Status || got.Reason != want.Reason || got.Message != want.Message):
		t.Errorf("%s condition = %s/%s %q, want %s/%s %q", ConditionEngineRoutesValid,
			got.Status, got.Reason, got.Message, want.Status, want.Reason, want.Message)
	}
}

func TestPodMigrationReconciler_ReportsInvalidEngineRoutes(t *testing.T) {
	tests := []struct {
		name               string
		podMigrationRoutes string
		previous           bool // the PodMigration already has the condition
		want               *metav1.Condition
	}{
		{
			name:               "routes that don't parse",
			podMigrationRoutes: "runc",
			want: &metav1.Condition{
				Status: metav1.ConditionFalse, Reason: ReasonInvalidEngineRoutes,
				Message: `invalid route "runc": want class=engine; the annotation is ignored`,
			},
		},
		{
			name:               "a route to an engine that isn't registered",
			podMigrationRoutes: "runc=missing",
			want: &metav1.Condition{
				Status: metav1.ConditionFalse, Reason: ReasonEngineNotRegistered,
				Message: `runtime class "runc" routes to engine "missing": engine not registered (registered: a); that route is ignored`,
			},
		},
		{
			name:     "the annotation was removed",
			previous: true,
			want:     nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)
			config := engineTestPodMigration("gs://my-test-bucket")
			if tt.podMigrationRoutes != "" {
				config.Annotations = map[string]string{engine.AnnotationRuntimeClassEngines: tt.podMigrationRoutes}
			}
			if tt.previous {
				config.Status.Conditions = []metav1.Condition{{
					Type: ConditionEngineRoutesValid, Status: metav1.ConditionTrue, Reason: ReasonEngineRoutesValid,
					Message: "Runtime class routes: gvisor=a", LastTransitionTime: metav1.Now(),
				}}
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config).
				WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()
			objA := engineObject("Policy", "default", "policy-a", nil)
			registry := mustRoutedRegistry(t, "gvisor=a",
				&renderingEngine{name: "a", classes: []string{"gvisor"}, resources: []*unstructured.Unstructured{objA}})

			r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
			if err := reconcileEngineTestPodMigration(t, r, config); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}

			got := &pmv1alpha1.PodMigration{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(config), got); err != nil {
				t.Fatalf("getting the PodMigration: %v", err)
			}
			// The cluster routes still serve the namespace.
			if !objectExists(t, c, objA) {
				t.Errorf("the cluster route's engine objects weren't synced")
			}
			if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond == nil || cond.Status != metav1.ConditionTrue {
				t.Errorf("Ready condition = %+v, want True", cond)
			}
			if tt.want != nil {
				tt.want.Type = ConditionEngineRoutesValid
			}
			assertEngineRoutesCondition(t, got, tt.want)
		})
	}
}

// Deleting a PodMigration deletes every engine's objects: GKE Pod
// Snapshots' as before, and the other engines' that exist, whatever the
// routes are now.
func TestPodMigrationReconciler_DeletesEveryEngineResources(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	config := engineTestPodMigration("gs://my-test-bucket")
	config.Finalizers = []string{StorageCleanupFinalizer}
	now := metav1.Now()
	config.DeletionTimestamp = &now

	gkeObjects, err := engine.NewGKEPodSnapshots(nil, nil).NamespaceResources(config)
	if err != nil {
		t.Fatalf("NamespaceResources: %v", err)
	}
	owned := map[string]string{engine.OwnerUIDLabel: string(config.UID)}
	ownedStorage := engineObject("StorageConfig", "", "storage-b", owned)
	policy := engineObject("Policy", "default", "policy-b", nil)
	othersStorage := engineObject("StorageConfig", "", "storage-c", map[string]string{engine.OwnerUIDLabel: "another-podmigration"})
	unlabeledStorage := engineObject("StorageConfig", "", "storage-d", nil)

	objects := []client.Object{config, ownedStorage.DeepCopy(), policy.DeepCopy(), othersStorage.DeepCopy(), unlabeledStorage.DeepCopy()}
	for _, o := range gkeObjects {
		objects = append(objects, o.DeepCopy())
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()

	// Nothing routes to b, c or d now; e can't render its objects.
	registry := mustRoutedRegistry(t, "gvisor="+engine.GKEPodSnapshotsName,
		engine.NewGKEPodSnapshots(c, scheme),
		&renderingEngine{name: "b", resources: []*unstructured.Unstructured{ownedStorage, policy}},
		&renderingEngine{name: "c", resources: []*unstructured.Unstructured{othersStorage}},
		&renderingEngine{name: "d", resources: []*unstructured.Unstructured{unlabeledStorage}},
		&renderingEngine{name: "e", err: errors.New("render failed")})
	r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
	if err := reconcileEngineTestPodMigration(t, r, config); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	for _, o := range gkeObjects {
		if objectExists(t, c, o) {
			t.Errorf("GKE Pod Snapshots' %s %s wasn't deleted", o.GetKind(), o.GetName())
		}
	}
	for _, o := range []*unstructured.Unstructured{ownedStorage, policy, unlabeledStorage} {
		if objectExists(t, c, o) {
			t.Errorf("%s %s wasn't deleted", o.GetKind(), o.GetName())
		}
	}
	if !objectExists(t, c, othersStorage) {
		t.Errorf("storage-c, labeled for another PodMigration, was deleted")
	}
	got := &pmv1alpha1.PodMigration{}
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(config), got); !apierrors.IsNotFound(err) {
		t.Errorf("the PodMigration is still there (finalizers %v, error %v), want it gone", got.Finalizers, err)
	}
}
