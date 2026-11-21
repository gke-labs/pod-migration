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
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// renderingEngine is an engine whose NamespaceResources returns fixed
// objects, or a fixed error.
type renderingEngine struct {
	name              string   // "" is rendering-engine
	apiGroup          string   // "" is engine.DefaultAPIGroup
	restoreAnnotation string   // "" is <apiGroup>/ps-name
	classes           []string // nil is @default
	resources         []*unstructured.Unstructured
	err               error
}

func (e *renderingEngine) Name() string {
	if e.name == "" {
		return "rendering-engine"
	}
	return e.name
}
func (e *renderingEngine) APIGroup() string {
	if e.apiGroup != "" {
		return e.apiGroup
	}
	return engine.DefaultAPIGroup
}
func (e *renderingEngine) RestoreAnnotation() string {
	if e.restoreAnnotation != "" {
		return e.restoreAnnotation
	}
	return engine.RestoreAnnotation(e.APIGroup())
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
				&renderingEngine{name: "b", classes: []string{"runc", "gvisor"}, resources: []*unstructured.Unstructured{objB}},
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

// renderForLookup renders an engine's objects for a PodMigration whose
// storage location the engine rejects, with a placeholder location: their
// kinds, namespaces and names are those of the objects synced before the
// location was edited (#233). Any other render error is returned as is.
func TestRenderForLookup(t *testing.T) {
	criu, err := engine.NewCRIU(nil, nil, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	valid := engineTestPodMigration("gs://my-test-bucket/snapshots")
	want, err := criu.NamespaceResources(valid)
	if err != nil {
		t.Fatalf("NamespaceResources: %v", err)
	}
	identity := func(objs []*unstructured.Unstructured) []string {
		var out []string
		for _, o := range objs {
			out = append(out, o.GroupVersionKind().String()+" "+o.GetNamespace()+"/"+o.GetName())
		}
		return out
	}

	t.Run("a valid location renders as usual", func(t *testing.T) {
		got, err := renderForLookup(criu, valid)
		if err != nil {
			t.Fatalf("renderForLookup: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("renderForLookup = %v, want the engine's own rendering %v", got, want)
		}
	})

	for _, location := range []string{"", "not-a-gcs-uri", "s3://bucket", "gs://", "gs:///path"} {
		t.Run("rejected location "+location, func(t *testing.T) {
			pm := engineTestPodMigration(location)
			got, err := renderForLookup(criu, pm)
			if err != nil {
				t.Fatalf("renderForLookup: %v", err)
			}
			if !reflect.DeepEqual(identity(got), identity(want)) {
				t.Errorf("renderForLookup renders %v, want the objects synced for a valid location %v", identity(got), identity(want))
			}
			if pm.Spec.Storage.Location != location {
				t.Errorf("renderForLookup changed the PodMigration's location to %q", pm.Spec.Storage.Location)
			}
		})
	}

	t.Run("another render error is returned", func(t *testing.T) {
		renderErr := errors.New("render failed")
		got, err := renderForLookup(&renderingEngine{err: renderErr}, valid)
		if !errors.Is(err, renderErr) || got != nil {
			t.Errorf("renderForLookup = %v, %v; want no objects and %v", got, err, renderErr)
		}
	})

	t.Run("an engine that rejects the placeholder too renders nothing", func(t *testing.T) {
		locationErr := &engine.StorageLocationError{Location: "x", Message: "never valid"}
		got, err := renderForLookup(&renderingEngine{err: locationErr}, valid)
		var gotErr *engine.StorageLocationError
		if !errors.As(err, &gotErr) || got != nil {
			t.Errorf("renderForLookup = %v, %v; want no objects and the *StorageLocationError", got, err)
		}
	})

	t.Run("a nil PodMigration", func(t *testing.T) {
		if got, err := renderForLookup(criu, nil); err == nil || got != nil {
			t.Errorf("renderForLookup(nil) = %v, %v; want an error", got, err)
		}
	})
}

// renderForLookup is only right for an engine whose rendered objects have the
// same kinds, namespaces and names whatever the storage location is: with a
// placeholder location it would otherwise look up, and delete, the wrong
// objects. Nothing in the Engine interface enforces that, so this checks it
// for every engine the controller registers. Add a new engine here.
func TestEngineObjectNamesDontDependOnTheStorageLocation(t *testing.T) {
	criu, err := engine.NewCRIU(nil, nil, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	criuSharedGroup, err := engine.NewCRIU(nil, nil, engine.DefaultAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	engines := map[string]engine.Engine{
		engine.GKEPodSnapshotsName:                engine.NewGKEPodSnapshots(nil, nil),
		engine.CRIUName:                           criu,
		engine.CRIUName + " in the default group": criuSharedGroup,
	}
	identity := func(t *testing.T, eng engine.Engine, location string) []string {
		t.Helper()
		objs, err := eng.NamespaceResources(engineTestPodMigration(location))
		if err != nil {
			t.Fatalf("NamespaceResources(%q): %v", location, err)
		}
		var out []string
		for _, o := range objs {
			out = append(out, o.GroupVersionKind().String()+" "+o.GetNamespace()+"/"+o.GetName())
		}
		return out
	}
	for name, eng := range engines {
		t.Run(name, func(t *testing.T) {
			want := identity(t, eng, lookupStorageLocation)
			if len(want) == 0 {
				t.Fatalf("the engine renders no objects for %s", lookupStorageLocation)
			}
			for _, location := range []string{"gs://bucket-a", "gs://bucket-b/some/long/path", "gs://another-bucket/"} {
				if got := identity(t, eng, location); !reflect.DeepEqual(got, want) {
					t.Errorf("objects for %s are %v, but for %s they are %v: renderForLookup would look up the wrong objects",
						location, got, lookupStorageLocation, want)
				}
			}
		})
	}
}

// A PodMigration's storage location can be edited to one that the engines
// reject after their objects were synced. Deleting it must still wait for a
// CRIU migration in flight, and then delete the CRIU policy and storage
// config: their names don't depend on the location (#233).
func TestPodMigrationReconciler_DeletionAfterInvalidLocationEdit(t *testing.T) {
	for _, location := range []string{"gs://", "gs:///snapshots", "not-a-gcs-uri", ""} {
		t.Run("location "+location, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = pmv1alpha1.AddToScheme(scheme)

			// The objects as they were synced, while the location was valid.
			config := engineTestPodMigration("gs://my-test-bucket/snapshots")
			gkeObjects, err := engine.NewGKEPodSnapshots(nil, nil).NamespaceResources(config)
			if err != nil {
				t.Fatalf("GKE Pod Snapshots NamespaceResources: %v", err)
			}
			criuForRender, err := engine.NewCRIU(nil, nil, engine.CRIUAPIGroup)
			if err != nil {
				t.Fatalf("NewCRIU: %v", err)
			}
			criuObjects, err := criuForRender.NamespaceResources(config)
			if err != nil {
				t.Fatalf("CRIU NamespaceResources: %v", err)
			}

			// Then the location is edited, and the PodMigration deleted.
			config.Spec.Storage.Location = location
			config.Finalizers = []string{StorageCleanupFinalizer}
			now := metav1.Now()
			config.DeletionTimestamp = &now

			// The eviction gate records only the policy that serves a job
			// (#185). For a CRIU job that is the CRIU policy, which only the
			// engine's rendering names.
			job := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: config.Namespace,
					Name:      "pmj-counter-0-abcd1234",
					Annotations: map[string]string{
						util.AnnotationPodSnapshotPolicy: engine.CRIUPodSnapshotPolicyName(config.Name),
					},
				},
				Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
			}

			objects := []client.Object{config, job}
			for _, o := range append(append([]*unstructured.Unstructured{}, gkeObjects...), criuObjects...) {
				objects = append(objects, o.DeepCopy())
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).
				WithStatusSubresource(&pmv1alpha1.PodMigration{}, &pmv1alpha1.PodMigrationJob{}).Build()
			criu, err := engine.NewCRIU(c, scheme, engine.CRIUAPIGroup)
			if err != nil {
				t.Fatalf("NewCRIU: %v", err)
			}
			registry := mustRoutedRegistry(t, "gvisor="+engine.GKEPodSnapshotsName+",@default="+engine.CRIUName,
				engine.NewGKEPodSnapshots(c, scheme), criu)
			r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(config)}

			// 1. The CRIU job is in flight: the deletion waits, and nothing
			// is deleted under it.
			res, err := r.Reconcile(context.Background(), req)
			if err != nil {
				t.Fatalf("Reconcile with a job in flight: %v", err)
			}
			if res.RequeueAfter == 0 {
				t.Errorf("Reconcile with a CRIU job in flight didn't requeue: the deletion wasn't deferred")
			}
			if cond := readyCondition(t, c, config); cond == nil || cond.Reason != "DeletionBlockedByInFlightMigrations" {
				t.Errorf("Ready condition with a CRIU job in flight = %+v, want reason DeletionBlockedByInFlightMigrations", cond)
			}
			for _, o := range append(append([]*unstructured.Unstructured{}, gkeObjects...), criuObjects...) {
				if !objectExists(t, c, o) {
					t.Errorf("%s %s was deleted while a CRIU job is in flight", o.GetKind(), o.GetName())
				}
			}

			// 2. The job ends: the deletion goes on, and deletes the CRIU
			// objects with GKE Pod Snapshots'.
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
			if err := c.Status().Update(context.Background(), job); err != nil {
				t.Fatalf("ending the job: %v", err)
			}
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("Reconcile after the job ended: %v", err)
			}
			for _, o := range criuObjects {
				if objectExists(t, c, o) {
					t.Errorf("CRIU %s %s wasn't deleted", o.GetKind(), o.GetName())
				}
			}
			for _, o := range gkeObjects {
				if objectExists(t, c, o) {
					t.Errorf("GKE Pod Snapshots' %s %s wasn't deleted", o.GetKind(), o.GetName())
				}
			}
			got := &pmv1alpha1.PodMigration{}
			if err := c.Get(context.Background(), req.NamespacedName, got); !apierrors.IsNotFound(err) {
				t.Errorf("the PodMigration is still there (finalizers %v, error %v), want it gone", got.Finalizers, err)
			}
		})
	}
}

// A CRIU storage config that another PodMigration rendered under the same
// name (OwnerUIDLabel) is kept, also when the location is invalid.
func TestPodMigrationReconciler_DeletionAfterInvalidLocationEditKeepsOthersObjects(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	config := engineTestPodMigration("gs://my-test-bucket/snapshots")
	criuForRender, err := engine.NewCRIU(nil, nil, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	criuObjects, err := criuForRender.NamespaceResources(config)
	if err != nil {
		t.Fatalf("CRIU NamespaceResources: %v", err)
	}
	storage, policy := criuObjects[0].DeepCopy(), criuObjects[1].DeepCopy()
	labels := storage.GetLabels()
	labels[engine.OwnerUIDLabel] = "another-podmigration"
	storage.SetLabels(labels)

	config.Spec.Storage.Location = "gs://"
	config.Finalizers = []string{StorageCleanupFinalizer}
	now := metav1.Now()
	config.DeletionTimestamp = &now

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(config, storage, policy).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).Build()
	criu, err := engine.NewCRIU(c, scheme, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	registry := mustRoutedRegistry(t, "gvisor="+engine.GKEPodSnapshotsName+",@default="+engine.CRIUName,
		engine.NewGKEPodSnapshots(c, scheme), criu)
	r := &PodMigrationReconciler{Client: c, Scheme: scheme, Engines: registry}
	if err := reconcileEngineTestPodMigration(t, r, config); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if !objectExists(t, c, storage) {
		t.Errorf("the CRIU storage config labeled for another PodMigration was deleted")
	}
	if objectExists(t, c, policy) {
		t.Errorf("the CRIU policy wasn't deleted")
	}
}
