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
	resources []*unstructured.Unstructured
	err       error
}

func (e *renderingEngine) Name() string             { return "rendering-engine" }
func (e *renderingEngine) RuntimeClasses() []string { return []string{"@default"} }
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

	want, err := engine.NewGKEPodSnapshots(nil, nil, nil).NamespaceResources(config)
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
