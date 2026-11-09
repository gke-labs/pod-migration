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
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestPodMigrationReconciler_WatchesManagedObjects runs the reconciler in a
// manager against a real API server, so the watches SetupWithManager
// registers are what wakes it. The PodMigration itself doesn't change while
// a spec edit is reverted or a deleted object is recreated: the
// PodMigration's resourceVersion is checked to prove no PodMigration event
// was involved (#144).
func TestPodMigrationReconciler_WatchesManagedObjects(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest watch test")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd/bases"},
		CRDs:                  buildDynamicPodSnapshotCRDs(),
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("adding the client-go scheme: %v", err)
	}
	if err := pmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding the pmv1alpha1 scheme: %v", err)
	}

	skipNameValidation := true // other tests in this binary may name a controller the same
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Logger:                 zap.New(zap.WriteTo(io.Discard)),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatalf("creating the manager: %v", err)
	}
	r := &PodMigrationReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme()}
	if err := r.SetupWithManager(mgr); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	mgrDone := make(chan error, 1)
	go func() { mgrDone <- mgr.Start(ctx) }()
	// Cleanups run in reverse order: the manager stops before envtest does.
	t.Cleanup(func() {
		cancel()
		if err := <-mgrDone; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating the client: %v", err)
	}
	namespace := createTestNamespace(t, ctx, c, "pm-watch")
	name := "watch"
	pmKey := types.NamespacedName{Namespace: namespace, Name: name}
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec:       pmv1alpha1.PodMigrationSpec{Storage: pmv1alpha1.StorageSpec{Location: "gs://sync-bucket/snaps"}},
	}
	if err := c.Create(ctx, pm); err != nil {
		t.Fatalf("creating the PodMigration: %v", err)
	}

	psscKey := types.NamespacedName{Name: getPSSCName(namespace, name)}
	pspKey := types.NamespacedName{Namespace: namespace, Name: getPSPManualName(name)}
	managed := []struct {
		kind     string
		gvk      schema.GroupVersionKind
		key      types.NamespacedName
		wantSpec map[string]interface{}
		drift    func(spec map[string]interface{})
	}{
		{
			kind:     "PodSnapshotStorageConfig",
			gvk:      psscGVKForTest,
			key:      psscKey,
			wantSpec: wantPSSCSpec(),
			drift: func(spec map[string]interface{}) {
				spec["snapshotStorageConfig"] = map[string]interface{}{
					"gcs": map[string]interface{}{"bucket": "stale-bucket"},
				}
			},
		},
		{
			kind:     "PodSnapshotPolicy",
			gvk:      pspGVKForTest,
			key:      pspKey,
			wantSpec: wantPSPSpec(psscKey.Name),
			drift: func(spec map[string]interface{}) {
				spec["triggerConfig"] = map[string]interface{}{"type": "manual", "postCheckpoint": "resume"}
			},
		},
	}

	getManaged := func(gvk schema.GroupVersionKind, key types.NamespacedName) (*unstructured.Unstructured, error) {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		return obj, c.Get(ctx, key, obj)
	}
	ready := func() *metav1.Condition {
		got := &pmv1alpha1.PodMigration{}
		if err := c.Get(ctx, pmKey, got); err != nil {
			return nil
		}
		return meta.FindStatusCondition(got.Status.Conditions, "Ready")
	}
	pmResourceVersion := func(t *testing.T) string {
		t.Helper()
		got := &pmv1alpha1.PodMigration{}
		if err := c.Get(ctx, pmKey, got); err != nil {
			t.Fatalf("Get PodMigration: %v", err)
		}
		return got.ResourceVersion
	}
	// waitFor polls cond until it holds; the reconciler acts within
	// milliseconds, and the deadline only bounds a failing test.
	waitFor := func(t *testing.T, what string, cond func() bool) {
		t.Helper()
		deadline := time.Now().Add(30 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	inDesiredState := func(gvk schema.GroupVersionKind, key types.NamespacedName, wantSpec map[string]interface{}) func() bool {
		return func() bool {
			obj, err := getManaged(gvk, key)
			return err == nil && obj.GetDeletionTimestamp() == nil && reflect.DeepEqual(obj.Object["spec"], wantSpec)
		}
	}

	waitFor(t, "the PodMigration to be Ready", func() bool {
		cond := ready()
		return cond != nil && cond.Status == metav1.ConditionTrue
	})
	for _, m := range managed {
		waitFor(t, m.kind+" to be created", inDesiredState(m.gvk, m.key, m.wantSpec))
	}

	t.Run("spec edit is reverted", func(t *testing.T) {
		for _, m := range managed {
			rv := pmResourceVersion(t)
			obj, err := getManaged(m.gvk, m.key)
			if err != nil {
				t.Fatalf("Get %s: %v", m.kind, err)
			}
			spec := obj.Object["spec"].(map[string]interface{})
			m.drift(spec)
			if err := c.Update(ctx, obj); err != nil {
				t.Fatalf("editing the %s spec: %v", m.kind, err)
			}
			waitFor(t, m.kind+" spec to be reverted", inDesiredState(m.gvk, m.key, m.wantSpec))
			if got := pmResourceVersion(t); got != rv {
				t.Errorf("PodMigration resourceVersion changed from %s to %s while reverting the %s", rv, got, m.kind)
			}
		}
	})

	t.Run("deleted object is recreated", func(t *testing.T) {
		for _, m := range managed {
			rv := pmResourceVersion(t)
			obj, err := getManaged(m.gvk, m.key)
			if err != nil {
				t.Fatalf("Get %s: %v", m.kind, err)
			}
			oldUID := obj.GetUID()
			if err := c.Delete(ctx, obj); err != nil {
				t.Fatalf("deleting the %s: %v", m.kind, err)
			}
			waitFor(t, m.kind+" to be recreated", func() bool {
				got, err := getManaged(m.gvk, m.key)
				return err == nil && got.GetUID() != oldUID && reflect.DeepEqual(got.Object["spec"], m.wantSpec)
			})
			if got := pmResourceVersion(t); got != rv {
				t.Errorf("PodMigration resourceVersion changed from %s to %s while recreating the %s", rv, got, m.kind)
			}
		}
	})

	t.Run("terminating object is left alone, then recreated", func(t *testing.T) {
		for _, m := range managed {
			obj, err := getManaged(m.gvk, m.key)
			if err != nil {
				t.Fatalf("Get %s: %v", m.kind, err)
			}
			oldUID := obj.GetUID()
			obj.SetFinalizers([]string{"example.com/hold"})
			m.drift(obj.Object["spec"].(map[string]interface{}))
			if err := c.Update(ctx, obj); err != nil {
				t.Fatalf("adding a finalizer to the %s: %v", m.kind, err)
			}
			// The drift may be reverted before the delete lands; what matters
			// is that nothing writes the object once it's terminating.
			if err := c.Delete(ctx, obj); err != nil {
				t.Fatalf("deleting the %s: %v", m.kind, err)
			}
			waitFor(t, "the PodMigration to report the terminating "+m.kind, func() bool {
				cond := ready()
				return cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == ReasonManagedObjectTerminating
			})
			terminating, err := getManaged(m.gvk, m.key)
			if err != nil {
				t.Fatalf("Get the terminating %s: %v", m.kind, err)
			}
			// Wait past a requeue: the terminating object still isn't written.
			time.Sleep(managedObjectTerminatingRequeue + time.Second)
			still, err := getManaged(m.gvk, m.key)
			if err != nil {
				t.Fatalf("Get the terminating %s again: %v", m.kind, err)
			}
			if still.GetResourceVersion() != terminating.GetResourceVersion() {
				t.Errorf("terminating %s was written: resourceVersion %s -> %s", m.kind, terminating.GetResourceVersion(), still.GetResourceVersion())
			}

			// Release the finalizer: the object goes away and is recreated.
			still.SetFinalizers(nil)
			if err := c.Update(ctx, still); err != nil && !apierrors.IsNotFound(err) {
				t.Fatalf("removing the finalizer from the %s: %v", m.kind, err)
			}
			waitFor(t, m.kind+" to be recreated", func() bool {
				got, err := getManaged(m.gvk, m.key)
				return err == nil && got.GetUID() != oldUID && got.GetDeletionTimestamp() == nil &&
					reflect.DeepEqual(got.Object["spec"], m.wantSpec)
			})
			waitFor(t, "the PodMigration to be Ready again", func() bool {
				cond := ready()
				return cond != nil && cond.Status == metav1.ConditionTrue
			})
		}
	})
}
