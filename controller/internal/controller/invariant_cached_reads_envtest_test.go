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
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
	ctrlcontroller "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// snapshotGETCounter counts the GET requests for single triggers and
// PodSnapshots that reach the API server. Informer LISTs and WATCHes are
// collection requests and don't count.
type snapshotGETCounter struct {
	mu   sync.Mutex
	gets map[string]int // resource -> count
}

func (c *snapshotGETCounter) wrap(rt http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		// /apis/<group>/<version>/namespaces/<ns>/<resource>/<name>
		parts := strings.Split(req.URL.Path, "/")
		if req.Method == http.MethodGet && len(parts) == 8 && parts[1] == "apis" && parts[4] == "namespaces" {
			switch parts[6] {
			case "podsnapshots", "podsnapshotmanualtriggers":
				c.mu.Lock()
				c.gets[parts[2]+"/"+parts[6]]++
				c.mu.Unlock()
			}
		}
		return rt.RoundTrip(req)
	})
}

func (c *snapshotGETCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.gets = map[string]int{}
}

func (c *snapshotGETCounter) snapshot() (int, map[string]int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.gets))
	n := 0
	for k, v := range c.gets {
		out[k] = v
		n += v
	}
	return n, out
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// countingCacheReader counts the reads that invariant evaluation sends to
// the manager's cache, so a test can tell they happened.
type countingCacheReader struct {
	client.Reader
	mu    sync.Mutex
	reads int
}

func (c *countingCacheReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	c.mu.Lock()
	c.reads++
	c.mu.Unlock()
	return c.Reader.Get(ctx, key, obj, opts...)
}

type cachedReadsEnv struct {
	admin   client.Client
	mgr     ctrl.Manager
	counter *snapshotGETCounter
}

// startCachedReadsEnv starts envtest with crds and a manager whose requests
// are counted. It doesn't start the manager's controllers, so nothing
// reconciles behind the test's back, only its cache.
func startCachedReadsEnv(t *testing.T, crds []*apiextensionsv1.CustomResourceDefinition) (*cachedReadsEnv, *runtime.Scheme) {
	t.Helper()
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd/bases"},
		CRDs:                  crds,
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
		t.Fatal(err)
	}
	if err := pmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating the admin client: %v", err)
	}

	counter := &snapshotGETCounter{gets: map[string]int{}}
	mgrCfg := rest.CopyConfig(cfg)
	mgrCfg.WrapTransport = counter.wrap
	skipNameValidation := true // other tests in this binary may name a controller the same
	mgr, err := ctrl.NewManager(mgrCfg, ctrl.Options{
		Scheme:                 scheme,
		Logger:                 zap.New(zap.WriteTo(io.Discard)),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatalf("creating the manager: %v", err)
	}
	if err := RegisterFieldIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		t.Fatalf("registering field indexes: %v", err)
	}
	return &cachedReadsEnv{admin: admin, mgr: mgr, counter: counter}, scheme
}

// startCache starts the manager's cache and the informers of the kinds the
// reconciler watches, as the manager does when it starts the controller.
func (e *cachedReadsEnv) startCache(t *testing.T, r *PodMigrationJobReconciler) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = e.mgr.GetCache().Start(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	if !e.mgr.GetCache().WaitForCacheSync(ctx) {
		t.Fatal("cache didn't sync")
	}
	for gvk := range r.invariantCachedGVKs {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		if _, err := e.mgr.GetCache().GetInformer(ctx, u); err != nil {
			t.Fatalf("starting the %s informer: %v", gvk, err)
		}
	}
}

func (e *cachedReadsEnv) createSnapshotObj(t *testing.T, gvk schema.GroupVersionKind, ns, name string, progress time.Time) {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	u.SetNamespace(ns)
	u.SetName(name)
	if err := e.admin.Create(context.Background(), u); err != nil {
		t.Fatalf("creating %s %s: %v", gvk.Kind, name, err)
	}
	if gvk.Kind != "PodSnapshot" {
		return
	}
	u.Object["status"] = map[string]interface{}{"conditions": []interface{}{
		map[string]interface{}{"type": "Checkpoint", "status": "False", "reason": "InProgress", "message": "checkpointing",
			"lastTransitionTime": progress.UTC().Format(time.RFC3339)},
	}}
	if err := e.admin.Status().Update(context.Background(), u); err != nil {
		t.Fatalf("setting the status of PodSnapshot %s: %v", name, err)
	}
}

// createJob creates a job in phase, which started Snapshotting 15 minutes
// ago, past its 10-minute timeout.
func (e *cachedReadsEnv) createJob(t *testing.T, ns, name, engineName string, phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
	t.Helper()
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "pod-" + name}, TargetPodUID: "uid-" + name},
	}
	if engineName != "" {
		job.Annotations = map[string]string{engine.AnnotationEngine: engineName}
	}
	ctx := context.Background()
	if err := e.admin.Create(ctx, job); err != nil {
		t.Fatalf("creating job %s: %v", name, err)
	}
	started := metav1.NewTime(time.Now().Add(-15 * time.Minute))
	job.Status.Phase = phase
	job.Status.SnapshottingStartTime = &started
	job.Status.SnapshotRef = "snap-" + name
	if phase == pmv1alpha1.PodMigrationJobPhaseRestoring {
		restoring := metav1.NewTime(time.Now().Add(-5 * time.Second))
		job.Status.RestoringStartTime = &restoring
	}
	if err := e.admin.Status().Update(ctx, job); err != nil {
		t.Fatalf("setting the status of job %s: %v", name, err)
	}
	return job
}

// waitForCachedJobs waits until the manager's cache has the jobs' statuses.
func (e *cachedReadsEnv) waitForCachedJobs(t *testing.T, ns string, n int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		list := &pmv1alpha1.PodMigrationJobList{}
		if err := e.mgr.GetCache().List(context.Background(), list, client.InNamespace(ns)); err == nil {
			ready := 0
			for _, j := range list.Items {
				if j.Status.Phase != "" {
					ready++
				}
			}
			if ready == n {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("the cache never showed the %d jobs in %s", n, ns)
}

func (e *cachedReadsEnv) reconcile(t *testing.T, r *PodMigrationJobReconciler, ns, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	type result struct {
		res ctrl.Result
		err error
	}
	done := make(chan result, 1)
	go func() {
		res, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
		done <- result{res, err}
	}()
	select {
	case got := <-done:
		if got.err != nil {
			t.Fatalf("Reconcile(%s/%s) = %v; the invariant evaluation only runs after a reconcile without error", ns, name, got.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("Reconcile(%s/%s) is stuck", ns, name)
	}
}

// TestPMJReconcile_StrictInvariantReadsServedFromCache_Envtest checks, against
// a real API server and the manager's real cache, that a PodMigrationJob
// reconcile in strict mode reads no trigger and no PodSnapshot from the API
// server (#199, #231 item 3). Job A is Restoring; peers B and C are still
// Snapshotting past their timeout, so A's invariant evaluation reads A's
// trigger, A's PodSnapshot and both peers' PodSnapshots. Before the fix
// those were four live GETs per reconcile.
func TestPMJReconcile_StrictInvariantReadsServedFromCache_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest cached invariant reads test")
	}
	env, _ := startCachedReadsEnv(t, buildDynamicPodSnapshotCRDs())
	ctx := context.Background()
	ns := createTestNamespace(t, ctx, env.admin, "inv-cache")

	gkeTrigger := snapshot.ManualTriggerGVK(snapshot.DefaultAPIGroup)
	gkeSnap := snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup)
	now := time.Now()
	env.createJob(t, ns, "a", "", pmv1alpha1.PodMigrationJobPhaseRestoring)
	env.createSnapshotObj(t, gkeTrigger, ns, util.FormatPSMTName("pod-a", "uid-a"), now)
	env.createSnapshotObj(t, gkeSnap, ns, "snap-a", now.Add(-time.Minute))
	for _, peer := range []string{"b", "c"} {
		env.createJob(t, ns, peer, "", pmv1alpha1.PodMigrationJobPhaseSnapshotting)
		env.createSnapshotObj(t, gkeSnap, ns, "snap-"+peer, now.Add(-10*time.Second))
	}

	r := &PodMigrationJobReconciler{
		Client:          env.mgr.GetClient(),
		APIReader:       env.mgr.GetAPIReader(),
		Scheme:          env.mgr.GetScheme(),
		InvariantEngine: invariants.NewEngine(invariants.ModeStrict, nil),
	}
	if err := r.SetupWithManager(env.mgr, ctrlcontroller.Options{}); err != nil {
		t.Fatalf("SetupWithManager: %v", err)
	}
	if !r.invariantCachedGVKs[gkeTrigger] || !r.invariantCachedGVKs[gkeSnap] {
		t.Fatalf("SetupWithManager cached kinds = %v, want the GKE trigger and PodSnapshot", r.invariantCachedGVKs)
	}
	env.startCache(t, r)
	env.waitForCachedJobs(t, ns, 3)
	cacheReader := &countingCacheReader{Reader: r.invariantReader}
	r.invariantReader = cacheReader

	// The reconcile's own steps for a Restoring job read no trigger or
	// PodSnapshot, so every GET counted comes from invariant evaluation.
	t.Run("before: evaluation reads through the client", func(t *testing.T) {
		saved := r.invariantReader
		r.invariantReader = nil // as before #199: unstructured Gets through the manager's client
		defer func() { r.invariantReader = saved }()
		env.counter.reset()
		env.reconcile(t, r, ns, "a")
		if n, by := env.counter.snapshot(); n != 4 {
			t.Fatalf("live trigger/PodSnapshot GETs = %d (%v), want 4", n, by)
		}
	})

	t.Run("after: evaluation reads from the cache", func(t *testing.T) {
		env.counter.reset()
		env.reconcile(t, r, ns, "a")
		if n, by := env.counter.snapshot(); n != 0 {
			t.Errorf("live trigger/PodSnapshot GETs = %d (%v), want 0", n, by)
		}
		if cacheReader.reads != 4 {
			t.Errorf("cached trigger/PodSnapshot reads = %d, want 4", cacheReader.reads)
		}
	})
}

// TestPMJReconcile_MissingSnapshotCRDs_Envtest is the kind CRIU cluster of
// #230: the CRIU engine's CRDs are installed, GKE Pod Snapshots' aren't. The
// manager must start, the reconciler must skip the GKE kinds, and a strict
// reconcile whose evaluation would read a GKE PodSnapshot (for a peer that
// GKE Pod Snapshots serves) must finish without error, without waiting on an
// informer that can't sync, and without a live GET.
func TestPMJReconcile_MissingSnapshotCRDs_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest missing CRD test")
	}
	env, scheme := startCachedReadsEnv(t, buildDynamicPodSnapshotCRDsForGroup(engine.CRIUAPIGroup))
	ctx := context.Background()
	ns := createTestNamespace(t, ctx, env.admin, "inv-nocrd")

	criuTrigger := snapshot.ManualTriggerGVK(engine.CRIUAPIGroup)
	criuSnap := snapshot.PodSnapshotGVK(engine.CRIUAPIGroup)
	now := time.Now()
	env.createJob(t, ns, "a", engine.CRIUName, pmv1alpha1.PodMigrationJobPhaseRestoring)
	env.createSnapshotObj(t, criuTrigger, ns, util.FormatPSMTName("pod-a", "uid-a"), now)
	env.createSnapshotObj(t, criuSnap, ns, "snap-a", now.Add(-time.Minute))
	env.createJob(t, ns, "b", engine.GKEPodSnapshotsName, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	env.createJob(t, ns, "c", engine.CRIUName, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
	env.createSnapshotObj(t, criuSnap, ns, "snap-c", now.Add(-10*time.Second))

	r := &PodMigrationJobReconciler{
		Client:          env.mgr.GetClient(),
		APIReader:       env.mgr.GetAPIReader(),
		Scheme:          env.mgr.GetScheme(),
		InvariantEngine: invariants.NewEngine(invariants.ModeStrict, nil),
		Engines:         buildSideBySideControllerRegistry(t, env.mgr.GetClient(), scheme),
	}
	if err := r.SetupWithManager(env.mgr, ctrlcontroller.Options{}); err != nil {
		t.Fatalf("SetupWithManager without the GKE CRDs: %v", err)
	}
	want := map[schema.GroupVersionKind]bool{criuTrigger: true, criuSnap: true}
	if len(r.invariantCachedGVKs) != len(want) || !r.invariantCachedGVKs[criuTrigger] || !r.invariantCachedGVKs[criuSnap] {
		t.Fatalf("SetupWithManager cached kinds = %v, want only the CRIU trigger and PodSnapshot", r.invariantCachedGVKs)
	}
	env.startCache(t, r)
	env.waitForCachedJobs(t, ns, 3)
	cacheReader := &countingCacheReader{Reader: r.invariantReader}
	r.invariantReader = cacheReader

	env.counter.reset()
	env.reconcile(t, r, ns, "a")
	if n, by := env.counter.snapshot(); n != 0 {
		t.Errorf("live trigger/PodSnapshot GETs = %d (%v), want 0", n, by)
	}
	// A's trigger, A's PodSnapshot and C's PodSnapshot; B's GKE PodSnapshot
	// is skipped.
	if cacheReader.reads != 3 {
		t.Errorf("cached trigger/PodSnapshot reads = %d, want 3", cacheReader.reads)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	if err := env.admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: "a"}, job); err != nil {
		t.Fatalf("getting job a: %v", err)
	}
	if job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("job a failed in strict mode: %+v", job.Status.Conditions)
	}
}
