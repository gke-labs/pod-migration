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
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func perEngineGroupTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("clientgoscheme.AddToScheme: %v", err)
	}
	if err := pmv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("pmv1alpha1.AddToScheme: %v", err)
	}
	return s
}

func buildSideBySideControllerRegistry(t *testing.T, cl client.Client, scheme *runtime.Scheme) *engine.Registry {
	t.Helper()
	policy, err := eligibility.ParseRuntimeClassPolicy("gvisor,@default,runc")
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy: %v", err)
	}
	reg, err := engine.NewDefaultRegistry(cl, scheme, policy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	routes, err := engine.ParseRoutes("gvisor=gke-pod-snapshots,@default=criu,runc=criu")
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	if err := reg.SetRoutes(routes); err != nil {
		t.Fatalf("SetRoutes: %v", err)
	}
	return reg
}

func TestPodMigrationReconciler_SideBySideSyncsAndDeletesBothAPIGroups(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"

	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  ns,
			Name:       "dual-mig",
			UID:        types.UID("pm-dual-uid"),
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://dual-bucket/" + ns + "/checkpoints"},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		Build()
	reg := buildSideBySideControllerRegistry(t, cl, scheme)

	rec := &PodMigrationReconciler{
		Client:  cl,
		Scheme:  scheme,
		Engines: reg,
	}

	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: pm.Name}}); err != nil {
		t.Fatalf("Reconcile sync: %v", err)
	}

	// Verify GKE objects exist in podsnapshot.gke.io/v1.
	gkePSSC := &unstructured.Unstructured{}
	gkePSSC.SetGroupVersionKind(snapshot.StorageConfigGVK(engine.DefaultAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Name: engine.PodSnapshotStorageConfigName(ns, pm.Name)}, gkePSSC); err != nil {
		t.Fatalf("expected GKE PSSC in %s: %v", engine.DefaultAPIGroup, err)
	}
	gkePSP := &unstructured.Unstructured{}
	gkePSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.DefaultAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.PodSnapshotPolicyName(pm.Name)}, gkePSP); err != nil {
		t.Fatalf("expected GKE PSP in %s: %v", engine.DefaultAPIGroup, err)
	}

	// Verify CRIU objects exist in criu.podmigration.gke.io/v1 with pod-migrate.io/engine=criu.
	criuPSSC := &unstructured.Unstructured{}
	criuPSSC.SetGroupVersionKind(snapshot.StorageConfigGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Name: engine.CRIUPodSnapshotStorageConfigName(ns, pm.Name)}, criuPSSC); err != nil {
		t.Fatalf("expected CRIU PSSC in %s: %v", engine.CRIUAPIGroup, err)
	}
	if criuPSSC.GetLabels()[engine.CRIUPolicyLabel] != engine.CRIUName {
		t.Errorf("CRIU PSSC labels = %v", criuPSSC.GetLabels())
	}
	criuPSP := &unstructured.Unstructured{}
	criuPSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.CRIUPodSnapshotPolicyName(pm.Name)}, criuPSP); err != nil {
		t.Fatalf("expected CRIU PSP in %s: %v", engine.CRIUAPIGroup, err)
	}
	if criuPSP.GetLabels()[engine.CRIUPolicyLabel] != engine.CRIUName {
		t.Errorf("CRIU PSP labels = %v", criuPSP.GetLabels())
	}

	// Delete the PodMigration and verify finalization cleans up BOTH API groups.
	livePM := &pmv1alpha1.PodMigration{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: pm.Name}, livePM); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	if err := cl.Delete(ctx, livePM); err != nil {
		t.Fatalf("Delete PodMigration: %v", err)
	}
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: pm.Name}}); err != nil {
		t.Fatalf("Reconcile delete: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Name: engine.CRIUPodSnapshotStorageConfigName(ns, pm.Name)}, criuPSSC); !apierrors.IsNotFound(err) {
		t.Errorf("expected CRIU PSSC deleted, got %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.CRIUPodSnapshotPolicyName(pm.Name)}, criuPSP); !apierrors.IsNotFound(err) {
		t.Errorf("expected CRIU PSP deleted, got %v", err)
	}
}

func TestPodMigrationReconciler_AddonOffNoMatchOnGKEGroupFinalizesCRIUCleanly(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"

	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:  ns,
			Name:       "criu-only-mig",
			UID:        types.UID("pm-criu-only-uid"),
			Generation: 1,
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://criu-only-bucket/" + ns + "/checkpoints"},
		},
	}

	noMatchIfGKEGroup := func(obj client.Object) error {
		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Group == engine.DefaultAPIGroup {
			return &meta.NoKindMatchError{
				GroupKind:        schema.GroupKind{Group: gvk.Group, Kind: gvk.Kind},
				SearchedVersions: []string{gvk.Version},
			}
		}
		return nil
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if err := noMatchIfGKEGroup(obj); err != nil {
					return err
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
				if err := noMatchIfGKEGroup(obj); err != nil {
					return err
				}
				return c.Delete(ctx, obj, opts...)
			},
		}).
		Build()

	policy, err := eligibility.ParseRuntimeClassPolicy("@default,runc")
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy: %v", err)
	}
	reg, err := engine.NewDefaultRegistry(cl, scheme, policy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	routes, err := engine.ParseRoutes("@default=criu,runc=criu")
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	if err := reg.SetRoutes(routes); err != nil {
		t.Fatalf("SetRoutes: %v", err)
	}

	recorder := record.NewFakeRecorder(16)
	rec := &PodMigrationReconciler{
		Client:          cl,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: invariants.NewEngine(invariants.ModeStrict, recorder),
		Engines:         reg,
	}

	// 1. Sync creates CRIU PSSC + PSP in criu.podmigration.gke.io without touching absent podsnapshot.gke.io CRDs.
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: pm.Name}}); err != nil {
		t.Fatalf("Reconcile sync on addon-off cluster: %v", err)
	}
	criuPSSC := &unstructured.Unstructured{}
	criuPSSC.SetGroupVersionKind(snapshot.StorageConfigGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Name: engine.CRIUPodSnapshotStorageConfigName(ns, pm.Name)}, criuPSSC); err != nil {
		t.Fatalf("expected CRIU PSSC in %s: %v", engine.CRIUAPIGroup, err)
	}
	criuPSP := &unstructured.Unstructured{}
	criuPSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.CRIUPodSnapshotPolicyName(pm.Name)}, criuPSP); err != nil {
		t.Fatalf("expected CRIU PSP in %s: %v", engine.CRIUAPIGroup, err)
	}

	// 2. Delete PodMigration: deleteStorageResources must ignore NoKindMatchError on podsnapshot.gke.io,
	// proceed to deleteEngineResources for criu.podmigration.gke.io, and remove StorageCleanupFinalizer.
	livePM := &pmv1alpha1.PodMigration{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: pm.Name}, livePM); err != nil {
		t.Fatalf("Get PodMigration: %v", err)
	}
	if err := cl.Delete(ctx, livePM); err != nil {
		t.Fatalf("Delete PodMigration: %v", err)
	}
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: pm.Name}}); err != nil {
		t.Fatalf("Reconcile finalization on addon-off cluster failed: %v", err)
	}

	if err := cl.Get(ctx, types.NamespacedName{Name: engine.CRIUPodSnapshotStorageConfigName(ns, pm.Name)}, criuPSSC); !apierrors.IsNotFound(err) {
		t.Errorf("expected CRIU PSSC deleted on finalization, got %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.CRIUPodSnapshotPolicyName(pm.Name)}, criuPSP); !apierrors.IsNotFound(err) {
		t.Errorf("expected CRIU PSP deleted on finalization, got %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: pm.Name}, livePM); err == nil {
		if controllerutil.ContainsFinalizer(livePM, StorageCleanupFinalizer) {
			t.Fatalf("StorageCleanupFinalizer remained stuck on PodMigration: %v", livePM.Finalizers)
		}
	} else if !apierrors.IsNotFound(err) {
		t.Fatalf("unexpected error checking deleted PodMigration: %v", err)
	}
}

func TestSideBySideCRIUAndGVisorEndToEndReconcileUnderStrictInvariants(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const (
		ns      = "default"
		criuAnn = engine.CRIUAPIGroup + "/ps-name"
		gkeAnn  = engine.DefaultAPIGroup + "/ps-name"
	)
	gvisorClass := "gvisor"

	// Source pods + PMJs for CRIU (@default) and GKE Pod Snapshots (gvisor) side by side.
	criuSource := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "criu-worker-0",
			UID:       "uid-criu-src-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName:   "node-a",
			Containers: []corev1.Container{{Name: "app", Command: []string{"/app"}}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	criuJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              util.FormatPMJName(criuSource.Name, string(criuSource.UID)),
			UID:               "pmj-criu-uid",
			CreationTimestamp: metav1.Now(),
			Annotations:       map[string]string{engine.AnnotationEngine: engine.CRIUName},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: criuSource.Name},
			TargetPodUID: string(criuSource.UID),
		},
	}

	gkeSource := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "gvisor-worker-0",
			UID:       "uid-gke-src-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorClass,
			NodeName:         "node-a",
			Containers:       []corev1.Container{{Name: "app", Command: []string{"/app"}}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
	gkeJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              util.FormatPMJName(gkeSource.Name, string(gkeSource.UID)),
			UID:               "pmj-gke-uid",
			CreationTimestamp: metav1.Now(),
			Annotations:       map[string]string{engine.AnnotationEngine: engine.GKEPodSnapshotsName},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: gkeSource.Name},
			TargetPodUID: string(gkeSource.UID),
		},
	}

	recorder := record.NewFakeRecorder(64)
	strictInvariants := invariants.NewEngine(invariants.ModeStrict, recorder)

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(criuSource, criuJob, gkeSource, gkeJob).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}, &corev1.Pod{}).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, func(o client.Object) []string {
			p := o.(*corev1.Pod)
			if v := p.Annotations[util.AnnotationAssignedPMJ]; v != "" {
				return []string{v}
			}
			return nil
		}).
		WithIndex(&corev1.Event{}, "involvedObject.uid", func(o client.Object) []string {
			ev := o.(*corev1.Event)
			return []string{string(ev.InvolvedObject.UID)}
		}).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, func(o client.Object) []string {
			j := o.(*pmv1alpha1.PodMigrationJob)
			if j.Status.SnapshotRef != "" {
				return []string{j.Status.SnapshotRef}
			}
			return nil
		}).
		Build()

	reg := buildSideBySideControllerRegistry(t, cl, scheme)

	pmjRec := &PodMigrationJobReconciler{
		Client:                  cl,
		APIReader:               cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 2 * time.Minute,
		InvariantEngine:         strictInvariants,
		Engines:                 reg,
	}
	gateRec := &PodGateReconciler{
		Client:          cl,
		APIReader:       cl,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: strictInvariants,
		Engines:         reg,
	}

	reconcileJob := func(name string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
			t.Fatalf("Reconcile(%s): %v", name, err)
		}
		got := &pmv1alpha1.PodMigrationJob{}
		if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, got); err != nil {
			t.Fatalf("Get(%s): %v", name, err)
		}
		if cond := meta.FindStatusCondition(got.Status.Conditions, "Ready"); cond != nil && cond.Reason == invariants.EventReasonInvariantViolation {
			t.Fatalf("strict invariant violation on %s: %s", name, cond.Message)
		}
		return got
	}

	// Step 1: Advance CRIU job Pending -> Snapshotting -> creates trigger in criu.podmigration.gke.io.
	reconcileJob(criuJob.Name) // adds fence finalizer
	reconcileJob(criuJob.Name) // Pending -> Snapshotting
	reconcileJob(criuJob.Name) // fences source + creates PSMT in criu.podmigration.gke.io

	criuTriggerName := util.FormatPSMTName(criuSource.Name, string(criuSource.UID))
	criuTrigger := &unstructured.Unstructured{}
	criuTrigger.SetGroupVersionKind(snapshot.ManualTriggerGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: criuTriggerName}, criuTrigger); err != nil {
		t.Fatalf("expected CRIU trigger in %s: %v", engine.CRIUAPIGroup, err)
	}

	// Simulate CRIU engine completing the snapshot in criu.podmigration.gke.io.
	const criuSnapName = "ps-criu-worker-0-snap"
	_ = unstructured.SetNestedField(criuTrigger.Object, criuSnapName, "status", "snapshotCreated", "name")
	if err := cl.Update(ctx, criuTrigger); err != nil {
		t.Fatalf("Update CRIU trigger: %v", err)
	}
	criuSnap := &unstructured.Unstructured{}
	criuSnap.SetGroupVersionKind(snapshot.PodSnapshotGVK(engine.CRIUAPIGroup))
	criuSnap.SetNamespace(ns)
	criuSnap.SetName(criuSnapName)
	criuSnap.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":               "Ready",
				"status":             "True",
				"reason":             "Succeeded",
				"lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	}
	if err := cl.Create(ctx, criuSnap); err != nil {
		t.Fatalf("Create CRIU PodSnapshot: %v", err)
	}

	// Step 2: Reconcile CRIU job -> Evicting; replace source with gated replacement pod; Reconcile -> Restoring.
	gotCRIU := reconcileJob(criuJob.Name)
	if gotCRIU.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting || gotCRIU.Status.SnapshotRef != criuSnapName {
		t.Fatalf("CRIU job = phase:%s snap:%s, want Evicting/%s", gotCRIU.Status.Phase, gotCRIU.Status.SnapshotRef, criuSnapName)
	}
	if err := cl.Delete(ctx, criuSource); err != nil {
		t.Fatalf("Delete CRIU source: %v", err)
	}
	criuRepl := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "criu-worker-0",
			UID:       "uid-criu-repl-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ:    criuJob.Name,
				engine.CRIUInjectedAnnotation: "true",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			Containers:      []corev1.Container{{Name: "app", Command: []string{"/pmhold/holder", "wrap"}}},
		},
	}
	if err := cl.Create(ctx, criuRepl); err != nil {
		t.Fatalf("Create CRIU replacement: %v", err)
	}
	gotCRIU = reconcileJob(criuJob.Name)
	if gotCRIU.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Fatalf("CRIU job phase = %s, want Restoring", gotCRIU.Status.Phase)
	}

	// Step 3: PodGateReconciler releases gate and injects criu.podmigration.gke.io/ps-name (NOT podsnapshot.gke.io/ps-name).
	if _, err := gateRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: criuRepl.Name}}); err != nil {
		t.Fatalf("PodGateReconciler CRIU: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: criuRepl.Name}, criuRepl); err != nil {
		t.Fatalf("Get CRIU replacement: %v", err)
	}
	if len(criuRepl.Spec.SchedulingGates) != 0 {
		t.Fatalf("expected scheduling gate removed, got %v", criuRepl.Spec.SchedulingGates)
	}
	if criuRepl.Annotations[criuAnn] != criuSnapName {
		t.Fatalf("replacement %s = %q, want %q", criuAnn, criuRepl.Annotations[criuAnn], criuSnapName)
	}
	if _, exists := criuRepl.Annotations[gkeAnn]; exists {
		t.Fatalf("CRIU replacement must not carry %s, got %q", gkeAnn, criuRepl.Annotations[gkeAnn])
	}

	// Step 4: Node agent stamps psengine-restore=restored and pod becomes Ready; CRIU job concludes Succeeded (RestoreVerified).
	criuRepl.Annotations[engine.CRIURestoreStatusAnnotation] = engine.CRIURestoreStatusRestored
	if err := cl.Update(ctx, criuRepl); err != nil {
		t.Fatalf("Update CRIU replacement outcome: %v", err)
	}
	criuRepl.Status.Phase = corev1.PodRunning
	criuRepl.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}
	if err := cl.Status().Update(ctx, criuRepl); err != nil {
		t.Fatalf("Update CRIU replacement status: %v", err)
	}

	gotCRIU = reconcileJob(criuJob.Name)
	if gotCRIU.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Fatalf("CRIU job phase = %s, want Succeeded (conditions: %+v)", gotCRIU.Status.Phase, gotCRIU.Status.Conditions)
	}
	restoredCond := meta.FindStatusCondition(gotCRIU.Status.Conditions, "Restored")
	if restoredCond == nil || restoredCond.Status != metav1.ConditionTrue || restoredCond.Reason != ReasonRestoreVerified {
		t.Fatalf("CRIU Restored condition = %+v, want True/%s", restoredCond, ReasonRestoreVerified)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: criuRepl.Name}, criuRepl); err != nil {
		t.Fatalf("Get CRIU replacement after completion: %v", err)
	}
	if v, ok := criuRepl.Annotations[criuAnn]; !ok || v != "" {
		t.Errorf("CompleteRestore did not set %s=\"\": got %q (ok=%t)", criuAnn, v, ok)
	}
	if criuRepl.Annotations[util.AnnotationRestoreCompleted] != criuSnapName {
		t.Errorf("restore-completed = %q, want %q", criuRepl.Annotations[util.AnnotationRestoreCompleted], criuSnapName)
	}

	// Step 5: Verify gVisor replacement wrapped by CRIU webhook (psengine-injected=true)
	// gets BOTH podsnapshot.gke.io/ps-name=<gke-snap> AND criu.podmigration.gke.io/ps-name=""
	// so the CRIU holder immediately falls through without stalling.
	reconcileJob(gkeJob.Name) // finalizer
	reconcileJob(gkeJob.Name) // Pending -> Snapshotting
	reconcileJob(gkeJob.Name) // fence + trigger in podsnapshot.gke.io

	gkeTriggerName := util.FormatPSMTName(gkeSource.Name, string(gkeSource.UID))
	gkeTrigger := &unstructured.Unstructured{}
	gkeTrigger.SetGroupVersionKind(snapshot.ManualTriggerGVK(engine.DefaultAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: gkeTriggerName}, gkeTrigger); err != nil {
		t.Fatalf("expected GKE trigger in %s: %v", engine.DefaultAPIGroup, err)
	}
	const gkeSnapName = "ps-gvisor-worker-0-snap"
	_ = unstructured.SetNestedField(gkeTrigger.Object, gkeSnapName, "status", "snapshotCreated", "name")
	_ = cl.Update(ctx, gkeTrigger)

	gkeSnap := &unstructured.Unstructured{}
	gkeSnap.SetGroupVersionKind(snapshot.PodSnapshotGVK(engine.DefaultAPIGroup))
	gkeSnap.SetNamespace(ns)
	gkeSnap.SetName(gkeSnapName)
	gkeSnap.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":               "Ready",
				"status":             "True",
				"reason":             "Succeeded",
				"lastTransitionTime": time.Now().UTC().Format(time.RFC3339Nano),
			},
		},
	}
	_ = cl.Create(ctx, gkeSnap)

	reconcileJob(gkeJob.Name) // Snapshotting -> Evicting
	_ = cl.Delete(ctx, gkeSource)

	gkeRepl := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "gvisor-worker-0",
			UID:       "uid-gke-repl-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ:    gkeJob.Name,
				engine.CRIUInjectedAnnotation: "true", // speculatively injected by CRIU webhook in Step 2
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorClass,
			SchedulingGates:  []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			Containers:       []corev1.Container{{Name: "app"}},
		},
	}
	_ = cl.Create(ctx, gkeRepl)
	reconcileJob(gkeJob.Name) // Evicting -> Restoring

	if _, err := gateRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: gkeRepl.Name}}); err != nil {
		t.Fatalf("PodGateReconciler GKE: %v", err)
	}
	_ = cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: gkeRepl.Name}, gkeRepl)
	if gkeRepl.Annotations[gkeAnn] != gkeSnapName {
		t.Errorf("GKE replacement %s = %q, want %q", gkeAnn, gkeRepl.Annotations[gkeAnn], gkeSnapName)
	}
	if v, ok := gkeRepl.Annotations[criuAnn]; !ok || v != "" {
		t.Errorf("CRIU-injected GKE replacement %s = %q (ok=%t), want explicit bypass \"\"", criuAnn, v, ok)
	}
}

func TestEvaluateInvariantsAndSnapshotMapper_PeerGroupIsolation(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"

	now := time.Now()
	pastNominal := metav1.NewTime(now.Add(-3 * time.Minute))

	// Primary GKE job (completed cleanly) reconciled while a peer CRIU job in the
	// same namespace is past its nominal 2m timeout in Snapshotting, with a fresh
	// Checkpoint=False/InProgress transition on its PodSnapshot in criu.podmigration.gke.io.
	primaryJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ns,
			Name:        "pmj-primary-gke",
			Annotations: map[string]string{engine.AnnotationEngine: engine.GKEPodSnapshotsName},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "primary-pod"},
			TargetPodUID: "uid-primary",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			SnapshotRef: "shared-snap-name",
		},
	}
	peerCRIUJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         ns,
			Name:              "pmj-peer-criu",
			CreationTimestamp: pastNominal,
			Annotations: map[string]string{
				engine.AnnotationEngine:         engine.CRIUName,
				util.AnnotationMigrationTimeout: "2m",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "peer-criu-pod"},
			TargetPodUID: "uid-peer-criu",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshottingStartTime: &pastNominal,
			SnapshotRef:           "shared-snap-name",
		},
	}

	criuSnap := &unstructured.Unstructured{}
	criuSnap.SetGroupVersionKind(snapshot.PodSnapshotGVK(engine.CRIUAPIGroup))
	criuSnap.SetNamespace(ns)
	criuSnap.SetName("shared-snap-name")
	criuSnap.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":               "Checkpoint",
				"status":             "False",
				"reason":             "InProgress",
				"lastTransitionTime": now.Add(-10 * time.Second).UTC().Format(time.RFC3339Nano),
			},
		},
	}

	recorder := record.NewFakeRecorder(16)
	strictInvariants := invariants.NewEngine(invariants.ModeStrict, recorder)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(primaryJob, peerCRIUJob, criuSnap).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, func(o client.Object) []string {
			j := o.(*pmv1alpha1.PodMigrationJob)
			if j.Status.SnapshotRef != "" {
				return []string{j.Status.SnapshotRef}
			}
			return nil
		}).
		Build()

	reg := buildSideBySideControllerRegistry(t, cl, scheme)
	r := &PodMigrationJobReconciler{
		Client:                  cl,
		APIReader:               cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 2 * time.Minute,
		InvariantEngine:         strictInvariants,
		Engines:                 reg,
	}

	var res ctrl.Result
	var recErr error
	r.evaluateInvariantsWithSnapshot(ctx, primaryJob, nil, &res, &recErr)
	select {
	case ev := <-recorder.Events:
		t.Fatalf("unexpected invariant violation event (peer otherSnapObj lookup missed CRIU group?): %s", ev)
	default:
	}

	// Verify mapSnapshotToPMJ only enqueues the job whose engine serves criuSnap's API group.
	reqs := r.mapSnapshotToPMJ(ctx, criuSnap)
	want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: ns, Name: peerCRIUJob.Name}}}
	if !reflect.DeepEqual(reqs, want) {
		t.Fatalf("mapSnapshotToPMJ(criuSnap) = %v, want %v", reqs, want)
	}
}

func TestPodGateReconciler_GVisorRoutedToCRIUStampsGKEColdStartBypass(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"
	gvisorClass := "gvisor"
	gkeKey := engine.RestoreAnnotation(engine.DefaultAPIGroup)
	criuKey := engine.RestoreAnnotation(engine.CRIUAPIGroup)

	allowPolicy, err := eligibility.ParseRuntimeClassPolicy("gvisor")
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy: %v", err)
	}

	t.Run("restore release on gvisor=criu stamps both CRIU snapshotRef and GKE empty bypass", func(t *testing.T) {
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        "pmj-gvisor-criu-restore",
				Annotations: map[string]string{engine.AnnotationEngine: engine.CRIUName},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "src-pod"},
				TargetPodUID: "src-uid",
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
				SnapshotRef: "ps-criu-snap-1",
			},
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        "repl-pod-restore",
				UID:         "repl-uid-1",
				Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: job.Name},
			},
			Spec: corev1.PodSpec{
				RuntimeClassName: &gvisorClass,
				SchedulingGates:  []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				Containers:       []corev1.Container{{Name: "app"}},
			},
		}
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			WithObjects(withReadySnapshots([]client.Object{job, pod}, engine.CRIUAPIGroup)...).
			Build()
		reg, err := engine.NewDefaultRegistry(cl, scheme, allowPolicy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
		if err != nil {
			t.Fatalf("NewDefaultRegistry: %v", err)
		}
		routes, _ := engine.ParseRoutes("gvisor=criu")
		if err := reg.SetRoutes(routes); err != nil {
			t.Fatalf("SetRoutes: %v", err)
		}

		rec := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme, Engines: reg}
		if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pod)}); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		var got corev1.Pod
		if err := cl.Get(ctx, client.ObjectKeyFromObject(pod), &got); err != nil {
			t.Fatalf("Get pod: %v", err)
		}
		if got.Annotations[criuKey] != "ps-criu-snap-1" {
			t.Errorf("%s = %q, want ps-criu-snap-1", criuKey, got.Annotations[criuKey])
		}
		if v, ok := got.Annotations[gkeKey]; !ok || v != "" {
			t.Errorf("%s = %q (ok=%t), want explicit empty GKE cold-start bypass", gkeKey, v, ok)
		}
	})

	t.Run("cold-start bypass on gvisor=criu and speculatively CRIU-injected gVisor job stamps both empty keys", func(t *testing.T) {
		failedCRIUJob := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        "pmj-gvisor-criu-failed",
				Annotations: map[string]string{engine.AnnotationEngine: engine.CRIUName},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseFailed,
			},
		}
		podRoutedCRIU := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        "repl-pod-bypass-criu",
				UID:         "repl-uid-2",
				Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: failedCRIUJob.Name},
			},
			Spec: corev1.PodSpec{
				RuntimeClassName: &gvisorClass,
				SchedulingGates:  []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				Containers:       []corev1.Container{{Name: "app"}},
			},
		}

		failedGKEJob := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        "pmj-gvisor-gke-failed",
				Annotations: map[string]string{engine.AnnotationEngine: engine.GKEPodSnapshotsName},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseFailed,
			},
		}
		podSpeculativelyInjected := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "repl-pod-speculative-criu",
				UID:       "repl-uid-3",
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ:    failedGKEJob.Name,
					engine.CRIUInjectedAnnotation: "true",
				},
			},
			Spec: corev1.PodSpec{
				RuntimeClassName: &gvisorClass,
				SchedulingGates:  []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				Containers:       []corev1.Container{{Name: "app"}},
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			WithObjects(failedCRIUJob, podRoutedCRIU, failedGKEJob, podSpeculativelyInjected).
			Build()

		// Case 1: gvisor=criu routed job fails -> both criuKey="" and gkeKey="" must be stamped.
		regCRIU, err := engine.NewDefaultRegistry(cl, scheme, allowPolicy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
		if err != nil {
			t.Fatalf("NewDefaultRegistry: %v", err)
		}
		routesCRIU, _ := engine.ParseRoutes("gvisor=criu")
		_ = regCRIU.SetRoutes(routesCRIU)
		recCRIU := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme, Engines: regCRIU}
		if _, err := recCRIU.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(podRoutedCRIU)}); err != nil {
			t.Fatalf("Reconcile(podRoutedCRIU): %v", err)
		}
		var gotRouted corev1.Pod
		_ = cl.Get(ctx, client.ObjectKeyFromObject(podRoutedCRIU), &gotRouted)
		if v, ok := gotRouted.Annotations[criuKey]; !ok || v != "" {
			t.Errorf("podRoutedCRIU %s = %q (ok=%t), want empty bypass", criuKey, v, ok)
		}
		if v, ok := gotRouted.Annotations[gkeKey]; !ok || v != "" {
			t.Errorf("podRoutedCRIU %s = %q (ok=%t), want empty GKE bypass", gkeKey, v, ok)
		}

		// Case 2: gvisor=gke-pod-snapshots job fails on a pod carrying psengine-injected=true ->
		// releaseWithColdStartBypass must ALSO stamp criuKey="" via the CRIUInjectedAnnotation branch.
		regGKE, err := engine.NewDefaultRegistry(cl, scheme, allowPolicy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
		if err != nil {
			t.Fatalf("NewDefaultRegistry: %v", err)
		}
		recGKE := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme, Engines: regGKE}
		if _, err := recGKE.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(podSpeculativelyInjected)}); err != nil {
			t.Fatalf("Reconcile(podSpeculativelyInjected): %v", err)
		}
		var gotSpec corev1.Pod
		_ = cl.Get(ctx, client.ObjectKeyFromObject(podSpeculativelyInjected), &gotSpec)
		if v, ok := gotSpec.Annotations[gkeKey]; !ok || v != "" {
			t.Errorf("podSpeculativelyInjected %s = %q (ok=%t), want empty GKE bypass", gkeKey, v, ok)
		}
		if v, ok := gotSpec.Annotations[criuKey]; !ok || v != "" {
			t.Errorf("podSpeculativelyInjected %s = %q (ok=%t), want empty CRIU bypass", criuKey, v, ok)
		}
	})
}

func TestFindLatestReadyManualStopPSP_AllowsUserPSPLabeledGKEAndExcludesCRIU(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"

	makePSP := func(name, engineLabel string) *unstructured.Unstructured {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(snapshot.PolicyGVK(engine.DefaultAPIGroup))
		u.SetNamespace(ns)
		u.SetName(name)
		if engineLabel != "" {
			u.SetLabels(map[string]string{util.LabelSnapshotEngine: engineLabel})
		}
		u.Object["spec"] = map[string]interface{}{
			"selector":      map[string]interface{}{"matchLabels": map[string]interface{}{"app": "demo"}},
			"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
		}
		u.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}},
		}
		return u
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(
			makePSP("psp-user-gke-labeled", engine.GKEPodSnapshotsName),
			makePSP("psp-criu-labeled", engine.CRIUName),
		).
		Build()

	got, err := util.FindLatestReadyManualStopPSP(ctx, cl, ns, map[string]string{"app": "demo"})
	if err != nil || got == nil {
		t.Fatalf("FindLatestReadyManualStopPSP = %v, %v; want psp-user-gke-labeled", got, err)
	}
	if got.GetName() != "psp-user-gke-labeled" {
		t.Errorf("FindLatestReadyManualStopPSP picked %q, want psp-user-gke-labeled", got.GetName())
	}
}

func TestPodMigrationReconciler_SkipsUnallowedRuntimeClassesAtRenderTime(t *testing.T) {
	ctx := context.Background()
	scheme := perEngineGroupTestScheme(t)
	const ns = "default"

	gvisorOnly, err := eligibility.ParseRuntimeClassPolicy("gvisor")
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy: %v", err)
	}
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ns,
			Name:        "pm",
			UID:         "pm-uid-1",
			Annotations: map[string]string{engine.AnnotationRuntimeClassEngines: "runc=criu"},
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/" + ns + "/snaps"},
		},
	}
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigration{}).
		WithObjects(pm).
		Build()

	reg, err := engine.NewDefaultRegistry(cl, scheme, gvisorOnly, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	rec := &PodMigrationReconciler{
		Client:             cl,
		Scheme:             scheme,
		Engines:            reg,
		RuntimeClassPolicy: gvisorOnly,
	}
	if _, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(pm)}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	criuPSP := &unstructured.Unstructured{}
	criuPSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.CRIUAPIGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: engine.CRIUPodSnapshotPolicyName("pm")}, criuPSP); !apierrors.IsNotFound(err) {
		t.Fatalf("expected CRIU PSP not rendered when runc is excluded by --migratable-runtime-classes=gvisor, got err=%v", err)
	}
}
