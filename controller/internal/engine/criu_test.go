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

package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

func TestCRIU_IdentityAndGroupValidation(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(testScheme()).Build()

	defEngine, err := NewCRIU(cl, testScheme(), "")
	if err != nil {
		t.Fatalf("NewCRIU default group: %v", err)
	}
	if got := defEngine.Name(); got != CRIUName {
		t.Errorf("Name() = %q, want %q", got, CRIUName)
	}
	if got := defEngine.APIGroup(); got != DefaultCRIUAPIGroup {
		t.Errorf("APIGroup() = %q, want %q", got, DefaultCRIUAPIGroup)
	}
	if got := defEngine.RestoreAnnotation(); got != "criu.podmigration.gke.io/ps-name" {
		t.Errorf("RestoreAnnotation() = %q, want criu.podmigration.gke.io/ps-name", got)
	}
	if got := defEngine.RuntimeClasses(); !reflect.DeepEqual(got, []string{eligibility.DefaultRuntimeClassToken, "runc"}) {
		t.Errorf("RuntimeClasses() = %v, want [@default runc]", got)
	}
	if got := defEngine.RestoreFailures(); got != nil {
		t.Errorf("RestoreFailures() = %v, want nil", got)
	}
	if got, want := defEngine.Guarantees(), (Guarantees{Fencing: FencingStopped, RestoreOnce: false}); got != want {
		t.Errorf("Guarantees() = %+v, want %+v", got, want)
	}

	legacyGroup := DefaultAPIGroup
	custom, err := NewCRIU(cl, testScheme(), legacyGroup)
	if err != nil {
		t.Fatalf("NewCRIU(%q): %v", legacyGroup, err)
	}
	if got := custom.APIGroup(); got != legacyGroup {
		t.Errorf("APIGroup() = %q, want %q", got, legacyGroup)
	}
	if got := custom.RestoreAnnotation(); got != legacyGroup+"/ps-name" {
		t.Errorf("RestoreAnnotation() = %q, want %s/ps-name", got, legacyGroup)
	}

	if _, err := NewCRIU(cl, testScheme(), "invalid_group"); err == nil {
		t.Fatal("expected NewCRIU to reject invalid group")
	}
	if _, err := NewDefaultRegistry(cl, testScheme(), nil, WithCRIUAPIGroup("nodot")); err == nil {
		t.Fatal("expected NewDefaultRegistry to reject invalid CRIU API group")
	}
}

func TestCRIU_Eligible(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	always := corev1.ContainerRestartPolicyAlways
	readyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-ready",
			Annotations: map[string]string{CRIUNodeReadyAnnotation: "true"},
		},
	}
	unreadyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-unready"},
	}

	singlePod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName:       readyNode.Name,
			InitContainers: []corev1.Container{{Name: "init-once"}},
			Containers:     []corev1.Container{{Name: "app"}},
		},
	}
	unscheduledPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app"}},
		},
	}
	multiPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName:   readyNode.Name,
			Containers: []corev1.Container{{Name: "app"}, {Name: "sidecar"}},
		},
	}
	nativeSidecarPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName:       readyNode.Name,
			InitContainers: []corev1.Container{{Name: "istio-proxy", RestartPolicy: &always}},
			Containers:     []corev1.Container{{Name: "app"}},
		},
	}

	if v := e.Eligible(nil, readyNode); v.OK {
		t.Errorf("Eligible(nil) = %+v, want refused", v)
	}
	if v := e.Eligible(singlePod, readyNode); !v.OK {
		t.Errorf("Eligible(singlePod, readyNode) = %+v, want OK", v)
	}
	if v := e.Eligible(singlePod, nil); v.OK {
		t.Errorf("Eligible(singlePod with NodeName, nil node) = %+v, want refused (fail-closed)", v)
	}
	if v := e.Eligible(unscheduledPod, nil); v.OK {
		t.Errorf("Eligible(unscheduledPod, nil node) = %+v, want refused (fail-closed)", v)
	}
	if v := e.Eligible(singlePod, unreadyNode); v.OK {
		t.Errorf("Eligible(singlePod, unreadyNode) = %+v, want refused", v)
	}
	if v := e.Eligible(multiPod, readyNode); v.OK {
		t.Errorf("Eligible(multiPod, readyNode) = %+v, want refused", v)
	}
	if v := e.Eligible(nativeSidecarPod, readyNode); v.OK {
		t.Errorf("Eligible(nativeSidecarPod, readyNode) = %+v, want refused", v)
	}
}

func TestCRIU_NamespaceResourcesAndPolicyPartitioning(t *testing.T) {
	ctx := context.Background()
	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "mig",
			UID:       types.UID("pm-uid-1"),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://bucket/prefix"},
		},
	}

	criuCustom, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	objs, err := criuCustom.NamespaceResources(pm)
	if err != nil || len(objs) != 2 {
		t.Fatalf("NamespaceResources = %v, %v; want 2 objects", objs, err)
	}
	if got, want := objs[0].GroupVersionKind(), snapshot.StorageConfigGVK(CRIUAPIGroup); got != want {
		t.Errorf("storageConfig GVK = %v, want %v", got, want)
	}
	if objs[0].GetLabels()[CRIUPolicyLabel] != CRIUName || objs[0].GetLabels()[OwnerUIDLabel] != "pm-uid-1" {
		t.Errorf("storageConfig labels = %v", objs[0].GetLabels())
	}
	if got, want := objs[1].GroupVersionKind(), snapshot.PolicyGVK(CRIUAPIGroup); got != want {
		t.Errorf("policy GVK = %v, want %v", got, want)
	}
	if objs[1].GetLabels()[CRIUPolicyLabel] != CRIUName {
		t.Errorf("policy labels = %v", objs[1].GetLabels())
	}

	// Verify policy partitioning even when GKE and CRIU share podsnapshot.gke.io:
	// GKE must only select unlabeled policies and CRIU must only select pod-migrate.io/engine=criu.
	markReady := func(u *unstructured.Unstructured) *unstructured.Unstructured {
		u.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{"type": "Ready", "status": "True"},
			},
		}
		return u
	}
	gkeTemp := NewGKEPodSnapshots(nil, nil)
	gkeRes, _ := gkeTemp.NamespaceResources(pm)
	criuSameTemp, _ := NewCRIU(nil, nil, DefaultAPIGroup)
	criuSameRes, _ := criuSameTemp.NamespaceResources(pm)

	cl := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(markReady(gkeRes[1]), markReady(criuSameRes[1]), markReady(objs[1])).
		Build()

	gke := NewGKEPodSnapshots(cl, testScheme())
	criuSame, _ := NewCRIU(cl, testScheme(), DefaultAPIGroup)
	criuDedicated, _ := NewCRIU(cl, testScheme(), CRIUAPIGroup)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "p",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
	}

	gkePol, err := gke.ResolvePolicy(ctx, pod)
	if err != nil || gkePol == nil || gkePol.Group != DefaultAPIGroup || gkePol.Name != PodSnapshotPolicyName("mig") {
		t.Fatalf("GKE ResolvePolicy = %+v, %v; want %s/%s", gkePol, err, DefaultAPIGroup, PodSnapshotPolicyName("mig"))
	}
	criuSamePol, err := criuSame.ResolvePolicy(ctx, pod)
	if err != nil || criuSamePol == nil || criuSamePol.Group != DefaultAPIGroup || criuSamePol.Name != CRIUPodSnapshotPolicyName("mig") {
		t.Fatalf("CRIU (same group) ResolvePolicy = %+v, %v; want %s/%s", criuSamePol, err, DefaultAPIGroup, CRIUPodSnapshotPolicyName("mig"))
	}
	criuDedPol, err := criuDedicated.ResolvePolicy(ctx, pod)
	if err != nil || criuDedPol == nil || criuDedPol.Group != CRIUAPIGroup || criuDedPol.Name != CRIUPodSnapshotPolicyName("mig") {
		t.Fatalf("CRIU (dedicated group) ResolvePolicy = %+v, %v; want %s/%s", criuDedPol, err, CRIUAPIGroup, CRIUPodSnapshotPolicyName("mig"))
	}
}

func TestCRIU_RestoreOrdersAndVerifyRestore(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	wantKey := CRIUAPIGroup + "/ps-name"

	pod := &corev1.Pod{}
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-1"},
		Status:     pmv1alpha1.PodMigrationJobStatus{SnapshotRef: "ps-app-1234"},
	}
	if err := e.OrderRestore(pod, job); err != nil {
		t.Fatalf("OrderRestore: %v", err)
	}
	if got := pod.Annotations[wantKey]; got != "ps-app-1234" {
		t.Fatalf("OrderRestore set %s = %q, want ps-app-1234", wantKey, got)
	}
	if snapRef, bypass, ok := e.InspectRestoreOrder(pod); !ok || bypass || snapRef != "ps-app-1234" {
		t.Fatalf("InspectRestoreOrder = (%q, %t, %t), want (ps-app-1234, false, true)", snapRef, bypass, ok)
	}
	if err := e.OrderColdStartBypass(pod); err != nil {
		t.Fatalf("OrderColdStartBypass: %v", err)
	}
	if snapRef, bypass, ok := e.InspectRestoreOrder(pod); !ok || !bypass || snapRef != "" {
		t.Fatalf("InspectRestoreOrder after bypass = (%q, %t, %t), want (\"\", true, true)", snapRef, bypass, ok)
	}

	readyCond := []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}

	// Injected + Ready without psengine-restore yet -> still RestoreInProgress (holder is Ready while node agent restores).
	inProg, err := e.VerifyRestore(context.Background(), job, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{CRIUInjectedAnnotation: "true"}},
		Status:     corev1.PodStatus{Conditions: readyCond},
	})
	if err != nil || inProg.Outcome != RestoreInProgress {
		t.Fatalf("VerifyRestore(injected, no outcome yet) = %+v, %v; want RestoreInProgress", inProg, err)
	}

	// Restored + Ready -> Confirmed Restored.
	restored, err := e.VerifyRestore(context.Background(), job, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIUInjectedAnnotation:      "true",
			CRIURestoreStatusAnnotation: CRIURestoreStatusRestored,
		}},
		Status: corev1.PodStatus{Conditions: readyCond},
	})
	if err != nil || restored.Outcome != Restored || !restored.Confirmed {
		t.Fatalf("VerifyRestore(restored+ready) = %+v, %v; want Confirmed Restored", restored, err)
	}

	// Failed -> ColdStart even before Ready.
	failed, err := e.VerifyRestore(context.Background(), job, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIUInjectedAnnotation:      "true",
			CRIURestoreStatusAnnotation: CRIURestoreStatusFailed,
		}},
	})
	if err != nil || failed.Outcome != ColdStart {
		t.Fatalf("VerifyRestore(failed) = %+v, %v; want ColdStart", failed, err)
	}

	// Uninjected + Ready -> ColdStart (even if psengine-restore=restored was spoofed!).
	uninjected, err := e.VerifyRestore(context.Background(), job, &corev1.Pod{
		Status: corev1.PodStatus{Conditions: readyCond},
	})
	if err != nil || uninjected.Outcome != ColdStart {
		t.Fatalf("VerifyRestore(uninjected+ready) = %+v, %v; want ColdStart", uninjected, err)
	}
	spoofedRestoredWithoutInjected, err := e.VerifyRestore(context.Background(), job, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIURestoreStatusAnnotation: CRIURestoreStatusRestored,
		}},
		Status: corev1.PodStatus{Conditions: readyCond},
	})
	if err != nil || spoofedRestoredWithoutInjected.Outcome != ColdStart || spoofedRestoredWithoutInjected.Confirmed {
		t.Fatalf("VerifyRestore(psengine-restore=restored without psengine-injected=true) = %+v, %v; want unconfirmed ColdStart", spoofedRestoredWithoutInjected, err)
	}
}

func TestRegistry_SideBySideGroupsAndRestoreAnnotations(t *testing.T) {
	cl := fake.NewClientBuilder().WithScheme(testScheme()).Build()

	// Stock default registry (without WithCRIUAPIGroup) registers ONLY gke-pod-snapshots,
	// so a PodMigration annotated runc=criu gets ErrEngineNotRegistered as on main.
	stockReg, err := NewDefaultRegistry(cl, testScheme(), nil)
	if err != nil {
		t.Fatalf("NewDefaultRegistry stock: %v", err)
	}
	if got := stockReg.Names(); !reflect.DeepEqual(got, []string{GKEPodSnapshotsName}) {
		t.Fatalf("stock Names() = %v, want [%s]", got, GKEPodSnapshotsName)
	}
	pmAnnotatedCRIU := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "pm",
			Annotations: map[string]string{AnnotationRuntimeClassEngines: "runc=criu"},
		},
	}
	if _, err := stockReg.ValidatePodMigrationRoutes(pmAnnotatedCRIU); !errors.Is(err, ErrEngineNotRegistered) {
		t.Fatalf("stock ValidatePodMigrationRoutes(runc=criu) = %v, want ErrEngineNotRegistered", err)
	}

	policy := mustParsePolicy(t, "gvisor,@default,runc")
	reg, err := NewDefaultRegistry(cl, testScheme(), policy, WithCRIUAPIGroup(CRIUAPIGroup))
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	routes, err := ParseRoutes("gvisor=gke-pod-snapshots,@default=criu,runc=criu")
	if err != nil {
		t.Fatalf("ParseRoutes: %v", err)
	}
	if err := reg.SetRoutes(routes); err != nil {
		t.Fatalf("SetRoutes: %v", err)
	}

	wantGroups := []string{DefaultAPIGroup, CRIUAPIGroup}
	if got := reg.APIGroups(); !reflect.DeepEqual(got, wantGroups) {
		t.Errorf("APIGroups() = %v, want %v", got, wantGroups)
	}
	wantAnns := []string{DefaultAPIGroup + "/ps-name", CRIUAPIGroup + "/ps-name"}
	if got := reg.RestoreAnnotations(); !reflect.DeepEqual(got, wantAnns) {
		t.Errorf("RestoreAnnotations() = %v, want %v", got, wantAnns)
	}

	// EnginesForAllowed respects --migratable-runtime-classes: when only gvisor is allowed,
	// a runc=criu route does not render CRIU namespace resources.
	gvisorOnlyAllow := mustParsePolicy(t, "gvisor")
	allowedEngines := reg.EnginesForAllowed(pmAnnotatedCRIU, gvisorOnlyAllow)
	if len(allowedEngines) != 1 || allowedEngines[0].Name() != GKEPodSnapshotsName {
		t.Fatalf("EnginesForAllowed(gvisor-only) = %v, want [gke-pod-snapshots]", allowedEngines)
	}

	gvisorEng, ok := reg.ForPod(podWithRuntimeClass("gvisor"), nil)
	if !ok || gvisorEng.Name() != GKEPodSnapshotsName || gvisorEng.APIGroup() != DefaultAPIGroup {
		t.Fatalf("ForPod(gvisor) = %v, %t; want gke-pod-snapshots on %s", gvisorEng, ok, DefaultAPIGroup)
	}
	defEng, ok := reg.ForPod(podWithRuntimeClass(""), nil)
	if !ok || defEng.Name() != CRIUName || defEng.APIGroup() != CRIUAPIGroup {
		t.Fatalf("ForPod(@default) = %v, %t; want criu on %s", defEng, ok, CRIUAPIGroup)
	}
}
