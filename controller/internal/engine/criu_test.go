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
	"strings"
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
			Containers:     []corev1.Container{{Name: "app", Command: []string{"/app"}}},
		},
	}
	unscheduledPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Command: []string{"/app"}}},
		},
	}
	multiPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName:   readyNode.Name,
			Containers: []corev1.Container{{Name: "app", Command: []string{"/app"}}, {Name: "sidecar", Command: []string{"/sidecar"}}},
		},
	}
	nativeSidecarPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			NodeName:       readyNode.Name,
			InitContainers: []corev1.Container{{Name: "istio-proxy", RestartPolicy: &always}},
			Containers:     []corev1.Container{{Name: "app", Command: []string{"/app"}}},
		},
	}

	if v := e.Eligible(nil, readyNode); v.OK {
		t.Errorf("Eligible(nil) = %+v, want refused", v)
	}
	if v := e.Eligible(singlePod, readyNode); !v.OK {
		t.Errorf("Eligible(singlePod, readyNode) = %+v, want OK", v)
	}
	if v := e.Eligible(singlePod, nil); v.OK || v.Code != "node_not_ready" {
		t.Errorf("Eligible(singlePod with NodeName, nil node) = %+v, want refused with Code=node_not_ready", v)
	}
	if v := e.Eligible(unscheduledPod, nil); v.OK || v.Code != "node_not_ready" {
		t.Errorf("Eligible(unscheduledPod, nil node) = %+v, want refused with Code=node_not_ready", v)
	}
	if v := e.Eligible(singlePod, unreadyNode); v.OK || v.Code != "node_not_ready" {
		t.Errorf("Eligible(singlePod, unreadyNode) = %+v, want refused with Code=node_not_ready", v)
	}
	if v := e.Eligible(multiPod, readyNode); v.OK || v.Code != "multi_container" {
		t.Errorf("Eligible(multiPod, readyNode) = %+v, want refused with Code=multi_container", v)
	}
	if v := e.Eligible(nativeSidecarPod, readyNode); v.OK || v.Code != "multi_container" {
		t.Errorf("Eligible(nativeSidecarPod, readyNode) = %+v, want refused with Code=multi_container", v)
	}
}

// Eligible refuses what the CRIU engine itself refuses or can't restore, so
// that such a pod isn't sent through an eviction 429 and a Refused trigger
// first (#233): hostNetwork (#229), a pod the engine already restored or
// wrapped with its holder (#236), and a container without an explicit command,
// whose replacement the restore webhook leaves unwired.
func TestCRIU_EligibleMirrorsEngineRefusals(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	readyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-ready",
			Annotations: map[string]string{CRIUNodeReadyAnnotation: "true"},
		},
	}
	basePod := func() *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "app-0"},
			Spec: corev1.PodSpec{
				NodeName:   readyNode.Name,
				Containers: []corev1.Container{{Name: "app", Command: []string{"/app"}}},
			},
		}
	}

	for _, tc := range []struct {
		name       string
		mutate     func(*corev1.Pod)
		wantOK     bool
		wantCode   string
		wantReason string // a substring of the refusal
	}{
		{name: "plain single-container pod with a command", mutate: func(*corev1.Pod) {}, wantOK: true},
		{
			name:       "hostNetwork",
			mutate:     func(p *corev1.Pod) { p.Spec.HostNetwork = true },
			wantCode:   "host_network",
			wantReason: "hostNetwork",
		},
		{
			name: "already restored by the engine",
			mutate: func(p *corev1.Pod) {
				p.Annotations = map[string]string{CRIURestoreStatusAnnotation: CRIURestoreStatusRestored}
			},
			wantCode:   "already_restored",
			wantReason: "already restored or wrapped",
		},
		{
			name: "already restored, with whitespace around the status",
			mutate: func(p *corev1.Pod) {
				p.Annotations = map[string]string{CRIURestoreStatusAnnotation: " " + CRIURestoreStatusRestored + "\n"}
			},
			wantCode:   "already_restored",
			wantReason: "already restored or wrapped",
		},
		{
			name: "wrapped by the holder, restore not decided yet",
			mutate: func(p *corev1.Pod) {
				p.Annotations = map[string]string{CRIUInjectedAnnotation: "true"}
			},
			wantCode:   "already_restored",
			wantReason: "already restored or wrapped",
		},
		{
			name: "wrapped by the holder, restore failed",
			mutate: func(p *corev1.Pod) {
				p.Annotations = map[string]string{
					CRIUInjectedAnnotation:      "true",
					CRIURestoreStatusAnnotation: CRIURestoreStatusFailed,
				}
			},
			wantCode:   "already_restored",
			wantReason: "already restored or wrapped",
		},
		{
			// The engine refuses only "restored" and injected=true: a failed
			// restore status alone, or injected with another value, is no
			// refusal there, so it's none here.
			name: "restore status failed without the holder",
			mutate: func(p *corev1.Pod) {
				p.Annotations = map[string]string{
					CRIURestoreStatusAnnotation: CRIURestoreStatusFailed,
					CRIUInjectedAnnotation:      "false",
				}
			},
			wantOK: true,
		},
		{
			name:       "container without an explicit command",
			mutate:     func(p *corev1.Pod) { p.Spec.Containers[0].Command = nil },
			wantCode:   "missing_command",
			wantReason: "explicit command",
		},
		{
			name: "container with args but no command",
			mutate: func(p *corev1.Pod) {
				p.Spec.Containers[0].Command = []string{}
				p.Spec.Containers[0].Args = []string{"--serve"}
			},
			wantCode:   "missing_command",
			wantReason: "explicit command",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pod := basePod()
			tc.mutate(pod)
			got := e.Eligible(pod, readyNode)
			if got.OK != tc.wantOK {
				t.Fatalf("Eligible = %+v, want OK=%v", got, tc.wantOK)
			}
			if tc.wantCode != "" && got.Code != tc.wantCode {
				t.Fatalf("Eligible Code = %q, want %q", got.Code, tc.wantCode)
			}
			if !tc.wantOK && !strings.Contains(got.Reason, tc.wantReason) {
				t.Errorf("Eligible reason = %q, want it to contain %q", got.Reason, tc.wantReason)
			}
		})
	}

	// These refusals don't depend on the node, so they come before the node
	// checks: a pod the engine would never take is refused for that, not for
	// a node that isn't in the cache yet.
	hostNet := basePod()
	hostNet.Spec.HostNetwork = true
	if got := e.Eligible(hostNet, nil); got.OK || !strings.Contains(got.Reason, "hostNetwork") {
		t.Errorf("Eligible(hostNetwork pod, nil node) = %+v, want the hostNetwork refusal", got)
	}
}

// A location without a bucket name is rejected like one without gs://: a
// storage config for it could never store a checkpoint (#233).
func TestCRIU_NamespaceResourcesRejectsUnusableLocations(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	for _, location := range []string{"", "bucket/path", "s3://bucket", "gs://", "gs:///", "gs:///path", "gs:// ", "gs:// /path", "gs://	/path", "gs://bucket/.", "gs://bucket/..", "gs://bucket/foo/../bar", "gs://bucket/foo//bar"} {
		t.Run(location, func(t *testing.T) {
			pm := &pmv1alpha1.PodMigration{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "mig", UID: types.UID("pm-uid-1")},
				Spec:       pmv1alpha1.PodMigrationSpec{Storage: pmv1alpha1.StorageSpec{Location: location}},
			}
			got, err := e.NamespaceResources(pm)
			var locationErr *StorageLocationError
			if !errors.As(err, &locationErr) {
				t.Fatalf("NamespaceResources(%q) error = %v, want a *StorageLocationError", location, err)
			}
			if locationErr.Location != location {
				t.Errorf("StorageLocationError.Location = %q, want %q", locationErr.Location, location)
			}
			if len(got) != 0 {
				t.Errorf("NamespaceResources(%q) returned objects %v with the error", location, got)
			}
		})
	}
	for _, location := range []string{"gs://bucket", "gs://bucket/", "gs://bucket/path"} {
		pm := &pmv1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "mig", UID: types.UID("pm-uid-1")},
			Spec:       pmv1alpha1.PodMigrationSpec{Storage: pmv1alpha1.StorageSpec{Location: location}},
		}
		if got, err := e.NamespaceResources(pm); err != nil || len(got) != 2 {
			t.Errorf("NamespaceResources(%q) = %v, %v; want 2 objects", location, got, err)
		}
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

// A failed CRIU restore's verdict says why, when the node agent recorded a
// reason (#244), and is the same cold start either way: only the evidence
// changes, so metrics and invariants that read Outcome and Reason don't.
func TestCRIU_VerifyRestoreRecordsTheFailureReason(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	failedWith := func(anns map[string]string) RestoreVerdict {
		t.Helper()
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIUInjectedAnnotation:      "true",
			CRIURestoreStatusAnnotation: CRIURestoreStatusFailed,
		}}}
		for k, v := range anns {
			pod.Annotations[k] = v
		}
		verdict, err := e.VerifyRestore(context.Background(), nil, pod)
		if err != nil {
			t.Fatalf("VerifyRestore: %v", err)
		}
		return verdict
	}

	without := failedWith(nil)
	if without.Outcome != ColdStart || without.Reason != criuColdStartReason || without.Confirmed {
		t.Fatalf("VerifyRestore(failed) = %+v, want an unconfirmed ColdStart", without)
	}
	if want := "annotation pod-migrate.io/psengine-restore=failed"; without.Evidence != want {
		t.Fatalf("evidence without a reason = %q, want %q", without.Evidence, want)
	}

	with := failedWith(map[string]string{CRIURestoreReasonAnnotation: "ArchiveSignatureInvalid"})
	if want := "annotation pod-migrate.io/psengine-restore=failed (reason ArchiveSignatureInvalid)"; with.Evidence != want {
		t.Fatalf("evidence = %q, want %q", with.Evidence, want)
	}
	if with.Outcome != without.Outcome || with.Reason != without.Reason || with.Confirmed != without.Confirmed {
		t.Fatalf("a reason changed more than the evidence: %+v vs %+v", with, without)
	}

	if blank := failedWith(map[string]string{CRIURestoreReasonAnnotation: "  "}); blank.Evidence != without.Evidence {
		t.Fatalf("a blank reason: evidence = %q, want %q", blank.Evidence, without.Evidence)
	}

	// A reason next to anything but failed says nothing (the engine's
	// admission policy doesn't let one be written there).
	restored, err := e.VerifyRestore(context.Background(), nil, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIUInjectedAnnotation:      "true",
			CRIURestoreStatusAnnotation: CRIURestoreStatusRestored,
			CRIURestoreReasonAnnotation: "ArchiveSignatureInvalid",
		}},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	})
	if err != nil || restored.Outcome != Restored || strings.Contains(restored.Evidence, "ArchiveSignatureInvalid") {
		t.Fatalf("VerifyRestore(restored, with a reason) = %+v, %v; want Restored without the reason", restored, err)
	}
}

// A CRIU restore that the node agent refused because the node's CPU model
// isn't the source's is a cold start that names the mismatched fact (#294),
// so that the job concludes RestoreIncompatibleNode. Any other reason names
// none.
func TestCRIU_VerifyRestoreNamesAnIncompatibleNode(t *testing.T) {
	e, err := NewCRIU(nil, nil, CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	verdict := func(t *testing.T, status, reason string) RestoreVerdict {
		t.Helper()
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{
			CRIUInjectedAnnotation:      "true",
			CRIURestoreStatusAnnotation: status,
			CRIURestoreReasonAnnotation: reason,
		}}, Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}}}
		v, err := e.VerifyRestore(context.Background(), nil, pod)
		if err != nil {
			t.Fatalf("VerifyRestore: %v", err)
		}
		return v
	}

	got := verdict(t, CRIURestoreStatusFailed, CRIURestoreReasonIncompatibleNode)
	if got.Outcome != ColdStart || got.Confirmed || got.IncompatibleFact != CRIUIncompatibleFactCPUModel {
		t.Fatalf("VerifyRestore(failed, %s) = %+v, want a ColdStart naming fact %s", CRIURestoreReasonIncompatibleNode, got, CRIUIncompatibleFactCPUModel)
	}
	if !strings.Contains(got.Evidence, "reason "+CRIURestoreReasonIncompatibleNode) {
		t.Errorf("evidence = %q, want the agent's reason", got.Evidence)
	}
	if got := verdict(t, CRIURestoreStatusFailed, "  "+CRIURestoreReasonIncompatibleNode+" "); got.IncompatibleFact != CRIUIncompatibleFactCPUModel {
		t.Errorf("a padded reason: IncompatibleFact = %q, want %s", got.IncompatibleFact, CRIUIncompatibleFactCPUModel)
	}
	for _, reason := range []string{"", "RestoreFailed", "ArchiveSignatureInvalid", "ArchiveDigestMismatch"} {
		if got := verdict(t, CRIURestoreStatusFailed, reason); got.Outcome != ColdStart || got.IncompatibleFact != "" {
			t.Errorf("VerifyRestore(failed, %q) = %+v, want a ColdStart naming no fact", reason, got)
		}
	}
	// The policy lets a reason go only with failed; next to restored it
	// says nothing.
	if got := verdict(t, CRIURestoreStatusRestored, CRIURestoreReasonIncompatibleNode); got.Outcome != Restored || got.IncompatibleFact != "" {
		t.Errorf("VerifyRestore(restored, %s) = %+v, want Restored naming no fact", CRIURestoreReasonIncompatibleNode, got)
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
