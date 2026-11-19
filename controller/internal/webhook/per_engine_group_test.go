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

package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	evanjsonpatch "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func sideBySideWebhookScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

func buildSideBySideRegistry(t *testing.T, cl client.Client, scheme *runtime.Scheme) (*eligibility.RuntimeClassPolicy, *engine.Registry) {
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
	return policy, reg
}

func applyAdmissionPatches(t *testing.T, raw []byte, resp admission.Response) *corev1.Pod {
	t.Helper()
	if len(resp.Patches) > 0 {
		patchBytes, err := json.Marshal(resp.Patches)
		if err != nil {
			t.Fatalf("marshal patches: %v", err)
		}
		patch, err := evanjsonpatch.DecodePatch(patchBytes)
		if err != nil {
			t.Fatalf("decode json patch: %v", err)
		}
		raw, err = patch.Apply(raw)
		if err != nil {
			t.Fatalf("apply json patch: %v", err)
		}
	}
	out := &corev1.Pod{}
	if err := json.Unmarshal(raw, out); err != nil {
		t.Fatalf("unmarshal patched pod: %v", err)
	}
	return out
}

func TestEvictionGate_SideBySideGVisorAndCRIUAPIGroups(t *testing.T) {
	scheme := sideBySideWebhookScheme()
	const ns = "default"
	gvisorClass := "gvisor"

	readyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "node-criu-ready",
			Annotations: map[string]string{engine.CRIUNodeReadyAnnotation: "true"},
		},
	}
	unreadyNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "node-criu-unready"},
	}

	gvisorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "gvisor-app",
			UID:       "uid-gvisor-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorClass,
			NodeName:         unreadyNode.Name, // gVisor ignores CRIU node annotation
			Containers:       []corev1.Container{{Name: "app"}},
		},
	}
	criuReadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "criu-app-ready",
			UID:       "uid-criu-1",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName:   readyNode.Name,
			Containers: []corev1.Container{{Name: "app"}},
		},
	}
	criuUnreadyPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "criu-app-unready",
			UID:       "uid-criu-2",
			Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
		},
		Spec: corev1.PodSpec{
			NodeName:   unreadyNode.Name,
			Containers: []corev1.Container{{Name: "app"}},
		},
	}

	gkePSP := &unstructured.Unstructured{}
	gkePSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.DefaultAPIGroup))
	gkePSP.SetNamespace(ns)
	gkePSP.SetName("psp-mig-manual")
	gkePSP.Object["spec"] = map[string]interface{}{
		"selector":      map[string]interface{}{"matchLabels": map[string]interface{}{"pod-migration.gke.io/enabled": "true"}},
		"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
	}
	gkePSP.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}},
	}

	criuPSP := &unstructured.Unstructured{}
	criuPSP.SetGroupVersionKind(snapshot.PolicyGVK(engine.CRIUAPIGroup))
	criuPSP.SetNamespace(ns)
	criuPSP.SetName("psp-mig-criu-manual")
	criuPSP.SetLabels(map[string]string{engine.CRIUPolicyLabel: engine.CRIUName})
	criuPSP.Object["spec"] = map[string]interface{}{
		"selector":      map[string]interface{}{"matchLabels": map[string]interface{}{"pod-migration.gke.io/enabled": "true"}},
		"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
	}
	criuPSP.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(readyNode, unreadyNode, gvisorPod, criuReadyPod, criuUnreadyPod, gkePSP, criuPSP).
		Build()

	policy, reg := buildSideBySideRegistry(t, cl, scheme)
	gate := &EvictionGate{
		Client:      cl,
		Eligibility: policy,
		Engines:     reg,
		decoder:     admission.NewDecoder(scheme),
	}

	evict := func(podName string) admission.Response {
		return gate.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace:   ns,
				Name:        podName,
				SubResource: "eviction",
				Operation:   admissionv1.Create,
			},
		})
	}

	// 1. gVisor pod creates a PMJ served by gke-pod-snapshots using podsnapshot.gke.io policy.
	if resp := evict(gvisorPod.Name); resp.Allowed || resp.Result.Code != http.StatusTooManyRequests {
		t.Fatalf("gVisor eviction = allowed:%t result:%+v, want 429 migration spawned", resp.Allowed, resp.Result)
	}
	gvisorJob := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: util.FormatPMJName(gvisorPod.Name, string(gvisorPod.UID))}, gvisorJob); err != nil {
		t.Fatalf("expected gVisor PMJ: %v", err)
	}
	if gvisorJob.Annotations[engine.AnnotationEngine] != engine.GKEPodSnapshotsName ||
		gvisorJob.Annotations[util.AnnotationPodSnapshotPolicy] != "psp-mig-manual" {
		t.Errorf("gVisor PMJ annotations = %v", gvisorJob.Annotations)
	}

	// 2. @default pod on criu-ready node creates a PMJ served by criu using criu.podmigration.gke.io policy.
	if resp := evict(criuReadyPod.Name); resp.Allowed || resp.Result.Code != http.StatusTooManyRequests {
		t.Fatalf("CRIU ready eviction = allowed:%t result:%+v, want 429 migration spawned", resp.Allowed, resp.Result)
	}
	criuJob := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: util.FormatPMJName(criuReadyPod.Name, string(criuReadyPod.UID))}, criuJob); err != nil {
		t.Fatalf("expected CRIU PMJ: %v", err)
	}
	if criuJob.Annotations[engine.AnnotationEngine] != engine.CRIUName ||
		criuJob.Annotations[util.AnnotationPodSnapshotPolicy] != "psp-mig-criu-manual" {
		t.Errorf("CRIU PMJ annotations = %v", criuJob.Annotations)
	}

	// 3. @default pod on non-criu-ready node is refused by Eligible(pod, sourceNode) and cold-evicted immediately.
	if resp := evict(criuUnreadyPod.Name); !resp.Allowed {
		t.Fatalf("CRIU unready-node eviction should be allowed immediately, got: %+v", resp.Result)
	}
}

func TestPodGateInjector_ProtectsRestoreAnnotationsAcrossAllEnginesOnCreateAndUpdate(t *testing.T) {
	scheme := sideBySideWebhookScheme()
	const (
		ns      = "default"
		criuAnn = engine.CRIUAPIGroup + "/ps-name"
		gkeAnn  = engine.DefaultAPIGroup + "/ps-name"
	)
	gvisorClass := "gvisor"

	activeCRIUJob := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   ns,
			Name:        util.FormatPMJName("gated-criu-pod", "orig1234"),
			Annotations: map[string]string{engine.AnnotationEngine: engine.CRIUName},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "gated-criu-pod"},
			TargetPodUID: "orig1234",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(activeCRIUJob).
		Build()

	_, reg := buildSideBySideRegistry(t, cl, scheme)
	injector := &PodGateInjector{
		Client:  cl,
		Engines: reg,
		decoder: admission.NewDecoder(scheme),
	}

	t.Run("CREATE scale-up CRIU pod strips spoofed restore annotations across all engines", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "scale-up-criu",
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{
					criuAnn:                            "victim-criu-snapshot",
					gkeAnn:                             "victim-gke-snapshot",
					util.AnnotationAssignedPMJ:         "fake-pmj",
					engine.CRIUInjectedAnnotation:      "true",
					engine.CRIURestoreStatusAnnotation: "restored",
				},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: ns,
				Name:      pod.Name,
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("unexpected deny: %+v", resp.Result)
		}
		got := applyAdmissionPatches(t, raw, resp)
		if v, ok := got.Annotations[criuAnn]; !ok || v != "" {
			t.Errorf("%s = %q (ok=%t), want empty cold-start bypass", criuAnn, v, ok)
		}
		if v, ok := got.Annotations[gkeAnn]; !ok || v != "" {
			t.Errorf("%s = %q (ok=%t), want empty cold-start bypass", gkeAnn, v, ok)
		}
		for _, forbidden := range []string{util.AnnotationAssignedPMJ, engine.CRIUInjectedAnnotation, engine.CRIURestoreStatusAnnotation} {
			if _, exists := got.Annotations[forbidden]; exists {
				t.Errorf("expected spoofed %s to be stripped, got %q", forbidden, got.Annotations[forbidden])
			}
		}
	})

	t.Run("CREATE scale-up gVisor pod preserves Kubeflow GKE ps-name while stripping spoofed CRIU ps-name", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "kubeflow-gvisor",
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{
					gkeAnn:  "ws-snapshot-123",
					criuAnn: "spoofed-criu-snapshot",
				},
			},
			Spec: corev1.PodSpec{RuntimeClassName: &gvisorClass},
		}
		raw, _ := json.Marshal(pod)
		resp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: ns,
				Name:      pod.Name,
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("unexpected deny: %+v", resp.Result)
		}
		got := applyAdmissionPatches(t, raw, resp)
		if got.Annotations[gkeAnn] != "ws-snapshot-123" {
			t.Errorf("%s = %q, want preserved ws-snapshot-123", gkeAnn, got.Annotations[gkeAnn])
		}
		if _, exists := got.Annotations[criuAnn]; exists {
			t.Errorf("expected spoofed %s to be stripped, got %q", criuAnn, got.Annotations[criuAnn])
		}
	})

	t.Run("CREATE gated replacement pod strips spoofed restore annotations even when gate pre-supplied", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "gated-criu-pod",
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{
					criuAnn:                            "spoofed-criu-snap",
					gkeAnn:                             "stale-template-snap",
					engine.CRIUInjectedAnnotation:      "true",
					engine.CRIURestoreStatusAnnotation: "restored",
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}},
			},
		}
		raw, _ := json.Marshal(pod)
		resp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Create,
				Namespace: ns,
				Name:      pod.Name,
				Object:    runtime.RawExtension{Raw: raw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("unexpected deny: %+v", resp.Result)
		}
		got := applyAdmissionPatches(t, raw, resp)
		if got.Annotations[util.AnnotationAssignedPMJ] != activeCRIUJob.Name {
			t.Errorf("assigned-pmj = %q, want %q", got.Annotations[util.AnnotationAssignedPMJ], activeCRIUJob.Name)
		}
		for _, stripped := range []string{criuAnn, gkeAnn, engine.CRIUInjectedAnnotation, engine.CRIURestoreStatusAnnotation} {
			if _, exists := got.Annotations[stripped]; exists {
				t.Errorf("expected %s to be stripped on gated replacement, got %q", stripped, got.Annotations[stripped])
			}
		}
	})

	t.Run("UPDATE by untrusted user reverts spoofed restore annotations while allowing trusted controller", func(t *testing.T) {
		oldPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "running-criu-pod",
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: map[string]string{
					criuAnn:                       "",
					util.AnnotationAssignedPMJ:    activeCRIUJob.Name,
					engine.CRIUInjectedAnnotation: "true",
				},
			},
		}
		tamperedPod := oldPod.DeepCopy()
		tamperedPod.Annotations[criuAnn] = "stolen-snapshot"
		tamperedPod.Annotations[gkeAnn] = "stolen-gke-snapshot"
		tamperedPod.Annotations["user.example.com/custom"] = "allowed-edit"

		oldRaw, _ := json.Marshal(oldPod)
		newRaw, _ := json.Marshal(tamperedPod)

		// Untrusted user: restore annotations reverted, unrelated annotation preserved.
		untrustedResp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Update,
				Namespace: ns,
				Name:      oldPod.Name,
				UserInfo:  authenticationv1.UserInfo{Username: "system:serviceaccount:default:attacker"},
				OldObject: runtime.RawExtension{Raw: oldRaw},
				Object:    runtime.RawExtension{Raw: newRaw},
			},
		})
		if !untrustedResp.Allowed {
			t.Fatalf("unexpected deny on untrusted update: %+v", untrustedResp.Result)
		}
		gotUntrusted := applyAdmissionPatches(t, newRaw, untrustedResp)
		if gotUntrusted.Annotations[criuAnn] != "" {
			t.Errorf("untrusted UPDATE changed %s to %q, want reverted to \"\"", criuAnn, gotUntrusted.Annotations[criuAnn])
		}
		if _, exists := gotUntrusted.Annotations[gkeAnn]; exists {
			t.Errorf("untrusted UPDATE added %s = %q, want deleted", gkeAnn, gotUntrusted.Annotations[gkeAnn])
		}
		if gotUntrusted.Annotations["user.example.com/custom"] != "allowed-edit" {
			t.Errorf("unrelated annotation lost: %v", gotUntrusted.Annotations)
		}

		// Trusted controller SA: allowed to set <criu-group>/ps-name.
		controllerPod := oldPod.DeepCopy()
		controllerPod.Annotations[criuAnn] = "ps-legitimate-restore"
		ctrlRaw, _ := json.Marshal(controllerPod)
		trustedResp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Update,
				Namespace: ns,
				Name:      oldPod.Name,
				UserInfo:  authenticationv1.UserInfo{Username: DefaultControllerUsername},
				OldObject: runtime.RawExtension{Raw: oldRaw},
				Object:    runtime.RawExtension{Raw: ctrlRaw},
			},
		})
		if !trustedResp.Allowed || len(trustedResp.Patches) != 0 {
			t.Fatalf("trusted controller UPDATE should be allowed unmutated, got allowed=%t patches=%+v", trustedResp.Allowed, trustedResp.Patches)
		}
	})

	t.Run("UPDATE newly opting into pod-migration.gke.io/enabled=true scrubs pre-existing spoofed ps-name", func(t *testing.T) {
		oldOptedOut := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "late-opt-in-pod",
				Annotations: map[string]string{
					criuAnn: "preplanted-snapshot",
				},
			},
		}
		newOptedIn := oldOptedOut.DeepCopy()
		newOptedIn.Labels = map[string]string{"pod-migration.gke.io/enabled": "true"}

		oldRaw, _ := json.Marshal(oldOptedOut)
		newRaw, _ := json.Marshal(newOptedIn)
		resp := injector.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Operation: admissionv1.Update,
				Namespace: ns,
				Name:      oldOptedOut.Name,
				UserInfo:  authenticationv1.UserInfo{Username: "system:serviceaccount:default:attacker"},
				OldObject: runtime.RawExtension{Raw: oldRaw},
				Object:    runtime.RawExtension{Raw: newRaw},
			},
		})
		if !resp.Allowed {
			t.Fatalf("unexpected deny: %+v", resp.Result)
		}
		got := applyAdmissionPatches(t, newRaw, resp)
		if v, ok := got.Annotations[criuAnn]; !ok || v != "" {
			t.Errorf("late opt-in %s = %q (ok=%t), want empty cold-start bypass", criuAnn, v, ok)
		}
	})
}
