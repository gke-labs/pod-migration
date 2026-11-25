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

package psengine

// Tests for an engine that serves its own API group (#135). Most pass the
// group explicitly. TestSetGroupReachesTheWebhookPreflightAndRestore calls
// contract.SetGroup and restores the default when it ends; no test in this
// package runs in parallel, so no other test sees the change.

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const ownGroup = "criu.podmigration.gke.io"

var (
	ourCRDLabels   = map[string]string{ComponentLabel: ComponentLabelValue}
	addonCRDLabels = map[string]string{"addonmanager.kubernetes.io/mode": "Reconcile"}
)

// crdsOf returns the contract CRDs of group, carrying labels.
func crdsOf(group string, labels map[string]string) []client.Object {
	var objs []client.Object
	for _, name := range contract.CRDNames(group) {
		u := &unstructured.Unstructured{}
		u.SetAPIVersion("apiextensions.k8s.io/v1")
		u.SetKind("CustomResourceDefinition")
		u.SetName(name)
		u.SetLabels(labels)
		objs = append(objs, u)
	}
	return objs
}

// useGroup makes the engine serve group until the test ends.
func useGroup(t *testing.T, group string) {
	t.Helper()
	if err := contract.SetGroup(group); err != nil {
		t.Fatalf("SetGroup(%q): %v", group, err)
	}
	t.Cleanup(func() {
		if err := contract.SetGroup(contract.DefaultGroup); err != nil {
			t.Errorf("restoring the default group: %v", err)
		}
	})
}

// On its own group, the engine names its restore order in the holder's wrap
// command.
func TestInjectHolderNamesTheRestoreOrderOfAnotherGroup(t *testing.T) {
	pod := gatedReplacementPod()
	if ok, reason := injectHolder(pod, "gcr.io/x/holder:v1", contract.RestoreAnnotation(ownGroup)); !ok {
		t.Fatalf("expected injection, got skip: %s", reason)
	}
	want := []string{"/pmhold/holder", "wrap", "--ps-name-annotation=" + ownGroup + "/ps-name", "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"}
	if got := pod.Spec.Containers[0].Command; !slices.Equal(got, want) {
		t.Fatalf("wrapped command:\n got %q\nwant %q", got, want)
	}
}

// On the GKE Pod Snapshots group the command doesn't name the key, so it's
// the command of before #135, and holder images without the flag keep
// working.
func TestInjectHolderOnTheGKEPodSnapshotsGroupKeepsTheCommand(t *testing.T) {
	pod := gatedReplacementPod()
	if ok, reason := injectHolder(pod, "gcr.io/x/holder:v1", contract.RestoreAnnotation(contract.GKEPodSnapshotsGroup)); !ok {
		t.Fatalf("expected injection, got skip: %s", reason)
	}
	want := []string{"/pmhold/holder", "wrap", "/pminfo/annotations", "--", "redis-server", "--appendonly", "yes"}
	if got := pod.Spec.Containers[0].Command; !slices.Equal(got, want) {
		t.Fatalf("wrapped command:\n got %q\nwant %q", got, want)
	}
}

// The preflight checks the CRDs of the engine's own group, so the add-on's
// CRDs next to them don't stop it, and it names the right CRDs and group
// when its own are missing.
func TestPreflightChecksTheEnginesGroup(t *testing.T) {
	addonCRDs := crdsOf(contract.GKEPodSnapshotsGroup, addonCRDLabels)

	t.Run("own CRDs next to the add-on's: pass", func(t *testing.T) {
		objs := append(slices.Clone(addonCRDs), crdsOf(ownGroup, ourCRDLabels)...)
		if err := verifyCRDOwnership(context.Background(), newCRDClient(t, objs...), ownGroup); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("only the add-on's CRDs: fail, naming the engine's group", func(t *testing.T) {
		err := verifyCRDOwnership(context.Background(), newCRDClient(t, addonCRDs...), ownGroup)
		if err == nil {
			t.Fatal("the engine's own CRDs are missing, so preflight must fail")
		}
		for _, want := range []string{"podsnapshotstorageconfigs." + ownGroup, "with its group set to " + ownGroup} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q doesn't contain %q", err, want)
			}
		}
	})
	t.Run("add-on's group: still refused next to the add-on", func(t *testing.T) {
		err := verifyCRDOwnership(context.Background(), newCRDClient(t, addonCRDs...), contract.GKEPodSnapshotsGroup)
		if err == nil || !strings.Contains(err.Error(), "GKE Pod Snapshots addon") {
			t.Fatalf("an engine on the add-on's group must refuse to run next to the add-on, got %v", err)
		}
	})
	t.Run("add-on's group, CRDs missing: the addon-off install hint", func(t *testing.T) {
		err := verifyCRDOwnership(context.Background(), newCRDClient(t), contract.GKEPodSnapshotsGroup)
		if err == nil || !strings.Contains(err.Error(), "(addon-off clusters only)") {
			t.Fatalf("want the addon-off install hint, got %v", err)
		}
	})
}

// With RestoreInjector scoped strictly to CRIU-routed replacement pods (#135),
// Preflight succeeds on criu.podmigration.gke.io even when the managed GKE Pod
// Snapshots addon owns podsnapshot.gke.io, and RestoreInjector.Handle leaves
// gVisor / non-CRIU replacement pods untouched.
func TestCoexistenceWithGKEPodSnapshotsAddon(t *testing.T) {
	ctx := context.Background()
	addonCRDs := crdsOf(contract.GKEPodSnapshotsGroup, addonCRDLabels)
	ourCRDs := crdsOf(contract.CRIUGroup, ourCRDLabels)

	t.Run("preflight passes when own CRDs sit next to the add-on's", func(t *testing.T) {
		objs := append(slices.Clone(addonCRDs), ourCRDs...)
		if err := Preflight(ctx, newCRDClient(t, objs...)); err != nil {
			t.Fatalf("Preflight on %s next to GKE add-on CRDs failed: %v", contract.Group, err)
		}
	})

	t.Run("RestoreInjector.Handle end-to-end scoping, zero-read short-circuit, and nil-reader safety", func(t *testing.T) {
		dec := admission.NewDecoder(testScheme(t))
		makeCreateReq := func(t *testing.T, reqNS string, pod *corev1.Pod) admission.Request {
			t.Helper()
			raw, err := json.Marshal(pod)
			if err != nil {
				t.Fatalf("marshal pod: %v", err)
			}
			return admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Namespace: reqNS,
					Name:      pod.Name,
					Operation: admissionv1.Create,
					Object:    runtime.RawExtension{Raw: raw},
				},
			}
		}

		criuJob := contract.New(contract.GVKPodMigrationJob)
		criuJob.SetNamespace("tenant-a")
		criuJob.SetName("pmj-criu")
		criuJob.SetAnnotations(map[string]string{contract.AnnotationOrchestratorEngine: contract.EngineCRIU})

		otherNSCRIUJob := contract.New(contract.GVKPodMigrationJob)
		otherNSCRIUJob.SetNamespace("other-ns")
		otherNSCRIUJob.SetName("pmj-other-ns")
		otherNSCRIUJob.SetAnnotations(map[string]string{contract.AnnotationOrchestratorEngine: contract.EngineCRIU})

		gkeJob := contract.New(contract.GVKPodMigrationJob)
		gkeJob.SetNamespace("tenant-a")
		gkeJob.SetName("pmj-gke")
		gkeJob.SetAnnotations(map[string]string{contract.AnnotationOrchestratorEngine: "gke-pod-snapshots"})

		legacyUnannotatedJob := contract.New(contract.GVKPodMigrationJob)
		legacyUnannotatedJob.SetNamespace("tenant-a")
		legacyUnannotatedJob.SetName("pmj-legacy")

		var pmjReads atomic.Int32
		c := newClientWithInterceptor(t, interceptor.Funcs{
			Get: func(ctx context.Context, cli client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if obj.GetObjectKind().GroupVersionKind() == contract.GVKPodMigrationJob {
					pmjReads.Add(1)
				}
				return cli.Get(ctx, key, obj, opts...)
			},
		}, criuJob, otherNSCRIUJob, gkeJob, legacyUnannotatedJob)
		inj := &RestoreInjector{Decoder: dec, HolderImage: "gcr.io/x/holder:v1", Reader: c}

		// 1. CRIU job with pod.Namespace == "" and req.Namespace == "tenant-a": patched.
		podForCRIU := gatedReplacementPod()
		podForCRIU.Namespace = ""
		podForCRIU.Annotations[contract.AnnotationAssignedPMJ] = "pmj-criu"
		resp := inj.Handle(ctx, makeCreateReq(t, "tenant-a", podForCRIU))
		if !resp.Allowed || len(resp.Patches) == 0 {
			t.Fatalf("expected CRIU replacement pod to be allowed and patched, got allowed=%v patches=%d result=%+v", resp.Allowed, len(resp.Patches), resp.Result)
		}
		patchJSON, err := json.Marshal(resp.Patches)
		if err != nil {
			t.Fatalf("marshal patches: %v", err)
		}
		for _, wantSubstr := range []string{
			"--ps-name-annotation=" + contract.AnnotationPSName,
			seedInitName,
			"psengine-injected",
		} {
			if !strings.Contains(string(patchJSON), wantSubstr) {
				t.Errorf("patch JSON %s missing %q", string(patchJSON), wantSubstr)
			}
		}
		if gotReads := pmjReads.Swap(0); gotReads != 1 {
			t.Fatalf("expected exactly 1 PMJ GET for CRIU candidate pod, got %d", gotReads)
		}

		// 2. GKE Pod Snapshots job, unannotated job, missing job, and job in another namespace: allowed with 0 patches.
		for _, tc := range []struct {
			name        string
			podNS       string
			assignedPMJ string
			wantMsg     string
		}{
			{name: "gke-pod-snapshots job", podNS: "", assignedPMJ: "pmj-gke", wantMsg: "gke-pod-snapshots"},
			{name: "legacy unannotated job", podNS: "", assignedPMJ: "pmj-legacy", wantMsg: "not criu"},
			{name: "missing job", podNS: "", assignedPMJ: "pmj-missing", wantMsg: "cannot read assigned PodMigrationJob"},
			{name: "job only in other-ns with empty pod.Namespace", podNS: "", assignedPMJ: "pmj-other-ns", wantMsg: "cannot read assigned PodMigrationJob tenant-a/pmj-other-ns"},
			{name: "req.Namespace overrides mismatched pod.Namespace", podNS: "other-ns", assignedPMJ: "pmj-other-ns", wantMsg: "cannot read assigned PodMigrationJob tenant-a/pmj-other-ns"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				pmjReads.Store(0)
				pod := gatedReplacementPod()
				pod.Namespace = tc.podNS
				pod.Annotations[contract.AnnotationAssignedPMJ] = tc.assignedPMJ
				r := inj.Handle(ctx, makeCreateReq(t, "tenant-a", pod))
				if !r.Allowed || len(r.Patches) != 0 {
					t.Fatalf("expected Allowed with 0 patches, got allowed=%v patches=%d result=%+v", r.Allowed, len(r.Patches), r.Result)
				}
				if r.Result == nil || !strings.Contains(r.Result.Message, tc.wantMsg) {
					t.Fatalf("result message = %+v, want substring %q", r.Result, tc.wantMsg)
				}
				if gotReads := pmjReads.Load(); gotReads != 1 {
					t.Fatalf("expected 1 PMJ GET, got %d", gotReads)
				}
			})
		}

		// 3. Pure local pod-shape checks short-circuit BEFORE reading PodMigrationJob (0 API reads).
		gvisorRC := "gvisor"
		always := corev1.ContainerRestartPolicyAlways
		for _, tc := range []struct {
			name   string
			mutate func(*corev1.Pod)
		}{
			{
				name: "gvisor runtimeClassName",
				mutate: func(p *corev1.Pod) {
					p.Spec.RuntimeClassName = &gvisorRC
				},
			},
			{
				name: "ungated pod",
				mutate: func(p *corev1.Pod) {
					p.Spec.SchedulingGates = nil
				},
			},
			{
				name: "native sidecar initContainer",
				mutate: func(p *corev1.Pod) {
					p.Spec.InitContainers = []corev1.Container{{Name: "istio-proxy", RestartPolicy: &always}}
				},
			},
			{
				name: "hostNetwork pod",
				mutate: func(p *corev1.Pod) {
					p.Spec.HostNetwork = true
				},
			},
			{
				name: "multi-container pod",
				mutate: func(p *corev1.Pod) {
					p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: "sidecar", Image: "busybox"})
				},
			},
			{
				name: "commandless container",
				mutate: func(p *corev1.Pod) {
					p.Spec.Containers[0].Command = nil
				},
			},
		} {
			t.Run("zero-read short-circuit: "+tc.name, func(t *testing.T) {
				pmjReads.Store(0)
				pod := gatedReplacementPod()
				pod.Namespace = ""
				pod.Annotations[contract.AnnotationAssignedPMJ] = "pmj-criu"
				tc.mutate(pod)
				r := inj.Handle(ctx, makeCreateReq(t, "tenant-a", pod))
				if !r.Allowed || len(r.Patches) != 0 {
					t.Fatalf("expected Allowed with 0 patches, got allowed=%v patches=%d", r.Allowed, len(r.Patches))
				}
				if gotReads := pmjReads.Load(); gotReads != 0 {
					t.Fatalf("expected 0 PodMigrationJob reads for %s, got %d", tc.name, gotReads)
				}
			})
		}

		// 4. Nil Reader fails safe (skips holder injection with 0 patches instead of injecting unscoped).
		nilReaderInj := &RestoreInjector{Decoder: dec, HolderImage: "gcr.io/x/holder:v1", Reader: nil}
		podNilReader := gatedReplacementPod()
		podNilReader.Namespace = ""
		podNilReader.Annotations[contract.AnnotationAssignedPMJ] = "pmj-criu"
		nilResp := nilReaderInj.Handle(ctx, makeCreateReq(t, "tenant-a", podNilReader))
		if !nilResp.Allowed || len(nilResp.Patches) != 0 {
			t.Fatalf("expected nil-reader RestoreInjector to fail safe with 0 patches, got allowed=%v patches=%d", nilResp.Allowed, len(nilResp.Patches))
		}
		if nilResp.Result == nil || !strings.Contains(nilResp.Result.Message, "restore webhook has no PodMigrationJob reader; skipping holder injection") {
			t.Fatalf("unexpected nil-reader message: %+v", nilResp.Result)
		}
	})
}

// contract.SetGroup reaches every reader of the group: the webhook names the
// engine's restore order, the preflight checks the engine's CRDs, and the
// node agent restores pods carrying <group>/ps-name and leaves pods that
// carry only podsnapshot.gke.io/ps-name alone.
func TestSetGroupReachesTheWebhookPreflightAndRestore(t *testing.T) {
	const customGroup = "custom.podmigration.gke.io"
	useGroup(t, customGroup)
	ctx := context.Background()
	const psName = "ps-web-0-deadbeef"
	ownKey := contract.RestoreAnnotation(customGroup)
	gkeKey := contract.RestoreAnnotation(contract.GKEPodSnapshotsGroup)

	t.Run("webhook", func(t *testing.T) {
		pod := gatedReplacementPod()
		if ok, reason := InjectHolder(pod, "gcr.io/x/holder:v1"); !ok {
			t.Fatalf("expected injection, got skip: %s", reason)
		}
		if !slices.Contains(pod.Spec.Containers[0].Command, "--ps-name-annotation="+ownKey) {
			t.Fatalf("wrap command doesn't name %s: %q", ownKey, pod.Spec.Containers[0].Command)
		}
	})

	t.Run("preflight", func(t *testing.T) {
		addonCRDs := crdsOf(contract.GKEPodSnapshotsGroup, addonCRDLabels)
		err := VerifyCRDOwnership(ctx, newCRDClient(t, addonCRDs...))
		if err == nil || !strings.Contains(err.Error(), "podsnapshotstorageconfigs."+customGroup) {
			t.Fatalf("preflight must look for the engine's own CRDs, got %v", err)
		}
		if err := Preflight(ctx, newCRDClient(t, addonCRDs...)); err == nil {
			t.Fatal("with the engine's own CRDs missing, preflight must fail")
		}
		objs := append(slices.Clone(addonCRDs), crdsOf(customGroup, ourCRDLabels)...)
		if err := VerifyCRDOwnership(ctx, newCRDClient(t, objs...)); err != nil {
			t.Fatalf("own CRDs next to the add-on's: %v", err)
		}
		if err := Preflight(ctx, newCRDClient(t, objs...)); err != nil {
			t.Fatalf("with scoped webhook on %s, the engine coexists next to the add-on: %v", customGroup, err)
		}
	})

	restore := func(t *testing.T, pod *corev1.Pod) (*fakeRestoreOps, *corev1.Pod) {
		t.Helper()
		ps := workOrderSnapshot(psName)
		if got := ps.GroupVersionKind().Group; got != customGroup {
			t.Fatalf("work order group = %q, want %q", got, customGroup)
		}
		if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded}); err != nil {
			t.Fatal(err)
		}
		c := newClient(t, ps, pod)
		ops := &fakeRestoreOps{}
		r := &NodeRestoreReconciler{Keyring: testArchiveKeyring, ClusterUID: testClusterUID, Client: c, NodeName: "node-a", Ops: ops, Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir()}
		key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
			t.Fatal(err)
		}
		got := &corev1.Pod{}
		if err := c.Get(ctx, key, got); err != nil {
			t.Fatal(err)
		}
		return ops, got
	}

	t.Run("restore order under the engine's group: restored", func(t *testing.T) {
		pod := replacementPodOnNode(psName)
		if pod.Annotations[ownKey] != psName {
			t.Fatalf("replacement pod doesn't carry %s: %v", ownKey, pod.Annotations)
		}
		ops, got := restore(t, pod)
		if len(ops.restored) != 1 {
			t.Fatalf("restore calls: %d", len(ops.restored))
		}
		if s := got.Annotations[contract.AnnotationRestoreStatus]; s != contract.RestoreStatusRestored {
			t.Fatalf("restore status: %q", s)
		}
	})

	t.Run("restore order only under podsnapshot.gke.io: left alone", func(t *testing.T) {
		pod := replacementPodOnNode(psName)
		delete(pod.Annotations, ownKey)
		pod.Annotations[gkeKey] = psName
		ops, got := restore(t, pod)
		if len(ops.restored) != 0 {
			t.Fatalf("another engine's restore order must not be acted on, restore calls: %d", len(ops.restored))
		}
		if s, ok := got.Annotations[contract.AnnotationRestoreStatus]; ok {
			t.Fatalf("another engine's pod must not be stamped, got restore status %q", s)
		}
	})
}
