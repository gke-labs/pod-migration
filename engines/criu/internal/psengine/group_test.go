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
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

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
	if contract.AnnotationPSName != holderDefaultPSNameAnnotation {
		t.Fatalf("by default the engine must use the holder's default restore order: %q != %q", contract.AnnotationPSName, holderDefaultPSNameAnnotation)
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

// The restore webhook wraps every gated replacement pod, so an engine on its
// own group with the webhook on refuses to start next to the add-on, whose
// gVisor pods the webhook would rewrite.
func TestRestoreWebhookScopeRefusesTheAddon(t *testing.T) {
	ctx := context.Background()

	t.Run("own group next to the add-on: refused", func(t *testing.T) {
		err := verifyRestoreWebhookScope(ctx, newCRDClient(t, crdsOf(contract.GKEPodSnapshotsGroup, addonCRDLabels)...), ownGroup)
		if err == nil {
			t.Fatal("the webhook would rewrite the add-on's pods, so preflight must fail")
		}
		for _, want := range []string{
			"podsnapshotstorageconfigs.podsnapshot.gke.io",
			"GKE Pod Snapshots addon",
			"--api-group=" + ownGroup,
			"--enable-webhooks=false",
		} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q doesn't contain %q", err, want)
			}
		}
	})
	t.Run("our label applied over the add-on's CRDs: refused", func(t *testing.T) {
		clobbered := map[string]string{ComponentLabel: ComponentLabelValue, "addonmanager.kubernetes.io/mode": "Reconcile"}
		if err := verifyRestoreWebhookScope(ctx, newCRDClient(t, crdsOf(contract.GKEPodSnapshotsGroup, clobbered)...), ownGroup); err == nil {
			t.Fatal("the add-on's markers mean the add-on is still there, so preflight must fail")
		}
	})
	t.Run("own group, no add-on: pass", func(t *testing.T) {
		if err := verifyRestoreWebhookScope(ctx, newCRDClient(t, crdsOf(ownGroup, ourCRDLabels)...), ownGroup); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("own group next to this engine's podsnapshot.gke.io CRDs: pass", func(t *testing.T) {
		if err := verifyRestoreWebhookScope(ctx, newCRDClient(t, crdsOf(contract.GKEPodSnapshotsGroup, ourCRDLabels)...), ownGroup); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("GKE Pod Snapshots group: left to VerifyCRDOwnership", func(t *testing.T) {
		if err := verifyRestoreWebhookScope(ctx, newCRDClient(t, crdsOf(contract.GKEPodSnapshotsGroup, addonCRDLabels)...), contract.GKEPodSnapshotsGroup); err != nil {
			t.Fatal(err)
		}
	})
}

// contract.SetGroup reaches every reader of the group: the webhook names the
// engine's restore order, the preflight checks the engine's CRDs, and the
// node agent restores pods carrying <group>/ps-name and leaves pods that
// carry only podsnapshot.gke.io/ps-name alone.
func TestSetGroupReachesTheWebhookPreflightAndRestore(t *testing.T) {
	useGroup(t, ownGroup)
	ctx := context.Background()
	const psName = "ps-web-0-deadbeef"
	ownKey := contract.RestoreAnnotation(ownGroup)
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
		if err == nil || !strings.Contains(err.Error(), "podsnapshotstorageconfigs."+ownGroup) {
			t.Fatalf("preflight must look for the engine's own CRDs, got %v", err)
		}
		if err := Preflight(ctx, newCRDClient(t, addonCRDs...), false); err == nil {
			t.Fatal("with the engine's own CRDs missing, preflight must fail even with the webhook off")
		}
		objs := append(slices.Clone(addonCRDs), crdsOf(ownGroup, ourCRDLabels)...)
		if err := VerifyCRDOwnership(ctx, newCRDClient(t, objs...)); err != nil {
			t.Fatalf("own CRDs next to the add-on's: %v", err)
		}
		if err := Preflight(ctx, newCRDClient(t, objs...), false); err != nil {
			t.Fatalf("with the webhook off, the engine can run next to the add-on: %v", err)
		}
		if err := Preflight(ctx, newCRDClient(t, objs...), true); err == nil || !strings.Contains(err.Error(), "--enable-webhooks=false") {
			t.Fatalf("with the webhook on, the engine must still refuse to run next to the add-on, got %v", err)
		}
	})

	restore := func(t *testing.T, pod *corev1.Pod) (*fakeRestoreOps, *corev1.Pod) {
		t.Helper()
		ps := workOrderSnapshot(psName)
		if got := ps.GroupVersionKind().Group; got != ownGroup {
			t.Fatalf("work order group = %q, want %q", got, ownGroup)
		}
		if err := contract.SetCondition(ps, metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionTrue, Reason: contract.ReasonSucceeded}); err != nil {
			t.Fatal(err)
		}
		c := newClient(t, ps, pod)
		ops := &fakeRestoreOps{}
		r := &NodeRestoreReconciler{Client: c, NodeName: "node-a", Ops: ops, Store: readySnapshotWithArchive(t, newMemStore()), WorkDir: t.TempDir()}
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
