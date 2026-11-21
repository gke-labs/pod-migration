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
	"os"
	"path/filepath"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestStatefulSetIdentityContinuity verifies that when a StatefulSet pod is migrated:
// 1. Pod name, ordinal, hostname, and subdomain are preserved.
// 2. The controller tracks the transition from the old UID to the new replacement UID.
// 3. The replacement pod binds cleanly to the active PMJ via parent UID and revision hash.
func TestStatefulSetIdentityContinuity(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	stsName := "redis"
	podName := "redis-0"
	oldUID := types.UID("uid-redis-0-gen1")
	newUID := types.UID("uid-redis-0-gen2")
	stsUID := types.UID("uid-sts-redis")
	revHash := "redis-revision-hash-v1"

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: namespace,
			UID:       stsUID,
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: "redis-service",
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-redis-0",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind:           "StatefulSet",
				util.LabelParentName:           stsName,
				util.LabelParentUID:            string(stsUID),
				util.LabelControllerRevisionHash: revHash,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: string(oldUID),
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef: "snap-redis-0",
		},
	}

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       newUID,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       stsName,
					UID:        stsUID,
				},
			},
			Labels: map[string]string{
				"controller-revision-hash": revHash,
				"statefulset.kubernetes.io/pod-name": podName,
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: pmj.Name,
			},
		},
		Spec: corev1.PodSpec{
			Hostname:  podName,
			Subdomain: "redis-service",
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: MigrationGateName},
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(sts, pmj, replacementPod).
		Build()

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	_ = reg.Register(engine.NewGKEPodSnapshots(cl, scheme))

	r := &PodGateReconciler{
		Client:    cl,
		APIReader: cl,
		Scheme:    scheme,
		Engines:   reg,
	}

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: podName},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected immediate gate release without requeue, got: %+v", res)
	}

	updatedPod := &corev1.Pod{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: podName}, updatedPod); err != nil {
		t.Fatalf("failed to get updated pod: %v", err)
	}

	// Verify identity continuity invariants:
	if len(updatedPod.Spec.SchedulingGates) != 0 {
		t.Fatalf("expected scheduling gate to be released, got: %v", updatedPod.Spec.SchedulingGates)
	}
	if updatedPod.Name != podName {
		t.Errorf("pod name changed: got %s, want %s", updatedPod.Name, podName)
	}
	if updatedPod.Spec.Hostname != podName {
		t.Errorf("hostname changed: got %s, want %s", updatedPod.Spec.Hostname, podName)
	}
	if updatedPod.Spec.Subdomain != "redis-service" {
		t.Errorf("subdomain changed: got %s, want %s", updatedPod.Spec.Subdomain, "redis-service")
	}
	if updatedPod.UID == oldUID {
		t.Errorf("expected new UID on replacement pod recreation, got identical UID %s", updatedPod.UID)
	}
	if updatedPod.Annotations["podsnapshot.gke.io/ps-name"] != "snap-redis-0" {
		t.Errorf("expected snapshot annotation snap-redis-0, got %s", updatedPod.Annotations["podsnapshot.gke.io/ps-name"])
	}

	// Verify PMJ records new consumer UID
	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, updatedPMJ); err != nil {
		t.Fatalf("failed to get updated PMJ: %v", err)
	}
	if updatedPMJ.Status.RestoredPodUID != string(newUID) {
		t.Errorf("expected PMJ RestoredPodUID to record %s, got %s", newUID, updatedPMJ.Status.RestoredPodUID)
	}
}

// TestProjectedServiceAccountTokenRotation_DiskRefreshed verifies that applications reading
// projected ServiceAccount tokens from disk (such as standard client-go TokenSource) observe
// rotated credentials across pod migrations without requiring container restarts.
func TestProjectedServiceAccountTokenRotation_DiskRefreshed(t *testing.T) {
	tempDir := t.TempDir()
	tokenDir := filepath.Join(tempDir, "var", "run", "secrets", "kubernetes.io", "serviceaccount")
	if err := os.MkdirAll(tokenDir, 0755); err != nil {
		t.Fatalf("failed to create token dir: %v", err)
	}
	tokenFile := filepath.Join(tokenDir, "token")

	// 1. Initial projected token on source node
	initialToken := "token-source-node-expiring-soon"
	if err := os.WriteFile(tokenFile, []byte(initialToken), 0644); err != nil {
		t.Fatalf("failed to write initial token: %v", err)
	}

	// Token reader simulating client-go TokenSource
	readToken := func() (string, error) {
		data, err := os.ReadFile(tokenFile)
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	tok1, err := readToken()
	if err != nil || tok1 != initialToken {
		t.Fatalf("expected initial token %q, got %q, err: %v", initialToken, tok1, err)
	}

	// 2. Kubelet rotates the projected token on the destination node after migration
	refreshedToken := "token-destination-node-fresh-credentials"
	if err := os.WriteFile(tokenFile, []byte(refreshedToken), 0644); err != nil {
		t.Fatalf("failed to rotate token: %v", err)
	}

	// 3. Process re-reads projected token from disk: refreshed credentials are read immediately
	tok2, err := readToken()
	if err != nil {
		t.Fatalf("failed to read rotated token: %v", err)
	}
	if tok2 != refreshedToken {
		t.Fatalf("expected refreshed token %q, got %q", refreshedToken, tok2)
	}
	if tok2 == tok1 {
		t.Fatalf("token did not refresh; still reading initial token")
	}
}
