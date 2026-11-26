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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestCollisionResolution_FiltersUnmanagedAndMismatchedPods verifies that when an unmanaged
// or differently-parented pod carries an assigned-pmj annotation, collision resolution
// filters it out in favor of the genuine parent-workload replacement pod.
func TestCollisionResolution_FiltersUnmanagedAndMismatchedPods(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	t0 := metav1.NewTime(time.Now().Add(-time.Minute))
	t1 := metav1.NewTime(time.Now())
	gate := []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}}

	isController := true
	rsOwner := []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       "web-rs",
			UID:        "rs-uid-123",
			Controller: &isController,
		},
	}

	// Unmanaged pod created earlier (t0) carrying assigned-pmj, but without
	// opt-in label and without workload owner reference.
	unmanaged := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "unmanaged-pod",
			Namespace:         "default",
			UID:               "unmanaged-uid",
			CreationTimestamp: t0,
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "pmj-src",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates:  gate,
			RuntimeClassName: stringPtr("gvisor"),
			Containers:       []corev1.Container{{Name: "a", Image: "batch-worker"}},
		},
	}

	// Genuine replacement pod created later (t1) by the ReplicaSet, opted in.
	genuine := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-abc",
			Namespace:         "default",
			UID:               "genuine-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "pmj-src",
			},
			OwnerReferences: rsOwner,
		},
		Spec: corev1.PodSpec{
			SchedulingGates:  gate,
			RuntimeClassName: stringPtr("gvisor"),
			Containers:       []corev1.Container{{Name: "a", Image: "app"}},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-src",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src"},
			TargetPodUID: "src-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef: "snap-1",
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
	}

	objs := withReadySnapshots([]client.Object{unmanaged, genuine, pmj, rs})
	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(objs...).Build()

	r := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme}

	// Reconcile unmanaged first, then genuine replacement
	for _, n := range []string{"unmanaged-pod", "web-abc"} {
		if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: n}}); err != nil {
			t.Fatalf("reconcile %s err=%v", n, err)
		}
	}

	unmanagedPod := &corev1.Pod{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "unmanaged-pod"}, unmanagedPod); err != nil {
		t.Fatalf("get unmanaged: %v", err)
	}

	genuinePod := &corev1.Pod{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "web-abc"}, genuinePod); err != nil {
		t.Fatalf("get genuine: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-src"}, updatedPMJ); err != nil {
		t.Fatalf("get pmj: %v", err)
	}

	// 1. Unmanaged pod must not receive the snapshot restore annotation
	if unmanagedPod.Annotations != nil && unmanagedPod.Annotations["podsnapshot.gke.io/ps-name"] == "snap-1" {
		t.Fatalf("Unmanaged pod incorrectly assigned restore: annotations=%v", unmanagedPod.Annotations)
	}

	// 2. Unmanaged pod must not be recorded as the restored consumer on the PMJ
	if updatedPMJ.Status.RestoredPodUID == "unmanaged-uid" || updatedPMJ.Status.RestoredPodName == "unmanaged-pod" {
		t.Fatalf("PMJ incorrectly recorded unmanaged pod as consumer: restoredUID=%s, restoredName=%s",
			updatedPMJ.Status.RestoredPodUID, updatedPMJ.Status.RestoredPodName)
	}

	// 3. Genuine replacement pod must be the winner that receives the snapshot
	if genuinePod.Annotations == nil || genuinePod.Annotations["podsnapshot.gke.io/ps-name"] != "snap-1" {
		t.Fatalf("Genuine replacement did not receive snapshot: annotations=%v", genuinePod.Annotations)
	}

	// 4. PMJ must record genuine replacement pod
	if updatedPMJ.Status.RestoredPodUID != "genuine-uid" || updatedPMJ.Status.RestoredPodName != "web-abc" {
		t.Fatalf("PMJ did not record genuine replacement: restoredUID=%s, restoredName=%s",
			updatedPMJ.Status.RestoredPodUID, updatedPMJ.Status.RestoredPodName)
	}

	if !updatedPMJ.Status.Consumed {
		t.Fatalf("PMJ was not marked consumed by genuine replacement")
	}

	// Verify that if unmanaged is reconciled again after genuine consumed the PMJ,
	// unmanaged does not re-claim the PMJ.
	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "unmanaged-pod"}}); err != nil {
		t.Fatalf("second reconcile unmanaged err=%v", err)
	}

	finalPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "pmj-src"}, finalPMJ); err != nil {
		t.Fatalf("get final pmj: %v", err)
	}
	if finalPMJ.Status.RestoredPodUID != "genuine-uid" {
		t.Fatalf("Unmanaged pod took over consumed PMJ on re-reconcile: %s", finalPMJ.Status.RestoredPodUID)
	}
}

