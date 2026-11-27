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

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
			RuntimeClassName: ptr.To("gvisor"),
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
			RuntimeClassName: ptr.To("gvisor"),
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

// TestCollisionResolution_DeploymentPathAndPodTemplateHash verifies that in the Deployment path,
// a contender with a mismatched pod-template-hash is filtered out so the genuine replacement
// with matching hash wins, even if the mismatched contender was created earlier.
func TestCollisionResolution_DeploymentPathAndPodTemplateHash(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

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

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 created earlier at t0, but with old/mismatched pod-template-hash
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-canary",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"app":                                  "web",
				appsv1.DefaultDeploymentUniqueLabelKey: "hash-old",
				util.LabelPodMigrationEnabled:          "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
			OwnerReferences: rsOwner,
		},
	}

	// Pod 2 created later at t1, with matching pod-template-hash
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-target",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"app":                                  "web",
				appsv1.DefaultDeploymentUniqueLabelKey: "hash-target",
				util.LabelPodMigrationEnabled:          "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
			OwnerReferences: rsOwner,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-deploy",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelParentUID:       "deploy-uid-123",
				util.LabelPodTemplateHash: "hash-target",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src"},
			TargetPodUID: "src-uid",
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "web-deploy",
					UID:        "deploy-uid-123",
					Controller: &isController,
				},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rs, pmj).Build()

	// Pod 2 (matching hash) reconciles. Pod 1 must be filtered out, so Pod 2 is the winner.
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed {
		t.Fatalf("expected pod2 to win and remain unchanged, got changed=true, corrected=%q", corrected)
	}
	if corrected != "pmj-deploy" {
		t.Fatalf("expected pod2 to keep pmj-deploy, got %q", corrected)
	}

	// Pod 1 (mismatched hash) reconciles. It must be cleared.
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared (changed=true, corrected=\"\"), got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_StatefulSet verifies that a StatefulSet replacement pod with
// matching pod name wins collision over an earlier pod from the same StatefulSet with a different name.
func TestCollisionResolution_StatefulSet(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isController := true
	stsOwner := []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
			Name:       "redis",
			UID:        "sts-uid-123",
			Controller: &isController,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-redis-0",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "redis",
				util.LabelParentKind: "StatefulSet",
				util.LabelParentUID:  "sts-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "redis-0"},
			TargetPodUID: "redis-0-src-uid",
		},
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 (redis-1) created earlier at t0, but wrong pod name for this PMJ
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "redis-1",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"app":                         "redis",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-redis-0"},
			OwnerReferences: stsOwner,
		},
	}

	// Pod 2 (redis-0) created later at t1, matching target pod name
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "redis-0",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"app":                         "redis",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-redis-0"},
			OwnerReferences: stsOwner,
		},
	}

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis",
			Namespace: "default",
			UID:       "sts-uid-123",
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "redis"},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, sts, pmj).Build()

	// redis-0 reconciles: redis-1 is filtered out, redis-0 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-redis-0", "redis", "StatefulSet", "sts-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-redis-0" {
		t.Fatalf("expected redis-0 to win without change, got changed=%v, corrected=%q", changed, corrected)
	}

	// redis-1 reconciles: name mismatch, cleared
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-redis-0", "redis", "StatefulSet", "sts-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected redis-1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_JobIndex verifies that an indexed Job replacement pod with
// matching completion index wins collision over an earlier pod with a different index.
func TestCollisionResolution_JobIndex(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isController := true
	jobOwner := []metav1.OwnerReference{
		{
			APIVersion: "batch/v1",
			Kind:       "Job",
			Name:       "batch-job",
			UID:        "job-uid-123",
			Controller: &isController,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-job-0",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName:         "batch-job",
				util.LabelParentKind:         "Job",
				util.LabelParentUID:          "job-uid-123",
				util.LabelJobCompletionIndex: "0",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "batch-job-src"},
			TargetPodUID: "src-uid",
		},
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 has index 1, created earlier at t0
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "batch-job-1",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"job-name":                    "batch-job",
				util.LabelJobCompletionIndex:  "1",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-job-0"},
			OwnerReferences: jobOwner,
		},
	}

	// Pod 2 has index 0, created later at t1
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "batch-job-0",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"job-name":                    "batch-job",
				util.LabelJobCompletionIndex:  "0",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-job-0"},
			OwnerReferences: jobOwner,
		},
	}

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "batch-job",
			Namespace: "default",
			UID:       "job-uid-123",
		},
		Spec: batchv1.JobSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"job-name": "batch-job"},
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, job, pmj).Build()

	// Pod 2 (index 0) reconciles: pod1 filtered out, pod2 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-job-0", "batch-job", "Job", "job-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-job-0" {
		t.Fatalf("expected pod2 to win, got changed=%v, corrected=%q", changed, corrected)
	}

	// Pod 1 (index 1) reconciles: index mismatch, cleared
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-job-0", "batch-job", "Job", "job-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_OperatorReplicaKey verifies that an operator-owned replacement pod
// with matching replica key wins collision over an earlier pod with a different replica key.
func TestCollisionResolution_OperatorReplicaKey(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isController := true
	opOwner := []metav1.OwnerReference{
		{
			APIVersion: "ray.io/v1",
			Kind:       "RayCluster",
			Name:       "sample-ray",
			UID:        "ray-uid-123",
			Controller: &isController,
		},
	}

	headKey := "ray.io/group=headgroup,ray.io/node-type=head"
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-ray-head",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "sample-ray",
				util.LabelParentKind: "RayCluster",
				util.LabelParentUID:  "ray-uid-123",
			},
			Annotations: map[string]string{
				util.AnnotationReplicaIdentityKey: headKey,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head"},
			TargetPodUID: "src-uid",
		},
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 is a worker pod, created earlier at t0
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sample-ray-worker-1",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"ray.io/group":                "workergroup",
				"ray.io/node-type":            "worker",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-ray-head"},
			OwnerReferences: opOwner,
		},
	}

	// Pod 2 is a head pod, created later at t1
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "sample-ray-head-1",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"ray.io/group":                "headgroup",
				"ray.io/node-type":            "head",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-ray-head"},
			OwnerReferences: opOwner,
		},
	}

	rayCluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "ray.io/v1",
			"kind":       "RayCluster",
			"metadata": map[string]interface{}{
				"name":      "sample-ray",
				"namespace": "default",
				"uid":       "ray-uid-123",
			},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rayCluster, pmj).Build()

	// Pod 2 (head pod) reconciles: worker pod1 filtered out, pod2 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-ray-head", "sample-ray", "RayCluster", "ray-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-ray-head" {
		t.Fatalf("expected pod2 to win, got changed=%v, corrected=%q", changed, corrected)
	}

	// Pod 1 (worker pod) reconciles: replica key mismatch, cleared
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-ray-head", "sample-ray", "RayCluster", "ray-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_OptInPrecedence verifies that when two pods from the same workload
// collide on a PMJ, the opted-in pod wins over an un-opted-in pod even if the un-opted-in
// pod was created earlier.
func TestCollisionResolution_OptInPrecedence(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

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

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs",
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
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 created at t0, not opted in
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-unopted",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"app": "web",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: rsOwner,
		},
	}

	// Pod 2 created at t1, opted in
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-opted",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: rsOwner,
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rs, pmj).Build()

	// Pod 2 (opted-in) reconciles: pod1 un-opted-in filtered out, pod2 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-rs", "web-rs", "ReplicaSet", "rs-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-rs" {
		t.Fatalf("expected pod2 to win, got changed=%v, corrected=%q", changed, corrected)
	}

	// Pod 1 (un-opted-in) reconciles: displaced by opted-in pod, cleared
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-rs", "web-rs", "ReplicaSet", "rs-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_ParentUIDMismatch verifies that a contender whose parent UID
// does not match the PMJ's LabelParentUID is filtered out of collision resolution.
func TestCollisionResolution_ParentUIDMismatch(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isController := true

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-original",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src"},
			TargetPodUID: "src-uid",
		},
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 belongs to a different ReplicaSet with different UID, created earlier at t0
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-stale-parent",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "web-rs",
					UID:        "rs-uid-stale",
					Controller: &isController,
				},
			},
		},
	}

	// Pod 2 belongs to the matching ReplicaSet UID, created at t1
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-matching-parent",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "web-rs",
					UID:        "rs-uid-original",
					Controller: &isController,
				},
			},
		},
	}

	rsOriginal := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-original",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rsOriginal, pmj).Build()

	// Pod 2 reconciles: pod1 filtered out by parent UID mismatch, pod2 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-rs", "web-rs", "ReplicaSet", "rs-uid-original")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-rs" {
		t.Fatalf("expected pod2 to win, got changed=%v, corrected=%q", changed, corrected)
	}

	// Pod 1 reconciles: UID mismatch, cleared
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-rs", "web-rs", "ReplicaSet", "rs-uid-stale")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_ParentKindAndNameMismatch verifies that a contender from a different
// parent kind or name is filtered out of collision resolution.
func TestCollisionResolution_ParentKindAndNameMismatch(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isController := true

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs",
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
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 belongs to a different StatefulSet parent, created earlier at t0
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "other-workload-pod",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "StatefulSet",
					Name:       "other-sts",
					UID:        "rs-uid-123",
					Controller: &isController,
				},
			},
		},
	}

	// Pod 2 belongs to the matching ReplicaSet, created at t1
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-repl",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-rs"},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "web-rs",
					UID:        "rs-uid-123",
					Controller: &isController,
				},
			},
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
	}

	otherSts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-sts",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rs, otherSts, pmj).Build()

	// Pod 2 reconciles: pod1 filtered out by parent kind/name mismatch, pod2 wins!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-rs", "web-rs", "ReplicaSet", "rs-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if changed || corrected != "pmj-rs" {
		t.Fatalf("expected pod2 to win, got changed=%v, corrected=%q", changed, corrected)
	}

	// Pod 1 reconciles: kind/name mismatch, cleared (even with matching parent UID)
	corrected1, changed1, err1 := util.ResolveCollision(ctx, cl, pod1, "pmj-rs", "other-sts", "StatefulSet", "rs-uid-123")
	if err1 != nil {
		t.Fatalf("ResolveCollision err=%v", err1)
	}
	if !changed1 || corrected1 != "" {
		t.Fatalf("expected pod1 to be cleared, got changed=%v, corrected=%q", changed1, corrected1)
	}
}

// TestCollisionResolution_OptedInLoserReassignedToAlternativePMJ verifies that when two opted-in pods
// collide on a PMJ, the loser is reassigned to an available alternative active PMJ rather than starting cold.
func TestCollisionResolution_OptedInLoserReassignedToAlternativePMJ(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

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

	// Primary PMJ collided on
	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs-1",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src-1"},
			TargetPodUID: "src-uid-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Alternative active unassigned PMJ
	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs-2",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src-2"},
			TargetPodUID: "src-uid-2",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	t0 := metav1.NewTime(time.Now().Add(-10 * time.Second))
	t1 := metav1.NewTime(time.Now())

	// Pod 1 created at t0, opted in, assigned pmj-rs-1 (winner)
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-repl-1",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: t0,
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs-1"},
			OwnerReferences: rsOwner,
		},
	}

	// Pod 2 created at t1, opted in, also assigned pmj-rs-1 (loser of collision)
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-repl-2",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: t1,
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs-1"},
			OwnerReferences: rsOwner,
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rs, pmj1, pmj2).Build()

	// Pod 2 (loser of pmj-rs-1) reconciles: must be reassigned to pmj-rs-2!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod2, "pmj-rs-1", "web-rs", "ReplicaSet", "rs-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if !changed {
		t.Fatalf("expected loser pod2 to be changed=true")
	}
	if corrected != "pmj-rs-2" {
		t.Fatalf("expected loser pod2 to be reassigned to pmj-rs-2, got %q", corrected)
	}
}

// TestCollisionResolution_ClearAndReleaseCold verifies that an ineligible contender reconciling
// alone in PodGateReconciler has its assigned-pmj annotation removed and scheduling gate released cold.
func TestCollisionResolution_ClearAndReleaseCold(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	gate := []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}}

	// Unmanaged pod carrying assigned-pmj for a managed PMJ
	unmanaged := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stray-pod",
			Namespace: "default",
			UID:       "stray-uid",
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "pmj-managed",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: gate,
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-managed",
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
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(unmanaged, pmj).Build()

	r := &PodGateReconciler{Client: cl, APIReader: cl, Scheme: scheme}

	if _, err := r.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "stray-pod"}}); err != nil {
		t.Fatalf("Reconcile err=%v", err)
	}

	updated := &corev1.Pod{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "default", Name: "stray-pod"}, updated); err != nil {
		t.Fatalf("get pod err=%v", err)
	}

	// Must be released with cold-start bypass:
	// 1. assigned-pmj annotation removed
	if _, ok := updated.Annotations[util.AnnotationAssignedPMJ]; ok {
		t.Fatalf("expected assigned-pmj annotation removed, got %v", updated.Annotations)
	}
	// 2. ps-name cleared
	if updated.Annotations["podsnapshot.gke.io/ps-name"] != "" {
		t.Fatalf("expected ps-name cleared, got %q", updated.Annotations["podsnapshot.gke.io/ps-name"])
	}
	// 3. Scheduling gate removed
	if len(updated.Spec.SchedulingGates) != 0 {
		t.Fatalf("expected scheduling gates removed, got %v", updated.Spec.SchedulingGates)
	}
}

// TestCollisionResolution_NonControllerRefsAndDirectDeploymentRejected verifies that
// owner references with controller: false, direct Deployment controller references,
// or owner selector mismatches fail ResolveParentWorkload and are cleared as ineligible contenders.
func TestCollisionResolution_NonControllerRefsAndDirectDeploymentRejected(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = batchv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	t.Run("ReplicaSet owner ref with controller: false is rejected", func(t *testing.T) {
		notController := false
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-pod",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "web",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "web-rs",
						UID:        "rs-uid",
						Controller: &notController,
					},
				},
			},
		}
		isCtrl := true
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-rs",
				Namespace: "default",
				UID:       "rs-uid",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "web-deploy",
						UID:        "deploy-uid",
						Controller: &isCtrl,
					},
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-deploy",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "web-deploy",
					util.LabelParentKind: "Deployment",
					util.LabelParentUID:  "deploy-uid",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rs, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected pod with controller: false to be cleared, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("ReplicaSet Deployment ref with controller: false is rejected", func(t *testing.T) {
		isCtrl := true
		notCtrl := false
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-pod",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "web",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "web-rs",
						UID:        "rs-uid",
						Controller: &isCtrl,
					},
				},
			},
		}
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-rs",
				Namespace: "default",
				UID:       "rs-uid",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "web-deploy",
						UID:        "deploy-uid",
						Controller: &notCtrl,
					},
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-deploy",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "web-deploy",
					util.LabelParentKind: "Deployment",
					util.LabelParentUID:  "deploy-uid",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rs, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected RS with controller: false Deployment owner to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("StatefulSet ref with controller: false is rejected", func(t *testing.T) {
		notCtrl := false
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "redis",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-sts"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "redis",
						UID:        "sts-uid",
						Controller: &notCtrl,
					},
				},
			},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis",
				Namespace: "default",
				UID:       "sts-uid",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-sts",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "redis",
					util.LabelParentKind: "StatefulSet",
					util.LabelParentUID:  "sts-uid",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "redis-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, sts, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-sts", "redis", "StatefulSet", "sts-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected StatefulSet with controller: false to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Job ref with controller: false is rejected", func(t *testing.T) {
		notCtrl := false
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"job-name": "batch-job",
					"batch.kubernetes.io/job-completion-index": "0",
					util.LabelPodMigrationEnabled:              "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-job"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       "batch-job",
						UID:        "job-uid",
						Controller: &notCtrl,
					},
				},
			},
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job",
				Namespace: "default",
				UID:       "job-uid",
			},
			Spec: batchv1.JobSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-name": "batch-job"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-job",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName:                       "batch-job",
					util.LabelParentKind:                       "Job",
					util.LabelParentUID:                        "job-uid",
					"batch.kubernetes.io/job-completion-index": "0",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "batch-job-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, job, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-job", "batch-job", "Job", "job-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Job with controller: false to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Direct Deployment controller ref on pod is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "fake-deploy-pod",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "web",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "web-deploy",
						UID:        "deploy-uid",
						Controller: &isCtrl,
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-deploy",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "web-deploy",
					util.LabelParentKind: "Deployment",
					util.LabelParentUID:  "deploy-uid",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected direct Deployment controller ref to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Owner selector mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-pod",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "wrong-app",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-deploy"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "web-rs",
						UID:        "rs-uid",
						Controller: &isCtrl,
					},
				},
			},
		}
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "web-rs",
				Namespace: "default",
				UID:       "rs-uid",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "web-deploy",
						UID:        "deploy-uid",
						Controller: &isCtrl,
					},
				},
			},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-deploy",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "web-deploy",
					util.LabelParentKind: "Deployment",
					util.LabelParentUID:  "deploy-uid",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rs, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-deploy", "web-deploy", "Deployment", "deploy-uid")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected selector mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("StatefulSet owner UID mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "redis",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-sts"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "redis",
						UID:        "sts-uid-stale",
						Controller: &isCtrl,
					},
				},
			},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis",
				Namespace: "default",
				UID:       "sts-uid-live",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-sts",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "redis",
					util.LabelParentKind: "StatefulSet",
					util.LabelParentUID:  "sts-uid-live",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "redis-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, sts, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-sts", "redis", "StatefulSet", "sts-uid-live")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected STS UID mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("StatefulSet owner selector mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "wrong-app",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-sts"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "StatefulSet",
						Name:       "redis",
						UID:        "sts-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		sts := &appsv1.StatefulSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "redis",
				Namespace: "default",
				UID:       "sts-uid-123",
			},
			Spec: appsv1.StatefulSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "redis"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-sts",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "redis",
					util.LabelParentKind: "StatefulSet",
					util.LabelParentUID:  "sts-uid-123",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "redis-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, sts, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-sts", "redis", "StatefulSet", "sts-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected STS selector mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Job owner UID mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"job-name": "batch-job",
					"batch.kubernetes.io/job-completion-index": "0",
					util.LabelPodMigrationEnabled:              "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-job"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       "batch-job",
						UID:        "job-uid-stale",
						Controller: &isCtrl,
					},
				},
			},
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job",
				Namespace: "default",
				UID:       "job-uid-live",
			},
			Spec: batchv1.JobSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-name": "batch-job"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-job",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName:                       "batch-job",
					util.LabelParentKind:                       "Job",
					util.LabelParentUID:                        "job-uid-live",
					"batch.kubernetes.io/job-completion-index": "0",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "batch-job-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, job, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-job", "batch-job", "Job", "job-uid-live")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Job UID mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Job owner selector mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job-0",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"job-name": "wrong-job",
					"batch.kubernetes.io/job-completion-index": "0",
					util.LabelPodMigrationEnabled:              "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-job"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "batch/v1",
						Kind:       "Job",
						Name:       "batch-job",
						UID:        "job-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "batch-job",
				Namespace: "default",
				UID:       "job-uid-123",
			},
			Spec: batchv1.JobSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"job-name": "batch-job"}},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-job",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName:                       "batch-job",
					util.LabelParentKind:                       "Job",
					util.LabelParentUID:                        "job-uid-123",
					"batch.kubernetes.io/job-completion-index": "0",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "batch-job-0"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, job, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-job", "batch-job", "Job", "job-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Job selector mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Operator owner UID mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-ray-head-001",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "ray",
					"ray.io/group":                "headgroup",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-ray"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       "sample-ray",
						UID:        "ray-uid-stale",
						Controller: &isCtrl,
					},
				},
			},
		}
		rayCluster := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "ray.io/v1",
				"kind":       "RayCluster",
				"metadata": map[string]interface{}{
					"name":      "sample-ray",
					"namespace": "default",
					"uid":       "ray-uid-live",
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{"app": "ray"},
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-ray",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "sample-ray",
					util.LabelParentKind: "RayCluster",
					util.LabelParentUID:  "ray-uid-live",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rayCluster, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-ray", "sample-ray", "RayCluster", "ray-uid-live")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Operator UID mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Operator owner selector mismatch is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-ray-head-001",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "wrong-app",
					"ray.io/group":                "headgroup",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-ray"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       "sample-ray",
						UID:        "ray-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		rayCluster := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "ray.io/v1",
				"kind":       "RayCluster",
				"metadata": map[string]interface{}{
					"name":      "sample-ray",
					"namespace": "default",
					"uid":       "ray-uid-123",
				},
				"spec": map[string]interface{}{
					"selector": map[string]interface{}{
						"matchLabels": map[string]interface{}{"app": "ray"},
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-ray",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "sample-ray",
					util.LabelParentKind: "RayCluster",
					util.LabelParentUID:  "ray-uid-123",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rayCluster, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-ray", "sample-ray", "RayCluster", "ray-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Operator selector mismatch to be rejected, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Operator owner undecodable selector fails closed", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-ray-head-001",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "ray",
					"ray.io/group":                "headgroup",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-ray"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       "sample-ray",
						UID:        "ray-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		// Malformed selector field that cannot be decoded as LabelSelector or map
		rayCluster := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "ray.io/v1",
				"kind":       "RayCluster",
				"metadata": map[string]interface{}{
					"name":      "sample-ray",
					"namespace": "default",
					"uid":       "ray-uid-123",
				},
				"spec": map[string]interface{}{
					"selector": "invalid-string-selector-not-map",
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-ray",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "sample-ray",
					util.LabelParentKind: "RayCluster",
					util.LabelParentUID:  "ray-uid-123",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-src"},
				TargetPodUID: "src-uid",
			},
		}
		cl := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, rayCluster, pmj).Build()

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-ray", "sample-ray", "RayCluster", "ray-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Operator undecodable selector to fail closed, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("Forbidden error on owner Get in ResolveCollision is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-ray-head-001",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "ray",
					"ray.io/group":                "headgroup",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-ray"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       "sample-ray",
						UID:        "ray-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-ray",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "sample-ray",
					util.LabelParentKind: "RayCluster",
					util.LabelParentUID:  "ray-uid-123",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-src"},
				TargetPodUID: "src-uid",
			},
		}
		baseClient := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, pmj).Build()

		cl := interceptor.NewClient(baseClient, interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "sample-ray" {
					return apierrors.NewForbidden(schema.GroupResource{Group: "ray.io", Resource: "rayclusters"}, key.Name, nil)
				}
				return client.Get(ctx, key, obj, opts...)
			},
		})

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-ray", "sample-ray", "RayCluster", "ray-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected Forbidden owner to be rejected as ineligible contender, got changed=%v, corrected=%q", changed, corrected)
		}
	})

	t.Run("NoMatch error on owner Get in ResolveCollision is rejected", func(t *testing.T) {
		isCtrl := true
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "sample-ray-head-001",
				Namespace: "default",
				UID:       "pod-uid",
				Labels: map[string]string{
					"app":                         "ray",
					"ray.io/group":                "headgroup",
					util.LabelPodMigrationEnabled: "true",
				},
				Annotations: map[string]string{util.AnnotationAssignedPMJ: "pmj-ray"},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "ray.io/v1",
						Kind:       "RayCluster",
						Name:       "sample-ray",
						UID:        "ray-uid-123",
						Controller: &isCtrl,
					},
				},
			},
		}
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-ray",
				Namespace: "default",
				Labels: map[string]string{
					util.LabelParentName: "sample-ray",
					util.LabelParentKind: "RayCluster",
					util.LabelParentUID:  "ray-uid-123",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-src"},
				TargetPodUID: "src-uid",
			},
		}
		baseClient := fake.NewClientBuilder().WithScheme(scheme).
			WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
			WithObjects(pod, pmj).Build()

		cl := interceptor.NewClient(baseClient, interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if key.Name == "sample-ray" {
					return &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "ray.io", Kind: "RayCluster"}}
				}
				return client.Get(ctx, key, obj, opts...)
			},
		})

		corrected, changed, err := util.ResolveCollision(ctx, cl, pod, "pmj-ray", "sample-ray", "RayCluster", "ray-uid-123")
		if err != nil {
			t.Fatalf("ResolveCollision err=%v", err)
		}
		if !changed || corrected != "" {
			t.Fatalf("expected NoMatch owner to be rejected as ineligible contender, got changed=%v, corrected=%q", changed, corrected)
		}
	})
}

// TestCollisionResolution_OptedInDisplacementLoserReassigned verifies that when an un-opted-in pod
// is displaced by an opted-in contender, the displaced loser is reassigned to an available alternative
// active PMJ under the same parent rather than starting cold.
func TestCollisionResolution_OptedInDisplacementLoserReassigned(t *testing.T) {
	ctx := context.TODO()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	isCtrl := true
	rsOwner := []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       "web-rs",
			UID:        "rs-uid-123",
			Controller: &isCtrl,
		},
	}

	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs-1",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src-1"},
			TargetPodUID: "src-uid-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-rs-2",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-src-2"},
			TargetPodUID: "src-uid-2",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Pod 1 is un-opted-in (does not have pod-migration.gke.io/enabled: "true")
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-repl-1",
			Namespace:         "default",
			UID:               "pod1-uid",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-10 * time.Second)),
			Labels: map[string]string{
				"app": "web",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs-1"},
			OwnerReferences: rsOwner,
		},
	}

	// Pod 2 is opted in (has pod-migration.gke.io/enabled: "true")
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "web-repl-2",
			Namespace:         "default",
			UID:               "pod2-uid",
			CreationTimestamp: metav1.NewTime(time.Now()),
			Labels: map[string]string{
				"app":                         "web",
				util.LabelPodMigrationEnabled: "true",
			},
			Annotations:     map[string]string{util.AnnotationAssignedPMJ: "pmj-rs-1"},
			OwnerReferences: rsOwner,
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: "default",
			UID:       "rs-uid-123",
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
		},
	}

	cl := fake.NewClientBuilder().WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(pod1, pod2, rs, pmj1, pmj2).Build()

	// Un-opted-in pod1 reconciles: displaced by opted-in pod2, it must be reassigned to pmj-rs-2!
	corrected, changed, err := util.ResolveCollision(ctx, cl, pod1, "pmj-rs-1", "web-rs", "ReplicaSet", "rs-uid-123")
	if err != nil {
		t.Fatalf("ResolveCollision err=%v", err)
	}
	if !changed {
		t.Fatalf("expected displaced un-opted-in loser pod1 to be changed=true")
	}
	if corrected != "pmj-rs-2" {
		t.Fatalf("expected displaced loser pod1 to be reassigned to pmj-rs-2, got %q", corrected)
	}
}
