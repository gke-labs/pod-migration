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
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestAreOriginVolumesDetached(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = storagev1.AddToScheme(scheme)

	pvName := "pv-data-1"
	pvOther := "pv-data-2"
	originNode := "node-origin"
	destNode := "node-dest"

	vaOriginAttached := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-origin"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	vaOriginDetached := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-origin-detached"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	vaDestAttached := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-dest"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: destNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	vaOtherAttached := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-other"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvOther,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	t.Run("empty pvs returns true", func(t *testing.T) {
		cl := fake.NewClientBuilder().WithScheme(scheme).Build()
		detached, err := areOriginVolumesDetached(context.Background(), cl, originNode, nil)
		if err != nil || !detached {
			t.Fatalf("expected true, nil; got %v, %v", detached, err)
		}
	})

	t.Run("volume attached on origin node returns false", func(t *testing.T) {
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
			WithObjects(vaOriginAttached).
			Build()

		detached, err := areOriginVolumesDetached(context.Background(), cl, originNode, []string{pvName})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if detached {
			t.Fatalf("expected detached=false when volume is attached to origin node")
		}
	})

	t.Run("volume detached on origin node returns true", func(t *testing.T) {
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
			WithObjects(vaOriginDetached).
			Build()

		detached, err := areOriginVolumesDetached(context.Background(), cl, originNode, []string{pvName})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !detached {
			t.Fatalf("expected detached=true when volume attachment is false")
		}
	})

	t.Run("volume attached on destination node does not block detachment from origin", func(t *testing.T) {
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
			WithObjects(vaDestAttached).
			Build()

		detached, err := areOriginVolumesDetached(context.Background(), cl, originNode, []string{pvName})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !detached {
			t.Fatalf("expected detached=true because attachment is on destination node, not origin node")
		}
	})

	t.Run("multiple PVs: returns false if any PV is still attached to origin", func(t *testing.T) {
		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
			WithObjects(vaOriginDetached, vaOtherAttached).
			Build()

		detached, err := areOriginVolumesDetached(context.Background(), cl, originNode, []string{pvName, pvOther})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if detached {
			t.Fatalf("expected detached=false because pvOther is still attached")
		}
	})
}

func makeTestRegistry(cl ...runtime.Object) *engine.Registry {
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	return reg
}

func TestPodMigrationJob_VolumeDetachTimeout_ConcludesSucceededWithoutRestore(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	pvName := "pv-database-0"
	originNode := "node-a"
	now := metav1.Now()
	// Started evicting 15 minutes ago; default timeout is 10 minutes.
	evictingStart := metav1.NewTime(now.Add(-15 * time.Minute))

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pmj-test-volume-timeout",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "database-0"},
			TargetPodUID: "uid-db-0",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef:           "snap-db-0-durable",
			OriginNodeName:        originNode,
			PVsToDetach:           []string{pvName},
			EvictingStartTime:     &evictingStart,
			SnapshottingStartTime: &evictingStart,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             "WaitingForVolumeDetach",
					Message:            fmt.Sprintf("Waiting for PVs to detach: [%s]", pvName),
					ObservedGeneration: 1,
				},
			},
		},
	}

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-database"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pmj, va).
		Build()

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	_ = reg.Register(engine.NewGKEPodSnapshots(cl, scheme))

	r := &PodMigrationJobReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		DefaultMigrationTimeout: 10 * time.Minute,
		Engines:                 reg,
	}

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pmj.Namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling PMJ on volume detach timeout: %v", err)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Fatalf("expected terminal conclusion with no requeue, got: %+v", res)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pmj.Namespace, Name: pmj.Name}, updatedPMJ); err != nil {
		t.Fatalf("failed to get updated PMJ: %v", err)
	}

	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}

	readyCond := meta.FindStatusCondition(updatedPMJ.Status.Conditions, "Ready")
	if readyCond == nil || readyCond.Reason != ReasonVolumeDetachTimeout {
		t.Fatalf("expected Ready condition reason %s, got: %+v", ReasonVolumeDetachTimeout, readyCond)
	}
	// Durable snapshot reference must be preserved for operator triage.
	if updatedPMJ.Status.SnapshotRef != "snap-db-0-durable" {
		t.Fatalf("expected snapshotRef snap-db-0-durable to be preserved, got: %s", updatedPMJ.Status.SnapshotRef)
	}
}

func TestPodGate_HoldsReplacementGateUntilVolumesDetached_OnColdFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	pvName := "pv-data-cold"
	originNode := "node-source"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-cold-fallback",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "app-0"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			OriginNodeName: originNode,
			PVsToDetach:    []string{pvName},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-0",
			Namespace: "default",
			UID:       "new-replacement-uid",
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: pmj.Name,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: MigrationGateName},
			},
		},
	}

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-cold"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pmj, pod, va).
		Build()

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	_ = reg.Register(engine.NewGKEPodSnapshots(cl, scheme))

	r := &PodGateReconciler{
		Client:    cl,
		APIReader: cl,
		Scheme:    scheme,
		Engines:   reg,
	}

	// First pass: volume is still attached to origin node -> must hold gate and requeue.
	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 3*time.Second {
		t.Fatalf("expected requeue after 3s while volume is attached, got: %v", res.RequeueAfter)
	}

	checkPod := &corev1.Pod{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, checkPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if len(checkPod.Spec.SchedulingGates) == 0 {
		t.Fatalf("scheduling gate was prematurely removed while volume was still attached to origin node!")
	}

	// Second pass: volume finishes detaching.
	va.Status.Attached = false
	if err := cl.Status().Update(context.Background(), va); err != nil {
		t.Fatalf("failed to update volume attachment status: %v", err)
	}

	res, err = r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error after detachment: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected immediate completion without requeue, got: %v", res.RequeueAfter)
	}

	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, checkPod); err != nil {
		t.Fatalf("failed to get pod after detachment: %v", err)
	}
	if len(checkPod.Spec.SchedulingGates) != 0 {
		t.Fatalf("expected scheduling gate to be removed once volume detached, but gate remains")
	}
	if checkPod.Annotations["podsnapshot.gke.io/ps-name"] != "" {
		t.Fatalf("expected empty snapshot annotation on cold-start bypass, got %v", checkPod.Annotations)
	}
}

func TestPodGate_HoldsReplacementGateUntilVolumesDetached_OnWarmRestore(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	pvName := "pv-data-warm"
	originNode := "node-source-warm"

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-warm-restore",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "db-0"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef:    "snap-db-0",
			OriginNodeName: originNode,
			PVsToDetach:    []string{pvName},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "db-0",
			Namespace: "default",
			UID:       "uid-replacement-warm",
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: pmj.Name,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: MigrationGateName},
			},
		},
	}

	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-warm"},
		Spec: storagev1.VolumeAttachmentSpec{
			NodeName: originNode,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{Attached: true},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pmj, pod, va).
		Build()

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	_ = reg.Register(engine.NewGKEPodSnapshots(cl, scheme))

	r := &PodGateReconciler{
		Client:    cl,
		APIReader: cl,
		Scheme:    scheme,
		Engines:   reg,
	}

	// 1. When volume is still attached to origin node, warm restore gate is held.
	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}
	if res.RequeueAfter != 3*time.Second {
		t.Fatalf("expected 3s requeue while volume is attached to origin node, got: %v", res.RequeueAfter)
	}

	checkPod := &corev1.Pod{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, checkPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if len(checkPod.Spec.SchedulingGates) == 0 {
		t.Fatalf("warm restore gate was removed prematurely while volume was still attached to origin node!")
	}

	// 2. Once detached from origin node, gate is released and snapshot reference is injected.
	va.Status.Attached = false
	if err := cl.Status().Update(context.Background(), va); err != nil {
		t.Fatalf("failed to update volume attachment status: %v", err)
	}

	res, err = r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, checkPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if len(checkPod.Spec.SchedulingGates) != 0 {
		t.Fatalf("expected gate to be released once volume detached, got: %v", checkPod.Spec.SchedulingGates)
	}
	if checkPod.Annotations["podsnapshot.gke.io/ps-name"] != "snap-db-0" {
		t.Fatalf("expected snapshot annotation snap-db-0, got %s", checkPod.Annotations["podsnapshot.gke.io/ps-name"])
	}
}
