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
	_ = corev1.AddToScheme(scheme)
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
					Reason:             ReasonWaitingForVolumeDetach,
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

// TestVolumeDetach_VANeverDetaches_DegradesToColdStart verifies that when an origin VolumeAttachment
// never detaches through the migration deadline, the PMJ concludes as SucceededWithoutRestore / VolumeDetachTimeout,
// and the PodGateReconciler releases the replacement pod cold on the first pass without wedging.
func TestVolumeDetach_VANeverDetaches_DegradesToColdStart(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	pvName := "pv-data-never-detach"
	originNode := "node-source-stuck"
	now := metav1.Now()
	evictingStart := metav1.NewTime(now.Add(-15 * time.Minute))

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "pmj-stuck-va",
			Namespace:         "default",
			CreationTimestamp: metav1.NewTime(now.Add(-20 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "app-0"},
			TargetPodUID: "uid-app-0",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseEvicting,
			SnapshotRef:           "snap-app-0",
			OriginNodeName:        originNode,
			PVsToDetach:           []string{pvName},
			EvictingStartTime:     &evictingStart,
			SnapshottingStartTime: &evictingStart,
			Conditions: []metav1.Condition{
				{
					Type:               "Ready",
					Status:             metav1.ConditionFalse,
					Reason:             ReasonWaitingForVolumeDetach,
					Message:            fmt.Sprintf("Waiting for PVs to detach: [%s]", pvName),
					ObservedGeneration: 1,
				},
			},
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

	// VolumeAttachment remains attached on origin node indefinitely.
	va := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{Name: "va-stuck"},
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

	pmjReconciler := &PodMigrationJobReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		DefaultMigrationTimeout: 10 * time.Minute,
		Engines:                 reg,
	}

	// 1. PMJ reconciler runs: timeout exceeded -> transitions to SucceededWithoutRestore (VolumeDetachTimeout).
	pmjRes, err := pmjReconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pmj.Namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling PMJ: %v", err)
	}
	if pmjRes.Requeue || pmjRes.RequeueAfter != 0 {
		t.Fatalf("expected terminal PMJ with no requeue, got: %+v", pmjRes)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pmj.Namespace, Name: pmj.Name}, updatedPMJ); err != nil {
		t.Fatalf("failed to get updated PMJ: %v", err)
	}
	if updatedPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("expected phase %s, got %s", pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, updatedPMJ.Status.Phase)
	}

	// 2. Pod Gate reconciler runs: job is terminal -> must release replacement pod cold on first pass.
	gateReconciler := &PodGateReconciler{
		Client:    cl,
		APIReader: cl,
		Scheme:    scheme,
		Engines:   reg,
	}

	gateRes, err := gateReconciler.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling pod gate: %v", err)
	}
	if gateRes.RequeueAfter != 0 {
		t.Fatalf("expected immediate completion without requeue, got: %v", gateRes.RequeueAfter)
	}

	checkPod := &corev1.Pod{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}, checkPod); err != nil {
		t.Fatalf("failed to get pod: %v", err)
	}
	if len(checkPod.Spec.SchedulingGates) != 0 {
		t.Fatalf("scheduling gate was not removed; replacement was wedged!")
	}
	if checkPod.Annotations["podsnapshot.gke.io/ps-name"] != "" {
		t.Fatalf("expected cold-start bypass (empty ps-name annotation), got: %s", checkPod.Annotations["podsnapshot.gke.io/ps-name"])
	}
}

// TestPodMigrationJob_VolumeAnalysis_FiltersNonRWOVolumes verifies that during Pending phase,
// only RWO and RWOP PersistentVolumes are added to PVsToDetach, while RWX and ROX volumes are excluded.
func TestPodMigrationJob_VolumeAnalysis_FiltersNonRWOVolumes(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = storagev1.AddToScheme(scheme)

	namespace := "default"
	podName := "app-volumes-0"
	nodeName := "node-1"

	pvcRWO := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pvc-rwo"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-rwo",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
		},
	}
	pvcRWOP := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pvc-rwop"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-rwop",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOncePod},
		},
	}
	pvcRWX := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pvc-rwx"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-rwx",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteMany},
		},
	}
	pvcROX := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: "pvc-rox"},
		Spec: corev1.PersistentVolumeClaimSpec{
			VolumeName:  "pv-rox",
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadOnlyMany},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			UID:       "uid-app-vol",
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
			Volumes: []corev1.Volume{
				{
					Name: "vol-rwo",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcRWO.Name},
					},
				},
				{
					Name: "vol-rwop",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcRWOP.Name},
					},
				},
				{
					Name: "vol-rwx",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcRWX.Name},
					},
				},
				{
					Name: "vol-rox",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvcROX.Name},
					},
				},
			},
		},
	}

	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: nodeName,
			Labels: map[string]string{
				"node.kubernetes.io/instance-type": "e2-standard-4",
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              "pmj-volume-analysis",
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid-app-vol",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithObjects(pmj, pod, node, pvcRWO, pvcRWOP, pvcRWX, pvcROX).
		Build()

	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	_ = reg.Register(engine.NewGKEPodSnapshots(cl, scheme))

	r := &PodMigrationJobReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		DefaultMigrationTimeout: 10 * time.Minute,
		Engines:                 reg,
	}

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("unexpected error reconciling PMJ in Pending phase: %v", err)
	}

	updatedPMJ := &pmv1alpha1.PodMigrationJob{}
	if err := cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, updatedPMJ); err != nil {
		t.Fatalf("failed to get updated PMJ: %v", err)
	}

	// PVsToDetach should only contain RWO and RWOP volumes.
	expected := map[string]bool{
		"pv-rwo":  true,
		"pv-rwop": true,
	}
	if len(updatedPMJ.Status.PVsToDetach) != len(expected) {
		t.Fatalf("expected %d PVs in PVsToDetach, got %d: %v", len(expected), len(updatedPMJ.Status.PVsToDetach), updatedPMJ.Status.PVsToDetach)
	}
	for _, pv := range updatedPMJ.Status.PVsToDetach {
		if !expected[pv] {
			t.Errorf("unexpected volume %q in PVsToDetach: %v", pv, updatedPMJ.Status.PVsToDetach)
		}
	}
}
