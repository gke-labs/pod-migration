package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPodAssignedPMJIndex_ListsOnlyAssignedPods(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)

	assigned := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "assigned-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "pmj-a",
			},
		},
	}
	otherPMJ := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "other-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "pmj-b",
			},
		},
	}
	unassigned := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unassigned-pod",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(assigned, otherPMJ, unassigned).
		Build()

	podList := &corev1.PodList{}
	err := cl.List(context.Background(), podList,
		client.InNamespace("default"),
		client.MatchingFields{PodAssignedPMJIndex: "pmj-a"})
	if err != nil {
		t.Fatalf("indexed list failed: %v", err)
	}

	if len(podList.Items) != 1 {
		t.Fatalf("expected exactly 1 pod indexed under pmj-a, got %d", len(podList.Items))
	}
	if podList.Items[0].Name != "assigned-pod" {
		t.Errorf("expected assigned-pod, got %s", podList.Items[0].Name)
	}
}

func TestVolumeAttachmentPVIndex_ListsOnlyMatchingAttachments(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = storagev1.AddToScheme(scheme)

	pvTarget := "target-pv"
	pvOther := "other-pv"
	emptyPV := ""

	targetAtt1 := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-target-1",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvTarget,
			},
			NodeName: "node-1",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	targetAtt2 := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-target-2",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvTarget,
			},
			NodeName: "node-2",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: false,
		},
	}

	otherAtt := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-other",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvOther,
			},
			NodeName: "node-3",
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}

	nilPVAtt := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-nil-pv",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: nil,
			},
			NodeName: "node-4",
		},
	}

	emptyPVAtt := &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "va-empty-pv",
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "csi-driver",
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &emptyPV,
			},
			NodeName: "node-5",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue).
		WithObjects(targetAtt1, targetAtt2, otherAtt, nilPVAtt, emptyPVAtt).
		Build()

	vaList := &storagev1.VolumeAttachmentList{}
	err := cl.List(context.Background(), vaList, client.MatchingFields{VolumeAttachmentPVIndex: pvTarget})
	if err != nil {
		t.Fatalf("indexed list failed: %v", err)
	}

	if len(vaList.Items) != 2 {
		t.Fatalf("expected exactly 2 attachments indexed under target-pv, got %d", len(vaList.Items))
	}
	found := make(map[string]bool)
	for _, item := range vaList.Items {
		found[item.Name] = true
	}
	if !found["va-target-1"] || !found["va-target-2"] {
		t.Errorf("expected va-target-1 and va-target-2, got items: %v", vaList.Items)
	}
}

func TestIndexValues_NilSafety(t *testing.T) {
	// Untyped nil interface
	if got := PodAssignedPMJIndexValue(nil); got != nil {
		t.Errorf("expected nil for untyped nil pod, got %v", got)
	}
	if got := VolumeAttachmentPVIndexValue(nil); got != nil {
		t.Errorf("expected nil for untyped nil VolumeAttachment, got %v", got)
	}

	// Typed nil interface
	var nilPod *corev1.Pod
	if got := PodAssignedPMJIndexValue(nilPod); got != nil {
		t.Errorf("expected nil for typed nil pod, got %v", got)
	}
	var nilVA *storagev1.VolumeAttachment
	if got := VolumeAttachmentPVIndexValue(nilVA); got != nil {
		t.Errorf("expected nil for typed nil VolumeAttachment, got %v", got)
	}
}
