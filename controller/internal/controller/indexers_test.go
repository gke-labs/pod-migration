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

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
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

func TestPMJParentKeyIndex_ListsOnlyMatchingPMJs(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)

	deployPMJ1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-deploy-1",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "my-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
	}
	deployPMJ2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-deploy-2",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "my-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
	}
	otherDeployPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-other-deploy",
			Namespace: "default",
			Labels: map[string]string{
				util.LabelParentName: "other-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
	}
	barePodPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-bare-pod",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "my-bare-pod"},
		},
	}
	unindexablePMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-unindexable",
			Namespace: "default",
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJParentKeyIndex, PMJParentKeyIndexValue).
		WithObjects(deployPMJ1, deployPMJ2, otherDeployPMJ, barePodPMJ, unindexablePMJ).
		Build()

	// Query for my-deploy
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	err := cl.List(context.Background(), pmjList,
		client.InNamespace("default"),
		client.MatchingFields{PMJParentKeyIndex: "my-deploy/Deployment"})
	if err != nil {
		t.Fatalf("indexed list for my-deploy/Deployment failed: %v", err)
	}
	if len(pmjList.Items) != 2 {
		t.Fatalf("expected exactly 2 PMJs for my-deploy/Deployment, got %d", len(pmjList.Items))
	}
	found := make(map[string]bool)
	for _, item := range pmjList.Items {
		found[item.Name] = true
	}
	if !found["pmj-deploy-1"] || !found["pmj-deploy-2"] {
		t.Errorf("expected pmj-deploy-1 and pmj-deploy-2, got %v", pmjList.Items)
	}

	// Query for bare pod
	bareList := &pmv1alpha1.PodMigrationJobList{}
	err = cl.List(context.Background(), bareList,
		client.InNamespace("default"),
		client.MatchingFields{PMJParentKeyIndex: "my-bare-pod/Pod"})
	if err != nil {
		t.Fatalf("indexed list for my-bare-pod/Pod failed: %v", err)
	}
	if len(bareList.Items) != 1 || bareList.Items[0].Name != "pmj-bare-pod" {
		t.Fatalf("expected exactly pmj-bare-pod, got %v", bareList.Items)
	}

	// Query for nonexistent parent
	emptyList := &pmv1alpha1.PodMigrationJobList{}
	err = cl.List(context.Background(), emptyList,
		client.InNamespace("default"),
		client.MatchingFields{PMJParentKeyIndex: "nonexistent/Deployment"})
	if err != nil {
		t.Fatalf("indexed list for nonexistent failed: %v", err)
	}
	if len(emptyList.Items) != 0 {
		t.Fatalf("expected 0 PMJs for nonexistent, got %d", len(emptyList.Items))
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
	if got := PMJParentKeyIndexValue(nil); got != nil {
		t.Errorf("expected nil for untyped nil PMJ, got %v", got)
	}
	if got := PMJSnapshotRefIndexValue(nil); got != nil {
		t.Errorf("expected nil for untyped nil PMJ snapshotRef, got %v", got)
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
	var nilPMJ *pmv1alpha1.PodMigrationJob
	if got := PMJParentKeyIndexValue(nilPMJ); got != nil {
		t.Errorf("expected nil for typed nil PMJ, got %v", got)
	}
	if got := PMJSnapshotRefIndexValue(nilPMJ); got != nil {
		t.Errorf("expected nil for typed nil PMJ snapshotRef, got %v", got)
	}
}
