package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// PodAssignedPMJIndex is the cache index key mapping pods to the PMJ named in
// their assigned-pmj annotation.  Defined in util so ResolveCollision can use
// the index without an import cycle.
const PodAssignedPMJIndex = util.PodAssignedPMJIndexKey

// PodAssignedPMJIndexValue extracts the index value for a pod.
func PodAssignedPMJIndexValue(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod == nil {
		return nil
	}
	if v := pod.Annotations[util.AnnotationAssignedPMJ]; v != "" {
		return []string{v}
	}
	return nil
}

// VolumeAttachmentPVIndex is the cache index key mapping VolumeAttachments to
// the persistent volume name they attach.
const VolumeAttachmentPVIndex = "spec.source.persistentVolumeName"

// VolumeAttachmentPVIndexValue extracts the persistent volume name for a VolumeAttachment.
func VolumeAttachmentPVIndexValue(obj client.Object) []string {
	va, ok := obj.(*storagev1.VolumeAttachment)
	if !ok || va == nil || va.Spec.Source.PersistentVolumeName == nil || *va.Spec.Source.PersistentVolumeName == "" {
		return nil
	}
	return []string{*va.Spec.Source.PersistentVolumeName}
}

// PMJSnapshotRefIndex is the cache index key mapping PodMigrationJobs to their Status.SnapshotRef.
const PMJSnapshotRefIndex = ".status.snapshotRef"

// PMJSnapshotRefIndexValue extracts the snapshotRef status value for a PodMigrationJob.
func PMJSnapshotRefIndexValue(obj client.Object) []string {
	job, ok := obj.(*pmv1alpha1.PodMigrationJob)
	if !ok || job == nil {
		return nil
	}
	if job.Status.SnapshotRef != "" {
		return []string{job.Status.SnapshotRef}
	}
	return nil
}

// RegisterFieldIndexes registers all cache indexes the controllers rely on.
// Must be called before the manager starts.
func RegisterFieldIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	if err := indexer.IndexField(ctx, &corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue); err != nil {
		return err
	}
	if err := indexer.IndexField(ctx, &storagev1.VolumeAttachment{}, VolumeAttachmentPVIndex, VolumeAttachmentPVIndexValue); err != nil {
		return err
	}
	return indexer.IndexField(ctx, &pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue)
}
