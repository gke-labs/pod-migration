package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// PodAssignedPMJIndex is the cache index key mapping pods to the PMJ named in
// their assigned-pmj annotation.  Defined in util so ResolveCollision can use
// the index without an import cycle.
const PodAssignedPMJIndex = util.PodAssignedPMJIndexKey

// PodAssignedPMJIndexValue extracts the index value for a pod.
func PodAssignedPMJIndexValue(obj client.Object) []string {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	if v := pod.Annotations[util.AnnotationAssignedPMJ]; v != "" {
		return []string{v}
	}
	return nil
}

// RegisterFieldIndexes registers all cache indexes the controllers rely on.
// Must be called before the manager starts.
func RegisterFieldIndexes(ctx context.Context, indexer client.FieldIndexer) error {
	return indexer.IndexField(ctx, &corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue)
}
