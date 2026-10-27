package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// RestoreCompleter is an optional Provider capability. Once a migration's
// restore is verified, the controller calls it, so that later restarts of the
// replacement start cold instead of restoring the migration snapshot again
// (#152). A provider without it leaves the replacement as it is.
type RestoreCompleter interface {
	// CompleteRestore is called with the replacement the job restored. It
	// must be idempotent, because the controller calls it again until it
	// succeeds. A replacement that's gone, or whose name now belongs to
	// another pod, needs nothing.
	CompleteRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string, podUID types.UID) error
}

// CompleteRestore sets the cold-start bypass (ps-name: "") and records the
// durable restore-completed marker (pod-migration.gke.io/restore-completed: <snapshotRef>)
// on a replacement whose restore was verified (#152). The pod gate set ps-name
// to the migration snapshot for the restore, and the agent reads it whenever
// it creates the pod's sandbox. So a later sandbox restart, such as a sandbox
// crash or a containerd restart, would restore that snapshot again: the app
// would roll back to its state at migration time and lose what it did since.
// With the bypass, a later restart starts cold, as any other pod's does.
//
// The patch carries the replacement's UID, so a successor under the same name
// is never patched. When the patch fails, the replacement is read again: if
// it's gone or replaced, there's nothing left to do.
func (p *GKEProvider) CompleteRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string, podUID types.UID) error {
	snapRef := job.Status.SnapshotRef
	current := &corev1.Pod{}
	if err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, current); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("getting replacement pod %s/%s: %w", job.Namespace, podName, err)
	}
	if podUID != "" && current.UID != podUID {
		return nil
	}

	// Idempotent retry: already patched with the cold-start bypass and matching marker.
	if snapRef != "" && current.Annotations[psNameAnnotation] == "" && current.Annotations[util.AnnotationRestoreCompleted] == snapRef {
		return nil
	}

	// If ps-name isn't exactly SnapshotRef (including missing ps-name, ps-name="",
	// foreign snapshot, or snapRef=""), write nothing and return nil. The job
	// concludes, and I2 evaluates whether this pod was legitimately warm-restored (#152).
	if snapRef == "" || current.Annotations[psNameAnnotation] != snapRef {
		return nil
	}

	gone, err := p.setRestoreCompleted(ctx, job.Namespace, podName, string(current.UID), current.ResourceVersion, snapRef)
	if err == nil || gone {
		return nil
	}
	cur := &corev1.Pod{}
	if getErr := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, cur); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			return nil
		}
		return fmt.Errorf("completing the restore of %s/%s: %w", job.Namespace, podName, err)
	}
	if podUID != "" && cur.UID != podUID {
		return nil
	}
	return fmt.Errorf("completing the restore of %s/%s: %w", job.Namespace, podName, err)
}

// setRestoreCompleted merge-patches ps-name: "" and
// pod-migration.gke.io/restore-completed: <snapshotRef> onto a pod. A non-empty
// uid and resourceVersion are sent as preconditions, so a pod replaced under the same
// name or concurrently modified is rejected. gone reports that the pod no longer exists.
func (p *GKEProvider) setRestoreCompleted(ctx context.Context, namespace, podName, uid, resourceVersion, snapshotRef string) (gone bool, err error) {
	meta := map[string]interface{}{
		"annotations": map[string]interface{}{
			psNameAnnotation:                "",
			util.AnnotationRestoreCompleted: snapshotRef,
		},
	}
	if uid != "" {
		meta["uid"] = uid
	}
	if resourceVersion != "" {
		meta["resourceVersion"] = resourceVersion
	}
	data, err := json.Marshal(map[string]interface{}{"metadata": meta})
	if err != nil {
		return false, fmt.Errorf("encoding the restore completion patch: %w", err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace}}
	if err := p.client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, data)); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("patching %s and %s on pod %s/%s: %w", psNameAnnotation, util.AnnotationRestoreCompleted, namespace, podName, err)
	}
	return false, nil
}
