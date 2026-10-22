package snapshot

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
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

// CompleteRestore sets the cold-start bypass (ps-name: "") on a replacement
// whose restore was verified (#152). The pod gate set ps-name to the migration
// snapshot for the restore, and the agent reads it whenever it creates the
// pod's sandbox. So a later sandbox restart, such as a sandbox crash or a
// containerd restart, would restore that snapshot again: the app would roll
// back to its state at migration time and lose what it did since. With the
// bypass, a later restart starts cold, as any other pod's does.
//
// The patch carries the replacement's UID, so a successor under the same name
// is never patched. When the patch fails, the replacement is read again: if
// it's gone or replaced, there's nothing left to do.
func (p *GKEProvider) CompleteRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string, podUID types.UID) error {
	gone, err := p.setColdStartBypass(ctx, job.Namespace, podName, string(podUID))
	if err == nil || gone {
		return nil
	}
	current := &corev1.Pod{}
	if getErr := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: podName}, current); getErr != nil {
		if apierrors.IsNotFound(getErr) {
			return nil
		}
		return fmt.Errorf("completing the restore of %s/%s: %w", job.Namespace, podName, err)
	}
	if podUID != "" && current.UID != podUID {
		return nil
	}
	return fmt.Errorf("completing the restore of %s/%s: %w", job.Namespace, podName, err)
}
