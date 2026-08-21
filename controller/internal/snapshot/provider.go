package snapshot

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// Phase represents the current state of a snapshot operation.
type Phase string

const (
	PhaseInProgress Phase = "InProgress"
	PhaseReady      Phase = "Ready"
	PhaseFailed     Phase = "Failed"
)

// Status represents the observed result of a snapshot check.
type Status struct {
	Phase       Phase
	SnapshotRef string
	Reason      string
	Message     string
}

// Provider abstracts the snapshot / checkpoint engine (e.g. GKE Pod Snapshots, CRIU, MicroVM).
type Provider interface {
	// EnsureTrigger creates or verifies the snapshot trigger for the origin pod.
	EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error)

	// CheckStatus checks whether the snapshot is ready, still in progress, or failed.
	CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*Status, error)

	// Cleanup deletes any trigger or temporary metadata created for the snapshot.
	Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error
}
