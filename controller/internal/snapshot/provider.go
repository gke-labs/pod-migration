package snapshot

import (
	"context"
	"time"

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
	Phase            Phase
	SnapshotRef      string
	Reason           string
	Message          string
	LastProgressTime time.Time
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

// CheckpointState is what an engine reports about a source's checkpoint after
// the migration failed (#131).
type CheckpointState struct {
	// Settled is true when no checkpoint of the source is running or can
	// still start: the trigger is gone, and every snapshot it created has
	// finished or is gone.
	Settled bool
	// Completed is true when a checkpoint of the source completed. The engine
	// may then stop the source, even though the migration failed.
	Completed bool
}

// CheckpointReporter is an optional Provider capability. After a migration
// fails, the controller asks it whether the engine could still stop the
// source, to decide whether the fenced source can go back into service. The
// controller keeps the source of a provider without it fenced.
type CheckpointReporter interface {
	// CheckpointState reports the state of the job's checkpoint of podName.
	// Call it after Cleanup: a trigger that still exists can start a
	// checkpoint, so it isn't Settled.
	CheckpointState(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (CheckpointState, error)
}
