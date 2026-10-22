package util

const (
	// LabelFenced marks a migration source whose readiness the fence webhook
	// holds at False, so the source stays out of its Services' endpoints (#131).
	LabelFenced = "pod-migration.gke.io/fenced"

	// ReasonPodMigrationFenced is the reason on the Ready condition that the
	// fence webhook holds at False.
	ReasonPodMigrationFenced = "PodMigrationFenced"

	// FenceReadyMessage is the message on the Ready condition that the fence
	// holds at False.
	FenceReadyMessage = "Readiness is held False while this pod is a migration source (label " +
		LabelFenced + "); the ContainersReady condition shows the kubelet's view."

	// ConditionSourceFenced is the PodMigrationJob condition that records the
	// source's fence (#131). True means the source carries LabelFenced and is
	// held NotReady; ReasonFenced while the migration runs, ReasonFenceKept
	// once a failed migration decided to keep it fenced. False with
	// ReasonUnfenced means a failed migration returned it to service.
	ConditionSourceFenced = "SourceFenced"

	// ReasonFenced is the SourceFenced reason while the migration runs.
	ReasonFenced = "Fenced"
	// ReasonFenceKept is the SourceFenced reason when a failed migration kept
	// the source fenced, because the engine may have stopped it or may still
	// stop it.
	ReasonFenceKept = "FenceKept"
	// ReasonUnfenced is the SourceFenced reason when a failed migration
	// returned the source to service.
	ReasonUnfenced = "Unfenced"

	// AnnotationFenceBaseline on a PodMigrationJob records the source's
	// containers (IDs and restart counts) when the source was fenced, before
	// its checkpoint was triggered. After a failure, a source whose containers
	// still match never stopped.
	AnnotationFenceBaseline = "pod-migration.gke.io/fence-baseline"
)
