package util

const (
	// LabelFenced marks a migration source whose readiness the fence webhook
	// holds at False, so the source stays out of its Services' endpoints (#131).
	LabelFenced = "pod-migration.gke.io/fenced"

	// ReasonPodMigrationFenced is the reason on the Ready condition that the
	// fence webhook holds at False.
	ReasonPodMigrationFenced = "PodMigrationFenced"
)
