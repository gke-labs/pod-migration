package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageSpec defines GCS storage target configuration.
type StorageSpec struct {
	// Location is the GCS location URI (e.g. gs://bucket-name/prefix-path)
	Location string `json:"location"`
	// AutoProvision enables controller-driven folder and IAM creation.
	// +optional
	AutoProvision bool `json:"autoProvision,omitempty"`
}

// MigrationPolicy defines optional checkpointing and scheduling rules.
type MigrationPolicy struct {
	// TriggerType specifies when checkpoints are taken (OnEviction, Periodic).
	// +optional
	TriggerType string `json:"triggerType,omitempty"`
	// PeriodicIntervalMinutes specifies interval for periodic backups (required if triggerType is Periodic).
	// +optional
	PeriodicIntervalMinutes int32 `json:"periodicIntervalMinutes,omitempty"`
	// PostCheckpoint defines container behavior after snapshot: Stop, Resume.
	// +optional
	PostCheckpoint string `json:"postCheckpoint,omitempty"`
	// GroupingLabels lists pod label names used to isolate and group snapshots.
	// +optional
	GroupingLabels []string `json:"groupingLabels,omitempty"`
}

// PodMigrationSpec defines the desired config.
type PodMigrationSpec struct {
	Storage         StorageSpec      `json:"storage"`
	MigrationPolicy *MigrationPolicy `json:"migrationPolicy,omitempty"`
}

// PodMigrationStatus defines the observed state.
type PodMigrationStatus struct {
	// Active determines if GKE storage config and policy have been generated.
	Active bool `json:"active,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// PodMigration is the Schema for the podmigrations configuration API.
type PodMigration struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodMigrationSpec   `json:"spec,omitempty"`
	Status PodMigrationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PodMigrationList contains a list of PodMigration.
type PodMigrationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodMigration `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PodMigration{}, &PodMigrationList{})
}
