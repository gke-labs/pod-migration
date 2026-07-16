package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodMigrationJobSpec defines the execution targets.
type PodMigrationJobSpec struct {
	// PodRef references the pod to migrate.
	PodRef corev1.LocalObjectReference `json:"podRef"`
}

// PodMigrationJobPhase defines the current state in the lifecycle.
type PodMigrationJobPhase string

const (
	PodMigrationJobPhasePending      PodMigrationJobPhase = "Pending"
	PodMigrationJobPhaseSnapshotting PodMigrationJobPhase = "Snapshotting"
	PodMigrationJobPhaseEvicting     PodMigrationJobPhase = "Evicting"
	PodMigrationJobPhaseSucceeded    PodMigrationJobPhase = "Succeeded"
	PodMigrationJobPhaseFailed       PodMigrationJobPhase = "Failed"
)

// PodMigrationJobStatus defines the observed state.
type PodMigrationJobStatus struct {
	Phase      PodMigrationJobPhase `json:"phase,omitempty"`
	Conditions []metav1.Condition   `json:"conditions,omitempty"`
	// SnapshotRef references GKE's native PodSnapshot name.
	SnapshotRef string `json:"snapshotRef,omitempty"`
	// PVsToDetach lists the Persistent Volume names we are waiting to detach.
	// +optional
	PVsToDetach []string `json:"pvsToDetach,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"

// PodMigrationJob tracks the execution of a single Pod Migration event.
type PodMigrationJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PodMigrationJobSpec   `json:"spec,omitempty"`
	Status PodMigrationJobStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// PodMigrationJobList contains a list of PodMigrationJob.
type PodMigrationJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PodMigrationJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PodMigrationJob{}, &PodMigrationJobList{})
}
