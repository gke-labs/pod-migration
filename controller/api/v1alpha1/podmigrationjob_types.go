package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodMigrationJobSpec defines the execution targets.
type PodMigrationJobSpec struct {
	// PodRef references the pod to migrate.
	PodRef corev1.LocalObjectReference `json:"podRef"`
	// TargetPodUID is the unique UID of the pod instance being migrated.
	TargetPodUID string `json:"targetPodUID"`
}

// PodMigrationJobPhase defines the current state in the lifecycle.
type PodMigrationJobPhase string

const (
	PodMigrationJobPhasePending                 PodMigrationJobPhase = "Pending"
	PodMigrationJobPhaseSnapshotting            PodMigrationJobPhase = "Snapshotting"
	PodMigrationJobPhaseEvicting                PodMigrationJobPhase = "Evicting"
	PodMigrationJobPhaseRestoring               PodMigrationJobPhase = "Restoring"
	PodMigrationJobPhaseSucceeded               PodMigrationJobPhase = "Succeeded"
	PodMigrationJobPhaseSucceededWithoutRestore PodMigrationJobPhase = "SucceededWithoutRestore"
	PodMigrationJobPhaseFailed                  PodMigrationJobPhase = "Failed"
)

// PodMigrationJobStatus defines the observed state.
type PodMigrationJobStatus struct {
	Phase PodMigrationJobPhase `json:"phase,omitempty"`
	// SnapshotRef references the name of the created pod snapshot.
	SnapshotRef string `json:"snapshotRef,omitempty"`
	// PVsToDetach lists the Persistent Volume names we are waiting to detach.
	// +optional
	PVsToDetach []string `json:"pvsToDetach,omitempty"`
	// CompletionTime is the timestamp when this job transitioned to a terminal phase (Succeeded/Failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// Consumed indicates whether this migration snapshot has already been adopted by a replacement pod.
	// Once true, subsequent pods will ignore this PMJ, preventing cross-generational stale state resurrection.
	// +optional
	Consumed bool `json:"consumed,omitempty"`

	// RestoredPodUID records the UID of the replacement pod that adopted this migration snapshot.
	// +optional
	RestoredPodUID string `json:"restoredPodUID,omitempty"`

	// Conditions represent the latest available observations of the job's current state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
