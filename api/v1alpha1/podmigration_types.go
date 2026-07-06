package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageSpec defines GCS storage target configuration.
type StorageSpec struct {
	// Location is the GCS location URI (e.g. gs://bucket-name/prefix-path)
	Location string `json:"location"`
	// FolderManagement specifies how the target snapshot folders are managed.
	// +optional
	FolderManagement string `json:"folderManagement,omitempty"`
}

// PodMigrationSpec defines the desired config.
type PodMigrationSpec struct {
	Storage StorageSpec `json:"storage"`
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
