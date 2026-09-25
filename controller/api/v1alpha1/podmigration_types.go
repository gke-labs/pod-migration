package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// StorageSpec defines the snapshot storage target configuration.
type StorageSpec struct {
	// Location is the storage location URI (e.g. gs://bucket-name/prefix-path)
	Location string `json:"location"`
}

// PodMigrationSpec defines the desired config.
type PodMigrationSpec struct {
	Storage StorageSpec `json:"storage"`

	// ExcludedPodSelectors specifies label selector requirements to exclude pods from
	// the automatically generated PodSnapshotPolicy. Only exclusion operators ('NotIn' and 'DoesNotExist')
	// are permitted. Useful when coexisting with external controllers that reconcile their own
	// PodSnapshotPolicies (e.g. Kubeflow Notebooks).
	// +optional
	// +kubebuilder:validation:XValidation:rule="self.all(e, e.operator in ['NotIn', 'DoesNotExist'])",message="excludedPodSelectors operator must be NotIn or DoesNotExist"
	// +kubebuilder:validation:XValidation:rule="self.all(e, e.key != 'pod-migration.gke.io/enabled')",message="key 'pod-migration.gke.io/enabled' is reserved and cannot be excluded"
	// +kubebuilder:validation:XValidation:rule="self.all(e, (e.operator == 'NotIn' && size(e.values) > 0) || (e.operator == 'DoesNotExist' && (!has(e.values) || size(e.values) == 0)))",message="NotIn requires non-empty values; DoesNotExist requires empty values"
	ExcludedPodSelectors []metav1.LabelSelectorRequirement `json:"excludedPodSelectors,omitempty"`
}

// PodMigrationStatus defines the observed state.
type PodMigrationStatus struct {
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty"`
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
