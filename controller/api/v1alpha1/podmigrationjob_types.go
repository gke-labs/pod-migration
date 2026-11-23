// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

	// OriginNodeName records the name of the node where the origin pod was running.
	// Used during the Evicting phase to scope volume detachment checks specifically
	// to the origin node.
	// +optional
	OriginNodeName string `json:"originNodeName,omitempty"`

	// OriginNodeInstanceType records the machine type of the origin node, its
	// node.kubernetes.io/instance-type label, while the job is Pending. A checkpoint
	// only restores on a node of a compatible machine type. Empty when the node
	// couldn't be read or has no such label.
	// +optional
	OriginNodeInstanceType string `json:"originNodeInstanceType,omitempty"`

	// OriginNodeKubeletVersion records the kubelet version of the origin node while
	// the job is Pending. On GKE that's the node's GKE version, which GKE Pod
	// Snapshots checks against the node it restores on. Empty when the node couldn't
	// be read.
	// +optional
	OriginNodeKubeletVersion string `json:"originNodeKubeletVersion,omitempty"`

	// CompletionTime is the timestamp when this job transitioned to a terminal phase (Succeeded/Failed).
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// SnapshottingStartTime is the timestamp when this job transitioned to the Snapshotting phase.
	// +optional
	SnapshottingStartTime *metav1.Time `json:"snapshottingStartTime,omitempty"`

	// EvictingStartTime is the timestamp when this job transitioned to the Evicting phase.
	// +optional
	EvictingStartTime *metav1.Time `json:"evictingStartTime,omitempty"`

	// RestoringStartTime is the timestamp when this job transitioned to the Restoring phase.
	// +optional
	RestoringStartTime *metav1.Time `json:"restoringStartTime,omitempty"`

	// TrafficStoppedTime is the timestamp when the source pod stopped serving traffic
	// (either when it was fenced or when its checkpoint was initiated).
	// +optional
	TrafficStoppedTime *metav1.Time `json:"trafficStoppedTime,omitempty"`

	// ReplacementReadyTime is the timestamp when the replacement pod transitioned to Ready
	// (or most recently transitioned to Ready if readiness flapped).
	// +optional
	ReplacementReadyTime *metav1.Time `json:"replacementReadyTime,omitempty"`

	// ApplicationPauseDuration is the elapsed duration between when traffic stopped on the source
	// pod and when the replacement pod first became Ready.
	// +optional
	ApplicationPauseDuration *metav1.Duration `json:"applicationPauseDuration,omitempty"`

	// Consumed indicates whether this migration snapshot has been adopted by a replacement pod.
	// If the recorded consumer pod is deleted before its scheduling gate is released (GateReleased == false),
	// a subsequent replacement pod may recover and re-adopt this PMJ. Once GateReleased is true, subsequent
	// pods will ignore this PMJ, preventing cross-generational stale state resurrection.
	// +optional
	Consumed bool `json:"consumed,omitempty"`

	// RestoredPodUID records the UID of the replacement pod that adopted this migration snapshot.
	// +optional
	RestoredPodUID string `json:"restoredPodUID,omitempty"`

	// RestoredPodName stores the metadata.name of the replacement pod that adopted this PMJ.
	// +optional
	RestoredPodName string `json:"restoredPodName,omitempty"`

	// GateReleased indicates whether the scheduling gate on the replacement pod has been
	// successfully removed. Written after the Pod update succeeds or self-healed by the PMJ controller.
	// +optional
	GateReleased bool `json:"gateReleased,omitempty"`

	// Conditions represent the latest available observations of the job's current state.
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=pmj
// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase"
// +kubebuilder:printcolumn:name="Snapshot",type="string",JSONPath=".status.snapshotRef"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

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
