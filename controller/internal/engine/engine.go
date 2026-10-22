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

// Package engine is how the controller reaches a checkpoint/restore engine.
//
// An Engine bundles what the controller needs from one engine: the pods it
// can serve, the policy and storage objects it needs in a namespace, its
// snapshot provider, its restore-failure classifiers and the guarantees it
// declares. A Registry holds the engines and picks one for a pod, by routing
// the pod's runtime class to an engine, or for a job, by the engine recorded
// on the job.
// The contract is docs/design/engine-contract.md (#81); the clause numbers
// in the comments (C1, C2, ...) refer to it.
//
// GKE Pod Snapshots is the first engine. It hands out the implementations
// the controller used before this package existed, so routing a call site
// through the registry changes nothing for gVisor pods.
package engine

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// OwnerUIDLabel holds the UID of the PodMigration that an engine rendered a
// cluster-scoped object for. The PodMigration reconciler deletes such an
// object only when the label is absent or matches, so a PodMigration that was
// recreated under the same name keeps its objects.
const OwnerUIDLabel = "podmigration.gke.io/owner-uid"

const (
	// AnnotationEngine names, on a PodMigrationJob, the engine that serves
	// the job (C1). The eviction webhook and the preemption trigger record
	// the engine they picked for the pod when they create the job.
	AnnotationEngine = "pod-migration.gke.io/engine"

	// AnnotationRuntimeClassEngines, on a PodMigration, routes runtime
	// classes to engines for the pods of its namespace, in ParseRoutes
	// syntax: "gvisor=gke-pod-snapshots,@default=other-engine". Each class
	// it routes overrides the controller's route for that class.
	AnnotationRuntimeClassEngines = "pod-migration.gke.io/runtime-class-engines"
)

// Engine is one checkpoint/restore engine.
type Engine interface {
	// Name is the registry key, and the name recorded on a job (C1).
	Name() string

	// RuntimeClasses lists the runtime classes the engine serves (C1).
	// eligibility.DefaultRuntimeClassToken stands for pods without a
	// runtimeClassName.
	RuntimeClasses() []string

	// NamespaceResources renders the policy and storage objects the engine
	// needs for a PodMigration, in the order they are synced (C1). A
	// storage location the engine can't use is a *StorageLocationError.
	NamespaceResources(pm *pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error)

	// Eligible reports whether the engine can migrate pod, judged from the
	// pod and its source node as the caller's cache has them. node may be
	// nil. It makes no API calls, so it fits the eviction webhook's
	// deadline (C2).
	Eligible(pod *corev1.Pod, node *corev1.Node) Verdict

	// ResolvePolicy finds the engine's policy for pod, from the cache. A nil
	// PolicyRef with a nil error means that no policy serves the pod, and
	// the migration is skipped (C2).
	ResolvePolicy(ctx context.Context, pod *corev1.Pod) (*PolicyRef, error)

	// Snapshots is the engine's snapshot provider: it creates the
	// checkpoint trigger, reads the outcome and cleans up (C1, C3, C9).
	Snapshots() snapshot.Provider

	// RestoreFailures classify a replacement pod's start failure as the
	// engine's own restore failure (C7).
	RestoreFailures() []restore.Engine

	// Guarantees are the fencing mode and restore semantics the engine
	// declares (C4, C5). Nothing enforces them yet.
	Guarantees() Guarantees
}

// Verdict is an eligibility decision. Reason explains a refusal.
type Verdict struct {
	OK     bool
	Reason string
}

// PolicyRef names the policy object that serves a pod.
type PolicyRef struct {
	Group string
	Kind  string
	Name  string
}

// FencingMode is how an engine keeps the source from serving or writing
// after the checkpoint instant (C4).
type FencingMode string

const (
	// FencingFrozen: the source is frozen at the checkpoint and never exits,
	// so the kubelet has nothing to restart. Sufficient on its own.
	FencingFrozen FencingMode = "Frozen"
	// FencingStopped: the source is stopped after the checkpoint. The
	// kubelet may restart it with empty state, so it needs the
	// controller-side endpoint fence (#131).
	FencingStopped FencingMode = "Stopped"
	// FencingNone: the source keeps running after the checkpoint.
	FencingNone FencingMode = "None"
)

// Guarantees are what an engine declares about fencing and restores.
type Guarantees struct {
	Fencing FencingMode
	// RestoreOnce: the engine restores a replacement at most once, even
	// across retries and its own restarts (C5).
	RestoreOnce bool
}

// RestoreOrderer is an optional capability (C5): the engine defines how the
// controller orders a restore into a replacement pod, and how it releases a
// replacement without one. Both methods edit pod in memory; the caller
// writes it.
type RestoreOrderer interface {
	// OrderRestore orders job's checkpoint restored into pod.
	OrderRestore(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) error
	// OrderColdStartBypass orders pod started without a restore.
	OrderColdStartBypass(pod *corev1.Pod) error
}

// StorageLocationError reports a PodMigration storage location that an
// engine can't use. The PodMigration reconciler reports it as the
// InvalidStorageLocation condition, with Message as the condition message.
type StorageLocationError struct {
	// Location is the rejected spec.storage.location.
	Location string
	// Message says what's wrong with it.
	Message string
}

func (e *StorageLocationError) Error() string { return e.Message }
