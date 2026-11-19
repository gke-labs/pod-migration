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

// Package contract pins the snapshot engine surface the gke-labs
// pod-migration orchestrator reads and writes. This is the entire engine
// interface: four unstructured CRs plus one restore annotation. Every
// accessor here mirrors a specific call site in the orchestrator (citations
// inline, paths relative to its repo's controller/ directory). If the
// orchestrator moves a field, this package — and its tests — is the single
// place that breaks.
//
// The CRs live in the engine's API group (Group), which defaults to
// DefaultGroup; SetGroup moves them to another group, and the restore
// annotation follows the group (#135).
package contract

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// GKEPodSnapshotsGroup is the API group of the managed GKE Pod
	// Snapshots add-on. An engine serving it can only run where the add-on
	// is off, because the add-on owns those CRDs.
	GKEPodSnapshotsGroup = "podsnapshot.gke.io"
	// DefaultGroup is the API group the engine serves unless SetGroup
	// picks another.
	DefaultGroup = GKEPodSnapshotsGroup
	// Version of the snapshot engine API.
	Version = "v1"

	// restoreAnnotationName is the name part of the restore order; the
	// engine's group is its prefix (RestoreAnnotation).
	restoreAnnotationName = "ps-name"

	// AnnotationAssignedPMJ marks a replacement pod's PMJ assignment
	// (replacement_webhook.go:179) — the trigger for holder injection.
	AnnotationAssignedPMJ = "pod-migration.gke.io/assigned-pmj"
	// LabelEnabled is the workload opt-in (replacement_webhook.go:65).
	LabelEnabled = "pod-migration.gke.io/enabled"
	// SchedulingGate holds replacement pods (replacement_webhook.go:171).
	SchedulingGate = "gke.io/pod-migration-gate"

	// LabelEngine partitions PSPs (and the objects the engine creates)
	// between engines; the CRIU engine only marks Ready — and only acts on —
	// policies carrying LabelEngine=EngineCRIU.
	LabelEngine = "pod-migrate.io/engine"
	EngineCRIU  = "criu"
	// LabelTriggeredBy is the label GKE Pod Snapshots puts on the PodSnapshots
	// it creates for a manual trigger, valued with the trigger's name. The
	// orchestrator's CheckpointState finds a job's snapshots by it once the
	// trigger is gone (gke_provider.go snapshotTriggeredByLabel).
	LabelTriggeredBy = "gke-pod-snapshot-triggered-by"

	// Condition types/reasons the orchestrator's PMJ controller polls on
	// PodSnapshot (podmigrationjob_controller.go:310-324): ready iff
	// Ready=True OR (Checkpoint=True AND reason=Succeeded).
	CondReady      = "Ready"
	CondCheckpoint = "Checkpoint"
	// CondScopeSingleContainer is engine-published on PSPs as a v1 scope
	// declaration.
	CondScopeSingleContainer = "ScopeSingleContainer"

	ReasonSucceeded = "Succeeded"
	// ReasonFailed = a bug or environment problem; ReasonRefused = a policy
	// outcome (multi-container, non-runc, node not criu-ready). Both degrade
	// to the orchestrator's cold-eviction path; they stay distinct for humans
	// and metrics.
	ReasonFailed  = "Failed"
	ReasonRefused = "Refused"
	ReasonPending = "Pending"
	// ReasonCancelRequested (on Checkpoint=Unknown) = the engine controller
	// saw the PodSnapshotManualTrigger deleted and asked the node agent to
	// cancel (C9, #106). Not terminal: the orchestrator keeps waiting.
	ReasonCancelRequested = "CancelRequested"
	// ReasonCancelled = the node agent abandoned the work order after its
	// trigger was deleted, and did not stop the source (C9, #106). Terminal;
	// written only once the agent's work has stopped. The orchestrator
	// treats it like Failed.
	ReasonCancelled = "Cancelled"
	// ReasonCheckpointTimedOut (on Checkpoint=Unknown) = the kubelet
	// checkpoint call timed out, so the dump may still be running and the
	// source may be frozen, past the kubelet's CRI deadline until resumed by
	// hand (#106). Deliberately not terminal, and the node agent's last word
	// on the snapshot: the orchestrator never sees it settle, so it keeps the
	// source fenced and cold-evicts it rather than unfencing a source that
	// may be frozen. Must not be "DeadlineExceeded", which the orchestrator
	// treats as a terminal failure.
	ReasonCheckpointTimedOut = "CheckpointTimedOut"

	// CondSourceFrozen indicates whether the source container was frozen by
	// the node agent before checkpointing (C4, #146, #148).
	CondSourceFrozen = "SourceFrozen"
	ReasonFrozen     = "Frozen"
	ReasonThawed     = "Thawed"

	// FencingMode declares the engine's fencing mode (C4, #146, #148).
	FencingMode = "Frozen"

	// AnnotationRestoreStatus is engine-internal: the node agent stamps it on
	// the replacement pod so the holder wrapper — which only sees the
	// downward-API annotations file — learns the restore outcome (hold vs
	// cold-start fall-through).
	AnnotationRestoreStatus = "pod-migrate.io/psengine-restore"
	RestoreStatusRestored   = "restored"
	RestoreStatusFailed     = "failed"
)

// The engine's API group and what follows from it. They hold DefaultGroup
// until SetGroup changes them. They aren't synchronized: main sets the group
// once at startup, before any client, cache or controller reads them.
var (
	// Group is the API group of the four contract CRs.
	Group = DefaultGroup

	// AnnotationPSName is the restore order on a replacement pod, with Group
	// as its prefix. The orchestrator sets it to the snapshot name
	// (gke_pod_snapshots.go:243), and to "" for a restore bypass
	// (gke_pod_snapshots.go:257, replacement_webhook.go:150) — so far only
	// for DefaultGroup, until it can target another group (#135).
	AnnotationPSName = RestoreAnnotation(DefaultGroup)

	// GroupVersionKinds of the four contract CRs, in Group.
	GVKStorageConfig = gvk(DefaultGroup, kindStorageConfig)
	GVKPolicy        = gvk(DefaultGroup, kindPolicy)
	GVKManualTrigger = gvk(DefaultGroup, kindManualTrigger)
	GVKPodSnapshot   = gvk(DefaultGroup, kindPodSnapshot)

	// GVKPodMigrationJob is the orchestrator's own job kind. Its group
	// doesn't follow Group.
	GVKPodMigrationJob = schema.GroupVersionKind{Group: "podmigration.gke.io", Version: "v1alpha1", Kind: "PodMigrationJob"}
)

// Kinds of the four contract CRs, and their CRD plurals.
const (
	kindStorageConfig = "PodSnapshotStorageConfig"
	kindPolicy        = "PodSnapshotPolicy"
	kindManualTrigger = "PodSnapshotManualTrigger"
	kindPodSnapshot   = "PodSnapshot"
)

var crdPlurals = []string{
	"podsnapshotstorageconfigs",
	"podsnapshotpolicies",
	"podsnapshotmanualtriggers",
	"podsnapshots",
}

func gvk(group, kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: group, Version: Version, Kind: kind}
}

// SetGroup sets the engine's API group; Group, AnnotationPSName and the four
// contract GVKs follow it. It rejects a group that a
// CustomResourceDefinition can't declare (ValidateGroup), and then changes
// nothing. Call it once at startup, before anything reads the contract; it
// isn't safe for concurrent use.
func SetGroup(group string) error {
	if err := ValidateGroup(group); err != nil {
		return err
	}
	Group = group
	AnnotationPSName = RestoreAnnotation(group)
	GVKStorageConfig = gvk(group, kindStorageConfig)
	GVKPolicy = gvk(group, kindPolicy)
	GVKManualTrigger = gvk(group, kindManualTrigger)
	GVKPodSnapshot = gvk(group, kindPodSnapshot)
	return nil
}

// ValidateGroup checks that group can be the API group of the contract
// CRDs: a DNS subdomain with at least one dot (apiextensions rejects a CRD
// group without one), short enough that every CRD name, <plural>.<group>,
// is a valid DNS subdomain too.
func ValidateGroup(group string) error {
	if errs := validation.IsDNS1123Subdomain(group); len(errs) > 0 {
		return fmt.Errorf("invalid API group %q: %s", group, strings.Join(errs, "; "))
	}
	if !strings.Contains(group, ".") {
		return fmt.Errorf("invalid API group %q: a CustomResourceDefinition group must contain a dot", group)
	}
	for _, name := range CRDNames(group) {
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			return fmt.Errorf("invalid API group %q: CRD name %q: %s", group, name, strings.Join(errs, "; "))
		}
	}
	return nil
}

// RestoreAnnotation returns the restore-order annotation key of an engine
// that serves group: "<group>/ps-name".
func RestoreAnnotation(group string) string {
	return group + "/" + restoreAnnotationName
}

// CRDNames returns the names of the four contract CRDs in group.
func CRDNames(group string) []string {
	names := make([]string, 0, len(crdPlurals))
	for _, plural := range crdPlurals {
		names = append(names, plural+"."+group)
	}
	return names
}

// New returns an empty unstructured object of the given contract kind.
func New(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	return u
}

// NewList returns an empty unstructured list of the given contract kind.
func NewList(gvk schema.GroupVersionKind) *unstructured.UnstructuredList {
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(schema.GroupVersionKind{Group: gvk.Group, Version: gvk.Version, Kind: gvk.Kind + "List"})
	return l
}

// ShortUID mirrors the orchestrator's util.ShortUID (first 8 chars).
func ShortUID(uid string) string {
	if len(uid) <= 8 {
		return uid
	}
	return uid[:8]
}

// PodSnapshotName is the engine's deterministic snapshot name for a pod
// instance. The orchestrator does not compute this — it learns it from
// PSMT status.snapshotCreated.name (podmigrationjob_controller.go:247) —
// so the format is ours; determinism gives PSMT-watcher idempotency.
func PodSnapshotName(podName, podUID string) string {
	return fmt.Sprintf("ps-%s-%s", podName, ShortUID(podUID))
}

// --- PodSnapshotStorageConfig (cluster-scoped) ------------------------------

// GCSLocation extracts spec.snapshotStorageConfig.gcs.{bucket,path} — the
// exact fields the orchestrator's config reconciler writes
// (podmigration_controller.go:88-98).
func GCSLocation(pssc *unstructured.Unstructured) (bucket, path string, err error) {
	bucket, found, err := unstructured.NestedString(pssc.Object, "spec", "snapshotStorageConfig", "gcs", "bucket")
	if err != nil || !found || bucket == "" {
		return "", "", fmt.Errorf("PSSC %s: missing spec.snapshotStorageConfig.gcs.bucket", pssc.GetName())
	}
	path, _, _ = unstructured.NestedString(pssc.Object, "spec", "snapshotStorageConfig", "gcs", "path")
	return bucket, path, nil
}

// --- PodSnapshotPolicy -------------------------------------------------------

// PolicyStorageConfigName returns spec.storageConfigName
// (written by podmigration_controller.go:119).
func PolicyStorageConfigName(psp *unstructured.Unstructured) (string, bool) {
	name, found, err := unstructured.NestedString(psp.Object, "spec", "storageConfigName")
	return name, err == nil && found && name != ""
}

// PolicyIsManualStop reports whether spec.triggerConfig is
// {type: manual, postCheckpoint: stop} — the two checks the eviction
// webhook applies (eviction_webhook.go:258, :319).
func PolicyIsManualStop(psp *unstructured.Unstructured) bool {
	typ, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "type")
	if err != nil || !found || typ != "manual" {
		return false
	}
	pc, found, err := unstructured.NestedString(psp.Object, "spec", "triggerConfig", "postCheckpoint")
	return err == nil && found && pc == "stop"
}

// PolicySelector parses spec.selector exactly the way the eviction webhook
// does (eviction_webhook.go:264-281): JSON round-trip into a
// metav1.LabelSelector.
func PolicySelector(psp *unstructured.Unstructured) (labels.Selector, error) {
	m, found, err := unstructured.NestedMap(psp.Object, "spec", "selector")
	if err != nil || !found {
		return nil, fmt.Errorf("PSP %s: missing spec.selector", psp.GetName())
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var ls metav1.LabelSelector
	if err := json.Unmarshal(b, &ls); err != nil {
		return nil, err
	}
	return metav1.LabelSelectorAsSelector(&ls)
}

// --- PodSnapshotManualTrigger -------------------------------------------------

// TriggerTargetPod returns spec.targetPod (written by
// podmigrationjob_controller.go:516-518).
func TriggerTargetPod(psmt *unstructured.Unstructured) (string, bool) {
	name, found, err := unstructured.NestedString(psmt.Object, "spec", "targetPod")
	return name, err == nil && found && name != ""
}

// SetTriggerSnapshotCreated sets status.snapshotCreated.name — the one PSMT
// status field the orchestrator reads (podmigrationjob_controller.go:247).
func SetTriggerSnapshotCreated(psmt *unstructured.Unstructured, snapshotName string) error {
	return unstructured.SetNestedField(psmt.Object, snapshotName, "status", "snapshotCreated", "name")
}

// TriggerSnapshotCreated reads status.snapshotCreated.name back.
func TriggerSnapshotCreated(psmt *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(psmt.Object, "status", "snapshotCreated", "name")
	return name
}

// --- Conditions on unstructured status ---------------------------------------

// Conditions decodes status.conditions from an unstructured contract object.
func Conditions(u *unstructured.Unstructured) []metav1.Condition {
	raw, found, err := unstructured.NestedSlice(u.Object, "status", "conditions")
	if err != nil || !found {
		return nil
	}
	b, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var conds []metav1.Condition
	if err := json.Unmarshal(b, &conds); err != nil {
		return nil
	}
	return conds
}

// SetCondition upserts a condition in status.conditions (apimachinery
// semantics via meta.SetStatusCondition) and writes the slice back in the
// loosely-typed form the orchestrator's polling loop consumes
// (podmigrationjob_controller.go:310-324: map with "type"/"status"/"reason").
func SetCondition(u *unstructured.Unstructured, cond metav1.Condition) error {
	conds := Conditions(u)
	meta.SetStatusCondition(&conds, cond)
	b, err := json.Marshal(conds)
	if err != nil {
		return err
	}
	var raw []interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	return unstructured.SetNestedSlice(u.Object, raw, "status", "conditions")
}

// IsSnapshotReady applies the orchestrator's exact readiness predicate
// (podmigrationjob_controller.go:310-324): Ready=True, or Checkpoint=True
// with reason Succeeded.
func IsSnapshotReady(ps *unstructured.Unstructured) bool {
	for _, c := range Conditions(ps) {
		if c.Type == CondReady && c.Status == metav1.ConditionTrue {
			return true
		}
		if c.Type == CondCheckpoint && c.Status == metav1.ConditionTrue && c.Reason == ReasonSucceeded {
			return true
		}
	}
	return false
}

// IsSourceFrozen reports whether ps has condition SourceFrozen=True.
func IsSourceFrozen(ps *unstructured.Unstructured) bool {
	for _, c := range Conditions(ps) {
		if c.Type == CondSourceFrozen && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

// SetSourceFrozen sets or clears the SourceFrozen condition on ps.
func SetSourceFrozen(ps *unstructured.Unstructured, frozen bool, reason, msg string) error {
	status := metav1.ConditionFalse
	if frozen {
		status = metav1.ConditionTrue
	}
	return SetCondition(ps, metav1.Condition{
		Type:    CondSourceFrozen,
		Status:  status,
		Reason:  reason,
		Message: msg,
	})
}
