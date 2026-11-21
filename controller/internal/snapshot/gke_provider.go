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

package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// transientReadyFailureSuppressionWindow bounds how long a Ready=False (Failed)
// condition is suppressed while a constituent sub-condition (Checkpoint or
// StorageReplicated) still reports an in-progress reason (#75, #95).
// The transient race observed in #75 lasted ~337ms; 60s covers that race by two
// orders of magnitude while ensuring a hung or crashed node snapshot agent still
// fast-fails instead of waiting out the memory-scaled migration deadline.
const transientReadyFailureSuppressionWindow = 60 * time.Second

type GKEProvider struct {
	client client.Client
	scheme *runtime.Scheme
	group  string
	now    func() time.Time
}

func NewGKEProvider(c client.Client, s *runtime.Scheme) *GKEProvider {
	return &GKEProvider{client: c, scheme: s, group: DefaultAPIGroup}
}

// NewGKEProviderForGroup returns a GKEProvider bound to the four snapshot
// contract CRs in group and the matching <group>/ps-name restore annotation (#135).
func NewGKEProviderForGroup(c client.Client, s *runtime.Scheme, group string) (*GKEProvider, error) {
	if err := ValidateGroup(group); err != nil {
		return nil, err
	}
	return &GKEProvider{client: c, scheme: s, group: group}, nil
}

// APIGroup returns the API group of the snapshot CRs this provider reads and writes.
func (p *GKEProvider) APIGroup() string {
	if p == nil || p.group == "" {
		return DefaultAPIGroup
	}
	return p.group
}

// RestoreAnnotation returns the restore-order annotation key (<group>/ps-name)
// this provider stamps for cold-start bypass and restore completion.
func (p *GKEProvider) RestoreAnnotation() string {
	return RestoreAnnotation(p.APIGroup())
}

func (p *GKEProvider) nowFunc() time.Time {
	if p != nil && p.now != nil {
		return p.now()
	}
	return time.Now()
}

func (p *GKEProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(ManualTriggerGVK(p.APIGroup()))
	trigger.SetName(triggerName)
	trigger.SetNamespace(job.Namespace)
	trigger.Object["spec"] = map[string]interface{}{
		"targetPod": podName,
	}

	// Set owner reference to allow cascade deletion and ownership verification
	if err := controllerutil.SetControllerReference(job, trigger, p.scheme); err != nil {
		logger.Error(err, "Failed to set owner reference on trigger")
		return ctrl.Result{}, err
	}

	// Fetch existing trigger first to verify ownership and avoid write amplification on 1s polling loops
	existingTrigger := &unstructured.Unstructured{}
	existingTrigger.SetGroupVersionKind(trigger.GroupVersionKind())
	err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, existingTrigger)
	if err == nil {
		isOwned := false
		for _, ref := range existingTrigger.GetOwnerReferences() {
			if ref.UID == job.UID {
				isOwned = true
				break
			}
		}
		if isOwned {
			logger.Info("Trigger already exists and is owned by this job, proceeding")
			return ctrl.Result{}, nil
		}
		logger.Info("Stale trigger found (owned by another job), deleting it first")
		_ = p.client.Delete(ctx, existingTrigger)
		return ctrl.Result{Requeue: true}, nil
	} else if !apierrors.IsNotFound(err) {
		logger.Error(err, "Failed to get PodSnapshotManualTrigger")
		return ctrl.Result{}, err
	}

	// Never trigger without the cold-start bypass on the source (#151).
	sourceGone, err := p.ensureSourceColdStartBypass(ctx, job, podName)
	if err != nil {
		logger.Error(err, "Failed to set the cold-start bypass on the source pod; not creating the trigger", "pod", podName)
		return ctrl.Result{}, err
	}
	if sourceGone {
		logger.Info("Source pod not found, so nothing can restart from the snapshot; creating the trigger as before", "pod", podName)
	}

	logger.Info("Creating PodSnapshotManualTrigger")
	err = p.client.Create(ctx, trigger)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			logger.Info("Trigger already exists after create race, proceeding")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to create PodSnapshotManualTrigger")
		return ctrl.Result{}, err
	}

	logger.Info("Successfully created PodSnapshotManualTrigger")
	return ctrl.Result{}, nil
}

// ensureSourceColdStartBypass sets the cold-start bypass (ps-name: "") on the
// source pod right before its checkpoint is triggered (#151).
//
// After the checkpoint, GKE Pod Snapshots stops the source (postCheckpoint:
// stop), and the kubelet restarts it in a new sandbox. Without the bypass, the
// agent can restore that sandbox from the snapshot that was just taken, so two
// copies run from one checkpoint. With an empty ps-name the restart is cold.
// The agent reads the annotation when it creates a sandbox, so setting it on a
// running pod takes effect at the next restart and changes nothing before.
//
// The merge patch is idempotent, and it also replaces a stale snapshot name
// left on a pod that an earlier migration restored. It carries the source's
// UID as a precondition, so a pod that was replaced under the same name is never
// patched. sourceGone reports that the source no longer exists, so nothing can
// restart from the snapshot.
func (p *GKEProvider) ensureSourceColdStartBypass(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (sourceGone bool, err error) {
	return p.setColdStartBypass(ctx, job.Namespace, podName, job.Spec.TargetPodUID, job.Name)
}

// setColdStartBypass merge-patches ps-name: "" and the bypassing job's name
// onto a pod (#151, #163). A non-empty uid is sent as a precondition, so a pod
// replaced under the same name is never patched. gone reports that the pod no
// longer exists.
func (p *GKEProvider) setColdStartBypass(ctx context.Context, namespace, podName, uid, bypassingJob string) (gone bool, err error) {
	restoreAnn := p.RestoreAnnotation()
	annotations := map[string]interface{}{
		restoreAnn: "",
	}
	if bypassingJob != "" {
		annotations[util.AnnotationColdStartBypassJob] = bypassingJob
	}
	meta := map[string]interface{}{
		"annotations": annotations,
	}
	if uid != "" {
		meta["uid"] = uid
	}
	data, err := json.Marshal(map[string]interface{}{"metadata": meta})
	if err != nil {
		return false, fmt.Errorf("encoding the cold-start bypass patch: %w", err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace}}
	if err := p.client.Patch(ctx, pod, client.RawPatch(types.MergePatchType, data)); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("patching %s on pod %s/%s: %w", restoreAnn, namespace, podName, err)
	}
	return false, nil
}

func (p *GKEProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*Status, error) {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(ManualTriggerGVK(p.APIGroup()))
	err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, trigger)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Waiting for PodSnapshotManualTrigger to be created...", "trigger", triggerName)
			return &Status{
				Phase:   PhaseInProgress,
				Reason:  "Snapshotting",
				Message: "Waiting for GKE PodSnapshotManualTrigger to be created",
			}, nil
		}
		logger.Error(err, "Failed to get PodSnapshotManualTrigger")
		return nil, err
	}

	// 1. Fast-fail if GKE Snapshot Trigger (PSMT) reported a terminal failure
	psmtStatus, _ := trigger.Object["status"].(map[string]interface{})
	if psmtConditions, ok := psmtStatus["conditions"].([]interface{}); ok {
		for _, c := range psmtConditions {
			if cond, ok := c.(map[string]interface{}); ok {
				cType, _ := cond["type"].(string)
				cStatus, _ := cond["status"].(string)
				cReason, _ := cond["reason"].(string)
				if cType == "Triggered" && cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
					errMsg, _ := cond["message"].(string)
					logger.Info("PSMT reported terminal failure", "trigger", triggerName, "reason", cReason, "message", errMsg)
					return &Status{
						Phase:   phaseForTerminalReason(cReason),
						Reason:  "SnapshotTriggerFailed",
						Message: fmt.Sprintf("GKE PodSnapshotManualTrigger failed (%s): %s", cReason, errMsg),
					}, nil
				}
			}
		}
	}

	snapshotName, found, err := unstructured.NestedString(trigger.Object, "status", "snapshotCreated", "name")
	if err != nil {
		logger.Error(err, "Failed to read snapshotCreated.name from trigger status")
		return nil, err
	}
	if !found || snapshotName == "" {
		logger.Info("Waiting for snapshot name to be populated in trigger status...", "trigger", triggerName)
		return &Status{
			Phase:             PhaseInProgress,
			Reason:            "Snapshotting",
			Message:           "Waiting for GKE to populate snapshot name in trigger status",
			TriggerUnanswered: true,
		}, nil
	}

	targetSnapshot := &unstructured.Unstructured{}
	targetSnapshot.SetGroupVersionKind(PodSnapshotGVK(p.APIGroup()))
	err = p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: snapshotName}, targetSnapshot)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Waiting for PodSnapshot to be created...", "snapshot", snapshotName)
			return &Status{
				Phase:       PhaseInProgress,
				SnapshotRef: snapshotName,
				Reason:      "Snapshotting",
				Message:     fmt.Sprintf("Waiting for GKE PodSnapshot object %q to be created", snapshotName),
			}, nil
		}
		logger.Error(err, "Failed to get PodSnapshot")
		return nil, err
	}

	return checkPodSnapshotStatus(targetSnapshot, p.nowFunc(), logger.Info), nil
}

// CheckPodSnapshotStatus evaluates the progress and readiness of a cached GKE PodSnapshot
// unstructured object without issuing any API server requests.
func CheckPodSnapshotStatus(targetSnapshot *unstructured.Unstructured, now time.Time) *Status {
	return checkPodSnapshotStatus(targetSnapshot, now, nil)
}

func checkPodSnapshotStatus(targetSnapshot *unstructured.Unstructured, now time.Time, logInfo func(string, ...interface{})) *Status {
	if targetSnapshot == nil {
		return nil
	}
	snapshotName := targetSnapshot.GetName()
	snapStatus, ok := targetSnapshot.Object["status"].(map[string]interface{})
	if !ok {
		if logInfo != nil {
			logInfo("Snapshot status subresource not found, waiting...")
		}
		return &Status{
			Phase:       PhaseInProgress,
			SnapshotRef: snapshotName,
			Reason:      "Snapshotting",
			Message:     fmt.Sprintf("Waiting for GKE PodSnapshot %q status to be populated", snapshotName),
			Found:       true,
		}
	}

	rawConditions, _ := snapStatus["conditions"].([]interface{})
	conditions := make([]metav1.Condition, 0, len(rawConditions))
	for _, c := range rawConditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		cType, _ := cond["type"].(string)
		cStatus, _ := cond["status"].(string)
		cReason, _ := cond["reason"].(string)
		cMsg, _ := cond["message"].(string)
		mc := metav1.Condition{
			Type:    cType,
			Status:  metav1.ConditionStatus(cStatus),
			Reason:  cReason,
			Message: cMsg,
		}
		if lttStr, ok := cond["lastTransitionTime"].(string); ok {
			if t, err := time.Parse(time.RFC3339Nano, lttStr); err == nil {
				mc.LastTransitionTime = metav1.NewTime(t)
			}
		}
		conditions = append(conditions, mc)
	}
	topPhase, _ := snapStatus["phase"].(string)
	st := statusFromConditions(snapshotName, conditions, targetSnapshot.GetCreationTimestamp().Time, topPhase, now, logInfo)
	st = demoteDeletingSnapshot(targetSnapshot, st, logInfo)
	if st != nil {
		st.Found = true
	}
	return st
}

// demoteDeletingSnapshot keeps a PodSnapshot that's being deleted from being
// reported Ready, or as making progress (#265). Its archive is going away, so
// it can't be restored, and the job mustn't move to Evicting on it or extend
// its deadline on it. It isn't reported Failed either: the checkpoint may
// have completed, so the source must stay fenced (C4). The reason
// SnapshotDeleting lets the controller fail the job and record the fence
// decision while the snapshot can still tell whether the checkpoint
// completed. A terminal failure is still reported as it is.
func demoteDeletingSnapshot(snap *unstructured.Unstructured, st *Status, logInfo func(string, ...interface{})) *Status {
	if st == nil || snap.GetDeletionTimestamp() == nil || st.Phase == PhaseFailed || st.Phase == PhaseRefused {
		return st
	}
	if logInfo != nil {
		logInfo("PodSnapshot is being deleted, so it can't be restored", "snapshot", snap.GetName(), "reportedPhase", st.Phase)
	}
	return &Status{
		Phase:       PhaseInProgress,
		SnapshotRef: st.SnapshotRef,
		Reason:      ReasonSnapshotDeleting,
		Message:     fmt.Sprintf("PodSnapshot %q is being deleted, so it can't be restored", snap.GetName()),
	}
}

// StatusFromConditions evaluates GKE PodSnapshot status from already-extracted
// conditions without issuing any API server requests.
func StatusFromConditions(snapshotName string, conditions []metav1.Condition, creationTime time.Time, topPhase string, now time.Time) *Status {
	return statusFromConditions(snapshotName, conditions, creationTime, topPhase, now, nil)
}

func statusFromConditions(snapshotName string, conditions []metav1.Condition, creationTime time.Time, topPhase string, now time.Time, logInfo func(string, ...interface{})) *Status {
	// Scan conditions for terminal failures, progress, transition timestamps, and readiness in a single pass.
	var (
		lastProgress            time.Time
		terminalSubcond         *metav1.Condition
		readyTerminalCond       *metav1.Condition
		readyTerminalTransition time.Time
		subconditionInProgress  bool
		isReady                 bool
	)

	for i := range conditions {
		cond := &conditions[i]
		cType := cond.Type
		cStatus := string(cond.Status)
		cReason := cond.Reason

		var condTransitionTime time.Time
		if !cond.LastTransitionTime.IsZero() {
			t := cond.LastTransitionTime.Time
			condTransitionTime = t
			if t.After(lastProgress) && !isSnapshotStalledReason(cType, cReason) {
				lastProgress = t
			}
		}

		switch cType {
		case "Checkpoint", "StorageReplicated":
			if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				terminalSubcond = cond
			} else if cStatus == "False" && isSnapshotSubconditionInProgressReason(cReason) {
				subconditionInProgress = true
			}
			if cType == "Checkpoint" && cStatus == "True" && cReason == "Succeeded" {
				isReady = true
			}
		case "Ready":
			if cStatus == "True" {
				isReady = true
			} else if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				readyTerminalCond = cond
				readyTerminalTransition = condTransitionTime
			}
		}
	}
	if lastProgress.IsZero() {
		lastProgress = creationTime
	}

	// 1. If an underlying constituent sub-condition has terminally failed, fast-fail immediately.
	if terminalSubcond != nil {
		if logInfo != nil {
			logInfo("PodSnapshot reported terminal failure in sub-condition", "snapshot", snapshotName, "type", terminalSubcond.Type, "reason", terminalSubcond.Reason, "message", terminalSubcond.Message)
		}
		return &Status{
			Phase:       phaseForTerminalReason(terminalSubcond.Reason),
			SnapshotRef: snapshotName,
			Reason:      "SnapshotFailed",
			Message:     fmt.Sprintf("GKE PodSnapshot %s failed (%s): %s", terminalSubcond.Type, terminalSubcond.Reason, terminalSubcond.Message),
		}
	}

	// 2. Check Ready condition for terminal failure (#75, #95).
	// In high-concurrency waves (Issue #75), GKE PodSnapshot may briefly exhibit a transient
	// Ready=False (Failed) condition (~337ms) while Checkpoint is still InProgress or
	// StorageReplicated is AwaitingCheckpoint. To avoid prematurely aborting the PMJ on that
	// transient race while still fast-failing if the node snapshot agent genuinely hangs or
	// crashes (#95), we suppress Ready=False only while a sub-condition is in progress AND
	// Ready's own lastTransitionTime is valid and within transientReadyFailureSuppressionWindow (60s).
	// Anchoring on Ready's lastTransitionTime (rather than the sub-condition's) ensures a transient
	// Ready blip midway through a multi-minute large-pod checkpoint still gets a full 60s window.
	// Note: lastTransitionTime is stamped by the node agent's clock while now is ours
	// (cross-process comparison); 60s dwarfs normal NTP skew, so do not shrink this to a few seconds.
	if readyTerminalCond != nil {
		if now.IsZero() {
			now = time.Now()
		}
		inSuppressionWindow := !readyTerminalTransition.IsZero() &&
			now.Sub(readyTerminalTransition) <= transientReadyFailureSuppressionWindow
		if subconditionInProgress && inSuppressionWindow {
			if logInfo != nil {
				logInfo("PodSnapshot reported Ready=False failure while sub-conditions are still in progress within suppression window; ignoring transient failure",
					"snapshot", snapshotName,
					"reason", readyTerminalCond.Reason,
					"message", readyTerminalCond.Message,
					"readyLastTransitionTime", readyTerminalTransition,
					"window", transientReadyFailureSuppressionWindow,
				)
			}
		} else {
			if logInfo != nil {
				if subconditionInProgress {
					logInfo("PodSnapshot reported Ready=False failure and Ready transition exceeded suppression window; treating as terminal failure",
						"snapshot", snapshotName,
						"reason", readyTerminalCond.Reason,
						"message", readyTerminalCond.Message,
						"readyLastTransitionTime", readyTerminalTransition,
						"window", transientReadyFailureSuppressionWindow,
					)
				} else {
					logInfo("PodSnapshot reported terminal failure", "snapshot", snapshotName, "type", "Ready", "reason", readyTerminalCond.Reason, "message", readyTerminalCond.Message)
				}
			}
			return &Status{
				Phase:       phaseForTerminalReason(readyTerminalCond.Reason),
				SnapshotRef: snapshotName,
				Reason:      "SnapshotFailed",
				Message:     fmt.Sprintf("GKE PodSnapshot %s failed (%s): %s", "Ready", readyTerminalCond.Reason, readyTerminalCond.Message),
			}
		}
	}

	// 3. Top-level status.phase terminal failure check (unconditional).
	if topPhase != "" && isTerminalSnapshotFailureReason(topPhase) {
		if logInfo != nil {
			logInfo("PodSnapshot reported terminal failure via phase", "snapshot", snapshotName, "phase", topPhase)
		}
		return &Status{
			Phase:       phaseForTerminalReason(topPhase),
			SnapshotRef: snapshotName,
			Reason:      "SnapshotFailed",
			Message:     fmt.Sprintf("GKE PodSnapshot failed with phase=%s", topPhase),
		}
	}

	// 4. Check for readiness
	if isReady {
		return &Status{
			Phase:       PhaseReady,
			SnapshotRef: snapshotName,
		}
	}

	if logInfo != nil {
		logInfo("Snapshot is not ready yet, waiting...")
	}
	return &Status{
		Phase:            PhaseInProgress,
		SnapshotRef:      snapshotName,
		Reason:           "Snapshotting",
		Message:          fmt.Sprintf("Waiting for GKE PodSnapshot %q checkpoint to complete", snapshotName),
		LastProgressTime: lastProgress,
	}
}

func (p *GKEProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	logger := log.FromContext(ctx).WithValues("trigger", triggerName)

	logger.Info("Deleting manual trigger", "trigger", triggerName)
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(ManualTriggerGVK(p.APIGroup()))
	trigger.SetName(triggerName)
	trigger.SetNamespace(job.Namespace)

	err := p.client.Delete(ctx, trigger)
	if err != nil && !apierrors.IsNotFound(err) && !meta.IsNoMatchError(err) {
		logger.Error(err, "Failed to delete trigger")
		return err
	}
	logger.Info("Successfully deleted trigger")
	return nil
}

// snapshotTriggeredByLabel is the label GKE Pod Snapshots puts on every
// PodSnapshot it creates for a manual trigger. Its value is the trigger's name.
// The CRIU engine's controller sets it too, when the trigger's name fits in a
// label value (#106).
const snapshotTriggeredByLabel = "gke-pod-snapshot-triggered-by"

// CheckpointState reports whether the job's checkpoint of the source has
// settled and whether it completed (#131). It finds the job's PodSnapshots by
// the trigger's status, the snapshot recorded on the job, and the label the
// engine puts on every snapshot it creates for the trigger, so it still finds
// them after Cleanup deleted the trigger.
//
// A trigger that still exists can start a checkpoint, and a snapshot that's
// neither finished nor gone can still complete, so either one means not
// Settled. A snapshot whose Checkpoint condition is True, or that is Ready,
// is a completed checkpoint: GKE stops the source after it.
func (p *GKEProvider) CheckpointState(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (CheckpointState, error) {
	triggerName := util.FormatPSMTName(podName, job.Spec.TargetPodUID)
	state := CheckpointState{Settled: true}
	names := map[string]bool{}
	if job.Status.SnapshotRef != "" {
		names[job.Status.SnapshotRef] = true
	}

	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(ManualTriggerGVK(p.APIGroup()))
	err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: triggerName}, trigger)
	switch {
	case err == nil:
		state.Settled = false
		if name, found, _ := unstructured.NestedString(trigger.Object, "status", "snapshotCreated", "name"); found && name != "" {
			names[name] = true
		}
	case apierrors.IsNotFound(err) || meta.IsNoMatchError(err):
	default:
		return CheckpointState{}, fmt.Errorf("reading trigger %s: %w", triggerName, err)
	}

	snapshotGVK := PodSnapshotGVK(p.APIGroup())
	snapshots := map[string]*unstructured.Unstructured{}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(snapshotGVK.GroupVersion().WithKind("PodSnapshotList"))
	if err := p.client.List(ctx, list, client.InNamespace(job.Namespace), client.MatchingLabels{snapshotTriggeredByLabel: triggerName}); err != nil {
		if !meta.IsNoMatchError(err) {
			return CheckpointState{}, fmt.Errorf("listing the snapshots of trigger %s: %w", triggerName, err)
		}
	}
	for i := range list.Items {
		snapshots[list.Items[i].GetName()] = &list.Items[i]
	}
	for name := range names {
		if _, ok := snapshots[name]; ok {
			continue
		}
		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(snapshotGVK)
		if err := p.client.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: name}, snap); err != nil {
			if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
				continue
			}
			return CheckpointState{}, fmt.Errorf("reading snapshot %s: %w", name, err)
		}
		snapshots[name] = snap
	}

	for _, snap := range snapshots {
		completed, finished := podSnapshotCheckpointProgress(snap)
		if completed {
			state.Completed = true
		}
		if !finished {
			state.Settled = false
		}
	}
	return state, nil
}

// podSnapshotCheckpointProgress reports whether a PodSnapshot's checkpoint
// completed, and whether the snapshot finished: Ready, or failed terminally.
// A snapshot being deleted hasn't finished until it's gone.
func podSnapshotCheckpointProgress(snap *unstructured.Unstructured) (completed, finished bool) {
	conditions, _, _ := unstructured.NestedSlice(snap.Object, "status", "conditions")
	var readyFailed, subconditionInProgress bool
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if !ok {
			continue
		}
		cType, _ := cond["type"].(string)
		cStatus, _ := cond["status"].(string)
		cReason, _ := cond["reason"].(string)
		switch cType {
		case "Checkpoint", "StorageReplicated":
			if cType == "Checkpoint" && cStatus == "True" {
				completed = true
			}
			if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				finished = true
			} else if cStatus == "False" && isSnapshotSubconditionInProgressReason(cReason) {
				subconditionInProgress = true
			}
		case "Ready":
			if cStatus == "True" {
				completed, finished = true, true
			} else if cStatus == "False" && isTerminalSnapshotFailureReason(cReason) {
				readyFailed = true
			}
		}
	}
	// A Ready=False failure while a sub-condition is still in progress can be
	// the transient race of #75, so it alone doesn't finish the snapshot.
	if readyFailed && !subconditionInProgress {
		finished = true
	}
	if phase, _, _ := unstructured.NestedString(snap.Object, "status", "phase"); isTerminalSnapshotFailureReason(phase) {
		finished = true
	}
	if snap.GetDeletionTimestamp() != nil {
		finished = false
	}
	return completed, finished
}

// isTerminalSnapshotFailureReason checks if a condition reason indicates that the snapshot operation
// has terminally failed and will not recover or complete.
// Note on retries (#21): When a snapshot provider emits a terminal failure reason (such as Checkpoint failed or
// DeadlineExceeded), retrying trigger creation within the same PMJ prolongs migration disruption and risks
// double-checkpointing an actively degrading pod; failing fast allows the controller or workload controller
// to cleanly initiate fallback or reschedule rather than burning the migration deadline on known terminal states.
// Refused means the snapshot engine declined to checkpoint this pod (for example, a pod shape it does not
// support). That is a policy decision that will not change within the same PMJ, so it is terminal as well.
// Cancelled means the engine abandoned the checkpoint because its trigger was deleted, without stopping the
// source (C9, #106). It is handled as Failed.
func isTerminalSnapshotFailureReason(reason string) bool {
	r := strings.ToLower(reason)
	if r == "" || r == "noerror" || strings.HasPrefix(r, "not") || strings.HasPrefix(r, "non") {
		return false
	}
	switch r {
	case "failed", "error", "deadlineexceeded", "refused", "cancelled":
		return true
	}
	return false
}

// isSnapshotStalledReason: a CRIU engine condition that isn't progress, so
// its lastTransitionTime mustn't extend the job's deadline (#106). A
// checkpoint that timed out won't move again, and a cancel waits for the node
// agent to confirm it.
func isSnapshotStalledReason(cType, reason string) bool {
	return cType == "Checkpoint" && (reason == "CheckpointTimedOut" || reason == "CancelRequested")
}

func isSnapshotRefusedReason(reason string) bool {
	return strings.EqualFold(strings.TrimSpace(reason), "refused")
}

func phaseForTerminalReason(reason string) Phase {
	if isSnapshotRefusedReason(reason) {
		return PhaseRefused
	}
	return PhaseFailed
}

// isSnapshotSubconditionInProgressReason checks if a sub-condition reason indicates that the
// underlying checkpoint or storage replication operation is actively in progress (#75).
func isSnapshotSubconditionInProgressReason(reason string) bool {
	r := strings.ToLower(strings.TrimSpace(reason))
	switch r {
	case "inprogress", "awaitingcheckpoint", "pending":
		return true
	}
	return false
}

// ReasonSnapshotDeleting is the reason of the InProgress status reported for
// a PodSnapshot that's being deleted (#265).
const ReasonSnapshotDeleting = "SnapshotDeleting"

// ReasonSnapshotGone is the reason with which the controller records that a
// job's PodSnapshot, seen before, no longer exists (#269).
const ReasonSnapshotGone = "SnapshotGone"

// PodSnapshotUnrestorable reads the PodSnapshot name, in group, and reports
// why it can't be restored: ReasonSnapshotDeleting while it's being deleted
// (#265), ReasonSnapshotGone when it doesn't exist (#269), or "" otherwise.
// A group whose PodSnapshot CRD isn't installed reports "", so callers keep
// their behavior for it. Read it with an API reader: a cached miss can be a
// lagging informer. NotFound only means gone to a caller that knows the
// snapshot existed; one can also not be created yet.
func PodSnapshotUnrestorable(ctx context.Context, r client.Reader, group, namespace, name string) (string, error) {
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(PodSnapshotGVK(group))
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, snap); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			return ReasonSnapshotGone, nil
		case meta.IsNoMatchError(err):
			return "", nil
		}
		return "", fmt.Errorf("reading snapshot %s/%s: %w", namespace, name, err)
	}
	if snap.GetDeletionTimestamp() != nil {
		return ReasonSnapshotDeleting, nil
	}
	return "", nil
}

// PodSnapshotBeingDeleted reports whether the PodSnapshot name, in group, is
// being deleted: its deletionTimestamp is set, and a finalizer (the CRIU
// engine controller's archive cleanup, or GKE's own) holds it while its
// archive goes away (#265). Such a snapshot must never be restored. A snapshot
// that's gone, or a group whose PodSnapshot CRD isn't installed, isn't
// reported as being deleted, so callers keep their behavior for those.
func PodSnapshotBeingDeleted(ctx context.Context, r client.Reader, group, namespace, name string) (bool, error) {
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(PodSnapshotGVK(group))
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, snap); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return false, nil
		}
		return false, fmt.Errorf("reading snapshot %s/%s: %w", namespace, name, err)
	}
	return snap.GetDeletionTimestamp() != nil, nil
}
