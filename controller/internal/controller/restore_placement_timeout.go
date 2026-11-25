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

package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

const (
	// ReasonNodeConstraintsAdded, on a True RestorePlacement condition: the
	// pod gate released the replacement with the snapshot engine's node
	// constraints in its required node affinity. The condition's last
	// transition time is the release, and the placement deadline counts
	// from it.
	ReasonNodeConstraintsAdded = "NodeConstraintsAdded"
	// ReasonPlacementTimeout: the replacement was released with the
	// restore's node constraints but wasn't scheduled within the placement
	// deadline, while a node that doesn't meet them could take it, or by the
	// restore ceiling. The job concludes SucceededWithoutRestore, and the
	// pod is deleted so that its ReplicaSet or StatefulSet recreates it; the
	// new pod starts cold. It's also the reason of the Warning events when a
	// job that concluded RestoreTimeout deletes such a pod later
	// (recreateUnplacedAfterRestoreTimeout); that job keeps RestoreTimeout.
	ReasonPlacementTimeout = "PlacementTimeout"
	// DefaultRestorePlacementDeadline is how long a replacement released
	// with the restore's node constraints may stay unscheduled: time for a
	// node to become Ready, to be replaced, or to be added by the cluster
	// autoscaler.
	DefaultRestorePlacementDeadline = 3 * time.Minute
	// restorePlacementCeilingMargin is how much shorter than the restore
	// ceiling the placement deadline must be, so that a replacement that a
	// node without the constraints could take is recreated at the deadline,
	// at least this long before the ceiling deletes any that is still
	// unscheduled.
	restorePlacementCeilingMargin = time.Minute
	// reasonRestoreTimeout is the reason with which the restore ceiling
	// concludes a job that has no verdict by then.
	reasonRestoreTimeout = "RestoreTimeout"
	// placementRecheckInterval is how soon a concluded job looks again at
	// its replacement, released with the restore's node constraints, while
	// that pod isn't scheduled yet: after RestoreTimeout, or after
	// PlacementTimeout when the pod got a nominated node instead of being
	// deleted, or its delete failed.
	placementRecheckInterval = 15 * time.Second
)

// ValidateRestorePlacementDeadline checks a placement deadline against the
// restore ceiling for the given stabilization window. The deadline must be
// positive, and at least restorePlacementCeilingMargin shorter than the
// ceiling: 4m30s with the default window.
func ValidateRestorePlacementDeadline(deadline, stabilization time.Duration) error {
	ceiling := restoreTimeout
	if stabilization > 0 {
		ceiling += stabilization
	}
	longest := ceiling - restorePlacementCeilingMargin
	if deadline <= 0 || deadline > longest {
		return fmt.Errorf("restore-placement-deadline %v out of range (0s, %v]: it must be at least %v shorter than the restore ceiling, %v plus the restore stabilization window",
			deadline, longest, restorePlacementCeilingMargin, restoreTimeout)
	}
	return nil
}

func (r *PodMigrationJobReconciler) restorePlacementDeadline() time.Duration {
	if r.RestorePlacementDeadline > 0 {
		return r.RestorePlacementDeadline
	}
	return DefaultRestorePlacementDeadline
}

// restorePlacementRelease returns the True RestorePlacement condition with
// which the pod gate recorded that it released job's replacement with the
// restore's node constraints.
func restorePlacementRelease(job *pmv1alpha1.PodMigrationJob) (*metav1.Condition, bool) {
	if !job.Status.GateReleased || !job.Status.Consumed || job.Status.RestoredPodName == "" || job.Status.RestoredPodUID == "" {
		return nil, false
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionRestorePlacement)
	if cond == nil || cond.Status != metav1.ConditionTrue || cond.Reason != ReasonNodeConstraintsAdded {
		return nil, false
	}
	return cond, true
}

// placementTimedOut reports whether job concluded with reason
// PlacementTimeout.
func placementTimedOut(job *pmv1alpha1.PodMigrationJob) bool {
	return concludedWith(job, ReasonPlacementTimeout)
}

// concludedWith reports whether job concluded SucceededWithoutRestore with
// reason.
func concludedWith(job *pmv1alpha1.PodMigrationJob, reason string) bool {
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		return false
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, "Restored")
	return cond != nil && cond.Reason == reason
}

// recreateUnplacedAfterRestoreTimeout handles a job that concluded
// RestoreTimeout while its replacement, released with the restore's node
// constraints, wasn't scheduled (C8, #14): the scheduler hadn't tried it yet
// at the restore ceiling, a node it nominated for it fell through, or the
// pod gate, on a view of the job from before the conclusion, released it
// only after the ceiling. Nothing else looks at the pod once the job has
// concluded, so once the pod is unschedulable, it's deleted for its
// ReplicaSet or StatefulSet to recreate, as the placement backstop does at
// the ceiling, and the new pod starts cold. The job keeps its conclusion. A
// pod that doesn't carry the constraints is left alone, whatever the job
// records: the pod gate releases a pod whose job has concluded cold,
// without them. It returns how soon to look again: soon while the pod waits
// to be scheduled, never once it's scheduled, gone or being deleted, or when
// no ReplicaSet or StatefulSet would recreate it. The job is deleted 30
// minutes after it concluded, which ends the checks.
func (r *PodMigrationJobReconciler) recreateUnplacedAfterRestoreTimeout(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (time.Duration, error) {
	if !concludedWith(job, reasonRestoreTimeout) {
		return 0, nil
	}
	if _, ok := restorePlacementRelease(job); !ok {
		return 0, nil
	}
	key := types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}
	cached := &corev1.Pod{}
	if err := r.Get(ctx, key, cached); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return placementRecheckInterval, fmt.Errorf("failed to read replacement pod %s: %w", key.Name, err)
	}
	if string(cached.UID) != job.Status.RestoredPodUID || cached.DeletionTimestamp != nil ||
		cached.Spec.NodeName != "" || !recreatedByOwner(cached) {
		return 0, nil
	}
	if !unschedulable(cached) {
		return placementRecheckInterval, nil
	}
	pod := &corev1.Pod{}
	if err := r.liveReader().Get(ctx, key, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return 0, nil
		}
		return placementRecheckInterval, fmt.Errorf("failed to live-read replacement pod %s: %w", key.Name, err)
	}
	if string(pod.UID) != job.Status.RestoredPodUID || pod.DeletionTimestamp != nil || !unschedulable(pod) || !recreatedByOwner(pod) {
		return placementRecheckInterval, nil
	}
	carries, err := r.carriesRestorePlacement(ctx, job, pod)
	if err != nil {
		return placementRecheckInterval, err
	}
	if !carries {
		return 0, nil
	}
	uid, resourceVersion := pod.UID, pod.ResourceVersion
	if err := r.Delete(ctx, pod, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion}); err != nil {
		switch {
		case apierrors.IsNotFound(err):
			return 0, nil
		case apierrors.IsConflict(err):
			// It changed since the read, perhaps scheduled: look again.
			return placementRecheckInterval, nil
		}
		return placementRecheckInterval, fmt.Errorf("failed to delete replacement pod %s: %w", pod.Name, err)
	}
	message := r.placementTimeoutMessage(ctx, job, pod, fmt.Sprintf("Replacement pod %s, released with the restore's node constraints, still wasn't scheduled after the job concluded %s, so it's deleted for its owner to recreate, and the new pod starts cold",
		pod.Name, reasonRestoreTimeout))
	log.FromContext(ctx).Info("Replacement released with the restore's node constraints is unschedulable after the restore timeout; deleting it",
		"job", job.Name, "pod", pod.Name)
	r.eventf(job, corev1.EventTypeWarning, ReasonPlacementTimeout, "%s", message)
	if r.Recorder != nil {
		r.Recorder.Eventf(pod, corev1.EventTypeWarning, ReasonPlacementTimeout, "%s", message)
	}
	return 0, nil
}

// carriesRestorePlacement reports whether every term of pod's required node
// affinity has the node constraints that job's engine states, as the pod
// gate adds them.
func (r *PodMigrationJobReconciler) carriesRestorePlacement(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (bool, error) {
	advisor, ok := r.jobEngine(job).(engine.PlacementAdvisor)
	if !ok {
		return false, nil
	}
	reqs, err := advisor.Placement(ctx, job)
	if err != nil || len(reqs) == 0 {
		return false, err
	}
	for _, req := range reqs {
		if !everyTermHas(pod, req) {
			return false, nil
		}
	}
	return true, nil
}

// failedPlacementKeys returns the keys of the node constraints that kept
// pod, job's replacement, from being scheduled, for the PlacementTimeout
// messages (#294): of the constraints the pod gate added to it
// (addedPlacementRequirements), the ones that no node meets among the nodes
// that would take pod without them. Those nodes are found as
// blockedByRestorePlacement finds them: withoutRequirements gives the pod as
// its owner recreates it, and nodeFits checks each node for it. For GKE Pod
// Snapshots, that's node.kubernetes.io/instance-type; for CRIU, the
// arch, CPU, version and missing-CPU-flag label keys that differ.
//
// When that singles out none, it returns the keys of all the added
// constraints: when no node would take the pod without them, when each one
// is met by some such node and only their combination isn't, or when the
// nodes can't be listed. So does a pod whose engine states no constraints
// any more, without looking at nodes. The keys are in the engine's order,
// compacted by compactRequirementKeys. This only shapes a message, so it
// never fails.
func (r *PodMigrationJobReconciler) failedPlacementKeys(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) []string {
	var reqs []corev1.NodeSelectorRequirement
	if advisor, ok := r.jobEngine(job).(engine.PlacementAdvisor); ok {
		var err error
		if reqs, err = advisor.Placement(ctx, job); err != nil {
			log.FromContext(ctx).V(1).Info("Couldn't read the restore's node constraints; naming the ones the pod records", "job", job.Name, "err", err.Error())
			reqs = nil
		}
	}
	added := addedPlacementRequirements(pod, reqs)
	keys := func(reqs []corev1.NodeSelectorRequirement) []string {
		out := make([]string, 0, len(reqs))
		for _, req := range reqs {
			out = append(out, req.Key)
		}
		return compactRequirementKeys(out)
	}
	if len(reqs) == 0 || len(added) == 0 {
		return keys(added)
	}
	recreated := withoutRequirements(pod, reqs)
	recreatedAffinity := nodeaffinity.GetRequiredNodeAffinity(recreated)
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, client.UnsafeDisableDeepCopy); err != nil {
		log.FromContext(ctx).V(1).Info("Couldn't list nodes; naming every constraint the pod gate added", "job", job.Name, "err", err.Error())
		return keys(added)
	}
	met := make([]bool, len(added))
	candidates := 0
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if !nodeFits(ctx, node, recreated, recreatedAffinity) {
			continue
		}
		candidates++
		for j, req := range added {
			if selector, ok := labelSelectorFor([]corev1.NodeSelectorRequirement{req}); ok && selector.Matches(labels.Set(node.Labels)) {
				met[j] = true
			}
		}
	}
	var failed []corev1.NodeSelectorRequirement
	for j, req := range added {
		if !met[j] {
			failed = append(failed, req)
		}
	}
	if candidates == 0 || len(failed) == 0 {
		return keys(added)
	}
	return keys(failed)
}

// addedPlacementRequirements returns the node selector requirements that the
// pod gate appended to pod's required node affinity, as pod records them
// (RestorePlacementAddedAnnotation), each once, in term order. With reqs,
// the engine's constraints, the record must agree with them (as for
// withoutRequirements); without the record, they're reqs. Without reqs, the
// record is taken as it is: the engine may state none any more, for
// example when the CRIU snapshot is gone.
func addedPlacementRequirements(pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) []corev1.NodeSelectorRequirement {
	var terms []corev1.NodeSelectorTerm
	if a := pod.Spec.Affinity; a != nil && a.NodeAffinity != nil && a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		terms = a.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	}
	var counts []int
	if len(reqs) > 0 {
		added, ok := addedRequirements(pod, terms, reqs)
		if !ok {
			return reqs
		}
		counts = added
	} else {
		parts := strings.Split(pod.Annotations[RestorePlacementAddedAnnotation], ",")
		if _, found := pod.Annotations[RestorePlacementAddedAnnotation]; !found || len(parts) != len(terms) {
			return nil
		}
		for i, part := range parts {
			n, err := strconv.Atoi(part)
			if err != nil || n < 0 || n > len(terms[i].MatchExpressions) {
				return nil
			}
			counts = append(counts, n)
		}
	}
	var out []corev1.NodeSelectorRequirement
	for i, n := range counts {
		exprs := terms[i].MatchExpressions
		for _, expr := range exprs[len(exprs)-n:] {
			if !hasNodeSelectorRequirement(out, expr) {
				out = append(out, expr)
			}
		}
	}
	return out
}

// placementTimeoutMessage returns text, a PlacementTimeout message about
// pod, job's replacement, with the fields that name the engine and the
// constraints that failed (#294).
func (r *PodMigrationJobReconciler) placementTimeoutMessage(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, text string) string {
	return withSignal(placementSignal(r.jobEngine(job).Name(), r.failedPlacementKeys(ctx, job, pod)), text)
}

// keptPodPlacementSignal returns the fields for a PlacementTimeout event
// about a replacement that job, concluded PlacementTimeout, kept after all:
// the ones its conclusion recorded, or, for a conclusion by an earlier
// version, the engine's name and no keys.
func (r *PodMigrationJobReconciler) keptPodPlacementSignal(job *pmv1alpha1.PodMigrationJob) string {
	if cond := meta.FindStatusCondition(job.Status.Conditions, "Restored"); cond != nil {
		if signal, ok := placementSignalOf(cond.Message); ok {
			return signal
		}
	}
	return placementSignal(r.jobEngine(job).Name(), nil)
}

// unschedulable reports whether the scheduler tried pod and found no node
// for it. A pod that it nominated a node for, to make room there by
// preempting lower-priority pods, is scheduled once they're gone, so it
// doesn't count. Nor does a pod that another scheduling gate holds: the
// scheduler doesn't try it, and reports it SchedulingGated.
func unschedulable(pod *corev1.Pod) bool {
	if pod.Spec.NodeName != "" || pod.Status.NominatedNodeName != "" {
		return false
	}
	for _, cond := range pod.Status.Conditions {
		if cond.Type == corev1.PodScheduled {
			return cond.Status == corev1.ConditionFalse && cond.Reason == corev1.PodReasonUnschedulable
		}
	}
	return false
}

// placementBackstop handles a Restoring job whose replacement the pod gate
// released with the restore's node constraints, but which wasn't scheduled
// within the placement deadline (C8, #14): no node that meets them came up,
// or the pod can't be scheduled for a reason the gate's check doesn't cover,
// such as a bound volume's zone. The constraints can't be removed from the
// pod, so the job concludes without a restore and the pod is deleted for
// its ReplicaSet or StatefulSet to recreate; the new pod starts cold. It
// reports whether it handled the job.
//
// At the deadline, it only does that when deleting the pod can help right
// away: when a node that doesn't meet the constraints would take the
// recreated pod. Otherwise the recreated pod would wait for the same node,
// for example in a pool of one machine type that is full until the cluster
// autoscaler adds a node, so the pod is left to wait and keeps its chance to
// restore.
//
// From the restore ceiling on, it does it even if no such node exists: the
// job gives up on the restore then anyway, and a constraint that no node may
// ever meet must not keep the pod Pending. Without the constraints, the
// cluster autoscaler can add any node for the recreated pod, for example in
// a fallback pool that scales from zero. The deadline counts from the
// release, which can come late, so the ceiling can come first.
//
// A pod that the scheduler hasn't tried or has nominated a node for is left
// alone. Then the restore ceiling concludes the job RestoreTimeout as
// before, and recreateUnplacedAfterRestoreTimeout deletes the pod later,
// once it's unschedulable. So is a pod that no ReplicaSet or StatefulSet
// would recreate, which only an earlier version of the pod gate released
// with the constraints; nothing deletes it.
//
// The cache only tells it whether to look: the decision is taken on a live
// read, since a pod that the cache shows unscheduled may have been scheduled
// since.
func (r *PodMigrationJobReconciler) placementBackstop(ctx context.Context, job *pmv1alpha1.PodMigrationJob) (ctrl.Result, bool, error) {
	release, ok := restorePlacementRelease(job)
	if !ok {
		return ctrl.Result{}, false, nil
	}
	deadline := r.restorePlacementDeadline()
	pastCeiling := job.Status.RestoringStartTime != nil && time.Since(job.Status.RestoringStartTime.Time) >= r.restoreCeiling()
	if !pastCeiling && time.Since(release.LastTransitionTime.Time) < deadline {
		return ctrl.Result{}, false, nil
	}
	key := types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}
	cached := &corev1.Pod{}
	if err := r.Get(ctx, key, cached); err != nil || string(cached.UID) != job.Status.RestoredPodUID ||
		cached.DeletionTimestamp != nil || !unschedulable(cached) || !recreatedByOwner(cached) {
		return ctrl.Result{}, false, client.IgnoreNotFound(err)
	}
	why := fmt.Sprintf("wasn't scheduled before the restore ceiling, %s after the job started restoring", r.restoreCeiling())
	if !pastCeiling {
		// The pod's node affinity can't change once it's released, so the
		// cached pod answers this.
		blocked, err := r.blockedByRestorePlacement(ctx, job, cached)
		if err != nil {
			return ctrl.Result{}, true, err
		}
		if !blocked {
			log.FromContext(ctx).V(1).Info("Replacement is still unscheduled past the placement deadline, but no node without the restore's node constraints could take it either; leaving it to wait until the restore ceiling",
				"job", job.Name, "pod", key.Name)
			return ctrl.Result{}, false, nil
		}
		why = fmt.Sprintf("wasn't scheduled within %s, and a node without them could take it", deadline)
	}
	pod := &corev1.Pod{}
	if err := r.liveReader().Get(ctx, key, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, false, nil
		}
		return ctrl.Result{}, true, fmt.Errorf("failed to live-read replacement pod %s: %w", key.Name, err)
	}
	if string(pod.UID) != job.Status.RestoredPodUID || pod.DeletionTimestamp != nil || !unschedulable(pod) || !recreatedByOwner(pod) {
		return ctrl.Result{}, false, nil
	}
	return r.concludePlacementTimeout(ctx, job, pod, why)
}

// blockedByRestorePlacement reports whether the restore's node constraints
// are what keep pod, job's replacement, from being scheduled right now:
// whether some node that pod can't run on, by its required node affinity,
// would take it without the constraints, as its owner recreates it. For
// example, in a cluster with a full pool of the source machine type and a
// pool of another type with room, it would, and the recreated pod starts
// cold in the other pool. In a pool of one machine type that is full until
// the cluster autoscaler adds a node, no node would. Only nodes that exist
// count, not ones the autoscaler could add. The nodes are checked as the pod
// gate checks them (nodeFits), so resources, volume zones, host ports, and
// pod affinity and topology spread aren't looked at either. The constraints
// are the ones the engine states for job, which the pod gate added; without
// them, it reports false.
func (r *PodMigrationJobReconciler) blockedByRestorePlacement(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (bool, error) {
	advisor, ok := r.jobEngine(job).(engine.PlacementAdvisor)
	if !ok {
		return false, nil
	}
	reqs, err := advisor.Placement(ctx, job)
	if err != nil || len(reqs) == 0 {
		return false, err
	}
	recreated := withoutRequirements(pod, reqs)
	recreatedAffinity := nodeaffinity.GetRequiredNodeAffinity(recreated)
	affinity := nodeaffinity.GetRequiredNodeAffinity(pod)
	// The nodes are only read, so they needn't be copied out of the cache.
	opts := []client.ListOption{client.UnsafeDisableDeepCopy}
	if selector, ok := wayOutSelector(recreated, reqs); ok {
		opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
	}
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, opts...); err != nil {
		return false, err
	}
	for i := range nodes.Items {
		node := &nodes.Items[i]
		if match, err := affinity.Match(node); err != nil || match {
			continue
		}
		if nodeFits(ctx, node, recreated, recreatedAffinity) {
			return true, nil
		}
	}
	return false, nil
}

// wayOutSelector returns a label selector that narrows a node list to the
// nodes that could take pod, recreated without reqs, where it can't run with
// them: nodes with the labels of its node selector, and, when reqs is one
// requirement that a label selector can negate, nodes that don't meet it.
// For GKE Pod Snapshots' instance-type In [type], that's instance-type
// NotIn [type], so in a pool of one machine type no node is listed. A node
// that meets a single requirement can't be a way out: the pod's terms with
// it match the node exactly when the recreated pod's terms do. It reports
// false when it can't narrow; blockedByRestorePlacement checks every node it
// lists anyway.
func wayOutSelector(pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) (labels.Selector, bool) {
	selector := labels.NewSelector()
	for key, value := range pod.Spec.NodeSelector {
		requirement, err := labels.NewRequirement(key, selection.Equals, []string{value})
		if err != nil {
			return nil, false
		}
		selector = selector.Add(*requirement)
	}
	negated := map[corev1.NodeSelectorOperator]selection.Operator{
		corev1.NodeSelectorOpIn:           selection.NotIn,
		corev1.NodeSelectorOpNotIn:        selection.In,
		corev1.NodeSelectorOpExists:       selection.DoesNotExist,
		corev1.NodeSelectorOpDoesNotExist: selection.Exists,
	}
	if len(reqs) != 1 {
		return selector, !selector.Empty()
	}
	if op, ok := negated[reqs[0].Operator]; ok {
		var values []string
		if op == selection.In || op == selection.NotIn {
			values = reqs[0].Values
		}
		if requirement, err := labels.NewRequirement(reqs[0].Key, op, values); err == nil {
			selector = selector.Add(*requirement)
		}
	}
	return selector, !selector.Empty()
}

// concludePlacementTimeout concludes job SucceededWithoutRestore with reason
// PlacementTimeout, then deletes pod, its replacement, read live; why says
// why it's deleted. As for a restore crash, the status write lands before
// the delete: if the controller stops in between, the conclusion survives,
// and the completion path in Reconcile deletes the pod.
func (r *PodMigrationJobReconciler) concludePlacementTimeout(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod, why string) (ctrl.Result, bool, error) {
	logger := log.FromContext(ctx)
	message := r.placementTimeoutMessage(ctx, job, pod, fmt.Sprintf("Replacement pod %s, released with the restore's node constraints, %s, so it's deleted for its owner to recreate, and the new pod starts cold",
		pod.Name, why))
	logger.Info("Replacement is still unscheduled past the placement deadline; concluding without a restore and deleting it",
		"job", job.Name, "pod", pod.Name, "why", why)
	orig := job.DeepCopy()
	if err := r.markSucceededWithoutRestore(ctx, job, orig, ReasonPlacementTimeout, message, client.MergeFromWithOptimisticLock{}); err != nil {
		// Nothing was written, so the invariant checks after this
		// reconcile must see the job as it is, not concluded.
		*job = *orig
		res, err := r.handleStatusError(ctx, err, "Failed to update job status to SucceededWithoutRestore on PlacementTimeout")
		return res, true, err
	}
	metrics.RecordRestoreVerdict(r.jobEngine(job).Name(), "placement_timeout")
	r.eventf(job, corev1.EventTypeWarning, ReasonPlacementTimeout, "%s", message)
	if r.Recorder != nil {
		r.Recorder.Eventf(pod, corev1.EventTypeWarning, ReasonPlacementTimeout, "%s", message)
	}
	// The job has concluded, so a failed delete is retried from the
	// completion path in Reconcile, like a pod left alone for now.
	recheck, err := r.ensurePlacementTimedOutPodDeleted(ctx, job, pod)
	if err != nil {
		logger.Error(err, "Failed to delete the replacement after the placement deadline; will retry", "pod", pod.Name)
	}
	return ctrl.Result{RequeueAfter: recheck}, true, nil
}

// ensurePlacementTimedOutPodDeleted deletes the replacement of a job that
// concluded PlacementTimeout, idempotently, so that the completion path can
// retry a delete that failed. It deletes only the instance the job recorded,
// only while the scheduler reports it unschedulable (unschedulable), and
// only if a ReplicaSet or StatefulSet would recreate it. prefetched, if set,
// is the pod read live; otherwise it reads the pod live itself. The UID and
// resourceVersion preconditions keep it from deleting a same-named
// successor, or a pod that changed after the read.
//
// A pod bound to a node is kept: the job's conclusion says it was deleted,
// so when the delete finds it bound, a Warning event on the job says it was
// kept, once. A pod that has a nominated node, which the scheduler places
// once it has preempted pods there, is left alone for now, like any pod
// that the scheduler doesn't report unschedulable, and so is one that
// changed after the read in another way. The job doesn't watch pods, so it returns how soon to look
// again: placementRecheckInterval while the pod waits like that, or after
// an error, and 0 once the pod is gone, deleted, bound, or not the job's to
// delete.
func (r *PodMigrationJobReconciler) ensurePlacementTimedOutPodDeleted(ctx context.Context, job *pmv1alpha1.PodMigrationJob, prefetched *corev1.Pod) (time.Duration, error) {
	if job.Status.RestoredPodName == "" {
		return 0, nil
	}
	key := types.NamespacedName{Namespace: job.Namespace, Name: job.Status.RestoredPodName}
	pod := prefetched
	if pod == nil {
		pod = &corev1.Pod{}
		if err := r.liveReader().Get(ctx, key, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return 0, nil
			}
			return placementRecheckInterval, fmt.Errorf("failed to live-read replacement pod %s: %w", key.Name, err)
		}
	}
	if job.Status.RestoredPodUID != "" && string(pod.UID) != job.Status.RestoredPodUID {
		return 0, nil
	}
	if pod.DeletionTimestamp != nil || pod.Spec.NodeName != "" || !recreatedByOwner(pod) {
		return 0, nil
	}
	if !unschedulable(pod) {
		// A nominated node can fall through, and another gate can go away,
		// so the pod may become unschedulable again: look again soon.
		log.FromContext(ctx).V(1).Info("Replacement past the placement deadline has a nominated node or another scheduling gate; leaving it for now",
			"job", job.Name, "pod", pod.Name, "nominatedNode", pod.Status.NominatedNodeName)
		return placementRecheckInterval, nil
	}
	uid, resourceVersion := pod.UID, pod.ResourceVersion
	err := r.Delete(ctx, pod, client.Preconditions{UID: &uid, ResourceVersion: &resourceVersion})
	switch {
	case err == nil, apierrors.IsNotFound(err):
		return 0, nil
	case apierrors.IsConflict(err):
		current := &corev1.Pod{}
		if getErr := r.liveReader().Get(ctx, key, current); getErr == nil && current.UID == uid {
			switch {
			case current.Spec.NodeName != "":
				r.eventf(job, corev1.EventTypeWarning, ReasonPlacementTimeout,
					"%s: Replacement pod %s was scheduled to node %s after all, so it's kept, and it may still restore the checkpoint, although the job concluded without a restore",
					r.keptPodPlacementSignal(job), pod.Name, current.Spec.NodeName)
				return 0, nil
			case current.Status.NominatedNodeName != "":
				r.eventf(job, corev1.EventTypeWarning, ReasonPlacementTimeout,
					"%s: Replacement pod %s was nominated to node %s, so it's kept while the scheduler makes room there. It's deleted if it becomes unschedulable again; if it's scheduled, it may still restore the checkpoint, although the job concluded without a restore",
					r.keptPodPlacementSignal(job), pod.Name, current.Status.NominatedNodeName)
			}
		}
		// It changed since the read: look again on a fresh read.
		return placementRecheckInterval, nil
	default:
		return placementRecheckInterval, fmt.Errorf("failed to delete replacement pod %s: %w", pod.Name, err)
	}
}
