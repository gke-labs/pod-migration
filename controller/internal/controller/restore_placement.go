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
	"errors"
	"fmt"
	"slices"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	schedulinghelper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

const (
	// ConditionRestorePlacement, on a PodMigrationJob, records how the pod
	// gate reconciler placed the restore (C8, #14). True with reason
	// ReasonNodeConstraintsAdded: the replacement was released with the
	// snapshot engine's node constraints. False with reason
	// ReasonNoCompatibleNode: an earlier version of the pod gate held the
	// replacement to start cold, because no node met them.
	ConditionRestorePlacement = "RestorePlacement"
	// ReasonNoCompatibleNode: an earlier version of the pod gate, with
	// placement on, found no node that met the snapshot engine's node
	// constraints when the replacement was due for release, and held it to
	// start cold. The pod gate doesn't hold replacements any more, but a job
	// with that record still concludes SucceededWithoutRestore with this
	// reason, and its replacement starts cold, as recorded.
	ReasonNoCompatibleNode = "NoCompatibleNode"

	// tolerationComparisonOperators makes the node check match Gt and Lt
	// tolerations by number, as the scheduler does with the
	// TaintTolerationComparisonOperators feature gate. The API server
	// accepts new ones only with that gate on, so most clusters have none.
	// If a pod keeps one after the scheduler's gate is turned off, the check
	// can accept a node that the scheduler then rejects.
	tolerationComparisonOperators = true
)

// restorePlacement returns the node constraints to add to pod, the
// replacement, for restoring job's checkpoint (C8, #14), from the engine
// that orders the restore: the job's engine, or GKE Pod Snapshots without a
// registry, as for the restore order. An engine without the
// PlacementAdvisor capability states none, and so does every engine while
// RestorePlacement is off.
//
// Only a pod that its ReplicaSet or StatefulSet would recreate
// (recreatedByOwner) gets any: if it can't be scheduled with them, the job
// reconciler deletes it for that owner to recreate without them. Any other
// pod, such as a Job's, an operator's, or one without a controller, is
// placed as with RestorePlacement off: nothing would recreate it without
// the constraints if it got stuck with them, and the node check can't tell
// whether a node that meets them will take it, since it doesn't look at
// resources, for one. Without them it can land on a node of another machine
// type, where its restore may fail; the restore-crash fallback then handles
// it, as with placement off.
//
// Constraints that aren't valid node label requirements are dropped too:
// the API server would reject the update that adds them, so the release,
// retried as is, would keep failing, or the scheduler could never match
// them. None are added either when pod's own node selector, or every term
// of its required node affinity, already requires them: they'd narrow
// nothing, and pod is placed by its own pins, as with RestorePlacement off.
// Like jobRestoreOrderer, it fails when job names an engine that isn't
// registered: another engine's constraints would place the wrong restore,
// and the job reconciler fails such a job (#80).
func (r *PodGateReconciler) restorePlacement(ctx context.Context, job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) ([]corev1.NodeSelectorRequirement, error) {
	if !r.RestorePlacement || !recreatedByOwner(pod) {
		return nil, nil
	}
	var eng engine.Engine
	if r.Engines != nil {
		var err error
		if eng, err = r.Engines.ForJob(job); errors.Is(err, engine.ErrEngineNotRegistered) {
			return nil, err
		}
	}
	if eng == nil {
		eng = engine.NewGKEPodSnapshots(r.Client, r.Scheme)
	}
	advisor, ok := eng.(engine.PlacementAdvisor)
	if !ok {
		return nil, nil
	}
	reqs, err := advisor.Placement(ctx, job)
	if err != nil {
		return nil, err
	}
	if len(reqs) == 0 {
		// For GKE Pod Snapshots: the job recorded no machine type.
		log.FromContext(ctx).Info("The snapshot engine states no node constraints for the restore, so the replacement gets none",
			"pmj", job.Name, "engine", eng.Name())
		return nil, nil
	}
	if _, ok := labelSelectorFor(reqs); !ok {
		// An engine bug: the replacement gets no constraints, as for data
		// the engine can't use (C8), rather than staying gated.
		log.FromContext(ctx).Error(errInvalidPlacement, "The snapshot engine states node constraints that aren't valid node label requirements, so the replacement gets none",
			"pmj", job.Name, "engine", eng.Name(), "constraints", describeRequirements(reqs))
		return nil, nil
	}
	if !narrows(pod, reqs) {
		// The pod is pinned to what the restore needs by its own node
		// selector or affinity, which a pod recreated in its place would
		// have too. So deleting it at the placement deadline couldn't get it
		// scheduled anywhere else.
		log.FromContext(ctx).Info("The replacement already requires the restore's node constraints, so none are added",
			"pmj", job.Name, "constraints", describeRequirements(reqs))
		return nil, nil
	}
	return reqs, nil
}

// errInvalidPlacement is logged when an engine states node constraints that
// aren't valid node label requirements.
var errInvalidPlacement = errors.New("invalid node constraints")

// narrows reports whether reqs narrow where pod can run: whether one of them
// isn't already required by the pod's node selector, or by every term of its
// required node affinity.
func narrows(pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) bool {
	for _, req := range reqs {
		if !selectorRequires(pod.Spec.NodeSelector, req) && !everyTermHas(pod, req) {
			return true
		}
	}
	return false
}

// selectorRequires reports whether nodeSelector only matches nodes that meet
// req.
func selectorRequires(nodeSelector map[string]string, req corev1.NodeSelectorRequirement) bool {
	value, ok := nodeSelector[req.Key]
	switch req.Operator {
	case corev1.NodeSelectorOpIn:
		return ok && slices.Contains(req.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return ok && !slices.Contains(req.Values, value)
	case corev1.NodeSelectorOpExists:
		return ok
	}
	return false
}

// everyTermHas reports whether every term of pod's required node affinity
// has req. An empty term matches no node, so it doesn't count, as in
// addRequiredNodeAffinity.
func everyTermHas(pod *corev1.Pod, req corev1.NodeSelectorRequirement) bool {
	if pod.Spec.Affinity == nil || pod.Spec.Affinity.NodeAffinity == nil ||
		pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return false
	}
	terms := pod.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(terms) == 0 {
		return false
	}
	for _, term := range terms {
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		if !hasNodeSelectorRequirement(term.MatchExpressions, req) {
			return false
		}
	}
	return true
}

// recreatedByOwner reports whether pod's controller is a ReplicaSet or a
// StatefulSet, which recreates the pod if it's deleted, at no cost to the
// workload. That's what the placement deadline relies on. Another
// controller may not recreate it the same way: a Job, for one, counts a
// deleted pod as failed toward its backoffLimit.
func recreatedByOwner(pod *corev1.Pod) bool {
	ref := metav1.GetControllerOf(pod)
	if ref == nil {
		return false
	}
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil || gv.Group != appsv1.GroupName {
		return false
	}
	return ref.Kind == "ReplicaSet" || ref.Kind == "StatefulSet"
}

// releaseUnderWay reports whether pod's release with a restore order has
// started: the job records the gate as released, or pod has consumed the
// job. The release then goes ahead without a new node check, whatever the
// job's phase is now. restorePlacement is asked again, so the constraints
// match the first attempt's unless placement was turned on or off in
// between.
func releaseUnderWay(job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) bool {
	return job.Status.GateReleased || (job.Status.Consumed && job.Status.RestoredPodUID == string(pod.UID))
}

// noCompatibleNode returns the condition with which an earlier version of
// the pod gate reconciler recorded on job that no node could restore its
// checkpoint, and held the replacement to start cold. It only counts while
// the job is Restoring, the only phase in which the job reconciler
// concludes on it, and no replacement has been released with a restore
// order.
func noCompatibleNode(job *pmv1alpha1.PodMigrationJob) (*metav1.Condition, bool) {
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || job.Status.GateReleased {
		return nil, false
	}
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionRestorePlacement)
	if cond == nil || cond.Status != metav1.ConditionFalse || cond.Reason != ReasonNoCompatibleNode {
		return nil, false
	}
	return cond, true
}

// restorePlacementRecorded reports whether job records that its replacement
// was released with the restore's node constraints.
func restorePlacementRecorded(job *pmv1alpha1.PodMigrationJob) bool {
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionRestorePlacement)
	return cond != nil && cond.Status == metav1.ConditionTrue && cond.Reason == ReasonNodeConstraintsAdded
}

// recordRestorePlacement records on job that its replacement is released
// with reqs in its required node affinity, and whether no node met them at
// the release. The condition's last transition time is the release, which
// the job reconciler's placement deadline counts from.
func recordRestorePlacement(job *pmv1alpha1.PodMigrationJob, reqs []corev1.NodeSelectorRequirement, noNodeYet bool) {
	message := fmt.Sprintf("Released with required node affinity %s", describeRequirements(reqs))
	if noNodeYet {
		message += "; no node met it at the release"
	}
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionRestorePlacement,
		Status:             metav1.ConditionTrue,
		Reason:             ReasonNodeConstraintsAdded,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
}

// addRequiredNodeAffinity adds reqs to pod's required node affinity. Its
// terms are ORed, so each requirement goes into every term that doesn't have
// it yet, and a pod without terms gets one term with reqs. An empty term
// matches no node, and adding to it would make it match some, so it's left
// alone.
//
// Kubernetes accepts this change only while the pod has a scheduling gate,
// because it only narrows where the pod can run: a gated pod's terms may be
// set when it has none, and requirements may be added to existing terms. So
// it must go into the update that removes the gate.
func addRequiredNodeAffinity(pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) {
	if len(reqs) == 0 {
		return
	}
	if pod.Spec.Affinity == nil {
		pod.Spec.Affinity = &corev1.Affinity{}
	}
	if pod.Spec.Affinity.NodeAffinity == nil {
		pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	nodeAffinity := pod.Spec.Affinity.NodeAffinity
	required := nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		term := corev1.NodeSelectorTerm{}
		for _, req := range reqs {
			term.MatchExpressions = append(term.MatchExpressions, *req.DeepCopy())
		}
		nodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{term},
		}
		return
	}
	for i := range required.NodeSelectorTerms {
		term := &required.NodeSelectorTerms[i]
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		for _, req := range reqs {
			if !hasNodeSelectorRequirement(term.MatchExpressions, req) {
				term.MatchExpressions = append(term.MatchExpressions, *req.DeepCopy())
			}
		}
	}
}

func hasNodeSelectorRequirement(reqs []corev1.NodeSelectorRequirement, req corev1.NodeSelectorRequirement) bool {
	for _, have := range reqs {
		if equality.Semantic.DeepEqual(have, req) {
			return true
		}
	}
	return false
}

// withoutRequirements returns a copy of pod with reqs taken out of its
// required node affinity: the pod as its owner recreates it, without the
// constraints that addRequiredNodeAffinity added. A term left with nothing
// held only reqs, the term added to a pod without terms, so it's dropped,
// and a pod left without terms has no required node affinity. An empty term
// that the pod had is kept: addRequiredNodeAffinity leaves those alone. A
// requirement that one of the pod's own terms already had is taken out too;
// restorePlacement adds none when every term has them, so that only matters
// for a pod whose terms differ in it.
func withoutRequirements(pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) *corev1.Pod {
	stripped := pod.DeepCopy()
	if stripped.Spec.Affinity == nil || stripped.Spec.Affinity.NodeAffinity == nil ||
		stripped.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return stripped
	}
	required := stripped.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	var terms []corev1.NodeSelectorTerm
	for _, term := range required.NodeSelectorTerms {
		if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
			terms = append(terms, term)
			continue
		}
		var kept []corev1.NodeSelectorRequirement
		for _, expr := range term.MatchExpressions {
			if !hasNodeSelectorRequirement(reqs, expr) {
				kept = append(kept, expr)
			}
		}
		if len(kept) == 0 && len(term.MatchFields) == 0 {
			continue
		}
		term.MatchExpressions = kept
		terms = append(terms, term)
	}
	if len(terms) == 0 {
		stripped.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = nil
		return stripped
	}
	required.NodeSelectorTerms = terms
	return stripped
}

// someNodeFits reports whether some node could run pod once reqs are added
// to its required node affinity. It checks the nodes in the cache with a
// subset of the scheduler's filters: the node isn't being deleted, isn't
// cordoned (unless the pod tolerates that), matches the pod's node selector
// and required node affinity with reqs added, and has no NoSchedule or
// NoExecute taint that the pod doesn't tolerate. It also requires the node to
// be Ready, which is stricter than the scheduler, which leaves that to the
// not-ready taints; it errs toward "no node". It doesn't check resources,
// the node affinity of bound volumes, host ports, pod affinity and topology
// spread, or other scheduling gates, so a node that passes may still not
// take the pod. The nodes are narrowed to those whose labels meet reqs.
func (r *PodGateReconciler) someNodeFits(ctx context.Context, pod *corev1.Pod, reqs []corev1.NodeSelectorRequirement) (bool, error) {
	placed := pod.DeepCopy()
	addRequiredNodeAffinity(placed, reqs)
	affinity := nodeaffinity.GetRequiredNodeAffinity(placed)

	// The nodes are only read, so they needn't be copied out of the cache.
	opts := []client.ListOption{client.UnsafeDisableDeepCopy}
	if selector, ok := labelSelectorFor(reqs); ok {
		opts = append(opts, client.MatchingLabelsSelector{Selector: selector})
	}
	nodes := &corev1.NodeList{}
	if err := r.List(ctx, nodes, opts...); err != nil {
		return false, err
	}
	for i := range nodes.Items {
		if nodeFits(ctx, &nodes.Items[i], placed, affinity) {
			return true, nil
		}
	}
	return false, nil
}

// nodeFits reports whether node passes the checks that someNodeFits applies
// for pod.
func nodeFits(ctx context.Context, node *corev1.Node, pod *corev1.Pod, affinity nodeaffinity.RequiredNodeAffinity) bool {
	if node.DeletionTimestamp != nil || !nodeReady(node) {
		return false
	}
	logger := log.FromContext(ctx)
	cordoned := &corev1.Taint{Key: corev1.TaintNodeUnschedulable, Effect: corev1.TaintEffectNoSchedule}
	if node.Spec.Unschedulable && !schedulinghelper.TolerationsTolerateTaint(logger, pod.Spec.Tolerations, cordoned, tolerationComparisonOperators) {
		return false
	}
	// Match returns an error only together with false: no term matched,
	// and a term couldn't be parsed. So an error doesn't count as a fit.
	if match, _ := affinity.Match(node); !match {
		return false
	}
	_, untolerated := schedulinghelper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations, func(t *corev1.Taint) bool {
		return t.Effect == corev1.TaintEffectNoSchedule || t.Effect == corev1.TaintEffectNoExecute
	}, tolerationComparisonOperators)
	return !untolerated
}

func nodeReady(node *corev1.Node) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// labelSelectorFor converts reqs to a label selector. It reports false when
// one doesn't convert, because it isn't a valid label requirement:
// restorePlacement then drops the engine's constraints, and someNodeFits
// otherwise uses the selector to narrow the node list.
func labelSelectorFor(reqs []corev1.NodeSelectorRequirement) (labels.Selector, bool) {
	operators := map[corev1.NodeSelectorOperator]selection.Operator{
		corev1.NodeSelectorOpIn:           selection.In,
		corev1.NodeSelectorOpNotIn:        selection.NotIn,
		corev1.NodeSelectorOpExists:       selection.Exists,
		corev1.NodeSelectorOpDoesNotExist: selection.DoesNotExist,
		corev1.NodeSelectorOpGt:           selection.GreaterThan,
		corev1.NodeSelectorOpLt:           selection.LessThan,
	}
	selector := labels.NewSelector()
	for _, req := range reqs {
		op, ok := operators[req.Operator]
		if !ok {
			return nil, false
		}
		requirement, err := labels.NewRequirement(req.Key, op, req.Values)
		if err != nil {
			return nil, false
		}
		selector = selector.Add(*requirement)
	}
	return selector, true
}

// describeRequirements renders reqs for events and logs, for example
// "node.kubernetes.io/instance-type In [n2-standard-4]".
func describeRequirements(reqs []corev1.NodeSelectorRequirement) string {
	parts := make([]string, 0, len(reqs))
	for _, req := range reqs {
		if len(req.Values) == 0 {
			parts = append(parts, fmt.Sprintf("%s %s", req.Key, req.Operator))
			continue
		}
		parts = append(parts, fmt.Sprintf("%s %s [%s]", req.Key, req.Operator, strings.Join(req.Values, ", ")))
	}
	return strings.Join(parts, ", ")
}
