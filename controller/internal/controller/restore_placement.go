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
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	schedulinghelper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

const (
	// ConditionRestorePlacement, on a PodMigrationJob, reports that the pod
	// gate reconciler couldn't place the restore (C8, #14). It is only set
	// False, with reason ReasonNoCompatibleNode.
	ConditionRestorePlacement = "RestorePlacement"
	// ReasonNoCompatibleNode: when the replacement was due for release, no
	// node met the snapshot engine's placement constraints, so the job
	// concludes SucceededWithoutRestore and the replacement starts cold.
	ReasonNoCompatibleNode = "NoCompatibleNode"

	// tolerationComparisonOperators makes the node check match Gt and Lt
	// tolerations by number, as the scheduler does with the
	// TaintTolerationComparisonOperators feature gate. The API server
	// accepts new ones only with that gate on, so most clusters have none.
	// If a pod keeps one after the scheduler's gate is turned off, the check
	// can accept a node that the scheduler then rejects.
	tolerationComparisonOperators = true
)

// restorePlacement returns the node constraints that restoring job's
// checkpoint needs (C8, #14), from the engine that orders the restore: the
// job's engine, or GKE Pod Snapshots without a registry, as for the restore
// order. An engine without the PlacementAdvisor capability states none, and
// so does every engine while RestorePlacement is off. Constraints that
// aren't valid node label requirements are dropped too: the API server would
// reject the update that adds them, so the release, retried as is, would
// keep failing, or the scheduler could never match them. Like
// jobRestoreOrderer, it fails when job names an engine that isn't
// registered: another engine's constraints would place the wrong restore,
// and the job reconciler fails such a job (#80).
func (r *PodGateReconciler) restorePlacement(ctx context.Context, job *pmv1alpha1.PodMigrationJob) ([]corev1.NodeSelectorRequirement, error) {
	if !r.RestorePlacement {
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
	return reqs, nil
}

// errInvalidPlacement is logged when an engine states node constraints that
// aren't valid node label requirements.
var errInvalidPlacement = errors.New("invalid node constraints")

// releaseUnderWay reports whether pod's release with a restore order has
// started: the job records the gate as released, or pod has consumed the
// job. The release then goes ahead without a placement check. A check
// normally passed before pod consumed the job (unless placement was turned
// on in between), and holding a pod that consumed the job would leave a
// concluded, consumed job with the pod still gated.
func releaseUnderWay(job *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) bool {
	return job.Status.GateReleased || (job.Status.Consumed && job.Status.RestoredPodUID == string(pod.UID))
}

// noCompatibleNode returns the condition with which the pod gate reconciler
// recorded on job that no node could restore its checkpoint. It only counts
// while the job is Restoring, the only phase in which the job reconciler
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

// holdForNoCompatibleNode records on job that no node can restore its
// checkpoint, and leaves pod gated. The job reconciler then concludes the job
// SucceededWithoutRestore with reason NoCompatibleNode, and the gate is
// released with the cold-start bypass and without the constraints. The job
// concludes before the pod starts, so a pod that started cold is never
// counted as restored (I2).
//
// deadConsumer says that job records another pod as its consumer, one that
// no longer exists and whose gate was never released, so it never restored.
// The same patch clears that record: the job then concludes unconsumed, and
// the cold release records pod as the one that fell back. A job that
// concluded consumed while pod is still gated would be reported by I3 until
// the release.
func (r *PodGateReconciler) holdForNoCompatibleNode(ctx context.Context, pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob, reqs []corev1.NodeSelectorRequirement, deadConsumer bool) (ctrl.Result, error) {
	message := fmt.Sprintf("No Ready node that pod %s can run on, by its node selector, node affinity and tolerations, meets the restore's node constraints (%s), so the pod starts without the restore",
		pod.Name, describeRequirements(reqs))
	// The conditions list is replaced as a whole by a merge patch, so don't
	// overwrite a concurrent change to it. The resourceVersion check also
	// keeps a dead consumer's record from being cleared on a stale view.
	orig := job.DeepCopy()
	patch := client.MergeFromWithOptions(orig, client.MergeFromWithOptimisticLock{})
	meta.SetStatusCondition(&job.Status.Conditions, metav1.Condition{
		Type:               ConditionRestorePlacement,
		Status:             metav1.ConditionFalse,
		Reason:             ReasonNoCompatibleNode,
		Message:            message,
		ObservedGeneration: job.Generation,
	})
	if deadConsumer {
		job.Status.Consumed = false
		job.Status.RestoredPodUID = ""
		job.Status.RestoredPodName = ""
	}
	if err := r.Status().Patch(ctx, job, patch); err != nil {
		// Nothing was written, so the invariant checks after this
		// reconcile must see the job as it is.
		*job = *orig
		if apierrors.IsConflict(err) {
			// The job changed after it was read, for example it concluded:
			// decide again on the current job.
			log.FromContext(ctx).Info("The job changed while recording that no node can restore its checkpoint; requeuing", "pmj", job.Name)
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	keysAndValues := []any{"pmj", job.Name, "constraints", describeRequirements(reqs)}
	if deadConsumer {
		keysAndValues = append(keysAndValues,
			"clearedConsumerPodName", orig.Status.RestoredPodName, "clearedConsumerPodUID", orig.Status.RestoredPodUID)
	}
	log.FromContext(ctx).Info("No node can restore the checkpoint; holding the replacement until the job concludes without a restore", keysAndValues...)
	if r.Recorder != nil {
		r.Recorder.Eventf(pod, corev1.EventTypeWarning, ReasonNoCompatibleNode, "%s", message)
	}
	// The PMJ watch enqueues the pod when the job concludes; this is a
	// backstop.
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
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

// someNodeFits reports whether some node could run pod once reqs are added
// to its required node affinity. It checks the nodes in the cache with a
// subset of the scheduler's filters: the node is Ready and not being
// deleted, isn't cordoned (unless the pod tolerates that), matches the pod's
// node selector and required node affinity with reqs added, and has no
// NoSchedule or NoExecute taint that the pod doesn't tolerate. It doesn't
// check resources, the node affinity of bound volumes, host ports, or pod
// affinity and topology spread, so a node that passes may still not take
// the pod. The nodes are narrowed to those whose labels meet reqs.
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
