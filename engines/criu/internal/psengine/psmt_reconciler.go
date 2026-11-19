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

package psengine

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// NodeReadyLabel is set by the node agent's capability probe
// (internal/node/capability.go) on nodes whose kernel + CRIU check pass — the
// engine only checkpoints there.
const NodeReadyLabel = "pod-migrate.io/criu-ready"

// CheckpointRequest is the engine's order to the node agent: kubelet
// ContainerCheckpoint -> upload to gs://Bucket/Path/SnapshotName/ (verified)
// -> freeze the container (preCheckpoint: freeze semantics — the kubelet API
// does not freeze; the orchestrator's flow requires it).
type CheckpointRequest struct {
	Namespace    string
	PodName      string
	PodUID       string
	Container    string
	NodeName     string
	SnapshotName string
	Bucket       string
	Path         string
}

// Checkpointer is the optional direct-call mode (used by tests). In the
// default deployment it is nil and the handoff is the PodSnapshot CR itself:
// the PSMT watcher writes a complete work order into its spec and the node
// agent's NodeSnapshotReconciler (pipeline.go) picks it up.
type Checkpointer interface {
	Checkpoint(ctx context.Context, req CheckpointRequest) error
}

// PSMTReconciler consumes the orchestrator's work orders: it watches
// PodSnapshotManualTriggers (created by podmigrationjob_controller.go:504-558
// with spec.targetPod), decides eligibility, materializes the PodSnapshot CR,
// and answers status.snapshotCreated.name — the one field the orchestrator
// polls at 1s (podmigrationjob_controller.go:247).
//
// Refusals are terminal PodSnapshot conditions (reason=Refused), never
// silence, so the orchestrator can transition cleanly and fall open to cold
// eviction.
type PSMTReconciler struct {
	client.Client
	Checkpointer Checkpointer // nil in production (CR handoff to the node agent)
}

// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotmanualtriggers,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotmanualtriggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch;create
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods;nodes,verbs=get;list;watch

func (r *PSMTReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("psmt", req.NamespacedName)

	psmt := contract.New(contract.GVKManualTrigger)
	if err := r.Get(ctx, req.NamespacedName, psmt); err != nil {
		if apierrors.IsNotFound(err) {
			// PSMT deletion = orchestrator moved on: Evicting cleanup after
			// the snapshot went Ready, or a failure or timeout. A Ready
			// PodSnapshot survives, since the replacement pod's restore
			// still needs it. A work order that hasn't finished gets a
			// cancel request (C9, #106); the node agent acts on it.
			return ctrl.Result{}, r.cancelWorkOrders(ctx, req.NamespacedName)
		}
		return ctrl.Result{}, err
	}
	if psmt.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, r.cancelWorkOrders(ctx, req.NamespacedName)
	}

	podName, ok := contract.TriggerTargetPod(psmt)
	if !ok {
		logger.Info("PSMT has no spec.targetPod; ignoring")
		return ctrl.Result{}, nil
	}

	pod := &corev1.Pod{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			// Source already gone: nothing to checkpoint; stay silent and let
			// the orchestrator's own pod-existence checks fail the PMJ.
			logger.Info("target pod not found", "pod", podName)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Engine routing: only act if the pod matches a CRIU-engine PSP this
	// controller marked Ready. Otherwise the PSMT belongs to another engine.
	psp, err := r.matchingEnginePSP(ctx, req.Namespace, pod.Labels)
	if err != nil {
		return ctrl.Result{}, err
	}
	if psp == nil {
		logger.V(1).Info("no Ready criu-engine PSP matches pod; not ours", "pod", podName)
		return ctrl.Result{}, nil
	}

	snapshotName := contract.PodSnapshotName(pod.Name, string(pod.UID))
	refusal := r.refusalReason(ctx, pod)
	bucket, path, storageErr := r.storageFor(ctx, psp)

	// 1. Materialize the PodSnapshot (idempotent by deterministic name). Its
	// spec is the complete work order for the node agent.
	ps := contract.New(contract.GVKPodSnapshot)
	err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: snapshotName}, ps)
	if apierrors.IsNotFound(err) {
		ps = contract.New(contract.GVKPodSnapshot)
		ps.SetName(snapshotName)
		ps.SetNamespace(req.Namespace)
		psLabels := map[string]string{contract.LabelEngine: contract.EngineCRIU}
		// The label GKE Pod Snapshots puts on its snapshots, so the
		// orchestrator's CheckpointState finds this one after the trigger is
		// gone, even if the job never recorded status.snapshotRef. A trigger
		// name too long for a label value (pod names over 49 characters)
		// gets no label; the create would otherwise be rejected.
		if len(validation.IsValidLabelValue(req.Name)) == 0 {
			psLabels[contract.LabelTriggeredBy] = req.Name
		}
		ps.SetLabels(psLabels)
		spec := map[string]interface{}{
			"sourcePod":    pod.Name,
			"sourcePodUID": string(pod.UID),
			"nodeName":     pod.Spec.NodeName,
			// The trigger this work order answers: its deletion cancels
			// the work order (cancelWorkOrders, podSnapshotToTrigger).
			specTriggerName: req.Name,
		}
		if len(pod.Spec.Containers) > 0 {
			spec["container"] = pod.Spec.Containers[0].Name
		}
		// A refused snapshot gets NO actionable work order — the node agent
		// keys off bucket presence, and a refusal must never checkpoint.
		if storageErr == nil && refusal == "" {
			spec["bucket"], spec["path"] = bucket, path
		}
		ps.Object["spec"] = spec
		if err := r.Create(ctx, ps); err != nil && !apierrors.IsAlreadyExists(err) {
			return ctrl.Result{}, err
		}
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// 2. Answer the orchestrator: status.snapshotCreated.name.
	if contract.TriggerSnapshotCreated(psmt) != snapshotName {
		if err := contract.SetTriggerSnapshotCreated(psmt, snapshotName); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Status().Update(ctx, psmt); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("PSMT answered", "snapshot", snapshotName)
	}

	// 3. Eligibility -> refusal / failure / handoff.
	if refusal != "" {
		return ctrl.Result{}, r.setSnapshotTerminal(ctx, req.Namespace, snapshotName, contract.ReasonRefused, refusal)
	}
	if storageErr != nil {
		return ctrl.Result{}, r.setSnapshotTerminal(ctx, req.Namespace, snapshotName, contract.ReasonFailed,
			fmt.Sprintf("resolving storage: %v", storageErr))
	}
	if !hasBucket(ps) {
		// An earlier pass withheld the bucket (refused, or no storage then)
		// and failed to record that outcome. Node agents ignore a work order
		// without a bucket, so it would sit Pending until the job's deadline.
		// Fail it, as before agents ignored such orders, rather than fill in
		// the work order under an agent that may be watching it.
		return ctrl.Result{}, r.setSnapshotTerminal(ctx, req.Namespace, snapshotName, contract.ReasonFailed,
			"the work order was created without storage on an earlier pass that couldn't record why; it can't be handed to the node agent")
	}

	if r.Checkpointer != nil {
		// Direct-call mode (tests / future fast path).
		if err := r.Checkpointer.Checkpoint(ctx, CheckpointRequest{
			Namespace: pod.Namespace, PodName: pod.Name, PodUID: string(pod.UID),
			Container: pod.Spec.Containers[0].Name, NodeName: pod.Spec.NodeName,
			SnapshotName: snapshotName, Bucket: bucket, Path: path,
		}); err != nil {
			return ctrl.Result{}, r.setSnapshotTerminal(ctx, req.Namespace, snapshotName, contract.ReasonFailed, err.Error())
		}
		return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	// CR handoff: the node agent's NodeSnapshotReconciler executes the work
	// order and owns the terminal conditions. Record the wait honestly.
	if err := r.setSnapshotPending(ctx, req.Namespace, snapshotName,
		fmt.Sprintf("awaiting node agent on %s", pod.Spec.NodeName)); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// refusalReason returns "" when the pod is eligible, else the human-readable
// refusal (refusals are policy outcomes, distinct from runtime failures).
func (r *PSMTReconciler) refusalReason(ctx context.Context, pod *corev1.Pod) string {
	if pod.Labels[contract.LabelEnabled] != "true" {
		return "pod is not opted in (pod-migration.gke.io/enabled != true)"
	}
	if len(pod.Spec.Containers) != 1 {
		return "multi-container pods unsupported by criu engine v1"
	}
	if pod.Spec.NodeName == "" {
		return "pod is not scheduled"
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node); err != nil {
		return fmt.Sprintf("cannot resolve node %s: %v", pod.Spec.NodeName, err)
	}
	if node.Annotations[NodeReadyLabel] != "true" {
		return fmt.Sprintf("node %s is not criu-ready (%s != true)", pod.Spec.NodeName, NodeReadyLabel)
	}
	return ""
}

// matchingEnginePSP returns the first Ready criu-engine PSP in the namespace
// whose selector matches the pod labels, or nil.
func (r *PSMTReconciler) matchingEnginePSP(ctx context.Context, namespace string, podLabels map[string]string) (*unstructured.Unstructured, error) {
	list := contract.NewList(contract.GVKPolicy)
	if err := r.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	set := labels.Set(podLabels)
	for i := range list.Items {
		psp := &list.Items[i]
		if psp.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
			continue
		}
		if !hasReadyTrue(psp) || !contract.PolicyIsManualStop(psp) {
			continue
		}
		sel, err := contract.PolicySelector(psp)
		if err != nil || !sel.Matches(set) {
			continue
		}
		return psp, nil
	}
	return nil, nil
}

func hasReadyTrue(u *unstructured.Unstructured) bool {
	for _, c := range contract.Conditions(u) {
		if c.Type == contract.CondReady && c.Status == metav1.ConditionTrue {
			return true
		}
	}
	return false
}

func (r *PSMTReconciler) storageFor(ctx context.Context, psp *unstructured.Unstructured) (bucket, path string, err error) {
	scName, ok := contract.PolicyStorageConfigName(psp)
	if !ok {
		return "", "", fmt.Errorf("PSP %s has no storageConfigName", psp.GetName())
	}
	pssc := contract.New(contract.GVKStorageConfig)
	if err := r.Get(ctx, types.NamespacedName{Name: scName}, pssc); err != nil {
		return "", "", err
	}
	return contract.GCSLocation(pssc)
}

// setSnapshotTerminal records a terminal outcome, counting it only when it
// was written (the PSMT is re-enqueued by its own writes and by the
// PodSnapshot watch).
func (r *PSMTReconciler) setSnapshotTerminal(ctx context.Context, namespace, name, reason, msg string) error {
	wrote, err := r.patchSnapshotConditions(ctx, namespace, name,
		metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionFalse, Reason: reason, Message: msg},
		metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg},
	)
	if err == nil && wrote {
		checkpointOutcomes.WithLabelValues(reason).Inc()
	}
	return err
}

func (r *PSMTReconciler) setSnapshotPending(ctx context.Context, namespace, name, msg string) error {
	_, err := r.patchSnapshotConditions(ctx, namespace, name,
		metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonPending, Message: msg},
	)
	return err
}

// patchSnapshotConditions sets conds on the PodSnapshot and reports whether
// it wrote them. It writes nothing when the conditions wouldn't change.
func (r *PSMTReconciler) patchSnapshotConditions(ctx context.Context, namespace, name string, conds ...metav1.Condition) (bool, error) {
	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, ps); err != nil {
		return false, err
	}
	// Never regress a snapshot the node agent already progressed: Ready
	// (upload verified), terminal reasons and a timed-out checkpoint outrank
	// the controller half.
	if hasOutcome(ps) {
		return false, nil
	}
	before := contract.Conditions(ps)
	for _, c := range conds {
		if err := contract.SetCondition(ps, c); err != nil {
			return false, err
		}
	}
	if conditionsEqual(before, contract.Conditions(ps)) {
		return false, nil
	}
	if err := r.Status().Update(ctx, ps); err != nil {
		return false, err
	}
	return true, nil
}

// hasBucket: the work order can be handed to a node agent. The engine
// controller withholds the bucket for a refusal or a storage failure, and
// agents ignore an order without one.
func hasBucket(ps *unstructured.Unstructured) bool {
	b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket")
	return b != ""
}

// awaitingCancel: a cancel was requested that a node agent can still
// confirm. A bucketless order's request can't be confirmed by any agent.
func awaitingCancel(ps *unstructured.Unstructured) bool {
	return cancelRequested(ps) && hasBucket(ps)
}

// specTriggerName is the PodSnapshot spec field naming the PSMT the work
// order answers. Engine-internal: the orchestrator doesn't read it.
const specTriggerName = "triggerName"

func snapshotTrigger(ps *unstructured.Unstructured) string {
	name, _, _ := unstructured.NestedString(ps.Object, "spec", specTriggerName)
	return name
}

// cancelWorkOrders asks the node agent to cancel every unfinished CRIU work
// order that answers the deleted trigger (Checkpoint=Unknown/CancelRequested).
// The request is deliberately not terminal: the agent writes Cancelled once
// its work has actually stopped, so the orchestrator doesn't treat the
// checkpoint as settled while the source may still be frozen. Work orders
// created before the trigger name was recorded can't be matched here (the
// agent still honours a deleted PodSnapshot). If a trigger with the same name
// is created again before the agent acts, its reconcile replaces the request
// with Pending and the work order carries on for the new trigger.
//
// A work order without a bucket (the trigger was deleted before this
// controller recorded its refusal or storage failure) never reaches an agent,
// so nothing would confirm a request. The source was never touched, so this
// controller records Cancelled itself, also over a request already written.
// A PodSnapshot deleted since the List is skipped.
func (r *PSMTReconciler) cancelWorkOrders(ctx context.Context, trigger types.NamespacedName) error {
	list := contract.NewList(contract.GVKPodSnapshot)
	if err := r.List(ctx, list, client.InNamespace(trigger.Namespace),
		client.MatchingLabels{contract.LabelEngine: contract.EngineCRIU}); err != nil {
		return err
	}
	for i := range list.Items {
		ps := &list.Items[i]
		if snapshotTrigger(ps) != trigger.Name || hasOutcome(ps) || awaitingCancel(ps) {
			continue
		}
		var err error
		if !hasBucket(ps) {
			log.FromContext(ctx).Info("trigger deleted before its work order was handed to a node agent; cancelling it", "snapshot", ps.GetName())
			err = r.setSnapshotTerminal(ctx, trigger.Namespace, ps.GetName(), contract.ReasonCancelled,
				fmt.Sprintf("trigger %s was deleted before the work order was handed to a node agent; the source container was not touched", trigger.Name))
		} else {
			log.FromContext(ctx).Info("trigger deleted; asking the node agent to cancel its work order", "snapshot", ps.GetName())
			_, err = r.patchSnapshotConditions(ctx, trigger.Namespace, ps.GetName(), metav1.Condition{
				Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonCancelRequested,
				Message: fmt.Sprintf("trigger %s was deleted; waiting for the node agent to confirm the cancel", trigger.Name),
			})
		}
		if client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// podSnapshotToTrigger maps an unfinished CRIU work order to its trigger, so
// the trigger is reconciled (and a deleted one cancels the work order) even
// when the PSMT delete event was missed, for example across an engine restart.
func podSnapshotToTrigger(_ context.Context, obj client.Object) []reconcile.Request {
	ps, ok := obj.(*unstructured.Unstructured)
	if !ok || ps.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
		return nil
	}
	trigger := snapshotTrigger(ps)
	if trigger == "" || hasOutcome(ps) || awaitingCancel(ps) {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: ps.GetNamespace(), Name: trigger}}}
}

// SetupWithManager registers the PSMT watcher, plus a PodSnapshot watch that
// maps each unfinished work order back to its trigger.
func (r *PSMTReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(contract.New(contract.GVKManualTrigger)).
		Watches(contract.New(contract.GVKPodSnapshot), handler.EnqueueRequestsFromMapFunc(podSnapshotToTrigger)).
		Named("psengine-psmt").
		Complete(r)
}
