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
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
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

// CheckpointRequest is the engine's order to the node agent: freeze the
// container -> kubelet ContainerCheckpoint -> upload to
// gs://Bucket/Path/SnapshotName/ (verified) (preCheckpoint: freeze semantics —
// the kubelet API does not freeze; the orchestrator's flow requires it).
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
// Eligibility is decided once, when the work order is created. Once a work
// order carries a bucket, the node agent owns its outcome: later passes don't
// re-run the eligibility checks, so they can't write Refused or Failed onto a
// running checkpoint (#218).
//
// Refusals are terminal PodSnapshot conditions (reason=Refused), never
// silence, so the orchestrator can transition cleanly and fall open to cold
// eviction.
type PSMTReconciler struct {
	client.Client
	Checkpointer Checkpointer // nil in production (CR handoff to the node agent)
	// APIReader reads live, uncached state. It checks that no job or pod
	// still uses a work order before the work order is deleted (#262).
	// Nil means the cached Client.
	APIReader client.Reader
	// Recorder records on the trigger why it's left unanswered on purpose
	// (WorkOrderInUse, WaitingForWorkOrderDeletion). Nil records nothing.
	Recorder record.EventRecorder

	// waits maps a trigger's NamespacedName to the last wait recorded for
	// it (a recordedWait), so a wait the requeue sees again isn't recorded
	// again. In memory: a restart records each wait once more.
	waits sync.Map
	// now is the clock (tests). Nil means time.Now.
	now func() time.Time
}

// waitEventInterval is how often a wait that goes on is recorded again.
// Events expire (the kube-apiserver's --event-ttl, 1 h by default), and a
// recent event shows the wait is still going.
const waitEventInterval = 10 * time.Minute

type recordedWait struct {
	token string
	at    time.Time
}

// Reasons of the events the PSMT reconciler records on a trigger it leaves
// unanswered on purpose.
const (
	// EventReasonWorkOrderInUse (Warning): an earlier trigger's work order
	// with an outcome is still used by a job or pod, so the trigger isn't
	// answered (#262). Its job may time out and evict the pod cold.
	EventReasonWorkOrderInUse = "WorkOrderInUse"
	// EventReasonWaitingForWorkOrderDeletion (Normal): the work order is
	// being deleted, and the trigger gets a new one once it's gone (#260).
	EventReasonWaitingForWorkOrderDeletion = "WaitingForWorkOrderDeletion"
)

// recordWait records an event on the trigger for a wait, unless the same wait
// (same trigger instance, reason, work order and message) was already
// recorded in the last waitEventInterval. The requeue sees a wait every 2 or
// 30 s; recording it each time would write the Event on every look.
func (r *PSMTReconciler) recordWait(key types.NamespacedName, psmt, ps *unstructured.Unstructured, eventType, reason, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	token := strings.Join([]string{string(psmt.GetUID()), reason, string(ps.GetUID()), msg}, "\x00")
	now := time.Now()
	if r.now != nil {
		now = r.now()
	}
	if prev, ok := r.waits.Load(key); ok {
		if p := prev.(recordedWait); p.token == token && now.Sub(p.at) < waitEventInterval {
			return
		}
	}
	r.waits.Store(key, recordedWait{token: token, at: now})
	if r.Recorder != nil {
		r.Recorder.Event(psmt, eventType, reason, msg)
	}
}

// forgetWait drops the trigger's recorded wait: it's answered, or gone.
func (r *PSMTReconciler) forgetWait(key types.NamespacedName) {
	r.waits.Delete(key)
}

// recordedWaits counts the remembered waits (tests).
func (r *PSMTReconciler) recordedWaits() int {
	n := 0
	r.waits.Range(func(any, any) bool { n++; return true })
	return n
}

func (r *PSMTReconciler) liveReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

// +kubebuilder:rbac:groups=criu.podmigration.gke.io,resources=podsnapshotmanualtriggers,verbs=get;list;watch
// +kubebuilder:rbac:groups=criu.podmigration.gke.io,resources=podsnapshotmanualtriggers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=criu.podmigration.gke.io,resources=podsnapshots,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=criu.podmigration.gke.io,resources=podsnapshots/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=pods;nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get;list
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

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
			r.forgetWait(req.NamespacedName)
			return ctrl.Result{}, r.cancelWorkOrders(ctx, req.NamespacedName)
		}
		return ctrl.Result{}, err
	}
	if psmt.GetDeletionTimestamp() != nil {
		r.forgetWait(req.NamespacedName)
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
	ps := contract.New(contract.GVKPodSnapshot)
	err = r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: snapshotName}, ps)
	if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if err == nil && ps.GetDeletionTimestamp() != nil {
		// Its archive is being deleted (ArchiveFinalizer, #107): never hand it
		// to a trigger re-created for the same pod instance. Once it's gone,
		// that trigger gets a new work order. The orchestrator deletes a
		// job's PodSnapshot only after the job's own trigger is gone, and
		// this controller deletes one only to replace it (#262).
		logger.Info("work order is being deleted; waiting for it to be gone", "snapshot", snapshotName)
		r.recordWait(req.NamespacedName, psmt, ps, corev1.EventTypeNormal, EventReasonWaitingForWorkOrderDeletion,
			"Not answering this trigger yet: work order (PodSnapshot) %s is being deleted (its archive is deleted first). "+
				"The trigger gets a new work order once it is gone. If this goes on, see the PodSnapshot's ArchiveDeleteFailed events.", snapshotName)
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}
	if err == nil && fromEarlierTrigger(ps, psmt) {
		switch {
		case contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps):
			// An earlier instance of this trigger (an earlier job for the
			// same pod instance) left this work order with an outcome. A
			// Ready one holds the source's state at that earlier
			// checkpoint, and the source may have run since. A failure is
			// no outcome of this attempt. Delete it, and give the trigger a
			// new work order once the archive is gone (#262). But never
			// delete one that a job or pod still uses.
			user, ownJob, err := r.workOrderUser(ctx, req.Namespace, snapshotName, psmt)
			if err != nil {
				return ctrl.Result{}, err
			}
			if ownJob {
				// The job that owns this trigger already recorded the work
				// order as its snapshot: the trigger was deleted and created
				// again by the same job, so the checkpoint is the job's own.
				// Take it over and answer with it.
				logger.Info("trigger was created again by the job that already uses the work order; taking it over", "snapshot", snapshotName, "job", user)
				if res, err := r.adoptWorkOrder(ctx, ps, psmt); err != nil || !res.IsZero() {
					return res, err
				}
				break
			}
			if user != "" {
				if workOrderString(ps, specTriggerUID) == "" {
					// Created before the engine recorded the trigger's UID,
					// and judged only by age: answer it as before.
					logger.Info("work order may be an earlier trigger's, but it's still in use; keeping it", "snapshot", snapshotName, "usedBy", user)
					break
				}
				// It belongs to an earlier trigger, so it isn't this
				// trigger's answer either. Leave the trigger unanswered
				// until nothing uses the work order.
				logger.Info("work order was left by an earlier trigger and is still in use; not answering the trigger with it",
					"snapshot", snapshotName, "usedBy", user)
				r.recordWait(req.NamespacedName, psmt, ps, corev1.EventTypeWarning, EventReasonWorkOrderInUse,
					"Not answering this trigger: work order (PodSnapshot) %s, which an earlier trigger left with an outcome, is still used by %s. "+
						"The engine looks again every 30s. Until nothing uses it, this trigger's job waits and may time out and evict the pod cold.",
					snapshotName, user)
				return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
			}
			logger.Info("work order was left by an earlier trigger; deleting it so this trigger gets its own",
				"snapshot", snapshotName, "trigger", string(psmt.GetUID()), "workOrderTrigger", workOrderString(ps, specTriggerUID))
			uid, rv := ps.GetUID(), ps.GetResourceVersion()
			if err := r.Delete(ctx, ps, client.Preconditions{UID: &uid, ResourceVersion: &rv}); err != nil &&
				!apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
		case !hasOutcome(ps):
			// Still in flight: it carries on for this trigger, which takes
			// it over, so its outcome becomes this trigger's (#262). The
			// source has stayed frozen since the checkpoint started, if it
			// started at all.
			if res, err := r.adoptWorkOrder(ctx, ps, psmt); err != nil || !res.IsZero() {
				return res, err
			}
			logger.Info("trigger took over an unfinished work order an earlier trigger created", "snapshot", snapshotName)
		}
		// CheckpointTimedOut never settles: the orchestrator keeps the
		// source fenced and evicts it cold, so it's left as it is.
	}
	// No wait from here on: the trigger is answered below.
	r.forgetWait(req.NamespacedName)
	// Eligibility is decided once, when the work order is created (#218). A
	// work order with a bucket was judged eligible and may already be running
	// on the node agent, which owns its outcome from here. Later passes (the
	// 30 s requeue, the PodSnapshot watch) don't re-judge it: a transient
	// node or storage-config read error, or a pod or node that changed since,
	// must not write Refused or Failed onto a checkpoint in progress. They
	// only answer the trigger and keep the Pending wait honest.
	judged := err == nil && hasBucket(ps)

	var refusal, bucket, path string
	var storageErr error
	if !judged {
		refusal = r.refusalReason(ctx, pod)
		bucket, path, storageErr = r.storageFor(ctx, psp)
	}

	// 1. Materialize the PodSnapshot (idempotent by deterministic name, for
	// one instance of the trigger). Its spec is the complete work order for
	// the node agent.
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
		// The engine deletes the archive before the work order goes away
		// (archive_gc.go, #107).
		ps.SetFinalizers([]string{ArchiveFinalizer})
		spec := map[string]interface{}{
			"sourcePod":    pod.Name,
			"sourcePodUID": string(pod.UID),
			"nodeName":     pod.Spec.NodeName,
			// The trigger this work order answers: its deletion cancels
			// the work order (cancelWorkOrders, podSnapshotToTrigger).
			specTriggerName: req.Name,
			// Which instance of the trigger: a trigger created again under
			// the same name never gets this work order's outcome (#262).
			specTriggerUID: string(psmt.GetUID()),
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
		if err := r.Create(ctx, ps); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Another writer created the work order since the Get. This
				// pass's verdict didn't see it; judge again from a fresh read.
				return ctrl.Result{RequeueAfter: time.Second}, nil
			}
			return ctrl.Result{}, err
		}
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
			SnapshotName: snapshotName, Bucket: workOrderString(ps, "bucket"), Path: workOrderString(ps, "path"),
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
	if ok, rc := isSupportedCRIURuntimeClass(pod.Spec.RuntimeClassName); !ok {
		return fmt.Sprintf("runtimeClassName %q unsupported by criu engine v1", rc)
	}
	if pod.Spec.HostNetwork {
		return "hostNetwork pods unsupported by criu engine"
	}
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		if ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return "sidecar initContainers unsupported by criu engine v1"
		}
	}
	if len(pod.Spec.Containers) != 1 {
		return "multi-container pods unsupported by criu engine v1"
	}
	if strings.TrimSpace(pod.Annotations[contract.AnnotationRestoreStatus]) == contract.RestoreStatusRestored || pod.Annotations[AnnotationInjected] == "true" {
		return "re-checkpointing sibling-restored or holder-wrapped pods is unsupported"
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
// agents ignore an order without one. A bucket also means the trigger was
// judged eligible, once, when the order was created (#218).
func hasBucket(ps *unstructured.Unstructured) bool {
	return workOrderString(ps, "bucket") != ""
}

func workOrderString(ps *unstructured.Unstructured, field string) string {
	v, _, _ := unstructured.NestedString(ps.Object, "spec", field)
	return v
}

// awaitingCancel: a cancel was requested that a node agent can still
// confirm. A bucketless order's request can't be confirmed by any agent.
func awaitingCancel(ps *unstructured.Unstructured) bool {
	return cancelRequested(ps) && hasBucket(ps)
}

// specTriggerName is the PodSnapshot spec field naming the PSMT the work
// order answers. Engine-internal: the orchestrator doesn't read it.
const specTriggerName = "triggerName"

// specTriggerUID is the PodSnapshot spec field holding the UID of the PSMT
// the work order was created for. Engine-internal, like specTriggerName.
const specTriggerUID = "triggerUID"

// fromEarlierTrigger reports whether ps is a work order that an earlier
// instance of psmt (same name, since deleted) created, rather than psmt
// itself. A work order with a trigger UID is matched by UID. One created
// before the engine recorded the UID is judged by age: one created before the
// trigger can't be the trigger's own. Creation timestamps have one-second
// resolution, so a tie counts as the trigger's own. A work order that names
// another trigger is never an earlier instance: that trigger may still be
// using it.
func fromEarlierTrigger(ps, psmt *unstructured.Unstructured) bool {
	if name := snapshotTrigger(ps); name != "" && name != psmt.GetName() {
		return false
	}
	if uid := workOrderString(ps, specTriggerUID); uid != "" {
		return uid != string(psmt.GetUID())
	}
	return ps.GetCreationTimestamp().Time.Before(psmt.GetCreationTimestamp().Time)
}

// adoptWorkOrder records psmt as the trigger the work order answers. A
// non-zero result means look again: someone else wrote the work order since.
func (r *PSMTReconciler) adoptWorkOrder(ctx context.Context, ps, psmt *unstructured.Unstructured) (ctrl.Result, error) {
	before := ps.DeepCopy()
	if err := unstructured.SetNestedField(ps.Object, string(psmt.GetUID()), "spec", specTriggerUID); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Patch(ctx, ps, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// workOrderUser names a PodMigrationJob or pod that still uses the work
// order, or returns "" when nothing does. It reads live, right before a
// delete (#262):
//   - any PodMigrationJob whose status.snapshotRef names it, whatever its
//     phase: the orchestrator releases a job's snapshot itself (#107), and a
//     job concluded without a restore keeps it (#32). ownJob reports that
//     this job is the one that owns psmt (its controller owner reference):
//     the work order is then that job's own checkpoint.
//   - any pod that hasn't terminated whose restore order (<group>/ps-name)
//     names it, until the restore completed (the node agent's restored
//     status or snapshot annotation, or the orchestrator's restore-completed
//     marker, names it). A restored pod no longer reads the archive.
func (r *PSMTReconciler) workOrderUser(ctx context.Context, namespace, name string, psmt *unstructured.Unstructured) (user string, ownJob bool, err error) {
	var owner types.UID
	if ref := metav1.GetControllerOf(psmt); ref != nil {
		owner = ref.UID
	}
	jobs := contract.NewList(contract.GVKPodMigrationJob)
	if err := r.liveReader().List(ctx, jobs, client.InNamespace(namespace)); err != nil {
		return "", false, fmt.Errorf("listing PodMigrationJobs: %w", err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if ref, _, _ := unstructured.NestedString(job.Object, "status", "snapshotRef"); ref == name {
			return "PodMigrationJob " + job.GetName(), owner != "" && job.GetUID() == owner, nil
		}
	}
	pods := &corev1.PodList{}
	if err := r.liveReader().List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return "", false, fmt.Errorf("listing pods: %w", err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		if strings.TrimSpace(pod.Annotations[contract.AnnotationPSName]) != name {
			continue
		}
		restored := strings.TrimSpace(pod.Annotations[contract.AnnotationRestoreStatus]) == contract.RestoreStatusRestored ||
			strings.TrimSpace(pod.Annotations[contract.AnnotationSnapshot]) == name ||
			strings.TrimSpace(pod.Annotations[annotationRestoreCompleted]) == name
		if !restored {
			return "pod " + pod.Name, false, nil
		}
	}
	return "", false, nil
}

// annotationRestoreCompleted is the orchestrator's durable marker of a
// verified restore (#152), valued with the snapshot name.
const annotationRestoreCompleted = "pod-migration.gke.io/restore-completed"

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
// with Pending and the work order carries on for the new trigger. Once a work
// order has an outcome, a trigger created again never gets it: its reconcile
// deletes it and creates a new one (fromEarlierTrigger, #262).
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
