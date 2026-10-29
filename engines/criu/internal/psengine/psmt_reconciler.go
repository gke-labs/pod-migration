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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// NodeReadyLabel is set by the node agent's capability probe
// (internal/node/capability.go) on nodes whose kernel + CRIU check pass — the
// engine only checkpoints there.
const NodeReadyLabel = "pod-migrate.io/criu-ready"

// CheckpointRequest is the engine's order to the node agent: kubelet
// ContainerCheckpoint -> upload to gs://Bucket/Path/SnapshotName/ (verified)
// -> stop the container (postCheckpoint: stop semantics — the kubelet API
// does not stop; the orchestrator's flow requires it).
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
			// PSMT deletion = orchestrator moved on (Evicting cleanup or
			// timeout, podmigrationjob_controller.go:91-99,405-419). The
			// PodSnapshot deliberately survives — the replacement pod's
			// restore still needs it.
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
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
		ps.SetLabels(map[string]string{contract.LabelEngine: contract.EngineCRIU})
		spec := map[string]interface{}{
			"sourcePod":    pod.Name,
			"sourcePodUID": string(pod.UID),
			"nodeName":     pod.Spec.NodeName,
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

func (r *PSMTReconciler) setSnapshotTerminal(ctx context.Context, namespace, name, reason, msg string) error {
	checkpointOutcomes.WithLabelValues(reason).Inc()
	return r.patchSnapshotConditions(ctx, namespace, name,
		metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionFalse, Reason: reason, Message: msg},
		metav1.Condition{Type: contract.CondReady, Status: metav1.ConditionFalse, Reason: reason, Message: msg},
	)
}

func (r *PSMTReconciler) setSnapshotPending(ctx context.Context, namespace, name, msg string) error {
	return r.patchSnapshotConditions(ctx, namespace, name,
		metav1.Condition{Type: contract.CondCheckpoint, Status: metav1.ConditionUnknown, Reason: contract.ReasonPending, Message: msg},
	)
}

func (r *PSMTReconciler) patchSnapshotConditions(ctx context.Context, namespace, name string, conds ...metav1.Condition) error {
	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, ps); err != nil {
		return err
	}
	// Never regress a snapshot the node agent already progressed: Ready
	// (upload verified) and terminal reasons outrank the controller half.
	if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
		return nil
	}
	for _, c := range conds {
		if err := contract.SetCondition(ps, c); err != nil {
			return err
		}
	}
	return r.Status().Update(ctx, ps)
}

// SetupWithManager registers the PSMT watcher.
func (r *PSMTReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(contract.New(contract.GVKManualTrigger)).
		Named("psengine-psmt").
		Complete(r)
}
