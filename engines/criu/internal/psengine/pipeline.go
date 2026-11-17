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

// Node-side checkpoint pipeline: the engine controller's PSMT watcher
// materializes a PodSnapshot whose spec is a complete work order (source pod,
// container, node, bucket, path); the agent on that node runs this reconciler
// and performs the checkpoint and upload. The PodSnapshot CR is the handoff
// channel.
//
// Cancel (C9, #106): the PodSnapshotManualTrigger is the order. When the
// orchestrator deletes it, the controller half asks for a cancel on the
// PodSnapshot (Checkpoint=Unknown/CancelRequested, psmt_reconciler.go). The
// agent looks for that request, and with live reads for the trigger itself,
// before it starts and right before the source stop, the one step it can't
// undo; cached reads cover the stages in between, and a poller aborts an
// in-flight upload. Only the agent writes the terminal Cancelled, once its
// work has actually stopped, so the orchestrator never sees a settled
// snapshot while the source may still be frozen or about to be stopped.
//
// The kubelet checkpoint call is not cancelled. Cancelling it doesn't end
// the freeze: the kubelet cancels, but the dump runs to its end and only then
// does containerd resume the container (#106). Waiting for the call keeps
// Cancelled honest (the source is running again when it's written), keeps
// the per-node single-flight covering the dump, and lets the agent delete the
// tar it was handed.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// NodeOps is the node agent mechanism seam (implemented over internal/node's
// kubelet client, containerd access and CRIU bundle; faked in tests).
type NodeOps interface {
	// Checkpoint drives the kubelet ContainerCheckpoint API and validates the
	// tar; returns its local path.
	Checkpoint(ctx context.Context, ns, pod, container string) (tarPath string, err error)
	// SourceInfo captures the source container's OCI runtime spec + image —
	// both travel with the archive; the restore node has no source to inspect.
	SourceInfo(ctx context.Context, ns, pod, container string) (specJSON []byte, imageName string, err error)
	// StopContainer enforces postCheckpoint:stop after the verified upload
	// (the kubelet API checkpoints without stopping; the orchestrator's
	// downstream choreography depends on the stop).
	StopContainer(ctx context.Context, ns, pod, container string) error
}

// Manifest is the archive's integrity + identity record; its presence in GCS
// marks the upload complete (it is written last).
type Manifest struct {
	Snapshot      string    `json:"snapshot"`
	SourcePod     string    `json:"sourcePod"`
	SourcePodUID  string    `json:"sourcePodUID"`
	Container     string    `json:"container"`
	Node          string    `json:"node"`
	ImageName     string    `json:"imageName"`
	TarSHA256     string    `json:"tarSha256"`
	TarBytes      int64     `json:"tarBytes"`
	SpecSHA256    string    `json:"specSha256"`
	CPUFamily     string    `json:"cpuFamily,omitempty"` // restore-side compat guard
	EngineVersion string    `json:"engineVersion"`
	CreatedAt     time.Time `json:"createdAt"`
}

const engineVersion = "psengine/v1alpha1"

// snapshotSpec is the work order the PSMT watcher wrote into the PodSnapshot.
type snapshotSpec struct {
	SourcePod    string
	SourcePodUID string
	Container    string
	NodeName     string
	Bucket       string
	Path         string
}

func readSnapshotSpec(ps *unstructured.Unstructured) (snapshotSpec, error) {
	var s snapshotSpec
	get := func(field string) string {
		v, _, _ := unstructured.NestedString(ps.Object, "spec", field)
		return v
	}
	s.SourcePod, s.SourcePodUID = get("sourcePod"), get("sourcePodUID")
	s.Container, s.NodeName = get("container"), get("nodeName")
	s.Bucket, s.Path = get("bucket"), get("path")
	if s.SourcePod == "" || s.Container == "" || s.NodeName == "" || s.Bucket == "" {
		return s, fmt.Errorf("incomplete work order: sourcePod=%q container=%q nodeName=%q bucket=%q",
			s.SourcePod, s.Container, s.NodeName, s.Bucket)
	}
	return s, nil
}

// ArchivePrefix is the per-snapshot GCS prefix: <path>/<snapshot>/.
func ArchivePrefix(pathPrefix, snapshot string) string {
	return path.Join(pathPrefix, snapshot)
}

// NodeSnapshotReconciler runs in the node agent and executes the checkpoint
// pipeline for PodSnapshots targeting this node.
type NodeSnapshotReconciler struct {
	client.Client
	NodeName  string
	Ops       NodeOps
	Store     ObjectStore
	CPUFamily string // from the node agent's capability probe; recorded in the manifest
	// APIReader reads past the cache: the PodSnapshot and its trigger for the
	// cancel checks before the pipeline starts and right before the source
	// stop, and the PodSnapshot in finish. Nil falls back to Client (tests).
	APIReader client.Reader
	// CancelPollInterval is how often an in-flight upload looks for a cancel
	// (cached read). Zero means defaultCancelPollInterval.
	CancelPollInterval time.Duration

	inflight sync.Map
}

const defaultCancelPollInterval = 2 * time.Second

// The live check before the source stop retries for about 3.5 s (waits of
// 0.5, 1 and 2 s between four attempts), each attempt bounded by
// preStopCheckTimeout, before it fails closed. A brief API server blip then
// doesn't turn a durable archive into Failed, and one hung request can't use
// up the budget. Variables so tests can shrink the waits.
var (
	preStopCheckBackoff = wait.Backoff{Steps: 4, Duration: 500 * time.Millisecond, Factor: 2}
	preStopCheckTimeout = 2 * time.Second
)

// errCancelled marks a pipeline that stopped because its work order was
// withdrawn (trigger deleted).
var errCancelled = errors.New("cancelled: the trigger was deleted")

// errFinished marks a pipeline that stopped because its PodSnapshot already
// has an outcome; there is nothing to write.
var errFinished = errors.New("the PodSnapshot already has an outcome")

// errStopCommitted ends the cancel poller once the pipeline commits to the
// source stop.
var errStopCommitted = errors.New("source stop committed")

func (r *NodeSnapshotReconciler) liveReader() client.Reader {
	if r.APIReader != nil {
		return r.APIReader
	}
	return r.Client
}

func (r *NodeSnapshotReconciler) cancelPollInterval() time.Duration {
	if r.CancelPollInterval > 0 {
		return r.CancelPollInterval
	}
	return defaultCancelPollInterval
}

// workOrderState is what cancelCheck found.
type workOrderState int

const (
	// workOrderActive: carry on.
	workOrderActive workOrderState = iota
	// workOrderCancelled: the work order was withdrawn.
	workOrderCancelled
	// workOrderFinished: the PodSnapshot already has an outcome (Ready, or a
	// terminal reason). Nothing more to do, and nothing to write.
	workOrderFinished
)

// cancelCheck reports whether the work order was withdrawn or already has an
// outcome, and why. Withdrawn: the PodSnapshot is gone, being deleted, or
// carries a cancel request; or, with checkTrigger, the trigger it answers is
// gone or being deleted. Reading the trigger directly means a cancel isn't
// missed while the engine controller is down or behind. Work orders from
// before the trigger name was recorded skip the trigger check. An outcome is
// checked before the trigger, because deleting the trigger of a Ready
// snapshot is the normal Evicting cleanup, not a cancel.
func cancelCheck(ctx context.Context, rd client.Reader, key types.NamespacedName, checkTrigger bool) (workOrderState, string, error) {
	ps := contract.New(contract.GVKPodSnapshot)
	if err := rd.Get(ctx, key, ps); err != nil {
		if apierrors.IsNotFound(err) {
			return workOrderCancelled, "the PodSnapshot was deleted", nil
		}
		return workOrderActive, "", err
	}
	if ps.GetDeletionTimestamp() != nil {
		return workOrderCancelled, "the PodSnapshot is being deleted", nil
	}
	if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
		return workOrderFinished, "the PodSnapshot already has an outcome", nil
	}
	if cancelRequested(ps) {
		return workOrderCancelled, "the engine controller saw the trigger deleted", nil
	}
	trigger := snapshotTrigger(ps)
	if !checkTrigger || trigger == "" {
		return workOrderActive, "", nil
	}
	psmt := contract.New(contract.GVKManualTrigger)
	if err := rd.Get(ctx, types.NamespacedName{Namespace: key.Namespace, Name: trigger}, psmt); err != nil {
		if apierrors.IsNotFound(err) {
			return workOrderCancelled, fmt.Sprintf("trigger %s was deleted", trigger), nil
		}
		return workOrderActive, "", err
	}
	if psmt.GetDeletionTimestamp() != nil {
		return workOrderCancelled, fmt.Sprintf("trigger %s is being deleted", trigger), nil
	}
	return workOrderActive, "", nil
}

// cancelRequested: the controller half asked for a cancel, or the agent
// already recorded one.
func cancelRequested(ps *unstructured.Unstructured) bool {
	for _, c := range contract.Conditions(ps) {
		if c.Type == contract.CondCheckpoint &&
			(c.Reason == contract.ReasonCancelRequested || c.Reason == contract.ReasonCancelled) {
			return true
		}
	}
	return false
}

func (r *NodeSnapshotReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("podsnapshot", req.NamespacedName, "node", r.NodeName)

	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, req.NamespacedName, ps); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if ps.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
		return ctrl.Result{}, nil
	}
	if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
		return ctrl.Result{}, nil
	}
	if b, _, _ := unstructured.NestedString(ps.Object, "spec", "bucket"); b == "" {
		// No bucket means no work order: the controller half withheld it for
		// a refusal or a storage failure, and records that outcome itself.
		// Writing Failed here (from every node's agent) would pre-empt its
		// Refused, which patchSnapshotConditions then won't overwrite.
		return ctrl.Result{}, nil
	}
	spec, err := readSnapshotSpec(ps)
	if err != nil {
		// An unreadable work order that does carry a bucket is a bug —
		// report as Failed.
		return ctrl.Result{}, r.finish(ctx, req, contract.ReasonFailed, err.Error())
	}
	if spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil // another node's work
	}

	if _, busy := r.inflight.LoadOrStore(req.String(), true); busy {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	defer r.inflight.Delete(req.String())

	// The cache can lag a cancel, and the engine controller may not have
	// seen the trigger's deletion yet; ask the API server before taking a
	// checkpoint. The cache can also lag an outcome: a stale Pending copy of
	// a snapshot that already went Ready, whose trigger the orchestrator
	// then deleted, must not be cancelled.
	switch state, why, err := cancelCheck(ctx, r.liveReader(), req.NamespacedName, true); {
	case err != nil:
		return ctrl.Result{}, err
	case state == workOrderFinished:
		return ctrl.Result{}, nil
	case state == workOrderCancelled:
		logger.Info("work order cancelled before the pipeline started; not checkpointing", "why", why)
		return ctrl.Result{}, r.finish(ctx, req, contract.ReasonCancelled,
			fmt.Sprintf("cancelled before the checkpoint started (%s); the source container was not touched", why))
	}

	logger.Info("checkpoint pipeline start", "pod", spec.SourcePod, "container", spec.Container,
		"dest", fmt.Sprintf("gs://%s/%s", spec.Bucket, ArchivePrefix(spec.Path, ps.GetName())))

	if err := r.run(ctx, req.NamespacedName, spec); err != nil {
		if errors.Is(err, errFinished) {
			logger.Info("the PodSnapshot got an outcome elsewhere; source container not stopped", "detail", err.Error())
			return ctrl.Result{}, nil
		}
		if errors.Is(err, errCancelled) {
			logger.Info("checkpoint pipeline cancelled; source container left running", "detail", err.Error())
			return ctrl.Result{}, r.finish(ctx, req, contract.ReasonCancelled, truncateMsg(err.Error(), 1500))
		}
		logger.Error(err, "checkpoint pipeline failed")
		return ctrl.Result{}, r.finish(ctx, req, contract.ReasonFailed, truncateMsg(err.Error(), 1500))
	}
	return ctrl.Result{}, r.finish(ctx, req, contract.ReasonSucceeded, "archive uploaded + verified; source container stopped")
}

// run executes: checkpoint -> hash -> upload spec.json + checkpoint.tar ->
// manifest.json (last, verified) -> stop container. Any error before the stop
// leaves the source running, and so does a cancel (errCancelled).
func (r *NodeSnapshotReconciler) run(parent context.Context, key types.NamespacedName, spec snapshotSpec) error {
	snapshot, ns := key.Name, key.Namespace

	// ctx carries the uploads. The poller cancels it with errCancelled,
	// which aborts an in-flight upload. The checkpoint call and the stop run
	// on parent (see the file comment).
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	pollerDone := make(chan struct{})
	go func() {
		defer close(pollerDone)
		r.pollForCancel(ctx, cancel, key)
	}()
	cancelledAt := func(stage string) error {
		cancelled := errors.Is(context.Cause(ctx), errCancelled)
		if !cancelled {
			// A cached read: cheap, and the live read before the stop is
			// the one that has to be fresh.
			state, _, _ := cancelCheck(parent, r.Client, key, false)
			cancelled = state == workOrderCancelled
		}
		if cancelled {
			return fmt.Errorf("%w %s; source container not stopped", errCancelled, stage)
		}
		return nil
	}

	tarPath, err := r.Ops.Checkpoint(parent, ns, spec.SourcePod, spec.Container)
	if tarPath != "" {
		defer os.Remove(tarPath) // uploaded or failed — either way the node copy is scratch
	}
	if isTimeout(err) {
		// Checked before a cancel: Cancelled says the source runs again, and
		// after a timeout the agent doesn't know that.
		return fmt.Errorf("kubelet checkpoint timed out; the dump may still be running, and past the kubelet's CRI deadline the source can stay frozen until it is resumed by hand (#106); source container not stopped: %w", err)
	}
	if c := cancelledAt("during the checkpoint"); c != nil {
		return c
	}
	if err != nil {
		return fmt.Errorf("kubelet checkpoint: %w", err)
	}

	specJSON, imageName, err := r.Ops.SourceInfo(ctx, ns, spec.SourcePod, spec.Container)
	if c := cancelledAt("after the checkpoint"); c != nil {
		return c
	}
	if err != nil {
		return fmt.Errorf("source spec capture: %w", err)
	}

	f, err := os.Open(tarPath)
	if err != nil {
		return err
	}
	tarSHA, tarBytes, err := SHA256Reader(f)
	f.Close()
	if err != nil {
		return fmt.Errorf("hash tar: %w", err)
	}
	specSHA, _, err := SHA256Reader(strings.NewReader(string(specJSON)))
	if err != nil {
		return err
	}

	prefix := ArchivePrefix(spec.Path, snapshot)
	if _, err := VerifiedPut(ctx, r.Store, spec.Bucket, path.Join(prefix, "spec.json"), strings.NewReader(string(specJSON))); err != nil {
		if c := cancelledAt("during the upload"); c != nil {
			return c
		}
		return fmt.Errorf("upload spec.json: %w", err)
	}
	f, err = os.Open(tarPath)
	if err != nil {
		return err
	}
	n, err := VerifiedPut(ctx, r.Store, spec.Bucket, path.Join(prefix, "checkpoint.tar"), f)
	f.Close()
	if c := cancelledAt("during the upload"); c != nil {
		return c
	}
	if err != nil {
		return fmt.Errorf("upload checkpoint.tar: %w", err)
	}
	if n != tarBytes {
		return fmt.Errorf("upload checkpoint.tar: streamed %d bytes, hashed %d", n, tarBytes)
	}

	mf := Manifest{
		Snapshot: snapshot, SourcePod: spec.SourcePod, SourcePodUID: spec.SourcePodUID,
		Container: spec.Container, Node: spec.NodeName, ImageName: imageName,
		TarSHA256: tarSHA, TarBytes: tarBytes, SpecSHA256: specSHA,
		CPUFamily: r.CPUFamily, EngineVersion: engineVersion, CreatedAt: time.Now().UTC(),
	}
	mfJSON, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return err
	}
	if _, err := VerifiedPut(ctx, r.Store, spec.Bucket, path.Join(prefix, "manifest.json"), strings.NewReader(string(mfJSON))); err != nil {
		if c := cancelledAt("during the upload"); c != nil {
			return c
		}
		return fmt.Errorf("upload manifest.json: %w", err)
	}

	// Commit point. Stop the poller, then confirm with live reads that the
	// trigger still exists and no cancel was requested. A read that keeps
	// failing fails closed: the source keeps running and the snapshot goes
	// Failed. Once past this check the stop runs on the parent context, so a
	// late cancel can't interrupt a SIGKILL half-way. A trigger deleted after
	// this read is not seen; the stop then completes and the snapshot goes
	// Ready, which the orchestrator reads as a completed checkpoint.
	cancel(errStopCommitted)
	<-pollerDone
	if errors.Is(context.Cause(ctx), errCancelled) {
		return fmt.Errorf("%w before the stop; source container not stopped", errCancelled)
	}
	var state workOrderState
	var why string
	err = retry.OnError(preStopCheckBackoff,
		// Forbidden won't fix itself, and a done parent means the agent is
		// shutting down.
		func(err error) bool { return parent.Err() == nil && !apierrors.IsForbidden(err) },
		func() (err error) {
			attempt, cancelAttempt := context.WithTimeout(parent, preStopCheckTimeout)
			defer cancelAttempt()
			state, why, err = cancelCheck(attempt, r.liveReader(), key, true)
			return err
		})
	if err != nil {
		return fmt.Errorf("archive durable, but could not confirm the trigger before stopping the source (source container not stopped): %w", err)
	}
	switch state {
	case workOrderCancelled:
		return fmt.Errorf("%w before the stop (%s); source container not stopped", errCancelled, why)
	case workOrderFinished:
		return fmt.Errorf("%w before the stop; source container not stopped", errFinished)
	}

	// postCheckpoint: stop — only after the archive is provably durable.
	if err := r.Ops.StopContainer(parent, ns, spec.SourcePod, spec.Container); err != nil {
		return fmt.Errorf("archive durable but container stop failed (orchestrator's 30s force-delete will supersede): %w", err)
	}
	return nil
}

// pollForCancel cancels ctx with errCancelled once the work order is
// cancelled. It returns when ctx ends.
func (r *NodeSnapshotReconciler) pollForCancel(ctx context.Context, cancel context.CancelCauseFunc, key types.NamespacedName) {
	t := time.NewTicker(r.cancelPollInterval())
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if state, _, _ := cancelCheck(ctx, r.Client, key, false); state == workOrderCancelled && ctx.Err() == nil {
				cancel(errCancelled)
				return
			}
		}
	}
}

// finish writes the terminal conditions the orchestrator polls. It retries on
// conflict, re-reading past the cache: the controller half may write the same
// PodSnapshot concurrently (Pending, or CancelRequested), and a dropped
// outcome would re-run the pipeline on the requeue. Like
// patchSnapshotConditions, it never overwrites an outcome already recorded:
// a Cancelled or Failed written over Ready would misreport a source that was
// already stopped.
func (r *NodeSnapshotReconciler) finish(ctx context.Context, req ctrl.Request, reason, msg string) error {
	status := metav1.ConditionFalse
	if reason == contract.ReasonSucceeded {
		status = metav1.ConditionTrue
	}
	wrote := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		wrote = false
		ps := contract.New(contract.GVKPodSnapshot)
		if err := r.liveReader().Get(ctx, req.NamespacedName, ps); err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			// Fall back to the cache; a stale copy just conflicts again.
			if err := r.Get(ctx, req.NamespacedName, ps); err != nil {
				return client.IgnoreNotFound(err)
			}
		}
		if contract.IsSnapshotReady(ps) || hasTerminalCheckpoint(ps) {
			return nil
		}
		for _, c := range []metav1.Condition{
			{Type: contract.CondCheckpoint, Status: status, Reason: reason, Message: msg},
			{Type: contract.CondReady, Status: status, Reason: reason, Message: msg},
		} {
			if err := contract.SetCondition(ps, c); err != nil {
				return err
			}
		}
		if err := r.Status().Update(ctx, ps); err != nil {
			return err
		}
		wrote = true
		return nil
	})
	if err == nil && wrote {
		checkpointOutcomes.WithLabelValues(reason).Inc()
	}
	return err
}

// isTimeout: the error says it ran out of time (the node agent's kubelet
// client marks both its own timeout and the kubelet's CRI deadline).
func isTimeout(err error) bool {
	var te interface{ Timeout() bool }
	return errors.As(err, &te) && te.Timeout()
}

func hasTerminalCheckpoint(ps *unstructured.Unstructured) bool {
	for _, c := range contract.Conditions(ps) {
		if c.Type == contract.CondCheckpoint && c.Status == metav1.ConditionFalse &&
			(c.Reason == contract.ReasonFailed || c.Reason == contract.ReasonRefused || c.Reason == contract.ReasonCancelled) {
			return true
		}
	}
	return false
}

func truncateMsg(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// SetupWithManager registers the node pipeline (single-flight per node: one
// checkpoint at a time keeps kubelet + disk pressure bounded).
func (r *NodeSnapshotReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(contract.New(contract.GVKPodSnapshot)).
		Named("psengine-node-checkpoint").
		Complete(r)
}
