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

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

	inflight sync.Map
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
	spec, err := readSnapshotSpec(ps)
	if err != nil {
		// The controller half owns refusals; an unreadable work order on an
		// engine-labeled snapshot is a bug — report as Failed.
		return ctrl.Result{}, r.finish(ctx, req, contract.ReasonFailed, err.Error())
	}
	if spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil // another node's work
	}

	if _, busy := r.inflight.LoadOrStore(req.String(), true); busy {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	defer r.inflight.Delete(req.String())

	logger.Info("checkpoint pipeline start", "pod", spec.SourcePod, "container", spec.Container,
		"dest", fmt.Sprintf("gs://%s/%s", spec.Bucket, ArchivePrefix(spec.Path, ps.GetName())))

	if err := r.run(ctx, ps.GetName(), ps.GetNamespace(), spec); err != nil {
		logger.Error(err, "checkpoint pipeline failed")
		return ctrl.Result{}, r.finish(ctx, req, contract.ReasonFailed, truncateMsg(err.Error(), 1500))
	}
	return ctrl.Result{}, r.finish(ctx, req, contract.ReasonSucceeded, "archive uploaded + verified; source container stopped")
}

// run executes: checkpoint -> hash -> upload spec.json + checkpoint.tar ->
// manifest.json (last, verified) -> stop container. Any error before the stop
// leaves the source running.
func (r *NodeSnapshotReconciler) run(ctx context.Context, snapshot, ns string, spec snapshotSpec) error {
	tarPath, err := r.Ops.Checkpoint(ctx, ns, spec.SourcePod, spec.Container)
	if err != nil {
		return fmt.Errorf("kubelet checkpoint: %w", err)
	}
	defer os.Remove(tarPath) // uploaded or failed — either way the node copy is scratch

	specJSON, imageName, err := r.Ops.SourceInfo(ctx, ns, spec.SourcePod, spec.Container)
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
		return fmt.Errorf("upload spec.json: %w", err)
	}
	f, err = os.Open(tarPath)
	if err != nil {
		return err
	}
	n, err := VerifiedPut(ctx, r.Store, spec.Bucket, path.Join(prefix, "checkpoint.tar"), f)
	f.Close()
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
		return fmt.Errorf("upload manifest.json: %w", err)
	}

	// postCheckpoint: stop — only after the archive is provably durable.
	if err := r.Ops.StopContainer(ctx, ns, spec.SourcePod, spec.Container); err != nil {
		return fmt.Errorf("archive durable but container stop failed (orchestrator's 30s force-delete will supersede): %w", err)
	}
	return nil
}

// finish writes the terminal conditions the orchestrator polls.
func (r *NodeSnapshotReconciler) finish(ctx context.Context, req ctrl.Request, reason, msg string) error {
	checkpointOutcomes.WithLabelValues(reason).Inc()
	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, req.NamespacedName, ps); err != nil {
		return client.IgnoreNotFound(err)
	}
	status := metav1.ConditionFalse
	if reason == contract.ReasonSucceeded {
		status = metav1.ConditionTrue
	}
	for _, c := range []metav1.Condition{
		{Type: contract.CondCheckpoint, Status: status, Reason: reason, Message: msg},
		{Type: contract.CondReady, Status: status, Reason: reason, Message: msg},
	} {
		if err := contract.SetCondition(ps, c); err != nil {
			return err
		}
	}
	return r.Status().Update(ctx, ps)
}

func hasTerminalCheckpoint(ps *unstructured.Unstructured) bool {
	for _, c := range contract.Conditions(ps) {
		if c.Type == contract.CondCheckpoint && c.Status == metav1.ConditionFalse &&
			(c.Reason == contract.ReasonFailed || c.Reason == contract.ReasonRefused) {
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
