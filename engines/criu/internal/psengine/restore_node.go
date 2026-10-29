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

// Node-side restore: when a replacement pod lands on this node carrying a
// non-empty podsnapshot.gke.io/ps-name annotation (the orchestrator's restore
// order) and our webhook's holder wiring, the agent pulls the archive from
// GCS, verifies it against the manifest, and performs the sandbox-sibling runc
// restore into the holder's namespaces. The outcome is stamped back as an
// annotation so the holder wrapper can fall through to a cold start on failure.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// RestoreInputs carries everything the mechanism layer needs (mirrors
// internal/node/restore.go's restoreInputs).
type RestoreInputs struct {
	SnapshotName  string
	PodNamespace  string
	TargetPod     string
	ContainerName string
	SourcePodUID  string
	TargetPodUID  string
	CheckpointDir string // extracted kubelet tar
	SpecJSON      []byte // source OCI runtime spec
	ImageName     string
}

// RestoreOps is the node agent mechanism seam for the restore half.
type RestoreOps interface {
	// Extract unpacks the kubelet checkpoint tar into dstDir (safe untar).
	Extract(tarPath, dstDir string) error
	// Restore performs the sandbox-sibling runc restore into the target
	// pod's namespaces (the holder keeps the sandbox alive).
	Restore(ctx context.Context, in RestoreInputs) (pid int, durMs int64, err error)
}

// NodeRestoreReconciler watches pods on its node and executes restores.
type NodeRestoreReconciler struct {
	client.Client
	NodeName string
	Ops      RestoreOps
	Store    ObjectStore
	WorkDir  string // default /var/lib/pod-migrate/psengine
	// CPUFamily is this node's CPU model; a manifest recorded on a different
	// family is refused (cross-family restores can crash glibc-heavy apps).
	// Empty disables the guard (both sides honest-absent).
	CPUFamily string

	inflight sync.Map
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshots,verbs=get;list;watch

func (r *NodeRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("pod", req.NamespacedName, "node", r.NodeName)

	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if pod.Spec.NodeName != r.NodeName {
		return ctrl.Result{}, nil
	}
	psName := pod.Annotations[contract.AnnotationPSName]
	if psName == "" || pod.Annotations[AnnotationInjected] != "true" {
		return ctrl.Result{}, nil // not a restore order for this engine
	}
	if pod.Annotations[contract.AnnotationRestoreStatus] != "" {
		return ctrl.Result{}, nil // already decided
	}
	if pod.Status.Phase != corev1.PodRunning {
		// The holder must be up (sandbox alive) before a sibling restore.
		return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
	}

	if _, busy := r.inflight.LoadOrStore(req.String(), true); busy {
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	defer r.inflight.Delete(req.String())

	err := r.restore(ctx, pod, psName)
	status := contract.RestoreStatusRestored
	if err != nil {
		logger.Error(err, "psengine restore failed; holder will cold-start")
		status = contract.RestoreStatusFailed
	} else {
		logger.Info("psengine restore complete", "snapshot", psName)
	}
	restoreOutcomes.WithLabelValues(status).Inc()
	// Stamp the outcome — the holder wrapper reads it via the downward API.
	latest := &corev1.Pod{}
	if gerr := r.Get(ctx, req.NamespacedName, latest); gerr != nil {
		return ctrl.Result{}, client.IgnoreNotFound(gerr)
	}
	if latest.Annotations == nil {
		latest.Annotations = map[string]string{}
	}
	latest.Annotations[contract.AnnotationRestoreStatus] = status
	if uerr := r.Update(ctx, latest); uerr != nil {
		return ctrl.Result{}, uerr
	}
	return ctrl.Result{}, nil
}

func (r *NodeRestoreReconciler) restore(ctx context.Context, pod *corev1.Pod, psName string) error {
	// Resolve the PodSnapshot work order (bucket/path/source identity).
	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: psName}, ps); err != nil {
		return fmt.Errorf("resolve PodSnapshot %s: %w", psName, err)
	}
	spec, err := readSnapshotSpec(ps)
	if err != nil {
		return err
	}

	pmjName := pod.Annotations[contract.AnnotationAssignedPMJ]
	if pmjName == "" {
		return fmt.Errorf("pod %s lacks %s annotation; refusing unverified restore", pod.Name, contract.AnnotationAssignedPMJ)
	}
	pmj := contract.New(contract.GVKPodMigrationJob)
	if err := r.Get(ctx, types.NamespacedName{Namespace: pod.Namespace, Name: pmjName}, pmj); err != nil {
		return fmt.Errorf("resolve assigned PodMigrationJob %s: %w", pmjName, err)
	}
	pmjSrcPod, _, _ := unstructured.NestedString(pmj.Object, "spec", "podName")
	if pmjSrcPod != spec.SourcePod {
		return fmt.Errorf("PodMigrationJob %s targets sourcePod %q, but PodSnapshot %s is for %q", pmjName, pmjSrcPod, psName, spec.SourcePod)
	}
	if pmjSnap, _, _ := unstructured.NestedString(pmj.Object, "status", "snapshotName"); pmjSnap != "" && pmjSnap != psName {
		return fmt.Errorf("PodMigrationJob %s targets snapshot %q, but PodSnapshot %s is %q", pmjName, pmjSnap, psName, psName)
	}
	if !contract.IsSnapshotReady(ps) {
		return fmt.Errorf("PodSnapshot %s is not Ready; refusing to restore an unverified archive", psName)
	}

	dir := filepath.Join(r.workDir(), psName)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	// staged copies are scratch; GCS stays authoritative
	defer func() { _ = os.RemoveAll(dir) }()

	prefix := ArchivePrefix(spec.Path, psName)

	// manifest first — it is the upload-complete marker and integrity record.
	mfBytes, err := r.fetch(ctx, spec.Bucket, path.Join(prefix, "manifest.json"), filepath.Join(dir, "manifest.json"), "")
	if err != nil {
		return fmt.Errorf("fetch manifest: %w", err)
	}
	var mf Manifest
	if err := json.Unmarshal(mfBytes, &mf); err != nil {
		return fmt.Errorf("manifest decode: %w", err)
	}
	// CPU-family guard: refuse a cross-family restore before touching runc
	// (the holder then cold-starts — state lost, app correct).
	if mf.CPUFamily != "" && r.CPUFamily != "" && mf.CPUFamily != r.CPUFamily {
		return fmt.Errorf("cpu family mismatch: archive from %q, this node is %q — refusing cross-family restore", mf.CPUFamily, r.CPUFamily)
	}

	specJSON, err := r.fetch(ctx, spec.Bucket, path.Join(prefix, "spec.json"), filepath.Join(dir, "spec.json"), mf.SpecSHA256)
	if err != nil {
		return fmt.Errorf("fetch spec.json: %w", err)
	}
	tarPath := filepath.Join(dir, "checkpoint.tar")
	if _, err := r.fetch(ctx, spec.Bucket, path.Join(prefix, "checkpoint.tar"), tarPath, mf.TarSHA256); err != nil {
		return fmt.Errorf("fetch checkpoint.tar: %w", err)
	}

	extracted := filepath.Join(dir, "extracted")
	if err := r.Ops.Extract(tarPath, extracted); err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	container := spec.Container
	if len(pod.Spec.Containers) > 0 {
		container = pod.Spec.Containers[0].Name // holder keeps the app container's name
	}
	_, _, err = r.Ops.Restore(ctx, RestoreInputs{
		SnapshotName: psName, PodNamespace: pod.Namespace, TargetPod: pod.Name,
		ContainerName: container, SourcePodUID: mf.SourcePodUID, TargetPodUID: string(pod.UID),
		CheckpointDir: extracted, SpecJSON: specJSON, ImageName: mf.ImageName,
	})
	return err
}

// fetch downloads bucket/object to dst, verifying sha256 when expected != "".
// Returns the content bytes only for small objects (expected use: manifest,
// spec); for the tar the caller reads from dst.
func (r *NodeRestoreReconciler) fetch(ctx context.Context, bucket, object, dst, expectedSHA string) ([]byte, error) {
	rc, err := r.Store.Get(ctx, bucket, object)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	f, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	h := sha256.New()
	small := &boundedBuffer{limit: 4 << 20}
	n, err := io.Copy(io.MultiWriter(f, h, small), rc)
	cerr := f.Close()
	if err != nil {
		return nil, err
	}
	if cerr != nil {
		return nil, cerr
	}
	if expectedSHA != "" {
		if got := hex.EncodeToString(h.Sum(nil)); got != expectedSHA {
			return nil, fmt.Errorf("%s: sha256 %s != manifest %s (%d bytes)", object, got, expectedSHA, n)
		}
	}
	return small.bytes, nil
}

type boundedBuffer struct {
	bytes []byte
	limit int
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if len(b.bytes) < b.limit {
		room := b.limit - len(b.bytes)
		if room > len(p) {
			room = len(p)
		}
		b.bytes = append(b.bytes, p[:room]...)
	}
	return len(p), nil
}

func (r *NodeRestoreReconciler) workDir() string {
	if r.WorkDir != "" {
		return r.WorkDir
	}
	return "/var/lib/pod-migrate/psengine"
}

// SetupWithManager watches pods; the ps-name/injected filter runs in
// Reconcile (annotation predicates on the informer would filter the same
// events at equal cost).
func (r *NodeRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Named("psengine-node-restore").
		Complete(r)
}
