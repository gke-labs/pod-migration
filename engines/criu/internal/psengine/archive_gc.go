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

// Archive garbage collection (C9, #107). The orchestrator deletes a job's
// PodSnapshot once it no longer needs the checkpoint (restore verified, or the
// job failed terminally, after an optional retention). The engine owns the
// archive: a finalizer on every CRIU PodSnapshot holds it until the engine has
// deleted the archive objects from the bucket.
//
// The way out: a work order without a bucket has no archive and is released
// right away; a missing archive (or bucket) is success; and a delete that
// keeps failing (for example a bucket permission error) is retried with
// backoff and reported, then given up after GiveUpAfter, with a Warning event
// naming the archive left behind. So deleting a namespace never wedges on the
// finalizer while the engine controller runs. If the engine controller is gone,
// nothing removes the finalizer: delete the CRIU PodSnapshots before
// uninstalling the engine (engines/criu/README.md).

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// ArchiveFinalizer holds a CRIU PodSnapshot until the engine deleted its
// archive from the bucket.
const ArchiveFinalizer = "pod-migrate.io/archive-cleanup"

// ArchiveObjects are the objects the node agent uploads under a snapshot's
// archive prefix (pipeline.go run). The manifest marks an archive complete,
// so it's deleted first: a half-deleted archive never looks restorable.
var ArchiveObjects = []string{"manifest.json", "checkpoint.tar", "spec.json"}

const (
	// DefaultArchiveGCGiveUpAfter bounds how long the engine retries deleting
	// an archive, from its first failed attempt, before it lets the
	// PodSnapshot go and reports the leak.
	DefaultArchiveGCGiveUpAfter = 10 * time.Minute
	// DefaultArchiveGCSettleWait bounds how long a deleted work order without
	// an outcome waits for the node agent to record one (a deletion cancels
	// it), so an upload in flight can't recreate objects after the delete.
	// It covers the agent's 150 s kubelet checkpoint wait plus the upload.
	DefaultArchiveGCSettleWait = 5 * time.Minute

	archiveGCSettlePoll = 5 * time.Second

	ReasonArchiveDeleted         = "ArchiveDeleted"
	ReasonArchiveDeleteFailed    = "ArchiveDeleteFailed"
	ReasonArchiveDeleteAbandoned = "ArchiveDeleteAbandoned"
)

var archiveGCOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "pm_psengine_archive_gc_total",
	Help: "criu-snapshot-engine archive deletions on PodSnapshot delete, by result (deleted|no_archive|failed|abandoned)",
}, []string{"result"})

func init() {
	metrics.Registry.MustRegister(archiveGCOutcomes)
}

// ArchiveGCReconciler puts ArchiveFinalizer on the engine's PodSnapshots and,
// when one is deleted, deletes its archive before releasing it.
type ArchiveGCReconciler struct {
	client.Client
	Store    ObjectStore
	Recorder record.EventRecorder
	// GiveUpAfter: zero means DefaultArchiveGCGiveUpAfter.
	GiveUpAfter time.Duration
	// SettleWait: zero means DefaultArchiveGCSettleWait.
	SettleWait time.Duration

	now func() time.Time
	// firstFailure maps a PodSnapshot UID to its first failed delete, so a
	// controller that was down past GiveUpAfter doesn't give up on its first
	// transient error. In memory: a restart starts the bound over.
	firstFailure sync.Map
}

// +kubebuilder:rbac:groups=criu.podmigration.gke.io,resources=podsnapshots,verbs=get;list;watch;update;patch

func (r *ArchiveGCReconciler) clock() time.Time {
	if r.now != nil {
		return r.now()
	}
	return time.Now()
}

func (r *ArchiveGCReconciler) giveUpAfter() time.Duration {
	if r.GiveUpAfter > 0 {
		return r.GiveUpAfter
	}
	return DefaultArchiveGCGiveUpAfter
}

func (r *ArchiveGCReconciler) settleWait() time.Duration {
	if r.SettleWait > 0 {
		return r.SettleWait
	}
	return DefaultArchiveGCSettleWait
}

func (r *ArchiveGCReconciler) event(ps *unstructured.Unstructured, eventType, reason, format string, args ...any) {
	if r.Recorder != nil {
		r.Recorder.Eventf(ps, eventType, reason, format, args...)
	}
}

func (r *ArchiveGCReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("podsnapshot", req.NamespacedName)
	ps := contract.New(contract.GVKPodSnapshot)
	if err := r.Get(ctx, req.NamespacedName, ps); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if ps.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
		return ctrl.Result{}, nil
	}

	deletedAt := ps.GetDeletionTimestamp()
	if deletedAt == nil {
		// Also adopts PodSnapshots created before the engine had the
		// finalizer, so their archives are deleted with them too.
		if controllerutil.ContainsFinalizer(ps, ArchiveFinalizer) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, r.patchFinalizers(ctx, ps, func(u *unstructured.Unstructured) {
			controllerutil.AddFinalizer(u, ArchiveFinalizer)
		})
	}
	if !controllerutil.ContainsFinalizer(ps, ArchiveFinalizer) {
		return ctrl.Result{}, nil
	}

	bucket := workOrderString(ps, "bucket")
	if bucket == "" {
		// No bucket, no archive: a refusal or a storage failure.
		archiveGCOutcomes.WithLabelValues("no_archive").Inc()
		return ctrl.Result{}, r.release(ctx, ps)
	}
	prefix := ArchivePrefix(workOrderString(ps, "path"), ps.GetName())
	since := r.clock().Sub(deletedAt.Time)

	if !hasOutcome(ps) && since < r.settleWait() {
		logger.V(1).Info("work order has no outcome yet; waiting for the node agent to cancel it before deleting the archive")
		return ctrl.Result{RequeueAfter: archiveGCSettlePoll}, nil
	}

	var deleteErr error
	for _, name := range ArchiveObjects {
		if err := r.Store.Delete(ctx, bucket, path.Join(prefix, name)); err != nil {
			deleteErr = fmt.Errorf("deleting gs://%s/%s: %w", bucket, path.Join(prefix, name), err)
			break
		}
	}
	if deleteErr != nil {
		first, _ := r.firstFailure.LoadOrStore(ps.GetUID(), r.clock())
		if r.clock().Sub(first.(time.Time)) >= r.giveUpAfter() {
			// Release first.  If the finalizer patch fails (a conflict), the
			// retry must still see the original first failure, and the event
			// and metric must only be recorded once.
			if err := r.release(ctx, ps); err != nil {
				return ctrl.Result{}, err
			}
			r.firstFailure.Delete(ps.GetUID())
			logger.Error(deleteErr, "gave up deleting the archive; released the PodSnapshot and left the archive behind", "archive", "gs://"+bucket+"/"+prefix)
			r.event(ps, corev1.EventTypeWarning, ReasonArchiveDeleteAbandoned,
				"Gave up deleting archive gs://%s/%s after %s; it's left in the bucket: %v", bucket, prefix, r.giveUpAfter(), deleteErr)
			archiveGCOutcomes.WithLabelValues("abandoned").Inc()
			return ctrl.Result{}, nil
		}
		r.event(ps, corev1.EventTypeWarning, ReasonArchiveDeleteFailed, "Failed to delete archive gs://%s/%s, retrying: %v", bucket, prefix, deleteErr)
		archiveGCOutcomes.WithLabelValues("failed").Inc()
		return ctrl.Result{}, deleteErr
	}

	if err := r.release(ctx, ps); err != nil {
		return ctrl.Result{}, err
	}
	r.firstFailure.Delete(ps.GetUID())
	logger.Info("deleted the archive", "archive", "gs://"+bucket+"/"+prefix)
	r.event(ps, corev1.EventTypeNormal, ReasonArchiveDeleted, "Deleted archive gs://%s/%s", bucket, prefix)
	archiveGCOutcomes.WithLabelValues("deleted").Inc()
	return ctrl.Result{}, nil
}

// release removes ArchiveFinalizer.
func (r *ArchiveGCReconciler) release(ctx context.Context, ps *unstructured.Unstructured) error {
	return r.patchFinalizers(ctx, ps, func(u *unstructured.Unstructured) {
		controllerutil.RemoveFinalizer(u, ArchiveFinalizer)
	})
}

// patchFinalizers merge-patches the finalizer list. The patch checks the
// resourceVersion, so another writer's finalizer is never dropped; a conflict
// is retried. A PodSnapshot that's gone has nothing left to patch.
func (r *ArchiveGCReconciler) patchFinalizers(ctx context.Context, ps *unstructured.Unstructured, mutate func(*unstructured.Unstructured)) error {
	before := ps.DeepCopy()
	mutate(ps)
	if err := r.Patch(ctx, ps, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// SetupWithManager registers the archive GC on the engine's PodSnapshots.
// Failed deletes back off from 1 s up to 1 min, so the give-up bound is
// reached on time.
func (r *ArchiveGCReconciler) SetupWithManager(mgr ctrl.Manager) error {
	isCRIU := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[contract.LabelEngine] == contract.EngineCRIU
	})
	return ctrl.NewControllerManagedBy(mgr).
		For(contract.New(contract.GVKPodSnapshot), builder.WithPredicates(isCRIU)).
		WithOptions(controller.Options{
			RateLimiter: workqueue.NewTypedItemExponentialFailureRateLimiter[reconcile.Request](time.Second, time.Minute),
		}).
		Named("psengine-archive-gc").
		Complete(r)
}
