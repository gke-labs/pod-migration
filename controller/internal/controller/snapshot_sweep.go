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

// The orphan sweep, the second half of the controller's checkpoint garbage
// collection (C9, #107). releaseSnapshot deletes a concluded job's own
// PodSnapshot while the job exists. Some snapshots outlive their job: a job
// deleted by hand inside its retention, a job that failed before recording
// snapshotRef, a hold still active when the job was garbage collected, and
// jobs garbage collected before #260. The sweep deletes the PodSnapshots that
// our triggers created and whose job is gone. As with the release, the delete
// is the signal to the engine to delete the archive.

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	// DefaultOrphanSnapshotSweepInterval is how often the leader sweeps
	// orphaned PodSnapshots by default.
	DefaultOrphanSnapshotSweepInterval = 10 * time.Minute
	// DefaultOrphanSnapshotMinAge is how old an orphaned PodSnapshot must be
	// before the sweep deletes it, by default.
	DefaultOrphanSnapshotMinAge = time.Hour
	// OrphanSnapshotMinAgeMargin is how much longer than --snapshot-retention
	// the minimum age must be.
	OrphanSnapshotMinAgeMargin = 10 * time.Minute
	// DefaultOrphanSnapshotSweepBatch bounds the deletions per pass.
	DefaultOrphanSnapshotSweepBatch = 50

	// ReasonOrphanSnapshotDeleted is the event reason on a PodSnapshot the
	// sweep deleted.
	ReasonOrphanSnapshotDeleted = "OrphanSnapshotDeleted"

	orphanSweepPageSize = 500

	// annotationCRIURestoredSnapshot is the CRIU node agent's record of the
	// snapshot it restored into a pod (pod-migrate.io/psengine-snapshot in
	// engines/criu). It stays for the pod's lifetime.
	annotationCRIURestoredSnapshot = "pod-migrate.io/psengine-snapshot"
	// restoreOrderSuffix matches every engine's restore order key,
	// <group>/ps-name, whatever the group.
	restoreOrderSuffix = "/ps-name"
)

// ourTriggerName matches the names util.FormatPSMTName gives our triggers:
// psmt-<pod>-<first 8 hex characters of the pod UID>.
var ourTriggerName = regexp.MustCompile(`^psmt-[a-z0-9]([-a-z0-9.]*[a-z0-9])?-[0-9a-f]{8}$`)

// ValidateOrphanSnapshotSweep checks the sweep flags. An interval of 0
// turns the sweep off. Otherwise the minimum age must be at least
// --snapshot-retention plus OrphanSnapshotMinAgeMargin.
func ValidateOrphanSnapshotSweep(interval, minAge, retention time.Duration) error {
	if interval < 0 {
		return fmt.Errorf("orphan-snapshot-sweep-interval %v must not be negative", interval)
	}
	if interval == 0 {
		return nil
	}
	if floor := retention + OrphanSnapshotMinAgeMargin; minAge < floor {
		return fmt.Errorf("orphan-snapshot-min-age %v must be at least --snapshot-retention (%v) plus %v", minAge, retention, OrphanSnapshotMinAgeMargin)
	}
	return nil
}

// The sweep needs no RBAC beyond the PodMigrationJob reconciler's markers:
// list and delete on podsnapshots, and list on triggers, pods and jobs.

// OrphanSnapshotSweeper periodically deletes PodSnapshots that our triggers
// created and whose PodMigrationJob is gone. Only the leader runs it.
//
// A PodSnapshot is a candidate only if all of these hold:
//   - its gke-pod-snapshot-triggered-by label has the form of our trigger
//     names, psmt-<pod>-<uid8>. GKE sets the label on every snapshot it
//     creates, and the CRIU engine sets it when the name fits. A snapshot
//     without the label is never deleted: there's no other record that one
//     of our triggers made it. A snapshot that a Kubeflow pause/resume
//     created on the shared policy (#60) carries its own trigger's name.
//   - it's older than MinAge and isn't being deleted.
//
// A candidate is kept while any of these hold:
//   - a PodMigrationJob, in any phase, has it as its trigger
//     (FormatPSMTName of its pod and UID), has the job name that goes with
//     the trigger, or names the snapshot in status.snapshotRef.
//   - a trigger with the label's name exists, in any engine group, even
//     while it's being deleted, or any trigger was answered with the
//     snapshot (CRIU names snapshots after the pod).
//   - a pod that isn't Succeeded or Failed carries a restore order naming it
//     (any <group>/ps-name), the restore-completed annotation, or the CRIU
//     engine's restored-snapshot annotation, or is assigned to the job.
//   - it records SourceFrozen=True and its source pod (the label's pod name
//     and UID prefix) still exists: the CRIU node agent may still need it to
//     thaw the source. A snapshot whose checkpoint never finished is
//     otherwise deleted: its job and trigger are gone, so nothing waits for
//     it, and the CRIU engine treats the delete as a cancel.
//
// The checks run first against the cache, then again live (APIReader) right
// before the delete, with the job list read last, since a job is created
// before its trigger. The delete carries a UID precondition.
type OrphanSnapshotSweeper struct {
	// Client reads pods and jobs from the cache. It reads unstructured
	// PodSnapshots and triggers from the API server, and deletes.
	Client client.Client
	// APIReader reads everything live for the check right before a delete.
	APIReader client.Reader
	Recorder  record.EventRecorder
	// Groups are the API groups of the registered snapshot engines.
	Groups   []string
	Interval time.Duration
	MinAge   time.Duration
	// MaxPerPass bounds the live checks and deletes in one pass.
	// DefaultOrphanSnapshotSweepBatch when 0.
	MaxPerPass int
	// Now defaults to time.Now.
	Now func() time.Time
}

// NeedLeaderElection makes the manager run the sweep on the leader only.
func (s *OrphanSnapshotSweeper) NeedLeaderElection() bool { return true }

// Start sweeps every Interval until ctx is done. The first pass runs one
// Interval after the replica becomes the leader.
func (s *OrphanSnapshotSweeper) Start(ctx context.Context) error {
	if s.Interval <= 0 {
		<-ctx.Done()
		return nil
	}
	logger := log.FromContext(ctx).WithName("orphan-snapshot-sweep")
	ctx = log.IntoContext(ctx, logger)
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			deleted, err := s.SweepOnce(ctx)
			if err != nil {
				logger.Error(err, "Orphan PodSnapshot sweep had errors; will retry next pass", "deleted", deleted)
			} else if deleted > 0 {
				logger.Info("Orphan PodSnapshot sweep done", "deleted", deleted)
			}
		}
	}
}

func (s *OrphanSnapshotSweeper) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *OrphanSnapshotSweeper) apiReader() client.Reader {
	if s.APIReader != nil {
		return s.APIReader
	}
	return s.Client
}

// SweepOnce runs one pass and returns how many PodSnapshots it deleted.
func (s *OrphanSnapshotSweeper) SweepOnce(ctx context.Context) (int, error) {
	logger := log.FromContext(ctx)
	budget := s.MaxPerPass
	if budget <= 0 {
		budget = DefaultOrphanSnapshotSweepBatch
	}
	now := s.now()
	deleted := 0
	var errs []error
	views := map[string]*sweepView{}
	for _, group := range s.Groups {
		candidates, err := s.candidates(ctx, group, now)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, snap := range candidates {
			if budget <= 0 {
				logger.V(1).Info("Orphan PodSnapshot sweep reached its per-pass bound; the rest waits for the next pass")
				return deleted, errors.Join(errs...)
			}
			ns := snap.GetNamespace()
			view, ok := views[ns]
			if !ok {
				view, err = s.view(ctx, s.Client, ns)
				if err != nil {
					errs = append(errs, err)
					continue
				}
				views[ns] = view
			}
			if why := view.holds(snap); why != "" {
				logger.V(1).Info("Keeping PodSnapshot", "namespace", ns, "snapshot", snap.GetName(), "why", why)
				continue
			}
			budget--
			ok, err := s.deleteIfStillOrphaned(ctx, group, snap, now)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if ok {
				deleted++
			}
		}
	}
	return deleted, errors.Join(errs...)
}

// candidates lists the PodSnapshots in group that our triggers made, that
// aren't being deleted, and that are older than MinAge, oldest first.
func (s *OrphanSnapshotSweeper) candidates(ctx context.Context, group string, now time.Time) ([]*unstructured.Unstructured, error) {
	var out []*unstructured.Unstructured
	cont := ""
	for {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(snapshot.PodSnapshotGVK(group).GroupVersion().WithKind("PodSnapshotList"))
		opts := []client.ListOption{client.Limit(orphanSweepPageSize)}
		if cont != "" {
			opts = append(opts, client.Continue(cont))
		}
		if err := s.Client.List(ctx, list, opts...); err != nil {
			if meta.IsNoMatchError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("listing PodSnapshots in %s: %w", group, err)
		}
		for i := range list.Items {
			snap := &list.Items[i]
			if s.isCandidate(snap, now) {
				out = append(out, snap)
			}
		}
		cont = list.GetContinue()
		if cont == "" {
			break
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].GetCreationTimestamp().Time.Before(out[j].GetCreationTimestamp().Time)
	})
	return out, nil
}

func (s *OrphanSnapshotSweeper) isCandidate(snap *unstructured.Unstructured, now time.Time) bool {
	if snap.GetDeletionTimestamp() != nil {
		return false
	}
	if !ourTriggerName.MatchString(snap.GetLabels()[snapshot.TriggeredByLabel]) {
		return false
	}
	created := snap.GetCreationTimestamp()
	return !created.IsZero() && now.Sub(created.Time) >= s.MinAge
}

// deleteIfStillOrphaned reads the snapshot and its namespace live, and
// deletes the snapshot if it's still a candidate that nothing holds. It
// reports whether it deleted it.
func (s *OrphanSnapshotSweeper) deleteIfStillOrphaned(ctx context.Context, group string, listed *unstructured.Unstructured, now time.Time) (bool, error) {
	ns, name := listed.GetNamespace(), listed.GetName()
	logger := log.FromContext(ctx).WithValues("namespace", ns, "snapshot", name, "group", group)
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	if err := s.apiReader().Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, snap); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("reading PodSnapshot %s/%s: %w", ns, name, err)
	}
	if snap.GetUID() != listed.GetUID() || !s.isCandidate(snap, now) {
		return false, nil
	}
	view, err := s.view(ctx, s.apiReader(), ns)
	if err != nil {
		return false, err
	}
	trigger := snap.GetLabels()[snapshot.TriggeredByLabel]
	if why := view.holds(snap); why != "" {
		logger.V(1).Info("Keeping PodSnapshot after the live check", "why", why)
		return false, nil
	}
	uid := snap.GetUID()
	if err := s.Client.Delete(ctx, snap, client.Preconditions{UID: &uid}); err != nil {
		if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
			return false, nil
		}
		return false, fmt.Errorf("deleting PodSnapshot %s/%s: %w", ns, name, err)
	}
	logger.Info("Deleted orphaned PodSnapshot whose job is gone", "triggeredBy", trigger, "created", snap.GetCreationTimestamp().Time)
	if s.Recorder != nil {
		s.Recorder.Eventf(snap, corev1.EventTypeNormal, ReasonOrphanSnapshotDeleted,
			"Deleted PodSnapshot %s: its trigger %s and job are gone, and no pod or trigger references it", name, trigger)
	}
	return true, nil
}

// sweepView is what holds snapshots in one namespace.
type sweepView struct {
	jobTriggers  map[string]string // trigger name -> job
	jobNames     map[string]bool
	snapshotRefs map[string]string // snapshot -> job
	triggers     map[string]bool
	answered     map[string]string // snapshot -> trigger
	podRefs      map[string]string // snapshot -> pod
	assigned     map[string]string // job -> pod
	podUIDs      map[string]string // pod -> UID, every pod in any phase
}

// holds returns why snap must be kept, or "" when nothing holds it.
func (v *sweepView) holds(snap *unstructured.Unstructured) string {
	name := snap.GetName()
	trigger := snap.GetLabels()[snapshot.TriggeredByLabel]
	suffix := strings.TrimPrefix(trigger, "psmt-")
	jobName := "pmj-" + suffix
	// The source: psmt-<pod>-<uid8>.
	source, uid8 := "", ""
	if i := strings.LastIndex(suffix, "-"); i > 0 {
		source, uid8 = suffix[:i], suffix[i+1:]
	}
	switch {
	case v.jobTriggers[trigger] != "":
		return fmt.Sprintf("job %s has trigger %s", v.jobTriggers[trigger], trigger)
	case v.jobNames[jobName]:
		return fmt.Sprintf("job %s exists", jobName)
	case v.snapshotRefs[name] != "":
		return fmt.Sprintf("job %s names it", v.snapshotRefs[name])
	case v.triggers[trigger]:
		return fmt.Sprintf("trigger %s exists", trigger)
	case v.answered[name] != "":
		return fmt.Sprintf("trigger %s was answered with it", v.answered[name])
	case v.podRefs[name] != "":
		return fmt.Sprintf("pod %s references it", v.podRefs[name])
	case v.assigned[jobName] != "":
		return fmt.Sprintf("pod %s is assigned to job %s", v.assigned[jobName], jobName)
	case sourceFrozen(snap) && uid8 != "" && strings.HasPrefix(v.podUIDs[source], uid8):
		// The CRIU node agent keeps retrying to thaw a source it froze
		// (SourceFrozen=True) and reads that from the PodSnapshot, and a
		// CheckpointTimedOut source stays frozen until it's evicted or
		// resumed by hand. Keep the record while the source exists.
		return fmt.Sprintf("its source pod %s may still be frozen", source)
	}
	return ""
}

// view reads what holds snapshots in ns through reader: triggers, then
// pods, then jobs. A job is created before its trigger and its snapshot, so
// reading jobs last leaves the shortest window for a new job to be missed.
func (s *OrphanSnapshotSweeper) view(ctx context.Context, reader client.Reader, ns string) (*sweepView, error) {
	v := &sweepView{
		jobTriggers:  map[string]string{},
		jobNames:     map[string]bool{},
		snapshotRefs: map[string]string{},
		triggers:     map[string]bool{},
		answered:     map[string]string{},
		podRefs:      map[string]string{},
		assigned:     map[string]string{},
		podUIDs:      map[string]string{},
	}

	for _, group := range s.Groups {
		triggers := &unstructured.UnstructuredList{}
		triggers.SetGroupVersionKind(snapshot.ManualTriggerGVK(group).GroupVersion().WithKind("PodSnapshotManualTriggerList"))
		if err := reader.List(ctx, triggers, client.InNamespace(ns)); err != nil {
			if meta.IsNoMatchError(err) {
				continue
			}
			return nil, fmt.Errorf("listing triggers in %s/%s: %w", group, ns, err)
		}
		for i := range triggers.Items {
			t := &triggers.Items[i]
			v.triggers[t.GetName()] = true
			if snap, _, _ := unstructured.NestedString(t.Object, "status", "snapshotCreated", "name"); snap != "" {
				v.answered[snap] = t.GetName()
			}
		}
	}

	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(ns)); err != nil {
		return nil, fmt.Errorf("listing pods in %s: %w", ns, err)
	}
	for i := range pods.Items {
		pod := &pods.Items[i]
		v.podUIDs[pod.Name] = string(pod.UID)
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for key, val := range pod.Annotations {
			if val == "" {
				continue
			}
			switch {
			case strings.HasSuffix(key, restoreOrderSuffix),
				key == util.AnnotationRestoreCompleted,
				key == annotationCRIURestoredSnapshot:
				v.podRefs[val] = pod.Name
			case key == util.AnnotationAssignedPMJ:
				v.assigned[val] = pod.Name
			}
		}
	}

	jobs := &pmv1alpha1.PodMigrationJobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(ns)); err != nil {
		return nil, fmt.Errorf("listing PodMigrationJobs in %s: %w", ns, err)
	}
	for i := range jobs.Items {
		job := &jobs.Items[i]
		v.jobNames[job.Name] = true
		v.jobTriggers[util.FormatPSMTName(job.Spec.PodRef.Name, job.Spec.TargetPodUID)] = job.Name
		if job.Status.SnapshotRef != "" {
			v.snapshotRefs[job.Status.SnapshotRef] = job.Name
		}
	}
	return v, nil
}

// sourceFrozen reports whether a PodSnapshot records SourceFrozen=True, the
// CRIU node agent's record that it froze the source and hasn't thawed it.
func sourceFrozen(snap *unstructured.Unstructured) bool {
	conditions, _, _ := unstructured.NestedSlice(snap.Object, "status", "conditions")
	for _, c := range conditions {
		cond, ok := c.(map[string]interface{})
		if ok && cond["type"] == "SourceFrozen" && cond["status"] == "True" {
			return true
		}
	}
	return false
}
