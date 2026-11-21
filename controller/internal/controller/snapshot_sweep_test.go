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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

var sweepGroups = []string{snapshot.DefaultAPIGroup, snapshot.CRIUAPIGroup}

// sweepSnap returns a Ready PodSnapshot in group, created age ago, labelled
// as created by trigger (no label when trigger is empty).
func sweepSnap(group, name, trigger string, age time.Duration) *unstructured.Unstructured {
	u := relSnapshot(group, name, trigger)
	u.SetUID(types.UID("uid-" + group + "-" + name))
	u.SetCreationTimestamp(metav1.NewTime(time.Now().Add(-age)))
	return u
}

func sweepPod(name string, phase corev1.PodPhase, annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: name, Annotations: annotations},
		Status:     corev1.PodStatus{Phase: phase},
	}
}

func sweepTrigger(group, name, answeredWith string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(snapshot.ManualTriggerGVK(group))
	u.SetNamespace(relNS)
	u.SetName(name)
	if answeredWith != "" {
		u.Object["status"] = map[string]interface{}{"snapshotCreated": map[string]interface{}{"name": answeredWith}}
	}
	return u
}

func newSweeper(c client.Client) *OrphanSnapshotSweeper {
	return &OrphanSnapshotSweeper{
		Client:    c,
		APIReader: c,
		Groups:    sweepGroups,
		Interval:  DefaultOrphanSnapshotSweepInterval,
		MinAge:    DefaultOrphanSnapshotMinAge,
	}
}

func sweep(t *testing.T, s *OrphanSnapshotSweeper) int {
	t.Helper()
	n, err := s.SweepOnce(context.Background())
	if err != nil {
		t.Fatalf("SweepOnce: %v", err)
	}
	return n
}

const oldAge = 2 * time.Hour

func TestOrphanSweep_DeletesOrphansInBothGroups(t *testing.T) {
	c := relClient(
		sweepSnap(snapshot.DefaultAPIGroup, "gke-snap", ownTrigger(), oldAge),
		sweepSnap(snapshot.CRIUAPIGroup, "criu-snap", util.FormatPSMTName("db-0", "0f0e0d0c-1111"), oldAge),
	)
	if n := sweep(t, newSweeper(c)); n != 2 {
		t.Fatalf("deleted %d, want 2", n)
	}
	if snapExists(t, c, snapshot.DefaultAPIGroup, "gke-snap") || snapExists(t, c, snapshot.CRIUAPIGroup, "criu-snap") {
		t.Fatal("an orphaned snapshot whose job is gone is still there")
	}
}

func TestOrphanSweep_OwnershipFilter(t *testing.T) {
	for _, tc := range []struct {
		name, trigger string
	}{
		{"unlabelled", ""},
		{"kubeflow pause/resume", "ws-notebook-pause-7d9f"},
		{"not our prefix", "manual-web-0-a1b2c3d4"},
		{"suffix not a uid prefix", "psmt-web-0-nothex!!"},
		{"short uid suffix", "psmt-web-0-a1b2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, tc.trigger, oldAge))
			if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatalf("deleted a snapshot our triggers didn't make (label %q)", tc.trigger)
			}
		})
	}
}

func TestOrphanSweep_MinAge(t *testing.T) {
	c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), DefaultOrphanSnapshotMinAge-time.Minute))
	if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("deleted a snapshot younger than the minimum age")
	}
}

func TestOrphanSweep_NeverDeletesWhatAJobNames(t *testing.T) {
	otherJob := func(mut func(*pmv1alpha1.PodMigrationJob)) *pmv1alpha1.PodMigrationJob {
		j := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "pmj-other-12345678"},
			Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "other"}, TargetPodUID: "12345678-aaaa"},
		}
		mut(j)
		return j
	}
	for _, tc := range []struct {
		name string
		job  *pmv1alpha1.PodMigrationJob
	}{
		{"own job, Failed", relJob(pmv1alpha1.PodMigrationJobPhaseFailed, time.Hour)},
		{"own job, Pending, no snapshotRef", func() *pmv1alpha1.PodMigrationJob {
			j := relJob(pmv1alpha1.PodMigrationJobPhasePending, 0)
			j.Status = pmv1alpha1.PodMigrationJobStatus{}
			return j
		}()},
		{"another job's snapshotRef, Succeeded", otherJob(func(j *pmv1alpha1.PodMigrationJob) {
			j.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
			j.Status.SnapshotRef = relSnap
		})},
		{"job with the trigger's job name but other spec", otherJob(func(j *pmv1alpha1.PodMigrationJob) {
			j.Name = relJobName
		})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge), tc.job)
			if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatal("deleted a snapshot that an existing job names")
			}
		})
	}
}

func TestOrphanSweep_NeverDeletesWhatALivePodReferences(t *testing.T) {
	for _, tc := range []struct {
		name string
		pod  *corev1.Pod
	}{
		{"GKE restore order", sweepPod("r1", corev1.PodRunning, map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap})},
		{"CRIU restore order", sweepPod("r1", corev1.PodPending, map[string]string{snapshot.RestoreAnnotation(snapshot.CRIUAPIGroup): relSnap})},
		{"custom-group restore order", sweepPod("r1", corev1.PodRunning, map[string]string{"criu.example.com/ps-name": relSnap})},
		{"restore-completed", sweepPod("r1", corev1.PodRunning, map[string]string{util.AnnotationRestoreCompleted: relSnap})},
		{"CRIU restored snapshot", sweepPod("r1", corev1.PodRunning, map[string]string{"pod-migrate.io/psengine-snapshot": relSnap})},
		{"gated and assigned to the trigger's job", func() *corev1.Pod {
			p := sweepPod("r1", corev1.PodPending, map[string]string{util.AnnotationAssignedPMJ: relJobName})
			p.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: MigrationGateName}}
			return p
		}()},
		{"terminating pod", func() *corev1.Pod {
			p := sweepPod("r1", corev1.PodRunning, map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap})
			now := metav1.Now()
			p.DeletionTimestamp = &now
			p.Finalizers = []string{"test/hold"}
			return p
		}()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge), tc.pod)
			if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatal("deleted a snapshot that a live pod references")
			}
		})
	}
}

// A pod that went through the pod gate keeps assigned-pmj for its lifetime;
// it holds nothing once ungated.
func TestOrphanSweep_UngatedAssignedPodDoesntHold(t *testing.T) {
	c := relClient(
		sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge),
		sweepPod("r1", corev1.PodRunning, map[string]string{util.AnnotationAssignedPMJ: relJobName}),
	)
	if n := sweep(t, newSweeper(c)); n != 1 {
		t.Fatalf("deleted %d, want 1: an ungated pod has been through the gate", n)
	}
}

func TestOrphanSweep_FinishedPodsDontHold(t *testing.T) {
	c := relClient(
		sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge),
		sweepPod("done", corev1.PodSucceeded, map[string]string{util.AnnotationRestoreCompleted: relSnap}),
		sweepPod("crashed", corev1.PodFailed, map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap}),
	)
	if n := sweep(t, newSweeper(c)); n != 1 {
		t.Fatalf("deleted %d, want 1: a finished pod can't restore", n)
	}
}

func TestOrphanSweep_NeverDeletesWhatATriggerHolds(t *testing.T) {
	for _, tc := range []struct {
		name    string
		trigger *unstructured.Unstructured
	}{
		{"own trigger exists", sweepTrigger(snapshot.DefaultAPIGroup, ownTrigger(), "")},
		{"own trigger in the other group", sweepTrigger(snapshot.CRIUAPIGroup, ownTrigger(), "")},
		{"another trigger answered with it", sweepTrigger(snapshot.DefaultAPIGroup, "psmt-web-0-ffffffff", relSnap)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge), tc.trigger)
			if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatal("deleted a snapshot that a live trigger holds")
			}
		})
	}
}

// The cache can trail the API server: a job created since the cache last
// synced must still hold the snapshot.
func TestOrphanSweep_LiveRecheck(t *testing.T) {
	snap := sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge)
	cached := relClient(snap.DeepCopy())
	live := relClient(snap.DeepCopy(), relJob(pmv1alpha1.PodMigrationJobPhasePending, 0))
	s := newSweeper(cached)
	s.APIReader = live
	if n := sweep(t, s); n != 0 || !snapExists(t, cached, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("deleted a snapshot whose job only the live read sees")
	}
}

// A snapshot recreated under the same name since the list isn't the one
// the sweep judged.
func TestOrphanSweep_RecreatedSnapshotIsLeftAlone(t *testing.T) {
	listed := sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge)
	recreated := listed.DeepCopy()
	recreated.SetUID("uid-recreated")
	cached := relClient(listed)
	live := relClient(recreated)
	s := newSweeper(cached)
	s.APIReader = live
	if n := sweep(t, s); n != 0 || !snapExists(t, cached, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatal("deleted a snapshot that was recreated since the list")
	}
}

func TestOrphanSweep_SkipsSnapshotsBeingDeleted(t *testing.T) {
	snap := sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge)
	now := metav1.Now()
	snap.SetDeletionTimestamp(&now)
	snap.SetFinalizers([]string{"test/hold"})
	if n := sweep(t, newSweeper(relClient(snap))); n != 0 {
		t.Fatalf("deleted %d snapshots already being deleted", n)
	}
}

func TestOrphanSweep_BoundedPerPass(t *testing.T) {
	var objs []client.Object
	for _, name := range []string{"a", "b", "c"} {
		objs = append(objs, sweepSnap(snapshot.DefaultAPIGroup, "snap-"+name, util.FormatPSMTName(name, "abcdef01-0000"), oldAge))
	}
	c := relClient(objs...)
	s := newSweeper(c)
	s.MaxPerPass = 2
	if n := sweep(t, s); n != 2 {
		t.Fatalf("first pass deleted %d, want 2", n)
	}
	if n := sweep(t, s); n != 1 {
		t.Fatalf("second pass deleted %d, want 1", n)
	}
}

func TestOrphanSweep_LeaderOnly(t *testing.T) {
	var r manager.LeaderElectionRunnable = &OrphanSnapshotSweeper{}
	if !r.NeedLeaderElection() {
		t.Fatal("the sweep must run on the leader only")
	}
}

func TestValidateOrphanSnapshotSweep(t *testing.T) {
	for _, tc := range []struct {
		interval, minAge, retention time.Duration
		ok                          bool
	}{
		{DefaultOrphanSnapshotSweepInterval, DefaultOrphanSnapshotMinAge, 0, true},
		{DefaultOrphanSnapshotSweepInterval, DefaultOrphanSnapshotMinAge, MaxSnapshotRetention, true},
		{0, 0, 0, true}, // off
		{-time.Minute, DefaultOrphanSnapshotMinAge, 0, false},
		{time.Second, DefaultOrphanSnapshotMinAge, 0, false},
		{MinOrphanSnapshotSweepInterval, DefaultOrphanSnapshotMinAge, 0, true},
		{DefaultOrphanSnapshotSweepInterval, 5 * time.Minute, 0, false},
		{DefaultOrphanSnapshotSweepInterval, 25 * time.Minute, 20 * time.Minute, false},
		{DefaultOrphanSnapshotSweepInterval, 30 * time.Minute, 20 * time.Minute, true},
	} {
		err := ValidateOrphanSnapshotSweep(tc.interval, tc.minAge, tc.retention)
		if (err == nil) != tc.ok {
			t.Errorf("ValidateOrphanSnapshotSweep(%v, %v, %v) = %v, want ok=%v", tc.interval, tc.minAge, tc.retention, err, tc.ok)
		}
	}
}

func TestOrphanSweep_FrozenSource(t *testing.T) {
	const uid = "a1b2c3d4-0000-4000-8000-000000000001"
	frozen := func() *unstructured.Unstructured {
		u := sweepSnap(snapshot.CRIUAPIGroup, relSnap, util.FormatPSMTName(relPod, uid), oldAge)
		u.Object["status"] = map[string]interface{}{"conditions": []interface{}{
			map[string]interface{}{"type": "Checkpoint", "status": "Unknown", "reason": "CheckpointTimedOut"},
			map[string]interface{}{"type": "SourceFrozen", "status": "True", "reason": "Frozen"},
		}}
		return u
	}
	source := func(podUID string) *corev1.Pod {
		p := sweepPod(relPod, corev1.PodRunning, nil)
		p.UID = types.UID(podUID)
		return p
	}

	t.Run("source still exists", func(t *testing.T) {
		c := relClient(frozen(), source(uid))
		if n := sweep(t, newSweeper(c)); n != 0 || !snapExists(t, c, snapshot.CRIUAPIGroup, relSnap) {
			t.Fatal("deleted the record of a source that may still be frozen")
		}
	})
	t.Run("source replaced under the same name", func(t *testing.T) {
		c := relClient(frozen(), source("ffffffff-0000"))
		if n := sweep(t, newSweeper(c)); n != 1 {
			t.Fatalf("deleted %d, want 1: the frozen source is gone", n)
		}
	})
	t.Run("source gone", func(t *testing.T) {
		c := relClient(frozen())
		if n := sweep(t, newSweeper(c)); n != 1 {
			t.Fatalf("deleted %d, want 1: the frozen source is gone", n)
		}
	})
	t.Run("unfinished, not frozen, source exists", func(t *testing.T) {
		u := frozen()
		u.Object["status"] = map[string]interface{}{"conditions": []interface{}{
			map[string]interface{}{"type": "Checkpoint", "status": "Unknown", "reason": "CheckpointTimedOut"},
		}}
		c := relClient(u, source(uid))
		if n := sweep(t, newSweeper(c)); n != 1 {
			t.Fatalf("deleted %d, want 1: job and trigger are gone, nothing waits for it", n)
		}
	})
}

// Only the live targeted reads see a job or trigger created after the live
// namespace view: they must still hold the snapshot.
func TestOrphanSweep_TargetedRecheckBeforeDelete(t *testing.T) {
	for _, tc := range []struct {
		name string
		obj  client.Object
	}{
		{"job", relJob(pmv1alpha1.PodMigrationJobPhasePending, 0)},
		{"trigger", sweepTrigger(snapshot.CRIUAPIGroup, ownTrigger(), "")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge)
			c := relClient(snap.DeepCopy())
			live := relClient(snap.DeepCopy(), tc.obj)
			s := newSweeper(c)
			// Views (LISTs) see the cache-like client; targeted GETs see live.
			s.APIReader = &listFrom{Reader: live, lists: c}
			if n := sweep(t, s); n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatalf("deleted a snapshot whose %s only the targeted live read sees", tc.name)
			}
		})
	}
}

// listFrom answers Get from Reader and List from lists.
type listFrom struct {
	client.Reader
	lists client.Reader
}

func (l *listFrom) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return l.lists.List(ctx, list, opts...)
}

// A delete that keeps failing must not starve the snapshots behind it.
func TestOrphanSweep_FailingDeletesDontStarveTheRest(t *testing.T) {
	var objs []client.Object
	for i, name := range []string{"a", "b", "c"} {
		objs = append(objs, sweepSnap(snapshot.DefaultAPIGroup, "snap-"+name, util.FormatPSMTName(name, "abcdef01-0000"), oldAge+time.Duration(3-i)*time.Minute))
	}
	base := relClient(objs...)
	c := interceptor.NewClient(base.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == "snap-a" {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "podsnapshots"}, obj.GetName(), errors.New("denied"))
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
	s := newSweeper(c)
	s.MaxPerPass = 2
	n, err := s.SweepOnce(context.Background())
	if err == nil {
		t.Fatal("a failed delete must be reported")
	}
	if n != 2 {
		t.Fatalf("deleted %d, want 2: the oldest snapshot's failing delete starved the rest", n)
	}
}

// A live read that fails deletes nothing.
func TestOrphanSweep_LiveReadErrorDeletesNothing(t *testing.T) {
	c := relClient(sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge))
	live := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*corev1.PodList); ok {
				return errors.New("apiserver unavailable")
			}
			return cl.List(ctx, list, opts...)
		},
	})
	s := newSweeper(c)
	s.APIReader = live
	if n, err := s.SweepOnce(context.Background()); err == nil || n != 0 || !snapExists(t, c, snapshot.DefaultAPIGroup, relSnap) {
		t.Fatalf("SweepOnce with a failing live pod list: deleted %d, err %v", n, err)
	}
}

// A pod or another job that only the live namespace view sees (the cache
// trails it) must still hold the snapshot.
func TestOrphanSweep_LiveViewHolds(t *testing.T) {
	other := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: relNS, Name: "pmj-other-12345678"},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: "other"}, TargetPodUID: "12345678-aaaa"},
		Status:     pmv1alpha1.PodMigrationJobStatus{SnapshotRef: relSnap},
	}
	for _, tc := range []struct {
		name string
		obj  client.Object
	}{
		{"pod with a restore order", sweepPod("r1", corev1.PodPending, map[string]string{snapshot.RestoreAnnotation(snapshot.DefaultAPIGroup): relSnap})},
		{"another job's snapshotRef", other},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snap := sweepSnap(snapshot.DefaultAPIGroup, relSnap, ownTrigger(), oldAge)
			cached := relClient(snap.DeepCopy())
			s := newSweeper(cached)
			s.APIReader = relClient(snap.DeepCopy(), tc.obj)
			if n := sweep(t, s); n != 0 || !snapExists(t, cached, snapshot.DefaultAPIGroup, relSnap) {
				t.Fatalf("deleted a snapshot that only the live view sees held (%s)", tc.name)
			}
		})
	}
}
