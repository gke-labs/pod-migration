package controller

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// TestPodMigrationJobReconciler_DeadlineRestartsAtSnapshotting: a job's
// deadline runs from its creation while it is Pending, and from the start of
// Snapshotting after that, so a wait for the disruption budget in Pending (#42)
// doesn't use up the migration's own time.
func TestPodMigrationJobReconciler_DeadlineRestartsAtSnapshotting(t *testing.T) {
	for name, tc := range map[string]struct {
		createdAgo, snapshottingAgo time.Duration
		wantTimeout                 bool
	}{
		"waited most of the timeout in Pending, snapshotting for a minute": {12 * time.Minute, time.Minute, false},
		"snapshotting past the timeout":                                    {25 * time.Minute, 11 * time.Minute, true},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			started := metav1.NewTime(time.Now().Add(-tc.snapshottingAgo))
			job := budgetJob(time.Now().Add(-tc.createdAgo), nil)
			job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
			job.Status.SnapshottingStartTime = &started
			c := fake.NewClientBuilder().WithScheme(budgetScheme()).
				WithObjects(budgetSourcePod(), job).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
			r := &PodMigrationJobReconciler{
				Client: c, Scheme: c.Scheme(), Recorder: record.NewFakeRecorder(20),
				SnapshotProvider: &fakeSnapshotProvider{checkStatusResult: &snapshot.Status{Phase: snapshot.PhaseInProgress}},
			}
			key := client.ObjectKeyFromObject(job)
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			got := &pmv1alpha1.PodMigrationJob{}
			if err := c.Get(ctx, key, got); err != nil {
				t.Fatalf("get job: %v", err)
			}
			ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			timedOut := got.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed && ready != nil && ready.Reason == "Timeout"
			if timedOut != tc.wantTimeout {
				t.Errorf("timed out = %t (phase %q, Ready %+v), want %t", timedOut, got.Status.Phase, ready, tc.wantTimeout)
			}
		})
	}
}
