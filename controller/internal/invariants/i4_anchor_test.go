package invariants

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestEvaluateI4_SnapshottingStartTimeAnchor: the deadline runs from creation
// while the PMJ is Pending, and from Status.SnapshottingStartTime once it has
// left Pending, so a long wait in Pending for the disruption budget (#42)
// doesn't use up the migration's own time. The 2h hard cap restarts there too.
func TestEvaluateI4_SnapshottingStartTimeAnchor(t *testing.T) {
	now := time.Now()
	ago := func(d time.Duration) *metav1.Time {
		ts := metav1.NewTime(now.Add(-d))
		return &ts
	}
	job := func(phase pmv1alpha1.PodMigrationJobPhase, created time.Duration, snapshottingStart, evictingStart *metav1.Time) *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-anchor", CreationTimestamp: *ago(created)},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:                 phase,
				SnapshottingStartTime: snapshottingStart,
				EvictingStartTime:     evictingStart,
			},
		}
	}

	for name, tc := range map[string]struct {
		job         *pmv1alpha1.PodMigrationJob
		wantViolate bool
		wantAnchor  string
	}{
		"long Pending wait, then a normal Snapshotting": {
			job: job(pmv1alpha1.PodMigrationJobPhaseSnapshotting, 12*time.Minute, ago(2*time.Minute), nil),
		},
		"stuck in Snapshotting past the restarted deadline": {
			job:         job(pmv1alpha1.PodMigrationJobPhaseSnapshotting, 25*time.Minute, ago(11*time.Minute), nil),
			wantViolate: true, wantAnchor: "snapshotting started at",
		},
		"stuck in Pending past creation plus the timeout": {
			job:         job(pmv1alpha1.PodMigrationJobPhasePending, 11*time.Minute, nil, nil),
			wantViolate: true, wantAnchor: "created at",
		},
		"Evicting within the hard cap restarted at Snapshotting": {
			job: job(pmv1alpha1.PodMigrationJobPhaseEvicting, 130*time.Minute, ago(60*time.Minute), ago(time.Minute)),
		},
		"Evicting past the hard cap restarted at Snapshotting": {
			job:         job(pmv1alpha1.PodMigrationJobPhaseEvicting, 130*time.Minute, ago(121*time.Minute), ago(time.Minute)),
			wantViolate: true, wantAnchor: "snapshotting started at",
		},
	} {
		t.Run(name, func(t *testing.T) {
			vs := EvaluateI4(&ReconcileSnapshot{Now: now, PrimaryPMJ: tc.job})
			if !tc.wantViolate {
				if len(vs) != 0 {
					t.Fatalf("expected no I4 violation, got %+v", vs)
				}
				return
			}
			if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
				t.Fatalf("expected one I4 ActivePMJExceededDeadline violation, got %+v", vs)
			}
			if !strings.Contains(vs[0].Message, tc.wantAnchor) {
				t.Errorf("message %q doesn't name the deadline anchor %q", vs[0].Message, tc.wantAnchor)
			}
		})
	}
}
