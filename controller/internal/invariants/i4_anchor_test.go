package invariants

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/gke-labs/pod-migration/controller/internal/util"
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

func TestEvaluateI4_SnapshottingProgress(t *testing.T) {
	now := time.Now()
	ago := func(d time.Duration) *metav1.Time {
		ts := metav1.NewTime(now.Add(-d))
		return &ts
	}
	snapJob := func(name, snapRef string, snapshottingStart time.Duration, timeout string) *pmv1alpha1.PodMigrationJob {
		var anns map[string]string
		if timeout != "" {
			anns = map[string]string{AnnotationMigrationTimeout: timeout}
		}
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              name,
				CreationTimestamp: *ago(snapshottingStart + 5*time.Minute),
				Annotations:       anns,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
				SnapshotRef:           snapRef,
				SnapshottingStartTime: ago(snapshottingStart),
			},
		}
	}

	t.Run("positive_progressing_via_SnapshotProgress", func(t *testing.T) {
		job := snapJob("pmj-prog", "snap-prog", 45*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-prog": {LastProgressTime: now.Add(-1 * time.Minute)},
			},
		})
		if len(vs) != 0 {
			t.Fatalf("expected 0 I4 violations for progressing Snapshotting job, got %+v", vs)
		}
	})

	t.Run("positive_ready_snapshot_within_MaxMigrationTimeout", func(t *testing.T) {
		job := snapJob("pmj-ready", "snap-ready", 45*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-ready": {Ready: true},
			},
		})
		if len(vs) != 0 {
			t.Fatalf("expected 0 I4 violations for ready Snapshotting job within 2h cap, got %+v", vs)
		}
	})

	t.Run("positive_progressing_via_PrimarySnapshotConditions_fallback", func(t *testing.T) {
		job := snapJob("pmj-cond", "snap-cond", 25*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			PrimarySnapshotConditions: []metav1.Condition{
				{
					Type:               "Checkpoint",
					Status:             metav1.ConditionFalse,
					Reason:             "InProgress",
					LastTransitionTime: *ago(2 * time.Minute),
				},
			},
		})
		if len(vs) != 0 {
			t.Fatalf("expected 0 I4 violations when PrimarySnapshotConditions shows recent InProgress, got %+v", vs)
		}
	})

	t.Run("positive_namespace_pmj_progressing", func(t *testing.T) {
		primary := snapJob("pmj-primary", "snap-primary", 2*time.Minute, "10m")
		other := snapJob("pmj-other", "snap-other", 30*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:           now,
			PrimaryPMJ:    primary,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{*primary, *other},
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-other": {LastProgressTime: now.Add(-1 * time.Minute)},
			},
		})
		if len(vs) != 0 {
			t.Fatalf("expected 0 I4 violations when NamespacePMJs job is actively progressing, got %+v", vs)
		}
	})

	t.Run("negative_stalled_past_effectiveTimeout", func(t *testing.T) {
		job := snapJob("pmj-stalled", "snap-stalled", 25*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-stalled": {LastProgressTime: now.Add(-12 * time.Minute)},
			},
		})
		if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
			t.Fatalf("expected one I4 ActivePMJExceededDeadline violation for stalled snapshot, got %+v", vs)
		}
	})

	t.Run("negative_exceeded_MaxMigrationTimeout_even_while_progressing", func(t *testing.T) {
		job := snapJob("pmj-cap", "snap-cap", util.MaxMigrationTimeout+time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-cap": {LastProgressTime: now.Add(-30 * time.Second)},
			},
		})
		if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
			t.Fatalf("expected I4 violation past MaxMigrationTimeout hard cap, got %+v", vs)
		}
	})

	t.Run("negative_exceeded_MaxMigrationTimeout_even_when_annotation_exceeds_2h", func(t *testing.T) {
		job := snapJob("pmj-over-cap", "snap-over-cap", util.MaxMigrationTimeout+time.Minute, "4h")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-over-cap": {Ready: true},
			},
		})
		if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
			t.Fatalf("expected I4 violation past MaxMigrationTimeout even with >2h annotation, got %+v", vs)
		}
	})

	t.Run("negative_missing_SnapshotRef", func(t *testing.T) {
		job := snapJob("pmj-noref", "", 20*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-noref": {LastProgressTime: now.Add(-1 * time.Minute)},
			},
		})
		if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
			t.Fatalf("expected I4 violation when SnapshotRef is empty, got %+v", vs)
		}
	})

	t.Run("negative_failed_snapshot_not_tolerated", func(t *testing.T) {
		job := snapJob("pmj-failed", "snap-failed", 20*time.Minute, "10m")
		vs := EvaluateI4(&ReconcileSnapshot{
			Now:        now,
			PrimaryPMJ: job,
			SnapshotProgress: map[string]SnapshotProgress{
				"default/pmj-failed": {Failed: true, LastProgressTime: now.Add(-10 * time.Second)},
			},
		})
		if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
			t.Fatalf("expected I4 violation when SnapshotProgress reports Failed, got %+v", vs)
		}
	})

	t.Run("negative_stalled_CRIU_reasons_ignored_in_PrimarySnapshotConditions", func(t *testing.T) {
		for _, reason := range []string{"CheckpointTimedOut", "CancelRequested"} {
			job := snapJob("pmj-criu-"+reason, "snap-criu", 20*time.Minute, "10m")
			vs := EvaluateI4(&ReconcileSnapshot{
				Now:        now,
				PrimaryPMJ: job,
				PrimarySnapshotConditions: []metav1.Condition{
					{
						Type:               "Checkpoint",
						Status:             metav1.ConditionUnknown,
						Reason:             reason,
						LastTransitionTime: *ago(10 * time.Second),
					},
				},
			})
			if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
				t.Fatalf("expected I4 violation for stalled CRIU reason %s, got %+v", reason, vs)
			}
		}
	})
}
