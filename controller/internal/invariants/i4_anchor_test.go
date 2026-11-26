package invariants

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
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

// TestEvaluateI4_ReadySnapshotBound (#231): a Ready snapshot's
// LastProgressTime is when it became Ready. The reconciler moves a job to
// Evicting as soon as it sees a Ready snapshot, so past the job's deadline I4
// tolerates it in Snapshotting for at most the effective timeout plus 30
// seconds after its snapshot became Ready, the same budget as for a snapshot
// still in progress. A Ready snapshot that doesn't say when it became Ready
// is tolerated up to the 2h hard cap. Peers in NamespacePMJs are
// judged the same way, by their own progress.
func TestEvaluateI4_ReadySnapshotBound(t *testing.T) {
	now := time.Now()
	ago := func(d time.Duration) *metav1.Time {
		ts := metav1.NewTime(now.Add(-d))
		return &ts
	}
	snapJob := func(name string, snapshotting time.Duration, timeout string) pmv1alpha1.PodMigrationJob {
		var anns map[string]string
		if timeout != "" {
			anns = map[string]string{AnnotationMigrationTimeout: timeout}
		}
		return pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, CreationTimestamp: *ago(snapshotting + 5*time.Minute), Annotations: anns},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
				SnapshotRef:           "snap-" + name,
				SnapshottingStartTime: ago(snapshotting),
			},
		}
	}
	readyFor := func(d time.Duration) SnapshotProgress {
		return SnapshotProgress{Ready: true, LastProgressTime: now.Add(-d)}
	}
	const budget = MaxMigrationDuration + 30*time.Second

	tests := map[string]struct {
		primary    pmv1alpha1.PodMigrationJob
		peers      []pmv1alpha1.PodMigrationJob
		progress   map[string]SnapshotProgress
		conditions []metav1.Condition
		want       []string // the jobs I4 flags
	}{
		"Ready a minute ago": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(time.Minute)},
		},
		"Ready exactly the timeout plus 30s ago": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(budget)},
		},
		"Ready without a time": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": {Ready: true}},
		},
		"Ready long ago, Snapshotting only since 2 minutes": {
			primary:  snapJob("pmj-a", 2*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(time.Hour)},
		},
		"Ready 20 minutes ago, within a 30m timeout": {
			primary:  snapJob("pmj-a", 45*time.Minute, "30m"),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(20 * time.Minute)},
		},
		"Ready a minute ago, from the snapshot's conditions": {
			primary: snapJob("pmj-a", 45*time.Minute, ""),
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSnapshotsAvailable", LastTransitionTime: *ago(time.Minute)},
			},
		},
		"peer Ready a minute ago": {
			primary:  snapJob("pmj-a", 2*time.Minute, ""),
			peers:    []pmv1alpha1.PodMigrationJob{snapJob("pmj-b", 30*time.Minute, "")},
			progress: map[string]SnapshotProgress{"default/pmj-b": readyFor(time.Minute)},
		},
		"Ready 12 minutes ago": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(12 * time.Minute)},
			want:     []string{"pmj-a"},
		},
		"no time and not Ready, as for a snapshot being deleted": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": {}},
			want:     []string{"pmj-a"},
		},
		"Ready a second more than the timeout plus 30s ago": {
			primary:  snapJob("pmj-a", 45*time.Minute, ""),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(budget + time.Second)},
			want:     []string{"pmj-a"},
		},
		"Ready 31 minutes ago, past a 30m timeout": {
			primary:  snapJob("pmj-a", 45*time.Minute, "30m"),
			progress: map[string]SnapshotProgress{"default/pmj-a": readyFor(31 * time.Minute)},
			want:     []string{"pmj-a"},
		},
		"Ready 12 minutes ago, from the snapshot's conditions": {
			primary: snapJob("pmj-a", 45*time.Minute, ""),
			conditions: []metav1.Condition{
				{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSnapshotsAvailable", LastTransitionTime: *ago(12 * time.Minute)},
			},
			want: []string{"pmj-a"},
		},
		"checkpoint done 12 minutes ago, still uploading, from the snapshot's conditions": {
			primary: snapJob("pmj-a", 45*time.Minute, ""),
			conditions: []metav1.Condition{
				{Type: "Checkpoint", Status: metav1.ConditionTrue, Reason: "Succeeded", LastTransitionTime: *ago(12 * time.Minute)},
				{Type: "StorageReplicated", Status: metav1.ConditionFalse, Reason: "InProgress", LastTransitionTime: *ago(time.Minute)},
			},
			want: []string{"pmj-a"},
		},
		"peer Ready 12 minutes ago": {
			primary:  snapJob("pmj-a", 2*time.Minute, ""),
			peers:    []pmv1alpha1.PodMigrationJob{snapJob("pmj-b", 30*time.Minute, "")},
			progress: map[string]SnapshotProgress{"default/pmj-b": readyFor(12 * time.Minute)},
			want:     []string{"pmj-b"},
		},
		"peer stalled in progress": {
			primary:  snapJob("pmj-a", 2*time.Minute, ""),
			peers:    []pmv1alpha1.PodMigrationJob{snapJob("pmj-b", 30*time.Minute, "")},
			progress: map[string]SnapshotProgress{"default/pmj-b": {LastProgressTime: now.Add(-12 * time.Minute)}},
			want:     []string{"pmj-b"},
		},
		"peer without reported progress": {
			primary: snapJob("pmj-a", 2*time.Minute, ""),
			peers:   []pmv1alpha1.PodMigrationJob{snapJob("pmj-b", 30*time.Minute, "")},
			want:    []string{"pmj-b"},
		},
		"peer Ready a minute ago, past the hard cap": {
			primary:  snapJob("pmj-a", 2*time.Minute, ""),
			peers:    []pmv1alpha1.PodMigrationJob{snapJob("pmj-b", util.MaxMigrationTimeout+time.Minute, "")},
			progress: map[string]SnapshotProgress{"default/pmj-b": readyFor(time.Minute)},
			want:     []string{"pmj-b"},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			primary := tc.primary
			vs := EvaluateI4(&ReconcileSnapshot{
				Now:                       now,
				PrimaryPMJ:                &primary,
				NamespacePMJs:             append([]pmv1alpha1.PodMigrationJob{primary}, tc.peers...),
				SnapshotProgress:          tc.progress,
				PrimarySnapshotConditions: tc.conditions,
			})
			var got []string
			for _, v := range vs {
				if v.InvariantID != "I4" || v.Reason != "ActivePMJExceededDeadline" {
					t.Fatalf("unexpected violation %+v", v)
				}
				got = append(got, v.PMJName)
			}
			sort.Strings(got)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("I4 flagged %v, want %v (violations %+v)", got, tc.want, vs)
			}
		})
	}
}

// TestEvaluateI4_ReadySnapshotFailsJobInStrictMode (#231) follows a job's
// snapshot from its conditions through I4 to a strict failure of the job. The
// job has been in Snapshotting for 45 minutes, past its 10m timeout. Each case
// runs twice: with SnapshotProgress built from the PodSnapshot by
// snapshot.CheckPodSnapshotStatus, as the job reconciler builds it, and with
// the conditions in PrimarySnapshotConditions. For a Ready snapshot, the
// violation says when it became Ready. That time is fixed, so the message
// doesn't change between reconciles.
func TestEvaluateI4_ReadySnapshotFailsJobInStrictMode(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) metav1.Time { return metav1.NewTime(now.Add(-d)) }
	ready := func(d time.Duration) []metav1.Condition {
		return []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "AllSnapshotsAvailable", LastTransitionTime: at(d)}}
	}
	// podSnapshot is the job's PodSnapshot as the reconciler reads it.
	podSnapshot := func(conds []metav1.Condition) *unstructured.Unstructured {
		var raw []interface{}
		for _, c := range conds {
			raw = append(raw, map[string]interface{}{
				"type":               c.Type,
				"status":             string(c.Status),
				"reason":             c.Reason,
				"lastTransitionTime": c.LastTransitionTime.UTC().Format(time.RFC3339),
			})
		}
		return &unstructured.Unstructured{Object: map[string]interface{}{
			"metadata": map[string]interface{}{"namespace": "default", "name": "snap-pmj-a"},
			"status":   map[string]interface{}{"conditions": raw},
		}}
	}
	tests := map[string]struct {
		conditions []metav1.Condition
		wantFail   bool
		wantDetail string // how the I4 message ends
	}{
		"Ready a minute ago": {conditions: ready(time.Minute)},
		"Ready 12 minutes ago": {
			conditions: ready(12 * time.Minute),
			wantFail:   true,
			wantDetail: "(snapshotting started at 2026-11-26T11:15:00Z, snapshot Ready since 2026-11-26T11:48:00Z)",
		},
		"stalled in progress for 12 minutes": {
			conditions: []metav1.Condition{{Type: "Checkpoint", Status: metav1.ConditionFalse, Reason: "InProgress", LastTransitionTime: at(12 * time.Minute)}},
			wantFail:   true,
			wantDetail: "(snapshotting started at 2026-11-26T11:15:00Z)",
		},
	}
	for name, tc := range tests {
		for _, via := range []string{"SnapshotProgress", "PrimarySnapshotConditions"} {
			t.Run(name+", via "+via, func(t *testing.T) {
				started := at(45 * time.Minute)
				job := pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-a", CreationTimestamp: at(50 * time.Minute)},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
						SnapshotRef:           "snap-pmj-a",
						SnapshottingStartTime: &started,
					},
				}
				s := &ReconcileSnapshot{
					Now:           now,
					Reconciler:    "PodMigrationJobReconciler",
					PrimaryPMJ:    &job,
					NamespacePMJs: []pmv1alpha1.PodMigrationJob{job},
				}
				if via == "SnapshotProgress" {
					st := snapshot.CheckPodSnapshotStatus(podSnapshot(tc.conditions), now)
					s.SnapshotProgress = map[string]SnapshotProgress{"default/pmj-a": {
						Ready:            st.Phase == snapshot.PhaseReady,
						Failed:           st.Phase == snapshot.PhaseFailed || st.Phase == snapshot.PhaseRefused,
						LastProgressTime: st.LastProgressTime,
					}}
				} else {
					s.PrimarySnapshotConditions = tc.conditions
				}
				vs, shouldFail := NewEngine(ModeStrict, nil).Evaluate(context.Background(), s)
				if shouldFail != tc.wantFail {
					t.Fatalf("shouldFail = %v, want %v (violations %+v)", shouldFail, tc.wantFail, vs)
				}
				if !tc.wantFail {
					if len(vs) != 0 {
						t.Fatalf("want no violations, got %+v", vs)
					}
					return
				}
				if v := vs[0]; v.InvariantID != "I4" || v.PMJName != "pmj-a" || !strings.HasSuffix(v.Message, tc.wantDetail) {
					t.Errorf("violations[0] = %+v, want I4 for pmj-a ending in %q", v, tc.wantDetail)
				}
			})
		}
	}
}
