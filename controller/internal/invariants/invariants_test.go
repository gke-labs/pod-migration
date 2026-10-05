package invariants

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

func TestParseMode(t *testing.T) {
	cases := []struct {
		raw     string
		want    Mode
		wantErr bool
	}{
		{"", ModeDisabled, false},
		{"disabled", ModeDisabled, false},
		{"observe", ModeObserve, false},
		{"OBSERVE", ModeObserve, false},
		{"strict", ModeStrict, false},
		{"bogus", "", true},
	}
	for _, tc := range cases {
		got, err := ParseMode(tc.raw)
		if (err != nil) != tc.wantErr {
			t.Fatalf("ParseMode(%q) err=%v, wantErr=%v", tc.raw, err, tc.wantErr)
		}
		if got != tc.want {
			t.Fatalf("ParseMode(%q) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

func TestInvariants_I1_AtMostOnceRestore(t *testing.T) {
	clean := &ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-1"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
				SnapshotRef:    "snapshot-1",
				Consumed:       true,
				GateReleased:   true,
				RestoredPodUID: "uid-pod-a",
			},
		},
		NamespacePods: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "pod-a",
					UID:         types.UID("uid-pod-a"),
					Annotations: map[string]string{AnnotationPSName: "snapshot-1"},
				},
			},
		},
	}
	if vs := EvaluateI1(clean); len(vs) != 0 {
		t.Fatalf("expected 0 violations on clean I1 state, got %+v", vs)
	}

	violating := &ReconcileSnapshot{
		PrimaryPMJ: clean.PrimaryPMJ,
		NamespacePods: []corev1.Pod{
			clean.NamespacePods[0],
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "pod-b",
					UID:         types.UID("uid-pod-b"),
					Annotations: map[string]string{AnnotationPSName: "snapshot-1"},
				},
			},
		},
	}
	vs := EvaluateI1(violating)
	if len(vs) == 0 || vs[0].InvariantID != "I1" {
		t.Fatalf("expected I1 violation on duplicate snapshot consumption, got %+v", vs)
	}
}

func TestInvariants_I2_NoSilentColdStart(t *testing.T) {
	// 1. Violating: PMJ Succeeded with empty SnapshotRef
	v1 := EvaluateI2(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-empty-snap"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
			},
		},
	})
	if len(v1) != 1 || v1[0].InvariantID != "I2" {
		t.Fatalf("expected I2 violation for Succeeded without SnapshotRef, got %+v", v1)
	}

	// 2. Violating: PMJ Succeeded but replacement pod carries explicit cold-start bypass (`podsnapshot.gke.io/ps-name: ""`)
	v2 := EvaluateI2(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-cold-pod"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
				SnapshotRef:     "snap-99",
				RestoredPodName: "repl-pod",
			},
		},
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "repl-pod",
				Annotations: map[string]string{
					AnnotationPSName: "",
				},
			},
		},
	})
	if len(v2) != 1 || v2[0].InvariantID != "I2" || v2[0].Reason != "SucceededPodMissingRestoreAnnotation" {
		t.Fatalf("expected I2 violation for Succeeded with ps-name=\"\" cold-start bypass, got %+v", v2)
	}

	// 3. Violating: Bare gate removal on a pod still carrying assigned-pmj without ps-name key
	v3 := EvaluateI2(&ReconcileSnapshot{
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "bare-ungated-pod",
				Annotations: map[string]string{
					AnnotationAssignedPMJ: "pmj-active",
				},
			},
		},
	})
	if len(v3) != 1 || v3[0].Reason != "GateReleasedWithoutSnapshotOrColdStartBypass" {
		t.Fatalf("expected I2 GateReleasedWithoutSnapshotOrColdStartBypass violation, got %+v", v3)
	}
}

func TestInvariants_I3_GateLivenessAndGraceWindow(t *testing.T) {
	now := time.Now()
	justNow := metav1.NewTime(now.Add(-5 * time.Second))
	longAgo := metav1.NewTime(now.Add(-45 * time.Second))

	// 1. Within 30s two-step release grace window: no false positive
	withinGrace := EvaluateI3(&ReconcileSnapshot{
		Now: now,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-3"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed:           true,
				GateReleased:       true,
				RestoredPodUID:     "uid-3",
				RestoringStartTime: &justNow,
			},
		},
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "default",
				Name:        "pod-3",
				UID:         types.UID("uid-3"),
				Annotations: map[string]string{AnnotationAssignedPMJ: "pmj-3"},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			},
		},
	})
	if len(withinGrace) != 0 {
		t.Fatalf("expected 0 violations within 30s gate release grace window, got %+v", withinGrace)
	}

	// 2. Past 30s grace window: flags PodGatedAfterGateReleasedStatus
	pastGrace := EvaluateI3(&ReconcileSnapshot{
		Now: now,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-3"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed:           true,
				GateReleased:       true,
				RestoredPodUID:     "uid-3",
				RestoringStartTime: &longAgo,
			},
		},
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "default",
				Name:        "pod-3",
				UID:         types.UID("uid-3"),
				Annotations: map[string]string{AnnotationAssignedPMJ: "pmj-3"},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			},
		},
	})
	if len(pastGrace) != 1 || pastGrace[0].InvariantID != "I3" {
		t.Fatalf("expected I3 violation past 30s grace window, got %+v", pastGrace)
	}
}

func TestInvariants_I4_DynamicTimeoutAnnotationAndStaticMessage(t *testing.T) {
	now := time.Now()
	created12mAgo := metav1.NewTime(now.Add(-12 * time.Minute))

	// 1. Default 10m timeout exceeded at 12m -> triggers I4 with static message
	pmjDefault := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-stuck",
			CreationTimestamp: created12mAgo,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}
	vs1 := EvaluateI4(&ReconcileSnapshot{Now: now, PrimaryPMJ: pmjDefault})
	vs2 := EvaluateI4(&ReconcileSnapshot{Now: now.Add(5 * time.Second), PrimaryPMJ: pmjDefault})
	if len(vs1) != 1 || len(vs2) != 1 {
		t.Fatalf("expected I4 violation at 12m on default 10m deadline, got %+v", vs1)
	}
	if vs1[0].Message != vs2[0].Message {
		t.Fatalf("expected I4 violation message to be static across reconcile ticks for client-go EventAggregator; got %q vs %q",
			vs1[0].Message, vs2[0].Message)
	}

	// 2. PR #57 `pod-migration.gke.io/timeout: "15m27s"` stamped on PMJ persists even after origin pod is gone!
	pmjAnnotated := pmjDefault.DeepCopy()
	pmjAnnotated.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
	pmjAnnotated.Annotations = map[string]string{AnnotationMigrationTimeout: "15m27s"}
	if vs := EvaluateI4(&ReconcileSnapshot{Now: now, PrimaryPMJ: pmjAnnotated}); len(vs) != 0 {
		t.Fatalf("expected 0 violations at 12m when PMJ annotation pod-migration.gke.io/timeout=15m27s (origin pod already evicted), got %+v", vs)
	}

	// 3. PR #57 Evicting phase anchored on `pod-migration.gke.io/evicting-since` (30s ago) -> within budget!
	pmjEvicting := pmjDefault.DeepCopy()
	pmjEvicting.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
	pmjEvicting.Annotations = map[string]string{
		AnnotationEvictingSince: now.Add(-30 * time.Second).Format(time.RFC3339Nano),
	}
	if vs := EvaluateI4(&ReconcileSnapshot{Now: now, PrimaryPMJ: pmjEvicting}); len(vs) != 0 {
		t.Fatalf("expected 0 violations when Evicting phase started 30s ago via evicting-since, got %+v", vs)
	}
}

func TestInvariants_I5_ZeroResourceLeak(t *testing.T) {
	vs := EvaluateI5(&ReconcileSnapshot{
		HasOrphanedTrigger: true,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-done"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I5" {
		t.Fatalf("expected I5 violation for orphaned trigger on Succeeded PMJ, got %+v", vs)
	}

	// SucceededWithoutRestore deliberately leaves triggers/snapshots intact per #32
	vsSWR := EvaluateI5(&ReconcileSnapshot{
		HasOrphanedTrigger: true,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-swr"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			},
		},
	})
	if len(vsSWR) != 0 {
		t.Fatalf("expected 0 I5 violations on SucceededWithoutRestore PMJ, got %+v", vsSWR)
	}
}

func TestInvariants_I6_RevisionAndIdentityFidelity(t *testing.T) {
	isCtrl := true
	vs := EvaluateI6(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pmj-rev",
				Labels: map[string]string{
					LabelPMJPodTemplateHash:     "hash-v1",
					LabelControllerRevisionHash: "sts-rev-1",
					LabelJobCompletionIndex:     "0",
					LabelPMJParentKind:          "StatefulSet",
					LabelPMJParentUID:           "sts-uid-1",
				},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
				RestoredPodName: "pod-v2",
			},
		},
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-v2",
				Labels: map[string]string{
					LabelPodTemplateHash:        "hash-v2",
					LabelControllerRevisionHash: "sts-rev-2",
					LabelJobCompletionIndex:     "1",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						Kind:       "StatefulSet",
						Name:       "web",
						UID:        types.UID("sts-uid-2"),
						Controller: &isCtrl,
					},
				},
			},
		},
	})
	if len(vs) != 4 {
		t.Fatalf("expected all 4 I6 identity checks (pod-template-hash, controller-revision-hash, job-completion-index, parent-uid) to fire, got %d: %+v", len(vs), vs)
	}
}

func TestInvariants_I7_DisruptionBoundingPDBCompliance(t *testing.T) {
	// 1. Live detection: PMJ in Restoring while BlockedByPDB condition is still True
	vsPDB := EvaluateI7(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-pdb"},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
				Conditions: []metav1.Condition{
					{
						Type:   "BlockedByPDB",
						Status: metav1.ConditionTrue,
						Reason: "PDBBudgetExhausted",
					},
				},
			},
		},
	})
	if len(vsPDB) != 1 || vsPDB[0].Reason != "EvictionBypassedWhileBlockedByPDB" {
		t.Fatalf("expected live I7 EvictionBypassedWhileBlockedByPDB violation, got %+v", vsPDB)
	}

	// 2. Live detection: PMJ in Restoring while origin pod (TargetPodUID) is still running without eviction
	vsLive := EvaluateI7(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-premature-restore"},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "origin-pod"},
				TargetPodUID: "origin-uid-1",
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
			},
		},
		NamespacePods: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "origin-pod",
					UID:       types.UID("origin-uid-1"),
				},
			},
		},
	})
	if len(vsLive) != 1 || vsLive[0].Reason != "OriginPodStillRunningInRestoringPhase" {
		t.Fatalf("expected live I7 OriginPodStillRunningInRestoringPhase violation, got %+v", vsLive)
	}
}

func TestInvariants_I8_PlatformAndControlPlaneIsolation(t *testing.T) {
	vs := EvaluateI8(&ReconcileSnapshot{
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "kube-system", Name: "coredns-1"},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I8" {
		t.Fatalf("expected I8 violation for gated kube-system pod, got %+v", vs)
	}
}

func TestInvariants_I9_DeterministicFallbackOverCrashloop_LiveClassifier(t *testing.T) {
	vs := EvaluateI9(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-crash"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
				RestoredPodName: "crash-pod",
				RestoredPodUID:  "crash-uid-1",
			},
		},
		NamespacePods: []corev1.Pod{
			{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "crash-pod",
					UID:       types.UID("crash-uid-1"),
				},
				Status: corev1.PodStatus{
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "app",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 128,
									Reason:   "StartError",
									Message:  "failed to start containerd task: OCI runtime restore failed: bad image",
								},
							},
						},
					},
				},
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I9" {
		t.Fatalf("expected live I9 violation when restore.Classify detects StartError/128 restore crash, got %+v", vs)
	}
}

func TestEngine_ObserveStrictDeduplicationAndForgetObject(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-test"},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
			// Empty SnapshotRef triggers I2
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	beforeCount := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I2"))

	// 1. Observe mode: 5 consecutive reconcile evaluations of the same stuck PMJ
	// must increment the counter and emit the Warning event ONLY ONCE.
	obsEngine := NewEngine(ModeObserve, recorder)
	for i := 0; i < 5; i++ {
		vs, strict := obsEngine.Evaluate(context.Background(), snap)
		if len(vs) != 1 || strict {
			t.Fatalf("iter %d: len(vs)=%d, strict=%v; want 1, false", i, len(vs), strict)
		}
	}
	afterObsCount := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I2"))
	if afterObsCount != beforeCount+1 {
		t.Fatalf("Deduplication failed: metric count=%v after 5 identical evaluations, want %v", afterObsCount, beforeCount+1)
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("Deduplication failed: expected exactly 1 Warning event across 5 evaluations, got %d", len(recorder.Events))
	}
	ev := <-recorder.Events
	if !strings.Contains(ev, EventReasonInvariantViolation) || !strings.Contains(ev, "[I2:NoSilentColdStart]") {
		t.Fatalf("unexpected event: %s", ev)
	}

	// 2. ForgetObject clears deduplication state when the object is deleted (`NotFound`)
	obsEngine.ForgetObject("PodMigrationJobReconciler", "pmj", "default", "pmj-test")
	obsEngine.Evaluate(context.Background(), snap)
	afterForgetCount := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I2"))
	if afterForgetCount != afterObsCount+1 {
		t.Fatalf("expected counter to increment after ForgetObject + re-creation; got %v, want %v", afterForgetCount, afterObsCount+1)
	}

	// 3. Strict mode returns shouldFailStrict=true
	strictEngine := NewEngine(ModeStrict, recorder)
	vs, strict := strictEngine.Evaluate(context.Background(), snap)
	if len(vs) != 1 || !strict {
		t.Fatalf("Strict mode: len(vs)=%d, strict=%v; want 1, true", len(vs), strict)
	}
}

func TestEvaluateI4_EvictingStartTimeAnchor(t *testing.T) {
	now := time.Now()
	evictingStart := metav1.NewTime(now.Add(-2 * time.Minute))
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "pmj-evicting-status-anchor",
			CreationTimestamp: metav1.NewTime(now.Add(-12 * time.Minute)),
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:             pmv1alpha1.PodMigrationJobPhaseEvicting,
			EvictingStartTime: &evictingStart,
		},
	}
	vs := EvaluateI4(&ReconcileSnapshot{
		Now:        now,
		PrimaryPMJ: job,
	})
	if len(vs) != 0 {
		t.Fatalf("expected Status.EvictingStartTime within 10m budget to satisfy I4, got %+v", vs)
	}

	expiredStart := metav1.NewTime(now.Add(-12 * time.Minute))
	job.Status.EvictingStartTime = &expiredStart
	vs = EvaluateI4(&ReconcileSnapshot{
		Now:        now,
		PrimaryPMJ: job,
	})
	if len(vs) != 1 || vs[0].Reason != "ActivePMJExceededDeadline" {
		t.Fatalf("expected Status.EvictingStartTime past 10m30s budget to violate I4, got %+v", vs)
	}

	// Hard 2h cap: created 2h1m ago, even with a fresh EvictingStartTime 30s ago, must violate I4.
	job.CreationTimestamp = metav1.NewTime(now.Add(-121 * time.Minute))
	freshEvictStart := metav1.NewTime(now.Add(-30 * time.Second))
	job.Status.EvictingStartTime = &freshEvictStart
	vs = EvaluateI4(&ReconcileSnapshot{
		Now:        now,
		PrimaryPMJ: job,
	})
	if len(vs) != 1 || vs[0].Reason != "ActivePMJExceededDeadline" {
		t.Fatalf("expected PMJ created 2h1m ago to violate I4 2h hard cap despite recent EvictingStartTime, got %+v", vs)
	}
}

func TestEngine_MaxActiveViolationScopesCap(t *testing.T) {
	eng := NewEngine(ModeObserve, nil)
	for i := 0; i < maxActiveViolationScopes+25; i++ {
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      fmt.Sprintf("pmj-cap-%d", i),
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded,
				// Empty SnapshotRef triggers I2
			},
		}
		eng.Evaluate(context.Background(), &ReconcileSnapshot{
			Reconciler: "PodMigrationJobReconciler",
			PrimaryPMJ: pmj,
		})
	}
	eng.mu.Lock()
	defer eng.mu.Unlock()
	if len(eng.activeViolations) > maxActiveViolationScopes {
		t.Fatalf("expected activeViolations map size <= %d, got %d", maxActiveViolationScopes, len(eng.activeViolations))
	}
}

func TestEngine_MultiReconcileDebounce_StrictEscalation(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, recorder)
	eng.Rules = []Rule{debouncedRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-debounce-test",
			UID:       "uid-pmj-debounce-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-debounce-test/uid-pmj-debounce-1"

	// Sample 1: first violation. In strict mode, debounce suppresses immediate abort.
	vs1, strict1 := eng.Evaluate(context.Background(), snap)
	if len(vs1) != 1 {
		t.Fatalf("sample 1: expected 1 violation, got %d", len(vs1))
	}
	if strict1 {
		t.Fatalf("sample 1: expected strict=false during debounce window, got true")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("sample 1: expected ConsecutiveCount=1, got %d", count)
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("sample 1: expected 1 event, got %d", len(recorder.Events))
	}
	ev1 := <-recorder.Events
	if !strings.Contains(ev1, "[sample 1/3]") {
		t.Fatalf("sample 1: expected event to include [sample 1/3], got: %s", ev1)
	}

	// Sample 2: second consecutive violation. Still in debounce window.
	vs2, strict2 := eng.Evaluate(context.Background(), snap)
	if len(vs2) != 1 {
		t.Fatalf("sample 2: expected 1 violation, got %d", len(vs2))
	}
	if strict2 {
		t.Fatalf("sample 2: expected strict=false during debounce window, got true")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 2 {
		t.Fatalf("sample 2: expected ConsecutiveCount=2, got %d", count)
	}
	// Deduplication should suppress repeat Warning event for identical message on sample 2.
	if len(recorder.Events) != 0 {
		t.Fatalf("sample 2: expected 0 new events due to deduplication, got %d", len(recorder.Events))
	}

	// Sample 3: third consecutive violation reaches threshold (3/3). Escalates to strict abort!
	vs3, strict3 := eng.Evaluate(context.Background(), snap)
	if len(vs3) != 1 {
		t.Fatalf("sample 3: expected 1 violation, got %d", len(vs3))
	}
	if !strict3 {
		t.Fatalf("sample 3: expected strict=true on reaching consecutive threshold 3, got false")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 3 {
		t.Fatalf("sample 3: expected ConsecutiveCount=3, got %d", count)
	}
	// An escalating event must be emitted notifying the operator of strict abort.
	if len(recorder.Events) != 1 {
		t.Fatalf("sample 3: expected 1 escalation event, got %d", len(recorder.Events))
	}
	ev3 := <-recorder.Events
	if !strings.Contains(ev3, "escalating to strict abort") {
		t.Fatalf("sample 3: expected escalation event, got: %s", ev3)
	}

	// Sample 4 (Suggestion 3): sample N+1 / conflict-retry check.
	// When update conflicts and reconcile repeats, strict abort remains active,
	// but the escalation event is NOT re-emitted.
	vs4, strict4 := eng.Evaluate(context.Background(), snap)
	if len(vs4) != 1 {
		t.Fatalf("sample 4: expected 1 violation, got %d", len(vs4))
	}
	if !strict4 {
		t.Fatalf("sample 4: expected strict=true on sample N+1, got false")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 4 {
		t.Fatalf("sample 4: expected ConsecutiveCount=4, got %d", count)
	}
	if len(recorder.Events) != 0 {
		t.Fatalf("sample 4: expected 0 new escalation events on sample N+1, got %d", len(recorder.Events))
	}
}

func TestEngine_MultiReconcileDebounce_CleanSampleReset(t *testing.T) {
	shouldViolate := true
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			if !shouldViolate {
				return nil
			}
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{debouncedRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-reset-test",
			UID:       "uid-reset-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-reset-test/uid-reset-1"

	// Sample 1: violation occurs (count=1).
	vs1, strict1 := eng.Evaluate(context.Background(), snap)
	if len(vs1) != 1 || strict1 || eng.ConsecutiveCount(subjKey, "I10-Test") != 1 {
		t.Fatalf("sample 1: len(vs)=%d, strict=%v, count=%d", len(vs1), strict1, eng.ConsecutiveCount(subjKey, "I10-Test"))
	}

	// Sample 2: clean reconcile (replacement pod appeared!). Counter resets to 0.
	shouldViolate = false
	vs2, strict2 := eng.Evaluate(context.Background(), snap)
	if len(vs2) != 0 || strict2 {
		t.Fatalf("sample 2: expected 0 violations, strict=false; got %d, %v", len(vs2), strict2)
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 0 {
		t.Fatalf("sample 2: expected ConsecutiveCount to reset to 0 on clean sample, got %d", count)
	}

	// Sample 3: violation recurs. Counter must start over at 1, NOT 2.
	shouldViolate = true
	vs3, strict3 := eng.Evaluate(context.Background(), snap)
	if len(vs3) != 1 || strict3 {
		t.Fatalf("sample 3: len(vs)=%d, strict=%v", len(vs3), strict3)
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("sample 3: expected ConsecutiveCount to restart at 1, got %d", count)
	}
}

func TestEngine_MultiReconcileDebounce_TerminalPhaseReset(t *testing.T) {
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{debouncedRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-term-test",
			UID:       "uid-term-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-term-test/uid-term-1"

	// Sample 1: count=1
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("expected count=1, got %d", count)
	}

	// Sample 2: PMJ reaches terminal phase Succeeded. Debounce counter is cleared.
	pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 0 {
		t.Fatalf("expected debounce counter cleared on terminal phase, got %d", count)
	}
}

func TestEngine_MultiReconcileDebounce_PerViolationOverride(t *testing.T) {
	// A rule that returns Violation with ConsecutiveRequired = 2 override.
	overrideRule := funcRule{
		id:   "Custom-Debounce",
		name: "CustomRule",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:         "Custom-Debounce",
				InvariantName:       "CustomRule",
				Reason:              "TransientFlap",
				Message:             "transient flap detected",
				Namespace:           s.PrimaryPMJ.Namespace,
				PMJName:             s.PrimaryPMJ.Name,
				ConsecutiveRequired: 2,
			}}
		},
	}

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{overrideRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-override-test",
			UID:       "uid-override-1",
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	// Sample 1: strict=false (count=1 < 2)
	_, strict1 := eng.Evaluate(context.Background(), snap)
	if strict1 {
		t.Fatalf("sample 1: expected strict=false, got true")
	}

	// Sample 2: strict=true (count=2 >= 2)
	_, strict2 := eng.Evaluate(context.Background(), snap)
	if !strict2 {
		t.Fatalf("sample 2: expected strict=true, got false")
	}
}

func TestEngine_MultiReconcileDebounce_MixedRulesImmediateAndDebounced(t *testing.T) {
	immediateRule := funcRule{
		id:   "I1-Immediate",
		name: "ImmediateInvariant",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I1-Immediate",
				InvariantName: "ImmediateInvariant",
				Reason:        "ImmediateFailure",
				Message:       "immediate violation",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Debounced",
		name: "DebouncedInvariant",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Debounced",
				InvariantName: "DebouncedInvariant",
				Reason:        "AbsenceDebounce",
				Message:       "absence violation in debounce",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 5, 0)

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-mixed-test",
			UID:       "uid-mixed-1",
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	// Case A: Only debounced rule is active. Sample 1 must NOT fail strict mode.
	engDebouncedOnly := NewEngine(ModeStrict, nil)
	engDebouncedOnly.Rules = []Rule{debouncedRule}
	vsA, strictA := engDebouncedOnly.Evaluate(context.Background(), snap)
	if len(vsA) != 1 || strictA {
		t.Fatalf("debounced only: len=%d, strict=%v; want 1, false", len(vsA), strictA)
	}

	// Case B: Both immediate and debounced rules fire on sample 1.
	// Strict mode must fail immediately due to the immediate invariant.
	engMixed := NewEngine(ModeStrict, nil)
	engMixed.Rules = []Rule{immediateRule, debouncedRule}
	vsB, strictB := engMixed.Evaluate(context.Background(), snap)
	if len(vsB) != 2 || !strictB {
		t.Fatalf("mixed rules: len=%d, strict=%v; want 2, true", len(vsB), strictB)
	}
}

func TestEngine_MultiReconcileDebounce_ForgetObject(t *testing.T) {
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{debouncedRule}

	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-forget-test",
			UID:       "uid-forget-target-123",
		},
	}
	snap1 := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj1,
	}

	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-forget-test-sibling",
			UID:       "uid-forget-sibling-456",
		},
	}
	snap2 := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj2,
	}

	eng.Evaluate(context.Background(), snap1)
	eng.Evaluate(context.Background(), snap1)
	eng.Evaluate(context.Background(), snap2)

	subjKey1 := "PodMigrationJobReconciler/pmj/default/pmj-forget-test/uid-forget-target-123"
	subjKey2 := "PodMigrationJobReconciler/pmj/default/pmj-forget-test-sibling/uid-forget-sibling-456"

	if count := eng.ConsecutiveCount(subjKey1, "I10-Test"); count != 2 {
		t.Fatalf("expected count=2 before ForgetObject on target, got %d", count)
	}
	if count := eng.ConsecutiveCount(subjKey2, "I10-Test"); count != 1 {
		t.Fatalf("expected count=1 before ForgetObject on sibling, got %d", count)
	}

	// Sibling with similar name prefix (pmj-forget-test vs pmj-forget-test-sibling)
	// and real Kubernetes object UID. ForgetObject must clear target and NOT sibling.
	eng.ForgetObject("PodMigrationJobReconciler", "pmj", "default", "pmj-forget-test")
	if count := eng.ConsecutiveCount(subjKey1, "I10-Test"); count != 0 {
		t.Fatalf("expected count=0 after ForgetObject on target with UID, got %d", count)
	}
	if count := eng.ConsecutiveCount(subjKey2, "I10-Test"); count != 1 {
		t.Fatalf("expected sibling count=1 to be preserved after ForgetObject, got %d", count)
	}
}

func TestEngine_MultiReconcileDebounce_DurationGate(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 30*time.Second)

	eng := NewEngine(ModeStrict, recorder)
	eng.Rules = []Rule{debouncedRule}

	baseTime := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-duration-test",
			UID:       "uid-duration-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
		Now:        baseTime,
	}
	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-duration-test/uid-duration-1"

	// Sample 1 (t=0s): count=1, elapsed=0s -> strict=false
	_, strict1 := eng.Evaluate(context.Background(), snap)
	if strict1 || eng.ConsecutiveCount(subjKey, "I10-Test") != 1 {
		t.Fatalf("sample 1: strict=%v, count=%d", strict1, eng.ConsecutiveCount(subjKey, "I10-Test"))
	}

	// Sample 2 (t=10s): count=2, elapsed=10s -> strict=false
	snap.Now = baseTime.Add(10 * time.Second)
	_, strict2 := eng.Evaluate(context.Background(), snap)
	if strict2 || eng.ConsecutiveCount(subjKey, "I10-Test") != 2 {
		t.Fatalf("sample 2: strict=%v, count=%d", strict2, eng.ConsecutiveCount(subjKey, "I10-Test"))
	}

	// Sample 3 (t=20s): count=3 reaches count threshold N=3, but elapsed 20s < 30s minDuration!
	// strict must remain false!
	snap.Now = baseTime.Add(20 * time.Second)
	_, strict3 := eng.Evaluate(context.Background(), snap)
	if strict3 {
		t.Fatalf("sample 3: expected strict=false when elapsed 20s < 30s duration threshold, got true")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 3 {
		t.Fatalf("sample 3: expected count=3, got %d", count)
	}

	// Sample 4 (t=30s): count=4, elapsed=30s >= 30s. Both count and duration thresholds satisfied!
	// Escalates to strict abort!
	snap.Now = baseTime.Add(30 * time.Second)
	_, strict4 := eng.Evaluate(context.Background(), snap)
	if !strict4 {
		t.Fatalf("sample 4: expected strict=true when count >= 3 AND elapsed >= 30s, got false")
	}
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 4 {
		t.Fatalf("sample 4: expected count=4, got %d", count)
	}
	if len(recorder.Events) < 2 {
		t.Fatalf("expected at least 2 events (first observation + escalation), got %d", len(recorder.Events))
	}

	// Sample 5 (t=32s): post-escalation check.
	// Continues failing strict mode, but does NOT emit duplicate escalation event.
	evLenBefore := len(recorder.Events)
	snap.Now = baseTime.Add(32 * time.Second)
	_, strict5 := eng.Evaluate(context.Background(), snap)
	if !strict5 {
		t.Fatalf("sample 5: expected strict=true on post-escalation sample, got false")
	}
	if len(recorder.Events) != evLenBefore {
		t.Fatalf("sample 5: expected 0 new escalation events, got %d new", len(recorder.Events)-evLenBefore)
	}
}

func TestEngine_MultiReconcileDebounce_DegradedInconclusive(t *testing.T) {
	shouldViolate := true
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			if !shouldViolate {
				return nil
			}
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{debouncedRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-degraded-test",
			UID:       "uid-degraded-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}
	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-degraded-test/uid-degraded-1"

	// Sample 1: normal violation -> count=1
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("sample 1: expected count=1, got %d", count)
	}

	// Sample 2: PodListFailed=true, rule returns no violations (simulating informer cache error).
	// Degraded sample must be inconclusive: counter must NOT be reset to 0!
	shouldViolate = false
	snap.PodListFailed = true
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("sample 2: expected count to be preserved at 1 during PodListFailed=true, got %d", count)
	}

	// Sample 3: PMJListFailed=true, also degraded. Counter must remain 1.
	snap.PodListFailed = false
	snap.PMJListFailed = true
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 1 {
		t.Fatalf("sample 3: expected count to be preserved at 1 during PMJListFailed=true, got %d", count)
	}

	// Sample 4: Informer recovers (PodListFailed=false, PMJListFailed=false) and violation returns.
	// Counter increments from 1 to 2!
	snap.PMJListFailed = false
	shouldViolate = true
	eng.Evaluate(context.Background(), snap)
	if count := eng.ConsecutiveCount(subjKey, "I10-Test"); count != 2 {
		t.Fatalf("sample 4: expected count to increment to 2 after degraded condition clears, got %d", count)
	}
}

func TestEngine_MultiReconcileDebounce_PartialReset(t *testing.T) {
	violateA := true
	violateB := true

	ruleA := NewDebouncedRule(funcRule{
		id:   "Rule-A",
		name: "DebouncedRuleA",
		fn: func(s *ReconcileSnapshot) []Violation {
			if !violateA {
				return nil
			}
			return []Violation{{
				InvariantID:   "Rule-A",
				InvariantName: "DebouncedRuleA",
				Reason:        "ReasonA",
				Message:       "violation A",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	ruleB := NewDebouncedRule(funcRule{
		id:   "Rule-B",
		name: "DebouncedRuleB",
		fn: func(s *ReconcileSnapshot) []Violation {
			if !violateB {
				return nil
			}
			return []Violation{{
				InvariantID:   "Rule-B",
				InvariantName: "DebouncedRuleB",
				Reason:        "ReasonB",
				Message:       "violation B",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 3, 0)

	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{ruleA, ruleB}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-partial-test",
			UID:       "uid-partial-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}
	subjKey := "PodMigrationJobReconciler/pmj/default/pmj-partial-test/uid-partial-1"

	// Sample 1: Both A and B violate.
	eng.Evaluate(context.Background(), snap)
	if countA := eng.ConsecutiveCount(subjKey, "Rule-A"); countA != 1 {
		t.Fatalf("sample 1: expected Rule-A count=1, got %d", countA)
	}
	if countB := eng.ConsecutiveCount(subjKey, "Rule-B"); countB != 1 {
		t.Fatalf("sample 1: expected Rule-B count=1, got %d", countB)
	}

	// Sample 2: Rule-A recovers (clean), Rule-B continues violating.
	// Rule-A counter must reset to 0; Rule-B counter must advance to 2!
	violateA = false
	eng.Evaluate(context.Background(), snap)
	if countA := eng.ConsecutiveCount(subjKey, "Rule-A"); countA != 0 {
		t.Fatalf("sample 2: expected Rule-A count to reset to 0, got %d", countA)
	}
	if countB := eng.ConsecutiveCount(subjKey, "Rule-B"); countB != 2 {
		t.Fatalf("sample 2: expected Rule-B count to advance to 2, got %d", countB)
	}

	// Sample 3: Rule-A violates again, Rule-B continues violating.
	// Rule-A restarts at 1; Rule-B reaches 3 and triggers strict abort!
	violateA = true
	_, strict3 := eng.Evaluate(context.Background(), snap)
	if !strict3 {
		t.Fatalf("sample 3: expected strict=true when Rule-B reaches threshold 3, got false")
	}
	if countA := eng.ConsecutiveCount(subjKey, "Rule-A"); countA != 1 {
		t.Fatalf("sample 3: expected Rule-A count to restart at 1, got %d", countA)
	}
	if countB := eng.ConsecutiveCount(subjKey, "Rule-B"); countB != 3 {
		t.Fatalf("sample 3: expected Rule-B count=3, got %d", countB)
	}
}

func TestEngine_MultiReconcileDebounce_ObserveModeEscalationEvent(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	debouncedRule := NewDebouncedRule(funcRule{
		id:   "I10-Test",
		name: "RestoringReplacementLiveness",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{
				InvariantID:   "I10-Test",
				InvariantName: "RestoringReplacementLiveness",
				Reason:        "RestoringReplacementPodMissing",
				Message:       "replacement pod missing during restore",
				Namespace:     s.PrimaryPMJ.Namespace,
				PMJName:       s.PrimaryPMJ.Name,
			}}
		},
	}, 2, 0)

	eng := NewEngine(ModeObserve, recorder)
	eng.Rules = []Rule{debouncedRule}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-obs-test",
			UID:       "uid-obs-1",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}
	snap := &ReconcileSnapshot{
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: pmj,
	}

	// Sample 1: Observe mode records violation, strict=false
	_, strict1 := eng.Evaluate(context.Background(), snap)
	if strict1 {
		t.Fatalf("sample 1: expected strict=false in observe mode, got true")
	}

	// Sample 2: Reaches threshold. In observe mode, strict is STILL false,
	// but an event notifying operators that it would escalate is emitted.
	_, strict2 := eng.Evaluate(context.Background(), snap)
	if strict2 {
		t.Fatalf("sample 2: expected strict=false in observe mode, got true")
	}

	foundWouldEscalate := false
	for len(recorder.Events) > 0 {
		ev := <-recorder.Events
		if strings.Contains(ev, "would escalate to strict abort") {
			foundWouldEscalate = true
			break
		}
	}
	if !foundWouldEscalate {
		t.Fatalf("expected observe mode to emit event containing 'would escalate to strict abort'")
	}
}
