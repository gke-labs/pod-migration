package invariants

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
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

func TestInvariants_I6_VPAResizeMatchingDistilledSpecAllowsTemplateHashDifference(t *testing.T) {
	isCtrl := true
	originSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "app",
				Image: "redis:7.0",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
				},
			},
		},
	}
	digest, err := util.DistilledPodSpecDigest(&originSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}

	// Replacement pod has VPA-mutated memory (4Gi) and different pod-template-hash ("hash-v2")
	replacementSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "app",
				Image: "redis:7.0",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			},
		},
	}

	vs := EvaluateI6(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pmj-vpa",
				Labels: map[string]string{
					LabelPMJPodTemplateHash: "hash-v1",
					LabelPMJParentKind:      "Deployment",
					LabelPMJParentUID:       "deploy-uid-1",
				},
				Annotations: map[string]string{
					util.AnnotationDistilledSpecDigest: digest,
				},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
				RestoredPodName: "pod-repl",
			},
		},
		PrimaryPod: &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pod-repl",
				Labels: map[string]string{
					LabelPodTemplateHash: "hash-v2", // different due to VPA mutation
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						Kind:       "Deployment",
						Name:       "my-deploy",
						UID:        types.UID("deploy-uid-1"),
						Controller: &isCtrl,
					},
				},
			},
			Spec: replacementSpec,
		},
	})

	for _, v := range vs {
		if v.Reason == "PodTemplateHashMismatch" {
			t.Fatalf("unexpected PodTemplateHashMismatch violation for VPA pod with matching distilled spec: %+v", v)
		}
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
