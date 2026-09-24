package invariants

import (
	"context"
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
		{"", ModeObserve, false},
		{"observe", ModeObserve, false},
		{"OBSERVE", ModeObserve, false},
		{"strict", ModeStrict, false},
		{"disabled", ModeDisabled, false},
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
	// Clean: single replacement pod bound to snapshot-1
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

	// Violating: two distinct pods bound to snapshot-1
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
	// Violating: PMJ Succeeded with empty SnapshotRef
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

	// Violating: PMJ Succeeded but replacement pod carries cold-start-bypass=true
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
					AnnotationColdStartBypass: "true",
				},
			},
		},
	})
	if len(v2) != 1 || v2[0].InvariantID != "I2" {
		t.Fatalf("expected I2 violation for Succeeded with cold-start replacement pod, got %+v", v2)
	}
}

func TestInvariants_I3_GateLiveness(t *testing.T) {
	// Violating: Pod still has scheduling gate after PMJ marked GateReleased=true
	vs := EvaluateI3(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-3"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:          pmv1alpha1.PodMigrationJobPhaseRestoring,
				Consumed:       true,
				GateReleased:   true,
				RestoredPodUID: "uid-3",
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
	if len(vs) != 1 || vs[0].InvariantID != "I3" {
		t.Fatalf("expected I3 violation for gated pod after GateReleased=true, got %+v", vs)
	}
}

func TestInvariants_I4_TerminalProgress(t *testing.T) {
	now := time.Now()
	old := metav1.NewTime(now.Add(-12 * time.Minute))

	// Violating: PMJ in Snapshotting for 12m (> 10m30s)
	vs := EvaluateI4(&ReconcileSnapshot{
		Now: now,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-stuck",
				CreationTimestamp: old,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I4" {
		t.Fatalf("expected I4 violation for stuck active PMJ, got %+v", vs)
	}

	// Violating: PodMigration deleting with finalizer for 12m (> 10m30s)
	vsMig := EvaluateI4(&ReconcileSnapshot{
		Now: now,
		PrimaryMigration: &pmv1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "mig-stuck",
				DeletionTimestamp: &old,
				Finalizers:        []string{StorageCleanupFinalizer},
			},
		},
	})
	if len(vsMig) != 1 || vsMig[0].InvariantID != "I4" {
		t.Fatalf("expected I4 violation for stuck deleting PodMigration finalizer, got %+v", vsMig)
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
		t.Fatalf("expected I5 violation for orphaned trigger on terminal PMJ, got %+v", vs)
	}
}

func TestInvariants_I6_RevisionAndIdentityFidelity(t *testing.T) {
	vs := EvaluateI6(&ReconcileSnapshot{
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pmj-rev",
				Labels: map[string]string{
					LabelPMJPodTemplateHash: "hash-v1",
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
					LabelPodTemplateHash: "hash-v2",
				},
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I6" {
		t.Fatalf("expected I6 violation for pod-template-hash mismatch, got %+v", vs)
	}
}

func TestInvariants_I7_DisruptionBoundingPDBCompliance(t *testing.T) {
	vs := EvaluateI7(&ReconcileSnapshot{
		UsedBareDeleteBeforeDeadline: true,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-evict"},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I7" {
		t.Fatalf("expected I7 violation for bare delete before deadline, got %+v", vs)
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

func TestInvariants_I9_DeterministicFallbackOverCrashloop(t *testing.T) {
	vs := EvaluateI9(&ReconcileSnapshot{
		RestoreCrashSignatureMatched: true,
		PrimaryPMJ: &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-crash"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseRestoring,
				RestoredPodName: "crash-pod",
			},
		},
	})
	if len(vs) != 1 || vs[0].InvariantID != "I9" {
		t.Fatalf("expected I9 violation when matched restore crash remains in Restoring, got %+v", vs)
	}
}

func TestEngine_ObserveAndStrictTelemetry(t *testing.T) {
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

	// 1. Observe mode: increments counter, emits Warning Event, returns shouldFailStrict=false
	obsEngine := NewEngine(ModeObserve, recorder)
	vs, strict := obsEngine.Evaluate(context.Background(), snap)
	if len(vs) != 1 || strict {
		t.Fatalf("Observe mode: len(vs)=%d, strict=%v; want 1, false", len(vs), strict)
	}
	afterObsCount := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I2"))
	if afterObsCount != beforeCount+1 {
		t.Fatalf("Observe mode: metric count=%v, want %v", afterObsCount, beforeCount+1)
	}
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, EventReasonInvariantViolation) || !strings.Contains(ev, "[I2:NoSilentColdStart]") {
			t.Fatalf("unexpected event: %s", ev)
		}
	default:
		t.Fatal("expected Warning event in observe mode, got none")
	}

	// 2. Strict mode: increments counter, emits Warning Event, returns shouldFailStrict=true
	strictEngine := NewEngine(ModeStrict, recorder)
	vs, strict = strictEngine.Evaluate(context.Background(), snap)
	if len(vs) != 1 || !strict {
		t.Fatalf("Strict mode: len(vs)=%d, strict=%v; want 1, true", len(vs), strict)
	}
	afterStrictCount := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I2"))
	if afterStrictCount != afterObsCount+1 {
		t.Fatalf("Strict mode: metric count=%v, want %v", afterStrictCount, afterObsCount+1)
	}
}
