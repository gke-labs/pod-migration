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

package invariants

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// Strict mode fails only the reconciled job, and only for violations that name
// that job or name no job (#198).

var scopeT0 = time.Date(2026, 11, 1, 12, 0, 0, 0, time.UTC)

func scopeRuleI2() Rule {
	return funcRule{id: "I2", name: "NoSilentColdStart", fn: EvaluateI2}
}

func scopeRuleI11() Rule {
	return NewDebouncedRule(RuleI11(), 3, 30*time.Second)
}

// scopeActiveJob returns an active (Snapshotting) job that no rule flags on its own.
func scopeActiveJob(name, uid string) pmv1alpha1.PodMigrationJob {
	return pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID(uid)},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: name + "-src"},
			TargetPodUID: uid + "-src",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting},
	}
}

// scopeSucceededNoSnapshot returns a Succeeded job with an empty SnapshotRef (an I2 violation).
func scopeSucceededNoSnapshot(name, uid string) pmv1alpha1.PodMigrationJob {
	return pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID(uid)},
		Status:     pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhaseSucceeded},
	}
}

// scopeFence marks job as SourceFenced=True long enough ago that I11's grace window has passed.
func scopeFence(job *pmv1alpha1.PodMigrationJob) {
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
	job.Status.Conditions = []metav1.Condition{{
		Type:               ConditionSourceFenced,
		Status:             metav1.ConditionTrue,
		Reason:             "Fenced",
		LastTransitionTime: metav1.NewTime(scopeT0.Add(-time.Hour)),
	}}
}

// scopeSourcePod returns job's source pod, fenced correctly when fenced is true
// and missing both the fence label and Ready=False otherwise (two I11 violations).
func scopeSourcePod(job *pmv1alpha1.PodMigrationJob, fenced bool) corev1.Pod {
	pod := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: job.Namespace,
			Name:      job.Spec.PodRef.Name,
			UID:       types.UID(job.Spec.TargetPodUID),
		},
	}
	if fenced {
		pod.Labels = map[string]string{LabelPodFenced: "true"}
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: ReasonPodMigrationFenced,
		}}
	}
	return pod
}

func scopeSubjectKey(job *pmv1alpha1.PodMigrationJob) string {
	return "PodMigrationJobReconciler/pmj/" + job.Namespace + "/" + job.Name + "/" + string(job.UID)
}

// TestEngine_StrictScope_ImmediateOtherJobViolationDoesNotFailPrimary is the
// #198 repro: active job A next to a Succeeded job B with an empty SnapshotRef.
// B's I2 violation is still returned and evented, but it must not fail A.
func TestEngine_StrictScope_ImmediateOtherJobViolationDoesNotFailPrimary(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	eng := NewEngine(ModeStrict, recorder)
	eng.Rules = []Rule{scopeRuleI2()}

	a := scopeActiveJob("pmj-a", "uid-a")
	b := scopeSucceededNoSnapshot("pmj-b", "uid-b")
	snap := &ReconcileSnapshot{
		Now:           scopeT0,
		Reconciler:    "PodMigrationJobReconciler",
		PrimaryPMJ:    &a,
		NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
	}

	vs, shouldFail := eng.Evaluate(context.Background(), snap)
	if len(vs) != 1 || vs[0].InvariantID != "I2" || vs[0].PMJName != "pmj-b" {
		t.Fatalf("expected B's I2 violation to still be reported, got %+v", vs)
	}
	if shouldFail {
		t.Fatalf("strict mode must not fail job A for job B's I2 violation")
	}
	if len(recorder.Events) != 1 {
		t.Fatalf("expected B's violation to still be evented once, got %d events", len(recorder.Events))
	}

	// Positive control: when B itself is reconciled, the same violation is B's own.
	bSnap := &ReconcileSnapshot{
		Now:           scopeT0,
		Reconciler:    "PodMigrationJobReconciler",
		PrimaryPMJ:    &b,
		NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
	}
	if _, bFail := eng.Evaluate(context.Background(), bSnap); !bFail {
		t.Fatalf("strict mode must still report shouldFail for job B's own I2 violation")
	}
}

// TestEngine_StrictScope_OwnViolationFailsAndIsReportedFirst checks that when
// the primary job has its own violation and another job's violation comes first
// in rule order, the job still fails and violations[0] is its own (the reconciler
// copies violations[0] into the failed job's Ready message).
func TestEngine_StrictScope_OwnViolationFailsAndIsReportedFirst(t *testing.T) {
	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{scopeRuleI2()}

	a := scopeActiveJob("pmj-a", "uid-a")
	b := scopeSucceededNoSnapshot("pmj-b", "uid-b")
	// A's replacement had its gate removed without a restore order: I2 names A.
	// EvaluateI2 reports job violations before pod violations, so B's comes first.
	repl := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "pmj-a-dst",
			Annotations: map[string]string{AnnotationAssignedPMJ: "pmj-a"},
		},
	}
	snap := &ReconcileSnapshot{
		Now:           scopeT0,
		Reconciler:    "PodMigrationJobReconciler",
		PrimaryPMJ:    &a,
		NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
		NamespacePods: []corev1.Pod{repl},
	}
	if raw := EvaluateI2(snap); len(raw) != 2 || raw[0].PMJName != "pmj-b" || raw[1].PMJName != "pmj-a" {
		t.Fatalf("test setup: expected I2 to report [pmj-b, pmj-a], got %+v", raw)
	}

	vs, shouldFail := eng.Evaluate(context.Background(), snap)
	if !shouldFail {
		t.Fatalf("strict mode must fail job A for its own I2 violation")
	}
	if len(vs) != 2 {
		t.Fatalf("expected both violations to be returned, got %+v", vs)
	}
	if vs[0].PMJName != "pmj-a" || vs[0].Reason != "GateReleasedWithoutSnapshotOrColdStartBypass" {
		t.Fatalf("violations[0] must be job A's own violation, got %+v", vs[0])
	}
}

// TestEngine_StrictScope_UnownedViolationFailsPrimary checks that a violation
// naming no job still fails the reconciled job.
func TestEngine_StrictScope_UnownedViolationFailsPrimary(t *testing.T) {
	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{funcRule{
		id:   "IX-Unowned",
		name: "Unowned",
		fn: func(s *ReconcileSnapshot) []Violation {
			return []Violation{{InvariantID: "IX-Unowned", InvariantName: "Unowned", Reason: "NoJob", Message: "no job", Namespace: "default"}}
		},
	}}
	a := scopeActiveJob("pmj-a", "uid-a")
	_, shouldFail := eng.Evaluate(context.Background(), &ReconcileSnapshot{
		Now:        scopeT0,
		Reconciler: "PodMigrationJobReconciler",
		PrimaryPMJ: &a,
	})
	if !shouldFail {
		t.Fatalf("strict mode must fail the reconciled job for a violation that names no job")
	}
}

// TestEngine_StrictScope_DebouncedOtherJobViolationDoesNotFailPrimary runs the
// debounced I11 past its threshold (3 samples, 30s) with only job B violating.
// A must never fail, and B's samples must not accumulate under A's debounce key.
func TestEngine_StrictScope_DebouncedOtherJobViolationDoesNotFailPrimary(t *testing.T) {
	recorder := record.NewFakeRecorder(50)
	eng := NewEngine(ModeStrict, recorder)
	eng.Rules = []Rule{scopeRuleI11()}

	a := scopeActiveJob("pmj-a", "uid-a")
	b := scopeActiveJob("pmj-b", "uid-b")
	scopeFence(&b)
	pods := []corev1.Pod{scopeSourcePod(&a, true), scopeSourcePod(&b, false)}

	for i := 0; i < 5; i++ {
		now := scopeT0.Add(time.Duration(i) * 15 * time.Second)
		vs, shouldFail := eng.Evaluate(context.Background(), &ReconcileSnapshot{
			Now:           now,
			Reconciler:    "PodMigrationJobReconciler",
			PrimaryPMJ:    &a,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
			NamespacePods: pods,
		})
		if len(vs) == 0 {
			t.Fatalf("sample %d: expected B's I11 violations to still be reported", i+1)
		}
		for _, v := range vs {
			if v.PMJName != "pmj-b" {
				t.Fatalf("sample %d: test setup: unexpected violation %+v", i+1, v)
			}
		}
		if shouldFail {
			t.Fatalf("sample %d: strict mode must not fail job A for job B's I11 violation", i+1)
		}
		if c := eng.ConsecutiveCount(scopeSubjectKey(&a), "I11"); c != 0 {
			t.Fatalf("sample %d: B's I11 samples must not count under A's debounce key, got %d", i+1, c)
		}
	}
	for len(recorder.Events) > 0 {
		if ev := <-recorder.Events; strings.Contains(ev, "escalating to strict abort") {
			t.Fatalf("job A must not receive an escalation event for job B's violation: %s", ev)
		}
	}

	// Positive control: B reconciled on its own still escalates after the debounce.
	var bFail bool
	for i := 0; i < 3; i++ {
		_, bFail = eng.Evaluate(context.Background(), &ReconcileSnapshot{
			Now:           scopeT0.Add(time.Duration(i) * 15 * time.Second),
			Reconciler:    "PodMigrationJobReconciler",
			PrimaryPMJ:    &b,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
			NamespacePods: pods,
		})
	}
	if !bFail {
		t.Fatalf("strict mode must still fail job B for its own I11 violation after the debounce")
	}
}

// TestEngine_StrictScope_DebounceKeyedByViolatingJob checks that the primary
// job's debounce count tracks only its own I11 samples: B's earlier samples do
// not give A a head start, and B's ongoing violation does not keep A's stale
// count alive once A is clean.
func TestEngine_StrictScope_DebounceKeyedByViolatingJob(t *testing.T) {
	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{scopeRuleI11()}

	a := scopeActiveJob("pmj-a", "uid-a")
	scopeFence(&a)
	b := scopeActiveJob("pmj-b", "uid-b")
	scopeFence(&b)
	aKey := scopeSubjectKey(&a)

	sample := func(i int, aFenced bool) bool {
		_, shouldFail := eng.Evaluate(context.Background(), &ReconcileSnapshot{
			Now:           scopeT0.Add(time.Duration(i) * 15 * time.Second),
			Reconciler:    "PodMigrationJobReconciler",
			PrimaryPMJ:    &a,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
			NamespacePods: []corev1.Pod{scopeSourcePod(&a, aFenced), scopeSourcePod(&b, false)},
		})
		return shouldFail
	}

	// Samples 0-2 (0s..30s): only B violates.
	for i := 0; i < 3; i++ {
		if sample(i, true) {
			t.Fatalf("sample %d: A must not fail while only B violates", i)
		}
	}
	// Sample 3 (45s): A starts violating too. This is A's first sample, not its fourth.
	if sample(3, false) {
		t.Fatalf("sample 3: A failed on its first own I11 sample; B's samples counted toward A")
	}
	if c := eng.ConsecutiveCount(aKey, "I11"); c != 1 {
		t.Fatalf("sample 3: expected A's I11 count 1, got %d", c)
	}
	// Sample 4: A violates again (count 2).
	if sample(4, false) {
		t.Fatalf("sample 4: A must not fail before its own debounce threshold")
	}
	// Sample 5: A is clean, B still violates. A's count must reset.
	if sample(5, true) {
		t.Fatalf("sample 5: A must not fail while only B violates")
	}
	if c := eng.ConsecutiveCount(aKey, "I11"); c != 0 {
		t.Fatalf("sample 5: A's I11 count must reset on A's clean sample even though B still violates, got %d", c)
	}
}

// TestEngine_StrictScope_DebouncedOwnViolationReportedFirst checks that when the
// primary job fails on its own debounced I11 while another job's immediate
// violation comes first in rule order, violations[0] is the primary's own.
func TestEngine_StrictScope_DebouncedOwnViolationReportedFirst(t *testing.T) {
	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{scopeRuleI2(), scopeRuleI11()}

	a := scopeActiveJob("pmj-a", "uid-a")
	scopeFence(&a)
	b := scopeSucceededNoSnapshot("pmj-b", "uid-b")

	var vs []Violation
	var shouldFail bool
	for i := 0; i < 3; i++ {
		vs, shouldFail = eng.Evaluate(context.Background(), &ReconcileSnapshot{
			Now:           scopeT0.Add(time.Duration(i) * 15 * time.Second),
			Reconciler:    "PodMigrationJobReconciler",
			PrimaryPMJ:    &a,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
			NamespacePods: []corev1.Pod{scopeSourcePod(&a, false)},
		})
		if i < 2 && shouldFail {
			t.Fatalf("sample %d: A must not fail before its I11 debounce threshold (B's I2 is not A's)", i)
		}
	}
	if !shouldFail {
		t.Fatalf("A must fail once its own I11 reaches the debounce threshold")
	}
	if len(vs) < 2 || vs[0].PMJName != "pmj-a" || vs[0].InvariantID != "I11" {
		t.Fatalf("violations[0] must be A's own I11 violation, got %+v", vs)
	}
}

// TestEngine_StrictScope_ViolationOnPrimaryReplacementFailsPrimary covers the
// I1 rebound check when the pod bound to B's consumed snapshot is A's
// replacement. I1 reports it against A, the job the pod is attached to, so A
// still fails instead of going on to restore from B's snapshot.
func TestEngine_StrictScope_ViolationOnPrimaryReplacementFailsPrimary(t *testing.T) {
	eng := NewEngine(ModeStrict, nil)
	eng.Rules = []Rule{funcRule{id: "I1", name: "AtMostOnceRestore", fn: EvaluateI1}}

	a := scopeActiveJob("pmj-a", "uid-a")
	a.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	a.Status.RestoredPodName = "pmj-a-dst"
	b := pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-b", UID: "uid-b"},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			SnapshotRef:    "snap-b",
			Consumed:       true,
			GateReleased:   true,
			RestoredPodUID: "uid-b-dst",
		},
	}
	repl := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-a-dst",
			UID:       "uid-a-dst",
			Annotations: map[string]string{
				AnnotationAssignedPMJ: "pmj-a",
				AnnotationPSName:      "snap-b",
			},
		},
	}
	snap := &ReconcileSnapshot{
		Now:           scopeT0,
		Reconciler:    "PodMigrationJobReconciler",
		PrimaryPMJ:    &a,
		NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
		NamespacePods: []corev1.Pod{repl},
	}
	raw := EvaluateI1(snap)
	if len(raw) != 1 || raw[0].Reason != "ConsumedSnapshotReboundAfterGateRelease" || raw[0].PMJName != "pmj-a" || raw[0].PodName != "pmj-a-dst" {
		t.Fatalf("expected the I1 rebound violation to name pmj-a, the job its pod is attached to, got %+v", raw)
	}
	if !strings.Contains(raw[0].Message, "pmj-b") {
		t.Fatalf("the I1 rebound message must still name the consuming job pmj-b, got %q", raw[0].Message)
	}

	vs, shouldFail := eng.Evaluate(context.Background(), snap)
	if !shouldFail {
		t.Fatalf("strict mode must fail job A for a violation on its own replacement pod")
	}
	if len(vs) != 1 || vs[0].PodName != "pmj-a-dst" {
		t.Fatalf("expected the rebound violation first, got %+v", vs)
	}
}

// TestEngine_StrictScope_OtherJobDebouncedEventHasNoSampleSuffix checks that
// another job's debounced violation is evented without a sample count, since
// it is not counted for the reconciled job.
func TestEngine_StrictScope_OtherJobDebouncedEventHasNoSampleSuffix(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	eng := NewEngine(ModeStrict, recorder)
	eng.Rules = []Rule{scopeRuleI11()}

	a := scopeActiveJob("pmj-a", "uid-a")
	b := scopeActiveJob("pmj-b", "uid-b")
	scopeFence(&b)
	_, _ = eng.Evaluate(context.Background(), &ReconcileSnapshot{
		Now:           scopeT0,
		Reconciler:    "PodMigrationJobReconciler",
		PrimaryPMJ:    &a,
		NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, b},
		NamespacePods: []corev1.Pod{scopeSourcePod(&a, true), scopeSourcePod(&b, false)},
	})
	if len(recorder.Events) == 0 {
		t.Fatalf("expected B's I11 violations to be evented")
	}
	for len(recorder.Events) > 0 {
		if ev := <-recorder.Events; strings.Contains(ev, "[sample") {
			t.Fatalf("another job's violation must be evented without a sample count: %s", ev)
		}
	}
}

func TestViolationInScope(t *testing.T) {
	a := scopeActiveJob("pmj-a", "uid-a")
	s := &ReconcileSnapshot{PrimaryPMJ: &a}
	cases := []struct {
		name string
		v    Violation
		want bool
	}{
		{"own job", Violation{Namespace: "default", PMJName: "pmj-a"}, true},
		{"no job", Violation{Namespace: "default"}, true},
		{"other job", Violation{Namespace: "default", PMJName: "pmj-b"}, false},
		{"same name, other namespace", Violation{Namespace: "other", PMJName: "pmj-a"}, false},
		{"own job, no namespace", Violation{PMJName: "pmj-a"}, true},
	}
	for _, tc := range cases {
		if got := violationInScope(s, tc.v); got != tc.want {
			t.Errorf("%s: violationInScope = %v, want %v", tc.name, got, tc.want)
		}
	}
	if !violationInScope(&ReconcileSnapshot{}, Violation{PMJName: "pmj-b"}) {
		t.Errorf("without a PrimaryPMJ every violation must be in scope")
	}
}

// TestEngine_StrictScope_StatefulSetNameReuse covers a StatefulSet migrated
// more than once: an old Succeeded job X recorded RestoredPodName=web-0, and
// the current job A's replacement is also named web-0 (new UID, assigned to
// A). Rules that report X's violations against X's RestoredPodName must not
// fail A.
func TestEngine_StrictScope_StatefulSetNameReuse(t *testing.T) {
	a := scopeActiveJob("pmj-a", "uid-a")
	repl := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "web-0",
			UID:         "uid-new",
			Labels:      map[string]string{"controller-revision-hash": "rev2"},
			Annotations: map[string]string{AnnotationAssignedPMJ: "pmj-a"},
		},
		Spec: corev1.PodSpec{SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}}},
	}
	oldJob := func(mutate func(*pmv1alpha1.PodMigrationJob)) pmv1alpha1.PodMigrationJob {
		x := pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-x", UID: "uid-x"},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
				SnapshotRef:     "snap-x",
				RestoredPodName: "web-0",
				RestoredPodUID:  "uid-old",
			},
		}
		mutate(&x)
		return x
	}
	cases := []struct {
		name string
		rule Rule
		x    pmv1alpha1.PodMigrationJob
	}{
		{"I12", RuleI12(), oldJob(func(x *pmv1alpha1.PodMigrationJob) {
			x.Status.Conditions = []metav1.Condition{{Type: ConditionRestored, Status: metav1.ConditionFalse, Reason: "NotRestored"}}
		})},
		{"I6", funcRule{id: "I6", name: "RevisionAndIdentityFidelity", fn: EvaluateI6}, oldJob(func(x *pmv1alpha1.PodMigrationJob) {
			x.Labels = map[string]string{"controller-revision-hash": "rev1"}
		})},
		{"I9", funcRule{id: "I9", name: "DeterministicFallbackOverCrashloop", fn: EvaluateI9}, oldJob(func(x *pmv1alpha1.PodMigrationJob) {
			x.Status.Conditions = []metav1.Condition{{Type: "RestoreCrashed", Status: metav1.ConditionTrue, Reason: "Crashed"}}
		})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eng := NewEngine(ModeStrict, nil)
			eng.Rules = []Rule{tc.rule}
			snap := &ReconcileSnapshot{
				Now:           scopeT0,
				Reconciler:    "PodMigrationJobReconciler",
				PrimaryPMJ:    &a,
				NamespacePMJs: []pmv1alpha1.PodMigrationJob{a, tc.x},
				NamespacePods: []corev1.Pod{repl},
			}
			vs, shouldFail := eng.Evaluate(context.Background(), snap)
			for _, v := range vs {
				if v.PMJName == "pmj-a" {
					t.Fatalf("test setup: unexpected violation of A: %+v", v)
				}
			}
			if shouldFail {
				t.Fatalf("strict mode must not fail job A for job X's %s violation on a reused pod name: %+v", tc.name, vs)
			}
		})
	}
}
