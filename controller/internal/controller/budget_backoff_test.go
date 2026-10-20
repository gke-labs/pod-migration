package controller

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestBudgetRecheckAfter: the recheck interval grows with how long the budget
// has blocked the job, from 5 s up to 30 s, plus up to 20% jitter, but never
// runs more than a second past the job's deadline.
func TestBudgetRecheckAfter(t *testing.T) {
	now := time.Now()
	later := now.Add(time.Hour)
	blockedFor := func(d time.Duration) *metav1.Condition {
		return &metav1.Condition{Type: ConditionDisruptionBudgetChecked, Status: metav1.ConditionFalse, LastTransitionTime: metav1.NewTime(now.Add(-d))}
	}
	for name, tc := range map[string]struct {
		cond     *metav1.Condition
		deadline time.Time
		want     time.Duration
		exact    bool // no jitter: the deadline decides
	}{
		"no condition yet":      {cond: nil, deadline: later, want: 5 * time.Second},
		"just blocked":          {cond: blockedFor(0), deadline: later, want: 5 * time.Second},
		"blocked for 20 s":      {cond: blockedFor(20 * time.Second), deadline: later, want: 10 * time.Second},
		"blocked for 2 min":     {cond: blockedFor(2 * time.Minute), deadline: later, want: 30 * time.Second},
		"blocked for 9 min":     {cond: blockedFor(9 * time.Minute), deadline: later, want: 30 * time.Second},
		"clock skew: future ts": {cond: blockedFor(-time.Minute), deadline: later, want: 5 * time.Second},
		"deadline in 10 s":      {cond: blockedFor(9 * time.Minute), deadline: now.Add(10 * time.Second), want: 11 * time.Second, exact: true},
		"deadline passed":       {cond: blockedFor(9 * time.Minute), deadline: now.Add(-time.Minute), want: time.Second, exact: true},
	} {
		t.Run(name, func(t *testing.T) {
			for range 20 {
				got := budgetRecheckAfter(tc.cond, tc.deadline, now)
				if tc.exact && got != tc.want {
					t.Fatalf("budgetRecheckAfter = %v, want %v", got, tc.want)
				}
				if !tc.exact && (got < tc.want || got > tc.want*6/5) {
					t.Fatalf("budgetRecheckAfter = %v, want %v plus up to 20%% jitter", got, tc.want)
				}
			}
		})
	}
}

// TestPodMigrationJobReconciler_BudgetRecheckStopsAtTheDeadline: a job that
// has waited most of its timeout rechecks at its deadline, not a full backed-off
// interval later, so it times out on time.
func TestPodMigrationJobReconciler_BudgetRecheckStopsAtTheDeadline(t *testing.T) {
	// Created 9m50s ago (the default timeout is 10 minutes), blocked since.
	blocked := metav1.Condition{Type: ConditionDisruptionBudgetChecked, Status: metav1.ConditionFalse, Reason: reasonBudgetExhausted,
		LastTransitionTime: metav1.NewTime(time.Now().Add(-9*time.Minute - 45*time.Second))}
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).
		WithObjects(budgetSourcePod(), budgetJob(time.Now().Add(-9*time.Minute-50*time.Second), nil, blocked)).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	res, _ := reconcileBudget(t, c, &stubBudgetChecker{reason: reasonBudgetExhausted, err: budgetRefusal()}, record.NewFakeRecorder(20))
	if res.RequeueAfter < 9*time.Second || res.RequeueAfter > 12*time.Second {
		t.Errorf("RequeueAfter = %v, want about 11 s: the deadline is 10 s away", res.RequeueAfter)
	}
}

// TestPodMigrationJobReconciler_BudgetRecheckSkipsUnchangedStatus: a recheck
// whose result hasn't changed doesn't patch the job's status.
func TestPodMigrationJobReconciler_BudgetRecheckSkipsUnchangedStatus(t *testing.T) {
	patches := 0
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).
		WithObjects(budgetSourcePod(), budgetJob(time.Now(), nil)).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, cl client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if subResource == "status" {
					patches++
				}
				return cl.SubResource(subResource).Patch(ctx, obj, patch, opts...)
			},
		}).Build()
	checker := &stubBudgetChecker{reason: reasonBudgetExhausted, err: budgetRefusal()}
	recorder := record.NewFakeRecorder(20)

	reconcileBudget(t, c, checker, recorder)
	if patches != 1 {
		t.Fatalf("first blocked check: %d status patches, want 1", patches)
	}
	patches = 0
	reconcileBudget(t, c, checker, recorder)
	if patches != 0 {
		t.Errorf("unchanged recheck: %d status patches, want 0", patches)
	}
	// A different answer is recorded.
	checker.reason, checker.err = reasonBudgetCheckError, context.DeadlineExceeded
	reconcileBudget(t, c, checker, recorder)
	if patches != 1 {
		t.Errorf("changed recheck: %d status patches, want 1", patches)
	}
}
