package controller

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func ifPod(name string, ready bool) *corev1.Pod {
	status := corev1.ConditionFalse
	if ready {
		status = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID("uid-" + name), Labels: map[string]string{"app": "db"}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: status}}},
	}
}

// ifPDB's cached status allows a disruption, so checks reach the dry run and
// the in-flight count.
func ifPDB(desiredHealthy int32, app string, policy *policyv1.UnhealthyPodEvictionPolicyType) *policyv1.PodDisruptionBudget {
	return &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "db", Generation: 1},
		Spec: policyv1.PodDisruptionBudgetSpec{
			Selector:                   &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
			UnhealthyPodEvictionPolicy: policy,
		},
		Status: policyv1.PodDisruptionBudgetStatus{ObservedGeneration: 1, DesiredHealthy: desiredHealthy, DisruptionsAllowed: 1},
	}
}

func ifJob(name, pod string, phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name, UID: types.UID("job-" + name)},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: pod}, TargetPodUID: "uid-" + pod},
		Status:     pmv1alpha1.PodMigrationJobStatus{Phase: phase},
	}
}

// allowDryRun is a stateless dry run that always allows, safe for concurrent checks.
type allowDryRun struct{}

func (allowDryRun) Check(context.Context, *pmv1alpha1.PodMigrationJob, string) (string, error) {
	return reasonBudgetAllowed, nil
}

func ifChecker(objs ...client.Object) (*InFlightBudgetChecker, client.Client) {
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).WithObjects(objs...).Build()
	return &InFlightBudgetChecker{Client: c, DryRun: allowDryRun{}}, c
}

func TestInFlightBudgetChecker_CountsOtherMigrations(t *testing.T) {
	alwaysAllow := policyv1.AlwaysAllow
	ifHealthy := policyv1.IfHealthyBudget
	self := ifJob("self", "a", pmv1alpha1.PodMigrationJobPhasePending)
	gone := ifPod("b", true)
	gone.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	gone.Finalizers = []string{"test/keep"}
	emptySelectorPDB := ifPDB(3, "db", nil)
	emptySelectorPDB.Spec.Selector = &metav1.LabelSelector{}
	nilSelectorPDB := ifPDB(3, "db", nil)
	nilSelectorPDB.Spec.Selector = nil
	for name, tc := range map[string]struct {
		objs       []client.Object
		wantReason string
	}{
		"no other migration": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", nil), self},
			reasonBudgetAllowed,
		},
		"another migration in flight": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", nil), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseSnapshotting)},
			reasonMigrationsInFlight,
		},
		"the other source is terminating and its replacement is ready": {
			[]client.Object{ifPod("a", true), gone, ifPod("c", true), ifPod("d", true), ifPDB(2, "db", nil), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			reasonBudgetAllowed,
		},
		"terminal and waiting jobs don't count": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", nil), self,
				ifJob("failed", "b", pmv1alpha1.PodMigrationJobPhaseFailed), ifJob("waiting", "c", pmv1alpha1.PodMigrationJobPhasePending)},
			reasonBudgetAllowed,
		},
		"a PDB that doesn't cover the source": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "other", nil), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseSnapshotting)},
			reasonBudgetAllowed,
		},
		"a PDB with an empty selector covers all pods in the namespace": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), emptySelectorPDB, self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseSnapshotting)},
			reasonMigrationsInFlight,
		},
		"a PDB with a nil selector covers nothing": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), nilSelectorPDB, self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseSnapshotting)},
			reasonBudgetAllowed,
		},
		"NotReady source, IfHealthyBudget, the others are enough": {
			[]client.Object{ifPod("a", false), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", &ifHealthy), self},
			reasonBudgetAllowed,
		},
		"NotReady source, IfHealthyBudget, the others aren't enough": {
			[]client.Object{ifPod("a", false), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", &ifHealthy), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseEvicting)},
			reasonMigrationsInFlight,
		},
		"NotReady source, AlwaysAllow": {
			[]client.Object{ifPod("a", false), ifPod("b", true), ifPod("c", true), ifPDB(3, "db", &alwaysAllow), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseEvicting)},
			reasonBudgetAllowed,
		},
	} {
		t.Run(name, func(t *testing.T) {
			checker, _ := ifChecker(tc.objs...)
			reason, err := checker.Check(context.Background(), self, "a")
			if reason != tc.wantReason {
				t.Fatalf("Check = (%q, %v), want %q", reason, err, tc.wantReason)
			}
		})
	}
}

func TestInFlightBudgetChecker_DryRunRefusalShortCircuits(t *testing.T) {
	checker, _ := ifChecker(ifPod("a", true), ifPDB(0, "db", nil))
	checker.DryRun = &stubBudgetChecker{reason: reasonBudgetExhausted}
	if reason, _ := checker.Check(context.Background(), ifJob("self", "a", pmv1alpha1.PodMigrationJobPhasePending), "a"); reason != reasonBudgetExhausted {
		t.Fatalf("reason = %q, want %q", reason, reasonBudgetExhausted)
	}
}

func TestInFlightBudgetChecker_ReservationBlocksTheNextJob(t *testing.T) {
	jobA := ifJob("job-a", "a", pmv1alpha1.PodMigrationJobPhasePending)
	jobB := ifJob("job-b", "b", pmv1alpha1.PodMigrationJobPhasePending)
	checker, c := ifChecker(ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", nil), jobA, jobB)
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	checker.now = func() time.Time { return now }
	ctx := context.Background()

	if reason, err := checker.Check(ctx, jobA, "a"); reason != reasonBudgetAllowed {
		t.Fatalf("first job: (%q, %v), want Allowed", reason, err)
	}
	// The cache still shows job-a Pending: only its reservation blocks job-b.
	if reason, _ := checker.Check(ctx, jobB, "b"); reason != reasonMigrationsInFlight {
		t.Fatalf("second job while the first is reserved: %q, want %q", reason, reasonMigrationsInFlight)
	}
	// The cache catches up: job-a is Snapshotting. Its reservation goes, and its phase still blocks job-b.
	jobA.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
	if err := c.Update(ctx, jobA); err != nil {
		t.Fatalf("update job-a: %v", err)
	}
	if reason, _ := checker.Check(ctx, jobB, "b"); reason != reasonMigrationsInFlight {
		t.Fatalf("second job while the first is Snapshotting: %q, want %q", reason, reasonMigrationsInFlight)
	}
	if n := len(checker.reserved["default/db"]); n != 0 {
		t.Errorf("reservations left after the cache caught up: %d, want 0", n)
	}
	// A reservation for a job that never left Pending expires after the TTL.
	jobA.Status.Phase = pmv1alpha1.PodMigrationJobPhasePending
	if err := c.Update(ctx, jobA); err != nil {
		t.Fatalf("update job-a: %v", err)
	}
	checker.reserve("default/db", jobA.UID)
	now = now.Add(inFlightReservationTTL + time.Second)
	if reason, err := checker.Check(ctx, jobB, "b"); reason != reasonBudgetAllowed {
		t.Fatalf("after the TTL: (%q, %v), want Allowed", reason, err)
	}
}

func TestInFlightBudgetChecker_ConcurrentChecks(t *testing.T) {
	for round := 0; round < 20; round++ {
		objs := []client.Object{ifPDB(2, "db", nil)}
		var jobs []*pmv1alpha1.PodMigrationJob
		for _, p := range []string{"a", "b", "c"} {
			objs = append(objs, ifPod(p, true))
			j := ifJob("job-"+p, p, pmv1alpha1.PodMigrationJobPhasePending)
			jobs = append(jobs, j)
			objs = append(objs, j)
		}
		checker, _ := ifChecker(objs...)
		var wg sync.WaitGroup
		var mu sync.Mutex
		allowed := 0
		for _, j := range jobs {
			wg.Add(1)
			go func(j *pmv1alpha1.PodMigrationJob) {
				defer wg.Done()
				if reason, _ := checker.Check(context.Background(), j, j.Spec.PodRef.Name); reason == reasonBudgetAllowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}(j)
		}
		wg.Wait()
		if allowed != 1 {
			t.Fatalf("round %d: %d jobs passed, want exactly 1 (one disruption allowed)", round, allowed)
		}
	}
}

// TestInFlightBudgetChecker_CachedBudgetSkipsTheDryRun: a healthy source
// under one PDB whose cached status allows no disruption waits without a dry
// run (Brian's #165 review, item 4). Anything the cache doesn't settle goes to
// the dry run.
func TestInFlightBudgetChecker_CachedBudgetSkipsTheDryRun(t *testing.T) {
	exhausted := ifPDB(2, "db", nil)
	exhausted.Status.DisruptionsAllowed = 0
	exhausted.Status.CurrentHealthy = 2
	lagging := ifPDB(2, "db", nil)
	lagging.Generation = 2 // the disruption controller hasn't seen the new spec yet
	second := exhausted.DeepCopy()
	second.Name = "db-2"
	emptySelector := ifPDB(2, "db", nil)
	emptySelector.Spec.Selector = &metav1.LabelSelector{}
	emptySelector.Status.DisruptionsAllowed = 0
	emptySelector.Status.CurrentHealthy = 2
	nilSelector := ifPDB(2, "db", nil)
	nilSelector.Spec.Selector = nil
	nilSelector.Status.DisruptionsAllowed = 0
	self := ifJob("self", "a", pmv1alpha1.PodMigrationJobPhasePending)

	for name, tc := range map[string]struct {
		objs        []client.Object
		wantReason  string
		wantDryRuns int
		wantCause   string // in the DisruptionBudget cause, when the cache refused
	}{
		"healthy source, budget at zero": {
			[]client.Object{ifPod("a", true), ifPod("b", true), exhausted, self},
			reasonBudgetExhausted, 0, "The disruption budget db needs 2 healthy pods and has 2 currently",
		},
		"healthy source, status behind the spec": {
			[]client.Object{ifPod("a", true), ifPod("b", true), lagging, self},
			reasonBudgetExhausted, 0, "The disruption budget db is still being processed by the server.",
		},
		"NotReady source: the dry run decides": {
			[]client.Object{ifPod("a", false), ifPod("b", true), ifPod("c", true), exhausted, self},
			reasonBudgetAllowed, 1, "",
		},
		"two PDBs: the dry run gets the eviction API's error": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), exhausted, second, self},
			reasonBudgetAllowed, 1, "",
		},
		"source not in the cache: the dry run decides": {
			[]client.Object{ifPod("b", true), exhausted, self},
			reasonBudgetAllowed, 1, "",
		},
		"an empty selector covers all pods in the namespace": {
			[]client.Object{ifPod("a", true), ifPod("b", true), emptySelector, self},
			reasonBudgetExhausted, 0, "The disruption budget db needs 2 healthy pods and has 2 currently",
		},
		"a nil selector covers nothing": {
			[]client.Object{ifPod("a", true), ifPod("b", true), nilSelector, self},
			reasonBudgetAllowed, 1, "",
		},
		"in-flight siblings exhaust budget: dry run is skipped": {
			[]client.Object{ifPod("a", true), ifPod("b", true), ifPod("c", true), ifPDB(2, "db", nil), self,
				ifJob("other", "b", pmv1alpha1.PodMigrationJobPhaseSnapshotting)},
			reasonMigrationsInFlight, 0, "",
		},
	} {
		t.Run(name, func(t *testing.T) {
			checker, _ := ifChecker(tc.objs...)
			dryRun := &stubBudgetChecker{reason: reasonBudgetAllowed}
			checker.DryRun = dryRun
			reason, err := checker.Check(context.Background(), self, "a")
			if reason != tc.wantReason {
				t.Fatalf("Check = (%q, %v), want %q", reason, err, tc.wantReason)
			}
			if dryRun.calls != tc.wantDryRuns {
				t.Errorf("dry runs = %d, want %d", dryRun.calls, tc.wantDryRuns)
			}
			if tc.wantCause != "" {
				cause, ok := apierrors.StatusCause(err, policyv1.DisruptionBudgetCause)
				if !apierrors.IsTooManyRequests(err) || !ok || !strings.Contains(cause.Message, tc.wantCause) {
					t.Errorf("refusal %v: want a 429 whose DisruptionBudget cause says %q", err, tc.wantCause)
				}
			}
		})
	}
}
