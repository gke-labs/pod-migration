package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	budgetPod = "budget-pod"
	budgetUID = "budget-uid-1"
)

type stubBudgetChecker struct {
	reason string
	err    error
	calls  int
}

func (s *stubBudgetChecker) Check(context.Context, *pmv1alpha1.PodMigrationJob, string) (string, error) {
	s.calls++
	return s.reason, s.err
}

func budgetScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = policyv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	return scheme
}

func budgetJob(created time.Time, labels map[string]string, conds ...metav1.Condition) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              util.FormatPMJName(budgetPod, budgetUID),
			CreationTimestamp: metav1.NewTime(created),
			Labels:            labels,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: budgetPod},
			TargetPodUID: budgetUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending, Conditions: conds},
	}
}

func budgetSourcePod() *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: budgetPod, UID: types.UID(budgetUID)}}
}

func reconcileBudget(t *testing.T, c client.Client, checker DisruptionBudgetChecker, recorder record.EventRecorder) (ctrl.Result, *pmv1alpha1.PodMigrationJob) {
	t.Helper()
	r := &PodMigrationJobReconciler{Client: c, Scheme: c.Scheme(), Recorder: recorder}
	if checker != nil {
		r.BudgetChecker = checker
	}
	key := types.NamespacedName{Namespace: "default", Name: util.FormatPMJName(budgetPod, budgetUID)}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), key, job); err != nil {
		t.Fatalf("get job: %v", err)
	}
	return res, job
}

// budgetRefusal is the 429 the eviction API returns when a budget refuses an
// eviction: it carries the DisruptionBudget cause, which names the budget.
func budgetRefusal() error {
	err := apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{
		Type:    policyv1.DisruptionBudgetCause,
		Message: "The disruption budget db needs 1 healthy pods and has 1 currently",
	})
	return err
}

func TestEvictionBudgetChecker_DryRunEviction(t *testing.T) {
	for name, tc := range map[string]struct {
		err        error
		wantReason string
	}{
		"allowed":          {nil, reasonBudgetAllowed},
		"budget exhausted": {budgetRefusal(), reasonBudgetExhausted},
		// API Priority and Fairness throttling answers 429 too, without the cause.
		"throttled":   {apierrors.NewTooManyRequests("Too many requests, please try again later.", 1), reasonBudgetCheckError},
		"other error": {apierrors.NewInternalError(errors.New("This pod has more than one PodDisruptionBudget")), reasonBudgetCheckError},
	} {
		t.Run(name, func(t *testing.T) {
			var gotDryRun []string
			var gotUID types.UID
			c := fake.NewClientBuilder().WithScheme(budgetScheme()).WithObjects(budgetSourcePod()).
				WithInterceptorFuncs(interceptor.Funcs{
					SubResourceCreate: func(ctx context.Context, cl client.Client, subResource string, obj client.Object, sub client.Object, opts ...client.SubResourceCreateOption) error {
						if subResource != "eviction" {
							t.Errorf("subresource = %q, want eviction", subResource)
						}
						o := &client.SubResourceCreateOptions{}
						o.ApplyOptions(opts)
						gotDryRun = o.DryRun
						if ev, ok := sub.(*policyv1.Eviction); ok && ev.DeleteOptions != nil && ev.DeleteOptions.Preconditions != nil {
							gotUID = *ev.DeleteOptions.Preconditions.UID
						}
						return tc.err
					},
				}).Build()
			reason, err := EvictionBudgetChecker{Client: c}.Check(context.Background(), budgetJob(time.Now(), nil), budgetPod)
			if reason != tc.wantReason || (err == nil) != (tc.err == nil) {
				t.Errorf("Check = (%q, %v), want (%q, %v)", reason, err, tc.wantReason, tc.err)
			}
			if len(gotDryRun) != 1 || gotDryRun[0] != metav1.DryRunAll {
				t.Errorf("dry run = %v, want [All]", gotDryRun)
			}
			if gotUID != types.UID(budgetUID) {
				t.Errorf("UID precondition = %q, want %q", gotUID, budgetUID)
			}
		})
	}
}

func TestPodMigrationJobReconciler_BudgetCheckInPending(t *testing.T) {
	tooMany := budgetRefusal()
	for name, tc := range map[string]struct {
		checker    *stubBudgetChecker
		labels     map[string]string
		wantPhase  pmv1alpha1.PodMigrationJobPhase
		wantStatus metav1.ConditionStatus // "" means no condition
		wantReason string
		wantCalls  int
		wantEvent  bool
	}{
		"allowed": {
			checker: &stubBudgetChecker{reason: reasonBudgetAllowed}, wantPhase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			wantStatus: metav1.ConditionTrue, wantReason: reasonBudgetAllowed, wantCalls: 1,
		},
		"budget exhausted": {
			checker: &stubBudgetChecker{reason: reasonBudgetExhausted, err: tooMany}, wantPhase: pmv1alpha1.PodMigrationJobPhasePending,
			wantStatus: metav1.ConditionFalse, wantReason: reasonBudgetExhausted, wantCalls: 1, wantEvent: true,
		},
		"check error": {
			checker: &stubBudgetChecker{reason: reasonBudgetCheckError, err: errors.New("boom")}, wantPhase: pmv1alpha1.PodMigrationJobPhasePending,
			wantStatus: metav1.ConditionUnknown, wantReason: reasonBudgetCheckError, wantCalls: 1,
		},
		"spot preemption skips the check": {
			checker:   &stubBudgetChecker{reason: reasonBudgetExhausted, err: tooMany},
			labels:    map[string]string{util.LabelTriggerSource: util.TriggerSourceSpotPreemption},
			wantPhase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(budgetScheme()).
				WithObjects(budgetSourcePod(), budgetJob(time.Now(), tc.labels)).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
			recorder := record.NewFakeRecorder(20)
			res, job := reconcileBudget(t, c, tc.checker, recorder)
			if job.Status.Phase != tc.wantPhase {
				t.Errorf("phase = %q, want %q", job.Status.Phase, tc.wantPhase)
			}
			cond := meta.FindStatusCondition(job.Status.Conditions, ConditionDisruptionBudgetChecked)
			switch {
			case tc.wantStatus == "" && cond != nil:
				t.Errorf("unexpected condition %+v", cond)
			case tc.wantStatus != "" && (cond == nil || cond.Status != tc.wantStatus || cond.Reason != tc.wantReason):
				t.Errorf("condition = %+v, want %s/%s", cond, tc.wantStatus, tc.wantReason)
			}
			if tc.checker.calls != tc.wantCalls {
				t.Errorf("checker calls = %d, want %d", tc.checker.calls, tc.wantCalls)
			}
			if tc.wantPhase == pmv1alpha1.PodMigrationJobPhasePending &&
				(res.RequeueAfter < budgetRecheckInterval || res.RequeueAfter > budgetRecheckInterval*6/5) {
				t.Errorf("RequeueAfter = %v, want %v plus up to 20%% jitter", res.RequeueAfter, budgetRecheckInterval)
			}
			if tc.wantReason == reasonBudgetExhausted && !strings.Contains(cond.Message, "The disruption budget db needs") {
				t.Errorf("condition message %q doesn't name the budget", cond.Message)
			}
			if got := countEvents(recorder, "MigrationDeferredByPDB"); (got == 1) != tc.wantEvent || got > 1 {
				t.Errorf("MigrationDeferredByPDB events = %d, want %t", got, tc.wantEvent)
			}
			if tc.wantEvent {
				// A second blocked check doesn't repeat the event.
				reconcileBudget(t, c, tc.checker, recorder)
				if got := countEvents(recorder, "MigrationDeferredByPDB"); got != 0 {
					t.Errorf("second check emitted %d more MigrationDeferredByPDB events, want 0", got)
				}
			}
		})
	}
}

func TestPodMigrationJobReconciler_BudgetWaitTimesOut(t *testing.T) {
	blocked := metav1.Condition{Type: ConditionDisruptionBudgetChecked, Status: metav1.ConditionFalse, Reason: reasonBudgetExhausted, LastTransitionTime: metav1.Now()}
	for name, tc := range map[string]struct {
		conds      []metav1.Condition
		wantReason string
	}{
		"waiting for the budget": {[]metav1.Condition{blocked}, util.ReasonDisruptionBudgetTimeout},
		"not waiting":            {nil, "Timeout"},
	} {
		t.Run(name, func(t *testing.T) {
			// Created past the default 10 minute migration timeout.
			c := fake.NewClientBuilder().WithScheme(budgetScheme()).
				WithObjects(budgetSourcePod(), budgetJob(time.Now().Add(-11*time.Minute), nil, tc.conds...)).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
			_, job := reconcileBudget(t, c, &stubBudgetChecker{reason: reasonBudgetExhausted, err: errors.New("429")}, record.NewFakeRecorder(20))
			ready := meta.FindStatusCondition(job.Status.Conditions, "Ready")
			if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || ready == nil || ready.Reason != tc.wantReason {
				t.Fatalf("phase %q, Ready %+v; want Failed/%s", job.Status.Phase, ready, tc.wantReason)
			}
			if util.FailedOnDisruptionBudget(job) != (tc.wantReason == util.ReasonDisruptionBudgetTimeout) {
				t.Errorf("FailedOnDisruptionBudget = %t", util.FailedOnDisruptionBudget(job))
			}
		})
	}
}

func countEvents(recorder *record.FakeRecorder, reason string) int {
	n := 0
	for {
		select {
		case e := <-recorder.Events:
			if strings.Contains(e, reason) {
				n++
			}
		default:
			return n
		}
	}
}
