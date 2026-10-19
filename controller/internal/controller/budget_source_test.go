package controller

import (
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodMigrationJobReconciler_BudgetWaitRechecksTheSource: a job waiting for
// the budget checks its source again on every pass, even once it has captured
// the PVs, the origin node and the timeout (a pod with a PVC and a
// memory-scaled timeout), so a source deleted, replaced, or finished during the
// wait fails the job instead of leaving it waiting out its timeout.
func TestPodMigrationJobReconciler_BudgetWaitRechecksTheSource(t *testing.T) {
	blocked := metav1.Condition{Type: ConditionDisruptionBudgetChecked, Status: metav1.ConditionFalse, Reason: reasonBudgetExhausted, LastTransitionTime: metav1.Now()}
	replaced := budgetSourcePod()
	replaced.UID = types.UID("budget-uid-2")
	finished := budgetSourcePod()
	finished.Status.Phase = corev1.PodSucceeded

	for name, tc := range map[string]struct {
		source     *corev1.Pod // nil: deleted
		wantReason string
	}{
		"source deleted":  {nil, "PodNotFound"},
		"source replaced": {replaced, "PodNotFound"},
		"source finished": {finished, "OriginPodTerminated"},
	} {
		t.Run(name, func(t *testing.T) {
			job := budgetJob(time.Now(), nil, blocked)
			job.Annotations = map[string]string{util.AnnotationMigrationTimeout: "15m"}
			job.Status.PVsToDetach = []string{"pv-1"}
			job.Status.OriginNodeName = "node-a"
			objs := []client.Object{job}
			if tc.source != nil {
				objs = append(objs, tc.source)
			}
			c := fake.NewClientBuilder().WithScheme(budgetScheme()).WithObjects(objs...).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
			checker := &stubBudgetChecker{reason: reasonBudgetExhausted, err: errors.New("429")}
			_, got := reconcileBudget(t, c, checker, record.NewFakeRecorder(20))
			ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || ready == nil || ready.Reason != tc.wantReason {
				t.Fatalf("phase %q, Ready %+v; want Failed/%s", got.Status.Phase, ready, tc.wantReason)
			}
			if checker.calls != 0 {
				t.Errorf("checker calls = %d, want 0: the source check comes before the dry run", checker.calls)
			}
		})
	}
}

// TestPodMigrationJobReconciler_BudgetCheckFindsNoSource: a dry run that finds
// no source pod (404) fails the job at once, even when the cache still has the pod.
func TestPodMigrationJobReconciler_BudgetCheckFindsNoSource(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).
		WithObjects(budgetSourcePod(), budgetJob(time.Now(), nil)).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	recorder := record.NewFakeRecorder(20)
	notFound := apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, budgetPod)
	res, got := reconcileBudget(t, c, &stubBudgetChecker{reason: reasonBudgetCheckError, err: notFound}, recorder)
	ready := meta.FindStatusCondition(got.Status.Conditions, "Ready")
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || ready == nil || ready.Reason != "PodNotFound" {
		t.Fatalf("phase %q, Ready %+v; want Failed/PodNotFound", got.Status.Phase, ready)
	}
	if res.Requeue || res.RequeueAfter != 0 {
		t.Errorf("result = %+v, want no requeue", res)
	}
	if got := countEvents(recorder, "MigrationFailed"); got != 1 {
		t.Errorf("MigrationFailed events = %d, want 1", got)
	}
}
