package webhook

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	budgetTestPod = "budget-pod"
	budgetTestUID = "budget-uid-1"
)

func budgetTestObjects(job *pmv1alpha1.PodMigrationJob) []client.Object {
	gvisor := "gvisor"
	psp := &unstructured.Unstructured{}
	psp.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotPolicy"})
	psp.SetName("psp-manual")
	psp.SetNamespace("default")
	psp.Object["spec"] = map[string]interface{}{
		"selector":      map[string]interface{}{"matchLabels": map[string]interface{}{"pod-migration.gke.io/enabled": "true"}},
		"triggerConfig": map[string]interface{}{"type": "manual", "postCheckpoint": "stop"},
	}
	psp.Object["status"] = map[string]interface{}{"conditions": []interface{}{map[string]interface{}{"type": "Ready", "status": "True"}}}
	objs := []client.Object{
		&corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      budgetTestPod,
				UID:       types.UID(budgetTestUID),
				Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
			},
			Spec: corev1.PodSpec{RuntimeClassName: &gvisor},
		},
		psp,
	}
	if job != nil {
		objs = append(objs, job)
	}
	return objs
}

func budgetTestJob(phase pmv1alpha1.PodMigrationJobPhase, conds ...metav1.Condition) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      util.FormatPMJName(budgetTestPod, budgetTestUID),
			UID:       types.UID("old-job-uid"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: budgetTestPod},
			TargetPodUID: budgetTestUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{Phase: phase, Conditions: conds},
	}
}

func handleBudgetEviction(t *testing.T, objs []client.Object, dryRun bool) (admission.Response, client.Client, *record.FakeRecorder) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	recorder := record.NewFakeRecorder(10)
	gate := &EvictionGate{Client: c, APIReader: c, Recorder: recorder}
	req := admission.Request{}
	req.Namespace = "default"
	req.Name = budgetTestPod
	req.SubResource = "eviction"
	req.DryRun = &dryRun
	return gate.Handle(context.Background(), req), c, recorder
}

func TestEvictionGate_ReplacesJobThatTimedOutOnBudget(t *testing.T) {
	expired := budgetTestJob(pmv1alpha1.PodMigrationJobPhaseFailed,
		metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: util.ReasonDisruptionBudgetTimeout, LastTransitionTime: metav1.Now()})
	resp, c, recorder := handleBudgetEviction(t, budgetTestObjects(expired), false)
	if resp.Allowed || resp.Result == nil || resp.Result.Code != 429 {
		t.Fatalf("expected 429, got allowed=%t result=%+v", resp.Allowed, resp.Result)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expired.Name}, job); err != nil {
		t.Fatalf("expected a new job: %v", err)
	}
	if job.UID == expired.UID || job.Status.Phase == pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Errorf("expected a fresh job, got UID %q phase %q", job.UID, job.Status.Phase)
	}
	select {
	case ev := <-recorder.Events:
		if !strings.Contains(ev, "MigrationRestartedAfterBudgetWait") {
			t.Errorf("unexpected event %q", ev)
		}
	default:
		t.Error("expected a MigrationRestartedAfterBudgetWait event")
	}
}

func TestEvictionGate_OtherFailedJobsStillFallBackCold(t *testing.T) {
	failed := budgetTestJob(pmv1alpha1.PodMigrationJobPhaseFailed,
		metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Timeout", LastTransitionTime: metav1.Now()})
	resp, c, _ := handleBudgetEviction(t, budgetTestObjects(failed), false)
	if !resp.Allowed {
		t.Fatalf("expected the cold-eviction fallback, got %+v", resp.Result)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: failed.Name}, job); err != nil || job.UID != failed.UID {
		t.Errorf("the failed job must stay as it was: uid=%q err=%v", job.UID, err)
	}
}

func TestEvictionGate_WaitingForBudgetAnswers429(t *testing.T) {
	waiting := budgetTestJob(pmv1alpha1.PodMigrationJobPhasePending,
		metav1.Condition{Type: util.ConditionDisruptionBudgetChecked, Status: metav1.ConditionFalse, Reason: "BudgetExhausted", LastTransitionTime: metav1.Now()})
	resp, _, _ := handleBudgetEviction(t, budgetTestObjects(waiting), false)
	if resp.Allowed || resp.Result == nil || resp.Result.Code != 429 || !strings.Contains(resp.Result.Message, "PodDisruptionBudget") {
		t.Fatalf("expected a 429 that names the PodDisruptionBudget, got allowed=%t result=%+v", resp.Allowed, resp.Result)
	}
}

func TestEvictionGate_DryRunNeverReplacesAJob(t *testing.T) {
	expired := budgetTestJob(pmv1alpha1.PodMigrationJobPhaseFailed,
		metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: util.ReasonDisruptionBudgetTimeout, LastTransitionTime: metav1.Now()})
	resp, c, _ := handleBudgetEviction(t, budgetTestObjects(expired), true)
	if !resp.Allowed {
		t.Fatalf("expected a dry run to be allowed, got %+v", resp.Result)
	}
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "default", Name: expired.Name}, job); err != nil || job.UID != expired.UID {
		t.Errorf("a dry run must not replace the job: uid=%q err=%v", job.UID, err)
	}
}
