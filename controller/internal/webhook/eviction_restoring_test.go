package webhook

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const restoringTestJob = "pmj-orig-0-1a2b3c4d"

// restoringTestObjects returns the budget test pod as the replacement of
// restoringTestJob (its assigned-pmj annotation), the policy, and job1 when set.
func restoringTestObjects(job1 *pmv1alpha1.PodMigrationJob) []client.Object {
	objs := budgetTestObjects(nil)
	pod := objs[0].(*corev1.Pod)
	pod.Annotations = map[string]string{util.AnnotationAssignedPMJ: restoringTestJob}
	if job1 != nil {
		objs = append(objs, job1)
	}
	return objs
}

// restoringTestJob1 returns the job that restored the pod, in phase, with the
// restored pod's UID recorded as restoredUID.
func restoringTestJob1(phase pmv1alpha1.PodMigrationJobPhase, restoredUID string) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: restoringTestJob, UID: "job1-uid"},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "orig-0"},
			TargetPodUID: "orig-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:           phase,
			RestoredPodName: budgetTestPod,
			RestoredPodUID:  restoredUID,
		},
	}
}

func handleRestoringEviction(t *testing.T, objs []client.Object, dryRun bool, funcs *interceptor.Funcs) (admission.Response, client.Client) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{})
	if funcs != nil {
		b = b.WithInterceptorFuncs(*funcs)
	}
	c := b.Build()
	gate := &EvictionGate{Client: c, APIReader: c, Recorder: record.NewFakeRecorder(10)}
	req := admission.Request{}
	req.Namespace = "default"
	req.Name = budgetTestPod
	req.SubResource = "eviction"
	req.DryRun = &dryRun
	return gate.Handle(context.Background(), req), c
}

// secondJobExists reports whether the eviction created a job for the pod itself.
func secondJobExists(t *testing.T, c client.Client) bool {
	t.Helper()
	job := &pmv1alpha1.PodMigrationJob{}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: util.FormatPMJName(budgetTestPod, budgetTestUID)}, job)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting the second job: %v", err)
	}
	return err == nil
}

// TestEvictionGate_ReplacementStillRestoringIsNotMigratedAgain: while the job
// that restored a pod hasn't concluded, an eviction of that pod answers 429
// and creates no second job (#162). Once the first job concluded, or if it
// restored another pod under the same name, the eviction migrates the pod.
func TestEvictionGate_ReplacementStillRestoringIsNotMigratedAgain(t *testing.T) {
	tests := map[string]struct {
		job1    *pmv1alpha1.PodMigrationJob
		blocked bool
	}{
		"restoring this pod":                       {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseRestoring, budgetTestUID), true},
		"restoring, gate not released yet":         {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseRestoring, ""), true},
		"evicting its source":                      {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseEvicting, ""), true},
		"succeeded":                                {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseSucceeded, budgetTestUID), false},
		"succeeded without restore":                {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, budgetTestUID), false},
		"failed":                                   {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseFailed, budgetTestUID), false},
		"garbage-collected":                        {nil, false},
		"restored another pod under the same name": {restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseRestoring, "older-instance-uid"), false},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			resp, c := handleRestoringEviction(t, restoringTestObjects(tc.job1), false, nil)
			if resp.Allowed || resp.Result == nil || resp.Result.Code != 429 {
				t.Fatalf("expected 429, got allowed=%t result=%+v", resp.Allowed, resp.Result)
			}
			stillRestoring := strings.Contains(resp.Result.Message, "still being restored by migration job "+restoringTestJob)
			if stillRestoring != tc.blocked {
				t.Errorf("message = %q, want blocked=%t", resp.Result.Message, tc.blocked)
			}
			if got := secondJobExists(t, c); got == tc.blocked {
				t.Errorf("second job created = %t, want %t", got, !tc.blocked)
			}
		})
	}
}

// TestEvictionGate_RestoringJobReadErrorAnswers429: a failed read of the job
// that restored the pod is retried, not taken as permission.
func TestEvictionGate_RestoringJobReadErrorAnswers429(t *testing.T) {
	funcs := &interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == restoringTestJob {
				return apierrors.NewServiceUnavailable("apiserver busy")
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	resp, c := handleRestoringEviction(t, restoringTestObjects(restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseRestoring, budgetTestUID)), false, funcs)
	if resp.Allowed || resp.Result == nil || resp.Result.Code != 429 || !strings.Contains(resp.Result.Message, "transient error") {
		t.Fatalf("expected a transient 429, got allowed=%t result=%+v", resp.Allowed, resp.Result)
	}
	if secondJobExists(t, c) {
		t.Error("a second job was created although the first job couldn't be read")
	}
}

// TestEvictionGate_DryRunOfRestoringReplacementIsAllowed: dry runs keep
// bypassing the migration path, so the budget check still works.
func TestEvictionGate_DryRunOfRestoringReplacementIsAllowed(t *testing.T) {
	resp, c := handleRestoringEviction(t, restoringTestObjects(restoringTestJob1(pmv1alpha1.PodMigrationJobPhaseRestoring, budgetTestUID)), true, nil)
	if !resp.Allowed {
		t.Fatalf("expected a dry run to be allowed, got %+v", resp.Result)
	}
	if secondJobExists(t, c) {
		t.Error("a dry run created a job")
	}
}

// TestEvictionGate_StaleOriginDrainOnSameNameReplacementAnswers404 verifies
// that when kubectl drain on the origin node (node-a, unschedulable=true)
// retries POST /pods/pm-redis-0/eviction by name after the origin pod was
// evicted and replaced by a new StatefulSet pod with the same name (unscheduled
// or on uncordoned node-b), EvictionGate answers 404 NotFound (so kubectl drain
// exits cleanly without deleting the replacement or spawning a second job).
// When node-b is itself cordoned (rolling upgrade draining node-b), EvictionGate
// answers 429 while Restoring (#162) and spawns a new migration job once Succeeded.
func TestEvictionGate_StaleOriginDrainOnSameNameReplacementAnswers404(t *testing.T) {
	now := metav1.Now()
	staleCompletion := metav1.NewTime(time.Now().Add(-2 * time.Minute))

	tests := map[string]struct {
		phase              pmv1alpha1.PodMigrationJobPhase
		completionTime     *metav1.Time
		replacementNode    string
		originCordoned     bool
		destCordoned       bool
		wantCode           int32
		wantReason         metav1.StatusReason
		wantSecondJobBuilt bool
	}{
		"restoring on uncordoned node-b while node-a is cordoned -> 404": {
			phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			replacementNode:    "node-b",
			originCordoned:     true,
			destCordoned:       false,
			wantCode:           http.StatusNotFound,
			wantReason:         metav1.StatusReasonNotFound,
			wantSecondJobBuilt: false,
		},
		"evicting while replacement is unscheduled and node-a is cordoned -> 404": {
			phase:              pmv1alpha1.PodMigrationJobPhaseEvicting,
			replacementNode:    "",
			originCordoned:     true,
			destCordoned:       false,
			wantCode:           http.StatusNotFound,
			wantReason:         metav1.StatusReasonNotFound,
			wantSecondJobBuilt: false,
		},
		"just succeeded (<15s) on uncordoned node-b while node-a is cordoned -> 404": {
			phase:              pmv1alpha1.PodMigrationJobPhaseSucceeded,
			completionTime:     &now,
			replacementNode:    "node-b",
			originCordoned:     true,
			destCordoned:       false,
			wantCode:           http.StatusNotFound,
			wantReason:         metav1.StatusReasonNotFound,
			wantSecondJobBuilt: false,
		},
		"restoring on cordoned node-b (rolling upgrade of destination node) -> 429 hold (#162)": {
			phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			replacementNode:    "node-b",
			originCordoned:     true,
			destCordoned:       true,
			wantCode:           http.StatusTooManyRequests,
			wantReason:         "",
			wantSecondJobBuilt: false,
		},
		"succeeded on cordoned node-b (rolling upgrade of destination node) -> 429 new job spawned": {
			phase:              pmv1alpha1.PodMigrationJobPhaseSucceeded,
			completionTime:     &now,
			replacementNode:    "node-b",
			originCordoned:     true,
			destCordoned:       true,
			wantCode:           http.StatusTooManyRequests,
			wantReason:         "",
			wantSecondJobBuilt: true,
		},
		"succeeded long ago (>15s) on uncordoned node-b -> 429 new job spawned": {
			phase:              pmv1alpha1.PodMigrationJobPhaseSucceeded,
			completionTime:     &staleCompletion,
			replacementNode:    "node-b",
			originCordoned:     true,
			destCordoned:       false,
			wantCode:           http.StatusTooManyRequests,
			wantReason:         "",
			wantSecondJobBuilt: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			job1 := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: restoringTestJob, UID: "job1-uid"},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: budgetTestPod},
					TargetPodUID: "orig-uid",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:           tc.phase,
					OriginNodeName:  "node-a",
					RestoredPodName: budgetTestPod,
					RestoredPodUID:  budgetTestUID,
					CompletionTime:  tc.completionTime,
				},
			}
			objs := restoringTestObjects(job1)
			pod := objs[0].(*corev1.Pod)
			pod.Spec.NodeName = tc.replacementNode
			objs = append(objs,
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "node-a"},
					Spec:       corev1.NodeSpec{Unschedulable: tc.originCordoned},
				},
				&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: "node-b"},
					Spec:       corev1.NodeSpec{Unschedulable: tc.destCordoned},
				},
			)

			resp, c := handleRestoringEviction(t, objs, false, nil)
			if resp.Allowed || resp.Result == nil || resp.Result.Code != tc.wantCode {
				t.Fatalf("got allowed=%t result=%+v, want code=%d", resp.Allowed, resp.Result, tc.wantCode)
			}
			if tc.wantReason != "" && resp.Result.Reason != tc.wantReason {
				t.Errorf("result.Reason = %q, want %q", resp.Result.Reason, tc.wantReason)
			}
			if got := secondJobExists(t, c); got != tc.wantSecondJobBuilt {
				t.Errorf("secondJobExists = %t, want %t", got, tc.wantSecondJobBuilt)
			}
		})
	}
}
