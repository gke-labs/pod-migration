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

// simulateKubectlDrainStep models one 5-second iteration of kubectl/pkg/drain's
// evictPod + waitForDelete loop for a single pod name:
//  1. Sends POST /api/v1/namespaces/<ns>/pods/<podName>/eviction by name only
//     (no Preconditions.UID).
//  2. On 404 NotFound (or 200 Allowed), breaks out of evictPod and runs
//     waitForDelete, which checks whether the pod is gone or its UID changed
//     from originalUID.
//  3. On 429 TooManyRequests, calls GET /pods/<podName> and only checks
//     IsNotFound(err) (does NOT compare UID inside evictPod), remaining in the
//     retry loop.
func simulateKubectlDrainStep(
	ctx context.Context,
	t *testing.T,
	c client.Client,
	gate *EvictionGate,
	namespace, podName string,
	originalUID types.UID,
) (drainComplete bool, statusCode int32) {
	t.Helper()
	dryRun := false
	req := admission.Request{}
	req.Namespace = namespace
	req.Name = podName
	req.SubResource = "eviction"
	req.DryRun = &dryRun
	resp := gate.Handle(ctx, req)

	if resp.Allowed {
		var p corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &p); err == nil {
			_ = c.Delete(ctx, &p)
		}
		var after corev1.Pod
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &after)
		if err != nil || after.UID != originalUID {
			return true, http.StatusOK
		}
		return false, http.StatusOK
	}

	code := int32(0)
	if resp.Result != nil {
		code = resp.Result.Code
	}
	if code == http.StatusNotFound && resp.Result.Reason == metav1.StatusReasonNotFound {
		// kubectl drain evictPod treats apierrors.IsNotFound(err) as success (nil)
		// and proceeds to waitForDelete, which verifies UID != originalUID.
		var after corev1.Pod
		err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &after)
		if err != nil || after.UID != originalUID {
			return true, http.StatusNotFound
		}
		t.Fatalf("EvictionGate returned 404 NotFound while original pod UID %s is still present", originalUID)
	}

	if code == http.StatusTooManyRequests {
		// kubectl drain evictPod sleeps 5s, then GETs the pod by name and only
		// exits if IsNotFound(err) is true (ignoring UID changes until POST /eviction
		// returns 200 or 404).
		var current corev1.Pod
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &current); err != nil {
			return true, http.StatusTooManyRequests
		}
		return false, http.StatusTooManyRequests
	}

	t.Fatalf("unexpected admission response in kubectl drain loop: allowed=%t result=%+v", resp.Allowed, resp.Result)
	return false, code
}

func countPMJsInNamespace(ctx context.Context, t *testing.T, c client.Client, namespace string) int {
	t.Helper()
	var list pmv1alpha1.PodMigrationJobList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		t.Fatalf("list PodMigrationJobs: %v", err)
	}
	return len(list.Items)
}

// TestKubectlDrainLoop_StatefulSetSingleHopAndRollingDrain is the Tier-1 (T1)
// guardrail regression test for #192: it drives the full client-go `kubectl drain`
// 5-second retry loop across a StatefulSet pod migration lifecycle and asserts:
//  1. Single-node drain of `node-a` terminates cleanly in 1 migration hop (never
//     spawning a 2nd PMJ when the same-name replacement pod lands on uncordoned `node-b`).
//  2. Sequential rolling drain (`node-a` followed by `node-b`) holds with 429 on
//     `node-b` while hop 1 is still Restoring (#162), then spawns exactly 1 second
//     migration (`node-b` -> `node-c`) once hop 1 reaches Succeeded, and terminates
//     cleanly at 2 total PMJs (never a 3rd hop).
func TestKubectlDrainLoop_StatefulSetSingleHopAndRollingDrain(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	const (
		ns      = "default"
		podName = "pm-redis-0"
		uid1    = types.UID("11111111-aaaa-bbbb-cccc-000000000001")
		uid2    = types.UID("22222222-aaaa-bbbb-cccc-000000000002")
		uid3    = types.UID("33333333-aaaa-bbbb-cccc-000000000003")
	)

	gvisorClass := "gvisor"
	makeStatefulSetPod := func(uid types.UID, nodeName string, annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   ns,
				Name:        podName,
				UID:         uid,
				Labels:      map[string]string{"pod-migration.gke.io/enabled": "true", "app": "redis"},
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				NodeName:         nodeName,
				RuntimeClassName: &gvisorClass,
			},
			Status: corev1.PodStatus{Phase: corev1.PodRunning},
		}
	}

	baseObjs := budgetTestObjects(nil)
	// Replace the default budgetTestPod with our StatefulSet pod pm-redis-0 on node-a.
	objs := []client.Object{
		baseObjs[1], // PodMigration policy in default namespace
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-a"}, Spec: corev1.NodeSpec{Unschedulable: true}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-b"}, Spec: corev1.NodeSpec{Unschedulable: false}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-c"}, Spec: corev1.NodeSpec{Unschedulable: false}},
		makeStatefulSetPod(uid1, "node-a", nil),
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()

	gate := &EvictionGate{
		Client:    c,
		APIReader: c,
		Recorder:  record.NewFakeRecorder(32),
	}

	// --- Hop 1: kubectl drain node-a ---
	// Tick 1 (T=0s): initial eviction creates J1 and returns 429.
	done, code := simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid1)
	if done || code != http.StatusTooManyRequests {
		t.Fatalf("Hop 1 Tick 1: got done=%t code=%d, want done=false code=429", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 1 {
		t.Fatalf("Hop 1 Tick 1: expected 1 PMJ, got %d", got)
	}

	job1Name := util.FormatPMJName(podName, string(uid1))
	var job1 pmv1alpha1.PodMigrationJob
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: job1Name}, &job1); err != nil {
		t.Fatalf("get job1 %s: %v", job1Name, err)
	}
	job1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
	job1.Status.OriginNodeName = "node-a"
	if err := c.Status().Update(ctx, &job1); err != nil {
		t.Fatalf("update job1 Snapshotting: %v", err)
	}

	// Tick 2 (T=5s): J1 is Snapshotting -> 429.
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid1)
	if done || code != http.StatusTooManyRequests {
		t.Fatalf("Hop 1 Tick 2: got done=%t code=%d, want done=false code=429", done, code)
	}

	// Controller deletes source pod (uid1) on node-a, transitions J1 to Restoring,
	// and StatefulSet creates same-name replacement pod (uid2) on uncordoned node-b.
	if err := c.Delete(ctx, makeStatefulSetPod(uid1, "node-a", nil)); err != nil {
		t.Fatalf("delete source pod uid1: %v", err)
	}
	pod2 := makeStatefulSetPod(uid2, "node-b", map[string]string{
		util.AnnotationAssignedPMJ: job1Name,
	})
	if err := c.Create(ctx, pod2); err != nil {
		t.Fatalf("create replacement pod uid2: %v", err)
	}
	job1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	job1.Status.RestoredPodName = podName
	job1.Status.RestoredPodUID = string(uid2)
	if err := c.Status().Update(ctx, &job1); err != nil {
		t.Fatalf("update job1 Restoring: %v", err)
	}

	// Tick 3 (T=10s): kubectl drain node-a retries while J1 is Restoring and uid2 is on uncordoned node-b.
	// EvictionGate MUST answer 404 NotFound so kubectl drain exits cleanly in 1 hop.
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid1)
	if !done || code != http.StatusNotFound {
		t.Fatalf("Hop 1 Tick 3: got done=%t code=%d, want done=true code=404", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 1 {
		t.Fatalf("after Hop 1 drain completion: expected exactly 1 PMJ, got %d", got)
	}

	// Verify that even if J1 completes (Succeeded) right as a late origin-drain retry arrives,
	// EvictionGate still answers 404 and does NOT spawn a 2nd PMJ while node-b is uncordoned.
	completedAt := metav1.Now()
	job1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	job1.Status.CompletionTime = &completedAt
	if err := c.Status().Update(ctx, &job1); err != nil {
		t.Fatalf("update job1 Succeeded: %v", err)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: podName}, pod2); err != nil {
		t.Fatalf("get pod2: %v", err)
	}
	pod2.Annotations[util.AnnotationRestoreCompleted] = "snap-1"
	if err := c.Update(ctx, pod2); err != nil {
		t.Fatalf("update pod2 restore-completed: %v", err)
	}
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid1)
	if !done || code != http.StatusNotFound {
		t.Fatalf("Hop 1 post-Succeeded retry: got done=%t code=%d, want done=true code=404", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 1 {
		t.Fatalf("after Hop 1 post-Succeeded retry: expected 1 PMJ, got %d", got)
	}

	// --- Hop 2: Sequential rolling drain of node-b ---
	// First put J1 back in Restoring and cordon node-b to verify #162 holds with 429 during Restoring.
	job1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	job1.Status.CompletionTime = nil
	if err := c.Status().Update(ctx, &job1); err != nil {
		t.Fatalf("revert job1 to Restoring for rolling drain test: %v", err)
	}
	var nodeB corev1.Node
	if err := c.Get(ctx, types.NamespacedName{Name: "node-b"}, &nodeB); err != nil {
		t.Fatalf("get node-b: %v", err)
	}
	nodeB.Spec.Unschedulable = true
	if err := c.Update(ctx, &nodeB); err != nil {
		t.Fatalf("cordon node-b: %v", err)
	}

	// Tick 1 on node-b (while J1 is Restoring): must return 429 (#162) and NOT spawn J2 yet.
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid2)
	if done || code != http.StatusTooManyRequests {
		t.Fatalf("Hop 2 Tick 1 (J1 Restoring on cordoned node-b): got done=%t code=%d, want done=false code=429", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 1 {
		t.Fatalf("Hop 2 Tick 1: expected still 1 PMJ while J1 is Restoring, got %d", got)
	}

	// J1 finishes Succeeded; next 5s tick of `kubectl drain node-b` must spawn J2 (429).
	completedAt2 := metav1.Now()
	job1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	job1.Status.CompletionTime = &completedAt2
	if err := c.Status().Update(ctx, &job1); err != nil {
		t.Fatalf("update job1 Succeeded for Hop 2: %v", err)
	}
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid2)
	if done || code != http.StatusTooManyRequests {
		t.Fatalf("Hop 2 Tick 2 (J1 Succeeded on cordoned node-b): got done=%t code=%d, want done=false code=429", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 2 {
		t.Fatalf("Hop 2 Tick 2: expected 2 PMJs after J2 spawned, got %d", got)
	}

	// Controller migrates uid2 (node-b) -> uid3 (uncordoned node-c) under J2.
	job2Name := util.FormatPMJName(podName, string(uid2))
	var job2 pmv1alpha1.PodMigrationJob
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: job2Name}, &job2); err != nil {
		t.Fatalf("get job2 %s: %v", job2Name, err)
	}
	if err := c.Delete(ctx, pod2); err != nil {
		t.Fatalf("delete pod2: %v", err)
	}
	pod3 := makeStatefulSetPod(uid3, "node-c", map[string]string{
		util.AnnotationAssignedPMJ: job2Name,
	})
	if err := c.Create(ctx, pod3); err != nil {
		t.Fatalf("create pod3 on node-c: %v", err)
	}
	job2.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
	job2.Status.OriginNodeName = "node-b"
	job2.Status.RestoredPodName = podName
	job2.Status.RestoredPodUID = string(uid3)
	if err := c.Status().Update(ctx, &job2); err != nil {
		t.Fatalf("update job2 Restoring: %v", err)
	}

	// Tick 3 on node-b: uid3 is now on uncordoned node-c -> EvictionGate answers 404 and node-b drain completes!
	done, code = simulateKubectlDrainStep(ctx, t, c, gate, ns, podName, uid2)
	if !done || code != http.StatusNotFound {
		t.Fatalf("Hop 2 Tick 3: got done=%t code=%d, want done=true code=404", done, code)
	}
	if got := countPMJsInNamespace(ctx, t, c, ns); got != 2 {
		t.Fatalf("after Hop 2 drain completion: expected 2 total PMJs, got %d", got)
	}
}
