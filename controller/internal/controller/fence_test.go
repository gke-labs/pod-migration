package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	fenceTestNS  = "default"
	fenceTestPod = "web-0"
	fenceTestUID = "source-uid"
	fenceTestJob = "pmj-web-0"
	fenceTestCID = "containerd://aaa"
)

// fenceTestProvider is a snapshot provider that also reports the checkpoint's
// state, like the GKE provider.
type fenceTestProvider struct {
	mu           sync.Mutex
	ensureErr    error
	onEnsure     func()
	ensureCalls  int
	cleanupCalls int
	state        snapshot.CheckpointState
	stateErr     error
}

func (p *fenceTestProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	p.mu.Lock()
	p.ensureCalls++
	onEnsure := p.onEnsure
	p.mu.Unlock()
	if onEnsure != nil {
		onEnsure()
	}
	return ctrl.Result{}, p.ensureErr
}

func (p *fenceTestProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	return &snapshot.Status{Phase: snapshot.PhaseInProgress, Reason: "Snapshotting", Message: "checkpointing"}, nil
}

func (p *fenceTestProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cleanupCalls++
	return nil
}

func (p *fenceTestProvider) CheckpointState(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (snapshot.CheckpointState, error) {
	return p.state, p.stateErr
}

// plainProvider hides the CheckpointReporter capability.
type plainProvider struct{ p *fenceTestProvider }

func (w plainProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	return w.p.EnsureTrigger(ctx, job, podName)
}

func (w plainProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	return w.p.CheckStatus(ctx, job, podName)
}

func (w plainProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	return w.p.Cleanup(ctx, job, podName)
}

func fenceTestScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = pmv1alpha1.AddToScheme(s)
	return s
}

// fenceTestSource returns a running, Ready source pod owned by a ReplicaSet.
func fenceTestSource(containerID string, restarts int32) *corev1.Pod {
	isController := true
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fenceTestPod,
			Namespace: fenceTestNS,
			UID:       fenceTestUID,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web", UID: "rs-uid", Controller: &isController,
			}},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "app"}}},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", ContainerID: containerID, RestartCount: restarts, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

func fenceTestSnapshottingJob() *pmv1alpha1.PodMigrationJob {
	now := metav1.Now()
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: fenceTestJob, Namespace: fenceTestNS, UID: "job-uid", CreationTimestamp: now},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: fenceTestPod},
			TargetPodUID: fenceTestUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshottingStartTime: &now,
		},
	}
}

// fenceTestFailedJob returns a Failed job that fenced its source, failed ago,
// and recorded the baseline when baseline is true.
func fenceTestFailedJob(baseline bool, failedAgo time.Duration) *pmv1alpha1.PodMigrationJob {
	job := fenceTestSnapshottingJob()
	failedAt := metav1.NewTime(time.Now().Add(-failedAgo))
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
	job.Status.CompletionTime = &failedAt
	job.Status.Conditions = []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionFalse, Reason: "SnapshotFailed", LastTransitionTime: failedAt},
		{Type: ConditionSourceFenced, Status: metav1.ConditionTrue, Reason: util.ReasonFenced, LastTransitionTime: metav1.NewTime(failedAt.Add(-time.Minute))},
	}
	if baseline {
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}
	}
	return job
}

// fencedPod marks pod the way a fence leaves it.
func fencedPod(pod *corev1.Pod) *corev1.Pod {
	pod.Labels = map[string]string{util.LabelFenced: "true"}
	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: util.ReasonPodMigrationFenced}}
	return pod
}

// writeLog records the writes the reconciler makes, in order.
type writeLog struct {
	mu     sync.Mutex
	writes []string
}

func (l *writeLog) add(w string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.writes = append(l.writes, w)
}

func (l *writeLog) list() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.writes...)
}

func kindOf(obj client.Object) string {
	switch obj.(type) {
	case *corev1.Pod:
		return "pod"
	case *pmv1alpha1.PodMigrationJob:
		return "job"
	}
	return "other"
}

type fenceFixture struct {
	client   client.WithWatch
	r        *PodMigrationJobReconciler
	provider *fenceTestProvider
	recorder *record.FakeRecorder
	writes   *writeLog
}

func newFenceFixture(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *fenceFixture {
	t.Helper()
	f := &fenceFixture{provider: &fenceTestProvider{}, recorder: record.NewFakeRecorder(100), writes: &writeLog{}}
	patch := funcs.Patch
	funcs.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
		f.writes.add(kindOf(obj) + " patch")
		if patch != nil {
			return patch(ctx, c, obj, p, opts...)
		}
		return c.Patch(ctx, obj, p, opts...)
	}
	subPatch := funcs.SubResourcePatch
	funcs.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
		f.writes.add(kindOf(obj) + " " + sub)
		if subPatch != nil {
			return subPatch(ctx, c, sub, obj, p, opts...)
		}
		return c.SubResource(sub).Patch(ctx, obj, p, opts...)
	}
	subCreate := funcs.SubResourceCreate
	funcs.SubResourceCreate = func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
		f.writes.add(kindOf(obj) + " " + sub)
		if subCreate != nil {
			return subCreate(ctx, c, sub, obj, subObj, opts...)
		}
		return c.SubResource(sub).Create(ctx, obj, subObj, opts...)
	}
	f.client = fake.NewClientBuilder().
		WithScheme(fenceTestScheme()).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, PMJSnapshotRefIndex, PMJSnapshotRefIndexValue).
		WithObjects(objs...).
		WithInterceptorFuncs(funcs).
		Build()
	f.r = &PodMigrationJobReconciler{
		Client:           f.client,
		Scheme:           fenceTestScheme(),
		SnapshotProvider: f.provider,
		Recorder:         f.recorder,
	}
	return f
}

func (f *fenceFixture) reconcile(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	return f.r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}})
}

func (f *fenceFixture) job(t *testing.T) *pmv1alpha1.PodMigrationJob {
	t.Helper()
	job := &pmv1alpha1.PodMigrationJob{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, job); err != nil {
		t.Fatalf("getting job: %v", err)
	}
	return job
}

// pod returns the source pod, or nil once it's gone.
func (f *fenceFixture) pod(t *testing.T) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestPod}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		t.Fatalf("getting pod: %v", err)
	}
	return pod
}

func (f *fenceFixture) events() []string {
	var events []string
	for {
		select {
		case e := <-f.recorder.Events:
			events = append(events, e)
		default:
			return events
		}
	}
}

func assertFenced(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if pod == nil {
		t.Fatal("source pod is gone, want it fenced")
	}
	if pod.Labels[util.LabelFenced] != "true" {
		t.Errorf("source labels = %v, want %s=true", pod.Labels, util.LabelFenced)
	}
	if !readyHeldByFence(pod) {
		t.Errorf("source conditions = %+v, want Ready=False/%s", pod.Status.Conditions, util.ReasonPodMigrationFenced)
	}
}

func fenceCondition(t *testing.T, job *pmv1alpha1.PodMigrationJob) *metav1.Condition {
	t.Helper()
	cond := meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced)
	if cond == nil {
		t.Fatalf("job has no %s condition: %+v", ConditionSourceFenced, job.Status.Conditions)
	}
	return cond
}

func hasEvent(events []string, reason string) bool {
	for _, e := range events {
		if strings.Contains(e, " "+reason+" ") {
			return true
		}
	}
	return false
}

// TestFenceSource_FencesBeforeTheTrigger: the source is labeled and held
// NotReady, and the job records SourceFenced and the baseline, before the
// trigger is created; SourceFenced is written before the label (I11).
func TestFenceSource_FencesBeforeTheTrigger(t *testing.T) {
	f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 2), fenceTestSnapshottingJob())
	f.provider.onEnsure = func() {
		assertFenced(t, f.pod(t))
		job := f.job(t)
		if cond := fenceCondition(t, job); cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenced {
			t.Errorf("at trigger time SourceFenced = %s/%s, want True/%s", cond.Status, cond.Reason, util.ReasonFenced)
		}
		baseline, ok := jobFenceBaseline(job)
		if !ok || baseline.Containers["app"] != (fencedContainer{ID: fenceTestCID, Restarts: 2}) {
			t.Errorf("at trigger time baseline = %+v (recorded %t), want app=%s with 2 restarts", baseline, ok, fenceTestCID)
		}
	}
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if f.provider.ensureCalls != 1 {
		t.Fatalf("EnsureTrigger calls = %d, want 1", f.provider.ensureCalls)
	}
	want := []string{"job patch", "job status", "pod patch", "pod status", "job patch"}
	if got := f.writes.list(); len(got) < len(want) || strings.Join(got[:len(want)], ",") != strings.Join(want, ",") {
		t.Errorf("writes = %v, want them to start with %v", got, want)
	}
	if !hasEvent(f.events(), "SourceFenced") {
		t.Error("no SourceFenced event")
	}

	// The fence is done, so the next pass writes nothing for it.
	f.writes = &writeLog{}
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	for _, w := range f.writes.list() {
		if strings.HasPrefix(w, "pod ") {
			t.Errorf("second pass wrote %q to the source; the fence should be done", w)
		}
	}
}

// TestFenceSource_NoTriggerUntilTheFenceIsInPlace: a failed fence write
// returns an error before the trigger, and the next pass finishes the fence.
func TestFenceSource_NoTriggerUntilTheFenceIsInPlace(t *testing.T) {
	failStatus := true
	f := newFenceFixture(t, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod && failStatus {
				return apierrors.NewInternalError(errors.New("fence webhook unreachable"))
			}
			return c.SubResource(sub).Patch(ctx, obj, p, opts...)
		},
	}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())

	if _, err := f.reconcile(t); err == nil {
		t.Fatal("Reconcile succeeded although the source couldn't be held NotReady")
	}
	if f.provider.ensureCalls != 0 {
		t.Fatalf("EnsureTrigger calls = %d, want 0 before the fence is in place", f.provider.ensureCalls)
	}
	if _, ok := jobFenceBaseline(f.job(t)); ok {
		t.Error("baseline recorded before the fence was in place")
	}

	failStatus = false
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	assertFenced(t, f.pod(t))
	if f.provider.ensureCalls != 1 {
		t.Errorf("EnsureTrigger calls = %d, want 1 once the fence is in place", f.provider.ensureCalls)
	}
}

// TestFenceSource_NothingToFence: a source that's gone, replaced, or finished
// isn't fenced, and the trigger step runs as before.
func TestFenceSource_NothingToFence(t *testing.T) {
	replaced := fenceTestSource(fenceTestCID, 0)
	replaced.UID = "successor-uid"
	finished := fenceTestSource(fenceTestCID, 0)
	finished.Status.Phase = corev1.PodSucceeded
	for name, objs := range map[string][]client.Object{
		"gone":     nil,
		"replaced": {replaced},
		"finished": {finished},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFenceFixture(t, interceptor.Funcs{}, append(objs, fenceTestSnapshottingJob())...)
			if _, err := f.reconcile(t); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			if f.provider.ensureCalls != 1 {
				t.Errorf("EnsureTrigger calls = %d, want 1", f.provider.ensureCalls)
			}
			for _, w := range f.writes.list() {
				if strings.HasPrefix(w, "pod ") {
					t.Errorf("wrote %q to a pod that isn't the source", w)
				}
			}
			if meta.FindStatusCondition(f.job(t).Status.Conditions, ConditionSourceFenced) != nil {
				t.Error("SourceFenced recorded without a source to fence")
			}
		})
	}
}

// TestTriggerRejected_FailsAndUnfences: a trigger the API server rejects fails
// the job at once, and the source, never checkpointed, goes back into service.
func TestTriggerRejected_FailsAndUnfences(t *testing.T) {
	for name, rejection := range map[string]error{
		"forbidden": apierrors.NewForbidden(schema.GroupResource{Group: "podsnapshot.gke.io", Resource: "podsnapshotmanualtriggers"}, "t", errors.New("denied by policy")),
		"invalid":   apierrors.NewInvalid(schema.GroupKind{Group: "podsnapshot.gke.io", Kind: "PodSnapshotManualTrigger"}, "t", nil),
	} {
		t.Run(name, func(t *testing.T) {
			f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
			f.provider.ensureErr = rejection
			f.provider.state = snapshot.CheckpointState{Settled: true}
			if _, err := f.reconcile(t); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			job := f.job(t)
			if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
				t.Fatalf("phase = %s, want Failed", job.Status.Phase)
			}
			if ready := meta.FindStatusCondition(job.Status.Conditions, "Ready"); ready == nil || ready.Reason != ReasonTriggerCreateFailed {
				t.Errorf("Ready = %+v, want reason %s", ready, ReasonTriggerCreateFailed)
			}

			// The Failed job resolves the fence.
			if _, err := f.reconcile(t); err != nil {
				t.Fatalf("resolving Reconcile: %v", err)
			}
			if pod := f.pod(t); pod == nil || pod.Labels[util.LabelFenced] != "" {
				t.Fatalf("source = %+v, want it unfenced", pod)
			}
			if cond := fenceCondition(t, f.job(t)); cond.Status != metav1.ConditionFalse || cond.Reason != util.ReasonUnfenced {
				t.Errorf("SourceFenced = %s/%s, want False/%s", cond.Status, cond.Reason, util.ReasonUnfenced)
			}
			if !hasEvent(f.events(), "SourceUnfenced") {
				t.Error("no SourceUnfenced event")
			}
		})
	}
}

// TestTriggerTransientError_CappedWhileFenced: transient trigger errors are
// retried while the source is fenced, until the pre-checkpoint cap.
func TestTriggerTransientError_CappedWhileFenced(t *testing.T) {
	f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
	f.provider.ensureErr = apierrors.NewServiceUnavailable("apiserver busy")
	if _, err := f.reconcile(t); err == nil {
		t.Fatal("Reconcile returned no error for a transient trigger error")
	}
	if phase := f.job(t).Status.Phase; phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s after one transient error, want Snapshotting", phase)
	}

	// Backdate the fence past the cap.
	job := f.job(t)
	orig := job.DeepCopy()
	cond := fenceCondition(t, job)
	cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-DefaultFencePreCheckpointCap - time.Second))
	if err := f.client.Status().Patch(context.Background(), job, client.MergeFrom(orig)); err != nil {
		t.Fatalf("backdating SourceFenced: %v", err)
	}
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile past the cap: %v", err)
	}
	job = f.job(t)
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s past the cap, want Failed", job.Status.Phase)
	}
	if ready := meta.FindStatusCondition(job.Status.Conditions, "Ready"); ready == nil || ready.Reason != ReasonFenceCapExceeded {
		t.Errorf("Ready = %+v, want reason %s", ready, ReasonFenceCapExceeded)
	}
	if f.provider.cleanupCalls == 0 {
		t.Error("the trigger wasn't deleted when the cap failed the job")
	}
}

// TestFenceSource_StatusPatchFailure_CappedWhileFenced: a failing pods/status
// patch (e.g. mpodfence webhook unreachable) is retried while the source is
// fenced, until the pre-checkpoint cap expires. The job then fails with
// FenceCapExceeded and unfences the source pod (removing the label).
func TestFenceSource_StatusPatchFailure_CappedWhileFenced(t *testing.T) {
	f := newFenceFixture(t, interceptor.Funcs{
		SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, p client.Patch, opts ...client.SubResourcePatchOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				return apierrors.NewInternalError(errors.New("fence webhook unreachable"))
			}
			return c.SubResource(sub).Patch(ctx, obj, p, opts...)
		},
	}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
	f.provider.state = snapshot.CheckpointState{Settled: true}

	if _, err := f.reconcile(t); err == nil {
		t.Fatal("Reconcile returned no error while pods/status patch failed")
	}
	if phase := f.job(t).Status.Phase; phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s after failed status patch, want Snapshotting", phase)
	}
	if f.provider.ensureCalls != 0 {
		t.Fatalf("EnsureTrigger calls = %d, want 0 before fence is in place", f.provider.ensureCalls)
	}
	// The label was set before the status patch failed.
	if f.pod(t).Labels[util.LabelFenced] != "true" {
		t.Fatalf("source pod not labeled %s before status patch failure", util.LabelFenced)
	}

	// Backdate the fence past the cap.
	job := f.job(t)
	orig := job.DeepCopy()
	cond := fenceCondition(t, job)
	cond.LastTransitionTime = metav1.NewTime(time.Now().Add(-DefaultFencePreCheckpointCap - time.Second))
	if err := f.client.Status().Patch(context.Background(), job, client.MergeFrom(orig)); err != nil {
		t.Fatalf("backdating SourceFenced: %v", err)
	}

	// Next reconcile: cap has elapsed, so the job fails.
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile past the cap: %v", err)
	}
	job = f.job(t)
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
		t.Fatalf("phase = %s past the cap, want Failed", job.Status.Phase)
	}
	if ready := meta.FindStatusCondition(job.Status.Conditions, "Ready"); ready == nil || ready.Reason != ReasonFenceCapExceeded {
		t.Errorf("Ready = %+v, want reason %s", ready, ReasonFenceCapExceeded)
	}

	// The Failed job resolves the fence and removes the label.
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("resolving fence: %v", err)
	}
	if pod := f.pod(t); pod == nil || pod.Labels[util.LabelFenced] != "" {
		t.Fatalf("source = %+v, want fenced label removed", pod)
	}
	if cond := fenceCondition(t, f.job(t)); cond.Status != metav1.ConditionFalse || cond.Reason != util.ReasonUnfenced {
		t.Errorf("SourceFenced = %s/%s, want False/%s", cond.Status, cond.Reason, util.ReasonUnfenced)
	}
	if !hasEvent(f.events(), "SourceUnfenced") {
		t.Error("no SourceUnfenced event")
	}
}

// TestResolveSourceFence covers the unfence rules after a failure.
func TestResolveSourceFence(t *testing.T) {
	changedID := fencedPod(fenceTestSource("containerd://bbb", 0))
	restarted := fencedPod(fenceTestSource(fenceTestCID, 1))
	stopped := fencedPod(fenceTestSource(fenceTestCID, 0))
	stopped.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}

	tests := map[string]struct {
		job          *pmv1alpha1.PodMigrationJob
		pod          *corev1.Pod
		state        snapshot.CheckpointState
		stateErr     error
		noReporter   bool
		wantStatus   metav1.ConditionStatus
		wantReason   string
		wantEvicted  bool
		wantHandled  bool
		wantRequeue  time.Duration
		wantUnchaged bool
	}{
		"fence never finished: unfence at once": {
			job: fenceTestFailedJob(false, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionFalse, wantReason: util.ReasonUnfenced,
		},
		"upgrade in flight without baseline, checkpoint completed: keep and evict": {
			job: fenceTestFailedJob(false, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Settled: true, Completed: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"checkpoint state unreadable within settle window: retry": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			stateErr:   errors.New("transient engine failure"),
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenced, wantHandled: true, wantRequeue: fenceSettleInterval,
		},
		"checkpoint state unreadable past settle window: keep and evict": {
			job: fenceTestFailedJob(true, fenceSettleTimeout+time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			stateErr:   errors.New("persistent engine failure"),
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"checkpoint settled without completing, containers unchanged: unfence and watch": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionFalse, wantReason: util.ReasonUnfenced, wantHandled: true, wantRequeue: lateStopPollInterval,
		},
		"checkpoint completed: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Settled: true, Completed: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"checkpoint completed while the upload still runs: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Completed: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"checkpoint still running: wait": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenced, wantHandled: true, wantRequeue: fenceSettleInterval,
		},
		"checkpoint still running past the settle timeout: keep and evict": {
			job: fenceTestFailedJob(true, fenceSettleTimeout+time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"container changed: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: changedID,
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"container restarted: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: restarted,
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"container stopped: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: stopped,
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"an engine that can't report its checkpoint: keep and evict": {
			job: fenceTestFailedJob(true, time.Second), pod: fencedPod(fenceTestSource(fenceTestCID, 0)),
			noReporter: true,
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"the job reached Evicting: keep and evict": {
			job: func() *pmv1alpha1.PodMigrationJob {
				job := fenceTestFailedJob(true, time.Second)
				now := metav1.Now()
				job.Status.EvictingStartTime = &now
				return job
			}(),
			pod:        fencedPod(fenceTestSource(fenceTestCID, 0)),
			state:      snapshot.CheckpointState{Settled: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept, wantEvicted: true, wantHandled: true,
		},
		"a pod without a controller stays fenced and isn't evicted": {
			job: fenceTestFailedJob(true, time.Second),
			pod: func() *corev1.Pod {
				pod := fencedPod(fenceTestSource(fenceTestCID, 0))
				pod.OwnerReferences = nil
				return pod
			}(),
			state:      snapshot.CheckpointState{Settled: true, Completed: true},
			wantStatus: metav1.ConditionTrue, wantReason: util.ReasonFenceKept,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			f := newFenceFixture(t, interceptor.Funcs{}, tc.pod, tc.job)
			f.provider.state = tc.state
			f.provider.stateErr = tc.stateErr
			if tc.noReporter {
				f.r.SnapshotProvider = plainProvider{f.provider}
			}
			job := f.job(t)
			res, handled, err := f.r.resolveSourceFence(context.Background(), job, job.DeepCopy())
			if err != nil {
				t.Fatalf("resolveSourceFence: %v", err)
			}
			if handled != tc.wantHandled {
				t.Errorf("handled = %t, want %t", handled, tc.wantHandled)
			}
			if tc.wantRequeue != 0 && res.RequeueAfter != tc.wantRequeue {
				t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, tc.wantRequeue)
			}
			cond := fenceCondition(t, f.job(t))
			if cond.Status != tc.wantStatus || cond.Reason != tc.wantReason {
				t.Errorf("SourceFenced = %s/%s (%s), want %s/%s", cond.Status, cond.Reason, cond.Message, tc.wantStatus, tc.wantReason)
			}
			pod := f.pod(t)
			switch {
			case tc.wantEvicted && pod != nil:
				t.Error("source wasn't evicted")
			case !tc.wantEvicted && pod == nil:
				t.Error("source was evicted")
			case pod != nil && tc.wantReason == util.ReasonUnfenced && pod.Labels[util.LabelFenced] != "":
				t.Errorf("unfenced source still labeled: %v", pod.Labels)
			case pod != nil && tc.wantReason != util.ReasonUnfenced:
				assertFenced(t, pod)
			}
			events := f.events()
			switch tc.wantReason {
			case util.ReasonUnfenced:
				if !hasEvent(events, "SourceUnfenced") {
					t.Errorf("events = %v, want SourceUnfenced", events)
				}
			case util.ReasonFenceKept:
				if !hasEvent(events, "SourceKeptFenced") {
					t.Errorf("events = %v, want SourceKeptFenced", events)
				}
			}
		})
	}
}

// TestResolveSourceFence_UnfenceRecordsTheJobFirst: SourceFenced=False is
// written before the label is removed, so a job never claims a fence that's
// gone (I11).
func TestResolveSourceFence_UnfenceRecordsTheJobFirst(t *testing.T) {
	f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), fenceTestFailedJob(true, time.Second))
	f.provider.state = snapshot.CheckpointState{Settled: true}
	job := f.job(t)
	if _, _, err := f.r.resolveSourceFence(context.Background(), job, job.DeepCopy()); err != nil {
		t.Fatalf("resolveSourceFence: %v", err)
	}
	if got, want := strings.Join(f.writes.list(), ","), "job status,pod patch"; got != want {
		t.Errorf("writes = %s, want %s", got, want)
	}
}

// TestResolveSourceFence_NothingLeftToDo: a source that's gone or
// terminating, a Succeeded job, or a job that never fenced needs no work.
func TestResolveSourceFence_NothingLeftToDo(t *testing.T) {
	terminating := fencedPod(fenceTestSource(fenceTestCID, 0))
	now := metav1.Now()
	terminating.DeletionTimestamp = &now
	terminating.Finalizers = []string{"test/keep"}
	succeeded := fenceTestFailedJob(true, time.Second)
	succeeded.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceeded
	neverFenced := fenceTestFailedJob(true, time.Second)
	neverFenced.Status.Conditions = neverFenced.Status.Conditions[:1]

	for name, objs := range map[string][]client.Object{
		"source gone":        {fenceTestFailedJob(true, time.Second)},
		"source terminating": {terminating, fenceTestFailedJob(true, time.Second)},
		"job succeeded":      {fencedPod(fenceTestSource(fenceTestCID, 0)), succeeded},
		"job never fenced":   {fencedPod(fenceTestSource(fenceTestCID, 0)), neverFenced},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFenceFixture(t, interceptor.Funcs{}, objs...)
			job := f.job(t)
			_, handled, err := f.r.resolveSourceFence(context.Background(), job, job.DeepCopy())
			if err != nil || handled {
				t.Errorf("resolveSourceFence = (handled %t, %v), want (false, nil)", handled, err)
			}
			if w := f.writes.list(); len(w) != 0 {
				t.Errorf("writes = %v, want none", w)
			}
		})
	}
}

// TestResolveSourceFence_EvictionRefusedRetriesAndDefersGC: a kept source
// whose eviction the budget refuses is retried, and the job isn't garbage
// collected meanwhile.
func TestResolveSourceFence_EvictionRefusedRetriesAndDefersGC(t *testing.T) {
	job := fenceTestFailedJob(true, time.Hour)
	job.Status.Conditions[1].Reason = util.ReasonFenceKept
	f := newFenceFixture(t, interceptor.Funcs{
		SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
			return apierrors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
		},
	}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)

	res, err := f.reconcile(t)
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if res.RequeueAfter != keptSourceEvictionRetryInterval {
		t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, keptSourceEvictionRetryInterval)
	}
	f.job(t) // still there: the 30-minute GC TTL has passed, but the fence isn't settled
	assertFenced(t, f.pod(t))
}

// TestLateStopWatch covers the watch that follows an unfence.
func TestLateStopWatch(t *testing.T) {
	unfencedJob := func(unfencedAgo time.Duration) *pmv1alpha1.PodMigrationJob {
		job := fenceTestFailedJob(true, unfencedAgo)
		job.Status.Conditions[1] = metav1.Condition{
			Type: ConditionSourceFenced, Status: metav1.ConditionFalse, Reason: util.ReasonUnfenced,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-unfencedAgo)),
		}
		return job
	}

	t.Run("unchanged source: keep watching", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), unfencedJob(time.Second))
		res, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.RequeueAfter != lateStopPollInterval {
			t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, lateStopPollInterval)
		}
		if w := f.writes.list(); len(w) != 0 {
			t.Errorf("writes = %v, want none", w)
		}
	})

	t.Run("source stopped: fence again and evict", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource("containerd://restarted", 0), unfencedJob(time.Second))
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		cond := fenceCondition(t, f.job(t))
		if cond.Status != metav1.ConditionTrue || cond.Reason != util.ReasonFenceKept {
			t.Errorf("SourceFenced = %s/%s, want True/%s", cond.Status, cond.Reason, util.ReasonFenceKept)
		}
		if got, want := strings.Join(f.writes.list(), ","), "job status,pod patch,pod status,pod eviction"; got != want {
			t.Errorf("writes = %s, want %s (fence again, then evict)", got, want)
		}
		if f.pod(t) != nil {
			t.Error("source wasn't evicted")
		}
		if !hasEvent(f.events(), "SourceRefencedLateStop") {
			t.Error("no SourceRefencedLateStop event")
		}
	})

	t.Run("after the window: done", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource("containerd://restarted", 0), unfencedJob(DefaultFenceLateStopWatch+time.Second))
		job := f.job(t)
		_, handled, err := f.r.resolveSourceFence(context.Background(), job, job.DeepCopy())
		if err != nil || handled {
			t.Errorf("resolveSourceFence = (handled %t, %v), want (false, nil)", handled, err)
		}
		if f.pod(t) == nil {
			t.Error("source evicted after the watch ended")
		}
	})

	t.Run("an unfence interrupted before the label was removed finishes", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), unfencedJob(time.Second))
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if pod := f.pod(t); pod == nil || pod.Labels[util.LabelFenced] != "" {
			t.Errorf("source = %+v, want the label removed", pod)
		}
	})
}

// TestFenceBaselineUnchanged covers the comparison against the baseline.
func TestFenceBaselineUnchanged(t *testing.T) {
	baseline := baselineOf(fenceTestSource(fenceTestCID, 3))
	pending := fenceTestSource(fenceTestCID, 3)
	pending.Status.Phase = corev1.PodPending
	noStatus := fenceTestSource(fenceTestCID, 3)
	noStatus.Status.ContainerStatuses = nil
	for name, tc := range map[string]struct {
		baseline fenceBaseline
		pod      *corev1.Pod
		want     bool
	}{
		"same containers":          {baseline, fenceTestSource(fenceTestCID, 3), true},
		"new container ID":         {baseline, fenceTestSource("containerd://bbb", 3), false},
		"restarted":                {baseline, fenceTestSource(fenceTestCID, 4), false},
		"not running":              {baseline, pending, false},
		"container status missing": {baseline, noStatus, false},
		"empty baseline":           {fenceBaseline{}, fenceTestSource(fenceTestCID, 3), false},
	} {
		t.Run(name, func(t *testing.T) {
			if got, diff := tc.baseline.unchanged(tc.pod); got != tc.want {
				t.Errorf("unchanged = %t (%s), want %t", got, diff, tc.want)
			}
		})
	}

	// A baseline that doesn't parse counts as recorded, and matches nothing.
	job := fenceTestFailedJob(false, 0)
	job.Annotations = map[string]string{util.AnnotationFenceBaseline: "{not json"}
	b, recorded := jobFenceBaseline(job)
	if ok, _ := b.unchanged(fenceTestSource(fenceTestCID, 0)); !recorded || ok {
		t.Errorf("unparseable baseline: recorded %t, matches %t; want recorded, no match", recorded, ok)
	}
}

// TestFenceSource_SatisfiesI11: I11 (SourceFencedAfterCheckpoint) holds for a
// source the controller fenced, and fires once the label is gone, so the rule
// and the fence agree on the names they share.
func TestFenceSource_SatisfiesI11(t *testing.T) {
	f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	job, pod := f.job(t), f.pod(t)
	// Evaluate past I11's grace window.
	snapshotAt := func(p *corev1.Pod) *invariants.ReconcileSnapshot {
		return &invariants.ReconcileSnapshot{
			Now:           time.Now().Add(invariants.SourceFencedGraceWindow + time.Second),
			PrimaryPMJ:    job,
			NamespacePMJs: []pmv1alpha1.PodMigrationJob{*job},
			NamespacePods: []corev1.Pod{*p},
		}
	}
	if vs := invariants.EvaluateI11(snapshotAt(pod)); len(vs) != 0 {
		t.Fatalf("I11 violations on a fenced source: %+v", vs)
	}
	unlabeled := pod.DeepCopy()
	delete(unlabeled.Labels, util.LabelFenced)
	if vs := invariants.EvaluateI11(snapshotAt(unlabeled)); len(vs) == 0 {
		t.Fatal("I11 didn't fire for a fenced job whose source lost the label")
	}
}

// TestFenceSource_FinalizerLifecycle covers adding SourceFenceFinalizer before
// fencing and removing it when transitioning past eviction or settling (#167).
func TestFenceSource_FinalizerLifecycle(t *testing.T) {
	t.Run("finalizer added before trigger", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
		f.provider.onEnsure = func() {
			job := f.job(t)
			if !controllerutil.ContainsFinalizer(job, SourceFenceFinalizer) {
				t.Errorf("at trigger time job missing finalizer %s: %v", SourceFenceFinalizer, job.Finalizers)
			}
		}
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if f.provider.ensureCalls != 1 {
			t.Fatalf("EnsureTrigger calls = %d, want 1", f.provider.ensureCalls)
		}
		job := f.job(t)
		if !controllerutil.ContainsFinalizer(job, SourceFenceFinalizer) {
			t.Errorf("job missing finalizer %s: %v", SourceFenceFinalizer, job.Finalizers)
		}
	})

	t.Run("finalizer retained across multiple active reconciles", func(t *testing.T) {
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), fenceTestSnapshottingJob())
		// First reconcile: adds finalizer, fences pod, sets baseline.
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("first Reconcile: %v", err)
		}
		if !controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Fatalf("finalizer missing after first reconcile")
		}
		// Second reconcile while still active Snapshotting: finalizer must NOT be stripped!
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("second Reconcile: %v", err)
		}
		if !controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Fatalf("finalizer was stripped on second active reconcile")
		}
	})

	t.Run("pre-upgrade job with baseline acquires finalizer", func(t *testing.T) {
		job := fenceTestSnapshottingJob()
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}
		// No finalizer yet (simulating pre-upgrade)
		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if !controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Errorf("pre-upgrade job did not acquire finalizer %s", SourceFenceFinalizer)
		}
	})

	t.Run("finalizer removed when settling fence on failure without watch", func(t *testing.T) {
		job := fenceTestFailedJob(false, time.Second)
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		f.provider.state = snapshot.CheckpointState{Settled: true}
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Errorf("finalizer %s was not removed after fence settled", SourceFenceFinalizer)
		}
	})

	t.Run("finalizer removed after late-stop watch expires", func(t *testing.T) {
		job := fenceTestFailedJob(true, DefaultFenceLateStopWatch+time.Second)
		job.Status.Conditions[1] = metav1.Condition{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionFalse,
			Reason:             util.ReasonUnfenced,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-DefaultFenceLateStopWatch - time.Second)),
		}
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), job)
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Errorf("finalizer %s was not removed after late-stop watch expired", SourceFenceFinalizer)
		}
	})

	t.Run("finalizer retained during Evicting and removed at Restoring transition", func(t *testing.T) {
		job := fenceTestSnapshottingJob()
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		// Pod eviction refused by PDB: job stays in Evicting across reconciles
		f := newFenceFixture(t, interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
				return apierrors.NewTooManyRequests("PDB refused eviction", 0)
			},
		}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)

		// First reconcile in Evicting: finalizer retained
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("first Evicting reconcile: %v", err)
		}
		if !controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Errorf("finalizer removed on first Evicting reconcile")
		}

		// Second reconcile in Evicting: finalizer still retained (IsTerminalPhase guard)
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("second Evicting reconcile: %v", err)
		}
		if !controllerutil.ContainsFinalizer(f.job(t), SourceFenceFinalizer) {
			t.Errorf("finalizer removed on second Evicting reconcile")
		}

		// Source pod is evicted (gone). Next reconcile transitions to Restoring and removes finalizer.
		if err := f.client.Delete(context.Background(), fencedPod(fenceTestSource(fenceTestCID, 0))); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("deleting source pod: %v", err)
		}
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("reconcile after source pod evicted: %v", err)
		}
		j := f.job(t)
		if j.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase = %s, want Restoring", j.Status.Phase)
		}
		if controllerutil.ContainsFinalizer(j, SourceFenceFinalizer) {
			t.Errorf("finalizer %s was not removed upon transition to Restoring", SourceFenceFinalizer)
		}
	})

	t.Run("job deleted during late-stop watch exits watch and removes finalizer", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestFailedJob(true, time.Minute)
		job.Status.Conditions[1] = metav1.Condition{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionFalse,
			Reason:             util.ReasonUnfenced,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-time.Minute)),
		}
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now

		f := newFenceFixture(t, interceptor.Funcs{}, fenceTestSource(fenceTestCID, 0), job)
		res, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.RequeueAfter != 0 {
			t.Errorf("RequeueAfter = %v, want 0 (watch should exit immediately on delete)", res.RequeueAfter)
		}
		deletedJob := &pmv1alpha1.PodMigrationJob{}
		if err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob); err == nil {
			if controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
				t.Errorf("finalizer %s was not removed on delete during watch", SourceFenceFinalizer)
			}
		}
	})
}

// TestDeleteActiveJob_PreCheckpoint verifies that deleting an active job before
// its checkpoint completes releases the source pod and removes the finalizer (#167).
func TestDeleteActiveJob_PreCheckpoint(t *testing.T) {
	t.Run("deleted before checkpoint was triggered", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: now,
		}}

		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		f.provider.state = snapshot.CheckpointState{Settled: true}

		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		// Source pod must be unfenced.
		pod := f.pod(t)
		if pod == nil {
			t.Fatal("source pod was deleted, want it unfenced and preserved")
		}
		if pod.Labels[util.LabelFenced] != "" {
			t.Errorf("source pod labels = %v, want %s removed", pod.Labels, util.LabelFenced)
		}

		// Finalizer must be removed from the job.
		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("job still contains %s finalizer: %v", SourceFenceFinalizer, deletedJob.Finalizers)
		}

		if !hasEvent(f.events(), "SourceUnfenced") {
			t.Error("expected SourceUnfenced event")
		}
	})

	t.Run("deleted after trigger created but checkpoint never completed", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: now,
		}}
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}

		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		// Checkpoint provider reports trigger settled without completing.
		f.provider.state = snapshot.CheckpointState{Settled: true, Completed: false}

		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		pod := f.pod(t)
		if pod == nil {
			t.Fatal("source pod was deleted, want it unfenced")
		}
		if pod.Labels[util.LabelFenced] != "" {
			t.Errorf("source pod labels = %v, want %s removed", pod.Labels, util.LabelFenced)
		}

		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("job still contains %s finalizer: %v", SourceFenceFinalizer, deletedJob.Finalizers)
		}
		if !hasEvent(f.events(), "SourceUnfenced") {
			t.Error("expected SourceUnfenced event")
		}
	})

	t.Run("deleted mid-checkpoint while trigger in-flight", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: now,
		}}
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}

		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		// Checkpoint provider: not settled yet, in flight.
		f.provider.state = snapshot.CheckpointState{Settled: false, Completed: false}

		res, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		// While within fenceSettleTimeout, must requeue to wait for checkpoint to settle.
		if res.RequeueAfter != fenceSettleInterval {
			t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, fenceSettleInterval)
		}
		if f.provider.cleanupCalls == 0 {
			t.Error("expected Cleanup to be called on deletion")
		}
		// Phase must be transitioned to Failed with reason Deleted so eviction webhook allows cold eviction if needed.
		updatedJob := f.job(t)
		if updatedJob.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Errorf("phase = %s, want Failed", updatedJob.Status.Phase)
		}
		if ready := meta.FindStatusCondition(updatedJob.Status.Conditions, "Ready"); ready == nil || ready.Reason != "Deleted" {
			t.Errorf("Ready condition = %+v, want reason Deleted", ready)
		}
		// Now simulate checkpoint settled without completing:
		f.provider.state = snapshot.CheckpointState{Settled: true, Completed: false}
		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("second Reconcile: %v", err)
		}
		// Pod is unfenced, finalizer removed.
		pod := f.pod(t)
		if pod == nil || pod.Labels[util.LabelFenced] != "" {
			t.Fatalf("pod = %+v, want unfenced", pod)
		}
		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err = f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("finalizer %s was not removed", SourceFenceFinalizer)
		}
	})

	t.Run("controller down at delete: active snapshotting job deleted 3m ago waits for settle window before unwedge", func(t *testing.T) {
		// DeletionTimestamp is 3 minutes in the past, but Phase is Snapshotting and CompletionTime is nil
		// (simulating controller being down/upgrading for 3 minutes after kubectl delete).
		deletedAt := metav1.NewTime(time.Now().Add(-3 * time.Minute))
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &deletedAt
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: deletedAt,
		}}
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}

		// Checkpoint is not yet settled.
		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		f.provider.state = snapshot.CheckpointState{Settled: false, Completed: false}

		res, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("first Reconcile: %v", err)
		}

		// First reconcile: should transition to Failed (reason Deleted), set CompletionTime to ~now,
		// and requeue for fence settle (fenceSettleInterval).
		updatedJob := f.job(t)
		if updatedJob.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Errorf("phase = %s, want Failed", updatedJob.Status.Phase)
		}
		if ready := meta.FindStatusCondition(updatedJob.Status.Conditions, "Ready"); ready == nil || ready.Reason != "Deleted" {
			t.Errorf("Ready condition = %+v, want Reason=Deleted", ready)
		}
		if updatedJob.Status.CompletionTime == nil {
			t.Fatal("CompletionTime was not set on first reconcile")
		}
		if res.RequeueAfter != fenceSettleInterval {
			t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, fenceSettleInterval)
		}

		// MUST retain SourceFenceFinalizer and NOT fire SourceFenceDeleteTimedOut!
		if !controllerutil.ContainsFinalizer(updatedJob, SourceFenceFinalizer) {
			t.Errorf("finalizer %s was prematurely removed on first reconcile", SourceFenceFinalizer)
		}
		if hasEvent(f.events(), "SourceFenceDeleteTimedOut") {
			t.Error("unexpected SourceFenceDeleteTimedOut warning event on first reconcile")
		}
	})
}

// TestDeleteActiveJob_PostCheckpoint verifies that deleting an active job after
// its checkpoint completed keeps the source fenced, evicts it, and removes the finalizer (#167).
func TestDeleteActiveJob_PostCheckpoint(t *testing.T) {
	t.Run("checkpoint completed: source kept fenced and evicted", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: now,
		}}
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}

		f := newFenceFixture(t, interceptor.Funcs{}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)
		f.provider.state = snapshot.CheckpointState{Settled: true, Completed: true}

		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		// In fake client, eviction deletes the source pod.
		if f.pod(t) != nil {
			t.Error("source pod was not evicted")
		}

		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("job still contains %s finalizer: %v", SourceFenceFinalizer, deletedJob.Finalizers)
		}
		if !hasEvent(f.events(), "SourceKeptFenced") {
			t.Error("expected SourceKeptFenced event")
		}
	})

	t.Run("source container changed: kept fenced and evicted even if checkpoint not completed", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenced,
			LastTransitionTime: now,
		}}
		data, _ := json.Marshal(baselineOf(fenceTestSource(fenceTestCID, 0)))
		job.Annotations = map[string]string{util.AnnotationFenceBaseline: string(data)}

		// Source pod has restarted container
		restartedPod := fencedPod(fenceTestSource("containerd://restarted", 1))
		f := newFenceFixture(t, interceptor.Funcs{}, restartedPod, job)
		f.provider.state = snapshot.CheckpointState{Settled: true, Completed: false}

		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		if f.pod(t) != nil {
			t.Error("restarted source pod was not evicted")
		}

		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("job still contains %s finalizer: %v", SourceFenceFinalizer, deletedJob.Finalizers)
		}
		if !hasEvent(f.events(), "SourceKeptFenced") {
			t.Error("expected SourceKeptFenced event")
		}
	})

	t.Run("eviction refused by budget retries and retains finalizer", func(t *testing.T) {
		now := metav1.Now()
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &now
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenceKept,
			LastTransitionTime: now,
		}}

		f := newFenceFixture(t, interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
				return apierrors.NewTooManyRequests("PDB refused eviction", 0)
			},
		}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)

		res, err := f.reconcile(t)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.RequeueAfter != keptSourceEvictionRetryInterval {
			t.Errorf("RequeueAfter = %v, want %v", res.RequeueAfter, keptSourceEvictionRetryInterval)
		}

		// Source must still be fenced and finalizer must still be present.
		assertFenced(t, f.pod(t))
		retainedJob := f.job(t)
		if !controllerutil.ContainsFinalizer(retainedJob, SourceFenceFinalizer) {
			t.Error("finalizer was removed while eviction was refused")
		}
	})

	t.Run("eviction blocked past unwedge deadline removes finalizer to avoid permanent wedge", func(t *testing.T) {
		// Both DeletionTimestamp and CompletionTime are 3 minutes ago (> DefaultFenceDeleteUnwedgeTimeout 2m).
		deletedAt := metav1.NewTime(time.Now().Add(-3 * time.Minute))
		job := fenceTestSnapshottingJob()
		controllerutil.AddFinalizer(job, SourceFenceFinalizer)
		job.DeletionTimestamp = &deletedAt
		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseFailed
		job.Status.CompletionTime = &deletedAt
		job.Status.Conditions = []metav1.Condition{{
			Type:               ConditionSourceFenced,
			Status:             metav1.ConditionTrue,
			Reason:             util.ReasonFenceKept,
			LastTransitionTime: deletedAt,
		}}

		f := newFenceFixture(t, interceptor.Funcs{
			SubResourceCreate: func(ctx context.Context, c client.Client, sub string, obj client.Object, subObj client.Object, opts ...client.SubResourceCreateOption) error {
				return apierrors.NewTooManyRequests("PDB refused eviction", 0)
			},
		}, fencedPod(fenceTestSource(fenceTestCID, 0)), job)

		if _, err := f.reconcile(t); err != nil {
			t.Fatalf("Reconcile: %v", err)
		}

		// Source pod stays fenced because eviction failed.
		assertFenced(t, f.pod(t))
		// But finalizer MUST be removed so deleting the PMJ doesn't wedge forever!
		deletedJob := &pmv1alpha1.PodMigrationJob{}
		err := f.client.Get(context.Background(), types.NamespacedName{Namespace: fenceTestNS, Name: fenceTestJob}, deletedJob)
		if err == nil && controllerutil.ContainsFinalizer(deletedJob, SourceFenceFinalizer) {
			t.Errorf("job still contains %s finalizer after unwedge deadline", SourceFenceFinalizer)
		}
		if !hasEvent(f.events(), "SourceFenceDeleteTimedOut") {
			t.Error("expected SourceFenceDeleteTimedOut warning event")
		}
	})
}

// TestDeleteJob_NeverFenced verifies that deleting a job that never fenced a source
// completes immediately without touching the source pod (#167).
func TestDeleteJob_NeverFenced(t *testing.T) {
	now := metav1.Now()
	job := fenceTestSnapshottingJob()
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhasePending
	job.Finalizers = []string{"other.io/cleanup"}
	job.DeletionTimestamp = &now
	// No SourceFenceFinalizer, never fenced.
	pod := fenceTestSource(fenceTestCID, 0)

	f := newFenceFixture(t, interceptor.Funcs{}, pod, job)
	if _, err := f.reconcile(t); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Source pod must remain untouched.
	p := f.pod(t)
	if p == nil {
		t.Fatal("source pod was deleted")
	}
	if p.Labels[util.LabelFenced] != "" {
		t.Errorf("source pod was fenced unexpectedly: %v", p.Labels)
	}
}
