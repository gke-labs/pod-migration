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

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

// verifyingEngine is a routedEngine whose restore verifier returns a fixed
// verdict, or a fixed error, and counts its calls.
type verifyingEngine struct {
	routedEngine
	verdict engine.RestoreVerdict
	err     error
	calls   int
}

func (e *verifyingEngine) VerifyRestore(context.Context, *pmv1alpha1.PodMigrationJob, *corev1.Pod) (engine.RestoreVerdict, error) {
	e.calls++
	return e.verdict, e.err
}

func newVerifyingEngine(verdict engine.RestoreVerdict) *verifyingEngine {
	return &verifyingEngine{
		routedEngine: routedEngine{name: "verifying", provider: &fakeSnapshotProvider{}},
		verdict:      verdict,
	}
}

// readyPodStatus is the status of a replacement that has been Ready for
// readyFor, and whose app container restarted restarts times.
func readyPodStatus(readyFor time.Duration, restarts int32) corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{{
			Type:               corev1.PodReady,
			Status:             corev1.ConditionTrue,
			LastTransitionTime: metav1.NewTime(time.Now().Add(-readyFor)),
		}},
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:         "app",
			Ready:        true,
			RestartCount: restarts,
			State:        corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		}},
	}
}

// startingPodStatus is the status of a replacement whose container is still
// being created.
func startingPodStatus() corev1.PodStatus {
	return corev1.PodStatus{
		Phase: corev1.PodPending,
		ContainerStatuses: []corev1.ContainerStatus{{
			Name:  "app",
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}},
		}},
	}
}

type restoringRun struct {
	result ctrl.Result
	err    error
	job    *pmv1alpha1.PodMigrationJob
	events []string
}

// reconcileRestoring reconciles a Restoring job whose replacement pod has
// podStatus, once, with the given engine (nil: no registry, so GKE Pod
// Snapshots) and stabilization window. restoringFor sets how long the job
// has been Restoring.
func reconcileRestoring(t *testing.T, eng engine.Engine, window, restoringFor time.Duration, pod *corev1.Pod) restoringRun {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	fixturePod, job := newRestoreCrashFixture("default", "pmj-test-pod", "test-pod", "restored-uid-1234", corev1.PodStatus{})
	if pod == nil {
		pod = fixturePod
	}
	start := metav1.NewTime(time.Now().Add(-restoringFor))
	job.Status.RestoringStartTime = &start

	c := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, job).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()
	recorder := record.NewFakeRecorder(20)
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, Recorder: recorder, RestoreStabilizationWindow: window}
	if eng != nil {
		r.Engines = registryOf(t, eng)
	}
	res, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}})

	got := &pmv1alpha1.PodMigrationJob{}
	if getErr := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pmj-test-pod"}, got); getErr != nil {
		t.Fatalf("getting the job: %v", getErr)
	}
	var events []string
	for len(recorder.Events) > 0 {
		events = append(events, <-recorder.Events)
	}
	return restoringRun{result: res, err: err, job: got, events: events}
}

// replacementPod returns the fixture's replacement pod with status.
func replacementPod(status corev1.PodStatus) *corev1.Pod {
	pod, _ := newRestoreCrashFixture("default", "pmj-test-pod", "test-pod", "restored-uid-1234", status)
	return pod
}

func verdictCount(engineName, verdict string) float64 {
	return testutil.ToFloat64(metrics.RestoreVerdictsTotal.WithLabelValues(engineName, verdict))
}

func outcomeCount(outcome string) float64 {
	return testutil.ToFloat64(metrics.OutcomesTotal.WithLabelValues(outcome))
}

func TestRestoreVerdict_ReadyReplacement(t *testing.T) {
	const window = 30 * time.Second
	confirmed := engine.RestoreVerdict{Outcome: engine.Restored, Confirmed: true, Evidence: "restore marker on the pod"}
	inferred := engine.RestoreVerdict{Outcome: engine.Restored, Evidence: "no failure seen"}
	tests := []struct {
		name         string
		verdict      engine.RestoreVerdict
		window       time.Duration
		status       corev1.PodStatus
		wantPhase    pmv1alpha1.PodMigrationJobPhase
		wantReason   string // the Restored condition's reason; "" means no Restored condition
		wantMessage  string // a substring of its message
		wantVerdict  string // the verdict counter that moves
		wantOutcome  string // the outcome counter that moves
		wantRequeue  time.Duration
		wantLostEvts bool
	}{
		{
			name:        "a confirmed restore concludes at once, inside the window",
			verdict:     confirmed,
			window:      window,
			status:      readyPodStatus(time.Second, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceeded,
			wantReason:  ReasonRestoreVerified,
			wantMessage: "engine verifying confirmed the restore: restore marker on the pod",
			wantVerdict: "confirmed",
			wantOutcome: "succeeded",
		},
		{
			name:        "an inferred restore waits for the window",
			verdict:     inferred,
			window:      window,
			status:      readyPodStatus(time.Second, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseRestoring,
			wantRequeue: window - time.Second,
		},
		{
			name:        "an inferred restore concludes after the window",
			verdict:     inferred,
			window:      window,
			status:      readyPodStatus(window+time.Second, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceeded,
			wantReason:  ReasonRestoreInferred,
			wantMessage: "stayed Ready for 30s without a container restart, and engine verifying inferred the restore without positive evidence: no failure seen",
			wantVerdict: "inferred",
			wantOutcome: "succeeded",
		},
		{
			name:        "without a window, an inferred restore concludes at once",
			verdict:     inferred,
			status:      readyPodStatus(0, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceeded,
			wantReason:  ReasonRestoreInferred,
			wantMessage: "is Ready, and engine verifying inferred the restore",
			wantVerdict: "inferred",
			wantOutcome: "succeeded",
		},
		{
			name:        "a cold start",
			verdict:     engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "the engine skipped the restore", Evidence: "event X"},
			window:      window,
			status:      readyPodStatus(time.Second, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			wantReason:  ReasonFallbackToColdStart,
			wantMessage: "the engine skipped the restore (event X)",
			wantVerdict: "cold_start",
			wantOutcome: "fallback",
		},
		{
			name:        "a failed restore",
			verdict:     engine.RestoreVerdict{Outcome: engine.RestoreFailed, Evidence: "status Y"},
			window:      window,
			status:      readyPodStatus(time.Second, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			wantReason:  ReasonRestoreFailed,
			wantMessage: "Engine verifying reported Failed for replacement pod test-pod (status Y)",
			wantVerdict: "failed",
			wantOutcome: "succeeded_without_restore",
		},
		{
			name:         "a restarted container loses the restore, even a confirmed one",
			verdict:      confirmed,
			window:       window,
			status:       readyPodStatus(window+time.Second, 1),
			wantPhase:    pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			wantReason:   ReasonRestoreLost,
			wantMessage:  `Container "app" of replacement pod test-pod restarted before the restore was verified (1 restart(s))`,
			wantVerdict:  "lost",
			wantOutcome:  "restore_lost",
			wantLostEvts: true,
		},
		{
			name:        "a cold start the engine reports wins over a restart",
			verdict:     engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "the engine skipped the restore"},
			window:      window,
			status:      readyPodStatus(time.Second, 2),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			wantReason:  ReasonFallbackToColdStart,
			wantVerdict: "cold_start",
			wantOutcome: "fallback",
		},
		{
			name:        "a restore still in progress",
			verdict:     engine.RestoreVerdict{Outcome: engine.RestoreInProgress},
			window:      window,
			status:      readyPodStatus(time.Minute, 0),
			wantPhase:   pmv1alpha1.PodMigrationJobPhaseRestoring,
			wantRequeue: 2 * time.Second,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng := newVerifyingEngine(tt.verdict)
			var verdictBefore, outcomeBefore float64
			if tt.wantVerdict != "" {
				verdictBefore, outcomeBefore = verdictCount("verifying", tt.wantVerdict), outcomeCount(tt.wantOutcome)
			}

			run := reconcileRestoring(t, eng, tt.window, time.Minute, replacementPod(tt.status))
			if run.err != nil {
				t.Fatalf("Reconcile: %v", run.err)
			}
			if eng.calls == 0 {
				t.Errorf("the engine's verifier wasn't asked")
			}
			if run.job.Status.Phase != tt.wantPhase {
				t.Fatalf("phase = %s, want %s", run.job.Status.Phase, tt.wantPhase)
			}
			cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
			if tt.wantReason == "" {
				if cond != nil {
					t.Errorf("Restored condition = %+v, want none yet", cond)
				}
				if tt.wantRequeue > 0 && (run.result.RequeueAfter < tt.wantRequeue-2*time.Second || run.result.RequeueAfter > tt.wantRequeue+2*time.Second) {
					t.Errorf("RequeueAfter = %s, want about %s", run.result.RequeueAfter, tt.wantRequeue)
				}
				return
			}
			if cond == nil || cond.Reason != tt.wantReason || !strings.Contains(cond.Message, tt.wantMessage) {
				t.Errorf("Restored condition = %+v, want reason %s with a message containing %q", cond, tt.wantReason, tt.wantMessage)
			}
			if ready := meta.FindStatusCondition(run.job.Status.Conditions, "Ready"); ready == nil || ready.Reason != tt.wantReason {
				t.Errorf("Ready condition = %+v, want reason %s", ready, tt.wantReason)
			}
			if got := verdictCount("verifying", tt.wantVerdict) - verdictBefore; got != 1 {
				t.Errorf("%s verdicts recorded = %v, want 1", tt.wantVerdict, got)
			}
			if got := outcomeCount(tt.wantOutcome) - outcomeBefore; got != 1 {
				t.Errorf("%s outcomes recorded = %v, want 1", tt.wantOutcome, got)
			}
			lostEvents := 0
			for _, ev := range run.events {
				if strings.HasPrefix(ev, "Warning "+ReasonRestoreLost+" ") {
					lostEvents++
				}
			}
			if want := map[bool]int{true: 2, false: 0}[tt.wantLostEvts]; lostEvents != want {
				t.Errorf("%s events = %d (events %q), want %d: one on the job and one on the pod", ReasonRestoreLost, lostEvents, run.events, want)
			}
		})
	}
}

func TestRestoreVerdict_ReplacementNotReady(t *testing.T) {
	t.Run("a cold start concludes", func(t *testing.T) {
		eng := newVerifyingEngine(engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "skipped"})
		run := reconcileRestoring(t, eng, 30*time.Second, time.Minute, replacementPod(startingPodStatus()))
		if run.err != nil {
			t.Fatalf("Reconcile: %v", run.err)
		}
		if cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored"); run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
			cond == nil || cond.Reason != ReasonFallbackToColdStart {
			t.Errorf("phase %s, Restored %+v; want SucceededWithoutRestore with %s", run.job.Status.Phase, cond, ReasonFallbackToColdStart)
		}
	})
	t.Run("a restore needs a Ready pod", func(t *testing.T) {
		eng := newVerifyingEngine(engine.RestoreVerdict{Outcome: engine.Restored, Confirmed: true})
		run := reconcileRestoring(t, eng, 0, time.Minute, replacementPod(startingPodStatus()))
		if run.err != nil {
			t.Fatalf("Reconcile: %v", run.err)
		}
		if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || run.result.RequeueAfter != 2*time.Second {
			t.Errorf("phase %s, RequeueAfter %s; want Restoring, polled again in 2s", run.job.Status.Phase, run.result.RequeueAfter)
		}
	})
}

func TestRestoreVerdict_VerifierError(t *testing.T) {
	eng := newVerifyingEngine(engine.RestoreVerdict{})
	eng.err = errors.New("events unavailable")
	run := reconcileRestoring(t, eng, 0, time.Minute, replacementPod(readyPodStatus(time.Minute, 0)))
	if run.err == nil || !strings.Contains(run.err.Error(), "events unavailable") {
		t.Fatalf("Reconcile error = %v, want the verifier's error", run.err)
	}
	if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("phase = %s, want Restoring", run.job.Status.Phase)
	}
}

// An engine without a verifier can't confirm a restore: a Ready replacement
// concludes as RestoreInferred.
func TestRestoreVerdict_EngineWithoutVerifier(t *testing.T) {
	plain := &routedEngine{name: "plain", provider: &fakeSnapshotProvider{}}
	run := reconcileRestoring(t, plain, 0, time.Minute, replacementPod(readyPodStatus(time.Minute, 0)))
	if run.err != nil {
		t.Fatalf("Reconcile: %v", run.err)
	}
	cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
	if run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded || cond == nil || cond.Reason != ReasonRestoreInferred ||
		!strings.Contains(cond.Message, "the engine reports no restore verdict") {
		t.Errorf("phase %s, Restored %+v; want Succeeded with %s", run.job.Status.Phase, cond, ReasonRestoreInferred)
	}
}

// GKE Pod Snapshots has no positive restore signal, so its restores conclude
// as RestoreInferred, after the window.
func TestRestoreVerdict_GKEPodSnapshotsInfersRestores(t *testing.T) {
	run := reconcileRestoring(t, nil, 30*time.Second, time.Minute, replacementPod(readyPodStatus(5*time.Second, 0)))
	if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Fatalf("inside the window: phase %s, error %v; want Restoring", run.job.Status.Phase, run.err)
	}
	run = reconcileRestoring(t, nil, 30*time.Second, time.Minute, replacementPod(readyPodStatus(31*time.Second, 0)))
	cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
	if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded || cond == nil || cond.Reason != ReasonRestoreInferred ||
		!strings.Contains(cond.Message, "engine gke-pod-snapshots inferred the restore") {
		t.Errorf("after the window: phase %s, Restored %+v, error %v; want Succeeded with %s", run.job.Status.Phase, cond, run.err, ReasonRestoreInferred)
	}
}

// The ceiling is 5 minutes plus the window, so a replacement that becomes
// Ready just before 5 minutes can stay up through its window.
func TestRestoreVerdict_CeilingLeavesRoomForTheWindow(t *testing.T) {
	const window = 30 * time.Second
	inferred := engine.RestoreVerdict{Outcome: engine.Restored}

	t.Run("Ready just before 5 minutes", func(t *testing.T) {
		run := reconcileRestoring(t, newVerifyingEngine(inferred), window, 5*time.Minute+10*time.Second, replacementPod(readyPodStatus(15*time.Second, 0)))
		if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase %s, error %v; want Restoring until the window ends", run.job.Status.Phase, run.err)
		}
	})
	t.Run("past 5 minutes plus the window", func(t *testing.T) {
		eng := newVerifyingEngine(inferred)
		before := verdictCount("verifying", "timeout")
		run := reconcileRestoring(t, eng, window, 5*time.Minute+window+time.Second, replacementPod(startingPodStatus()))
		cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
		if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore ||
			cond == nil || cond.Reason != "RestoreTimeout" || cond.Message != "Restore timed out after 5m30s" {
			t.Errorf("phase %s, Restored %+v, error %v; want SucceededWithoutRestore, RestoreTimeout after 5m30s", run.job.Status.Phase, cond, run.err)
		}
		if got := verdictCount("verifying", "timeout") - before; got != 1 {
			t.Errorf("timeout verdicts recorded = %v, want 1", got)
		}
	})
	t.Run("Ready past the stabilization window at ceiling concludes restored", func(t *testing.T) {
		eng := newVerifyingEngine(inferred)
		// Restoring 5m30.3s, Ready for 30.5s, 0 restarts
		run := reconcileRestoring(t, eng, window, 5*time.Minute+30*time.Second+300*time.Millisecond, replacementPod(readyPodStatus(30*time.Second+500*time.Millisecond, 0)))
		cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
		if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded ||
			cond == nil || cond.Reason != ReasonRestoreInferred {
			t.Errorf("phase %s, Restored %+v, error %v; want Succeeded, %s", run.job.Status.Phase, cond, run.err, ReasonRestoreInferred)
		}
	})
}

func TestContainerRestarts(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	pod := &corev1.Pod{
		Spec: corev1.PodSpec{InitContainers: []corev1.Container{
			{Name: "setup"},
			{Name: "sidecar", RestartPolicy: &always},
		}},
	}
	if n, name := containerRestarts(pod); n != 0 || name != "" {
		t.Errorf("containerRestarts() of a pod without statuses = %d, %q; want 0", n, name)
	}

	// A one-off init container that retried doesn't count: it ran before
	// the app started.
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "setup", RestartCount: 3}}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app"}, {Name: "worker"}}
	if n, _ := containerRestarts(pod); n != 0 {
		t.Errorf("containerRestarts() with only an init container's retries = %d, want 0", n)
	}

	pod.Status.InitContainerStatuses = append(pod.Status.InitContainerStatuses, corev1.ContainerStatus{Name: "sidecar", RestartCount: 1})
	if n, name := containerRestarts(pod); n != 1 || name != "sidecar" {
		t.Errorf("containerRestarts() with a restarted sidecar = %d, %q; want 1, sidecar", n, name)
	}

	pod.Status.ContainerStatuses[1].RestartCount = 2
	if n, name := containerRestarts(pod); n != 3 || name != "worker" {
		t.Errorf("containerRestarts() = %d, %q; want 3, worker", n, name)
	}
}

func TestStabilizationRemaining(t *testing.T) {
	const window = 30 * time.Second
	start := metav1.NewTime(time.Now().Add(-20 * time.Second))
	job := &pmv1alpha1.PodMigrationJob{Status: pmv1alpha1.PodMigrationJobStatus{RestoringStartTime: &start}}
	r := &PodMigrationJobReconciler{RestoreStabilizationWindow: window}

	near := func(got, want time.Duration) bool { return got > want-2*time.Second && got <= want }
	if got := r.stabilizationRemaining(job, replacementPod(readyPodStatus(5*time.Second, 0))); !near(got, 25*time.Second) {
		t.Errorf("Ready for 5s: remaining = %s, want about 25s", got)
	}
	if got := r.stabilizationRemaining(job, replacementPod(readyPodStatus(time.Minute, 0))); got != 0 {
		t.Errorf("Ready for 1m: remaining = %s, want 0", got)
	}
	// Without a Ready transition time, the window runs from RestoringStartTime.
	noTime := replacementPod(readyPodStatus(0, 0))
	noTime.Status.Conditions[0].LastTransitionTime = metav1.Time{}
	if got := r.stabilizationRemaining(job, noTime); !near(got, 10*time.Second) {
		t.Errorf("no Ready time: remaining = %s, want about 10s", got)
	}
	if got := (&PodMigrationJobReconciler{}).stabilizationRemaining(job, replacementPod(readyPodStatus(0, 0))); got != 0 {
		t.Errorf("no window: remaining = %s, want 0", got)
	}
	// Node clock is ahead of controller clock (6 minutes in the future):
	// falls back to RestoringStartTime (which was 20s ago), so remaining is about 10s.
	futurePod := replacementPod(readyPodStatus(-6*time.Minute, 0))
	if got := r.stabilizationRemaining(job, futurePod); !near(got, 10*time.Second) {
		t.Errorf("Ready 6m in the future with RestoringStartTime: remaining = %s, want about 10s", got)
	}
	// Without RestoringStartTime, clock skew in the future falls back to now,
	// requiring the full stabilization window.
	jobNoStart := &pmv1alpha1.PodMigrationJob{}
	if got := r.stabilizationRemaining(jobNoStart, futurePod); got != window {
		t.Errorf("Ready 6m in the future without RestoringStartTime: remaining = %s, want %s", got, window)
	}
}

func TestPodMigrationJobReconciler_CeilingClockSkew_ConcludesRestored(t *testing.T) {
	const window = 30 * time.Second
	inferred := engine.RestoreVerdict{Outcome: engine.Restored}

	for _, skew := range []time.Duration{90 * time.Second, 6 * time.Minute} {
		t.Run(fmt.Sprintf("Ready %s in the future at ceiling concludes restored", skew), func(t *testing.T) {
			eng := newVerifyingEngine(inferred)
			// Restoring for 5m31s, Ready is skew in the future (negative readyFor), 0 restarts.
			run := reconcileRestoring(t, eng, window, 5*time.Minute+31*time.Second, replacementPod(readyPodStatus(-skew, 0)))
			cond := meta.FindStatusCondition(run.job.Status.Conditions, "Restored")
			if run.err != nil || run.job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded ||
				cond == nil || cond.Reason != ReasonRestoreInferred {
				t.Fatalf("phase %s, Restored %+v, error %v; want Succeeded, %s", run.job.Status.Phase, cond, run.err, ReasonRestoreInferred)
			}
		})
	}
}
