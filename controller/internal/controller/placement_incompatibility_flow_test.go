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

package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

// TestRestorePlacement_CRIUPlacementTimeoutFlow is
// TestRestorePlacement_PlacementTimeoutFlow for the CRIU engine (#257,
// #294): the pod gate releases the replacement with the CRIU constraints,
// the only node with room has an older kernel, and at the deadline the job
// concludes PlacementTimeout, naming the engine and the kernel version, and
// deletes the pod. Its owner recreates it, and the new pod starts cold. No
// invariant may fire on the way.
func TestRestorePlacement_CRIUPlacementTimeoutFlow(t *testing.T) {
	// The eviction webhook records the serving engine on every job it
	// creates; the invariants read restore orders per that engine (#234).
	criuJob := placementJob()
	if criuJob.Annotations == nil {
		criuJob.Annotations = map[string]string{}
	}
	criuJob.Annotations[engine.AnnotationEngine] = engine.CRIUName
	f := newPlacementFlowFor(t, ownedPlacementPod(), criuJob, criuNode("old-kernel", olderKernel()))
	reg := criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts))
	f.gate.Engines, f.jobs.Engines = reg, reg
	ctx := context.Background()

	f.reconcileGate("pod gate releases with the CRIU constraints")
	pod := f.pod()
	if podHasMigrationGate(pod) || requiredTerms(pod) == nil || pod.Annotations[engine.RestoreAnnotation(engine.CRIUAPIGroup)] != placementSnapshot {
		t.Fatalf("pod gated %t, terms %+v, annotations %v; want it released with the CRIU constraints and restore order",
			podHasMigrationGate(pod), requiredTerms(pod), pod.Annotations)
	}

	pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable}}
	if err := f.cl.Status().Update(ctx, pod); err != nil {
		t.Fatalf("marking the pod unschedulable: %v", err)
	}
	job := f.job()
	released := metav1.NewTime(time.Now().Add(-DefaultRestorePlacementDeadline - time.Minute))
	job.Status.RestoringStartTime = &released
	for i := range job.Status.Conditions {
		if job.Status.Conditions[i].Type == ConditionRestorePlacement {
			job.Status.Conditions[i].LastTransitionTime = released
		}
	}
	if err := f.cl.Status().Update(ctx, job); err != nil {
		t.Fatalf("moving the release back past the deadline: %v", err)
	}

	f.reconcileJob("job concludes and deletes the pod")
	job = f.job()
	cond := meta.FindStatusCondition(job.Status.Conditions, "Restored")
	signal := "engine=criu requirements=" + criuplacement.LabelKernelVersion + ": "
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || cond == nil || cond.Reason != ReasonPlacementTimeout ||
		!strings.HasPrefix(cond.Message, signal) {
		t.Fatalf("job status = %+v, want SucceededWithoutRestore with reason %s and a message starting %q", job.Status, ReasonPlacementTimeout, signal)
	}
	if err := f.cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, &corev1.Pod{}); !apierrors.IsNotFound(err) {
		t.Fatalf("getting the pod: %v, want it deleted", err)
	}

	// The StatefulSet recreates the pod. No job claims it, so it starts
	// cold, and nothing touches it.
	recreated := ownedPlacementPod()
	recreated.UID = "recreated-uid"
	recreated.Annotations = nil
	recreated.Spec.SchedulingGates = nil
	if err := f.cl.Create(ctx, recreated); err != nil {
		t.Fatalf("recreating the pod: %v", err)
	}
	f.reconcileJob("job settles")
	if err := f.cl.Get(ctx, types.NamespacedName{Namespace: placementNamespace, Name: placementPodName}, &corev1.Pod{}); err != nil {
		t.Fatalf("getting the recreated pod: %v, want it kept", err)
	}

	events := f.events()
	for _, v := range violations(events) {
		t.Errorf("invariant violation: %s", v)
	}
	n := 0
	for _, e := range events {
		if strings.HasPrefix(e, "Warning "+ReasonPlacementTimeout+" "+signal) {
			n++
		}
	}
	if n != 2 {
		t.Errorf("events = %q, want 2 %s Warnings starting %q", events, ReasonPlacementTimeout, signal)
	}
}

// incompatibleNodePod is the CRIU replacement that the pod gate released
// with the restore order and that runs, Ready, on node-b, where the node
// agent refused the restore because of the CPU model and the holder started
// the app cold.
func incompatibleNodePod(readySince time.Time) *corev1.Pod {
	pod := ownedPlacementPod()
	pod.Spec.SchedulingGates = nil
	pod.Spec.NodeName = "node-b"
	pod.Annotations[engine.RestoreAnnotation(engine.CRIUAPIGroup)] = placementSnapshot
	pod.Annotations[engine.CRIUInjectedAnnotation] = "true"
	pod.Annotations[engine.CRIURestoreStatusAnnotation] = engine.CRIURestoreStatusFailed
	pod.Annotations[engine.CRIURestoreReasonAnnotation] = engine.CRIURestoreReasonIncompatibleNode
	pod.Spec.Containers = []corev1.Container{{Name: "app", Image: "app"}}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodScheduled, Status: corev1.ConditionTrue},
		{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(readySince)},
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "app", Ready: true, State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
	return pod
}

// incompatibleNodeJob is the Restoring CRIU job whose replacement is
// incompatibleNodePod; its source ran on node-a.
func incompatibleNodeJob() *pmv1alpha1.PodMigrationJob {
	job := placementJob()
	job.Annotations[engine.AnnotationEngine] = engine.CRIUName
	job.Status.OriginNodeName = "node-a"
	job.Status.Consumed = true
	job.Status.GateReleased = true
	job.Status.RestoredPodName = placementPodName
	job.Status.RestoredPodUID = placementPodUID
	stopped := metav1.NewTime(time.Now().Add(-20 * time.Second))
	job.Status.TrafficStoppedTime = &stopped
	return job
}

// TestCRIURestoreIncompatibleNodeFlow: the CRIU node agent refused the
// restore because the node's CPU model isn't the source's (#294). The job
// concludes SucceededWithoutRestore with reason RestoreIncompatibleNode,
// with Warning events on the job and the pod that name the engine, the
// fact, and both nodes, and the replacement keeps running cold, as before:
// it isn't deleted, its downtime is recorded as a cold start, and no
// invariant fires.
func TestCRIURestoreIncompatibleNodeFlow(t *testing.T) {
	readySince := time.Now().Add(-5 * time.Second)
	f := newPlacementFlowFor(t, incompatibleNodePod(readySince), incompatibleNodeJob())
	reg := criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts))
	f.gate.Engines, f.jobs.Engines = reg, reg

	coldStarts := verdictCount(engine.CRIUName, "cold_start")
	fallbacks := outcomeCount("fallback")
	hist := metrics.ApplicationDowntimeSeconds.WithLabelValues(engine.CRIUName, metrics.OutcomeColdStart)
	samples := getHistogramSampleCount(hist)

	f.reconcileJob("job concludes")
	job := f.job()
	signal := "engine=criu fact=cpu-model node=node-b sourceNode=node-a: "
	if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
		t.Fatalf("phase = %s, want SucceededWithoutRestore", job.Status.Phase)
	}
	for _, condType := range []string{"Restored", "Ready"} {
		cond := meta.FindStatusCondition(job.Status.Conditions, condType)
		if cond == nil || (condType == "Restored" && cond.Status != metav1.ConditionFalse) || cond.Reason != ReasonRestoreIncompatibleNode ||
			!strings.HasPrefix(cond.Message, signal) || !strings.Contains(cond.Message, "reason "+engine.CRIURestoreReasonIncompatibleNode) {
			t.Errorf("%s = %+v, want %s (Restored False) with a message starting %q and the agent's reason", condType, cond, ReasonRestoreIncompatibleNode, signal)
		}
	}
	if job.Status.ReplacementReadyTime == nil || job.Status.ApplicationPauseDuration == nil {
		t.Errorf("downtime = %v / %v, want the cold start's recorded", job.Status.ReplacementReadyTime, job.Status.ApplicationPauseDuration)
	}
	if got := verdictCount(engine.CRIUName, "cold_start") - coldStarts; got != 1 {
		t.Errorf("cold_start verdicts recorded = %v, want 1", got)
	}
	if got := outcomeCount("fallback") - fallbacks; got != 1 {
		t.Errorf("fallback outcomes recorded = %v, want 1", got)
	}
	if got := getHistogramSampleCount(hist) - samples; got != 1 {
		t.Errorf("cold-start downtime samples = %d, want 1", got)
	}

	// The cold start is clean: the replacement keeps running as it is,
	// through later reconciles of both reconcilers.
	f.reconcileJob("job settles")
	f.reconcileGate("pod gate leaves the pod")
	pod := f.pod()
	if string(pod.UID) != placementPodUID || pod.DeletionTimestamp != nil || pod.Spec.NodeName != "node-b" || !isPodReady(pod) {
		t.Errorf("pod = %+v, want the cold-started replacement kept, Ready on node-b", pod.ObjectMeta)
	}

	events := f.events()
	for _, v := range violations(events) {
		t.Errorf("invariant violation: %s", v)
	}
	n := 0
	for _, e := range events {
		if strings.HasPrefix(e, "Warning "+ReasonRestoreIncompatibleNode+" "+signal) {
			n++
		}
	}
	if n != 2 {
		t.Errorf("events = %q, want 2 %s Warnings (job and pod) starting %q", events, ReasonRestoreIncompatibleNode, signal)
	}
}

// Any other failed CRIU restore still concludes FallbackToColdStart, without
// the incompatible-node events.
func TestCRIURestoreOtherFailureStaysFallbackToColdStart(t *testing.T) {
	pod := incompatibleNodePod(time.Now().Add(-5 * time.Second))
	pod.Annotations[engine.CRIURestoreReasonAnnotation] = "ArchiveSignatureInvalid"
	f := newPlacementFlowFor(t, pod, incompatibleNodeJob())
	reg := criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts))
	f.gate.Engines, f.jobs.Engines = reg, reg

	f.reconcileJob("job concludes")
	cond := meta.FindStatusCondition(f.job().Status.Conditions, "Restored")
	if cond == nil || cond.Reason != ReasonFallbackToColdStart || strings.HasPrefix(cond.Message, "engine=") {
		t.Errorf("Restored = %+v, want %s with the message as before", cond, ReasonFallbackToColdStart)
	}
	if hasEventWith(f.events(), ReasonRestoreIncompatibleNode) {
		t.Error("an incompatible-node event for another failure")
	}
}

// An IncompatibleFact that would break the message format (a space or a
// colon) is ignored: the job concludes FallbackToColdStart.
func TestConcludeWithoutRestore_MalformedIncompatibleFact(t *testing.T) {
	f := newPlacementFlowFor(t, incompatibleNodePod(time.Now().Add(-5*time.Second)), incompatibleNodeJob())
	job := f.job()
	verdict := engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "cold", IncompatibleFact: "cpu model: x"}
	if err := f.jobs.concludeWithoutRestore(context.Background(), job, job.DeepCopy(), f.pod(), engine.CRIUName, verdict); err != nil {
		t.Fatalf("concludeWithoutRestore: %v", err)
	}
	if cond := meta.FindStatusCondition(f.job().Status.Conditions, "Restored"); cond == nil || cond.Reason != ReasonFallbackToColdStart {
		t.Errorf("Restored = %+v, want %s", cond, ReasonFallbackToColdStart)
	}
}

// A job that concluded RestoreIncompatibleNode before its cold-started
// replacement was Ready gets its downtime backfilled, as for
// FallbackToColdStart.
func TestCRIURestoreIncompatibleNodeBackfillsDowntime(t *testing.T) {
	readySince := time.Now().Add(-2 * time.Second)
	job := incompatibleNodeJob()
	completed := metav1.NewTime(time.Now().Add(-10 * time.Second))
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore
	job.Status.CompletionTime = &completed
	job.Status.Conditions = []metav1.Condition{{Type: "Restored", Status: metav1.ConditionFalse, Reason: ReasonRestoreIncompatibleNode, Message: "m"}}
	f := newPlacementFlowFor(t, incompatibleNodePod(readySince), job)
	f.jobs.Engines = criuPlacementRegistry(t, sourceNodeOf(criuSourceFacts))

	recheck, err := f.jobs.backfillColdStartDowntime(context.Background(), f.job())
	if err != nil || recheck != 0 {
		t.Fatalf("backfillColdStartDowntime() = %v, %v; want 0, nil", recheck, err)
	}
	if got := f.job(); got.Status.ReplacementReadyTime == nil || got.Status.ApplicationPauseDuration == nil {
		t.Errorf("downtime = %v / %v, want it backfilled", got.Status.ReplacementReadyTime, got.Status.ApplicationPauseDuration)
	}
}
