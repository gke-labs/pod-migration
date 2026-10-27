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
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

// A runc replacement whose start failure carries gVisor's restore-failure
// wording is reported, not acted on (#132): the gVisor engine classifies only
// gVisor pods, so the pod isn't deleted and the job keeps Restoring until its
// restore ceiling, as for any unrecognized start failure.
func TestPodMigrationJobReconciler_Restoring_RuncStartFailureIsNotAGVisorRestoreCrash(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace, podName, podUID := "default", "runc-pod", "restored-uid-runc"
	jobName := "pmj-" + podName
	pod, pmj := newRestoreCrashFixture(namespace, jobName, podName, podUID,
		restoreCrashPodStatus(restoreCrashOCIMessage))
	runc := "runc"
	pod.Spec.RuntimeClassName = &runc

	c := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		Build()
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(10)}

	fallbackBefore := counterValue(t, metrics.RestoreCrashFallbackTotal)
	unmatchedBefore := counterValue(t, metrics.RestoreCrashUnmatchedTotal)

	ctx := context.Background()
	if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	got := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: jobName}, got); err != nil {
		t.Fatalf("getting the job: %v", err)
	}
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseRestoring)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionRestoreCrashed); cond != nil && cond.Status == metav1.ConditionTrue {
		t.Errorf("%s = %+v; a runc start failure must not be attributed to gVisor", ConditionRestoreCrashed, cond)
	}
	if cond := meta.FindStatusCondition(got.Status.Conditions, ConditionUnrecognizedRestoreCrash); cond == nil || cond.Status != metav1.ConditionTrue {
		t.Errorf("%s = %+v, want True (reported, not acted on)", ConditionUnrecognizedRestoreCrash, cond)
	}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: podName}, &corev1.Pod{}); err != nil {
		t.Errorf("the runc replacement was deleted (err=%v); it must be left alone", err)
	}
	if n := counterValue(t, metrics.RestoreCrashFallbackTotal) - fallbackBefore; n != 0 {
		t.Errorf("fallback counter +%v, want unchanged", n)
	}
	if n := counterValue(t, metrics.RestoreCrashUnmatchedTotal) - unmatchedBefore; n != 1 {
		t.Errorf("unmatched counter +%v, want +1", n)
	}
}
