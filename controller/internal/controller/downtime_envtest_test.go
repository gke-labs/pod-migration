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
	"os"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
)

// TestApplicationDowntime_Envtest verifies application downtime recording and Prometheus
// histogram emissions across warm-restore and cold-fallback paths against a real API server.
func TestApplicationDowntime_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest application downtime test")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding corev1 scheme: %v", err)
	}
	if err := pmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("adding pmv1alpha1 scheme: %v", err)
	}

	k8sClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}

	ctx := context.Background()
	r := &PodMigrationJobReconciler{
		Client:   k8sClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}

	t.Run("warm restore verified persists status and observes histogram", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		trafficStop := metav1.NewTime(now.Add(-8 * time.Second))
		readyTime := metav1.NewTime(now.Add(-3 * time.Second))

		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-downtime-warm",
				Namespace: "default",
				Annotations: map[string]string{
					"pod-migration.gke.io/engine": "criu",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "source-warm"},
				TargetPodUID: "uid-warm-target",
			},
		}
		if err := k8sClient.Create(ctx, job); err != nil {
			t.Fatalf("creating PMJ: %v", err)
		}

		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		job.Status.TrafficStoppedTime = &trafficStop
		if err := k8sClient.Status().Update(ctx, job); err != nil {
			t.Fatalf("updating PMJ status: %v", err)
		}

		replacementPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "replacement-warm",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "c1", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, replacementPod); err != nil {
			t.Fatalf("creating replacement pod: %v", err)
		}
		replacementPod.Status.Phase = corev1.PodRunning
		replacementPod.Status.Conditions = []corev1.PodCondition{
			{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: readyTime,
			},
		}
		if err := k8sClient.Status().Update(ctx, replacementPod); err != nil {
			t.Fatalf("updating replacement pod status: %v", err)
		}

		hist := metrics.ApplicationDowntimeSeconds.WithLabelValues("criu", "RestoreVerified")
		beforeCount := getHistogramSampleCount(hist)

		origJob := job.DeepCopy()
		verdict := engine.RestoreVerdict{Outcome: engine.Restored, Confirmed: true, Evidence: "confirmed warm restore"}
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job.Name}}
		if _, err := r.concludeRestored(ctx, req, job, origJob, replacementPod, "criu", verdict); err != nil {
			t.Fatalf("concludeRestored failed: %v", err)
		}

		afterCount := getHistogramSampleCount(hist)
		if afterCount != beforeCount+1 {
			t.Errorf("expected sample count to increment by 1, got %d (before: %d)", afterCount, beforeCount)
		}

		var fetched pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, req.NamespacedName, &fetched); err != nil {
			t.Fatalf("fetching PMJ: %v", err)
		}

		if fetched.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			t.Errorf("expected Phase Succeeded, got %s", fetched.Status.Phase)
		}
		if fetched.Status.TrafficStoppedTime == nil || !fetched.Status.TrafficStoppedTime.Equal(&trafficStop) {
			t.Errorf("expected TrafficStoppedTime %v, got %v", trafficStop, fetched.Status.TrafficStoppedTime)
		}
		if fetched.Status.ReplacementReadyTime == nil || !fetched.Status.ReplacementReadyTime.Equal(&readyTime) {
			t.Errorf("expected ReplacementReadyTime %v, got %v", readyTime, fetched.Status.ReplacementReadyTime)
		}
		if fetched.Status.ApplicationPauseDuration == nil {
			t.Fatalf("expected ApplicationPauseDuration non-nil")
		}
		if fetched.Status.ApplicationPauseDuration.Duration != 5*time.Second {
			t.Errorf("expected ApplicationPauseDuration 5s, got %v", fetched.Status.ApplicationPauseDuration.Duration)
		}
	})

	t.Run("cold start fallback persists status and observes histogram", func(t *testing.T) {
		now := time.Now().Truncate(time.Second)
		trafficStop := metav1.NewTime(now.Add(-10 * time.Second))
		readyTime := metav1.NewTime(now.Add(-1 * time.Second))

		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pmj-downtime-cold",
				Namespace: "default",
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "source-cold"},
				TargetPodUID: "uid-cold-target",
			},
		}
		if err := k8sClient.Create(ctx, job); err != nil {
			t.Fatalf("creating PMJ: %v", err)
		}

		job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		job.Status.TrafficStoppedTime = &trafficStop
		if err := k8sClient.Status().Update(ctx, job); err != nil {
			t.Fatalf("updating PMJ status: %v", err)
		}

		replacementPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "replacement-cold",
				Namespace: "default",
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "c1", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, replacementPod); err != nil {
			t.Fatalf("creating replacement pod: %v", err)
		}
		replacementPod.Status.Phase = corev1.PodRunning
		replacementPod.Status.Conditions = []corev1.PodCondition{
			{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: readyTime,
			},
		}
		if err := k8sClient.Status().Update(ctx, replacementPod); err != nil {
			t.Fatalf("updating replacement pod status: %v", err)
		}

		hist := metrics.ApplicationDowntimeSeconds.WithLabelValues("gke-pod-snapshots", "ColdStart")
		beforeCount := getHistogramSampleCount(hist)

		origJob := job.DeepCopy()
		verdict := engine.RestoreVerdict{Outcome: engine.ColdStart, Reason: "Cold start fallback triggered"}
		if err := r.concludeWithoutRestore(ctx, job, origJob, replacementPod, "gke-pod-snapshots", verdict); err != nil {
			t.Fatalf("concludeWithoutRestore failed: %v", err)
		}

		afterCount := getHistogramSampleCount(hist)
		if afterCount != beforeCount+1 {
			t.Errorf("expected sample count to increment by 1, got %d (before: %d)", afterCount, beforeCount)
		}

		var fetched pmv1alpha1.PodMigrationJob
		req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: job.Name}}
		if err := k8sClient.Get(ctx, req.NamespacedName, &fetched); err != nil {
			t.Fatalf("fetching PMJ: %v", err)
		}

		if fetched.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Errorf("expected Phase SucceededWithoutRestore, got %s", fetched.Status.Phase)
		}
		if fetched.Status.TrafficStoppedTime == nil || !fetched.Status.TrafficStoppedTime.Equal(&trafficStop) {
			t.Errorf("expected TrafficStoppedTime %v, got %v", trafficStop, fetched.Status.TrafficStoppedTime)
		}
		if fetched.Status.ReplacementReadyTime == nil || !fetched.Status.ReplacementReadyTime.Equal(&readyTime) {
			t.Errorf("expected ReplacementReadyTime %v, got %v", readyTime, fetched.Status.ReplacementReadyTime)
		}
		if fetched.Status.ApplicationPauseDuration == nil {
			t.Fatalf("expected ApplicationPauseDuration non-nil")
		}
		if fetched.Status.ApplicationPauseDuration.Duration != 9*time.Second {
			t.Errorf("expected ApplicationPauseDuration 9s, got %v", fetched.Status.ApplicationPauseDuration.Duration)
		}
	})
}
