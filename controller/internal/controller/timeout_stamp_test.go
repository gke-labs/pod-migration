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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// i4PendingViolations evaluates I4 on job as a Pending job created age ago.
func i4PendingViolations(job *pmv1alpha1.PodMigrationJob, age time.Duration) []invariants.Violation {
	now := time.Now()
	j := job.DeepCopy()
	j.CreationTimestamp = metav1.NewTime(now.Add(-age))
	j.Status = pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending}
	return invariants.EvaluateI4(&invariants.ReconcileSnapshot{Now: now, PrimaryPMJ: j})
}

// The issue's two examples (#330), for a job the eviction webhook creates:
// it records the timeout the reconciler holds it to, so I4 reads the same.
func TestCreatedJobCarriesReconcilerTimeoutForI4(t *testing.T) {
	for name, tc := range map[string]struct {
		podTimeout string
		flag       time.Duration
		age        time.Duration
	}{
		"--migration-timeout=30m, no pod annotation, Pending 12m": {flag: 30 * time.Minute, age: 12 * time.Minute},
		"pod annotated 10s, Pending 45s":                          {podTimeout: "10s", flag: 10 * time.Minute, age: 45 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "src", UID: "uid-src"}}
			if tc.podTimeout != "" {
				pod.Annotations = map[string]string{util.AnnotationMigrationTimeout: tc.podTimeout}
			}
			labels, ann := util.BuildPMJLabelsAndAnnotationsForPolicy(pod, "web", "ReplicaSet", "rs-uid", "", tc.flag, util.TriggerSourceEviction)
			job := &pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{
				Namespace: "default", Name: "pmj-src", Labels: labels, Annotations: ann,
			}}

			r := &PodMigrationJobReconciler{DefaultMigrationTimeout: tc.flag}
			if got, want := invariants.EffectiveMigrationTimeout(job), r.getMigrationTimeout(job); got != want {
				t.Errorf("I4's timeout %v != reconciler's %v (annotation %q)", got, want, ann[util.AnnotationMigrationTimeout])
			}
			if vs := i4PendingViolations(job, tc.age); len(vs) != 0 {
				t.Fatalf("I4 flagged a job within the reconciler's timeout: %+v", vs)
			}
		})
	}
}

// A job created without the annotation (by hand, or before #330) gets it on
// its first Pending pass, also when the timeout equals --migration-timeout,
// and the deadline it publishes doesn't change.
func TestPending_StampsTimeoutEqualToFlag(t *testing.T) {
	for name, tc := range map[string]struct {
		podTimeout string
		flag       time.Duration
		want       string
		wantD      time.Duration
		age        time.Duration
	}{
		"--migration-timeout=30m, no pod annotation": {flag: 30 * time.Minute, want: "30m0s", wantD: 30 * time.Minute, age: 12 * time.Minute},
		"default flag, no pod annotation":            {want: "10m0s", wantD: 10 * time.Minute, age: 9 * time.Minute},
		"pod annotated 10s":                          {podTimeout: "10s", flag: 10 * time.Minute, want: "1m0s", wantD: time.Minute, age: 45 * time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "src", UID: "uid-src"}}
			if tc.podTimeout != "" {
				pod.Annotations = map[string]string{util.AnnotationMigrationTimeout: tc.podTimeout}
			}
			created := metav1.NewTime(time.Now().Add(-time.Second).Truncate(time.Second))
			pmj := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "pmj-src", CreationTimestamp: created},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: "src"},
					TargetPodUID: "uid-src",
				},
				Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithObjects(pod, pmj).Build()
			r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, DefaultMigrationTimeout: tc.flag}

			// The deadline the job is held to before the stamp.
			before := r.calculateDeadline(pmj)

			if _, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: "default", Name: "pmj-src"},
			}); err != nil {
				t.Fatalf("Reconcile: %v", err)
			}
			got := &pmv1alpha1.PodMigrationJob{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: "pmj-src"}, got); err != nil {
				t.Fatalf("Get: %v", err)
			}
			if a := got.Annotations[util.AnnotationMigrationTimeout]; a != tc.want {
				t.Fatalf("timeout annotation = %q, want %q", a, tc.want)
			}
			if d := r.getMigrationTimeout(got); d != tc.wantD {
				t.Errorf("reconciler reads the stamped annotation as %v, want %v", d, tc.wantD)
			}
			if tc.podTimeout == "" {
				pending := got.DeepCopy()
				pending.Status = pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending}
				if after := r.calculateDeadline(pending); !after.Equal(before) {
					t.Errorf("Pending deadline changed from %v to %v", before, after)
				}
			}
			if vs := i4PendingViolations(got, tc.age); len(vs) != 0 {
				t.Fatalf("I4 flagged a job within the reconciler's timeout: %+v", vs)
			}
		})
	}
}
