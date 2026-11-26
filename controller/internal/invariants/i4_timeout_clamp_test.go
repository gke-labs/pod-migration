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

package invariants

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// I4 reads a job's timeout annotation as the reconciler does, clamped to
// [util.MinMigrationTimeout, util.MaxMigrationTimeout], and falls back to
// MaxMigrationDuration when it's unset or invalid (#330).
func TestEffectiveMigrationTimeout_ClampsLikeReconciler(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want time.Duration
	}{
		{raw: "", want: MaxMigrationDuration},
		{raw: "soon", want: MaxMigrationDuration},
		{raw: "0s", want: MaxMigrationDuration},
		{raw: "-5m", want: MaxMigrationDuration},
		{raw: "1ns", want: util.MinMigrationTimeout},
		{raw: "10s", want: util.MinMigrationTimeout},
		{raw: "59s", want: util.MinMigrationTimeout},
		{raw: "1m", want: time.Minute},
		{raw: "30m0s", want: 30 * time.Minute},
		{raw: "2h", want: util.MaxMigrationTimeout},
		{raw: "3h", want: util.MaxMigrationTimeout},
	} {
		job := &pmv1alpha1.PodMigrationJob{}
		if tc.raw != "" {
			job.Annotations = map[string]string{AnnotationMigrationTimeout: tc.raw}
		}
		got := EffectiveMigrationTimeout(job)
		if got != tc.want {
			t.Errorf("EffectiveMigrationTimeout(%q) = %v, want %v", tc.raw, got, tc.want)
		}
		// The reconciler's reading (getMigrationTimeout), with I4's fallback
		// as its default.
		if d := util.CalculateMigrationTimeout(tc.raw, 0, MaxMigrationDuration); got != d {
			t.Errorf("EffectiveMigrationTimeout(%q) = %v, but the reconciler reads %v", tc.raw, got, d)
		}
	}
}

// I4 reads the annotation the controller stamps.
func TestAnnotationMigrationTimeoutMatchesController(t *testing.T) {
	if AnnotationMigrationTimeout != util.AnnotationMigrationTimeout {
		t.Fatalf("AnnotationMigrationTimeout = %q, controller stamps %q", AnnotationMigrationTimeout, util.AnnotationMigrationTimeout)
	}
}

// The issue's examples (#330): a job held to 1m by the reconciler because its
// annotation is below the minimum, and a job that records a 30m
// --migration-timeout, aren't flagged inside those deadlines plus I4's 30s
// slack, and are flagged past them.
func TestEvaluateI4_TimeoutClampedLikeReconciler(t *testing.T) {
	now := time.Now()
	pending := func(timeout string, age time.Duration) *pmv1alpha1.PodMigrationJob {
		return &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "default",
				Name:              "pmj-timeout",
				CreationTimestamp: metav1.NewTime(now.Add(-age)),
				Annotations:       map[string]string{AnnotationMigrationTimeout: timeout},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending},
		}
	}
	snapshotting := func(timeout string) *pmv1alpha1.PodMigrationJob {
		j := pending(timeout, 10*time.Minute)
		sst := metav1.NewTime(now.Add(-5 * time.Minute))
		j.Status = pmv1alpha1.PodMigrationJobStatus{
			Phase:                 pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			SnapshotRef:           "snap-pmj-timeout",
			SnapshottingStartTime: &sst,
		}
		return j
	}
	progressAgo := func(d time.Duration) map[string]SnapshotProgress {
		return map[string]SnapshotProgress{"default/pmj-timeout": {LastProgressTime: now.Add(-d)}}
	}
	for name, tc := range map[string]struct {
		job         *pmv1alpha1.PodMigrationJob
		progress    map[string]SnapshotProgress
		wantViolate bool
	}{
		// Past the deadline, the snapshot's progress is held to the same
		// clamped timeout plus 30s.
		"10s annotation, Snapshotting, progress 60s ago": {job: snapshotting("10s"), progress: progressAgo(60 * time.Second)},
		"10s annotation, Snapshotting, progress 91s ago": {job: snapshotting("10s"), progress: progressAgo(91 * time.Second), wantViolate: true},
		"10s annotation, Pending 45s":                    {job: pending("10s", 45*time.Second)},
		"10s annotation, Pending 89s":                    {job: pending("10s", 89*time.Second)},
		"10s annotation, Pending 91s":                    {job: pending("10s", 91*time.Second), wantViolate: true},
		"30m recorded, Pending 12m":                      {job: pending("30m0s", 12*time.Minute)},
		"30m recorded, Pending 31m":                      {job: pending("30m0s", 31*time.Minute), wantViolate: true},
		"3h annotation, Pending 2h0m29s":                 {job: pending("3h", 2*time.Hour+29*time.Second)},
		"3h annotation, Pending 2h0m31s":                 {job: pending("3h", 2*time.Hour+31*time.Second), wantViolate: true},
		"invalid annotation, Pending 10m29s":             {job: pending("soon", 10*time.Minute+29*time.Second)},
		"invalid annotation, Pending 10m31s":             {job: pending("soon", 10*time.Minute+31*time.Second), wantViolate: true},
	} {
		t.Run(name, func(t *testing.T) {
			vs := EvaluateI4(&ReconcileSnapshot{Now: now, PrimaryPMJ: tc.job, SnapshotProgress: tc.progress})
			if !tc.wantViolate {
				if len(vs) != 0 {
					t.Fatalf("expected no I4 violation, got %+v", vs)
				}
				return
			}
			if len(vs) != 1 || vs[0].InvariantID != "I4" || vs[0].Reason != "ActivePMJExceededDeadline" {
				t.Fatalf("expected one I4 ActivePMJExceededDeadline violation, got %+v", vs)
			}
		})
	}
}
