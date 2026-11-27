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

package util

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// A new job always records the timeout it's held to, read back as the same
// duration, including when that is the controller's --migration-timeout
// (#330).
func TestBuildPMJLabelsAndAnnotations_AlwaysStampsEffectiveTimeout(t *testing.T) {
	podWith := func(timeout string, mem string) *corev1.Pod {
		pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"}}
		if timeout != "" {
			pod.Annotations = map[string]string{AnnotationMigrationTimeout: timeout}
		}
		if mem != "" {
			pod.Spec.Containers = []corev1.Container{{
				Name: "c",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(mem)},
				},
			}}
		}
		return pod
	}

	for name, tc := range map[string]struct {
		pod  *corev1.Pod
		base time.Duration
		want string
	}{
		"no pod annotation, no memory: the flag's value": {
			pod: podWith("", ""), base: 30 * time.Minute, want: "30m0s",
		},
		"no pod annotation, no memory, default flag": {
			pod: podWith("", ""), base: 0, want: "10m0s",
		},
		"pod annotation below the minimum is stamped clamped": {
			pod: podWith("10s", ""), base: 10 * time.Minute, want: "1m0s",
		},
		"pod annotation above the maximum is stamped clamped": {
			pod: podWith("3h", ""), base: 10 * time.Minute, want: "2h0m0s",
		},
		"pod annotation in range": {
			pod: podWith("25m", ""), base: 10 * time.Minute, want: "25m0s",
		},
		"pod annotation equal to the flag": {
			pod: podWith("10m", ""), base: 10 * time.Minute, want: "10m0s",
		},
		"malformed pod annotation, no memory: the flag's value": {
			pod: podWith("soon", ""), base: 30 * time.Minute, want: "30m0s",
		},
		"memory scaling": {
			pod: podWith("", "16Gi"), base: 10 * time.Minute, want: "15m27s",
		},
		"Spot base": {
			pod: podWith("", ""), base: PreemptionDefaultTimeout, want: "2m0s",
		},
		// The Spot controller keeps a pod's own annotation, so a malformed one
		// leaves this value in place. Before #330 such a job had no
		// annotation and ran on --migration-timeout.
		"Spot base, malformed pod annotation": {
			pod: podWith("soon", ""), base: PreemptionDefaultTimeout, want: "2m0s",
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, ann := BuildPMJLabelsAndAnnotationsForPolicy(tc.pod, "web", "ReplicaSet", "rs-uid", "", tc.base, TriggerSourceEviction)
			got := ann[AnnotationMigrationTimeout]
			if got != tc.want {
				t.Fatalf("timeout annotation = %q, want %q", got, tc.want)
			}
			if d, ok := ParseClampedTimeout(got); !ok || d.String() != got {
				t.Errorf("annotation %q reads back as %v (ok=%v), not the stamped duration", got, d, ok)
			}
		})
	}
}

// A default outside [MinMigrationTimeout, MaxMigrationTimeout] would read
// back clamped to another value, so it isn't stamped. The controller refuses
// such a --migration-timeout; only tests build a reconciler with one.
func TestTimeoutAnnotationValue(t *testing.T) {
	for _, tc := range []struct {
		d      time.Duration
		want   string
		wantOK bool
	}{
		{d: MinMigrationTimeout, want: "1m0s", wantOK: true},
		{d: 30 * time.Minute, want: "30m0s", wantOK: true},
		{d: 15*time.Minute + 27*time.Second, want: "15m27s", wantOK: true},
		{d: MaxMigrationTimeout, want: "2h0m0s", wantOK: true},
		{d: 50 * time.Millisecond},
		{d: 59 * time.Second},
		{d: MaxMigrationTimeout + time.Second},
		{d: 0},
	} {
		got, ok := TimeoutAnnotationValue(tc.d)
		if got != tc.want || ok != tc.wantOK {
			t.Errorf("TimeoutAnnotationValue(%v) = (%q, %v), want (%q, %v)", tc.d, got, ok, tc.want, tc.wantOK)
		}
		if ok {
			if back, _ := ParseClampedTimeout(got); back != tc.d {
				t.Errorf("TimeoutAnnotationValue(%v) = %q reads back as %v", tc.d, got, back)
			}
		}
	}
}
