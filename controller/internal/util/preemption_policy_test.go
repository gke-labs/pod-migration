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
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// BuildPMJLabelsAndAnnotationsForPolicy records the same labels and
// annotations as BuildPMJLabelsAndAnnotations, given the policy's name.
func TestBuildPMJLabelsAndAnnotationsForPolicy(t *testing.T) {
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name:        "web-0",
		Namespace:   "default",
		Labels:      map[string]string{"pod-template-hash": "abc123"},
		Annotations: map[string]string{AnnotationMigrationTimeout: "20m"},
	}}
	psp := &unstructured.Unstructured{}
	psp.SetName("psp-web-manual")

	wantLabels, wantAnnotations := BuildPMJLabelsAndAnnotations(pod, "web", "ReplicaSet", "rs-uid", psp, 10*time.Minute, TriggerSourceEviction)
	gotLabels, gotAnnotations := BuildPMJLabelsAndAnnotationsForPolicy(pod, "web", "ReplicaSet", "rs-uid", "psp-web-manual", 10*time.Minute, TriggerSourceEviction)
	if !reflect.DeepEqual(gotLabels, wantLabels) {
		t.Errorf("labels = %v, want %v", gotLabels, wantLabels)
	}
	if !reflect.DeepEqual(gotAnnotations, wantAnnotations) {
		t.Errorf("annotations = %v, want %v", gotAnnotations, wantAnnotations)
	}
	if gotAnnotations[AnnotationPodSnapshotPolicy] != "psp-web-manual" {
		t.Errorf("policy annotation = %q, want psp-web-manual", gotAnnotations[AnnotationPodSnapshotPolicy])
	}

	// No policy name records no policy, as a nil policy object does.
	_, noName := BuildPMJLabelsAndAnnotationsForPolicy(pod, "web", "ReplicaSet", "rs-uid", "", 10*time.Minute, TriggerSourceEviction)
	_, nilPolicy := BuildPMJLabelsAndAnnotations(pod, "web", "ReplicaSet", "rs-uid", nil, 10*time.Minute, TriggerSourceEviction)
	if _, ok := noName[AnnotationPodSnapshotPolicy]; ok {
		t.Errorf("an empty policy name recorded a policy annotation: %v", noName)
	}
	if !reflect.DeepEqual(noName, nilPolicy) {
		t.Errorf("annotations without a policy name = %v, want %v (nil policy)", noName, nilPolicy)
	}
}
