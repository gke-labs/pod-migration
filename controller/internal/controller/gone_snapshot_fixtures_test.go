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
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// withReadySnapshots returns objs plus a Ready PodSnapshot, in each of
// groups (the GKE Pod Snapshots group if none), for every job in objs that
// names one in status.snapshotRef. A job only reaches Evicting on a Ready
// snapshot, so a fixture with a Restoring job needs it: the pod gate holds a
// replacement whose snapshot is gone (#269).
func withReadySnapshots[T runtime.Object](objs []T, groups ...string) []T {
	if len(groups) == 0 {
		groups = []string{snapshot.DefaultAPIGroup}
	}
	out := append([]T{}, objs...)
	seen := map[string]bool{}
	for _, o := range objs {
		job, ok := any(o).(*pmv1alpha1.PodMigrationJob)
		if !ok || job == nil || job.Status.SnapshotRef == "" {
			continue
		}
		for _, group := range groups {
			key := group + "/" + job.Namespace + "/" + job.Status.SnapshotRef
			if seen[key] {
				continue
			}
			seen[key] = true
			out = append(out, any(readySnapshot(group, job.Namespace, job.Status.SnapshotRef)).(T))
		}
	}
	return out
}

// readySnapshot returns a Ready PodSnapshot.
func readySnapshot(group, namespace, name string) *unstructured.Unstructured {
	snap := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"conditions": []interface{}{map[string]interface{}{
			"type": "Ready", "status": "True", "reason": "AllSnapshotsAvailable",
			"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
		}}},
	}}
	snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
	snap.SetNamespace(namespace)
	snap.SetName(name)
	return snap
}
