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

package snapshot

import (
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// TriggeredByLabel is the label GKE Pod Snapshots puts on every PodSnapshot it
// creates for a manual trigger, with the trigger's name as its value. The CRIU
// engine's controller sets it too, when the trigger's name fits in a label
// value (#106).
const TriggeredByLabel = snapshotTriggeredByLabel

// CheckpointFinished reports whether a PodSnapshot's checkpoint has finished:
// it's Ready, or it failed terminally. A snapshot that's being deleted hasn't
// finished. The controller deletes a job's PodSnapshot only once its
// checkpoint finished, or once its source is gone (#107).
func CheckpointFinished(snap *unstructured.Unstructured) bool {
	if snap == nil {
		return false
	}
	_, finished := podSnapshotCheckpointProgress(snap)
	return finished
}
