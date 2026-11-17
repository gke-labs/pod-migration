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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// recordSourceNode records in job's status the machine type and the kubelet
// version of nodeName, the node the source pod runs on (#14). A checkpoint
// only restores on a node of a compatible machine type and, for GKE Pod
// Snapshots, the same GKE version, and the source node can be gone by the
// time the replacement is released, so these facts are captured before the
// checkpoint.
//
// It only sets job.Status in memory, like the caller does for
// status.originNodeName, so the values go out with the status write that
// already ends the Pending pass and cost no write of their own. Only the
// controller is granted write access to the status subresource, so editing
// the job's metadata or spec (its annotations, say) can't change them.
//
// It is best effort and never holds the job back. It reads the node from the
// cache; a node it can't read leaves the missing fields empty, and a node
// without the label leaves the machine type empty. It never replaces a
// recorded value.
func (r *PodMigrationJobReconciler) recordSourceNode(ctx context.Context, job *pmv1alpha1.PodMigrationJob, nodeName string) {
	if nodeName == "" {
		return
	}
	if job.Status.OriginNodeInstanceType != "" && job.Status.OriginNodeKubeletVersion != "" {
		return
	}
	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		log.FromContext(ctx).Info("Couldn't read the source node; not recording its machine type and version",
			"node", nodeName, "error", err.Error())
		return
	}
	if job.Status.OriginNodeInstanceType == "" {
		job.Status.OriginNodeInstanceType = node.Labels[corev1.LabelInstanceTypeStable]
	}
	if job.Status.OriginNodeKubeletVersion == "" {
		job.Status.OriginNodeKubeletVersion = node.Status.NodeInfo.KubeletVersion
	}
}
