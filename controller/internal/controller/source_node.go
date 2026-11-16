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
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// recordSourceNode records on job the machine type and the kubelet version of
// nodeName, the node the source pod runs on (#14). A checkpoint only restores
// on a node of a compatible shape and, for GKE Pod Snapshots, the same GKE
// version, and the source node can be gone by the time the replacement is
// released, so these facts are captured before the checkpoint.
//
// It is best effort and never holds the job back. It reads the node from the
// cache; a node it can't read, or a value the node doesn't carry, leaves that
// annotation unset. It writes only the missing annotations, with a merge patch
// that doesn't conflict with other writers, and only logs when the patch
// fails. It never overwrites a non-empty annotation. On success it copies the
// patched metadata into job and origJob, so the caller's later writes start
// from it; job's in-memory status is left alone.
func (r *PodMigrationJobReconciler) recordSourceNode(ctx context.Context, job, origJob *pmv1alpha1.PodMigrationJob, nodeName string) {
	missing := r.sourceNodeAnnotations(ctx, job, nodeName)
	if len(missing) == 0 {
		return
	}

	patched := job.DeepCopy()
	if patched.Annotations == nil {
		patched.Annotations = make(map[string]string)
	}
	for key, value := range missing {
		patched.Annotations[key] = value
	}
	if err := r.Patch(ctx, patched, client.MergeFrom(job)); err != nil {
		log.FromContext(ctx).Info("Couldn't record the source node's machine type and version; continuing without them",
			"node", nodeName, "error", err.Error())
		return
	}
	job.ObjectMeta = *patched.ObjectMeta.DeepCopy()
	if origJob != nil {
		origJob.ObjectMeta = *patched.ObjectMeta.DeepCopy()
	}
}

// sourceNodeAnnotations returns the source node annotations that job doesn't
// have yet and that nodeName can provide: its node.kubernetes.io/instance-type
// label and its kubelet version. It returns nil when there's nothing to add,
// including when the node can't be read.
func (r *PodMigrationJobReconciler) sourceNodeAnnotations(ctx context.Context, job *pmv1alpha1.PodMigrationJob, nodeName string) map[string]string {
	if nodeName == "" {
		return nil
	}
	haveType := job.Annotations[util.AnnotationSourceInstanceType] != ""
	haveVersion := job.Annotations[util.AnnotationSourceNodeVersion] != ""
	if haveType && haveVersion {
		return nil
	}

	node := &corev1.Node{}
	if err := r.Get(ctx, types.NamespacedName{Name: nodeName}, node); err != nil {
		log.FromContext(ctx).Info("Couldn't read the source node; not recording its machine type and version",
			"node", nodeName, "error", err.Error())
		return nil
	}

	missing := make(map[string]string)
	if instanceType := node.Labels[corev1.LabelInstanceTypeStable]; !haveType && instanceType != "" {
		missing[util.AnnotationSourceInstanceType] = instanceType
	}
	if version := node.Status.NodeInfo.KubeletVersion; !haveVersion && version != "" {
		missing[util.AnnotationSourceNodeVersion] = version
	}
	return missing
}
