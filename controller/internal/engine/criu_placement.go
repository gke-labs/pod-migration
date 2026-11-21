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

package engine

import (
	"context"
	"errors"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

var _ PlacementAdvisor = (*CRIU)(nil)

// Placement implements PlacementAdvisor (C8, #257). A CRIU checkpoint only
// restores on a node compatible with the one it was taken on
// (criuplacement.Compatible): the same CPU vendor and model, the source's
// x86 instruction set and XSAVE state, and the same or newer kernel, CRIU,
// runc and containerd. The CRIU node agent records the source node's facts in
// the job's PodSnapshot, status.sourceNode, and publishes every node's own
// facts as labels, so the constraints are label requirements on those
// (criuplacement.Requirements). They depend only on the snapshot, so they're
// the same each time for a job.
//
// No constraints when the snapshot is gone, has no source facts, or was
// taken by an agent that doesn't publish the labels (an agent from before
// #257): placement then works as before, on any node. A fact unknown on the
// source adds no constraint, and one unknown on a node is published so that
// it meets every constraint, so unknown facts never exclude a node. A read
// error other than NotFound is returned, for the pod gate to retry.
func (e *CRIU) Placement(ctx context.Context, job *pmv1alpha1.PodMigrationJob) ([]corev1.NodeSelectorRequirement, error) {
	if job == nil {
		return nil, errors.New("placing a restore needs a job")
	}
	if job.Status.SnapshotRef == "" {
		return nil, nil
	}
	ps := &unstructured.Unstructured{}
	ps.SetGroupVersionKind(snapshot.PodSnapshotGVK(e.APIGroup()))
	if err := e.reader.Get(ctx, types.NamespacedName{Namespace: job.Namespace, Name: job.Status.SnapshotRef}, ps); err != nil {
		if apierrors.IsNotFound(err) || meta.IsNoMatchError(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading PodSnapshot %s/%s for the restore's placement: %w", job.Namespace, job.Status.SnapshotRef, err)
	}
	logger := log.FromContext(ctx).WithValues("pmj", job.Name, "snapshot", job.Status.SnapshotRef)
	facts, ok := criuplacement.SourceFacts(ps)
	if !ok {
		logger.V(1).Info("The CRIU snapshot records no source node facts, so the restore gets no node constraints")
		return nil, nil
	}
	reqs := criuplacement.Requirements(facts)
	if len(reqs) == 0 {
		if facts.PlacementLabels != criuplacement.PlacementLabelsVersion {
			logger.Info("The CRIU node agent that took the snapshot doesn't publish placement labels in a format this controller reads, so the restore gets no node constraints",
				"placementLabels", facts.PlacementLabels, "want", criuplacement.PlacementLabelsVersion)
		}
		return nil, nil
	}
	if unknown := criuplacement.Unknown(facts); len(unknown) > 0 {
		logger.V(1).Info("Some source node facts are unknown, so nodes aren't checked for them", "unknown", unknown)
	}
	return reqs, nil
}
