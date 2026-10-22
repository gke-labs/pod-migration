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
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// StorageCleanupFinalizer is the finalizer added to PodMigration resources
// to ensure cluster-scoped PodSnapshotStorageConfig and associated policies are
// cleaned up when the PodMigration CR is deleted.
const StorageCleanupFinalizer = "podmigration.gke.io/storage-cleanup"

// SelectActivePodMigration deterministically elects the single active PodMigration
// CR in a namespace:
//  1. If any deleting PodMigration still holds StorageCleanupFinalizer, its
//     PodSnapshotPolicy is still present in the namespace, so it remains the
//     active owner until finalization completes.
//  2. Otherwise, the non-deleting PodMigration with the oldest CreationTimestamp
//     (tie-broken by lexicographically smallest metadata.name) wins.
func SelectActivePodMigration(items []pmv1alpha1.PodMigration) *pmv1alpha1.PodMigration {
	var deletingWithFinalizer *pmv1alpha1.PodMigration
	var active *pmv1alpha1.PodMigration
	for i := range items {
		candidate := &items[i]
		if !candidate.DeletionTimestamp.IsZero() {
			if controllerutil.ContainsFinalizer(candidate, StorageCleanupFinalizer) {
				if deletingWithFinalizer == nil || podMigrationPrecedes(candidate, deletingWithFinalizer) {
					deletingWithFinalizer = candidate
				}
			}
			continue
		}
		if active == nil || podMigrationPrecedes(candidate, active) {
			active = candidate
		}
	}
	if deletingWithFinalizer != nil {
		return deletingWithFinalizer
	}
	return active
}

func podMigrationPrecedes(a, b *pmv1alpha1.PodMigration) bool {
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	return a.Name < b.Name
}

// ActivePodMigration returns the active PodMigration of namespace, as
// SelectActivePodMigration elects it, from c (typically the informer cache).
// It returns nil when the namespace has no PodMigration.
func ActivePodMigration(ctx context.Context, c client.Reader, namespace string) (*pmv1alpha1.PodMigration, error) {
	var list pmv1alpha1.PodMigrationList
	if err := c.List(ctx, &list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return SelectActivePodMigration(list.Items), nil
}
