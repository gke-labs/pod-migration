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
	"context"
	"fmt"
	"strings"

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
				if deletingWithFinalizer == nil || PodMigrationPrecedes(candidate, deletingWithFinalizer) {
					deletingWithFinalizer = candidate
				}
			}
			continue
		}
		if active == nil || PodMigrationPrecedes(candidate, active) {
			active = candidate
		}
	}
	if deletingWithFinalizer != nil {
		return deletingWithFinalizer
	}
	return active
}

// PodMigrationPrecedes reports whether a has precedence over b.
// The oldest CreationTimestamp wins; ties are broken lexicographically by namespace,
// then name.
func PodMigrationPrecedes(a, b *pmv1alpha1.PodMigration) bool {
	if a == nil {
		return false
	}
	if b == nil {
		return true
	}
	if !a.CreationTimestamp.Equal(&b.CreationTimestamp) {
		return a.CreationTimestamp.Before(&b.CreationTimestamp)
	}
	if a.Namespace != b.Namespace {
		return a.Namespace < b.Namespace
	}
	return a.Name < b.Name
}

// ParseStorageLocation parses a storage location URI (e.g. gs://bucket/path) into
// bucket and clean path without leading or trailing slashes. It returns an error if
// the location does not start with gs:// or has an empty bucket name.
func ParseStorageLocation(location string) (bucket, path string, err error) {
	if !strings.HasPrefix(location, "gs://") {
		return "", "", fmt.Errorf("invalid GCS location: %s (must start with gs://)", location)
	}
	trimmed := strings.TrimPrefix(location, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = strings.TrimSpace(parts[0])
	if bucket == "" {
		return "", "", fmt.Errorf("invalid GCS location: %s (missing bucket name)", location)
	}
	if len(parts) > 1 {
		path = strings.Trim(strings.TrimSpace(parts[1]), "/")
	}
	return bucket, path, nil
}

// StorageLocationsConflict reports whether two parsed GCS locations conflict.
// Locations conflict if they use the same bucket and either:
// 1. Both use the identical path (including both using the root bucket);
// 2. Either location targets the entire bucket (empty path), encompassing all subpaths;
// 3. One location is a parent/ancestor path of the other (e.g. "tenant-a" and "tenant-a/nested").
func StorageLocationsConflict(bucketA, pathA, bucketB, pathB string) bool {
	if bucketA == "" || bucketB == "" || bucketA != bucketB {
		return false
	}
	if pathA == "" || pathB == "" {
		return true
	}
	if pathA == pathB || strings.HasPrefix(pathB, pathA+"/") || strings.HasPrefix(pathA, pathB+"/") {
		return true
	}
	return false
}

// FindConflictingStoragePodMigration finds an active PodMigration in items (across namespaces)
// whose storage location conflicts with target and has precedence over target.
// Returns the conflicting PodMigration that precedes target, or nil if target is the winner
// or no conflict exists.
func FindConflictingStoragePodMigration(target *pmv1alpha1.PodMigration, items []pmv1alpha1.PodMigration) *pmv1alpha1.PodMigration {
	if target == nil {
		return nil
	}
	tBucket, tPath, err := ParseStorageLocation(target.Spec.Storage.Location)
	if err != nil {
		return nil
	}

	// Group items by namespace to identify the active singleton per namespace
	nsItems := make(map[string][]pmv1alpha1.PodMigration)
	for i := range items {
		ns := items[i].Namespace
		nsItems[ns] = append(nsItems[ns], items[i])
	}
	activePerNS := make(map[string]*pmv1alpha1.PodMigration)
	for ns, group := range nsItems {
		activePerNS[ns] = SelectActivePodMigration(group)
	}

	var oldestPreceding *pmv1alpha1.PodMigration
	for i := range items {
		other := &items[i]
		if (target.UID != "" && other.UID == target.UID) || (other.Namespace == target.Namespace && other.Name == target.Name) {
			continue
		}
		// A candidate must either hold the StorageCleanupFinalizer or be the active singleton of its namespace.
		// Duplicate/demoted PMs without finalizers are inactive and cannot block other namespaces.
		isActive := false
		if active := activePerNS[other.Namespace]; active != nil && active.Name == other.Name {
			isActive = true
		}
		hasFinalizer := controllerutil.ContainsFinalizer(other, StorageCleanupFinalizer)
		if !isActive && !hasFinalizer {
			continue
		}
		// If other is deleting and doesn't hold the finalizer, it is effectively gone
		if !other.DeletionTimestamp.IsZero() && !hasFinalizer {
			continue
		}

		oBucket, oPath, err := ParseStorageLocation(other.Spec.Storage.Location)
		if err != nil {
			continue
		}
		if StorageLocationsConflict(tBucket, tPath, oBucket, oPath) {
			if PodMigrationPrecedes(other, target) {
				if oldestPreceding == nil || PodMigrationPrecedes(other, oldestPreceding) {
					oldestPreceding = other
				}
			}
		}
	}
	return oldestPreceding
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
