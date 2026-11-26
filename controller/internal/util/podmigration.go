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
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
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
// the location does not start with gs://, has an empty bucket name, contains whitespace,
// or contains invalid path segments ('.', '..', or empty segments from consecutive slashes).
func ParseStorageLocation(location string) (bucket, path string, err error) {
	if !strings.HasPrefix(location, "gs://") {
		return "", "", fmt.Errorf("invalid GCS location: %s (must start with gs://)", location)
	}
	trimmed := strings.TrimPrefix(location, "gs://")
	parts := strings.SplitN(trimmed, "/", 2)
	bucket = parts[0]
	if bucket == "" || strings.ContainsAny(bucket, " 	\r\n") {
		return "", "", fmt.Errorf("invalid GCS location: %s (missing or invalid bucket name)", location)
	}
	if len(parts) > 1 {
		if strings.HasPrefix(parts[1], "/") || strings.Contains(parts[1], "//") {
			return "", "", fmt.Errorf("invalid storage path %q: path segments must not be '.', '..', or empty", parts[1])
		}
		if strings.ContainsAny(parts[1], " 	\r\n") {
			return "", "", fmt.Errorf("invalid storage path %q: path segments must not contain whitespace", parts[1])
		}
		rawPath := strings.Trim(parts[1], "/")
		if rawPath != "" {
			segments := strings.Split(rawPath, "/")
			cleanSegments := make([]string, 0, len(segments))
			for _, seg := range segments {
				if seg == "" || seg == "." || seg == ".." {
					return "", "", fmt.Errorf("invalid storage path %q: path segments must not be '.', '..', or empty", rawPath)
				}
				cleanSegments = append(cleanSegments, seg)
			}
			path = strings.Join(cleanSegments, "/")
		}
	}
	return bucket, path, nil
}

// BucketRootBindings binds namespaces to the buckets whose root (gs://<bucket> or
// gs://<bucket>/) they may use as a storage location, set with
// --allow-bucket-root-storage-namespaces=<namespace>=<bucket>,... Each bucket is
// bound to at most one namespace, and each namespace to at most one bucket. A bound
// bucket is reserved for its namespace: every other namespace is refused any storage
// location on it, scoped or not. The zero value binds nothing.
type BucketRootBindings struct {
	bucketByNamespace map[string]string
	namespaceByBucket map[string]string
}

// NewBucketRootBindings builds bindings from namespace -> bucket pairs. It returns an
// error for an invalid namespace or bucket name, or when two namespaces are bound to
// the same bucket (bucket names compare case-insensitively).
func NewBucketRootBindings(pairs map[string]string) (BucketRootBindings, error) {
	b := BucketRootBindings{
		bucketByNamespace: map[string]string{},
		namespaceByBucket: map[string]string{},
	}
	namespaces := make([]string, 0, len(pairs))
	for ns := range pairs {
		namespaces = append(namespaces, ns)
	}
	sort.Strings(namespaces)
	for _, ns := range namespaces {
		if err := b.bind(ns, pairs[ns]); err != nil {
			return BucketRootBindings{}, err
		}
	}
	return b, nil
}

func (b *BucketRootBindings) bind(namespace, bucket string) error {
	if errs := validation.IsDNS1123Label(namespace); len(errs) > 0 {
		return fmt.Errorf("%q is not a valid namespace name: %s", namespace, strings.Join(errs, "; "))
	}
	if err := validateBucketName(bucket); err != nil {
		return err
	}
	key := strings.ToLower(bucket)
	if prev, ok := b.bucketByNamespace[namespace]; ok {
		if strings.ToLower(prev) == key {
			return nil
		}
		return fmt.Errorf("namespace %q is bound to two buckets, %q and %q; bind each namespace to one bucket", namespace, prev, bucket)
	}
	if owner, ok := b.namespaceByBucket[key]; ok {
		return fmt.Errorf("bucket %q is bound to two namespaces, %q and %q; a bucket-root storage location is only appropriate when exactly one namespace can create PodMigrations on that bucket", bucket, owner, namespace)
	}
	b.bucketByNamespace[namespace] = bucket
	b.namespaceByBucket[key] = namespace
	return nil
}

// validateBucketName checks a Cloud Storage bucket name: 3 to 222 characters of
// lowercase letters, digits, '-', '_' and '.', starting and ending with a letter or
// digit, with dot-separated components of 1 to 63 characters.
func validateBucketName(bucket string) error {
	if len(bucket) < 3 || len(bucket) > 222 {
		return fmt.Errorf("%q is not a valid bucket name: must be 3 to 222 characters", bucket)
	}
	for _, c := range bucket {
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("%q is not a valid bucket name: only lowercase letters, digits, '-', '_' and '.' are allowed (give the bucket name, not a gs:// location)", bucket)
		}
	}
	isAlnum := func(c byte) bool { return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' }
	if !isAlnum(bucket[0]) || !isAlnum(bucket[len(bucket)-1]) {
		return fmt.Errorf("%q is not a valid bucket name: must start and end with a letter or digit", bucket)
	}
	for _, part := range strings.Split(bucket, ".") {
		if part == "" || len(part) > 63 {
			return fmt.Errorf("%q is not a valid bucket name: dot-separated parts must be 1 to 63 characters", bucket)
		}
	}
	return nil
}

// RootOwner returns the namespace the bucket is bound to, if any. Bucket names
// compare case-insensitively.
func (b BucketRootBindings) RootOwner(bucket string) (string, bool) {
	ns, ok := b.namespaceByBucket[strings.ToLower(bucket)]
	return ns, ok
}

// List returns the bindings as sorted "<namespace>=<bucket>" entries.
func (b BucketRootBindings) List() []string {
	if len(b.bucketByNamespace) == 0 {
		return nil
	}
	out := make([]string, 0, len(b.bucketByNamespace))
	for ns, bucket := range b.bucketByNamespace {
		out = append(out, ns+"="+bucket)
	}
	sort.Strings(out)
	return out
}

// ParseBucketRootBindings builds the bindings from the
// --allow-bucket-root-storage-namespaces value (comma-separated
// <namespace>=<bucket> entries; spaces and empty entries are ignored) and the older
// --allow-bucket-root-storage boolean. The list alone is the opt-in. The boolean no
// longer allows any namespace by itself: when it is true the list must bind at least
// one namespace, so that a configuration written for the old flag fails at startup
// instead of either allowing every namespace or silently allowing none.
func ParseBucketRootBindings(allowBucketRoot bool, raw string) (BucketRootBindings, error) {
	b := BucketRootBindings{
		bucketByNamespace: map[string]string{},
		namespaceByBucket: map[string]string{},
	}
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		ns, bucket, ok := strings.Cut(entry, "=")
		ns, bucket = strings.TrimSpace(ns), strings.TrimSpace(bucket)
		if !ok || ns == "" || bucket == "" {
			return BucketRootBindings{}, fmt.Errorf("--allow-bucket-root-storage-namespaces: entry %q must be <namespace>=<bucket>", entry)
		}
		if err := b.bind(ns, bucket); err != nil {
			return BucketRootBindings{}, fmt.Errorf("--allow-bucket-root-storage-namespaces: %w", err)
		}
	}
	if allowBucketRoot && len(b.bucketByNamespace) == 0 {
		return BucketRootBindings{}, fmt.Errorf("--allow-bucket-root-storage=true needs --allow-bucket-root-storage-namespaces to bind each namespace that may use a bucket-root storage location to its bucket (<namespace>=<bucket>); it no longer applies to every namespace")
	}
	return b, nil
}

// BucketReservedError reports a storage location on a bucket that
// --allow-bucket-root-storage-namespaces binds to another namespace.
type BucketReservedError struct {
	Location  string
	Bucket    string
	Namespace string
	Owner     string
}

func (e *BucketReservedError) Error() string {
	return fmt.Sprintf("bucket %q is reserved for the bucket-root storage of namespace %q (--allow-bucket-root-storage-namespaces); namespace %q can't use storage location %q on it", e.Bucket, e.Owner, e.Namespace, e.Location)
}

// ValidateStorageLocationForNamespace checks that the storage location path is
// properly scoped to the given namespace for multi-tenant isolation.
// A bucket that bindings reserves for another namespace's bucket-root storage is
// refused first, whatever the path (*BucketReservedError).
// Otherwise the first path segment must match the namespace (e.g. gs://bucket/<namespace> or gs://bucket/<namespace>/...).
// An empty path (targeting the root bucket) is only allowed on the bucket that
// bindings binds to the namespace (explicit operator opt-in, --allow-bucket-root-storage-namespaces).
func ValidateStorageLocationForNamespace(location, namespace string, bindings BucketRootBindings) error {
	bucket, path, err := ParseStorageLocation(location)
	if err != nil {
		return err
	}
	owner, reserved := bindings.RootOwner(bucket)
	if reserved && owner != namespace {
		return &BucketReservedError{Location: location, Bucket: bucket, Namespace: namespace, Owner: owner}
	}
	if path == "" {
		if reserved {
			return nil
		}
		return fmt.Errorf("storage location %q targets the bucket root without a path; storage paths must be scoped to namespace %q (e.g. gs://<bucket>/%s) unless --allow-bucket-root-storage-namespaces binds the namespace to this bucket", location, namespace, namespace)
	}
	segments := strings.Split(path, "/")
	if segments[0] != namespace {
		return fmt.Errorf("storage location path %q is not scoped to namespace %q (first path segment must be %q)", path, namespace, namespace)
	}
	return nil
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
// Candidates that fail their own namespace check (including a bucket that bindings
// reserves for another namespace's bucket-root storage) or have invalid syntax are skipped.
// Returns the conflicting PodMigration that precedes target, or nil if target is the winner
// or no conflict exists.
func FindConflictingStoragePodMigration(target *pmv1alpha1.PodMigration, items []pmv1alpha1.PodMigration, bindings BucketRootBindings) *pmv1alpha1.PodMigration {
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
		// Skip candidates that fail their own namespace check
		if err := ValidateStorageLocationForNamespace(other.Spec.Storage.Location, other.Namespace, bindings); err != nil {
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
