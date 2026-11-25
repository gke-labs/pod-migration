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
	"errors"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

func TestActivePodMigration(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pm := func(namespace, name string, age time.Duration) *pmv1alpha1.PodMigration {
		return &pmv1alpha1.PodMigration{ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace, Name: name, CreationTimestamp: metav1.NewTime(created.Add(-age)),
		}}
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
		pm("team-a", "newer", time.Minute),
		pm("team-a", "older", time.Hour),
		pm("team-b", "only", time.Minute),
	).Build()

	for namespace, want := range map[string]string{"team-a": "older", "team-b": "only", "team-c": ""} {
		t.Run(namespace, func(t *testing.T) {
			got, err := ActivePodMigration(context.Background(), c, namespace)
			if err != nil {
				t.Fatalf("ActivePodMigration: %v", err)
			}
			switch {
			case want == "" && got != nil:
				t.Errorf("ActivePodMigration(%s) = %s, want none", namespace, got.Name)
			case want != "" && (got == nil || got.Name != want):
				t.Errorf("ActivePodMigration(%s) = %v, want %s", namespace, got, want)
			}
		})
	}

	t.Run("list error", func(t *testing.T) {
		failing := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
				return errors.New("cache not synced")
			},
		}).Build()
		if got, err := ActivePodMigration(context.Background(), failing, "team-a"); err == nil || got != nil {
			t.Errorf("ActivePodMigration() = %v, %v; want the list error", got, err)
		}
	})
}

func TestParseStorageLocation(t *testing.T) {
	tests := []struct {
		input       string
		wantBucket  string
		wantPath    string
		expectError bool
	}{
		{"gs://my-bucket", "my-bucket", "", false},
		{"gs://my-bucket/", "my-bucket", "", false},
		{"gs://my-bucket/snapshots", "my-bucket", "snapshots", false},
		{"gs://my-bucket/snapshots/", "my-bucket", "snapshots", false},
		{"gs://my-bucket/tenant-a/subpath", "my-bucket", "tenant-a/subpath", false},
		{"gs://my-bucket/.", "", "", true},
		{"gs://my-bucket/..", "", "", true},
		{"gs://my-bucket/tenant-a/.", "", "", true},
		{"gs://my-bucket/tenant-a/..", "", "", true},
		{"gs://my-bucket/tenant-a/../tenant-b", "", "", true},
		{"gs://my-bucket/tenant-a/./sub", "", "", true},
		{"gs://my-bucket/tenant-a//sub", "", "", true},
		{"s3://my-bucket", "", "", true},
		{"gs://", "", "", true},
		{"gs:///path", "", "", true},
		{"not-a-url", "", "", true},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			b, p, err := ParseStorageLocation(tc.input)
			if (err != nil) != tc.expectError {
				t.Fatalf("ParseStorageLocation(%q) error = %v, expectError = %v", tc.input, err, tc.expectError)
			}
			if b != tc.wantBucket || p != tc.wantPath {
				t.Errorf("ParseStorageLocation(%q) = (%q, %q), want (%q, %q)", tc.input, b, p, tc.wantBucket, tc.wantPath)
			}
		})
	}
}

func TestValidateStorageLocationForNamespace(t *testing.T) {
	tests := []struct {
		name        string
		location    string
		namespace   string
		expectError bool
	}{
		{"root bucket allowed", "gs://my-bucket", "tenant-a", false},
		{"root bucket with trailing slash allowed", "gs://my-bucket/", "tenant-a", false},
		{"exact namespace path", "gs://my-bucket/tenant-a", "tenant-a", false},
		{"nested namespace path", "gs://my-bucket/tenant-a/snapshots", "tenant-a", false},
		{"cross-namespace mismatch", "gs://my-bucket/tenant-b", "tenant-a", true},
		{"cross-namespace nested mismatch", "gs://my-bucket/tenant-b/snapshots", "tenant-a", true},
		{"partial name match mismatch", "gs://my-bucket/tenant-a-other", "tenant-a", true},
		{"invalid syntax dot-dot", "gs://my-bucket/tenant-a/../tenant-b", "tenant-a", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStorageLocationForNamespace(tc.location, tc.namespace)
			if (err != nil) != tc.expectError {
				t.Errorf("ValidateStorageLocationForNamespace(%q, %q) error = %v, expectError = %v", tc.location, tc.namespace, err, tc.expectError)
			}
		})
	}
}

func TestStorageLocationsConflict(t *testing.T) {
	tests := []struct {
		name     string
		bA, pA   string
		bB, pB   string
		conflict bool
	}{
		{"different buckets", "bucket-a", "path", "bucket-b", "path", false},
		{"identical bucket and path", "bucket-a", "snapshots", "bucket-a", "snapshots", true},
		{"identical bucket root", "bucket-a", "", "bucket-a", "", true},
		{"root claims subpath", "bucket-a", "", "bucket-a", "tenant-b", true},
		{"subpath claims root", "bucket-a", "tenant-a", "bucket-a", "", true},
		{"distinct subpaths in same bucket", "bucket-a", "tenant-a", "bucket-a", "tenant-b", false},
		{"distinct nested subpaths", "bucket-a", "tenants/a", "bucket-a", "tenants/b", false},
		{"nested ancestor subpath conflict (A encompasses B)", "bucket-a", "tenant-a", "bucket-a", "tenant-a/nested", true},
		{"nested descendant subpath conflict (B encompasses A)", "bucket-a", "tenant-a/nested", "bucket-a", "tenant-a", true},
		{"nested prefix with partial name match is not conflict", "bucket-a", "tenant-a", "bucket-a", "tenant-a-other", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := StorageLocationsConflict(tc.bA, tc.pA, tc.bB, tc.pB)
			if got != tc.conflict {
				t.Errorf("StorageLocationsConflict(%q, %q, %q, %q) = %v, want %v", tc.bA, tc.pA, tc.bB, tc.pB, got, tc.conflict)
			}
		})
	}
}

func TestFindConflictingStoragePodMigration(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	older := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-a", Name: "mig", CreationTimestamp: metav1.NewTime(created.Add(-time.Hour)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/snapshots"},
		},
	}
	newer := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-b", Name: "mig", CreationTimestamp: metav1.NewTime(created),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/snapshots"},
		},
	}
	distinctPath := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-c", Name: "mig", CreationTimestamp: metav1.NewTime(created),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/tenant-c"},
		},
	}

	items := []pmv1alpha1.PodMigration{*older, *newer, *distinctPath}

	// For newer, older is a conflicting PM with precedence
	conflict := FindConflictingStoragePodMigration(newer, items)
	if conflict == nil || conflict.Namespace != "tenant-a" {
		t.Errorf("FindConflictingStoragePodMigration(newer) = %v, want tenant-a", conflict)
	}

	// For older, newer does NOT precede older, so older has no conflict
	if conflict := FindConflictingStoragePodMigration(older, items); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(older) = %v, want nil", conflict)
	}

	// For distinctPath, different subpath in bucket -> no conflict
	if conflict := FindConflictingStoragePodMigration(distinctPath, items); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(distinctPath) = %v, want nil", conflict)
	}

	// Inactive duplicate in another namespace without finalizer cannot DoS a valid PM
	inactiveDuplicate := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "tenant-d",
			Name:              "mig-duplicate",
			CreationTimestamp: metav1.NewTime(created.Add(-2 * time.Hour)), // older timestamp!
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/snapshots"},
		},
	}
	activeInTenantD := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "tenant-d",
			Name:              "mig-active",
			CreationTimestamp: metav1.NewTime(created.Add(-3 * time.Hour)), // elected active
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://other-bucket/snapshots"},
		},
	}

	itemsWithDuplicate := []pmv1alpha1.PodMigration{*activeInTenantD, *inactiveDuplicate, *newer}
	// newer should NOT be blocked by inactiveDuplicate because inactiveDuplicate is demoted in tenant-d and lacks finalizer
	if conflict := FindConflictingStoragePodMigration(newer, itemsWithDuplicate); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(newer) = %v, expected nil because tenant-d duplicate is inactive", conflict)
	}
}
