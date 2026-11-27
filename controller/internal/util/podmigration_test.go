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
	"strings"
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
		{"gs://b/ a /x", "", "", true},
		{"gs://b//ns", "", "", true},
		{"gs://b///ns", "", "", true},
		{"gs://b/foo /bar", "", "", true},
		{"gs://b/foo/ bar", "", "", true},
		{"gs:// b/foo", "", "", true},
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

func mustBucketRootBindings(t *testing.T, pairs map[string]string) BucketRootBindings {
	t.Helper()
	b, err := NewBucketRootBindings(pairs)
	if err != nil {
		t.Fatalf("NewBucketRootBindings(%v): %v", pairs, err)
	}
	return b
}

func TestValidateStorageLocationForNamespace(t *testing.T) {
	bound := mustBucketRootBindings(t, map[string]string{"tenant-a": "my-bucket"})
	none := BucketRootBindings{}
	tests := []struct {
		name        string
		location    string
		namespace   string
		bindings    BucketRootBindings
		expectError bool
		reserved    bool
	}{
		{name: "root bucket rejected without opt-in", location: "gs://my-bucket", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "root bucket with trailing slash rejected without opt-in", location: "gs://my-bucket/", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "root of the bound bucket allowed for its namespace", location: "gs://my-bucket", namespace: "tenant-a", bindings: bound},
		{name: "root of the bound bucket with trailing slash allowed for its namespace", location: "gs://my-bucket/", namespace: "tenant-a", bindings: bound},
		{name: "bound namespace may still scope a location on its own bucket", location: "gs://my-bucket/tenant-a/snapshots", namespace: "tenant-a", bindings: bound},
		{name: "bound namespace can't claim the root of another bucket", location: "gs://other-bucket", namespace: "tenant-a", bindings: bound, expectError: true},
		{name: "bound namespace keeps the prefix rule on other buckets", location: "gs://other-bucket/tenant-a", namespace: "tenant-a", bindings: bound},
		{name: "other namespace refused the root of a bound bucket", location: "gs://my-bucket", namespace: "tenant-b", bindings: bound, expectError: true, reserved: true},
		{name: "other namespace refused a scoped location on a bound bucket", location: "gs://my-bucket/tenant-b", namespace: "tenant-b", bindings: bound, expectError: true, reserved: true},
		{name: "other namespace refused a bound bucket whatever the case", location: "gs://My-Bucket/tenant-b", namespace: "tenant-b", bindings: bound, expectError: true, reserved: true},
		{name: "other namespace keeps scoped locations on unbound buckets", location: "gs://other-bucket/tenant-b", namespace: "tenant-b", bindings: bound},
		{name: "exact namespace path", location: "gs://my-bucket/tenant-a", namespace: "tenant-a", bindings: none},
		{name: "nested namespace path", location: "gs://my-bucket/tenant-a/snapshots", namespace: "tenant-a", bindings: none},
		{name: "cross-namespace mismatch", location: "gs://my-bucket/tenant-b", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "cross-namespace nested mismatch", location: "gs://my-bucket/tenant-b/snapshots", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "partial name match mismatch", location: "gs://my-bucket/tenant-a-other", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "invalid syntax dot-dot", location: "gs://my-bucket/tenant-a/../tenant-b", namespace: "tenant-a", bindings: none, expectError: true},
		{name: "invalid whitespace inside segment", location: "gs://my-bucket/ tenant-a /snapshots", namespace: "tenant-a", bindings: none, expectError: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateStorageLocationForNamespace(tc.location, tc.namespace, tc.bindings)
			if (err != nil) != tc.expectError {
				t.Fatalf("ValidateStorageLocationForNamespace(%q, %q, %v) error = %v, expectError = %v", tc.location, tc.namespace, tc.bindings.List(), err, tc.expectError)
			}
			var reservedErr *BucketReservedError
			if got := errors.As(err, &reservedErr); got != tc.reserved {
				t.Errorf("ValidateStorageLocationForNamespace(%q, %q) error = %v, want BucketReservedError = %v", tc.location, tc.namespace, err, tc.reserved)
			}
		})
	}
}

func TestParseBucketRootBindings(t *testing.T) {
	tests := []struct {
		name      string
		allow     bool
		raw       string
		want      []string
		expectErr bool
	}{
		{name: "default binds nothing", raw: "", want: nil},
		{name: "list alone binds", raw: "ns1=bucket-a,ns2=bucket-b", want: []string{"ns1=bucket-a", "ns2=bucket-b"}},
		{name: "boolean with a list binds only the list", allow: true, raw: "ns1=bucket-a", want: []string{"ns1=bucket-a"}},
		{name: "spaces and empty entries are ignored", raw: " ns2 = bucket-b , ,ns1=bucket-a,", want: []string{"ns1=bucket-a", "ns2=bucket-b"}},
		{name: "repeated identical entry collapses", raw: "ns1=bucket-a,ns1=bucket-a", want: []string{"ns1=bucket-a"}},
		{name: "dotted bucket name", raw: "ns1=snapshots.example.com", want: []string{"ns1=snapshots.example.com"}},
		{name: "boolean without a list is refused", allow: true, raw: "", expectErr: true},
		{name: "boolean with only separators is refused", allow: true, raw: " , ", expectErr: true},
		{name: "two namespaces bound to one bucket are refused", raw: "ns1=bucket-a,ns2=bucket-a", expectErr: true},
		{name: "two namespaces bound to one bucket in different case are refused", raw: "ns1=bucket-a,ns2=Bucket-A", expectErr: true},
		{name: "one namespace bound to two buckets is refused", raw: "ns1=bucket-a,ns1=bucket-b", expectErr: true},
		{name: "entry without a bucket is refused", raw: "ns1", expectErr: true},
		{name: "entry with an empty bucket is refused", raw: "ns1=", expectErr: true},
		{name: "entry with an empty namespace is refused", raw: "=bucket-a", expectErr: true},
		{name: "invalid namespace name is refused", raw: "Not_A_Namespace=bucket-a", expectErr: true},
		{name: "wildcard namespace is refused", raw: "*=bucket-a", expectErr: true},
		{name: "bucket with a path is refused", raw: "ns1=bucket-a/ns1", expectErr: true},
		{name: "bucket with a scheme is refused", raw: "ns1=gs://bucket-a", expectErr: true},
		{name: "too short bucket name is refused", raw: "ns1=ab", expectErr: true},
		{name: "upper-case bucket name is refused", raw: "ns1=bUcket-a", expectErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseBucketRootBindings(tc.allow, tc.raw)
			if tc.expectErr {
				if err == nil {
					t.Fatalf("ParseBucketRootBindings(%v, %q) = %v, want error", tc.allow, tc.raw, got.List())
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseBucketRootBindings(%v, %q) error = %v", tc.allow, tc.raw, err)
			}
			if gotList := got.List(); strings.Join(gotList, ",") != strings.Join(tc.want, ",") {
				t.Errorf("ParseBucketRootBindings(%v, %q) = %v, want %v", tc.allow, tc.raw, gotList, tc.want)
			}
		})
	}
}

func TestBucketRootBindingsZeroValueBindsNothing(t *testing.T) {
	var b BucketRootBindings
	if owner, ok := b.RootOwner("any-bucket"); ok {
		t.Errorf("zero BucketRootBindings reports %q as the root owner of any-bucket", owner)
	}
	if err := ValidateStorageLocationForNamespace("gs://any-bucket", "default", b); err == nil {
		t.Errorf("zero BucketRootBindings allows a bucket-root location")
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
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket"},
		},
	}
	newer := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-b", Name: "mig", CreationTimestamp: metav1.NewTime(created),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket"},
		},
	}
	distinctPath := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "tenant-c", Name: "mig", CreationTimestamp: metav1.NewTime(created),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://other-bucket/tenant-c"},
		},
	}

	items := []pmv1alpha1.PodMigration{*older, *newer, *distinctPath}

	// tenant-a is bound to shared-bucket, so its root claim passes its own check and,
	// being older, takes precedence over tenant-b's (which its own reconcile refuses).
	bindA := mustBucketRootBindings(t, map[string]string{"tenant-a": "shared-bucket"})
	conflict := FindConflictingStoragePodMigration(newer, items, bindA)
	if conflict == nil || conflict.Namespace != "tenant-a" {
		t.Errorf("FindConflictingStoragePodMigration(newer) = %v, want tenant-a", conflict)
	}

	// For older, newer does NOT precede older, so older has no conflict
	if conflict := FindConflictingStoragePodMigration(older, items, bindA); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(older) = %v, want nil", conflict)
	}

	// For distinctPath, different subpath in bucket -> no conflict
	if conflict := FindConflictingStoragePodMigration(distinctPath, items, bindA); conflict != nil {
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
	if conflict := FindConflictingStoragePodMigration(newer, itemsWithDuplicate, bindA); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(newer) = %v, expected nil because tenant-d duplicate is inactive", conflict)
	}

	// Point 2 verification: An unscoped older candidate in tenant-a (gs://shared-bucket/tenant-b)
	// must NOT block a valid candidate in tenant-b (gs://shared-bucket/tenant-b).
	unscopedOlder := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "tenant-a",
			Name:              "mig-unscoped-older",
			CreationTimestamp: metav1.NewTime(created.Add(-5 * time.Hour)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/tenant-b/snapshots"},
		},
	}
	validTenantB := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "tenant-b",
			Name:              "mig-valid-b",
			CreationTimestamp: metav1.NewTime(created),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://shared-bucket/tenant-b/snapshots"},
		},
	}
	itemsWithUnscopedOlder := []pmv1alpha1.PodMigration{*unscopedOlder, *validTenantB}
	if conflict := FindConflictingStoragePodMigration(validTenantB, itemsWithUnscopedOlder, BucketRootBindings{}); conflict != nil {
		t.Errorf("FindConflictingStoragePodMigration(validTenantB) = %v, expected nil because tenant-a candidate is unscoped", conflict)
	}
}

// TestFindConflictingStoragePodMigration_BucketRootBindings covers the
// --allow-bucket-root-storage-namespaces bindings in conflict precedence: a
// PodMigration in another namespace on a bucket bound to a root namespace fails its
// own check, so it never takes precedence over the root namespace, whatever its age
// and whether its location is scoped or not.
func TestFindConflictingStoragePodMigration_BucketRootBindings(t *testing.T) {
	created := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pm := func(namespace, location string, age time.Duration) *pmv1alpha1.PodMigration {
		return &pmv1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace, Name: "mig", CreationTimestamp: metav1.NewTime(created.Add(-age)),
			},
			Spec: pmv1alpha1.PodMigrationSpec{Storage: pmv1alpha1.StorageSpec{Location: location}},
		}
	}
	bindA := mustBucketRootBindings(t, map[string]string{"tenant-a": "shared-bucket"})
	root := pm("tenant-a", "gs://shared-bucket", time.Hour)
	for _, location := range []string{"gs://shared-bucket/tenant-b", "gs://shared-bucket", "gs://shared-bucket/"} {
		olderOther := pm("tenant-b", location, 2*time.Hour)
		if conflict := FindConflictingStoragePodMigration(root, []pmv1alpha1.PodMigration{*olderOther, *root}, bindA); conflict != nil {
			t.Errorf("FindConflictingStoragePodMigration(root) with older tenant-b at %q = %s/%s, want nil: shared-bucket is bound to tenant-a", location, conflict.Namespace, conflict.Name)
		}
	}

	// Without the binding, an older scoped location still takes precedence over a
	// root claim by age (here the root claim fails its own check, but
	// FindConflictingStoragePodMigration doesn't check the target).
	scopedOlder := pm("tenant-b", "gs://shared-bucket/tenant-b", 2*time.Hour)
	if conflict := FindConflictingStoragePodMigration(root, []pmv1alpha1.PodMigration{*scopedOlder, *root}, BucketRootBindings{}); conflict == nil || conflict.Namespace != "tenant-b" {
		t.Errorf("FindConflictingStoragePodMigration(root) without bindings = %v, want tenant-b/mig", conflict)
	}
}
