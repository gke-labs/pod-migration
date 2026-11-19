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
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// DefaultAPIGroup is the API group of the managed GKE Pod Snapshots add-on.
	DefaultAPIGroup = "podsnapshot.gke.io"

	// CRIUAPIGroup is the dedicated API group of the CRIU snapshot engine (#135).
	CRIUAPIGroup = "criu.podmigration.gke.io"

	// APIVersion is the API version of the four snapshot contract CRs.
	APIVersion = "v1"

	restoreAnnotationSuffix = "ps-name"

	// KindStorageConfig is the Kind of the cluster-scoped storage config CR.
	KindStorageConfig = "PodSnapshotStorageConfig"
	// KindPolicy is the Kind of the namespaced snapshot policy CR.
	KindPolicy = "PodSnapshotPolicy"
	// KindManualTrigger is the Kind of the namespaced manual checkpoint trigger CR.
	KindManualTrigger = "PodSnapshotManualTrigger"
	// KindPodSnapshot is the Kind of the namespaced snapshot CR.
	KindPodSnapshot = "PodSnapshot"
)

var crdPlurals = []string{
	"podsnapshotstorageconfigs",
	"podsnapshotpolicies",
	"podsnapshotmanualtriggers",
	"podsnapshots",
}

// ValidateGroup checks that group can be the API group of the snapshot
// contract CRDs: a DNS-1123 subdomain with at least one dot (apiextensions
// rejects a CRD group without one), short enough that every CRD name
// (<plural>.<group>) is also a valid DNS-1123 subdomain (<=253 characters).
func ValidateGroup(group string) error {
	if errs := validation.IsDNS1123Subdomain(group); len(errs) > 0 {
		return fmt.Errorf("invalid API group %q: %s", group, strings.Join(errs, "; "))
	}
	if !strings.Contains(group, ".") {
		return fmt.Errorf("invalid API group %q: a CustomResourceDefinition group must contain a dot", group)
	}
	for _, name := range CRDNames(group) {
		if errs := validation.IsDNS1123Subdomain(name); len(errs) > 0 {
			return fmt.Errorf("invalid API group %q: CRD name %q: %s", group, name, strings.Join(errs, "; "))
		}
	}
	return nil
}

// RestoreAnnotation returns the restore-order annotation key of an engine
// serving group: "<group>/ps-name". An empty group defaults to DefaultAPIGroup.
func RestoreAnnotation(group string) string {
	if group == "" {
		group = DefaultAPIGroup
	}
	return group + "/" + restoreAnnotationSuffix
}

// CRDNames returns the names of the four snapshot contract CRDs in group.
func CRDNames(group string) []string {
	names := make([]string, 0, len(crdPlurals))
	for _, plural := range crdPlurals {
		names = append(names, plural+"."+group)
	}
	return names
}

func normalizeGroup(group string) string {
	if group == "" {
		return DefaultAPIGroup
	}
	return group
}

// StorageConfigGVK returns the GroupVersionKind for PodSnapshotStorageConfig in group.
func StorageConfigGVK(group string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: normalizeGroup(group), Version: APIVersion, Kind: KindStorageConfig}
}

// PolicyGVK returns the GroupVersionKind for PodSnapshotPolicy in group.
func PolicyGVK(group string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: normalizeGroup(group), Version: APIVersion, Kind: KindPolicy}
}

// ManualTriggerGVK returns the GroupVersionKind for PodSnapshotManualTrigger in group.
func ManualTriggerGVK(group string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: normalizeGroup(group), Version: APIVersion, Kind: KindManualTrigger}
}

// PodSnapshotGVK returns the GroupVersionKind for PodSnapshot in group.
func PodSnapshotGVK(group string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: normalizeGroup(group), Version: APIVersion, Kind: KindPodSnapshot}
}
