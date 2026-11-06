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

package psengine

// Addon-detection preflight: the CRIU engine must never contend with the
// managed GKE Pod Snapshots addon for the podsnapshot.gke.io group. The CRDs
// must exist and carry our component label (installed from
// engines/criu/deploy/crds.yaml). A foreign (unlabeled) CRD means the addon —
// or another provider — owns the group: refuse to start rather than
// double-execute PSMTs.

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ComponentLabel marks resources installed by this engine.
const (
	ComponentLabel      = "pod-migrate.io/component"
	ComponentLabelValue = "psengine"
)

var contractCRDs = []string{
	"podsnapshotstorageconfigs.podsnapshot.gke.io",
	"podsnapshotpolicies.podsnapshot.gke.io",
	"podsnapshotmanualtriggers.podsnapshot.gke.io",
	"podsnapshots.podsnapshot.gke.io",
}

// VerifyCRDOwnership fails when the contract CRDs are missing (apply
// engines/criu/deploy/crds.yaml first) or foreign-owned (addon-enabled
// cluster — this engine must not run there until coexistence rules land).
func VerifyCRDOwnership(ctx context.Context, c client.Reader) error {
	for _, name := range contractCRDs {
		crd := &unstructured.Unstructured{}
		crd.SetGroupVersionKind(schema.GroupVersionKind{
			Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition",
		})
		if err := c.Get(ctx, types.NamespacedName{Name: name}, crd); err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("preflight: CRD %s not installed — apply engines/criu/deploy/crds.yaml first (addon-off clusters only)", name)
			}
			return fmt.Errorf("preflight: reading CRD %s: %w", name, err)
		}
		if crd.GetLabels()[ComponentLabel] != ComponentLabelValue {
			return fmt.Errorf("preflight: CRD %s exists but is not ours (missing %s=%s)%s — "+
				"refusing to start psengine: two engines double-executing PodSnapshotManualTriggers would corrupt migrations. "+
				"Either disable the other owner and apply engines/criu/deploy/crds.yaml, "+
				"or run psengine on an addon-off cluster",
				name, ComponentLabel, ComponentLabelValue, describeForeignOwner(crd))
		}
		if ownerDesc := describeForeignOwner(crd); ownerDesc != "; owner unknown (no addon markers found)" {
			return fmt.Errorf("preflight: CRD %s has our component label but also carries foreign markers (%s) — it appears engines/criu/deploy/crds.yaml was applied over an existing GKE Pod Snapshots addon CRD! Refusing to start.", name, ownerDesc)
		}
	}
	return nil
}

// describeForeignOwner inspects ownership markers on a foreign CRD to make
// the refusal actionable: the managed GKE Pod Snapshots addon stamps
// addon-manager labels and writes via google-authored field managers.
// Absent markers are reported as unknown — never invented.
func describeForeignOwner(crd *unstructured.Unstructured) string {
	if mode, ok := crd.GetLabels()["addonmanager.kubernetes.io/mode"]; ok {
		return fmt.Sprintf("; owner looks like the managed GKE Pod Snapshots addon (addonmanager.kubernetes.io/mode=%s)", mode)
	}
	if _, ok := crd.GetLabels()["app.kubernetes.io/managed-by"]; ok {
		return fmt.Sprintf("; owner: app.kubernetes.io/managed-by=%s", crd.GetLabels()["app.kubernetes.io/managed-by"])
	}
	for _, mf := range crd.GetManagedFields() {
		m := strings.ToLower(mf.Manager)
		if strings.Contains(m, "google") || strings.Contains(m, "gke") || strings.Contains(m, "addon") {
			return fmt.Sprintf("; owner looks like the managed GKE addon (field manager %q)", mf.Manager)
		}
	}
	return "; owner unknown (no addon markers found)"
}
