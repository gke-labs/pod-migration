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

// Addon-detection preflight: the CRIU engine must never contend with another
// provider for the contract CRDs of its API group (contract.Group). On its
// dedicated default group (criu.podmigration.gke.io), the engine coexists
// cleanly alongside the managed GKE Pod Snapshots addon (podsnapshot.gke.io),
// because CRDs, triggers, restore annotations, and RestoreInjector webhook
// mutations are scoped strictly to CRIU-routed replacement pods (#135). The
// contract CRDs of contract.Group must exist and carry our component label
// (installed from engines/criu/deploy/crds.yaml); a foreign (unlabeled) CRD
// means another provider owns that group, so the engine refuses to start.

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// ComponentLabel marks resources installed by this engine.
const (
	ComponentLabel      = "pod-migrate.io/component"
	ComponentLabelValue = "psengine"
)

// Preflight runs the engine's startup checks (VerifyCRDOwnership) for
// contract.Group. cmd/engine runs it before the manager starts, because the
// manager starts the webhook server ahead of leader-elected runnables.
func Preflight(ctx context.Context, c client.Reader) error {
	return VerifyCRDOwnership(ctx, c)
}

// VerifyCRDOwnership fails when the contract CRDs of the engine's API group
// are missing (apply engines/criu/deploy/crds.yaml first) or foreign-owned
// (for example, when --api-group=podsnapshot.gke.io is passed on an
// addon-enabled cluster — this engine must not share that group with the addon).
func VerifyCRDOwnership(ctx context.Context, c client.Reader) error {
	return verifyCRDOwnership(ctx, c, contract.Group)
}

func verifyCRDOwnership(ctx context.Context, c client.Reader, group string) error {
	for _, name := range contract.CRDNames(group) {
		crd, err := getCRD(ctx, c, name)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("preflight: CRD %s not installed — apply engines/criu/deploy/crds.yaml first %s", name, installScope(group))
			}
			return fmt.Errorf("preflight: reading CRD %s: %w", name, err)
		}
		if crd.GetLabels()[ComponentLabel] != ComponentLabelValue {
			return fmt.Errorf("preflight: CRD %s exists but is not ours (missing %s=%s)%s — "+
				"refusing to start psengine: two engines double-executing PodSnapshotManualTriggers would corrupt migrations. "+
				"Either use the dedicated %s API group, or disable the other owner",
				name, ComponentLabel, ComponentLabelValue, describeForeignOwner(crd), contract.CRIUGroup)
		}
		if ownerDesc := describeForeignOwner(crd); ownerDesc != ownerUnknown {
			return fmt.Errorf("preflight: CRD %s has our component label but also carries foreign markers (%s) — it appears engines/criu/deploy/crds.yaml was applied over an existing GKE Pod Snapshots addon CRD! Refusing to start.", name, ownerDesc)
		}
	}
	return nil
}

// getCRD reads the CustomResourceDefinition name, unstructured.
func getCRD(ctx context.Context, c client.Reader, name string) (*unstructured.Unstructured, error) {
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{
		Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition",
	})
	return crd, c.Get(ctx, types.NamespacedName{Name: name}, crd)
}

// ownerUnknown is what describeForeignOwner reports for a CRD without any
// addon markers.
const ownerUnknown = "; owner unknown (no addon markers found)"

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
	return ownerUnknown
}

// installScope says where the CRDs of group belong: CRDs in the GKE Pod
// Snapshots group only on clusters where the addon is off, and for any other
// group, crds.yaml has to carry that group.
func installScope(group string) string {
	if group == contract.GKEPodSnapshotsGroup {
		return "(addon-off clusters only)"
	}
	return fmt.Sprintf("with its group set to %s (the engine's --api-group)", group)
}
