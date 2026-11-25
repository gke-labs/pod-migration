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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// CRIUName is the registry name of the CRIU snapshot engine.
const CRIUName = "criu"

const (
	// CRIUPolicyLabel partitions PodSnapshotPolicies and storage configs owned by
	// the CRIU engine from those owned by GKE Pod Snapshots.
	CRIUPolicyLabel = util.LabelSnapshotEngine

	// CRIUNodeReadyAnnotation is published on a Node by the CRIU node agent once
	// its node-capability probe confirms CRIU + containerd readiness.
	CRIUNodeReadyAnnotation = "pod-migrate.io/criu-ready"

	// CRIUInjectedAnnotation is stamped on a gated replacement Pod at CREATE by
	// the CRIU engine's restore-injection mutating webhook.
	CRIUInjectedAnnotation = "pod-migrate.io/psengine-injected"

	// CRIURestoreStatusAnnotation is stamped on a replacement Pod by the CRIU
	// node agent once sibling restore completes ("restored") or falls back to
	// cold start ("failed").
	CRIURestoreStatusAnnotation = "pod-migrate.io/psengine-restore"
	CRIURestoreStatusRestored   = "restored"
	CRIURestoreStatusFailed     = "failed"

	// CRIURestoreReasonAnnotation says why the CRIU node agent's restore
	// failed: a short code (for example ArchiveSignatureInvalid) that the
	// agent stamps in the same update as CRIURestoreStatusAnnotation=failed
	// (#244). VerifyRestore puts it in the cold start's evidence.
	CRIURestoreReasonAnnotation = "pod-migrate.io/psengine-restore-reason"

	criuColdStartReason           = "CRIU snapshot restore failed; replacement fell back to cold start"
	criuUninjectedColdStartReason = "Replacement pod was not wired by the CRIU restore webhook and started cold"
)

// CRIU is the controller-side Engine implementation for the CRIU snapshot
// engine (engines/criu), parameterized by its configured Kubernetes API group
// and matching <group>/ps-name restore annotation (#135).
type CRIU struct {
	reader            client.Reader
	provider          *snapshot.GKEProvider
	group             string
	restoreAnnotation string
}

var (
	_ Engine          = (*CRIU)(nil)
	_ RestoreOrderer  = (*CRIU)(nil)
	_ RestoreVerifier = (*CRIU)(nil)
)

// NewCRIU returns a CRIU snapshot engine bound to apiGroup (defaulting to
// DefaultCRIUAPIGroup when empty). It validates apiGroup with ValidateAPIGroup.
func NewCRIU(c client.Client, scheme *runtime.Scheme, apiGroup string) (*CRIU, error) {
	if apiGroup == "" {
		apiGroup = DefaultCRIUAPIGroup
	}
	provider, err := snapshot.NewGKEProviderForGroup(c, scheme, apiGroup)
	if err != nil {
		return nil, err
	}
	return &CRIU{
		reader:            c,
		provider:          provider,
		group:             apiGroup,
		restoreAnnotation: RestoreAnnotation(apiGroup),
	}, nil
}

// Name implements Engine.
func (*CRIU) Name() string { return CRIUName }

// APIGroup implements Engine.
func (e *CRIU) APIGroup() string {
	if e == nil || e.group == "" {
		return DefaultCRIUAPIGroup
	}
	return e.group
}

// RestoreAnnotation implements Engine.
func (e *CRIU) RestoreAnnotation() string {
	if e == nil || e.restoreAnnotation == "" {
		return RestoreAnnotation(DefaultCRIUAPIGroup)
	}
	return e.restoreAnnotation
}

// RuntimeClasses implements Engine: pods without an explicit runtimeClassName
// (@default) and explicit runc pods.
func (*CRIU) RuntimeClasses() []string {
	return []string{eligibility.DefaultRuntimeClassToken, "runc"}
}

// Eligible implements Engine with pure local checks (no API calls). It mirrors
// the CRIU engine's own refusals (engines/criu, C2), so that a pod the engine
// would refuse is not sent through an eviction 429 and a Refused trigger first
// (#233):
//
//   - a single application container, with an explicit command: the engine's
//     restore webhook leaves a replacement without one unwired, so it would
//     start cold;
//   - no native sidecar initContainers;
//   - no hostNetwork (#229);
//   - not already restored by the engine, and not wrapped by its holder (#236);
//   - a cached source node verified CRIU-ready (pod-migrate.io/criu-ready=true).
//
// If node is nil (unscheduled pod or source node missing from cache), Eligible
// fails closed so no PodMigrationJob is spawned for an unverified node.
func (*CRIU) Eligible(pod *corev1.Pod, node *corev1.Node) Verdict {
	if pod == nil {
		return Verdict{Code: "ineligible", Reason: "no pod to migrate"}
	}
	if len(pod.Spec.Containers) != 1 {
		return Verdict{Code: "multi_container", Reason: fmt.Sprintf("CRIU engine requires a single-container pod (got %d containers)", len(pod.Spec.Containers))}
	}
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		if ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return Verdict{Code: "multi_container", Reason: "CRIU engine does not support sidecar initContainers (restartPolicy=Always)"}
		}
	}
	if pod.Spec.HostNetwork {
		return Verdict{Code: "host_network", Reason: "CRIU engine does not support hostNetwork pods"}
	}
	if strings.TrimSpace(pod.Annotations[CRIURestoreStatusAnnotation]) == CRIURestoreStatusRestored ||
		pod.Annotations[CRIUInjectedAnnotation] == "true" {
		return Verdict{Code: "already_restored", Reason: fmt.Sprintf(
			"CRIU engine does not checkpoint a pod it already restored or wrapped (%s=%s or %s=true)",
			CRIURestoreStatusAnnotation, CRIURestoreStatusRestored, CRIUInjectedAnnotation)}
	}
	if len(pod.Spec.Containers[0].Command) == 0 {
		return Verdict{Code: "missing_command", Reason: fmt.Sprintf(
			"CRIU engine requires an explicit command on container %s: its replacement could not be restored and would start cold",
			pod.Spec.Containers[0].Name)}
	}
	if node == nil {
		if pod.Spec.NodeName != "" {
			return Verdict{Code: "node_not_ready", Reason: fmt.Sprintf("source node %s not found; cannot verify %s=true", pod.Spec.NodeName, CRIUNodeReadyAnnotation)}
		}
		return Verdict{Code: "node_not_ready", Reason: fmt.Sprintf("pod is not scheduled on a CRIU-ready node (missing %s=true)", CRIUNodeReadyAnnotation)}
	}
	if node.Annotations[CRIUNodeReadyAnnotation] != "true" {
		return Verdict{Code: "node_not_ready", Reason: fmt.Sprintf("source node %s is not CRIU-ready (missing %s=true)", node.Name, CRIUNodeReadyAnnotation)}
	}
	return Verdict{OK: true}
}

// ResolvePolicy implements Engine: finds the newest Ready, manual+stop
// PodSnapshotPolicy in e.APIGroup()/v1 labeled pod-migrate.io/engine=criu that
// selects pod. Missing CRDs mean no policy.
func (e *CRIU) ResolvePolicy(ctx context.Context, pod *corev1.Pod) (*PolicyRef, error) {
	if pod == nil {
		return nil, errors.New("resolving a PodSnapshotPolicy for a nil pod")
	}
	psp, err := util.FindLatestReadyManualStopPSPInGroup(
		ctx,
		e.reader,
		e.APIGroup(),
		pod.Namespace,
		pod.Labels,
		map[string]string{CRIUPolicyLabel: CRIUName},
	)
	if err != nil || psp == nil {
		return nil, err
	}
	return &PolicyRef{Group: e.APIGroup(), Kind: snapshot.KindPolicy, Name: psp.GetName()}, nil
}

// Snapshots implements Engine with the group-parameterized snapshot provider.
func (e *CRIU) Snapshots() snapshot.Provider { return e.provider }

// RestoreFailures implements Engine. CRIU has no container StartError
// signatures, and gVisor signatures must not run on runc pods (C7, #132).
func (*CRIU) RestoreFailures() []restore.Engine { return nil }

// Guarantees implements Engine: the CRIU engine freezes the source container
// before checkpointing (#148), while the controller conservatively declares
// FencingStopped (keeping the endpoint fence and cold-evicting on failure past
// checkpoint) until the controller-side SourceFrozen handling in #148 lands.
// CRIU does not guarantee at-most-once restore on its own without controller
// completion (#152).
func (*CRIU) Guarantees() Guarantees {
	return Guarantees{Fencing: FencingStopped, RestoreOnce: false}
}

// NamespaceResources implements Engine. For a PodMigration storing snapshots at
// gs://bucket/path, it renders in e.APIGroup()/v1:
//
//   - a cluster-scoped PodSnapshotStorageConfig named pssc-criu-<hash>, labeled
//     with the PodMigration's UID and pod-migrate.io/engine=criu;
//   - a namespaced PodSnapshotPolicy named psp-<name>-criu-manual, labeled
//     pod-migrate.io/engine=criu, with manual trigger and postCheckpoint: stop.
func (e *CRIU) NamespaceResources(pm *pmv1alpha1.PodMigration) ([]*unstructured.Unstructured, error) {
	if pm == nil {
		return nil, errors.New("rendering namespace resources for a nil PodMigration")
	}
	location := pm.Spec.Storage.Location
	bucket, path, err := util.ParseStorageLocation(location)
	if err != nil {
		return nil, &StorageLocationError{
			Location: location,
			Message:  err.Error(),
		}
	}
	gcs := map[string]interface{}{
		"bucket": bucket,
	}
	if path != "" {
		gcs["path"] = path
	}

	group := e.APIGroup()
	storageConfigName := CRIUPodSnapshotStorageConfigName(pm.Namespace, pm.Name)
	storageConfig := &unstructured.Unstructured{}
	storageConfig.SetGroupVersionKind(snapshot.StorageConfigGVK(group))
	storageConfig.SetName(storageConfigName)
	storageConfig.SetLabels(map[string]string{
		OwnerUIDLabel:   string(pm.UID),
		CRIUPolicyLabel: CRIUName,
	})
	storageConfig.Object["spec"] = map[string]interface{}{
		"snapshotStorageConfig": map[string]interface{}{
			"gcs": gcs,
		},
	}

	policy := &unstructured.Unstructured{}
	policy.SetGroupVersionKind(snapshot.PolicyGVK(group))
	policy.SetName(CRIUPodSnapshotPolicyName(pm.Name))
	policy.SetNamespace(pm.Namespace)
	policy.SetLabels(map[string]string{
		CRIUPolicyLabel: CRIUName,
	})
	policy.Object["spec"] = map[string]interface{}{
		"storageConfigName": storageConfigName,
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      migrationEnabledLabel,
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
	}

	return []*unstructured.Unstructured{storageConfig, policy}, nil
}

// OrderRestore implements RestoreOrderer: sets <group>/ps-name to the job's snapshot.
func (e *CRIU) OrderRestore(pod *corev1.Pod, job *pmv1alpha1.PodMigrationJob) error {
	if pod == nil || job == nil {
		return errors.New("ordering a restore needs a pod and a job")
	}
	if job.Status.SnapshotRef == "" {
		return fmt.Errorf("job %s/%s has no snapshot to restore", job.Namespace, job.Name)
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[e.RestoreAnnotation()] = job.Status.SnapshotRef
	return nil
}

// OrderColdStartBypass implements RestoreOrderer: sets <group>/ps-name to "".
func (e *CRIU) OrderColdStartBypass(pod *corev1.Pod) error {
	if pod == nil {
		return errors.New("ordering a cold start needs a pod")
	}
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[e.RestoreAnnotation()] = ""
	return nil
}

// InspectRestoreOrder implements RestoreOrderer for <group>/ps-name.
func (e *CRIU) InspectRestoreOrder(pod *corev1.Pod) (snapshotRef string, coldStartBypass bool, present bool) {
	if pod == nil || pod.Annotations == nil {
		return "", false, false
	}
	val, exists := pod.Annotations[e.RestoreAnnotation()]
	if !exists {
		return "", false, false
	}
	trimmed := strings.TrimSpace(val)
	if trimmed == "" {
		return "", true, true
	}
	return trimmed, false, true
}

// VerifyRestore implements RestoreVerifier using the CRIU node agent's positive
// outcome annotation (pod-migrate.io/psengine-restore):
//   - "failed": ColdStart verdict immediately, with the agent's reason
//     (pod-migrate.io/psengine-restore-reason) in the evidence when it set one;
//   - missing pod-migrate.io/psengine-injected=true + pod Ready: ColdStart
//     verdict (replacement was not wired by the CRIU restore webhook, even if
//     psengine-restore=restored is present);
//   - pod-migrate.io/psengine-injected=true + "restored" + pod Ready: confirmed
//     Restored verdict (RestoreVerified);
//   - otherwise: RestoreInProgress.
func (*CRIU) VerifyRestore(_ context.Context, _ *pmv1alpha1.PodMigrationJob, pod *corev1.Pod) (RestoreVerdict, error) {
	if pod == nil {
		return RestoreVerdict{}, errors.New("verifying the restore of a nil pod")
	}
	if strings.TrimSpace(pod.Annotations[CRIURestoreStatusAnnotation]) == CRIURestoreStatusFailed {
		evidence := fmt.Sprintf("annotation %s=%s", CRIURestoreStatusAnnotation, CRIURestoreStatusFailed)
		if reason := strings.TrimSpace(pod.Annotations[CRIURestoreReasonAnnotation]); reason != "" {
			evidence += fmt.Sprintf(" (reason %s)", reason)
		}
		return RestoreVerdict{
			Outcome:  ColdStart,
			Reason:   criuColdStartReason,
			Evidence: evidence,
		}, nil
	}
	if pod.Annotations[CRIUInjectedAnnotation] != "true" {
		if !podReady(pod) {
			return RestoreVerdict{Outcome: RestoreInProgress}, nil
		}
		return RestoreVerdict{
			Outcome:  ColdStart,
			Reason:   criuUninjectedColdStartReason,
			Evidence: fmt.Sprintf("missing annotation %s=true", CRIUInjectedAnnotation),
		}, nil
	}
	if strings.TrimSpace(pod.Annotations[CRIURestoreStatusAnnotation]) == CRIURestoreStatusRestored {
		if !podReady(pod) {
			return RestoreVerdict{Outcome: RestoreInProgress}, nil
		}
		return RestoreVerdict{
			Outcome:   Restored,
			Confirmed: true,
			Evidence:  fmt.Sprintf("annotation %s=%s", CRIURestoreStatusAnnotation, CRIURestoreStatusRestored),
		}, nil
	}
	return RestoreVerdict{Outcome: RestoreInProgress}, nil
}

// CRIUPodSnapshotStorageConfigName names the cluster-scoped storage config of
// the PodMigration namespace/name for the CRIU engine. The -criu- prefix keeps
// it distinct from GKE Pod Snapshots even when both engines share an API group.
func CRIUPodSnapshotStorageConfigName(namespace, name string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s/%s", namespace, name)))
	return fmt.Sprintf("pssc-criu-%s", hex.EncodeToString(h.Sum(nil))[:16])
}

// CRIUPodSnapshotPolicyName names the CRIU PodSnapshotPolicy of the
// PodMigration name, in the PodMigration's namespace.
func CRIUPodSnapshotPolicyName(name string) string {
	return fmt.Sprintf("psp-%s-criu-manual", name)
}
