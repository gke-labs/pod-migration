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

// Restore-injection webhook: replacement pods are created by the workload
// controller and gated by the orchestrator's own webhook; the ps-name restore
// order arrives later, after checkpoint completion, when pod spec is already
// immutable — so the holder wiring must be injected speculatively at CREATE,
// and the holder wrapper decides at runtime (restore vs. cold-start) from the
// downward-API annotations file.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const (
	holdVolume     = "pmhold"
	infoVolume     = "pminfo"
	holdMount      = "/pmhold"
	infoMount      = "/pminfo"
	annotationsRel = "annotations"
	seedInitName   = "pm-holder-seed"
	// AnnotationInjected marks pods this webhook rewired (idempotency +
	// restore-reconciler filter).
	AnnotationInjected = "pod-migrate.io/psengine-injected"
)

// holderDefaultPSNameAnnotation is the restore order the holder reads when
// its wrap command doesn't name one: the key of the GKE Pod Snapshots group,
// which every holder image understands. The command names the key only when
// it differs, so on that group the injected command is unchanged.
var holderDefaultPSNameAnnotation = contract.RestoreAnnotation(contract.GKEPodSnapshotsGroup)

// RestoreInjector mutates CRIU-routed gated replacement pods at CREATE:
// holder-seed initContainer + command wrapper + downward-API annotations
// volume. Replacement pods routed to another engine (such as GKE Pod Snapshots
// for gVisor) or pods it cannot make cold-start-safe (no explicit command) are
// left untouched — fail-open, they simply proceed on their own engine or cold-start.
type RestoreInjector struct {
	Decoder     admission.Decoder
	HolderImage string
	Reader      client.Reader
}

// +kubebuilder:rbac:groups=podmigration.gke.io,resources=podmigrationjobs,verbs=get

func (h *RestoreInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	if h.Decoder == nil {
		return admission.Errored(http.StatusInternalServerError, errors.New("restore webhook decoder is nil"))
	}
	pod := &corev1.Pod{}
	if err := h.Decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Run pure in-memory pod-shape, gate, runtimeClass, and sidecar checks
	// before reading the API server so non-candidate pods (gVisor, ungated,
	// multi-container, sidecar, hostNetwork, commandless) short-circuit with 0
	// API reads.
	if ok, reason := canInjectHolder(pod); !ok {
		return admission.Allowed(reason)
	}

	ns := req.Namespace
	if ns == "" {
		ns = pod.Namespace
	}

	// Verify that the assigned PodMigrationJob belongs to the CRIU snapshot
	// engine (#135) so another engine's replacement pods (even on default/runc
	// runtime classes routed elsewhere) are never rewritten.
	if ok, reason := h.verifyAssignedJobEngine(ctx, ns, pod); !ok {
		return admission.Allowed(reason)
	}

	mutated, reason := InjectHolder(pod, h.HolderImage)
	if !mutated {
		return admission.Allowed(reason)
	}
	logger.Info("injected psengine holder wiring", "pod", pod.Name)
	raw, err := json.Marshal(pod)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, raw)
}

func (h *RestoreInjector) verifyAssignedJobEngine(ctx context.Context, namespace string, pod *corev1.Pod) (bool, string) {
	if h.Reader == nil {
		log.FromContext(ctx).Error(nil, "restore webhook has no PodMigrationJob reader; skipping holder injection")
		return false, "restore webhook has no PodMigrationJob reader; skipping holder injection"
	}
	assignedPMJ := ""
	if pod != nil && pod.Annotations != nil {
		assignedPMJ = strings.TrimSpace(pod.Annotations[contract.AnnotationAssignedPMJ])
	}
	if assignedPMJ == "" {
		return false, "not a gated migration replacement pod"
	}
	if namespace == "" {
		return false, "missing pod namespace for PodMigrationJob lookup; skipping holder injection"
	}
	key := types.NamespacedName{Namespace: namespace, Name: assignedPMJ}
	pmj := contract.New(contract.GVKPodMigrationJob)
	if err := h.Reader.Get(ctx, key, pmj); err != nil {
		log.FromContext(ctx).Error(err, "failed to read assigned PodMigrationJob for restore injection; skipping holder injection", "pmj", key)
		return false, fmt.Sprintf("cannot read assigned PodMigrationJob %s/%s; skipping holder injection", namespace, assignedPMJ)
	}
	jobEngine := strings.TrimSpace(pmj.GetAnnotations()[contract.AnnotationOrchestratorEngine])
	if jobEngine != contract.EngineCRIU {
		return false, fmt.Sprintf("assigned PodMigrationJob belongs to snapshot engine %q, not %s", jobEngine, contract.EngineCRIU)
	}
	return true, ""
}

// isSupportedCRIURuntimeClass reports whether runtimeClassName is within the
// CRIU engine's v1 runtime scope: unset/empty (@default) or "runc".
func isSupportedCRIURuntimeClass(runtimeClassName *string) (bool, string) {
	if runtimeClassName == nil {
		return true, ""
	}
	rc := strings.TrimSpace(*runtimeClassName)
	if rc == "" || rc == "runc" {
		return true, ""
	}
	return false, rc
}

// canInjectHolder runs all pure in-memory pod eligibility checks for holder
// injection without touching the API server or mutating pod.
func canInjectHolder(pod *corev1.Pod) (bool, string) {
	if pod == nil {
		return false, "not a gated migration replacement pod"
	}
	// Only migration replacement candidates: the orchestrator's replacement
	// webhook injected the gate + assignment in the same admission chain
	// (replacement_webhook.go:171-180); ours runs after via reinvocation.
	gated := false
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == contract.SchedulingGate {
			gated = true
			break
		}
	}
	if !gated || strings.TrimSpace(pod.Annotations[contract.AnnotationAssignedPMJ]) == "" {
		return false, "not a gated migration replacement pod"
	}
	if pod.Annotations[AnnotationInjected] == "true" {
		return false, "already injected"
	}
	if ok, rc := isSupportedCRIURuntimeClass(pod.Spec.RuntimeClassName); !ok {
		return false, fmt.Sprintf("runtimeClassName %q is outside criu engine v1 scope", rc)
	}
	if pod.Spec.HostNetwork {
		return false, "hostNetwork pods unsupported by criu engine"
	}
	for i := range pod.Spec.InitContainers {
		ic := &pod.Spec.InitContainers[i]
		if ic.RestartPolicy != nil && *ic.RestartPolicy == corev1.ContainerRestartPolicyAlways {
			return false, "sidecar initContainers: outside criu engine v1 scope"
		}
	}
	if len(pod.Spec.Containers) != 1 {
		return false, "multi-container pod: outside criu engine v1 scope"
	}
	if len(pod.Spec.Containers[0].Command) == 0 {
		// Without an explicit command the wrapper cannot cold-start the
		// original entrypoint (image config is not visible from inside).
		// Leave the pod untouched: it cold-starts exactly as without the
		// engine.
		return false, "container has no explicit command; cold-start fall-through impossible, skipping injection"
	}
	return true, ""
}

// InjectHolder applies the mutation in place; returns (false, why) when the
// pod is skipped. The wrapper waits for the engine's restore order,
// contract.AnnotationPSName.
func InjectHolder(pod *corev1.Pod, holderImage string) (bool, string) {
	return injectHolder(pod, holderImage, contract.AnnotationPSName)
}

// injectHolder is InjectHolder with the restore-order annotation as a
// parameter. Pure function — unit-tested directly.
func injectHolder(pod *corev1.Pod, holderImage, psNameAnnotation string) (bool, string) {
	if ok, reason := canInjectHolder(pod); !ok {
		return false, reason
	}
	app := &pod.Spec.Containers[0]

	orig := append(append([]string{}, app.Command...), app.Args...)

	pod.Spec.Volumes = append(pod.Spec.Volumes,
		corev1.Volume{Name: holdVolume, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		corev1.Volume{Name: infoVolume, VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{
			Items: []corev1.DownwardAPIVolumeFile{{
				Path:     annotationsRel,
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations"},
			}},
		}}},
	)
	pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
		Name:    seedInitName,
		Image:   holderImage,
		Command: []string{"/holder", "seed", holdMount + "/holder"},
		VolumeMounts: []corev1.VolumeMount{
			{Name: holdVolume, MountPath: holdMount},
		},
	})
	app.VolumeMounts = append(app.VolumeMounts,
		corev1.VolumeMount{Name: holdVolume, MountPath: holdMount, ReadOnly: true},
		corev1.VolumeMount{Name: infoVolume, MountPath: infoMount, ReadOnly: true},
	)
	// The app container starts as the holder wrapper; at runtime it reads
	// ps-name from the downward-API file and either holds (agent restores a
	// sibling) or execs the original entrypoint (cold start).
	app.Command = append(holderWrapCommand(psNameAnnotation), orig...)
	app.Args = nil

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[AnnotationInjected] = "true"
	return true, "injected"
}

// holderWrapCommand returns the wrapper part of the app command, ending in
// "--". It names the restore-order annotation only when that isn't the
// holder's default.
func holderWrapCommand(psNameAnnotation string) []string {
	cmd := []string{holdMount + "/holder", "wrap"}
	if psNameAnnotation != holderDefaultPSNameAnnotation {
		cmd = append(cmd, "--ps-name-annotation="+psNameAnnotation)
	}
	return append(cmd, infoMount+"/"+annotationsRel, "--")
}
