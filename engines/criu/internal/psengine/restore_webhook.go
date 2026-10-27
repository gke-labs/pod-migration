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
	"net/http"

	corev1 "k8s.io/api/core/v1"
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

// RestoreInjector mutates gated replacement pods at CREATE: holder-seed
// initContainer + command wrapper + downward-API annotations volume. Pods it
// cannot make cold-start-safe (no explicit command) are left untouched —
// fail-open, they simply cold-start.
type RestoreInjector struct {
	Decoder     admission.Decoder
	HolderImage string
}

func (h *RestoreInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	pod := &corev1.Pod{}
	if err := h.Decoder.Decode(req, pod); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
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

// InjectHolder applies the mutation in place; returns (false, why) when the
// pod is skipped. Pure function — unit-tested directly.
func InjectHolder(pod *corev1.Pod, holderImage string) (bool, string) {
	// Only migration replacement candidates: the orchestrator's replacement
	// webhook injected the gate + assignment in the same admission chain
	// (replacement_webhook.go:83-91); ours runs after via reinvocation.
	gated := false
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == contract.SchedulingGate {
			gated = true
			break
		}
	}
	if !gated || pod.Annotations[contract.AnnotationAssignedPMJ] == "" {
		return false, "not a gated migration replacement pod"
	}
	if pod.Annotations[AnnotationInjected] == "true" {
		return false, "already injected"
	}
	if len(pod.Spec.Containers) != 1 {
		return false, "multi-container pod: outside criu engine v1 scope"
	}
	app := &pod.Spec.Containers[0]
	if len(app.Command) == 0 {
		// Without an explicit command the wrapper cannot cold-start the
		// original entrypoint (image config is not visible from inside).
		// Leave the pod untouched: it cold-starts exactly as without the
		// engine.
		return false, "container has no explicit command; cold-start fall-through impossible, skipping injection"
	}

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
	app.Command = append([]string{holdMount + "/holder", "wrap", infoMount + "/" + annotationsRel, "--"}, orig...)
	app.Args = nil

	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[AnnotationInjected] = "true"
	return true, "injected"
}
