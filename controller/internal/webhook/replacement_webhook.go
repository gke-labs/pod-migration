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

package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"slices"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	gkeRestoreAnnotationKey = "podsnapshot.gke.io/ps-name"
)

// PodGateInjector injects the scheduling gate into replacement pods and
// scrubs/stamps per-engine restore-order annotations (<group>/ps-name) on Pod
// CREATE (#135).
type PodGateInjector struct {
	Client    client.Client
	APIReader client.Reader
	Engines   *engine.Registry
	decoder   admission.Decoder
}

func (a *PodGateInjector) restoreAnnotations() []string {
	out := []string{gkeRestoreAnnotationKey}
	if a.Engines != nil {
		for _, ann := range a.Engines.RestoreAnnotations() {
			if ann != "" && !slices.Contains(out, ann) {
				out = append(out, ann)
			}
		}
	}
	return out
}

func (a *PodGateInjector) podEngine(ctx context.Context, pod *corev1.Pod) (engine.Engine, bool) {
	if a.Engines == nil {
		return engine.NewGKEPodSnapshots(a.Client, nil), true
	}
	pm, err := util.ActivePodMigration(ctx, a.Client, pod.Namespace)
	if err != nil {
		pm = nil
	}
	return a.Engines.ForPod(pod, pm)
}

// Handle inspects pod CREATE requests, injecting the scheduling gate on
// migration replacement pods and scrubbing/stamping <group>/ps-name across all
// registered engines (#135).
func (a *PodGateInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)
	logger.Info("Intercepted pod creation for scheduling gate check")

	pod := &corev1.Pod{}
	err := a.decoder.Decode(req, pod)
	if err != nil {
		logger.Error(err, "Failed to decode pod")
		return admission.Errored(http.StatusBadRequest, err)
	}

	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}
	if req.Name == "" {
		if pod.Name != "" {
			logger = log.FromContext(ctx).WithValues("pod", pod.Name, "namespace", pod.Namespace)
		} else if pod.GenerateName != "" {
			logger = log.FromContext(ctx).WithValues("pod", pod.GenerateName, "namespace", pod.Namespace)
		}
	}

	// Check if pod opted in
	if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
		logger.Info("Pod not opted in, bypassing scheduling gate injection")
		return admission.Allowed("pod not opted in")
	}

	// Resolve parent owner details
	parentName, parentKind, parentUID, err := util.ResolveParentWorkload(ctx, a.Client, pod)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("Parent ReplicaSet not found (likely deleted), treating as bare pod")
		} else {
			logger.Error(err, "Failed to resolve parent workload")
			return admission.Errored(http.StatusInternalServerError, err)
		}
	}

	var podTemplateHash string
	var jobCompletionIndex string
	var replicaKey string
	if pod.Labels != nil {
		podTemplateHash = pod.Labels[appsv1.DefaultDeploymentUniqueLabelKey]
		jobCompletionIndex = pod.Labels[util.LabelJobCompletionIndex]
	}
	if !util.IsBuiltInWorkloadKind(parentKind) {
		replicaKey, _ = util.ResolveOperatorReplicaKey(pod, parentKind)
	}

	// Find active unassigned PMJ
	assignedPMJ, err := util.FindUnassignedActivePMJ(ctx, a.Client, req.Namespace, pod.Name, parentName, parentKind, parentUID, podTemplateHash, jobCompletionIndex, true, replicaKey)
	if err != nil {
		logger.Error(err, "Failed to check unassigned active PMJs")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Fallback to APIReader on cache miss to protect against cross-replica informer lag:
	// If a peer replica just created a PMJ, it may not yet have synced to our local informer cache.
	// A false miss here would irreversibly stamp ps-name: "" and skip the scheduling gate,
	// permanently forcing a cold start and stranding the PMJ.
	if assignedPMJ == "" && a.APIReader != nil {
		liveParentName, liveParentKind, liveParentUID, err := util.ResolveParentWorkload(ctx, a.APIReader, pod)
		if err != nil {
			if apierrors.IsNotFound(err) {
				logger.Info("Parent ReplicaSet not found via live APIReader, treating as bare pod")
			} else {
				logger.Error(err, "Failed to resolve parent workload via live APIReader")
				return admission.Errored(http.StatusInternalServerError, err)
			}
		}
		liveReplicaKey := replicaKey
		if !util.IsBuiltInWorkloadKind(liveParentKind) {
			liveReplicaKey, _ = util.ResolveOperatorReplicaKey(pod, liveParentKind)
		}
		assignedPMJ, err = util.FindUnassignedActivePMJ(ctx, a.APIReader, pod.Namespace, pod.Name, liveParentName, liveParentKind, liveParentUID, podTemplateHash, jobCompletionIndex, false, liveReplicaKey)
		if err != nil {
			logger.Error(err, "Failed to check unassigned active PMJs via APIReader fallback")
			return admission.Errored(http.StatusInternalServerError, err)
		}
		if assignedPMJ != "" {
			logger.Info("Found unassigned active PMJ via live APIReader fallback", "pmj", assignedPMJ)
		}
	}

	// If there is no active unassigned migration job, this is a scale-up or unrelated pod.
	// For GKE Pod Snapshots (or unrouted runtime classes, matching main), preserve a
	// pre-existing non-empty podsnapshot.gke.io/ps-name (e.g. Kubeflow workspace resume
	// or Sandbox template restore, authorized by GKE's own admission policy, C10 R1 / #55).
	// Pods routed to non-GKE engines (such as CRIU) never accept user-supplied restore
	// orders: strip/overwrite <group>/ps-name across all registered engines with "".
	if assignedPMJ == "" {
		eng, hasEngine := a.podEngine(ctx, pod)
		_, isGKEEngine := eng.(*engine.GKEPodSnapshots)
		existingGKEPS := pod.Annotations[gkeRestoreAnnotationKey]
		preserveGKEPSName := (!hasEngine || isGKEEngine) && existingGKEPS != ""

		scrubbed := false
		if pod.Annotations != nil {
			if _, ok := pod.Annotations[util.AnnotationAssignedPMJ]; ok {
				delete(pod.Annotations, util.AnnotationAssignedPMJ)
				scrubbed = true
			}
			if _, ok := pod.Annotations[engine.CRIUInjectedAnnotation]; ok {
				delete(pod.Annotations, engine.CRIUInjectedAnnotation)
				scrubbed = true
			}
			if _, ok := pod.Annotations[engine.CRIURestoreStatusAnnotation]; ok {
				delete(pod.Annotations, engine.CRIURestoreStatusAnnotation)
				scrubbed = true
			}
			for _, ann := range a.restoreAnnotations() {
				if ann == gkeRestoreAnnotationKey && preserveGKEPSName {
					continue
				}
				if _, ok := pod.Annotations[ann]; ok {
					delete(pod.Annotations, ann)
					scrubbed = true
				}
			}
		}

		if preserveGKEPSName && !scrubbed {
			logger.Info("Preserving pre-existing snapshot annotation (no active PMJ)", "snapshotName", existingGKEPS)
			return admission.Allowed("preserving pre-existing podsnapshot.gke.io/ps-name annotation")
		}

		if !preserveGKEPSName {
			logger.Info("Bypassing scheduling gate injection (scale-up pod)")
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			// Why this matters: GKE Pod Snapshots restores a pod that has no ps-name from a
			// matching snapshot when its sandbox is created. Without "", a new replica, a pod
			// recreated after a delete, or a restarted migration source can come back from a
			// leftover snapshot as a copy or with stale state (#151, #157). Don't remove it
			// without a replacement; the empty value costs one Warning event per pod (#74).
			pod.Annotations[gkeRestoreAnnotationKey] = ""
			if hasEngine {
				if orderer, ok := eng.(engine.RestoreOrderer); ok {
					_ = orderer.OrderColdStartBypass(pod)
				}
			}
		}
		marshaledPod, err := json.Marshal(pod)
		if err != nil {
			logger.Error(err, "Failed to marshal scale-up pod")
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
	}

	// Replacement pod matched an active unassigned PMJ: scrub any pre-existing or
	// spoofed restore annotations across ALL registered engines (plus engine
	// injection/status markers) so PodGateReconciler is the sole authority to
	// order the restore once the checkpoint is durable (#135, C10 R1).
	scrubbed := false
	if pod.Annotations != nil {
		for _, ann := range a.restoreAnnotations() {
			if _, ok := pod.Annotations[ann]; ok {
				delete(pod.Annotations, ann)
				scrubbed = true
			}
		}
		if _, ok := pod.Annotations[engine.CRIUInjectedAnnotation]; ok {
			delete(pod.Annotations, engine.CRIUInjectedAnnotation)
			scrubbed = true
		}
		if _, ok := pod.Annotations[engine.CRIURestoreStatusAnnotation]; ok {
			delete(pod.Annotations, engine.CRIURestoreStatusAnnotation)
			scrubbed = true
		}
	}

	// Check if the gate is already present
	hasGate := false
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == "gke.io/pod-migration-gate" {
			hasGate = true
			break
		}
	}

	if hasGate && pod.Annotations[util.AnnotationAssignedPMJ] == assignedPMJ && !scrubbed {
		logger.Info("Pod already has the scheduling gate and PMJ assignment, bypassing")
		return admission.Allowed("scheduling gate already present")
	}

	if !hasGate {
		logger.Info("Injecting scheduling gate gke.io/pod-migration-gate")
		pod.Spec.SchedulingGates = append(pod.Spec.SchedulingGates, corev1.PodSchedulingGate{
			Name: "gke.io/pod-migration-gate",
		})
	}

	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[util.AnnotationAssignedPMJ] = assignedPMJ

	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		logger.Error(err, "Failed to marshal modified pod")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

// InjectDecoder injects the decoder.
func (a *PodGateInjector) InjectDecoder(d admission.Decoder) error {
	a.decoder = d
	return nil
}

// SetupReplacementWebhookWithManager registers the mutating webhook on the manager.
func SetupReplacementWebhookWithManager(mgr ctrl.Manager, apiReader client.Reader, engines ...*engine.Registry) error {
	var reg *engine.Registry
	if len(engines) > 0 {
		reg = engines[0]
	}
	dec := admission.NewDecoder(mgr.GetScheme())
	mgr.GetWebhookServer().Register(
		"/mutate-v1-pod-scheduling-gate",
		&admission.Webhook{
			Handler: &PodGateInjector{
				Client:    mgr.GetClient(),
				APIReader: apiReader,
				Engines:   reg,
				decoder:   dec,
			},
		},
	)
	return nil
}
