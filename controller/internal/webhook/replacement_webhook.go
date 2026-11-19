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
	"os"
	"slices"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
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
	// DefaultControllerUsername is the Kubernetes ServiceAccount identity of the
	// pod-migration-controller deployment in deploy.yaml. Only this identity may
	// mutate <group>/ps-name or pod-migration.gke.io/assigned-pmj on Pod UPDATEs.
	DefaultControllerUsername = "system:serviceaccount:pod-migration-system:pod-migration-controller"

	// DefaultCRIUNodeAgentUsername is the Kubernetes ServiceAccount identity of
	// the CRIU node agent DaemonSet in engines/criu/deploy/deploy.yaml. Only this
	// identity (or the controller) may mutate pod-migrate.io/psengine-restore on
	// Pod UPDATEs.
	DefaultCRIUNodeAgentUsername = "system:serviceaccount:pod-migration-criu-system:pod-migrate-agent"

	defaultServiceAccountNamespacePath = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"

	gkeRestoreAnnotationKey = "podsnapshot.gke.io/ps-name"
	migrationEnabledLabel   = "pod-migration.gke.io/enabled"
	schedulingGateName      = "gke.io/pod-migration-gate"
)

// PodGateInjector injects the scheduling gate into replacement pods and
// protects per-engine restore-order annotations (<group>/ps-name) against
// untrusted spoofing on Pod CREATE and UPDATE (#135).
type PodGateInjector struct {
	Client                      client.Client
	APIReader                   client.Reader
	Engines                     *engine.Registry
	ControllerUsername          string
	CRIUNodeAgentUsername       string
	serviceAccountNamespacePath string
	decoder                     admission.Decoder
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

func (a *PodGateInjector) isTrustedController(req admission.Request) bool {
	user := strings.TrimSpace(req.UserInfo.Username)
	if user == "" {
		return false
	}
	if explicit := strings.TrimSpace(a.ControllerUsername); explicit != "" {
		return user == explicit
	}
	if ns := strings.TrimSpace(os.Getenv("POD_NAMESPACE")); ns != "" {
		return user == "system:serviceaccount:"+ns+":pod-migration-controller"
	}
	nsPath := a.serviceAccountNamespacePath
	if nsPath == "" {
		nsPath = defaultServiceAccountNamespacePath
	}
	if b, err := os.ReadFile(nsPath); err == nil {
		if ns := strings.TrimSpace(string(b)); ns != "" {
			return user == "system:serviceaccount:"+ns+":pod-migration-controller"
		}
	}
	return user == DefaultControllerUsername
}

func (a *PodGateInjector) isTrustedCRIUNodeAgent(req admission.Request) bool {
	user := strings.TrimSpace(req.UserInfo.Username)
	if user == "" {
		return false
	}
	expected := strings.TrimSpace(a.CRIUNodeAgentUsername)
	if expected == "" {
		expected = strings.TrimSpace(os.Getenv("CRIU_AGENT_SERVICE_ACCOUNT"))
	}
	if expected == "" {
		expected = DefaultCRIUNodeAgentUsername
	}
	return user == expected
}

func hasLifecycleMigrationState(pod *corev1.Pod) bool {
	if pod == nil {
		return false
	}
	for _, gate := range pod.Spec.SchedulingGates {
		if gate.Name == schedulingGateName {
			return true
		}
	}
	return pod.Annotations[util.AnnotationAssignedPMJ] != "" ||
		pod.Annotations[engine.CRIUInjectedAnnotation] == "true" ||
		pod.Annotations[util.AnnotationRestoreCompleted] != ""
}

// Handle inspects pod CREATE and UPDATE requests, injecting the scheduling gate
// on migration replacement pods and scrubbing/protecting <group>/ps-name across
// all registered engines (#135).
func (a *PodGateInjector) Handle(ctx context.Context, req admission.Request) admission.Response {
	if req.Operation == admissionv1.Update {
		return a.handleUpdate(ctx, req)
	}
	return a.handleCreate(ctx, req)
}

func (a *PodGateInjector) handleUpdate(ctx context.Context, req admission.Request) admission.Response {
	if req.SubResource != "" {
		return admission.Allowed("subresource update")
	}
	if a.isTrustedController(req) {
		return admission.Allowed("trusted controller update")
	}

	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)
	pod := &corev1.Pod{}
	if err := a.decoder.Decode(req, pod); err != nil {
		logger.Error(err, "Failed to decode pod update")
		return admission.Errored(http.StatusBadRequest, err)
	}
	if pod.Namespace == "" {
		pod.Namespace = req.Namespace
	}

	oldPod := &corev1.Pod{}
	if len(req.OldObject.Raw) > 0 {
		if err := a.decoder.DecodeRaw(req.OldObject, oldPod); err != nil {
			logger.Error(err, "Failed to decode old pod on update")
			return admission.Errored(http.StatusBadRequest, err)
		}
	}

	oldOptedIn := oldPod.Labels[migrationEnabledLabel] == "true"
	newOptedIn := pod.Labels[migrationEnabledLabel] == "true"
	if !oldOptedIn && !newOptedIn {
		return admission.Allowed("pod not opted in")
	}

	// If a pod is newly opting into pod-migration.gke.io/enabled=true via UPDATE,
	// always scrub ALL pre-existing restore annotations across every engine (only
	// Pod CREATE preserves Kubeflow/Sandbox GKE ps-name) so an attacker cannot
	// plant <group>/ps-name while opted out and then label the pod enabled=true.
	if !oldOptedIn {
		eng, hasEngine := a.podEngine(ctx, pod)
		if pod.Annotations == nil {
			pod.Annotations = make(map[string]string)
		}
		if pod.Annotations[util.AnnotationAssignedPMJ] != "" {
			delete(pod.Annotations, util.AnnotationAssignedPMJ)
		}
		delete(pod.Annotations, engine.CRIUInjectedAnnotation)
		delete(pod.Annotations, engine.CRIURestoreStatusAnnotation)
		for _, ann := range a.restoreAnnotations() {
			delete(pod.Annotations, ann)
		}
		pod.Annotations[gkeRestoreAnnotationKey] = ""
		if hasEngine {
			if orderer, ok := eng.(engine.RestoreOrderer); ok {
				_ = orderer.OrderColdStartBypass(pod)
			}
		}
		marshaledPod, err := json.Marshal(pod)
		if err != nil {
			logger.Error(err, "Failed to marshal pod on opt-in update")
			return admission.Errored(http.StatusInternalServerError, err)
		}
		return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
	}

	// oldPod was opted in: enforce protected migration/restore annotations even if
	// the caller also attempted to drop pod-migration.gke.io/enabled=true in the
	// same PATCH, and lock enabled=true while migration/injection/restore state is
	// present so the pod cannot shed objectSelector mid-lifecycle.
	changed := false
	if !newOptedIn && hasLifecycleMigrationState(oldPod) {
		if pod.Labels == nil {
			pod.Labels = make(map[string]string)
		}
		pod.Labels[migrationEnabledLabel] = "true"
		changed = true
	}

	protectedKeys := append(
		a.restoreAnnotations(),
		util.AnnotationAssignedPMJ,
		engine.CRIUInjectedAnnotation,
		util.AnnotationRestoreCompleted,
	)
	if !a.isTrustedCRIUNodeAgent(req) {
		protectedKeys = append(protectedKeys, engine.CRIURestoreStatusAnnotation)
	}
	for _, key := range protectedKeys {
		oldVal, oldOK := oldPod.Annotations[key]
		newVal, newOK := pod.Annotations[key]
		if oldOK != newOK || oldVal != newVal {
			changed = true
			if oldOK {
				if pod.Annotations == nil {
					pod.Annotations = make(map[string]string)
				}
				pod.Annotations[key] = oldVal
			} else if pod.Annotations != nil {
				delete(pod.Annotations, key)
			}
		}
	}
	if !changed {
		return admission.Allowed("restore annotations unchanged")
	}

	logger.Info("Reverted untrusted mutation to protected migration/restore metadata on Pod UPDATE")
	marshaledPod, err := json.Marshal(pod)
	if err != nil {
		logger.Error(err, "Failed to marshal reverted pod update")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, marshaledPod)
}

func (a *PodGateInjector) handleCreate(ctx context.Context, req admission.Request) admission.Response {
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
	// For GKE Pod Snapshots, preserve a pre-existing non-empty podsnapshot.gke.io/ps-name
	// (e.g. Kubeflow workspace resume or Sandbox template restore, authorized by GKE's own
	// admission policy, C10 R1 / #55). Non-GKE engines (such as CRIU) never accept
	// user-supplied restore orders: strip/overwrite <group>/ps-name across all registered
	// engines with the cold-start bypass ("").
	if assignedPMJ == "" {
		eng, hasEngine := a.podEngine(ctx, pod)
		_, isGKEEngine := eng.(*engine.GKEPodSnapshots)
		existingGKEPS := pod.Annotations[gkeRestoreAnnotationKey]
		preserveGKEPSName := isGKEEngine && existingGKEPS != ""

		scrubbed := false
		if pod.Annotations != nil {
			if pod.Annotations[util.AnnotationAssignedPMJ] != "" {
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
