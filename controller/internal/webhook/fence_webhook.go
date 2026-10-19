package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// FencePath is the path of the fence webhook on the webhook server.
const FencePath = "/mutate-v1-pod-fence"

// fenceMessage is the message on a Ready condition that the fence holds at False.
const fenceMessage = "Readiness is held False while this pod is a migration source (label " +
	util.LabelFenced + "); the ContainersReady condition shows the kubelet's view."

// fencePod is the part of a Pod that the fence webhook reads. The raw object is
// decoded only this far, so fields that this controller's API version doesn't
// know are never touched: the response patches only the Ready condition.
type fencePod struct {
	Metadata struct {
		Labels map[string]string `json:"labels"`
	} `json:"metadata"`
	Status *struct {
		Conditions []fenceCondition `json:"conditions"`
	} `json:"status"`
}

type fenceCondition struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
	Message            string `json:"message,omitempty"`
	LastTransitionTime string `json:"lastTransitionTime,omitempty"`
}

// PodFenceMutator holds the Ready condition of a fenced pod at False (#131).
//
// The controller fences a migration source by labeling it and patching Ready
// to False through pods/status. The kubelet writes the pod's real status at
// every status sync, so without this webhook that write would set Ready back
// to True, and the source would return to its Services' endpoints. The
// webhook configuration's objectSelector sends only labeled pods here, and
// the handler checks the label again.
type PodFenceMutator struct {
	// now returns the current time; nil means time.Now.
	now func() time.Time
}

// Handle rewrites the Ready condition of a fenced pod's status update.
func (m *PodFenceMutator) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues("pod", req.Name, "namespace", req.Namespace)

	if req.Operation != admissionv1.Update || req.SubResource != "status" {
		return recordFence("skipped", admission.Allowed("not a pod status update"))
	}
	var pod fencePod
	if err := json.Unmarshal(req.Object.Raw, &pod); err != nil {
		// Never block a status write on a decode problem of our own.
		logger.Error(err, "Failed to decode the pod status update; allowing it unchanged")
		return recordFence("decode_error", admission.Allowed("undecodable pod"))
	}
	// The objectSelector matches when either the old or the new object has the
	// label; only the new object counts.
	if pod.Metadata.Labels[util.LabelFenced] != "true" {
		return recordFence("not_fenced", admission.Allowed("pod is not fenced"))
	}
	if pod.Status == nil {
		return recordFence("skipped", admission.Allowed("no status in the update"))
	}

	var stored *fenceCondition
	if len(req.OldObject.Raw) > 0 {
		var old fencePod
		// Best effort: only the stored Ready condition's transition time is read.
		if err := json.Unmarshal(req.OldObject.Raw, &old); err == nil && old.Status != nil {
			stored = findFenceCondition(old.Status.Conditions, "Ready")
		}
	}
	patches := fencePatches(pod.Status.Conditions, stored, m.clock())
	if len(patches) == 0 {
		return recordFence("unchanged", admission.Allowed("already fenced"))
	}
	return recordFence("fenced", admission.Patched("held Ready at False for a fenced pod", patches...))
}

func (m *PodFenceMutator) clock() time.Time {
	if m.now != nil {
		return m.now()
	}
	return time.Now()
}

// fencePatches returns the JSON patch that sets Ready to False with the fence's
// reason and message, or nil when the condition already says that. While the
// stored Ready is False, its transition time is kept, so the kubelet's repeated
// writes leave the stored condition unchanged.
func fencePatches(conds []fenceCondition, stored *fenceCondition, now time.Time) []jsonpatch.JsonPatchOperation {
	ltt := now.UTC().Format(time.RFC3339)
	if stored != nil && stored.Status == "False" && stored.LastTransitionTime != "" {
		ltt = stored.LastTransitionTime
	}
	want := fenceCondition{
		Type:               "Ready",
		Status:             "False",
		Reason:             util.ReasonPodMigrationFenced,
		Message:            fenceMessage,
		LastTransitionTime: ltt,
	}

	idx := -1
	for i := range conds {
		if conds[i].Type == "Ready" {
			idx = i
			break
		}
	}
	switch {
	case idx < 0 && len(conds) == 0:
		return []jsonpatch.JsonPatchOperation{{Operation: "add", Path: "/status/conditions", Value: []fenceCondition{want}}}
	case idx < 0:
		return []jsonpatch.JsonPatchOperation{{Operation: "add", Path: "/status/conditions/-", Value: want}}
	case conds[idx] == want:
		return nil
	}
	base := fmt.Sprintf("/status/conditions/%d", idx)
	return []jsonpatch.JsonPatchOperation{
		{Operation: "add", Path: base + "/status", Value: want.Status},
		{Operation: "add", Path: base + "/reason", Value: want.Reason},
		{Operation: "add", Path: base + "/message", Value: want.Message},
		{Operation: "add", Path: base + "/lastTransitionTime", Value: want.LastTransitionTime},
	}
}

func findFenceCondition(conds []fenceCondition, condType string) *fenceCondition {
	for i := range conds {
		if conds[i].Type == condType {
			return &conds[i]
		}
	}
	return nil
}

func recordFence(action string, resp admission.Response) admission.Response {
	metrics.RecordFenceWebhookRequest(action)
	return resp
}

// SetupFenceWebhookWithManager registers the fence webhook on the manager.
func SetupFenceWebhookWithManager(mgr ctrl.Manager) error {
	mgr.GetWebhookServer().Register(FencePath, &admission.Webhook{Handler: &PodFenceMutator{}})
	return nil
}
