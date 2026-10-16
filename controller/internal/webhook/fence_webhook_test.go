package webhook

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	jsonpatchapply "github.com/evanphx/json-patch/v5"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/controller/internal/util"
)

var fenceTestNow = time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)

func fenceRequest(t *testing.T, subResource string, newPod, oldPod map[string]interface{}) admission.Request {
	t.Helper()
	raw := func(obj map[string]interface{}) runtime.RawExtension {
		if obj == nil {
			return runtime.RawExtension{}
		}
		b, err := json.Marshal(obj)
		if err != nil {
			t.Fatalf("marshal pod: %v", err)
		}
		return runtime.RawExtension{Raw: b}
	}
	return admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation:   admissionv1.Update,
		Name:        "source",
		Namespace:   "default",
		SubResource: subResource,
		Object:      raw(newPod),
		OldObject:   raw(oldPod),
	}}
}

// fenceTestPod builds a raw pod with the given labels and conditions, plus a status
// field that this controller's API version doesn't know.
func fenceTestPod(labels map[string]string, conditions ...map[string]interface{}) map[string]interface{} {
	conds := make([]interface{}, 0, len(conditions))
	for _, c := range conditions {
		conds = append(conds, c)
	}
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]interface{}{"name": "source", "namespace": "default", "labels": labels},
		"status": map[string]interface{}{
			"phase":            "Running",
			"podIP":            "10.0.0.7",
			"futureFieldNoOne": map[string]interface{}{"kept": true},
			"conditions":       conds,
		},
	}
}

func cond(condType, status, reason, ltt string) map[string]interface{} {
	c := map[string]interface{}{"type": condType, "status": status, "lastTransitionTime": ltt}
	if reason != "" {
		c["reason"] = reason
	}
	return c
}

var fencedLabels = map[string]string{"app": "db", util.LabelFenced: "true"}

// applyFence runs the handler and returns the response and the status after its patch.
func applyFence(t *testing.T, req admission.Request) (admission.Response, map[string]interface{}) {
	t.Helper()
	resp := (&PodFenceMutator{now: func() time.Time { return fenceTestNow }}).Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("response not allowed: %+v", resp.Result)
	}
	patched := req.Object.Raw
	if len(resp.Patches) > 0 {
		ops, err := json.Marshal(resp.Patches)
		if err != nil {
			t.Fatalf("marshal patches: %v", err)
		}
		patch, err := jsonpatchapply.DecodePatch(ops)
		if err != nil {
			t.Fatalf("decode patch %s: %v", ops, err)
		}
		if patched, err = patch.Apply(req.Object.Raw); err != nil {
			t.Fatalf("apply patch %s: %v", ops, err)
		}
	}
	var pod map[string]interface{}
	if err := json.Unmarshal(patched, &pod); err != nil {
		t.Fatalf("unmarshal patched pod: %v", err)
	}
	return resp, pod["status"].(map[string]interface{})
}

func readyOf(t *testing.T, status map[string]interface{}) map[string]interface{} {
	t.Helper()
	for _, c := range status["conditions"].([]interface{}) {
		if m := c.(map[string]interface{}); m["type"] == "Ready" {
			return m
		}
	}
	t.Fatalf("no Ready condition in %v", status)
	return nil
}

func TestPodFenceMutator_ReadyTrueBecomesFalse(t *testing.T) {
	stored := fenceTestPod(fencedLabels, cond("Ready", "True", "", "2026-10-16T11:00:00Z"))
	incoming := fenceTestPod(fencedLabels,
		cond("Initialized", "True", "", "2026-10-16T10:00:00Z"),
		cond("Ready", "True", "", "2026-10-16T11:00:00Z"),
		cond("ContainersReady", "True", "", "2026-10-16T11:00:00Z"))
	resp, status := applyFence(t, fenceRequest(t, "status", incoming, stored))
	if len(resp.Patches) == 0 {
		t.Fatal("expected a patch")
	}
	ready := readyOf(t, status)
	if ready["status"] != "False" || ready["reason"] != util.ReasonPodMigrationFenced || ready["message"] != fenceMessage {
		t.Errorf("Ready = %v, want False/%s with the fence message", ready, util.ReasonPodMigrationFenced)
	}
	if ready["lastTransitionTime"] != fenceTestNow.Format(time.RFC3339) {
		t.Errorf("lastTransitionTime = %v, want now (%s)", ready["lastTransitionTime"], fenceTestNow.Format(time.RFC3339))
	}
	conds := status["conditions"].([]interface{})
	if len(conds) != 3 || conds[2].(map[string]interface{})["status"] != "True" {
		t.Errorf("other conditions changed: %v", conds)
	}
	if status["futureFieldNoOne"] == nil || status["podIP"] != "10.0.0.7" {
		t.Errorf("fields outside the Ready condition were not kept: %v", status)
	}
}

func TestPodFenceMutator_KeepsStoredTransitionTime(t *testing.T) {
	// The stored condition is already fenced; the kubelet sends its own view again.
	storedReady := cond("Ready", "False", util.ReasonPodMigrationFenced, "2026-10-16T11:30:00Z")
	storedReady["message"] = fenceMessage
	stored := fenceTestPod(fencedLabels, storedReady)
	for _, incomingReady := range []map[string]interface{}{
		cond("Ready", "True", "", "2026-10-16T11:59:00Z"),
		cond("Ready", "False", "ContainersNotReady", "2026-10-16T11:59:00Z"),
	} {
		_, status := applyFence(t, fenceRequest(t, "status", fenceTestPod(fencedLabels, incomingReady), stored))
		ready := readyOf(t, status)
		if ready["status"] != "False" || ready["reason"] != util.ReasonPodMigrationFenced {
			t.Errorf("incoming %v: Ready = %v, want False/%s", incomingReady, ready, util.ReasonPodMigrationFenced)
		}
		if ready["lastTransitionTime"] != "2026-10-16T11:30:00Z" {
			t.Errorf("incoming %v: lastTransitionTime = %v, want the stored 2026-10-16T11:30:00Z", incomingReady, ready["lastTransitionTime"])
		}
	}
}

func TestPodFenceMutator_AlreadyFencedIsUnchanged(t *testing.T) {
	ready := cond("Ready", "False", util.ReasonPodMigrationFenced, "2026-10-16T11:30:00Z")
	ready["message"] = fenceMessage
	pod := fenceTestPod(fencedLabels, ready)
	resp, _ := applyFence(t, fenceRequest(t, "status", pod, pod))
	if len(resp.Patches) != 0 {
		t.Errorf("expected no patch for an already fenced condition, got %v", resp.Patches)
	}
}

func TestPodFenceMutator_AddsMissingReady(t *testing.T) {
	for name, incoming := range map[string]map[string]interface{}{
		"other conditions only": fenceTestPod(fencedLabels, cond("PodScheduled", "True", "", "2026-10-16T10:00:00Z")),
		"no conditions":         fenceTestPod(fencedLabels),
	} {
		t.Run(name, func(t *testing.T) {
			_, status := applyFence(t, fenceRequest(t, "status", incoming, nil))
			ready := readyOf(t, status)
			if ready["status"] != "False" || ready["reason"] != util.ReasonPodMigrationFenced {
				t.Errorf("Ready = %v, want False/%s", ready, util.ReasonPodMigrationFenced)
			}
		})
	}
}

func TestPodFenceMutator_LeavesOtherRequestsAlone(t *testing.T) {
	unfenced := fenceTestPod(map[string]string{"app": "db"}, cond("Ready", "True", "", "2026-10-16T11:00:00Z"))
	fenced := fenceTestPod(fencedLabels, cond("Ready", "True", "", "2026-10-16T11:00:00Z"))
	undecodable := fenceRequest(t, "status", nil, nil)
	undecodable.Object = runtime.RawExtension{Raw: []byte(`{"metadata":`)}
	for name, req := range map[string]admission.Request{
		// The objectSelector also matches when only the old object has the label.
		"label removed":     fenceRequest(t, "status", unfenced, fenced),
		"not a status call": fenceRequest(t, "", fenced, fenced),
		"undecodable":       undecodable,
	} {
		t.Run(name, func(t *testing.T) {
			resp := (&PodFenceMutator{}).Handle(context.Background(), req)
			if !resp.Allowed || len(resp.Patches) != 0 {
				t.Errorf("expected allowed without a patch, got allowed=%t patches=%v", resp.Allowed, resp.Patches)
			}
		})
	}
}
