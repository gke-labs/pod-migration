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

package controller

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"

	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

const (
	// The admission policies of #233, as the manifests name them.
	restoreOrderPolicyName  = "pod-migration-restore-order"
	restoreStatusPolicyName = "pod-migrate-psengine-restore-status"

	// The only identities the policies let write the protected annotations.
	controllerUsername = "system:serviceaccount:pod-migration-system:pod-migration-controller"
	criuAgentUsername  = "system:serviceaccount:pod-migration-criu-system:pod-migrate-agent"

	// optInLabel opts a pod into migration.
	optInLabel = "pod-migration.gke.io/enabled"
)

func controllerManifestPath() string {
	return filepath.Join("..", "..", "deploy.yaml")
}

func criuManifestPath() string {
	return filepath.Join("..", "..", "..", "engines", "criu", "deploy", "deploy.yaml")
}

// manifestObjects decodes every object of a multi-document manifest.
func manifestObjects(t *testing.T, path string) []*unstructured.Unstructured {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	decoder := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096)
	var out []*unstructured.Unstructured
	for {
		obj := &unstructured.Unstructured{}
		if err := decoder.Decode(&obj.Object); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			t.Fatalf("decoding %s: %v", path, err)
		}
		if len(obj.Object) == 0 {
			continue
		}
		out = append(out, obj)
	}
	return out
}

// manifestObject decodes the object of the given kind and name into into.
func manifestObject(t *testing.T, path, kind, name string, into interface{}) {
	t.Helper()
	for _, obj := range manifestObjects(t, path) {
		if obj.GetKind() != kind || obj.GetName() != name {
			continue
		}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, into); err != nil {
			t.Fatalf("converting %s %s of %s: %v", kind, name, path, err)
		}
		return
	}
	t.Fatalf("%s has no %s named %s", path, kind, name)
}

func admissionPolicy(t *testing.T, path, name string) (*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
	t.Helper()
	policy := &admissionregistrationv1.ValidatingAdmissionPolicy{}
	manifestObject(t, path, "ValidatingAdmissionPolicy", name, policy)
	binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
	manifestObject(t, path, "ValidatingAdmissionPolicyBinding", name, binding)
	return policy, binding
}

// assertPodUpdatePolicy checks what both policies of #233 have in common:
// they see only updates of pods and of pods/status (a status write can change
// annotations too), fail closed, are skipped unless a protected annotation
// changes, and their binding denies.
func assertPodUpdatePolicy(t *testing.T, policy *admissionregistrationv1.ValidatingAdmissionPolicy, binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding) {
	t.Helper()
	if policy.Spec.FailurePolicy == nil || *policy.Spec.FailurePolicy != admissionregistrationv1.Fail {
		t.Errorf("failurePolicy = %v, want Fail", policy.Spec.FailurePolicy)
	}
	if policy.Spec.ParamKind != nil {
		t.Errorf("paramKind = %v, want none: the binding has no params", policy.Spec.ParamKind)
	}
	if policy.Spec.MatchConstraints == nil || len(policy.Spec.MatchConstraints.ResourceRules) != 1 {
		t.Fatalf("matchConstraints = %+v, want exactly one resource rule", policy.Spec.MatchConstraints)
	}
	rule := policy.Spec.MatchConstraints.ResourceRules[0]
	if !reflect.DeepEqual(rule.Operations, []admissionregistrationv1.OperationType{admissionregistrationv1.Update}) {
		t.Errorf("operations = %v, want only UPDATE: creation is the webhooks' job", rule.Operations)
	}
	if !reflect.DeepEqual(rule.APIGroups, []string{""}) || !reflect.DeepEqual(rule.Resources, []string{"pods", "pods/status"}) {
		t.Errorf("rule matches groups %v resources %v, want pods and pods/status of the core group", rule.APIGroups, rule.Resources)
	}
	if len(rule.ResourceNames) != 0 {
		t.Errorf("resourceNames = %v, want none", rule.ResourceNames)
	}
	if len(policy.Spec.MatchConditions) == 0 {
		t.Errorf("no matchConditions: the policy would be evaluated for every pod update")
	}
	if len(policy.Spec.Validations) == 0 {
		t.Errorf("no validations")
	}
	if binding.Spec.PolicyName != policy.Name {
		t.Errorf("binding's policyName = %q, want %q", binding.Spec.PolicyName, policy.Name)
	}
	if !reflect.DeepEqual(binding.Spec.ValidationActions, []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny}) {
		t.Errorf("binding's validationActions = %v, want [Deny]", binding.Spec.ValidationActions)
	}
	if binding.Spec.MatchResources != nil || binding.Spec.ParamRef != nil {
		t.Errorf("binding narrows the policy (matchResources %v, paramRef %v), want it bound as is", binding.Spec.MatchResources, binding.Spec.ParamRef)
	}
}

func validationExpressions(policy *admissionregistrationv1.ValidatingAdmissionPolicy) string {
	var b strings.Builder
	for _, v := range policy.Spec.Validations {
		b.WriteString(v.Expression)
		b.WriteString("\n")
	}
	return b.String()
}

// The restore order policy in controller/deploy.yaml names the controller's
// service account, and has the scope of mpodgate, the webhook that handles
// restore orders when a pod is created (#233). The manifest has no templating,
// so nothing else keeps these in step.
func TestRestoreOrderPolicy_MatchesTheControllerManifest(t *testing.T) {
	path := controllerManifestPath()
	policy, binding := admissionPolicy(t, path, restoreOrderPolicyName)
	assertPodUpdatePolicy(t, policy, binding)

	// The identity the policy allows is the one the controller runs as.
	deployment := &appsv1.Deployment{}
	manifestObject(t, path, "Deployment", "pod-migration-controller", deployment)
	username := "system:serviceaccount:" + deployment.Namespace + ":" + deployment.Spec.Template.Spec.ServiceAccountName
	if username != controllerUsername {
		t.Errorf("the controller runs as %q, want %q", username, controllerUsername)
	}
	if got := validationExpressions(policy); !strings.Contains(got, "'"+username+"'") {
		t.Errorf("the policy's validations don't name the controller's identity %q:\n%s", username, got)
	}

	// Same pods and namespaces as mpodgate.
	webhooks := &admissionregistrationv1.MutatingWebhookConfiguration{}
	manifestObject(t, path, "MutatingWebhookConfiguration", "mutating-webhook-configuration", webhooks)
	var gate *admissionregistrationv1.MutatingWebhook
	for i := range webhooks.Webhooks {
		if webhooks.Webhooks[i].Name == "mpodgate.podmigration.gke.io" {
			gate = &webhooks.Webhooks[i]
		}
	}
	if gate == nil {
		t.Fatalf("%s has no mpodgate webhook", path)
	}
	if !reflect.DeepEqual(policy.Spec.MatchConstraints.ObjectSelector, gate.ObjectSelector) {
		t.Errorf("objectSelector = %v, want mpodgate's %v", policy.Spec.MatchConstraints.ObjectSelector, gate.ObjectSelector)
	}
	if !reflect.DeepEqual(policy.Spec.MatchConstraints.NamespaceSelector, gate.NamespaceSelector) {
		t.Errorf("namespaceSelector = %v, want mpodgate's %v", policy.Spec.MatchConstraints.NamespaceSelector, gate.NamespaceSelector)
	}

	// The policy is written for any engine's group: it matches the suffix
	// that every engine's restore annotation has.
	const suffix = "/ps-name"
	for _, group := range []string{engine.DefaultAPIGroup, engine.DefaultCRIUAPIGroup, "snapshots.example.com"} {
		if got := engine.RestoreAnnotation(group); !strings.HasSuffix(got, suffix) {
			t.Errorf("RestoreAnnotation(%q) = %q, want the %q suffix that the policy matches", group, got, suffix)
		}
	}
	for _, mc := range policy.Spec.MatchConditions {
		if !strings.Contains(mc.Expression, "endsWith('"+suffix+"')") {
			t.Errorf("matchCondition %s doesn't match annotations by the %q suffix:\n%s", mc.Name, suffix, mc.Expression)
		}
	}
}

// The restore status policy in the CRIU engine's manifest names the node
// agent's service account, and the annotations and values that the controller
// reads: the outcome (#233) and the reason of a failure (#244).
func TestRestoreStatusPolicy_MatchesTheCRIUManifest(t *testing.T) {
	path := criuManifestPath()
	policy, binding := admissionPolicy(t, path, restoreStatusPolicyName)
	assertPodUpdatePolicy(t, policy, binding)

	agent := &appsv1.DaemonSet{}
	manifestObject(t, path, "DaemonSet", "pod-migrate-agent", agent)
	username := "system:serviceaccount:" + agent.Namespace + ":" + agent.Spec.Template.Spec.ServiceAccountName
	if username != criuAgentUsername {
		t.Errorf("the node agent runs as %q, want %q", username, criuAgentUsername)
	}
	validations := validationExpressions(policy)
	if !strings.Contains(validations, "'"+username+"'") {
		t.Errorf("the policy's validations don't name the node agent's identity %q:\n%s", username, validations)
	}
	for _, value := range []string{engine.CRIURestoreStatusRestored, engine.CRIURestoreStatusFailed} {
		if !strings.Contains(validations, "'"+value+"'") {
			t.Errorf("the policy's validations don't allow the outcome %q:\n%s", value, validations)
		}
	}
	// The policy also guards the reason that VerifyRestore puts in a cold
	// start's evidence (#244). matchConditions are ANDed: each must notice a
	// change to either annotation.
	for _, mc := range policy.Spec.MatchConditions {
		for _, key := range []string{engine.CRIURestoreStatusAnnotation, engine.CRIURestoreReasonAnnotation} {
			if !strings.Contains(mc.Expression, "'"+key+"'") {
				t.Errorf("matchCondition %s doesn't watch %s:\n%s", mc.Name, key, mc.Expression)
			}
		}
	}
	if !strings.Contains(validations, "'"+engine.CRIURestoreReasonAnnotation+"'") {
		t.Errorf("the policy's validations don't check %s:\n%s", engine.CRIURestoreReasonAnnotation, validations)
	}

	// Unlike the restore order, the outcome is the engine's own on any pod:
	// no opt-in label limits the policy. Its namespaces are those of the
	// engine's restore webhook.
	if policy.Spec.MatchConstraints.ObjectSelector != nil {
		t.Errorf("objectSelector = %v, want none", policy.Spec.MatchConstraints.ObjectSelector)
	}
	webhooks := &admissionregistrationv1.MutatingWebhookConfiguration{}
	manifestObject(t, path, "MutatingWebhookConfiguration", "pod-migrate-psengine-restore-inject", webhooks)
	if len(webhooks.Webhooks) != 1 {
		t.Fatalf("the restore webhook configuration has %d webhooks, want 1", len(webhooks.Webhooks))
	}
	if !reflect.DeepEqual(policy.Spec.MatchConstraints.NamespaceSelector, webhooks.Webhooks[0].NamespaceSelector) {
		t.Errorf("namespaceSelector = %v, want the restore webhook's %v",
			policy.Spec.MatchConstraints.NamespaceSelector, webhooks.Webhooks[0].NamespaceSelector)
	}
}
