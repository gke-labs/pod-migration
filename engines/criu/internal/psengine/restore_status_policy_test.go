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

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	k8syaml "sigs.k8s.io/yaml"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// The admission policy in deploy.yaml lets only the node agent record a
// restore outcome on an existing pod, once (#233). The manifest has no
// templating, so this keeps the policy in step with the agent's identity and
// with the annotation and values that the agent writes
// (NodeRestoreReconciler). The controller module's envtest applies the policy
// to a real API server.
func TestDeployManifestRestoreStatusPolicyMatchesTheAgent(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deploy.yaml"))
	if err != nil {
		t.Fatalf("read deploy.yaml: %v", err)
	}
	const name = "pod-migrate-psengine-restore-status"
	var (
		policy  *admissionregistrationv1.ValidatingAdmissionPolicy
		binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding
		agent   *appsv1.DaemonSet
	)
	// Pick each object by kind and name, so that another policy, binding or
	// DaemonSet added to the manifest later can't be taken for these.
	for _, doc := range strings.Split(string(raw), "\n---") {
		var meta struct {
			Kind     string `json:"kind"`
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
		}
		if err := k8syaml.Unmarshal([]byte(doc), &meta); err != nil {
			t.Fatalf("unmarshal a document of deploy.yaml: %v", err)
		}
		switch {
		case meta.Kind == "ValidatingAdmissionPolicy" && meta.Metadata.Name == name:
			policy = &admissionregistrationv1.ValidatingAdmissionPolicy{}
			if err := k8syaml.Unmarshal([]byte(doc), policy); err != nil {
				t.Fatalf("unmarshal ValidatingAdmissionPolicy: %v", err)
			}
		case meta.Kind == "ValidatingAdmissionPolicyBinding" && meta.Metadata.Name == name:
			binding = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
			if err := k8syaml.Unmarshal([]byte(doc), binding); err != nil {
				t.Fatalf("unmarshal ValidatingAdmissionPolicyBinding: %v", err)
			}
		case meta.Kind == "DaemonSet" && meta.Metadata.Name == "pod-migrate-agent":
			agent = &appsv1.DaemonSet{}
			if err := k8syaml.Unmarshal([]byte(doc), agent); err != nil {
				t.Fatalf("unmarshal DaemonSet: %v", err)
			}
		}
	}
	if policy == nil {
		t.Fatalf("deploy.yaml has no ValidatingAdmissionPolicy %s", name)
	}
	if binding == nil || binding.Spec.PolicyName != name {
		t.Fatalf("deploy.yaml has no ValidatingAdmissionPolicyBinding for %s (found %+v)", name, binding)
	}
	if agent == nil {
		t.Fatal("deploy.yaml has no pod-migrate-agent DaemonSet")
	}
	if len(binding.Spec.ValidationActions) != 1 || binding.Spec.ValidationActions[0] != admissionregistrationv1.Deny {
		t.Errorf("binding's validationActions = %v, want [Deny]", binding.Spec.ValidationActions)
	}

	var validations strings.Builder
	for _, v := range policy.Spec.Validations {
		validations.WriteString(v.Expression + "\n")
	}
	username := "system:serviceaccount:" + agent.Namespace + ":" + agent.Spec.Template.Spec.ServiceAccountName
	if !strings.Contains(validations.String(), "'"+username+"'") {
		t.Errorf("the policy's validations don't name the agent's identity %q:\n%s", username, validations.String())
	}
	for _, value := range []string{contract.RestoreStatusRestored, contract.RestoreStatusFailed} {
		if !strings.Contains(validations.String(), "'"+value+"'") {
			t.Errorf("the policy's validations don't allow the outcome %q:\n%s", value, validations.String())
		}
	}
	if len(policy.Spec.MatchConditions) == 0 {
		t.Fatal("the policy has no matchConditions: it would be evaluated for every pod update")
	}
	// matchConditions are ANDed: each must notice a change to either
	// annotation, or a change to the reason alone would skip the policy.
	for _, mc := range policy.Spec.MatchConditions {
		for _, key := range []string{contract.AnnotationRestoreStatus, contract.AnnotationRestoreReason} {
			if !strings.Contains(mc.Expression, "'"+key+"'") {
				t.Errorf("matchCondition %s doesn't watch %s:\n%s", mc.Name, key, mc.Expression)
			}
		}
	}
	// The reason goes only with failed (#244).
	if !strings.Contains(validations.String(), "'"+contract.AnnotationRestoreReason+"'") {
		t.Errorf("the policy's validations don't check %s:\n%s", contract.AnnotationRestoreReason, validations.String())
	}
}
