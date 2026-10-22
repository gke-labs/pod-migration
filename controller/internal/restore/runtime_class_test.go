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

package restore

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runtimeClassEngine is a scoped engine: it matches messages containing
// "restore failed", but only in pods of its runtime classes.
type runtimeClassEngine struct {
	name    string
	classes []string
}

func (e runtimeClassEngine) Name() string { return e.name }
func (e runtimeClassEngine) MatchesRestoreFailure(msg string) (string, bool) {
	if strings.Contains(strings.ToLower(msg), "restore failed") {
		return e.name + "-signature", true
	}
	return "", false
}
func (e runtimeClassEngine) CoversRuntimeClass(runtimeClass string) bool {
	for _, c := range e.classes {
		if c == runtimeClass {
			return true
		}
	}
	return false
}

// crashedPod is a pod of the given runtime class ("" for none) whose container
// failed to start with message.
func crashedPod(runtimeClass, message string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "p"},
		Status:     restoreCrashPodStatus(message),
	}
	if runtimeClass != "" {
		pod.Spec.RuntimeClassName = &runtimeClass
	}
	return pod
}

func TestGVisor_CoversOnlyTheGVisorRuntimeClass(t *testing.T) {
	for class, want := range map[string]bool{
		"gvisor":       true,
		"runc":         false,
		"@default":     false,
		"":             false,
		"gvisor-debug": false,
	} {
		if got := (GVisor{}).CoversRuntimeClass(class); got != want {
			t.Errorf("CoversRuntimeClass(%q) = %t, want %t", class, got, want)
		}
	}
}

// Only the engines that cover a pod's runtime class may classify its start
// failure (#132). gVisor's generalised "restore failed" signature also matches
// runc and CRIU wording, so applying it to a runc pod would delete the pod on
// a failure that isn't gVisor's.
func TestClassify_OnlyEnginesThatCoverThePodRuntimeClass(t *testing.T) {
	const runcRestoreFailure = "runc: restore failed: criu returned 1"
	criu := runtimeClassEngine{name: "criu", classes: []string{"runc", "@default"}}

	tests := []struct {
		name          string
		runtimeClass  string
		engines       []Engine
		wantClass     FailureClass
		wantEngine    string
		wantSignature string
	}{
		{
			name:          "gVisor still classifies gVisor pods",
			runtimeClass:  "gvisor",
			engines:       DefaultEngines(),
			wantClass:     FailureFatal,
			wantEngine:    "gvisor",
			wantSignature: "restore failed",
		},
		{
			name:         "a runc pod gets no gVisor verdict",
			runtimeClass: "runc",
			engines:      DefaultEngines(),
			wantClass:    FailureUnrecognized,
		},
		{
			name:         "a pod without a runtime class gets no gVisor verdict",
			runtimeClass: "",
			engines:      DefaultEngines(),
			wantClass:    FailureUnrecognized,
		},
		{
			name:          "an engine that covers runc classifies the runc pod",
			runtimeClass:  "runc",
			engines:       []Engine{GVisor{}, criu},
			wantClass:     FailureFatal,
			wantEngine:    "criu",
			wantSignature: "criu-signature",
		},
		{
			name:          "the default token covers pods without a runtime class",
			runtimeClass:  "",
			engines:       []Engine{GVisor{}, criu},
			wantClass:     FailureFatal,
			wantEngine:    "criu",
			wantSignature: "criu-signature",
		},
		{
			name:          "an engine that covers runc isn't offered gVisor pods",
			runtimeClass:  "gvisor",
			engines:       []Engine{criu, GVisor{}},
			wantClass:     FailureFatal,
			wantEngine:    "gvisor",
			wantSignature: "restore failed",
		},
		{
			name:          "an unscoped engine is offered every pod",
			runtimeClass:  "runc",
			engines:       []Engine{matchAllEngine{name: "unscoped"}},
			wantClass:     FailureFatal,
			wantEngine:    "unscoped",
			wantSignature: "match-all",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Classify(crashedPod(tt.runtimeClass, runcRestoreFailure), tt.engines...)
			if got.Class != tt.wantClass {
				t.Fatalf("class = %v, want %v (verdict %+v)", got.Class, tt.wantClass, got)
			}
			if got.Engine != tt.wantEngine {
				t.Errorf("engine = %q, want %q", got.Engine, tt.wantEngine)
			}
			if got.Signature != tt.wantSignature {
				t.Errorf("signature = %q, want %q", got.Signature, tt.wantSignature)
			}
			if got.Container != "app" {
				t.Errorf("container = %q, want app", got.Container)
			}
		})
	}
}
