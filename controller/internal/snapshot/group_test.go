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

package snapshot

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestValidateGroup(t *testing.T) {
	// Longest plural is "podsnapshotstorageconfigs" (25 chars) + "." (1 char) = 26 chars.
	// DNS-1123 subdomain max length is 253 chars, so group max is 227 chars; each label <= 63 chars.
	label60 := strings.Repeat("a", 60)
	group227 := label60 + "." + label60 + "." + label60 + "." + strings.Repeat("b", 44) // 60+1+60+1+60+1+44 = 227
	group228 := group227 + "c"

	tests := []struct {
		name    string
		group   string
		wantErr bool
	}{
		{name: "default GKE group", group: DefaultAPIGroup},
		{name: "dedicated CRIU group", group: CRIUAPIGroup},
		{name: "max length 227 chars", group: group227},
		{name: "exceeds 253-char CRD name (228 chars)", group: group228, wantErr: true},
		{name: "empty", group: "", wantErr: true},
		{name: "single label without dot", group: "criu", wantErr: true},
		{name: "uppercase", group: "CRIU.podmigration.gke.io", wantErr: true},
		{name: "underscore", group: "criu_engine.gke.io", wantErr: true},
		{name: "trailing dot", group: "criu.podmigration.gke.io.", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGroup(tt.group)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidateGroup(%q) error = %v, wantErr = %t", tt.group, err, tt.wantErr)
			}
		})
	}
}

func TestGKEProviderForGroup_UsesConfiguredGroupAndAnnotation(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	const (
		ns          = "default"
		sourceName  = "app-0"
		sourceUID   = "uid-source-12345678"
		replName    = "app-0-repl"
		replUID     = "uid-repl-87654321"
		snapName    = "ps-app-0-uid-sour"
		customGroup = CRIUAPIGroup
		customAnn   = customGroup + "/ps-name"
	)

	sourcePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      sourceName,
			UID:       types.UID(sourceUID),
		},
	}
	replPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      replName,
			UID:       types.UID(replUID),
			Annotations: map[string]string{
				customAnn: snapName,
			},
		},
	}
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      util.FormatPMJName(sourceName, sourceUID),
			UID:       "job-uid-1",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: sourceName},
			TargetPodUID: sourceUID,
		},
	}

	// Decoy failed trigger in podsnapshot.gke.io with the exact same name:
	// a provider bound to criu.podmigration.gke.io must ignore it completely.
	triggerName := util.FormatPSMTName(sourceName, sourceUID)
	decoyTrigger := &unstructured.Unstructured{}
	decoyTrigger.SetGroupVersionKind(ManualTriggerGVK(DefaultAPIGroup))
	decoyTrigger.SetNamespace(ns)
	decoyTrigger.SetName(triggerName)
	decoyTrigger.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":    "Triggered",
				"status":  "False",
				"reason":  "Failed",
				"message": "decoy failure in default group",
			},
		},
	}

	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(sourcePod, replPod, job, decoyTrigger).
		Build()

	provider, err := NewGKEProviderForGroup(cl, scheme, customGroup)
	if err != nil {
		t.Fatalf("NewGKEProviderForGroup: %v", err)
	}
	if got := provider.APIGroup(); got != customGroup {
		t.Fatalf("APIGroup() = %q, want %q", got, customGroup)
	}
	if got := provider.RestoreAnnotation(); got != customAnn {
		t.Fatalf("RestoreAnnotation() = %q, want %q", got, customAnn)
	}

	ctx := context.Background()

	// 1. EnsureTrigger stamps <customGroup>/ps-name="" on source and creates trigger in customGroup.
	if _, err := provider.EnsureTrigger(ctx, job, sourceName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}
	gotSource := &corev1.Pod{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: sourceName}, gotSource); err != nil {
		t.Fatalf("Get source pod: %v", err)
	}
	if v, ok := gotSource.Annotations[customAnn]; !ok || v != "" {
		t.Errorf("source %s = %q (present=%t), want empty cold-start bypass", customAnn, v, ok)
	}
	if _, ok := gotSource.Annotations[psNameAnnotation]; ok {
		t.Errorf("source unexpectedly received %s annotation", psNameAnnotation)
	}

	createdTrigger := &unstructured.Unstructured{}
	createdTrigger.SetGroupVersionKind(ManualTriggerGVK(customGroup))
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: triggerName}, createdTrigger); err != nil {
		t.Fatalf("expected trigger in %s: %v", customGroup, err)
	}

	// 2. Populate snapshotCreated and Ready PodSnapshot in customGroup; CheckStatus must report Ready.
	_ = unstructured.SetNestedField(createdTrigger.Object, snapName, "status", "snapshotCreated", "name")
	if err := cl.Update(ctx, createdTrigger); err != nil {
		t.Fatalf("Update trigger: %v", err)
	}
	snap := &unstructured.Unstructured{}
	snap.SetGroupVersionKind(PodSnapshotGVK(customGroup))
	snap.SetNamespace(ns)
	snap.SetName(snapName)
	snap.SetLabels(map[string]string{snapshotTriggeredByLabel: triggerName})
	snap.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{"type": "Ready", "status": "True", "reason": "Succeeded"},
		},
	}
	if err := cl.Create(ctx, snap); err != nil {
		t.Fatalf("Create PodSnapshot in %s: %v", customGroup, err)
	}

	st, err := provider.CheckStatus(ctx, job, sourceName)
	if err != nil {
		t.Fatalf("CheckStatus: %v", err)
	}
	if st.Phase != PhaseReady || st.SnapshotRef != snapName {
		t.Fatalf("CheckStatus = %+v, want PhaseReady with SnapshotRef %q", st, snapName)
	}

	// 3. CompleteRestore clears <customGroup>/ps-name to "" and records restore-completed.
	job.Status.SnapshotRef = snapName
	if err := provider.CompleteRestore(ctx, job, replName, types.UID(replUID)); err != nil {
		t.Fatalf("CompleteRestore: %v", err)
	}
	gotRepl := &corev1.Pod{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: replName}, gotRepl); err != nil {
		t.Fatalf("Get replacement pod: %v", err)
	}
	if v, ok := gotRepl.Annotations[customAnn]; !ok || v != "" {
		t.Errorf("replacement %s = %q (present=%t), want empty cold-start bypass", customAnn, v, ok)
	}
	if got := gotRepl.Annotations[util.AnnotationRestoreCompleted]; got != snapName {
		t.Errorf("replacement %s = %q, want %q", util.AnnotationRestoreCompleted, got, snapName)
	}

	// 4. Cleanup deletes the trigger in customGroup while leaving the decoy in DefaultAPIGroup untouched,
	// and CheckpointState reads snapshots in customGroup.
	if err := provider.Cleanup(ctx, job, sourceName); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: triggerName}, createdTrigger); !apierrors.IsNotFound(err) {
		t.Fatalf("expected trigger in %s to be deleted, got err=%v", customGroup, err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: triggerName}, decoyTrigger); err != nil {
		t.Fatalf("decoy trigger in %s should remain untouched: %v", DefaultAPIGroup, err)
	}
	cpState, err := provider.CheckpointState(ctx, job, sourceName)
	if err != nil {
		t.Fatalf("CheckpointState: %v", err)
	}
	if !cpState.Settled || !cpState.Completed {
		t.Errorf("CheckpointState = %+v, want {Settled:true Completed:true}", cpState)
	}
}
