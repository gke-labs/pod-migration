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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestEvictionGate_SkippedMigrationMetrics(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	gvisor := "gvisor"
	unsupported := "unsupported-runtime"

	// 1. Not opted in
	notOptedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-not-opted",
			UID:       types.UID("uid-not-opted"),
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisor,
		},
	}

	// 2. Unsupported runtime
	unsupportedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-unsupported-runtime",
			UID:       types.UID("uid-unsupported"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &unsupported,
		},
	}

	// 3. PDB timed out
	pdbTimedOutPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-pdb-timed-out",
			UID:       types.UID("uid-pdb-timed-out"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationPDBEvictionTimeout: "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisor,
		},
	}

	// 4. Readiness gates unsatisfied
	gateType := corev1.PodConditionType("snapshot.workspaces.gke.io/active")
	unsatisfiedGatePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-unsatisfied-gates",
			UID:       types.UID("uid-unsatisfied-gates"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisor,
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: gateType},
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   gateType,
					Status: corev1.ConditionFalse,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(notOptedPod, unsupportedPod, pdbTimedOutPod, unsatisfiedGatePod).
		Build()

	el, _ := eligibility.ParseRuntimeClassPolicy("gvisor")
	gate := &EvictionGate{
		Client:      fakeClient,
		APIReader:   fakeClient,
		Eligibility: el,
	}

	callGate := func(podName string) admission.Response {
		return gate.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace:   "default",
				Name:        podName,
				SubResource: "eviction",
			},
		})
	}

	// Test 1: Not opted in
	resp := callGate("pod-not-opted")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("not_opted_in", "none")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[not_opted_in, none] = 1, got %v", got)
	}

	// Test 2: Unsupported runtime
	resp = callGate("pod-unsupported-runtime")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("unsupported_runtime", "none")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[unsupported_runtime, none] = 1, got %v", got)
	}

	// Test 3: PDB timed out
	resp = callGate("pod-pdb-timed-out")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("pdb_timed_out", "none")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[pdb_timed_out, none] = 1, got %v", got)
	}

	// Test 4: Readiness gates unsatisfied
	resp = callGate("pod-unsatisfied-gates")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("readiness_gates_unsatisfied", "none")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[readiness_gates_unsatisfied, none] = 1, got %v", got)
	}
}

func TestEvictionGate_EngineSkippedMigrationMetrics(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	runc := "runc"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Annotations: map[string]string{
				engine.CRIUNodeReadyAnnotation: "true",
			},
		},
	}

	// Pod with multi-container spec -> CRIU engine returns multi_container
	podMultiContainer := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-multi-container",
			UID:       types.UID("uid-multi-container"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "node-1",
			RuntimeClassName: &runc,
			Containers: []corev1.Container{
				{Name: "c1", Image: "img1"},
				{Name: "c2", Image: "img2"},
			},
		},
	}

	// Pod with hostNetwork -> CRIU returns host_network
	podHostNetwork := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-host-network",
			UID:       types.UID("uid-host-network"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "node-1",
			RuntimeClassName: &runc,
			HostNetwork:      true,
			Containers: []corev1.Container{
				{Name: "c1", Image: "img1"},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, podMultiContainer, podHostNetwork).
		Build()

	criu, err := engine.NewCRIU(fakeClient, scheme, engine.CRIUAPIGroup)
	if err != nil {
		t.Fatalf("NewCRIU: %v", err)
	}
	reg := engine.NewRegistry(criu.Name())
	if err := reg.Register(criu); err != nil {
		t.Fatalf("Register: %v", err)
	}
	routes, _ := engine.ParseRoutes("runc=criu")
	_ = reg.SetRoutes(routes)

	el, _ := eligibility.ParseRuntimeClassPolicy("runc")
	gate := &EvictionGate{
		Client:      fakeClient,
		APIReader:   fakeClient,
		Eligibility: el,
		Engines:     reg,
	}

	callGate := func(podName string) admission.Response {
		return gate.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace:   "default",
				Name:        podName,
				SubResource: "eviction",
			},
		})
	}

	// 1. Multi container
	resp := callGate("pod-multi-container")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("multi_container", "criu")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[multi_container, criu] = 1, got %v", got)
	}

	// 2. Host network
	resp = callGate("pod-host-network")
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("host_network", "criu")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[host_network, criu] = 1, got %v", got)
	}
}

func TestEvictionGate_SkippedMigrationMetrics_NoPolicyAndUnfenced(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	runtimeClass := "custom-runtime"
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	// Pod with no matching policy
	podNoPolicy := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-no-policy",
			UID:       types.UID("uid-no-policy"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "node-1",
			RuntimeClassName: &runtimeClass,
		},
	}

	// Pod with unfenced engine and no allow-unfenced annotation
	podUnfenced := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-unfenced",
			UID:       types.UID("uid-unfenced"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         "node-1",
			RuntimeClassName: &runtimeClass,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(node, podNoPolicy, podUnfenced).
		Build()

	testEng := &evictionTestEngine{
		name:       "test-unfenced",
		classes:    []string{runtimeClass},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingNone},
	}
	reg := engine.NewRegistry(testEng.Name())
	_ = reg.Register(testEng)

	el, _ := eligibility.ParseRuntimeClassPolicy(runtimeClass)
	gate := &EvictionGate{
		Client:      fakeClient,
		APIReader:   fakeClient,
		Eligibility: el,
		Engines:     reg,
	}

	// 1. Unfenced engine refusal
	resp := gate.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace:   "default",
			Name:        "pod-unfenced",
			SubResource: "eviction",
		},
	})
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("unfenced_engine", "test-unfenced")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[unfenced_engine, test-unfenced] = 1, got %v", got)
	}

	// 2. Allow fencing, but policy is nil -> no_ready_policy
	testEng.guarantees = &engine.Guarantees{Fencing: engine.FencingStopped}
	testEng.policy = nil
	resp = gate.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace:   "default",
			Name:        "pod-no-policy",
			SubResource: "eviction",
		},
	})
	if !resp.Allowed {
		t.Fatalf("expected allowed, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("no_ready_policy", "test-unfenced")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[no_ready_policy, test-unfenced] = 1, got %v", got)
	}
}

func TestEvictionGate_SkippedMigrationMetrics_OperatorReplicaKeysAndEngineResolution(t *testing.T) {
	metrics.ResetCoverageForTest()

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	runtimeClass := "gvisor"
	trueVal := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "custom.io/v1",
		Kind:       "CustomOperator",
		Name:       "custom-parent",
		UID:        types.UID("parent-uid-123"),
		Controller: &trueVal,
	}

	// 1. Operator-owned pod without replica key
	podNoKey := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "pod-no-replica-key",
			GenerateName:    "custom-parent-",
			UID:             types.UID("uid-no-key"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	// 2. Operator-owned pods sharing ambiguous replica key
	podAmbiguous1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "pod-ambiguous-1",
			UID:             types.UID("uid-ambig-1"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				util.LabelReplicaKey:           "shard-0",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	podAmbiguous2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "pod-ambiguous-2",
			UID:             types.UID("uid-ambig-2"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				util.LabelReplicaKey:           "shard-0",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	// 3. Pod with PDB eviction timeout annotation
	podPDB := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-pdb",
			UID:       types.UID("uid-pdb"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationPDBEvictionTimeout: "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	// 4. Pod with unsatisfied readiness gates
	gateType := corev1.PodConditionType("snapshot.workspaces.gke.io/active")
	podGate := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod-gate",
			UID:       types.UID("uid-gate"),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
			ReadinessGates: []corev1.PodReadinessGate{
				{ConditionType: gateType},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: gateType, Status: corev1.ConditionFalse},
			},
		},
	}

	// 5. Operator-owned pod with active in-flight PMJ for the same replica key
	podPMJInFlight := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       "default",
			Name:            "pod-ambig-pmj",
			UID:             types.UID("uid-ambig-pmj"),
			OwnerReferences: []metav1.OwnerReference{ownerRef},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				util.LabelReplicaKey:           "shard-unique",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runtimeClass,
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pmjExisting := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pmj-in-flight-existing",
			Labels: map[string]string{
				util.LabelParentName: "custom-parent",
				util.LabelParentKind: "CustomOperator",
			},
			Annotations: map[string]string{
				util.AnnotationReplicaIdentityKey: "shard-unique",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	pm := &pmv1alpha1.PodMigration{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "default",
			Name:              "test-migration",
			CreationTimestamp: metav1.NewTime(time.Now().Add(-5 * time.Minute)),
		},
		Spec: pmv1alpha1.PodMigrationSpec{
			Storage: pmv1alpha1.StorageSpec{Location: "gs://test-bucket/snapshots"},
		},
	}

	customParent := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "custom.io/v1",
			"kind":       "CustomOperator",
			"metadata": map[string]interface{}{
				"name":      "custom-parent",
				"namespace": "default",
				"uid":       "parent-uid-123",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pm, podNoKey, podAmbiguous1, podAmbiguous2, podPDB, podGate, podPMJInFlight, pmjExisting, customParent).
		WithStatusSubresource(pm).
		Build()

	testEng := &evictionTestEngine{
		name:       "test-operator-eng",
		classes:    []string{runtimeClass},
		verdict:    engine.Verdict{OK: true},
		guarantees: &engine.Guarantees{Fencing: engine.FencingStopped},
		policy:     &engine.PolicyRef{Group: "podsnapshot.gke.io", Kind: "PodSnapshotPolicy", Name: "psp"},
	}
	reg := engine.NewRegistry(testEng.Name())
	_ = reg.Register(testEng)

	el, _ := eligibility.ParseRuntimeClassPolicy(runtimeClass)
	gate := &EvictionGate{
		Client:      fakeClient,
		APIReader:   fakeClient,
		Eligibility: el,
		Engines:     reg,
	}

	callGate := func(name string) admission.Response {
		var p corev1.Pod
		_ = fakeClient.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: name}, &p)
		pName, pKind, _, pErr := util.ResolveParentWorkload(context.Background(), fakeClient, &p)
		rKey, hasKey := util.ResolveOperatorReplicaKey(&p, pKind)
		t.Logf("Pod %s: parentKind=%q, parentName=%q, pErr=%v, hasKey=%v, rKey=%q", name, pKind, pName, pErr, hasKey, rKey)
		return gate.Handle(context.Background(), admission.Request{
			AdmissionRequest: admissionv1.AdmissionRequest{
				Namespace:   "default",
				Name:        name,
				SubResource: "eviction",
			},
		})
	}

	// 1. No replica key
	resp := callGate("pod-no-replica-key")
	if !resp.Allowed {
		t.Fatalf("expected allowed for no-replica-key pod, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("no_replica_key", "test-operator-eng")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[no_replica_key, test-operator-eng] = 1, got %v", got)
	}

	// 2. Ambiguous replica key
	resp = callGate("pod-ambiguous-1")
	if !resp.Allowed {
		t.Fatalf("expected allowed for ambiguous-replica-key pod, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("ambiguous_replica_key", "test-operator-eng")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[ambiguous_replica_key, test-operator-eng] = 1, got %v", got)
	}

	// 3. PDB timed out with engine resolved
	resp = callGate("pod-pdb")
	if !resp.Allowed {
		t.Fatalf("expected allowed for pod-pdb, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("pdb_timed_out", "test-operator-eng")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[pdb_timed_out, test-operator-eng] = 1, got %v", got)
	}

	// 4. Readiness gates unsatisfied with engine resolved
	resp = callGate("pod-gate")
	if !resp.Allowed {
		t.Fatalf("expected allowed for pod-gate, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("readiness_gates_unsatisfied", "test-operator-eng")); got != 1 {
		t.Errorf("expected SkippedMigrationsTotal[readiness_gates_unsatisfied, test-operator-eng] = 1, got %v", got)
	}

	// 5. Ambiguous replica key with active in-flight PMJ
	resp = callGate("pod-ambig-pmj")
	if !resp.Allowed {
		t.Fatalf("expected allowed for pod-ambig-pmj, got %v", resp)
	}
	if got := testutil.ToFloat64(metrics.SkippedMigrationsTotal.WithLabelValues("ambiguous_replica_key", "test-operator-eng")); got != 2 {
		t.Errorf("expected SkippedMigrationsTotal[ambiguous_replica_key, test-operator-eng] = 2, got %v", got)
	}
}
