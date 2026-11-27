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
	"fmt"
	"net/http"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestPodGateInjector(t *testing.T) {
	tests := []struct {
		name            string
		pod             *corev1.Pod
		initObjs        []runtime.Object
		expectedAllowed bool
		expectGate      bool
		expectBypass    bool
	}{
		{
			name: "Pod not opted in",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    false,
		},
		{
			name: "Pod opted in, gate bypassed (no active PMJ)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    true,
		},
		{
			name: "Pod opted in with pre-existing podsnapshot.gke.io/ps-name (e.g. Kubeflow UI resume), no active PMJ, preserves ps-name",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-workspace-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					Annotations: map[string]string{
						"podsnapshot.gke.io/ps-name": "ws-snapshot-123",
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    false,
		},
		{
			name: "Pod already has gate, bypassed (no active PMJ)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{
						{Name: "gke.io/pod-migration-gate"},
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    true,
		},
		{
			name: "Pod opted in, active PMJ exists, gate injected",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
			},
			initObjs: []runtime.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      util.FormatPMJName("test-pod", "12345678"),
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef: corev1.LocalObjectReference{
							Name: "test-pod",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      true,
			expectBypass:    false,
		},
		{
			name: "Pod opted in, parent workload exists, no active PMJ (scale-up), bypass gate",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod-12345",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "test-rs",
							UID:        "rs-uid",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-rs",
						UID:       "rs-uid",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "test-deployment",
								UID:        "deploy-uid",
								Controller: ptr(true),
							},
						},
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    true,
		},
		{
			name: "Pod opted in, parent workload exists, active PMJ exists, gate injected",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod-12345",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "test-rs",
							UID:        "rs-uid",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-rs",
						UID:       "rs-uid",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "test-deployment",
								UID:        "deploy-uid",
								Controller: ptr(true),
							},
						},
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-test",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name": "test-deployment",
							"pod-migration.gke.io/parent-kind": "Deployment",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      true,
			expectBypass:    false,
		},
		{
			name: "Deployment rolling update: pod for new revision (v2) does not inject gate or attach to v1 PMJ (bypass gate)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-deploy-v2-99999",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "test-rs-v2",
							UID:        "rs-v2-uid",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-rs-v2",
						UID:       "rs-v2-uid",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "test-deployment",
								UID:        "deploy-uid",
								Controller: ptr(true),
							},
						},
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-test-v1",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":       "test-deployment",
							"pod-migration.gke.io/parent-kind":       "Deployment",
							"pod-migration.gke.io/pod-template-hash": "hash-v1",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    true,
		},
		{
			name: "Deployment rolling update: pod for old revision (v1) matches v1 PMJ and injects gate",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-deploy-v1-88888",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "test-rs-v1",
							UID:        "rs-v1-uid",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-rs-v1",
						UID:       "rs-v1-uid",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "test-deployment",
								UID:        "deploy-uid",
								Controller: ptr(true),
							},
						},
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-test-v1",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":       "test-deployment",
							"pod-migration.gke.io/parent-kind":       "Deployment",
							"pod-migration.gke.io/pod-template-hash": "hash-v1",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      true,
			expectBypass:    false,
		},
		{
			name: "Deployment rolling update: pod for new revision (v2) matches v2 PMJ when both v1 and v2 have PMJs",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-deploy-v2-77777",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "test-rs-v2",
							UID:        "rs-v2-uid",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-rs-v2",
						UID:       "rs-v2-uid",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "test-deployment",
								UID:        "deploy-uid",
								Controller: ptr(true),
							},
						},
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-test-v1",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":       "test-deployment",
							"pod-migration.gke.io/parent-kind":       "Deployment",
							"pod-migration.gke.io/pod-template-hash": "hash-v1",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-test-v2",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":       "test-deployment",
							"pod-migration.gke.io/parent-kind":       "Deployment",
							"pod-migration.gke.io/pod-template-hash": "hash-v2",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      true,
			expectBypass:    false,
		},
		{
			name: "Indexed Job pod index 0 matches PMJ for index 0 (gate injected)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-job-0-abc",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":             "true",
						"batch.kubernetes.io/job-completion-index": "0",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "test-indexed-job",
							UID:        "job-uid-1",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-indexed-job",
						UID:       "job-uid-1",
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-job-0",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":         "test-indexed-job",
							"pod-migration.gke.io/parent-kind":         "Job",
							"batch.kubernetes.io/job-completion-index": "0",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      true,
			expectBypass:    false,
		},
		{
			name: "Indexed Job pod index 2 with only index 0 PMJ bypasses gate (scale-up pod)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-job-2-xyz",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":             "true",
						"batch.kubernetes.io/job-completion-index": "2",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "test-indexed-job",
							UID:        "job-uid-1",
							Controller: ptr(true),
						},
					},
				},
			},
			initObjs: []runtime.Object{
				&batchv1.Job{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "test-indexed-job",
						UID:       "job-uid-1",
					},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-job-0",
						Labels: map[string]string{
							"pod-migration.gke.io/parent-name":         "test-indexed-job",
							"pod-migration.gke.io/parent-kind":         "Job",
							"batch.kubernetes.io/job-completion-index": "0",
						},
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
					},
				},
			},
			expectedAllowed: true,
			expectGate:      false,
			expectBypass:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = batchv1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)
			dec := admission.NewDecoder(scheme)

			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
				WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
				WithRuntimeObjects(tt.initObjs...).
				Build()

			emptyReader := fake.NewClientBuilder().WithScheme(scheme).Build()

			handler := &PodGateInjector{
				Client:    cl,
				APIReader: emptyReader,
				decoder:   dec,
			}

			rawPod, err := json.Marshal(tt.pod)
			if err != nil {
				t.Fatalf("Failed to marshal pod: %v", err)
			}

			req := admission.Request{}
			req.Namespace = tt.pod.Namespace
			req.Name = tt.pod.Name
			req.Object = runtime.RawExtension{Raw: rawPod}

			resp := handler.Handle(context.Background(), req)

			if tt.expectedAllowed != resp.Allowed {
				t.Errorf("Expected allowed %t, got %t", tt.expectedAllowed, resp.Allowed)
			}

			gateAdded := false
			bypassAnnotated := false

			for _, patch := range resp.Patches {
				if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
					gateAdded = true
				}
				if patch.Path == "/metadata/annotations" {
					if valMap, ok := patch.Value.(map[string]interface{}); ok {
						if psName, ok := valMap["podsnapshot.gke.io/ps-name"]; ok && psName == "" {
							bypassAnnotated = true
						}
					}
				}
				if patch.Path == "/metadata/annotations/podsnapshot.gke.io~1ps-name" {
					if valStr, ok := patch.Value.(string); ok && valStr == "" {
						bypassAnnotated = true
					}
				}
			}

			if tt.expectGate != gateAdded {
				t.Errorf("Expected gateAdded %t, got %t (patches: %+v)", tt.expectGate, gateAdded, resp.Patches)
			}

			if tt.expectBypass != bypassAnnotated {
				t.Errorf("Expected bypassAnnotated %t, got %t (patches: %+v)", tt.expectBypass, bypassAnnotated, resp.Patches)
			}
		})
	}
}

func TestPodGateInjector_APIReaderFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-pod"

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pmj-test-pod-uid1234",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: "uid1234",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("Failed to marshal pod: %v", err)
	}

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:      podName,
			Namespace: namespace,
			Object: runtime.RawExtension{
				Raw: rawPod,
			},
		},
	}

	t.Run("Cache miss with live hit injects scheduling gate and assigned PMJ", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			Build()
		fakeAPIReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj).Build()

		handler := &PodGateInjector{
			Client:    fakeClient,
			APIReader: fakeAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		resp := handler.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("Expected allowed with patches, got denied: %v", resp.Result)
		}

		gateAdded := false
		pmjAssigned := false
		for _, patch := range resp.Patches {
			if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
				gateAdded = true
			}
			if patch.Path == "/metadata/annotations" {
				if valMap, ok := patch.Value.(map[string]interface{}); ok {
					if assigned, ok := valMap[util.AnnotationAssignedPMJ]; ok && assigned == pmj.Name {
						pmjAssigned = true
					}
				}
			}
			if patch.Path == "/metadata/annotations/pod-migration.gke.io~1assigned-pmj" {
				if valStr, ok := patch.Value.(string); ok && valStr == pmj.Name {
					pmjAssigned = true
				}
			}
		}

		if !gateAdded {
			t.Errorf("Expected scheduling gate to be injected via live APIReader hit, got patches: %+v", resp.Patches)
		}
		if !pmjAssigned {
			t.Errorf("Expected assigned-pmj annotation to be stamped via live APIReader hit, got patches: %+v", resp.Patches)
		}
	})

	t.Run("Cache miss with live miss bypasses gate with cold-start bypass", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			Build()
		fakeAPIReader := fake.NewClientBuilder().WithScheme(scheme).Build()

		handler := &PodGateInjector{
			Client:    fakeClient,
			APIReader: fakeAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		resp := handler.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("Expected allowed with bypass patches, got denied: %v", resp.Result)
		}

		bypassAnnotated := false
		gateAdded := false
		for _, patch := range resp.Patches {
			if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
				gateAdded = true
			}
			if patch.Path == "/metadata/annotations" {
				if valMap, ok := patch.Value.(map[string]interface{}); ok {
					if psName, ok := valMap["podsnapshot.gke.io/ps-name"]; ok && psName == "" {
						bypassAnnotated = true
					}
				}
			}
			if patch.Path == "/metadata/annotations/podsnapshot.gke.io~1ps-name" {
				if valStr, ok := patch.Value.(string); ok && valStr == "" {
					bypassAnnotated = true
				}
			}
		}

		if gateAdded {
			t.Errorf("Expected gate NOT to be added on scale-up pod, got patches: %+v", resp.Patches)
		}
		if !bypassAnnotated {
			t.Errorf("Expected ps-name: \"\" bypass annotation on scale-up pod, got patches: %+v", resp.Patches)
		}
	})

	t.Run("Live read error returns 500 error", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			Build()
		errClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, client client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				return fmt.Errorf("simulated live apiserver list error")
			},
		}).Build()

		handler := &PodGateInjector{
			Client:    fakeClient,
			APIReader: errClient,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		resp := handler.Handle(context.Background(), req)
		if resp.Allowed {
			t.Fatalf("Expected admission error on live read failure, got allowed")
		}
		if resp.Result == nil || resp.Result.Code != http.StatusInternalServerError {
			t.Fatalf("Expected status code 500, got: %+v", resp.Result)
		}
	})

	t.Run("Cached parent missing but live parent resolved via APIReader injects scheduling gate", func(t *testing.T) {
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "test-deploy-rs-v1",
				UID:       "rs-v1-uid",
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "Deployment",
						Name:       "test-deploy",
						UID:        "deploy-uid",
						Controller: ptr(true),
					},
				},
			},
		}

		deployPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "test-deploy-replacement-xyz",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled": "true",
					"pod-template-hash":            "hash-v1",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "test-deploy-rs-v1",
						UID:        "rs-v1-uid",
						Controller: ptr(true),
					},
				},
			},
		}

		deployPMJ := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pmj-test-deploy-orig-1234",
				Labels: map[string]string{
					util.LabelParentName:      "test-deploy",
					util.LabelParentKind:      "Deployment",
					util.LabelParentUID:       "deploy-uid",
					util.LabelPodTemplateHash: "hash-v1",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				TargetPodUID: "orig-uid-1234",
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			},
		}

		// Cache is empty (lagging), live APIReader has the ReplicaSet and PMJ
		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			Build()
		fakeAPIReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, deployPMJ).Build()

		handler := &PodGateInjector{
			Client:    fakeClient,
			APIReader: fakeAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		rawDeployPod, _ := json.Marshal(deployPod)
		deployReq := admission.Request{}
		deployReq.Namespace = deployPod.Namespace
		deployReq.Name = deployPod.Name
		deployReq.Object = runtime.RawExtension{Raw: rawDeployPod}

		resp := handler.Handle(context.Background(), deployReq)
		if !resp.Allowed {
			t.Fatalf("Expected allowed, got denied: %v", resp.Result)
		}

		gateAdded := false
		pmjAssigned := false
		for _, patch := range resp.Patches {
			if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
				gateAdded = true
			}
			if patch.Path == "/metadata/annotations" {
				if valMap, ok := patch.Value.(map[string]interface{}); ok {
					if val, ok := valMap["pod-migration.gke.io/assigned-pmj"]; ok && val == deployPMJ.Name {
						pmjAssigned = true
					}
				}
			}
			if patch.Path == "/metadata/annotations/pod-migration.gke.io~1assigned-pmj" {
				if valStr, ok := patch.Value.(string); ok && valStr == deployPMJ.Name {
					pmjAssigned = true
				}
			}
		}

		if !gateAdded {
			t.Errorf("Expected scheduling gate to be added via live parent resolution, got patches: %+v", resp.Patches)
		}
		if !pmjAssigned {
			t.Errorf("Expected assigned-pmj annotation to be stamped via live parent resolution, got patches: %+v", resp.Patches)
		}
	})

	t.Run("Live parent resolution error returns 500 error", func(t *testing.T) {
		deployPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "test-deploy-replacement-xyz",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled": "true",
					"pod-template-hash":            "hash-v1",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       "test-deploy-rs-v1",
						UID:        "rs-v1-uid",
						Controller: ptr(true),
					},
				},
			},
		}

		fakeClient := fake.NewClientBuilder().
			WithScheme(scheme).
			WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
			Build()
		errAPIReader := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*appsv1.ReplicaSet); ok {
					return fmt.Errorf("simulated live apiserver error resolving ReplicaSet")
				}
				return nil
			},
		}).Build()

		handler := &PodGateInjector{
			Client:    fakeClient,
			APIReader: errAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		rawDeployPod, _ := json.Marshal(deployPod)
		deployReq := admission.Request{}
		deployReq.Namespace = deployPod.Namespace
		deployReq.Name = deployPod.Name
		deployReq.Object = runtime.RawExtension{Raw: rawDeployPod}

		resp := handler.Handle(context.Background(), deployReq)
		if resp.Allowed {
			t.Fatalf("Expected admission error on live parent resolution failure, got allowed")
		}
		if resp.Result == nil || resp.Result.Code != http.StatusInternalServerError {
			t.Fatalf("Expected status code 500, got: %+v", resp.Result)
		}
	})
}

func TestReplacementWebhook_ColdStartBypassPreservesGKEBypassOnRouteChange(t *testing.T) {
	// Verifies that scale-up or unassigned replacement pod cold-start bypass stamps
	// podsnapshot.gke.io/ps-name: "" to prevent stale GKE snapshot restoration even
	// when a route change is in effect.
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "scale-up-pod",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "",
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		Build()

	handler := &PodGateInjector{
		Client: fakeClient,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	rawPod, _ := json.Marshal(pod)
	req := admission.Request{}
	req.Namespace = pod.Namespace
	req.Name = pod.Name
	req.Object = runtime.RawExtension{Raw: rawPod}

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got denied: %+v", resp.Result)
	}

	hasBypass := false
	for _, patch := range resp.Patches {
		if patch.Path == "/metadata/annotations/podsnapshot.gke.io~1ps-name" && patch.Value == "" {
			hasBypass = true
			break
		}
	}
	if !hasBypass {
		t.Errorf("Expected cold start bypass patch for podsnapshot.gke.io/ps-name, got patches: %+v", resp.Patches)
	}
}

func TestReplacementWebhook_RayHead_GatedAndAssigned(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	isController := true

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-head-origin",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: clusterName,
				util.LabelParentKind: clusterKind,
				util.LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				util.AnnotationReplicaIdentityKey: "ray.io/group=headgroup,ray.io/node-type=head",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-head-origin"},
			TargetPodUID: "origin-head-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    namespace,
			GenerateName: "sample-ray-head-",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				"ray.io/group":                 "headgroup",
				"ray.io/node-type":             "head",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
	}

	rayCluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "ray.io/v1",
			"kind":       clusterKind,
			"metadata": map[string]interface{}{
				"name":      clusterName,
				"namespace": namespace,
				"uid":       clusterUID,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(pmj, rayCluster).
		Build()

	handler := &PodGateInjector{
		Client: fakeClient,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	rawPod, _ := json.Marshal(replacementPod)
	req := admission.Request{}
	req.Namespace = namespace
	req.Object = runtime.RawExtension{Raw: rawPod}

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got denied: %+v", resp.Result)
	}

	hasGate := false
	hasAssignedPMJ := false
	for _, patch := range resp.Patches {
		if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
			hasGate = true
		}
		if patch.Path == "/metadata/annotations" {
			if valMap, ok := patch.Value.(map[string]interface{}); ok {
				if val, ok := valMap[util.AnnotationAssignedPMJ]; ok && val == pmj.Name {
					hasAssignedPMJ = true
				}
			}
		}
		if patch.Path == "/metadata/annotations/pod-migration.gke.io~1assigned-pmj" && patch.Value == pmj.Name {
			hasAssignedPMJ = true
		}
	}

	if !hasGate {
		t.Errorf("Expected scheduling gate injected for Ray head replacement pod, got patches: %+v", resp.Patches)
	}
	if !hasAssignedPMJ {
		t.Errorf("Expected assigned-pmj annotation %q on Ray head replacement pod, got patches: %+v", pmj.Name, resp.Patches)
	}
}

func TestReplacementWebhook_RayWorker_AmbiguousSibling_BypassedColdStart(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	clusterName := "sample-ray"
	clusterKind := "RayCluster"
	clusterUID := "ray-uid-111"
	isController := true

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-sample-ray-worker-1",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: clusterName,
				util.LabelParentKind: clusterKind,
				util.LabelParentUID:  clusterUID,
			},
			Annotations: map[string]string{
				util.AnnotationReplicaIdentityKey: "ray.io/group=workergroup,ray.io/node-type=worker",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "sample-ray-worker-1"},
			TargetPodUID: "worker-1-uid",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseRestoring,
		},
	}

	// Sibling worker-2 is running
	siblingWorker2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "sample-ray-worker-2",
			UID:       "worker-2-uid",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				"ray.io/group":                 "workergroup",
				"ray.io/node-type":             "worker",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	replacementWorker := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    namespace,
			GenerateName: "sample-ray-worker-",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				"ray.io/group":                 "workergroup",
				"ray.io/node-type":             "worker",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "ray.io/v1",
					Kind:       clusterKind,
					Name:       clusterName,
					UID:        types.UID(clusterUID),
					Controller: &isController,
				},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(pmj, siblingWorker2).
		Build()

	handler := &PodGateInjector{
		Client: fakeClient,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	rawPod, _ := json.Marshal(replacementWorker)
	req := admission.Request{}
	req.Namespace = namespace
	req.Object = runtime.RawExtension{Raw: rawPod}

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got denied: %+v", resp.Result)
	}

	hasBypass := false
	hasGate := false
	for _, patch := range resp.Patches {
		if patch.Path == "/metadata/annotations" {
			if valMap, ok := patch.Value.(map[string]interface{}); ok {
				if val, ok := valMap["podsnapshot.gke.io/ps-name"]; ok && val == "" {
					hasBypass = true
				}
			}
		}
		if patch.Path == "/metadata/annotations/podsnapshot.gke.io~1ps-name" && patch.Value == "" {
			hasBypass = true
		}
		if patch.Operation == "add" && (patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-") {
			hasGate = true
		}
	}

	if hasGate {
		t.Errorf("Expected ambiguous worker replacement NOT to be gated, but gate was injected")
	}
	if !hasBypass {
		t.Errorf("Expected cold-start bypass (ps-name='') for ambiguous worker replacement, got patches: %+v", resp.Patches)
	}
}

func TestPodGateInjector_AlreadyGatedReinvocationScrubbing(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "replacement-pod"
	pmjName := util.FormatPMJName(podName, "12345678")

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
			UID:       types.UID("pmj-uid-1234"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseSnapshotting,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	handler := &PodGateInjector{
		Client: fakeClient,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	// Case 1: An already-gated pod with matching assigned-pmj, carrying injected CRIU marker
	// AND spoofed restore annotations (GKE ps-name, CRIU restore outcome/reason).
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ:         pmjName,
				engine.CRIUInjectedAnnotation:      "true",
				engine.CRIURestoreStatusAnnotation: "restored",
				engine.CRIURestoreReasonAnnotation: "pre-supplied",
				"podsnapshot.gke.io/ps-name":       "spoofed-snap",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "pause",
				},
			},
		},
	}

	rawPod, _ := json.Marshal(pod)
	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: namespace,
			Object:    runtime.RawExtension{Raw: rawPod},
		},
	}

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got denied: %+v", resp.Result)
	}

	// Because spoofed annotations were scrubbed, response must contain patches rather than unmutated Allowed.
	if len(resp.Patches) == 0 {
		t.Fatalf("Expected patches removing spoofed annotations, got empty patches")
	}

	patchedPod := applyReadinessAdmissionPatches(t, rawPod, resp)
	if _, ok := patchedPod.Annotations["podsnapshot.gke.io/ps-name"]; ok {
		t.Errorf("podsnapshot.gke.io/ps-name survived scrubbing on reinvocation")
	}
	if _, ok := patchedPod.Annotations[engine.CRIURestoreStatusAnnotation]; ok {
		t.Errorf("%s survived scrubbing on reinvocation", engine.CRIURestoreStatusAnnotation)
	}
	if _, ok := patchedPod.Annotations[engine.CRIURestoreReasonAnnotation]; ok {
		t.Errorf("%s survived scrubbing on reinvocation", engine.CRIURestoreReasonAnnotation)
	}
	if val, ok := patchedPod.Annotations[engine.CRIUInjectedAnnotation]; !ok || val != "true" {
		t.Errorf("%s was scrubbed: got %v, want 'true'", engine.CRIUInjectedAnnotation, val)
	}

	// Case 2: Clean already-gated pod with matching assigned-pmj and CRIU injected marker,
	// with NO spoofed annotations. Must return pure Allowed (0 patches, no-op).
	cleanPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ:    pmjName,
				engine.CRIUInjectedAnnotation: "true",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: "gke.io/pod-migration-gate"},
			},
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "pause",
				},
			},
		},
	}

	rawCleanPod, _ := json.Marshal(cleanPod)
	cleanReq := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Namespace: namespace,
			Object:    runtime.RawExtension{Raw: rawCleanPod},
		},
	}

	cleanResp := handler.Handle(context.Background(), cleanReq)
	if !cleanResp.Allowed {
		t.Fatalf("Expected clean pod allowed, got: %+v", cleanResp.Result)
	}
	if len(cleanResp.Patches) != 0 {
		t.Errorf("Expected 0 patches on clean already-gated reinvocation, got %d patches: %+v", len(cleanResp.Patches), cleanResp.Patches)
	}
}

func TestPodGateInjector_OperatorOwned_OwnerForbiddenOrNoMatch_ScrubsAndBypassesColdStart(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "custom-op-pod"
	gvisorClass := "gvisor"
	isController := true

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ:    "stale-pmj",
				engine.CRIUInjectedAnnotation: "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "custom.io/v1",
					Kind:       "CustomCR",
					Name:       "sample-custom",
					UID:        "custom-uid-123",
					Controller: &isController,
				},
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorClass,
		},
	}

	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("Failed to marshal pod: %v", err)
	}

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:      podName,
			Namespace: namespace,
			Object: runtime.RawExtension{
				Raw: rawPod,
			},
		},
	}

	baseClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod).Build()
	cl := interceptor.NewClient(baseClient, interceptor.Funcs{
		Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "sample-custom" {
				return apierrors.NewForbidden(schema.GroupResource{Group: "custom.io", Resource: "customcrs"}, key.Name, nil)
			}
			return client.Get(ctx, key, obj, opts...)
		},
	})

	handler := &PodGateInjector{
		Client:    cl,
		APIReader: cl,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got denied: %v", resp.Result)
	}

	// Gate must NOT be added
	for _, patch := range resp.Patches {
		if patch.Path == "/spec/schedulingGates" || patch.Path == "/spec/schedulingGates/-" {
			t.Fatalf("Expected NO scheduling gate injected for unreadable owner, but got patch: %+v", patch)
		}
	}

	// Must have patches scrubbing stale annotations and stamping ps-name: ""
	if len(resp.Patches) == 0 {
		t.Fatalf("Expected patches scrubbing stale annotations and stamping cold start bypass, got 0 patches")
	}

	hasColdStartBypass := false
	scrubbedAssignedPMJ := false
	for _, patch := range resp.Patches {
		if patch.Path == "/metadata/annotations" {
			if valMap, ok := patch.Value.(map[string]interface{}); ok {
				if val, exists := valMap["podsnapshot.gke.io/ps-name"]; exists && val == "" {
					hasColdStartBypass = true
				}
				if _, exists := valMap[util.AnnotationAssignedPMJ]; !exists {
					scrubbedAssignedPMJ = true
				}
			}
		} else if patch.Path == "/metadata/annotations/podsnapshot.gke.io~1ps-name" && patch.Value == "" {
			hasColdStartBypass = true
		} else if patch.Operation == "remove" && patch.Path == "/metadata/annotations/pod-migration.gke.io~1assigned-pmj" {
			scrubbedAssignedPMJ = true
		}
	}

	if !hasColdStartBypass {
		t.Errorf("Expected cold start bypass annotation podsnapshot.gke.io/ps-name: \"\" in patches, got %+v", resp.Patches)
	}
	if !scrubbedAssignedPMJ {
		t.Errorf("Expected assigned-pmj annotation to be scrubbed, got patches: %+v", resp.Patches)
	}
}

func TestPodGateInjector_APIReaderFallback_ReusesClientParentResolution(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-rs-pod"
	isController := true

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "web-rs",
			UID:       "rs-uid-123",
			Labels: map[string]string{
				"app": "web",
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
			},
		},
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      podName,
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
				"app":                          "web",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "ReplicaSet",
					Name:       "web-rs",
					UID:        "rs-uid-123",
					Controller: &isController,
				},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "pmj-web-rs",
			Labels: map[string]string{
				util.LabelParentName: "web-rs",
				util.LabelParentKind: "ReplicaSet",
				util.LabelParentUID:  "rs-uid-123",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "old-pod"},
			TargetPodUID: "uid-old",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("Failed to marshal pod: %v", err)
	}

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:      podName,
			Namespace: namespace,
			Object: runtime.RawExtension{
				Raw: rawPod,
			},
		},
	}

	// Client has the ReplicaSet, but NO PMJ (simulating informer cache lag for the PMJ)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(rs).
		Build()

	// APIReader has both the ReplicaSet and the PMJ
	baseAPIReader := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithObjects(rs, pmj).
		Build()

	var apiReaderRSGetCount int
	trackedAPIReader := interceptor.NewClient(baseAPIReader, interceptor.Funcs{
		Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "web-rs" {
				apiReaderRSGetCount++
			}
			return client.Get(ctx, key, obj, opts...)
		},
	})

	handler := &PodGateInjector{
		Client:    fakeClient,
		APIReader: trackedAPIReader,
	}
	_ = handler.InjectDecoder(admission.NewDecoder(scheme))

	resp := handler.Handle(context.Background(), req)
	if !resp.Allowed {
		t.Fatalf("Expected allowed, got %v", resp.Result)
	}

	// Because Client already resolved the parent ReplicaSet, APIReader fallback MUST NOT re-fetch web-rs!
	if apiReaderRSGetCount != 0 {
		t.Fatalf("Expected 0 ReplicaSet GET calls via APIReader (parent workload must be reused from Client resolution), got %d", apiReaderRSGetCount)
	}
}
