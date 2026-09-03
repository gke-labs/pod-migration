package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						},
					},
				},
			},
			initObjs: []runtime.Object{
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
						},
					},
				},
			},
			initObjs: []runtime.Object{
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
						Phase: pmv1alpha1.PodMigrationJobPhasePending,
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
			_ = pmv1alpha1.AddToScheme(scheme)
			dec := admission.NewDecoder(scheme)

			cl := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tt.initObjs...).Build()

			handler := &PodGateInjector{
				Client:  cl,
				decoder: dec,
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
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
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
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
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
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
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
}
