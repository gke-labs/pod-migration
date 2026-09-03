package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestPodStatusMutator(t *testing.T) {
	now := metav1.Now()
	tests := []struct {
		name               string
		pod                *corev1.Pod
		initObjects        []client.Object
		subResource        string
		expectedAllowed    bool
		expectedStatusCode int32
		verifyMutation     func(t *testing.T, resp admission.Response)
	}{
		{
			name: "Not a status update",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			subResource:     "",
			expectedAllowed: true,
		},
		{
			name: "Pod not opted in",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			subResource:     "status",
			expectedAllowed: true,
		},
		{
			name: "Pod phase is not Succeeded",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodRunning,
				},
			},
			subResource:     "status",
			expectedAllowed: true,
		},
		{
			name: "Pod Succeeded but no active PMJ",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			subResource:     "status",
			expectedAllowed: true,
		},
		{
			name: "Pod Succeeded with active PMJ (Mutates status)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "default",
					Name:              "test-pod",
					UID:               "test-pod-uid",
					DeletionTimestamp: &now,
					Finalizers:        []string{"pod-migration.gke.io/test-finalizer"},
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "test-job",
							UID:        "job-uid",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
					ContainerStatuses: []corev1.ContainerStatus{
						{
							Name: "main",
							State: corev1.ContainerState{
								Terminated: &corev1.ContainerStateTerminated{
									ExitCode: 0,
								},
							},
						},
					},
				},
			},
			initObjects: []client.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      util.FormatPMJName("test-pod", "test-pod-uid"),
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						TargetPodUID: "test-pod-uid",
					},
				},
			},
			subResource:     "status",
			expectedAllowed: true, // Mutating webhooks return Allowed with patches
			verifyMutation: func(t *testing.T, resp admission.Response) {
				if len(resp.Patches) == 0 {
					t.Fatalf("Expected patches, got none")
				}
				foundPhasePatch := false
				foundExitCodePatch := false
				for _, patch := range resp.Patches {
					if patch.Operation == "replace" {
						if patch.Path == "/status/phase" && patch.Value == "Failed" {
							foundPhasePatch = true
						}
						if patch.Path == "/status/containerStatuses/0/state/terminated/exitCode" && patch.Value == float64(137) {
							foundExitCodePatch = true
						}
					}
				}
				if !foundPhasePatch {
					t.Errorf("Expected patch to change phase to Failed, but not found. Patches: %v", resp.Patches)
				}
				if !foundExitCodePatch {
					t.Errorf("Expected patch to change exitCode to 137, but not found. Patches: %v", resp.Patches)
				}
			},
		},
		{
			name: "Pod Succeeded with active PMJ but not owned by a Job (Bypasses mutation)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					UID:       "test-pod-uid",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "StatefulSet",
							Name:       "test-ss",
							UID:        "ss-uid",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			initObjects: []client.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      util.FormatPMJName("test-pod", "test-pod-uid"),
					},
				},
			},
			subResource:     "status",
			expectedAllowed: true,
			verifyMutation: func(t *testing.T, resp admission.Response) {
				if len(resp.Patches) > 0 {
					t.Fatalf("Expected no patches, got %d patches", len(resp.Patches))
				}
			},
		},
		{
			name: "Pod Succeeded with active PMJ but UID mismatch (Bypasses mutation)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "default",
					Name:              "test-pod",
					UID:               "test-pod-uid",
					DeletionTimestamp: &now,
					Finalizers:        []string{"pod-migration.gke.io/test-finalizer"},
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "test-job",
							UID:        "job-uid",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			initObjects: []client.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      util.FormatPMJName("test-pod", "test-pod-uid"),
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						TargetPodUID: "mismatched-uid",
					},
				},
			},
			subResource:     "status",
			expectedAllowed: true,
			verifyMutation: func(t *testing.T, resp admission.Response) {
				if len(resp.Patches) > 0 {
					t.Fatalf("Expected no patches, got %d patches", len(resp.Patches))
				}
			},
		},
		{
			name: "Pod Succeeded with active PMJ but DeletionTimestamp is nil (Bypasses mutation)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         "default",
					Name:              "test-pod",
					UID:               "test-pod-uid",
					DeletionTimestamp: nil,
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "test-job",
							UID:        "job-uid",
						},
					},
				},
				Status: corev1.PodStatus{
					Phase: corev1.PodSucceeded,
				},
			},
			initObjects: []client.Object{
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      util.FormatPMJName("test-pod", "test-pod-uid"),
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						TargetPodUID: "test-pod-uid",
					},
				},
			},
			subResource:     "status",
			expectedAllowed: true,
			verifyMutation: func(t *testing.T, resp admission.Response) {
				if len(resp.Patches) > 0 {
					t.Fatalf("Expected no patches, got %d patches", len(resp.Patches))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			initObjs := append(tt.initObjects, tt.pod)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initObjs...).Build()

			handler := &PodStatusMutator{Client: fakeClient}
			dec := admission.NewDecoder(scheme)
			_ = handler.InjectDecoder(dec)

			podRaw, err := json.Marshal(tt.pod)
			if err != nil {
				t.Fatalf("Failed to marshal pod: %v", err)
			}

			req := admission.Request{
				AdmissionRequest: admissionv1.AdmissionRequest{
					Name:        tt.pod.Name,
					Namespace:   tt.pod.Namespace,
					SubResource: tt.subResource,
					Object: runtime.RawExtension{
						Raw: podRaw,
					},
				},
			}

			resp := handler.Handle(context.Background(), req)

			if tt.expectedAllowed && !resp.Allowed {
				t.Errorf("Expected allowed, got denied: %s", resp.Result.Message)
			}
			if !tt.expectedAllowed && resp.Allowed {
				t.Errorf("Expected denied, got allowed")
			}

			if tt.expectedStatusCode != 0 && (resp.Result == nil || resp.Result.Code != tt.expectedStatusCode) {
				t.Errorf("Expected status code %d, got %v", tt.expectedStatusCode, resp.Result)
			}

			if tt.verifyMutation != nil {
				tt.verifyMutation(t, resp)
			}
		})
	}
}

func TestPodStatusMutator_APIReaderFallback(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	podName := "test-job-pod"
	podUID := "pod-uid-12345"
	pmjName := util.FormatPMJName(podName, podUID)

	now := metav1.Now()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         namespace,
			Name:              podName,
			UID:               types.UID(podUID),
			DeletionTimestamp: &now,
			Finalizers:        []string{"pod-migration.gke.io/test-finalizer"},
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       "test-job",
					UID:        "job-uid",
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "main",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
						},
					},
				},
			},
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      pmjName,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: podName},
			TargetPodUID: podUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
		},
	}

	podRaw, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("Failed to marshal pod: %v", err)
	}

	req := admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Name:        podName,
			Namespace:   namespace,
			SubResource: "status",
			Object: runtime.RawExtension{
				Raw: podRaw,
			},
		},
	}

	t.Run("Cache miss with live hit mutates status to Failed", func(t *testing.T) {
		// Empty local cache
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		// Live API reader contains the PMJ
		fakeAPIReader := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj).Build()

		handler := &PodStatusMutator{
			Client:    fakeClient,
			APIReader: fakeAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		resp := handler.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("Expected allowed (mutated), got denied: %v", resp.Result)
		}
		if len(resp.Patches) == 0 {
			t.Fatalf("Expected patches mutating status to Failed, got 0 patches")
		}
	})

	t.Run("Cache miss with live miss allows normal completion", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		fakeAPIReader := fake.NewClientBuilder().WithScheme(scheme).Build()

		handler := &PodStatusMutator{
			Client:    fakeClient,
			APIReader: fakeAPIReader,
		}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		resp := handler.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("Expected allowed, got denied: %v", resp.Result)
		}
		if len(resp.Patches) > 0 {
			t.Fatalf("Expected 0 patches, got %d patches", len(resp.Patches))
		}
	})

	t.Run("Live read error returns 500 error", func(t *testing.T) {
		fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
		errClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, client client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				return fmt.Errorf("simulated apiserver connection error")
			},
		}).Build()

		handler := &PodStatusMutator{
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
