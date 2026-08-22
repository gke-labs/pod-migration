package webhook

import (
	"context"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestEvictionGate(t *testing.T) {
	// Helper to create a fake PodSnapshotPolicy
	createPSP := func(name, triggerType, postCheckpoint string) *unstructured.Unstructured {
		psp := &unstructured.Unstructured{}
		psp.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotPolicy",
		})
		psp.SetName(name)
		psp.SetNamespace("default")
		psp.Object["spec"] = map[string]interface{}{
			"selector": map[string]interface{}{
				"matchExpressions": []interface{}{
					map[string]interface{}{
						"key":      "pod-migration.gke.io/enabled",
						"operator": "In",
						"values":   []interface{}{"true"},
					},
				},
			},
			"triggerConfig": map[string]interface{}{
				"type":           triggerType,
				"postCheckpoint": postCheckpoint,
			},
		}
		// Inject Ready=True condition in status
		psp.Object["status"] = map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":   "Ready",
					"status": "True",
				},
			},
		}
		return psp
	}

	gvisorRuntime := "gvisor"
	otherRuntime := "other"

	tests := []struct {
		name               string
		pod                *corev1.Pod
		initObjects        []client.Object
		subResource        string
		expectedAllowed    bool
		expectedStatusCode int32
		expectedMessage    string
		verifyPMJCreated   bool
		expectedLabels     map[string]string
	}{
		{
			name: "Not an eviction request",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			subResource:     "status",
			expectedAllowed: true,
			expectedMessage: "not an eviction request",
		},
		{
			name: "Feature not enabled",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "feature not enabled",
		},
		{
			name: "Trigger migration successful (Manual)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					UID: "test-uid-12345",
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &gvisorRuntime,
				},
			},
			initObjects: []client.Object{
				createPSP("psp-test-manual", "manual", "stop"),
			},
			subResource:        "eviction",
			expectedAllowed:    false,
			expectedStatusCode: 429,
			expectedMessage:    "migration job spawned",
			verifyPMJCreated:   true,
		},
		{
			name: "Trigger migration on Deployment pod sets parent labels and pod-template-hash",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "deploy-pod-xyz",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-deploy-v1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       "deploy-rs-v1",
							UID:        "rs-uid-111",
						},
					},
					UID: "deploy-pod-uid-999",
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &gvisorRuntime,
				},
			},
			initObjects: []client.Object{
				createPSP("psp-test-manual", "manual", "stop"),
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "deploy-rs-v1",
						UID:       "rs-uid-111",
						OwnerReferences: []metav1.OwnerReference{
							{
								APIVersion: "apps/v1",
								Kind:       "Deployment",
								Name:       "my-deployment",
								UID:        "deploy-uid-111",
							},
						},
					},
				},
			},
			subResource:        "eviction",
			expectedAllowed:    false,
			expectedStatusCode: 429,
			expectedMessage:    "migration job spawned",
			verifyPMJCreated:   true,
			expectedLabels: map[string]string{
				util.LabelParentName:      "my-deployment",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: "hash-deploy-v1",
			},
		},
		{
			name: "Trigger migration on Indexed Job pod sets parent labels and job completion index",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "indexed-job-pod-0",
					UID:       "pod-uid-job-0",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":           "true",
						"batch.kubernetes.io/job-completion-index": "0",
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "batch/v1",
							Kind:       "Job",
							Name:       "my-indexed-job",
							UID:        "job-uid-999",
						},
					},
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &gvisorRuntime,
				},
			},
			initObjects: []client.Object{
				createPSP("psp-test-manual", "manual", "stop"),
			},
			subResource:        "eviction",
			expectedAllowed:    false,
			expectedStatusCode: 429,
			expectedMessage:    "migration job spawned",
			verifyPMJCreated:   true,
			expectedLabels: map[string]string{
				util.LabelParentName:         "my-indexed-job",
				util.LabelParentKind:         "Job",
				util.LabelJobCompletionIndex: "0",
			},
		},
		{
			name: "Bypass migration when no policy found (No Policy)",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					UID: "test-uid-12345",
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &gvisorRuntime,
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "skipping migration: no valid manual+stop policy found",
		},
		{
			name: "Bypass migration when policy has resume instead of stop",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					UID: "test-uid-12345",
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &gvisorRuntime,
				},
			},
			initObjects: []client.Object{
				createPSP("psp-test-manual-resume", "manual", "resume"),
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "skipping migration: no valid manual+stop policy found",
		},
		{
			name: "Pod lacks runtimeClassName",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					UID: "test-uid-12345",
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "Pod does not use gvisor runtime, skipping migration",
		},
		{
			name: "Pod has non-gvisor runtimeClassName",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "test-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled": "true",
					},
					UID: "test-uid-12345",
				},
				Spec: corev1.PodSpec{
					RuntimeClassName: &otherRuntime,
				},
			},
			subResource:     "eviction",
			expectedAllowed: true,
			expectedMessage: "Pod does not use gvisor runtime, skipping migration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = appsv1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			initObjs := append(tt.initObjects, tt.pod)
			fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(initObjs...).Build()

			handler := &EvictionGate{Client: fakeClient}

			req := admission.Request{}
			req.Namespace = tt.pod.Namespace
			req.Name = tt.pod.Name
			req.SubResource = tt.subResource

			resp := handler.Handle(context.Background(), req)

			if tt.expectedAllowed && !resp.Allowed {
				t.Errorf("Expected allowed, got denied: %s", resp.Result.Message)
			}

			if !tt.expectedAllowed && resp.Allowed {
				t.Errorf("Expected denied, got allowed")
			}

			if tt.expectedStatusCode != 0 {
				if resp.Result == nil {
					t.Errorf("Expected status code %d, but resp.Result is nil", tt.expectedStatusCode)
				} else if resp.Result.Code != tt.expectedStatusCode {
					t.Errorf("Expected status code %d, got %d", tt.expectedStatusCode, resp.Result.Code)
				}
			}

			if tt.expectedMessage != "" {
				gotMsg := ""
				if resp.Result != nil {
					gotMsg = resp.Result.Message
				}
				if gotMsg != tt.expectedMessage {
					t.Errorf("Expected message %q, got %q", tt.expectedMessage, gotMsg)
				}
			}

			if tt.verifyPMJCreated {
				pmj := &pmv1alpha1.PodMigrationJob{}
				err := fakeClient.Get(context.Background(), client.ObjectKey{Namespace: tt.pod.Namespace, Name: util.FormatPMJName(tt.pod.Name, string(tt.pod.UID))}, pmj)
				if err != nil {
					t.Errorf("Failed to find expected PodMigrationJob: %v", err)
				}
				for k, v := range tt.expectedLabels {
					if pmj.Labels[k] != v {
						t.Errorf("Expected label %s=%s, got %s", k, v, pmj.Labels[k])
					}
				}
				// Verify that PodSnapshot was NOT created in the webhook
				psList := &unstructured.UnstructuredList{}
				psList.SetGroupVersionKind(schema.GroupVersionKind{
					Group:   "podsnapshot.gke.io",
					Version: "v1",
					Kind:    "PodSnapshotList",
				})
				err = fakeClient.List(context.Background(), psList, client.InNamespace(tt.pod.Namespace))
				if err == nil && len(psList.Items) > 0 {
					t.Errorf("Expected no PodSnapshot to be created by webhook, but found %d", len(psList.Items))
				}
			}
		})
	}
}
