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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestWebhook_CreationReinvocation_Envtest tests that when an opt-in label
// (pod-migration.gke.io/enabled: "true") is added by another mutating admission webhook
// (e.g. workload-class) that runs AFTER our webhook in lexicographical order,
// reinvocationPolicy: IfNeeded with namespaceSelector correctly reinvokes mpodgate,
// injecting the scheduling gate and assigning the active PMJ (#303).
func TestWebhook_CreationReinvocation_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping creation reinvocation webhook envtest")
	}

	failPolicy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	reinvokeIfNeeded := admissionregistrationv1.IfNeededReinvocationPolicy
	reinvokeNever := admissionregistrationv1.NeverReinvocationPolicy

	mpodgatePath := "/mutate-v1-pod-scheduling-gate"
	labelStamperPath := "/mutate-workload-class"

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MutatingWebhooks: []*admissionregistrationv1.MutatingWebhookConfiguration{
				{
					// Named "a-mpodgate" so it is called BEFORE "b-workload-class"
					ObjectMeta: metav1.ObjectMeta{Name: "a-mpodgate"},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "mpodgate.podmigration.gke.io",
							AdmissionReviewVersions: []string{"v1"},
							FailurePolicy:           &failPolicy,
							SideEffects:             &sideEffects,
							ReinvocationPolicy:      &reinvokeIfNeeded,
							ClientConfig: admissionregistrationv1.WebhookClientConfig{
								Service: &admissionregistrationv1.ServiceReference{
									Path: &mpodgatePath,
								},
							},
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									util.LabelNamespaceManaged: "true",
								},
							},
							Rules: []admissionregistrationv1.RuleWithOperations{
								{
									Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{""},
										APIVersions: []string{"v1"},
										Resources:   []string{"pods"},
									},
								},
							},
						},
					},
				},
				{
					// Named "b-workload-class" so it is called AFTER "a-mpodgate"
					ObjectMeta: metav1.ObjectMeta{Name: "b-workload-class"},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "label-stamper.workload-class.gke.io",
							AdmissionReviewVersions: []string{"v1"},
							FailurePolicy:           &failPolicy,
							SideEffects:             &sideEffects,
							ReinvocationPolicy:      &reinvokeNever,
							ClientConfig: admissionregistrationv1.WebhookClientConfig{
								Service: &admissionregistrationv1.ServiceReference{
									Path: &labelStamperPath,
								},
							},
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									util.LabelNamespaceManaged: "true",
								},
							},
							Rules: []admissionregistrationv1.RuleWithOperations{
								{
									Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{""},
										APIVersions: []string{"v1"},
										Resources:   []string{"pods"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	whOpts := &testEnv.WebhookInstallOptions
	webhookServer := ctrlwebhook.NewServer(ctrlwebhook.Options{
		Host:    whOpts.LocalServingHost,
		Port:    whOpts.LocalServingPort,
		CertDir: whOpts.LocalServingCertDir,
	})

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:        scheme,
		Metrics:       metricsserver.Options{BindAddress: "0"},
		WebhookServer: webhookServer,
	})
	if err != nil {
		t.Fatalf("creating manager: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	if err := mgr.GetFieldIndexer().IndexField(ctx, &pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue); err != nil {
		cancel()
		t.Fatalf("indexing PMJ parent key: %v", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue); err != nil {
		cancel()
		t.Fatalf("indexing Pod assigned PMJ: %v", err)
	}

	if err := SetupReplacementWebhookWithManager(mgr, mgr.GetAPIReader(), true); err != nil {
		cancel()
		t.Fatalf("setting up replacement webhook: %v", err)
	}

	// Register the simulated workload-class webhook handler that stamps
	// pod-migration.gke.io/enabled: "true" when pod has label "workload: test"
	mgr.GetWebhookServer().Register(labelStamperPath, &ctrlwebhook.Admission{
		Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			pod := &corev1.Pod{}
			if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
				return admission.Errored(http.StatusBadRequest, err)
			}
			if pod.Labels != nil && pod.Labels["workload"] == "test" {
				pod.Labels["pod-migration.gke.io/enabled"] = "true"
			}
			marshaled, err := json.Marshal(pod)
			if err != nil {
				return admission.Errored(http.StatusInternalServerError, err)
			}
			return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
		}),
	})

	mgrDone := make(chan error, 1)
	go func() {
		mgrDone <- mgr.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		if err := <-mgrDone; err != nil {
			t.Errorf("manager: %v", err)
		}
	})

	addr := fmt.Sprintf("%s:%d", whOpts.LocalServingHost, whOpts.LocalServingPort)
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, dialErr := tls.DialWithDialer(&net.Dialer{Timeout: 200 * time.Millisecond}, "tcp", addr, &tls.Config{InsecureSkipVerify: true})
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("webhook server at %s did not become ready: %v", addr, dialErr)
		}
		time.Sleep(50 * time.Millisecond)
	}

	syncCtx, syncCancel := context.WithTimeout(ctx, 10*time.Second)
	defer syncCancel()
	if !mgr.GetCache().WaitForCacheSync(syncCtx) {
		t.Fatal("manager informer caches did not sync in time")
	}

	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}

	// 1. Managed namespace: has pod-migration.gke.io/managed: "true"
	managedNS := "managed-ns"
	if err := admin.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: managedNS,
			Labels: map[string]string{
				util.LabelNamespaceManaged: "true",
			},
		},
	}); err != nil {
		t.Fatalf("creating managed namespace: %v", err)
	}

	// 2. Unmanaged namespace: lacks pod-migration.gke.io/managed: "true"
	unmanagedNS := "unmanaged-ns"
	if err := admin.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: unmanagedNS,
		},
	}); err != nil {
		t.Fatalf("creating unmanaged namespace: %v", err)
	}

	createPMJ := func(namespace, podName string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      util.FormatPMJName(podName, "11223344"),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: podName},
			},
		}
		if err := admin.Create(ctx, pmj); err != nil {
			t.Fatalf("creating PMJ %s/%s: %v", namespace, pmj.Name, err)
		}
		pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmj.Status.SnapshotRef = "snap-test"
		if err := admin.Status().Update(ctx, pmj); err != nil {
			t.Fatalf("updating PMJ status: %v", err)
		}
		return pmj
	}

	t.Run("replacement pod opted in by second webhook is gated via reinvocation", func(t *testing.T) {
		pmj := createPMJ(managedNS, "work-pod-reinvoke")

		// Pod is created WITHOUT pod-migration.gke.io/enabled: "true", but WITH workload: test.
		// a-mpodgate runs first -> skips gate.
		// b-workload-class runs second -> adds pod-migration.gke.io/enabled: "true".
		// Reinvocation triggers a-mpodgate again -> injects gate and assigns PMJ!
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "work-pod-reinvoke",
				Labels: map[string]string{
					"workload": "test",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10"},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}

		// Verify label was stamped
		if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
			t.Errorf("pod-migration.gke.io/enabled = %q, want 'true'", pod.Labels["pod-migration.gke.io/enabled"])
		}

		// Verify scheduling gate was injected on reinvocation
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != "gke.io/pod-migration-gate" {
			t.Errorf("schedulingGates = %+v, want [gke.io/pod-migration-gate]", pod.Spec.SchedulingGates)
		}

		// Verify assigned PMJ was stamped
		if pod.Annotations[util.AnnotationAssignedPMJ] != pmj.Name {
			t.Errorf("assigned-pmj = %q, want %q", pod.Annotations[util.AnnotationAssignedPMJ], pmj.Name)
		}
	})

	t.Run("reinvocation on already-gated pod is a no-op", func(t *testing.T) {
		pmj := createPMJ(managedNS, "already-gated")

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "already-gated",
				Labels: map[string]string{
					"workload":                     "test",
					"pod-migration.gke.io/enabled": "true",
				},
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ: pmj.Name,
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{
					{Name: "gke.io/pod-migration-gate"},
				},
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10"},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating already-gated pod: %v", err)
		}

		if len(pod.Spec.SchedulingGates) != 1 {
			t.Errorf("schedulingGates count = %d, want 1", len(pod.Spec.SchedulingGates))
		}
	})

	t.Run("unlabeled pod in managed namespace is untouched", func(t *testing.T) {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "unlabeled-pod",
				Labels: map[string]string{
					"other": "label",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10"},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating unlabeled pod: %v", err)
		}

		if len(pod.Spec.SchedulingGates) != 0 {
			t.Errorf("unlabeled pod got schedulingGates: %+v, want none", pod.Spec.SchedulingGates)
		}
		if _, ok := pod.Annotations[util.AnnotationAssignedPMJ]; ok {
			t.Errorf("unlabeled pod got assigned-pmj: %s, want none", pod.Annotations[util.AnnotationAssignedPMJ])
		}
	})

	t.Run("pod in unmanaged namespace is untouched even with workload label", func(t *testing.T) {
		_ = createPMJ(unmanagedNS, "unmanaged-work-pod")

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: unmanagedNS,
				Name:      "unmanaged-work-pod",
				Labels: map[string]string{
					"workload": "test",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10"},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod in unmanaged namespace: %v", err)
		}

		// Webhook was skipped because namespaceSelector did not match
		if len(pod.Spec.SchedulingGates) != 0 {
			t.Errorf("unmanaged pod got schedulingGates: %+v, want none", pod.Spec.SchedulingGates)
		}
		if _, ok := pod.Annotations[util.AnnotationAssignedPMJ]; ok {
			t.Errorf("unmanaged pod got assigned-pmj: %s, want none", pod.Annotations[util.AnnotationAssignedPMJ])
		}
	})
}

// TestWebhook_OutageFailOpenFailClosed_Envtest verifies that when the webhook endpoint
// is unreachable (controller outage), creating a runc pod succeeds (fails open with Ignore)
// while creating a gVisor pod fails closed (FailurePolicy: Fail) (#303).
func TestWebhook_OutageFailOpenFailClosed_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping outage fail-open/fail-closed envtest")
	}

	failPolicy := admissionregistrationv1.Fail
	ignorePolicy := admissionregistrationv1.Ignore
	sideEffects := admissionregistrationv1.SideEffectClassNone
	unreachablePath := "/mutate-v1-pod-scheduling-gate"

	// Dummy port where nothing is listening to simulate unreachable webhook endpoint.
	unreachablePort := int32(59999)

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MutatingWebhooks: []*admissionregistrationv1.MutatingWebhookConfiguration{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "pod-migration-mutating-webhook-configuration"},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "mpodgate.podmigration.gke.io",
							AdmissionReviewVersions: []string{"v1"},
							FailurePolicy:           &failPolicy,
							SideEffects:             &sideEffects,
							TimeoutSeconds:          ptr(int32(1)),
							ClientConfig: admissionregistrationv1.WebhookClientConfig{
								Service: &admissionregistrationv1.ServiceReference{
									Namespace: "pod-migration-system",
									Name:      "webhook-service",
									Path:      &unreachablePath,
									Port:      &unreachablePort,
								},
							},
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									util.LabelNamespaceManaged: "true",
								},
							},
							MatchConditions: []admissionregistrationv1.MatchCondition{
								{
									Name:       "gvisor-runtime-class",
									Expression: "has(object.spec.runtimeClassName) && object.spec.runtimeClassName == 'gvisor'",
								},
							},
							Rules: []admissionregistrationv1.RuleWithOperations{
								{
									Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{""},
										APIVersions: []string{"v1"},
										Resources:   []string{"pods"},
									},
								},
							},
						},
						{
							Name:                    "mpodgate-runc.podmigration.gke.io",
							AdmissionReviewVersions: []string{"v1"},
							FailurePolicy:           &ignorePolicy,
							SideEffects:             &sideEffects,
							TimeoutSeconds:          ptr(int32(1)),
							ClientConfig: admissionregistrationv1.WebhookClientConfig{
								Service: &admissionregistrationv1.ServiceReference{
									Namespace: "pod-migration-system",
									Name:      "webhook-service",
									Path:      &unreachablePath,
									Port:      &unreachablePort,
								},
							},
							NamespaceSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{
									util.LabelNamespaceManaged: "true",
								},
							},
							MatchConditions: []admissionregistrationv1.MatchCondition{
								{
									Name:       "non-gvisor-runtime-class",
									Expression: "!has(object.spec.runtimeClassName) || object.spec.runtimeClassName != 'gvisor'",
								},
							},
							Rules: []admissionregistrationv1.RuleWithOperations{
								{
									Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
									Rule: admissionregistrationv1.Rule{
										APIGroups:   []string{""},
										APIVersions: []string{"v1"},
										Resources:   []string{"pods"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})

	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = nodev1.AddToScheme(scheme)

	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}

	// Create RuntimeClass objects so the API server does not reject pods with "RuntimeClass not found"
	for _, rc := range []string{"runc", "gvisor"} {
		if err := admin.Create(context.Background(), &nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: rc},
			Handler:    rc,
		}); err != nil {
			t.Fatalf("creating runtimeClass %s: %v", rc, err)
		}
	}

	managedNS := "outage-managed-ns"
	if err := admin.Create(context.Background(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: managedNS,
			Labels: map[string]string{
				util.LabelNamespaceManaged: "true",
			},
		},
	}); err != nil {
		t.Fatalf("creating managed namespace: %v", err)
	}

	// 1. runc pod without runtimeClassName: fails open (Ignore) -> creation SUCCEEDS
	runcDefaultPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: managedNS,
			Name:      "runc-default-pod",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Name: "app", Image: "registry.k8s.io/pause:3.10"},
			},
		},
	}
	if err := admin.Create(context.Background(), runcDefaultPod); err != nil {
		t.Errorf("creating runc pod without runtimeClassName during outage failed: %v, want success (fail-open)", err)
	}

	// 2. runc pod with explicit runtimeClassName != 'gvisor': fails open (Ignore) -> creation SUCCEEDS
	runcNamedClass := "runc"
	runcExplicitPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: managedNS,
			Name:      "runc-explicit-pod",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &runcNamedClass,
			Containers: []corev1.Container{
				{Name: "app", Image: "registry.k8s.io/pause:3.10"},
			},
		},
	}
	if err := admin.Create(context.Background(), runcExplicitPod); err != nil {
		t.Errorf("creating runc pod with explicit runtimeClassName during outage failed: %v, want success (fail-open)", err)
	}

	// 3. gVisor pod with runtimeClassName == 'gvisor': fails closed (Fail) -> creation FAILS
	gvisorClass := "gvisor"
	gvisorPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: managedNS,
			Name:      "gvisor-pod",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorClass,
			Containers: []corev1.Container{
				{Name: "app", Image: "registry.k8s.io/pause:3.10"},
			},
		},
	}
	err = admin.Create(context.Background(), gvisorPod)
	if err == nil {
		t.Errorf("creating gvisor pod during outage succeeded, want error (fail-closed)")
	} else if !strings.Contains(err.Error(), "mpodgate.podmigration.gke.io") {
		t.Errorf("creating gvisor pod error = %v, want denial mentioning mpodgate.podmigration.gke.io", err)
	}
}

func ptr[T any](v T) *T {
	return &v
}
