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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/yaml"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
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
		if err := apiruntime.DefaultUnstructuredConverter.FromUnstructured(obj.Object, into); err != nil {
			t.Fatalf("converting %s %s of %s: %v", kind, name, path, err)
		}
		return
	}
	t.Fatalf("%s has no %s named %s", path, kind, name)
}

// TestWebhook_CreationReinvocation_Envtest verifies the complete mutating webhook chain
// loaded directly from the shipped manifests (controller/deploy.yaml and CRIU deploy.yaml),
// exercising both lexicographical orders with third-party label-stamping webhooks sorting
// before (a-stamper) and after (z-stamper) our webhooks (#303).
//
// Invariants verified:
// 1. Shipped mpodgate uses reinvocationPolicy: IfNeeded with namespaceSelector on managed namespaces.
// 2. mpodgate-runc matches all pods in managed namespaces (no matchConditions restriction) so pods
//    whose runtimeClassName or opt-in label is added by later webhooks are reinvoked and gated.
// 3. mpodgate is ordered BEFORE CRIU restore-inject (mutating-webhook-configuration < pod-migrate-psengine-restore-inject).
// 4. On reinvocation, mpodgate does NOT scrub pod-migrate.io/psengine-injected on an already-gated pod.
func TestWebhook_CreationReinvocation_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping creation reinvocation webhook envtest")
	}

	// 1. Load shipped mutating-webhook-configuration from controller/deploy.yaml.
	shippedMpodgate := &admissionregistrationv1.MutatingWebhookConfiguration{}
	manifestObject(t, controllerManifestPath(), "MutatingWebhookConfiguration", "mutating-webhook-configuration", shippedMpodgate)

	// Pin shipped configuration invariants
	if shippedMpodgate.Name != "mutating-webhook-configuration" {
		t.Fatalf("shipped mutating webhook configuration name = %q, want 'mutating-webhook-configuration'", shippedMpodgate.Name)
	}
	if len(shippedMpodgate.Webhooks) != 4 {
		t.Fatalf("shipped mutating webhook configuration has %d webhooks, want 4 (mpodgate, mpodgate-runc, mpodstatus, mpodfence)", len(shippedMpodgate.Webhooks))
	}
	for _, wh := range shippedMpodgate.Webhooks {
		if wh.Name == "mpodgate.podmigration.gke.io" || wh.Name == "mpodgate-runc.podmigration.gke.io" {
			if wh.ReinvocationPolicy == nil || *wh.ReinvocationPolicy != admissionregistrationv1.IfNeededReinvocationPolicy {
				t.Fatalf("webhook %s reinvocationPolicy = %v, want IfNeeded", wh.Name, wh.ReinvocationPolicy)
			}
		}
		if wh.Name == "mpodgate-runc.podmigration.gke.io" {
			if len(wh.MatchConditions) != 0 {
				t.Fatalf("webhook %s has matchConditions (%+v), want none so it matches every pod in managed namespaces", wh.Name, wh.MatchConditions)
			}
			if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregistrationv1.Ignore {
				t.Fatalf("webhook %s failurePolicy = %v, want Ignore", wh.Name, wh.FailurePolicy)
			}
		}
		if wh.Name == "mpodgate.podmigration.gke.io" {
			if wh.FailurePolicy == nil || *wh.FailurePolicy != admissionregistrationv1.Fail {
				t.Fatalf("webhook %s failurePolicy = %v, want Fail", wh.Name, wh.FailurePolicy)
			}
		}
	}

	// 2. Load shipped pod-migrate-psengine-restore-inject from engines/criu/deploy/deploy.yaml.
	shippedCRIU := &admissionregistrationv1.MutatingWebhookConfiguration{}
	manifestObject(t, criuManifestPath(), "MutatingWebhookConfiguration", "pod-migrate-psengine-restore-inject", shippedCRIU)
	if shippedCRIU.Name != "pod-migrate-psengine-restore-inject" {
		t.Fatalf("shipped CRIU webhook configuration name = %q, want 'pod-migrate-psengine-restore-inject'", shippedCRIU.Name)
	}
	if shippedMpodgate.Name >= shippedCRIU.Name {
		t.Fatalf("ordering violation: %q must sort alphabetically before %q so mpodgate gates before restore-inject", shippedMpodgate.Name, shippedCRIU.Name)
	}

	// 3. Construct early and late label-stamping webhooks to test both ordering paths.
	earlyStamperPath := "/mutate-stamper-early"
	lateStamperPath := "/mutate-stamper-late"
	failPolicy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	reinvokeNever := admissionregistrationv1.NeverReinvocationPolicy

	// a-stamper sorts BEFORE mutating-webhook-configuration
	aStamper := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "a-stamper"},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name:                    "early-stamper.test.gke.io",
				AdmissionReviewVersions: []string{"v1"},
				FailurePolicy:           &failPolicy,
				SideEffects:             &sideEffects,
				ReinvocationPolicy:      &reinvokeNever,
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Path: &earlyStamperPath,
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
	}

	// z-stamper sorts AFTER pod-migrate-psengine-restore-inject
	zStamper := &admissionregistrationv1.MutatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: "z-stamper"},
		Webhooks: []admissionregistrationv1.MutatingWebhook{
			{
				Name:                    "late-stamper.test.gke.io",
				AdmissionReviewVersions: []string{"v1"},
				FailurePolicy:           &failPolicy,
				SideEffects:             &sideEffects,
				ReinvocationPolicy:      &reinvokeNever,
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					Service: &admissionregistrationv1.ServiceReference{
						Path: &lateStamperPath,
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
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MutatingWebhooks: []*admissionregistrationv1.MutatingWebhookConfiguration{
				aStamper,
				shippedMpodgate,
				shippedCRIU,
				zStamper,
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

	scheme := apiruntime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = nodev1.AddToScheme(scheme)

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
	t.Cleanup(cancel)

	if err := mgr.GetFieldIndexer().IndexField(ctx, &pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue); err != nil {
		t.Fatalf("indexing PMJ parent key: %v", err)
	}
	if err := mgr.GetFieldIndexer().IndexField(ctx, &corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue); err != nil {
		t.Fatalf("indexing Pod assigned PMJ: %v", err)
	}

	// 1. Shipped replacement webhook handler: /mutate-v1-pod-scheduling-gate
	if err := SetupReplacementWebhookWithManager(mgr, mgr.GetAPIReader(), true); err != nil {
		t.Fatalf("setting up replacement webhook: %v", err)
	}
	if err := SetupStatusWebhookWithManager(mgr, mgr.GetAPIReader()); err != nil {
		t.Fatalf("setting up status webhook: %v", err)
	}
	if err := SetupFenceWebhookWithManager(mgr); err != nil {
		t.Fatalf("setting up fence webhook: %v", err)
	}

	// 2. Shipped CRIU restore-inject webhook handler: /psengine-restore-inject
	// Stand-in for CRIU restore-inject webhook handler modeling psengine.RestoreInjector and InjectHolder (#135):
	// Injects pmhold and pm-podinfo volumes, pm-holder-seed initContainer,
	// and holder command wrap, and stamps pod-migrate.io/psengine-injected: "true"
	// only when pod carries gke.io/pod-migration-gate and an assigned PMJ (#135).
	// If already injected (pod-migrate.io/psengine-injected == "true"), returns Allowed without mutating.
	mgr.GetWebhookServer().Register("/psengine-restore-inject", &ctrlwebhook.Admission{
		Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			pod := &corev1.Pod{}
			if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
				return admission.Errored(http.StatusBadRequest, err)
			}
			hasGate := false
			for _, g := range pod.Spec.SchedulingGates {
				if g.Name == "gke.io/pod-migration-gate" {
					hasGate = true
					break
				}
			}
			assignedPMJ := pod.Annotations[util.AnnotationAssignedPMJ]
			if !hasGate || assignedPMJ == "" {
				return admission.Allowed("not a gated migration replacement pod")
			}
			if pod.Annotations[engine.CRIUInjectedAnnotation] == "true" {
				return admission.Allowed("already injected")
			}

			// Stand-in modeling CRIU psengine.InjectHolder to pin duplicate detection
			pod.Spec.Volumes = append(pod.Spec.Volumes,
				corev1.Volume{Name: "pmhold", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				corev1.Volume{Name: "pm-podinfo", VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{
					Items: []corev1.DownwardAPIVolumeFile{{
						Path:     "annotations",
						FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations"},
					}},
				}}},
			)
			pod.Spec.InitContainers = append(pod.Spec.InitContainers, corev1.Container{
				Name:    "pm-holder-seed",
				Image:   "gcr.io/gke-labs/holder:v1",
				Command: []string{"/holder", "seed", "/pmhold/holder"},
				VolumeMounts: []corev1.VolumeMount{
					{Name: "pmhold", MountPath: "/pmhold"},
				},
			})
			if len(pod.Spec.Containers) == 1 {
				app := &pod.Spec.Containers[0]
				orig := append(append([]string{}, app.Command...), app.Args...)
				app.VolumeMounts = append(app.VolumeMounts,
					corev1.VolumeMount{Name: "pmhold", MountPath: "/pmhold", ReadOnly: true},
					corev1.VolumeMount{Name: "pm-podinfo", MountPath: "/etc/pod-migrate", ReadOnly: true},
				)
				app.Command = append([]string{"/pmhold/holder", "wrap", "--"}, orig...)
				app.Args = nil
			}
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations[engine.CRIUInjectedAnnotation] = "true"

			marshaled, err := json.Marshal(pod)
			if err != nil {
				return admission.Errored(http.StatusInternalServerError, err)
			}
			return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
		}),
	})

	// 3. Early stamper (a-stamper): stamps enabled: "true" when workload == "test-early"
	mgr.GetWebhookServer().Register(earlyStamperPath, &ctrlwebhook.Admission{
		Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			pod := &corev1.Pod{}
			if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
				return admission.Errored(http.StatusBadRequest, err)
			}
			if pod.Labels != nil && pod.Labels["workload"] == "test-early" {
				pod.Labels["pod-migration.gke.io/enabled"] = "true"
			}
			marshaled, err := json.Marshal(pod)
			if err != nil {
				return admission.Errored(http.StatusInternalServerError, err)
			}
			return admission.PatchResponseFromRaw(req.Object.Raw, marshaled)
		}),
	})

	// 4. Late stamper (z-stamper): stamps enabled: "true" when workload == "test-late",
	// or stamps gvisor + enabled: "true" when workload == "test-set-runtime-and-label",
	// or touches annotation when workload == "trigger-reinvocation".
	mgr.GetWebhookServer().Register(lateStamperPath, &ctrlwebhook.Admission{
		Handler: admission.HandlerFunc(func(ctx context.Context, req admission.Request) admission.Response {
			pod := &corev1.Pod{}
			if err := json.Unmarshal(req.Object.Raw, pod); err != nil {
				return admission.Errored(http.StatusBadRequest, err)
			}
			if pod.Labels != nil && pod.Labels["workload"] == "test-late" {
				pod.Labels["pod-migration.gke.io/enabled"] = "true"
			}
			if pod.Labels != nil && pod.Labels["workload"] == "test-set-runtime-and-label" {
				gvisor := "gvisor"
				pod.Spec.RuntimeClassName = &gvisor
				pod.Labels["pod-migration.gke.io/enabled"] = "true"
			}
			if pod.Labels != nil && pod.Labels["workload"] == "trigger-reinvocation" {
				if pod.Annotations == nil {
					pod.Annotations = make(map[string]string)
				}
				pod.Annotations["stamper-touched"] = "true"
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

	// Register RuntimeClass for gvisor and runc so API server accepts pods with runtimeClassName: gvisor
	for _, rc := range []string{"runc", "gvisor"} {
		if err := admin.Create(ctx, &nodev1.RuntimeClass{
			ObjectMeta: metav1.ObjectMeta{Name: rc},
			Handler:    rc,
		}); err != nil {
			t.Fatalf("creating runtimeClass %s: %v", rc, err)
		}
	}

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

	t.Run("replacement pod opted in by later webhook (z-stamper) gets gated and CRIU holder injected", func(t *testing.T) {
		pmj := createPMJ(managedNS, "work-pod-late")

		// Pod created without opt-in label:
		// Round 1: mpodgate skips (no label), restore-inject skips (no gate), z-stamper stamps label.
		// Round 2: mpodgate gates pod and assigns PMJ, restore-inject wraps holder and stamps psengine-injected.
		// Round 3: mpodgate reinvokes but does NOT scrub psengine-injected!
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "work-pod-late",
				Labels: map[string]string{
					"workload": "test-late",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10", Command: []string{"/app"}},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}

		if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
			t.Errorf("pod-migration.gke.io/enabled = %q, want 'true'", pod.Labels["pod-migration.gke.io/enabled"])
		}
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != "gke.io/pod-migration-gate" {
			t.Errorf("schedulingGates = %+v, want [gke.io/pod-migration-gate]", pod.Spec.SchedulingGates)
		}
		if pod.Annotations[util.AnnotationAssignedPMJ] != pmj.Name {
			t.Errorf("assigned-pmj = %q, want %q", pod.Annotations[util.AnnotationAssignedPMJ], pmj.Name)
		}
		if pod.Annotations[engine.CRIUInjectedAnnotation] != "true" {
			t.Errorf("psengine-injected = %q, want 'true'", pod.Annotations[engine.CRIUInjectedAnnotation])
		}
		if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "pm-holder-seed" {
			t.Errorf("initContainers = %+v, want exactly 1 [pm-holder-seed]", pod.Spec.InitContainers)
		}
		holderVolumes := 0
		for _, v := range pod.Spec.Volumes {
			if v.Name == "pmhold" {
				holderVolumes++
			}
		}
		if holderVolumes != 1 {
			t.Errorf("pmhold volumes = %d, want exactly 1", holderVolumes)
		}
		cmd := pod.Spec.Containers[0].Command
		if len(cmd) < 3 || cmd[0] != "/pmhold/holder" || cmd[1] != "wrap" || cmd[2] != "--" {
			t.Errorf("command = %v, want wrapped with /pmhold/holder wrap --", cmd)
		} else if len(cmd) >= 6 && cmd[3] == "/pmhold/holder" && cmd[4] == "wrap" {
			t.Errorf("command double-wrapped: %v", cmd)
		}
	})

	t.Run("replacement pod opted in by earlier webhook (a-stamper) gets gated and CRIU holder injected", func(t *testing.T) {
		pmj := createPMJ(managedNS, "work-pod-early")

		// Pod created without opt-in label:
		// Round 1: a-stamper stamps label, mpodgate gates pod and assigns PMJ, restore-inject wraps holder and stamps psengine-injected.
		// Round 2: mpodgate reinvokes and does NOT scrub psengine-injected!
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "work-pod-early",
				Labels: map[string]string{
					"workload": "test-early",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "app", Image: "registry.k8s.io/pause:3.10", Command: []string{"/app"}},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}

		if pod.Labels["pod-migration.gke.io/enabled"] != "true" {
			t.Errorf("pod-migration.gke.io/enabled = %q, want 'true'", pod.Labels["pod-migration.gke.io/enabled"])
		}
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != "gke.io/pod-migration-gate" {
			t.Errorf("schedulingGates = %+v, want [gke.io/pod-migration-gate]", pod.Spec.SchedulingGates)
		}
		if pod.Annotations[util.AnnotationAssignedPMJ] != pmj.Name {
			t.Errorf("assigned-pmj = %q, want %q", pod.Annotations[util.AnnotationAssignedPMJ], pmj.Name)
		}
		if pod.Annotations[engine.CRIUInjectedAnnotation] != "true" {
			t.Errorf("psengine-injected = %q, want 'true'", pod.Annotations[engine.CRIUInjectedAnnotation])
		}
		if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "pm-holder-seed" {
			t.Errorf("initContainers = %+v, want exactly 1 [pm-holder-seed]", pod.Spec.InitContainers)
		}
		holderEarlyVolumes := 0
		for _, v := range pod.Spec.Volumes {
			if v.Name == "pmhold" {
				holderEarlyVolumes++
			}
		}
		if holderEarlyVolumes != 1 {
			t.Errorf("pmhold volumes = %d, want exactly 1", holderEarlyVolumes)
		}
		cmdEarly := pod.Spec.Containers[0].Command
		if len(cmdEarly) < 3 || cmdEarly[0] != "/pmhold/holder" || cmdEarly[1] != "wrap" || cmdEarly[2] != "--" {
			t.Errorf("command = %v, want wrapped with /pmhold/holder wrap --", cmdEarly)
		} else if len(cmdEarly) >= 6 && cmdEarly[3] == "/pmhold/holder" && cmdEarly[4] == "wrap" {
			t.Errorf("command double-wrapped: %v", cmdEarly)
		}
	})

	t.Run("runtimeClassName set to gvisor by later webhook is gated via runc entry reinvocation", func(t *testing.T) {
		pmj := createPMJ(managedNS, "work-pod-runtime-later")

		// Pod created without runtimeClassName and without opt-in label.
		// Round 1: mpodgate (gVisor) matchConditions is false.
		//          mpodgate-runc matches every pod in managed namespaces! But opt-in label is missing.
		//          z-stamper sets runtimeClassName: gvisor AND pod-migration.gke.io/enabled: "true".
		// Round 2: mpodgate-runc is reinvoked, sees opt-in label, matches PMJ, and injects scheduling gate!
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "work-pod-runtime-later",
				Labels: map[string]string{
					"workload": "test-set-runtime-and-label",
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

		if pod.Spec.RuntimeClassName == nil || *pod.Spec.RuntimeClassName != "gvisor" {
			t.Errorf("runtimeClassName = %v, want 'gvisor'", pod.Spec.RuntimeClassName)
		}
		if len(pod.Spec.SchedulingGates) != 1 || pod.Spec.SchedulingGates[0].Name != "gke.io/pod-migration-gate" {
			t.Errorf("schedulingGates = %+v, want [gke.io/pod-migration-gate]", pod.Spec.SchedulingGates)
		}
		if pod.Annotations[util.AnnotationAssignedPMJ] != pmj.Name {
			t.Errorf("assigned-pmj = %q, want %q", pod.Annotations[util.AnnotationAssignedPMJ], pmj.Name)
		}
	})

	t.Run("already-gated pod with CRIU injected marker preserves marker on reinvocation", func(t *testing.T) {
		pmj := createPMJ(managedNS, "already-gated-reinvoke")

		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: managedNS,
				Name:      "already-gated-reinvoke",
				Labels: map[string]string{
					"workload":                     "trigger-reinvocation",
					"pod-migration.gke.io/enabled": "true",
				},
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ:    pmj.Name,
					engine.CRIUInjectedAnnotation: "true",
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{
					{Name: "gke.io/pod-migration-gate"},
				},
				Volumes: []corev1.Volume{
					{Name: "pmhold", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					{Name: "pm-podinfo", VolumeSource: corev1.VolumeSource{DownwardAPI: &corev1.DownwardAPIVolumeSource{
						Items: []corev1.DownwardAPIVolumeFile{{Path: "annotations", FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.annotations"}}},
					}}},
				},
				InitContainers: []corev1.Container{
					{
						Name:    "pm-holder-seed",
						Image:   "gcr.io/gke-labs/holder:v1",
						Command: []string{"/holder", "seed", "/pmhold/holder"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "pmhold", MountPath: "/pmhold"},
						},
					},
				},
				Containers: []corev1.Container{
					{
						Name:    "app",
						Image:   "registry.k8s.io/pause:3.10",
						Command: []string{"/pmhold/holder", "wrap", "--", "/app"},
						VolumeMounts: []corev1.VolumeMount{
							{Name: "pmhold", MountPath: "/pmhold", ReadOnly: true},
							{Name: "pm-podinfo", MountPath: "/etc/pod-migrate", ReadOnly: true},
						},
					},
				},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating already-gated pod: %v", err)
		}

		// Verify reinvocation did not strip the CRIU injected marker
		if pod.Annotations[engine.CRIUInjectedAnnotation] != "true" {
			t.Errorf("psengine-injected was stripped on reinvocation: got %q, want 'true'", pod.Annotations[engine.CRIUInjectedAnnotation])
		}
		if pod.Annotations["stamper-touched"] != "true" {
			t.Errorf("stamper-touched = %q, want 'true' (confirming z-stamper reinvocation occurred)", pod.Annotations["stamper-touched"])
		}
		if len(pod.Spec.SchedulingGates) != 1 {
			t.Errorf("schedulingGates count = %d, want 1", len(pod.Spec.SchedulingGates))
		}
		if len(pod.Spec.InitContainers) != 1 || pod.Spec.InitContainers[0].Name != "pm-holder-seed" {
			t.Errorf("initContainers = %+v, want exactly 1 [pm-holder-seed]", pod.Spec.InitContainers)
		}
		holderReinvokeVolumes := 0
		for _, v := range pod.Spec.Volumes {
			if v.Name == "pmhold" {
				holderReinvokeVolumes++
			}
		}
		if holderReinvokeVolumes != 1 {
			t.Errorf("pmhold volumes = %d, want exactly 1", holderReinvokeVolumes)
		}
		cmdReinvoke := pod.Spec.Containers[0].Command
		if len(cmdReinvoke) < 3 || cmdReinvoke[0] != "/pmhold/holder" || cmdReinvoke[1] != "wrap" || cmdReinvoke[2] != "--" {
			t.Errorf("command = %v, want wrapped with /pmhold/holder wrap --", cmdReinvoke)
		} else if len(cmdReinvoke) >= 6 && cmdReinvoke[3] == "/pmhold/holder" && cmdReinvoke[4] == "wrap" {
			t.Errorf("command double-wrapped: %v", cmdReinvoke)
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
					"workload": "test-late",
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
// is unreachable (controller outage), the shipped MutatingWebhookConfiguration from
// controller/deploy.yaml ensures:
// 1. runc pod without runtimeClassName succeeds (fails open with Ignore)
// 2. runc pod with explicit runtimeClassName != 'gvisor' succeeds (fails open with Ignore)
// 3. gVisor pod with runtimeClassName == 'gvisor' fails closed (FailurePolicy: Fail)
// 4. Custom gVisor runtime class (e.g. 'gvisor-custom') fails open with Ignore (#303, #83).
func TestWebhook_OutageFailOpenFailClosed_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping outage fail-open/fail-closed envtest")
	}

	shippedMpodgate := &admissionregistrationv1.MutatingWebhookConfiguration{}
	manifestObject(t, controllerManifestPath(), "MutatingWebhookConfiguration", "mutating-webhook-configuration", shippedMpodgate)

	// Dummy port where nothing is listening to simulate unreachable webhook endpoint.
	unreachablePort := int32(59999)
	for i := range shippedMpodgate.Webhooks {
		shippedMpodgate.Webhooks[i].TimeoutSeconds = ptr(int32(1))
		if shippedMpodgate.Webhooks[i].ClientConfig.Service != nil {
			shippedMpodgate.Webhooks[i].ClientConfig.Service.Port = &unreachablePort
		}
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MutatingWebhooks: []*admissionregistrationv1.MutatingWebhookConfiguration{
				shippedMpodgate,
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

	scheme := apiruntime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = nodev1.AddToScheme(scheme)

	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}

	// Create RuntimeClass objects so the API server does not reject pods with "RuntimeClass not found"
	for _, rc := range []string{"runc", "gvisor", "gvisor-custom"} {
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

	// 4. Custom gVisor class name (e.g. 'gvisor-custom'): falls through to mpodgate-runc (Ignore) -> succeeds
	gvisorCustomClass := "gvisor-custom"
	customPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: managedNS,
			Name:      "gvisor-custom-pod",
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			RuntimeClassName: &gvisorCustomClass,
			Containers: []corev1.Container{
				{Name: "app", Image: "registry.k8s.io/pause:3.10"},
			},
		},
	}
	if err := admin.Create(context.Background(), customPod); err != nil {
		t.Errorf("creating gvisor-custom pod during outage failed: %v, want success (fail-open)", err)
	}
}

func ptr[T any](v T) *T {
	return &v
}
