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
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	ctrlwebhook "sigs.k8s.io/controller-runtime/pkg/webhook"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestReplacementWebhook_ReadinessDelay_Envtest exercises SetupReplacementWebhookWithManager
// through a real kube-apiserver MutatingWebhookConfiguration to verify that the
// RFC 6902 JSONPatch removing readinessProbe.initialDelaySeconds passes API server
// admission, defaulting, and Pod validation for both main containers and native
// sidecar initContainers (#141).
func TestReplacementWebhook_ReadinessDelay_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping replacement webhook envtest")
	}

	failPolicy := admissionregistrationv1.Fail
	sideEffects := admissionregistrationv1.SideEffectClassNone
	webhookPath := "/mutate-v1-pod-scheduling-gate"

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
		WebhookInstallOptions: envtest.WebhookInstallOptions{
			MutatingWebhooks: []*admissionregistrationv1.MutatingWebhookConfiguration{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "test-mpodgate"},
					Webhooks: []admissionregistrationv1.MutatingWebhook{
						{
							Name:                    "mpodgate.podmigration.gke.io",
							AdmissionReviewVersions: []string{"v1"},
							FailurePolicy:           &failPolicy,
							SideEffects:             &sideEffects,
							ClientConfig: admissionregistrationv1.WebhookClientConfig{
								Service: &admissionregistrationv1.ServiceReference{
									Path: &webhookPath,
								},
							},
							ObjectSelector: &metav1.LabelSelector{
								MatchLabels: map[string]string{"pod-migration.gke.io/enabled": "true"},
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
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
		WebhookServer: ctrlwebhook.NewServer(ctrlwebhook.Options{
			Host:    whOpts.LocalServingHost,
			Port:    whOpts.LocalServingPort,
			CertDir: whOpts.LocalServingCertDir,
		}),
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

	mgrDone := make(chan error, 1)
	go func() {
		mgrDone <- mgr.Start(ctx)
	}()
	// Cleanups run in reverse order: stop the manager before stopping envtest.
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

	createActivePMJ := func(podName string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      util.FormatPMJName(podName, "abcdef12"),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: podName},
			},
		}
		if err := admin.Create(ctx, pmj); err != nil {
			t.Fatalf("creating PMJ %s: %v", pmj.Name, err)
		}
		pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmj.Status.SnapshotRef = "snap-1"
		if err := admin.Status().Update(ctx, pmj); err != nil {
			t.Fatalf("updating PMJ %s status: %v", pmj.Name, err)
		}
		return pmj
	}

	always := corev1.ContainerRestartPolicyAlways
	makePod := func(name string, annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   "default",
				Name:        name,
				Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
				Annotations: annotations,
			},
			Spec: corev1.PodSpec{
				InitContainers: []corev1.Container{
					{
						Name:           "sidecar",
						Image:          "registry.k8s.io/pause:3.10",
						RestartPolicy:  &always,
						ReadinessProbe: httpGetProbe("/readyz", 45, 10),
						LivenessProbe:  httpGetProbe("/livez", 45, 10),
					},
				},
				Containers: []corev1.Container{
					{
						Name:           "app",
						Image:          "registry.k8s.io/pause:3.10",
						ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						LivenessProbe:  httpGetProbe("/livez", 50, 20),
					},
				},
			},
		}
	}

	// 1. Restore-candidate replacement pod: gated, readiness initialDelaySeconds cleared to 0, liveness preserved at 50.
	pmj1 := createActivePMJ("envtest-repl")
	repl := makePod("envtest-repl", nil)
	if err := admin.Create(ctx, repl); err != nil {
		t.Fatalf("creating replacement pod: %v", err)
	}
	if repl.Annotations[util.AnnotationAssignedPMJ] != pmj1.Name {
		t.Errorf("assigned-pmj = %q, want %q", repl.Annotations[util.AnnotationAssignedPMJ], pmj1.Name)
	}
	if len(repl.Spec.SchedulingGates) != 1 || repl.Spec.SchedulingGates[0].Name != "gke.io/pod-migration-gate" {
		t.Errorf("schedulingGates = %+v, want [gke.io/pod-migration-gate]", repl.Spec.SchedulingGates)
	}
	if got := repl.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds; got != 0 {
		t.Errorf("container readiness initialDelaySeconds = %d, want 0", got)
	}
	if got := repl.Spec.Containers[0].ReadinessProbe.PeriodSeconds; got != 20 {
		t.Errorf("container readiness periodSeconds = %d, want 20", got)
	}
	if got := repl.Spec.Containers[0].LivenessProbe.InitialDelaySeconds; got != 50 {
		t.Errorf("container liveness initialDelaySeconds = %d, want 50", got)
	}
	if got := repl.Spec.InitContainers[0].ReadinessProbe.InitialDelaySeconds; got != 0 {
		t.Errorf("sidecar readiness initialDelaySeconds = %d, want 0", got)
	}
	if got := repl.Spec.InitContainers[0].LivenessProbe.InitialDelaySeconds; got != 45 {
		t.Errorf("sidecar liveness initialDelaySeconds = %d, want 45", got)
	}

	// 2. Opt-out annotation: gated, readiness initialDelaySeconds preserved.
	pmj2 := createActivePMJ("envtest-optout")
	optout := makePod("envtest-optout", map[string]string{util.AnnotationPreserveReadinessDelay: "true"})
	if err := admin.Create(ctx, optout); err != nil {
		t.Fatalf("creating opt-out replacement pod: %v", err)
	}
	if optout.Annotations[util.AnnotationAssignedPMJ] != pmj2.Name {
		t.Errorf("opt-out assigned-pmj = %q, want %q", optout.Annotations[util.AnnotationAssignedPMJ], pmj2.Name)
	}
	if got := optout.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds; got != 50 {
		t.Errorf("opt-out container readiness initialDelaySeconds = %d, want 50", got)
	}
	if got := optout.Spec.InitContainers[0].ReadinessProbe.InitialDelaySeconds; got != 45 {
		t.Errorf("opt-out sidecar readiness initialDelaySeconds = %d, want 45", got)
	}

	// 3. Scale-up pod (no matching PMJ): cold-start bypass stamped, readiness initialDelaySeconds preserved.
	scaleup := makePod("envtest-scaleup", nil)
	if err := admin.Create(ctx, scaleup); err != nil {
		t.Fatalf("creating scale-up pod: %v", err)
	}
	if v, ok := scaleup.Annotations["podsnapshot.gke.io/ps-name"]; !ok || v != "" {
		t.Errorf("scale-up annotations = %v, want ps-name=\"\"", scaleup.Annotations)
	}
	if got := scaleup.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds; got != 50 {
		t.Errorf("scale-up container readiness initialDelaySeconds = %d, want 50", got)
	}
}
