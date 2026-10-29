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

package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var metricsAddr, probeAddr string
	var leaderElect bool
	var enableWebhooks bool
	var webhookCertDir string
	var holderImage string
	var kubeAPIQPS float64
	var kubeAPIBurst int

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "metrics endpoint bind address")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "health probe bind address")
	flag.BoolVar(&leaderElect, "leader-elect", true, "enable leader election for the engine controller manager")
	flag.BoolVar(&enableWebhooks, "enable-webhooks", true, "serve the /psengine-restore-inject admission webhook on :9443")
	flag.StringVar(&webhookCertDir, "webhook-cert-dir", "/tmp/k8s-webhook-server/serving-certs", "TLS cert directory for the webhook server")
	flag.StringVar(&holderImage, "holder-image", os.Getenv("HOLDER_IMAGE"), "placeholder container image injected into gated replacement pods")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", 50, "Kubernetes API client QPS limit")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", 100, "Kubernetes API client burst limit")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	if enableWebhooks && holderImage == "" {
		log.Error(fmt.Errorf("--holder-image (or HOLDER_IMAGE) is required when --enable-webhooks is true"), "missing holder image")
		os.Exit(1)
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         leaderElect,
		LeaderElectionID:       "psengine.pod-migrate.io",
	}
	if enableWebhooks {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    9443,
			CertDir: webhookCertDir,
		})
	}

	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = float32(kubeAPIQPS)
	cfg.Burst = kubeAPIBurst
	mgr, err := ctrl.NewManager(cfg, mgrOpts)
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Preflight: refuse to start if the podsnapshot.gke.io CRDs are missing or
	// owned by another provider (such as the managed GKE Pod Snapshots addon).
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		return psengine.VerifyCRDOwnership(ctx, mgr.GetAPIReader())
	})); err != nil {
		log.Error(err, "unable to register psengine preflight")
		os.Exit(1)
	}

	gcsStore := psengine.NewGCSStore()
	if err := (&psengine.PSPReconciler{
		Client: mgr.GetClient(),
		Probe:  psengine.NewGCSProber(gcsStore),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine PSP reconciler")
		os.Exit(1)
	}
	if err := (&psengine.PSMTReconciler{
		Client: mgr.GetClient(),
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine PSMT reconciler")
		os.Exit(1)
	}

	if enableWebhooks {
		dec := admission.NewDecoder(mgr.GetScheme())
		ws := mgr.GetWebhookServer()
		ws.Register("/psengine-restore-inject", &webhook.Admission{
			Handler: &psengine.RestoreInjector{Decoder: dec, HolderImage: holderImage},
		})
		log.Info("registered /psengine-restore-inject webhook", "certDir", webhookCertDir, "holderImage", holderImage)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	log.Info("starting criu-snapshot-engine manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited")
		os.Exit(1)
	}
}
