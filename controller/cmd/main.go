/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

main.go — wires every Reconciler + Webhook into a single Manager.

This is the file that demonstrates THE big idea: there's exactly one binary,
one Manager, and many controllers/webhooks living inside it. The Manager
owns the shared cache, the typed client, leader election, the metrics server,
and the webhook HTTPS server.

The structure is: 3 reconcilers + 3 webhooks + 1 Manager. Read this top-to-bottom
and the entire control plane shape will click.
*/
package main

import (
	"context"
	"flag"
	"os"

	// Import every API group whose types we need to read/write/serialize.
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	ctrlruntime "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/controller"
	"github.com/gke-labs/pod-migration/controller/internal/util"
	pmwebhook "github.com/gke-labs/pod-migration/controller/internal/webhook"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	// Built-in types (Pods, ConfigMaps, ...) live in client-go's scheme.
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	// Register our custom resource schemas
	utilruntime.Must(pmv1alpha1.AddToScheme(scheme))
	_ = corev1.AddToScheme // already in clientgoscheme; explicit just for clarity
}

func main() {
	var (
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		clientGoQPS          float64
		clientGoBurst        int
		maxConcurrent        int
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election so only one replica is active at a time.")
	flag.Float64Var(&clientGoQPS, "client-go-qps", 500.0, "QPS for client-go REST config")
	flag.IntVar(&clientGoBurst, "client-go-burst", 1000, "Burst for client-go REST config")
	flag.IntVar(&maxConcurrent, "max-concurrent-reconciles", 50, "Maximum number of concurrent reconciles for PodMigrationJobReconciler")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// Reject values client-go would silently reinterpret (QPS==0 falls back to
	// the 5-QPS default; negative disables rate limiting).
	if err := util.ValidateClientGoRateLimits(clientGoQPS, clientGoBurst); err != nil {
		setupLog.Error(err, "invalid client-go rate limit flags")
		os.Exit(1)
	}

	restConfig := ctrl.GetConfigOrDie()
	restConfig.QPS = float32(clientGoQPS)
	restConfig.Burst = clientGoBurst

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "pod-migration-leader.example.com",
		// Webhook server is on :9443 by default.
		WebhookServer: webhook.NewServer(webhook.Options{Port: 9443}),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Cache indexes must be registered before any controller starts; the gate
	// mapper and the restore-timeout deferral both List pods by assigned-pmj.
	if err := controller.RegisterFieldIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		setupLog.Error(err, "unable to register field indexes")
		os.Exit(1)
	}

	reconcilerOpts := ctrlruntime.Options{
		MaxConcurrentReconciles: maxConcurrent,
	}

	if err := (&controller.PodMigrationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodMigrationReconciler")
		os.Exit(1)
	}
	if err := (&controller.PodMigrationJobReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr, reconcilerOpts); err != nil {
		setupLog.Error(err, "unable to create PodMigrationJobReconciler")
		os.Exit(1)
	}

	// PodGateReconciler hardcodes MaxConcurrentReconciles: 1 in its own
	// SetupWithManager — see the comment there for the serialization contract.
	if err := (&controller.PodGateReconciler{
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodGateReconciler")
		os.Exit(1)
	}

	// --- Webhooks ------------------------------------------------------------
	if err := pmwebhook.SetupEvictionWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register eviction webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupReplacementWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register replacement mutating webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupStatusWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register pod status mutating webhook")
		os.Exit(1)
	}

	// --- Health/readiness probes --------------------------------------------
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up readyz")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}
