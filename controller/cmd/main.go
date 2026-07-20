/*
Copyright 2026 The PM Learning Demo Authors.

main.go — wires every Reconciler + Webhook into a single Manager.

This is the file that demonstrates THE big idea: there's exactly one binary,
one Manager, and many controllers/webhooks living inside it. The Manager
owns the shared cache, the typed client, leader election, the metrics server,
and the webhook HTTPS server.

In Scott's PoC the structure is identical to this file: 3 reconcilers + 1
webhook + 1 Manager. Read this top-to-bottom and the entire control plane
shape will click.
*/
package main

import (
	"flag"
	"os"

	// Import every API group whose types we need to read/write/serialize.
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pmv1alpha1 "github.com/ahahadelyaly/gke-pod-migration/controller/api/v1alpha1"
	"github.com/ahahadelyaly/gke-pod-migration/controller/internal/controller"
	pmwebhook "github.com/ahahadelyaly/gke-pod-migration/controller/internal/webhook"
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
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election so only one replica is active at a time.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "pod-migration-leader.example.com",
		// Webhook server is on :9443 by default. The Service in
		// config/webhook/service.yaml points at this port.
		WebhookServer: webhook.NewServer(webhook.Options{Port: 9443}),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err := (&controller.PodMigrationReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodMigrationReconciler")
		os.Exit(1)
	}
	if err := (&controller.PodMigrationJobReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodMigrationJobReconciler")
		os.Exit(1)
	}
	if err := (&controller.DeferredEvictionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create DeferredEvictionReconciler")
		os.Exit(1)
	}
	if err := (&controller.PodGateReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
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
