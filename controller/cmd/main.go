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
	"fmt"
	"net/http"
	"os"
	"sync/atomic"

	// Import every API group whose types we need to read/write/serialize.
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/controller"
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
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              "pod-migration-leader.gke.io",
		LeaderElectionReleaseOnCancel: true,
		Cache: cache.Options{
			DefaultTransform: cache.TransformStripManagedFields(),
		},
		// Webhook server is on :9443 by default.
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
		Client:    mgr.GetClient(),
		APIReader: mgr.GetAPIReader(),
		Scheme:    mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodMigrationJobReconciler")
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
	if err := pmwebhook.SetupReplacementWebhookWithManager(mgr, mgr.GetAPIReader()); err != nil {
		setupLog.Error(err, "unable to register replacement mutating webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupStatusWebhookWithManager(mgr, mgr.GetAPIReader()); err != nil {
		setupLog.Error(err, "unable to register pod status mutating webhook")
		os.Exit(1)
	}

	// --- Health/readiness probes --------------------------------------------
	var startupCacheSynced atomic.Bool
	if err := mgr.Add(&startupCacheSyncRunnable{
		cache:  mgr.GetCache(),
		synced: &startupCacheSynced,
	}); err != nil {
		setupLog.Error(err, "unable to add startup cache sync runnable")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up healthz")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", func(req *http.Request) error {
		if err := mgr.GetWebhookServer().StartedChecker()(req); err != nil {
			return err
		}
		if !startupCacheSynced.Load() {
			return fmt.Errorf("initial startup informer caches not synced yet")
		}
		return nil
	}); err != nil {
		setupLog.Error(err, "unable to set up readyz")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "manager exited")
		os.Exit(1)
	}
}

// startupCacheSyncRunnable waits for the manager's initial informer caches to sync
// on manager startup. It implements manager.LeaderElectionRunnable with NeedLeaderElection() = false
// so that standby replicas also synchronize caches immediately and pass the /readyz probe.
type startupCacheSyncRunnable struct {
	cache  cache.Cache
	synced *atomic.Bool
}

func (s *startupCacheSyncRunnable) Start(ctx context.Context) error {
	if s.cache.WaitForCacheSync(ctx) {
		s.synced.Store(true)
	}
	return nil
}

func (s *startupCacheSyncRunnable) NeedLeaderElection() bool {
	return false
}

