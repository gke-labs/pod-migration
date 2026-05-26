package main

import (
	"flag"
	"os"

	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"k8s.io/gke-autoscaling/pod-migration/pkg/webhook"
	"k8s.io/gke-autoscaling/pod-migration/pkg/controller"
)

func main() {
	klog.InitFlags(nil)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	klog.Info("Starting Pod Migration Controller")

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{})
	if err != nil {
		klog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Setup webhooks
	decoder := admission.NewDecoder(mgr.GetScheme())

	guard := &webhook.EvictionGuard{Client: mgr.GetClient()}
	guard.InjectDecoder(decoder)

	mgr.GetWebhookServer().Register("/validate-eviction", &admission.Webhook{
		Handler: guard,
	})

	// Setup controllers
	if err = (&controller.PodReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		klog.Error(err, "unable to create controller", "controller", "Pod")
		os.Exit(1)
	}
	if err = (&controller.SnapshotReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		klog.Error(err, "unable to create controller", "controller", "Snapshot")
		os.Exit(1)
	}
	if err = (&controller.DeferredEvictionReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		klog.Error(err, "unable to create controller", "controller", "DeferredEviction")
		os.Exit(1)
	}

	klog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		klog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
