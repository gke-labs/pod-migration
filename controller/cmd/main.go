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

/*
main.go — wires every Reconciler + Webhook into a single Manager.

This is the file that demonstrates THE big idea: there's exactly one binary,
one Manager, and many controllers/webhooks living inside it. The Manager
owns the shared cache, the typed client, leader election, the metrics server,
and the webhook HTTPS server.

The structure is: 3 reconcilers + 4 webhooks + 1 Manager. Read this top-to-bottom
and the entire control plane shape will click.
*/
package main

import (
	"context"
	"flag"
	"fmt"
	"maps"
	"net/http"
	"os"
	"slices"
	"sync/atomic"
	"time"

	// Import every API group whose types we need to read/write/serialize.
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlruntime "sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/controller"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	// Imported for its side effect: registers the pod-migration collectors on
	// the controller-runtime metrics registry served at --metrics-bind-address.
	_ "github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
	"github.com/gke-labs/pod-migration/controller/internal/version"
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
		metricsAddr             string
		probeAddr               string
		enableLeaderElection    bool
		clientGoQPS             float64
		clientGoBurst           int
		maxConcurrent           int
		migrationTimeout        time.Duration
		invariantModeRaw        string
		spotPreemptionBudget    int64
		runtimeClassesRaw       string
		fenceCap                time.Duration
		fenceLateStopWatch      time.Duration
		runtimeClassEngines     string
		criuAPIGroup            string
		restoreStabilization    time.Duration
		restorePlacement        bool
		placementDeadline       time.Duration
		shortenReadinessDelay   bool
		snapshotRetention       time.Duration
		orphanSweepInterval     time.Duration
		orphanMinAge            time.Duration
		coverageRefreshInterval time.Duration
		storageIsolationMode    string
		allowBucketRootStorage  bool
		showVersion             bool
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager. Enabling this will ensure there is only one active controller manager.")
	flag.Float64Var(&clientGoQPS, "client-go-qps", 500.0, "QPS for client-go REST config")
	flag.IntVar(&clientGoBurst, "client-go-burst", 1000, "Burst for client-go REST config")
	flag.IntVar(&maxConcurrent, "max-concurrent-reconciles", 50, "Maximum number of concurrent reconciles for PodMigrationJobReconciler")
	flag.DurationVar(&migrationTimeout, "migration-timeout", util.DefaultMigrationTimeout, "Default timeout for active migrations (Pending, Snapshotting, Evicting)")
	flag.StringVar(&invariantModeRaw, "invariant-mode", string(invariants.ModeDisabled), "Correctness invariant evaluation mode: disabled (default), observe, or strict (CI gate). Enabling observe/strict adds one informer namespace List of Pods and PMJs per PodMigrationJob reconcile.")
	flag.Int64Var(&spotPreemptionBudget, "spot-preemption-node-budget", util.DefaultSpotPreemptionNodeBudget, "Total memory request budget in bytes per node for spot preemption migrations (default 15GiB)")
	flag.StringVar(&runtimeClassesRaw, "migratable-runtime-classes", eligibility.DefaultMigratableRuntimeClasses, "Comma-separated RuntimeClass names whose pods are migrated on eviction; "+eligibility.DefaultRuntimeClassToken+" matches pods without a runtimeClassName. List only classes the installed snapshot engine supports.")
	flag.DurationVar(&fenceCap, "fence-pre-checkpoint-cap", controller.DefaultFencePreCheckpointCap, "How long a fenced migration source waits for its checkpoint trigger while creating it fails with retryable errors; then the migration fails")
	flag.DurationVar(&fenceLateStopWatch, "fence-late-stop-watch", controller.DefaultFenceLateStopWatch, "How long the controller watches a source that a failed migration returned to service, to fence and evict it again if the snapshot engine stops it anyway")
	flag.StringVar(&runtimeClassEngines, "runtime-class-engines", "", "Comma-separated class=engine routes that pick the snapshot engine for the pods of each migratable runtime class, e.g. gvisor="+engine.GKEPodSnapshotsName+" ("+eligibility.DefaultRuntimeClassToken+" matches pods without a runtimeClassName). Unset routes every --migratable-runtime-classes class to the default engine, "+engine.GKEPodSnapshotsName+". A PodMigration overrides routes for its namespace with the "+engine.AnnotationRuntimeClassEngines+" annotation.")
	flag.StringVar(&criuAPIGroup, "criu-api-group", engine.DefaultCRIUAPIGroup, "Kubernetes API group of the CRIU snapshot engine's CRDs and <group>/ps-name restore annotation; defaults to "+engine.DefaultCRIUAPIGroup+" (can be set to "+engine.DefaultAPIGroup+" for legacy single-group CRIU installs).")
	flag.DurationVar(&restoreStabilization, "restore-stabilization-window", controller.DefaultRestoreStabilizationWindow, fmt.Sprintf("How long a replacement pod must stay Ready without a container restart before a restore that its snapshot engine only inferred concludes as RestoreInferred; 0 concludes as soon as the pod is Ready. A restore the engine confirms (RestoreVerified) doesn't wait. At most %s.", controller.MaxRestoreStabilizationWindow))
	flag.BoolVar(&restorePlacement, "restore-placement", true, "Place a replacement pod that restores a snapshot on a node its snapshot engine can restore on (on by default; --restore-placement=false turns it off). Only a pod that its ReplicaSet or StatefulSet would recreate is placed: the engine's node constraints (for GKE Pod Snapshots, the source node's machine type, if the job recorded it; for CRIU, the CPU and versions the source node's agent recorded, matched against the labels CRIU node agents publish, #257) go into its required node affinity when its scheduling gate is released, unless its own node selector or node affinity already requires them, and even if no node meets them yet, so that a node can still come up, for example one that the cluster autoscaler adds; see --restore-placement-deadline. Any other pod, such as a Job's, is released without them, as with --restore-placement=false, since nothing would recreate it without them if it got stuck. The release event says whether a node met them then: a Ready node (stricter than the scheduler, which leaves not-ready nodes to their taints) that the pod's node selector, node affinity and tolerations allow, cordoned only if the pod tolerates it; resources, volume zones, host ports, pod affinity and topology spread, and other scheduling gates aren't looked at.")
	flag.DurationVar(&placementDeadline, "restore-placement-deadline", controller.DefaultRestorePlacementDeadline, "How long a replacement pod released with --restore-placement's node constraints may stay unschedulable. Then, if a node that doesn't meet the constraints could take it, the job concludes SucceededWithoutRestore with reason "+controller.ReasonPlacementTimeout+", and the pod is deleted so that its ReplicaSet or StatefulSet recreates it to start cold. Otherwise it keeps waiting, because deleting it wouldn't get it scheduled sooner, and at the restore ceiling the job concludes "+controller.ReasonPlacementTimeout+" and deletes it anyway, so that any node can be added for it. Only a pod that the scheduler reports unschedulable is deleted. A pod that the scheduler hasn't tried (another scheduling gate may hold it) or has nominated a node for is left alone; if it's still like that at the restore ceiling, the job concludes RestoreTimeout, then checks the pod every 15s and deletes it once the scheduler reports it unschedulable. A pod nominated just before the delete, or whose delete failed, is also checked every 15s. The checks end when the job is garbage collected, 30m after it concluded. Must be more than 0, and at least 1m shorter than the restore ceiling, 5m plus --restore-stabilization-window: at most 4m30s with the default window.")
	flag.BoolVar(&shortenReadinessDelay, "shorten-restored-readiness-delay", true, "Clear readinessProbe.initialDelaySeconds on gated restore-candidate replacement pods unless annotated with "+util.AnnotationPreserveReadinessDelay+"=true; liveness and startup probes are never changed")
	flag.DurationVar(&snapshotRetention, "snapshot-retention", 0, fmt.Sprintf("How long a job that concluded Succeeded (restore verified or inferred) or Failed keeps the PodSnapshot its trigger created before the controller deletes it, which tells the snapshot engine to delete the checkpoint archive (#107). 0 (the default) deletes it as soon as nothing can still restore from it. Jobs that concluded SucceededWithoutRestore keep their snapshot until they're garbage collected; then the orphan sweep (--orphan-snapshot-sweep-interval) may delete it. At most %s, so the deletion can still be retried before concluded jobs are garbage collected 30m after they concluded.", controller.MaxSnapshotRetention))
	flag.DurationVar(&orphanSweepInterval, "orphan-snapshot-sweep-interval", controller.DefaultOrphanSnapshotSweepInterval, "How often the leader deletes orphaned PodSnapshots: ones our triggers created (gke-pod-snapshot-triggered-by is psmt-<pod>-<uid>) whose PodMigrationJob is gone, and that no pod, trigger or job references (#107). Deleting a PodSnapshot tells its snapshot engine to delete the checkpoint archive. 0 turns the sweep off; otherwise at least 1m.")
	flag.DurationVar(&orphanMinAge, "orphan-snapshot-min-age", controller.DefaultOrphanSnapshotMinAge, fmt.Sprintf("How old an orphaned PodSnapshot must be, from its creation, before the sweep deletes it. When the sweep is on, at least --snapshot-retention plus %s.", controller.OrphanSnapshotMinAgeMargin))
	flag.DurationVar(&coverageRefreshInterval, "coverage-refresh-interval", controller.DefaultCoverageRefreshInterval, "How often active PodMigration coverage metrics are refreshed without re-syncing managed resources. 0 turns periodic refresh off; otherwise at least 5s.")
	flag.StringVar(&storageIsolationMode, "storage-isolation-mode", controller.DefaultStorageIsolationMode, "Multi-tenant storage isolation mode for cross-namespace conflicts when --allow-bucket-root-storage is enabled: warn (default, log and emit Warning events while permitting upgrade migration) or enforce (fail-closed on cross-namespace collision).")
	flag.BoolVar(&allowBucketRootStorage, "allow-bucket-root-storage", false, "Allow PodMigration storage locations to target the bucket root (gs://bucket or gs://bucket/) without namespace path scoping. Intended only for single-tenant clusters where the entire bucket is dedicated to one namespace.")
	flag.BoolVar(&showVersion, "version", false, "Print version information and exit.")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	var criuGroupFlagSet bool
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "criu-api-group" {
			criuGroupFlagSet = true
		}
	})

	if showVersion {
		fmt.Println(version.Get().String())
		os.Exit(0)
	}

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	invariantMode, err := invariants.ParseMode(invariantModeRaw)
	if err != nil {
		setupLog.Error(err, "invalid --invariant-mode flag")
		os.Exit(1)
	}

	runtimeClassPolicy, err := eligibility.ParseRuntimeClassPolicy(runtimeClassesRaw)
	if err != nil {
		setupLog.Error(err, "invalid --migratable-runtime-classes flag")
		os.Exit(1)
	}

	// Checked against the registered engines once the registry is built.
	runtimeClassRoutes, err := engine.ParseRoutes(runtimeClassEngines)
	if err != nil {
		setupLog.Error(err, "invalid --runtime-class-engines flag")
		os.Exit(1)
	}

	if err := validateCRIUAPIGroup(criuAPIGroup); err != nil {
		setupLog.Error(err, "invalid --criu-api-group flag")
		os.Exit(1)
	}

	ver := version.Get()
	setupLog.Info("Starting pod-migration-controller",
		"version", ver.GitVersion,
		"commit", ver.GitCommit,
		"buildDate", ver.BuildDate,
		"go", ver.GoVersion,
		"platform", ver.Platform,
		"invariantMode", invariantMode,
		"migratableRuntimeClasses", runtimeClassPolicy.String(),
		"criuAPIGroup", criuAPIGroup,
		"restoreStabilizationWindow", restoreStabilization,
		"restorePlacement", restorePlacement,
		"restorePlacementDeadline", placementDeadline,
		"shortenRestoredReadinessDelay", shortenReadinessDelay,
		"snapshotRetention", snapshotRetention,
		"orphanSnapshotSweepInterval", orphanSweepInterval,
		"orphanSnapshotMinAge", orphanMinAge)

	// Reject values client-go would silently reinterpret (QPS==0 falls back to
	// the 5-QPS default; negative disables rate limiting).
	if err := util.ValidateClientGoRateLimits(clientGoQPS, clientGoBurst); err != nil {
		setupLog.Error(err, "invalid client-go rate limit flags")
		os.Exit(1)
	}

	if migrationTimeout < util.MinMigrationTimeout || migrationTimeout > util.MaxMigrationTimeout {
		setupLog.Error(fmt.Errorf("migration-timeout %v out of range [%v, %v]", migrationTimeout, util.MinMigrationTimeout, util.MaxMigrationTimeout), "invalid migration timeout flag")
		os.Exit(1)
	}

	if restoreStabilization < 0 || restoreStabilization > controller.MaxRestoreStabilizationWindow {
		setupLog.Error(fmt.Errorf("restore-stabilization-window %v out of range [0s, %v]", restoreStabilization, controller.MaxRestoreStabilizationWindow), "invalid restore stabilization window flag")
		os.Exit(1)
	}

	if err := controller.ValidateSnapshotRetention(snapshotRetention); err != nil {
		setupLog.Error(err, "invalid snapshot retention flag")
		os.Exit(1)
	}

	if err := controller.ValidateOrphanSnapshotSweep(orphanSweepInterval, orphanMinAge, snapshotRetention); err != nil {
		setupLog.Error(err, "invalid orphan snapshot sweep flags")
		os.Exit(1)
	}

	if err := controller.ValidateRestorePlacementDeadline(placementDeadline, restoreStabilization); err != nil {
		setupLog.Error(err, "invalid restore placement deadline flag")
		os.Exit(1)
	}

	if err := controller.ValidateCoverageRefreshInterval(coverageRefreshInterval); err != nil {
		setupLog.Error(err, "invalid coverage refresh interval flag")
		os.Exit(1)
	}

	restConfig := ctrl.GetConfigOrDie()
	restConfig.QPS = float32(clientGoQPS)
	restConfig.Burst = clientGoBurst

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		Scheme:                        scheme,
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                enableLeaderElection,
		LeaderElectionID:              "pod-migration-controller-leader",
		LeaderElectionReleaseOnCancel: true,
		// The client keeps reading unstructured objects (the engines' triggers
		// and PodSnapshots) from the API server: the engines' state machines
		// need fresh reads, and a cached read of a kind the controller doesn't
		// watch would start an informer that blocks until it syncs. Invariant
		// evaluation reads the kinds the PodMigrationJob reconciler watches
		// from this cache instead (#199).
		Cache: cache.Options{
			DefaultTransform: cache.TransformStripManagedFields(),
			ByObject: map[client.Object]cache.ByObject{
				&corev1.Node{}: {
					Transform: func(obj interface{}) (interface{}, error) {
						node, ok := obj.(*corev1.Node)
						if !ok {
							return obj, nil
						}
						// Strip heavy cached image metadata. Keep labels and
						// status.nodeInfo: the node preemption controller reads
						// node state, and the PodMigrationJob reconciler records
						// the source node's instance-type label and kubelet
						// version (#14). The pod gate's placement check also
						// reads labels, spec.unschedulable, spec.taints and the
						// Ready condition.
						node.Status.Images = nil
						node.ManagedFields = nil
						return node, nil
					},
				},
			},
		},
		// Webhook server is on :9443 by default.
		WebhookServer: webhook.NewServer(webhook.Options{Port: 9443}),
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Cache indexes must be registered before any controller starts:
	// 1. Pods by assigned PMJ (used by gate mapper, collision resolution, and admission assignment)
	// 2. Pods by spec.nodeName (used by node preemption reconciler)
	// 3. VolumeAttachments by persistent volume name (used by PMJ detachment wait)
	// 4. PodMigrationJobs by parent workload key (used by replacement webhook admission)
	// 5. PodMigrationJobs by snapshot reference (used by cleanup and snapshot watcher)
	if err := controller.RegisterFieldIndexes(context.Background(), mgr.GetFieldIndexer()); err != nil {
		setupLog.Error(err, "unable to register field indexes")
		os.Exit(1)
	}

	reconcilerOpts := ctrlruntime.Options{
		MaxConcurrentReconciles: maxConcurrent,
	}

	eventRecorder := mgr.GetEventRecorderFor("pod-migration-controller")
	invariantEngine := invariants.NewEngine(invariantMode, eventRecorder)

	// Snapshot engines, shared by every reconciler and the eviction/replacement
	// webhooks. GKE Pod Snapshots is the default engine; CRIU is registered only
	// when explicitly requested (--criu-api-group set or --runtime-class-engines
	// routes a class to criu), keeping stock GKE-only installs unchanged.
	registerCRIU := shouldRegisterCRIU(criuGroupFlagSet, runtimeClassRoutes)
	if registerCRIU && criuAPIGroup != engine.DefaultAPIGroup && criuAPIGroup != engine.CRIUAPIGroup {
		setupLog.Info("WARNING: custom --criu-api-group is not covered by shipped ClusterRole RBAC; add get/list/watch/create/update/patch/delete rules for that group in pod-migration-controller-role",
			"criuAPIGroup", criuAPIGroup)
	}
	registryOpts := []engine.GKEOption{
		// The restore verifier reads the replacement's events, which aren't cached.
		engine.WithAPIReader(mgr.GetAPIReader()),
	}
	if registerCRIU {
		registryOpts = append(registryOpts, engine.WithCRIUAPIGroup(criuAPIGroup))
	}
	engines, err := engine.NewDefaultRegistry(mgr.GetClient(), mgr.GetScheme(), runtimeClassPolicy, registryOpts...)
	if err != nil {
		setupLog.Error(err, "unable to build the snapshot engine registry")
		os.Exit(1)
	}
	if len(runtimeClassRoutes) > 0 {
		if err := engines.SetRoutes(runtimeClassRoutes); err != nil {
			setupLog.Error(err, "invalid --runtime-class-engines flag")
			os.Exit(1)
		}
	}
	logEngineRoutes(engines, runtimeClassPolicy, len(runtimeClassRoutes) == 0)

	if err := (&controller.PodMigrationReconciler{
		Client:                 mgr.GetClient(),
		Scheme:                 mgr.GetScheme(),
		Recorder:               eventRecorder,
		InvariantEngine:        invariantEngine,
		Engines:                engines,
		RuntimeClassPolicy:     runtimeClassPolicy,
		StorageIsolationMode:   storageIsolationMode,
		AllowBucketRootStorage: allowBucketRootStorage,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodMigrationReconciler")
		os.Exit(1)
	}
	if err := (&controller.PodMigrationJobReconciler{
		Client:                     mgr.GetClient(),
		APIReader:                  mgr.GetAPIReader(),
		Scheme:                     mgr.GetScheme(),
		Recorder:                   eventRecorder,
		DefaultMigrationTimeout:    migrationTimeout,
		RestoreStabilizationWindow: restoreStabilization,
		RestorePlacementDeadline:   placementDeadline,
		InvariantEngine:            invariantEngine,
		BudgetChecker: &controller.InFlightBudgetChecker{
			Client: mgr.GetClient(),
			DryRun: controller.EvictionBudgetChecker{Client: mgr.GetClient()},
		},
		Engines:               engines,
		FencePreCheckpointCap: fenceCap,
		FenceLateStopWatch:    fenceLateStopWatch,
		SnapshotRetention:     snapshotRetention,
	}).SetupWithManager(mgr, reconcilerOpts); err != nil {
		setupLog.Error(err, "unable to create PodMigrationJobReconciler")
		os.Exit(1)
	}

	// PodGateReconciler hardcodes MaxConcurrentReconciles: 1 in its own
	// SetupWithManager — see the comment there for the serialization contract.
	if err := (&controller.PodGateReconciler{
		Client:           mgr.GetClient(),
		APIReader:        mgr.GetAPIReader(),
		Scheme:           mgr.GetScheme(),
		Recorder:         eventRecorder,
		InvariantEngine:  invariantEngine,
		Engines:          engines,
		RestorePlacement: restorePlacement,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create PodGateReconciler")
		os.Exit(1)
	}

	if err := (&controller.NodePreemptionReconciler{
		Client:                  mgr.GetClient(),
		APIReader:               mgr.GetAPIReader(),
		Scheme:                  mgr.GetScheme(),
		Recorder:                eventRecorder,
		DefaultMigrationTimeout: migrationTimeout,
		SpotPreemptionBudget:    spotPreemptionBudget,
		RuntimeClassPolicy:      runtimeClassPolicy,
		Engines:                 engines,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create NodePreemptionReconciler")
		os.Exit(1)
	}

	// Orphaned PodSnapshots whose job is gone (#107). Leader only.
	if orphanSweepInterval > 0 {
		if err := mgr.Add(&controller.OrphanSnapshotSweeper{
			Client:    mgr.GetClient(),
			APIReader: mgr.GetAPIReader(),
			Recorder:  eventRecorder,
			Groups:    engines.APIGroups(),
			Interval:  orphanSweepInterval,
			MinAge:    orphanMinAge,
		}); err != nil {
			setupLog.Error(err, "unable to add the orphan PodSnapshot sweep")
			os.Exit(1)
		}
	}

	// Periodic migratable coverage refresh without full reconciliation overhead (#253). Leader only.
	if coverageRefreshInterval > 0 {
		if err := mgr.Add(&controller.CoverageRefresher{
			Client:             mgr.GetClient(),
			Engines:            engines,
			RuntimeClassPolicy: runtimeClassPolicy,
			Interval:           coverageRefreshInterval,
		}); err != nil {
			setupLog.Error(err, "unable to add the coverage refresher")
			os.Exit(1)
		}
	}

	// --- Webhooks ------------------------------------------------------------
	if err := pmwebhook.SetupEvictionWebhookWithManager(mgr, mgr.GetAPIReader(), migrationTimeout, runtimeClassPolicy, engines); err != nil {
		setupLog.Error(err, "unable to register eviction webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupReplacementWebhookWithManager(mgr, mgr.GetAPIReader(), shortenReadinessDelay, engines); err != nil {
		setupLog.Error(err, "unable to register replacement mutating webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupStatusWebhookWithManager(mgr, mgr.GetAPIReader()); err != nil {
		setupLog.Error(err, "unable to register pod status mutating webhook")
		os.Exit(1)
	}
	if err := pmwebhook.SetupFenceWebhookWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to register pod fence mutating webhook")
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

func validateCRIUAPIGroup(group string) error {
	return engine.ValidateAPIGroup(group)
}

func shouldRegisterCRIU(criuGroupFlagSet bool, routes engine.Routes) bool {
	return criuGroupFlagSet || routes.NamesEngine(engine.CRIUName)
}

// logEngineRoutes logs the snapshot engines and the routes from runtime
// classes to engines, which come from the allow-list when fromAllowList is
// set. When --runtime-class-engines sets them, it also reports the
// allow-listed classes that no route serves, and the routes for classes the
// allow-list doesn't migrate.
func logEngineRoutes(engines *engine.Registry, policy *eligibility.RuntimeClassPolicy, fromAllowList bool) {
	routes := engines.Routes()
	setupLog.Info("Snapshot engines",
		"engines", engines.Names(),
		"apiGroups", engines.APIGroups(),
		"runtimeClassRoutes", routes.String(),
		"routesFromMigratableRuntimeClasses", fromAllowList)
	if fromAllowList {
		return
	}
	allowed := map[string]bool{}
	for _, class := range policy.Classes() {
		allowed[class] = true
		if _, ok := routes[class]; !ok {
			setupLog.Info("No --runtime-class-engines route serves a migratable runtime class: its pods are migrated only in namespaces whose PodMigration routes it",
				"runtimeClass", class)
		}
	}
	for _, class := range slices.Sorted(maps.Keys(routes)) {
		if !allowed[class] {
			setupLog.Info("A --runtime-class-engines route has no effect: --migratable-runtime-classes doesn't migrate its runtime class",
				"runtimeClass", class, "engine", routes[class])
		}
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
