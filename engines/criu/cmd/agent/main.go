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
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/gke-labs/pod-migration/engines/criu/internal/node"
	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine"
	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var probeAddr string
	var apiGroup string
	var kubeAPIQPS float64
	var kubeAPIBurst int
	var archiveKeysDir, archiveKeyID string
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8091", "health probe bind address (hostNetwork)")
	flag.StringVar(&apiGroup, "api-group", contract.DefaultGroup, "API group of the snapshot engine CRDs; must match the engine controller's --api-group")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", 20, "Kubernetes API client QPS limit")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", 40, "Kubernetes API client burst limit")
	flag.StringVar(&archiveKeysDir, "archive-signing-keys-dir", psengine.DefaultArchiveSigningKeysDir,
		"directory of archive signing keys (a mounted Secret): each file is one key, named by its key ID (#244)")
	flag.StringVar(&archiveKeyID, "archive-signing-key-id", "",
		"key ID that signs new archive manifests; may be empty when the directory holds exactly one key")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	if err := contract.SetGroup(apiGroup); err != nil {
		log.Error(err, "invalid --api-group")
		os.Exit(1)
	}
	log.Info("serving the snapshot engine API", "group", contract.Group, "restoreAnnotation", contract.AnnotationPSName)

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Error(nil, "NODE_NAME env (downward API spec.nodeName) is required")
		os.Exit(1)
	}

	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = float32(kubeAPIQPS)
	cfg.Burst = kubeAPIBurst
	mgr, err := ctrl.NewManager(cfg, agentManagerOptions(probeAddr))
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	nd, err := node.NewNode(mgr.GetClient(), nodeName)
	if err != nil {
		log.Error(err, "unable to init node worker")
		os.Exit(1)
	}
	nd.APIReader = mgr.GetAPIReader()

	// Run CRIU bundle installation + node capability annotation once the cache starts,
	// run orphan restore sweep, and run a background probe loop to recover node readiness when containerd restarts.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return nil
		}
		runNodeWorker(ctx, nd)
		return nil
	})); err != nil {
		log.Error(err, "unable to register bootstrap runnable")
		os.Exit(1)
	}

	store := psengine.NewGCSStore()
	ops := &nodeOpsAdapter{n: nd}
	cpuFam := node.PSEngineCPUFamily()

	// Archive provenance (C10 R2, #244): the cluster UID every manifest
	// binds, and the keys that sign and verify manifests.
	ctx := ctrl.SetupSignalHandler()
	clusterUID, err := psengine.ReadClusterUID(ctx, mgr.GetAPIReader(), clusterUIDBackoff)
	if err != nil {
		log.Error(err, "can't read the cluster UID that archive manifests are bound to; the agent needs get on the kube-system Namespace")
		os.Exit(1)
	}
	keyring, signer := loadArchiveKeys(archiveKeysDir, archiveKeyID)

	if err := (&psengine.NodeSnapshotReconciler{
		Client: mgr.GetClient(), APIReader: mgr.GetAPIReader(), NodeName: nodeName,
		Ops: ops, Store: store, CPUFamily: cpuFam,
		SourceNodeFacts: nd.PSEngineSourceNodeFacts,
		Signer:          signer, ClusterUID: clusterUID,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine node snapshot reconciler")
		os.Exit(1)
	}
	if err := (&psengine.NodeRestoreReconciler{
		Client: mgr.GetClient(), NodeName: nodeName,
		Ops: ops, Store: store, CPUFamily: cpuFam,
		Recorder: mgr.GetEventRecorderFor("psengine-node-restore"),
		Keyring:  keyring, ClusterUID: clusterUID,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine node restore reconciler")
		os.Exit(1)
	}
	log.Info("psengine node pipelines enabled", "node", nodeName, "cpuFamily", cpuFam, "clusterUID", clusterUID)

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	log.Info("starting criu-engine node agent", "node", nodeName)
	if err := mgr.Start(ctx); err != nil {
		log.Error(err, "agent exited")
		os.Exit(1)
	}
}

// clusterUIDBackoff bounds how long the agent waits for the API server to
// return the cluster UID at startup before it exits: six attempts, with
// about 31 s of waits between them, each attempt bounded by
// psengine.ReadClusterUIDAttemptTimeout (5 s), so about a minute at most.
var clusterUIDBackoff = wait.Backoff{Steps: 6, Duration: time.Second, Factor: 2}

// loadArchiveKeys loads the archive signing keys from dir, and picks the one
// that signs (keyID, or the only key). It never fails the agent: without
// keys, every checkpoint fails before it freezes its source and every
// restore refuses its archive (cold start); without a signing key, only
// checkpoints fail. Either is logged as an error.
func loadArchiveKeys(dir, keyID string) (*psengine.Keyring, *psengine.Signer) {
	log := ctrl.Log.WithName("setup")
	keyring, err := psengine.LoadKeyring(dir)
	if err != nil {
		log.Error(err, "ARCHIVE SIGNING KEYS NOT LOADED: every checkpoint will fail before freezing its source, "+
			"and every restore will refuse its archive and cold-start; create the pod-migrate-archive-signing-keys Secret (see the engine's README)",
			"dir", dir)
		return nil, nil
	}
	signer, err := keyring.Signer(keyID)
	if err != nil {
		log.Error(err, "NO ACTIVE ARCHIVE SIGNING KEY: every checkpoint will fail before freezing its source; restores still verify with the loaded keys. "+
			"Set --archive-signing-key-id", "dir", dir, "keyIDs", keyring.KeyIDs())
		return keyring, nil
	}
	log.Info("archive signing keys loaded", "dir", dir, "keyIDs", keyring.KeyIDs(), "signingKeyID", signer.KeyID())
	return keyring, signer
}

// agentManagerOptions builds the controller-runtime manager options for the
// node agent. ClusterRole/pod-migrate-psengine-agent intentionally grants only
// get and patch on core/nodes (no list/watch), so *corev1.Node reads bypass
// the informer cache and talk directly to the API server.
// nodeWorker is what runNodeWorker runs: *node.Node.
type nodeWorker interface {
	Bootstrap(ctx context.Context) error
	RunNodeFactsLoop(ctx context.Context, interval, retry time.Duration)
	SweepOrphanRestores(ctx context.Context) error
	RunProbeLoop(ctx context.Context, interval time.Duration)
}

// runNodeWorker bootstraps the node, starts publishing its facts, sweeps
// orphan restores, then runs the readiness probe loop until ctx is done.
func runNodeWorker(ctx context.Context, nd nodeWorker) {
	log := ctrl.Log.WithName("setup")
	if err := nd.Bootstrap(ctx); err != nil {
		log.Error(err, "CRIU bootstrap failed (continuing; node remains unready)")
	}
	// Publish this node's facts as restore placement labels (#257) after
	// bootstrap, so the CRIU bundle's version is readable, and keep them
	// current. A failed bootstrap still publishes: a version the agent
	// can't read is published as unknown. Without this, checkpoints still
	// say placementLabels v1, and every constrained restore waits out the
	// restore ceiling for a node with the labels.
	go nd.RunNodeFactsLoop(ctx, node.DefaultNodeFactsInterval, node.DefaultNodeFactsRetry)
	if err := nd.SweepOrphanRestores(ctx); err != nil {
		log.Error(err, "orphan restore sweep failed (continuing)")
	}
	nd.RunProbeLoop(ctx, 10*time.Second)
}

func agentManagerOptions(probeAddr string) ctrl.Options {
	return ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: probeAddr,
		Client: client.Options{
			Cache: &client.CacheOptions{
				DisableFor: []client.Object{&corev1.Node{}},
			},
		},
	}
}

// nodeOpsAdapter bridges psengine.{NodeOps,RestoreOps} to node.Node's
// exported PSEngine* methods without introducing a node->psengine import.
type nodeOpsAdapter struct{ n *node.Node }

func (a *nodeOpsAdapter) Checkpoint(ctx context.Context, ns, pod, container string) (string, error) {
	return a.n.PSEngineCheckpoint(ctx, ns, pod, container)
}

func (a *nodeOpsAdapter) SourceInfo(ctx context.Context, ns, pod, podUID, container string) ([]byte, string, error) {
	return a.n.PSEngineSourceInfo(ctx, ns, pod, podUID, container)
}

func (a *nodeOpsAdapter) Freeze(ctx context.Context, ns, pod, podUID, container string) error {
	return a.n.PSEngineFreezeContainer(ctx, ns, pod, podUID, container)
}

func (a *nodeOpsAdapter) Unfreeze(ctx context.Context, ns, pod, podUID, container string) error {
	return a.n.PSEngineUnfreezeContainer(ctx, ns, pod, podUID, container)
}

func (a *nodeOpsAdapter) Extract(tarPath, dstDir string) error {
	return a.n.PSEngineExtract(tarPath, dstDir)
}

func (a *nodeOpsAdapter) Restore(ctx context.Context, in psengine.RestoreInputs) (int, int64, error) {
	return a.n.PSEngineRestore(ctx, node.PSEngineRestoreInputs{
		SnapshotName: in.SnapshotName, PodNamespace: in.PodNamespace, TargetPod: in.TargetPod,
		ContainerName: in.ContainerName, SourcePodUID: in.SourcePodUID, TargetPodUID: in.TargetPodUID,
		CheckpointDir: in.CheckpointDir, SpecJSON: in.SpecJSON, ImageName: in.ImageName,
	})
}

func (a *nodeOpsAdapter) Cleanup(ctx context.Context, in psengine.RestoreInputs) error {
	return a.n.PSEngineCleanup(ctx, in.SnapshotName)
}

func (a *nodeOpsAdapter) Signal(ctx context.Context, in psengine.RestoreInputs, sig int) error {
	return a.n.PSEngineSignal(ctx, in.SnapshotName, sig)
}

func (a *nodeOpsAdapter) IsSiblingRunning(ctx context.Context, in psengine.RestoreInputs) (bool, error) {
	return a.n.PSEngineIsSiblingRunning(ctx, in.SnapshotName)
}

func (a *nodeOpsAdapter) SignalHolder(ctx context.Context, in psengine.RestoreInputs, sig int) error {
	return a.n.PSEngineSignalHolder(ctx, in.PodNamespace, in.TargetPod, in.TargetPodUID, in.ContainerName, sig)
}
