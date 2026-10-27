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

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/gke-labs/pod-migration/engines/criu/internal/node"
	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var probeAddr string
	var kubeAPIQPS float64
	var kubeAPIBurst int
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8091", "health probe bind address (hostNetwork)")
	flag.Float64Var(&kubeAPIQPS, "kube-api-qps", 20, "Kubernetes API client QPS limit")
	flag.IntVar(&kubeAPIBurst, "kube-api-burst", 40, "Kubernetes API client burst limit")
	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	nodeName := os.Getenv("NODE_NAME")
	if nodeName == "" {
		log.Error(nil, "NODE_NAME env (downward API spec.nodeName) is required")
		os.Exit(1)
	}

	cfg := ctrl.GetConfigOrDie()
	cfg.QPS = float32(kubeAPIQPS)
	cfg.Burst = kubeAPIBurst
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	nd, err := node.NewNode(mgr.GetClient(), nodeName)
	if err != nil {
		log.Error(err, "unable to init node worker")
		os.Exit(1)
	}

	// Run CRIU bundle installation + node capability annotation once the cache starts.
	if err := mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		if !mgr.GetCache().WaitForCacheSync(ctx) {
			return nil
		}
		if err := nd.Bootstrap(ctx); err != nil {
			log.Error(err, "CRIU bootstrap failed (continuing; node remains unready)")
		}
		return nil
	})); err != nil {
		log.Error(err, "unable to register bootstrap runnable")
		os.Exit(1)
	}

	store := psengine.NewGCSStore()
	ops := &nodeOpsAdapter{n: nd}
	cpuFam := node.PSEngineCPUFamily()
	if err := (&psengine.NodeSnapshotReconciler{
		Client: mgr.GetClient(), NodeName: nodeName,
		Ops: ops, Store: store, CPUFamily: cpuFam,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine node snapshot reconciler")
		os.Exit(1)
	}
	if err := (&psengine.NodeRestoreReconciler{
		Client: mgr.GetClient(), NodeName: nodeName,
		Ops: ops, Store: store, CPUFamily: cpuFam,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up psengine node restore reconciler")
		os.Exit(1)
	}
	log.Info("psengine node pipelines enabled", "node", nodeName, "cpuFamily", cpuFam)

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		os.Exit(1)
	}
	log.Info("starting criu-engine node agent", "node", nodeName)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "agent exited")
		os.Exit(1)
	}
}

// nodeOpsAdapter bridges psengine.{NodeOps,RestoreOps} to node.Node's
// exported PSEngine* methods without introducing a node->psengine import.
type nodeOpsAdapter struct{ n *node.Node }

func (a *nodeOpsAdapter) Checkpoint(ctx context.Context, ns, pod, container string) (string, error) {
	return a.n.PSEngineCheckpoint(ctx, ns, pod, container)
}

func (a *nodeOpsAdapter) SourceInfo(ctx context.Context, ns, pod, container string) ([]byte, string, error) {
	return a.n.PSEngineSourceInfo(ctx, ns, pod, container)
}

func (a *nodeOpsAdapter) StopContainer(ctx context.Context, ns, pod, container string) error {
	return a.n.PSEngineStopContainer(ctx, ns, pod, container)
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
