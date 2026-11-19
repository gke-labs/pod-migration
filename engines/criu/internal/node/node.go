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

// Package node is the node-side worker (privileged DaemonSet) for the CRIU
// snapshot engine. It owns node-level operations: CRIU bundle installation,
// capability annotation, kubelet checkpoint execution, and sandbox-sibling
// container restore.
package node

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// Node holds the node-local clients (containerd, kubelet checkpoint API, and
// runc restorer) and runs the startup CRIU bundle installation and node
// capability probe.
type Node struct {
	client.Client
	APIReader client.Reader
	NodeName  string

	Kubelet  *kubeletCheckpoint
	Restorer *restorer

	SocketPath    string
	SweepInterval time.Duration // default 5m

	mu                 sync.Mutex
	ready              bool
	version            string
	lastSocketInode    uint64
	unreadyReason      string
	unreadyRemediation string

	statSocketFn func(string) (uint64, error)
	bootstrapFn  func(context.Context) error
}

// NewNode wires the node worker against containerd and the local kubelet
// checkpoint endpoint.
func NewNode(c client.Client, nodeName string) (*Node, error) {
	cc, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	return &Node{
		Client:       c,
		NodeName:     nodeName,
		Kubelet:      newKubeletCheckpoint(),
		Restorer:     newRestorer(cc),
		SocketPath:   "/run/containerd/containerd.sock",
		statSocketFn: getSocketInode,
	}, nil
}

func getSocketInode(path string) (uint64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, fmt.Errorf("unexpected FileInfo.Sys() type %T", fi.Sys())
	}
	return stat.Ino, nil
}

// Bootstrap installs the CRIU bundle, verifies it against the host kernel, and
// annotates the node with its capability verdict.
func (n *Node) Bootstrap(ctx context.Context) error {
	n.recordSocketInode()
	if n.bootstrapFn != nil {
		return n.bootstrapFn(ctx)
	}
	return n.bootstrapInternal(ctx)
}

func (n *Node) recordSocketInode() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.recordSocketInodeLocked()
}

func (n *Node) recordSocketInodeLocked() {
	if n.statSocketFn != nil && n.SocketPath != "" {
		if ino, err := n.statSocketFn(n.SocketPath); err == nil {
			n.lastSocketInode = ino
		}
	}
}

func (n *Node) bootstrapInternal(ctx context.Context) error {
	log := logf.FromContext(ctx)
	var nodeObj corev1.Node
	if err := n.Client.Get(ctx, types.NamespacedName{Name: n.NodeName}, &nodeObj); err != nil {
		return err
	}
	if err := CheckContainerdVersion(nodeObj.Status.NodeInfo.ContainerRuntimeVersion); err != nil {
		_ = annotateNode(ctx, n.Client, n.NodeName, "", "unsupported containerd version: "+err.Error(), "upgrade containerd", false)
		return err
	}
	version, err := installCRIUBundle()
	if err != nil {
		return err
	}
	ok, raw := verifyCRIU(ctx, n.Restorer.execHost)
	log.Info("criu capability", "version", version, "checkAll", ok, "raw", truncate(raw, 200))

	n.mu.Lock()
	n.version = version
	n.ready = ok
	n.recordSocketInodeLocked()
	n.mu.Unlock()

	var reason, remediation string
	if !ok {
		reason = "criu check --all failed"
		remediation = "check host kernel support for CRIU"
	}
	return annotateNode(ctx, n.Client, n.NodeName, version, reason, remediation, ok)
}

// DemoteNode marks the node unready with reason and remediation, preserving existing version.
func (n *Node) DemoteNode(ctx context.Context, reason, remediation string) error {
	n.mu.Lock()
	n.ready = false
	n.unreadyReason = reason
	n.unreadyRemediation = remediation
	n.recordSocketInodeLocked()
	ver := n.version
	n.mu.Unlock()

	return annotateNode(ctx, n.Client, n.NodeName, ver, reason, remediation, false)
}

// IsReady returns whether the node is currently considered CRIU-ready.
func (n *Node) IsReady() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ready
}

// ProbeReadiness checks whether conditions have changed to restore node readiness.
// If the node is currently unready, it checks whether containerd was restarted
// (new socket inode). If a new socket inode is detected, it re-runs Bootstrap
// to re-verify CRIU and restore node readiness.
func (n *Node) ProbeReadiness(ctx context.Context) (bool, error) {
	n.mu.Lock()
	isReady := n.ready
	lastIno := n.lastSocketInode
	statFn := n.statSocketFn
	sockPath := n.SocketPath
	n.mu.Unlock()

	if isReady {
		return true, nil
	}

	if statFn == nil || sockPath == "" {
		return false, nil
	}

	currIno, err := statFn(sockPath)
	if err != nil {
		// Socket may be temporarily missing/unreachable during containerd restart
		return false, nil
	}

	if currIno != 0 && currIno != lastIno {
		logf.FromContext(ctx).Info("containerd restart detected, re-probing CRIU readiness",
			"oldSocketInode", lastIno, "newSocketInode", currIno)
		if err := n.Bootstrap(ctx); err != nil {
			return false, err
		}
		return n.IsReady(), nil
	}

	return false, nil
}

// RunProbeLoop runs periodic readiness probes while ctx is active.
// Orphan restore sweeps run on their own longer interval (default 5m).
func (n *Node) RunProbeLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	sweepInterval := n.SweepInterval
	if sweepInterval <= 0 {
		sweepInterval = 5 * time.Minute
	}
	sweepTicker := time.NewTicker(sweepInterval)
	defer sweepTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := n.ProbeReadiness(ctx); err != nil {
				logf.FromContext(ctx).Error(err, "readiness re-probe failed", "node", n.NodeName)
			}
		case <-sweepTicker.C:
			if err := n.SweepOrphanRestores(ctx); err != nil {
				logf.FromContext(ctx).Error(err, "periodic orphan restore sweep failed", "node", n.NodeName)
			}
		}
	}
}

// SweepOrphanRestores lists pods on this node from the API reader and cleans up
// any leftover CRIU restore containers, mounts, snapshots, or leases whose pods
// are gone or terminating.
func (n *Node) SweepOrphanRestores(ctx context.Context) error {
	sweepStart := time.Now()
	var podList corev1.PodList
	reader := n.APIReader
	if reader == nil {
		reader = n.Client
	}
	if reader == nil {
		return nil
	}
	var listOpts []client.ListOption
	if n.NodeName != "" {
		listOpts = append(listOpts, client.MatchingFields{"spec.nodeName": n.NodeName})
	}
	if err := reader.List(ctx, &podList, listOpts...); err != nil {
		return fmt.Errorf("list pods on node %s: %w", n.NodeName, err)
	}
	livingUIDs := make(map[string]bool)
	livingSnaps := make(map[string]bool)
	now := time.Now()
	for _, p := range podList.Items {
		if p.Spec.NodeName != n.NodeName && n.NodeName != "" {
			continue
		}
		// Count terminating pods as alive until their grace deadline has elapsed.
		alive := p.DeletionTimestamp == nil
		if !alive && p.DeletionGracePeriodSeconds != nil && *p.DeletionGracePeriodSeconds > 0 {
			if now.Before(p.DeletionTimestamp.Time) {
				alive = true
			}
		}
		if alive {
			livingUIDs[string(p.UID)] = true
			for k, v := range p.Annotations {
				if strings.HasSuffix(k, "/ps-name") && v != "" {
					livingSnaps[v] = true
				}
			}
		}
	}
	if n.Restorer == nil {
		return nil
	}
	swept, err := n.Restorer.SweepOrphans(ctx, livingUIDs, livingSnaps, sweepStart)
	if len(swept) > 0 {
		logf.FromContext(ctx).Info("swept orphan restore artifacts", "count", len(swept), "migrations", swept)
	}
	return err
}

func joinNotes(notes []string) string {
	s := ""
	for i, n := range notes {
		if i > 0 {
			s += " ; "
		}
		s += n
	}
	return s
}
