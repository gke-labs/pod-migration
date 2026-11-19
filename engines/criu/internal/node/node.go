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
	"k8s.io/client-go/util/retry"

	"context"
	"fmt"
	"os"
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

	SocketPath string
	Inspector  *ContainerdReadinessInspector

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
	r := n.APIReader
	if r == nil {
		r = n.Client
	}
	var nodeObj corev1.Node
	if err := retry.OnError(retry.DefaultBackoff, func(err error) bool { return true }, func() error {
		return r.Get(ctx, types.NamespacedName{Name: n.NodeName}, &nodeObj)
	}); err != nil {
		return err
	}
	if err := CheckContainerdVersion(nodeObj.Status.NodeInfo.ContainerRuntimeVersion); err != nil {
		_ = annotateNode(ctx, n.Client, n.APIReader, n.NodeName, "", "unsupported containerd version: "+err.Error(), "upgrade containerd", false)
		return err
	}
	version, err := installCRIUBundle()
	if err != nil {
		return err
	}
	ok, raw := verifyCRIU(ctx, n.Restorer.execHost)
	log.Info("criu capability", "version", version, "checkAll", ok, "raw", truncate(raw, 200))

	var reason, remediation string
	if !ok {
		reason = "criu check --all failed"
		remediation = "check host kernel support for CRIU"
	} else {
		// Kernel check passed; verify containerd readiness so an unready containerd
		// is never sent a checkpoint that would poison its lookup cache (#130).
		insp := n.Inspector
		if insp == nil {
			var execHost func(context.Context, []string, string, ...string) (string, error)
			if n.Restorer != nil {
				execHost = n.Restorer.execHost
			}
			insp = NewDefaultContainerdInspector(execHost)
		}
		cOk, cReason, cRemediation, cErr := CheckContainerdReadiness(insp, n.SocketPath)
		if cErr != nil {
			log.Error(cErr, "containerd readiness check error")
			ok = false
			reason = "containerd readiness probe error: " + cErr.Error()
			remediation = "verify containerd is running and accessible"
		} else if !cOk {
			ok = false
			reason = cReason
			remediation = cRemediation
		}
	}

	n.mu.Lock()
	n.version = version
	n.ready = ok
	n.recordSocketInodeLocked()
	n.mu.Unlock()

	return annotateNode(ctx, n.Client, n.APIReader, n.NodeName, version, reason, remediation, ok)
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

	return annotateNode(ctx, n.Client, n.APIReader, n.NodeName, ver, reason, remediation, false)
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
func (n *Node) RunProbeLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := n.ProbeReadiness(ctx); err != nil {
				logf.FromContext(ctx).Error(err, "readiness re-probe failed", "node", n.NodeName)
			}
		}
	}
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
