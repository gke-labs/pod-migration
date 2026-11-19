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
	"errors"
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
	NodeName string

	Kubelet  *kubeletCheckpoint
	Restorer *restorer

	SocketPath string

	// annotateMu serializes Node annotation GET/PATCH calls so concurrent
	// Bootstrap and DemoteNode writes never land out of order on the API server.
	annotateMu sync.Mutex

	mu                 sync.Mutex
	ready              bool
	version            string
	lastSocketInode    uint64
	poisonedInode      uint64
	demoteSeq          uint64
	demoteInode        uint64
	pendingDemote      bool
	unreadyReason      string
	unreadyRemediation string

	statSocketFn    func(string) (uint64, error)
	installBundleFn func() (string, error)
	execHostFn      func(context.Context, []string, string, ...string) (string, error)
	bootstrapFn     func(context.Context) error
}

// NewNode wires the node worker against containerd and the local kubelet
// checkpoint endpoint.
func NewNode(c client.Client, nodeName string) (*Node, error) {
	cc, err := containerd.New("/run/containerd/containerd.sock",
		containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return nil, fmt.Errorf("containerd client: %w", err)
	}
	r := newRestorer(cc)
	return &Node{
		Client:          c,
		NodeName:        nodeName,
		Kubelet:         newKubeletCheckpoint(),
		Restorer:        r,
		SocketPath:      "/run/containerd/containerd.sock",
		statSocketFn:    getSocketInode,
		installBundleFn: installCRIUBundle,
		execHostFn:      r.execHost,
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
// annotates the node with its capability verdict. In-memory state and
// lastSocketInode are committed only once the Node annotation is persisted, so
// deterministic host failures stay quiet while transient API errors retry on
// the next ProbeReadiness tick.
func (n *Node) Bootstrap(ctx context.Context) error {
	n.mu.Lock()
	startDemoteSeq := n.demoteSeq
	n.mu.Unlock()
	startIno, _ := n.statCurrentSocket()
	if n.bootstrapFn != nil {
		if err := n.bootstrapFn(ctx); err != nil {
			return err
		}
		currIno, hasCurr := n.statCurrentSocket()
		n.mu.Lock()
		if n.demoteSeq == startDemoteSeq && (!hasCurr || currIno == 0 || startIno == 0 || currIno == startIno) {
			if startIno != 0 {
				n.lastSocketInode = startIno
			}
			if n.ready {
				n.poisonedInode = 0
				n.pendingDemote = false
			}
		}
		n.mu.Unlock()
		return nil
	}
	return n.bootstrapInternal(ctx, startIno, startDemoteSeq)
}

func (n *Node) statCurrentSocket() (uint64, bool) {
	n.mu.Lock()
	statFn := n.statSocketFn
	sockPath := n.SocketPath
	n.mu.Unlock()
	if statFn == nil || sockPath == "" {
		return 0, false
	}
	ino, err := statFn(sockPath)
	if err != nil {
		return 0, false
	}
	return ino, true
}

func (n *Node) isPoisonedInode(ino uint64) bool {
	if ino == 0 {
		return false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	return ino == n.poisonedInode
}

type nodeVerdict struct {
	ready          bool
	version        string
	reason         string
	remediation    string
	ino            uint64
	startDemoteSeq uint64
}

func isStaleDemote(bootIno, demoteIno, currIno uint64, hasCurr bool) bool {
	return bootIno != 0 && demoteIno != 0 && demoteIno != bootIno && hasCurr && currIno == bootIno
}

func (n *Node) publish(ctx context.Context, v nodeVerdict) error {
	n.annotateMu.Lock()
	defer n.annotateMu.Unlock()

	currIno, hasCurr := n.statCurrentSocket()
	if v.ino != 0 && hasCurr && currIno != 0 && currIno != v.ino {
		return fmt.Errorf("containerd socket inode changed during bootstrap (%d -> %d); deferring to next probe", v.ino, currIno)
	}

	n.mu.Lock()
	poisoned := v.ready && v.ino != 0 && v.ino == n.poisonedInode
	concurrentDemote := v.ready && n.demoteSeq != v.startDemoteSeq && !isStaleDemote(v.ino, n.demoteInode, currIno, hasCurr)
	demoteReason := n.unreadyReason
	demoteRemediation := n.unreadyRemediation
	prePatchDemoteSeq := n.demoteSeq
	n.mu.Unlock()

	if poisoned {
		v.ready = false
		v.reason = ReasonContainerdCachedCRIU
		v.remediation = RemediationRestartContainerd
	} else if concurrentDemote {
		v.ready = false
		v.reason = demoteReason
		v.remediation = demoteRemediation
	}

	if err := annotateNode(ctx, n.Client, n.NodeName, v.version, v.reason, v.remediation, v.ready); err != nil {
		return err
	}

	postCurrIno, hasPostCurr := n.statCurrentSocket()
	n.mu.Lock()
	defer n.mu.Unlock()
	if v.version != "" {
		n.version = v.version
	}
	if n.demoteSeq != prePatchDemoteSeq && !isStaleDemote(v.ino, n.demoteInode, postCurrIno, hasPostCurr) {
		if v.ino != 0 && n.lastSocketInode == 0 {
			n.lastSocketInode = v.ino
		}
		if v.ino != 0 && v.ino == n.poisonedInode {
			return ErrContainerdCachedCRIU
		}
		return nil
	}
	n.ready = v.ready
	if v.ino != 0 && (!hasPostCurr || postCurrIno == 0 || postCurrIno == v.ino) {
		n.lastSocketInode = v.ino
	}
	if v.ready {
		n.poisonedInode = 0
		n.unreadyReason = ""
		n.unreadyRemediation = ""
	} else {
		n.unreadyReason = v.reason
		n.unreadyRemediation = v.remediation
	}
	n.pendingDemote = false
	if poisoned {
		return ErrContainerdCachedCRIU
	}
	return nil
}

func (n *Node) bootstrapInternal(ctx context.Context, startIno, startDemoteSeq uint64) error {
	log := logf.FromContext(ctx)

	getCtx, cancel := context.WithTimeout(ctx, nodeAPITimeout)
	var nodeObj corev1.Node
	err := n.Client.Get(getCtx, types.NamespacedName{Name: n.NodeName}, &nodeObj)
	cancel()
	if err != nil {
		return err
	}
	if err := CheckContainerdVersion(nodeObj.Status.NodeInfo.ContainerRuntimeVersion); err != nil {
		pubErr := n.publish(ctx, nodeVerdict{
			ready:          false,
			reason:         "unsupported containerd version: " + err.Error(),
			remediation:    "upgrade containerd",
			ino:            startIno,
			startDemoteSeq: startDemoteSeq,
		})
		return errors.Join(err, pubErr)
	}
	installFn := n.installBundleFn
	if installFn == nil {
		installFn = installCRIUBundle
	}
	version, err := installFn()
	if err != nil {
		pubErr := n.publish(ctx, nodeVerdict{
			ready:          false,
			reason:         "criu bundle install failed: " + err.Error(),
			remediation:    "check node disk space and writable host paths, then restart pod-migrate-agent",
			ino:            startIno,
			startDemoteSeq: startDemoteSeq,
		})
		return errors.Join(err, pubErr)
	}
	if n.isPoisonedInode(startIno) {
		pubErr := n.publish(ctx, nodeVerdict{
			ready:          false,
			version:        version,
			reason:         ReasonContainerdCachedCRIU,
			remediation:    RemediationRestartContainerd,
			ino:            startIno,
			startDemoteSeq: startDemoteSeq,
		})
		return errors.Join(ErrContainerdCachedCRIU, pubErr)
	}
	execFn := n.execHostFn
	if execFn == nil && n.Restorer != nil {
		execFn = n.Restorer.execHost
	}
	if execFn == nil {
		return fmt.Errorf("host command runner is not configured")
	}
	ok, raw := verifyCRIU(ctx, execFn)
	log.Info("criu capability", "version", version, "checkAll", ok, "raw", truncate(raw, 200))

	var reason, remediation string
	if !ok {
		reason = "criu check --all failed"
		remediation = "check host kernel support for CRIU"
	}
	return n.publish(ctx, nodeVerdict{
		ready:          ok,
		version:        version,
		reason:         reason,
		remediation:    remediation,
		ino:            startIno,
		startDemoteSeq: startDemoteSeq,
	})
}

// DemoteNode marks the node unready with reason and remediation, preserving existing version.
func (n *Node) DemoteNode(ctx context.Context, reason, remediation string) error {
	ino, _ := n.statCurrentSocket()
	return n.demoteNodeAtInode(ctx, reason, remediation, ino)
}

func (n *Node) demoteNodeAtInode(ctx context.Context, reason, remediation string, ino uint64) error {
	currIno, hasCurr := n.statCurrentSocket()
	n.mu.Lock()
	if ino != 0 && hasCurr && currIno != 0 && currIno != ino && n.ready && n.lastSocketInode == currIno {
		n.mu.Unlock()
		return nil
	}
	n.ready = false
	n.unreadyReason = reason
	n.unreadyRemediation = remediation
	n.demoteSeq++
	seq := n.demoteSeq
	n.demoteInode = ino
	n.pendingDemote = true
	if ino != 0 {
		n.lastSocketInode = ino
		if reason == ReasonContainerdCachedCRIU {
			n.poisonedInode = ino
		}
	}
	n.mu.Unlock()

	n.annotateMu.Lock()
	defer n.annotateMu.Unlock()
	n.mu.Lock()
	if n.ready || n.demoteSeq != seq {
		n.mu.Unlock()
		return nil
	}
	ver := n.version
	n.mu.Unlock()
	if err := annotateNode(ctx, n.Client, n.NodeName, ver, reason, remediation, false); err != nil {
		return err
	}
	n.mu.Lock()
	if !n.ready && n.demoteSeq == seq {
		n.pendingDemote = false
	}
	n.mu.Unlock()
	return nil
}

// IsReady returns whether the node is currently considered CRIU-ready.
func (n *Node) IsReady() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ready
}

// ProbeReadiness checks whether conditions have changed to restore node readiness.
// If the node is currently unready, it checks whether containerd was restarted
// (new socket inode) or whether an earlier Bootstrap failed transiently before
// committing its socket inode, and retries any unwritten demotion annotation.
func (n *Node) ProbeReadiness(ctx context.Context) (bool, error) {
	n.mu.Lock()
	isReady := n.ready
	lastIno := n.lastSocketInode
	pendingDemote := n.pendingDemote
	seq := n.demoteSeq
	ver := n.version
	reason := n.unreadyReason
	remediation := n.unreadyRemediation
	n.mu.Unlock()

	if isReady {
		return true, nil
	}

	currIno, ok := n.statCurrentSocket()
	if ok && currIno != 0 && currIno != lastIno {
		logf.FromContext(ctx).Info("containerd restart or uncommitted bootstrap detected, probing CRIU readiness",
			"oldSocketInode", lastIno, "newSocketInode", currIno)
		if err := n.Bootstrap(ctx); err != nil {
			return false, err
		}
		return n.IsReady(), nil
	}

	if pendingDemote {
		n.annotateMu.Lock()
		defer n.annotateMu.Unlock()
		n.mu.Lock()
		stillPending := !n.ready && n.pendingDemote && n.demoteSeq == seq
		ver = n.version
		reason = n.unreadyReason
		remediation = n.unreadyRemediation
		n.mu.Unlock()
		if stillPending {
			if err := annotateNode(ctx, n.Client, n.NodeName, ver, reason, remediation, false); err != nil {
				return false, err
			}
			n.mu.Lock()
			if !n.ready && n.demoteSeq == seq {
				n.pendingDemote = false
			}
			n.mu.Unlock()
		}
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
