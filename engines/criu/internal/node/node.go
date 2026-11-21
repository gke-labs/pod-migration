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
	"strings"
	"sync"
	"syscall"
	"time"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
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
	Inspector     *ContainerdReadinessInspector

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
	pendingVerdict     *nodeVerdict
	unreadyReason      string
	unreadyRemediation string
	transientReadiness bool
	transientIno       uint64
	transientDemoteSeq uint64

	statSocketFn    func(string) (uint64, error)
	installBundleFn func() (string, error)
	execHostFn      func(context.Context, []string, string, ...string) (string, error)
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
		Inspector:       NewDefaultContainerdInspector(r.execHost),
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
// annotates the node with its capability verdict. If the final Node annotation
// write fails, the computed verdict is buffered in pendingVerdict so subsequent
// ProbeReadiness ticks retry only the annotation write without re-installing
// the bundle or re-running `criu check --all`.
func (n *Node) Bootstrap(ctx context.Context) error {
	startIno, _ := n.statCurrentSocket()
	n.mu.Lock()
	startDemoteSeq := n.demoteSeq
	n.transientReadiness = false
	n.transientIno = 0
	if startIno != 0 && n.pendingVerdict != nil && n.pendingVerdict.ino != startIno {
		n.pendingVerdict = nil
	}
	n.mu.Unlock()
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
		n.mu.Lock()
		if n.pendingVerdict != nil && n.pendingVerdict.ino == v.ino {
			n.pendingVerdict = nil
		}
		n.mu.Unlock()
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
		postCurrIno, hasPostCurr := n.statCurrentSocket()
		n.mu.Lock()
		if v.version != "" {
			n.version = v.version
		}
		socketMoved := hasPostCurr && postCurrIno != 0 && v.ino != 0 && postCurrIno != v.ino
		midPatchDemote := n.demoteSeq != prePatchDemoteSeq && !isStaleDemote(v.ino, n.demoteInode, postCurrIno, hasPostCurr)
		if socketMoved {
			n.pendingVerdict = nil
		} else if midPatchDemote {
			if n.demoteInode != 0 {
				n.pendingVerdict = nil
			} else if ctx.Err() == nil && v.ino != 0 {
				cp := v
				cp.ready = false
				cp.reason = n.unreadyReason
				cp.remediation = n.unreadyRemediation
				cp.startDemoteSeq = n.demoteSeq
				n.pendingVerdict = &cp
			}
		} else if ctx.Err() == nil && (!concurrentDemote || n.demoteInode == 0) {
			cp := v
			if concurrentDemote {
				cp.startDemoteSeq = prePatchDemoteSeq
			}
			n.pendingVerdict = &cp
		} else if concurrentDemote && n.demoteInode != 0 {
			n.pendingVerdict = nil
		}
		n.mu.Unlock()
		if poisoned {
			return errors.Join(ErrContainerdCachedCRIU, err)
		}
		return err
	}

	postCurrIno, hasPostCurr := n.statCurrentSocket()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.pendingVerdict = nil
	if v.version != "" {
		n.version = v.version
	}
	if n.demoteSeq != prePatchDemoteSeq && !isStaleDemote(v.ino, n.demoteInode, postCurrIno, hasPostCurr) {
		if v.ino != 0 && n.demoteInode == 0 && (!hasPostCurr || postCurrIno == 0 || postCurrIno == v.ino) {
			n.lastSocketInode = v.ino
		}
		if v.ino != 0 && v.ino == n.poisonedInode {
			return ErrContainerdCachedCRIU
		}
		return nil
	}
	n.ready = v.ready
	if v.ino != 0 && (!concurrentDemote || n.demoteInode == 0 || n.demoteInode == v.ino) && (!hasPostCurr || postCurrIno == 0 || postCurrIno == v.ino) {
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	log.Info("criu capability", "version", version, "checkAll", ok, "raw", truncate(raw, 200))

	var reason, remediation string
	var transientErr error
	if !ok {
		reason = "criu check --all failed"
		remediation = "check host kernel support for CRIU"
	} else {
		// Kernel check passed; verify containerd readiness so an unready containerd
		// is never sent a checkpoint that would poison its lookup cache (#130).
		insp := n.Inspector
		if insp == nil {
			insp = NewDefaultContainerdInspector(execFn)
		}
		cOk, cReason, cRemediation, cErr := CheckContainerdReadiness(ctx, insp, n.SocketPath)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if cErr != nil {
			log.Error(cErr, "containerd readiness check error")
			ok = false
			reason = "containerd readiness probe error: " + truncate(cErr.Error(), 200)
			remediation = "verify containerd is running and accessible"
			transientErr = cErr
		} else if !cOk {
			ok = false
			reason = cReason
			remediation = cRemediation
		}
	}

	verdictIno := startIno
	if transientErr != nil {
		// Transient lookup error during probe/restart: do NOT commit socket inode.
		// Passing ino: 0 prevents publish from recording startIno as lastSocketInode.
		// Mark transientReadiness so subsequent ProbeReadiness ticks retry only the
		// containerd readiness check cheaply without full Bootstrap (no bundle reinstall).
		verdictIno = 0
		n.mu.Lock()
		n.transientReadiness = true
		n.transientIno = startIno
		n.transientDemoteSeq = startDemoteSeq
		n.mu.Unlock()
	} else {
		n.mu.Lock()
		n.transientReadiness = false
		n.transientIno = 0
		n.mu.Unlock()
	}
	pubErr := n.publish(ctx, nodeVerdict{
		ready:          ok,
		version:        version,
		reason:         reason,
		remediation:    remediation,
		ino:            verdictIno,
		startDemoteSeq: startDemoteSeq,
	})
	if transientErr != nil {
		return errors.Join(transientErr, pubErr)
	}
	return pubErr
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
		n.pendingVerdict = nil
		n.lastSocketInode = ino
		n.transientReadiness = false
		n.transientIno = 0
		if reason == ReasonContainerdCachedCRIU {
			n.poisonedInode = ino
		}
	} else if n.pendingVerdict != nil {
		n.pendingVerdict.ready = false
		n.pendingVerdict.reason = reason
		n.pendingVerdict.remediation = remediation
		n.pendingVerdict.startDemoteSeq = seq
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
	transientRetry := n.transientReadiness
	transIno := n.transientIno
	var pv *nodeVerdict
	if n.pendingVerdict != nil {
		vc := *n.pendingVerdict
		pv = &vc
	}
	n.mu.Unlock()

	if isReady {
		return true, nil
	}

	currIno, ok := n.statCurrentSocket()
	if ok && currIno != 0 && currIno != lastIno {
		if pv != nil && (pv.ino == currIno || (pv.ino == 0 && transientRetry && currIno == transIno)) {
			if err := n.publish(ctx, *pv); err != nil {
				return false, err
			}
			if pv.ready || (!transientRetry || currIno != transIno) {
				return n.IsReady(), nil
			}
		}
		if transientRetry && currIno == transIno {
			// Containerd socket is unchanged after a transient readiness error.
			// Retry only the readiness check cheaply without full Bootstrap (no Node GET,
			// no bundle reinstall, no criu check --all).
			return n.retryContainerdReadiness(ctx, currIno)
		}
		n.mu.Lock()
		n.transientReadiness = false
		n.transientIno = 0
		n.mu.Unlock()
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
		ver := n.version
		reason := n.unreadyReason
		remediation := n.unreadyRemediation
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
		return false, nil
	}

	if pv != nil && (!ok || currIno == 0 || pv.ino == currIno) {
		if err := n.publish(ctx, *pv); err != nil {
			return false, err
		}
		return n.IsReady(), nil
	}

	return false, nil
}

func (n *Node) retryContainerdReadiness(ctx context.Context, ino uint64) (bool, error) {
	n.mu.Lock()
	version := n.version
	startDemoteSeq := n.transientDemoteSeq
	n.mu.Unlock()

	insp := n.Inspector
	if insp == nil {
		execFn := n.execHostFn
		if execFn == nil && n.Restorer != nil {
			execFn = n.Restorer.execHost
		}
		insp = NewDefaultContainerdInspector(execFn)
	}

	cOk, cReason, cRemediation, cErr := CheckContainerdReadiness(ctx, insp, n.SocketPath)
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	if cErr != nil {
		// Still transiently failing (e.g. dial error).
		// Keep transientReadiness active; do not reinstall bundle or re-patch node.
		return false, cErr
	}

	n.mu.Lock()
	n.transientReadiness = false
	n.transientIno = 0
	n.mu.Unlock()

	var reason, remediation string
	if !cOk {
		reason = cReason
		remediation = cRemediation
	}

	if err := n.publish(ctx, nodeVerdict{
		ready:          cOk,
		version:        version,
		reason:         reason,
		remediation:    remediation,
		ino:            ino,
		startDemoteSeq: startDemoteSeq,
	}); err != nil {
		return false, err
	}
	return n.IsReady(), nil
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
		// Treat terminal pods (Succeeded, Failed) or failed restores as dead.
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if p.Annotations != nil && p.Annotations[contract.AnnotationRestoreStatus] == contract.RestoreStatusFailed {
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
			if snap := p.Annotations[contract.AnnotationSnapshot]; snap != "" {
				livingSnaps[snap] = true
			}
			if snap := p.Annotations["pod-migration.gke.io/restore-completed"]; snap != "" {
				livingSnaps[snap] = true
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
