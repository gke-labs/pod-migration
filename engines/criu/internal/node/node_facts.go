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

package node

// Target node facts (#257): the agent publishes its own node's facts on the
// Node, so the controller can place a CRIU restore only on a node that can
// restore the checkpoint (contract/node_placement.go has the format).

import (
	"context"
	"maps"
	"reflect"
	"runtime"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const (
	// DefaultNodeFactsInterval is how often the agent reads its node's facts
	// again, to republish them if they changed (containerd or runc upgraded
	// in place, say). The kernel only changes with a reboot, which restarts
	// the agent.
	DefaultNodeFactsInterval = 10 * time.Minute
	// DefaultNodeFactsRetry is how soon a failed publish is retried.
	DefaultNodeFactsRetry = 30 * time.Second
)

// NodeFacts reports this node's facts as a restore target: like
// PSEngineSourceNodeFacts, but with the CPU features every processor has.
func (n *Node) NodeFacts(ctx context.Context) contract.SourceNodeFacts {
	p := n.newFactsProbe()
	p.ownNode = true
	f := p.gather(ctx)
	f.Arch = runtime.GOARCH
	f.PlacementLabels = contract.PlacementLabelsVersion
	return f
}

// PublishNodeFacts publishes f, this node's facts, as the placement labels
// and facts annotations on the Node. Placement labels it no longer
// publishes, such as the missing-flag label of a flag the node gained, are
// removed. The merge patch only touches those keys, and is skipped when
// nothing changed.
func (n *Node) PublishNodeFacts(ctx context.Context, f contract.SourceNodeFacts) error {
	n.annotateMu.Lock()
	defer n.annotateMu.Unlock()
	return publishNodeFacts(ctx, n.Client, n.NodeName, f)
}

func publishNodeFacts(ctx context.Context, c client.Client, nodeName string, f contract.SourceNodeFacts) error {
	apiCtx, cancel := context.WithTimeout(ctx, nodeAPITimeout)
	defer cancel()
	var node corev1.Node
	if err := c.Get(apiCtx, types.NamespacedName{Name: nodeName}, &node); err != nil {
		return err
	}
	before := node.DeepCopy()
	labels := maps.Clone(node.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	want := contract.NodePlacementLabels(f)
	for key := range labels {
		if _, ok := want[key]; !ok && contract.IsPlacementLabel(key) {
			delete(labels, key)
		}
	}
	maps.Copy(labels, want)
	annotations := maps.Clone(node.Annotations)
	if annotations == nil {
		annotations = map[string]string{}
	}
	delete(annotations, contract.AnnotationNodeCPUFeatures)
	maps.Copy(annotations, contract.NodeFactsAnnotations(f))
	if maps.Equal(labels, node.Labels) && maps.Equal(annotations, node.Annotations) {
		return nil
	}
	node.Labels = labels
	node.Annotations = annotations
	return c.Patch(apiCtx, &node, client.MergeFrom(before))
}

// RunNodeFactsLoop publishes this node's facts now, then reads and
// publishes them again every interval, until ctx is done, so they follow an
// in-place upgrade and come back if someone removes them. Publishing facts
// that the Node already shows writes nothing. A failed publish is retried
// after retry. So are facts published with something still unknown
// (nodeFactsComplete), such as a version read while containerd restarts or
// before a failed bootstrap is retried, so that the node doesn't show it as
// unknown for a whole interval; after interval/retry such retries in a row,
// the loop falls back to interval, for a fact that stays unknown. Zero
// durations take the defaults.
func (n *Node) RunNodeFactsLoop(ctx context.Context, interval, retry time.Duration) {
	runNodeFactsLoop(ctx, interval, retry, n.NodeName, n.NodeFacts, n.PublishNodeFacts, time.After)
}

func runNodeFactsLoop(ctx context.Context, interval, retry time.Duration, nodeName string,
	read func(context.Context) contract.SourceNodeFacts,
	publish func(context.Context, contract.SourceNodeFacts) error,
	after func(time.Duration) <-chan time.Time) {
	if interval <= 0 {
		interval = DefaultNodeFactsInterval
	}
	if retry <= 0 {
		retry = DefaultNodeFactsRetry
	}
	log := logf.FromContext(ctx).WithValues("node", nodeName)
	maxIncompleteRetries := int(interval / retry)
	var published *contract.SourceNodeFacts
	incompleteRetries := 0
	for {
		wait := interval
		f := read(ctx)
		if err := publish(ctx, f); err != nil {
			log.Error(err, "publishing node facts for restore placement failed; retrying")
			wait = retry
		} else {
			if published == nil || !reflect.DeepEqual(*published, f) {
				log.Info("published node facts for restore placement", "facts", contract.NodeFactsAnnotations(f)[contract.AnnotationNodeFacts])
				published = &f
			}
			switch {
			case nodeFactsComplete(f):
				incompleteRetries = 0
			case incompleteRetries < maxIncompleteRetries:
				incompleteRetries++
				wait = retry
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-after(wait):
		}
	}
}

// nodeFactsComplete reports whether f, this node's facts, has every fact the
// agent can read on a healthy node: the architecture, the CPU features, the
// four versions, and on x86 the CPU vendor and model, which arm64's
// /proc/cpuinfo doesn't have.
func nodeFactsComplete(f contract.SourceNodeFacts) bool {
	if f.Arch == "" || len(f.CPUFeatures) == 0 || f.KernelVersion == "" || f.CRIUVersion == "" ||
		f.RuncVersion == "" || f.ContainerdVersion == "" {
		return false
	}
	if f.Arch == "amd64" || f.Arch == "386" {
		return f.CPUVendor != "" && f.CPUModel != ""
	}
	return true
}
