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
		Client:   c,
		NodeName: nodeName,
		Kubelet:  newKubeletCheckpoint(),
		Restorer: newRestorer(cc),
	}, nil
}

// Bootstrap installs the CRIU bundle, verifies it against the host kernel, and
// annotates the node with its capability verdict.
func (n *Node) Bootstrap(ctx context.Context) error {
	log := logf.FromContext(ctx)
	var nodeObj corev1.Node
	if err := n.Client.Get(ctx, types.NamespacedName{Name: n.NodeName}, &nodeObj); err != nil {
		return err
	}
	if err := CheckContainerdVersion(nodeObj.Status.NodeInfo.ContainerRuntimeVersion); err != nil {
		_ = annotateNode(ctx, n.Client, n.NodeName, "", false)
		return err
	}
	version, err := installCRIUBundle()
	if err != nil {
		return err
	}
	ok, raw := verifyCRIU(ctx, n.Restorer.execHost)
	log.Info("criu capability", "version", version, "checkAll", ok, "raw", truncate(raw, 200))
	return annotateNode(ctx, n.Client, n.NodeName, version, ok)
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
