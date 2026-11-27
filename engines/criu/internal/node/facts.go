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

// Source node facts (#257): what the node agent knows about this node when it
// takes a CRIU checkpoint, recorded with the checkpoint so a restore can be
// placed on a compatible node. Every probe is best effort: a fact the agent
// can't read is left empty, and never fails the checkpoint.

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// defaultFactsProbeTimeout bounds each probe that leaves the agent (the runc
// exec and the containerd version call).
const defaultFactsProbeTimeout = 5 * time.Second

// hostRuncPATH is the PATH the agent looks runc up on in the host mount
// namespace, for the sandbox-sibling restore (restore.go), the runc calls that
// manage the restored container, and the runc probe, so all of them mean the
// same runc: the distro directories, COS's /home/kubernetes/bin, and last
// /usr/local, where kind and containerd's install guide put runc.
const hostRuncPATH = "/usr/sbin:/usr/bin:/sbin:/bin:/home/kubernetes/bin:/usr/local/sbin:/usr/local/bin"

// hostRuncEnv is the environment of every runc the agent runs on the host,
// except the restore, which also puts the CRIU bundle first.
var hostRuncEnv = []string{"PATH=" + hostRuncPATH}

// factsProbe gathers SourceNodeFacts. Its fields are the seams tests replace.
type factsProbe struct {
	// procRoot is the procfs root: cpuinfo and sys/kernel/osrelease. The
	// agent shares the host kernel, so its own procfs reports the host's.
	procRoot string
	// criuVersionFile is the CRIU bundle's VERSION file (the first line of
	// `criu --version`, written when the agent image is built).
	criuVersionFile string
	// execHost runs a command in the host namespaces; nil skips runc.
	execHost func(ctx context.Context, env []string, name string, args ...string) (string, error)
	// containerdVersion asks containerd for its version; nil skips it.
	containerdVersion func(ctx context.Context) (string, error)
	// timeout bounds each probe that leaves the agent; zero means
	// defaultFactsProbeTimeout.
	timeout time.Duration
	// ownNode gathers the node's facts as a restore target (node_facts.go):
	// the CPU features every processor has, rather than any processor.
	ownNode bool
}

// PSEngineSourceNodeFacts reports this node's facts for a checkpoint taken
// now. Unknown facts are empty.
func (n *Node) PSEngineSourceNodeFacts(ctx context.Context) contract.SourceNodeFacts {
	f := n.newFactsProbe().gather(ctx)
	if !f.IsEmpty() {
		f.Arch = runtime.GOARCH
		f.PlacementLabels = contract.PlacementLabelsVersion
	}
	logf.FromContext(ctx).V(1).Info("source node facts", "facts", f)
	return f
}

// newFactsProbe returns the probe for this node, on the host's procfs, the
// CRIU bundle in the agent's image (which every layout installs, #109), and the agent's host runner and containerd client.
func (n *Node) newFactsProbe() factsProbe {
	p := factsProbe{
		procRoot:        "/proc",
		criuVersionFile: filepath.Join(imageBundleDir, "VERSION"),
		execHost:        n.execHostFn,
	}
	if n.Restorer != nil {
		if p.execHost == nil {
			p.execHost = n.Restorer.execHost
		}
		if cc := n.Restorer.cclient; cc != nil {
			p.containerdVersion = func(ctx context.Context) (string, error) {
				v, err := cc.Version(ctx)
				return v.Version, err
			}
		}
	}
	return p
}

func (p factsProbe) gather(ctx context.Context) contract.SourceNodeFacts {
	var f contract.SourceNodeFacts
	if b, err := os.ReadFile(filepath.Join(p.procRoot, "cpuinfo")); err == nil {
		if p.ownNode {
			f.CPUModel, f.CPUFeatures = parseCPUInfoCommon(string(b))
		} else {
			f.CPUModel, f.CPUFeatures = parseCPUInfo(string(b))
		}
		f.CPUVendor = parseCPUVendor(string(b))
	}
	if b, err := os.ReadFile(filepath.Join(p.procRoot, "sys", "kernel", "osrelease")); err == nil {
		f.KernelVersion = strings.TrimSpace(string(b))
	}
	if p.criuVersionFile != "" {
		if b, err := os.ReadFile(p.criuVersionFile); err == nil {
			f.CRIUVersion = parseCRIUVersion(string(b))
		}
	}
	timeout := p.timeout
	if timeout <= 0 {
		timeout = defaultFactsProbeTimeout
	}
	if p.execHost != nil {
		pctx, cancel := context.WithTimeout(ctx, timeout)
		out, err := p.execHost(pctx, hostRuncEnv, "runc", "--version")
		cancel()
		if err == nil {
			f.RuncVersion = parseRuncVersion(out)
		}
	}
	if p.containerdVersion != nil {
		pctx, cancel := context.WithTimeout(ctx, timeout)
		v, err := p.containerdVersion(pctx)
		cancel()
		if err == nil {
			f.ContainerdVersion = strings.TrimPrefix(strings.TrimSpace(v), "v")
		}
	}
	return f
}

// parseCPUInfo returns the first "model name" and the union of every
// processor's feature flags ("flags" on x86, "Features" on arm64), sorted.
func parseCPUInfo(s string) (model string, features []string) {
	seen := map[string]bool{}
	for _, line := range strings.Split(s, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "model name":
			if model == "" {
				model = strings.TrimSpace(v)
			}
		case "flags", "Features":
			for _, flag := range strings.Fields(v) {
				if !seen[flag] {
					seen[flag] = true
					features = append(features, flag)
				}
			}
		}
	}
	sort.Strings(features)
	return model, features
}

// parseCPUInfoCommon is parseCPUInfo with the feature flags that every
// processor has: the intersection, which is what a restore target offers.
func parseCPUInfoCommon(s string) (model string, features []string) {
	var common map[string]bool
	for _, line := range strings.Split(s, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		switch strings.TrimSpace(k) {
		case "model name":
			if model == "" {
				model = strings.TrimSpace(v)
			}
		case "flags", "Features":
			these := map[string]bool{}
			for _, flag := range strings.Fields(v) {
				these[flag] = true
			}
			if common == nil {
				common = these
				continue
			}
			for flag := range common {
				if !these[flag] {
					delete(common, flag)
				}
			}
		}
	}
	for flag := range common {
		features = append(features, flag)
	}
	sort.Strings(features)
	return model, features
}

// parseCPUVendor returns the first "vendor_id" (x86). Empty when there is
// none, as on arm64.
func parseCPUVendor(s string) string {
	for _, line := range strings.Split(s, "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "vendor_id" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

var versionNumber = regexp.MustCompile(`[0-9]+\.[0-9]+(\.[0-9]+)?`)

// parseCRIUVersion extracts the version number from `criu --version` output
// ("Version: 4.1"). Empty when there is none.
func parseCRIUVersion(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return versionNumber.FindString(line)
}

// parseRuncVersion extracts the version from `runc --version` output
// ("runc version 1.2.5"). Empty when the first line isn't of that form.
func parseRuncVersion(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	v, ok := strings.CutPrefix(strings.TrimSpace(line), "runc version ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(v)
}
