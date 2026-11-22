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

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

const x86CPUInfo = `processor	: 0
vendor_id	: GenuineIntel
model name	: Intel(R) Xeon(R) CPU @ 2.20GHz
flags		: fpu sse4_2 avx2 fpu

processor	: 1
vendor_id	: GenuineIntel
model name	: Intel(R) Xeon(R) CPU @ 2.20GHz
flags		: fpu sse4_2 avx512f
`

const arm64CPUInfo = `processor	: 0
BogoMIPS	: 50.00
Features	: fp asimd evtstrm aes

processor	: 1
BogoMIPS	: 50.00
Features	: fp asimd evtstrm aes
`

// writeFactsFixture lays out a fake procfs and CRIU bundle under a temp dir.
func writeFactsFixture(t *testing.T, cpuinfo, osrelease, criuVersion string) (procRoot, versionFile string) {
	t.Helper()
	dir := t.TempDir()
	procRoot = filepath.Join(dir, "proc")
	if err := os.MkdirAll(filepath.Join(procRoot, "sys", "kernel"), 0o755); err != nil {
		t.Fatal(err)
	}
	if cpuinfo != "" {
		if err := os.WriteFile(filepath.Join(procRoot, "cpuinfo"), []byte(cpuinfo), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if osrelease != "" {
		if err := os.WriteFile(filepath.Join(procRoot, "sys", "kernel", "osrelease"), []byte(osrelease), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	versionFile = filepath.Join(dir, "VERSION")
	if criuVersion != "" {
		if err := os.WriteFile(versionFile, []byte(criuVersion), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return procRoot, versionFile
}

func TestSourceNodeFactsAllKnown(t *testing.T) {
	procRoot, versionFile := writeFactsFixture(t, x86CPUInfo, "6.6.72+\n", "Version: 4.1\n")
	var runcArgs, runcEnv []string
	p := factsProbe{
		procRoot:        procRoot,
		criuVersionFile: versionFile,
		execHost: func(_ context.Context, env []string, name string, args ...string) (string, error) {
			runcEnv = env
			runcArgs = append([]string{name}, args...)
			return "runc version 1.2.5\ncommit: v1.2.5-0-g59923ef\nspec: 1.2.0\ngo: go1.22.12\nlibseccomp: 2.5.5\n", nil
		},
		containerdVersion: func(context.Context) (string, error) { return "v2.0.4", nil },
	}
	got := p.gather(context.Background())
	want := contract.SourceNodeFacts{
		CPUVendor:         "GenuineIntel",
		CPUModel:          "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures:       []string{"avx2", "avx512f", "fpu", "sse4_2"}, // union across processors, sorted, deduped
		KernelVersion:     "6.6.72+",
		CRIUVersion:       "4.1",
		RuncVersion:       "1.2.5",
		ContainerdVersion: "2.0.4",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("facts:\n got %+v\nwant %+v", got, want)
	}
	if !reflect.DeepEqual(runcArgs, []string{"runc", "--version"}) {
		t.Fatalf("runc probe ran %v", runcArgs)
	}
	// Same lookup PATH as the restore's runc, so both mean the same binary.
	if len(runcEnv) != 1 || !strings.Contains(runcEnv[0], "/home/kubernetes/bin") {
		t.Fatalf("runc probe env = %v, want the restore PATH", runcEnv)
	}
}

func TestSourceNodeFactsARM64Features(t *testing.T) {
	procRoot, versionFile := writeFactsFixture(t, arm64CPUInfo, "6.1.0", "")
	got := factsProbe{procRoot: procRoot, criuVersionFile: versionFile}.gather(context.Background())
	if want := []string{"aes", "asimd", "evtstrm", "fp"}; !reflect.DeepEqual(got.CPUFeatures, want) {
		t.Fatalf("arm64 Features: got %v, want %v", got.CPUFeatures, want)
	}
	if got.CPUModel != "" {
		t.Fatalf("no model name line: CPUModel must stay empty, got %q", got.CPUModel)
	}
}

// Missing facts never fail: every probe failing yields empty facts.
func TestSourceNodeFactsEverythingMissing(t *testing.T) {
	dir := t.TempDir()
	p := factsProbe{
		procRoot:        filepath.Join(dir, "no-proc"),
		criuVersionFile: filepath.Join(dir, "no-VERSION"),
		execHost: func(context.Context, []string, string, ...string) (string, error) {
			return "nsenter: exec runc: No such file or directory", errors.New("exit status 127")
		},
		containerdVersion: func(context.Context) (string, error) { return "", errors.New("connection refused") },
	}
	if got := p.gather(context.Background()); !got.IsEmpty() {
		t.Fatalf("want empty facts, got %+v", got)
	}
	// Nil probes are skipped, not dereferenced.
	if got := (factsProbe{procRoot: filepath.Join(dir, "no-proc")}).gather(context.Background()); !got.IsEmpty() {
		t.Fatalf("want empty facts with nil probes, got %+v", got)
	}
}

// Unparseable version output is left empty rather than recorded raw.
func TestSourceNodeFactsUnparseableVersions(t *testing.T) {
	procRoot, versionFile := writeFactsFixture(t, "", "", "criu (some build)\n")
	p := factsProbe{
		procRoot:        procRoot,
		criuVersionFile: versionFile,
		execHost: func(context.Context, []string, string, ...string) (string, error) {
			return "something else entirely", nil
		},
		containerdVersion: func(context.Context) (string, error) { return "  ", nil },
	}
	got := p.gather(context.Background())
	if !got.IsEmpty() {
		t.Fatalf("want empty facts for unparseable output, got %+v", got)
	}
}

// A hung probe is bounded: gather returns within its timeout with what it has.
func TestSourceNodeFactsBoundedProbes(t *testing.T) {
	procRoot, versionFile := writeFactsFixture(t, x86CPUInfo, "6.6.72+", "Version: 4.1")
	p := factsProbe{
		procRoot:        procRoot,
		criuVersionFile: versionFile,
		timeout:         50 * time.Millisecond,
		execHost: func(ctx context.Context, _ []string, _ string, _ ...string) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
		containerdVersion: func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		},
	}
	start := time.Now()
	got := p.gather(context.Background())
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("gather took %v; probes must be bounded", d)
	}
	if got.KernelVersion != "6.6.72+" || got.CRIUVersion != "4.1" || got.RuncVersion != "" || got.ContainerdVersion != "" {
		t.Fatalf("want local facts only, got %+v", got)
	}
}

func TestParseCRIUVersion(t *testing.T) {
	for in, want := range map[string]string{
		"Version: 4.1\n":        "4.1",
		"Version: 3.19":         "3.19",
		"Version: 4.1.1\nGitID": "4.1.1",
		"4.0":                   "4.0",
		"":                      "",
		"no digits here":        "",
	} {
		if got := parseCRIUVersion(in); got != want {
			t.Errorf("parseCRIUVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestParseRuncVersion(t *testing.T) {
	for in, want := range map[string]string{
		"runc version 1.2.5\ncommit: x\n": "1.2.5",
		"runc version 1.1.12-0ubuntu3":    "1.1.12-0ubuntu3",
		"runc version":                    "",
		"":                                "",
	} {
		if got := parseRuncVersion(in); got != want {
			t.Errorf("parseRuncVersion(%q) = %q, want %q", in, got, want)
		}
	}
}

// The Node wiring uses its host runner for runc and tolerates a Node with no
// containerd client.
func TestNodePSEngineSourceNodeFactsWiring(t *testing.T) {
	n := &Node{execHostFn: func(_ context.Context, _ []string, name string, _ ...string) (string, error) {
		if name != "runc" {
			return "", errors.New("unexpected " + name)
		}
		return "runc version 1.2.5\n", nil
	}}
	if got := n.PSEngineSourceNodeFacts(context.Background()); got.RuncVersion != "1.2.5" {
		t.Fatalf("RuncVersion = %q, want 1.2.5 via execHostFn", got.RuncVersion)
	}
	n = &Node{Restorer: &restorer{execHost: func(context.Context, []string, string, ...string) (string, error) {
		return "runc version 1.1.0\n", nil
	}}}
	if got := n.PSEngineSourceNodeFacts(context.Background()); got.RuncVersion != "1.1.0" || got.ContainerdVersion != "" {
		t.Fatalf("restorer fallback: got %+v", got)
	}
}
