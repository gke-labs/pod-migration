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
	"reflect"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

func TestParseCPUInfoCommon(t *testing.T) {
	model, features := parseCPUInfoCommon(x86CPUInfo)
	if model != "Intel(R) Xeon(R) CPU @ 2.20GHz" {
		t.Errorf("model = %q", model)
	}
	// avx2 is only on processor 0 and avx512f only on processor 1: a
	// target offers neither on every processor.
	if want := []string{"fpu", "sse4_2"}; !reflect.DeepEqual(features, want) {
		t.Errorf("features = %v, want the intersection %v", features, want)
	}
	if _, features := parseCPUInfoCommon(arm64CPUInfo); !reflect.DeepEqual(features, []string{"aes", "asimd", "evtstrm", "fp"}) {
		t.Errorf("arm64 features = %v", features)
	}
	if _, features := parseCPUInfoCommon("processor : 0\n"); features != nil {
		t.Errorf("no flags line: features = %v, want unknown", features)
	}
}

func TestParseCPUVendor(t *testing.T) {
	if got := parseCPUVendor(x86CPUInfo); got != "GenuineIntel" {
		t.Errorf("vendor = %q", got)
	}
	if got := parseCPUVendor(arm64CPUInfo); got != "" {
		t.Errorf("arm64 vendor = %q, want unknown", got)
	}
}

func TestOwnNodeFactsProbe(t *testing.T) {
	procRoot, versionFile := writeFactsFixture(t, x86CPUInfo, "6.6.72+\n", "Version: 4.1\n")
	p := factsProbe{procRoot: procRoot, criuVersionFile: versionFile, ownNode: true}
	got := p.gather(context.Background())
	want := contract.SourceNodeFacts{
		CPUVendor:     "GenuineIntel",
		CPUModel:      "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures:   []string{"fpu", "sse4_2"},
		KernelVersion: "6.6.72+",
		CRIUVersion:   "4.1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("facts:\n got %+v\nwant %+v", got, want)
	}
}

func TestPSEngineSourceNodeFactsStampsLabelFormat(t *testing.T) {
	n := &Node{execHostFn: func(context.Context, []string, string, ...string) (string, error) {
		return "runc version 1.2.5\n", nil
	}}
	if got := n.PSEngineSourceNodeFacts(context.Background()); got.PlacementLabels != contract.PlacementLabelsVersion {
		t.Errorf("PlacementLabels = %q, want %q", got.PlacementLabels, contract.PlacementLabelsVersion)
	}
	src := n.PSEngineSourceNodeFacts(context.Background())
	own := n.NodeFacts(context.Background())
	if own.PlacementLabels != contract.PlacementLabelsVersion {
		t.Errorf("NodeFacts PlacementLabels = %q, want %q", own.PlacementLabels, contract.PlacementLabelsVersion)
	}
	if src.Arch != goruntime.GOARCH || own.Arch != goruntime.GOARCH {
		t.Errorf("Arch = %q (source), %q (own), want %q", src.Arch, own.Arch, goruntime.GOARCH)
	}
}

func factsNode(labels, annotations map[string]string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: labels, Annotations: annotations}}
}

func newFactsClient(t *testing.T, node *corev1.Node, funcs interceptor.Funcs) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(node).WithInterceptorFuncs(funcs).Build()
}

var intelFacts = contract.SourceNodeFacts{
	CPUVendor: "GenuineIntel", CPUModel: "Intel(R) Xeon(R) CPU @ 2.20GHz",
	CPUFeatures: append([]string(nil), contract.InstructionFlags...), KernelVersion: "6.6.72+",
	CRIUVersion: "4.1", RuncVersion: "1.2.5", ContainerdVersion: "2.0.4", PlacementLabels: contract.PlacementLabelsVersion,
}

func TestPublishNodeFacts(t *testing.T) {
	ctx := context.Background()

	t.Run("publishes the labels and annotations, keeping everything else", func(t *testing.T) {
		c := newFactsClient(t, factsNode(map[string]string{"kubernetes.io/hostname": "n1"}, map[string]string{"pod-migrate.io/criu-ready": "true"}), interceptor.Funcs{})
		if err := publishNodeFacts(ctx, c, "n1", intelFacts); err != nil {
			t.Fatal(err)
		}
		got := &corev1.Node{}
		if err := c.Get(ctx, types.NamespacedName{Name: "n1"}, got); err != nil {
			t.Fatal(err)
		}
		for key, value := range contract.NodePlacementLabels(intelFacts) {
			if got.Labels[key] != value {
				t.Errorf("label %s = %q, want %q", key, got.Labels[key], value)
			}
		}
		if got.Labels["kubernetes.io/hostname"] != "n1" || got.Annotations["pod-migrate.io/criu-ready"] != "true" {
			t.Errorf("other labels/annotations lost: %v %v", got.Labels, got.Annotations)
		}
		if got.Annotations[contract.AnnotationNodeCPUFeatures] == "" || !strings.Contains(got.Annotations[contract.AnnotationNodeFacts], `"kernelVersion":"6.6.72+"`) {
			t.Errorf("annotations = %v", got.Annotations)
		}
	})

	t.Run("drops placement labels it no longer publishes", func(t *testing.T) {
		stale := map[string]string{
			contract.MissingCPUFlagLabelPrefix + "avx2": "true", // the node has avx2 now
			contract.LabelPrefix + "retired-label":      "x",    // a label from another format
			"team":                                      "a",
		}
		c := newFactsClient(t, factsNode(stale, map[string]string{contract.AnnotationNodeCPUFeatures: "old"}), interceptor.Funcs{})
		// Features unknown now: no missing-flag labels, no features annotation.
		facts := intelFacts
		facts.CPUFeatures = nil
		if err := publishNodeFacts(ctx, c, "n1", facts); err != nil {
			t.Fatal(err)
		}
		got := &corev1.Node{}
		if err := c.Get(ctx, types.NamespacedName{Name: "n1"}, got); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{contract.MissingCPUFlagLabelPrefix + "avx2", contract.LabelPrefix + "retired-label"} {
			if _, ok := got.Labels[key]; ok {
				t.Errorf("stale label %s kept", key)
			}
		}
		if got.Labels["team"] != "a" {
			t.Error("unrelated label removed")
		}
		if got.Labels[contract.LabelXState] != contract.LabelValueUnknown {
			t.Errorf("xstate = %q, want unknown", got.Labels[contract.LabelXState])
		}
		if _, ok := got.Annotations[contract.AnnotationNodeCPUFeatures]; ok {
			t.Error("stale features annotation kept")
		}
	})

	t.Run("writes nothing when the node already shows the facts", func(t *testing.T) {
		patches := 0
		c := newFactsClient(t, factsNode(nil, nil), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			patches++
			return c.Patch(ctx, obj, patch, opts...)
		}})
		for range 2 {
			if err := publishNodeFacts(ctx, c, "n1", intelFacts); err != nil {
				t.Fatal(err)
			}
		}
		if patches != 1 {
			t.Errorf("patches = %d, want 1", patches)
		}
	})

	t.Run("returns the patch error", func(t *testing.T) {
		c := newFactsClient(t, factsNode(nil, nil), interceptor.Funcs{Patch: func(context.Context, client.WithWatch, client.Object, client.Patch, ...client.PatchOption) error {
			return errors.New("forbidden")
		}})
		if err := publishNodeFacts(ctx, c, "n1", intelFacts); err == nil {
			t.Error("want the patch error")
		}
	})
}

func TestRunNodeFactsLoopRetriesAndRepublishes(t *testing.T) {
	patches := 0
	fail := true
	c := newFactsClient(t, factsNode(nil, nil), interceptor.Funcs{Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
		patches++
		if fail {
			fail = false
			return errors.New("transient")
		}
		return c.Patch(ctx, obj, patch, opts...)
	}})
	n := &Node{Client: c, NodeName: "n1", execHostFn: func(context.Context, []string, string, ...string) (string, error) {
		return "runc version 1.2.5\n", nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		n.RunNodeFactsLoop(ctx, time.Hour, 10*time.Millisecond)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := &corev1.Node{}
		if err := c.Get(context.Background(), types.NamespacedName{Name: "n1"}, got); err != nil {
			t.Fatal(err)
		}
		if got.Labels[contract.LabelRuncVersion] == "1002005" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("labels never published after a failed patch: %v", got.Labels)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
	if patches != 2 {
		t.Errorf("patches = %d, want the failed one and its retry", patches)
	}
}
