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

package controller

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
)

// TestCRIURestorePlacement_Envtest checks against a real API server that the
// CRIU engine's constraints for a realistic x86 source (In, Gt and one
// DoesNotExist per instruction flag) are valid in the update that releases
// the gate (#257).
func TestCRIURestorePlacement_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest CRIU placement test")
	}
	testEnv := &envtest.Environment{}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}
	ctx := context.Background()
	const ns = "criu-placement-test"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}

	source := criuplacement.Facts{
		CPUVendor: "GenuineIntel",
		CPUModel:  "Intel(R) Xeon(R) CPU @ 2.20GHz",
		CPUFeatures: strings.Fields("fpu tsc cx8 sep cmov clflush mmx fxsr sse sse2 syscall rdtscp rep_good nopl pni " +
			"pclmulqdq ssse3 cx16 sse4_1 sse4_2 movbe popcnt aes xsave avx f16c rdrand abm 3dnowprefetch fsgsbase " +
			"bmi1 hle avx2 bmi2 erms rtm mpx avx512f avx512dq rdseed adx clflushopt avx512cd avx512bw avx512vl xsaveopt " +
			"xsavec xgetbv1 xsaves avx512_vnni ospke md_clear hypervisor"),
		KernelVersion: "6.6.72+", CRIUVersion: "4.1", RuncVersion: "1.2.5", ContainerdVersion: "2.0.4",
		PlacementLabels: criuplacement.PlacementLabelsVersion,
	}
	reqs := criuplacement.Requirements(source)
	if len(reqs) < 50 {
		t.Fatalf("got %d requirements, want one per compared flag plus the fixed ones", len(reqs))
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "criu-replacement", Namespace: ns},
		Spec: corev1.PodSpec{
			Containers:      []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}},
			SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
		},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	pod.Annotations = map[string]string{"criu.podmigration.gke.io/ps-name": "snapshot-1"}
	addRequiredNodeAffinity(pod, reqs)
	(&PodGateReconciler{}).removeGate(pod)
	if err := c.Update(ctx, pod); err != nil {
		t.Fatalf("releasing the gate with the CRIU constraints: %v", err)
	}
	got := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: pod.Name}, got); err != nil {
		t.Fatal(err)
	}
	want := []corev1.NodeSelectorTerm{{MatchExpressions: reqs}}
	if terms := requiredTerms(got); !reflect.DeepEqual(terms, want) {
		t.Errorf("terms = %+v\nwant %+v", terms, want)
	}
}
