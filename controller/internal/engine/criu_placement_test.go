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

package engine

import (
	"context"
	"errors"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/criuplacement"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

// criuSnapshot returns a CRIU PodSnapshot in ns with status.sourceNode set
// to sourceNode, if it isn't nil.
func criuSnapshot(ns, name string, sourceNode map[string]interface{}) *unstructured.Unstructured {
	ps := &unstructured.Unstructured{}
	ps.SetGroupVersionKind(snapshot.PodSnapshotGVK(CRIUAPIGroup))
	ps.SetNamespace(ns)
	ps.SetName(name)
	if sourceNode != nil {
		ps.Object["status"] = map[string]interface{}{"sourceNode": sourceNode}
	}
	return ps
}

func criuJob(snapshotRef string) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pmj"},
		Status:     pmv1alpha1.PodMigrationJobStatus{SnapshotRef: snapshotRef},
	}
}

var intelSourceNode = map[string]interface{}{
	"cpuVendor":         "GenuineIntel",
	"cpuModel":          "Intel(R) Xeon(R) CPU @ 2.20GHz",
	"cpuFeatures":       []interface{}{"avx", "avx2", "fpu", "hypervisor", "sse2"},
	"kernelVersion":     "6.6.72+",
	"criuVersion":       "4.1",
	"runcVersion":       "1.2.5",
	"containerdVersion": "2.0.4",
	"placementLabels":   "v1",
}

func newCRIUWith(t *testing.T, funcs interceptor.Funcs, objs ...client.Object) *CRIU {
	t.Helper()
	cl := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(objs...).WithInterceptorFuncs(funcs).Build()
	e, err := NewCRIU(cl, testScheme(), CRIUAPIGroup)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestCRIU_Placement(t *testing.T) {
	ctx := context.Background()

	t.Run("states the rule's requirements for the snapshot's source node", func(t *testing.T) {
		e := newCRIUWith(t, interceptor.Funcs{}, criuSnapshot("ns", "snap", intelSourceNode))
		got, err := e.Placement(ctx, criuJob("snap"))
		if err != nil {
			t.Fatal(err)
		}
		facts, _ := criuplacement.SourceFacts(criuSnapshot("ns", "snap", intelSourceNode))
		want := criuplacement.Requirements(facts)
		if len(want) == 0 || !reflect.DeepEqual(got, want) {
			t.Fatalf("Placement = %v, want %v", got, want)
		}
		// avx, avx2, fpu and sse2 are compared; hypervisor isn't.
		missing := 0
		for _, req := range got {
			if req.Operator == corev1.NodeSelectorOpDoesNotExist {
				missing++
			}
			if req.Key == criuplacement.MissingCPUFlagLabelPrefix+"hypervisor" {
				t.Error("hypervisor is required")
			}
		}
		if missing != 4 {
			t.Errorf("%d missing-flag requirements, want 4", missing)
		}
		// The same each time.
		again, _ := e.Placement(ctx, criuJob("snap"))
		if !reflect.DeepEqual(got, again) {
			t.Error("Placement isn't stable")
		}
	})

	t.Run("snapshot in another engine group isn't read", func(t *testing.T) {
		other := criuSnapshot("ns", "snap", intelSourceNode)
		other.SetGroupVersionKind(snapshot.PodSnapshotGVK(snapshot.DefaultAPIGroup))
		e := newCRIUWith(t, interceptor.Funcs{}, other)
		if got, err := e.Placement(ctx, criuJob("snap")); err != nil || got != nil {
			t.Errorf("Placement = %v, %v; want none", got, err)
		}
	})

	for _, tc := range []struct {
		name string
		job  *pmv1alpha1.PodMigrationJob
		objs []client.Object
	}{
		{"no snapshot yet", criuJob(""), nil},
		{"snapshot gone", criuJob("snap"), nil},
		{"no source facts", criuJob("snap"), []client.Object{criuSnapshot("ns", "snap", nil)}},
		{"agent from before the label format", criuJob("snap"), []client.Object{criuSnapshot("ns", "snap", map[string]interface{}{
			"cpuModel": "Intel(R) Xeon(R) CPU @ 2.20GHz", "kernelVersion": "6.6.72+",
		})}},
		{"every fact unknown", criuJob("snap"), []client.Object{criuSnapshot("ns", "snap", map[string]interface{}{"placementLabels": "v1"})}},
	} {
		t.Run(tc.name+": no constraints", func(t *testing.T) {
			e := newCRIUWith(t, interceptor.Funcs{}, tc.objs...)
			got, err := e.Placement(ctx, tc.job)
			if err != nil || len(got) != 0 {
				t.Errorf("Placement = %v, %v; want none", got, err)
			}
		})
	}

	t.Run("a read error is returned for a retry", func(t *testing.T) {
		e := newCRIUWith(t, interceptor.Funcs{Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("connection refused")
		}})
		if _, err := e.Placement(ctx, criuJob("snap")); err == nil {
			t.Error("want the read error")
		}
	})

	t.Run("nil job", func(t *testing.T) {
		if _, err := newCRIUWith(t, interceptor.Funcs{}).Placement(ctx, nil); err == nil {
			t.Error("want an error")
		}
	})
}
