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

package invariants

import (
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
)

// More per-engine restore order cases (#234): the I1 rebind check, engines the
// registry reads in unusual ways, and the restore-completed cold-start bypass.

// engineWithoutRestoreOrders is an engine that doesn't implement
// engine.RestoreOrderer: embedding the interface promotes only Engine methods.
// The registry orders its restores under podsnapshot.gke.io/ps-name.
type engineWithoutRestoreOrders struct{ engine.Engine }

func (engineWithoutRestoreOrders) Name() string { return "plain" }

// gkeOnlyRestoreOrders returns the RestoreOrders of a registry with only GKE
// Pod Snapshots, such as a controller restarted without the CRIU engine.
func gkeOnlyRestoreOrders(t *testing.T) RestoreOrderFunc {
	t.Helper()
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(nil, nil)); err != nil {
		t.Fatalf("registering GKE Pod Snapshots: %v", err)
	}
	return reg.InspectRestoreOrder
}

// The rebind check counts the same orders as the duplicate check for a pod
// whose job is known, under a key the consumed job's engine reads.
func TestI1_RebindCountsOnlyOrdersThePodRestores(t *testing.T) {
	const rebound = "I1/ConsumedSnapshotReboundAfterGateRelease"
	multiGroup := registryRestoreOrders(t, engine.CRIUAPIGroup)
	singleGroup := registryRestoreOrders(t, engine.DefaultAPIGroup)
	consumedGKE := succeededPMJWithEngine("pmj-done-g", engine.GKEPodSnapshotsName, "snap-g", "repl-x")
	consumedCRIU := succeededPMJWithEngine("pmj-done-c", engine.CRIUName, "snap-c", "repl-x")
	criu1 := pmjWithEngine("pmj-c1", engine.CRIUName)
	gke1 := pmjWithEngine("pmj-g1", engine.GKEPodSnapshotsName)

	cases := []struct {
		name          string
		restoreOrders RestoreOrderFunc
		jobs          []pmv1alpha1.PodMigrationJob
		pod           corev1.Pod
		want          []string
		wantPMJ       string
	}{
		{"a stray GKE name on a runc pod of a CRIU job is not a rebind of a consumed GKE job", multiGroup,
			[]pmv1alpha1.PodMigrationJob{consumedGKE, criu1},
			replacementPod("a", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "", gkeOrderKey: "snap-g"}), nil, ""},
		{"a GKE name on a gVisor pod of a CRIU job is a rebind of a consumed GKE job", multiGroup,
			[]pmv1alpha1.PodMigrationJob{consumedGKE, criu1},
			replacementPod("a", gvisorClass, "pmj-c1", false, podAnn{criuOrderKey: "", gkeOrderKey: "snap-g"}), []string{rebound}, "pmj-c1"},
		{"single-group install: a CRIU job's order in the shared key is a rebind of a consumed GKE job", singleGroup,
			[]pmv1alpha1.PodMigrationJob{consumedGKE, criu1},
			replacementPod("a", runcClass, "pmj-c1", false, podAnn{gkeOrderKey: "snap-g"}), []string{rebound}, "pmj-c1"},
		{"another CRIU job's replacement restoring the consumed CRIU snapshot is a rebind", multiGroup,
			[]pmv1alpha1.PodMigrationJob{consumedCRIU, criu1},
			replacementPod("a", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "snap-c"}), []string{rebound}, "pmj-c1"},
		{"a GKE order of the same name on a GKE job's pod is not a rebind of a consumed CRIU job", multiGroup,
			[]pmv1alpha1.PodMigrationJob{consumedCRIU, gke1},
			replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-c"}), nil, ""},
		{"a pod whose job isn't in the snapshot is matched on the consumed job's engine order", multiGroup,
			[]pmv1alpha1.PodMigrationJob{consumedCRIU},
			replacementPod("a", gvisorClass, "pmj-gone", false, podAnn{gkeOrderKey: "", criuOrderKey: "snap-c"}), []string{rebound}, "pmj-gone"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vs := EvaluateI1(&ReconcileSnapshot{NamespacePMJs: tc.jobs, NamespacePods: []corev1.Pod{tc.pod}, RestoreOrders: tc.restoreOrders})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI1 = %v, want %v (%+v)", got, tc.want, vs)
			}
			for _, v := range vs {
				if v.PMJName != tc.wantPMJ {
					t.Errorf("violation names PMJ %q, want the rebound pod's job %q", v.PMJName, tc.wantPMJ)
				}
			}
		})
	}
}

// Registries that read restore orders in less common ways.
func TestI2_EngineRegistryEdgeCases(t *testing.T) {
	const (
		gateReleased  = "I2/GateReleasedWithoutSnapshotOrColdStartBypass"
		missingBypass = "I2/GVisorPodMissingGKEColdStartBypass"
	)
	withPlainEngine := func(t *testing.T) RestoreOrderFunc {
		reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
		if err := reg.Register(engine.NewGKEPodSnapshots(nil, nil)); err != nil {
			t.Fatalf("registering GKE Pod Snapshots: %v", err)
		}
		criu, err := engine.NewCRIU(nil, nil, engine.CRIUAPIGroup)
		if err != nil {
			t.Fatalf("NewCRIU: %v", err)
		}
		if err := reg.Register(engineWithoutRestoreOrders{criu}); err != nil {
			t.Fatalf("registering the plain engine: %v", err)
		}
		return reg.InspectRestoreOrder
	}
	const customGroup = "criu.example.com"
	customKey := engine.RestoreAnnotation(customGroup)
	criuJob := pmjWithEngine("pmj-c", engine.CRIUName)
	plainJob := pmjWithEngine("pmj-p", "plain")

	cases := []struct {
		name          string
		restoreOrders RestoreOrderFunc
		job           pmv1alpha1.PodMigrationJob
		pod           corev1.Pod
		want          []string
	}{
		// The controller fails such a job (EngineNotRegistered); until its pod
		// goes away, no order of the job's engine can be read on it.
		{"job naming an engine the registry doesn't have: its released replacement is reported", gkeOnlyRestoreOrders(t), criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{gkeOrderKey: "", criuOrderKey: "snap-c"}), []string{gateReleased}},
		{"engine without RestoreOrderer orders under the GKE key: GKE order", withPlainEngine(t), plainJob,
			replacementPod("repl", gvisorClass, "pmj-p", false, podAnn{gkeOrderKey: "snap-p"}), nil},
		{"engine without RestoreOrderer orders under the GKE key: a CRIU order is not enough", withPlainEngine(t), plainJob,
			replacementPod("repl", gvisorClass, "pmj-p", false, podAnn{criuOrderKey: "snap-p"}), []string{gateReleased}},
		{"custom CRIU group: its order and the GKE bypass", registryRestoreOrders(t, customGroup), criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{customKey: "snap-c", gkeOrderKey: ""}), nil},
		{"custom CRIU group: its order without the GKE bypass on a gVisor pod", registryRestoreOrders(t, customGroup), criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{customKey: "snap-c"}), []string{missingBypass}},
		{"custom CRIU group: the default CRIU group's key is not its order", registryRestoreOrders(t, customGroup), criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{criuOrderKey: "snap-c"}), []string{gateReleased}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vs := EvaluateI2(&ReconcileSnapshot{
				NamespacePMJs: []pmv1alpha1.PodMigrationJob{tc.job},
				NamespacePods: []corev1.Pod{tc.pod},
				RestoreOrders: tc.restoreOrders,
			})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI2 = %v, want %v (%+v)", got, tc.want, vs)
			}
		})
	}
}

// After a verified restore, the controller sets the job engine's cold-start
// bypass and the restore-completed marker on the replacement (#152). Another
// engine's bypass doesn't count.
func TestI2_RestoreCompletedBypassCountsForTheJobEngineOnly(t *testing.T) {
	const missing = "I2/SucceededPodMissingRestoreAnnotation"
	restoreOrders := registryRestoreOrders(t, engine.CRIUAPIGroup)
	restored := func(engineName, reason string) pmv1alpha1.PodMigrationJob {
		job := succeededPMJWithEngine("pmj-1", engineName, "snap-1", "repl")
		job.Status.Conditions = []metav1.Condition{{Type: ConditionRestored, Status: metav1.ConditionTrue, Reason: reason}}
		return job
	}
	cases := []struct {
		name  string
		job   pmv1alpha1.PodMigrationJob
		class string
		ann   podAnn
		want  []string
	}{
		{"CRIU job: CRIU bypass and the marker", restored(engine.CRIUName, ReasonRestoreVerified), runcClass,
			podAnn{criuOrderKey: "", AnnotationRestoreCompleted: "snap-1"}, nil},
		{"CRIU job: only the GKE bypass and the marker", restored(engine.CRIUName, ReasonRestoreVerified), runcClass,
			podAnn{gkeOrderKey: "", AnnotationRestoreCompleted: "snap-1"}, []string{missing}},
		{"job naming no engine: GKE bypass and the marker", restored("", ReasonRestoreInferred), gvisorClass,
			podAnn{gkeOrderKey: "", AnnotationRestoreCompleted: "snap-1"}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := replacementPod("repl", tc.class, "", false, tc.ann)
			vs := EvaluateI2(&ReconcileSnapshot{PrimaryPMJ: &tc.job, NamespacePods: []corev1.Pod{pod}, RestoreOrders: restoreOrders})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI2 = %v, want %v (%+v)", got, tc.want, vs)
			}
		})
	}
}
