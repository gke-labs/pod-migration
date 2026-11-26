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
	"k8s.io/apimachinery/pkg/types"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/restore"
)

// These tests cover restore orders in a cluster that runs more than one
// snapshot engine (#234). GKE Pod Snapshots orders a restore under
// podsnapshot.gke.io/ps-name, and CRIU, by default, under
// criu.podmigration.gke.io/ps-name. The tests read restore orders through the
// production engine registry, wired as the reconcilers wire it.

var (
	gkeOrderKey  = engine.RestoreAnnotation(engine.DefaultAPIGroup)
	criuOrderKey = engine.RestoreAnnotation(engine.CRIUAPIGroup)
)

const (
	runcClass   = ""
	gvisorClass = restore.GVisorRuntimeClass
)

// podAnn holds the annotations of a test pod.
type podAnn map[string]string

// registryRestoreOrders returns Registry.InspectRestoreOrder of a registry that
// runs GKE Pod Snapshots, the default engine, next to CRIU on criuGroup.
func registryRestoreOrders(t *testing.T, criuGroup string) RestoreOrderFunc {
	t.Helper()
	reg := engine.NewRegistry(engine.GKEPodSnapshotsName)
	if err := reg.Register(engine.NewGKEPodSnapshots(nil, nil)); err != nil {
		t.Fatalf("registering GKE Pod Snapshots: %v", err)
	}
	criu, err := engine.NewCRIU(nil, nil, criuGroup)
	if err != nil {
		t.Fatalf("NewCRIU(%q): %v", criuGroup, err)
	}
	if err := reg.Register(criu); err != nil {
		t.Fatalf("registering CRIU: %v", err)
	}
	return reg.InspectRestoreOrder
}

// pmjWithEngine returns a job in namespace default that names engineName; ""
// means the job names no engine, so GKE Pod Snapshots serves it.
func pmjWithEngine(name, engineName string) pmv1alpha1.PodMigrationJob {
	job := pmv1alpha1.PodMigrationJob{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name}}
	if engineName != "" {
		job.Annotations = map[string]string{AnnotationEngine: engineName}
	}
	return job
}

// succeededPMJWithEngine returns a Succeeded, consumed and released job that
// restored snapshotRef into the pod podName (UID uid-<podName>).
func succeededPMJWithEngine(name, engineName, snapshotRef, podName string) pmv1alpha1.PodMigrationJob {
	job := pmjWithEngine(name, engineName)
	job.Status = pmv1alpha1.PodMigrationJobStatus{
		Phase:           pmv1alpha1.PodMigrationJobPhaseSucceeded,
		SnapshotRef:     snapshotRef,
		Consumed:        true,
		GateReleased:    true,
		RestoredPodName: podName,
		RestoredPodUID:  "uid-" + podName,
	}
	return job
}

// replacementPod returns a pod in namespace default with UID uid-<name>, of
// runtime class class (runcClass for none), attached to the job assigned (""
// for none), still gated or not, with the annotations ann.
func replacementPod(name, class, assigned string, gated bool, ann podAnn) corev1.Pod {
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace:   "default",
		Name:        name,
		UID:         types.UID("uid-" + name),
		Annotations: map[string]string{},
	}}
	for k, v := range ann {
		pod.Annotations[k] = v
	}
	if assigned != "" {
		pod.Annotations[AnnotationAssignedPMJ] = assigned
	}
	if class != runcClass {
		rc := class
		pod.Spec.RuntimeClassName = &rc
	}
	if gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: MigrationGateName}}
	}
	return pod
}

// violationReasons returns "<invariant>/<reason>" for each violation.
func violationReasons(vs []Violation) []string {
	var out []string
	for _, v := range vs {
		out = append(out, v.InvariantID+"/"+v.Reason)
	}
	return out
}

// A released replacement must carry the restore order (or the cold-start
// bypass) of its own job's engine: another engine's order doesn't count, and a
// job that names no engine is a GKE Pod Snapshots job.
func TestI2_ReleasedReplacementCarriesItsJobEngineOrder(t *testing.T) {
	const gateReleased = "I2/GateReleasedWithoutSnapshotOrColdStartBypass"
	restoreOrders := registryRestoreOrders(t, engine.CRIUAPIGroup)
	criuJob := pmjWithEngine("pmj-c", engine.CRIUName)
	gkeJob := pmjWithEngine("pmj-g", engine.GKEPodSnapshotsName)
	noEngineJob := pmjWithEngine("pmj-n", "")

	cases := []struct {
		name string
		job  *pmv1alpha1.PodMigrationJob // nil: the pod's job isn't in the snapshot
		pod  corev1.Pod
		want []string
	}{
		{"CRIU job, only the GKE cold-start bypass", &criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{gkeOrderKey: ""}), []string{gateReleased}},
		{"CRIU job, only a GKE snapshot name", &criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{gkeOrderKey: "snap-1"}), []string{gateReleased}},
		{"CRIU job, CRIU snapshot name", &criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{criuOrderKey: "snap-1"}), nil},
		{"CRIU job, CRIU cold-start bypass", &criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{criuOrderKey: ""}), nil},
		{"GKE job, GKE snapshot name", &gkeJob,
			replacementPod("repl", gvisorClass, "pmj-g", false, podAnn{gkeOrderKey: "snap-1"}), nil},
		{"GKE job, only a CRIU snapshot name", &gkeJob,
			replacementPod("repl", gvisorClass, "pmj-g", false, podAnn{criuOrderKey: "snap-1"}), []string{gateReleased}},
		{"job naming no engine is a GKE job: GKE cold-start bypass", &noEngineJob,
			replacementPod("repl", gvisorClass, "pmj-n", false, podAnn{gkeOrderKey: ""}), nil},
		{"job naming no engine is a GKE job: only a CRIU snapshot name", &noEngineJob,
			replacementPod("repl", gvisorClass, "pmj-n", false, podAnn{criuOrderKey: "snap-1"}), []string{gateReleased}},
		{"job not in the snapshot: any engine's order is accepted", nil,
			replacementPod("repl", runcClass, "pmj-gone", false, podAnn{criuOrderKey: ""}), nil},
		{"job not in the snapshot: no order at all", nil,
			replacementPod("repl", runcClass, "pmj-gone", false, nil), []string{gateReleased}},
		{"still gated: not checked", &criuJob,
			replacementPod("repl", runcClass, "pmj-c", true, nil), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &ReconcileSnapshot{NamespacePods: []corev1.Pod{tc.pod}, RestoreOrders: restoreOrders}
			if tc.job != nil {
				s.NamespacePMJs = []pmv1alpha1.PodMigrationJob{*tc.job}
			}
			vs := EvaluateI2(s)
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI2 = %v, want %v (%+v)", got, tc.want, vs)
			}
			for _, v := range vs {
				if v.PMJName != tc.pod.Annotations[AnnotationAssignedPMJ] || v.PodName != tc.pod.Name {
					t.Errorf("violation names PMJ %q pod %q, want the pod's job %q and pod %q",
						v.PMJName, v.PodName, tc.pod.Annotations[AnnotationAssignedPMJ], tc.pod.Name)
				}
			}
		})
	}
}

// A Succeeded job's replacement must carry the job's snapshot under its own
// engine's key, and a job that names no engine is a GKE Pod Snapshots job.
func TestI2_SucceededReplacementCarriesItsJobEngineOrder(t *testing.T) {
	const missing = "I2/SucceededPodMissingRestoreAnnotation"
	restoreOrders := registryRestoreOrders(t, engine.CRIUAPIGroup)
	cases := []struct {
		name       string
		engineName string
		class      string
		ann        podAnn
		want       []string
	}{
		{"job naming no engine is a GKE job: a CRIU order is not enough", "", gvisorClass,
			podAnn{criuOrderKey: "snap-1"}, []string{missing}},
		{"job naming no engine is a GKE job: GKE order", "", gvisorClass,
			podAnn{gkeOrderKey: "snap-1"}, nil},
		{"CRIU job: a GKE order is not enough", engine.CRIUName, runcClass,
			podAnn{gkeOrderKey: "snap-1"}, []string{missing}},
		{"CRIU job: CRIU order", engine.CRIUName, runcClass,
			podAnn{criuOrderKey: "snap-1"}, nil},
		{"GKE job: GKE order", engine.GKEPodSnapshotsName, gvisorClass,
			podAnn{gkeOrderKey: "snap-1"}, nil},
		{"GKE job: a CRIU order is not enough", engine.GKEPodSnapshotsName, gvisorClass,
			podAnn{criuOrderKey: "snap-1"}, []string{missing}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			job := succeededPMJWithEngine("pmj-1", tc.engineName, "snap-1", "repl")
			// No assigned-pmj annotation, so only the Succeeded check reads the pod.
			pod := replacementPod("repl", tc.class, "", false, tc.ann)
			vs := EvaluateI2(&ReconcileSnapshot{PrimaryPMJ: &job, NamespacePods: []corev1.Pod{pod}, RestoreOrders: restoreOrders})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI2 = %v, want %v (%+v)", got, tc.want, vs)
			}
		})
	}
}

// I1 reads each pod's restore order for the engine of the pod's job, plus the
// GKE Pod Snapshots order of a gVisor pod, which the GKE runtime acts on at
// sandbox creation whatever engine serves the job (#151).
func TestI1_RestoreOrdersFollowTheJobEngine(t *testing.T) {
	const (
		dup     = "I1/DuplicateSnapshotConsumption"
		rebound = "I1/ConsumedSnapshotReboundAfterGateRelease"
	)
	restoreOrders := registryRestoreOrders(t, engine.CRIUAPIGroup)
	criu1 := pmjWithEngine("pmj-c1", engine.CRIUName)
	criu2 := pmjWithEngine("pmj-c2", engine.CRIUName)
	gke1 := pmjWithEngine("pmj-g1", engine.GKEPodSnapshotsName)
	noEngine := pmjWithEngine("pmj-n", "")
	consumedNoEngine := succeededPMJWithEngine("pmj-done-n", "", "snap-1", "repl-a")
	consumedCRIU := succeededPMJWithEngine("pmj-done-c", engine.CRIUName, "snap-1", "repl-a")

	cases := []struct {
		name string
		jobs []pmv1alpha1.PodMigrationJob
		pods []corev1.Pod
		want []string
	}{
		{"two CRIU replacements restore one CRIU snapshot",
			[]pmv1alpha1.PodMigrationJob{criu1, criu2},
			[]corev1.Pod{
				replacementPod("a", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "snap-1"}),
				replacementPod("b", runcClass, "pmj-c2", false, podAnn{criuOrderKey: "snap-1"}),
			}, []string{dup}},
		{"two GKE replacements restore one GKE snapshot",
			[]pmv1alpha1.PodMigrationJob{gke1, noEngine},
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1"}),
				replacementPod("b", gvisorClass, "pmj-n", false, podAnn{gkeOrderKey: "snap-1"}),
			}, []string{dup}},
		{"a stray GKE name on a runc pod of a CRIU job is not a restore",
			[]pmv1alpha1.PodMigrationJob{criu1, gke1},
			[]corev1.Pod{
				replacementPod("a", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "", gkeOrderKey: "snap-g"}),
				replacementPod("b", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-g"}),
			}, nil},
		{"a gVisor pod restores its GKE name even when its job is CRIU",
			[]pmv1alpha1.PodMigrationJob{criu1, gke1},
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-c1", false, podAnn{criuOrderKey: "snap-c", gkeOrderKey: "snap-g"}),
				replacementPod("b", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-g"}),
			}, []string{dup}},
		{"a consumed job naming no engine matches the GKE order only",
			[]pmv1alpha1.PodMigrationJob{consumedNoEngine},
			[]corev1.Pod{replacementPod("b", runcClass, "", false, podAnn{criuOrderKey: "snap-1"})}, nil},
		{"a consumed job naming no engine still catches a GKE rebind",
			[]pmv1alpha1.PodMigrationJob{consumedNoEngine},
			[]corev1.Pod{replacementPod("b", gvisorClass, "", false, podAnn{gkeOrderKey: "snap-1"})}, []string{rebound}},
		{"a consumed CRIU job catches a CRIU rebind",
			[]pmv1alpha1.PodMigrationJob{consumedCRIU},
			[]corev1.Pod{replacementPod("b", runcClass, "", false, podAnn{criuOrderKey: "snap-1"})}, []string{rebound}},
		{"a consumed CRIU job ignores a GKE order of the same name",
			[]pmv1alpha1.PodMigrationJob{consumedCRIU},
			[]corev1.Pod{replacementPod("b", gvisorClass, "", false, podAnn{gkeOrderKey: "snap-1"})}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vs := EvaluateI1(&ReconcileSnapshot{NamespacePMJs: tc.jobs, NamespacePods: tc.pods, RestoreOrders: restoreOrders})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI1 = %v, want %v (%+v)", got, tc.want, vs)
			}
		})
	}
}

// A snapshot is an object of one API group: the same name in two groups is
// two snapshots, while engines that share a group share its snapshots.
func TestI1_SnapshotsOfDifferentAPIGroupsAreDifferent(t *testing.T) {
	const dup = "I1/DuplicateSnapshotConsumption"
	gke1 := pmjWithEngine("pmj-g1", engine.GKEPodSnapshotsName)
	criu1 := pmjWithEngine("pmj-c1", engine.CRIUName)
	opaque1 := pmjWithEngine("pmj-o1", "opaque")
	multiGroup := registryRestoreOrders(t, engine.CRIUAPIGroup)
	// The opaque engine orders a restore with a token that names the snapshot,
	// so no annotation holds the snapshot name and its API group is unknown.
	const opaqueKey = "opaque.example.com/restore-token"
	withOpaque := func(engineName string, pod *corev1.Pod) (string, bool, bool) {
		if engineName != "opaque" {
			return multiGroup(engineName, pod)
		}
		if pod != nil && pod.Annotations[opaqueKey] == "token-for-snap-1" {
			return "snap-1", false, true
		}
		return "", false, false
	}
	cases := []struct {
		name          string
		restoreOrders RestoreOrderFunc
		pods          []corev1.Pod
		want          []string
	}{
		{"same name in the GKE and CRIU groups is two snapshots", multiGroup,
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1"}),
				replacementPod("b", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "snap-1"}),
			}, nil},
		{"same name in the CRIU group on a gVisor pod of a GKE job is two snapshots", multiGroup,
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1", criuOrderKey: ""}),
				replacementPod("b", gvisorClass, "pmj-c1", false, podAnn{gkeOrderKey: "", criuOrderKey: "snap-1"}),
			}, nil},
		{"a GKE job's pod is matched under the GKE key, not a CRIU key holding the same name", multiGroup,
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1", criuOrderKey: "snap-1"}),
				replacementPod("b", runcClass, "pmj-c1", false, podAnn{criuOrderKey: "snap-1"}),
			}, nil},
		{"single-group install: CRIU on podsnapshot.gke.io shares GKE's snapshots", registryRestoreOrders(t, engine.DefaultAPIGroup),
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1"}),
				replacementPod("b", runcClass, "pmj-c1", false, podAnn{gkeOrderKey: "snap-1"}),
			}, []string{dup}},
		{"an order whose API group is unknown can be any group's snapshot", withOpaque,
			[]corev1.Pod{
				replacementPod("a", runcClass, "pmj-o1", false, podAnn{opaqueKey: "token-for-snap-1"}),
				replacementPod("b", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1"}),
			}, []string{dup}},
		{"no RestoreOrders: every order is a GKE order", nil,
			[]corev1.Pod{
				replacementPod("a", gvisorClass, "pmj-g1", false, podAnn{gkeOrderKey: "snap-1"}),
				replacementPod("b", runcClass, "pmj-c1", false, podAnn{gkeOrderKey: "snap-1"}),
			}, []string{dup}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vs := EvaluateI1(&ReconcileSnapshot{
				NamespacePMJs: []pmv1alpha1.PodMigrationJob{gke1, criu1, opaque1},
				NamespacePods: tc.pods,
				RestoreOrders: tc.restoreOrders,
			})
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI1 = %v, want %v (%+v)", got, tc.want, vs)
			}
		})
	}
}

// A released gVisor replacement whose job's engine orders restores under a key
// other than podsnapshot.gke.io/ps-name must carry the GKE cold-start bypass
// podsnapshot.gke.io/ps-name: "", or GKE Pod Snapshots can restore a leftover
// snapshot into its sandbox (#151).
func TestI2_ReleasedGVisorPodOfNonGKEJobCarriesGKEBypass(t *testing.T) {
	const (
		missingBypass = "I2/GVisorPodMissingGKEColdStartBypass"
		gateReleased  = "I2/GateReleasedWithoutSnapshotOrColdStartBypass"
	)
	multiGroup := registryRestoreOrders(t, engine.CRIUAPIGroup)
	singleGroup := registryRestoreOrders(t, engine.DefaultAPIGroup)
	criuJob := pmjWithEngine("pmj-c", engine.CRIUName)
	gkeJob := pmjWithEngine("pmj-g", engine.GKEPodSnapshotsName)
	noEngineJob := pmjWithEngine("pmj-n", "")

	cases := []struct {
		name          string
		restoreOrders RestoreOrderFunc
		job           *pmv1alpha1.PodMigrationJob // nil: the pod's job isn't in the snapshot
		pod           corev1.Pod
		want          []string
	}{
		{"CRIU job, gVisor, CRIU order without a GKE key", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{criuOrderKey: "snap-c"}), []string{missingBypass}},
		{"CRIU job, gVisor, CRIU order and a GKE snapshot name", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{criuOrderKey: "snap-c", gkeOrderKey: "snap-g"}), []string{missingBypass}},
		{"CRIU job, gVisor, CRIU order and the GKE bypass", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{criuOrderKey: "snap-c", gkeOrderKey: ""}), nil},
		{"CRIU job, gVisor, both cold-start bypasses", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{criuOrderKey: "", gkeOrderKey: ""}), nil},
		{"CRIU job, gVisor, no order at all", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, nil), []string{gateReleased, missingBypass}},
		{"no RestoreOrders: every engine orders under the GKE key, so not checked", nil, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{gkeOrderKey: "snap-c"}), nil},
		{"CRIU job, gVisor, still gated: not checked", multiGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", true, nil), nil},
		{"CRIU job, runc: not checked", multiGroup, &criuJob,
			replacementPod("repl", runcClass, "pmj-c", false, podAnn{criuOrderKey: "snap-c"}), nil},
		{"GKE job, gVisor, GKE order", multiGroup, &gkeJob,
			replacementPod("repl", gvisorClass, "pmj-g", false, podAnn{gkeOrderKey: "snap-g"}), nil},
		{"job naming no engine, gVisor, GKE order", multiGroup, &noEngineJob,
			replacementPod("repl", gvisorClass, "pmj-n", false, podAnn{gkeOrderKey: "snap-g"}), nil},
		{"single-group install: CRIU job, gVisor, CRIU order in the shared key", singleGroup, &criuJob,
			replacementPod("repl", gvisorClass, "pmj-c", false, podAnn{gkeOrderKey: "snap-c"}), nil},
		{"job not in the snapshot: not checked", multiGroup, nil,
			replacementPod("repl", gvisorClass, "pmj-gone", false, podAnn{criuOrderKey: "snap-c"}), nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &ReconcileSnapshot{NamespacePods: []corev1.Pod{tc.pod}, RestoreOrders: tc.restoreOrders}
			if tc.job != nil {
				s.NamespacePMJs = []pmv1alpha1.PodMigrationJob{*tc.job}
			}
			vs := EvaluateI2(s)
			if got := violationReasons(vs); !slices.Equal(got, tc.want) {
				t.Fatalf("EvaluateI2 = %v, want %v (%+v)", got, tc.want, vs)
			}
			for _, v := range vs {
				if v.PMJName != tc.pod.Annotations[AnnotationAssignedPMJ] || v.PodName != tc.pod.Name {
					t.Errorf("violation names PMJ %q pod %q, want the pod's job %q and pod %q",
						v.PMJName, v.PodName, tc.pod.Annotations[AnnotationAssignedPMJ], tc.pod.Name)
				}
			}
		})
	}
}
