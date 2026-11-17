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
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestPodMigrationJobReconciler_Pending_RecordsSourceNode checks that a
// pending job records the machine type and version of its source pod's node
// in its status before the checkpoint (#14), that recording never holds the
// job back, and that the values go out with the status write that moves the
// job on, with no write of their own.
func TestPodMigrationJobReconciler_Pending_RecordsSourceNode(t *testing.T) {
	const (
		namespace = "default"
		podName   = "pm-redis-0"
		podUID    = "3f7c789b-f817-48d7-884a-5875c93ccb64"
		nodeName  = "gvisor-node-a"
	)
	jobName := util.FormatPMJName(podName, podUID)

	node := func(labels map[string]string, kubeletVersion string) *corev1.Node {
		return &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: labels},
			Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: kubeletVersion}},
		}
	}
	n2Standard4 := map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"}
	failNodeReads := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, key.Name, errors.New("injected"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}

	tests := []struct {
		name        string
		node        *corev1.Node // nil means the node is gone
		podNodeName string
		podMemory   string            // the pod's memory request, if any
		annotations map[string]string // the job's annotations before the reconcile
		funcs       interceptor.Funcs
		// recordedType and recordedVersion are already in the job's status
		// before the reconcile.
		recordedType, recordedVersion string
		wantType, wantVersion         string
		// wantTimeout, if set, is the expected timeout annotation.
		wantTimeout string
		// wantJobWrites is how many writes the reconcile makes to the job
		// itself, outside its status.
		wantJobWrites int
		// skipsNodeRead: the reconcile doesn't read the node at all.
		skipsNodeRead bool
	}{
		{
			name:        "records the machine type and the version",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			wantType:    "n2-standard-4",
			wantVersion: "v1.37.1-gke.2097000",
		},
		{
			name:        "a node without an instance-type label records the version only",
			node:        node(nil, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			wantVersion: "v1.37.1-gke.2097000",
		},
		{
			name:        "a node that is gone records nothing",
			podNodeName: nodeName,
		},
		{
			name:        "a node read that fails records nothing",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			funcs:       failNodeReads,
		},
		{
			name:          "an unscheduled pod records nothing",
			node:          node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName:   "",
			skipsNodeRead: true,
		},
		{
			name:            "recorded values are not replaced",
			node:            node(map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-8"}, "v1.37.1-gke.2097000"),
			podNodeName:     nodeName,
			recordedType:    "n2-standard-4",
			recordedVersion: "v1.37.1-gke.1697000",
			wantType:        "n2-standard-4",
			wantVersion:     "v1.37.1-gke.1697000",
			skipsNodeRead:   true,
		},
		{
			name:            "only the missing machine type is recorded",
			node:            node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName:     nodeName,
			recordedVersion: "v1.37.1-gke.1697000",
			wantType:        "n2-standard-4",
			wantVersion:     "v1.37.1-gke.1697000",
		},
		{
			name:         "only the missing version is recorded",
			node:         node(map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-8"}, "v1.37.1-gke.2097000"),
			podNodeName:  nodeName,
			recordedType: "n2-standard-4",
			wantType:     "n2-standard-4",
			wantVersion:  "v1.37.1-gke.2097000",
		},
		{
			name:        "a timeout set when the job was created is kept",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			annotations: map[string]string{util.AnnotationMigrationTimeout: "12m"},
			wantType:    "n2-standard-4",
			wantVersion: "v1.37.1-gke.2097000",
			wantTimeout: "12m",
		},
		{
			// The timeout's Update writes the whole job; the recorded values
			// are in its status, so they must survive it.
			name:          "a timeout computed while pending is written too",
			node:          node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName:   nodeName,
			podMemory:     "100Mi", // 10m default + 100Mi / 50MiB/s = 10m2s
			wantType:      "n2-standard-4",
			wantVersion:   "v1.37.1-gke.2097000",
			wantTimeout:   "10m2s",
			wantJobWrites: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: podName, UID: types.UID(podUID)},
				Spec:       corev1.PodSpec{NodeName: tc.podNodeName},
			}
			if tc.podMemory != "" {
				pod.Spec.Containers = []corev1.Container{{
					Name: "app",
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse(tc.podMemory)},
					},
				}}
			}
			pmj := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:         namespace,
					Name:              jobName,
					Annotations:       tc.annotations,
					CreationTimestamp: metav1.Now(),
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: podName},
					TargetPodUID: podUID,
				},
				Status: pmv1alpha1.PodMigrationJobStatus{
					Phase:                    pmv1alpha1.PodMigrationJobPhasePending,
					OriginNodeInstanceType:   tc.recordedType,
					OriginNodeKubeletVersion: tc.recordedVersion,
				},
			}
			objs := []client.Object{pod, pmj}
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			// Count the node reads, the writes to the job itself, and the
			// writes to its status, on top of the case's own interceptors.
			nodeReads, jobWrites, statusWrites := 0, 0, 0
			funcs := tc.funcs
			funcs.Get = func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Node); ok {
					nodeReads++
				}
				if tc.funcs.Get != nil {
					return tc.funcs.Get(ctx, c, key, obj, opts...)
				}
				return c.Get(ctx, key, obj, opts...)
			}
			funcs.Patch = func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok {
					jobWrites++
				}
				if tc.funcs.Patch != nil {
					return tc.funcs.Patch(ctx, c, obj, patch, opts...)
				}
				return c.Patch(ctx, obj, patch, opts...)
			}
			funcs.Update = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok {
					jobWrites++
				}
				if tc.funcs.Update != nil {
					return tc.funcs.Update(ctx, c, obj, opts...)
				}
				return c.Update(ctx, obj, opts...)
			}
			funcs.SubResourcePatch = func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && sub == "status" {
					statusWrites++
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			}
			funcs.SubResourceUpdate = func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && sub == "status" {
					statusWrites++
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithInterceptorFuncs(funcs).
				Build()
			r := &PodMigrationJobReconciler{Client: c, Scheme: scheme}

			key := types.NamespacedName{Namespace: namespace, Name: jobName}
			if _, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key}); err != nil {
				t.Fatalf("Reconcile failed: %v", err)
			}

			got := &pmv1alpha1.PodMigrationJob{}
			if err := c.Get(ctx, key, got); err != nil {
				t.Fatalf("Failed to get PMJ: %v", err)
			}
			if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
				t.Errorf("phase = %s, want %s", got.Status.Phase, pmv1alpha1.PodMigrationJobPhaseSnapshotting)
			}
			if got.Status.OriginNodeName != tc.podNodeName {
				t.Errorf("originNodeName = %q, want %q", got.Status.OriginNodeName, tc.podNodeName)
			}
			if got.Status.OriginNodeInstanceType != tc.wantType {
				t.Errorf("originNodeInstanceType = %q, want %q", got.Status.OriginNodeInstanceType, tc.wantType)
			}
			if got.Status.OriginNodeKubeletVersion != tc.wantVersion {
				t.Errorf("originNodeKubeletVersion = %q, want %q", got.Status.OriginNodeKubeletVersion, tc.wantVersion)
			}
			if tc.wantTimeout != "" && got.Annotations[util.AnnotationMigrationTimeout] != tc.wantTimeout {
				t.Errorf("timeout annotation = %q, want %q", got.Annotations[util.AnnotationMigrationTimeout], tc.wantTimeout)
			}
			if jobWrites != tc.wantJobWrites {
				t.Errorf("writes to the job outside its status = %d, want %d", jobWrites, tc.wantJobWrites)
			}
			// The values ride the one status write that moves the job on.
			if statusWrites != 1 {
				t.Errorf("status writes = %d, want 1", statusWrites)
			}
			if tc.skipsNodeRead && nodeReads != 0 {
				t.Errorf("node reads = %d, want none", nodeReads)
			}
		})
	}
}

// TestPodMigrationJobReconciler_Pending_SourceNodeSurvivesBudgetWait checks
// that a job held in Pending by its disruption budget saves the source node's
// machine type and version with the status write that records the wait, that
// a recheck with nothing new writes nothing, and that the values are kept
// when the budget lets the job move on.
func TestPodMigrationJobReconciler_Pending_SourceNodeSurvivesBudgetWait(t *testing.T) {
	const nodeName = "gvisor-node-a"
	pod := budgetSourcePod()
	pod.Spec.NodeName = nodeName
	job := budgetJob(time.Now(), nil)
	job.Annotations = map[string]string{util.AnnotationMigrationTimeout: "12m"}
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nodeName, Labels: map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-4"}},
		Status:     corev1.NodeStatus{NodeInfo: corev1.NodeSystemInfo{KubeletVersion: "v1.37.1-gke.2097000"}},
	}
	statusWrites := 0
	c := fake.NewClientBuilder().WithScheme(budgetScheme()).
		WithObjects(pod, job, node).
		WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourcePatch: func(ctx context.Context, c client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
				if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok && sub == "status" {
					statusWrites++
				}
				return c.SubResource(sub).Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	checker := &stubBudgetChecker{reason: reasonBudgetExhausted, err: budgetRefusal()}
	recorded := func(t *testing.T, got *pmv1alpha1.PodMigrationJob) {
		t.Helper()
		if got.Status.OriginNodeInstanceType != "n2-standard-4" || got.Status.OriginNodeKubeletVersion != "v1.37.1-gke.2097000" {
			t.Errorf("originNodeInstanceType = %q, originNodeKubeletVersion = %q; want n2-standard-4, v1.37.1-gke.2097000",
				got.Status.OriginNodeInstanceType, got.Status.OriginNodeKubeletVersion)
		}
	}

	_, got := reconcileBudget(t, c, checker, nil)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
		t.Fatalf("phase = %s, want Pending while the budget is exhausted", got.Status.Phase)
	}
	recorded(t, got)
	if statusWrites != 1 {
		t.Errorf("status writes on the first pass = %d, want 1", statusWrites)
	}

	_, _ = reconcileBudget(t, c, checker, nil)
	if statusWrites != 1 {
		t.Errorf("status writes after a recheck with nothing new = %d, want still 1", statusWrites)
	}

	checker.reason, checker.err = reasonBudgetAllowed, nil
	_, got = reconcileBudget(t, c, checker, nil)
	if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
		t.Fatalf("phase = %s, want Snapshotting once the budget allows it", got.Status.Phase)
	}
	recorded(t, got)
}
