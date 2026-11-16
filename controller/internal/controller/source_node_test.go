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
// before the checkpoint (#14), and that recording never holds the job back.
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
	recorded := map[string]string{
		util.AnnotationSourceInstanceType: "n2-standard-4",
		util.AnnotationSourceNodeVersion:  "v1.37.1-gke.2097000",
	}
	nothingRecorded := map[string]string{
		util.AnnotationSourceInstanceType: "",
		util.AnnotationSourceNodeVersion:  "",
	}
	failNodeReads := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.Node); ok {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, key.Name, errors.New("injected"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	failJobPatches := interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*pmv1alpha1.PodMigrationJob); ok {
				return apierrors.NewInternalError(errors.New("injected"))
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}

	tests := []struct {
		name        string
		node        *corev1.Node // nil means the node is gone
		podNodeName string
		podMemory   string            // the pod's memory request, if any
		annotations map[string]string // the job's annotations before the reconcile
		funcs       interceptor.Funcs
		// want maps annotations to their expected values; "" means unset.
		want map[string]string
	}{
		{
			name:        "records the machine type and the version",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			want:        recorded,
		},
		{
			name:        "a node without an instance-type label records the version only",
			node:        node(nil, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			want: map[string]string{
				util.AnnotationSourceInstanceType: "",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.2097000",
			},
		},
		{
			name:        "a node that is gone records nothing",
			podNodeName: nodeName,
			want:        nothingRecorded,
		},
		{
			name:        "a node read that fails records nothing",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			funcs:       failNodeReads,
			want:        nothingRecorded,
		},
		{
			name:        "a patch that fails records nothing and doesn't hold the job back",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			funcs:       failJobPatches,
			want:        nothingRecorded,
		},
		{
			name:        "an unscheduled pod records nothing",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: "",
			want:        nothingRecorded,
		},
		{
			name:        "recorded values are not overwritten",
			node:        node(map[string]string{corev1.LabelInstanceTypeStable: "n2-standard-8"}, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			annotations: map[string]string{
				util.AnnotationSourceInstanceType: "n2-standard-4",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.1697000",
			},
			want: map[string]string{
				util.AnnotationSourceInstanceType: "n2-standard-4",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.1697000",
			},
		},
		{
			name:        "only the missing value is recorded",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			annotations: map[string]string{util.AnnotationSourceNodeVersion: "v1.37.1-gke.1697000"},
			want: map[string]string{
				util.AnnotationSourceInstanceType: "n2-standard-4",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.1697000",
			},
		},
		{
			name:        "empty values are filled in",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			annotations: nothingRecorded,
			want:        recorded,
		},
		{
			name:        "a timeout set when the job was created is kept",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			annotations: map[string]string{util.AnnotationMigrationTimeout: "12m"},
			want: map[string]string{
				util.AnnotationSourceInstanceType: "n2-standard-4",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.2097000",
				util.AnnotationMigrationTimeout:   "12m",
			},
		},
		{
			// The node annotations are patched first, so the timeout's own
			// Update has to start from the patched object or it conflicts.
			name:        "a timeout computed while pending is written too",
			node:        node(n2Standard4, "v1.37.1-gke.2097000"),
			podNodeName: nodeName,
			podMemory:   "100Mi", // 10m default + 100Mi / 50MiB/s = 10m2s
			want: map[string]string{
				util.AnnotationSourceInstanceType: "n2-standard-4",
				util.AnnotationSourceNodeVersion:  "v1.37.1-gke.2097000",
				util.AnnotationMigrationTimeout:   "10m2s",
			},
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
				Status: pmv1alpha1.PodMigrationJobStatus{Phase: pmv1alpha1.PodMigrationJobPhasePending},
			}
			objs := []client.Object{pod, pmj}
			if tc.node != nil {
				objs = append(objs, tc.node)
			}
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(objs...).
				WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
				WithInterceptorFuncs(tc.funcs).
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
			for annotation, want := range tc.want {
				value, ok := got.Annotations[annotation]
				if want == "" {
					if ok {
						t.Errorf("annotation %s = %q, want it unset", annotation, value)
					}
					continue
				}
				if value != want {
					t.Errorf("annotation %s = %q, want %q", annotation, value, want)
				}
			}
		})
	}
}
