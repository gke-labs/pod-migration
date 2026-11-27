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

package webhook

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/go-logr/logr/funcr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func httpGetProbe(path string, initialDelay, period int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				Path:   path,
				Port:   intstr.FromInt32(10250),
				Scheme: corev1.URISchemeHTTPS,
			},
		},
		InitialDelaySeconds: initialDelay,
		PeriodSeconds:       period,
		FailureThreshold:    3,
	}
}

func applyReadinessAdmissionPatches(t *testing.T, rawPod []byte, resp admission.Response) *corev1.Pod {
	t.Helper()
	if !resp.Allowed {
		t.Fatalf("admission denied: %v", resp.Result)
	}
	outRaw := rawPod
	if len(resp.Patches) > 0 {
		patchBytes, err := json.Marshal(resp.Patches)
		if err != nil {
			t.Fatalf("marshal patches: %v", err)
		}
		patch, err := jsonpatch.DecodePatch(patchBytes)
		if err != nil {
			t.Fatalf("decode patches %s: %v", string(patchBytes), err)
		}
		outRaw, err = patch.Apply(rawPod)
		if err != nil {
			t.Fatalf("apply patches %s: %v", string(patchBytes), err)
		}
	}
	var out corev1.Pod
	if err := json.Unmarshal(outRaw, &out); err != nil {
		t.Fatalf("unmarshal patched pod: %v", err)
	}
	return &out
}

func newReadinessTestScheme() *runtime.Scheme {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	return scheme
}

func newIndexedClient(scheme *runtime.Scheme, objs ...runtime.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&pmv1alpha1.PodMigrationJob{}, util.PMJParentKeyIndexKey, util.PMJParentKeyIndexValue).
		WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, util.PodAssignedPMJIndexValue).
		WithRuntimeObjects(objs...).
		Build()
}

func activeBarePMJ(podName string, phase pmv1alpha1.PodMigrationJobPhase) *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      util.FormatPMJName(podName, "12345678"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: podName},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: phase,
		},
	}
}

func TestPodGateInjector_ShortensReadinessInitialDelay(t *testing.T) {
	always := corev1.ContainerRestartPolicyAlways
	onFailure := corev1.ContainerRestartPolicyOnFailure

	tests := []struct {
		name                   string
		preserveByFlag         bool
		pod                    *corev1.Pod
		cacheObjs              []runtime.Object
		liveObjs               []runtime.Object
		wantGate               bool
		wantBypass             bool
		wantReadinessDelays    []int32 // per spec.containers; -1 means ReadinessProbe is nil
		wantLivenessDelays     []int32 // per spec.containers; -1 means LivenessProbe is nil
		wantStartupDelays      []int32 // per spec.containers; -1 means StartupProbe is nil
		wantInitReadinessDelay []int32 // per spec.initContainers; -1 means ReadinessProbe is nil
	}{
		{
			name: "gated replacement clears readiness initialDelaySeconds and preserves liveness, startup, and periodSeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "ms-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "metrics-server",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
							LivenessProbe:  httpGetProbe("/livez", 50, 20),
							StartupProbe:   httpGetProbe("/livez", 15, 2),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("ms-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{50},
			wantStartupDelays:   []int32{15},
		},
		{
			name: "multi-container and native sidecar initContainer clear only readiness delays while regular initContainers stay untouched",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "multi-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					InitContainers: []corev1.Container{
						{
							Name:           "sidecar-always",
							RestartPolicy:  &always,
							ReadinessProbe: httpGetProbe("/readyz", 45, 10),
							LivenessProbe:  httpGetProbe("/livez", 45, 10),
						},
						{
							Name:           "init-regular-nil-policy",
							ReadinessProbe: httpGetProbe("/readyz", 25, 10),
						},
						{
							Name:           "init-onfailure",
							RestartPolicy:  &onFailure,
							ReadinessProbe: httpGetProbe("/readyz", 30, 10),
						},
					},
					Containers: []corev1.Container{
						{
							Name:           "app-with-delay",
							ReadinessProbe: httpGetProbe("/readyz", 60, 15),
						},
						{
							Name:          "worker-without-readiness",
							LivenessProbe: httpGetProbe("/livez", 30, 10),
						},
						{
							Name:           "app-zero-delay",
							ReadinessProbe: httpGetProbe("/readyz", 0, 5),
						},
					},
				},
			},
			cacheObjs:              []runtime.Object{activeBarePMJ("multi-pod", pmv1alpha1.PodMigrationJobPhaseEvicting)},
			wantGate:               true,
			wantReadinessDelays:    []int32{0, -1, 0},
			wantLivenessDelays:     []int32{-1, 30, -1},
			wantStartupDelays:      []int32{-1, -1, -1},
			wantInitReadinessDelay: []int32{0, 25, 30},
		},
		{
			name: "APIReader cache-miss fallback also clears readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "lag-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 40, 10),
							LivenessProbe:  httpGetProbe("/livez", 40, 10),
						},
					},
				},
			},
			liveObjs:            []runtime.Object{activeBarePMJ("lag-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{40},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod annotation preserve-readiness-delay=true keeps readiness initialDelaySeconds while still gating",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "optout-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: map[string]string{
						util.AnnotationPreserveReadinessDelay: "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("optout-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod annotation preserve-readiness-delay=false does not opt out",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "false-optout-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: map[string]string{
						util.AnnotationPreserveReadinessDelay: "false",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("false-optout-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name:           "controller PreserveReadinessDelay=true keeps readiness initialDelaySeconds while still gating",
			preserveByFlag: true,
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "flag-off-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("flag-off-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "scale-up pod (no active PMJ) gets cold-start bypass and keeps readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "scaleup-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			wantGate:            false,
			wantBypass:          true,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod created while only Pending PMJ exists takes scale-up path and keeps readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "pending-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("pending-pod", pmv1alpha1.PodMigrationJobPhasePending)},
			wantGate:            false,
			wantBypass:          true,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "Deployment scale-up while Evicting PMJ's origin pod is still running preserves readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "deploy-scaleup-pod",
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1",
					},
					OwnerReferences: []metav1.OwnerReference{
						{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "app-rs", UID: "rs-uid"},
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs: []runtime.Object{
				&appsv1.ReplicaSet{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "app-rs",
						UID:       "rs-uid",
						OwnerReferences: []metav1.OwnerReference{
							{APIVersion: "apps/v1", Kind: "Deployment", Name: "app-deploy", UID: "deploy-uid"},
						},
					},
				},
				&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "app-origin-pod",
						UID:       "origin-uid",
					},
					Status: corev1.PodStatus{Phase: corev1.PodRunning},
				},
				&pmv1alpha1.PodMigrationJob{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Name:      "pmj-app-origin",
						Labels: map[string]string{
							util.LabelParentName:      "app-deploy",
							util.LabelParentKind:      "Deployment",
							util.LabelParentUID:       "deploy-uid",
							util.LabelPodTemplateHash: "hash-v1",
						},
					},
					Spec: pmv1alpha1.PodMigrationJobSpec{
						PodRef:       corev1.LocalObjectReference{Name: "app-origin-pod"},
						TargetPodUID: "origin-uid",
					},
					Status: pmv1alpha1.PodMigrationJobStatus{
						Phase: pmv1alpha1.PodMigrationJobPhaseEvicting,
					},
				},
			},
			wantGate:            false,
			wantBypass:          true,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod not opted in is untouched",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "plain-pod",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("plain-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            false,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod already carrying migration scheduling gate and PMJ assignment clears newly added readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "already-gated-pod",
					Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: map[string]string{util.AnnotationAssignedPMJ: util.FormatPMJName("already-gated-pod", "12345678")},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}},
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("already-gated-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod already carrying migration scheduling gate, PMJ assignment, and zero readiness delay is untouched",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "already-gated-zero-delay-pod",
					Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: map[string]string{util.AnnotationAssignedPMJ: util.FormatPMJName("already-gated-zero-delay-pod", "12345678")},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}},
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 0, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("already-gated-zero-delay-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod with pre-supplied scheduling gate but no assigned-pmj is assigned and clears readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
					Name:      "presupplied-gate-pod",
					Labels:    map[string]string{"pod-migration.gke.io/enabled": "true"},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: "gke.io/pod-migration-gate"}},
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			cacheObjs:           []runtime.Object{activeBarePMJ("presupplied-gate-pod", pmv1alpha1.PodMigrationJobPhaseRestoring)},
			wantGate:            true,
			wantReadinessDelays: []int32{0},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
		{
			name: "pod with pre-existing ps-name and no active PMJ preserves both ps-name and readiness initialDelaySeconds",
			pod: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   "default",
					Name:        "workspace-pod",
					Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
					Annotations: map[string]string{"podsnapshot.gke.io/ps-name": "ws-snap-1"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:           "app",
							ReadinessProbe: httpGetProbe("/readyz", 50, 20),
						},
					},
				},
			},
			wantGate:            false,
			wantBypass:          false,
			wantReadinessDelays: []int32{50},
			wantLivenessDelays:  []int32{-1},
			wantStartupDelays:   []int32{-1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			scheme := newReadinessTestScheme()
			cl := newIndexedClient(scheme, tt.cacheObjs...)
			liveReader := fake.NewClientBuilder().WithScheme(scheme).WithRuntimeObjects(tt.liveObjs...).Build()

			handler := newPodGateInjector(cl, liveReader, !tt.preserveByFlag, admission.NewDecoder(scheme))

			rawPod, err := json.Marshal(tt.pod)
			if err != nil {
				t.Fatalf("marshal input pod: %v", err)
			}
			req := admission.Request{}
			req.Namespace = tt.pod.Namespace
			req.Name = tt.pod.Name
			req.Object = runtime.RawExtension{Raw: rawPod}

			resp := handler.Handle(context.Background(), req)
			got := applyReadinessAdmissionPatches(t, rawPod, resp)

			hasGate := false
			for _, g := range got.Spec.SchedulingGates {
				if g.Name == "gke.io/pod-migration-gate" {
					hasGate = true
				}
			}
			if hasGate != tt.wantGate {
				t.Errorf("hasGate = %v, want %v", hasGate, tt.wantGate)
			}
			psVal, hasPS := got.Annotations["podsnapshot.gke.io/ps-name"]
			gotBypass := hasPS && psVal == ""
			if gotBypass != tt.wantBypass {
				t.Errorf("gotBypass = %v (annotations=%v), want %v", gotBypass, got.Annotations, tt.wantBypass)
			}

			checkProbeDelays := func(label string, containers []corev1.Container, origContainers []corev1.Container, want []int32, get func(*corev1.Container) *corev1.Probe) {
				if want == nil {
					return
				}
				if len(containers) != len(want) {
					t.Fatalf("%s len = %d, want %d", label, len(containers), len(want))
				}
				for i := range containers {
					p := get(&containers[i])
					if want[i] < 0 {
						if p != nil {
							t.Errorf("%s[%d] probe = %+v, want nil", label, i, p)
						}
						continue
					}
					if p == nil {
						t.Fatalf("%s[%d] probe = nil, want initialDelaySeconds=%d", label, i, want[i])
					}
					if p.InitialDelaySeconds != want[i] {
						t.Errorf("%s[%d] initialDelaySeconds = %d, want %d", label, i, p.InitialDelaySeconds, want[i])
					}
					origP := get(&origContainers[i])
					if origP != nil && p.PeriodSeconds != origP.PeriodSeconds {
						t.Errorf("%s[%d] periodSeconds = %d, want preserved %d", label, i, p.PeriodSeconds, origP.PeriodSeconds)
					}
				}
			}

			checkProbeDelays("containers.readinessProbe", got.Spec.Containers, tt.pod.Spec.Containers, tt.wantReadinessDelays, func(c *corev1.Container) *corev1.Probe { return c.ReadinessProbe })
			checkProbeDelays("containers.livenessProbe", got.Spec.Containers, tt.pod.Spec.Containers, tt.wantLivenessDelays, func(c *corev1.Container) *corev1.Probe { return c.LivenessProbe })
			checkProbeDelays("containers.startupProbe", got.Spec.Containers, tt.pod.Spec.Containers, tt.wantStartupDelays, func(c *corev1.Container) *corev1.Probe { return c.StartupProbe })
			checkProbeDelays("initContainers.readinessProbe", got.Spec.InitContainers, tt.pod.Spec.InitContainers, tt.wantInitReadinessDelay, func(c *corev1.Container) *corev1.Probe { return c.ReadinessProbe })
		})
	}
}

func TestPodGateInjector_LogsNonTruePreserveAnnotation(t *testing.T) {
	scheme := newReadinessTestScheme()
	cl := newIndexedClient(scheme, activeBarePMJ("typo-pod", pmv1alpha1.PodMigrationJobPhaseRestoring))
	handler := newPodGateInjector(cl, fake.NewClientBuilder().WithScheme(scheme).Build(), true, admission.NewDecoder(scheme))

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   "default",
			Name:        "typo-pod",
			Labels:      map[string]string{"pod-migration.gke.io/enabled": "true"},
			Annotations: map[string]string{util.AnnotationPreserveReadinessDelay: "True"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", ReadinessProbe: httpGetProbe("/readyz", 50, 20)}},
		},
	}
	rawPod, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod: %v", err)
	}

	var logs []string
	logger := funcr.New(func(prefix, args string) {
		logs = append(logs, prefix+" "+args)
	}, funcr.Options{})
	ctx := log.IntoContext(context.Background(), logger)

	req := admission.Request{}
	req.Namespace = pod.Namespace
	req.Name = pod.Name
	req.Object = runtime.RawExtension{Raw: rawPod}

	resp := handler.Handle(ctx, req)
	got := applyReadinessAdmissionPatches(t, rawPod, resp)
	if got.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds != 0 {
		t.Fatalf("initialDelaySeconds = %d, want 0", got.Spec.Containers[0].ReadinessProbe.InitialDelaySeconds)
	}
	joined := strings.Join(logs, "\n")
	if !strings.Contains(joined, "Ignoring preserve-readiness-delay annotation") {
		t.Errorf("expected log warning about non-\"true\" annotation value, got logs:\n%s", joined)
	}
}
