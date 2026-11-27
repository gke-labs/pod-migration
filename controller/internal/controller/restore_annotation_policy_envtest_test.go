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
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestRestoreAnnotationPolicies_Envtest applies the two admission policies of
// #233, exactly as controller/deploy.yaml and the CRIU engine's manifest ship
// them, to a real API server, and updates pods as the controller, as the CRIU
// node agent (each bound to the ClusterRole its manifest gives it), as a
// tenant who may edit pods, and as a cluster admin.
//
//   - Restore order (<group>/ps-name, C10 R1): on a pod opted into migration,
//     only the controller sets, changes or removes it, for any engine group.
//     Every other update goes through, including removing the opt-in label.
//   - Restore outcome (pod-migrate.io/psengine-restore): only the node agent
//     records it, once, as restored or failed.
//
// Both hold for a write to pods/status too, which can change annotations.
func TestRestoreAnnotationPolicies_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest restore annotation policy test")
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
	_ = rbacv1.AddToScheme(scheme)
	_ = admissionregistrationv1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	ctx := context.Background()

	// The tenant namespace of the test's pods.
	const ns = "apps"

	// The identities, each with the RBAC that its manifest gives it. A
	// tenant may edit pods, like a namespace's editors.
	agentRole := &rbacv1.ClusterRole{}
	manifestObject(t, criuManifestPath(), "ClusterRole", "pod-migrate-psengine-agent", agentRole)
	tenantRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "tenant-pod-editor"},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""},
			Resources: []string{"pods"},
			Verbs:     []string{"get", "list", "create", "update", "patch"},
		}},
	}
	// A status writer may write pods/status, like a kubelet or a controller
	// that sets pod conditions. Such a write can change annotations too.
	statusRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "pod-status-writer"},
		Rules: []rbacv1.PolicyRule{
			{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list"}},
			{APIGroups: []string{""}, Resources: []string{"pods/status"}, Verbs: []string{"get", "update", "patch"}},
		},
	}
	// Impostors: service accounts that only look like the allowed ones. Each
	// policy compares the whole username, so a service account with the
	// controller's name in a tenant namespace, or the engine's other service
	// account, must be denied like anyone else. They may edit pods, so a
	// denial is the policy's and not RBAC's.
	podEditorRole := func(name string) *rbacv1.ClusterRole {
		return &rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Rules: []rbacv1.PolicyRule{
				{APIGroups: []string{""}, Resources: []string{"pods"}, Verbs: []string{"get", "list", "update", "patch"}},
				{APIGroups: []string{""}, Resources: []string{"pods/status"}, Verbs: []string{"get", "update", "patch"}},
			},
		}
	}
	const (
		controllerImpostorUsername = "system:serviceaccount:" + ns + ":pod-migration-controller"
		engineControllerUsername   = "system:serviceaccount:pod-migration-criu-system:pod-migrate-psengine-controller"
	)
	controllerImpostor := clientForRole(ctx, t, testEnv, admin, scheme, controllerImpostorUsername, podEditorRole("controller-impostor-pod-editor"))
	engineController := clientForRole(ctx, t, testEnv, admin, scheme, engineControllerUsername, podEditorRole("engine-controller-pod-editor"))
	controller := clientForRole(ctx, t, testEnv, admin, scheme, controllerUsername, controllerClusterRole(t))
	statusWriter := clientForRole(ctx, t, testEnv, admin, scheme, "status-writer", statusRole)
	agent := clientForRole(ctx, t, testEnv, admin, scheme, criuAgentUsername, agentRole)
	tenant := clientForRole(ctx, t, testEnv, admin, scheme, "tenant", tenantRole)

	// The policies and their bindings, as shipped.
	orderPolicy, orderBinding := admissionPolicy(t, controllerManifestPath(), restoreOrderPolicyName)
	statusPolicy, statusBinding := admissionPolicy(t, criuManifestPath(), restoreStatusPolicyName)
	for _, obj := range []client.Object{orderPolicy, orderBinding, statusPolicy, statusBinding} {
		if err := admin.Create(ctx, obj); err != nil {
			t.Fatalf("creating %T %s: %v", obj, obj.GetName(), err)
		}
	}

	if err := admin.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: ns,
			Labels: map[string]string{
				util.LabelNamespaceManaged: "true",
			},
		},
	}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	optedIn := map[string]string{optInLabel: "true"}
	gkeOrder := engine.RestoreAnnotation(engine.DefaultAPIGroup)
	criuOrder := engine.RestoreAnnotation(engine.DefaultCRIUAPIGroup)
	const status = engine.CRIURestoreStatusAnnotation
	// The reason of a failed restore (#244), guarded by the same policy.
	const reason = engine.CRIURestoreReasonAnnotation

	// patch reads the pod as it is now, and sends mutate's change as c.
	patch := func(t *testing.T, c client.Client, pod *corev1.Pod, mutate func(*corev1.Pod)) error {
		t.Helper()
		current := &corev1.Pod{}
		if err := admin.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatalf("reading pod %s: %v", pod.Name, err)
		}
		base := current.DeepCopy()
		mutate(current)
		return c.Patch(ctx, current, client.MergeFrom(base))
	}

	newPod := func(t *testing.T, namespace, name string, labels, annotations map[string]string) *corev1.Pod {
		t.Helper()
		initialAnnotations := make(map[string]string)
		var restoreOrders map[string]string
		for k, v := range annotations {
			if strings.HasSuffix(k, "/ps-name") && k != gkeOrder && v != "" {
				if restoreOrders == nil {
					restoreOrders = make(map[string]string)
				}
				restoreOrders[k] = v
			} else {
				initialAnnotations[k] = v
			}
		}
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, Labels: labels, Annotations: initialAnnotations},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/app:1", Command: []string{"/app"}}},
			},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod %s/%s: %v", namespace, name, err)
		}
		if len(restoreOrders) > 0 {
			if err := patch(t, controller, pod, func(p *corev1.Pod) {
				if p.Annotations == nil {
					p.Annotations = make(map[string]string)
				}
				for k, v := range restoreOrders {
					p.Annotations[k] = v
				}
			}); err != nil {
				t.Fatalf("setting restore order on pod %s/%s: %v", namespace, name, err)
			}
		}
		return pod
	}
	// update is patch with a full update, which is how the node agent writes.
	update := func(t *testing.T, c client.Client, pod *corev1.Pod, mutate func(*corev1.Pod)) error {
		t.Helper()
		current := &corev1.Pod{}
		if err := admin.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatalf("reading pod %s: %v", pod.Name, err)
		}
		mutate(current)
		return c.Update(ctx, current)
	}
	// patchStatus is patch through the status subresource.
	patchStatus := func(t *testing.T, c client.Client, pod *corev1.Pod, mutate func(*corev1.Pod)) error {
		t.Helper()
		current := &corev1.Pod{}
		if err := admin.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatalf("reading pod %s: %v", pod.Name, err)
		}
		base := current.DeepCopy()
		mutate(current)
		return c.Status().Patch(ctx, current, client.MergeFrom(base))
	}
	// setCondition is a status write like the kubelet's: no annotation in it.
	setCondition := func(reason string) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			p.Status.Conditions = []corev1.PodCondition{{
				Type:               corev1.PodConditionType("example.com/checked"),
				Status:             corev1.ConditionTrue,
				Reason:             reason,
				LastTransitionTime: metav1.Now(),
			}}
		}
	}
	set := func(key, value string) func(*corev1.Pod) {
		return func(p *corev1.Pod) {
			if p.Annotations == nil {
				p.Annotations = map[string]string{}
			}
			p.Annotations[key] = value
		}
	}
	unset := func(key string) func(*corev1.Pod) {
		return func(p *corev1.Pod) { delete(p.Annotations, key) }
	}
	annotation := func(t *testing.T, pod *corev1.Pod, key string) (string, bool) {
		t.Helper()
		current := &corev1.Pod{}
		if err := admin.Get(ctx, client.ObjectKeyFromObject(pod), current); err != nil {
			t.Fatalf("reading pod %s: %v", pod.Name, err)
		}
		v, ok := current.Annotations[key]
		return v, ok
	}
	// deniedBy reports whether err is the given policy's denial, and not,
	// for example, an RBAC one.
	deniedBy := func(err error, policy string) bool {
		return err != nil && apierrors.IsForbidden(err) && strings.Contains(err.Error(), policy)
	}
	wantAllowed := func(t *testing.T, err error, what string) {
		t.Helper()
		if err != nil {
			t.Errorf("%s: denied, want allowed: %v", what, err)
		}
	}
	wantDenied := func(t *testing.T, err error, policy, what string) {
		t.Helper()
		if !deniedBy(err, policy) {
			t.Errorf("%s: got %v, want a denial by %s", what, err, policy)
		}
	}

	// The API server starts enforcing a new policy a moment after it and its
	// binding are created. Wait until both policies deny a tenant's write.
	waitEnforced := func(policy, key string) {
		t.Helper()
		probe := newPod(t, ns, "probe-"+policy, optedIn, nil)
		deadline := time.Now().Add(60 * time.Second)
		for i := 0; ; i++ {
			err := patch(t, tenant, probe, set(key, fmt.Sprintf("probe-%d", i)))
			if deniedBy(err, policy) {
				return
			}
			if err != nil {
				t.Fatalf("probing %s: %v", policy, err)
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s still doesn't deny a tenant's write to %s after 60s", policy, key)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	waitEnforced(restoreOrderPolicyName, gkeOrder)
	waitEnforced(restoreStatusPolicyName, status)

	// From here on both policies are enforced. A mistake in one of their
	// expressions would deny every matching update (failurePolicy: Fail), so
	// the allowed cases below also show that the expressions evaluate.

	t.Run("restore order", func(t *testing.T) {
		t.Run("a tenant can't set, change or remove it on an opted-in pod", func(t *testing.T) {
			for _, key := range []string{gkeOrder, criuOrder, "snapshots.example.com/ps-name"} {
				name := "order-" + strings.ReplaceAll(strings.SplitN(key, "/", 2)[0], ".", "-")
				fresh := newPod(t, ns, name+"-fresh", optedIn, nil)
				wantDenied(t, patch(t, tenant, fresh, set(key, "snap-evil")), restoreOrderPolicyName, "setting "+key)
				wantDenied(t, patch(t, tenant, fresh, set(key, "")), restoreOrderPolicyName, "setting "+key+" to the cold-start bypass")
				if v, ok := annotation(t, fresh, key); ok {
					t.Errorf("%s = %q after the denials, want it unset", key, v)
				}

				// Another annotation stays on the pod, so that removing the
				// order leaves the pod with annotations: the policy must
				// notice a key that is gone, not only a pod without any.
				ordered := newPod(t, ns, name+"-ordered", optedIn, map[string]string{key: "snap-a", "example.com/keep": "true"})
				wantDenied(t, patch(t, tenant, ordered, set(key, "snap-evil")), restoreOrderPolicyName, "changing "+key)
				wantDenied(t, patch(t, tenant, ordered, set(key, "")), restoreOrderPolicyName, "changing "+key+" to the cold-start bypass")
				wantDenied(t, patch(t, tenant, ordered, unset(key)), restoreOrderPolicyName, "removing "+key)
				wantDenied(t, update(t, tenant, ordered, set(key, "snap-evil")), restoreOrderPolicyName, "changing "+key+" with a full update")
				if v, _ := annotation(t, ordered, key); v != "snap-a" {
					t.Errorf("%s = %q after the denials, want snap-a", key, v)
				}
			}
		})

		t.Run("a cluster admin can't either while the binding exists", func(t *testing.T) {
			pod := newPod(t, ns, "order-admin", optedIn, map[string]string{gkeOrder: "snap-a"})
			err := patch(t, admin, pod, set(gkeOrder, "snap-b"))
			wantDenied(t, err, restoreOrderPolicyName, "an admin changing the restore order")
			// The message as the README quotes it.
			const want = "ValidatingAdmissionPolicy 'pod-migration-restore-order' with binding 'pod-migration-restore-order' denied request: " +
				"only the Pod Migration controller may set, change or remove a restore order (<group>/ps-name) on an existing pod that is opted into migration"
			if err != nil && !strings.Contains(err.Error(), want) {
				t.Errorf("denial message = %q, want it to contain %q", err.Error(), want)
			}
		})

		t.Run("a service account that only looks like the controller can't either", func(t *testing.T) {
			pod := newPod(t, ns, "order-impostor", optedIn, map[string]string{gkeOrder: "snap-a", "example.com/keep": "true"})
			for who, c := range map[string]client.Client{
				"a same-named service account in a tenant namespace": controllerImpostor,
				"the CRIU engine controller's service account":       engineController,
				"the CRIU node agent":                                agent,
			} {
				wantDenied(t, patch(t, c, pod, set(gkeOrder, "snap-evil")), restoreOrderPolicyName, who+" changing the restore order")
				wantDenied(t, patch(t, c, pod, set(criuOrder, "snap-evil")), restoreOrderPolicyName, who+" setting another group's restore order")
				wantDenied(t, patch(t, c, pod, unset(gkeOrder)), restoreOrderPolicyName, who+" removing the restore order")
			}
			wantDenied(t, patchStatus(t, controllerImpostor, pod, set(gkeOrder, "snap-evil")), restoreOrderPolicyName,
				"a same-named service account changing the restore order through pods/status")
			if v, _ := annotation(t, pod, gkeOrder); v != "snap-a" {
				t.Errorf("%s = %q after the denials, want snap-a", gkeOrder, v)
			}
			// They aren't locked out of the rest of the pod.
			wantAllowed(t, patch(t, controllerImpostor, pod, set("example.com/owner", "team-a")), "a same-named service account adding another annotation")
		})

		t.Run("the controller orders a restore and completes it", func(t *testing.T) {
			for _, key := range []string{gkeOrder, criuOrder} {
				name := "order-ctrl-" + strings.ReplaceAll(strings.SplitN(key, "/", 2)[0], ".", "-")
				pod := newPod(t, ns, name, optedIn, nil)
				wantAllowed(t, patch(t, controller, pod, set(key, "snap-a")), "the controller ordering a restore with "+key)
				// Completing a restore replaces the order with the
				// cold-start bypass and records the snapshot (#152).
				wantAllowed(t, patch(t, controller, pod, func(p *corev1.Pod) {
					p.Annotations[key] = ""
					p.Annotations[util.AnnotationRestoreCompleted] = "snap-a"
				}), "the controller completing the restore")
				if v, ok := annotation(t, pod, key); !ok || v != "" {
					t.Errorf("%s = %q (set: %v) after the controller's writes, want the cold-start bypass", key, v, ok)
				}
			}
		})

		t.Run("updates that leave it alone go through", func(t *testing.T) {
			pod := newPod(t, ns, "order-unrelated", optedIn, map[string]string{gkeOrder: "snap-a", criuOrder: ""})
			wantAllowed(t, patch(t, tenant, pod, set("example.com/owner", "team-a")), "adding another annotation")
			wantAllowed(t, patch(t, tenant, pod, func(p *corev1.Pod) { p.Labels["tier"] = "web" }), "adding a label")
			wantAllowed(t, update(t, tenant, pod, func(p *corev1.Pod) { p.Labels["tier"] = "api" }), "a full update that changes a label")
			wantAllowed(t, patch(t, tenant, pod, func(p *corev1.Pod) {
				p.Spec.Containers[0].Image = "registry.example.com/app:2"
			}), "changing the image")
			// Keys that only look like a restore order.
			wantAllowed(t, patch(t, tenant, pod, set("example.com/ps-name-backup", "x")), "an annotation that only starts like a restore order")
			wantAllowed(t, patch(t, tenant, pod, set("ps-name", "x")), "an annotation named ps-name without a group")
			// Sending the restore orders unchanged is no change.
			wantAllowed(t, patch(t, tenant, pod, func(p *corev1.Pod) {
				p.Annotations[gkeOrder] = "snap-a"
				p.Annotations["example.com/owner"] = "team-b"
			}), "a patch that repeats the restore order as it is")
		})

		t.Run("the status subresource is no way around it", func(t *testing.T) {
			pod := newPod(t, ns, "order-via-status", optedIn, map[string]string{gkeOrder: "snap-a", "example.com/keep": "true"})
			wantDenied(t, patchStatus(t, statusWriter, pod, set(gkeOrder, "snap-evil")), restoreOrderPolicyName, "changing the order through pods/status")
			wantDenied(t, patchStatus(t, statusWriter, pod, set(criuOrder, "snap-evil")), restoreOrderPolicyName, "setting another group's order through pods/status")
			wantDenied(t, patchStatus(t, statusWriter, pod, unset(gkeOrder)), restoreOrderPolicyName, "removing the order through pods/status")
			wantDenied(t, patchStatus(t, statusWriter, pod, func(p *corev1.Pod) {
				setCondition("WithOrder")(p)
				p.Annotations[gkeOrder] = "snap-evil"
			}), restoreOrderPolicyName, "a status write that also changes the order")
			if v, _ := annotation(t, pod, gkeOrder); v != "snap-a" {
				t.Errorf("%s = %q after the denials, want snap-a", gkeOrder, v)
			}
			if _, ok := annotation(t, pod, criuOrder); ok {
				t.Errorf("%s was set through pods/status", criuOrder)
			}

			// Status writes that leave the orders alone go through: the
			// kubelet's, and the controller's fence (#131).
			wantAllowed(t, patchStatus(t, statusWriter, pod, setCondition("Kubelet")), "a status write without annotations")
			wantAllowed(t, patchStatus(t, statusWriter, pod, set("example.com/seen", "true")), "a status write that changes another annotation")
			wantAllowed(t, patchStatus(t, controller, pod, setCondition("Fence")), "the controller's status write")
			unannotated := newPod(t, ns, "order-via-status-plain", optedIn, nil)
			wantAllowed(t, patchStatus(t, statusWriter, unannotated, setCondition("Kubelet")), "a status write on a pod without annotations")
		})

		t.Run("opting out stays possible after a restore", func(t *testing.T) {
			pod := newPod(t, ns, "order-opt-out", optedIn, map[string]string{
				gkeOrder:                        "",
				util.AnnotationRestoreCompleted: "snap-a",
			})
			wantAllowed(t, patch(t, tenant, pod, func(p *corev1.Pod) {
				delete(p.Labels, optInLabel)
			}), "removing the opt-in label")
			// The pod is out of migration now, and a restore order on it is
			// a restore outside a migration (C10 R1): not this policy's.
			wantAllowed(t, patch(t, tenant, pod, set(gkeOrder, "snap-own")), "a restore order on a pod that opted out")
		})

		t.Run("the opt-in label can't be changed together with the order", func(t *testing.T) {
			leaving := newPod(t, ns, "order-leaving", optedIn, map[string]string{gkeOrder: "snap-a"})
			wantDenied(t, patch(t, tenant, leaving, func(p *corev1.Pod) {
				delete(p.Labels, optInLabel)
				p.Annotations[gkeOrder] = "snap-evil"
			}), restoreOrderPolicyName, "opting out and changing the order in one update")

			joining := newPod(t, ns, "order-joining", nil, nil)
			wantDenied(t, patch(t, tenant, joining, func(p *corev1.Pod) {
				p.Labels = map[string]string{optInLabel: "true"}
				p.Annotations = map[string]string{gkeOrder: "snap-evil"}
			}), restoreOrderPolicyName, "opting in and setting an order in one update")
		})

		t.Run("pods outside its scope are left alone", func(t *testing.T) {
			// Not opted in: for example a workspace restore that sets
			// ps-name itself (#55).
			outside := newPod(t, ns, "order-not-opted-in", map[string]string{"app": "notebook"}, map[string]string{gkeOrder: "ws-snap-1"})
			wantAllowed(t, patch(t, tenant, outside, set(gkeOrder, "ws-snap-2")), "a restore order on a pod that isn't opted in")
			// An excluded namespace, like the webhooks'.
			system := newPod(t, "kube-system", "order-system", optedIn, nil)
			wantAllowed(t, patch(t, tenant, system, set(gkeOrder, "snap-a")), "a restore order in kube-system")
		})

		t.Run("non-GKE restore annotations on CREATE are denied while GKE restore annotations are allowed", func(t *testing.T) {
			// Tenant attempting to create a pod carrying a spoofed CRIU restore order (#303).
			criuPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   ns,
					Name:        "order-create-criu",
					Labels:      optedIn,
					Annotations: map[string]string{criuOrder: "snap-spoofed"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/app:1", Command: []string{"/app"}}},
				},
			}
			wantDenied(t, tenant.Create(ctx, criuPod), restoreOrderPolicyName, "tenant creating pod with non-GKE restore order")

			// Custom engine restore order is also denied on CREATE.
			customKey := "snapshots.example.com/ps-name"
			customPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   ns,
					Name:        "order-create-custom",
					Labels:      optedIn,
					Annotations: map[string]string{customKey: "snap-custom"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/app:1", Command: []string{"/app"}}},
				},
			}
			wantDenied(t, admin.Create(ctx, customPod), restoreOrderPolicyName, "admin creating pod with custom engine restore order")

			// GKE restore annotation on CREATE (Kubeflow resume flow) is allowed.
			gkePod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   ns,
					Name:        "order-create-gke-resume",
					Labels:      optedIn,
					Annotations: map[string]string{gkeOrder: "kubeflow-checkpoint-1"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/app:1", Command: []string{"/app"}}},
				},
			}
			wantAllowed(t, tenant.Create(ctx, gkePod), "tenant creating pod with GKE restore order (Kubeflow flow)")

			// In an unmanaged namespace (without pod-migration.gke.io/managed: "true"), non-GKE restore order is allowed.
			unmanagedNS := "unmanaged-tenant-ns"
			if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: unmanagedNS}}); err != nil {
				t.Fatalf("creating unmanaged namespace: %v", err)
			}
			unmanagedPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace:   unmanagedNS,
					Name:        "order-create-unmanaged",
					Labels:      optedIn,
					Annotations: map[string]string{criuOrder: "snap-unmanaged"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "registry.example.com/app:1", Command: []string{"/app"}}},
				},
			}
			wantAllowed(t, admin.Create(ctx, unmanagedPod), "creating pod with CRIU restore order in an unmanaged namespace")
		})
	})

	t.Run("restore outcome", func(t *testing.T) {
		t.Run("the node agent records it once", func(t *testing.T) {
			for _, outcome := range []string{engine.CRIURestoreStatusRestored, engine.CRIURestoreStatusFailed} {
				pod := newPod(t, ns, "status-agent-"+outcome, optedIn, map[string]string{
					criuOrder:                     "snap-a",
					engine.CRIUInjectedAnnotation: "true",
				})
				wantAllowed(t, update(t, agent, pod, func(p *corev1.Pod) {
					p.Annotations[status] = outcome
					p.Annotations["pod-migrate.io/psengine-snapshot"] = "snap-a"
				}), "the agent recording "+outcome)

				for _, other := range []string{engine.CRIURestoreStatusRestored, engine.CRIURestoreStatusFailed, ""} {
					if other == outcome {
						continue
					}
					wantDenied(t, update(t, agent, pod, set(status, other)), restoreStatusPolicyName,
						fmt.Sprintf("the agent changing %s to %q", outcome, other))
				}
				wantDenied(t, update(t, agent, pod, unset(status)), restoreStatusPolicyName, "the agent removing the outcome")
				wantDenied(t, patch(t, tenant, pod, set(status, engine.CRIURestoreStatusFailed+"x")), restoreStatusPolicyName, "a tenant changing the outcome")
				wantDenied(t, patch(t, tenant, pod, unset(status)), restoreStatusPolicyName, "a tenant removing the outcome")
				wantDenied(t, patch(t, admin, pod, unset(status)), restoreStatusPolicyName, "an admin removing the outcome")
				if v, _ := annotation(t, pod, status); v != outcome {
					t.Errorf("%s = %q after the denials, want %s", status, v, outcome)
				}

				// The pod stays editable otherwise, also by the agent.
				wantAllowed(t, patch(t, tenant, pod, set("example.com/owner", "team-a")), "a tenant adding another annotation")
				wantAllowed(t, update(t, agent, pod, set("pod-migrate.io/note", "x")), "the agent's later full update that leaves the outcome")
			}
		})

		t.Run("the node agent records it over an empty value", func(t *testing.T) {
			pod := newPod(t, ns, "status-empty", optedIn, map[string]string{status: ""})
			wantAllowed(t, update(t, agent, pod, set(status, engine.CRIURestoreStatusRestored)), "the agent recording restored over an empty value")
		})

		t.Run("the node agent records only restored or failed", func(t *testing.T) {
			pod := newPod(t, ns, "status-values", optedIn, nil)
			for _, value := range []string{"", "Restored", "restored ", "ok"} {
				wantDenied(t, update(t, agent, pod, set(status, value)), restoreStatusPolicyName, fmt.Sprintf("the agent recording %q", value))
			}
			if v, ok := annotation(t, pod, status); ok {
				t.Errorf("%s = %q after the denials, want it unset", status, v)
			}
		})

		t.Run("nobody else records it", func(t *testing.T) {
			pod := newPod(t, ns, "status-others", optedIn, nil)
			for who, c := range map[string]client.Client{
				"a tenant":       tenant,
				"the controller": controller,
				"an admin":       admin,
				// Service accounts that only look like the node agent's.
				"the CRIU engine controller's service account":       engineController,
				"a same-named service account in a tenant namespace": controllerImpostor,
			} {
				wantDenied(t, patch(t, c, pod, set(status, engine.CRIURestoreStatusRestored)), restoreStatusPolicyName, who+" recording restored")
				wantDenied(t, patch(t, c, pod, set(status, "")), restoreStatusPolicyName, who+" setting an empty outcome")
			}
			// It is the engine's annotation on any pod, opted in or not.
			plain := newPod(t, ns, "status-not-opted-in", nil, nil)
			wantDenied(t, patch(t, tenant, plain, set(status, engine.CRIURestoreStatusRestored)), restoreStatusPolicyName, "a tenant recording restored on a pod that isn't opted in")
			if v, ok := annotation(t, pod, status); ok {
				t.Errorf("%s = %q after the denials, want it unset", status, v)
			}
		})

		t.Run("the status subresource is no way around it", func(t *testing.T) {
			pod := newPod(t, ns, "status-via-status", optedIn, nil)
			wantDenied(t, patchStatus(t, statusWriter, pod, set(status, engine.CRIURestoreStatusRestored)), restoreStatusPolicyName, "recording an outcome through pods/status")
			if v, ok := annotation(t, pod, status); ok {
				t.Errorf("%s = %q after the denial, want it unset", status, v)
			}
			wantAllowed(t, patchStatus(t, statusWriter, pod, setCondition("Kubelet")), "a status write without annotations")

			recorded := newPod(t, ns, "status-via-status-recorded", optedIn, map[string]string{status: engine.CRIURestoreStatusRestored})
			wantDenied(t, patchStatus(t, statusWriter, recorded, set(status, engine.CRIURestoreStatusFailed)), restoreStatusPolicyName, "changing the outcome through pods/status")
			wantDenied(t, patchStatus(t, statusWriter, recorded, unset(status)), restoreStatusPolicyName, "removing the outcome through pods/status")
			wantAllowed(t, patchStatus(t, statusWriter, recorded, setCondition("Kubelet")), "a status write on a pod with an outcome")
			if v, _ := annotation(t, recorded, status); v != engine.CRIURestoreStatusRestored {
				t.Errorf("%s = %q after the denials, want restored", status, v)
			}
		})

		t.Run("excluded namespaces are left alone", func(t *testing.T) {
			system := newPod(t, "kube-system", "status-system", nil, nil)
			wantAllowed(t, patch(t, tenant, system, set(status, "anything")), "the annotation in kube-system")
		})

		// The reason of a failure (#244): the agent writes it with failed, in
		// the same update, and nobody changes or removes it afterwards.
		const why = "ArchiveSignatureInvalid"
		injected := func(t *testing.T, name string, extra map[string]string) *corev1.Pod {
			t.Helper()
			anns := map[string]string{criuOrder: "snap-a", engine.CRIUInjectedAnnotation: "true"}
			for k, v := range extra {
				anns[k] = v
			}
			return newPod(t, ns, name, optedIn, anns)
		}
		failWith := func(r string) func(*corev1.Pod) {
			return func(p *corev1.Pod) {
				set(status, engine.CRIURestoreStatusFailed)(p)
				set(reason, r)(p)
			}
		}

		t.Run("the node agent records a failure's reason with it, once", func(t *testing.T) {
			pod := injected(t, "reason-agent", nil)
			wantAllowed(t, update(t, agent, pod, failWith(why)), "the agent recording failed with its reason")
			if v, _ := annotation(t, pod, reason); v != why {
				t.Fatalf("%s = %q, want %s", reason, v, why)
			}

			wantDenied(t, update(t, agent, pod, set(reason, "ArchiveBindingMismatch")), restoreStatusPolicyName, "the agent changing the reason later")
			wantDenied(t, update(t, agent, pod, unset(reason)), restoreStatusPolicyName, "the agent removing the reason")
			wantDenied(t, patch(t, tenant, pod, set(reason, "RestoreFailed")), restoreStatusPolicyName, "a tenant changing the reason")
			wantDenied(t, patch(t, tenant, pod, unset(reason)), restoreStatusPolicyName, "a tenant removing the reason")
			wantDenied(t, patch(t, admin, pod, set(reason, "RestoreFailed")), restoreStatusPolicyName, "an admin changing the reason")
			wantDenied(t, patchStatus(t, statusWriter, pod, set(reason, "RestoreFailed")), restoreStatusPolicyName, "changing the reason through pods/status")
			if v, _ := annotation(t, pod, reason); v != why {
				t.Errorf("%s = %q after the denials, want %s", reason, v, why)
			}
			// Updates that leave both alone still go through.
			wantAllowed(t, update(t, agent, pod, set("pod-migrate.io/note", "x")), "the agent's later update that leaves the reason")
			wantAllowed(t, patch(t, tenant, pod, set("example.com/owner", "team-a")), "a tenant adding another annotation")
		})

		t.Run("nobody else records a reason", func(t *testing.T) {
			fresh := newPod(t, ns, "reason-others", optedIn, nil)
			failed := newPod(t, ns, "reason-others-failed", optedIn, map[string]string{status: engine.CRIURestoreStatusFailed})
			for who, c := range map[string]client.Client{
				"a tenant":       tenant,
				"the controller": controller,
				"an admin":       admin,
				"the CRIU engine controller's service account":       engineController,
				"a same-named service account in a tenant namespace": controllerImpostor,
			} {
				wantDenied(t, patch(t, c, fresh, set(reason, why)), restoreStatusPolicyName, who+" recording a reason")
				wantDenied(t, patch(t, c, fresh, failWith(why)), restoreStatusPolicyName, who+" recording failed with a reason")
				wantDenied(t, patch(t, c, failed, set(reason, why)), restoreStatusPolicyName, who+" adding a reason to a failed outcome")
			}
			wantDenied(t, patchStatus(t, statusWriter, fresh, set(reason, why)), restoreStatusPolicyName, "recording a reason through pods/status")
			if v, ok := annotation(t, fresh, reason); ok {
				t.Errorf("%s = %q after the denials, want it unset", reason, v)
			}
		})

		t.Run("a reason goes only with failed, in the same update", func(t *testing.T) {
			pod := injected(t, "reason-values", nil)
			wantDenied(t, update(t, agent, pod, func(p *corev1.Pod) {
				set(status, engine.CRIURestoreStatusRestored)(p)
				set(reason, why)(p)
			}), restoreStatusPolicyName, "the agent recording restored with a reason")
			wantDenied(t, update(t, agent, pod, set(reason, why)), restoreStatusPolicyName, "the agent recording a reason without an outcome")
			if _, ok := annotation(t, pod, reason); ok {
				t.Errorf("%s set after the denials", reason)
			}

			// Recorded without a reason, the outcome can't get one later.
			wantAllowed(t, update(t, agent, pod, set(status, engine.CRIURestoreStatusFailed)), "the agent recording failed without a reason")
			wantDenied(t, update(t, agent, pod, set(reason, why)), restoreStatusPolicyName, "the agent adding a reason after the outcome")
		})

		t.Run("the node agent drops a reason the pod was created with when it records restored", func(t *testing.T) {
			pod := injected(t, "reason-created-with", map[string]string{reason: why})
			wantDenied(t, update(t, agent, pod, set(status, engine.CRIURestoreStatusRestored)), restoreStatusPolicyName,
				"the agent recording restored next to a reason")
			wantAllowed(t, update(t, agent, pod, func(p *corev1.Pod) {
				p.Annotations[status] = engine.CRIURestoreStatusRestored
				delete(p.Annotations, reason)
			}), "the agent recording restored and dropping the reason")
		})
	})

	// Break glass, as the README describes it: with the binding's action set to
	// Audit the policy no longer denies, and it denies again once the action
	// is Deny. Nothing is deleted or applied again.
	t.Run("setting the binding to Audit lifts the restore order policy until it is Deny again", func(t *testing.T) {
		pod := newPod(t, ns, "order-break-glass", optedIn, map[string]string{gkeOrder: "snap-a"})
		setActions := func(actions ...admissionregistrationv1.ValidationAction) {
			t.Helper()
			binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{}
			if err := admin.Get(ctx, client.ObjectKeyFromObject(orderBinding), binding); err != nil {
				t.Fatalf("reading the binding: %v", err)
			}
			base := binding.DeepCopy()
			binding.Spec.ValidationActions = actions
			if err := admin.Patch(ctx, binding, client.MergeFrom(base)); err != nil {
				t.Fatalf("setting the binding's validationActions to %v: %v", actions, err)
			}
		}
		// The API server picks up a binding change within a moment.
		waitFor := func(what string, done func(err error) bool) {
			t.Helper()
			deadline := time.Now().Add(60 * time.Second)
			for i := 0; ; i++ {
				err := patch(t, admin, pod, set(gkeOrder, fmt.Sprintf("snap-manual-%s-%d", what, i)))
				if done(err) {
					return
				}
				if err != nil && !deniedBy(err, restoreOrderPolicyName) {
					t.Fatalf("changing the restore order while waiting for the policy to be %s: %v", what, err)
				}
				if time.Now().After(deadline) {
					t.Fatalf("the policy isn't %s 60s after the binding changed (last error: %v)", what, err)
				}
				time.Sleep(200 * time.Millisecond)
			}
		}

		setActions(admissionregistrationv1.Audit)
		waitFor("lifted", func(err error) bool { return err == nil })

		setActions(admissionregistrationv1.Deny)
		waitFor("enforced", func(err error) bool { return deniedBy(err, restoreOrderPolicyName) })
	})
}
