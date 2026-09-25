package controller

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
	"github.com/gke-labs/pod-migration/controller/internal/webhook"
)

// TestVPAReplaySuite verifies the 8-part replay verification suite covering VPA
// right-sizing, in-place resize coordination, distilled spec digest matching,
// and cold-start fallback under cgroup / memory boundary deviations.
func TestVPAReplaySuite(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	deployName := "vpa-stateful-app"
	deployUID := "deploy-vpa-uid-999"
	rsName := "vpa-stateful-app-rs-1"
	rsUID := "rs-vpa-uid-999"

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      rsName,
			UID:       types.UID(rsUID),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       deployName,
					UID:        types.UID(deployUID),
				},
			},
		},
	}

	baseOriginSpec := corev1.PodSpec{
		Containers: []corev1.Container{
			{
				Name:  "app-server",
				Image: "redis:7.0",
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1Gi"),
					},
					Limits: corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("1000m"),
						corev1.ResourceMemory: resource.MustParse("2Gi"),
					},
				},
			},
		},
	}

	originDigest, err := util.DistilledPodSpecDigest(&baseOriginSpec)
	if err != nil {
		t.Fatalf("DistilledPodSpecDigest failed: %v", err)
	}

	t.Run("Replay-Part1: VPA memory scale-up before eviction matches active PMJ", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "pmj-vpa-scale-up",
				Labels: map[string]string{
					util.LabelParentName:          deployName,
					util.LabelParentKind:          "Deployment",
					util.LabelParentUID:           deployUID,
					util.LabelPodTemplateHash:     "hash-v1-original",
					util.LabelDistilledSpecDigest: originDigest[:63],
				},
				Annotations: map[string]string{
					util.AnnotationDistilledSpecDigest: originDigest,
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, pmj).Build()

		// Replacement pod has VPA memory scaled up from 1Gi to 4Gi; replica-set computed a new template hash
		scaleUpPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "vpa-scale-up-pod",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled":         "true",
					appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2-scaled-up",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       rsName,
						UID:        types.UID(rsUID),
					},
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "app-server",
						Image: "redis:7.0",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("4Gi"),
							},
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("1000m"),
								corev1.ResourceMemory: resource.MustParse("8Gi"),
							},
						},
					},
				},
			},
		}

		replDigest, err := util.DistilledPodSpecDigest(&scaleUpPod.Spec)
		if err != nil {
			t.Fatalf("DistilledPodSpecDigest failed: %v", err)
		}
		if replDigest != originDigest {
			t.Fatalf("expected distilled digest to remain invariant under VPA memory scale-up, got %q vs %q", replDigest, originDigest)
		}

		matchedPMJ, err := util.FindUnassignedActivePMJ(context.Background(), cl, namespace, scaleUpPod.Name, deployName, "Deployment", deployUID, "hash-v2-scaled-up", "", replDigest)
		if err != nil {
			t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
		}
		if matchedPMJ != pmj.Name {
			t.Errorf("Expected scale-up pod to match PMJ %q via distilled spec digest, got %q", pmj.Name, matchedPMJ)
		}
	})

	t.Run("Replay-Part2: VPA memory scale-down before eviction matches active PMJ", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "pmj-vpa-scale-down",
				Labels: map[string]string{
					util.LabelParentName:          deployName,
					util.LabelParentKind:          "Deployment",
					util.LabelParentUID:           deployUID,
					util.LabelPodTemplateHash:     "hash-v1-original",
					util.LabelDistilledSpecDigest: originDigest[:63],
				},
				Annotations: map[string]string{
					util.AnnotationDistilledSpecDigest: originDigest,
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, pmj).Build()

		// Replacement pod has VPA memory scaled down from 1Gi to 256Mi
		scaleDownPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "vpa-scale-down-pod",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled":         "true",
					appsv1.DefaultDeploymentUniqueLabelKey: "hash-v3-scaled-down",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "app-server",
						Image: "redis:7.0",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("250m"),
								corev1.ResourceMemory: resource.MustParse("256Mi"),
							},
						},
					},
				},
			},
		}

		replDigest, _ := util.DistilledPodSpecDigest(&scaleDownPod.Spec)
		matchedPMJ, err := util.FindUnassignedActivePMJ(context.Background(), cl, namespace, scaleDownPod.Name, deployName, "Deployment", deployUID, "hash-v3-scaled-down", "", replDigest)
		if err != nil {
			t.Fatalf("FindUnassignedActivePMJ failed: %v", err)
		}
		if matchedPMJ != pmj.Name {
			t.Errorf("Expected scale-down pod to match PMJ %q, got %q", pmj.Name, matchedPMJ)
		}
	})

	t.Run("Replay-Part3: VPA CPU scale-up and limits adjustment preserves match", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "pmj-vpa-cpu-resize",
				Labels: map[string]string{
					util.LabelParentName:      deployName,
					util.LabelParentKind:      "Deployment",
					util.LabelParentUID:       deployUID,
					util.LabelPodTemplateHash: "hash-v1-original",
				},
				Annotations: map[string]string{
					util.AnnotationDistilledSpecDigest: originDigest,
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, pmj).Build()

		cpuResizedPod := &corev1.Pod{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "app-server",
						Image: "redis:7.0",
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("4000m"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
						},
					},
				},
			},
		}
		replDigest, _ := util.DistilledPodSpecDigest(&cpuResizedPod.Spec)

		matchedPMJ, err := util.FindUnassignedActivePMJ(context.Background(), cl, namespace, "cpu-pod", deployName, "Deployment", deployUID, "hash-cpu-v2", "", replDigest)
		if err != nil || matchedPMJ != pmj.Name {
			t.Errorf("Expected CPU resized pod to match PMJ %q, got %q, err=%v", pmj.Name, matchedPMJ, err)
		}
	})

	t.Run("Replay-Part4: In-place resize in progress during eviction defers snapshot", func(t *testing.T) {
		podUID := "origin-pod-uid-inplace"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "origin-pod-inplace",
				UID:       types.UID(podUID),
			},
			Status: corev1.PodStatus{
				Resize: corev1.PodResizeStatusInProgress,
			},
		}

		jobName := util.FormatPMJName(pod.Name, podUID)
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: pod.Name},
				TargetPodUID: podUID,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, pmj).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{Client: cl, Scheme: scheme}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if res.RequeueAfter != 2*time.Second {
			t.Errorf("Expected 2s requeue while resize is InProgress, got %v", res.RequeueAfter)
		}

		fetched := &pmv1alpha1.PodMigrationJob{}
		_ = cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, fetched)
		if fetched.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending {
			t.Errorf("Expected phase to remain Pending, got %s", fetched.Status.Phase)
		}
		cond := meta.FindStatusCondition(fetched.Status.Conditions, "Ready")
		if cond == nil || cond.Reason != "VPAResizeInProgress" {
			t.Errorf("Expected condition Ready=False Reason=VPAResizeInProgress, got %+v", cond)
		}
	})

	t.Run("Replay-Part5: In-place resize completion after handshake proceeds to snapshotting", func(t *testing.T) {
		podUID := "origin-pod-uid-resumed"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "origin-pod-resumed",
				UID:       types.UID(podUID),
			},
			Status: corev1.PodStatus{
				Resize: "", // resize completed
			},
		}

		jobName := util.FormatPMJName(pod.Name, podUID)
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: pod.Name},
				TargetPodUID: podUID,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, pmj).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{Client: cl, Scheme: scheme}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected immediate Requeue after transitioning to Snapshotting")
		}

		fetched := &pmv1alpha1.PodMigrationJob{}
		_ = cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, fetched)
		if fetched.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Errorf("Expected phase Snapshotting after resize complete, got %s", fetched.Status.Phase)
		}
	})

	t.Run("Replay-Part6: In-place resize deferred proceeds without deadlock", func(t *testing.T) {
		podUID := "origin-pod-uid-deferred"
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "origin-pod-deferred",
				UID:       types.UID(podUID),
			},
			Status: corev1.PodStatus{
				Resize: corev1.PodResizeStatusDeferred, // deferred resize does not block current stable snapshot
			},
		}

		jobName := util.FormatPMJName(pod.Name, podUID)
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         namespace,
				Name:              jobName,
				CreationTimestamp: metav1.Now(),
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: pod.Name},
				TargetPodUID: podUID,
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pod, pmj).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
		r := &PodMigrationJobReconciler{Client: cl, Scheme: scheme}

		res, err := r.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: jobName},
		})
		if err != nil {
			t.Fatalf("Reconcile failed: %v", err)
		}
		if !res.Requeue {
			t.Errorf("Expected Requeue for Snapshotting transition")
		}

		fetched := &pmv1alpha1.PodMigrationJob{}
		_ = cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: jobName}, fetched)
		if fetched.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Errorf("Expected phase Snapshotting when resize is Deferred, got %s", fetched.Status.Phase)
		}
	})

	t.Run("Replay-Part7: Cold-start fallback on unresolvable cgroup boundaries or restore failure", func(t *testing.T) {
		// When PMJ fails due to restore crash / unresolvable cgroup boundary,
		// PodGateReconciler releases the scheduling gate with cold-start bypass (ps-name: "")
		gatedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "gated-repl-pod",
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ: "pmj-restore-crashed",
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{
					{Name: "gke.io/pod-migration-gate"},
				},
			},
		}

		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "pmj-restore-crashed",
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			},
		}

		cl := fake.NewClientBuilder().
			WithScheme(scheme).
			WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).
			WithIndex(&corev1.Pod{}, util.PodAssignedPMJIndexKey, func(rawObj client.Object) []string {
				p, ok := rawObj.(*corev1.Pod)
				if !ok || p.Annotations == nil {
					return nil
				}
				if val := p.Annotations[util.AnnotationAssignedPMJ]; val != "" {
					return []string{val}
				}
				return nil
			}).
			WithObjects(gatedPod, pmj).
			Build()
		gateReconciler := &PodGateReconciler{Client: cl, Scheme: scheme}

		_, err := gateReconciler.Reconcile(context.Background(), ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: namespace, Name: gatedPod.Name},
		})
		if err != nil {
			t.Fatalf("PodGateReconciler failed: %v", err)
		}

		updatedPod := &corev1.Pod{}
		_ = cl.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: gatedPod.Name}, updatedPod)

		// Scheduling gate must be released
		if len(updatedPod.Spec.SchedulingGates) != 0 {
			t.Errorf("Expected scheduling gate to be removed on restore failure fallback, got %v", updatedPod.Spec.SchedulingGates)
		}
		// Cold start bypass annotation stamped
		if updatedPod.Annotations["podsnapshot.gke.io/ps-name"] != "" {
			t.Errorf("Expected podsnapshot.gke.io/ps-name to be empty string for cold start bypass, got %q", updatedPod.Annotations["podsnapshot.gke.io/ps-name"])
		}
	})

	t.Run("Replay-Part8: Distilled spec digest mismatch rejects genuine template rollouts", func(t *testing.T) {
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "pmj-vpa-rollout-isolate",
				Labels: map[string]string{
					util.LabelParentName:      deployName,
					util.LabelParentKind:      "Deployment",
					util.LabelParentUID:       deployUID,
					util.LabelPodTemplateHash: "hash-v1-original",
				},
				Annotations: map[string]string{
					util.AnnotationDistilledSpecDigest: originDigest,
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef: corev1.LocalObjectReference{Name: "origin-pod"},
			},
			Status: pmv1alpha1.PodMigrationJobStatus{
				Phase: pmv1alpha1.PodMigrationJobPhasePending,
			},
		}

		cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(rs, pmj).Build()
		handler := &webhook.PodGateInjector{Client: cl, APIReader: cl}
		_ = handler.InjectDecoder(admission.NewDecoder(scheme))

		// New rollout pod: updated image from redis:7.0 to redis:7.2
		rolloutPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      "rollout-pod-v2",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled":         "true",
					appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2-rollout",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       rsName,
						UID:        types.UID(rsUID),
					},
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "app-server",
						Image: "redis:7.2", // updated image
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceMemory: resource.MustParse("4Gi"),
							},
						},
					},
				},
			},
		}

		rawPod, _ := json.Marshal(rolloutPod)
		req := admission.Request{}
		req.Namespace = namespace
		req.Name = rolloutPod.Name
		req.Object = runtime.RawExtension{Raw: rawPod}

		resp := handler.Handle(context.Background(), req)
		if !resp.Allowed {
			t.Fatalf("Expected allowed, got denied: %+v", resp.Result)
		}

		// Gate must NOT be injected
		for _, p := range resp.Patches {
			if p.Path == "/spec/schedulingGates" || p.Path == "/spec/schedulingGates/-" {
				t.Fatalf("Rollout pod with different image must not have scheduling gate injected")
			}
		}
	})
}
