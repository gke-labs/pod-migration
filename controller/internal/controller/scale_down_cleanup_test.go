package controller

import (
	"context"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestPodMigrationJobReconciler_ScaleDown_DeletesUnassignedSucceededWithoutRestorePMJ_Deployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	replicas := int32(2)
	hash := "7bf476f477"
	rsName := "web-deploy-" + hash

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: namespace,
			UID:       "deploy-uid-123",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
			},
		},
	}

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rsName,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "web-deploy",
					UID:        "deploy-uid-123",
				},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                               "web",
					appsv1.DefaultDeploymentUniqueLabelKey: hash,
				},
			},
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-1",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-2",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	now := metav1.Now()
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-3",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: hash,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-3"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(deploy, rs, pod1, pod2, pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter: 0 on immediate proactive deletion, got %v", res.RequeueAfter)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, fetched)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected PMJ to be deleted, but Get returned err=%v", err)
	}

	// Verify Event was recorded
	select {
	case eventMsg := <-recorder.Events:
		if !strings.Contains(eventMsg, "ScaleDownZombieCleaned") {
			t.Errorf("Expected event Reason 'ScaleDownZombieCleaned', got %q", eventMsg)
		}
	default:
		t.Errorf("Expected an event to be recorded for scale-down proactive cleanup")
	}
}

func TestPodMigrationJobReconciler_ScaleDown_DeploymentRolloutTwoReplicaSets(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	deployReplicas := int32(5)
	oldRSReplicas := int32(3)
	newRSReplicas := int32(3)

	oldHash := "oldhash123"
	newHash := "newhash456"

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: namespace,
			UID:       "deploy-uid-123",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &deployReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
			},
		},
	}

	oldRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-" + oldHash,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: oldHash,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "web-deploy",
					UID:        "deploy-uid-123",
				},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &oldRSReplicas, // desired: 3
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                               "web",
					appsv1.DefaultDeploymentUniqueLabelKey: oldHash,
				},
			},
		},
	}

	newRS := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-" + newHash,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: newHash,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion: "apps/v1",
					Kind:       "Deployment",
					Name:       "web-deploy",
					UID:        "deploy-uid-123",
				},
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &newRSReplicas, // desired: 3
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                               "web",
					appsv1.DefaultDeploymentUniqueLabelKey: newHash,
				},
			},
		},
	}

	// 2 pods running on old RS (< 3 desired for old RS)
	oldPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-old-1",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: oldHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	oldPod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-old-2",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: oldHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	// 3 pods running on new RS
	newPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-new-1",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: newHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	newPod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-new-2",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: newHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	newPod3 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-new-3",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: newHash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	// Total pods across deployment: 2 old + 3 new = 5 pods.
	// Total pods (5) >= Deployment replicas (5).
	// Under a naive deployment-wide count, this PMJ would be prematurely deleted during rollout.
	// But against oldRS.Spec.Replicas (3), active old pods (2) < target (3), so the PMJ must survive.
	now := metav1.Now()
	pmjOld := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-old-3",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: oldHash,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-old-3"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(deploy, oldRS, newRS, oldPod1, oldPod2, newPod1, newPod2, newPod3, pmjOld).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	// Step 1: Reconcile during rollout before old RS scales down -> must NOT delete PMJ
	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmjOld.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 1*time.Minute {
		t.Errorf("Expected RequeueAfter: 1m while waiting for scale down, got %v", res.RequeueAfter)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmjOld.Name}, fetched); err != nil {
		t.Fatalf("Expected PMJ to survive rollout when old RS still needs replicas, got err=%v", err)
	}

	// Step 2: Scale down old RS to 2 replicas (active old pods 2 >= desired 2)
	scaledDownReplicas := int32(2)
	oldRS.Spec.Replicas = &scaledDownReplicas
	if err := c.Update(context.Background(), oldRS); err != nil {
		t.Fatalf("Failed to update oldRS: %v", err)
	}

	// Step 3: Reconcile again -> now old RS has satisfied its target replicas, PMJ should be deleted
	res, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmjOld.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Errorf("Expected RequeueAfter: 0 on deletion, got %v", res.RequeueAfter)
	}

	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmjOld.Name}, fetched)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected PMJ to be deleted once old RS scaled down, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_ReplicaSetAndStatefulSet(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	now := metav1.Now()

	// 1. ReplicaSet
	rsReplicas := int32(1)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-rs",
			Namespace: namespace,
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &rsReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"worker": "true"},
			},
		},
	}
	rsPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker-1",
			Namespace: namespace,
			Labels:    map[string]string{"worker": "true"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	rsPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-worker-2",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "worker-rs",
				util.LabelParentKind: "ReplicaSet",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	// 2. StatefulSet
	stsReplicas := int32(2)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis",
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &stsReplicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "redis"},
			},
		},
	}
	stsPod0 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-0",
			Namespace: namespace,
			Labels:    map[string]string{"app": "redis"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	stsPod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-1",
			Namespace: namespace,
			Labels:    map[string]string{"app": "redis"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	stsPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-redis-2",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "redis",
				util.LabelParentKind: "StatefulSet",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(rs, rsPod, rsPMJ, sts, stsPod0, stsPod1, stsPMJ).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	// Reconcile ReplicaSet PMJ
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: rsPMJ.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile RS PMJ failed: %v", err)
	}
	fetchedRS := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: rsPMJ.Name}, fetchedRS); !apierrors.IsNotFound(err) {
		t.Fatalf("Expected RS PMJ to be deleted, got err=%v", err)
	}

	// Reconcile StatefulSet PMJ
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: stsPMJ.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile STS PMJ failed: %v", err)
	}
	fetchedSTS := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: stsPMJ.Name}, fetchedSTS); !apierrors.IsNotFound(err) {
		t.Fatalf("Expected STS PMJ to be deleted, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_DoesNotDeleteWhenActivePodsBelowTarget(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	replicas := int32(3) // target is 3
	hash := "abc12345"

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-" + hash,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                               "web",
					appsv1.DefaultDeploymentUniqueLabelKey: hash,
				},
			},
		},
	}

	// Only 2 active pods exist (< 3 desired)
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-1",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-2",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	now := metav1.Now()
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-3",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: hash,
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(rs, pod1, pod2, pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}
	if res.RequeueAfter != 1*time.Minute {
		t.Errorf("Expected RequeueAfter: 1m when active pods below target, got %v", res.RequeueAfter)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, fetched)
	if err != nil {
		t.Fatalf("Expected PMJ to NOT be deleted, but Get returned err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_DoesNotDeleteWhenConsumedOrClaimed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	replicas := int32(2)
	hash := "hashxyz"

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy-" + hash,
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app":                               "web",
					appsv1.DefaultDeploymentUniqueLabelKey: hash,
				},
			},
		},
	}

	now := metav1.Now()
	// Case 1: Consumed is true
	pmjConsumed := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-consumed",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: hash,
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       true,
		},
	}

	// Case 2: Consumed is false, but a pod has AnnotationAssignedPMJ pointing to it
	pmjClaimed := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-claimed",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName:      "web-deploy",
				util.LabelParentKind:      "Deployment",
				util.LabelPodTemplateHash: hash,
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
			Consumed:       false,
		},
	}
	claimantPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-claimant",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
			Annotations: map[string]string{
				util.AnnotationAssignedPMJ: "pmj-claimed",
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	activePod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-active",
			Namespace: namespace,
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(rs, pmjConsumed, pmjClaimed, claimantPod, activePod).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	// 1. Reconcile pmjConsumed -> should NOT be deleted
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmjConsumed.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pmjConsumed failed: %v", err)
	}
	fetchedConsumed := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmjConsumed.Name}, fetchedConsumed); err != nil {
		t.Fatalf("Expected pmjConsumed to NOT be deleted, got err=%v", err)
	}

	// 2. Reconcile pmjClaimed -> should NOT be deleted because claimantPod claims it
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmjClaimed.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pmjClaimed failed: %v", err)
	}
	fetchedClaimed := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmjClaimed.Name}, fetchedClaimed); err != nil {
		t.Fatalf("Expected pmjClaimed to NOT be deleted, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_StatefulSet_OrdinalChecks(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	stsName := "redis"
	replicas := int32(2)

	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      stsName,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "redis"},
			},
		},
	}

	// Active pods: redis-1 and lingering redis-2
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-1",
			Namespace: namespace,
			UID:       "uid-redis-1",
			Labels:    map[string]string{"app": "redis"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "redis-2",
			Namespace: namespace,
			UID:       "uid-redis-2",
			Labels:    map[string]string{"app": "redis"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	now := metav1.Now()
	// PMJ for redis-0 (ordinal 0 < replicas 2): was NOT scaled down!
	pmj0 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-redis-0",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind: "StatefulSet",
				util.LabelParentName: stsName,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "redis-0"},
			TargetPodUID: "uid-redis-0-origin",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
		},
	}

	// PMJ for redis-2 (ordinal 2 >= replicas 2): WAS scaled down!
	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-redis-2",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind: "StatefulSet",
				util.LabelParentName: stsName,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "redis-2"},
			TargetPodUID: "uid-redis-2-origin",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(sts, pod1, pod2, pmj0, pmj2).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	// 1. Reconcile pmj0: ordinal 0 < 2, no replacement pod running -> MUST NOT be deleted
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj0.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pmj0 failed: %v", err)
	}
	fetched0 := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj0.Name}, fetched0); err != nil {
		t.Fatalf("Expected pmj0 to NOT be deleted because ordinal 0 is within target replicas (2), got err=%v", err)
	}

	// 2. Reconcile pmj2: ordinal 2 >= 2, active pods (redis-1, redis-2 = 2) >= targetReplicas (2) -> MUST be deleted
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj2.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile pmj2 failed: %v", err)
	}
	fetched2 := &pmv1alpha1.PodMigrationJob{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj2.Name}, fetched2)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected pmj2 to be deleted because ordinal 2 was scaled down, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_DeploymentReplicaSetNotFound_DoesNotDelete(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	hash := "missinghash"
	now := metav1.Now()

	// PMJ for Deployment where RS is completely absent from API
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-missing-rs",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind:         "Deployment",
				util.LabelParentName:         "long-deployment-name-that-is-missing-its-replicaset-object",
				util.LabelPodTemplateHash:    hash,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "pod-1"},
			TargetPodUID: "uid-1-origin",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, fetched); err != nil {
		t.Fatalf("Expected PMJ to NOT be deleted when RS is NotFound, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_OriginPodStillRunning_FailOpenPreserved(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	hash := "hash-v1"
	replicas := int32(1)
	now := metav1.Now()

	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-rs",
			Namespace: namespace,
			Labels: map[string]string{
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web", appsv1.DefaultDeploymentUniqueLabelKey: hash},
			},
		},
	}

	// Origin pod is still running (DeletionTimestamp == nil) e.g. after PDBEvictionTimeout
	originPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-origin",
			Namespace: namespace,
			UID:       "uid-origin-alive",
			Labels: map[string]string{
				"app":                               "web",
				appsv1.DefaultDeploymentUniqueLabelKey: hash,
			},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-pdb-timeout",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind:      "ReplicaSet",
				util.LabelParentName:      "web-rs",
				util.LabelPodTemplateHash: hash,
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "web-origin"},
			TargetPodUID: "uid-origin-alive",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(rs, originPod, pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, fetched); err != nil {
		t.Fatalf("Expected PMJ to NOT be deleted while origin pod is still alive, got err=%v", err)
	}
}

func TestPodMigrationJobReconciler_ScaleDown_UnsupportedParentKind_ShortCircuitsRequeue(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	now := metav1.NewTime(time.Now().Add(-5 * time.Minute))

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-daemonset",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentKind: "DaemonSet",
				util.LabelParentName: "node-exporter",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "exporter-0"},
			TargetPodUID: "uid-exporter-0",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore,
			CompletionTime: &now,
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(10),
	}

	res, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	// Should NOT requeue at 1m. Expected requeue around 25 minutes (30m - 5m)
	if res.RequeueAfter < 20*time.Minute {
		t.Errorf("Expected requeue around 25m for unsupported parentKind, got %v", res.RequeueAfter)
	}
}
