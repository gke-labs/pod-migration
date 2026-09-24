package controller

import (
	"context"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func TestPodMigrationJobReconciler_ScaleDown_DeletesUnassignedSucceededPMJ_Deployment(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	replicas := int32(2)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
			},
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-1",
			Namespace: namespace,
			Labels:    map[string]string{"app": "web"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-2",
			Namespace: namespace,
			Labels:    map[string]string{"app": "web"},
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
				util.LabelParentName: "web-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "web-3"},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(deploy, pod1, pod2, pmj).
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

func TestPodMigrationJobReconciler_ScaleDown_DeletesUnassignedSucceededWithoutRestorePMJ(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	namespace := "default"
	replicas := int32(1)
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-deploy",
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "api"},
			},
		},
	}

	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "api-1",
			Namespace: namespace,
			Labels:    map[string]string{"app": "api"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}

	now := metav1.Now()
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-api-2",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "api-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef: corev1.LocalObjectReference{Name: "api-2"},
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
		WithObjects(deploy, pod1, pmj).
		Build()

	r := &PodMigrationJobReconciler{
		Client:   c,
		Scheme:   scheme,
		Recorder: recorder,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: pmj.Name},
	})
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	fetched := &pmv1alpha1.PodMigrationJob{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: pmj.Name}, fetched)
	if !apierrors.IsNotFound(err) {
		t.Fatalf("Expected SucceededWithoutRestore PMJ to be deleted, got err=%v", err)
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
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
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
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
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
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
			},
		},
	}

	// Only 2 active pods exist (< 3 desired)
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-1",
			Namespace: namespace,
			Labels:    map[string]string{"app": "web"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-2",
			Namespace: namespace,
			Labels:    map[string]string{"app": "web"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	now := metav1.Now()
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-web-3",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "web-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       false,
		},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(deploy, pod1, pod2, pmj).
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
	// Since it wasn't deleted, it falls through to TTL requeue
	if res.RequeueAfter <= 0 {
		t.Errorf("Expected positive RequeueAfter for TTL window, got %v", res.RequeueAfter)
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
	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-deploy",
			Namespace: namespace,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web"},
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
				util.LabelParentName: "web-deploy",
				util.LabelParentKind: "Deployment",
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
				util.LabelParentName: "web-deploy",
				util.LabelParentKind: "Deployment",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       false,
		},
	}
	claimantPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "web-claimant",
			Namespace: namespace,
			Labels:    map[string]string{"app": "web"},
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
			Labels:    map[string]string{"app": "web"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}

	recorder := record.NewFakeRecorder(10)
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithObjects(deploy, pmjConsumed, pmjClaimed, claimantPod, activePod).
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

func TestScaleDownPredicates(t *testing.T) {
	rep10 := int32(10)
	rep5 := int32(5)
	rep12 := int32(12)

	depOld := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &rep10}}
	depDown := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &rep5}}
	depUp := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &rep12}}
	depSame := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Replicas: &rep10}}

	depPred := deploymentScaleDownPredicate()
	if !depPred.Update(event.UpdateEvent{ObjectOld: depOld, ObjectNew: depDown}) {
		t.Errorf("Expected deploymentScaleDownPredicate to return true on scale down (10 -> 5)")
	}
	if depPred.Update(event.UpdateEvent{ObjectOld: depOld, ObjectNew: depUp}) {
		t.Errorf("Expected deploymentScaleDownPredicate to return false on scale up (10 -> 12)")
	}
	if depPred.Update(event.UpdateEvent{ObjectOld: depOld, ObjectNew: depSame}) {
		t.Errorf("Expected deploymentScaleDownPredicate to return false on equal replicas (10 -> 10)")
	}
	if depPred.Create(event.CreateEvent{Object: depDown}) {
		t.Errorf("Expected deploymentScaleDownPredicate Create to return false")
	}

	rsOld := &appsv1.ReplicaSet{Spec: appsv1.ReplicaSetSpec{Replicas: &rep10}}
	rsDown := &appsv1.ReplicaSet{Spec: appsv1.ReplicaSetSpec{Replicas: &rep5}}
	rsPred := replicaSetScaleDownPredicate()
	if !rsPred.Update(event.UpdateEvent{ObjectOld: rsOld, ObjectNew: rsDown}) {
		t.Errorf("Expected replicaSetScaleDownPredicate to return true on scale down")
	}

	stsOld := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &rep10}}
	stsDown := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{Replicas: &rep5}}
	stsPred := statefulSetScaleDownPredicate()
	if !stsPred.Update(event.UpdateEvent{ObjectOld: stsOld, ObjectNew: stsDown}) {
		t.Errorf("Expected statefulSetScaleDownPredicate to return true on scale down")
	}
}

func TestScaleDownMapping(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = pmv1alpha1.AddToScheme(scheme)
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)

	namespace := "test-ns"
	now := metav1.Now()

	// Completed unassigned PMJ for Deployment my-dep
	pmj1 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-dep-1",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "my-dep",
				util.LabelParentKind: "Deployment",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       false,
		},
	}
	// Completed consumed PMJ for Deployment my-dep -> should NOT be mapped
	pmj2 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-dep-2",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "my-dep",
				util.LabelParentKind: "Deployment",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:          pmv1alpha1.PodMigrationJobPhaseSucceeded,
			CompletionTime: &now,
			Consumed:       true,
		},
	}
	// Active PMJ for Deployment my-dep -> should NOT be mapped
	pmj3 := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pmj-dep-3",
			Namespace: namespace,
			Labels: map[string]string{
				util.LabelParentName: "my-dep",
				util.LabelParentKind: "Deployment",
			},
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:    pmv1alpha1.PodMigrationJobPhaseSnapshotting,
			Consumed: false,
		},
	}

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pmj1, pmj2, pmj3).Build()
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme}

	deploy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "my-dep", Namespace: namespace},
	}
	reqs := r.mapDeploymentToPMJs(context.Background(), deploy)
	if len(reqs) != 1 {
		t.Fatalf("Expected exactly 1 request mapped for my-dep, got %d", len(reqs))
	}
	if reqs[0].Name != "pmj-dep-1" {
		t.Errorf("Expected mapped PMJ 'pmj-dep-1', got %s", reqs[0].Name)
	}

	// ReplicaSet owned by Deployment
	rsOwned := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-dep-rs",
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				{Kind: "Deployment", Name: "my-dep"},
			},
		},
	}
	rsReqs := r.mapReplicaSetToPMJs(context.Background(), rsOwned)
	if len(rsReqs) != 1 || rsReqs[0].Name != "pmj-dep-1" {
		t.Errorf("Expected ReplicaSet owned by Deployment to map to 'pmj-dep-1', got %v", rsReqs)
	}
}
