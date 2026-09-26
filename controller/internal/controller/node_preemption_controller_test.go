package controller

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func createTestNode(name string, preempting bool, signalType string) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{Type: corev1.NodeReady, Status: corev1.ConditionTrue},
			},
		},
	}
	if !preempting {
		return node
	}
	switch signalType {
	case "taint-gke":
		node.Spec.Taints = []corev1.Taint{
			{Key: util.TaintImpendingNodeTermination, Effect: corev1.TaintEffectNoSchedule},
		}
	case "taint-out-of-service":
		node.Spec.Taints = []corev1.Taint{
			{Key: util.TaintOutOfService, Effect: corev1.TaintEffectNoExecute},
		}
	case "condition-gke":
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:   util.ConditionImpendingNodeTermination,
			Status: corev1.ConditionTrue,
		})
	case "condition-terminating":
		node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
			Type:   util.ConditionTerminating,
			Status: corev1.ConditionTrue,
		})
	case "label-maintenance":
		node.Labels = map[string]string{
			util.LabelActiveNodeMaintenance: util.ValueMaintenanceOngoing,
		}
	default:
		node.Spec.Taints = []corev1.Taint{
			{Key: util.TaintImpendingNodeTermination, Effect: corev1.TaintEffectNoSchedule},
		}
	}
	return node
}

func createTestPSP(name, namespace string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotPolicy",
	})
	u.SetName(name)
	u.SetNamespace(namespace)
	u.Object["spec"] = map[string]interface{}{
		"triggerConfig": map[string]interface{}{
			"type":           "manual",
			"postCheckpoint": "stop",
		},
		"selector": map[string]interface{}{
			"matchExpressions": []interface{}{
				map[string]interface{}{
					"key":      "pod-migration.gke.io/enabled",
					"operator": "In",
					"values":   []interface{}{"true"},
				},
			},
		},
	}
	u.Object["status"] = map[string]interface{}{
		"conditions": []interface{}{
			map[string]interface{}{
				"type":   "Ready",
				"status": "True",
			},
		},
	}
	return u
}

func createTestPod(name, namespace, nodeName string, memReqStr string, uid string) *corev1.Pod {
	gvisor := "gvisor"
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(uid),
			Labels: map[string]string{
				"pod-migration.gke.io/enabled": "true",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:         nodeName,
			RuntimeClassName: &gvisor,
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "redis:7",
				},
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
		},
	}
	if memReqStr != "" {
		pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse(memReqStr),
			},
		}
	}
	return pod
}

func TestNodePreemptionReconciler_HealthyNode_NoOp(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	node := createTestNode("node-healthy", false, "")
	pod := createTestPod("pod-1", "default", "node-healthy", "1Gi", "uid-1")
	psp := createTestPSP("psp-default", "default")

	recorder := record.NewFakeRecorder(10)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(node, pod, psp).
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "node-healthy"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify no PMJ was created
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := cl.List(context.Background(), pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}
	if len(pmjList.Items) != 0 {
		t.Fatalf("expected 0 PMJs on healthy node, got %d", len(pmjList.Items))
	}
}

func TestNodePreemptionReconciler_AscendingBudgetOrdering(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	node := createTestNode("spot-node-1", true, "taint-gke")

	// 4 seeded pods as described in Issue #23:
	// pod1: 1Gi, pod2: 4Gi, pod3: 8Gi, pod4: 12Gi
	// Total budget: 15Gi
	// Ascending order:
	// - pod1 (1Gi) consumes 1Gi -> 14Gi left
	// - pod2 (4Gi) consumes 4Gi -> 10Gi left
	// - pod3 (8Gi) consumes 8Gi -> 2Gi left
	// - pod4 (12Gi) exceeds remaining 2Gi -> skipped!
	pod1 := createTestPod("pod-1gi", "default", "spot-node-1", "1Gi", "uid-1gi")
	pod2 := createTestPod("pod-4gi", "default", "spot-node-1", "4Gi", "uid-4gi")
	pod3 := createTestPod("pod-8gi", "default", "spot-node-1", "8Gi", "uid-8gi")
	pod4 := createTestPod("pod-12gi", "default", "spot-node-1", "12Gi", "uid-12gi")

	psp := createTestPSP("psp-default", "default")

	recorder := record.NewFakeRecorder(20)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(node, pod4, pod2, pod1, pod3, psp). // insert in non-sorted order to verify sorting
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
		SpotPreemptionBudget:    15 * 1024 * 1024 * 1024, // 15 GiB
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "spot-node-1"},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	// Verify exactly 3 PMJs were created (pod1, pod2, pod3)
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := cl.List(context.Background(), pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}

	if len(pmjList.Items) != 3 {
		t.Fatalf("expected 3 PMJs within 15Gi budget, got %d", len(pmjList.Items))
	}

	createdPods := make(map[string]bool)
	for _, pmj := range pmjList.Items {
		createdPods[pmj.Spec.PodRef.Name] = true
		if pmj.Labels[util.LabelTriggerSource] != util.TriggerSourceSpotPreemption {
			t.Errorf("expected PMJ %s to have trigger-source label %s, got %s",
				pmj.Name, util.TriggerSourceSpotPreemption, pmj.Labels[util.LabelTriggerSource])
		}
		if pmj.Annotations[util.AnnotationTriggerSource] != util.TriggerSourceSpotPreemption {
			t.Errorf("expected PMJ %s to have trigger-source annotation %s, got %s",
				pmj.Name, util.TriggerSourceSpotPreemption, pmj.Annotations[util.AnnotationTriggerSource])
		}
		if pmj.Annotations[util.AnnotationPodSnapshotPolicy] != "psp-default" {
			t.Errorf("expected PMJ %s to have psp annotation psp-default, got %s",
				pmj.Name, pmj.Annotations[util.AnnotationPodSnapshotPolicy])
		}
	}

	if !createdPods["pod-1gi"] || !createdPods["pod-4gi"] || !createdPods["pod-8gi"] {
		t.Errorf("expected PMJs for pod-1gi, pod-4gi, and pod-8gi, got %v", createdPods)
	}
	if createdPods["pod-12gi"] {
		t.Errorf("pod-12gi should have been skipped due to exceeding budget")
	}

	// Verify events
	var events []string
closeLoop:
	for {
		select {
		case ev := <-recorder.Events:
			events = append(events, ev)
		default:
			break closeLoop
		}
	}

	foundBudgetExceededEvent := false
	for _, ev := range events {
		if ev == "Warning MigrationSkippedPreemptionBudgetExceeded Pod requested 12.00Gi memory, exceeding remaining node preemption budget (2.00Gi remaining of 15.00Gi total); skipping spot preemption migration" {
			foundBudgetExceededEvent = true
			break
		}
	}
	if !foundBudgetExceededEvent {
		t.Errorf("expected MigrationSkippedPreemptionBudgetExceeded event for pod-12gi, got events: %v", events)
	}

	// Verify Prometheus metrics
	if testutil.ToFloat64(metrics.SpotPreemptionTriggeredTotal) < 3 {
		t.Errorf("expected SpotPreemptionTriggeredTotal >= 3, got %v", testutil.ToFloat64(metrics.SpotPreemptionTriggeredTotal))
	}
	if testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("budget_exceeded")) < 1 {
		t.Errorf("expected SpotPreemptionSkippedTotal[budget_exceeded] >= 1, got %v",
			testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("budget_exceeded")))
	}
}

func TestNodePreemptionReconciler_PodEligibilityFiltering(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	node := createTestNode("spot-node-eligibility", true, "taint-gke")
	psp := createTestPSP("psp-default", "default")

	// 1. Pod on other node
	podOtherNode := createTestPod("pod-other-node", "default", "other-node", "1Gi", "uid-other")

	// 2. Pod in kube-system
	podKubeSystem := createTestPod("pod-kube-system", "kube-system", "spot-node-eligibility", "1Gi", "uid-ks")

	// 3. Pod not enabled
	podNotEnabled := createTestPod("pod-not-enabled", "default", "spot-node-eligibility", "1Gi", "uid-ne")
	delete(podNotEnabled.Labels, "pod-migration.gke.io/enabled")

	// 4. Pod with non-gvisor runtime
	runc := "runc"
	podRunc := createTestPod("pod-runc", "default", "spot-node-eligibility", "1Gi", "uid-runc")
	podRunc.Spec.RuntimeClassName = &runc

	// 5. Pod with deletion timestamp (terminating)
	now := metav1.Now()
	podTerminating := createTestPod("pod-terminating", "default", "spot-node-eligibility", "1Gi", "uid-term")
	podTerminating.Finalizers = []string{"test-finalizer"}
	podTerminating.DeletionTimestamp = &now

	// 6. Pod in non-running phase
	podPending := createTestPod("pod-pending", "default", "spot-node-eligibility", "1Gi", "uid-pend")
	podPending.Status.Phase = corev1.PodPending

	// 7. Pod with prior PDB eviction timeout
	podPDBTimeout := createTestPod("pod-pdb-timeout", "default", "spot-node-eligibility", "1Gi", "uid-pdb")
	podPDBTimeout.Annotations = map[string]string{
		util.AnnotationPDBEvictionTimeout: "true",
	}

	// 8. Pod with already existing PMJ
	podWithPMJ := createTestPod("pod-with-pmj", "default", "spot-node-eligibility", "1Gi", "uid-pmj")
	existingPMJ := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      util.FormatPMJName("pod-with-pmj", "uid-pmj"),
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "pod-with-pmj"},
			TargetPodUID: "uid-pmj",
		},
	}

	// 9. Pod with no matching PSP
	podNoPSP := createTestPod("pod-no-psp", "no-psp-namespace", "spot-node-eligibility", "1Gi", "uid-nopsp")

	// 10. Valid eligible pod
	podValid := createTestPod("pod-valid", "default", "spot-node-eligibility", "1Gi", "uid-valid")

	recorder := record.NewFakeRecorder(20)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(
			node, psp,
			podOtherNode, podKubeSystem, podNotEnabled, podRunc,
			podTerminating, podPending, podPDBTimeout, podWithPMJ, existingPMJ,
			podNoPSP, podValid,
		).
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
		SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "spot-node-eligibility"},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	// Verify only 1 new PMJ created (for pod-valid), existingPMJ preserved
	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := cl.List(context.Background(), pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}

	if len(pmjList.Items) != 2 {
		t.Fatalf("expected 2 PMJs (1 pre-existing + 1 new), got %d", len(pmjList.Items))
	}

	validPMJName := util.FormatPMJName("pod-valid", "uid-valid")
	foundValid := false
	for _, pmj := range pmjList.Items {
		if pmj.Name == validPMJName {
			foundValid = true
			break
		}
	}
	if !foundValid {
		t.Errorf("expected PMJ %s to be created for pod-valid", validPMJName)
	}

	// Verify MigrationSkippedNoPolicy event for podNoPSP
	var events []string
closeLoop:
	for {
		select {
		case ev := <-recorder.Events:
			events = append(events, ev)
		default:
			break closeLoop
		}
	}

	foundNoPolicyEvent := false
	for _, ev := range events {
		if ev == "Warning MigrationSkippedNoPolicy Pod is opted into live migration, but no matching Ready manual+stop PodSnapshotPolicy was found; skipping spot preemption migration" {
			foundNoPolicyEvent = true
			break
		}
	}
	if !foundNoPolicyEvent {
		t.Errorf("expected MigrationSkippedNoPolicy event for podNoPSP, got events: %v", events)
	}

	if testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("no_policy")) < 1 {
		t.Errorf("expected SpotPreemptionSkippedTotal[no_policy] >= 1, got %v",
			testutil.ToFloat64(metrics.SpotPreemptionSkippedTotal.WithLabelValues("no_policy")))
	}
}

func TestNodePreemptionReconciler_AlternativePreemptionSignals(t *testing.T) {
	signals := []string{
		"taint-out-of-service",
		"condition-gke",
		"condition-terminating",
		"label-maintenance",
	}

	for _, sig := range signals {
		t.Run(sig, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = corev1.AddToScheme(scheme)
			_ = pmv1alpha1.AddToScheme(scheme)

			nodeName := fmt.Sprintf("node-%s", sig)
			node := createTestNode(nodeName, true, sig)
			pod := createTestPod(fmt.Sprintf("pod-%s", sig), "default", nodeName, "1Gi", fmt.Sprintf("uid-%s", sig))
			psp := createTestPSP("psp-default", "default")

			recorder := record.NewFakeRecorder(10)
			cl := fake.NewClientBuilder().
				WithScheme(scheme).
				WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
				WithObjects(node, pod, psp).
				Build()

			r := &NodePreemptionReconciler{
				Client:                  cl,
				Scheme:                  scheme,
				Recorder:                recorder,
				DefaultMigrationTimeout: 10 * time.Minute,
				SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
			}

			_, err := r.Reconcile(context.Background(), ctrl.Request{
				NamespacedName: types.NamespacedName{Name: nodeName},
			})
			if err != nil {
				t.Fatalf("unexpected reconcile error for signal %s: %v", sig, err)
			}

			pmjList := &pmv1alpha1.PodMigrationJobList{}
			if err := cl.List(context.Background(), pmjList); err != nil {
				t.Fatalf("failed to list PMJs: %v", err)
			}
			if len(pmjList.Items) != 1 {
				t.Fatalf("expected 1 PMJ triggered by signal %s, got %d", sig, len(pmjList.Items))
			}
		})
	}
}

func TestNodePreemptionReconciler_IdempotentOnSubsequentReconciles(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	node := createTestNode("spot-node-idempotent", true, "taint-gke")
	pod := createTestPod("pod-idempotent", "default", "spot-node-idempotent", "2Gi", "uid-idem")
	psp := createTestPSP("psp-default", "default")

	recorder := record.NewFakeRecorder(10)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(node, pod, psp).
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
		SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
	}

	// First pass creates the PMJ
	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "spot-node-idempotent"},
	})
	if err != nil {
		t.Fatalf("first reconcile failed: %v", err)
	}

	// Second pass should be a clean no-op
	_, err = r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "spot-node-idempotent"},
	})
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}

	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := cl.List(context.Background(), pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}
	if len(pmjList.Items) != 1 {
		t.Fatalf("expected exactly 1 PMJ across multiple reconciles, got %d", len(pmjList.Items))
	}
}

func TestNodePreemptionReconciler_SortOrderMaximizesSurvivorCount(t *testing.T) {
	// Demonstrates that ascending memory request sort strictly outperforms descending sort
	// under a tight node preemption budget (15 GiB).
	// Ascending: 1Gi + 4Gi + 8Gi = 13Gi <= 15Gi -> 3 pods migrate.
	// Descending: 12Gi + 1Gi = 13Gi -> only 2 pods migrate (8Gi and 4Gi starved).
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	node := createTestNode("spot-sort-test", true, "taint-gke")
	// Pass in descending order
	pod12 := createTestPod("pod-12gi", "default", "spot-sort-test", "12Gi", "uid-12")
	pod8 := createTestPod("pod-8gi", "default", "spot-sort-test", "8Gi", "uid-8")
	pod4 := createTestPod("pod-4gi", "default", "spot-sort-test", "4Gi", "uid-4")
	pod1 := createTestPod("pod-1gi", "default", "spot-sort-test", "1Gi", "uid-1")
	psp := createTestPSP("psp-default", "default")

	recorder := record.NewFakeRecorder(20)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, PodNodeNameIndex, PodNodeNameIndexValue).
		WithObjects(node, pod12, pod8, pod4, pod1, psp).
		Build()

	r := &NodePreemptionReconciler{
		Client:                  cl,
		Scheme:                  scheme,
		Recorder:                recorder,
		DefaultMigrationTimeout: 10 * time.Minute,
		SpotPreemptionBudget:    15 * 1024 * 1024 * 1024,
	}

	_, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Name: "spot-sort-test"},
	})
	if err != nil {
		t.Fatalf("unexpected reconcile error: %v", err)
	}

	pmjList := &pmv1alpha1.PodMigrationJobList{}
	if err := cl.List(context.Background(), pmjList); err != nil {
		t.Fatalf("failed to list PMJs: %v", err)
	}

	// Must be 3 pods, not 2
	if len(pmjList.Items) != 3 {
		t.Fatalf("expected ascending sort to maximize survivors at 3 pods, got %d", len(pmjList.Items))
	}
}
