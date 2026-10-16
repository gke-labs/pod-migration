package controller

import (
	"context"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
)

// TestEvictionBudgetChecker_Envtest checks the dry-run eviction against a real
// API server, with a hand-set PodDisruptionBudget status (envtest runs no
// disruption controller): an exhausted budget refuses it, a free budget allows
// it, and the pod is never deleted.
func TestEvictionBudgetChecker_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest budget check test")
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
	c, err := client.New(cfg, client.Options{Scheme: budgetScheme()})
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}
	ctx := context.Background()
	const ns = "budget-test"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "source", Namespace: ns, Labels: map[string]string{"app": "db"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	// The eviction API ignores budgets for pods that aren't running, so make it a running, ready pod.
	pod.Status = corev1.PodStatus{
		Phase:      corev1.PodRunning,
		Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}},
	}
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatalf("setting pod status: %v", err)
	}

	minAvailable := intstr.FromInt32(1)
	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{Name: "db", Namespace: ns},
		Spec: policyv1.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "db"}},
		},
	}
	if err := c.Create(ctx, pdb); err != nil {
		t.Fatalf("creating PDB: %v", err)
	}
	setAllowed := func(allowed int32) {
		t.Helper()
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "db"}, pdb); err != nil {
			t.Fatalf("getting PDB: %v", err)
		}
		pdb.Status = policyv1.PodDisruptionBudgetStatus{
			ObservedGeneration: pdb.Generation,
			DisruptionsAllowed: allowed,
			CurrentHealthy:     1 + allowed,
			DesiredHealthy:     1,
			ExpectedPods:       1 + allowed,
		}
		if err := c.Status().Update(ctx, pdb); err != nil {
			t.Fatalf("setting PDB status: %v", err)
		}
	}

	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: ns},
		Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: string(pod.UID)},
	}
	checker := EvictionBudgetChecker{Client: c}

	setAllowed(0)
	if reason, err := checker.Check(ctx, job, "source"); reason != reasonBudgetExhausted {
		t.Fatalf("exhausted budget: Check = (%q, %v), want %q", reason, err, reasonBudgetExhausted)
	}
	setAllowed(1)
	if reason, err := checker.Check(ctx, job, "source"); reason != reasonBudgetAllowed || err != nil {
		t.Fatalf("free budget: Check = (%q, %v), want %q", reason, err, reasonBudgetAllowed)
	}
	// Still there: the eviction was a dry run.
	got := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "source"}, got); err != nil || got.DeletionTimestamp != nil {
		t.Fatalf("the dry-run eviction touched the pod: err=%v deletionTimestamp=%v", err, got.DeletionTimestamp)
	}
	// A pod replaced under the same name is refused by the UID precondition.
	other := job.DeepCopy()
	other.Spec.TargetPodUID = "00000000-0000-0000-0000-000000000000"
	if reason, _ := checker.Check(ctx, other, "source"); reason != reasonBudgetCheckError {
		t.Errorf("UID mismatch: reason = %q, want %q", reason, reasonBudgetCheckError)
	}
}
