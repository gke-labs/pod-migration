package controller

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// envtestIndexedClient wraps a live envtest client.Client so controller field-index queries
// (PodAssignedPMJIndex, VolumeAttachmentPVIndex, PMJSnapshotRefIndex) work synchronously
// against kube-apiserver + etcd without informer-cache propagation delay.
type envtestIndexedClient struct {
	client.Client
}

func (c *envtestIndexedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	listOpts := &client.ListOptions{}
	for _, o := range opts {
		o.ApplyToList(listOpts)
	}

	if listOpts.FieldSelector != nil {
		reqs := listOpts.FieldSelector.Requirements()
		for _, req := range reqs {
			switch req.Field {
			case PodAssignedPMJIndex:
				var podList corev1.PodList
				forwardOpts := stripCustomFieldSelector(listOpts)
				if err := c.Client.List(ctx, &podList, forwardOpts...); err != nil {
					return err
				}
				target := list.(*corev1.PodList)
				target.ListMeta = podList.ListMeta
				target.Items = nil
				for i := range podList.Items {
					vals := PodAssignedPMJIndexValue(&podList.Items[i])
					for _, v := range vals {
						if v == req.Value {
							target.Items = append(target.Items, podList.Items[i])
							break
						}
					}
				}
				return nil

			case VolumeAttachmentPVIndex:
				var vaList storagev1.VolumeAttachmentList
				forwardOpts := stripCustomFieldSelector(listOpts)
				if err := c.Client.List(ctx, &vaList, forwardOpts...); err != nil {
					return err
				}
				target := list.(*storagev1.VolumeAttachmentList)
				target.ListMeta = vaList.ListMeta
				target.Items = nil
				for i := range vaList.Items {
					vals := VolumeAttachmentPVIndexValue(&vaList.Items[i])
					for _, v := range vals {
						if v == req.Value {
							target.Items = append(target.Items, vaList.Items[i])
							break
						}
					}
				}
				return nil

			case PMJSnapshotRefIndex:
				var pmjList pmv1alpha1.PodMigrationJobList
				forwardOpts := stripCustomFieldSelector(listOpts)
				if err := c.Client.List(ctx, &pmjList, forwardOpts...); err != nil {
					return err
				}
				target := list.(*pmv1alpha1.PodMigrationJobList)
				target.ListMeta = pmjList.ListMeta
				target.Items = nil
				for i := range pmjList.Items {
					vals := PMJSnapshotRefIndexValue(&pmjList.Items[i])
					for _, v := range vals {
						if v == req.Value {
							target.Items = append(target.Items, pmjList.Items[i])
							break
						}
					}
				}
				return nil

			case PMJParentKeyIndex:
				var pmjList pmv1alpha1.PodMigrationJobList
				forwardOpts := stripCustomFieldSelector(listOpts)
				if err := c.Client.List(ctx, &pmjList, forwardOpts...); err != nil {
					return err
				}
				target := list.(*pmv1alpha1.PodMigrationJobList)
				target.ListMeta = pmjList.ListMeta
				target.Items = nil
				for i := range pmjList.Items {
					vals := PMJParentKeyIndexValue(&pmjList.Items[i])
					for _, v := range vals {
						if v == req.Value {
							target.Items = append(target.Items, pmjList.Items[i])
							break
						}
					}
				}
				return nil
			}
		}
	}

	return c.Client.List(ctx, list, opts...)
}

func stripCustomFieldSelector(listOpts *client.ListOptions) []client.ListOption {
	var out []client.ListOption
	if listOpts.Namespace != "" {
		out = append(out, client.InNamespace(listOpts.Namespace))
	}
	if listOpts.LabelSelector != nil {
		out = append(out, client.MatchingLabelsSelector{Selector: listOpts.LabelSelector})
	}
	return out
}

func buildDynamicPodSnapshotCRDs() []*apiextensionsv1.CustomResourceDefinition {
	preserveUnknown := true
	makeCRD := func(plural, singular, kind string, scope apiextensionsv1.ResourceScope) *apiextensionsv1.CustomResourceDefinition {
		return &apiextensionsv1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: plural + ".podsnapshot.gke.io",
			},
			Spec: apiextensionsv1.CustomResourceDefinitionSpec{
				Group: "podsnapshot.gke.io",
				Names: apiextensionsv1.CustomResourceDefinitionNames{
					Plural:   plural,
					Singular: singular,
					Kind:     kind,
					ListKind: kind + "List",
				},
				Scope: scope,
				Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
					{
						Name:    "v1",
						Served:  true,
						Storage: true,
						Subresources: &apiextensionsv1.CustomResourceSubresources{
							Status: &apiextensionsv1.CustomResourceSubresourceStatus{},
						},
						Schema: &apiextensionsv1.CustomResourceValidation{
							OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
								Type:                   "object",
								XPreserveUnknownFields: &preserveUnknown,
							},
						},
					},
				},
			},
		}
	}

	return []*apiextensionsv1.CustomResourceDefinition{
		makeCRD("podsnapshotmanualtriggers", "podsnapshotmanualtrigger", "PodSnapshotManualTrigger", apiextensionsv1.NamespaceScoped),
		makeCRD("podsnapshots", "podsnapshot", "PodSnapshot", apiextensionsv1.NamespaceScoped),
		makeCRD("podsnapshotpolicies", "podsnapshotpolicy", "PodSnapshotPolicy", apiextensionsv1.NamespaceScoped),
		makeCRD("podsnapshotstorageconfigs", "podsnapshotstorageconfig", "PodSnapshotStorageConfig", apiextensionsv1.ClusterScoped),
	}
}

// totalInvariantViolations sums the process-global Prometheus counter across I1-I9.
// NOTE: Subtests in TestT1_EnvtestFaultInjectionMatrix must remain sequential (do not call
// t.Parallel()) because each subtest asserts before/after deltas against this shared counter.
func totalInvariantViolations() float64 {
	var sum float64
	for _, id := range []string{"I1", "I2", "I3", "I4", "I5", "I6", "I7", "I8", "I9"} {
		sum += testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues(id))
	}
	return sum
}

func createTestNamespace(t *testing.T, ctx context.Context, k8sClient client.Client, prefix string) string {
	t.Helper()
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: prefix + "-",
		},
	}
	if err := k8sClient.Create(ctx, ns); err != nil {
		t.Fatalf("failed to create test namespace %s: %v", prefix, err)
	}
	return ns.Name
}

// TestT1_EnvtestFaultInjectionMatrix is the Tier 1 (T1) per-merge fault-injection suite
// exercising Invariants I1-I9 against a real kube-apiserver + etcd (envtest) under
// --invariant-mode=strict.
func TestT1_EnvtestFaultInjectionMatrix(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest T1 fault-injection matrix")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd/bases"},
		CRDs:                  buildDynamicPodSnapshotCRDs(),
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("Failed to start envtest environment: %v", err)
	}
	t.Cleanup(func() {
		if stopErr := testEnv.Stop(); stopErr != nil {
			t.Errorf("failed to stop envtest environment: %v", stopErr)
		}
	})

	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add client-go scheme: %v", err)
	}
	if err := pmv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("failed to add pmv1alpha1 scheme: %v", err)
	}

	rawClient, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("failed to create envtest client: %v", err)
	}
	k8sClient := &envtestIndexedClient{Client: rawClient}
	ctx := context.Background()

	// -------------------------------------------------------------------------
	// Scenario 1: Concurrent PodGate Contenders (I1: AtMostOnceRestore, I3: GateLiveness)
	// Two replacement pods contend simultaneously for a single PodSnapshot in Restoring phase,
	// followed by a post-consumption third contender testing the Status.Consumed single-use
	// guard (#11) and strict-mode PMJ invariant enforcement.
	// -------------------------------------------------------------------------
	t.Run("S1_ConcurrentPodGateContenders_I1_I3", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s1-contend")
		beforeViolations := totalInvariantViolations()
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		replicas := int32(2)
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "web-rs-s1",
				Labels:    map[string]string{"app": "web-s1", appsv1.DefaultDeploymentUniqueLabelKey: "hash-s1"},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &replicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web-s1"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web-s1"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		if err := k8sClient.Create(ctx, rs); err != nil {
			t.Fatalf("create ReplicaSet: %v", err)
		}

		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-web-s1",
				Labels: map[string]string{
					util.LabelParentUID:       string(rs.UID),
					util.LabelParentKind:      "ReplicaSet",
					util.LabelPodTemplateHash: "hash-s1",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-origin-0"},
				TargetPodUID: "uid-origin-s1",
			},
		}
		if err := k8sClient.Create(ctx, pmj); err != nil {
			t.Fatalf("create PMJ: %v", err)
		}
		now := metav1.Now()
		pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmj.Status.SnapshotRef = "snap-s1-unique"
		pmj.Status.RestoringStartTime = &now
		if err := k8sClient.Status().Update(ctx, pmj); err != nil {
			t.Fatalf("update PMJ status to Restoring: %v", err)
		}

		isController := true
		makeContenderPod := func(name string) *corev1.Pod {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      name,
					Labels: map[string]string{
						"pod-migration.gke.io/enabled":         "true",
						"app":                                  "web-s1",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-s1",
					},
					Annotations: map[string]string{
						// Simulate admission/informer race where replacement pods claim
						// the same PMJ.
						util.AnnotationAssignedPMJ: pmj.Name,
					},
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "apps/v1",
							Kind:       "ReplicaSet",
							Name:       rs.Name,
							UID:        rs.UID,
							Controller: &isController,
						},
					},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
					Containers:      []corev1.Container{{Name: "app", Image: "busybox"}},
				},
			}
		}

		podA := makeContenderPod("web-contender-a")
		podB := makeContenderPod("web-contender-b")
		if err := k8sClient.Create(ctx, podA); err != nil {
			t.Fatalf("create podA: %v", err)
		}
		if err := k8sClient.Create(ctx, podB); err != nil {
			t.Fatalf("create podB: %v", err)
		}

		gateRec := &PodGateReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}
		pmjRec := &PodMigrationJobReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}

		// Drive concurrent PodGate reconciles against live envtest apiserver
		var wg sync.WaitGroup
		for _, podName := range []string{podA.Name, podB.Name} {
			wg.Add(1)
			go func(name string) {
				defer wg.Done()
				for attempt := 0; attempt < 4; attempt++ {
					_, _ = gateRec.Reconcile(ctx, ctrl.Request{
						NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
					})
				}
			}(podName)
		}
		wg.Wait()

		// Final deterministic pass to ensure any conflict-requeued contender settles
		for _, podName := range []string{podA.Name, podB.Name} {
			if _, err := gateRec.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: ns, Name: podName},
			}); err != nil {
				t.Fatalf("final PodGateReconciler.Reconcile(%s) failed: %v", podName, err)
			}
		}

		var gotA, gotB corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: podA.Name}, &gotA); err != nil {
			t.Fatalf("get podA: %v", err)
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: podB.Name}, &gotB); err != nil {
			t.Fatalf("get podB: %v", err)
		}

		// Assert I3: Zero stranded scheduling gates on either contender
		if podHasMigrationGate(&gotA) || podHasMigrationGate(&gotB) {
			t.Fatalf("I3 breach: expected both contenders to have scheduling gate removed; gotA=%v, gotB=%v",
				gotA.Spec.SchedulingGates, gotB.Spec.SchedulingGates)
		}

		// Assert I1 & I2: Exactly one pod got warm snapshot "snap-s1-unique" and the other got cold-start bypass ""
		warmCount := 0
		coldBypassCount := 0
		var winnerPod *corev1.Pod
		for _, p := range []*corev1.Pod{&gotA, &gotB} {
			psVal, exists := p.Annotations["podsnapshot.gke.io/ps-name"]
			if !exists {
				t.Fatalf("pod %s missing podsnapshot.gke.io/ps-name annotation after gate release", p.Name)
			}
			switch psVal {
			case "snap-s1-unique":
				warmCount++
				winnerPod = p
			case "":
				coldBypassCount++
			default:
				t.Fatalf("unexpected ps-name %q on pod %s", psVal, p.Name)
			}
		}
		if warmCount != 1 || coldBypassCount != 1 {
			t.Fatalf("I1 breach: expected 1 warm restore and 1 cold-start bypass, got warm=%d cold=%d", warmCount, coldBypassCount)
		}

		// Leg 2 (#11 regression probe): Strip AnnotationAssignedPMJ from winnerPod (while winnerPod
		// remains active with ps-name="snap-s1-unique" and PMJ is still in PhaseRestoring) so a
		// subsequent contender podC is NOT caught by ResolveCollision and directly exercises the
		// Status.Consumed single-use guard in pod_gate_controller.go:219.
		delete(winnerPod.Annotations, util.AnnotationAssignedPMJ)
		if err := k8sClient.Update(ctx, winnerPod); err != nil {
			t.Fatalf("strip AnnotationAssignedPMJ from winnerPod: %v", err)
		}
		podC := makeContenderPod("web-contender-c")
		if err := k8sClient.Create(ctx, podC); err != nil {
			t.Fatalf("create podC: %v", err)
		}
		if _, err := gateRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: podC.Name},
		}); err != nil {
			t.Fatalf("PodGateReconciler.Reconcile(podC) failed: %v", err)
		}
		// Reconcile PMJ while still in PhaseRestoring so strict-mode EvaluateI1 runs on the
		// post-podC state (if Status.Consumed guard in pod_gate_controller.go is disabled,
		// podC binds "snap-s1-unique" and strict-mode EvaluateI1 aborts PMJ to Failed here).
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmj.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile after podC failed: %v", err)
		}
		var midPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmj.Name}, &midPMJ); err != nil {
			t.Fatalf("get midPMJ after podC: %v", err)
		}
		if midPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			readyCond := meta.FindStatusCondition(midPMJ.Status.Conditions, "Ready")
			t.Fatalf("PMJ aborted under --invariant-mode=strict after contender podC: phase=%s readyCondition=%+v",
				midPMJ.Status.Phase, readyCond)
		}

		var gotC corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: podC.Name}, &gotC); err != nil {
			t.Fatalf("get podC: %v", err)
		}
		if podHasMigrationGate(&gotC) {
			t.Fatalf("I3 breach: expected podC scheduling gate removed, got %v", gotC.Spec.SchedulingGates)
		}
		if psVal, exists := gotC.Annotations["podsnapshot.gke.io/ps-name"]; !exists || psVal != "" {
			t.Fatalf("I1 breach (#11 Status.Consumed guard): expected podC cold-start bypass \"\", got %q (exists=%v)", psVal, exists)
		}

		// Mark the winner pod Ready and drive PMJ to Succeeded
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: winnerPod.Name}, winnerPod); err != nil {
			t.Fatalf("re-fetch winnerPod: %v", err)
		}
		winnerPod.Status.Phase = corev1.PodRunning
		winnerPod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		if err := k8sClient.Status().Update(ctx, winnerPod); err != nil {
			t.Fatalf("update winnerPod status to Ready: %v", err)
		}
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmj.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile failed: %v", err)
		}

		var finalPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmj.Name}, &finalPMJ); err != nil {
			t.Fatalf("get finalPMJ: %v", err)
		}
		if finalPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			t.Fatalf("expected PMJ phase Succeeded, got %s (conditions=%+v)",
				finalPMJ.Status.Phase, finalPMJ.Status.Conditions)
		}
		if delta := totalInvariantViolations() - beforeViolations; delta != 0 {
			t.Fatalf("expected 0 invariant violations on guarded S1 path, got delta=%v", delta)
		}

		// Leg 3 (Engine enforcement probe on S1 contenders): Verify that if a contender bypasses
		// the Status.Consumed guard and binds "snap-s1-unique" while an active Restoring PMJ tracks
		// an in-progress (non-Ready) winnerPod, a strict-mode PMJ reconcile detects the duplicate
		// consumption via EvaluateI1 and aborts the PMJ to Failed (Reason: InvariantViolation).
		winnerPod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionFalse},
		}
		if err := k8sClient.Status().Update(ctx, winnerPod); err != nil {
			t.Fatalf("reset winnerPod Ready=False for Leg 3: %v", err)
		}
		gotC.Annotations["podsnapshot.gke.io/ps-name"] = "snap-s1-unique"
		if err := k8sClient.Update(ctx, &gotC); err != nil {
			t.Fatalf("inject Consumed-bypass annotation on gotC: %v", err)
		}
		pmjBypass := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-web-s1-consumed-bypass",
				Labels: map[string]string{
					util.LabelParentUID:       string(rs.UID),
					util.LabelParentKind:      "ReplicaSet",
					util.LabelPodTemplateHash: "hash-s1",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "web-origin-0"},
				TargetPodUID: "uid-origin-s1",
			},
		}
		if err := k8sClient.Create(ctx, pmjBypass); err != nil {
			t.Fatalf("create pmjBypass: %v", err)
		}
		pmjBypass.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmjBypass.Status.SnapshotRef = "snap-s1-unique"
		pmjBypass.Status.Consumed = true
		pmjBypass.Status.GateReleased = true
		pmjBypass.Status.RestoredPodName = winnerPod.Name
		pmjBypass.Status.RestoredPodUID = string(winnerPod.UID)
		pmjBypass.Status.RestoringStartTime = &now
		if err := k8sClient.Status().Update(ctx, pmjBypass); err != nil {
			t.Fatalf("update pmjBypass status: %v", err)
		}
		beforeBypassI1 := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I1"))
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmjBypass.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile(pmjBypass) failed: %v", err)
		}
		var abortedBypassPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmjBypass.Name}, &abortedBypassPMJ); err != nil {
			t.Fatalf("get abortedBypassPMJ: %v", err)
		}
		if abortedBypassPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Fatalf("expected S1 Consumed-bypass PMJ to abort to Failed, got phase=%s", abortedBypassPMJ.Status.Phase)
		}
		bypassCond := meta.FindStatusCondition(abortedBypassPMJ.Status.Conditions, "Ready")
		if bypassCond == nil || bypassCond.Status != metav1.ConditionFalse || bypassCond.Reason != invariants.EventReasonInvariantViolation || !strings.Contains(bypassCond.Message, "[I1:AtMostOnceRestore]") {
			t.Fatalf("expected S1 Consumed-bypass Ready=False condition with Reason=InvariantViolation and [I1:AtMostOnceRestore], got %+v", bypassCond)
		}
		if afterBypassI1 := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I1")); afterBypassI1-beforeBypassI1 < 1 {
			t.Fatalf("expected I1 counter to increment on S1 Consumed-bypass, delta=%v", afterBypassI1-beforeBypassI1)
		}
	})

	// -------------------------------------------------------------------------
	// Scenario 2: Rolling Upgrade Mid-Drain (I6: RevisionAndIdentityFidelity)
	// Replacement pod spawned with mutated pod-template-hash; verify snapshot is
	// never applied across revisions and v2 pod is released for cold start.
	// -------------------------------------------------------------------------
	t.Run("S2_RollingUpgradeMidDrain_I6", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s2-rollout")
		beforeViolations := totalInvariantViolations()
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		zeroReplicas := int32(0)
		oneReplica := int32(1)
		rsV1 := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "app-rs-v1",
				Labels:    map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1"},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &zeroReplicas,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v1"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:1.0"}}},
				},
			},
		}
		rsV2 := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "app-rs-v2",
				Labels:    map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2"},
			},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &oneReplica,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2"}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "rollout-app", appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2"}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox:2.0"}}},
				},
			},
		}
		if err := k8sClient.Create(ctx, rsV1); err != nil {
			t.Fatalf("create rsV1: %v", err)
		}
		if err := k8sClient.Create(ctx, rsV2); err != nil {
			t.Fatalf("create rsV2: %v", err)
		}

		pmjV1 := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-rollout-v1",
				Labels: map[string]string{
					util.LabelParentUID:       string(rsV1.UID),
					util.LabelParentKind:      "ReplicaSet",
					util.LabelParentName:      rsV1.Name,
					util.LabelPodTemplateHash: "hash-v1",
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "app-rs-v1-pod0"},
				TargetPodUID: "uid-v1-pod0",
			},
		}
		if err := k8sClient.Create(ctx, pmjV1); err != nil {
			t.Fatalf("create pmjV1: %v", err)
		}
		now := metav1.Now()
		pmjV1.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmjV1.Status.SnapshotRef = "snap-v1-only"
		pmjV1.Status.RestoringStartTime = &now
		if err := k8sClient.Status().Update(ctx, pmjV1); err != nil {
			t.Fatalf("update pmjV1 status: %v", err)
		}

		isController := true
		podV2 := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "app-rs-v2-pod0",
				Labels: map[string]string{
					"pod-migration.gke.io/enabled":         "true",
					"app":                                  "rollout-app",
					appsv1.DefaultDeploymentUniqueLabelKey: "hash-v2",
				},
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: "apps/v1",
						Kind:       "ReplicaSet",
						Name:       rsV2.Name,
						UID:        rsV2.UID,
						Controller: &isController,
					},
				},
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				Containers:      []corev1.Container{{Name: "app", Image: "busybox:2.0"}},
			},
		}
		if err := k8sClient.Create(ctx, podV2); err != nil {
			t.Fatalf("create podV2: %v", err)
		}

		gateRec := &PodGateReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}
		pmjRec := &PodMigrationJobReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}

		// Reconcile v2 replacement pod: must NOT adopt v1 PMJ, and must be released for cold start
		if _, err := gateRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: podV2.Name},
		}); err != nil {
			t.Fatalf("gateRec.Reconcile(podV2) failed: %v", err)
		}

		var gotPodV2 corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: podV2.Name}, &gotPodV2); err != nil {
			t.Fatalf("get podV2: %v", err)
		}
		if podHasMigrationGate(&gotPodV2) {
			t.Fatalf("expected v2 pod scheduling gate to be released for cold start")
		}
		if got := gotPodV2.Annotations["podsnapshot.gke.io/ps-name"]; got != "" {
			t.Fatalf("I6 breach: v2 pod bound to v1 snapshot %q", got)
		}

		// Reconcile v1 PMJ: since rsV1 has scaled to 0 replicas, PMJ concludes SucceededWithoutRestore
		// and is cleaned up (#59) without any I6 violation.
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmjV1.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile(pmjV1) failed: %v", err)
		}

		if delta := totalInvariantViolations() - beforeViolations; delta != 0 {
			t.Fatalf("expected 0 invariant violations in S2, got delta=%v", delta)
		}
	})

	// -------------------------------------------------------------------------
	// Scenario 3: PDB HTTP 429 Eviction Refusal (I7: DisruptionBoundingPDBCompliance)
	// Origin pod protected by minAvailable: 1 PDB with DisruptionsAllowed: 0 in real
	// kube-apiserver; verify controller uses pods/eviction subresource, receives HTTP 429,
	// sets BlockedByPDB=True, and does NOT issue a raw DELETE.
	// -------------------------------------------------------------------------
	t.Run("S3_PDB429EvictionRefusal_I7", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s3-pdb")
		beforeViolations := totalInvariantViolations()
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		originPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pdb-guarded-0",
				Labels:    map[string]string{"app": "pdb-guarded"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, originPod); err != nil {
			t.Fatalf("create originPod: %v", err)
		}
		originPod.Status.Phase = corev1.PodRunning
		originPod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		if err := k8sClient.Status().Update(ctx, originPod); err != nil {
			t.Fatalf("update originPod status: %v", err)
		}

		minAvail := intstr.FromInt(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pdb-strict-100",
			},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvail,
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pdb-guarded"}},
			},
		}
		if err := k8sClient.Create(ctx, pdb); err != nil {
			t.Fatalf("create PDB: %v", err)
		}
		pdb.Status.DisruptionsAllowed = 0
		pdb.Status.CurrentHealthy = 1
		pdb.Status.DesiredHealthy = 1
		pdb.Status.ExpectedPods = 1
		pdb.Status.ObservedGeneration = pdb.Generation
		if err := k8sClient.Status().Update(ctx, pdb); err != nil {
			t.Fatalf("update PDB status: %v", err)
		}

		evictStart := metav1.NewTime(time.Now().Add(-45 * time.Second))
		pmj := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-pdb-guarded",
				Annotations: map[string]string{
					util.AnnotationEvictingSince: evictStart.Time.Format(time.RFC3339Nano),
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: originPod.Name},
				TargetPodUID: string(originPod.UID),
			},
		}
		if err := k8sClient.Create(ctx, pmj); err != nil {
			t.Fatalf("create PMJ: %v", err)
		}
		pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
		pmj.Status.SnapshotRef = "snap-pdb-1"
		pmj.Status.EvictingStartTime = &evictStart
		if err := k8sClient.Status().Update(ctx, pmj); err != nil {
			t.Fatalf("update PMJ status to Evicting: %v", err)
		}

		pmjRec := &PodMigrationJobReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}

		res, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmj.Name},
		})
		if err != nil {
			t.Fatalf("pmjRec.Reconcile during PDB block failed: %v", err)
		}
		if res.RequeueAfter == 0 {
			t.Fatalf("expected backoff RequeueAfter when blocked by PDB, got %+v", res)
		}

		var gotPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmj.Name}, &gotPMJ); err != nil {
			t.Fatalf("get PMJ: %v", err)
		}
		if gotPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseEvicting {
			t.Fatalf("expected PMJ to remain in Evicting while blocked by PDB, got %s", gotPMJ.Status.Phase)
		}
		pdbCond := meta.FindStatusCondition(gotPMJ.Status.Conditions, "BlockedByPDB")
		if pdbCond == nil || pdbCond.Status != metav1.ConditionTrue || pdbCond.Reason != "PDBBudgetExhausted" {
			t.Fatalf("expected BlockedByPDB=True (Reason=PDBBudgetExhausted), got %+v", pdbCond)
		}

		// Verify origin pod was NOT raw-deleted
		var stillAlive corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: originPod.Name}, &stillAlive); err != nil {
			t.Fatalf("I7 breach: origin pod was deleted while protected by PDB: %v", err)
		}

		if delta := totalInvariantViolations() - beforeViolations; delta != 0 {
			t.Fatalf("expected 0 invariant violations in S3, got delta=%v", delta)
		}
	})

	// -------------------------------------------------------------------------
	// Scenario 4: Runtime Restore Crash & Cold-Start Fallback (I9: DeterministicFallbackOverCrashloop)
	// Subcase A: Replacement pod enters StartError (exit 128) with fatal gVisor restore signature;
	// verify controller fails PMJ, deletes crashed pod, and triggers cold-start replacement.
	// Subcase B: Runtime emits RestoreFailed Warning event and falls back to cold start;
	// verify controller transitions PMJ to SucceededWithoutRestore within 1 reconcile.
	// -------------------------------------------------------------------------
	t.Run("S4_RuntimeRestoreCrash_I9", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s4-crash")
		beforeViolations := totalInvariantViolations()
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		pmjRec := &PodMigrationJobReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}

		// Subcase A: Fatal StartError (exit 128) restore crash
		crashedPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "restored-pod-crash",
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ:   "pmj-crash-a",
					"podsnapshot.gke.io/ps-name": "snap-corrupt-a",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, crashedPod); err != nil {
			t.Fatalf("create crashedPod: %v", err)
		}
		crashedPod.Status.Phase = corev1.PodPending
		crashedPod.Status.ContainerStatuses = []corev1.ContainerStatus{
			{
				Name: "app",
				State: corev1.ContainerState{
					Terminated: &corev1.ContainerStateTerminated{
						ExitCode: 128,
						Reason:   "StartError",
						Message:  `failed to create containerd task: failed to create shim task: OCI runtime restore failed: restoring container: corrupt image state`,
					},
				},
			},
		}
		if err := k8sClient.Status().Update(ctx, crashedPod); err != nil {
			t.Fatalf("update crashedPod status: %v", err)
		}

		now := metav1.Now()
		pmjA := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-crash-a",
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "origin-crash-a"},
				TargetPodUID: "uid-origin-crash-a",
			},
		}
		if err := k8sClient.Create(ctx, pmjA); err != nil {
			t.Fatalf("create pmjA: %v", err)
		}
		pmjA.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmjA.Status.SnapshotRef = "snap-corrupt-a"
		pmjA.Status.Consumed = true
		pmjA.Status.GateReleased = true
		pmjA.Status.RestoredPodName = crashedPod.Name
		pmjA.Status.RestoredPodUID = string(crashedPod.UID)
		pmjA.Status.RestoringStartTime = &now
		if err := k8sClient.Status().Update(ctx, pmjA); err != nil {
			t.Fatalf("update pmjA status: %v", err)
		}

		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmjA.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile(pmjA) failed: %v", err)
		}

		var gotPMJA pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmjA.Name}, &gotPMJA); err != nil {
			t.Fatalf("get pmjA: %v", err)
		}
		if gotPMJA.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Fatalf("expected pmjA to transition to Failed on fatal restore crash, got %s", gotPMJA.Status.Phase)
		}
		crashCond := meta.FindStatusCondition(gotPMJA.Status.Conditions, ConditionRestoreCrashed)
		if crashCond == nil || crashCond.Status != metav1.ConditionTrue {
			t.Fatalf("expected RestoreCrashed=True condition on pmjA, got %+v", crashCond)
		}
		var checkPod corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: crashedPod.Name}, &checkPod); !apierrors.IsNotFound(err) {
			t.Fatalf("expected crashed replacement pod to be deleted for cold start, got err=%v", err)
		}

		// Subcase B: Runtime emits RestoreFailed Warning event and falls back to cold start -> SucceededWithoutRestore
		fallbackPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "restored-pod-fallback",
				Annotations: map[string]string{
					util.AnnotationAssignedPMJ:   "pmj-fallback-b",
					"podsnapshot.gke.io/ps-name": "snap-corrupt-b",
				},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, fallbackPod); err != nil {
			t.Fatalf("create fallbackPod: %v", err)
		}
		fallbackPod.Status.Phase = corev1.PodRunning
		fallbackPod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue},
		}
		if err := k8sClient.Status().Update(ctx, fallbackPod); err != nil {
			t.Fatalf("update fallbackPod status: %v", err)
		}

		restoreStart := metav1.NewTime(time.Now().Add(-5 * time.Second))
		pmjB := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-fallback-b",
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "origin-fallback-b"},
				TargetPodUID: "uid-origin-fallback-b",
			},
		}
		if err := k8sClient.Create(ctx, pmjB); err != nil {
			t.Fatalf("create pmjB: %v", err)
		}
		pmjB.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		pmjB.Status.SnapshotRef = "snap-corrupt-b"
		pmjB.Status.Consumed = true
		pmjB.Status.GateReleased = true
		pmjB.Status.RestoredPodName = fallbackPod.Name
		pmjB.Status.RestoredPodUID = string(fallbackPod.UID)
		pmjB.Status.RestoringStartTime = &restoreStart
		if err := k8sClient.Status().Update(ctx, pmjB); err != nil {
			t.Fatalf("update pmjB status: %v", err)
		}

		fallbackEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "fallback-warning-event",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind:      "Pod",
				Namespace: ns,
				Name:      fallbackPod.Name,
				UID:       fallbackPod.UID,
			},
			Reason:        "RestoreFailed",
			Message:       "Failed to restore snapshot snap-corrupt-b, falling back to a cold start",
			Type:          corev1.EventTypeWarning,
			LastTimestamp: metav1.Now(),
		}
		if err := k8sClient.Create(ctx, fallbackEvent); err != nil {
			t.Fatalf("create fallbackEvent: %v", err)
		}

		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: pmjB.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile(pmjB) failed: %v", err)
		}

		var gotPMJB pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmjB.Name}, &gotPMJB); err != nil {
			t.Fatalf("get pmjB: %v", err)
		}
		if gotPMJB.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore {
			t.Fatalf("expected pmjB to transition to SucceededWithoutRestore on RestoreFailed event, got %s", gotPMJB.Status.Phase)
		}

		if delta := totalInvariantViolations() - beforeViolations; delta != 0 {
			t.Fatalf("expected 0 invariant violations in S4, got delta=%v", delta)
		}
	})

	// -------------------------------------------------------------------------
	// Scenario 5: Stalled In-Flight Deletion Deferral & Trigger Cleanup (I4, I5)
	// PodMigration deleted while an active PMJ with an owned PSMT is stuck past its
	// migration timeout; verify PMJ fails loud and deletes its PSMT (I4/I5), and
	// PodMigration cleans up cluster-scoped PSSC + PSP and removes its finalizer.
	// -------------------------------------------------------------------------
	t.Run("S5_StalledInFlightDeletionDeferral_I4_I5", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s5-cleanup")
		beforeViolations := totalInvariantViolations()
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		mig := &pmv1alpha1.PodMigration{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "mig-s5",
			},
			Spec: pmv1alpha1.PodMigrationSpec{
				Storage: pmv1alpha1.StorageSpec{
					Location: "gs://test-bucket-s5/checkpoints",
				},
			},
		}
		if err := k8sClient.Create(ctx, mig); err != nil {
			t.Fatalf("create PodMigration: %v", err)
		}

		migRec := &PodMigrationReconciler{
			Client:          k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}
		// Initial reconcile adds StorageCleanupFinalizer and creates cluster-scoped PSSC + namespaced PSP
		for i := 0; i < 2; i++ {
			if _, err := migRec.Reconcile(ctx, ctrl.Request{
				NamespacedName: types.NamespacedName{Namespace: ns, Name: mig.Name},
			}); err != nil {
				t.Fatalf("migRec initial Reconcile failed: %v", err)
			}
		}

		psscName := getPSSCName(ns, mig.Name)
		pssc := &unstructured.Unstructured{}
		pssc.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotStorageConfig",
		})
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: psscName}, pssc); err != nil {
			t.Fatalf("expected cluster-scoped PSSC %s to exist: %v", psscName, err)
		}

		// Create an active PMJ with an owned PSMT whose DefaultMigrationTimeout (50ms) expires
		stalledPMJ := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-stalled-s5",
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "s5-pod-0"},
				TargetPodUID: "uid-s5-pod-0",
			},
		}
		if err := k8sClient.Create(ctx, stalledPMJ); err != nil {
			t.Fatalf("create stalledPMJ: %v", err)
		}
		stalledPMJ.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
		if err := k8sClient.Status().Update(ctx, stalledPMJ); err != nil {
			t.Fatalf("update stalledPMJ status: %v", err)
		}

		psmtName := util.FormatPSMTName("s5-pod-0", "uid-s5-pod-0")
		psmt := &unstructured.Unstructured{}
		psmt.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		psmt.SetNamespace(ns)
		psmt.SetName(psmtName)
		psmt.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: pmv1alpha1.GroupVersion.String(),
				Kind:       "PodMigrationJob",
				Name:       stalledPMJ.Name,
				UID:        stalledPMJ.UID,
			},
		})
		if err := k8sClient.Create(ctx, psmt); err != nil {
			t.Fatalf("create PSMT: %v", err)
		}

		// Delete PodMigration while stalledPMJ is still in flight -> deletion is deferred
		if err := k8sClient.Delete(ctx, mig); err != nil {
			t.Fatalf("delete PodMigration: %v", err)
		}
		if _, err := migRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: mig.Name},
		}); err != nil {
			t.Fatalf("migRec.Reconcile during deferral failed: %v", err)
		}

		// Wait 100ms so stalledPMJ exceeds DefaultMigrationTimeout (50ms), then reconcile PMJ
		time.Sleep(100 * time.Millisecond)
		pmjRec := &PodMigrationJobReconciler{
			Client:                  k8sClient,
			APIReader:               k8sClient,
			Scheme:                  scheme,
			Recorder:                recorder,
			DefaultMigrationTimeout: 50 * time.Millisecond,
			InvariantEngine:         engine,
		}
		if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: stalledPMJ.Name},
		}); err != nil {
			t.Fatalf("pmjRec.Reconcile(stalledPMJ) failed: %v", err)
		}

		var gotStalledPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: stalledPMJ.Name}, &gotStalledPMJ); err != nil {
			t.Fatalf("get stalledPMJ: %v", err)
		}
		if gotStalledPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Fatalf("I4 breach: expected stalledPMJ to transition to Failed on timeout, got %s", gotStalledPMJ.Status.Phase)
		}

		// Assert I5: PSMT cleaned up upon PMJ transitioning to Failed
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: psmtName}, psmt); !apierrors.IsNotFound(err) {
			t.Fatalf("I5 breach: expected PSMT %s to be deleted on PMJ failure, got err=%v", psmtName, err)
		}

		// Reconcile PodMigration again now that in-flight PMJ is terminal -> PSSC deleted & finalizer removed
		if _, err := migRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: mig.Name},
		}); err != nil {
			t.Fatalf("migRec final Reconcile failed: %v", err)
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: psscName}, pssc); !apierrors.IsNotFound(err) {
			t.Fatalf("I5 breach: expected cluster-scoped PSSC %s to be deleted on PodMigration finalization, got err=%v", psscName, err)
		}
		var deletedMig pmv1alpha1.PodMigration
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: mig.Name}, &deletedMig); !apierrors.IsNotFound(err) {
			t.Fatalf("expected PodMigration %s to be garbage-collected after finalizer removal, got err=%v", mig.Name, err)
		}

		if delta := totalInvariantViolations() - beforeViolations; delta != 0 {
			t.Fatalf("expected 0 invariant violations in S5, got delta=%v", delta)
		}
	})

	// -------------------------------------------------------------------------
	// Acceptance Criterion #2: Deliberate Regression Injection
	// Inject a split-brain dual-consumer state (bypassing status.consumed) on an
	// active PMJ in envtest and verify ModeStrict fails the PMJ with InvariantViolation,
	// increments pod_migration_invariant_violations_total, emits a Warning event,
	// and cleans up its PSMT.
	// -------------------------------------------------------------------------
	t.Run("S6_DeliberateRegressionBreach_FailsLoudInStrictMode", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t1-s6-breach")
		beforeI1 := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I1"))
		recorder := record.NewFakeRecorder(32)
		engine := invariants.NewEngine(invariants.ModeStrict, recorder)

		// Inject two non-terminating pods bound to the same snapshot "snap-dup-breach"
		for _, podName := range []string{"dup-pod-1", "dup-pod-2"} {
			p := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      podName,
					Annotations: map[string]string{
						"podsnapshot.gke.io/ps-name": "snap-dup-breach",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
				},
			}
			if err := k8sClient.Create(ctx, p); err != nil {
				t.Fatalf("create %s: %v", podName, err)
			}
		}

		now := metav1.Now()
		breachPMJ := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      "pmj-breach-i1",
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: "origin-breach"},
				TargetPodUID: "uid-origin-breach",
			},
		}
		if err := k8sClient.Create(ctx, breachPMJ); err != nil {
			t.Fatalf("create breachPMJ: %v", err)
		}
		breachPMJ.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
		breachPMJ.Status.SnapshotRef = "snap-dup-breach"
		breachPMJ.Status.Consumed = true
		breachPMJ.Status.GateReleased = true
		breachPMJ.Status.RestoredPodName = "dup-pod-1"
		breachPMJ.Status.RestoringStartTime = &now
		if err := k8sClient.Status().Update(ctx, breachPMJ); err != nil {
			t.Fatalf("update breachPMJ status: %v", err)
		}

		// Attach an active PSMT to verify strict mode cleans it up when failing the PMJ
		psmtName := util.FormatPSMTName("origin-breach", "uid-origin-breach")
		psmt := &unstructured.Unstructured{}
		psmt.SetGroupVersionKind(schema.GroupVersionKind{
			Group:   "podsnapshot.gke.io",
			Version: "v1",
			Kind:    "PodSnapshotManualTrigger",
		})
		psmt.SetNamespace(ns)
		psmt.SetName(psmtName)
		psmt.SetOwnerReferences([]metav1.OwnerReference{
			{
				APIVersion: pmv1alpha1.GroupVersion.String(),
				Kind:       "PodMigrationJob",
				Name:       breachPMJ.Name,
				UID:        breachPMJ.UID,
			},
		})
		if err := k8sClient.Create(ctx, psmt); err != nil {
			t.Fatalf("create breach PSMT: %v", err)
		}

		pmjRec := &PodMigrationJobReconciler{
			Client:          k8sClient,
			APIReader:       k8sClient,
			Scheme:          scheme,
			Recorder:        recorder,
			InvariantEngine: engine,
		}
		res, err := pmjRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: breachPMJ.Name},
		})
		if err != nil {
			t.Fatalf("pmjRec.Reconcile(breachPMJ) failed: %v", err)
		}
		if res.Requeue || res.RequeueAfter != 0 {
			t.Fatalf("expected strict mode to suppress requeue on terminated PMJ, got %+v", res)
		}

		afterI1 := testutil.ToFloat64(metrics.InvariantViolationsTotal.WithLabelValues("I1"))
		if afterI1 <= beforeI1 {
			t.Fatalf("expected pod_migration_invariant_violations_total{invariant=\"I1\"} to increment; before=%v after=%v", beforeI1, afterI1)
		}

		var gotBreachPMJ pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: breachPMJ.Name}, &gotBreachPMJ); err != nil {
			t.Fatalf("get breachPMJ: %v", err)
		}
		if gotBreachPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed {
			t.Fatalf("expected strict mode to force breachPMJ to Failed, got %s", gotBreachPMJ.Status.Phase)
		}
		invCond := meta.FindStatusCondition(gotBreachPMJ.Status.Conditions, "Ready")
		if invCond == nil || invCond.Status != metav1.ConditionFalse || invCond.Reason != invariants.EventReasonInvariantViolation || !strings.Contains(invCond.Message, "[I1:AtMostOnceRestore]") {
			t.Fatalf("expected Ready=False condition with Reason=InvariantViolation and [I1:AtMostOnceRestore], got %+v", invCond)
		}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: psmtName}, psmt); !apierrors.IsNotFound(err) {
			t.Fatalf("expected strict mode to clean up PSMT %s, got err=%v", psmtName, err)
		}
	})
}
