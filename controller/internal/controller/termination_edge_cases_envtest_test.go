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
	"net"
	"os"
	"strings"
	"testing"
	"time"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

func ptrTo[T any](v T) *T { return &v }

// TestT1_TerminationEdgeCases (#295) covers how a job ends, and what happens
// to its source, in three cases, against a real kube-apiserver + etcd
// (envtest) with the invariant engine in strict mode:
//
//   - The source is force-deleted (DELETE, not an eviction) while the job waits
//     in Pending for the budget (a 429 from a PodDisruptionBudget or a 403 from
//     an admission policy) or while it's in Snapshotting. The job fails with
//     PodNotFound, deletes its trigger, drops its finalizer, and the
//     replacement is released cold, unless the checkpoint already completed:
//     then the job waits for the snapshot and restores it.
//   - The migration times out, or the final eviction is denied, in Snapshotting
//     or Evicting after the engine may have stopped the source. The source
//     stays fenced (FenceKept) and the controller retries its cold eviction
//     until it's allowed, so it's never left fenced with nothing evicting it.
//   - The controller's webhooks can't be reached. The fence webhook fails
//     closed (failurePolicy Fail) and the eviction webhook fails open (Ignore).
//     Neither leaves a source fenced with nothing driving it.
func TestT1_TerminationEdgeCases(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest termination edge cases")
	}

	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{"../../config/crd/bases"},
		CRDs:                  append(buildDynamicPodSnapshotCRDs(), buildDynamicPodSnapshotCRDsForGroup(engine.CRIUAPIGroup)...),
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

	policy, err := eligibility.ParseRuntimeClassPolicy("gvisor,@default,runc")
	if err != nil {
		t.Fatalf("ParseRuntimeClassPolicy: %v", err)
	}
	registry, err := engine.NewDefaultRegistry(k8sClient, scheme, policy, engine.WithCRIUAPIGroup(engine.CRIUAPIGroup))
	if err != nil {
		t.Fatalf("NewDefaultRegistry: %v", err)
	}
	const migrationTimeout = time.Minute
	group := engine.CRIUAPIGroup
	recorder := record.NewFakeRecorder(4096)
	invEngine := invariants.NewEngine(invariants.ModeStrict, recorder)
	// newJobReconciler returns a reconciler with no in-memory state, as after
	// a controller restart.
	newJobReconciler := func() *PodMigrationJobReconciler {
		return &PodMigrationJobReconciler{
			Client:                  k8sClient,
			APIReader:               k8sClient,
			Scheme:                  scheme,
			Recorder:                recorder,
			InvariantEngine:         invEngine,
			Engines:                 registry,
			DefaultMigrationTimeout: migrationTimeout,
			BudgetChecker:           EvictionBudgetChecker{Client: k8sClient},
		}
	}
	gateRec := &PodGateReconciler{
		Client:          k8sClient,
		APIReader:       k8sClient,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: invEngine,
	}
	violationsBefore := totalInvariantViolations()
	t.Cleanup(func() {
		if delta := totalInvariantViolations() - violationsBefore; delta != 0 {
			t.Errorf("expected 0 invariant violations across the termination edge cases in strict mode, got %v", delta)
		}
	})

	// ---- helpers -----------------------------------------------------------

	reconcile := func(t *testing.T, rec *PodMigrationJobReconciler, ns, name string) ctrl.Result {
		t.Helper()
		res, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}})
		if err != nil {
			t.Fatalf("Reconcile(%s): %v", name, err)
		}
		return res
	}
	getJob := func(t *testing.T, ns, name string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		job := &pmv1alpha1.PodMigrationJob{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, job); err != nil {
			t.Fatalf("get job %s: %v", name, err)
		}
		return job
	}
	getPod := func(t *testing.T, ns, name string) (*corev1.Pod, bool) {
		t.Helper()
		pod := &corev1.Pod{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, pod); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false
			}
			t.Fatalf("get pod %s: %v", name, err)
		}
		return pod, true
	}
	newReplicaSet := func(t *testing.T, ns, app string) *appsv1.ReplicaSet {
		t.Helper()
		one := int32(1)
		rs := &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: app + "-rs", Labels: map[string]string{"app": app}},
			Spec: appsv1.ReplicaSetSpec{
				Replicas: &one,
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": app}},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": app}},
					Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
				},
			},
		}
		if err := k8sClient.Create(ctx, rs); err != nil {
			t.Fatalf("create ReplicaSet: %v", err)
		}
		return rs
	}
	ownedBy := func(rs *appsv1.ReplicaSet) []metav1.OwnerReference {
		isController := true
		return []metav1.OwnerReference{{
			APIVersion: "apps/v1", Kind: "ReplicaSet", Name: rs.Name, UID: rs.UID, Controller: &isController,
		}}
	}
	// newSource creates a running, Ready source pod owned by rs.
	newSource := func(t *testing.T, rs *appsv1.ReplicaSet, name string) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       rs.Namespace,
				Name:            name,
				Labels:          map[string]string{"app": rs.Labels["app"], "pod-migration.gke.io/enabled": "true"},
				OwnerReferences: ownedBy(rs),
			},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "busybox"}}},
		}
		if err := k8sClient.Create(ctx, pod); err != nil {
			t.Fatalf("create source: %v", err)
		}
		pod.Status = corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", Image: "busybox", ImageID: "busybox", ContainerID: "containerd://" + name, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
			}},
		}
		if err := k8sClient.Status().Update(ctx, pod); err != nil {
			t.Fatalf("update source status: %v", err)
		}
		return pod
	}
	// newJob creates a CRIU job for source in phase, with status set by setStatus.
	newJob := func(t *testing.T, rs *appsv1.ReplicaSet, source *corev1.Pod, name string, annotations map[string]string, setStatus func(*pmv1alpha1.PodMigrationJobStatus)) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		ann := map[string]string{engine.AnnotationEngine: engine.CRIUName}
		for k, v := range annotations {
			ann[k] = v
		}
		job := &pmv1alpha1.PodMigrationJob{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   rs.Namespace,
				Name:        name,
				Annotations: ann,
				Labels: map[string]string{
					util.LabelParentUID:  string(rs.UID),
					util.LabelParentKind: "ReplicaSet",
					util.LabelParentName: rs.Name,
				},
			},
			Spec: pmv1alpha1.PodMigrationJobSpec{
				PodRef:       corev1.LocalObjectReference{Name: source.Name},
				TargetPodUID: string(source.UID),
			},
		}
		if err := k8sClient.Create(ctx, job); err != nil {
			t.Fatalf("create job: %v", err)
		}
		setStatus(&job.Status)
		if err := k8sClient.Status().Update(ctx, job); err != nil {
			t.Fatalf("update job status: %v", err)
		}
		return job
	}
	forceDelete := func(t *testing.T, pod *corev1.Pod) {
		t.Helper()
		if err := k8sClient.Delete(ctx, pod, client.GracePeriodSeconds(0)); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("force-delete %s: %v", pod.Name, err)
		}
		if _, ok := getPod(t, pod.Namespace, pod.Name); ok {
			t.Fatalf("%s still exists after a force delete", pod.Name)
		}
	}
	trigger := func(ns, podName string, uid types.UID) *unstructured.Unstructured {
		tr := &unstructured.Unstructured{}
		tr.SetGroupVersionKind(snapshot.ManualTriggerGVK(group))
		tr.SetNamespace(ns)
		tr.SetName(util.FormatPSMTName(podName, string(uid)))
		return tr
	}
	triggerExists := func(t *testing.T, ns, podName string, uid types.UID) bool {
		t.Helper()
		tr := trigger(ns, podName, uid)
		err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tr), tr)
		if err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("get trigger: %v", err)
		}
		return err == nil
	}
	snapCond := func(cType, status, reason string, at time.Time) map[string]interface{} {
		return map[string]interface{}{
			"type": cType, "status": status, "reason": reason,
			"lastTransitionTime": at.UTC().Format(time.RFC3339),
		}
	}
	setSnapshotConditions := func(t *testing.T, ns, snapName string, conds ...map[string]interface{}) {
		t.Helper()
		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: snapName}, snap); err != nil {
			t.Fatalf("get snapshot %s: %v", snapName, err)
		}
		list := make([]interface{}, 0, len(conds))
		for _, c := range conds {
			list = append(list, c)
		}
		snap.Object["status"] = map[string]interface{}{"conditions": list}
		if err := k8sClient.Status().Update(ctx, snap); err != nil {
			t.Fatalf("update snapshot %s status: %v", snapName, err)
		}
	}
	fenceCond := func(job *pmv1alpha1.PodMigrationJob) *metav1.Condition {
		return meta.FindStatusCondition(job.Status.Conditions, ConditionSourceFenced)
	}
	readyReason := func(job *pmv1alpha1.PodMigrationJob) string {
		if c := meta.FindStatusCondition(job.Status.Conditions, "Ready"); c != nil {
			return c.Reason
		}
		return ""
	}
	// wantFailedPodNotFound checks a job that lost its source: Failed with
	// PodNotFound, no trigger left, and no finalizer once settled.
	wantFailedPodNotFound := func(t *testing.T, job *pmv1alpha1.PodMigrationJob, source *corev1.Pod) {
		t.Helper()
		got := getJob(t, job.Namespace, job.Name)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || readyReason(got) != "PodNotFound" {
			t.Fatalf("job phase=%s Ready reason=%q, want Failed/PodNotFound", got.Status.Phase, readyReason(got))
		}
		if triggerExists(t, job.Namespace, source.Name, source.UID) {
			t.Fatalf("the trigger for the deleted source still exists, so a checkpoint can still start")
		}
		if controllerutil.ContainsFinalizer(got, SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s after its source is gone", SourceFenceFinalizer)
		}
		if got.Status.EvictingStartTime != nil || got.Status.Consumed && got.Status.RestoredPodName == "" {
			t.Fatalf("a job that lost its source must not move toward a restore: %+v", got.Status)
		}
	}
	// wantColdReplacement creates the replacement the workload controller
	// would create, gated and assigned to job as the pod gate webhook would,
	// and checks that the pod gate releases it cold.
	wantColdReplacement := func(t *testing.T, rs *appsv1.ReplicaSet, job *pmv1alpha1.PodMigrationJob, name string) {
		t.Helper()
		repl := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:       rs.Namespace,
				Name:            name,
				Labels:          map[string]string{"app": rs.Labels["app"], "pod-migration.gke.io/enabled": "true"},
				Annotations:     map[string]string{util.AnnotationAssignedPMJ: job.Name},
				OwnerReferences: ownedBy(rs),
			},
			Spec: corev1.PodSpec{
				SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
				Containers:      []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, repl); err != nil {
			t.Fatalf("create replacement: %v", err)
		}
		if _, err := gateRec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: rs.Namespace, Name: name}}); err != nil {
			t.Fatalf("gateRec.Reconcile(%s): %v", name, err)
		}
		got, _ := getPod(t, rs.Namespace, name)
		if podHasMigrationGate(got) {
			t.Fatalf("the replacement is still gated after its job failed: %v", got.Spec.SchedulingGates)
		}
		for k, v := range got.Annotations {
			if strings.HasSuffix(k, "/ps-name") && v != "" {
				t.Fatalf("the replacement was ordered to restore %s=%q after its job failed", k, v)
			}
		}
		if j := getJob(t, job.Namespace, job.Name); !j.Status.Consumed || j.Status.RestoredPodName != name {
			t.Fatalf("the job doesn't record the cold replacement: consumed=%t restoredPod=%q", j.Status.Consumed, j.Status.RestoredPodName)
		}
	}
	// startCheckpoint takes a source through the fence and the trigger, and
	// has the engine start a checkpoint with the snapshot conditions conds.
	startCheckpoint := func(t *testing.T, rec *PodMigrationJobReconciler, rs *appsv1.ReplicaSet, leg string, conds ...map[string]interface{}) (*pmv1alpha1.PodMigrationJob, *corev1.Pod, string) {
		t.Helper()
		source := newSource(t, rs, leg+"-source")
		started := metav1.Now()
		job := newJob(t, rs, source, leg+"-pmj", nil, func(s *pmv1alpha1.PodMigrationJobStatus) {
			s.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
			s.SnapshottingStartTime = &started
		})
		tr := trigger(rs.Namespace, source.Name, source.UID)
		for i := 0; ; i++ {
			reconcile(t, rec, rs.Namespace, job.Name)
			err := k8sClient.Get(ctx, client.ObjectKeyFromObject(tr), tr)
			if err == nil {
				break
			}
			if !apierrors.IsNotFound(err) || i == 4 {
				t.Fatalf("trigger %s wasn't created: %v", tr.GetName(), err)
			}
		}
		pod, _ := getPod(t, rs.Namespace, source.Name)
		if pod.Labels[util.LabelFenced] != "true" || !readyHeldByFence(pod) {
			t.Fatalf("source isn't fenced before its checkpoint: labels=%v conditions=%v", pod.Labels, pod.Status.Conditions)
		}
		snapName := leg + "-snap"
		tr.Object["status"] = map[string]interface{}{"snapshotCreated": map[string]interface{}{"name": snapName}}
		if err := k8sClient.Status().Update(ctx, tr); err != nil {
			t.Fatalf("update trigger status: %v", err)
		}
		snap := &unstructured.Unstructured{}
		snap.SetGroupVersionKind(snapshot.PodSnapshotGVK(group))
		snap.SetNamespace(rs.Namespace)
		snap.SetName(snapName)
		snap.SetLabels(map[string]string{"gke-pod-snapshot-triggered-by": tr.GetName()})
		if err := k8sClient.Create(ctx, snap); err != nil {
			t.Fatalf("create snapshot: %v", err)
		}
		setSnapshotConditions(t, rs.Namespace, snapName, conds...)
		reconcile(t, rec, rs.Namespace, job.Name)
		if got := getJob(t, rs.Namespace, job.Name); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting || got.Status.SnapshotRef != snapName {
			t.Fatalf("mid-checkpoint: job phase=%s snapshotRef=%q, want Snapshotting/%s", got.Status.Phase, got.Status.SnapshotRef, snapName)
		}
		if !controllerutil.ContainsFinalizer(getJob(t, rs.Namespace, job.Name), SourceFenceFinalizer) {
			t.Fatalf("a job with a fenced source must hold %s", SourceFenceFinalizer)
		}
		return getJob(t, rs.Namespace, job.Name), source, snapName
	}
	// evictedOrGone reports whether the source was evicted.
	evictedOrGone := func(t *testing.T, pod *corev1.Pod) bool {
		t.Helper()
		got, ok := getPod(t, pod.Namespace, pod.Name)
		return !ok || got.DeletionTimestamp != nil || string(got.UID) != string(pod.UID)
	}
	wantKeptFenced := func(t *testing.T, job *pmv1alpha1.PodMigrationJob, source *corev1.Pod, when string) {
		t.Helper()
		got := getJob(t, job.Namespace, job.Name)
		if c := fenceCond(got); c == nil || c.Status != metav1.ConditionTrue || c.Reason != util.ReasonFenceKept {
			t.Fatalf("%s: SourceFenced = %+v, want True/%s", when, c, util.ReasonFenceKept)
		}
		if !controllerutil.ContainsFinalizer(got, SourceFenceFinalizer) {
			t.Fatalf("%s: the job dropped %s while its source is still fenced", when, SourceFenceFinalizer)
		}
		pod, ok := getPod(t, source.Namespace, source.Name)
		if !ok || pod.DeletionTimestamp != nil {
			t.Fatalf("%s: the source was evicted while its eviction is denied", when)
		}
		if pod.Labels[util.LabelFenced] != "true" || !readyHeldByFence(pod) {
			t.Fatalf("%s: a source that may be frozen lost its fence: labels=%v conditions=%v", when, pod.Labels, pod.Status.Conditions)
		}
	}
	// waitFor polls probe until done reports true.
	waitFor := func(t *testing.T, what string, probe func() error, done func(error) bool) {
		t.Helper()
		deadline := time.Now().Add(60 * time.Second)
		for {
			err := probe()
			if done(err) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s; last error: %v", what, err)
			}
			time.Sleep(200 * time.Millisecond)
		}
	}
	dryRunEvict := func(pod *corev1.Pod) func() error {
		return func() error {
			uid := pod.UID
			return k8sClient.SubResource("eviction").Create(ctx, pod, &policyv1.Eviction{
				ObjectMeta:    metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace},
				DeleteOptions: &metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}},
			}, client.DryRunAll)
		}
	}
	// denyEvictions installs an admission policy that answers every eviction
	// in ns with 403, as a disruption-window controller does outside its
	// window, and returns a func that lifts it. probe is a pod in ns that the
	// policy is checked against.
	denyEvictions := func(t *testing.T, ns string, probe *corev1.Pod) (lift func()) {
		t.Helper()
		name := "deny-evictions-" + ns
		fail := admissionregistrationv1.Fail
		vap := &admissionregistrationv1.ValidatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
				FailurePolicy: &fail,
				MatchConstraints: &admissionregistrationv1.MatchResources{
					ResourceRules: []admissionregistrationv1.NamedRuleWithOperations{{
						RuleWithOperations: admissionregistrationv1.RuleWithOperations{
							Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
							Rule: admissionregistrationv1.Rule{
								APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods/eviction"},
							},
						},
					}},
				},
				Validations: []admissionregistrationv1.Validation{{
					Expression: "false",
					Message:    "evictions are outside the disruption window",
					Reason:     ptrTo(metav1.StatusReasonForbidden),
				}},
			},
		}
		if err := k8sClient.Create(ctx, vap); err != nil {
			t.Fatalf("create eviction-denying policy: %v", err)
		}
		binding := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
				PolicyName:        name,
				ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
				MatchResources: &admissionregistrationv1.MatchResources{
					NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns}},
				},
			},
		}
		if err := k8sClient.Create(ctx, binding); err != nil {
			t.Fatalf("create eviction-denying policy binding: %v", err)
		}
		waitFor(t, "the eviction-denying policy", dryRunEvict(probe), apierrors.IsForbidden)
		lifted := false
		lift = func() {
			if lifted {
				return
			}
			lifted = true
			_ = k8sClient.Delete(ctx, binding)
			_ = k8sClient.Delete(ctx, vap)
			waitFor(t, "the eviction-denying policy to be lifted", dryRunEvict(probe), func(err error) bool { return !apierrors.IsForbidden(err) })
		}
		t.Cleanup(lift)
		return lift
	}
	// unreachableURL returns an https URL on a local port nothing listens on.
	unreachableURL := func(t *testing.T, path string) string {
		t.Helper()
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addr := l.Addr().String()
		_ = l.Close()
		return "https://" + addr + path
	}
	nsSelector := func(ns string) *metav1.LabelSelector {
		return &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": ns}}
	}
	isWebhookCallFailure := func(err error) bool {
		return err != nil && strings.Contains(err.Error(), "failed calling webhook")
	}
	// fenceWebhookDown registers the fence webhook as deploy.yaml does
	// (mpodfence: pods/status UPDATE on fenced pods, failurePolicy Fail),
	// pointed at an address nothing serves, scoped to ns. It waits until a
	// status write on a fenced probe pod is rejected.
	fenceWebhookDown := func(t *testing.T, ns string, probe *corev1.Pod) {
		t.Helper()
		fail := admissionregistrationv1.Fail
		none := admissionregistrationv1.SideEffectClassNone
		ifNeeded := admissionregistrationv1.IfNeededReinvocationPolicy
		timeout := int32(2)
		url := unreachableURL(t, "/mutate-v1-pod-fence")
		mwc := &admissionregistrationv1.MutatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: "mpodfence-down-" + ns},
			Webhooks: []admissionregistrationv1.MutatingWebhook{{
				Name:                    "mpodfence.podmigration.gke.io",
				AdmissionReviewVersions: []string{"v1"},
				SideEffects:             &none,
				FailurePolicy:           &fail,
				ReinvocationPolicy:      &ifNeeded,
				TimeoutSeconds:          &timeout,
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &url},
				ObjectSelector:          &metav1.LabelSelector{MatchLabels: map[string]string{util.LabelFenced: "true"}},
				NamespaceSelector:       nsSelector(ns),
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Update},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods/status"}},
				}},
			}},
		}
		if err := k8sClient.Create(ctx, mwc); err != nil {
			t.Fatalf("create the unreachable fence webhook: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, mwc) })
		waitFor(t, "the unreachable fence webhook", func() error {
			p, _ := getPod(t, probe.Namespace, probe.Name)
			p.Status.Message = fmt.Sprintf("probe %d", time.Now().UnixNano())
			return k8sClient.Status().Update(ctx, p)
		}, isWebhookCallFailure)
	}
	// evictionWebhookDown registers the eviction webhook as deploy.yaml does
	// (veviction: pods/eviction CREATE, failurePolicy Ignore), pointed at an
	// address nothing serves, scoped to ns. It's registered with Fail first,
	// until an eviction of probe is rejected, so the API server has loaded
	// it, and then switched to Ignore.
	evictionWebhookDown := func(t *testing.T, ns string, probe *corev1.Pod) {
		t.Helper()
		fail := admissionregistrationv1.Fail
		ignore := admissionregistrationv1.Ignore
		noneOnDryRun := admissionregistrationv1.SideEffectClassNoneOnDryRun
		timeout := int32(2)
		url := unreachableURL(t, "/validate-v1-pod-eviction")
		vwc := &admissionregistrationv1.ValidatingWebhookConfiguration{
			ObjectMeta: metav1.ObjectMeta{Name: "veviction-down-" + ns},
			Webhooks: []admissionregistrationv1.ValidatingWebhook{{
				Name:                    "veviction.podmigration.gke.io",
				AdmissionReviewVersions: []string{"v1"},
				SideEffects:             &noneOnDryRun,
				FailurePolicy:           &fail,
				TimeoutSeconds:          &timeout,
				ClientConfig:            admissionregistrationv1.WebhookClientConfig{URL: &url},
				NamespaceSelector:       nsSelector(ns),
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.Create},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods/eviction"}},
				}},
			}},
		}
		if err := k8sClient.Create(ctx, vwc); err != nil {
			t.Fatalf("create the unreachable eviction webhook: %v", err)
		}
		t.Cleanup(func() { _ = k8sClient.Delete(ctx, vwc) })
		waitFor(t, "the unreachable eviction webhook", dryRunEvict(probe), isWebhookCallFailure)
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(vwc), vwc); err != nil {
			t.Fatalf("get eviction webhook: %v", err)
		}
		vwc.Webhooks[0].FailurePolicy = &ignore
		if err := k8sClient.Update(ctx, vwc); err != nil {
			t.Fatalf("switch the eviction webhook to Ignore: %v", err)
		}
		waitFor(t, "the eviction webhook to fail open", dryRunEvict(probe), func(err error) bool { return err == nil })
	}

	// ---- 1. Forced deletion -------------------------------------------------

	// pendingDeferred creates a job that has recorded its source's facts and
	// waits in Pending because the budget check was refused.
	pendingDeferred := func(t *testing.T, rec *PodMigrationJobReconciler, rs *appsv1.ReplicaSet, source *corev1.Pod, wantStatus metav1.ConditionStatus, wantReason string) *pmv1alpha1.PodMigrationJob {
		t.Helper()
		job := newJob(t, rs, source, "pending-pmj", map[string]string{util.AnnotationMigrationTimeout: "15m"}, func(s *pmv1alpha1.PodMigrationJobStatus) {
			s.Phase = pmv1alpha1.PodMigrationJobPhasePending
			// Captured on an earlier pass, so only the budget wait makes
			// the job read its source again.
			s.PVsToDetach = []string{"pv-captured"}
			s.OriginNodeName = "node-a"
		})
		res := reconcile(t, rec, rs.Namespace, job.Name)
		got := getJob(t, rs.Namespace, job.Name)
		c := meta.FindStatusCondition(got.Status.Conditions, ConditionDisruptionBudgetChecked)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhasePending || c == nil || c.Status != wantStatus || c.Reason != wantReason {
			t.Fatalf("deferred: phase=%s DisruptionBudgetChecked=%+v, want Pending with %s/%s", got.Status.Phase, c, wantStatus, wantReason)
		}
		if res.RequeueAfter == 0 {
			t.Fatalf("deferred: result %+v, want a recheck", res)
		}
		if fenceCond(got) != nil || triggerExists(t, rs.Namespace, source.Name, source.UID) {
			t.Fatalf("deferred: nothing may be fenced or triggered before the budget allows it")
		}
		return got
	}

	t.Run("ForcedDelete_PendingDeferredByPDB429", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-pdb429")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "pdb429")
		source := newSource(t, rs, "pdb429-source")
		minAvail := intstr.FromInt(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "pdb429"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvail,
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "pdb429"}},
			},
		}
		if err := k8sClient.Create(ctx, pdb); err != nil {
			t.Fatalf("create PDB: %v", err)
		}
		pdb.Status = policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, CurrentHealthy: 1, DesiredHealthy: 1, ExpectedPods: 1, ObservedGeneration: pdb.Generation}
		if err := k8sClient.Status().Update(ctx, pdb); err != nil {
			t.Fatalf("update PDB status: %v", err)
		}

		job := pendingDeferred(t, rec, rs, source, metav1.ConditionFalse, reasonBudgetExhausted)
		forceDelete(t, source)
		reconcile(t, rec, ns, job.Name)
		wantFailedPodNotFound(t, job, source)
		wantColdReplacement(t, rs, job, "pdb429-replacement")
	})

	t.Run("ForcedDelete_PendingDeferredByPolicy403", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-deny403")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "deny403")
		source := newSource(t, rs, "deny403-source")
		denyEvictions(t, ns, source)

		job := pendingDeferred(t, rec, rs, source, metav1.ConditionUnknown, reasonBudgetCheckError)
		forceDelete(t, source)
		reconcile(t, rec, ns, job.Name)
		wantFailedPodNotFound(t, job, source)
		wantColdReplacement(t, rs, job, "deny403-replacement")
	})

	t.Run("ForcedDelete_SnapshottingBeforeTheFence", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-snap-early")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "snapearly")
		source := newSource(t, rs, "snapearly-source")
		started := metav1.Now()
		job := newJob(t, rs, source, "snapearly-pmj", nil, func(s *pmv1alpha1.PodMigrationJobStatus) {
			s.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
			s.SnapshottingStartTime = &started
		})
		// The source is force-deleted after the job entered Snapshotting,
		// before the controller fenced it.
		forceDelete(t, source)
		for i := 0; i < 2; i++ {
			reconcile(t, rec, ns, job.Name)
		}
		wantFailedPodNotFound(t, job, source)
		wantColdReplacement(t, rs, job, "snapearly-replacement")
	})

	t.Run("ForcedDelete_SnapshottingMidCheckpoint", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-snap-mid")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "snapmid")
		job, source, _ := startCheckpoint(t, rec, rs, "snapmid", snapCond("Checkpoint", "False", "InProgress", time.Now()))

		// A drain that timed out deletes the source outright, bypassing the
		// eviction webhook that answers 429 while the job runs.
		forceDelete(t, source)
		for i := 0; i < 2; i++ {
			reconcile(t, rec, ns, job.Name)
		}
		wantFailedPodNotFound(t, job, source)
		wantColdReplacement(t, rs, job, "snapmid-replacement")
	})

	t.Run("ForcedDelete_SnapshottingAfterCheckpointCompleted", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-snap-upload")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "snapupload")
		// The checkpoint completed (Checkpoint=True, C9), and its archive is
		// still uploading.
		job, source, snapName := startCheckpoint(t, rec, rs, "snapupload",
			snapCond("Checkpoint", "True", "Dumped", time.Now()),
			snapCond("StorageReplicated", "False", "InProgress", time.Now()))

		// A source deleted now leaves a checkpoint that can still restore,
		// so the job waits for it and keeps its trigger.
		forceDelete(t, source)
		for i := 0; i < 2; i++ {
			reconcile(t, rec, ns, job.Name)
		}
		if got := getJob(t, ns, job.Name); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSnapshotting {
			t.Fatalf("a completed checkpoint was given up when its source was deleted: phase=%s reason=%q", got.Status.Phase, readyReason(got))
		}
		if !triggerExists(t, ns, source.Name, source.UID) {
			t.Fatalf("the trigger of a completed checkpoint was deleted while it uploads")
		}

		// The upload finishes: the job goes on, finds the source gone, and
		// restores the replacement.
		setSnapshotConditions(t, ns, snapName,
			snapCond("Checkpoint", "True", "Succeeded", time.Now()),
			snapCond("Ready", "True", "AllSnapshotsAvailable", time.Now()))
		for i := 0; i < 3; i++ {
			reconcile(t, rec, ns, job.Name)
		}
		got := getJob(t, ns, job.Name)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseRestoring || got.Status.SnapshotRef != snapName {
			t.Fatalf("after the upload: phase=%s snapshotRef=%q reason=%q, want Restoring/%s", got.Status.Phase, got.Status.SnapshotRef, readyReason(got), snapName)
		}
		if controllerutil.ContainsFinalizer(got, SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s with its source gone", SourceFenceFinalizer)
		}
	})

	// ---- 2. Timeout or denied final eviction -------------------------------

	t.Run("Timeout_SnapshottingKeptSourceEvictionDenied", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-snap-deny")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "snapdeny")
		// The engine froze the source, and its checkpoint call timed out:
		// the source may stay frozen, and the checkpoint never settles (C9).
		job, source, _ := startCheckpoint(t, rec, rs, "snapdeny",
			snapCond("SourceFrozen", "True", "Frozen", time.Now().Add(-5*migrationTimeout)),
			snapCond("Checkpoint", "Unknown", "CheckpointTimedOut", time.Now()))
		lift := denyEvictions(t, ns, source)

		// The deadline passes mid-checkpoint.
		got := getJob(t, ns, job.Name)
		past := metav1.NewTime(time.Now().Add(-2 * migrationTimeout))
		got.Status.SnapshottingStartTime = &past
		if err := k8sClient.Status().Update(ctx, got); err != nil {
			t.Fatalf("backdate SnapshottingStartTime: %v", err)
		}
		reconcile(t, rec, ns, job.Name)
		if got := getJob(t, ns, job.Name); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || readyReason(got) != "Timeout" {
			t.Fatalf("after the deadline: phase=%s reason=%q, want Failed/Timeout", got.Status.Phase, readyReason(got))
		}
		if triggerExists(t, ns, source.Name, source.UID) {
			t.Fatalf("after the deadline: the trigger must be deleted to cancel the checkpoint")
		}
		// Past the settle wait the fence is kept, and the cold eviction is
		// denied.
		got = getJob(t, ns, job.Name)
		concluded := metav1.NewTime(time.Now().Add(-fenceSettleTimeout - time.Second))
		got.Status.CompletionTime = &concluded
		if err := k8sClient.Status().Update(ctx, got); err != nil {
			t.Fatalf("backdate CompletionTime: %v", err)
		}
		for i := 0; i < 3; i++ {
			_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: job.Name}})
			if err == nil || !apierrors.IsForbidden(err) {
				t.Fatalf("denied cold eviction, pass %d: err=%v, want the 403 returned so the eviction is retried", i, err)
			}
			wantKeptFenced(t, job, source, "while the cold eviction is denied")
		}
		if !controllerutil.ContainsFinalizer(getJob(t, ns, job.Name), SourceFenceFinalizer) {
			t.Fatalf("the job dropped %s while its source is still fenced", SourceFenceFinalizer)
		}

		// The window opens: the next pass evicts the source.
		lift()
		res := reconcile(t, rec, ns, job.Name)
		if !evictedOrGone(t, source) {
			t.Fatalf("the kept source wasn't evicted once evictions were allowed (result %+v)", res)
		}
		// Once it's gone, the job settles and drops its finalizer.
		reconcile(t, rec, ns, job.Name)
		if controllerutil.ContainsFinalizer(getJob(t, ns, job.Name), SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s after its kept source was evicted", SourceFenceFinalizer)
		}
	})

	// evictingPastDeadline creates a fenced source whose checkpoint is
	// durable, and a job in Evicting past its migration deadline.
	evictingPastDeadline := func(t *testing.T, rs *appsv1.ReplicaSet, leg string, conds ...metav1.Condition) (*pmv1alpha1.PodMigrationJob, *corev1.Pod) {
		t.Helper()
		source := newSource(t, rs, leg+"-source")
		pod, _ := getPod(t, rs.Namespace, source.Name)
		pod.Labels[util.LabelFenced] = "true"
		if err := k8sClient.Update(ctx, pod); err != nil {
			t.Fatalf("label source fenced: %v", err)
		}
		pod.Status.Conditions = []corev1.PodCondition{{
			Type: corev1.PodReady, Status: corev1.ConditionFalse, Reason: util.ReasonPodMigrationFenced, LastTransitionTime: metav1.Now(),
		}}
		if err := k8sClient.Status().Update(ctx, pod); err != nil {
			t.Fatalf("hold source NotReady: %v", err)
		}
		snapStart := metav1.NewTime(time.Now().Add(-3 * migrationTimeout))
		evictStart := metav1.NewTime(time.Now().Add(-2 * migrationTimeout))
		job := newJob(t, rs, source, leg+"-pmj", map[string]string{
			util.AnnotationEvictingSince: evictStart.Format(time.RFC3339Nano),
			util.AnnotationFenceBaseline: `{"containers":{"app":{"id":"containerd://` + source.Name + `","restarts":0}}}`,
		}, func(s *pmv1alpha1.PodMigrationJobStatus) {
			s.Phase = pmv1alpha1.PodMigrationJobPhaseEvicting
			s.SnapshotRef = leg + "-snap"
			s.SnapshottingStartTime = &snapStart
			s.EvictingStartTime = &evictStart
			s.Conditions = append([]metav1.Condition{{
				Type: ConditionSourceFenced, Status: metav1.ConditionTrue, Reason: util.ReasonFenced,
				Message: "fenced", LastTransitionTime: snapStart,
			}}, conds...)
		})
		got := getJob(t, rs.Namespace, job.Name)
		controllerutil.AddFinalizer(got, SourceFenceFinalizer)
		if err := k8sClient.Update(ctx, got); err != nil {
			t.Fatalf("add finalizer: %v", err)
		}
		return got, source
	}

	t.Run("Timeout_EvictingFinalEvictionDeniedByPolicy403", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-evict-403")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "evict403")
		job, source := evictingPastDeadline(t, rs, "evict403")
		lift := denyEvictions(t, ns, source)

		// The disruption window closed after the checkpoint: the final
		// eviction is denied until the deadline passes.
		reconcile(t, rec, ns, job.Name)
		if got := getJob(t, ns, job.Name); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || readyReason(got) != "Timeout" {
			t.Fatalf("after the deadline: phase=%s reason=%q, want Failed/Timeout", got.Status.Phase, readyReason(got))
		}
		// The snapshot was durable, so the engine stopped the source: it
		// stays fenced, and its cold eviction is retried while it's denied.
		for i := 0; i < 3; i++ {
			_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: job.Name}})
			if err == nil || !apierrors.IsForbidden(err) {
				t.Fatalf("denied cold eviction, pass %d: err=%v, want the 403 returned so the eviction is retried", i, err)
			}
			wantKeptFenced(t, job, source, "while the final eviction is denied")
		}
		// After a restart, a controller with no memory of the job carries on.
		lift()
		reconcile(t, newJobReconciler(), ns, job.Name)
		if !evictedOrGone(t, source) {
			t.Fatalf("the kept source wasn't evicted once evictions were allowed")
		}
		reconcile(t, rec, ns, job.Name)
		if controllerutil.ContainsFinalizer(getJob(t, ns, job.Name), SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s after its kept source was evicted", SourceFenceFinalizer)
		}
		wantColdReplacement(t, rs, job, "evict403-replacement")
	})

	t.Run("Timeout_EvictingFinalEvictionRefusedByPDB429", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-evict-429")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "evict429")
		minAvail := intstr.FromInt(1)
		pdb := &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "evict429"},
			Spec: policyv1.PodDisruptionBudgetSpec{
				MinAvailable: &minAvail,
				Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"app": "evict429"}},
			},
		}
		if err := k8sClient.Create(ctx, pdb); err != nil {
			t.Fatalf("create PDB: %v", err)
		}
		pdb.Status = policyv1.PodDisruptionBudgetStatus{DisruptionsAllowed: 0, CurrentHealthy: 0, DesiredHealthy: 1, ExpectedPods: 1, ObservedGeneration: pdb.Generation}
		if err := k8sClient.Status().Update(ctx, pdb); err != nil {
			t.Fatalf("update PDB status: %v", err)
		}
		job, source := evictingPastDeadline(t, rs, "evict429", metav1.Condition{
			Type: "BlockedByPDB", Status: metav1.ConditionTrue, Reason: "PDBBudgetExhausted",
			Message: "Origin pod eviction delayed by PodDisruptionBudget; waiting for budget", LastTransitionTime: metav1.Now(),
		})

		reconcile(t, rec, ns, job.Name)
		got := getJob(t, ns, job.Name)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore || readyReason(got) != "PDBEvictionTimeout" {
			t.Fatalf("after the deadline: phase=%s reason=%q, want SucceededWithoutRestore/PDBEvictionTimeout", got.Status.Phase, readyReason(got))
		}
		for i := 0; i < 3; i++ {
			res := reconcile(t, rec, ns, job.Name)
			if res.RequeueAfter == 0 || res.RequeueAfter > keptSourceEvictionRetryInterval {
				t.Fatalf("refused cold eviction, pass %d: result %+v, want a retry within %v", i, res, keptSourceEvictionRetryInterval)
			}
			wantKeptFenced(t, job, source, "while the PDB refuses the eviction")
		}

		// The budget allows a disruption again.
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pdb), pdb); err != nil {
			t.Fatalf("get PDB: %v", err)
		}
		pdb.Status.DisruptionsAllowed = 1
		pdb.Status.CurrentHealthy = 2
		pdb.Status.ExpectedPods = 2
		if err := k8sClient.Status().Update(ctx, pdb); err != nil {
			t.Fatalf("update PDB status: %v", err)
		}
		reconcile(t, rec, ns, job.Name)
		if !evictedOrGone(t, source) {
			t.Fatalf("the kept source wasn't evicted once the PDB allowed it")
		}
		reconcile(t, rec, ns, job.Name)
		if controllerutil.ContainsFinalizer(getJob(t, ns, job.Name), SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s after its kept source was evicted", SourceFenceFinalizer)
		}
	})

	// ---- 3. Webhooks unreachable -------------------------------------------

	t.Run("WebhookDown_FenceWebhookFailsClosedAtTheFence", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-wh-fence")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "whfence")
		probe := newSource(t, rs, "whfence-probe")
		probe.Labels[util.LabelFenced] = "true"
		if err := k8sClient.Update(ctx, probe); err != nil {
			t.Fatalf("label probe: %v", err)
		}
		fenceWebhookDown(t, ns, probe)

		source := newSource(t, rs, "whfence-source")
		started := metav1.Now()
		job := newJob(t, rs, source, "whfence-pmj", nil, func(s *pmv1alpha1.PodMigrationJobStatus) {
			s.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
			s.SnapshottingStartTime = &started
		})
		// The fence's status write is rejected, so the fence never
		// completes and no trigger is created.
		for i := 0; i < 2; i++ {
			_, err := rec.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: job.Name}})
			if !isWebhookCallFailure(err) {
				t.Fatalf("fence pass %d: err=%v, want the rejected status write", i, err)
			}
		}
		if triggerExists(t, ns, source.Name, source.UID) {
			t.Fatalf("a trigger was created for a source whose fence didn't complete")
		}
		got := getJob(t, ns, job.Name)
		if _, recorded := jobFenceBaseline(got); recorded {
			t.Fatalf("the fence baseline was recorded although the fence didn't complete")
		}

		// Past the pre-checkpoint cap, the job fails and the source goes
		// back into service, with the webhook still down.
		c := fenceCond(got)
		if c == nil || c.Status != metav1.ConditionTrue {
			t.Fatalf("SourceFenced = %+v, want True while the fence is attempted", c)
		}
		meta.RemoveStatusCondition(&got.Status.Conditions, ConditionSourceFenced)
		c.LastTransitionTime = metav1.NewTime(time.Now().Add(-DefaultFencePreCheckpointCap - time.Second))
		got.Status.Conditions = append(got.Status.Conditions, *c)
		if err := k8sClient.Status().Update(ctx, got); err != nil {
			t.Fatalf("backdate SourceFenced: %v", err)
		}
		for i := 0; i < 3; i++ {
			reconcile(t, rec, ns, job.Name)
		}
		got = getJob(t, ns, job.Name)
		if got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || readyReason(got) != ReasonFenceCapExceeded {
			t.Fatalf("phase=%s reason=%q, want Failed/%s", got.Status.Phase, readyReason(got), ReasonFenceCapExceeded)
		}
		if c := fenceCond(got); c == nil || c.Status != metav1.ConditionFalse || c.Reason != util.ReasonUnfenced {
			t.Fatalf("SourceFenced = %+v, want False/%s", c, util.ReasonUnfenced)
		}
		pod, _ := getPod(t, ns, source.Name)
		if _, ok := pod.Labels[util.LabelFenced]; ok {
			t.Fatalf("the source is still labeled fenced with nothing left to unfence it: %v", pod.Labels)
		}
		if readyHeldByFence(pod) {
			t.Fatalf("the source is held NotReady by a fence that never completed: %v", pod.Status.Conditions)
		}
		if controllerutil.ContainsFinalizer(got, SourceFenceFinalizer) {
			t.Fatalf("the job still holds %s after unfencing", SourceFenceFinalizer)
		}
		if triggerExists(t, ns, source.Name, source.UID) {
			t.Fatalf("a trigger was created after the job failed")
		}
	})

	t.Run("WebhookDown_UnfenceAndColdEvictionWithBothWebhooksDown", func(t *testing.T) {
		ns := createTestNamespace(t, ctx, k8sClient, "t295-wh-both")
		rec := newJobReconciler()
		rs := newReplicaSet(t, ns, "whboth")

		// Leg A: a checkpoint that never completed, then a timeout.
		jobA, sourceA, snapA := startCheckpoint(t, rec, rs, "whboth-a", snapCond("Checkpoint", "False", "InProgress", time.Now().Add(-5*migrationTimeout)))
		// Leg B: a durable checkpoint, then a timed-out eviction.
		jobB, sourceB := evictingPastDeadline(t, rs, "whboth-b", metav1.Condition{
			Type: "BlockedByPDB", Status: metav1.ConditionFalse, Reason: "EvictionInitiated", Message: "-", LastTransitionTime: metav1.Now(),
		})

		// The controller goes down, and its webhooks with it.
		fenceWebhookDown(t, ns, sourceA)
		evictionWebhookDown(t, ns, sourceB)
		// A status write on the fenced source, as the kubelet's, is rejected:
		// the source stays out of its Services' endpoints.
		podA, _ := getPod(t, ns, sourceA.Name)
		podA.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		if err := k8sClient.Status().Update(ctx, podA); !isWebhookCallFailure(err) {
			t.Fatalf("a status write on a fenced source with the fence webhook down: err=%v, want it rejected", err)
		}

		// A new controller replica comes up, without its webhook serving yet.
		rec = newJobReconciler()

		// Leg A: the deadline passes, and the engine confirms it cancelled.
		got := getJob(t, ns, jobA.Name)
		past := metav1.NewTime(time.Now().Add(-2 * migrationTimeout))
		got.Status.SnapshottingStartTime = &past
		if err := k8sClient.Status().Update(ctx, got); err != nil {
			t.Fatalf("backdate SnapshottingStartTime: %v", err)
		}
		reconcile(t, rec, ns, jobA.Name)
		setSnapshotConditions(t, ns, snapA,
			snapCond("Checkpoint", "False", "Cancelled", time.Now()),
			snapCond("Ready", "False", "Cancelled", time.Now()))
		resA := reconcile(t, rec, ns, jobA.Name)
		got = getJob(t, ns, jobA.Name)
		if c := fenceCond(got); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || c == nil || c.Status != metav1.ConditionFalse || c.Reason != util.ReasonUnfenced {
			t.Fatalf("leg A: phase=%s SourceFenced=%+v, want Failed and False/%s", got.Status.Phase, c, util.ReasonUnfenced)
		}
		podA, _ = getPod(t, ns, sourceA.Name)
		if _, ok := podA.Labels[util.LabelFenced]; ok {
			t.Fatalf("leg A: the unfence couldn't remove the label with the fence webhook down: %v", podA.Labels)
		}
		// With the label gone, the kubelet's next status write sets Ready.
		podA.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}}
		if err := k8sClient.Status().Update(ctx, podA); err != nil {
			t.Fatalf("leg A: the unfenced source couldn't become Ready with the fence webhook down: %v", err)
		}
		// The late-stop watch still drives the job (C4, #106).
		if resA.RequeueAfter == 0 {
			t.Fatalf("leg A: result %+v, want the late-stop watch to requeue", resA)
		}

		// Leg B: the deadline has passed. The fence is kept, and the cold
		// eviction goes through the eviction webhook, which fails open.
		reconcile(t, rec, ns, jobB.Name)
		reconcile(t, rec, ns, jobB.Name)
		got = getJob(t, ns, jobB.Name)
		if c := fenceCond(got); got.Status.Phase != pmv1alpha1.PodMigrationJobPhaseFailed || c == nil || c.Status != metav1.ConditionTrue || c.Reason != util.ReasonFenceKept {
			t.Fatalf("leg B: phase=%s SourceFenced=%+v, want Failed and True/%s", got.Status.Phase, c, util.ReasonFenceKept)
		}
		if !evictedOrGone(t, sourceB) {
			t.Fatalf("leg B: the kept source wasn't evicted with the eviction webhook down")
		}
		reconcile(t, rec, ns, jobB.Name)
		if controllerutil.ContainsFinalizer(getJob(t, ns, jobB.Name), SourceFenceFinalizer) {
			t.Fatalf("leg B: the job still holds %s after its kept source was evicted", SourceFenceFinalizer)
		}
	})
}
