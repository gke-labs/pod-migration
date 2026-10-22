package controller

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// TestFenceSource_Envtest runs the fence against a real API server, as a user
// bound to exactly the controller's generated ClusterRole: the fence writes
// are allowed (pods/status needs its own rule), a pod replaced under the same
// name is never fenced, and the unfence works.
func TestFenceSource_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest fence test")
	}
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		t.Fatalf("starting envtest: %v", err)
	}
	t.Cleanup(func() {
		if err := testEnv.Stop(); err != nil {
			t.Errorf("stopping envtest: %v", err)
		}
	})
	scheme := fenceTestScheme()
	_ = rbacv1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating admin client: %v", err)
	}
	ctx := context.Background()

	role := controllerClusterRole(t)
	controllerClient := clientForRole(ctx, t, testEnv, admin, scheme, "controller", role)
	withoutStatus := role.DeepCopy()
	withoutStatus.Name = "controller-without-pods-status"
	withoutStatus.Rules = nil
	for _, rule := range role.Rules {
		var resources []string
		for _, res := range rule.Resources {
			if res != "pods/status" {
				resources = append(resources, res)
			}
		}
		if len(resources) > 0 {
			rule.Resources = resources
			withoutStatus.Rules = append(withoutStatus.Rules, rule)
		}
	}
	noStatusClient := clientForRole(ctx, t, testEnv, admin, scheme, "no-status", withoutStatus)

	const ns = "fence-test"
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	newSource := func(name string) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
			Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}},
		}
		if err := admin.Create(ctx, pod); err != nil {
			t.Fatalf("creating pod: %v", err)
		}
		pod.Status = corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name: "app", Image: "registry.k8s.io/pause:3.10", ImageID: "pause", ContainerID: fenceTestCID, Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.Now()}},
			}},
		}
		if err := admin.Status().Update(ctx, pod); err != nil {
			t.Fatalf("setting pod status: %v", err)
		}
		return pod
	}
	getPod := func(name string) *corev1.Pod {
		t.Helper()
		pod := &corev1.Pod{}
		if err := admin.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, pod); err != nil {
			t.Fatalf("getting pod: %v", err)
		}
		return pod
	}

	source := newSource("source")
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "pmj-source", Namespace: ns},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "source"},
			TargetPodUID: string(source.UID),
		},
	}
	if err := admin.Create(ctx, job); err != nil {
		t.Fatalf("creating job: %v", err)
	}
	job.Status.Phase = pmv1alpha1.PodMigrationJobPhaseSnapshotting
	if err := admin.Status().Update(ctx, job); err != nil {
		t.Fatalf("setting job status: %v", err)
	}

	r := &PodMigrationJobReconciler{Client: controllerClient, APIReader: controllerClient, Scheme: scheme}
	if err := r.fenceSource(ctx, job, job.DeepCopy()); err != nil {
		t.Fatalf("fenceSource as the controller's role: %v", err)
	}
	assertFenced(t, getPod("source"))
	stored := &pmv1alpha1.PodMigrationJob{}
	if err := admin.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil {
		t.Fatalf("getting job: %v", err)
	}
	if !meta.IsStatusConditionTrue(stored.Status.Conditions, ConditionSourceFenced) {
		t.Errorf("job conditions = %+v, want SourceFenced=True", stored.Status.Conditions)
	}
	if baseline, ok := jobFenceBaseline(stored); !ok || baseline.Containers["app"].ID != fenceTestCID {
		t.Errorf("job baseline = %+v (recorded %t), want app=%s", baseline, ok, fenceTestCID)
	}

	// The unfence removes the label under the same role.
	if err := r.removeFenceLabel(ctx, getPod("source")); err != nil {
		t.Fatalf("removeFenceLabel: %v", err)
	}
	if labels := getPod("source").Labels; labels[util.LabelFenced] != "" {
		t.Errorf("labels after unfence = %v", labels)
	}

	// A pod replaced under the same name has another UID and is never fenced.
	other := newSource("other")
	stale := other.DeepCopy()
	stale.UID = "00000000-0000-0000-0000-000000000000"
	if err := r.applyFence(ctx, stale); err == nil {
		t.Error("applyFence fenced a pod through a stale UID")
	}
	if labels := getPod("other").Labels; labels[util.LabelFenced] != "" {
		t.Errorf("replaced pod was labeled: %v", labels)
	}

	// Without the pods/status rule the source can't be held NotReady, which
	// is why the controller's role has it.
	noStatus := &PodMigrationJobReconciler{Client: noStatusClient, APIReader: noStatusClient, Scheme: scheme}
	if err := noStatus.applyFence(ctx, getPod("other")); !apierrors.IsForbidden(err) {
		t.Errorf("applyFence without pods/status = %v, want Forbidden", err)
	}
}

// controllerClusterRole reads the ClusterRole that make manifests generates
// from the kubebuilder markers.
func controllerClusterRole(t *testing.T) *rbacv1.ClusterRole {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "config", "rbac", "role.yaml"))
	if err != nil {
		t.Fatalf("reading the generated role: %v", err)
	}
	role := &rbacv1.ClusterRole{}
	if err := yaml.NewYAMLOrJSONDecoder(bytes.NewReader(data), 4096).Decode(role); err != nil {
		t.Fatalf("decoding the generated role: %v", err)
	}
	return role
}

// clientForRole returns a client for a new user bound to role.
func clientForRole(ctx context.Context, t *testing.T, testEnv *envtest.Environment, admin client.Client, scheme *runtime.Scheme, user string, role *rbacv1.ClusterRole) client.Client {
	t.Helper()
	role = role.DeepCopy()
	role.ResourceVersion = ""
	if err := admin.Create(ctx, role); err != nil {
		t.Fatalf("creating ClusterRole %s: %v", role.Name, err)
	}
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: user},
		RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: role.Name},
		Subjects:   []rbacv1.Subject{{APIGroup: rbacv1.GroupName, Kind: rbacv1.UserKind, Name: user}},
	}
	if err := admin.Create(ctx, binding); err != nil {
		t.Fatalf("creating ClusterRoleBinding %s: %v", user, err)
	}
	authenticated, err := testEnv.AddUser(envtest.User{Name: user}, nil)
	if err != nil {
		t.Fatalf("adding user %s: %v", user, err)
	}
	c, err := client.New(authenticated.Config(), client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating client for %s: %v", user, err)
	}
	return c
}
