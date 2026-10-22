package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

const (
	bypassTestPodName = "test-pod"
	bypassTestPodUID  = "pod-uid-456"
)

func bypassTestJob() *pmv1alpha1.PodMigrationJob {
	return &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-job",
			Namespace: "default",
			UID:       types.UID("job-uid-123"),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: bypassTestPodName},
			TargetPodUID: bypassTestPodUID,
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			SnapshotRef: "migration-snapshot",
		},
	}
}

func bypassTestPod(annotations map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        bypassTestPodName,
			Namespace:   "default",
			UID:         types.UID(bypassTestPodUID),
			Annotations: annotations,
		},
	}
}

func triggerExists(t *testing.T, c client.Client) bool {
	t.Helper()
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "podsnapshot.gke.io",
		Version: "v1",
		Kind:    "PodSnapshotManualTrigger",
	})
	key := types.NamespacedName{Namespace: "default", Name: util.FormatPSMTName(bypassTestPodName, bypassTestPodUID)}
	err := c.Get(context.Background(), key, trigger)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("getting trigger: %v", err)
	}
	return err == nil
}

func sourceAnnotations(t *testing.T, c client.Client) map[string]string {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: "default", Name: bypassTestPodName}, pod); err != nil {
		t.Fatalf("getting source pod: %v", err)
	}
	return pod.Annotations
}

func assertColdStartBypass(t *testing.T, annotations map[string]string) {
	t.Helper()
	v, ok := annotations[psNameAnnotation]
	if !ok || v != "" {
		t.Fatalf("source %s = %q (present: %t), want an empty value", psNameAnnotation, v, ok)
	}
}

func TestEnsureTrigger_SetsColdStartBypassBeforeCreatingTrigger(t *testing.T) {
	scheme := setupTestScheme()
	var ops []string
	var patchBody []byte
	var patchType types.PatchType
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(bypassTestPod(map[string]string{"keep": "me"})).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if _, ok := obj.(*corev1.Pod); ok {
					ops = append(ops, "patch source")
					patchType = patch.Type()
					data, err := patch.Data(obj)
					if err != nil {
						return err
					}
					patchBody = data
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
			Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
				if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == "PodSnapshotManualTrigger" {
					ops = append(ops, "create trigger")
				}
				return cl.Create(ctx, obj, opts...)
			},
		}).
		Build()

	if _, err := NewGKEProvider(c, scheme).EnsureTrigger(context.Background(), bypassTestJob(), bypassTestPodName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}

	if len(ops) != 2 || ops[0] != "patch source" || ops[1] != "create trigger" {
		t.Fatalf("operations = %v, want [patch source, create trigger]", ops)
	}
	if patchType != types.MergePatchType {
		t.Errorf("patch type = %q, want %q", patchType, types.MergePatchType)
	}
	var body struct {
		Metadata struct {
			UID         string            `json:"uid"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patchBody, &body); err != nil {
		t.Fatalf("decoding patch %s: %v", patchBody, err)
	}
	if body.Metadata.UID != bypassTestPodUID {
		t.Errorf("patch uid precondition = %q, want %q", body.Metadata.UID, bypassTestPodUID)
	}
	if v, ok := body.Metadata.Annotations[psNameAnnotation]; !ok || v != "" {
		t.Errorf("patch sets %s = %q (present: %t), want an empty value", psNameAnnotation, v, ok)
	}

	got := sourceAnnotations(t, c)
	assertColdStartBypass(t, got)
	if got["keep"] != "me" {
		t.Errorf("other annotations were not preserved: %v", got)
	}
}

func TestEnsureTrigger_ColdStartBypassReplacesStaleSnapshotName(t *testing.T) {
	scheme := setupTestScheme()
	// A source that an earlier migration restored still names that snapshot.
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(bypassTestPod(map[string]string{psNameAnnotation: "older-snapshot"})).
		Build()

	if _, err := NewGKEProvider(c, scheme).EnsureTrigger(context.Background(), bypassTestJob(), bypassTestPodName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}
	assertColdStartBypass(t, sourceAnnotations(t, c))
	if !triggerExists(t, c) {
		t.Fatal("expected the trigger to be created")
	}
}

func TestEnsureTrigger_ColdStartBypassIsIdempotent(t *testing.T) {
	scheme := setupTestScheme()
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(bypassTestPod(map[string]string{psNameAnnotation: "", "keep": "me"})).
		Build()
	provider := NewGKEProvider(c, scheme)
	job := bypassTestJob()

	if _, err := provider.EnsureTrigger(context.Background(), job, bypassTestPodName); err != nil {
		t.Fatalf("first EnsureTrigger: %v", err)
	}
	// Recreating the trigger (for example after a stale one was removed) patches again.
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotManualTrigger"})
	trigger.SetNamespace("default")
	trigger.SetName(util.FormatPSMTName(bypassTestPodName, bypassTestPodUID))
	if err := c.Delete(context.Background(), trigger); err != nil {
		t.Fatalf("deleting trigger: %v", err)
	}
	if _, err := provider.EnsureTrigger(context.Background(), job, bypassTestPodName); err != nil {
		t.Fatalf("second EnsureTrigger: %v", err)
	}

	got := sourceAnnotations(t, c)
	assertColdStartBypass(t, got)
	if got["keep"] != "me" {
		t.Errorf("other annotations were not preserved: %v", got)
	}
	if !triggerExists(t, c) {
		t.Fatal("expected the trigger to be recreated")
	}
}

func TestEnsureTrigger_NoTriggerWhenColdStartBypassFails(t *testing.T) {
	scheme := setupTestScheme()
	for name, patchErr := range map[string]error{
		// The API server's answer when the pod was replaced under the same name.
		"uid precondition fails": apierrors.NewConflict(schema.GroupResource{Resource: "pods"}, bypassTestPodName, errors.New("Precondition failed: UID in precondition")),
		"transient error":        errors.New("connection reset by peer"),
	} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().
				WithScheme(scheme).
				WithObjects(bypassTestPod(nil)).
				WithInterceptorFuncs(interceptor.Funcs{
					Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
						if _, ok := obj.(*corev1.Pod); ok {
							return patchErr
						}
						return cl.Patch(ctx, obj, patch, opts...)
					},
				}).
				Build()

			if _, err := NewGKEProvider(c, scheme).EnsureTrigger(context.Background(), bypassTestJob(), bypassTestPodName); err == nil {
				t.Fatal("expected an error so the job requeues")
			}
			if triggerExists(t, c) {
				t.Fatal("trigger was created without the cold-start bypass")
			}
		})
	}
}

func TestEnsureTrigger_SourceGoneStillCreatesTrigger(t *testing.T) {
	scheme := setupTestScheme()
	c := fake.NewClientBuilder().WithScheme(scheme).Build()

	if _, err := NewGKEProvider(c, scheme).EnsureTrigger(context.Background(), bypassTestJob(), bypassTestPodName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}
	if !triggerExists(t, c) {
		t.Fatal("expected the trigger to be created as before when the source is gone")
	}
}

func TestEnsureTrigger_ExistingTriggerSkipsBypass(t *testing.T) {
	scheme := setupTestScheme()
	job := bypassTestJob()
	trigger := &unstructured.Unstructured{}
	trigger.SetGroupVersionKind(schema.GroupVersionKind{Group: "podsnapshot.gke.io", Version: "v1", Kind: "PodSnapshotManualTrigger"})
	trigger.SetNamespace("default")
	trigger.SetName(util.FormatPSMTName(bypassTestPodName, bypassTestPodUID))
	trigger.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "podmigration.gke.io/v1alpha1", Kind: "PodMigrationJob", Name: job.Name, UID: job.UID}})
	patched := false
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(bypassTestPod(nil), trigger).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				patched = true
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	if _, err := NewGKEProvider(c, scheme).EnsureTrigger(context.Background(), job, bypassTestPodName); err != nil {
		t.Fatalf("EnsureTrigger: %v", err)
	}
	if patched {
		t.Error("the bypass is set when the trigger is created, not on every poll")
	}
}

// TestEnsureSourceColdStartBypass_Envtest checks the patch against a real API
// server: it sets the bypass, keeps other annotations, and is refused for a pod
// that was replaced under the same name.
func TestEnsureSourceColdStartBypass_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest cold-start bypass test")
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
	scheme := setupTestScheme()
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}
	ctx := context.Background()
	const ns = "bypass-test"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "source",
			Namespace:   ns,
			Annotations: map[string]string{psNameAnnotation: "older-snapshot", "keep": "me"},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	provider := NewGKEProvider(c, scheme)
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: ns},
		Spec:       pmv1alpha1.PodMigrationJobSpec{TargetPodUID: string(pod.UID)},
	}
	readAnnotations := func() map[string]string {
		got := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "source"}, got); err != nil {
			t.Fatalf("getting pod: %v", err)
		}
		return got.Annotations
	}

	gone, err := provider.ensureSourceColdStartBypass(ctx, job, "source")
	if err != nil || gone {
		t.Fatalf("ensureSourceColdStartBypass = (gone %t, %v), want (false, nil)", gone, err)
	}
	got := readAnnotations()
	assertColdStartBypass(t, got)
	if got["keep"] != "me" {
		t.Errorf("other annotations were not preserved: %v", got)
	}

	// Idempotent.
	if _, err := provider.ensureSourceColdStartBypass(ctx, job, "source"); err != nil {
		t.Fatalf("second ensureSourceColdStartBypass: %v", err)
	}
	assertColdStartBypass(t, readAnnotations())

	// A pod replaced under the same name has another UID and must not be patched.
	replaced := job.DeepCopy()
	replaced.Spec.TargetPodUID = "00000000-0000-0000-0000-000000000000"
	if err := c.Patch(ctx, pod, client.RawPatch(types.MergePatchType, []byte(`{"metadata":{"annotations":{"podsnapshot.gke.io/ps-name":"older-snapshot"}}}`))); err != nil {
		t.Fatalf("resetting annotation: %v", err)
	}
	if _, err := provider.ensureSourceColdStartBypass(ctx, replaced, "source"); err == nil {
		t.Fatal("expected the UID precondition to refuse the patch")
	}
	if v := readAnnotations()[psNameAnnotation]; v != "older-snapshot" {
		t.Errorf("annotation changed despite the UID mismatch: %q", v)
	}

	// A source that no longer exists is reported, not treated as an error.
	gone, err = provider.ensureSourceColdStartBypass(ctx, job, "missing")
	if err != nil || !gone {
		t.Fatalf("missing pod: ensureSourceColdStartBypass = (gone %t, %v), want (true, nil)", gone, err)
	}
}
