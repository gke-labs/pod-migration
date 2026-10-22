package snapshot

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// The replacement in these tests reuses the bypass tests' pod name and UID.

// TestCompleteRestore_SetsColdStartBypass: the replacement's ps-name is
// replaced by the empty cold-start bypass, through a merge patch that carries
// its UID, and its other annotations stay.
func TestCompleteRestore_SetsColdStartBypass(t *testing.T) {
	var patchBody []byte
	c := fake.NewClientBuilder().
		WithScheme(setupTestScheme()).
		WithObjects(bypassTestPod(map[string]string{psNameAnnotation: "migration-snapshot", "keep": "me"})).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				data, err := patch.Data(obj)
				if err != nil {
					return err
				}
				patchBody = data
				return cl.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()

	if err := NewGKEProvider(c, setupTestScheme()).CompleteRestore(context.Background(), bypassTestJob(), bypassTestPodName, bypassTestPodUID); err != nil {
		t.Fatalf("CompleteRestore: %v", err)
	}
	var body struct {
		Metadata struct {
			UID string `json:"uid"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(patchBody, &body); err != nil || body.Metadata.UID != bypassTestPodUID {
		t.Errorf("patch %s: uid precondition = %q (err %v), want %q", patchBody, body.Metadata.UID, err, bypassTestPodUID)
	}
	got := sourceAnnotations(t, c)
	assertColdStartBypass(t, got)
	if got[util.AnnotationRestoreCompleted] != "migration-snapshot" {
		t.Errorf("restore-completed annotation = %q, want %q", got[util.AnnotationRestoreCompleted], "migration-snapshot")
	}
	if got["keep"] != "me" {
		t.Errorf("other annotations were not preserved: %v", got)
	}

	// Idempotent.
	if err := NewGKEProvider(c, setupTestScheme()).CompleteRestore(context.Background(), bypassTestJob(), bypassTestPodName, bypassTestPodUID); err != nil {
		t.Fatalf("second CompleteRestore: %v", err)
	}
	gotSecond := sourceAnnotations(t, c)
	assertColdStartBypass(t, gotSecond)
	if gotSecond[util.AnnotationRestoreCompleted] != "migration-snapshot" {
		t.Errorf("restore-completed annotation after idempotent retry = %q, want %q", gotSecond[util.AnnotationRestoreCompleted], "migration-snapshot")
	}
}

// TestCompleteRestore_MismatchedOrMissingSnapshotRefLeavesPodUntouched checks that
// attempting to complete a restore when the pod carries a different snapshot name,
// empty ps-name (without marker), or missing ps-name returns nil and writes nothing,
// allowing I2 to catch a cold-released pod (#152).
func TestCompleteRestore_MismatchedOrMissingSnapshotRefLeavesPodUntouched(t *testing.T) {
	cases := map[string]struct {
		annotations map[string]string
		snapRef     string
	}{
		"mismatched snapshot name": {
			annotations: map[string]string{psNameAnnotation: "other-snapshot"},
			snapRef:     "migration-snapshot",
		},
		"empty ps-name without marker": {
			annotations: map[string]string{psNameAnnotation: ""},
			snapRef:     "migration-snapshot",
		},
		"missing ps-name annotation": {
			annotations: map[string]string{"keep": "me"},
			snapRef:     "migration-snapshot",
		},
		"empty job SnapshotRef": {
			annotations: map[string]string{psNameAnnotation: "migration-snapshot"},
			snapRef:     "",
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			pod := bypassTestPod(tc.annotations)
			job := bypassTestJob()
			job.Status.SnapshotRef = tc.snapRef
			c := fake.NewClientBuilder().
				WithScheme(setupTestScheme()).
				WithObjects(pod).
				Build()
			if err := NewGKEProvider(c, setupTestScheme()).CompleteRestore(context.Background(), job, bypassTestPodName, bypassTestPodUID); err != nil {
				t.Fatalf("CompleteRestore = %v, want nil", err)
			}
			got := sourceAnnotations(t, c)
			if got[util.AnnotationRestoreCompleted] != "" {
				t.Errorf("restore-completed marker was stamped unexpectedly: %q", got[util.AnnotationRestoreCompleted])
			}
			if got[psNameAnnotation] != tc.annotations[psNameAnnotation] {
				t.Errorf("%s = %q, want untouched %q", psNameAnnotation, got[psNameAnnotation], tc.annotations[psNameAnnotation])
			}
		})
	}
}

// TestCompleteRestore_NothingLeftToDo: a replacement that's gone, or whose
// name now belongs to another pod, isn't an error, and the other pod isn't
// touched.
func TestCompleteRestore_NothingLeftToDo(t *testing.T) {
	rejectUID := interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, obj.GetName(), nil)
		},
	}
	successor := bypassTestPod(map[string]string{psNameAnnotation: "its-own-snapshot"})
	successor.UID = "successor-uid"

	for name, tc := range map[string]struct {
		objs  []client.Object
		funcs interceptor.Funcs
	}{
		"gone":                         {nil, interceptor.Funcs{}},
		"replaced under the same name": {[]client.Object{successor}, rejectUID},
	} {
		t.Run(name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(setupTestScheme()).WithObjects(tc.objs...).WithInterceptorFuncs(tc.funcs).Build()
			if err := NewGKEProvider(c, setupTestScheme()).CompleteRestore(context.Background(), bypassTestJob(), bypassTestPodName, bypassTestPodUID); err != nil {
				t.Fatalf("CompleteRestore = %v, want nil", err)
			}
			if len(tc.objs) > 0 {
				if v := sourceAnnotations(t, c)[psNameAnnotation]; v != "its-own-snapshot" {
					t.Errorf("the successor's %s changed to %q", psNameAnnotation, v)
				}
			}
		})
	}
}

// TestCompleteRestore_ReturnsOtherErrors: a failed patch on the replacement
// itself is returned, so the controller tries again.
func TestCompleteRestore_ReturnsOtherErrors(t *testing.T) {
	c := fake.NewClientBuilder().
		WithScheme(setupTestScheme()).
		WithObjects(bypassTestPod(map[string]string{psNameAnnotation: "migration-snapshot"})).
		WithInterceptorFuncs(interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				return apierrors.NewServiceUnavailable("apiserver busy")
			},
		}).
		Build()
	if err := NewGKEProvider(c, setupTestScheme()).CompleteRestore(context.Background(), bypassTestJob(), bypassTestPodName, bypassTestPodUID); err == nil {
		t.Fatal("CompleteRestore = nil, want the patch error")
	}
}

// TestCompleteRestore_Envtest checks the patch against a real API server: it
// sets the bypass on the replacement, and leaves a pod that replaced it under
// the same name alone without failing.
func TestCompleteRestore_Envtest(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping envtest restore completion test")
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
	const ns = "restore-test"
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatalf("creating namespace: %v", err)
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "replacement", Namespace: ns, Annotations: map[string]string{psNameAnnotation: "migration-snapshot"}},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10"}}},
	}
	if err := c.Create(ctx, pod); err != nil {
		t.Fatalf("creating pod: %v", err)
	}
	job := bypassTestJob()
	job.Namespace = ns
	provider := NewGKEProvider(c, scheme)
	read := func() string {
		t.Helper()
		got := &corev1.Pod{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "replacement"}, got); err != nil {
			t.Fatalf("getting pod: %v", err)
		}
		return got.Annotations[psNameAnnotation]
	}

	// A stale UID: the name belongs to another pod. Nothing to do, and no error.
	if err := provider.CompleteRestore(ctx, job, "replacement", "00000000-0000-0000-0000-000000000000"); err != nil {
		t.Fatalf("CompleteRestore with a stale UID = %v, want nil", err)
	}
	if v := read(); v != "migration-snapshot" {
		t.Errorf("annotation changed through a stale UID: %q", v)
	}

	if err := provider.CompleteRestore(ctx, job, "replacement", pod.UID); err != nil {
		t.Fatalf("CompleteRestore: %v", err)
	}
	if v := read(); v != "" {
		t.Errorf("%s = %q after CompleteRestore, want the empty bypass", psNameAnnotation, v)
	}
	gotPod := &corev1.Pod{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "replacement"}, gotPod); err != nil {
		t.Fatalf("getting pod: %v", err)
	}
	if v := gotPod.Annotations[util.AnnotationRestoreCompleted]; v != "migration-snapshot" {
		t.Errorf("%s = %q after CompleteRestore, want %q", util.AnnotationRestoreCompleted, v, "migration-snapshot")
	}
}

// The GKE provider must keep implementing the capability the controller looks for.
var _ RestoreCompleter = (*GKEProvider)(nil)
