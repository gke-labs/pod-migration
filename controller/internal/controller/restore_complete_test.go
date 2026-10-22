package controller

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

const (
	completeTestNS   = "default"
	completeTestPod  = "web-0"
	completeTestUID  = "replacement-uid"
	completeTestJob  = "pmj-web-0"
	completeTestSnap = "snap-1"
)

// completingProvider is a snapshot provider with the RestoreCompleter
// capability. onComplete runs inside CompleteRestore.
type completingProvider struct {
	fakeSnapshotProvider
	calls      []string
	err        error
	onComplete func()
}

func (p *completingProvider) CompleteRestore(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string, podUID types.UID) error {
	p.calls = append(p.calls, podName+"/"+string(podUID))
	if p.onComplete != nil {
		p.onComplete()
	}
	return p.err
}

// completeTestFixture returns a client with a Restoring job whose replacement
// is Ready, with no fallback event, so the next reconcile verifies the restore.
func completeTestFixture(t *testing.T) (client.Client, *runtime.Scheme) {
	t.Helper()
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: completeTestNS, Name: completeTestPod, UID: types.UID(completeTestUID),
			Annotations: map[string]string{"podsnapshot.gke.io/ps-name": completeTestSnap},
		},
		Status: corev1.PodStatus{Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	now := metav1.Now()
	job := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{Namespace: completeTestNS, Name: completeTestJob, CreationTimestamp: now},
		Spec:       pmv1alpha1.PodMigrationJobSpec{PodRef: corev1.LocalObjectReference{Name: completeTestPod}},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:              pmv1alpha1.PodMigrationJobPhaseRestoring,
			RestoringStartTime: &now,
			SnapshotRef:        completeTestSnap,
			Consumed:           true,
			GateReleased:       true,
			RestoredPodName:    completeTestPod,
			RestoredPodUID:     completeTestUID,
		},
	}
	c := newFakeClientBuilderWithEventIndex(scheme).WithObjects(pod, job).WithStatusSubresource(&pmv1alpha1.PodMigrationJob{}).Build()
	return c, scheme
}

func completeTestReconcile(t *testing.T, r *PodMigrationJobReconciler) error {
	t.Helper()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: completeTestNS, Name: completeTestJob}})
	return err
}

func completeTestPhase(t *testing.T, c client.Client) pmv1alpha1.PodMigrationJobPhase {
	t.Helper()
	job := &pmv1alpha1.PodMigrationJob{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: completeTestNS, Name: completeTestJob}, job); err != nil {
		t.Fatalf("getting job: %v", err)
	}
	return job.Status.Phase
}

func completeTestPSName(t *testing.T, c client.Client) (string, bool) {
	t.Helper()
	pod := &corev1.Pod{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: completeTestNS, Name: completeTestPod}, pod); err != nil {
		t.Fatalf("getting pod: %v", err)
	}
	v, ok := pod.Annotations["podsnapshot.gke.io/ps-name"]
	return v, ok
}

// TestRestoreVerified_CompletesTheRestoreFirst: the engine completes the
// restore on the replacement it restored, before the job records Succeeded
// (#152).
func TestRestoreVerified_CompletesTheRestoreFirst(t *testing.T) {
	c, scheme := completeTestFixture(t)
	provider := &completingProvider{}
	provider.onComplete = func() {
		if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
			t.Errorf("phase when the restore was completed = %s, want Restoring", phase)
		}
	}
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, SnapshotProvider: provider}
	if err := completeTestReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if len(provider.calls) != 1 || provider.calls[0] != completeTestPod+"/"+completeTestUID {
		t.Errorf("CompleteRestore calls = %v, want [%s/%s]", provider.calls, completeTestPod, completeTestUID)
	}
	if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("phase = %s, want Succeeded", phase)
	}
}

// TestRestoreVerified_CompleteRestoreFailureRetries: until the engine
// completes the restore, the job stays Restoring and the reconcile is retried.
func TestRestoreVerified_CompleteRestoreFailureRetries(t *testing.T) {
	c, scheme := completeTestFixture(t)
	provider := &completingProvider{err: errors.New("apiserver busy")}
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, SnapshotProvider: provider}
	if err := completeTestReconcile(t, r); err == nil {
		t.Fatal("Reconcile = nil, want the CompleteRestore error")
	}
	if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseRestoring {
		t.Errorf("phase = %s, want Restoring until the restore is completed", phase)
	}

	provider.err = nil
	if err := completeTestReconcile(t, r); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("phase = %s, want Succeeded", phase)
	}
}

// TestRestoreVerified_GKEProviderSetsTheBypass: with the default GKE provider,
// the replacement ends up with the cold-start bypass instead of the
// migration snapshot's name.
func TestRestoreVerified_GKEProviderSetsTheBypass(t *testing.T) {
	c, scheme := completeTestFixture(t)
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme}
	if err := completeTestReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if v, ok := completeTestPSName(t, c); !ok || v != "" {
		t.Errorf("replacement ps-name = %q (present %t), want the empty bypass", v, ok)
	}
	if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("phase = %s, want Succeeded", phase)
	}
}

// TestRestoreVerified_ProviderWithoutTheCapability: a provider that can't
// complete the restore leaves the replacement as it is.
func TestRestoreVerified_ProviderWithoutTheCapability(t *testing.T) {
	c, scheme := completeTestFixture(t)
	r := &PodMigrationJobReconciler{Client: c, Scheme: scheme, SnapshotProvider: &fakeSnapshotProvider{checkStatusResult: &snapshot.Status{Phase: snapshot.PhaseReady}}}
	if err := completeTestReconcile(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if v, _ := completeTestPSName(t, c); v != completeTestSnap {
		t.Errorf("replacement ps-name = %q, want it untouched (%q)", v, completeTestSnap)
	}
	if phase := completeTestPhase(t, c); phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("phase = %s, want Succeeded", phase)
	}
}
