package controller

import (
	"context"
	"os"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus/testutil"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
)

type testSnapshotProvider struct {
	checkStatus *snapshot.Status
}

func (p *testSnapshotProvider) EnsureTrigger(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (ctrl.Result, error) {
	return ctrl.Result{}, nil
}

func (p *testSnapshotProvider) CheckStatus(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) (*snapshot.Status, error) {
	if p.checkStatus != nil {
		return p.checkStatus, nil
	}
	return &snapshot.Status{Phase: snapshot.PhaseReady, SnapshotRef: "snapshot-123"}, nil
}

func (p *testSnapshotProvider) Cleanup(ctx context.Context, job *pmv1alpha1.PodMigrationJob, podName string) error {
	return nil
}

func drainEvents(ch <-chan string) []string {
	var events []string
	for {
		select {
		case ev := <-ch:
			events = append(events, ev)
		default:
			return events
		}
	}
}

func containsEvent(events []string, eventType, reason string) bool {
	for _, ev := range events {
		if strings.Contains(ev, eventType) && strings.Contains(ev, reason) {
			return true
		}
	}
	return false
}

func TestEvents_SourcePod_MigrationLifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-111"),
		},
	}

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "test-pmj",
			Namespace:         "default",
			CreationTimestamp: metav1.Now(),
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "source-pod"},
			TargetPodUID: "pod-uid-111",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(pmj).
		Build()

	mockProvider := &testSnapshotProvider{
		checkStatus: &snapshot.Status{
			Phase:       snapshot.PhaseReady,
			SnapshotRef: "snapshot-test-1",
		},
	}

	fakeRecorder := record.NewFakeRecorder(50)
	r := &PodMigrationJobReconciler{
		Client:           fakeClient,
		Scheme:           scheme,
		SnapshotProvider: mockProvider,
		Recorder:         fakeRecorder,
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-pmj"}}

	// 1. Reconcile Pending -> Snapshotting
	res, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile pending failed: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue true when advancing from Pending")
	}

	events := drainEvents(fakeRecorder.Events)
	if !containsEvent(events, "Normal", "MigrationStarted") {
		t.Errorf("Expected MigrationStarted event, got events: %v", events)
	}

	// 2. Reconcile Snapshotting -> Evicting (mock provider reports Ready)
	res, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile snapshotting failed: %v", err)
	}
	if !res.Requeue {
		t.Errorf("Expected Requeue true when advancing from Snapshotting")
	}

	events = drainEvents(fakeRecorder.Events)
	if !containsEvent(events, "Normal", "CheckpointReady") {
		t.Errorf("Expected CheckpointReady event, got events: %v", events)
	}

	// 3. Reconcile Evicting -> records evicting-since and emits EvictedForMigration
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile evicting failed: %v", err)
	}

	events = drainEvents(fakeRecorder.Events)
	if !containsEvent(events, "Normal", "EvictedForMigration") {
		t.Errorf("Expected EvictedForMigration event, got events: %v", events)
	}
}

func TestEvents_SourcePod_MigrationFailed(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-failed-pmj",
			Namespace: "default",
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "missing-source-pod"},
			TargetPodUID: "pod-uid-missing",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj).
		WithStatusSubresource(pmj).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	r := &PodMigrationJobReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "test-failed-pmj"}}

	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile failed: %v", err)
	}

	events := drainEvents(fakeRecorder.Events)
	if !containsEvent(events, "Warning", "MigrationFailed") {
		t.Errorf("Expected MigrationFailed event on origin pod not found, got events: %v", events)
	}
}

func TestEvents_ReplacementPod_MigrationRestoreReleased(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-restore-pmj",
			Namespace: "default",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase:       pmv1alpha1.PodMigrationJobPhaseRestoring,
			SnapshotRef: "test-snapshot-123",
		},
	}

	replacementPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "replacement-pod",
			Namespace: "default",
			UID:       types.UID("rep-uid-999"),
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "test-restore-pmj",
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: MigrationGateName},
			},
		},
	}

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(pmj, replacementPod).
		WithIndex(&corev1.Pod{}, PodAssignedPMJIndex, PodAssignedPMJIndexValue).
		WithStatusSubresource(pmj).
		Build()

	fakeRecorder := record.NewFakeRecorder(10)
	gateReconciler := &PodGateReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: fakeRecorder,
	}

	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "replacement-pod"}}

	_, err := gateReconciler.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile gate failed: %v", err)
	}

	events := drainEvents(fakeRecorder.Events)
	if !containsEvent(events, "Normal", "MigrationRestoreReleased") {
		t.Errorf("Expected MigrationRestoreReleased event on replacement pod, got events: %v", events)
	}

	// Verify snapshot annotation was injected
	var updatedPod corev1.Pod
	if err := fakeClient.Get(ctx, req.NamespacedName, &updatedPod); err != nil {
		t.Fatalf("Failed to get updated pod: %v", err)
	}
	if updatedPod.Annotations["podsnapshot.gke.io/ps-name"] != "test-snapshot-123" {
		t.Errorf("Expected snapshot annotation 'test-snapshot-123', got %q", updatedPod.Annotations["podsnapshot.gke.io/ps-name"])
	}
	if podHasMigrationGate(&updatedPod) {
		t.Errorf("Expected scheduling gate to be removed from replacement pod")
	}
}

func TestMetrics_ActiveGaugeAndOutcomes(t *testing.T) {
	resetActiveJobsForTest()

	key1 := "default/active-pmj-1"
	key2 := "default/active-pmj-2"

	markPMJActive(key1)
	if val := testutil.ToFloat64(pmjActive); val != 1 {
		t.Errorf("Expected pmjActive == 1, got %v", val)
	}

	markPMJActive(key2)
	if val := testutil.ToFloat64(pmjActive); val != 2 {
		t.Errorf("Expected pmjActive == 2, got %v", val)
	}

	// Re-marking active key should not increase gauge
	markPMJActive(key1)
	if val := testutil.ToFloat64(pmjActive); val != 2 {
		t.Errorf("Expected pmjActive == 2 on duplicate mark, got %v", val)
	}

	markPMJInactive(key1)
	if val := testutil.ToFloat64(pmjActive); val != 1 {
		t.Errorf("Expected pmjActive == 1 after removing key1, got %v", val)
	}

	markPMJInactive(key2)
	if val := testutil.ToFloat64(pmjActive); val != 0 {
		t.Errorf("Expected pmjActive == 0 after removing key2, got %v", val)
	}

	// Test outcomes mapping
	cases := []struct {
		phase    pmv1alpha1.PodMigrationJobPhase
		reason   string
		expected string
	}{
		{pmv1alpha1.PodMigrationJobPhaseSucceeded, "RestoreVerified", "succeeded"},
		{pmv1alpha1.PodMigrationJobPhaseFailed, "Timeout", "timeout"},
		{pmv1alpha1.PodMigrationJobPhaseFailed, "PodNotFound", "failed"},
		{pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, "FallbackToColdStart", "fallback"},
		{pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, "RestoreTimeout", "timeout"},
		{pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, "PDBEvictionTimeout", "timeout"},
		{pmv1alpha1.PodMigrationJobPhaseSucceededWithoutRestore, "ReplacementPodMismatch", "succeeded_without_restore"},
	}

	for _, c := range cases {
		out := outcomeFor(c.phase, c.reason)
		if out != c.expected {
			t.Errorf("outcomeFor(%s, %s) = %q, expected %q", c.phase, c.reason, out, c.expected)
		}
	}

	beforeSucceeded := testutil.ToFloat64(pmjOutcomes.WithLabelValues("succeeded"))
	recordOutcome("succeeded")
	afterSucceeded := testutil.ToFloat64(pmjOutcomes.WithLabelValues("succeeded"))
	if afterSucceeded != beforeSucceeded+1 {
		t.Errorf("Expected succeeded outcome counter to increment by 1, before=%v after=%v", beforeSucceeded, afterSucceeded)
	}

	// Test phase duration recording
	recordPhaseDuration("pending", 1.5)
	recordPhaseDuration("snapshotting", 3.2)
	recordPhaseDuration("evicting", 5.0)
	recordPhaseDuration("restoring", 10.5)
}

func TestCRDSchema_PrinterColumnsAndShortName(t *testing.T) {
	// Verify CRD YAML content directly to ensure generated YAML reflects the kubebuilder markers
	crdPath := "../../config/crd/bases/podmigration.gke.io_podmigrationjobs.yaml"
	manifestBytes, err := os.ReadFile(crdPath)
	if err != nil {
		t.Fatalf("Failed to read CRD file %s: %v", crdPath, err)
	}

	manifest := string(manifestBytes)
	if !strings.Contains(manifest, "shortNames:\n    - pmj") {
		t.Errorf("CRD does not contain shortName 'pmj':\n%s", manifest)
	}
	if !strings.Contains(manifest, "name: Snapshot") {
		t.Errorf("CRD does not contain 'Snapshot' printcolumn")
	}
	if !strings.Contains(manifest, "name: Age") {
		t.Errorf("CRD does not contain 'Age' printcolumn")
	}
	if !strings.Contains(manifest, "name: Phase") {
		t.Errorf("CRD does not contain 'Phase' printcolumn")
	}
}
