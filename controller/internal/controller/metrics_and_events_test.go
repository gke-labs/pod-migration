package controller

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
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

func getHistogramSampleCount(h prometheus.Observer) uint64 {
	m := &dto.Metric{}
	if h.(prometheus.Metric).Write(m) == nil && m.Histogram != nil {
		return m.Histogram.GetSampleCount()
	}
	return 0
}

func getHistogramSampleSum(h prometheus.Observer) float64 {
	m := &dto.Metric{}
	if h.(prometheus.Metric).Write(m) == nil && m.Histogram != nil {
		return m.Histogram.GetSampleSum()
	}
	return 0
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
	metrics.ResetActiveJobsForTest()

	key1 := "default/active-pmj-1"
	key2 := "default/active-pmj-2"

	metrics.MarkPMJActive(key1)
	if val := testutil.ToFloat64(metrics.ActiveMigrations); val != 1 {
		t.Errorf("Expected ActiveMigrations == 1, got %v", val)
	}

	metrics.MarkPMJActive(key2)
	if val := testutil.ToFloat64(metrics.ActiveMigrations); val != 2 {
		t.Errorf("Expected ActiveMigrations == 2, got %v", val)
	}

	// Re-marking active key should not increase gauge
	metrics.MarkPMJActive(key1)
	if val := testutil.ToFloat64(metrics.ActiveMigrations); val != 2 {
		t.Errorf("Expected ActiveMigrations == 2 on duplicate mark, got %v", val)
	}

	metrics.MarkPMJInactive(key1)
	if val := testutil.ToFloat64(metrics.ActiveMigrations); val != 1 {
		t.Errorf("Expected ActiveMigrations == 1 after removing key1, got %v", val)
	}

	metrics.MarkPMJInactive(key2)
	if val := testutil.ToFloat64(metrics.ActiveMigrations); val != 0 {
		t.Errorf("Expected ActiveMigrations == 0 after removing key2, got %v", val)
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
		out := metrics.OutcomeFor(c.phase, c.reason)
		if out != c.expected {
			t.Errorf("OutcomeFor(%s, %s) = %q, expected %q", c.phase, c.reason, out, c.expected)
		}
	}

	beforeSucceeded := testutil.ToFloat64(metrics.OutcomesTotal.WithLabelValues("succeeded"))
	metrics.RecordOutcome("succeeded")
	afterSucceeded := testutil.ToFloat64(metrics.OutcomesTotal.WithLabelValues("succeeded"))
	if afterSucceeded != beforeSucceeded+1 {
		t.Errorf("Expected succeeded outcome counter to increment by 1, before=%v after=%v", beforeSucceeded, afterSucceeded)
	}
}

func TestMetrics_PhaseDurations_ReconcileDriven(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = corev1.AddToScheme(scheme)
	_ = pmv1alpha1.AddToScheme(scheme)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-pod",
			Namespace: "default",
			UID:       types.UID("pod-uid-222"),
		},
	}

	// Anchor pending duration: created 2s ago
	creationTime := metav1.NewTime(time.Now().Add(-2 * time.Second))
	pmj := &pmv1alpha1.PodMigrationJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "durations-pmj",
			Namespace:         "default",
			CreationTimestamp: creationTime,
		},
		Spec: pmv1alpha1.PodMigrationJobSpec{
			PodRef:       corev1.LocalObjectReference{Name: "source-pod"},
			TargetPodUID: "pod-uid-222",
		},
		Status: pmv1alpha1.PodMigrationJobStatus{
			Phase: pmv1alpha1.PodMigrationJobPhasePending,
		},
	}

	fakeClient := newFakeClientBuilderWithEventIndex(scheme).
		WithObjects(pod, pmj).
		WithStatusSubresource(pmj).
		Build()

	mockProvider := &testSnapshotProvider{
		checkStatus: &snapshot.Status{
			Phase:       snapshot.PhaseReady,
			SnapshotRef: "snapshot-durations",
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
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "default", Name: "durations-pmj"}}

	pendingHist := metrics.PhaseDurationSeconds.WithLabelValues("pending")
	snapshottingHist := metrics.PhaseDurationSeconds.WithLabelValues("snapshotting")
	evictingHist := metrics.PhaseDurationSeconds.WithLabelValues("evicting")
	restoringHist := metrics.PhaseDurationSeconds.WithLabelValues("restoring")

	pendingBeforeCount := getHistogramSampleCount(pendingHist)
	snapshottingBeforeCount := getHistogramSampleCount(snapshottingHist)
	evictingBeforeCount := getHistogramSampleCount(evictingHist)
	restoringBeforeCount := getHistogramSampleCount(restoringHist)

	// Step 1: Reconcile Pending -> Snapshotting
	_, err := r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile pending failed: %v", err)
	}

	// Pending duration recorded
	if getHistogramSampleCount(pendingHist) != pendingBeforeCount+1 {
		t.Errorf("Expected pending duration histogram to observe 1 sample")
	}

	// Verify SnapshottingStartTime was recorded on status
	var currentPMJ pmv1alpha1.PodMigrationJob
	if err := fakeClient.Get(ctx, req.NamespacedName, &currentPMJ); err != nil {
		t.Fatalf("Failed to fetch PMJ: %v", err)
	}
	if currentPMJ.Status.SnapshottingStartTime == nil {
		t.Fatalf("Expected SnapshottingStartTime to be set, got nil")
	}

	// Simulate Snapshotting duration: anchor start time to 3s ago
	snapStartTime := metav1.NewTime(time.Now().Add(-3 * time.Second))
	currentPMJ.Status.SnapshottingStartTime = &snapStartTime
	if err := fakeClient.Status().Update(ctx, &currentPMJ); err != nil {
		t.Fatalf("Failed to update SnapshottingStartTime: %v", err)
	}

	// Step 2: Reconcile Snapshotting -> Evicting (mock provider returns PhaseReady)
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile snapshotting failed: %v", err)
	}

	// Snapshotting duration recorded, ~3s
	if getHistogramSampleCount(snapshottingHist) != snapshottingBeforeCount+1 {
		t.Errorf("Expected snapshotting duration histogram to observe 1 sample")
	}

	// Step 3: Reconcile Evicting (first pass records evicting-since annotation)
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile evicting pass 1 failed: %v", err)
	}

	// Verify evicting-since annotation exists
	if err := fakeClient.Get(ctx, req.NamespacedName, &currentPMJ); err != nil {
		t.Fatalf("Failed to fetch PMJ: %v", err)
	}
	if currentPMJ.Annotations["pod-migration.gke.io/evicting-since"] == "" {
		t.Fatalf("Expected evicting-since annotation to be set")
	}

	// Set evicting-since to 1s ago to simulate realistic evicting duration
	currentPMJ.Annotations["pod-migration.gke.io/evicting-since"] = time.Now().Add(-1 * time.Second).Format(time.RFC3339)
	// Delete source pod to simulate successful eviction and detachment
	if err := fakeClient.Delete(ctx, pod); err != nil {
		t.Fatalf("Failed to delete source pod: %v", err)
	}
	if err := fakeClient.Update(ctx, &currentPMJ); err != nil {
		t.Fatalf("Failed to update PMJ annotations: %v", err)
	}

	evictingSumBefore := getHistogramSampleSum(evictingHist)

	// Step 4: Reconcile Evicting -> Restoring
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile evicting -> restoring failed: %v", err)
	}

	if getHistogramSampleCount(evictingHist) != evictingBeforeCount+1 {
		t.Errorf("Expected evicting duration histogram to observe 1 sample")
	}

	observedEvicting := getHistogramSampleSum(evictingHist) - evictingSumBefore
	// The evicting duration must measure ~1s (from evicting-since), NOT 1s + 3s = 4s (leaking snapshotting duration)
	if observedEvicting < 0.9 || observedEvicting > 2.5 {
		t.Errorf("Expected evicting duration ~1s without snapshotting leakage, got %v seconds", observedEvicting)
	}

	// Verify RestoringStartTime was recorded
	if err := fakeClient.Get(ctx, req.NamespacedName, &currentPMJ); err != nil {
		t.Fatalf("Failed to fetch PMJ: %v", err)
	}
	if currentPMJ.Status.RestoringStartTime == nil {
		t.Fatalf("Expected RestoringStartTime to be set, got nil")
	}

	// Simulate restoring duration: anchor start time to 2s ago
	restoringStartTime := metav1.NewTime(time.Now().Add(-2 * time.Second))
	currentPMJ.Status.RestoringStartTime = &restoringStartTime
	if err := fakeClient.Status().Update(ctx, &currentPMJ); err != nil {
		t.Fatalf("Failed to update RestoringStartTime: %v", err)
	}

	// Create replacement pod and mark Ready
	readyCond := corev1.PodCondition{
		Type:   corev1.PodReady,
		Status: corev1.ConditionTrue,
	}
	repPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "source-pod",
			Namespace: "default",
			UID:       types.UID("rep-pod-uid-333"),
			Annotations: map[string]string{
				"pod-migration.gke.io/assigned-pmj": "durations-pmj",
			},
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{readyCond},
		},
	}
	if err := fakeClient.Create(ctx, repPod); err != nil {
		t.Fatalf("Failed to create replacement pod: %v", err)
	}

	// Update PMJ to record consumption by replacement pod
	currentPMJ.Status.Consumed = true
	currentPMJ.Status.RestoredPodUID = string(repPod.UID)
	currentPMJ.Status.RestoredPodName = repPod.Name
	currentPMJ.Status.GateReleased = true
	if err := fakeClient.Status().Update(ctx, &currentPMJ); err != nil {
		t.Fatalf("Failed to update PMJ status for consumed pod: %v", err)
	}

	// Step 5: Reconcile Restoring -> Succeeded
	_, err = r.Reconcile(ctx, req)
	if err != nil {
		t.Fatalf("Reconcile restoring -> succeeded failed: %v", err)
	}

	if getHistogramSampleCount(restoringHist) != restoringBeforeCount+1 {
		t.Errorf("Expected restoring duration histogram to observe 1 sample")
	}

	if err := fakeClient.Get(ctx, req.NamespacedName, &currentPMJ); err != nil {
		t.Fatalf("Failed to fetch final PMJ: %v", err)
	}
	if currentPMJ.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
		t.Errorf("Expected PMJ to be in Succeeded phase, got %s", currentPMJ.Status.Phase)
	}
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
