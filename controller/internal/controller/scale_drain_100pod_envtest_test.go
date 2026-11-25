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
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/invariants"
	"github.com/gke-labs/pod-migration/controller/internal/snapshot"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// trackedScaleAPIReader intercepts and counts direct apiserver calls to ensure
// no per-reconcile uncached API storms (APF regression guard).
type trackedScaleAPIReader struct {
	reader    client.Reader
	getCount  atomic.Int64
	listCount atomic.Int64
}

func (t *trackedScaleAPIReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	t.getCount.Add(1)
	return t.reader.Get(ctx, key, obj, opts...)
}

func (t *trackedScaleAPIReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	t.listCount.Add(1)
	return t.reader.List(ctx, list, opts...)
}

// ScaleDrainSummary represents the structured CI artifact emitted by the 100-pod
// concurrent drain regression gate (#251).
type ScaleDrainSummary struct {
	TestName      string             `json:"testName"`
	Timestamp     string             `json:"timestamp"`
	PodCount      int                `json:"podCount"`
	Concurrency   int                `json:"concurrency"`
	InvariantMode string             `json:"invariantMode"`
	SLOGates      ScaleDrainSLOGates `json:"sloGates"`
	Results       ScaleDrainResults  `json:"results"`
	Passed        bool               `json:"passed"`
}

type ScaleDrainSLOGates struct {
	MaxDurationSeconds     float64 `json:"maxDurationSeconds"`
	MaxGateHoldP95Seconds  float64 `json:"maxGateHoldP95Seconds"`
	MaxMigrationP95Seconds float64 `json:"maxMigrationP95Seconds"`
	MaxHeapAllocBytes      uint64  `json:"maxHeapAllocBytes"`
	MaxHeapIncreaseBytes   int64   `json:"maxHeapIncreaseBytes"`
	MaxUncachedGetCalls    int64   `json:"maxUncachedGetCalls"`
	MaxUncachedListCalls   int64   `json:"maxUncachedListCalls"`
}

type ScaleDrainResults struct {
	DurationSeconds          float64 `json:"durationSeconds"`
	ThroughputPodsPerSec     float64 `json:"throughputPodsPerSec"`
	HeapAllocBytes           uint64  `json:"heapAllocBytes"`
	HeapIncreaseBytes        int64   `json:"heapIncreaseBytes"`
	HeapSysBytes             uint64  `json:"heapSysBytes"`
	NumGC                    uint32  `json:"numGC"`
	UncachedGetCalls         int64   `json:"uncachedGetCalls"`
	UncachedListCalls        int64   `json:"uncachedListCalls"`
	WedgedGatesCount         int     `json:"wedgedGatesCount"`
	InvariantViolationsTotal float64 `json:"invariantViolationsTotal"`
	GateHoldP50Seconds       float64 `json:"gateHoldP50Seconds"`
	GateHoldP90Seconds       float64 `json:"gateHoldP90Seconds"`
	GateHoldP95Seconds       float64 `json:"gateHoldP95Seconds"`
	GateHoldMaxSeconds       float64 `json:"gateHoldMaxSeconds"`
	MigrationP50Seconds      float64 `json:"migrationP50Seconds"`
	MigrationP90Seconds      float64 `json:"migrationP90Seconds"`
	MigrationP95Seconds      float64 `json:"migrationP95Seconds"`
	MigrationMaxSeconds      float64 `json:"migrationMaxSeconds"`
}

func percentile(sortedVals []float64, pct float64) float64 {
	if len(sortedVals) == 0 {
		return 0
	}
	idx := int(math.Ceil(pct*float64(len(sortedVals)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sortedVals) {
		idx = len(sortedVals) - 1
	}
	return sortedVals[idx]
}

// TestScale100PodConcurrentDrain implements the automated 100-pod concurrent
// drain scale regression gate under --invariant-mode=strict (#251).
// It verifies:
//  1. 100 stateful pods draining simultaneously under 50 concurrent PMJ workers
//     and serialized PodGate worker.
//  2. Zero wedged scheduling gates (all replacement pods un-gated).
//  3. Zero invariant violations under ModeStrict across I1-I12.
//  4. Hard SLO gates on completion time, controller RSS/heap memory, zero OOMs,
//     and zero uncached API storms.
//  5. Exports structured ScaleDrainSummary artifact for CI.
func TestScale100PodConcurrentDrain(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("KUBEBUILDER_ASSETS not set; skipping 100-pod scale drain envtest")
	}
	if os.Getenv("RUN_SCALE_DRAIN_GATE") == "" {
		t.Skip("RUN_SCALE_DRAIN_GATE not set; skipping 100-pod scale drain envtest")
	}

	podCount := 100
	if envCount := os.Getenv("SCALE_DRAIN_POD_COUNT"); envCount != "" {
		if c, err := strconv.Atoi(envCount); err == nil && c > 0 {
			podCount = c
		}
	}

	const (
		maxDurationSeconds     = 25.0
		maxGateHoldP95Seconds  = 12.0
		maxMigrationP95Seconds = 18.0
		maxHeapBytes           = uint64(64 * 1024 * 1024)
		maxHeapIncreaseBytes   = int64(32 * 1024 * 1024)
	)
	maxUncachedGets := int64(podCount + 10)
	maxUncachedLists := int64(podCount + 10)

	reportPath := os.Getenv("SCALE_SUMMARY_PATH")
	if reportPath == "" {
		reportPath = "/tmp/scale-100-drain-summary.json"
	}
	_ = os.Remove(reportPath)

	summary := &ScaleDrainSummary{
		TestName:      "100-Pod Concurrent Drain Scale Regression Gate",
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		PodCount:      podCount,
		Concurrency:   50,
		InvariantMode: "strict",
		SLOGates: ScaleDrainSLOGates{
			MaxDurationSeconds:     maxDurationSeconds,
			MaxGateHoldP95Seconds:  maxGateHoldP95Seconds,
			MaxMigrationP95Seconds: maxMigrationP95Seconds,
			MaxHeapAllocBytes:      maxHeapBytes,
			MaxHeapIncreaseBytes:   maxHeapIncreaseBytes,
			MaxUncachedGetCalls:    maxUncachedGets,
			MaxUncachedListCalls:   maxUncachedLists,
		},
		Passed: false,
	}

	defer func() {
		summary.Passed = !t.Failed()
		if summaryData, err := json.MarshalIndent(summary, "", "  "); err == nil {
			if writeErr := os.WriteFile(reportPath, summaryData, 0644); writeErr == nil {
				t.Logf("Emitted structured scale summary artifact to %s (passed=%v)", reportPath, summary.Passed)
			} else {
				t.Logf("Failed to write summary to %s: %v", reportPath, writeErr)
			}
		}
	}()

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

	scheme := k8sruntime.NewScheme()
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
	trackedReader := &trackedScaleAPIReader{reader: rawClient}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	ns := createTestNamespace(t, ctx, k8sClient, "scale-100-drain")
	recorder := record.NewFakeRecorder(4096)
	stopRecorderDrain := make(chan struct{})
	defer close(stopRecorderDrain)
	go func() {
		for {
			select {
			case <-stopRecorderDrain:
				return
			case <-recorder.Events:
			}
		}
	}()
	invEngine := invariants.NewEngine(invariants.ModeStrict, recorder)

	// Step 1: Create nodes to represent multi-node drain topology
	const nodeCount = 5
	for i := 0; i < nodeCount; i++ {
		nodeName := fmt.Sprintf("scale-node-%d", i)
		node := &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"node.kubernetes.io/instance-type": "e2-standard-4",
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{
					{
						Type:   corev1.NodeReady,
						Status: corev1.ConditionTrue,
					},
				},
			},
		}
		if err := k8sClient.Create(ctx, node); err != nil && !apierrors.IsAlreadyExists(err) {
			t.Fatalf("create node %s: %v", nodeName, err)
		}
	}

	// Step 2: Create parent ReplicaSet managing the 100 pods
	replicas := int32(podCount)
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns,
			Name:      "web-scale-rs",
			Labels: map[string]string{
				"app":                                  "web-scale",
				appsv1.DefaultDeploymentUniqueLabelKey: "hash-scale-100",
			},
		},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "web-scale"},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                                  "web-scale",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-scale-100",
						"pod-migration.gke.io/enabled":         "true",
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
				},
			},
		},
	}
	if err := k8sClient.Create(ctx, rs); err != nil {
		t.Fatalf("create parent ReplicaSet: %v", err)
	}

	isController := true
	ownerRef := metav1.OwnerReference{
		APIVersion: "apps/v1",
		Kind:       "ReplicaSet",
		Name:       rs.Name,
		UID:        rs.UID,
		Controller: &isController,
	}

	// Step 3: Populate 100 source pods across the 5 nodes
	t.Logf("Creating %d source pods across %d nodes...", podCount, nodeCount)
	for i := 0; i < podCount; i++ {
		podName := fmt.Sprintf("web-src-%d", i)
		nodeName := fmt.Sprintf("scale-node-%d", i%nodeCount)
		srcPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: ns,
				Name:      podName,
				Labels: map[string]string{
					"app":                                  "web-scale",
					appsv1.DefaultDeploymentUniqueLabelKey: "hash-scale-100",
					"pod-migration.gke.io/enabled":         "true",
				},
				OwnerReferences: []metav1.OwnerReference{ownerRef},
			},
			Spec: corev1.PodSpec{
				NodeName:   nodeName,
				Containers: []corev1.Container{{Name: "app", Image: "busybox"}},
			},
		}
		if err := k8sClient.Create(ctx, srcPod); err != nil {
			t.Fatalf("create source pod %s: %v", podName, err)
		}
		srcPod.Status.Phase = corev1.PodRunning
		srcPod.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.Now()},
		}
		if err := k8sClient.Status().Update(ctx, srcPod); err != nil {
			t.Fatalf("update source pod %s status: %v", podName, err)
		}
	}

	// Step 4: Prepare Reconcilers
	gateRec := &PodGateReconciler{
		Client:          k8sClient,
		APIReader:       trackedReader,
		Scheme:          scheme,
		Recorder:        recorder,
		InvariantEngine: invEngine,
	}
	pmjRec := &PodMigrationJobReconciler{
		Client:                  k8sClient,
		APIReader:               trackedReader,
		Scheme:                  scheme,
		Recorder:                recorder,
		InvariantEngine:         invEngine,
		DefaultMigrationTimeout: 2 * time.Minute,
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	testStartTime := time.Now()
	beforeViolations := totalInvariantViolations()

	// Step 5: Execute 100-Pod Concurrent Evacuation Wave
	// Create 100 PMJs, 100 ready snapshots, and 100 replacement pods contending for gates.
	type podTrace struct {
		name         string
		evictTime    time.Time
		gateDoneTime time.Time
		pmjDoneTime  time.Time
	}
	traces := make([]podTrace, podCount)

	t.Logf("Firing 100-pod concurrent evacuation wave...")
	var setupWg sync.WaitGroup
	for i := 0; i < podCount; i++ {
		setupWg.Add(1)
		go func(idx int) {
			defer setupWg.Done()
			traces[idx].name = fmt.Sprintf("web-src-%d", idx)
			traces[idx].evictTime = time.Now()

			pmjName := fmt.Sprintf("pmj-scale-%d", idx)
			snapName := fmt.Sprintf("snap-scale-%d", idx)
			replName := fmt.Sprintf("web-repl-%d", idx)

			// Create ready snapshot
			snapObj := readySnapshot(snapshot.DefaultAPIGroup, ns, snapName)
			if err := k8sClient.Create(ctx, snapObj); err != nil {
				t.Errorf("create snapshot %s: %v", snapName, err)
				return
			}

			// Create PMJ in PhaseRestoring with SnapshotRef
			pmj := &pmv1alpha1.PodMigrationJob{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      pmjName,
					Labels: map[string]string{
						util.LabelParentUID:       string(rs.UID),
						util.LabelParentKind:      "ReplicaSet",
						util.LabelPodTemplateHash: "hash-scale-100",
					},
				},
				Spec: pmv1alpha1.PodMigrationJobSpec{
					PodRef:       corev1.LocalObjectReference{Name: traces[idx].name},
					TargetPodUID: fmt.Sprintf("uid-src-%d", idx),
				},
			}
			if err := k8sClient.Create(ctx, pmj); err != nil {
				t.Errorf("create PMJ %s: %v", pmjName, err)
				return
			}
			now := metav1.Now()
			pmj.Status.Phase = pmv1alpha1.PodMigrationJobPhaseRestoring
			pmj.Status.SnapshotRef = snapName
			pmj.Status.RestoringStartTime = &now
			if err := k8sClient.Status().Update(ctx, pmj); err != nil {
				t.Errorf("update PMJ %s status: %v", pmjName, err)
				return
			}

			// Create replacement pod holding the migration scheduling gate
			replPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      replName,
					Labels: map[string]string{
						"app":                                  "web-scale",
						appsv1.DefaultDeploymentUniqueLabelKey: "hash-scale-100",
						"pod-migration.gke.io/enabled":         "true",
					},
					Annotations: map[string]string{
						util.AnnotationAssignedPMJ: pmjName,
					},
					OwnerReferences: []metav1.OwnerReference{ownerRef},
				},
				Spec: corev1.PodSpec{
					SchedulingGates: []corev1.PodSchedulingGate{{Name: MigrationGateName}},
					Containers:      []corev1.Container{{Name: "app", Image: "busybox"}},
				},
			}
			if err := k8sClient.Create(ctx, replPod); err != nil {
				t.Errorf("create replacement pod %s: %v", replName, err)
				return
			}
		}(i)
	}
	setupWg.Wait()

	// Step 6: Concurrent Gate Processing (Simulating PodGate Controller)
	// Serialized admission gate release (MaxConcurrentReconciles: 1)
	t.Logf("Processing 100 replacement scheduling gates...")
	for i := 0; i < podCount; i++ {
		replName := fmt.Sprintf("web-repl-%d", i)
		if _, err := gateRec.Reconcile(ctx, ctrl.Request{
			NamespacedName: types.NamespacedName{Namespace: ns, Name: replName},
		}); err != nil {
			t.Fatalf("PodGateReconciler reconcile %s failed: %v", replName, err)
		}
		traces[i].gateDoneTime = time.Now()

		// Update replacement pod to Ready in apiserver once un-gated
		var gotRepl corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: replName}, &gotRepl); err != nil {
			t.Fatalf("get un-gated replacement pod %s: %v", replName, err)
		}
		gotRepl.Status.Phase = corev1.PodRunning
		gotRepl.Status.Conditions = []corev1.PodCondition{
			{
				Type:               corev1.PodReady,
				Status:             corev1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
			},
		}
		if err := k8sClient.Status().Update(ctx, &gotRepl); err != nil {
			t.Fatalf("update replacement pod status %s: %v", replName, err)
		}
	}

	// Step 7: Concurrent PMJ Processing (50 Concurrent Workers per PR #33 default)
	t.Logf("Reconciling 100 PMJs across 50 concurrent worker routines under strict invariant mode...")
	const pmjWorkers = 50
	workQueue := make(chan int, podCount)
	for i := 0; i < podCount; i++ {
		workQueue <- i
	}
	close(workQueue)

	var workerWg sync.WaitGroup
	for w := 0; w < pmjWorkers; w++ {
		workerWg.Add(1)
		go func() {
			defer workerWg.Done()
			for idx := range workQueue {
				pmjName := fmt.Sprintf("pmj-scale-%d", idx)
				if _, err := pmjRec.Reconcile(ctx, ctrl.Request{
					NamespacedName: types.NamespacedName{Namespace: ns, Name: pmjName},
				}); err != nil {
					t.Errorf("PMJReconciler reconcile %s failed: %v", pmjName, err)
				}
				traces[idx].pmjDoneTime = time.Now()
			}
		}()
	}
	workerWg.Wait()

	totalElapsed := time.Since(testStartTime)
	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	netHeapAlloc := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
	actualGets := trackedReader.getCount.Load()
	actualLists := trackedReader.listCount.Load()

	summary.Results.DurationSeconds = math.Round(totalElapsed.Seconds()*1000) / 1000
	summary.Results.ThroughputPodsPerSec = math.Round((float64(podCount)/totalElapsed.Seconds())*100) / 100
	summary.Results.HeapAllocBytes = memAfter.HeapAlloc
	summary.Results.HeapIncreaseBytes = netHeapAlloc
	summary.Results.HeapSysBytes = memAfter.HeapSys
	summary.Results.NumGC = memAfter.NumGC
	summary.Results.UncachedGetCalls = actualGets
	summary.Results.UncachedListCalls = actualLists

	// Step 8: Assertions & Hard SLO Gates
	t.Logf("100-pod concurrent drain completed in %v. Verifying hard SLO gates...", totalElapsed)

	// SLO 1: Zero wedged scheduling gates
	wedgedCount := 0
	for i := 0; i < podCount; i++ {
		replName := fmt.Sprintf("web-repl-%d", i)
		var checkPod corev1.Pod
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: replName}, &checkPod); err != nil {
			t.Fatalf("get check pod %s: %v", replName, err)
		}
		if podHasMigrationGate(&checkPod) {
			wedgedCount++
		}
	}
	summary.Results.WedgedGatesCount = wedgedCount
	if wedgedCount > 0 {
		t.Fatalf("SLO Breach: %d/%d replacement pods have wedged scheduling gates", wedgedCount, podCount)
	}

	// SLO 2: All 100 PMJs Succeeded
	var gateHoldDurations []float64
	var migrationDurations []float64
	for i := 0; i < podCount; i++ {
		pmjName := fmt.Sprintf("pmj-scale-%d", i)
		var job pmv1alpha1.PodMigrationJob
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: pmjName}, &job); err != nil {
			t.Fatalf("get finished PMJ %s: %v", pmjName, err)
		}
		if job.Status.Phase != pmv1alpha1.PodMigrationJobPhaseSucceeded {
			t.Fatalf("SLO Breach: PMJ %s did not reach Succeeded, got phase=%s conditions=%+v",
				pmjName, job.Status.Phase, job.Status.Conditions)
		}

		gateDur := traces[i].gateDoneTime.Sub(traces[i].evictTime).Seconds()
		migDur := traces[i].pmjDoneTime.Sub(traces[i].evictTime).Seconds()
		gateHoldDurations = append(gateHoldDurations, gateDur)
		migrationDurations = append(migrationDurations, migDur)
	}
	sort.Float64s(gateHoldDurations)
	sort.Float64s(migrationDurations)

	p50Gate := percentile(gateHoldDurations, 0.50)
	p90Gate := percentile(gateHoldDurations, 0.90)
	p95Gate := percentile(gateHoldDurations, 0.95)
	maxGate := gateHoldDurations[len(gateHoldDurations)-1]

	p50Mig := percentile(migrationDurations, 0.50)
	p90Mig := percentile(migrationDurations, 0.90)
	p95Mig := percentile(migrationDurations, 0.95)
	maxMig := migrationDurations[len(migrationDurations)-1]

	violations := totalInvariantViolations() - beforeViolations

	summary.Results = ScaleDrainResults{
		DurationSeconds:          math.Round(totalElapsed.Seconds()*1000) / 1000,
		ThroughputPodsPerSec:     math.Round((float64(podCount)/totalElapsed.Seconds())*100) / 100,
		HeapAllocBytes:           memAfter.HeapAlloc,
		HeapIncreaseBytes:        netHeapAlloc,
		HeapSysBytes:             memAfter.HeapSys,
		NumGC:                    memAfter.NumGC,
		UncachedGetCalls:         actualGets,
		UncachedListCalls:        actualLists,
		WedgedGatesCount:         wedgedCount,
		InvariantViolationsTotal: violations,
		GateHoldP50Seconds:       math.Round(p50Gate*1000) / 1000,
		GateHoldP90Seconds:       math.Round(p90Gate*1000) / 1000,
		GateHoldP95Seconds:       math.Round(p95Gate*1000) / 1000,
		GateHoldMaxSeconds:       math.Round(maxGate*1000) / 1000,
		MigrationP50Seconds:      math.Round(p50Mig*1000) / 1000,
		MigrationP90Seconds:      math.Round(p90Mig*1000) / 1000,
		MigrationP95Seconds:      math.Round(p95Mig*1000) / 1000,
		MigrationMaxSeconds:      math.Round(maxMig*1000) / 1000,
	}

	// SLO 3: Invariant Violations must be 0
	if violations > 0 {
		t.Fatalf("SLO Breach: invariant engine reported %v violations in strict mode during 100-pod drain", violations)
	}

	// SLO 4: Wall-clock completion budget (< 25s for 100 pods)
	if totalElapsed.Seconds() > maxDurationSeconds {
		t.Fatalf("SLO Breach: total duration %.2fs exceeded hard ceiling %.2fs", totalElapsed.Seconds(), maxDurationSeconds)
	}

	// SLO 5: Gate Hold P95 latency (< 12s)
	if p95Gate > maxGateHoldP95Seconds {
		t.Fatalf("SLO Breach: gate hold P95 latency %.2fs exceeded hard ceiling %.2fs", p95Gate, maxGateHoldP95Seconds)
	}

	// SLO 6: Migration P95 latency (< 18s)
	if p95Mig > maxMigrationP95Seconds {
		t.Fatalf("SLO Breach: migration P95 latency %.2fs exceeded hard ceiling %.2fs", p95Mig, maxMigrationP95Seconds)
	}

	// SLO 7: Memory footprint (< 64 MiB heap alloc, < 32 MiB net heap increase)
	if memAfter.HeapAlloc > maxHeapBytes {
		t.Fatalf("SLO Breach: heap allocated %d bytes exceeded ceiling %d bytes", memAfter.HeapAlloc, maxHeapBytes)
	}
	if netHeapAlloc > maxHeapIncreaseBytes {
		t.Fatalf("SLO Breach: net heap increase %d bytes exceeded ceiling %d bytes", netHeapAlloc, maxHeapIncreaseBytes)
	}

	// SLO 8: Zero uncached API storms (APIReader Get/List bounds)
	if actualGets > maxUncachedGets {
		t.Fatalf("SLO Breach: uncached API get calls %d exceeded ceiling %d", actualGets, maxUncachedGets)
	}
	if actualLists > maxUncachedLists {
		t.Fatalf("SLO Breach: uncached API list calls %d exceeded ceiling %d", actualLists, maxUncachedLists)
	}

	t.Logf("Scale Regression Gate PASSED: %d pods in %.2fs (%.1f pods/sec), heap=%dMB (net=+%dMB), uncachedGets=%d, uncachedLists=%d, gateP95=%.2fs, migP95=%.2fs, violations=0",
		podCount, summary.Results.DurationSeconds, summary.Results.ThroughputPodsPerSec,
		summary.Results.HeapAllocBytes/(1024*1024), summary.Results.HeapIncreaseBytes/(1024*1024),
		summary.Results.UncachedGetCalls, summary.Results.UncachedListCalls,
		summary.Results.GateHoldP95Seconds, summary.Results.MigrationP95Seconds)
}
