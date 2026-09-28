package main

import (
	"testing"
)

func TestSimulate2000PodBurstDrain(t *testing.T) {
	cfg := DefaultConfig()
	res, err := Simulate(cfg)
	if err != nil {
		t.Fatalf("Simulate(DefaultConfig) failed: %v", err)
	}
	if res.Admitted != 2000 || res.Dropped != 0 {
		t.Errorf("Admitted=%d Dropped=%d, want 2000/0", res.Admitted, res.Dropped)
	}
	if !res.CapsHeld {
		t.Errorf("CapsHeld = false, want true")
	}
	if res.PeakClusterConcurrent > cfg.PerCluster {
		t.Errorf("PeakClusterConcurrent=%d > PerCluster=%d", res.PeakClusterConcurrent, cfg.PerCluster)
	}
	if res.PeakNodeOut > cfg.PerNodeOut {
		t.Errorf("PeakNodeOut=%d > PerNodeOut=%d", res.PeakNodeOut, cfg.PerNodeOut)
	}
	if res.PeakNodeIn > cfg.PerNodeIn {
		t.Errorf("PeakNodeIn=%d > PerNodeIn=%d", res.PeakNodeIn, cfg.PerNodeIn)
	}
	if !res.PriorityPreempted {
		t.Errorf("PriorityPreempted = false, want true (spotCompletedByWave=%d, totalWaves=%d)",
			res.SpotCompletedByWave, res.Waves)
	}
	if res.SpotCompletedByWave >= res.Waves {
		t.Errorf("SpotCompletedByWave=%d should be strictly less than total Waves=%d",
			res.SpotCompletedByWave, res.Waves)
	}
	if res.SerializedGateQueueS <= 0 || res.SerializedGateQueueS > 5.0 {
		t.Errorf("SerializedGateQueueS = %v, want (0, 5.0s]", res.SerializedGateQueueS)
	}
	if res.ElapsedMS > 2000 {
		t.Errorf("ElapsedMS = %.2fms, want < 2000ms", res.ElapsedMS)
	}
}

func TestSimulatePMJWorkerBound(t *testing.T) {
	cfg := DefaultConfig()
	cfg.N = 500
	cfg.PerCluster = 100
	cfg.PMJWorkers = 50 // Controller MaxConcurrentReconciles=50 bounds effective concurrency
	cfg.PerNodeOut = 10
	cfg.PerNodeIn = 10

	res, err := Simulate(cfg)
	if err != nil {
		t.Fatalf("Simulate failed: %v", err)
	}
	if res.PeakClusterConcurrent > 50 {
		t.Errorf("PeakClusterConcurrent = %d, want <= 50 (bounded by PMJWorkers)", res.PeakClusterConcurrent)
	}
}

func TestSimulateValidationErrors(t *testing.T) {
	if _, err := Simulate(Config{N: 0, Nodes: 10}); err == nil {
		t.Error("expected error for N=0")
	}
	if _, err := Simulate(Config{N: 10, Nodes: 1}); err == nil {
		t.Error("expected error for Nodes=1")
	}
}
