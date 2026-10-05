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

package continuity

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAnalyze_CleanContinuity(t *testing.T) {
	samples := []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 1, ObservedMaxSeq: 1, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:02Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 2, ObservedMaxSeq: 2, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:03Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 3, ObservedMaxSeq: 3, Phase: "migrating"},
		{TS: "2026-09-29T10:00:04Z", Pod: "redis-0", Error: "connection refused during blackout", Phase: "migrating"},
		{TS: "2026-09-29T10:00:05Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 4, ObservedMaxSeq: 4, Phase: "post-restore"},
		{TS: "2026-09-29T10:00:06Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 5, ObservedMaxSeq: 5, Phase: "post-restore"},
	}

	res := Analyze("redis", samples)
	if !res.Pass {
		t.Fatalf("expected clean continuity trace to pass, got detail=%q violations=%v", res.Detail, res.Violations)
	}
	if res.AckedWrites != 5 || res.MaxAckedSeq != 5 || res.FinalObservedMaxSeq != 5 {
		t.Fatalf("unexpected counts: %+v", res)
	}
	if len(res.LostAckedWrites) != 0 || res.EmptySourceReads != 0 || res.SequenceRollbacks != 0 {
		t.Fatalf("expected zero failures, got %+v", res)
	}
}

func TestAnalyze_LostAcknowledgedWrites(t *testing.T) {
	// Source acknowledges writes 1..5 (4 and 5 after checkpoint at seq=3).
	// Restored copy starts at seq=3 and never receives 4 or 5.
	samples := []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 1, ObservedMaxSeq: 1, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:02Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 2, ObservedMaxSeq: 2, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:03Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 3, ObservedMaxSeq: 3, Phase: "pre-checkpoint"},
		// Post-checkpoint writes acknowledged by source before eviction:
		{TS: "2026-09-29T10:00:04Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 4, ObservedMaxSeq: 4, Phase: "migrating"},
		{TS: "2026-09-29T10:00:05Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 5, ObservedMaxSeq: 5, Phase: "migrating"},
		// Restored pod comes up with only checkpointed state (seq=3):
		{TS: "2026-09-29T10:00:08Z", Op: "final", Pod: "redis-0", InstanceID: "run-a", ObservedMaxSeq: 3, Phase: "post-restore"},
	}

	res := Analyze("redis", samples)
	if res.Pass {
		t.Fatalf("expected lost acknowledged writes to fail, but passed: %+v", res)
	}
	if len(res.LostAckedWrites) != 2 || res.LostAckedWrites[0] != 4 || res.LostAckedWrites[1] != 5 {
		t.Fatalf("expected LostAckedWrites=[4 5], got %v", res.LostAckedWrites)
	}
	if res.DoubleRestoreCount != 0 || res.SequenceRollbacks != 0 || res.EmptySourceReads != 0 {
		t.Fatalf("expected single-restore lost writes NOT to be labeled as double-restore or empty-source, got %+v", res)
	}
	if !strings.Contains(res.Detail, "lost acknowledged writes") || strings.Contains(res.Detail, "double-restore") {
		t.Fatalf("expected detail to mention only lost acknowledged writes, got %q", res.Detail)
	}
}

func TestAnalyze_EmptySourceServing(t *testing.T) {
	// Source container is stopped after checkpoint at seq=3, kubelet restarts it
	// with empty state (run-b, observedMaxSeq=0) before eviction lands, then replacement
	// restores at seq=3 and advances to seq=4.
	samples := []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 1, ObservedMaxSeq: 1, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:02Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 2, ObservedMaxSeq: 2, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:03Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 3, ObservedMaxSeq: 3, Phase: "pre-checkpoint"},
		// Restarted empty source answers a client before eviction:
		{TS: "2026-09-29T10:00:04Z", Pod: "redis-0", InstanceID: "run-empty-restart", ObservedMaxSeq: 0, Phase: "migrating"},
		// Replacement restores from checkpoint and continues:
		{TS: "2026-09-29T10:00:08Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 4, ObservedMaxSeq: 4, Phase: "post-restore"},
	}

	res := Analyze("redis", samples)
	if res.Pass {
		t.Fatalf("expected empty-source serving to fail, but passed: %+v", res)
	}
	if res.EmptySourceReads != 1 {
		t.Fatalf("expected EmptySourceReads=1, got %d", res.EmptySourceReads)
	}
	if len(res.LostAckedWrites) != 0 || res.DoubleRestoreCount != 0 || res.SequenceRollbacks != 0 {
		t.Fatalf("expected only EmptySourceReads=1, got %+v", res)
	}
	if !strings.Contains(res.Detail, "empty-source serving") {
		t.Fatalf("expected detail to mention empty-source serving, got %q", res.Detail)
	}
}

func TestAnalyze_DoubleRestoreRollback(t *testing.T) {
	// Replacement restores at seq=3, acknowledges writes 4 and 5 in post-restore,
	// then gets restored a second time from the same checkpoint (regressing to seq=3)
	// before advancing to seq=6. Because the final sample reaches seq=6 (== maxAckedSeq),
	// the end-of-run lost-writes check does not fire; only the mid-stream double-restore
	// rollback detector catches the regression.
	samples := []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "counter-0", InstanceID: "inst-1", AckedSeq: 1, ObservedMaxSeq: 1, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:02Z", Pod: "counter-0", InstanceID: "inst-1", AckedSeq: 2, ObservedMaxSeq: 2, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:03Z", Pod: "counter-0", InstanceID: "inst-1", AckedSeq: 3, ObservedMaxSeq: 3, Phase: "migrating"},
		// First restore on replacement pod advances state to 4 and 5:
		{TS: "2026-09-29T10:00:06Z", Pod: "counter-1", InstanceID: "inst-1", AckedSeq: 4, ObservedMaxSeq: 4, Phase: "post-restore"},
		{TS: "2026-09-29T10:00:07Z", Pod: "counter-1", InstanceID: "inst-1", AckedSeq: 5, ObservedMaxSeq: 5, Phase: "post-restore"},
		// Second restore from the same checkpoint rolls back observedMaxSeq to 3:
		{TS: "2026-09-29T10:00:09Z", Pod: "counter-2", InstanceID: "inst-1", ObservedMaxSeq: 3, Phase: "post-restore"},
		// Subsequent write advances to 6:
		{TS: "2026-09-29T10:00:10Z", Pod: "counter-2", InstanceID: "inst-1", AckedSeq: 6, ObservedMaxSeq: 6, Phase: "post-restore"},
	}

	res := Analyze("counter", samples)
	if res.Pass {
		t.Fatalf("expected double-restore rollback to fail, but passed: %+v", res)
	}
	if res.SequenceRollbacks != 1 || res.DoubleRestoreCount != 1 {
		t.Fatalf("expected SequenceRollbacks=1 and DoubleRestoreCount=1, got %+v", res)
	}
	if len(res.LostAckedWrites) != 0 || res.EmptySourceReads != 0 {
		t.Fatalf("expected LostAckedWrites=[] and EmptySourceReads=0 when final seq advances past rollback, got %+v", res)
	}
	if !strings.Contains(res.Detail, "double-restore sequence rollback") {
		t.Fatalf("expected detail to mention double-restore sequence rollback, got %q", res.Detail)
	}
}

func TestAnalyze_MissingMigratingPhase(t *testing.T) {
	// If pre-checkpoint and post-restore samples exist but the background migrating
	// probe died without recording any migrating samples, Analyze must fail.
	samples := []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 1, ObservedMaxSeq: 1, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:02Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 2, ObservedMaxSeq: 2, Phase: "pre-checkpoint"},
		{TS: "2026-09-29T10:00:05Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 3, ObservedMaxSeq: 3, Phase: "post-restore"},
		{TS: "2026-09-29T10:00:06Z", Pod: "redis-0", InstanceID: "run-a", AckedSeq: 4, ObservedMaxSeq: 4, Phase: "post-restore"},
	}

	res := Analyze("redis", samples)
	if res.Pass {
		t.Fatalf("expected missing migrating phase to fail Analyze, got %+v", res)
	}
	if !strings.Contains(res.Detail, "missing migrating-phase continuity samples") {
		t.Fatalf("expected detail to mention missing migrating-phase continuity samples, got %q", res.Detail)
	}
}

func TestAppendCheckToRun(t *testing.T) {
	dir := t.TempDir()
	res := Analyze("postgres", []Sample{
		{TS: "2026-09-29T10:00:01Z", Pod: "pg-0", InstanceID: "pg-1", AckedSeq: 1, ObservedMaxSeq: 201},
	})
	if err := AppendCheckToRun(dir, res.ToCheck("")); err != nil {
		t.Fatalf("AppendCheckToRun failed: %v", err)
	}
	loaded, err := LoadSamples(filepath.Join(dir, "nonexistent.ndjson"))
	if err == nil || len(loaded) != 0 {
		t.Fatalf("expected error loading nonexistent file")
	}
}

func TestRunLiveProbe_Adapters(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "counter.state"), []byte("uuid-counter-1|10\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "redis.dbsize"), []byte("50000\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, "pg.seed"), []byte("200\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	fakeRedis := filepath.Join(dir, "redis-cli")
	redisScript := `#!/usr/bin/env bash
case "$1" in
  INFO)
    echo "run_id:redis-run-123"
    ;;
  DBSIZE)
    cat "` + stateDir + `/redis.dbsize"
    ;;
  GET)
    if [[ -f "` + stateDir + `/redis.seq" ]]; then
      cat "` + stateDir + `/redis.seq"
    fi
    ;;
  SET)
    echo "$3" > "` + stateDir + `/redis.seq"
    echo "OK"
    ;;
esac
`
	if err := os.WriteFile(fakeRedis, []byte(redisScript), 0o755); err != nil {
		t.Fatal(err)
	}

	fakePsql := filepath.Join(dir, "psql")
	psqlScript := `#!/usr/bin/env bash
query="${*: -1}"
if [[ "${query}" == *"pg_postmaster_start_time()"* ]]; then
  echo "1700000001"
elif [[ "${query}" == *"to_regclass('public.mig_continuity_seq')"* ]]; then
  if [[ -f "` + stateDir + `/pg.table" ]]; then
    echo "1"
  else
    echo "0"
  fi
elif [[ "${query}" == *"to_regclass('public.mig_seq')"* ]]; then
  cat "` + stateDir + `/pg.seed"
elif [[ "${query}" == *"CREATE TABLE IF NOT EXISTS mig_continuity_seq"* ]]; then
  touch "` + stateDir + `/pg.table"
elif [[ "${query}" == *"SELECT COALESCE((SELECT MAX(seq) FROM mig_continuity_seq), 0)"* ]]; then
  if [[ -f "` + stateDir + `/pg.seq" ]]; then
    cat "` + stateDir + `/pg.seq"
  else
    echo "0"
  fi
elif [[ "${query}" == *"INSERT INTO mig_continuity_seq"* ]]; then
  seq_val="$(echo "${query}" | sed -n 's/.*VALUES (\([0-9]*\),.*/\1/p')"
  echo "${seq_val}" > "` + stateDir + `/pg.seq"
fi
`
	if err := os.WriteFile(fakePsql, []byte(psqlScript), 0o755); err != nil {
		t.Fatal(err)
	}

	fakeKubectl := filepath.Join(dir, "fake-kubectl")
	kubectlScript := `#!/usr/bin/env bash
export PATH="` + dir + `:${PATH}"
if [[ "$1" == "get" && "$2" == "pods" ]]; then
  printf "test-pod-0"
  exit 0
fi
if [[ "$1" == "exec" ]]; then
  shift
  while [[ $# -gt 0 && "$1" != "--" ]]; do
    shift
  done
  shift # skip --
  shift # skip sh
  shift # skip -c
  script="${1//\/tmp\//` + stateDir + `/}"
  exec sh -c "${script}"
fi
`
	if err := os.WriteFile(fakeKubectl, []byte(kubectlScript), 0o755); err != nil {
		t.Fatal(err)
	}

	for _, wl := range []string{"counter", "redis", "postgres"} {
		tracePath := filepath.Join(dir, wl+".ndjson")
		ctx := context.Background()
		for _, phase := range []string{"pre-checkpoint", "migrating", "post-restore"} {
			err := RunLiveProbe(ctx, LiveProbeOptions{
				KubectlBin:  fakeKubectl,
				Namespace:   "default",
				Workload:    wl,
				PodSelector: "app=" + wl,
				OutPath:     tracePath,
				Interval:    time.Millisecond,
				Steps:       1,
				Phase:       phase,
			})
			if err != nil {
				t.Fatalf("RunLiveProbe(%s, %s) failed: %v", wl, phase, err)
			}
		}
		samples, err := LoadSamples(tracePath)
		if err != nil {
			t.Fatalf("LoadSamples(%s) failed: %v", wl, err)
		}
		res := Analyze(wl, samples)
		if !res.Pass {
			t.Fatalf("expected live probe trace for %s to pass Analyze, got detail=%q violations=%v", wl, res.Detail, res.Violations)
		}
		if res.AckedWrites != 3 || res.MaxAckedSeq != 3 || res.FinalObservedMaxSeq != 3 {
			t.Fatalf("unexpected sequence progression for %s: %+v", wl, res)
		}
	}
}
