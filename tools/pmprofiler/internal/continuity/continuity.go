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

// Package continuity implements the write-continuity probe and trace analyzer
// for GKE Live Pod Migration (Issue #103).
//
// It detects three stateful failure modes that a pre-checkpoint static token
// check cannot catch:
//  1. Lost acknowledged writes: the source continues acknowledging writes after
//     the checkpoint instant, and the restored instance does not have them.
//  2. Empty-source serving: the source container is stopped after checkpoint,
//     restarted by kubelet with empty state, and answers clients before eviction.
//  3. Double restore / sequence rollback: a replacement is restored twice from
//     the same checkpoint (or rolls back), losing writes it already acknowledged.
package continuity

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/analyze"
)

// Sample represents one observation recorded by the continuous write/read probe.
type Sample struct {
	TS             string  `json:"ts"`
	Op             string  `json:"op,omitempty"`             // "step" or "final"
	Pod            string  `json:"pod,omitempty"`            // serving pod name
	InstanceID     string  `json:"instanceId,omitempty"`     // e.g. redis run_id, counter UUID, postgres postmaster start time
	AckedSeq       int64   `json:"ackedSeq,omitempty"`       // >0 when this step acknowledged writing sequence N
	ObservedMaxSeq int64   `json:"observedMaxSeq"`           // highest sequence number observed on the serving instance prior to/after write
	MissingSeqs    []int64 `json:"missingSeqs,omitempty"`    // acknowledged sequence numbers missing from the serving instance
	Phase          string  `json:"phase,omitempty"`          // optional: "pre-checkpoint", "migrating", "post-restore"
	Error          string  `json:"error,omitempty"`          // transient error during blackout (not an acknowledgment)
}

// Result summarizes the write-continuity analysis over a sequence of samples.
type Result struct {
	Group               string   `json:"group,omitempty"`
	TotalSamples        int      `json:"totalSamples"`
	SuccessfulSamples   int      `json:"successfulSamples"`
	AckedWrites         int64    `json:"ackedWrites"`
	MaxAckedSeq         int64    `json:"maxAckedSeq"`
	FinalObservedMaxSeq int64    `json:"finalObservedMaxSeq"`
	DistinctInstances   []string `json:"distinctInstances,omitempty"`
	LostAckedWrites     []int64  `json:"lostAckedWrites,omitempty"`
	EmptySourceReads    int      `json:"emptySourceReads"`
	SequenceRollbacks   int      `json:"sequenceRollbacks"`
	DoubleRestoreCount  int      `json:"doubleRestoreCount"`
	Violations          []string `json:"violations,omitempty"`
	Pass                bool     `json:"pass"`
	Detail              string   `json:"detail"`
}

// LoadSamples reads an NDJSON file of Continuity Samples.
func LoadSamples(path string) ([]Sample, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var out []Sample
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var s Sample
		if err := json.Unmarshal([]byte(line), &s); err != nil {
			return nil, fmt.Errorf("parse continuity sample: %w", err)
		}
		out = append(out, s)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// Analyze inspects a chronological slice of continuity samples and flags:
//   - lost acknowledged writes after restore,
//   - reads served by an instance with empty state after writes were acknowledged,
//   - sequence rollbacks / double restores.
func Analyze(group string, samples []Sample) Result {
	res := Result{
		Group:        group,
		TotalSamples: len(samples),
	}
	if len(samples) == 0 {
		res.Pass = false
		res.Violations = []string{"no continuity samples recorded"}
		res.Detail = "no continuity samples recorded"
		return res
	}

	ackedSet := make(map[int64]bool)
	seenInstances := make(map[string]bool)
	var (
		highWaterSeq        int64
		checkpointCeiling   int64
		postRestoreAdvanced bool
		lastGoodSample      *Sample
		hasPreCheckpoint    bool
		hasPostRestore      bool
		migratingSamples    int
	)

	for i := range samples {
		s := &samples[i]
		switch s.Phase {
		case "pre-checkpoint":
			hasPreCheckpoint = true
		case "migrating":
			migratingSamples++
		case "post-restore":
			hasPostRestore = true
		}
		if s.Error != "" {
			continue
		}
		res.SuccessfulSamples++
		if s.InstanceID != "" && !seenInstances[s.InstanceID] {
			seenInstances[s.InstanceID] = true
			res.DistinctInstances = append(res.DistinctInstances, s.InstanceID)
		}

		// 1. Empty-source serving check:
		// Once at least one write has been acknowledged (res.MaxAckedSeq > 0 or highWaterSeq > 0),
		// any sample served by an instance reporting ObservedMaxSeq == 0 indicates an instance
		// started fresh with empty state (e.g., source container restarted after checkpoint
		// before eviction, or unverified cold start answering traffic).
		if (res.MaxAckedSeq > 0 || highWaterSeq > 0) && s.ObservedMaxSeq == 0 {
			res.EmptySourceReads++
			res.Violations = append(res.Violations, fmt.Sprintf(
				"empty-source serving at %s (pod=%s, instanceId=%s): observedMaxSeq=0 after seq=%d was already acknowledged",
				s.TS, s.Pod, s.InstanceID, maxInt64(res.MaxAckedSeq, highWaterSeq),
			))
		} else if s.ObservedMaxSeq > 0 && s.ObservedMaxSeq < highWaterSeq {
			// 2. Sequence rollback / double restore check:
			// ObservedMaxSeq regressed below a previously observed or acknowledged sequence.
			res.SequenceRollbacks++
			if postRestoreAdvanced || s.Phase == "post-restore" {
				res.DoubleRestoreCount++
				res.Violations = append(res.Violations, fmt.Sprintf(
					"double-restore sequence rollback at %s (pod=%s, instanceId=%s): observedMaxSeq regressed %d -> %d",
					s.TS, s.Pod, s.InstanceID, highWaterSeq, s.ObservedMaxSeq,
				))
			} else {
				res.Violations = append(res.Violations, fmt.Sprintf(
					"sequence rollback at %s (pod=%s, instanceId=%s): observedMaxSeq regressed %d -> %d",
					s.TS, s.Pod, s.InstanceID, highWaterSeq, s.ObservedMaxSeq,
				))
			}
		}

		if s.Phase != "post-restore" && s.ObservedMaxSeq > checkpointCeiling {
			checkpointCeiling = s.ObservedMaxSeq
		}
		if s.Phase == "post-restore" && checkpointCeiling > 0 && s.ObservedMaxSeq > checkpointCeiling {
			postRestoreAdvanced = true
		}

		if s.ObservedMaxSeq > highWaterSeq {
			highWaterSeq = s.ObservedMaxSeq
		}
		if s.AckedSeq > 0 {
			if !ackedSet[s.AckedSeq] {
				ackedSet[s.AckedSeq] = true
				res.AckedWrites++
			}
			if s.AckedSeq > res.MaxAckedSeq {
				res.MaxAckedSeq = s.AckedSeq
			}
			if s.AckedSeq > highWaterSeq {
				highWaterSeq = s.AckedSeq
			}
			if s.Phase == "post-restore" && checkpointCeiling > 0 && s.AckedSeq > checkpointCeiling {
				postRestoreAdvanced = true
			}
		}
		lastGoodSample = s
	}

	if res.SuccessfulSamples == 0 || lastGoodSample == nil {
		res.Pass = false
		res.Violations = append(res.Violations, "0 successful continuity samples recorded")
		res.Detail = strings.Join(res.Violations, "; ")
		return res
	}

	if hasPreCheckpoint && hasPostRestore && migratingSamples == 0 {
		res.Violations = append(res.Violations,
			"missing migrating-phase continuity samples: pre-checkpoint and post-restore phases present but 0 migrating samples recorded")
	}

	res.FinalObservedMaxSeq = lastGoodSample.ObservedMaxSeq

	// 3. Lost acknowledged writes check:
	// Every acknowledged sequence must survive in the final post-restore state.
	lostMap := make(map[int64]bool)
	for seq := range ackedSet {
		if seq > res.FinalObservedMaxSeq {
			lostMap[seq] = true
		}
	}
	for _, m := range lastGoodSample.MissingSeqs {
		if ackedSet[m] || m <= res.MaxAckedSeq {
			lostMap[m] = true
		}
	}
	if len(lostMap) > 0 {
		for seq := range lostMap {
			res.LostAckedWrites = append(res.LostAckedWrites, seq)
		}
		sort.Slice(res.LostAckedWrites, func(i, j int) bool {
			return res.LostAckedWrites[i] < res.LostAckedWrites[j]
		})
		res.Violations = append(res.Violations, fmt.Sprintf(
			"lost acknowledged writes: %d acked write(s) missing after restore (maxAckedSeq=%d, finalObservedMaxSeq=%d, missing=%v)",
			len(res.LostAckedWrites), res.MaxAckedSeq, res.FinalObservedMaxSeq, res.LostAckedWrites,
		))
	}

	res.Pass = len(res.Violations) == 0
	if res.Pass {
		res.Detail = fmt.Sprintf(
			"ackedWrites=%d (1..%d), finalObservedMaxSeq=%d, instances=%v, lostAcked=0, emptySource=0, rollbacks=0",
			res.AckedWrites, res.MaxAckedSeq, res.FinalObservedMaxSeq, res.DistinctInstances,
		)
	} else {
		res.Detail = strings.Join(res.Violations, "; ")
	}
	return res
}

// ToCheck converts a Continuity Result into a pmprofiler analyze.Check record.
func (r Result) ToCheck(customName string) analyze.Check {
	name := customName
	if name == "" {
		name = "write continuity (no lost acks, empty-source, or rollback)"
	}
	val := float64(r.AckedWrites - int64(len(r.LostAckedWrites)))
	if !r.Pass {
		val = 0
	}
	total := float64(r.AckedWrites)
	if total <= 0 {
		total = 1
		if r.Pass {
			val = 1
		}
	}
	return analyze.Check{
		TS:     time.Now().UTC().Format(time.RFC3339),
		Name:   name,
		Group:  r.Group,
		Pass:   r.Pass,
		Detail: r.Detail,
		Value:  val,
		Total:  total,
	}
}

// AppendCheckToRun writes the continuity Check to <runDir>/checks.ndjson.
func AppendCheckToRun(runDir string, c analyze.Check) error {
	f, err := os.OpenFile(filepath.Join(runDir, "checks.ndjson"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(f).Encode(struct {
		Kind string `json:"kind"`
		analyze.Check
	}{"check", c})
}

// LiveProbeOptions configures a live kubectl-exec write-continuity probe loop.
type LiveProbeOptions struct {
	KubectlBin  string
	Namespace   string
	Workload    string // "counter", "redis", "postgres"
	PodSelector string // e.g. "app=t2-counter"
	PodName     string // optional explicit pod name
	OutPath     string // NDJSON output path
	Interval    time.Duration
	Steps       int    // if >0, run a fixed number of steps and exit; otherwise run until ctx is canceled
	Phase       string // optional phase tag ("pre-checkpoint", "migrating", "post-restore")
	StartSeq    int64  // optional starting sequence number (0 = auto-discover from trace/target)
}

// RunLiveProbe executes monotonic sequence writes and reader-identity reads
// against a live pod in Kubernetes and appends each Sample to OutPath.
func RunLiveProbe(ctx context.Context, o LiveProbeOptions) error {
	if o.KubectlBin == "" {
		o.KubectlBin = "kubectl"
	}
	if o.Namespace == "" {
		o.Namespace = "default"
	}
	if o.Interval <= 0 {
		o.Interval = 250 * time.Millisecond
	}
	if o.OutPath == "" {
		return fmt.Errorf("--trace output path is required")
	}
	if err := os.MkdirAll(filepath.Dir(o.OutPath), 0o755); err != nil {
		return err
	}

	nextSeq := o.StartSeq
	if nextSeq <= 0 {
		if existing, err := LoadSamples(o.OutPath); err == nil {
			for _, s := range existing {
				if s.AckedSeq > nextSeq {
					nextSeq = s.AckedSeq
				}
				if s.ObservedMaxSeq > nextSeq {
					nextSeq = s.ObservedMaxSeq
				}
			}
		}
	}

	f, err := os.OpenFile(o.OutPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)

	stepCount := 0
	for {
		if o.Steps > 0 && stepCount >= o.Steps {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		stepCount++
		targetSeq := nextSeq + 1
		s := probeOneStep(ctx, o, targetSeq)
		if s.Error == "" && s.AckedSeq > nextSeq {
			nextSeq = s.AckedSeq
		}
		_ = enc.Encode(s)

		if o.Steps > 0 && stepCount >= o.Steps {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(o.Interval):
		}
	}
}

func probeOneStep(ctx context.Context, o LiveProbeOptions, nextSeq int64) Sample {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	pod := o.PodName
	if pod == "" && o.PodSelector != "" {
		out, err := execKubectl(ctx, o.KubectlBin, "get", "pods", "-n", o.Namespace,
			"-l", o.PodSelector, "--field-selector=status.phase=Running",
			"-o", "jsonpath={.items[0].metadata.name}")
		if err != nil || strings.TrimSpace(out) == "" {
			return Sample{TS: now, Op: "step", Phase: o.Phase, Error: "no active Running pod"}
		}
		pod = strings.TrimSpace(out)
	}

	switch o.Workload {
	case "counter":
		script := fmt.Sprintf(`
if [ ! -f /tmp/counter.state ]; then
  echo "EMPTY|0|0"
  exit 0
fi
STATE=$(cat /tmp/counter.state | tr -d '[:space:]')
INST=${STATE%%%%|*}
PREV=0
if [ -f /tmp/continuity.seq ]; then
  PREV=$(cat /tmp/continuity.seq | tr -d '[:space:]')
fi
echo "%d" > /tmp/continuity.seq.tmp && mv /tmp/continuity.seq.tmp /tmp/continuity.seq
echo "${INST}|${PREV}|%d"
`, nextSeq, nextSeq)
		out, err := execKubectl(ctx, o.KubectlBin, "exec", "-n", o.Namespace, pod, "--", "sh", "-c", script)
		if err != nil {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: err.Error()}
		}
		parts := strings.Split(strings.TrimSpace(out), "|")
		if len(parts) != 3 {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: "malformed counter probe output"}
		}
		if parts[0] == "EMPTY" {
			return Sample{TS: now, Op: "step", Pod: pod, InstanceID: "empty", ObservedMaxSeq: 0, Phase: o.Phase}
		}
		prevSeq, _ := strconv.ParseInt(parts[1], 10, 64)
		ackedSeq, _ := strconv.ParseInt(parts[2], 10, 64)
		obs := ackedSeq
		if prevSeq > 0 && prevSeq+1 < ackedSeq {
			obs = prevSeq
		}
		return Sample{
			TS:             now,
			Op:             "step",
			Pod:            pod,
			InstanceID:     parts[0],
			AckedSeq:       ackedSeq,
			ObservedMaxSeq: obs,
			Phase:          o.Phase,
		}

	case "redis":
		script := fmt.Sprintf(`
RUN_ID=$(redis-cli INFO server 2>/dev/null | grep '^run_id:' | cut -d: -f2 | tr -d '[:space:]')
DBSZ=$(redis-cli DBSIZE 2>/dev/null | tr -dc '0-9')
if [ "${DBSZ:-0}" -eq 0 ]; then
  echo "${RUN_ID}|0|0"
  exit 0
fi
PREV=$(redis-cli GET mig_continuity_seq 2>/dev/null | tr -dc '0-9')
PREV=${PREV:-0}
redis-cli SET mig_continuity_seq %d >/dev/null 2>&1 || exit 1
echo "${RUN_ID}|${PREV}|%d"
`, nextSeq, nextSeq)
		out, err := execKubectl(ctx, o.KubectlBin, "exec", "-n", o.Namespace, pod, "--", "sh", "-c", script)
		if err != nil {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: err.Error()}
		}
		parts := strings.Split(strings.TrimSpace(out), "|")
		if len(parts) != 3 {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: "malformed redis probe output"}
		}
		prevSeq, _ := strconv.ParseInt(parts[1], 10, 64)
		ackedSeq, _ := strconv.ParseInt(parts[2], 10, 64)
		if ackedSeq == 0 {
			return Sample{TS: now, Op: "step", Pod: pod, InstanceID: parts[0], ObservedMaxSeq: 0, Phase: o.Phase}
		}
		obs := ackedSeq
		if prevSeq > 0 && prevSeq+1 < ackedSeq {
			obs = prevSeq
		}
		return Sample{
			TS:             now,
			Op:             "step",
			Pod:            pod,
			InstanceID:     parts[0],
			AckedSeq:       ackedSeq,
			ObservedMaxSeq: obs,
			Phase:          o.Phase,
		}

	case "postgres":
		script := fmt.Sprintf(`
INST=$(psql -U postgres -d postgres -tAc "SELECT COALESCE(CAST(EXTRACT(EPOCH FROM pg_postmaster_start_time()) AS BIGINT)::TEXT, 'pg');" 2>/dev/null | tr -d '[:space:]')
if [ -z "${INST}" ]; then
  exit 1
fi
HAS_CONT=$(psql -U postgres -d postgres -tAc "SELECT CASE WHEN to_regclass('public.mig_continuity_seq') IS NULL THEN 0 ELSE 1 END;" 2>/dev/null | tr -d '[:space:]')
if [ "${HAS_CONT:-0}" -eq 0 ]; then
  HAS_SEED=$(psql -U postgres -d postgres -tAc "SELECT CASE WHEN to_regclass('public.mig_seq') IS NULL THEN 0 ELSE COALESCE((SELECT COUNT(*) FROM mig_seq), 0) END;" 2>/dev/null | tr -d '[:space:]')
  if [ "%d" -gt 1 ] || [ "${HAS_SEED:-0}" -le 0 ]; then
    echo "${INST}|0|0"
    exit 0
  fi
  psql -U postgres -d postgres -tAc "CREATE TABLE IF NOT EXISTS mig_continuity_seq (seq BIGINT PRIMARY KEY, instance TEXT NOT NULL, ts TIMESTAMPTZ DEFAULT now());" >/dev/null 2>&1 || exit 1
fi
PREV=$(psql -U postgres -d postgres -tAc "SELECT COALESCE((SELECT MAX(seq) FROM mig_continuity_seq), 0);" 2>/dev/null | tr -d '[:space:]')
PREV=${PREV:-0}
if [ "%d" -gt 1 ] && [ "${PREV}" -le 0 ]; then
  echo "${INST}|0|0"
  exit 0
fi
psql -U postgres -d postgres -tAc "INSERT INTO mig_continuity_seq (seq, instance) VALUES (%d, '${INST}') ON CONFLICT (seq) DO NOTHING;" >/dev/null 2>&1 || exit 1
POST=$(psql -U postgres -d postgres -tAc "SELECT COALESCE((SELECT MAX(seq) FROM mig_continuity_seq), 0);" 2>/dev/null | tr -d '[:space:]')
echo "${INST}|${PREV}|${POST:-0}"
`, nextSeq, nextSeq, nextSeq)
		out, err := execKubectl(ctx, o.KubectlBin, "exec", "-n", o.Namespace, pod, "--", "sh", "-c", script)
		if err != nil {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: err.Error()}
		}
		parts := strings.Split(strings.TrimSpace(out), "|")
		if len(parts) != 3 {
			return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: "malformed postgres probe output"}
		}
		prevMax, _ := strconv.ParseInt(parts[1], 10, 64)
		afterMax, _ := strconv.ParseInt(parts[2], 10, 64)
		if afterMax == 0 {
			return Sample{TS: now, Op: "step", Pod: pod, InstanceID: parts[0], ObservedMaxSeq: 0, Phase: o.Phase}
		}
		obs := afterMax
		if prevMax > 0 && prevMax+1 < nextSeq {
			obs = prevMax
		}
		return Sample{
			TS:             now,
			Op:             "step",
			Pod:            pod,
			InstanceID:     parts[0],
			AckedSeq:       nextSeq,
			ObservedMaxSeq: obs,
			Phase:          o.Phase,
		}
	default:
		return Sample{TS: now, Op: "step", Pod: pod, Phase: o.Phase, Error: "unsupported workload " + o.Workload}
	}
}

func execKubectl(ctx context.Context, bin string, args ...string) (string, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cctx, bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w (%s)", err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
