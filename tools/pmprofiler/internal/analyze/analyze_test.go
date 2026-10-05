package analyze

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var t0 = time.Date(2026, 8, 21, 10, 0, 0, 0, time.UTC)

func ts(offset time.Duration) string {
	return t0.Add(offset).Format(time.RFC3339Nano)
}

func rec(t *testing.T, w *os.File, at time.Duration, typ, gvr string, obj map[string]any) {
	t.Helper()
	raw, err := json.Marshal(obj)
	if err != nil {
		t.Fatal(err)
	}
	line, _ := json.Marshal(map[string]any{
		"ts": ts(at), "type": typ, "gvr": gvr, "obj": json.RawMessage(raw),
	})
	fmt.Fprintln(w, string(line))
}

func pmjObj(name, pod, uid, phase, snapRef string) map[string]any {
	status := map[string]any{}
	if phase != "" {
		status["phase"] = phase
	}
	if snapRef != "" {
		status["snapshotRef"] = snapRef
	}
	return map[string]any{
		"metadata": map[string]any{"name": name, "creationTimestamp": ts(0)},
		"spec": map[string]any{
			"podRef": map[string]any{"name": pod}, "targetPodUID": uid,
		},
		"status": status,
	}
}

func podObj(name, uid string, created time.Duration, extra func(map[string]any)) map[string]any {
	o := map[string]any{
		"metadata": map[string]any{
			"name": name, "uid": uid, "creationTimestamp": ts(created),
			"labels": map[string]any{"app": "web", "pod-migration.gke.io/enabled": "true"},
		},
		"spec":   map[string]any{"nodeName": "node-a"},
		"status": map[string]any{},
	}
	if extra != nil {
		extra(o)
	}
	return o
}

func writeTestRun(t *testing.T) string {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const snapGVR = "podsnapshots.v1alpha1.podsnapshot.gke.io"
	const podGVR = "pods.v1"

	// Source pod exists before the PMJ.
	rec(t, f, -time.Minute, "list", podGVR, podObj("web-0", "uid-src", -time.Minute, func(o map[string]any) {
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(-50 * time.Second)},
		}}
	}))
	// PMJ lifecycle.
	rec(t, f, 0, "add", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Pending", ""))
	rec(t, f, 2*time.Second, "update", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Snapshotting", ""))
	// Snapshot becomes ready at +15s.
	rec(t, f, 15*time.Second, "update", snapGVR, map[string]any{
		"metadata": map[string]any{"name": "snap-web-0", "creationTimestamp": ts(2 * time.Second)},
		"spec":     map[string]any{"podRef": map[string]any{"name": "web-0"}},
		"status": map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(15 * time.Second)},
		}},
	})
	rec(t, f, 16*time.Second, "update", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Evicting", "snap-web-0"))
	// Source deleted at +20s.
	rec(t, f, 20*time.Second, "update", podGVR, podObj("web-0", "uid-src", -time.Minute, func(o map[string]any) {
		o["metadata"].(map[string]any)["deletionTimestamp"] = ts(20 * time.Second)
	}))
	rec(t, f, 21*time.Second, "delete", podGVR, podObj("web-0", "uid-src", -time.Minute, nil))
	// Replacement created at +22s, explicitly signaled restored, Ready at
	// +30s. A generic snapshot-related annotation (restore-from) is present
	// too but must NOT be what marks the restore.
	rec(t, f, 22*time.Second, "add", podGVR, podObj("web-0", "uid-dst", 22*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{
			"podsnapshot.gke.io/restore-from": "snap-web-0",
			"pod-migrate.io/psengine-restore": "restored"}
	}))
	rec(t, f, 30*time.Second, "update", podGVR, podObj("web-0", "uid-dst", 22*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{
			"podsnapshot.gke.io/restore-from": "snap-web-0",
			"pod-migrate.io/psengine-restore": "restored"}
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(30 * time.Second)},
		}}
	}))
	rec(t, f, 31*time.Second, "update", pmjGVR, pmjObj("pmj-web-0", "web-0", "uid-src", "Succeeded", "snap-web-0"))
	// A warning event on the pod.
	rec(t, f, 18*time.Second, "add", "events.v1", map[string]any{
		"type": "Warning", "reason": "FailedScheduling", "message": "0/3 nodes available",
		"involvedObject": map[string]any{"kind": "Pod", "name": "web-0"},
		"lastTimestamp":  ts(18 * time.Second),
	})
	// Snapshot sizes.
	rec(t, f, 60*time.Second, "gcs", "", map[string]any{
		"totalBytes": int64(1500000000),
		"objects":    map[string]any{"gs://b/snap-web-0/mem.img": 1000000000},
	})

	// A wedged PMJ: never leaves Snapshotting, log ends 20 min later.
	rec(t, f, 40*time.Second, "add", pmjGVR, pmjObj("pmj-stuck-0", "stuck-0", "uid-s0", "Snapshotting", ""))
	rec(t, f, 20*time.Minute, "update", pmjGVR, pmjObj("pmj-stuck-0", "stuck-0", "uid-s0", "Snapshotting", ""))

	// Succeeded with a replacement but NO explicit restore signal either
	// way: "replaced", never claimed as a restore (and not a cold start —
	// nothing proves that either).
	rec(t, f, 0, "list", podGVR, podObj("cold-0", "uid-c-src", -time.Minute, nil))
	rec(t, f, 50*time.Second, "add", pmjGVR, pmjObj("pmj-cold-0", "cold-0", "uid-c-src", "Pending", ""))
	rec(t, f, 70*time.Second, "add", podGVR, podObj("cold-0", "uid-c-dst", 70*time.Second, nil))
	rec(t, f, 80*time.Second, "update", pmjGVR, pmjObj("pmj-cold-0", "cold-0", "uid-c-src", "Succeeded", ""))

	// Checks + series.
	cf, err := os.Create(filepath.Join(dir, "checks.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(cf, `{"kind":"check","ts":"2026-08-21T10:01:00Z","name":"nonce survived","group":"redis","pass":true,"detail":"50/50"}`)
	fmt.Fprintln(cf, `{"kind":"series","name":"p99-latency","unit":"ms","points":[[1755770400,1.2],[1755770460,0.9]]}`)
	cf.Close()
	return dir
}

func TestAnalyze(t *testing.T) {
	dir := writeTestRun(t)
	run, err := Analyze(Options{RunDir: dir, Scenario: "test", WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Migrations) != 3 {
		t.Fatalf("want 3 migrations, got %d", len(run.Migrations))
	}
	byPMJ := map[string]Migration{}
	for _, m := range run.Migrations {
		byPMJ[m.PMJ] = m
	}

	m := byPMJ["pmj-web-0"]
	if m.Outcome != OutcomeRestored {
		t.Errorf("web-0 outcome = %s, want restored", m.Outcome)
	}
	if m.RestoreSignal != "psengine-restore:restored" {
		t.Errorf("web-0 restoreSignal = %q, want psengine-restore:restored", m.RestoreSignal)
	}
	if m.DstPod != "web-0" {
		t.Errorf("web-0 dstPod = %q, want web-0", m.DstPod)
	}
	if m.SnapReadyS != 15 {
		t.Errorf("snapReadyS = %v, want 15 (from snapshot condition ltt)", m.SnapReadyS)
	}
	if m.EvictedS != 20 {
		t.Errorf("evictedS = %v, want 20", m.EvictedS)
	}
	if m.E2ES != 30 {
		t.Errorf("e2eS = %v, want 30", m.E2ES)
	}
	if m.DowntimeS != 10 {
		t.Errorf("downtimeS = %v, want 10", m.DowntimeS)
	}
	if m.SnapshotBytes != 1000000000 {
		t.Errorf("snapshotBytes = %d, want 1000000000", m.SnapshotBytes)
	}
	if m.SrcNode != "node-a" || m.DstNode != "node-b" {
		t.Errorf("nodes = %s → %s, want node-a → node-b", m.SrcNode, m.DstNode)
	}
	if m.App != "web" {
		t.Errorf("app = %q, want web", m.App)
	}
	if len(m.Warnings) != 1 {
		t.Errorf("warnings = %v, want 1 joined event", m.Warnings)
	}

	if got := byPMJ["pmj-stuck-0"].Outcome; got != OutcomeWedged {
		t.Errorf("stuck-0 outcome = %s, want wedged", got)
	}
	if got := byPMJ["pmj-cold-0"]; got.Outcome != OutcomeReplaced || got.RestoreSignal != "absent" {
		t.Errorf("cold-0 = %s/%q, want replaced with restoreSignal absent (no explicit engine signal)",
			got.Outcome, got.RestoreSignal)
	}

	if run.Outcomes[OutcomeRestored] != 1 || run.Outcomes[OutcomeWedged] != 1 || run.Outcomes[OutcomeReplaced] != 1 {
		t.Errorf("outcomes = %v", run.Outcomes)
	}
	if run.Populations["measured"] != 3 {
		t.Errorf("populations = %v, want 3 measured", run.Populations)
	}
	if len(run.Checks) != 1 || !run.Checks[0].Pass {
		t.Errorf("checks = %+v", run.Checks)
	}
	seriesNames := map[string]bool{}
	for _, s := range run.Series {
		seriesNames[s.Name] = true
	}
	if !seriesNames["p99-latency"] || !seriesNames["gcs-total-bytes"] {
		t.Errorf("series = %v", seriesNames)
	}
	if run.Stats["e2eS"].N != 1 || run.Stats["e2eS"].P50 != 30 {
		t.Errorf("e2e stats = %+v", run.Stats["e2eS"])
	}
	// Failures should include the wedge but neither the restore nor the
	// unproven-but-completed "replaced" row.
	kinds := map[string]bool{}
	for _, f := range run.Failures {
		kinds[f.Kind] = true
	}
	if !kinds[OutcomeWedged] || kinds[OutcomeReplaced] || kinds[OutcomeRestored] {
		t.Errorf("failure kinds = %v", kinds)
	}
}

// pmjObjAt is pmjObj with an explicit UID and creationTimestamp — needed to
// model PMJ incarnations (the benchmark GCs and recreates same-name PMJs).
func pmjObjAt(name, pod, uid, phase, objUID string, created time.Duration) map[string]any {
	o := pmjObj(name, pod, uid, phase, "")
	md := o["metadata"].(map[string]any)
	md["uid"] = objUID
	md["creationTimestamp"] = ts(created)
	return o
}

// TestPMJRecreatedIncarnations: a GC'd-and-recreated same-name PMJ must
// yield one Migration per incarnation. Merging them regressed the phase and
// misreported one wedged migration (live defect on the sxs-gvisor run).
func TestPMJRecreatedIncarnations(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	rec(t, f, 10*time.Second, "add", pmjGVR, pmjObjAt("pmj-r", "r-0", "uid-r1", "Succeeded", "inc-1", 0))
	rec(t, f, 60*time.Second, "delete", pmjGVR, pmjObjAt("pmj-r", "r-0", "uid-r1", "Succeeded", "inc-1", 0))
	rec(t, f, 2*time.Minute, "add", pmjGVR, pmjObjAt("pmj-r", "r-0", "uid-r2", "Snapshotting", "inc-2", 2*time.Minute))
	rec(t, f, 3*time.Minute, "update", pmjGVR, pmjObjAt("pmj-r", "r-0", "uid-r2", "Snapshotting", "inc-2", 2*time.Minute))
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Migrations) != 2 {
		t.Fatalf("want 2 migrations (one per incarnation), got %d: %+v", len(run.Migrations), run.Migrations)
	}
	phases := map[string]bool{}
	for _, m := range run.Migrations {
		if m.PMJ != "pmj-r" {
			t.Errorf("pmj name = %q, want pmj-r", m.PMJ)
		}
		phases[m.Phase] = true
	}
	if !phases["Succeeded"] || !phases["Snapshotting"] {
		t.Errorf("incarnation phases merged: %v", phases)
	}
	if run.Outcomes[OutcomeInFlight] != 1 {
		t.Errorf("second incarnation (progressing 1m before log end) should be in-flight: %v", run.Outcomes)
	}
}

// TestWedgeMeasuresStall: wedged means no phase PROGRESS within the
// threshold — a long-running PMJ that is still transitioning is in-flight.
func TestWedgeMeasuresStall(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	rec(t, f, 0, "add", pmjGVR, pmjObj("pmj-slow", "slow-0", "uid-slow", "Pending", ""))
	rec(t, f, 9*time.Minute, "update", pmjGVR, pmjObj("pmj-slow", "slow-0", "uid-slow", "Snapshotting", ""))
	rec(t, f, 18*time.Minute, "update", pmjGVR, pmjObj("pmj-slow", "slow-0", "uid-slow", "Evicting", ""))
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Migrations) != 1 || run.Migrations[0].Outcome != OutcomeInFlight {
		t.Fatalf("18m-old but progressing PMJ must be in-flight, got %+v", run.Migrations)
	}
}

// TestEvictionWindowRejectsTeardown: a source-pod deletion long after the
// PMJ's terminal phase (the benchmark's end-of-run teardown sweep) must not
// be read as the eviction (it produced ~2000s evictedS tails).
func TestEvictionWindowRejectsTeardown(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const podGVR = "pods.v1"
	rec(t, f, -time.Minute, "list", podGVR, podObj("tear-0", "uid-t-src", -time.Minute, nil))
	rec(t, f, 0, "add", pmjGVR, pmjObj("pmj-tear-0", "tear-0", "uid-t-src", "Pending", ""))
	rec(t, f, 30*time.Second, "update", pmjGVR, pmjObj("pmj-tear-0", "tear-0", "uid-t-src", "Succeeded", ""))
	// Teardown deletes the pod 40 minutes later.
	rec(t, f, 40*time.Minute, "update", podGVR, podObj("tear-0", "uid-t-src", -time.Minute, func(o map[string]any) {
		o["metadata"].(map[string]any)["deletionTimestamp"] = ts(40 * time.Minute)
	}))
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	m := run.Migrations[0]
	if m.EvictedS != -1 || m.TSrcDeleted != "" {
		t.Errorf("teardown deletion leaked into eviction: evictedS=%v tSrcDeleted=%q", m.EvictedS, m.TSrcDeleted)
	}
}

// TestNegativeGapClampsToZero: replacement Ready BEFORE the source went
// away = zero downtime, not a dropped negative sample (and never the -1
// unknown sentinel).
func TestNegativeGapClampsToZero(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const podGVR = "pods.v1"
	rec(t, f, -time.Minute, "list", podGVR, podObj("olap-0", "uid-o-src", -time.Minute, nil))
	rec(t, f, 0, "add", pmjGVR, pmjObj("pmj-olap-0", "olap-0", "uid-o-src", "Pending", ""))
	// Replacement is up and Ready at +10s while the source lives until +25s.
	rec(t, f, 2*time.Second, "add", podGVR, podObj("olap-0", "uid-o-dst", 2*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{"pod-migrate.io/psengine-restore": "restored"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(10 * time.Second)},
		}}
	}))
	rec(t, f, 25*time.Second, "update", podGVR, podObj("olap-0", "uid-o-src", -time.Minute, func(o map[string]any) {
		o["metadata"].(map[string]any)["deletionTimestamp"] = ts(25 * time.Second)
	}))
	rec(t, f, 30*time.Second, "update", pmjGVR, pmjObj("pmj-olap-0", "olap-0", "uid-o-src", "Succeeded", ""))
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	m := run.Migrations[0]
	if m.Outcome != OutcomeRestored {
		t.Fatalf("outcome = %s, want restored", m.Outcome)
	}
	if m.DowntimeS != 0 {
		t.Errorf("downtimeS = %v, want 0 (Ready before source deletion)", m.DowntimeS)
	}
}

// TestPreCollectionPopulation: objects created before the collector started
// cannot be reconstructed (their pods were never watched) and must be
// bucketed out of the headline outcomes. 8 such stubs misread as
// no-replacement failures on the real sxs runs.
func TestPreCollectionPopulation(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "meta.json"),
		[]byte(`{"startedAt":"`+ts(0)+`"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	rec(t, f, time.Second, "list", pmjGVR, pmjObjAt("pmj-old", "old-0", "uid-old", "Succeeded", "inc-old", -10*time.Minute))
	rec(t, f, 5*time.Second, "add", pmjGVR, pmjObjAt("pmj-new", "new-0", "uid-new", "Succeeded", "inc-new", 5*time.Second))
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if run.Populations[PopPreCollection] != 1 || run.Populations["measured"] != 1 {
		t.Fatalf("populations = %v, want 1 pre-collection + 1 measured", run.Populations)
	}
	total := 0
	for _, c := range run.Outcomes {
		total += c
	}
	if total != 1 {
		t.Errorf("headline outcomes must exclude pre-collection stubs: %v", run.Outcomes)
	}
	for _, m := range run.Migrations {
		if m.PMJ == "pmj-old" && m.Population != PopPreCollection {
			t.Errorf("pmj-old population = %q, want %q", m.Population, PopPreCollection)
		}
	}
	for _, fl := range run.Failures {
		if fl.PMJ == "pmj-old" {
			t.Error("pre-collection stub must not be a failure")
		}
	}
}

// TestStaleCRPopulation: a PodMigration CR that never showed any status
// during the run is a stale leftover, not a wedged migration (the
// "sxs-migration" phantom on the real gvisor run).
func TestStaleCRPopulation(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	rec(t, f, time.Second, "list", "podmigrations.v1alpha1.pod-migrate.io", map[string]any{
		"metadata": map[string]any{"name": "stale-pm", "creationTimestamp": ts(-time.Hour)},
		"spec":     map[string]any{"podName": "gone-0"},
	})
	// Something else keeps the log alive well past the wedge threshold.
	rec(t, f, 20*time.Minute, "add", "events.v1", map[string]any{
		"type": "Normal", "reason": "Tick", "message": "",
		"involvedObject": map[string]any{"kind": "Pod", "name": "x"},
	})
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if run.Populations[PopStaleCR] != 1 {
		t.Fatalf("populations = %v, want 1 stale-cr", run.Populations)
	}
	if run.Outcomes[OutcomeWedged] != 0 || len(run.Failures) != 0 {
		t.Errorf("stale CR leaked into headline: outcomes=%v failures=%v", run.Outcomes, run.Failures)
	}
}

// TestDistributionFilters: stats cover only migrations that completed with
// a replacement in the measured population; percentiles use nearest-rank
// (no small-n upward bias); negatives are sentinels, not samples.
func TestDistributionFilters(t *testing.T) {
	var ms []Migration
	for i := 1; i <= 10; i++ {
		ms = append(ms, Migration{Outcome: OutcomeRestored, E2ES: float64(i)})
	}
	ms = append(ms,
		Migration{Outcome: OutcomeWedged, E2ES: 2000},                          // teardown-poisoned wedge
		Migration{Outcome: OutcomeRestored, E2ES: -1},                          // unknown sentinel
		Migration{Outcome: OutcomeRestored, E2ES: 500, Population: PopStaleCR}, // excluded population
		Migration{Outcome: OutcomeNoDst, E2ES: 300},                            // no replacement
		Migration{Outcome: OutcomeInFlight, E2ES: 400},                         // not terminal
	)
	st := distribution(ms, func(m Migration) float64 { return m.E2ES })
	if st.N != 10 {
		t.Fatalf("n = %d, want 10 (wedged/in-flight/no-dst/excluded populations out)", st.N)
	}
	if st.Max != 10 {
		t.Errorf("max = %v, want 10", st.Max)
	}
	if st.P50 != 5 {
		t.Errorf("p50 = %v, want 5 (nearest-rank)", st.P50)
	}
	if st.P90 != 9 {
		t.Errorf("p90 = %v, want 9, not the max (small-n bias)", st.P90)
	}
}

// TestGKEPodMigrationJobNativeFields verifies reconstruction of gke-labs/pod-migration
// native PodMigrationJob fields (status.restoredPodName, status.restoredPodUID,
// status.evictingStartTime, status.conditions[type=="Restored"], SucceededWithoutRestore),
// while ignoring parent policy CRs (podmigrations.v1alpha1.podmigration.gke.io).
func TestGKEPodMigrationJobNativeFields(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmGVR = "podmigrations.v1alpha1.podmigration.gke.io"
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const podGVR = "pods.v1"

	// Parent policy CR (podmigrations.v1alpha1.podmigration.gke.io) must NOT be ingested as a per-pod migration.
	rec(t, f, 0, "add", pmGVR, map[string]any{
		"metadata": map[string]any{"name": "policy-counter", "creationTimestamp": ts(0)},
		"spec":     map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "counter"}}},
		"status":   map[string]any{"phase": "Active"},
	})

	// 1. Warm restore via PMJ status.conditions[type=="Restored"] (Reason=RestoreVerified)
	rec(t, f, -time.Minute, "list", podGVR, podObj("counter-0", "uid-cnt-src", -time.Minute, nil))
	rec(t, f, 10*time.Second, "add", podGVR, podObj("counter-0-restored", "uid-cnt-dst", 10*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(14 * time.Second)},
		}}
	}))
	pmjWarm := pmjObj("pmj-counter-0", "counter-0", "uid-cnt-src", "Succeeded", "snap-cnt-0")
	pmjWarm["status"].(map[string]any)["restoredPodName"] = "counter-0-restored"
	pmjWarm["status"].(map[string]any)["restoredPodUID"] = "uid-cnt-dst"
	pmjWarm["status"].(map[string]any)["evictingStartTime"] = ts(8 * time.Second)
	pmjWarm["status"].(map[string]any)["conditions"] = []any{
		map[string]any{
			"type":   "Restored",
			"status": "True",
			"reason": "RestoreVerified",
		},
	}
	rec(t, f, 15*time.Second, "add", pmjGVR, pmjWarm)

	// 2. Cold-start fallback (I9) via SucceededWithoutRestore (FallbackToColdStart)
	rec(t, f, -time.Minute, "list", podGVR, podObj("redis-0", "uid-red-src", -time.Minute, nil))
	rec(t, f, 20*time.Second, "add", podGVR, podObj("redis-0-fallback", "uid-red-dst", 20*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(25 * time.Second)},
		}}
	}))
	pmjCold := pmjObj("pmj-redis-0", "redis-0", "uid-red-src", "SucceededWithoutRestore", "snap-red-0")
	pmjCold["status"].(map[string]any)["restoredPodName"] = "redis-0-fallback"
	pmjCold["status"].(map[string]any)["restoredPodUID"] = "uid-red-dst"
	pmjCold["status"].(map[string]any)["conditions"] = []any{
		map[string]any{
			"type":   "Restored",
			"status": "False",
			"reason": "FallbackToColdStart",
		},
	}
	rec(t, f, 26*time.Second, "add", pmjGVR, pmjCold)

	// 3. Cold-start fallback (#41 concludeRestoreCrash) via Failed + RestoreCrashFallback
	// where the first replacement pod crashes and is replaced by a cold-started pod.
	rec(t, f, -time.Minute, "list", podGVR, podObj("pg-0", "uid-pg-src", -time.Minute, nil))
	rec(t, f, 30*time.Second, "add", podGVR, podObj("pg-0", "uid-pg-crashed", 30*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
	}))
	rec(t, f, 35*time.Second, "add", podGVR, podObj("pg-0", "uid-pg-cold", 35*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(40 * time.Second)},
		}}
	}))
	pmjCrashFallback := pmjObj("pmj-pg-0", "pg-0", "uid-pg-src", "Failed", "snap-pg-0")
	pmjCrashFallback["status"].(map[string]any)["restoredPodName"] = "pg-0"
	pmjCrashFallback["status"].(map[string]any)["restoredPodUID"] = "uid-pg-crashed"
	pmjCrashFallback["status"].(map[string]any)["conditions"] = []any{
		map[string]any{
			"type":   "Restored",
			"status": "False",
			"reason": "RestoreCrashFallback",
		},
	}
	rec(t, f, 33*time.Second, "add", pmjGVR, pmjCrashFallback)
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Migrations) != 3 {
		t.Fatalf("want 3 migrations (parent PodMigration policy CR ignored), got %d: %+v", len(run.Migrations), run.Migrations)
	}

	byPMJ := map[string]Migration{}
	for _, m := range run.Migrations {
		byPMJ[m.PMJ] = m
	}

	warm := byPMJ["pmj-counter-0"]
	if warm.Outcome != OutcomeRestored {
		t.Errorf("counter-0 outcome = %q, want %q", warm.Outcome, OutcomeRestored)
	}
	if warm.RestoreSignal != "pmj:RestoreVerified" {
		t.Errorf("counter-0 restoreSignal = %q, want pmj:RestoreVerified", warm.RestoreSignal)
	}
	if warm.DstPod != "counter-0-restored" {
		t.Errorf("counter-0 dstPod = %q, want counter-0-restored", warm.DstPod)
	}
	if warm.SnapReadyS != 8 {
		t.Errorf("counter-0 snapReadyS = %v, want 8 (from evictingStartTime fallback)", warm.SnapReadyS)
	}

	cold := byPMJ["pmj-redis-0"]
	if cold.Outcome != OutcomeColdStart {
		t.Errorf("redis-0 outcome = %q, want %q", cold.Outcome, OutcomeColdStart)
	}
	if cold.RestoreSignal != "pmj:FallbackToColdStart" {
		t.Errorf("redis-0 restoreSignal = %q, want pmj:FallbackToColdStart", cold.RestoreSignal)
	}

	crashCold := byPMJ["pmj-pg-0"]
	if crashCold.Outcome != OutcomeColdStart {
		t.Errorf("pg-0 outcome = %q, want %q", crashCold.Outcome, OutcomeColdStart)
	}
	if crashCold.RestoreSignal != "pmj:RestoreCrashFallback" {
		t.Errorf("pg-0 restoreSignal = %q, want pmj:RestoreCrashFallback", crashCold.RestoreSignal)
	}
	if crashCold.E2ES != 40 {
		t.Errorf("pg-0 e2eS = %v, want 40 (from cold-started Ready pod)", crashCold.E2ES)
	}

	// VerifyOutcomes should fail when allowColdStart=false, and pass when allowColdStart=true.
	if err := VerifyOutcomes(run, false); err == nil {
		t.Errorf("VerifyOutcomes(allowColdStart=false) succeeded on cold-start run, want error")
	}
	if err := VerifyOutcomes(run, true); err != nil {
		t.Errorf("VerifyOutcomes(allowColdStart=true) failed: %v", err)
	}
}

// TestInvariantViolationsAndGates verifies Prometheus metric parsing, prommetrics
// record ingestion, InvariantViolation K8s event ingestion, and VerifyInvariants.
func TestInvariantViolationsAndGates(t *testing.T) {
	promSample := `# HELP pod_migration_invariant_violations_total Total number of runtime invariant violations detected by the controller
# TYPE pod_migration_invariant_violations_total counter
pod_migration_invariant_violations_total{invariant="I1_DoubleExecution"} 0
pod_migration_invariant_violations_total{invariant="I3_VolumeSafeUngate"} 2
`
	parsed := ParseInvariantViolationsFromPrometheus(promSample)
	if parsed["I3_VolumeSafeUngate"] != 2 {
		t.Fatalf("parsed I3_VolumeSafeUngate = %v, want 2", parsed["I3_VolumeSafeUngate"])
	}
	if _, exists := parsed["I1_DoubleExecution"]; exists {
		t.Fatalf("zero-valued counter should not be recorded as a violation")
	}

	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const podGVR = "pods.v1"

	rec(t, f, -time.Minute, "list", podGVR, podObj("app-0", "uid-src", -time.Minute, nil))
	rec(t, f, 5*time.Second, "add", podGVR, podObj("app-0", "uid-dst", 5*time.Second, func(o map[string]any) {
		o["metadata"].(map[string]any)["annotations"] = map[string]any{"pod-migrate.io/psengine-restore": "restored"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(10 * time.Second)},
		}}
	}))
	rec(t, f, 12*time.Second, "add", pmjGVR, pmjObj("pmj-app-0", "app-0", "uid-src", "Succeeded", ""))

	// Clean run first.
	f.Close()

	cleanRun, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyInvariants(cleanRun); err != nil {
		t.Fatalf("VerifyInvariants on clean run failed: %v", err)
	}
	if err := VerifyOutcomes(cleanRun, false); err != nil {
		t.Fatalf("VerifyOutcomes on clean run failed: %v", err)
	}

	// Now append a prommetrics record and an InvariantViolation K8s event.
	f2, err := os.OpenFile(filepath.Join(dir, "records.ndjson"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	rec(t, f2, 15*time.Second, "prommetrics", "", map[string]any{
		"pod":  "pod-migration-controller-abc",
		"body": promSample,
	})
	rec(t, f2, 16*time.Second, "add", "events.v1", map[string]any{
		"type":           "Warning",
		"reason":         "InvariantViolation",
		"message":        "[I5_BoundedTermination] Undeploying timed out after 5m0s",
		"involvedObject": map[string]any{"kind": "PodMigration", "name": "policy-app"},
		"lastTimestamp":  ts(16 * time.Second),
	})
	f2.Close()

	dirtyRun, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if dirtyRun.TotalInvariantViolations != 3 {
		t.Fatalf("TotalInvariantViolations = %v, want 3 (2 from prommetrics I3 + 1 from event I5), map=%v",
			dirtyRun.TotalInvariantViolations, dirtyRun.InvariantViolations)
	}
	if err := VerifyInvariants(dirtyRun); err == nil {
		t.Fatalf("VerifyInvariants on dirty run succeeded, want non-nil error")
	}
}

// TestGateHoldReconstructionAndVerifySLO verifies that gateHoldS is reconstructed
// from MigrationRestoreReleased events, schedulingGates removal transitions, and
// PodScheduled condition LTT, that Stats.P95 is computed accurately, and that
// VerifySLO enforces I3/I4 wall-clock thresholds (T).
func TestGateHoldReconstructionAndVerifySLO(t *testing.T) {
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "records.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	const pmjGVR = "podmigrationjobs.v1alpha1.podmigration.gke.io"
	const podGVR = "pods.v1"

	// Migration 1: gateHoldS reconstructed via schedulingGates removal transition (created +10s, gate removed +13s => gateHoldS=3s).
	rec(t, f, -time.Minute, "list", podGVR, podObj("w1-0", "uid-w1-src", -time.Minute, nil))
	rec(t, f, 8*time.Second, "update", podGVR, podObj("w1-0", "uid-w1-src", -time.Minute, func(o map[string]any) {
		o["metadata"].(map[string]any)["deletionTimestamp"] = ts(8 * time.Second)
	}))
	rec(t, f, 10*time.Second, "add", podGVR, podObj("w1-0-dst", "uid-w1-dst", 10*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{
			"schedulingGates": []any{map[string]any{"name": "pod-migration.gke.io/restoring"}},
		}
	}))
	rec(t, f, 13*time.Second, "update", podGVR, podObj("w1-0-dst", "uid-w1-dst", 10*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
	}))
	rec(t, f, 20*time.Second, "update", podGVR, podObj("w1-0-dst", "uid-w1-dst", 10*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(20 * time.Second)},
		}}
	}))
	pmj1 := pmjObj("pmj-w1-0", "w1-0", "uid-w1-src", "Succeeded", "snap-w1")
	pmj1["status"].(map[string]any)["restoredPodName"] = "w1-0-dst"
	pmj1["status"].(map[string]any)["restoredPodUID"] = "uid-w1-dst"
	pmj1["status"].(map[string]any)["conditions"] = []any{
		map[string]any{"type": "Restored", "status": "True", "reason": "RestoreVerified"},
	}
	rec(t, f, 21*time.Second, "add", pmjGVR, pmj1)

	// Migration 2: gateHoldS reconstructed via MigrationRestoreReleased event (created +30s, event +35s => gateHoldS=5s).
	rec(t, f, -time.Minute, "list", podGVR, podObj("w2-0", "uid-w2-src", -time.Minute, nil))
	rec(t, f, 28*time.Second, "update", podGVR, podObj("w2-0", "uid-w2-src", -time.Minute, func(o map[string]any) {
		o["metadata"].(map[string]any)["deletionTimestamp"] = ts(28 * time.Second)
	}))
	rec(t, f, 30*time.Second, "add", podGVR, podObj("w2-0-dst", "uid-w2-dst", 30*time.Second, func(o map[string]any) {
		o["spec"] = map[string]any{"nodeName": "node-b"}
		o["status"] = map[string]any{"conditions": []any{
			map[string]any{"type": "Ready", "status": "True", "lastTransitionTime": ts(45 * time.Second)},
		}}
	}))
	rec(t, f, 35*time.Second, "add", "events.v1", map[string]any{
		"type":           "Normal",
		"reason":         "MigrationRestoreReleased",
		"message":        "Scheduling gate removed",
		"involvedObject": map[string]any{"kind": "Pod", "name": "w2-0-dst"},
		"lastTimestamp":  ts(35 * time.Second),
	})
	pmj2 := pmjObjAt("pmj-w2-0", "w2-0", "uid-w2-src", "Succeeded", "inc-w2", 25*time.Second)
	pmj2["status"].(map[string]any)["restoredPodName"] = "w2-0-dst"
	pmj2["status"].(map[string]any)["restoredPodUID"] = "uid-w2-dst"
	pmj2["status"].(map[string]any)["conditions"] = []any{
		map[string]any{"type": "Restored", "status": "True", "reason": "RestoreVerified"},
	}
	rec(t, f, 46*time.Second, "add", pmjGVR, pmj2)
	// Pre-migration scale-up Warning event (at -30s, before PMJ t0) must NOT be joined.
	rec(t, f, -30*time.Second, "add", "events.v1", map[string]any{
		"type":           "Warning",
		"reason":         "GKEPodSnapshotting",
		"message":        "falling back to a cold start of pod: resource name may not be empty",
		"involvedObject": map[string]any{"kind": "Pod", "name": "w1-0"},
		"lastTimestamp":  ts(-30 * time.Second),
	})
	f.Close()

	run, err := Analyze(Options{RunDir: dir, WedgeThreshold: 10 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	byPMJ := map[string]Migration{}
	for _, m := range run.Migrations {
		byPMJ[m.PMJ] = m
	}
	if len(byPMJ["pmj-w1-0"].Warnings) != 0 {
		t.Errorf("pmj-w1-0 warnings = %v, want empty (pre-t0 scale-up warning must be excluded by inWindow)", byPMJ["pmj-w1-0"].Warnings)
	}
	if got := byPMJ["pmj-w1-0"].GateHoldS; got != 3 {
		t.Errorf("pmj-w1-0 gateHoldS = %v, want 3 (from schedulingGates removal transition)", got)
	}
	if got := byPMJ["pmj-w2-0"].GateHoldS; got != 5 {
		t.Errorf("pmj-w2-0 gateHoldS = %v, want 5 (from MigrationRestoreReleased event)", got)
	}
	if st := run.Stats["gateHoldS"]; st.N != 2 || st.P95 != 5 {
		t.Errorf("gateHoldS stats = %+v, want N=2 P95=5", st)
	}

	// VerifySLO with default thresholds (60s/60s/180s) must pass.
	if err := VerifySLO(run, DefaultSLOThresholds()); err != nil {
		t.Fatalf("VerifySLO with DefaultSLOThresholds failed: %v", err)
	}

	// Tightening GateHoldP95 below 5s must fail I3:GateLiveness.
	if err := VerifySLO(run, SLOThresholds{GateHoldP95: 4 * time.Second, DowntimeP95: 60 * time.Second, E2EP95: 180 * time.Second}); err == nil {
		t.Errorf("VerifySLO with GateHoldP95=4s succeeded, want I3:GateLiveness error")
	}
	// Tightening DowntimeP95 below 17s must fail I4:BlackoutWindow.
	if err := VerifySLO(run, SLOThresholds{GateHoldP95: 60 * time.Second, DowntimeP95: 10 * time.Second, E2EP95: 180 * time.Second}); err == nil {
		t.Errorf("VerifySLO with DowntimeP95=10s succeeded, want I4:BlackoutWindow error")
	}
	// Tightening E2EP95 below 21s must fail I4:TerminalProgress.
	if err := VerifySLO(run, SLOThresholds{GateHoldP95: 60 * time.Second, DowntimeP95: 60 * time.Second, E2EP95: 15 * time.Second}); err == nil {
		t.Errorf("VerifySLO with E2EP95=15s succeeded, want I4:TerminalProgress error")
	}
	// Empty run must fail VerifySLO.
	emptyRun := &Run{Scenario: "empty", Stats: map[string]Stats{}}
	if err := VerifySLO(emptyRun, DefaultSLOThresholds()); err == nil {
		t.Errorf("VerifySLO on empty run succeeded, want error")
	}
}

func TestVerifyVerdictMatchesData(t *testing.T) {
	// 1. Consistent warm restore: Restored=true and state check Pass=true.
	warmRun := &Run{
		Scenario:    "warm-ok",
		Populations: map[string]int{"measured": 1},
		Outcomes:    map[string]int{OutcomeRestored: 1},
		Migrations: []Migration{{
			PMJ: "pmj-redis-0", Pod: "redis-0", Population: "measured",
			Outcome: OutcomeRestored, Restored: true, RestoreSignal: "pmj:RestoreVerified",
		}},
		Checks: []Check{{
			Name: "state survived (token-verified)", Group: "redis", Pass: true, Detail: "migkey matched",
		}},
	}
	if err := VerifyVerdictMatchesData(warmRun); err != nil {
		t.Fatalf("expected consistent warm restore to pass VerifyVerdictMatchesData, got: %v", err)
	}

	// 2. False-positive restore verdict: Restored=true while app is empty (Pass=false).
	falsePositiveRestore := &Run{
		Scenario:    "s7-false-positive",
		Populations: map[string]int{"measured": 1},
		Outcomes:    map[string]int{OutcomeRestored: 1},
		Migrations: []Migration{{
			PMJ: "pmj-redis-0", Pod: "redis-0", Population: "measured",
			Outcome: OutcomeRestored, Restored: true, RestoreSignal: "pmj:RestoreVerified",
		}},
		Checks: []Check{{
			Name: "write continuity", Group: "redis", Pass: false, Detail: "lost acknowledged writes: 50 missing",
		}},
	}
	if err := VerifyVerdictMatchesData(falsePositiveRestore); err == nil {
		t.Fatal("expected Restored=true with failed write continuity check to fail VerifyVerdictMatchesData")
	} else if !strings.Contains(err.Error(), "verdict-data mismatch [S7]") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// 3. Reverse mismatch: controller reports cold-start (Restored=false) while app check says state survived (Pass=true).
	reverseMismatch := &Run{
		Scenario:    "s7-reverse-mismatch",
		Populations: map[string]int{"measured": 1},
		Outcomes:    map[string]int{OutcomeColdStart: 1},
		Migrations: []Migration{{
			PMJ: "pmj-counter-0", Pod: "counter-0", Population: "measured",
			Outcome: OutcomeColdStart, Restored: false, RestoreSignal: "pmj:FallbackToColdStart",
		}},
		Checks: []Check{{
			Name: "state survived (token-verified)", Group: "counter", Pass: true, Detail: "instanceID preserved",
		}},
	}
	if err := VerifyVerdictMatchesData(reverseMismatch); err == nil {
		t.Fatal("expected Restored=false with Pass=true state check to fail VerifyVerdictMatchesData")
	} else if !strings.Contains(err.Error(), "verdict-data mismatch [S7]") {
		t.Fatalf("unexpected error message: %v", err)
	}

	// 4. Consistent cold-start fallback: Restored=false and pre-checkpoint state check Pass=false.
	coldStartConsistent := &Run{
		Scenario:    "s3-cold-start-ok",
		Populations: map[string]int{"measured": 1},
		Outcomes:    map[string]int{OutcomeColdStart: 1},
		Migrations: []Migration{{
			PMJ: "pmj-counter-0", Pod: "counter-0", Population: "measured",
			Outcome: OutcomeColdStart, Restored: false, RestoreSignal: "pmj:FallbackToColdStart",
		}},
		Checks: []Check{
			{Name: "I9 deterministic cold-start fallback (FallbackToColdStart)", Group: "counter", Pass: true},
			{Name: "state survived (token-verified)", Group: "counter", Pass: false, Detail: "fresh instanceID after cold start"},
		},
	}
	if err := VerifyVerdictMatchesData(coldStartConsistent); err != nil {
		t.Fatalf("expected consistent cold-start (Restored=false, state Pass=false) to pass VerifyVerdictMatchesData, got: %v", err)
	}
	if err := VerifyOutcomes(coldStartConsistent, true); err != nil {
		t.Fatalf("expected consistent cold-start with allowColdStart=true to pass VerifyOutcomes, got: %v", err)
	}

	// 5. Missing state check must fail VerifyVerdictMatchesData.
	noCheckRun := &Run{
		Scenario:    "no-checks",
		Populations: map[string]int{"measured": 1},
		Migrations: []Migration{{
			PMJ: "pmj-1", Pod: "pod-1", Population: "measured", Outcome: OutcomeRestored, Restored: true,
		}},
	}
	if err := VerifyVerdictMatchesData(noCheckRun); err == nil {
		t.Fatal("expected run without state checks to fail VerifyVerdictMatchesData")
	}
}
