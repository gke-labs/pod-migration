// Package analyze reconstructs per-migration timelines from a collector
// record log and emits the structured run.json consumed by the report
// generator.
//
// Timestamp policy: object-carried timestamps (creationTimestamp, condition
// lastTransitionTime, deletionTimestamp, completionTime) are preferred; the
// collector's receipt timestamp is the fallback for transitions the API
// server does not timestamp (e.g. PMJ phase changes).
package analyze

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Options configures analysis of one run directory.
type Options struct {
	RunDir         string
	Scenario       string
	WedgeThreshold time.Duration
	ControllerNS   string
	// PodPrefix, when set, restricts the analysis to migrations whose target
	// pod name starts with the prefix. Needed when several scenarios run
	// concurrently on one cluster and each collector sees all PMJs.
	PodPrefix string
	// MetricsURL optionally scrapes a live controller /metrics HTTP endpoint
	// (e.g. http://localhost:8080/metrics) during Analyze.
	MetricsURL string
	// MetricsFile optionally reads a saved Prometheus exposition file during Analyze.
	MetricsFile string
}

type pmjState struct {
	name       string
	created    time.Time
	pod        string
	targetUID  string
	lastPhase  string
	phaseFirst map[string]time.Time // phase -> first receipt time
	// lastProgress is the receipt time of the most recent phase CHANGE;
	// wedge detection measures stall from here, not from PMJ age.
	lastProgress       time.Time
	completion         time.Time
	evictingStart      time.Time
	restoringStart     time.Time
	gateReleased       bool
	snapshot           string
	restoredPodName    string
	restoredPodUID     string
	restoredCondStatus string
	restoredCondReason string
}

type podInc struct {
	name           string
	uid            string
	app            string
	jobIdx         string // job-idx label: Job replacement pods get new names
	created        time.Time
	node           string
	wasGated       bool
	gateReleasedAt time.Time
	scheduledLTT   time.Time
	readyLTT       time.Time
	deletion       time.Time
	deleteSeen     time.Time
	// psRestore is the engine's explicit outcome annotation
	// (pod-migrate.io/psengine-restore: restored|failed) verbatim. It is
	// the ONLY pod-carried restore signal: generic snapshot-related keys
	// (e.g. podsnapshot.gke.io/ps-name) are stamped on source and
	// cold-started pods alike and prove nothing.
	psRestore string
}

type snapState struct {
	created   time.Time
	readyAt   time.Time
	podName   string
	lastPhase string
	engine    string // pod-migrate.io/engine label (criu-snapshot-engine)
}

type eventRec struct {
	ts          time.Time
	typ, reason string
	kind, name  string
	message     string
}

// Analyze reads records.ndjson (+ checks.ndjson, controller logs, meta.json)
// and produces the Run.
func Analyze(o Options) (*Run, error) {
	if o.WedgeThreshold <= 0 {
		o.WedgeThreshold = 10 * time.Minute
	}
	f, err := os.Open(filepath.Join(o.RunDir, "records.ndjson"))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	pmjs := map[string]*pmjState{}
	pms := map[string]*pmState{}
	pods := map[string]map[string]*podInc{} // name -> uid -> incarnation
	snaps := map[string]*snapState{}
	var events []eventRec
	var lastTS time.Time
	gcsObjects := map[string]int64{}
	var gcsTotal, ctrlCPU, ctrlMem []timePoint
	var maxCPU, maxMem int64
	// perPodInvariants tracks the highest observed counter value per
	// (controllerPod, invariantID) from scraped /metrics payloads.
	perPodInvariants := map[string]map[string]float64{}
	recordPromMetrics := func(source, body string) {
		parsed := ParseInvariantViolationsFromPrometheus(body)
		if len(parsed) == 0 {
			return
		}
		if perPodInvariants[source] == nil {
			perPodInvariants[source] = map[string]float64{}
		}
		for id, val := range parsed {
			if val > perPodInvariants[source][id] {
				perPodInvariants[source][id] = val
			} else if _, exists := perPodInvariants[source][id]; !exists {
				perPodInvariants[source][id] = val
			}
		}
	}

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 4*1024*1024), 16*1024*1024)
	for sc.Scan() {
		var rec struct {
			TS   string          `json:"ts"`
			Type string          `json:"type"`
			GVR  string          `json:"gvr"`
			Obj  json.RawMessage `json:"obj"`
		}
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			continue
		}
		ts := parseTime(rec.TS)
		if ts.After(lastTS) {
			lastTS = ts
		}
		switch { //nolint:staticcheck // mixed type/GVR dispatch reads better unswitched
		case rec.Type == "gcs":
			var g struct {
				TotalBytes int64            `json:"totalBytes"`
				Objects    map[string]int64 `json:"objects"`
			}
			if json.Unmarshal(rec.Obj, &g) == nil {
				gcsTotal = append(gcsTotal, timePoint{ts, float64(g.TotalBytes)})
				for k, v := range g.Objects {
					if v > gcsObjects[k] {
						gcsObjects[k] = v
					}
				}
			}
		case rec.Type == "prommetrics":
			var pm struct {
				Pod  string `json:"pod"`
				Body string `json:"body"`
			}
			if json.Unmarshal(rec.Obj, &pm) == nil && pm.Body != "" {
				src := pm.Pod
				if src == "" {
					src = "controller"
				}
				recordPromMetrics(src, pm.Body)
			}
		case rec.Type == "podmetrics":
			cpu, mem, ns := podMetrics(rec.Obj)
			if o.ControllerNS != "" && ns == o.ControllerNS {
				ctrlCPU = append(ctrlCPU, timePoint{ts, float64(cpu)})
				ctrlMem = append(ctrlMem, timePoint{ts, float64(mem)})
				if cpu > maxCPU {
					maxCPU = cpu
				}
				if mem > maxMem {
					maxMem = mem
				}
			}
		case rec.Type == "nodemetrics", rec.Type == "meta", rec.Type == "error":
			// retained in the log; not needed for the core reconstruction
		default: // object records: list/add/update/delete
			var obj map[string]any
			if json.Unmarshal(rec.Obj, &obj) != nil {
				continue
			}
			u := &unstructured.Unstructured{Object: obj}
			switch {
			case strings.HasPrefix(rec.GVR, "podmigrationjobs."):
				ingestPMJ(pmjs, u, ts)
			case strings.HasPrefix(rec.GVR, "podmigrations.") && strings.HasSuffix(rec.GVR, ".pod-migrate.io"):
				// Only pod-migrate.io's PodMigration is a per-pod migration CR;
				// podmigration.gke.io's PodMigration is a parent policy CR.
				ingestPM(pms, u, ts)
			case strings.HasPrefix(rec.GVR, "podsnapshots."):
				ingestSnap(snaps, u, ts)
			case strings.HasPrefix(rec.GVR, "pods.v1"):
				ingestPod(pods, u, ts, rec.Type)
			case strings.HasPrefix(rec.GVR, "events."):
				if ev := ingestEvent(u, ts); ev != nil {
					events = append(events, *ev)
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("scan records: %w", err)
	}

	// Optional Prometheus /metrics file in <run>/metrics.prom or --metrics-file.
	if raw, err := os.ReadFile(filepath.Join(o.RunDir, "metrics.prom")); err == nil {
		recordPromMetrics("metrics.prom", string(raw))
	}
	if o.MetricsFile != "" {
		raw, err := os.ReadFile(o.MetricsFile)
		if err != nil {
			return nil, fmt.Errorf("read metrics file %s: %w", o.MetricsFile, err)
		}
		recordPromMetrics("metrics-file", string(raw))
	}
	if o.MetricsURL != "" {
		body, err := fetchMetricsURL(o.MetricsURL)
		if err != nil {
			return nil, fmt.Errorf("scrape metrics url %s: %w", o.MetricsURL, err)
		}
		recordPromMetrics("metrics-url", body)
	}

	invTotals := map[string]float64{}
	for _, podMap := range perPodInvariants {
		for id, val := range podMap {
			invTotals[id] += val
		}
	}
	// Also count any InvariantViolation Warning events emitted by the controller
	// in case /metrics was not scraped or lags behind the event stream.
	eventInvCounts := map[string]float64{}
	for _, ev := range events {
		if ev.reason == "InvariantViolation" {
			id := extractInvariantIDFromMessage(ev.message)
			eventInvCounts[id]++
		}
	}
	for id, c := range eventInvCounts {
		if c > invTotals[id] {
			invTotals[id] = c
		}
	}
	var totalInv float64
	for _, v := range invTotals {
		totalInv += v
	}
	if len(invTotals) == 0 {
		invTotals = nil
	}

	run := &Run{
		Scenario:    o.Scenario,
		Stats:       map[string]Stats{},
		Outcomes:    map[string]int{},
		Populations: map[string]int{},
		Controller: ControllerUsage{
			MaxCPUMilli: maxCPU, MaxMemBytes: maxMem,
		},
		InvariantViolations:      invTotals,
		TotalInvariantViolations: totalInv,
	}
	if raw, err := os.ReadFile(filepath.Join(o.RunDir, "meta.json")); err == nil {
		_ = json.Unmarshal(raw, &run.Meta)
	}
	// Collection start: objects created before it cannot be reconstructed
	// (their pods were never watched) and are bucketed out of the headline.
	var collStart time.Time
	if s, ok := run.Meta["startedAt"].(string); ok {
		collStart = parseTime(s)
	}
	run.Checks, run.Series = readChecks(o.RunDir)
	if len(gcsTotal) > 0 {
		run.Series = append(run.Series, toSeries("gcs-total-bytes", "bytes", gcsTotal))
	}
	if len(ctrlCPU) > 0 {
		run.Series = append(run.Series, toSeries("controller-cpu", "millicores", ctrlCPU))
		run.Series = append(run.Series, toSeries("controller-mem", "bytes", ctrlMem))
	}
	run.Controller.LogErrors, run.Controller.LogRetries = scanControllerLogs(o.RunDir)

	// add appends a migration, bucketing it into the measured population
	// (headline outcomes/failures) or a labeled excluded population.
	add := func(m Migration, statusless bool) {
		switch {
		case statusless:
			m.Population = PopStaleCR
		case !collStart.IsZero() && !parseTime(m.T0).IsZero() && parseTime(m.T0).Before(collStart):
			m.Population = PopPreCollection
		}
		if m.Population != "" {
			run.Populations[m.Population]++
			run.Migrations = append(run.Migrations, m)
			return
		}
		run.Populations["measured"]++
		run.Migrations = append(run.Migrations, m)
		run.Outcomes[m.Outcome]++
		switch m.Outcome {
		case OutcomeRestored, OutcomeReplaced, OutcomeInFlight, OutcomeRefused:
		default:
			run.Failures = append(run.Failures, Failure{
				PMJ: m.PMJ, Pod: m.Pod, Kind: m.Outcome, Phase: m.Phase, Events: m.Warnings,
			})
		}
	}

	// Reconstruct one Migration per PodMigration (this operator's CR —
	// object-carried measured timings, no receipt-time reconstruction).
	var pmNames []string
	for n := range pms {
		pmNames = append(pmNames, n)
	}
	sort.Strings(pmNames)
	for _, name := range pmNames {
		if o.PodPrefix != "" && !strings.HasPrefix(nestedStr(pms[name].obj, "spec", "podName"), o.PodPrefix) {
			continue
		}
		m := reconstructPM(name, pms[name], lastTS, o.WedgeThreshold)
		// A PM that never showed any status during the run is a stale
		// leftover CR, not a migration this run performed.
		_, hasStatus := pms[name].obj.Object["status"]
		add(m, !hasStatus)
	}

	// Reconstruct one Migration per PMJ incarnation (same-name PMJs that
	// were GC'd and recreated are separate incarnations, keyed by UID).
	keys := make([]string, 0, len(pmjs))
	for k := range pmjs {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool {
		ja, jb := pmjs[keys[a]], pmjs[keys[b]]
		if ja.name != jb.name {
			return ja.name < jb.name
		}
		return ja.created.Before(jb.created)
	})
	for _, k := range keys {
		j := pmjs[k]
		if o.PodPrefix != "" && !strings.HasPrefix(j.pod, o.PodPrefix) {
			continue
		}
		add(reconstruct(j.name, j, pods, snaps, gcsObjects, events, lastTS, o.WedgeThreshold), false)
	}
	run.Stats["snapReadyS"] = distribution(run.Migrations, func(m Migration) float64 { return m.SnapReadyS })
	run.Stats["evictedS"] = distribution(run.Migrations, func(m Migration) float64 { return m.EvictedS })
	run.Stats["e2eS"] = distribution(run.Migrations, func(m Migration) float64 { return m.E2ES })
	run.Stats["downtimeS"] = distribution(run.Migrations, func(m Migration) float64 { return m.DowntimeS })
	run.Stats["gateHoldS"] = distribution(run.Migrations, func(m Migration) float64 { return m.GateHoldS })
	run.Stats["checkpointUploadS"] = distribution(run.Migrations, func(m Migration) float64 {
		if m.CheckpointUploadS <= 0 {
			return -1
		}
		return m.CheckpointUploadS
	})
	return run, nil
}

func reconstruct(name string, j *pmjState, pods map[string]map[string]*podInc,
	snaps map[string]*snapState, gcsObjects map[string]int64,
	events []eventRec, lastTS time.Time, wedge time.Duration) Migration {

	m := Migration{
		PMJ: name, Pod: j.pod, T0: fmtTime(j.created), Phase: j.lastPhase,
		SnapReadyS: -1, EvictedS: -1, E2ES: -1, DowntimeS: -1, GateHoldS: -1,
		SnapshotName: j.snapshot,
	}
	if t, ok := j.phaseFirst["Snapshotting"]; ok {
		m.TSnapshotting = fmtTime(t)
	}
	if !j.evictingStart.IsZero() {
		m.TEvicting = fmtTime(j.evictingStart)
		m.SnapReadyS = j.evictingStart.Sub(j.created).Seconds()
	} else if t, ok := j.phaseFirst["Evicting"]; ok {
		m.TEvicting = fmtTime(t)
		m.SnapReadyS = t.Sub(j.created).Seconds()
	}

	// Migration window: pod links (replacement creation, source deletion)
	// are only accepted between just before the PMJ and shortly after its
	// terminal phase. Without the bound, the benchmark's end-of-run
	// teardown sweep read as ~2000s "evictions" on wedged rows.
	term := j.completion
	if term.IsZero() {
		if t, ok := j.phaseFirst["Succeeded"]; ok {
			term = t
		} else if t, ok := j.phaseFirst["SucceededWithoutRestore"]; ok {
			term = t
		} else if t, ok := j.phaseFirst["Failed"]; ok {
			term = t
		}
	}
	windowStart := j.created.Add(-time.Second)
	windowEnd := term.Add(2 * time.Minute)
	if term.IsZero() {
		windowEnd = j.created.Add(wedge)
	}
	inWindow := func(t time.Time) bool {
		return !t.IsZero() && !t.Before(windowStart) && !t.After(windowEnd)
	}

	// Source and replacement incarnations of the target pod.
	var src, dst *podInc
	if incs, ok := pods[j.pod]; ok {
		var ordered []*podInc
		for _, i := range incs {
			ordered = append(ordered, i)
		}
		sort.Slice(ordered, func(a, b int) bool { return ordered[a].created.Before(ordered[b].created) })
		for _, i := range ordered {
			if i.uid == j.targetUID {
				src = i
			} else if src != nil && inWindow(i.created) {
				if dst == nil || (dst.readyLTT.IsZero() && !i.readyLTT.IsZero()) {
					dst = i
				}
			}
		}
		if src == nil { // fall back: last incarnation created before the PMJ
			for _, i := range ordered {
				if !i.created.After(j.created) {
					src = i
				}
			}
		}
	}
	// Direct link via status.restoredPodName / status.restoredPodUID when
	// populated by gke-labs/pod-migration.
	if dst == nil && j.restoredPodName != "" {
		if incs, ok := pods[j.restoredPodName]; ok {
			if j.restoredPodUID != "" {
				if p, exists := incs[j.restoredPodUID]; exists {
					dst = p
				}
			}
			if dst == nil {
				for _, i := range incs {
					if src == nil || i.uid != src.uid {
						if dst == nil || i.created.After(dst.created) {
							dst = i
						}
					}
				}
			}
		}
	}
	// Job pods: the replacement is a differently-named pod sharing job-idx.
	if dst == nil && src != nil && src.jobIdx != "" {
		for _, incs := range pods {
			for _, i := range incs {
				if i.jobIdx == src.jobIdx && i.uid != src.uid && inWindow(i.created) {
					if dst == nil || i.created.Before(dst.created) {
						dst = i
					}
				}
			}
		}
	}
	// Deployment pods: replacements get fresh hash-suffixed NAMES, so the
	// same-name incarnation link never fires — link by app label instead
	// (earliest pod with the source's app label created inside the
	// migration window). Without this, every Deployment migration
	// reconstructed as "no-replacement" and the report showed 0% restored
	// (live defect).
	if dst == nil && src != nil && src.app != "" {
		for _, incs := range pods {
			for _, i := range incs {
				if i.app == src.app && i.uid != src.uid && inWindow(i.created) {
					if dst == nil || i.created.Before(dst.created) {
						dst = i
					}
				}
			}
		}
	}
	// If #41 RestoreCrashFallback deleted a crashed Deployment pod (readyLTT is zero),
	// prefer a subsequent Ready pod of the same app created after the crashed pod.
	if dst != nil && dst.readyLTT.IsZero() && j.restoredCondReason == "RestoreCrashFallback" && src != nil && src.app != "" {
		for _, incs := range pods {
			for _, i := range incs {
				if i.app == src.app && i.uid != src.uid && i.uid != dst.uid && i.created.After(dst.created) && inWindow(i.created) && !i.readyLTT.IsZero() {
					dst = i
				}
			}
		}
	}
	if src != nil {
		m.SrcNode = src.node
		m.App = src.app
		del := src.deletion
		if del.IsZero() {
			del = src.deleteSeen
		}
		if inWindow(del) {
			m.TSrcDeleted = fmtTime(del)
			m.EvictedS = del.Sub(j.created).Seconds()
		}
	}
	if dst != nil {
		m.DstPod = dst.name
		m.DstNode = dst.node
		m.TDstCreated = fmtTime(dst.created)
		if m.App == "" {
			m.App = dst.app
		}
		if !dst.created.IsZero() {
			var gateRel time.Time
			for _, ev := range events {
				if ev.kind == "Pod" && ev.name == dst.name && ev.reason == "MigrationRestoreReleased" && !ev.ts.Before(dst.created) {
					gateRel = ev.ts
					break
				}
			}
			if gateRel.IsZero() && !dst.gateReleasedAt.IsZero() && !dst.gateReleasedAt.Before(dst.created) {
				gateRel = dst.gateReleasedAt
			}
			if gateRel.IsZero() && (dst.wasGated || j.gateReleased) && !dst.scheduledLTT.IsZero() && !dst.scheduledLTT.Before(dst.created) {
				gateRel = dst.scheduledLTT
			}
			if gateRel.IsZero() && j.gateReleased && !j.restoringStart.IsZero() && !j.restoringStart.Before(dst.created) {
				gateRel = j.restoringStart
			}
			if !gateRel.IsZero() {
				m.TGateReleased = fmtTime(gateRel)
				hold := gateRel.Sub(dst.created).Seconds()
				if hold < 0 {
					hold = 0
				}
				m.GateHoldS = hold
			}
		}
		if !dst.readyLTT.IsZero() {
			m.TDstReady = fmtTime(dst.readyLTT)
			m.E2ES = dst.readyLTT.Sub(j.created).Seconds()
			if m.EvictedS >= 0 {
				// Service gap: replacement Ready minus source deletion. A
				// negative gap means the replacement was Ready before the
				// source went away — zero downtime, clamped (exactly -1
				// remains the only "unknown" sentinel).
				gap := dst.readyLTT.Sub(j.created.Add(time.Duration(m.EvictedS * float64(time.Second)))).Seconds()
				if gap < 0 {
					gap = 0
				}
				m.DowntimeS = gap
			}
		}
	}

	// Snapshot: prefer the PMJ's snapshotRef, else match by pod name.
	var snap *snapState
	if j.snapshot != "" {
		snap = snaps[j.snapshot]
	}
	if snap == nil {
		for sn, s := range snaps {
			if s.podName == j.pod || strings.Contains(sn, j.pod) {
				snap = s
				m.SnapshotName = sn
				break
			}
		}
	}
	if snap != nil && !snap.readyAt.IsZero() {
		m.TSnapReady = fmtTime(snap.readyAt)
		m.SnapReadyS = snap.readyAt.Sub(j.created).Seconds()
	}
	// psengine-native rows (HR-2): engine identity, checkpoint+upload
	// duration (PodSnapshot creation → Ready LTT — the engine sets Ready only
	// after the verified GCS upload), and the explicit restore outcome.
	if snap != nil && snap.engine != "" {
		m.Engine = snap.engine
		if !snap.readyAt.IsZero() && !snap.created.IsZero() {
			m.CheckpointUploadS = snap.readyAt.Sub(snap.created).Seconds()
		}
	}
	// Restore signal: explicit psengine-restore annotation or PodMigrationJob's
	// Restored status condition (gke-labs/pod-migration). A replacement without
	// either gets RestoreSignal "absent" and is never reported as restored.
	if dst != nil {
		if dst.psRestore != "" {
			m.RestoreOutcome = dst.psRestore
			m.Restored = dst.psRestore == "restored"
			m.RestoreSignal = "psengine-restore:" + dst.psRestore
		} else if j.restoredCondStatus == "True" {
			m.RestoreOutcome = "restored"
			m.Restored = true
			reason := j.restoredCondReason
			if reason == "" {
				reason = "RestoreVerified"
			}
			m.RestoreSignal = "pmj:" + reason
		} else if j.lastPhase == "SucceededWithoutRestore" || j.restoredCondStatus == "False" {
			m.RestoreOutcome = "failed"
			m.Restored = false
			reason := j.restoredCondReason
			if reason == "" {
				reason = "FallbackToColdStart"
			}
			m.RestoreSignal = "pmj:" + reason
		} else {
			m.RestoreSignal = "absent"
		}
	}
	if m.SnapshotName != "" {
		for path, size := range gcsObjects {
			if strings.Contains(path, m.SnapshotName) && size > m.SnapshotBytes {
				m.SnapshotBytes = size
			}
		}
	}

	if !j.completion.IsZero() {
		m.TTerminal = fmtTime(j.completion)
	} else if t, ok := j.phaseFirst["Succeeded"]; ok {
		m.TTerminal = fmtTime(t)
	} else if t, ok := j.phaseFirst["SucceededWithoutRestore"]; ok {
		m.TTerminal = fmtTime(t)
	} else if t, ok := j.phaseFirst["Failed"]; ok {
		m.TTerminal = fmtTime(t)
	}

	// Join warning events for the pod and the PMJ.
	for _, ev := range events {
		if ev.typ != "Warning" {
			continue
		}
		if (ev.kind == "Pod" && ev.name == j.pod) ||
			(ev.kind == "PodMigrationJob" && ev.name == name) {
			m.Warnings = append(m.Warnings, fmt.Sprintf("%s %s: %s",
				ev.ts.Format(time.RFC3339), ev.reason, ev.message))
		}
	}

	switch j.lastPhase {
	case "Succeeded":
		switch {
		case dst == nil:
			m.Outcome = OutcomeNoDst
		case m.Restored:
			m.Outcome = OutcomeRestored
		case m.RestoreOutcome == "failed":
			m.Outcome = OutcomeColdStart
		default:
			// Replacement observed but no engine signal either way:
			// completed, restore unproven.
			m.Outcome = OutcomeReplaced
		}
	case "SucceededWithoutRestore":
		if dst == nil {
			m.Outcome = OutcomeNoDst
		} else {
			m.Outcome = OutcomeColdStart
		}
	case "Failed":
		if (j.restoredCondReason == "RestoreCrashFallback" || j.restoredCondReason == "FallbackToColdStart") && dst != nil {
			m.Outcome = OutcomeColdStart
		} else {
			m.Outcome = OutcomeFailed
		}
	default:
		// Wedge = no phase progress within the threshold. A long-running
		// PMJ that is still transitioning is in-flight, not wedged.
		progress := j.lastProgress
		if progress.IsZero() {
			progress = j.created
		}
		if lastTS.Sub(progress) > wedge {
			m.Outcome = OutcomeWedged
		} else {
			m.Outcome = OutcomeInFlight
		}
	}
	return m
}

// ingestPMJ keys state per PMJ INCARNATION: the benchmark GC's PMJs per
// cycle and recreates same-name ones, and merging incarnations corrupted
// phases and timings (a recreated PMJ read as one wedged migration). The
// UID distinguishes incarnations; creationTimestamp is the fallback when a
// record carries no UID.
func ingestPMJ(pmjs map[string]*pmjState, u *unstructured.Unstructured, ts time.Time) {
	name := u.GetName()
	inc := string(u.GetUID())
	if inc == "" {
		inc = u.GetCreationTimestamp().Format(time.RFC3339Nano)
	}
	key := name + "\x00" + inc
	j, ok := pmjs[key]
	if !ok {
		j = &pmjState{name: name, created: u.GetCreationTimestamp().Time, phaseFirst: map[string]time.Time{}}
		pmjs[key] = j
	}
	if pod, ok, _ := unstructured.NestedString(u.Object, "spec", "podRef", "name"); ok {
		j.pod = pod
	}
	if uid, ok, _ := unstructured.NestedString(u.Object, "spec", "targetPodUID"); ok {
		j.targetUID = uid
	}
	if phase, ok, _ := unstructured.NestedString(u.Object, "status", "phase"); ok && phase != "" {
		if phase != j.lastPhase {
			j.lastPhase = phase
			j.lastProgress = ts
		}
		if _, seen := j.phaseFirst[phase]; !seen {
			j.phaseFirst[phase] = ts
		}
	}
	if ref, ok, _ := unstructured.NestedString(u.Object, "status", "snapshotRef"); ok && ref != "" {
		j.snapshot = ref
	}
	if rName, ok, _ := unstructured.NestedString(u.Object, "status", "restoredPodName"); ok && rName != "" {
		j.restoredPodName = rName
	}
	if rUID, ok, _ := unstructured.NestedString(u.Object, "status", "restoredPodUID"); ok && rUID != "" {
		j.restoredPodUID = rUID
	}
	if est, ok, _ := unstructured.NestedString(u.Object, "status", "evictingStartTime"); ok && est != "" {
		if t := parseTime(est); !t.IsZero() {
			j.evictingStart = t
		}
	}
	if rst, ok, _ := unstructured.NestedString(u.Object, "status", "restoringStartTime"); ok && rst != "" {
		if t := parseTime(rst); !t.IsZero() {
			j.restoringStart = t
		}
	}
	if gr, ok, _ := unstructured.NestedBool(u.Object, "status", "gateReleased"); ok && gr {
		j.gateReleased = true
	}
	if ct, ok, _ := unstructured.NestedString(u.Object, "status", "completionTime"); ok && ct != "" {
		j.completion = parseTime(ct)
	}
	if conds, ok, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); ok {
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "Restored" {
				if st, _ := cm["status"].(string); st != "" {
					j.restoredCondStatus = st
				}
				if rsn, _ := cm["reason"].(string); rsn != "" {
					j.restoredCondReason = rsn
				}
			}
		}
	}
}

func ingestSnap(snaps map[string]*snapState, u *unstructured.Unstructured, ts time.Time) {
	name := u.GetName()
	s, ok := snaps[name]
	if !ok {
		s = &snapState{created: u.GetCreationTimestamp().Time}
		snaps[name] = s
	}
	if eng := u.GetLabels()["pod-migrate.io/engine"]; eng != "" {
		s.engine = eng
	}
	for _, path := range [][]string{
		{"spec", "podRef", "name"}, {"spec", "podName"}, {"spec", "source", "podName"},
		{"spec", "sourcePod"}, // criu-snapshot-engine work orders
	} {
		if pod, ok, _ := unstructured.NestedString(u.Object, path...); ok && pod != "" {
			s.podName = pod
			break
		}
	}
	if phase, ok, _ := unstructured.NestedString(u.Object, "status", "phase"); ok {
		s.lastPhase = phase
	}
	ready := s.lastPhase == "Ready" || s.lastPhase == "Succeeded"
	var bestLTT time.Time
	if conds, ok, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); ok {
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			ct, _ := cm["type"].(string)
			cs, _ := cm["status"].(string)
			if (ct == "Ready" || ct == "Uploaded" || ct == "Finalized") && cs == "True" {
				ready = true
				if ltt, _ := cm["lastTransitionTime"].(string); ltt != "" {
					if t := parseTime(ltt); t.After(bestLTT) {
						bestLTT = t
					}
				}
			}
		}
	}
	if ready && s.readyAt.IsZero() {
		if !bestLTT.IsZero() {
			s.readyAt = bestLTT
		} else {
			s.readyAt = ts
		}
	}
}

func ingestPod(pods map[string]map[string]*podInc, u *unstructured.Unstructured, ts time.Time, recType string) {
	name, uid := u.GetName(), string(u.GetUID())
	incs, ok := pods[name]
	if !ok {
		incs = map[string]*podInc{}
		pods[name] = incs
	}
	p, ok := incs[uid]
	if !ok {
		p = &podInc{name: name, uid: uid, created: u.GetCreationTimestamp().Time,
			app: u.GetLabels()["app"], jobIdx: u.GetLabels()["job-idx"]}
		incs[uid] = p
	}
	if node, ok, _ := unstructured.NestedString(u.Object, "spec", "nodeName"); ok && node != "" {
		p.node = node
	}
	currentlyGated := false
	if gates, ok, _ := unstructured.NestedSlice(u.Object, "spec", "schedulingGates"); ok && len(gates) > 0 {
		for _, g := range gates {
			gm, _ := g.(map[string]any)
			if gn, _ := gm["name"].(string); gn == "gke.io/pod-migration-gate" || gn != "" {
				currentlyGated = true
				break
			}
		}
	}
	if currentlyGated {
		p.wasGated = true
	} else if p.wasGated && p.gateReleasedAt.IsZero() && !ts.IsZero() {
		p.gateReleasedAt = ts
	}
	if dt := u.GetDeletionTimestamp(); dt != nil {
		p.deletion = dt.Time
	}
	if recType == "delete" && p.deleteSeen.IsZero() {
		p.deleteSeen = ts
	}
	// Only the engine's explicit outcome annotation is a restore signal.
	// Generic snapshot-related keys (podsnapshot.gke.io/ps-name etc.) are
	// stamped on sources and cold-started replacements alike — matching
	// them by substring misreported cold starts as restores (live defect).
	if v, ok := u.GetAnnotations()["pod-migrate.io/psengine-restore"]; ok {
		p.psRestore = v
	}
	if conds, ok, _ := unstructured.NestedSlice(u.Object, "status", "conditions"); ok {
		for _, c := range conds {
			cm, _ := c.(map[string]any)
			if cm["type"] == "PodScheduled" && cm["status"] == "True" {
				if ltt, _ := cm["lastTransitionTime"].(string); ltt != "" {
					p.scheduledLTT = parseTime(ltt)
				}
			}
			if cm["type"] == "Ready" && cm["status"] == "True" {
				if ltt, _ := cm["lastTransitionTime"].(string); ltt != "" {
					p.readyLTT = parseTime(ltt)
				}
			}
		}
	}
}

func ingestEvent(u *unstructured.Unstructured, ts time.Time) *eventRec {
	ev := &eventRec{ts: ts}
	ev.typ, _, _ = unstructured.NestedString(u.Object, "type")
	ev.reason, _, _ = unstructured.NestedString(u.Object, "reason")
	ev.message, _, _ = unstructured.NestedString(u.Object, "message")
	ev.kind, _, _ = unstructured.NestedString(u.Object, "involvedObject", "kind")
	ev.name, _, _ = unstructured.NestedString(u.Object, "involvedObject", "name")
	if lt, ok, _ := unstructured.NestedString(u.Object, "lastTimestamp"); ok && lt != "" {
		ev.ts = parseTime(lt)
	}
	if ev.kind == "" && ev.name == "" {
		return nil
	}
	return ev
}

type timePoint struct {
	t time.Time
	v float64
}

func toSeries(name, unit string, pts []timePoint) Series {
	s := Series{Name: name, Unit: unit}
	for _, p := range pts {
		s.Points = append(s.Points, [2]float64{float64(p.t.Unix()), p.v})
	}
	return s
}

func podMetrics(raw json.RawMessage) (cpuMilli, memBytes int64, ns string) {
	var pm struct {
		Metadata struct {
			Namespace string `json:"namespace"`
		} `json:"metadata"`
		Containers []struct {
			Usage struct {
				CPU    string `json:"cpu"`
				Memory string `json:"memory"`
			} `json:"usage"`
		} `json:"containers"`
	}
	if json.Unmarshal(raw, &pm) != nil {
		return 0, 0, ""
	}
	for _, c := range pm.Containers {
		if q, err := resource.ParseQuantity(c.Usage.CPU); err == nil {
			cpuMilli += q.MilliValue()
		}
		if q, err := resource.ParseQuantity(c.Usage.Memory); err == nil {
			memBytes += q.Value()
		}
	}
	return cpuMilli, memBytes, pm.Metadata.Namespace
}

func readChecks(runDir string) ([]Check, []Series) {
	f, err := os.Open(filepath.Join(runDir, "checks.ndjson"))
	if err != nil {
		return nil, nil
	}
	defer f.Close()
	var checks []Check
	var series []Series
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var probe struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(sc.Bytes(), &probe) != nil {
			continue
		}
		if probe.Kind == "series" {
			var s Series
			if json.Unmarshal(sc.Bytes(), &s) == nil && s.Name != "" {
				series = append(series, s)
			}
			continue
		}
		var c Check
		if json.Unmarshal(sc.Bytes(), &c) == nil && c.Name != "" {
			checks = append(checks, c)
		}
	}
	return checks, series
}

func scanControllerLogs(runDir string) (errors, retries int) {
	matches, _ := filepath.Glob(filepath.Join(runDir, "controller-*.log"))
	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 4*1024*1024)
		for sc.Scan() {
			line := strings.ToLower(sc.Text())
			if strings.Contains(line, "error") {
				errors++
			}
			if strings.Contains(line, "requeue") || strings.Contains(line, "retry") {
				retries++
			}
		}
		f.Close()
	}
	return errors, retries
}

// distribution summarizes one duration metric across migrations that
// COMPLETED WITH A REPLACEMENT in the measured population (restored,
// replaced, cold-start). Wedged/failed/in-flight rows and excluded
// populations would poison the percentiles (e.g. teardown deletions read
// as ~2000s evictions on wedged rows) and are left out. Negative values
// are the "unknown" sentinel, never data.
func distribution(ms []Migration, get func(Migration) float64) Stats {
	var vals []float64
	for _, m := range ms {
		if m.Population != "" {
			continue
		}
		switch m.Outcome {
		case OutcomeRestored, OutcomeReplaced, OutcomeColdStart:
		default:
			continue
		}
		if v := get(m); v >= 0 {
			vals = append(vals, v)
		}
	}
	if len(vals) == 0 {
		return Stats{}
	}
	sort.Float64s(vals)
	var sum float64
	for _, v := range vals {
		sum += v
	}
	// Nearest-rank percentile: ceil(p*n)-1 avoids the small-n upward bias
	// of int(p*n) (which returned the max as "p90" for n=10).
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(vals)))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= len(vals) {
			idx = len(vals) - 1
		}
		return vals[idx]
	}
	return Stats{
		N: len(vals), P50: pct(0.50), P90: pct(0.90), P95: pct(0.95), P99: pct(0.99),
		Max: vals[len(vals)-1], Mean: sum / float64(len(vals)),
	}
}

func parseTime(s string) time.Time {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC()
		}
	}
	return time.Time{}
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

// ParseInvariantViolationsFromPrometheus extracts per-invariant counter values
// for `pod_migration_invariant_violations_total{invariant="I1..I9"}` from a
// Prometheus text exposition payload.
func ParseInvariantViolationsFromPrometheus(body string) map[string]float64 {
	out := map[string]float64{}
	sc := bufio.NewScanner(strings.NewReader(body))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "pod_migration_invariant_violations_total") {
			continue
		}
		rest := strings.TrimPrefix(line, "pod_migration_invariant_violations_total")
		invID := "unknown"
		if strings.HasPrefix(rest, "{") {
			closeIdx := strings.Index(rest, "}")
			if closeIdx < 0 {
				continue
			}
			labels := rest[1:closeIdx]
			rest = strings.TrimSpace(rest[closeIdx+1:])
			for _, part := range strings.Split(labels, ",") {
				kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
				if len(kv) == 2 && (kv[0] == "invariant" || kv[0] == "invariant_id") {
					invID = strings.Trim(kv[1], `"`)
				}
			}
		} else {
			rest = strings.TrimSpace(rest)
		}
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			continue
		}
		val, err := strconv.ParseFloat(fields[0], 64)
		if err != nil || val <= 0 {
			continue
		}
		out[invID] += val
	}
	return out
}

func fetchMetricsURL(url string) (string, error) {
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("unexpected HTTP status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// extractInvariantIDFromMessage extracts "I1".."I9" from controller event
// messages formatted like "[I1:AtMostOnceRestore] ...".
func extractInvariantIDFromMessage(msg string) string {
	if strings.HasPrefix(msg, "[") {
		if end := strings.IndexAny(msg, ":]"); end > 1 {
			return msg[1:end]
		}
	}
	return "unknown"
}

// VerifyInvariants returns an error if sum(pod_migration_invariant_violations_total) > 0.
func VerifyInvariants(run *Run) error {
	if run == nil || run.TotalInvariantViolations <= 0 {
		return nil
	}
	var keys []string
	for k, v := range run.InvariantViolations {
		if v > 0 {
			keys = append(keys, fmt.Sprintf("%s=%g", k, v))
		}
	}
	sort.Strings(keys)
	return fmt.Errorf("invariant violation assertion failed: sum(pod_migration_invariant_violations_total)=%g (%s)",
		run.TotalInvariantViolations, strings.Join(keys, ", "))
}

// VerifyOutcomes returns an error if the run contains any wedged, failed,
// no-replacement, or (unless allowColdStart is true) cold-start outcomes,
// or if any recorded driver check failed.
func VerifyOutcomes(run *Run, allowColdStart bool) error {
	if run == nil {
		return nil
	}
	if n := run.Outcomes[OutcomeWedged]; n > 0 {
		return fmt.Errorf("outcome assertion failed: %d wedged migration(s)", n)
	}
	if n := run.Outcomes[OutcomeFailed]; n > 0 {
		return fmt.Errorf("outcome assertion failed: %d failed migration(s)", n)
	}
	if n := run.Outcomes[OutcomeNoDst]; n > 0 {
		return fmt.Errorf("outcome assertion failed: %d no-replacement migration(s)", n)
	}
	if !allowColdStart {
		if n := run.Outcomes[OutcomeColdStart]; n > 0 {
			return fmt.Errorf("outcome assertion failed: %d unintended cold-start migration(s)", n)
		}
	}
	for _, c := range run.Checks {
		if !c.Pass {
			return fmt.Errorf("driver check %q (group=%s) failed: %s", c.Name, c.Group, c.Detail)
		}
	}
	return nil
}

// SLOThresholds configures wall-clock latency SLO thresholds (T) for T3 guardrail runs.
type SLOThresholds struct {
	// GateHoldP95 is the maximum allowed p95 scheduling-gate hold duration (I3: Gate Liveness).
	GateHoldP95 time.Duration
	// DowntimeP95 is the maximum allowed p95 serving blackout window (evict -> Ready, I4).
	DowntimeP95 time.Duration
	// E2EP95 is the maximum allowed p95 end-to-end migration duration (T0 -> Ready, I4).
	E2EP95 time.Duration
}

// DefaultSLOThresholds returns the canonical T3 wall-clock SLO thresholds (T):
//   - I3 Gate Liveness: p95(gateHoldS) <= 60s
//   - I4 Blackout Window: p95(downtimeS) <= 60s
//   - I4 Terminal Progress: p95(e2eS) <= 180s
func DefaultSLOThresholds() SLOThresholds {
	return SLOThresholds{
		GateHoldP95: 60 * time.Second,
		DowntimeP95: 60 * time.Second,
		E2EP95:      180 * time.Second,
	}
}

// VerifySLO enforces wall-clock latency thresholds (T) on gateHoldS (I3), downtimeS (I4), and e2eS (I4).
func VerifySLO(run *Run, slo SLOThresholds) error {
	if run == nil || run.Populations["measured"] == 0 {
		return fmt.Errorf("SLO assertion failed: 0 measured migrations in run")
	}
	if slo.GateHoldP95 > 0 {
		s := run.Stats["gateHoldS"]
		if s.N == 0 {
			return fmt.Errorf("SLO assertion failed [I3:GateLiveness]: no gateHoldS samples recorded")
		}
		if s.P95 > slo.GateHoldP95.Seconds() {
			return fmt.Errorf("SLO assertion failed [I3:GateLiveness]: gateHoldS p95=%.1fs exceeds threshold %.1fs",
				s.P95, slo.GateHoldP95.Seconds())
		}
	}
	if slo.DowntimeP95 > 0 {
		s := run.Stats["downtimeS"]
		if s.N == 0 {
			return fmt.Errorf("SLO assertion failed [I4:BlackoutWindow]: no downtimeS samples recorded")
		}
		if s.P95 > slo.DowntimeP95.Seconds() {
			return fmt.Errorf("SLO assertion failed [I4:BlackoutWindow]: downtimeS p95=%.1fs exceeds threshold %.1fs",
				s.P95, slo.DowntimeP95.Seconds())
		}
	}
	if slo.E2EP95 > 0 {
		s := run.Stats["e2eS"]
		if s.N == 0 {
			return fmt.Errorf("SLO assertion failed [I4:TerminalProgress]: no e2eS samples recorded")
		}
		if s.P95 > slo.E2EP95.Seconds() {
			return fmt.Errorf("SLO assertion failed [I4:TerminalProgress]: e2eS p95=%.1fs exceeds threshold %.1fs",
				s.P95, slo.E2EP95.Seconds())
		}
	}
	return nil
}

