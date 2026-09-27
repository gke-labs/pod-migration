package report

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/analyze"
)

func sampleRun() *analyze.Run {
	return &analyze.Run{
		Scenario: "S1 cache fleet drain",
		Migrations: []analyze.Migration{
			{PMJ: "pmj-a", Pod: "redis-0", App: "redis", Outcome: analyze.OutcomeRestored,
				SnapReadyS: 12, EvictedS: 15, E2ES: 28, DowntimeS: 13,
				SnapshotBytes: 2 << 30, SrcNode: "n1", DstNode: "n2",
				DstPod: "redis-0b", Restored: true, RestoreSignal: "psengine-restore:restored"},
			{PMJ: "pmj-b", Pod: "redis-1", App: "redis", Outcome: analyze.OutcomeWedged,
				SnapReadyS: -1, EvictedS: -1, E2ES: -1, DowntimeS: -1, Phase: "Snapshotting",
				Warnings: []string{"2026-08-21T10:00:00Z CheckpointFailed: boom"}},
			{PMJ: "pmj-c", Pod: "redis-2", App: "redis", Outcome: analyze.OutcomeReplaced,
				SnapReadyS: 10, EvictedS: 12, E2ES: 20, DowntimeS: 4, RestoreSignal: "absent"},
			{PMJ: "pmj-old", Pod: "redis-9", App: "redis", Outcome: analyze.OutcomeNoDst,
				SnapReadyS: -1, EvictedS: -1, E2ES: -1, DowntimeS: -1,
				Population: analyze.PopPreCollection},
		},
		Stats: map[string]analyze.Stats{
			"e2eS":      {N: 2, P50: 20, P90: 28, P99: 28, Max: 28, Mean: 24},
			"downtimeS": {N: 2, P50: 4, P90: 13, P99: 13, Max: 13, Mean: 8.5},
		},
		Outcomes: map[string]int{
			analyze.OutcomeRestored: 1, analyze.OutcomeWedged: 1, analyze.OutcomeReplaced: 1,
		},
		Populations: map[string]int{"measured": 3, analyze.PopPreCollection: 1},
		Checks: []analyze.Check{
			{Name: "state survived (token-verified)", Group: "redis", Pass: false,
				Detail: "1/2 migrations carried their state token across", Value: 1, Total: 2},
		},
		Series: []analyze.Series{
			{Name: "controller-cpu", Unit: "millicores",
				Points: [][2]float64{{1755770400, 12}, {1755770460, 48}, {1755770520, 21}}},
		},
		Controller: analyze.ControllerUsage{MaxCPUMilli: 48, MaxMemBytes: 100 << 20},
		Failures: []analyze.Failure{
			{PMJ: "pmj-b", Pod: "redis-1", Kind: analyze.OutcomeWedged, Phase: "Snapshotting"},
		},
	}
}

func TestGenerate(t *testing.T) {
	out := filepath.Join(t.TempDir(), "report.html")
	if err := Generate(out, "Test report", []*analyze.Run{sampleRun()},
		"# Findings\n\n## ISSUE-X\n\nA **bold** claim with `code`.\n\n- item one\n- item two\n\n| a | b |\n|---|---|\n| 1 | 2 |\n"); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	html := string(raw)
	for _, want := range []string{
		"Executive summary",
		"How to read this report",
		"State verified (token checks)",
		"Restore-signaled",
		"Proof of value", "Engineering",
		"S1 cache fleet drain",
		"Outcome by application",
		"replaced (no signal)", // unproven completions are separated
		"no replacement",       // failure column is visible
		"driver verified",      // checks joined into the app matrix
		"state survived",       // check row rendered
		"1 of 2",               // check magnitude (value/total) rendered
		"Per-migration phase timeline",
		"controller-cpu",
		"Failure drill-down",
		"pre-collection",             // excluded population is labeled
		"data-tip",                   // hover layer present
		"prefers-color-scheme: dark", // dark mode present
		"table view",                 // accessibility twin present
		"<details",                   // heavy sections collapse
	} {
		if !strings.Contains(html, want) {
			t.Errorf("report missing %q", want)
		}
	}
	if strings.Contains(html, "<script src") || strings.Contains(html, "http://") || strings.Contains(html, "https://") {
		t.Error("report must be self-contained (no external URLs)")
	}
	// This run carried snapshot sizes, so the tile is legitimate.
	if !strings.Contains(html, "Snapshot data moved") {
		t.Error("snapshot-data tile missing despite sampled bytes")
	}
}

// TestSnapshotTileOmittedWithoutSampling: when no GCS sampling ran, the
// tile must be absent — never an invented "0 B" (HR-2).
func TestSnapshotTileOmittedWithoutSampling(t *testing.T) {
	r := sampleRun()
	for i := range r.Migrations {
		r.Migrations[i].SnapshotBytes = 0
	}
	out := filepath.Join(t.TempDir(), "report.html")
	if err := Generate(out, "Test report", []*analyze.Run{r}, ""); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "Snapshot data moved") {
		t.Error("snapshot-data tile must be omitted when no GCS sampling ran")
	}
}

// TestNoDataTilesRenderDash: a metric with no samples renders "–", never an
// invented "0s" (the shipped artifact claimed "Median service gap 0s" with
// n=0 — live defect).
func TestNoDataTilesRenderDash(t *testing.T) {
	r := &analyze.Run{
		Scenario:   "empty",
		Migrations: []analyze.Migration{{PMJ: "p", Outcome: analyze.OutcomeNoDst, SnapReadyS: -1, EvictedS: -1, E2ES: -1, DowntimeS: -1}},
		Stats:      map[string]analyze.Stats{},
		Outcomes:   map[string]int{analyze.OutcomeNoDst: 1},
	}
	rv := buildRunView(r)
	byLabel := map[string]Tile{}
	for _, tl := range rv.Summary {
		byLabel[tl.Label] = tl
	}
	if got := byLabel["Median service gap"].Value; got != "–" {
		t.Errorf("median gap with n=0 = %q, want –", got)
	}
	if got := byLabel["p90 end-to-end"].Value; got != "–" {
		t.Errorf("p90 e2e with n=0 = %q, want –", got)
	}
	// statsTable must not fabricate zeros either.
	table := string(statsTable(map[string]analyze.Stats{"downtimeS": {}}))
	if strings.Contains(table, "0s") {
		t.Errorf("statsTable renders zeros for empty stats: %s", table)
	}
}

// TestAppMatrix: rows count terminal attempts, visible columns sum to the
// total (no hidden "other" bucket), no-replacement stays in the
// denominator, and excluded populations are left out.
func TestAppMatrix(t *testing.T) {
	rows := appMatrix([]analyze.Migration{
		{App: "redis", Outcome: analyze.OutcomeRestored},
		{App: "redis", Outcome: analyze.OutcomeReplaced},
		{App: "redis", Outcome: analyze.OutcomeNoDst},
		{App: "redis", Outcome: analyze.OutcomeWedged},
		{App: "redis", Outcome: analyze.OutcomeInFlight},                                 // not terminal
		{App: "redis", Outcome: analyze.OutcomeRestored, Population: analyze.PopStaleCR}, // excluded
	}, []analyze.Check{{Name: "state survived (token-verified)", Group: "redis", Value: 1, Total: 4}})
	if len(rows) != 1 {
		t.Fatalf("rows = %+v", rows)
	}
	r := rows[0]
	if r.Total != 4 {
		t.Errorf("total = %d, want 4 terminal attempts", r.Total)
	}
	if sum := r.Restored + r.Replaced + r.Cold + r.Failed + r.NoRepl + r.Wedged; sum != r.Total {
		t.Errorf("visible columns sum %d != total %d", sum, r.Total)
	}
	if r.Rate != "25.0%" {
		t.Errorf("restore-signaled rate = %q, want 25.0%% (1 of 4, no-replacement in denominator)", r.Rate)
	}
	if r.Verified != "1/4" {
		t.Errorf("verified = %q, want 1/4 from the driver check", r.Verified)
	}
}

func TestVerifiedTile(t *testing.T) {
	tl := verifiedTileFrom(47, 67)
	if tl.Value != "70.1%" || tl.Status != "critical" {
		t.Errorf("tile = %+v", tl)
	}
	if tl := verifiedTileFrom(0, 0); tl.Value != "–" {
		t.Errorf("no checks must render –, got %q", tl.Value)
	}
}

func TestHBarChartEmpty(t *testing.T) {
	if HBarChart(nil, "s") != "" {
		t.Error("empty input should render nothing")
	}
	if HBarChartStacked(nil, "s") != "" {
		t.Error("empty stacked input should render nothing")
	}
}

func TestGanttCaps(t *testing.T) {
	var ms []analyze.Migration
	for i := 0; i < 100; i++ {
		ms = append(ms, analyze.Migration{Pod: "p", E2ES: float64(i + 1), SnapReadyS: 1, EvictedS: 2})
	}
	svg, shown, total, _ := GanttChart(ms, 40)
	if svg == "" || shown != 40 || total != 100 {
		t.Errorf("shown=%d total=%d", shown, total)
	}
}

// TestGanttDomainCap: one ~2000s outlier must not compress every other row
// — the domain caps at p95 and the caller is told.
func TestGanttDomainCap(t *testing.T) {
	var ms []analyze.Migration
	for i := 0; i < 39; i++ {
		ms = append(ms, analyze.Migration{Pod: "p", E2ES: 10, SnapReadyS: 2, EvictedS: 4})
	}
	ms = append(ms, analyze.Migration{Pod: "outlier", E2ES: 2000, SnapReadyS: 2, EvictedS: 4})
	svg, _, _, capS := GanttChart(ms, 40)
	if capS <= 0 || capS >= 2000 {
		t.Fatalf("capS = %v, want p95 cap below the outlier", capS)
	}
	if !strings.Contains(svg, "clipped at domain cap") {
		t.Error("clipped outlier segment should say so in its tooltip")
	}
	// Uniform data: no cap.
	if _, _, _, capS := GanttChart(ms[:39], 40); capS != 0 {
		t.Errorf("uniform rows must not be capped, got %v", capS)
	}
}
