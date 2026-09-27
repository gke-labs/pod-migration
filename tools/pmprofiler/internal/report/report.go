// Package report turns analyzed runs into a single self-contained HTML file
// with an engineering view (distributions, timelines, failures, controller
// usage) and a customer proof-of-value view (survival rates, downtime,
// verified checks). No external assets: all charts are inline SVG, all
// styling is embedded, light and dark themes are both selected.
//
// Honesty rules: the headline "state verified" number comes from driver
// checks (measured app-level truth), never from timeline reconstruction;
// reconstruction-based restoration is labeled "restore-signaled" and only
// counts explicit engine signals; a metric with no data renders "–", never
// an invented zero.
package report

import (
	"fmt"
	"html/template"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/analyze"
)

// Tile is one KPI stat tile.
type Tile struct {
	Label  string
	Value  string
	Note   string
	Status string // "", good, warning, serious, critical
}

// Figure is a rendered chart with its accessibility twin.
type Figure struct {
	Title    string
	Caption  string
	SVG      template.HTML
	Legend   []LegendItem
	Table    template.HTML // table view (accessibility twin)
	Collapse bool          // heavy figure: render inside a collapsed <details>
}

// LegendItem pairs a swatch class with a label.
type LegendItem struct {
	Class string
	Label string
}

// AppRow is one row of the per-app outcome matrix. Total counts terminal
// attempts (everything measured except in-flight); the visible columns sum
// to Total so the table is self-consistent.
type AppRow struct {
	App      string
	Total    int
	Restored int
	Replaced int
	Cold     int
	Failed   int
	NoRepl   int
	Wedged   int
	Rate     string // restore-signaled / Total
	Status   string
	Verified string // driver-check verification for the matching group
}

// RunView is everything the template needs for one scenario.
type RunView struct {
	Scenario   string
	Anchor     string
	CardStats  []Tile // compact stats shown on the scenario card
	CardStatus string // status dot on the card
	Summary    []Tile
	AppRows    []AppRow
	Checks     []analyze.Check
	ChecksPass int
	Figures    []Figure // engineering charts
	ValueFigs  []Figure // customer-facing charts
	Failures   []analyze.Failure
	Outcomes   map[string]int
	Controller analyze.ControllerUsage

	// Executive-summary row (pre-formatted).
	Attempts      int
	PopNote       string
	StateVerified string
	RestoredRate  string
	MedianGap     string
	P90E2E        string
	WedgedN       int
}

// PageData drives the top-level template.
type PageData struct {
	Title     string
	Generated string
	Overall   []Tile
	Runs      []RunView
	Findings  template.HTML // optional engineering-findings narrative
}

// Generate writes the report for the given runs to outPath. findingsMD, when
// non-empty, is a markdown narrative rendered as its own top-level section.
func Generate(outPath, title string, runs []*analyze.Run, findingsMD string) error {
	page := PageData{
		Title:     title,
		Generated: time.Now().UTC().Format("2006-01-02 15:04 UTC"),
		Findings:  mdToHTML(findingsMD),
	}
	var totMeasured, totExcluded, totRestored, totAttempts, totChecks, totChecksPass int
	var totBytes int64
	var tokVal, tokTot float64
	worstE2E := -1.0
	var worstNote string
	for _, r := range runs {
		rv := buildRunView(r)
		page.Runs = append(page.Runs, rv)
		totMeasured += measuredCount(r)
		totExcluded += len(r.Migrations) - measuredCount(r)
		totRestored += r.Outcomes[analyze.OutcomeRestored]
		totAttempts += attemptCount(r)
		for _, m := range r.Migrations {
			totBytes += m.SnapshotBytes
			if m.Population == "" && isCompleted(m.Outcome) && m.E2ES > worstE2E {
				worstE2E = m.E2ES
				worstNote = fmt.Sprintf("%s · %s", m.PMJ, r.Scenario)
			}
		}
		v, t, _ := tokenCheckSums(r.Checks)
		tokVal += v
		tokTot += t
		totChecks += len(r.Checks)
		totChecksPass += rv.ChecksPass
	}
	page.Overall = []Tile{
		verifiedTileFrom(tokVal, tokTot),
		{Label: "Migrations executed", Value: fmt.Sprint(totMeasured),
			Note: excludedNote(totExcluded)},
		restoreSignalOverallTile(page.Runs, totRestored, totAttempts),
		{Label: "App-level checks passed", Value: rate(totChecksPass, totChecks),
			Note:   fmt.Sprintf("%d of %d check rows", totChecksPass, totChecks),
			Status: rateStatus(totChecksPass, totChecks)},
	}
	if worstE2E >= 0 {
		page.Overall = append(page.Overall, Tile{
			Label: "Worst end-to-end tail", Value: fmtVal(worstE2E, "s"), Note: worstNote,
		})
	}
	// Only claim data volume when GCS sampling actually ran (HR-2: absent,
	// never invented).
	if totBytes > 0 {
		page.Overall = append(page.Overall, Tile{Label: "Snapshot data moved", Value: fmtBytes(float64(totBytes))})
	}
	f, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer f.Close()
	return pageTmpl.Execute(f, page)
}

// isCompleted reports whether the outcome is terminal with a replacement
// (the population the duration stats are computed over).
func isCompleted(o string) bool {
	return o == analyze.OutcomeRestored || o == analyze.OutcomeReplaced || o == analyze.OutcomeColdStart
}

// measuredCount is the headline population: run.Outcomes only counts
// measured migrations (excluded populations are bucketed by analyze).
func measuredCount(r *analyze.Run) int {
	n := 0
	for _, c := range r.Outcomes {
		n += c
	}
	return n
}

// attemptCount = measured migrations that ran to some conclusion (only
// in-flight is excluded; wedged counts as a failed attempt, never as
// "completed").
func attemptCount(r *analyze.Run) int {
	n := 0
	for o, c := range r.Outcomes {
		if o != analyze.OutcomeInFlight {
			n += c
		}
	}
	return n
}

func rate(num, den int) string {
	if den == 0 {
		return "–"
	}
	return fmt.Sprintf("%.1f%%", 100*float64(num)/float64(den))
}

// restoreSignalOverallTile suppresses the blended percentage when signal
// availability is heterogeneous (one engine emits explicit restore signals,
// another structurally cannot): averaging them invites misreading — the
// per-scenario tiles carry the honest per-engine numbers.
func restoreSignalOverallTile(runs []RunView, totRestored, totAttempts int) Tile {
	withSignal := 0
	for _, sv := range runs {
		if sv.Outcomes["restored"] > 0 {
			withSignal++
		}
	}
	if withSignal > 0 && withSignal < len(runs) {
		return Tile{Label: "Restore-signaled", Value: "n/a",
			Note: "engines differ in signal availability (one emits explicit restore signals, one exposes none) — see per-scenario tiles; state survival is proven by token checks"}
	}
	return Tile{Label: "Restore-signaled", Value: rate(totRestored, totAttempts),
		Note:   fmt.Sprintf("%d of %d attempts carried an explicit engine restore signal", totRestored, totAttempts),
		Status: rateStatus(totRestored, totAttempts)}
}

func rateStatus(num, den int) string {
	if den == 0 {
		return ""
	}
	switch r := float64(num) / float64(den); {
	case r >= 0.99:
		return "good"
	case r >= 0.95:
		return "warning"
	default:
		return "critical"
	}
}

// statVal formats a distribution value, or "–" when the distribution holds
// no data (never an invented "0s").
func statVal(st analyze.Stats, v float64) string {
	if st.N == 0 {
		return "–"
	}
	return fmtVal(v, "s")
}

// tokenCheckSums aggregates driver checks that verify state SURVIVAL via a
// token (Value/Total carry verified-of-attempted magnitudes). Matching on
// "surviv" keeps served-only checks out — their names mention "no state
// token by design", so a bare "token" match would wrongly include them.
func tokenCheckSums(cs []analyze.Check) (val, total float64, n int) {
	for _, c := range cs {
		if c.Total > 0 && strings.Contains(strings.ToLower(c.Name), "surviv") {
			val += c.Value
			total += c.Total
			n++
		}
	}
	return val, total, n
}

func verifiedTileFrom(val, total float64) Tile {
	t := Tile{Label: "State verified (token checks)"}
	if total <= 0 {
		t.Value = "–"
		t.Note = "no driver state checks ingested (pmprofiler check --total)"
		return t
	}
	t.Value = fmt.Sprintf("%.1f%%", 100*val/total)
	t.Note = fmt.Sprintf("%.0f of %.0f token-checked migrations, measured by the scenario driver", val, total)
	t.Status = rateStatus(int(val), int(total))
	return t
}

func excludedNote(excluded int) string {
	if excluded == 0 {
		return "measured population"
	}
	return fmt.Sprintf("measured population; %d pre-collection/stale objects excluded", excluded)
}

func popNote(r *analyze.Run) string {
	parts := []string{fmt.Sprintf("%d measured", r.Populations["measured"])}
	for _, k := range []string{analyze.PopPreCollection, analyze.PopStaleCR} {
		if n := r.Populations[k]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d %s", n, k))
		}
	}
	if len(parts) == 1 {
		return "all observed objects measured"
	}
	return strings.Join(parts, " + ")
}

func buildRunView(r *analyze.Run) RunView {
	rv := RunView{
		Scenario: r.Scenario,
		Anchor:   strings.ToLower(strings.ReplaceAll(r.Scenario, " ", "-")),
		Outcomes: r.Outcomes, Controller: r.Controller,
		Checks: r.Checks, Failures: r.Failures,
	}
	for _, c := range r.Checks {
		if c.Pass {
			rv.ChecksPass++
		}
	}
	attempts := attemptCount(r)
	restored := r.Outcomes[analyze.OutcomeRestored]
	tokVal, tokTot, _ := tokenCheckSums(r.Checks)
	verified := verifiedTileFrom(tokVal, tokTot)

	rv.Attempts = attempts
	rv.PopNote = popNote(r)
	rv.StateVerified = verified.Value
	rv.RestoredRate = rate(restored, attempts)
	rv.MedianGap = statVal(r.Stats["downtimeS"], r.Stats["downtimeS"].P50)
	rv.P90E2E = statVal(r.Stats["e2eS"], r.Stats["e2eS"].P90)
	rv.WedgedN = r.Outcomes[analyze.OutcomeWedged]

	rv.Summary = []Tile{
		verified,
		{Label: "Restore-signaled", Value: rv.RestoredRate,
			Note:   fmt.Sprintf("%d of %d attempts (explicit engine signal only)", restored, attempts),
			Status: rateStatus(restored, attempts)},
		{Label: "Migrations observed", Value: fmt.Sprint(measuredCount(r)), Note: rv.PopNote},
		{Label: "Median service gap", Value: rv.MedianGap,
			Note: "replacement Ready minus source deleted; negative overlap counts as 0"},
		{Label: "p90 end-to-end", Value: rv.P90E2E},
	}
	if rv.WedgedN > 0 {
		rv.Summary = append(rv.Summary, Tile{Label: "Wedged", Value: fmt.Sprint(rv.WedgedN), Status: "critical"})
	}
	rv.CardStatus = verified.Status
	if rv.CardStatus == "" {
		rv.CardStatus = rateStatus(restored, attempts)
	}
	rv.CardStats = []Tile{
		{Label: "migrations", Value: fmt.Sprint(measuredCount(r))},
		{Label: "state verified", Value: verified.Value},
	}
	if st := r.Stats["e2eS"]; st.N > 0 {
		rv.CardStats = append(rv.CardStats, Tile{Label: "p90 e2e", Value: fmtVal(st.P90, "s")})
	} else {
		rv.CardStats = append(rv.CardStats, Tile{Label: "p90 e2e", Value: "–"})
	}
	if len(r.Checks) > 0 {
		rv.CardStats = append(rv.CardStats, Tile{Label: "checks", Value: fmt.Sprintf("%d/%d", rv.ChecksPass, len(r.Checks))})
	} else {
		rv.CardStats = append(rv.CardStats, Tile{Label: "checks", Value: "–"})
	}

	rv.AppRows = appMatrix(r.Migrations, r.Checks)

	// Engineering figures.
	if fig := percentileFigure(r); fig != nil {
		rv.Figures = append(rv.Figures, *fig)
	}
	if svg, shown, total, capS := GanttChart(r.Migrations, 40); svg != "" {
		cap := fmt.Sprintf("All %d migrations.", total)
		if shown < total {
			cap = fmt.Sprintf("Slowest %d of %d migrations (full data in the table view).", shown, total)
		}
		if capS > 0 {
			cap += fmt.Sprintf(" Time domain capped at p95 (%s); full range in the table view.", fmtVal(capS, "s"))
		}
		rv.Figures = append(rv.Figures, Figure{
			Title: "Per-migration phase timeline", Caption: cap,
			SVG:      template.HTML(svg),
			Legend:   []LegendItem{{"s1", "checkpoint + upload"}, {"s2", "evict window"}, {"s3", "restore to Ready"}},
			Table:    migrationsTable(r.Migrations),
			Collapse: true,
		})
	}
	for _, s := range r.Series {
		if fig := seriesFigure(s); fig != nil {
			rv.Figures = append(rv.Figures, *fig)
		}
	}

	// Customer figures: snapshot size vs restore time when sizes are known.
	if fig := sizeVsTimeFigure(r); fig != nil {
		rv.ValueFigs = append(rv.ValueFigs, *fig)
	}
	return rv
}

// checkForGroup renders the driver verification for an app row whose app
// label matches the check group (label "sxs-counter" matches group
// "counter").
func checkForGroup(app string, checks []analyze.Check) string {
	for _, c := range checks {
		if c.Group == "" || c.Total <= 0 {
			continue
		}
		if app == c.Group || strings.HasSuffix(app, "-"+c.Group) || strings.Contains(app, c.Group) {
			return fmt.Sprintf("%.0f/%.0f", c.Value, c.Total)
		}
	}
	return "–"
}

func appMatrix(ms []analyze.Migration, checks []analyze.Check) []AppRow {
	byApp := map[string]*AppRow{}
	for _, m := range ms {
		if m.Population != "" || m.Outcome == analyze.OutcomeInFlight {
			continue // matrix covers measured terminal attempts
		}
		app := m.App
		if app == "" {
			app = "(unlabeled)"
		}
		row, ok := byApp[app]
		if !ok {
			row = &AppRow{App: app}
			byApp[app] = row
		}
		row.Total++
		switch m.Outcome {
		case analyze.OutcomeRestored:
			row.Restored++
		case analyze.OutcomeReplaced:
			row.Replaced++
		case analyze.OutcomeColdStart:
			row.Cold++
		case analyze.OutcomeFailed, analyze.OutcomeRefused:
			row.Failed++
		case analyze.OutcomeNoDst:
			row.NoRepl++
		case analyze.OutcomeWedged:
			row.Wedged++
		}
	}
	var rows []AppRow
	for _, r := range byApp {
		r.Rate = rate(r.Restored, r.Total)
		r.Status = rateStatus(r.Restored, r.Total)
		r.Verified = checkForGroup(r.App, checks)
		rows = append(rows, *r)
	}
	sort.Slice(rows, func(a, b int) bool { return rows[a].App < rows[b].App })
	return rows
}

func percentileFigure(r *analyze.Run) *Figure {
	metrics := []struct{ key, label string }{
		{"snapReadyS", "snapshot ready (checkpoint + upload)"},
		{"checkpointUploadS", "checkpoint + upload (engine-measured)"},
		{"evictedS", "source deleted"},
		{"downtimeS", "service gap"},
		{"e2eS", "end-to-end"},
	}
	var stacks []BarStack
	var legend []LegendItem
	classes := []string{"s1", "s2", "s3", "s4"}
	ci := 0
	for _, mt := range metrics {
		st, ok := r.Stats[mt.key]
		if !ok || st.N == 0 {
			continue
		}
		cls := classes[ci%len(classes)]
		ci++
		stack := BarStack{Title: fmt.Sprintf("%s (n=%d)", mt.label, st.N)}
		for _, p := range []struct {
			name string
			v    float64
		}{{"p50", st.P50}, {"p90", st.P90}, {"p99", st.P99}, {"max", st.Max}} {
			stack.Bars = append(stack.Bars, BarGroup{
				Label: p.name, Value: p.v, Class: cls,
				Tip: fmt.Sprintf("%s %s = %s (n=%d)", mt.label, p.name, fmtVal(p.v, "s"), st.N),
			})
		}
		stacks = append(stacks, stack)
		legend = append(legend, LegendItem{cls, mt.label})
	}
	if len(stacks) == 0 {
		return nil
	}
	return &Figure{
		Title: "Migration phase durations",
		Caption: "Percentiles across migrations that completed with a replacement " +
			"(restored, no-signal, cold-start); wedged, failed and in-flight rows are excluded. " +
			"Each metric has its own scale. All times from migration intercept (t0).",
		SVG:    template.HTML(HBarChartStacked(stacks, "s")),
		Legend: legend,
		Table:  statsTable(r.Stats),
	}
}

func seriesFigure(s analyze.Series) *Figure {
	if len(s.Points) < 2 {
		return nil
	}
	t0 := s.Points[0][0]
	var pts []LinePoint
	for _, p := range s.Points {
		pts = append(pts, LinePoint{X: (p[0] - t0) / 60, Y: p[1]})
	}
	yUnit := s.Unit
	title := s.Name
	return &Figure{
		Title:   title,
		Caption: "x axis: minutes from first sample.",
		SVG: template.HTML(LineChart([]LineSeries{{Name: s.Name, Class: "s1", Points: pts}},
			"min", yUnit)),
		Table:    seriesTable(s),
		Collapse: true,
	}
}

func sizeVsTimeFigure(r *analyze.Run) *Figure {
	var pts []LinePoint
	for _, m := range r.Migrations {
		if m.SnapshotBytes > 0 && m.E2ES > 0 {
			pts = append(pts, LinePoint{X: float64(m.SnapshotBytes), Y: m.E2ES})
		}
	}
	if len(pts) < 2 {
		return nil
	}
	// Only meaningful when sizes actually vary (e.g. the RSS ladder); for a
	// homogeneous fleet this would render a single vertical smear.
	minX, maxX := pts[0].X, pts[0].X
	for _, p := range pts {
		minX = math.Min(minX, p.X)
		maxX = math.Max(maxX, p.X)
	}
	if maxX < 3*minX {
		return nil
	}
	return &Figure{
		Title:   "Restore time vs snapshot size",
		Caption: "Each point is one migration.",
		SVG: template.HTML(LineChart([]LineSeries{{Name: "end-to-end", Class: "s1", Points: pts}},
			"bytes", "s")),
		Table: migrationsTable(r.Migrations),
	}
}

func statsTable(stats map[string]analyze.Stats) template.HTML {
	var b strings.Builder
	b.WriteString("<table><thead><tr><th>metric</th><th>n</th><th>p50</th><th>p90</th><th>p99</th><th>max</th><th>mean</th></tr></thead><tbody>")
	var keys []string
	for k := range stats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := stats[k]
		cell := func(v float64) string { return statVal(s, v) }
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>",
			esc(k), s.N, cell(s.P50), cell(s.P90), cell(s.P99), cell(s.Max), cell(s.Mean))
	}
	b.WriteString("</tbody></table>")
	return template.HTML(b.String())
}

func migrationsTable(ms []analyze.Migration) template.HTML {
	var b strings.Builder
	b.WriteString("<table><thead><tr><th>pod</th><th>app</th><th>outcome</th><th>restore signal</th>" +
		"<th>population</th><th>snapshot s</th><th>evicted s</th><th>gap s</th><th>e2e s</th>" +
		"<th>engine</th><th>snapshot</th><th>replacement</th><th>src → dst</th></tr></thead><tbody>")
	for _, m := range ms {
		sig := m.RestoreSignal
		if sig == "" {
			sig = "–"
		}
		pop := m.Population
		if pop == "" {
			pop = "measured"
		}
		fmt.Fprintf(&b, "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s → %s</td></tr>",
			esc(m.Pod), esc(m.App), esc(m.Outcome), esc(sig), esc(pop),
			dur(m.SnapReadyS), dur(m.EvictedS), dur(m.DowntimeS), dur(m.E2ES),
			esc(m.Engine), fmtBytes(float64(m.SnapshotBytes)), esc(m.DstPod),
			esc(m.SrcNode), esc(m.DstNode))
	}
	b.WriteString("</tbody></table>")
	return template.HTML(b.String())
}

func seriesTable(s analyze.Series) template.HTML {
	var b strings.Builder
	fmt.Fprintf(&b, "<table><thead><tr><th>t (unix s)</th><th>%s (%s)</th></tr></thead><tbody>", esc(s.Name), esc(s.Unit))
	for _, p := range s.Points {
		fmt.Fprintf(&b, "<tr><td>%.0f</td><td>%s</td></tr>", p[0], trimFloat(p[1]))
	}
	b.WriteString("</tbody></table>")
	return template.HTML(b.String())
}

func dur(v float64) string {
	if v < 0 {
		return "–"
	}
	return trimFloat(v)
}
