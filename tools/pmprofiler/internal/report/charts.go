package report

// SVG chart builders. All charts are rendered server-side into inline SVG so
// the report is a single self-contained file that works offline. Colors are
// referenced by CSS custom property (var(--series-N), chrome tokens) so the
// light/dark theme swap happens purely in CSS.

import (
	"fmt"
	"html"
	"math"
	"sort"
	"strings"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/analyze"
)

const (
	chartW = 860.0
	padT   = 12.0
	padB   = 34.0
	rowH   = 16.0 // bar/gantt row height
	rowGap = 4.0
)

// leftPad sizes the label gutter to the widest row label (~6.2px/char at 11px).
func leftPad(labels []string) float64 {
	maxLen := 0
	for _, l := range labels {
		if len(l) > maxLen {
			maxLen = len(l)
		}
	}
	p := float64(maxLen)*6.2 + 14
	if p < 56 {
		p = 56
	}
	if p > 230 {
		p = 230
	}
	return p
}

// niceTicks returns ~n rounded tick values covering [0, max].
func niceTicks(max float64, n int) []float64 {
	if max <= 0 {
		return []float64{0, 1}
	}
	raw := max / float64(n)
	mag := math.Pow(10, math.Floor(math.Log10(raw)))
	var step float64
	for _, m := range []float64{1, 2, 5, 10} {
		if raw <= m*mag {
			step = m * mag
			break
		}
	}
	var ticks []float64
	for v := 0.0; v <= max+step/2; v += step {
		ticks = append(ticks, v)
	}
	return ticks
}

func fmtVal(v float64, unit string) string {
	switch unit {
	case "bytes":
		return fmtBytes(v)
	case "s":
		if v >= 60 {
			return fmt.Sprintf("%dm%02ds", int(v)/60, int(v)%60)
		}
		return trimFloat(v) + "s"
	default:
		s := trimFloat(v)
		if unit != "" {
			return s + " " + unit
		}
		return s
	}
}

func fmtBytes(v float64) string {
	for _, u := range []struct {
		f float64
		n string
	}{{1 << 30, "GiB"}, {1 << 20, "MiB"}, {1 << 10, "KiB"}} {
		if v >= u.f {
			return fmt.Sprintf("%.1f %s", v/u.f, u.n)
		}
	}
	return fmt.Sprintf("%.0f B", v)
}

func trimFloat(v float64) string {
	if v == math.Trunc(v) && math.Abs(v) < 1e6 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func esc(s string) string { return html.EscapeString(s) }

// gridAndAxisX renders vertical gridlines + x tick labels for a [0,max] scale.
func gridAndAxisX(b *strings.Builder, ticks []float64, max, plotH, padL, padR float64, unit string) {
	for _, t := range ticks {
		x := padL + (chartW-padL-padR)*t/max
		fmt.Fprintf(b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="grid"/>`,
			x, padT, x, padT+plotH)
		fmt.Fprintf(b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="middle">%s</text>`,
			x, padT+plotH+16, esc(fmtVal(t, unit)))
	}
}

// BarGroup is one labeled bar in a horizontal bar chart.
type BarGroup struct {
	Label string
	Value float64
	Class string // css class carrying fill, e.g. "s1"
	Tip   string
}

// HBarChart renders a horizontal bar chart (thin bars, rounded data-end).
func HBarChart(bars []BarGroup, unit string) string {
	if len(bars) == 0 {
		return ""
	}
	var max float64
	for _, b := range bars {
		if b.Value > max {
			max = b.Value
		}
	}
	ticks := niceTicks(max, 5)
	max = ticks[len(ticks)-1]
	var labels []string
	for _, bar := range bars {
		labels = append(labels, bar.Label)
	}
	padL, padR := leftPad(labels), 64.0
	plotH := float64(len(bars))*(rowH+rowGap) + rowGap
	h := padT + plotH + padB
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img">`, chartW, h)
	gridAndAxisX(&b, ticks, max, plotH, padL, padR, unit)
	for i, bar := range bars {
		y := padT + rowGap + float64(i)*(rowH+rowGap)
		w := (chartW - padL - padR) * bar.Value / max
		if w < 1 {
			w = 1
		}
		cls := bar.Class
		if cls == "" {
			cls = "s1"
		}
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="end">%s</text>`,
			padL-8, y+rowH-4, esc(bar.Label))
		fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="3" class="mark %s" data-tip="%s"/>`,
			padL, y, w, rowH, cls, esc(bar.Tip))
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="vlabel">%s</text>`,
			padL+w+6, y+rowH-4, esc(fmtVal(bar.Value, unit)))
	}
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="axis"/>`,
		padL, padT, padL, padT+plotH)
	b.WriteString(`</svg>`)
	return b.String()
}

// BarStack is one independently-scaled group in a stacked bar chart.
type BarStack struct {
	Title string
	Bars  []BarGroup
}

// HBarChartStacked renders several bar groups in one SVG, each group with
// its OWN x scale and axis. Used for phase-duration percentiles where a
// shared linear scale would compress second-scale medians into 1px slivers
// next to a long tail.
func HBarChartStacked(stacks []BarStack, unit string) string {
	var labels []string
	n := 0
	for _, st := range stacks {
		for _, bar := range st.Bars {
			labels = append(labels, bar.Label)
			n++
		}
	}
	if n == 0 {
		return ""
	}
	padL, padR := leftPad(labels), 64.0
	const titleH, axisH, stackGap = 18.0, 22.0, 14.0
	h := padT
	for _, st := range stacks {
		h += titleH + float64(len(st.Bars))*(rowH+rowGap) + rowGap + axisH + stackGap
	}
	h += padB - stackGap
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img">`, chartW, h)
	y := padT
	for _, st := range stacks {
		var max float64
		for _, bar := range st.Bars {
			if bar.Value > max {
				max = bar.Value
			}
		}
		ticks := niceTicks(max, 5)
		max = ticks[len(ticks)-1]
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="dlabel">%s</text>`, padL, y+12, esc(st.Title))
		y += titleH
		plotH := float64(len(st.Bars))*(rowH+rowGap) + rowGap
		for _, t := range ticks {
			x := padL + (chartW-padL-padR)*t/max
			fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="grid"/>`,
				x, y, x, y+plotH)
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="middle">%s</text>`,
				x, y+plotH+14, esc(fmtVal(t, unit)))
		}
		for i, bar := range st.Bars {
			by := y + rowGap + float64(i)*(rowH+rowGap)
			w := (chartW - padL - padR) * bar.Value / max
			if w < 1 {
				w = 1
			}
			cls := bar.Class
			if cls == "" {
				cls = "s1"
			}
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="end">%s</text>`,
				padL-8, by+rowH-4, esc(bar.Label))
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="3" class="mark %s" data-tip="%s"/>`,
				padL, by, w, rowH, cls, esc(bar.Tip))
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="vlabel">%s</text>`,
				padL+w+6, by+rowH-4, esc(fmtVal(bar.Value, unit)))
		}
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="axis"/>`,
			padL, y, padL, y+plotH)
		y += plotH + axisH + stackGap
	}
	b.WriteString(`</svg>`)
	return b.String()
}

// ganttPhase maps a migration phase segment to a categorical slot.
type ganttSeg struct {
	from, to float64
	class    string
	name     string
}

// GanttChart renders per-migration phase timelines. Rows are sorted by e2e
// descending; if more than maxRows migrations exist, the slowest maxRows are
// shown. The time domain is capped at the p95 of the plotted rows' spans so
// one outlier cannot compress every other row to invisibility; capS > 0
// reports the applied cap (caller must note it, full data lives in the
// table view).
func GanttChart(ms []analyze.Migration, maxRows int) (svg string, shown, total int, capS float64) {
	var rows []analyze.Migration
	for _, m := range ms {
		if m.E2ES > 0 || m.SnapReadyS > 0 || m.EvictedS > 0 {
			rows = append(rows, m)
		}
	}
	total = len(rows)
	sort.Slice(rows, func(a, b int) bool { return rows[a].E2ES > rows[b].E2ES })
	if len(rows) > maxRows {
		rows = rows[:maxRows]
	}
	shown = len(rows)
	if shown == 0 {
		return "", 0, total, 0
	}
	spans := make([]float64, 0, len(rows))
	var max float64
	for _, m := range rows {
		var span float64
		for _, v := range []float64{m.E2ES, m.EvictedS, m.SnapReadyS} {
			if v > span {
				span = v
			}
		}
		spans = append(spans, span)
		if span > max {
			max = span
		}
	}
	sort.Float64s(spans)
	p95 := spans[int(math.Ceil(0.95*float64(len(spans))))-1]
	if p95 > 0 && max > p95 {
		capS = p95
		max = p95
	}
	ticks := niceTicks(max, 6)
	max = ticks[len(ticks)-1]
	var labels []string
	for _, m := range rows {
		labels = append(labels, m.Pod)
	}
	padL, padR := leftPad(labels), 24.0
	plotH := float64(len(rows))*(rowH+rowGap) + rowGap
	h := padT + plotH + padB
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img">`, chartW, h)
	gridAndAxisX(&b, ticks, max, plotH, padL, padR, "s")
	xOf := func(v float64) float64 { return padL + (chartW-padL-padR)*v/max }
	for i, m := range rows {
		y := padT + rowGap + float64(i)*(rowH+rowGap)
		label := m.Pod
		if len(label) > 34 {
			label = label[:33] + "…"
		}
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="end">%s</text>`,
			padL-8, y+rowH-4, esc(label))
		var segs []ganttSeg
		snap := m.SnapReadyS
		ev := m.EvictedS
		e2e := m.E2ES
		// Snapshot Ready already includes the verified upload (the engine
		// sets Ready only after it), so s1 is checkpoint+upload and s2 is
		// purely the eviction window.
		if snap > 0 {
			segs = append(segs, ganttSeg{0, snap, "s1", "checkpoint + upload"})
		}
		if ev > snap && snap >= 0 {
			segs = append(segs, ganttSeg{math.Max(snap, 0), ev, "s2", "evict window"})
		}
		if e2e > 0 {
			start := math.Max(ev, math.Max(snap, 0))
			if e2e > start {
				segs = append(segs, ganttSeg{start, e2e, "s3", "restore to Ready"})
			}
		}
		for _, s := range segs {
			// Clip drawing at the (possibly capped) domain; the tooltip
			// keeps the real values.
			from, to := math.Min(s.from, max), math.Min(s.to, max)
			w := xOf(to) - xOf(from) - 2 // 2px surface gap between segments
			if w < 1 {
				w = 1
			}
			tip := fmt.Sprintf("%s · %s: %s → %s (%.1fs)", m.Pod, s.name,
				fmtVal(s.from, "s"), fmtVal(s.to, "s"), s.to-s.from)
			if s.to > max {
				tip += " · clipped at domain cap"
			}
			fmt.Fprintf(&b, `<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" rx="3" class="mark %s" data-tip="%s"/>`,
				xOf(from), y, w, rowH, s.class, esc(tip))
		}
	}
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="axis"/>`,
		padL, padT, padL, padT+plotH)
	b.WriteString(`</svg>`)
	return b.String(), shown, total, capS
}

// LinePoint is one point of a line chart series.
type LinePoint struct{ X, Y float64 }

// LineSeries is one named series.
type LineSeries struct {
	Name   string
	Class  string
	Points []LinePoint
}

// LineChart renders a multi-series line chart. xUnit/yUnit drive tick labels.
func LineChart(series []LineSeries, xUnit, yUnit string) string {
	var maxX, maxY float64
	n := 0
	for _, s := range series {
		for _, p := range s.Points {
			if p.X > maxX {
				maxX = p.X
			}
			if p.Y > maxY {
				maxY = p.Y
			}
			n++
		}
	}
	if n == 0 {
		return ""
	}
	xt := niceTicks(maxX, 6)
	yt := niceTicks(maxY, 4)
	maxX, maxY = xt[len(xt)-1], yt[len(yt)-1]
	plotH := 220.0
	h := padT + plotH + padB
	var yLabels []string
	for _, t := range yt {
		yLabels = append(yLabels, fmtVal(t, yUnit))
	}
	padL, padR := leftPad(yLabels), 24.0
	xOf := func(v float64) float64 { return padL + (chartW-padL-padR)*v/maxX }
	yOf := func(v float64) float64 { return padT + plotH - plotH*v/maxY }
	var b strings.Builder
	fmt.Fprintf(&b, `<svg viewBox="0 0 %.0f %.0f" role="img">`, chartW, h)
	for _, t := range yt {
		fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="grid"/>`,
			padL, yOf(t), chartW-padR, yOf(t))
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="end">%s</text>`,
			padL-8, yOf(t)+4, esc(fmtVal(t, yUnit)))
	}
	for _, t := range xt {
		fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="tick" text-anchor="middle">%s</text>`,
			xOf(t), padT+plotH+16, esc(fmtVal(t, xUnit)))
	}
	for _, s := range series {
		pts := append([]LinePoint(nil), s.Points...)
		sort.Slice(pts, func(a, b int) bool { return pts[a].X < pts[b].X })
		var path strings.Builder
		for i, p := range pts {
			cmd := "L"
			if i == 0 {
				cmd = "M"
			}
			fmt.Fprintf(&path, "%s%.1f %.1f ", cmd, xOf(p.X), yOf(p.Y))
		}
		fmt.Fprintf(&b, `<path d="%s" class="line %s" fill="none"/>`, path.String(), s.Class)
		for _, p := range pts {
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="8" class="hit" data-tip="%s"/>`,
				xOf(p.X), yOf(p.Y),
				esc(fmt.Sprintf("%s · %s: %s", s.Name, fmtVal(p.X, xUnit), fmtVal(p.Y, yUnit))))
			fmt.Fprintf(&b, `<circle cx="%.1f" cy="%.1f" r="2.5" class="dot %s"/>`,
				xOf(p.X), yOf(p.Y), s.Class)
		}
		if len(pts) > 0 {
			last := pts[len(pts)-1]
			lx, anchor := xOf(last.X)+6, "start"
			if lx > chartW-90 { // keep the label inside the viewBox
				lx, anchor = xOf(last.X)-6, "end"
			}
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" class="dlabel" text-anchor="%s">%s</text>`,
				lx, yOf(last.Y)-8, anchor, esc(s.Name))
		}
	}
	fmt.Fprintf(&b, `<line x1="%.1f" y1="%.1f" x2="%.1f" y2="%.1f" class="axis"/>`,
		padL, padT+plotH, chartW-padR, padT+plotH)
	b.WriteString(`</svg>`)
	return b.String()
}
