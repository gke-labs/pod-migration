package report

import "html/template"

// The page template. Palette values are the validated reference palette from
// the data-viz method; dark mode is its own selected steps, applied via both
// the OS media query and an explicit toggle, with the toggle winning.
//
// Navigation model: a sticky left menu (engineering findings first, then one
// entry per scenario); the main pane renders the selection with a prominent
// proof-of-value / engineering switch. Everything is pre-rendered; JS only
// toggles visibility.
var pageTmpl = template.Must(template.New("page").Parse(`{{define "figure"}}{{if .Collapse}}<details class="section">
  <summary>{{.Title}}<div class="c">{{.Caption}}</div></summary>
  <div class="body">
    {{.SVG}}
    {{if .Legend}}<div class="legend">{{range .Legend}}<span><span class="sw {{.Class}}"></span>{{.Label}}</span>{{end}}</div>{{end}}
    {{if .Table}}<details class="tableview"><summary>table view</summary>{{.Table}}</details>{{end}}
  </div>
</details>
{{else}}<figure class="viz">
  <figcaption><span class="t">{{.Title}}</span><div class="c">{{.Caption}}</div></figcaption>
  {{.SVG}}
  {{if .Legend}}<div class="legend">{{range .Legend}}<span><span class="sw {{.Class}}"></span>{{.Label}}</span>{{end}}</div>{{end}}
  {{if .Table}}<details class="tableview"><summary>table view</summary>{{.Table}}</details>{{end}}
</figure>
{{end}}{{end}}<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{{.Title}}</title>
<style>
:root {
  color-scheme: light;
  --page: #f9f9f7; --surface-1: #fcfcfb;
  --ink-1: #0b0b0b; --ink-2: #52514e; --muted: #898781;
  --grid: #e1e0d9; --axis: #c3c2b7; --ring: rgba(11,11,11,0.10);
  --series-1: #2a78d6; --series-2: #eb6834; --series-3: #1baf7a; --series-4: #eda100;
  --good: #0ca30c; --warning: #fab219; --serious: #ec835a; --critical: #d03b3b;
  --good-text: #006300; --switch-ink: #ffffff;
}
@media (prefers-color-scheme: dark) {
  :root:where(:not([data-theme="light"])) {
    color-scheme: dark;
    --page: #0d0d0d; --surface-1: #1a1a19;
    --ink-1: #ffffff; --ink-2: #c3c2b7; --muted: #898781;
    --grid: #2c2c2a; --axis: #383835; --ring: rgba(255,255,255,0.10);
    --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
    --good-text: #0ca30c;
  }
}
:root[data-theme="dark"] {
  color-scheme: dark;
  --page: #0d0d0d; --surface-1: #1a1a19;
  --ink-1: #ffffff; --ink-2: #c3c2b7; --muted: #898781;
  --grid: #2c2c2a; --axis: #383835; --ring: rgba(255,255,255,0.10);
  --series-1: #3987e5; --series-2: #d95926; --series-3: #199e70; --series-4: #c98500;
  --good-text: #0ca30c;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--page); color: var(--ink-1);
  font: 14px/1.45 system-ui, -apple-system, "Segoe UI", sans-serif; }
header { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; padding: 20px 28px 8px; }
header h1 { font-size: 20px; margin: 0; }
header .gen { color: var(--muted); font-size: 12px; }
header .spacer { flex: 1; }
#themeToggle { border: 1px solid var(--ring); background: var(--surface-1); color: var(--ink-2);
  border-radius: 8px; padding: 6px 10px; cursor: pointer; font: inherit; }

.layout { display: flex; gap: 26px; align-items: flex-start; padding: 8px 28px 48px; max-width: 1280px; margin: 0 auto; }
nav.menu { position: sticky; top: 14px; width: 250px; flex: none; display: flex; flex-direction: column; gap: 3px;
  max-height: calc(100vh - 28px); overflow-y: auto; }
nav.menu .heading { color: var(--ink-1); font-size: 12px; font-weight: 700; text-transform: uppercase;
  letter-spacing: 0.07em; margin: 16px 4px 6px; padding-top: 12px; border-top: 1px solid var(--grid); }
nav.menu .heading:first-child { border-top: 0; margin-top: 2px; padding-top: 0; }
nav.menu button { text-align: left; padding: 9px 12px; border-radius: 8px; border: 1px solid transparent;
  background: none; color: var(--ink-2); cursor: pointer; font: inherit; }
nav.menu button:hover { background: var(--surface-1); }
nav.menu button[aria-selected="true"] { background: var(--surface-1); border-color: var(--ring); color: var(--ink-1); font-weight: 600; }
nav.menu .dot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 8px; vertical-align: 1px; background: var(--muted); }
nav.menu .dot.good { background: var(--good); } nav.menu .dot.warning { background: var(--warning); }
nav.menu .dot.critical { background: var(--critical); }
nav.menu .sub { display: block; font-size: 11px; color: var(--muted); font-weight: 400; margin: 2px 0 0 16px; }

.content { flex: 1; min-width: 0; }
.tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(165px, 1fr)); gap: 12px; margin: 8px 0 16px; }
.tile { background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px; padding: 12px 14px; }
.tile .label { color: var(--ink-2); font-size: 12px; }
.tile .value { font-size: 25px; margin-top: 2px; }
.tile .note { color: var(--muted); font-size: 11px; margin-top: 2px; }
.tile.good .value::after { content: " ✓"; color: var(--good-text); font-size: 16px; }
.tile.warning .value::after { content: " !"; color: var(--serious); font-size: 16px; }
.tile.critical .value::after { content: " ✕"; color: var(--critical); font-size: 16px; }

section.run { display: none; }
section.run.active { display: block; }
section.run > h2 { font-size: 18px; margin: 4px 0 6px; }

.viewswitch { display: inline-flex; border: 1px solid var(--ring); border-radius: 999px;
  overflow: hidden; margin: 8px 0 18px; background: var(--surface-1); }
.viewswitch button { font-size: 14px; font-weight: 600; padding: 11px 26px; border: 0;
  background: none; color: var(--ink-2); cursor: pointer; font-family: inherit; }
.viewswitch button[aria-selected="true"] { background: var(--series-1); color: var(--switch-ink); }

figure.viz { background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px; margin: 14px 0; padding: 14px 16px; }
figure.viz figcaption { margin-bottom: 8px; }
figure.viz figcaption .t { font-weight: 600; }
figure.viz figcaption .c { color: var(--ink-2); font-size: 12px; margin-top: 1px; }
svg { width: 100%; height: auto; display: block; }
svg text { font: 11px system-ui, -apple-system, "Segoe UI", sans-serif; }
.grid { stroke: var(--grid); stroke-width: 1; }
.axis { stroke: var(--axis); stroke-width: 1; }
.tick { fill: var(--muted); font-variant-numeric: tabular-nums; }
.vlabel { fill: var(--ink-2); font-variant-numeric: tabular-nums; }
.dlabel { fill: var(--ink-2); font-weight: 600; }
.mark.s1, .dot.s1 { fill: var(--series-1); } .line.s1 { stroke: var(--series-1); }
.mark.s2, .dot.s2 { fill: var(--series-2); } .line.s2 { stroke: var(--series-2); }
.mark.s3, .dot.s3 { fill: var(--series-3); } .line.s3 { stroke: var(--series-3); }
.mark.s4, .dot.s4 { fill: var(--series-4); } .line.s4 { stroke: var(--series-4); }
.line { stroke-width: 2; }
.hit { fill: transparent; }
.mark:hover { opacity: 0.85; }
.legend { display: flex; gap: 14px; flex-wrap: wrap; margin-top: 8px; color: var(--ink-2); font-size: 12px; }
.legend .sw { display: inline-block; width: 10px; height: 10px; border-radius: 3px; margin-right: 5px; vertical-align: -1px; }
.sw.s1 { background: var(--series-1); } .sw.s2 { background: var(--series-2); }
.sw.s3 { background: var(--series-3); } .sw.s4 { background: var(--series-4); }
details.tableview { margin-top: 10px; }
details.tableview summary { cursor: pointer; color: var(--ink-2); font-size: 12px; }
table { border-collapse: collapse; width: 100%; margin-top: 8px; font-size: 12px; }
th, td { text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--grid); font-variant-numeric: tabular-nums; }
th { color: var(--ink-2); font-weight: 600; }
.status { font-weight: 600; }
.status.good { color: var(--good-text); } .status.good::before { content: "✓ "; }
.status.warning { color: var(--serious); } .status.warning::before { content: "! "; }
.status.critical { color: var(--critical); } .status.critical::before { content: "✕ "; }
.pill { display: inline-block; border: 1px solid var(--ring); border-radius: 999px; padding: 1px 9px; font-size: 12px; color: var(--ink-2); margin: 0 6px 6px 0; }
#tooltip { position: fixed; pointer-events: none; display: none; z-index: 10;
  background: var(--surface-1); border: 1px solid var(--axis); border-radius: 6px;
  padding: 5px 9px; font-size: 12px; color: var(--ink-1); max-width: 380px;
  box-shadow: 0 2px 10px rgba(0,0,0,0.18); }
.failure { border-left: 3px solid var(--critical); padding: 6px 12px; margin: 8px 0; background: var(--surface-1); border-radius: 0 8px 8px 0; }
.failure .k { font-weight: 600; }
.failure .ev { color: var(--muted); font-size: 11px; white-space: pre-wrap; }
details.section { background: var(--surface-1); border: 1px solid var(--ring); border-radius: 10px; margin: 14px 0; }
details.section > summary { cursor: pointer; padding: 12px 16px; font-weight: 600; }
details.section > summary .c { color: var(--ink-2); font-size: 12px; font-weight: 400; margin-top: 1px; }
details.section > .body { padding: 0 16px 14px; }
details.explainer { border-left: 3px solid var(--series-1); }
details.explainer dt { font-weight: 600; margin-top: 8px; }
details.explainer dd { margin: 2px 0 0 0; color: var(--ink-2); }
.findings-body { line-height: 1.55; }
.findings-body h2 { font-size: 16px; border-bottom: 1px solid var(--grid); padding-bottom: 4px; }
.findings-body h3 { font-size: 14px; margin-top: 18px; }
.findings-body code { background: var(--page); border: 1px solid var(--grid); border-radius: 4px; padding: 0 4px; font-size: 12px; }
.findings-body pre { background: var(--page); border: 1px solid var(--grid); border-radius: 8px; padding: 10px 12px; overflow-x: auto; font-size: 12px; }
.findings-body pre code { border: 0; padding: 0; background: none; }
@media (max-width: 900px) {
  .layout { flex-direction: column; }
  nav.menu { position: static; width: 100%; max-height: none; }
}
</style>
</head>
<body>
<header>
  <h1>{{.Title}}</h1>
  <span class="gen">generated {{.Generated}}</span>
  <span class="spacer"></span>
  <button id="themeToggle" type="button">◐ theme</button>
</header>

<div class="layout">
<nav class="menu" role="tablist" aria-label="sections">
  <div class="heading">Overview</div>
  <button role="tab" data-run="overview" aria-selected="true">Executive summary</button>
  {{if .Findings}}
  <div class="heading">Engineering findings</div>
  <button role="tab" data-run="findings" aria-selected="false"><span class="dot warning"></span>Findings &amp; proposed fixes</button>
  {{end}}
  <div class="heading">Scenarios</div>
  {{range .Runs}}
  <button role="tab" data-run="{{.Anchor}}" aria-selected="false">
    <span class="dot {{.CardStatus}}"></span>{{.Scenario}}
    <span class="sub">{{range $i, $s := .CardStats}}{{if $i}} · {{end}}{{$s.Value}} {{$s.Label}}{{end}}</span>
  </button>
  {{end}}
</nav>

<div class="content">

<section class="run" id="overview">
  <h2>Executive summary</h2>
  <div class="tiles">
  {{range .Overall}}<div class="tile {{.Status}}"><div class="label">{{.Label}}</div><div class="value">{{.Value}}</div>{{if .Note}}<div class="note">{{.Note}}</div>{{end}}</div>{{end}}
  </div>
  <figure class="viz">
    <figcaption><span class="t">Scenarios side by side</span>
    <div class="c">"State verified" is the driver's app-level token check — the authoritative number. "Restore-signaled" counts only explicit engine restore signals reconstructed from the cluster watch.</div></figcaption>
    <table>
      <thead><tr><th>scenario</th><th>attempts</th><th>state verified</th><th>restore-signaled</th><th>median gap</th><th>p90 e2e</th><th>wedged</th></tr></thead>
      <tbody>
      {{range .Runs}}<tr><td><span class="dot {{.CardStatus}}" style="display:inline-block;width:8px;height:8px;border-radius:50%"></span> {{.Scenario}}</td><td>{{.Attempts}}</td><td>{{.StateVerified}}</td><td>{{.RestoredRate}}</td><td>{{.MedianGap}}</td><td>{{.P90E2E}}</td><td>{{.WedgedN}}</td></tr>{{end}}
      </tbody>
    </table>
  </figure>
  <details class="section explainer">
    <summary>How to read this report</summary>
    <div class="body"><dl>
      <dt>State verified / token check</dt><dd>The scenario driver plants a state token (e.g. a redis nonce, a monotonic counter with an instance ID) in the pod before migration and checks it in the replacement. SURVIVED means the token crossed intact — the strongest evidence of a live migration. SERVED means the replacement answered requests; used for apps that hold no verifiable state by design (nginx, memcached).</dd>
      <dt>Restore-signaled</dt><dd>The migration engine explicitly annotated the replacement pod with its restore outcome. Counted from the cluster watch; weaker than a token check (it is the engine grading its own work).</dd>
      <dt>Replaced (no signal)</dt><dd>The migration finished and a replacement pod appeared, but no engine signal proves the state was restored. Not counted as a restore.</dd>
      <dt>Service gap</dt><dd>Seconds between the source pod being deleted and the replacement reporting Ready. If the replacement was Ready before the source disappeared, the gap is 0.</dd>
      <dt>Snapshot ready / checkpoint + upload</dt><dd>Time from migration start until the checkpoint was taken and durably uploaded — only then is the source allowed to be evicted ("evict-allowed").</dd>
      <dt>Evict window</dt><dd>Time from snapshot-ready until the source pod was actually deleted.</dd>
      <dt>Wedged</dt><dd>The migration stopped making phase progress for longer than the threshold (default 10 min) before the run ended.</dd>
      <dt>No replacement</dt><dd>The migration reported success but no replacement pod was observed inside the migration window.</dd>
      <dt>Populations</dt><dd>"Measured" migrations happened while the collector watched. Objects created before collection started (pre-collection) or stale leftover CRs cannot be reconstructed and are listed but excluded from every headline number. Warmup migrations that ran before the driver's recorded cycles are measured but not driver-verified.</dd>
    </dl></div>
  </details>
</section>

{{if .Findings}}
<section class="run" id="findings">
  <h2>Engineering findings</h2>
  <figure class="viz findings-body">{{.Findings}}</figure>
</section>
{{end}}

{{range $i, $r := .Runs}}
<section class="run" id="{{$r.Anchor}}">
  <h2>{{$r.Scenario}}</h2>
  <div class="tiles">
  {{range $r.Summary}}<div class="tile {{.Status}}"><div class="label">{{.Label}}</div><div class="value">{{.Value}}</div>{{if .Note}}<div class="note">{{.Note}}</div>{{end}}</div>{{end}}
  </div>
  <div class="viewswitch" role="tablist">
    <button role="tab" aria-selected="true" data-view="value">Proof of value</button>
    <button role="tab" aria-selected="false" data-view="eng">Engineering data</button>
  </div>

  <div data-pane="value">
    {{if $r.Checks}}
    <figure class="viz">
      <figcaption><span class="t">Verified application checks</span>
      <div class="c">Application-level state verification performed by the scenario driver, independent of Kubernetes status. This is the authoritative state-survival number.</div></figcaption>
      <table>
        <thead><tr><th>check</th><th>group</th><th>result</th><th>verified</th><th>detail</th></tr></thead>
        <tbody>
        {{range $r.Checks}}<tr><td>{{.Name}}</td><td>{{.Group}}</td><td>{{if .Pass}}<span class="status good">pass</span>{{else}}<span class="status critical">fail</span>{{end}}</td><td>{{if .Total}}{{.Value}} of {{.Total}}{{else}}–{{end}}</td><td>{{.Detail}}</td></tr>{{end}}
        </tbody>
      </table>
    </figure>
    {{end}}
    {{if $r.AppRows}}
    <figure class="viz">
      <figcaption><span class="t">Outcome by application</span>
      <div class="c">Terminal attempts per app. "Restored" requires an explicit engine restore signal; "replaced" means a replacement appeared but no signal proves the state came back. "Driver verified" joins the scenario driver's own checks.</div></figcaption>
      <table>
        <thead><tr><th>application</th><th>attempts</th><th>restored</th><th>replaced (no signal)</th><th>cold start</th><th>failed</th><th>no replacement</th><th>wedged</th><th>restore-signaled</th><th>driver verified</th></tr></thead>
        <tbody>
        {{range $r.AppRows}}<tr><td>{{.App}}</td><td>{{.Total}}</td><td>{{.Restored}}</td><td>{{.Replaced}}</td><td>{{.Cold}}</td><td>{{.Failed}}</td><td>{{.NoRepl}}</td><td>{{.Wedged}}</td><td><span class="status {{.Status}}">{{.Rate}}</span></td><td>{{.Verified}}</td></tr>{{end}}
        </tbody>
      </table>
    </figure>
    {{end}}
    {{range $r.ValueFigs}}
    {{template "figure" .}}
    {{end}}
  </div>

  <div data-pane="eng" hidden>
    <p>
      {{range $k, $v := $r.Outcomes}}<span class="pill">{{$k}}: {{$v}}</span>{{end}}
      <span class="pill">controller max cpu: {{$r.Controller.MaxCPUMilli}}m</span>
      <span class="pill">controller log errors: {{$r.Controller.LogErrors}}</span>
      <span class="pill">controller retries: {{$r.Controller.LogRetries}}</span>
    </p>
    {{range $r.Figures}}
    {{template "figure" .}}
    {{end}}
    {{if $r.Failures}}
    <details class="section">
      <summary>Failure drill-down ({{len $r.Failures}})<div class="c">Per-failure kind, last phase and joined warning events.</div></summary>
      <div class="body">
      {{range $r.Failures}}
      <div class="failure">
        <div><span class="k">{{.Kind}}</span> · {{.PMJ}}{{if .Pod}} · pod {{.Pod}}{{end}}{{if .Phase}} · last phase {{.Phase}}{{end}}</div>
        {{if .Events}}<div class="ev">{{range .Events}}{{.}}
{{end}}</div>{{end}}
      </div>
      {{end}}
      </div>
    </details>
    {{end}}
  </div>
</section>
{{end}}

</div>
</div>
<div id="tooltip"></div>
<script>
(function () {
  var toggle = document.getElementById('themeToggle');
  toggle.addEventListener('click', function () {
    var root = document.documentElement;
    var dark = root.dataset.theme === 'dark' ||
      (!root.dataset.theme && matchMedia('(prefers-color-scheme: dark)').matches);
    root.dataset.theme = dark ? 'light' : 'dark';
  });

  var items = document.querySelectorAll('nav.menu [data-run]');
  function select(id) {
    items.forEach(function (o) { o.setAttribute('aria-selected', o.dataset.run === id); });
    document.querySelectorAll('section.run').forEach(function (s) {
      s.classList.toggle('active', s.id === id);
    });
    window.scrollTo({ top: 0 });
  }
  items.forEach(function (b) {
    b.addEventListener('click', function () { select(b.dataset.run); });
  });
  select('overview');

  document.querySelectorAll('section.run').forEach(function (sec) {
    var tabs = sec.querySelectorAll('.viewswitch [role=tab]');
    tabs.forEach(function (b) {
      b.addEventListener('click', function () {
        tabs.forEach(function (o) { o.setAttribute('aria-selected', o === b); });
        sec.querySelectorAll('[data-pane]').forEach(function (p) {
          p.hidden = p.dataset.pane !== b.dataset.view;
        });
      });
    });
  });

  var tip = document.getElementById('tooltip');
  document.addEventListener('mousemove', function (e) {
    var t = e.target.closest ? e.target.closest('[data-tip]') : null;
    if (t && t.getAttribute('data-tip')) {
      tip.textContent = t.getAttribute('data-tip');
      tip.style.display = 'block';
      var x = Math.min(e.clientX + 14, innerWidth - tip.offsetWidth - 8);
      var y = Math.min(e.clientY + 14, innerHeight - tip.offsetHeight - 8);
      tip.style.left = x + 'px'; tip.style.top = y + 'px';
    } else {
      tip.style.display = 'none';
    }
  });
})();
</script>
</body>
</html>
`))
