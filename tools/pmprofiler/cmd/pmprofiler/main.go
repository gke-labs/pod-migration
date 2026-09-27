// pmprofiler profiles pod-migration activity on a cluster and renders the
// results as machine-readable JSON and a self-contained HTML report.
//
//	pmprofiler collect --run runs/s1 [--gcs-bucket gs://...] [--scenario s1]
//	pmprofiler check   --run runs/s1 --name "nonce survived" --pass --group redis
//	pmprofiler analyze --run runs/s1 --out runs/s1/run.json
//	pmprofiler report  --out report.html --title "Round 2" runs/*/run.json
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/analyze"
	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/collect"
	"github.com/gke-labs/pod-migration/tools/pmprofiler/internal/report"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var err error
	switch os.Args[1] {
	case "collect":
		err = cmdCollect(os.Args[2:])
	case "check":
		err = cmdCheck(os.Args[2:])
	case "analyze":
		err = cmdAnalyze(os.Args[2:])
	case "report":
		err = cmdReport(os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "pmprofiler:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: pmprofiler <collect|check|analyze|report> [flags]")
	os.Exit(2)
}

func cmdCollect(args []string) error {
	fs := flag.NewFlagSet("collect", flag.ExitOnError)
	o := collect.Options{}
	fs.StringVar(&o.RunDir, "run", "", "run directory (required)")
	fs.StringVar(&o.Kubeconfig, "kubeconfig", "", "kubeconfig path (default: standard loading rules)")
	fs.StringVar(&o.Context, "context", "", "kubeconfig context")
	fs.StringVar(&o.Namespace, "namespace", "", "optional workload namespace filter (default: all namespaces)")
	fs.StringVar(&o.PodSelector, "pod-selector", "pod-migration.gke.io/enabled=true", "label selector for workload pods")
	fs.StringVar(&o.ControllerNS, "controller-ns", "pod-migration-system", "controller namespace (logs + metrics)")
	fs.StringVar(&o.GCSBucket, "gcs-bucket", "", "gs:// bucket holding snapshots (optional size sampling)")
	fs.DurationVar(&o.MetricsEvery, "metrics-interval", 15*time.Second, "resource-metrics sampling interval")
	fs.DurationVar(&o.GCSEvery, "gcs-interval", 60*time.Second, "GCS size sampling interval")
	fs.StringVar(&o.Scenario, "scenario", "", "scenario tag stored in meta.json")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	if o.RunDir == "" {
		return fmt.Errorf("--run is required")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	fmt.Fprintf(os.Stderr, "collecting into %s (ctrl-c to stop)\n", o.RunDir)
	return collect.Run(ctx, o)
}

func cmdCheck(args []string) error {
	fs := flag.NewFlagSet("check", flag.ExitOnError)
	run := fs.String("run", "", "run directory (required)")
	name := fs.String("name", "", "check name (required)")
	group := fs.String("group", "", "grouping key, e.g. app name")
	pass := fs.Bool("pass", false, "check passed")
	detail := fs.String("detail", "", "free-form detail")
	value := fs.Float64("value", 0, "optional numeric value")
	total := fs.Float64("total", 0, "denominator for --value (e.g. verified migrations of total attempts)")
	unit := fs.String("unit", "", "unit for --value")
	series := fs.String("series-file", "", "ingest a timeseries instead: file of '<unix-s> <value>' lines; --name/--unit apply")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	if *run == "" || *name == "" {
		return fmt.Errorf("--run and --name are required")
	}
	f, err := os.OpenFile(filepath.Join(*run, "checks.ndjson"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	if *series != "" {
		s := analyze.Series{Name: *name, Unit: *unit}
		raw, err := os.ReadFile(*series)
		if err != nil {
			return err
		}
		var t, v float64
		for _, line := range splitLines(string(raw)) {
			if _, err := fmt.Sscanf(line, "%f %f", &t, &v); err == nil {
				s.Points = append(s.Points, [2]float64{t, v})
			}
		}
		return enc.Encode(struct {
			Kind string `json:"kind"`
			analyze.Series
		}{"series", s})
	}
	return enc.Encode(struct {
		Kind string `json:"kind"`
		analyze.Check
	}{"check", analyze.Check{
		TS:   time.Now().UTC().Format(time.RFC3339),
		Name: *name, Group: *group, Pass: *pass, Detail: *detail,
		Value: *value, Total: *total, Unit: *unit,
	}})
}

func splitLines(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '\n' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func cmdAnalyze(args []string) error {
	fs := flag.NewFlagSet("analyze", flag.ExitOnError)
	o := analyze.Options{}
	out := fs.String("out", "", "output path (default <run>/run.json)")
	fs.StringVar(&o.RunDir, "run", "", "run directory (required)")
	fs.StringVar(&o.Scenario, "scenario", "", "scenario name (default: from meta.json)")
	// Default matches `pmprofiler collect` — a mismatch silently drops the
	// controller CPU/mem series from the report.
	fs.StringVar(&o.ControllerNS, "controller-ns", "pod-migration-system", "controller namespace (must match collect)")
	fs.StringVar(&o.PodPrefix, "pod-prefix", "", "only analyze migrations of pods with this name prefix")
	fs.DurationVar(&o.WedgeThreshold, "wedge-threshold", 10*time.Minute, "non-terminal PMJ age counted as wedged")
	fs.StringVar(&o.MetricsURL, "metrics-url", "", "optional HTTP URL to scrape /metrics (e.g. http://localhost:8080/metrics)")
	fs.StringVar(&o.MetricsFile, "metrics-file", "", "optional Prometheus /metrics file path")
	assertZeroInv := fs.Bool("assert-zero-invariants", true, "fail if sum(pod_migration_invariant_violations_total) > 0")
	assertCleanOutcomes := fs.Bool("assert-clean-outcomes", false, "fail on wedged, failed, no-replacement, or unintended cold-start outcomes")
	allowColdStart := fs.Bool("allow-cold-start", false, "permit cold-start outcomes when --assert-clean-outcomes is set (e.g. intentional I9 fallback)")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	if o.RunDir == "" {
		return fmt.Errorf("--run is required")
	}
	run, err := analyze.Analyze(o)
	if err != nil {
		return err
	}
	if run.Scenario == "" {
		if s, ok := run.Meta["scenario"].(string); ok {
			run.Scenario = s
		}
	}
	if run.Scenario == "" {
		run.Scenario = filepath.Base(o.RunDir)
	}
	path := *out
	if path == "" {
		path = filepath.Join(o.RunDir, "run.json")
	}
	raw, err := json.MarshalIndent(run, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "%s: %d migrations, outcomes %v, invariantViolations=%g\n",
		run.Scenario, len(run.Migrations), run.Outcomes, run.TotalInvariantViolations)
	for k, s := range run.Stats {
		if s.N > 0 {
			fmt.Fprintf(os.Stderr, "  %-12s n=%-4d p50=%.1fs p90=%.1fs p99=%.1fs max=%.1fs\n",
				k, s.N, s.P50, s.P90, s.P99, s.Max)
		}
	}
	if *assertZeroInv {
		if err := analyze.VerifyInvariants(run); err != nil {
			return err
		}
	}
	if *assertCleanOutcomes {
		if err := analyze.VerifyOutcomes(run, *allowColdStart); err != nil {
			return err
		}
	}
	return nil
}

func cmdReport(args []string) error {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	out := fs.String("out", "report.html", "output HTML path")
	title := fs.String("title", "Pod migration benchmark report", "report title")
	findings := fs.String("findings", "", "markdown file rendered as an Engineering-findings section")
	_ = fs.Parse(args) // ExitOnError: never returns an error
	if fs.NArg() == 0 {
		return fmt.Errorf("usage: pmprofiler report --out report.html <run.json> [more run.json]")
	}
	var runs []*analyze.Run
	for _, path := range fs.Args() {
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var r analyze.Run
		if err := json.Unmarshal(raw, &r); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		runs = append(runs, &r)
	}
	var findingsMD string
	if *findings != "" {
		raw, err := os.ReadFile(*findings)
		if err != nil {
			return err
		}
		findingsMD = string(raw)
	}
	if err := report.Generate(*out, *title, runs, findingsMD); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d scenarios)\n", *out, len(runs))
	return nil
}
