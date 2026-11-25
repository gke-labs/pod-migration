// SPDX-License-Identifier: Apache-2.0
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

package metrics

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"gopkg.in/yaml.v3"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

type prometheusRuleFile struct {
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`
	Metadata   struct {
		Name      string `yaml:"name"`
		Namespace string `yaml:"namespace"`
	} `yaml:"metadata"`
	Spec struct {
		Groups []struct {
			Name  string `yaml:"name"`
			Rules []struct {
				Alert       string            `yaml:"alert"`
				Expr        string            `yaml:"expr"`
				For         string            `yaml:"for"`
				Labels      map[string]string `yaml:"labels"`
				Annotations map[string]string `yaml:"annotations"`
			} `yaml:"rules"`
		} `yaml:"groups"`
	} `yaml:"spec"`
}

var (
	// metricSelectorRegex extracts metric names appearing before { or [
	metricSelectorRegex = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(\{|\[)`)
	// podMigrationTokenRegex matches any pod_migration metric identifier, even without { or [
	podMigrationTokenRegex = regexp.MustCompile(`\b(pod_migration_[a-zA-Z0-9_]+)\b`)
	// labelMatcherRegex matches label matches like outcome=~"timeout|budget_timeout" or outcome="failed"
	labelMatcherRegex = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|=|!=|!~)\s*"([^"]+)"`)
	// labelFilterRegex matches expressions like metric_name{...}
	labelFilterRegex = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^}]+)\}`)
)

// initSampleMetrics touches all registered vectors with dummy label values so that
// ctrlmetrics.Registry.Gather() collects their descriptors and label names.
func initSampleMetrics() {
	OutcomesTotal.WithLabelValues("succeeded")
	RestoreVerdictsTotal.WithLabelValues("gke_pod_snapshots", "confirmed")
	PhaseDurationSeconds.WithLabelValues("restoring")
	InvariantViolationsTotal.WithLabelValues("I1")
	SpotPreemptionSkippedTotal.WithLabelValues("not_eligible")
	BudgetChecksTotal.WithLabelValues("allowed")
	FenceWebhookRequestsTotal.WithLabelValues("admitted")
	SourceFenceTotal.WithLabelValues("fenced")
	SourceFencedSeconds.WithLabelValues("completed")
	SkippedMigrationsTotal.WithLabelValues("not_migratable", "none")
	MigratableCoveredRunningPods.WithLabelValues("default")
	MigratableTotalRunningPods.WithLabelValues("default")
	MigratableCoverageRatio.WithLabelValues("default")
	ApplicationDowntimeSeconds.WithLabelValues("gke_pod_snapshots", "warm")
}

func TestPrometheusRulesIntegrity(t *testing.T) {
	initSampleMetrics()

	rulePath := filepath.Join("..", "..", "..", "monitoring", "prometheus-rules.yaml")
	data, err := os.ReadFile(rulePath)
	if err != nil {
		t.Fatalf("failed to read %s: %v", rulePath, err)
	}

	var pr prometheusRuleFile
	if err := yaml.Unmarshal(data, &pr); err != nil {
		t.Fatalf("failed to parse %s: %v", rulePath, err)
	}

	if pr.APIVersion != "monitoring.coreos.com/v1" {
		t.Errorf("expected apiVersion monitoring.coreos.com/v1, got %q", pr.APIVersion)
	}
	if pr.Kind != "PrometheusRule" {
		t.Errorf("expected kind PrometheusRule, got %q", pr.Kind)
	}
	if len(pr.Spec.Groups) == 0 {
		t.Fatal("expected at least one rule group")
	}

	// Dynamically gather registered controller metrics and their label names from ctrlmetrics.Registry
	mfs, err := ctrlmetrics.Registry.Gather()
	if err != nil {
		t.Fatalf("failed to gather from ctrlmetrics.Registry: %v", err)
	}

	registryMetrics := make(map[string]map[string]bool)
	for _, mf := range mfs {
		if mf.Name == nil {
			continue
		}
		mName := *mf.Name
		if !strings.HasPrefix(mName, "pod_migration_") {
			continue
		}
		labels := make(map[string]bool)
		if len(mf.Metric) > 0 {
			for _, l := range mf.Metric[0].Label {
				if l.Name != nil {
					labels[*l.Name] = true
				}
			}
		}
		if mf.GetType() == dto.MetricType_HISTOGRAM {
			labels["le"] = true
		}
		registryMetrics[mName] = labels
	}

	knownExternalMetrics := map[string]map[string]bool{
		"up": {
			"job": true,
		},
		"kube_pod_status_reason": {
			"reason":    true,
			"namespace": true,
			"pod":       true,
		},
	}

	knownLabelValues := map[string]map[string]bool{
		"pod_migration_outcomes_total:outcome": {
			"succeeded":                 true,
			"failed":                    true,
			"timeout":                   true,
			"budget_timeout":            true,
			"fallback":                  true,
			"succeeded_without_restore": true,
			"engine_not_registered":     true,
			"restore_lost":              true,
			"deleted":                   true,
		},
		"pod_migration_restore_verdicts_total:verdict": {
			"confirmed":          true,
			"inferred":           true,
			"cold_start":         true,
			"failed":             true,
			"lost":               true,
			"timeout":            true,
			"no_compatible_node": true,
			"placement_timeout":  true,
			"snapshot_deleting":  true,
			"snapshot_gone":      true,
		},
		"pod_migration_budget_checks_total:result": {
			"allowed":          true,
			"budget_exhausted": true,
			"in_flight":        true,
			"check_error":      true,
		},
		"pod_migration_source_fence_total:action": {
			"fenced":   true,
			"unfenced": true,
			"kept":     true,
			"refenced": true,
		},
		"pod_migration_phase_duration_seconds:phase": {
			"pending":      true,
			"snapshotting": true,
			"evicting":     true,
			"restoring":    true,
		},
		"kube_pod_status_reason:reason": {
			"SchedulingGated": true,
		},
	}

	validSeverities := map[string]bool{
		"critical": true,
		"warning":  true,
		"info":     true,
	}

	promqlKeywords := map[string]bool{
		"sum":                true,
		"rate":               true,
		"histogram_quantile": true,
		"by":                 true,
		"without":            true,
		"clamp_min":          true,
		"clamp_max":          true,
		"absent":             true,
		"and":                true,
		"or":                 true,
		"unless":             true,
		"on":                 true,
		"ignoring":           true,
		"group_left":         true,
		"group_right":        true,
		"bool":               true,
		"count":              true,
		"min":                true,
		"max":                true,
		"avg":                true,
		"le":                 true,
	}

	totalRules := 0
	foundControllerMetrics := make(map[string]int)

	for _, group := range pr.Spec.Groups {
		for _, rule := range group.Rules {
			totalRules++
			if rule.Alert == "" {
				t.Errorf("group %q: found rule without alert name", group.Name)
				continue
			}
			t.Run(rule.Alert, func(t *testing.T) {
				if strings.TrimSpace(rule.Expr) == "" {
					t.Errorf("alert %q: expr is empty", rule.Alert)
				}
				if rule.For == "" {
					t.Errorf("alert %q: for duration is missing", rule.Alert)
				}
				sev := rule.Labels["severity"]
				if !validSeverities[sev] {
					t.Errorf("alert %q: invalid or missing severity label %q", rule.Alert, sev)
				}
				if rule.Annotations["summary"] == "" {
					t.Errorf("alert %q: missing summary annotation", rule.Alert)
				}
				if rule.Annotations["description"] == "" {
					t.Errorf("alert %q: missing description annotation", rule.Alert)
				}
				runbookURL := rule.Annotations["runbook_url"]
				if runbookURL == "" {
					t.Errorf("alert %q: missing runbook_url annotation", rule.Alert)
				} else if strings.Contains(runbookURL, "docs/runbooks/") {
					idx := strings.Index(runbookURL, "docs/runbooks/")
					relDoc := runbookURL[idx:]
					fullDocPath := filepath.Join("..", "..", "..", relDoc)
					if _, err := os.Stat(fullDocPath); err != nil {
						t.Errorf("alert %q: runbook file %s does not exist: %v", rule.Alert, fullDocPath, err)
					}
				}

				// Extract and validate all metric identifiers in the PromQL expr
				for _, match := range metricSelectorRegex.FindAllStringSubmatch(rule.Expr, -1) {
					metricName := match[1]
					if promqlKeywords[metricName] {
						continue
					}
					if strings.HasPrefix(metricName, "pod_migration_") {
						baseMetric := strings.TrimSuffix(metricName, "_bucket")
						baseMetric = strings.TrimSuffix(baseMetric, "_count")
						baseMetric = strings.TrimSuffix(baseMetric, "_sum")
						if _, ok := registryMetrics[baseMetric]; !ok {
							t.Errorf("alert %q: expr references unknown pod_migration metric %q (base %q) not registered in ctrlmetrics.Registry", rule.Alert, metricName, baseMetric)
						} else {
							foundControllerMetrics[baseMetric]++
						}
					} else if _, ok := knownExternalMetrics[metricName]; ok {
						// Known external metric
					} else {
						t.Errorf("alert %q: expr references unlisted external metric or unknown symbol %q", rule.Alert, metricName)
					}
				}

				// Also check any standalone pod_migration_* identifiers (even without { or [)
				for _, match := range podMigrationTokenRegex.FindAllStringSubmatch(rule.Expr, -1) {
					metricName := match[1]
					baseMetric := strings.TrimSuffix(metricName, "_bucket")
					baseMetric = strings.TrimSuffix(baseMetric, "_count")
					baseMetric = strings.TrimSuffix(baseMetric, "_sum")
					if _, ok := registryMetrics[baseMetric]; !ok {
						t.Errorf("alert %q: expr contains bare unknown pod_migration metric %q (base %q) not registered in ctrlmetrics.Registry", rule.Alert, metricName, baseMetric)
					}
				}

				// Validate label selectors against known label names from the registry and known label values
				for _, m := range labelFilterRegex.FindAllStringSubmatch(rule.Expr, -1) {
					rawMetric := m[1]
					baseMetric := strings.TrimSuffix(rawMetric, "_bucket")
					baseMetric = strings.TrimSuffix(baseMetric, "_count")
					baseMetric = strings.TrimSuffix(baseMetric, "_sum")
					for _, lm := range labelMatcherRegex.FindAllStringSubmatch(m[2], -1) {
						lbl := lm[1]
						val := lm[3]

						// Check label name validity against the live registry
						if strings.HasPrefix(baseMetric, "pod_migration_") {
							if validLabels, ok := registryMetrics[baseMetric]; ok {
								if !validLabels[lbl] {
									t.Errorf("alert %q: metric %q has unknown label name %q (valid labels from registry: %v)", rule.Alert, rawMetric, lbl, validLabels)
								}
							}
						} else if validLabels, ok := knownExternalMetrics[baseMetric]; ok {
							if !validLabels[lbl] {
								t.Errorf("alert %q: external metric %q has unknown label name %q (valid labels: %v)", rule.Alert, rawMetric, lbl, validLabels)
							}
						}

						// Check label values against known domain values
						key := baseMetric + ":" + lbl
						if validVals, ok := knownLabelValues[key]; ok {
							for _, subVal := range strings.Split(val, "|") {
								subVal = strings.TrimSpace(subVal)
								if subVal != "" && !validVals[subVal] {
									t.Errorf("alert %q: metric %q has unknown label value %q for label %q", rule.Alert, baseMetric, subVal, lbl)
								}
							}
						}
					}
				}
			})
		}
	}

	if totalRules < 10 {
		t.Errorf("expected at least 10 alerts in prometheus-rules.yaml, got %d", totalRules)
	}

	if len(foundControllerMetrics) < 5 {
		t.Errorf("expected at least 5 distinct pod_migration metrics verified in rules, found %d: %v", len(foundControllerMetrics), foundControllerMetrics)
	}
}
