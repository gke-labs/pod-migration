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

package metrics_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
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
	metricRegex = regexp.MustCompile(`[a-zA-Z_:][a-zA-Z0-9_:]*`)
	// labelMatcherRegex matches label matches like outcome=~"timeout|budget_timeout" or outcome="failed"
	labelMatcherRegex = regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*)\s*(=~|=|!=|!~)\s*"([^"]+)"`)
	// labelFilterRegex matches expressions like metric_name{...}
	labelFilterRegex = regexp.MustCompile(`([a-zA-Z_:][a-zA-Z0-9_:]*)\s*\{([^}]+)\}`)
)

func TestPrometheusRulesIntegrity(t *testing.T) {
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

	knownControllerMetrics := map[string]bool{
		"pod_migration_active":                          true,
		"pod_migration_outcomes_total":                  true,
		"pod_migration_restore_verdicts_total":          true,
		"pod_migration_phase_duration_seconds":          true,
		"pod_migration_restore_crash_fallback_total":    true,
		"pod_migration_restore_crash_unmatched_total":   true,
		"pod_migration_invariant_violations_total":      true,
		"pod_migration_spot_preemption_triggered_total": true,
		"pod_migration_spot_preemption_skipped_total":   true,
		"pod_migration_budget_checks_total":             true,
		"pod_migration_fence_webhook_requests_total":    true,
		"pod_migration_source_fence_total":              true,
		"pod_migration_source_fenced_seconds":           true,
		"pod_migration_application_downtime_seconds":    true,
		"pod_migration_skipped_total":                   true,
		"pod_migration_covered_running_pods":            true,
		"pod_migration_total_running_pods":              true,
		"pod_migration_coverage_ratio":                  true,
	}

	knownExternalMetrics := map[string]bool{
		"up":                     true,
		"kube_pod_status_reason": true,
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
				for _, match := range metricRegex.FindAllString(rule.Expr, -1) {
					if promqlKeywords[match] {
						continue
					}
					if strings.HasPrefix(match, "pod_migration_") {
						baseMetric := strings.TrimSuffix(match, "_bucket")
						baseMetric = strings.TrimSuffix(baseMetric, "_count")
						baseMetric = strings.TrimSuffix(baseMetric, "_sum")
						if !knownControllerMetrics[baseMetric] {
							t.Errorf("alert %q: expr references unknown pod_migration metric %q (base %q)", rule.Alert, match, baseMetric)
						} else {
							foundControllerMetrics[baseMetric]++
						}
					} else if knownExternalMetrics[match] {
						// Known external metric
					}
				}

				// Validate label selectors against known label values
				for _, m := range labelFilterRegex.FindAllStringSubmatch(rule.Expr, -1) {
					metricName := strings.TrimSuffix(m[1], "_bucket")
					metricName = strings.TrimSuffix(metricName, "_count")
					metricName = strings.TrimSuffix(metricName, "_sum")
					for _, lm := range labelMatcherRegex.FindAllStringSubmatch(m[2], -1) {
						lbl := lm[1]
						val := lm[3]
						key := metricName + ":" + lbl
						if validVals, ok := knownLabelValues[key]; ok {
							for _, subVal := range strings.Split(val, "|") {
								subVal = strings.TrimSpace(subVal)
								if subVal != "" && !validVals[subVal] {
									t.Errorf("alert %q: metric %q has unknown label value %q for label %q", rule.Alert, metricName, subVal, lbl)
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
