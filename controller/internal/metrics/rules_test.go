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

	knownControllerMetrics := []string{
		"pod_migration_active",
		"pod_migration_outcomes_total",
		"pod_migration_restore_verdicts_total",
		"pod_migration_phase_duration_seconds",
		"pod_migration_restore_crash_fallback_total",
		"pod_migration_restore_crash_unmatched_total",
		"pod_migration_invariant_violations_total",
		"pod_migration_spot_preemption_triggered_total",
		"pod_migration_spot_preemption_skipped_total",
		"pod_migration_budget_checks_total",
		"pod_migration_fence_webhook_requests_total",
		"pod_migration_source_fence_total",
		"pod_migration_source_fenced_seconds",
		"pod_migration_application_downtime_seconds",
	}

	validSeverities := map[string]bool{
		"critical": true,
		"warning":  true,
		"info":     true,
	}

	totalRules := 0
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
					// Verify that the referenced markdown file actually exists on disk
					idx := strings.Index(runbookURL, "docs/runbooks/")
					relDoc := runbookURL[idx:]
					fullDocPath := filepath.Join("..", "..", "..", relDoc)
					if _, err := os.Stat(fullDocPath); err != nil {
						t.Errorf("alert %q: runbook file %s does not exist: %v", rule.Alert, fullDocPath, err)
					}
				}

				// If the expr references a pod_migration_ metric, assert it is in our known list
				for _, word := range strings.Fields(rule.Expr) {
					clean := strings.Trim(word, "(){}[],=~><+/*0123456789.\"")
					if strings.HasPrefix(clean, "pod_migration_") {
						baseMetric := strings.TrimSuffix(clean, "_bucket")
						baseMetric = strings.TrimSuffix(baseMetric, "_count")
						baseMetric = strings.TrimSuffix(baseMetric, "_sum")
						found := false
						for _, known := range knownControllerMetrics {
							if baseMetric == known {
								found = true
								break
							}
						}
						if !found {
							t.Errorf("alert %q: expr references unknown pod_migration metric %q", rule.Alert, clean)
						}
					}
				}
			})
		}
	}

	if totalRules < 10 {
		t.Errorf("expected at least 10 alerts in prometheus-rules.yaml, got %d", totalRules)
	}
}
