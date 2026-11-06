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

package psengine

// Engine outcome metrics: Refused and Failed are distinct facts — a refusal is
// the eligibility policy working as designed, while a failure is a runtime or
// environment problem — and the metrics keep them separate.

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	checkpointOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pm_psengine_checkpoint_outcomes_total",
		Help: "criu-snapshot-engine checkpoint outcomes by reason (Succeeded|Failed|Refused)",
	}, []string{"reason"})
	restoreOutcomes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pm_psengine_restore_outcomes_total",
		Help: "criu-snapshot-engine restore outcomes (restored|failed)",
	}, []string{"outcome"})
)

func init() {
	metrics.Registry.MustRegister(checkpointOutcomes, restoreOutcomes)
}
