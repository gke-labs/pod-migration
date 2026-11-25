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

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	pmv1alpha1 "github.com/gke-labs/pod-migration/controller/api/v1alpha1"
	"github.com/gke-labs/pod-migration/controller/internal/eligibility"
	"github.com/gke-labs/pod-migration/controller/internal/engine"
	"github.com/gke-labs/pod-migration/controller/internal/metrics"
	"github.com/gke-labs/pod-migration/controller/internal/util"
)

// DefaultCoverageRefreshInterval is how often CoverageRefresher recalculates
// migratable coverage metrics across active namespaces.
const DefaultCoverageRefreshInterval = 30 * time.Second

// MinCoverageRefreshInterval is the minimum allowable non-zero interval for
// periodic coverage calculation, guarding against excessive uncached API server list loops.
const MinCoverageRefreshInterval = 5 * time.Second

// ValidateCoverageRefreshInterval checks that interval is either 0 (disabled)
// or at least MinCoverageRefreshInterval.
func ValidateCoverageRefreshInterval(interval time.Duration) error {
	if interval < 0 {
		return fmt.Errorf("coverage-refresh-interval %v must not be negative", interval)
	}
	if interval == 0 {
		return nil
	}
	if interval < MinCoverageRefreshInterval {
		return fmt.Errorf("coverage-refresh-interval %v must be 0 (off) or at least %v", interval, MinCoverageRefreshInterval)
	}
	return nil
}

// CoverageRefresher periodically recalculates migratable coverage metrics
// for active PodMigration configurations without triggering full reconciliation
// of managed resources (PSSC/PSP) or invariant validation.
type CoverageRefresher struct {
	Client             client.Client
	Engines            *engine.Registry
	RuntimeClassPolicy *eligibility.RuntimeClassPolicy
	Interval           time.Duration

	mu                sync.Mutex
	trackedNamespaces map[string]struct{}
}

// NeedLeaderElection ensures only the leader replica updates coverage metrics in memory.
func (c *CoverageRefresher) NeedLeaderElection() bool { return true }

// Start starts the periodic coverage refresh loop.
func (c *CoverageRefresher) Start(ctx context.Context) error {
	if c.Interval <= 0 {
		<-ctx.Done()
		return nil
	}
	logger := log.FromContext(ctx).WithName("coverage-refresher")
	ctx = log.IntoContext(ctx, logger)
	ticker := time.NewTicker(c.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			c.RefreshOnce(ctx)
		}
	}
}

// RefreshOnce evaluates all active namespaces and updates their coverage metrics.
// The refresher owns the label set: each tick it clears series for any namespace
// that is no longer in that tick's active set.
func (c *CoverageRefresher) RefreshOnce(ctx context.Context) {
	var pmList pmv1alpha1.PodMigrationList
	if err := c.Client.List(ctx, &pmList); err != nil {
		log.FromContext(ctx).Error(err, "Failed to list PodMigrations for periodic coverage refresh")
		return
	}

	byNamespace := make(map[string][]pmv1alpha1.PodMigration)
	for i := range pmList.Items {
		pm := pmList.Items[i]
		byNamespace[pm.Namespace] = append(byNamespace[pm.Namespace], pm)
	}

	activeByNamespace := make(map[string]*pmv1alpha1.PodMigration)
	for ns, items := range byNamespace {
		active := selectActivePodMigration(items)
		if active != nil {
			activeByNamespace[ns] = active
		}
	}

	c.mu.Lock()
	if c.trackedNamespaces == nil {
		c.trackedNamespaces = make(map[string]struct{})
	}
	for _, ns := range metrics.CoverageNamespaces() {
		c.trackedNamespaces[ns] = struct{}{}
	}
	for ns := range c.trackedNamespaces {
		if _, ok := activeByNamespace[ns]; !ok {
			metrics.ClearNamespaceCoverage(ns)
			delete(c.trackedNamespaces, ns)
		}
	}
	c.mu.Unlock()

	for ns, active := range activeByNamespace {
		if err := ComputeCoverageForNamespace(ctx, c.Client, active, c.Engines, c.RuntimeClassPolicy); err != nil {
			log.FromContext(ctx).Error(err, "Periodic coverage refresh failed", "namespace", ns)
			continue
		}
		c.mu.Lock()
		c.trackedNamespaces[ns] = struct{}{}
		c.mu.Unlock()
	}
}

// ComputeCoverageForNamespace calculates migratable pod coverage for a given
// active PodMigration configuration.
func ComputeCoverageForNamespace(
	ctx context.Context,
	c client.Client,
	config *pmv1alpha1.PodMigration,
	registry *engine.Registry,
	runtimeClassPolicy *eligibility.RuntimeClassPolicy,
) error {
	if config == nil {
		return nil
	}
	var podList corev1.PodList
	if err := c.List(ctx, &podList, client.InNamespace(config.Namespace)); err != nil {
		return fmt.Errorf("failed to list pods to compute migratable coverage metrics in namespace %s: %w", config.Namespace, err)
	}

	// Pre-fetch PSPs for each registered engine's APIGroup once per namespace to avoid N uncached API server calls.
	pspListsByGroup := make(map[string][]unstructured.Unstructured)
	if registry != nil {
		for _, name := range registry.Names() {
			eng, ok := registry.Get(name)
			if !ok || eng == nil {
				continue
			}
			group := eng.APIGroup()
			if _, exists := pspListsByGroup[group]; exists {
				continue
			}
			pspList := &unstructured.UnstructuredList{}
			pspList.SetGroupVersionKind(schema.GroupVersionKind{
				Group:   group,
				Version: "v1",
				Kind:    "PodSnapshotPolicyList",
			})
			if err := c.List(ctx, pspList, client.InNamespace(config.Namespace)); err != nil {
				if meta.IsNoMatchError(err) {
					// Missing CRD: treat as no policies available
					pspListsByGroup[group] = nil
					continue
				}
				// Transient error (429 TooManyRequests, timeout, API server blip):
				// Return error so caller preserves existing coverage rather than publishing covered=0.
				return fmt.Errorf("failed to list PSPs in group %s for coverage calculation in namespace %s: %w", group, config.Namespace, err)
			}
			pspListsByGroup[group] = pspList.Items
		}
	}

	nodeCache := make(map[string]*corev1.Node)
	totalRunning := 0
	coveredRunning := 0

	for i := range podList.Items {
		pod := &podList.Items[i]
		if pod.Status.Phase != corev1.PodRunning || !pod.DeletionTimestamp.IsZero() {
			continue
		}
		totalRunning++

		if !util.IsPodMigrationEnabled(pod) {
			continue
		}

		if runtimeClassPolicy != nil && !runtimeClassPolicy.Allows(pod.Spec.RuntimeClassName) {
			continue
		}

		if registry == nil {
			continue
		}
		eng, ok := registry.ForPod(pod, config)
		if !ok || eng == nil {
			continue
		}

		groupPolicies := pspListsByGroup[eng.APIGroup()]
		var policy *unstructured.Unstructured
		switch eng.Name() {
		case engine.CRIUName:
			policy = util.MatchLatestReadyManualStopPSP(
				groupPolicies,
				pod.Labels,
				map[string]string{util.LabelSnapshotEngine: engine.CRIUName},
				"",
			)
		default:
			policy = util.MatchLatestReadyManualStopPSP(groupPolicies, pod.Labels, nil, "criu")
		}
		if policy == nil {
			continue
		}

		var node *corev1.Node
		if pod.Spec.NodeName != "" {
			if n, ok := nodeCache[pod.Spec.NodeName]; ok {
				node = n
			} else {
				n := &corev1.Node{}
				if err := c.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, n); err == nil {
					nodeCache[pod.Spec.NodeName] = n
					node = n
				}
			}
		}

		if verdict := eng.Eligible(pod, node); !verdict.OK {
			continue
		}

		if ok, _ := engine.AllowsFencing(pod, eng); !ok {
			continue
		}

		coveredRunning++
	}

	metrics.RecordNamespaceCoverage(config.Namespace, coveredRunning, totalRunning)
	return nil
}
