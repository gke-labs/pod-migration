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

package util

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// DefaultMetadataPreemptedURL is the GCE metadata endpoint indicating Spot VM preemption.
	DefaultMetadataPreemptedURL = "http://metadata.google.internal/computeMetadata/v1/instance/preempted"

	// DefaultMetadataMaintenanceURL is the GCE metadata endpoint indicating host maintenance.
	DefaultMetadataMaintenanceURL = "http://metadata.google.internal/computeMetadata/v1/instance/maintenance-event"

	// DefaultPreemptionWatcherPollInterval is the fallback polling frequency for metadata checks.
	DefaultPreemptionWatcherPollInterval = 200 * time.Millisecond
)

// PreemptionSignalType describes the detected host preemption or maintenance event.
type PreemptionSignalType string

const (
	SignalGCEPreempted        PreemptionSignalType = "GCEInstancePreempted"
	SignalGCEMaintenanceEvent PreemptionSignalType = "GCETerminateOnHostMaintenance"
	SignalSimulated           PreemptionSignalType = "SimulatedPreemptionNotice"
)

// PreemptionNotice contains details of a detected host preemption or maintenance notice.
type PreemptionNotice struct {
	SignalType PreemptionSignalType
	ReceivedAt time.Time
	NodeName   string
	Message    string
}

// PreemptionNotifier is called as soon as a low-latency preemption notice is detected.
type PreemptionNotifier func(ctx context.Context, notice PreemptionNotice) error

// MetadataPreemptionWatcher monitors host metadata server endpoints for
// sub-second preemption notices and dispatches callbacks immediately.
type MetadataPreemptionWatcher struct {
	NodeName       string
	MetadataURL    string
	MaintenanceURL string
	HTTPClient     *http.Client
	PollInterval   time.Duration
	Notifier       PreemptionNotifier

	mu       sync.Mutex
	notified bool
}

// NewMetadataPreemptionWatcher creates a watcher with sensible defaults.
func NewMetadataPreemptionWatcher(nodeName string, notifier PreemptionNotifier) *MetadataPreemptionWatcher {
	return &MetadataPreemptionWatcher{
		NodeName:       nodeName,
		MetadataURL:    DefaultMetadataPreemptedURL,
		MaintenanceURL: DefaultMetadataMaintenanceURL,
		HTTPClient:     &http.Client{Timeout: 5 * time.Second},
		PollInterval:   DefaultPreemptionWatcherPollInterval,
		Notifier:       notifier,
	}
}

// CheckOnce performs a single non-blocking check of the metadata endpoints.
// It returns (true, notice, nil) if a preemption signal is active.
func (w *MetadataPreemptionWatcher) CheckOnce(ctx context.Context) (bool, *PreemptionNotice, error) {
	client := w.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	// 1. Check instance/preempted
	if w.MetadataURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.MetadataURL, nil)
		if err != nil {
			return false, nil, fmt.Errorf("preemption request: %w", err)
		}
		req.Header.Set("Metadata-Flavor", "Google")
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 128))
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK && strings.TrimSpace(strings.ToUpper(string(body))) == "TRUE" {
				return true, &PreemptionNotice{
					SignalType: SignalGCEPreempted,
					ReceivedAt: time.Now(),
					NodeName:   w.NodeName,
					Message:    "GCE instance preempted notice received: TRUE",
				}, nil
			}
		}
	}

	// 2. Check instance/maintenance-event
	if w.MaintenanceURL != "" {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, w.MaintenanceURL, nil)
		if err != nil {
			return false, nil, fmt.Errorf("maintenance request: %w", err)
		}
		req.Header.Set("Metadata-Flavor", "Google")
		resp, err := client.Do(req)
		if err == nil {
			body, _ := io.ReadAll(io.LimitReader(resp.Body, 128))
			resp.Body.Close()
			trimmed := strings.TrimSpace(string(body))
			if resp.StatusCode == http.StatusOK && (trimmed == "TERMINATE_ON_HOST_MAINTENANCE" || trimmed == "MIGRATE_ON_HOST_MAINTENANCE") {
				return true, &PreemptionNotice{
					SignalType: SignalGCEMaintenanceEvent,
					ReceivedAt: time.Now(),
					NodeName:   w.NodeName,
					Message:    fmt.Sprintf("GCE maintenance-event received: %s", trimmed),
				}, nil
			}
		}
	}

	return false, nil, nil
}

// Start runs the watcher loop until ctx is cancelled or a notice is dispatched.
// Implements controller-runtime manager.Runnable.
func (w *MetadataPreemptionWatcher) Start(ctx context.Context) error {
	pollInterval := w.PollInterval
	if pollInterval <= 0 {
		pollInterval = DefaultPreemptionWatcherPollInterval
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			w.mu.Lock()
			alreadyNotified := w.notified
			w.mu.Unlock()
			if alreadyNotified {
				return nil
			}

			active, notice, err := w.CheckOnce(ctx)
			if err != nil {
				continue
			}
			if active && notice != nil {
				w.mu.Lock()
				if w.notified {
					w.mu.Unlock()
					return nil
				}
				w.notified = true
				w.mu.Unlock()

				if w.Notifier != nil {
					_ = w.Notifier(ctx, *notice)
				}
				return nil
			}
		}
	}
}

// NewNodePatcherNotifier returns a PreemptionNotifier that immediately stamps
// low-latency preemption markers (LabelNodePreempting, TaintPodMigrationPreempting,
// and ConditionPreemptionImminent) onto the targeted Node object.
func NewNodePatcherNotifier(c client.Client, nodeName string) PreemptionNotifier {
	return func(ctx context.Context, notice PreemptionNotice) error {
		if c == nil || nodeName == "" {
			return nil
		}
		node := &corev1.Node{}
		if err := c.Get(ctx, client.ObjectKey{Name: nodeName}, node); err != nil {
			return err
		}

		before := node.DeepCopy()

		// 1. Label
		if node.Labels == nil {
			node.Labels = make(map[string]string)
		}
		node.Labels[LabelNodePreempting] = ValueNodePreempting

		// 2. Taint
		hasTaint := false
		for _, t := range node.Spec.Taints {
			if t.Key == TaintPodMigrationPreempting {
				hasTaint = true
				break
			}
		}
		if !hasTaint {
			node.Spec.Taints = append(node.Spec.Taints, corev1.Taint{
				Key:    TaintPodMigrationPreempting,
				Value:  ValueNodePreempting,
				Effect: corev1.TaintEffectNoSchedule,
			})
		}

		if err := c.Patch(ctx, node, client.MergeFrom(before)); err != nil {
			return err
		}

		// 3. Condition on status
		statusBefore := node.DeepCopy()
		hasCond := false
		for i, cond := range node.Status.Conditions {
			if cond.Type == ConditionPreemptionImminent {
				node.Status.Conditions[i].Status = corev1.ConditionTrue
				node.Status.Conditions[i].Reason = string(notice.SignalType)
				node.Status.Conditions[i].Message = notice.Message
				node.Status.Conditions[i].LastTransitionTime = metav1.Now()
				hasCond = true
				break
			}
		}
		if !hasCond {
			node.Status.Conditions = append(node.Status.Conditions, corev1.NodeCondition{
				Type:               ConditionPreemptionImminent,
				Status:             corev1.ConditionTrue,
				Reason:             string(notice.SignalType),
				Message:            notice.Message,
				LastTransitionTime: metav1.Now(),
			})
		}

		return c.Status().Patch(ctx, node, client.MergeFrom(statusBefore))
	}
}
