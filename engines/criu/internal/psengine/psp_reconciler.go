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

// Package psengine implements the criu-snapshot-engine: a snapshot engine for
// the gke-labs/pod-migration orchestrator that answers the podsnapshot.gke.io
// CR contract with CRIU checkpoints of single-container runc pods.
package psengine

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/gke-labs/pod-migration/engines/criu/internal/psengine/contract"
)

// StorageProber verifies the engine can write to the PSSC's GCS location
// (write + read-back + delete of a probe object). nil disables probing
// (unit tests); the production GCS prober runs with the checkpoint pipeline.
type StorageProber func(ctx context.Context, bucket, path string) error

// PSPReconciler owns the engine's admission lever: it marks Ready=True only
// on PodSnapshotPolicies the CRIU engine can actually serve. The
// orchestrator's eviction webhook requires that condition before it will
// hold an eviction and spawn a PMJ (eviction_webhook.go:284-301); withholding
// Ready degrades to a plain cold eviction — fail-open by construction.
//
// Scope guard: only PSPs labeled pod-migrate.io/engine=criu are touched, so
// foreign policies are never contended.
type PSPReconciler struct {
	client.Client
	Probe StorageProber
}

// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=podsnapshot.gke.io,resources=podsnapshotstorageconfigs,verbs=get;list;watch

func (r *PSPReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("psp", req.NamespacedName)

	psp := contract.New(contract.GVKPolicy)
	if err := r.Get(ctx, req.NamespacedName, psp); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Not ours -> leave it alone (the addon or another engine owns it).
	if psp.GetLabels()[contract.LabelEngine] != contract.EngineCRIU {
		return ctrl.Result{}, nil
	}

	ready, reason, msg := r.validate(ctx, psp)

	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	before := contract.Conditions(psp)
	for _, c := range []metav1.Condition{
		{Type: contract.CondReady, Status: status, Reason: reason, Message: msg, ObservedGeneration: psp.GetGeneration()},
		// v1 scope declaration: published up front so operators see the
		// boundary before the first refusal.
		{Type: contract.CondScopeSingleContainer, Status: metav1.ConditionTrue, Reason: "V1Scope",
			Message: "criu engine v1 serves single-container runc pods on criu-ready nodes", ObservedGeneration: psp.GetGeneration()},
	} {
		if err := contract.SetCondition(psp, c); err != nil {
			return ctrl.Result{}, err
		}
	}
	if conditionsEqual(before, contract.Conditions(psp)) {
		return ctrl.Result{}, nil
	}
	if err := r.Status().Update(ctx, psp); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("PSP conditions updated", "ready", ready, "reason", reason)
	return ctrl.Result{}, nil
}

// validate applies the same shape checks the eviction webhook will
// (manual+stop, parseable selector: eviction_webhook.go:258,264-281,319),
// then resolves the PSSC and probes the bucket. Anything short of a fully
// serviceable policy stays Ready=False — honesty over eagerness.
func (r *PSPReconciler) validate(ctx context.Context, psp *unstructured.Unstructured) (ready bool, reason, msg string) {
	if !contract.PolicyIsManualStop(psp) {
		return false, "UnsupportedTrigger", "criu engine serves only triggerConfig {type: manual, postCheckpoint: stop}"
	}
	if _, err := contract.PolicySelector(psp); err != nil {
		return false, "InvalidSelector", fmt.Sprintf("spec.selector does not parse: %v", err)
	}
	scName, ok := contract.PolicyStorageConfigName(psp)
	if !ok {
		return false, "MissingStorageConfig", "spec.storageConfigName is empty"
	}
	bucket, path, err := r.resolvePSSC(ctx, scName)
	if err != nil {
		return false, "StorageConfigUnresolved", err.Error()
	}
	if r.Probe != nil {
		if err := r.Probe(ctx, bucket, path); err != nil {
			return false, "StorageUnwritable", fmt.Sprintf("gs://%s/%s: %v", bucket, path, err)
		}
	}
	return true, "Serviceable", fmt.Sprintf("criu engine will serve this policy (archives to gs://%s/%s)", bucket, path)
}

// resolvePSSC fetches the cluster-scoped PSSC named by the PSP and returns
// its GCS location (spec.snapshotStorageConfig.gcs.{bucket,path},
// podmigration_controller.go:88-98).
func (r *PSPReconciler) resolvePSSC(ctx context.Context, name string) (bucket, path string, err error) {
	pssc := contract.New(contract.GVKStorageConfig)
	if err := r.Get(ctx, types.NamespacedName{Name: name}, pssc); err != nil {
		return "", "", fmt.Errorf("storageConfigName %q: %w", name, err)
	}
	return contract.GCSLocation(pssc)
}

func conditionsEqual(a, b []metav1.Condition) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Type != b[i].Type || a[i].Status != b[i].Status ||
			a[i].Reason != b[i].Reason || a[i].Message != b[i].Message ||
			a[i].ObservedGeneration != b[i].ObservedGeneration {
			return false
		}
	}
	return true
}

// SetupWithManager registers the reconciler for PSPs (engine-labeled ones are
// filtered in Reconcile).
func (r *PSPReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(contract.New(contract.GVKPolicy)).
		Named("psengine-psp").
		Complete(r)
}
