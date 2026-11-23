# Pod Migration Operator Runbooks & Alerting Guide

This directory houses operational runbooks for `pod-migration-controller`. Each runbook pairs with reference Prometheus alerting rules defined in [`monitoring/prometheus-rules.yaml`](../../monitoring/prometheus-rules.yaml) to provide immediate triage steps, diagnostic queries, safe remediation procedures, and fault-injection verification tests.

---

## 1. Alerting Catalog & Runbook Matrix

| Alert Name                                         | Severity   | Primary Metric / Trigger                                                                    | Runbook Link                                               |
| -------------------------------------------------- | ---------- | ------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| **`PodMigrationJobPhaseStuck`**                    | `warning`  | `pod_migration_phase_duration_seconds` (P95 > 300s)                                         | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationJobTimeoutsHigh`**                  | `warning`  | `pod_migration_outcomes_total{outcome=~"timeout\|budget_timeout"}`                          | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationBudgetExhaustedHigh`**              | `warning`  | `pod_migration_budget_checks_total{result="budget_exhausted"}`                              | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationControllerDown`**                   | `critical` | `up{job="pod-migration-controller"} == 0`                                                   | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationSchedulingGateWedged`**             | `warning`  | `kube_pod_spec_scheduling_gate{gate_name="gke.io/pod-migration-gate"}` (> 10m)              | [orphaned-scheduling-gate.md](orphaned-scheduling-gate.md) |
| **`PodMigrationHighColdFallbackRatio`**            | `warning`  | `pod_migration_outcomes_total{outcome=~"fallback\|succeeded_without_restore"}` (> 15%)      | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationRestoreCrashFallbackDetected`**     | `warning`  | `pod_migration_restore_crash_fallback_total` (> 0)                                          | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationRestoreCrashSignatureDrift`**       | `info`     | `pod_migration_restore_crash_unmatched_total` (> 0)                                         | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationSnapshotStorageErrors`**            | `warning`  | `pod_migration_restore_verdicts_total{verdict=~"snapshot_deleting\|snapshot_gone\|failed"}` | [snapshot-storage-errors.md](snapshot-storage-errors.md)   |
| **`PodMigrationSnapshotStoragePermanentFailures`** | `critical` | `pod_migration_outcomes_total{outcome="failed"}`                                            | [snapshot-storage-errors.md](snapshot-storage-errors.md)   |
| **`PodMigrationInvariantViolationDetected`**       | `critical` | `pod_migration_invariant_violations_total` (> 0)                                            | [invariant-violations.md](invariant-violations.md)         |

---

## 2. Deploying Reference Prometheus Rules

The alerting rules are packaged as a Prometheus Operator `PrometheusRule` custom resource.

### Installation via kubectl

```bash
kubectl apply -f monitoring/prometheus-rules.yaml
```

Verify that Prometheus Operator loaded the rule group:

```bash
kubectl get prometheusrule pod-migration-alerts -n pod-migration-system -o yaml
```

### Scraping Considerations (Multi-Replica HA)

The controller serves Prometheus metrics on `:8080/metrics` across all replicas. Note the following operational constraints:

- **Leader vs Standby:** Only the active leader replica reconciles migrations and updates in-memory gauges (`pod_migration_active`). Standby replicas return `pod_migration_active == 0`.
- **Query Aggregation:** Always use `max(pod_migration_active)` when checking total active in-flight migrations.
- **Counter Rates:** For counters and histograms, aggregate across controller pods using `sum(rate(...[5m]))`.

---

## 3. General Diagnostic Toolkit

When responding to any alert, begin by gathering baseline cluster state:

```bash
# 1. Check all active or recently failed PodMigrationJobs
kubectl get pmj -A -o wide

# 2. Check controller pods and leader election status
kubectl get pods -n pod-migration-system -o wide
kubectl get lease pod-migration-controller-lock -n pod-migration-system -o yaml

# 3. Stream controller logs
kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager --tail=200 -f

# 4. Check scheduling-gated replacement pods
kubectl get pods -A -o jsonpath='{range .items[?(@.spec.schedulingGates)]}{.metadata.namespace}{"/"}{.metadata.name}{"\tGates: "}{range .spec.schedulingGates}{.name}{" "}{end}{"\tAssignedPMJ: "}{.metadata.annotations.pod-migration\.gke\.io/assigned-pmj}{"\n"}{end}'

# 5. Query live controller metrics directly from pod
kubectl exec -n pod-migration-system deployment/pod-migration-controller -c manager -- curl -s http://localhost:8080/metrics | grep pod_migration_
```
