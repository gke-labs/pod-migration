# Pod Migration Operator Runbooks & Alerting Guide

This directory houses operational runbooks for `pod-migration-controller`. Each runbook pairs with reference Prometheus alerting rules defined in [`monitoring/prometheus-rules.yaml`](../../monitoring/prometheus-rules.yaml) to provide immediate triage steps, diagnostic queries, safe remediation procedures, and fault-injection verification tests.

---

## 1. Alerting Catalog & Runbook Matrix

| Alert Name                                     | Severity   | Primary Metric / Trigger                                                                     | Runbook Link                                               |
| ---------------------------------------------- | ---------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------------------- |
| **`PodMigrationPhaseDurationElevated`**        | `warning`  | `pod_migration_phase_duration_seconds` (P95 > 300s on completion)                            | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationJobTimeoutsHigh`**              | `warning`  | `pod_migration_outcomes_total{outcome=~"timeout\|budget_timeout"}`                           | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationBudgetExhaustedHigh`**          | `warning`  | `pod_migration_budget_checks_total{result="budget_exhausted"}`                               | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationControllerDown`**               | `critical` | `up{job="pod-migration-controller"} == 0`                                                    | [pmj-phase-stuck.md](pmj-phase-stuck.md)                   |
| **`PodMigrationSchedulingGateWedged`**         | `warning`  | `kube_pod_status_reason{reason="SchedulingGated"}` (> 10m)                                   | [orphaned-scheduling-gate.md](orphaned-scheduling-gate.md) |
| **`PodMigrationHighColdFallbackRatio`**        | `warning`  | `pod_migration_outcomes_total{outcome=~"fallback\|succeeded_without_restore\|restore_lost"}` | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationRestoreCrashFallbackDetected`** | `warning`  | `pod_migration_restore_crash_fallback_total` (> 0)                                           | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationRestoreCrashSignatureDrift`**   | `info`     | `pod_migration_restore_crash_unmatched_total` (> 0)                                          | [high-cold-fallback-ratio.md](high-cold-fallback-ratio.md) |
| **`PodMigrationSnapshotStorageErrors`**        | `warning`  | `pod_migration_restore_verdicts_total{verdict=~"snapshot_deleting\|snapshot_gone\|failed"}`  | [snapshot-storage-errors.md](snapshot-storage-errors.md)   |
| **`PodMigrationJobFailuresHigh`**              | `warning`  | `pod_migration_outcomes_total{outcome="failed"}`                                             | [snapshot-storage-errors.md](snapshot-storage-errors.md)   |
| **`PodMigrationInvariantViolationDetected`**   | `critical` | `pod_migration_invariant_violations_total` (> 0)                                             | [invariant-violations.md](invariant-violations.md)         |

---

## 2. Deploying Reference Prometheus Rules

The alerting rules are packaged as a Prometheus Operator `PrometheusRule` custom resource.

### Installation via kubectl

```bash
# 1. Apply metrics scrape Service and ServiceMonitor
kubectl apply -f monitoring/prometheus-service.yaml

# 2. Apply alert rules
kubectl apply -f monitoring/prometheus-rules.yaml
```

Verify that Prometheus Operator loaded the rule group:

```bash
kubectl get prometheusrule pod-migration-alerts -n pod-migration-system -o yaml
```

### Scraping Considerations (Multi-Replica HA & Prometheus Setup)

The controller serves Prometheus metrics on `:8080/metrics` across all replicas. Note the following operational constraints:

- **Scrape Job Configuration:** The alerting rules assume a Prometheus scrape job named `job="pod-migration-controller"` targeting port 8080. `monitoring/prometheus-service.yaml` ships a Service and a `ServiceMonitor` with `jobLabel: app` to ensure the scraped `job` label matches `pod-migration-controller`.
- **Prometheus Operator Selector Notice:** Default Helm installations of `kube-prometheus-stack` restrict `ServiceMonitor` discovery to objects carrying `release: <release-name>` (via `serviceMonitorSelectorNilUsesHelmValues: true`). If using `kube-prometheus-stack`, ensure you either add your release label to `monitoring/prometheus-service.yaml` or set `serviceMonitorSelectorNilUsesHelmValues: false`.
- **Google Cloud Managed Service for Prometheus (GMP):** If running on GKE with Managed Prometheus enabled, GMP discovers scrape targets via `PodMonitoring` custom resources rather than `ServiceMonitor`. Convert `monitoring/prometheus-service.yaml` to a `PodMonitoring` resource targeting port `metrics` (8080).
- **Leader vs Standby:** Only the active leader replica reconciles migrations and updates in-memory gauges (`pod_migration_active`). Standby replicas return `pod_migration_active == 0`.
- **Query Aggregation:** Always use `max(pod_migration_active)` when checking total active in-flight migrations.
- **Counter Rates:** For counters and histograms, aggregate across controller pods using `sum(rate(...[5m]))`.
- **Invariant Rules Flag:** Correctness invariant monitoring (`pod_migration_invariant_violations_total`) requires running the controller with `--invariant-mode=observe` (or `--invariant-mode=strict`). The flag defaults to `disabled` in production. Enabling `observe` evaluates safety predicates during reconciliation, which incurs small in-memory CPU and cached informer reads.

---

## 3. General Diagnostic Toolkit

When responding to any alert, begin by gathering baseline cluster state:

```bash
# 1. Check all active or recently failed PodMigrationJobs
kubectl get pmj -A -o wide

# 2. Check controller pods and leader election status
kubectl get pods -n pod-migration-system -o wide
kubectl get lease pod-migration-controller-leader -n pod-migration-system -o yaml

# 3. Stream controller logs
kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager --tail=200 -f

# 4. Check scheduling-gated replacement pods
kubectl get pods -A -o jsonpath='{range .items[?(@.spec.schedulingGates)]}{.metadata.namespace}{"/"}{.metadata.name}{"\tGates: "}{.spec.schedulingGates[*].name}{"\tAssignedPMJ: "}{.metadata.annotations.pod-migration\.gke\.io/assigned-pmj}{"\n"}{end}'

# 5. Query live controller metrics directly via port-forward to the leader pod
# (The controller runs on a distroless container without curl/shell; port-forward to the active leader:)
LEADER_POD=$(kubectl get lease pod-migration-controller-leader -n pod-migration-system -o jsonpath='{.spec.holderIdentity}')
LEADER_POD="${LEADER_POD%%_*}"
kubectl port-forward -n pod-migration-system pod/$LEADER_POD 8080:8080 &
PF_PID=$!
curl -s http://localhost:8080/metrics | grep pod_migration_
kill $PF_PID
```
