# Operator Runbook: Stuck PodMigrationJob Phases & Timeouts

## Alerts Covered

- **`PodMigrationJobPhaseStuck`** (`severity: warning`): A migration phase (`Pending`, `Snapshotting`, `Evicting`, or `Restoring`) has taken longer than 300 seconds (P95).
- **`PodMigrationJobTimeoutsHigh`** (`severity: warning`): PodMigrationJobs are failing with `timeout` or `budget_timeout` outcomes.
- **`PodMigrationBudgetExhaustedHigh`** (`severity: warning`): Pre-checkpoint dry-run evictions are repeatedly blocked by PodDisruptionBudgets (`budget_exhausted`).
- **`PodMigrationControllerDown`** (`severity: critical`): Controller manager process is unreachable or not serving metrics.

---

## 1. Operational Impact

- **Pending Phase:** The migration has not yet started. The source workload continues running normally, but preemption or drain deadlines may be burning down.
- **Snapshotting Phase:** Source pod may be fenced (`pod-migration.gke.io/fenced=true`). Traffic is stopped while the engine writes checkpoint state. Prolonged snapshotting directly inflates application downtime or leaves the source paused.
- **Evicting Phase:** Snapshot completed, but source pod deletion is hung (e.g. terminating pods, stubborn finalizers). Replacement pod cannot be released without violating mutual exclusion ($I_3$).
- **Restoring Phase:** Replacement pod has been created with scheduling gate `gke.io/pod-migration-gate`, but has not become Ready within the restore timeout ceiling.

---

## 2. Diagnostic Workflow

### Step 1: Identify Stuck Jobs and Current Phase

```bash
# List all active jobs sorted by creation timestamp
kubectl get pmj -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,PHASE:.status.phase,SOURCE:.spec.sourcePod.name,REPLACEMENT:.status.replacementPod.name,AGE:.metadata.creationTimestamp'

# Get detailed status of the stuck job
kubectl describe pmj <pmj-name> -n <namespace>
```

Inspect `.status.phase`, `.status.conditions`, and recent events.

### Step 2: Phase-Specific Triage

#### A. If Phase == `Pending` (Budget or Gate Blocked)

Check whether a PodDisruptionBudget is preventing eviction:

```bash
# Check PDBs in the target namespace
kubectl get pdb -n <namespace>

# Check dry-run budget metric counts
curl -s http://localhost:8080/metrics | grep pod_migration_budget_checks_total
```

If `pod_migration_budget_checks_total{result="budget_exhausted"}` is rising, the workload owner configured a strict PDB (`maxUnavailable: 0` or `minAvailable: 100%`) that disallows eviction.

#### B. If Phase == `Snapshotting` (Checkpoint Stalled)

1. Check source pod fencing status:
   ```bash
   kubectl get pod <source-pod> -n <namespace> -o jsonpath='{.metadata.labels.pod-migration\.gke\.io/fenced}'
   ```
2. Check snapshot custom resources:
   ```bash
   # GKE Pod Snapshots engine
   kubectl get podsnapshots -n <namespace>
   kubectl get podsnapshotmanualtriggers -n <namespace>

   # CRIU engine
   kubectl get pods -n <namespace> -l app=criu-node-agent
   ```
3. Check storage write path and disk errors in controller logs:
   ```bash
   kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager | grep -E "snapshot|engine|Failed"
   ```

#### C. If Phase == `Evicting` (Eviction Hung)

1. Inspect source pod deletion timestamp and finalizers:
   ```bash
   kubectl get pod <source-pod> -n <namespace> -o jsonpath='{.metadata.deletionTimestamp}{"\tFinalizers: "}{.metadata.finalizers}{"\n"}'
   ```
2. If the pod is terminating but stuck, check node kubelet logs or volume unmount errors:
   ```bash
   kubectl describe pod <source-pod> -n <namespace>
   ```

#### D. If Phase == `Restoring` (Replacement Not Ready)

1. Find replacement pod:
   ```bash
   kubectl get pods -n <namespace> -l pod-migration.gke.io/assigned-pmj=<pmj-name>
   ```
2. Check if scheduling gate is still present:
   ```bash
   kubectl get pod <replacement-pod> -n <namespace> -o jsonpath='{.spec.schedulingGates}'
   ```
3. If scheduled, check container crash / restore failures:
   ```bash
   kubectl describe pod <replacement-pod> -n <namespace>
   ```

---

## 3. Safe Remediation Procedures

### Remediation A: Cancel Stuck PodMigrationJob

If a migration is wedged beyond recovery, delete the `PodMigrationJob` object:

```bash
kubectl delete pmj <pmj-name> -n <namespace> --timeout=30s
```

**Controller Safety Behavior on PMJ Deletion:**

- If the source was fenced, the controller unfences it (`pod-migration.gke.io/fenced-`) so it resumes serving traffic.
- If a replacement pod was created with a scheduling gate, the `PodGateReconciler` detects PMJ deletion via direct API server read and releases the gate with an explicit cold-start bypass annotation (`podsnapshot.gke.io/ps-name: ""`), ensuring the replacement boots cold safely.

### Remediation B: Mitigate PDB Starvation

If `PodMigrationBudgetExhaustedHigh` is firing during scheduled node drains:

1. Contact the workload owner or temporarily patch the PDB to allow at least 1 unavailable pod:
   ```bash
   kubectl patch pdb <pdb-name> -n <namespace> --type=merge -p '{"spec":{"maxUnavailable":1}}'
   ```
2. Once the migration completes, restore the original PDB specification.

### Remediation C: Recover Controller Lease or Deadlock

If `PodMigrationControllerDown` is firing:

1. Check pod status in `pod-migration-system`:
   ```bash
   kubectl get pods -n pod-migration-system
   ```
2. Check leader election lease:
   ```bash
   kubectl describe lease pod-migration-controller-lock -n pod-migration-system
   ```
3. If the active leader container OOMKilled or deadlocked on an external call:
   ```bash
   kubectl rollout restart deployment pod-migration-controller -n pod-migration-system
   ```

---

## 4. Fault-Injection Verification

To verify that the monitoring and alerting rules correctly detect stuck phases:

### Test Case: Simulating PDB Budget Timeout

1. Deploy a single-replica StatefulSet with `maxUnavailable: 0`:
   ```yaml
   apiVersion: policy/v1
   kind: PodDisruptionBudget
   metadata:
     name: test-strict-pdb
     namespace: default
   spec:
     minAvailable: 1
     selector:
       matchLabels:
         app: test-workload
   ```
2. Trigger a `PodMigrationJob` for the pod:
   ```bash
   kubectl apply -f - <<EOF
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigrationJob
   metadata:
     name: test-stuck-budget-pmj
     namespace: default
   spec:
     sourcePod:
       name: test-workload-0
     timeout: 60s
   EOF
   ```
3. Observe controller metrics:
   ```bash
   kubectl exec -n pod-migration-system deployment/pod-migration-controller -c manager -- curl -s http://localhost:8080/metrics | grep pod_migration_budget_checks_total
   ```
4. Verify that `pod_migration_budget_checks_total{result="budget_exhausted"}` increments and after 60s `pod_migration_outcomes_total{outcome="budget_timeout"}` increments, triggering `PodMigrationJobTimeoutsHigh`.
5. Clean up:
   ```bash
   kubectl delete pmj test-stuck-budget-pmj -n default
   kubectl delete pdb test-strict-pdb -n default
   ```
