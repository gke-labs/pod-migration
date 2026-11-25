# Operator Runbook: Correctness Invariant Violations

## Alert Covered

- **`PodMigrationInvariantViolationDetected`** (`severity: critical`, `for: 0m`): The invariant engine detected a breach of one of the controller's twelve correctness invariants (`I1`–`I12`). This alert fires **immediately** (0m delay).

---

## 1. Operational Impact

The twelve safety invariants (`I1`–`I12`) guard against data corruption, split-brain state, silent cold starts, and control plane deadlocks:

- **`I1` (At-Most-Once Restore):** Potential split-brain! Two active pods may be running concurrently with the same snapshot memory state.
- **`I2` (No Silent Cold-Start):** False success reporting; a migration reported as succeeded actually started cold without state.
- **`I3` (Gate Liveness):** Replacement pod stranded with scheduling gate after migration finished.
- **`I4` (Terminal Progress):** Active migration stuck beyond maximum timeout ceiling.
- **`I5` (Zero Resource Leak):** Stale `PodSnapshotManualTrigger` triggers leaking in etcd.
- **`I6` (Revision & Identity Fidelity):** Checkpoint restored into a pod of a different rollout revision or job index.
- **`I7` (Disruption Bounding & PDB Compliance):** Origin pod eviction violated PodDisruptionBudget.
- **`I8` (Platform & Control-Plane Isolation):** Migration attempted on system components (`kube-system`, `pod-migration-system`).
- **`I9` (Deterministic Fallback over Crashloop):** Replacement pod crashlooping instead of falling back to clean cold start.
- **`I10` (Restoring Replacement Liveness):** PMJ in `Restoring` phase whose replacement pod was deleted.
- **`I11` (Source Fenced After Checkpoint):** Checkpointed source pod resumed receiving traffic before replacement took over.
- **`I12` (Restore Outcome Confirmed):** Unverified engine restore claimed as verified.

---

## 2. Invariant Execution Modes & Operational Overhead

Check the controller's configured `--invariant-mode` flag:

```bash
kubectl get deployment pod-migration-controller -n pod-migration-system -o yaml | grep invariant-mode
```

- **`disabled` Mode (Default):** Invariant checks are skipped during reconciliation to minimize CPU overhead. The alert will not fire unless the flag is explicitly enabled.
- **`observe` Mode:** Violations log structured errors, increment the Prometheus counter `pod_migration_invariant_violations_total`, and emit a Kubernetes `Warning` Event (`Reason=InvariantViolation`). Active migrations continue.
  - _Operational Cost:_ Small in-memory CPU overhead during reconcile passes to evaluate predicates against local informer caches, plus event emission to the apiserver upon violation.
- **`strict` Mode:** The controller halts reconciliation, fails the violating `PodMigrationJob` with `Phase=Failed` (`Reason=InvariantViolation`), cleans up snapshot triggers, and releases gated replacement pods for a safe cold start. (Debounced rules `I10` and `I11` require 3 consecutive samples / 30s before aborting).

---

## 3. Diagnostic Workflow

### Step 1: Identify Which Invariant Fired

Query controller metrics via port-forward:

```bash
kubectl port-forward -n pod-migration-system deployment/pod-migration-controller 8080:8080 &
PF_PID=$!
curl -s http://localhost:8080/metrics | grep pod_migration_invariant_violations_total
kill $PF_PID
```

Note the label value (e.g. `invariant="I1"`, `invariant="I3"`, `invariant="I11"`).

### Step 2: Search Controller Logs for Invariant Violation Context

```bash
kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager --tail=500 | grep -E "INVARIANT_VIOLATION|InvariantViolation|invariant="
```

The log entry records the exact violating rule name, reconciled object UID, namespace, and predicate reason:

```
ERROR Invariant violation detected {"invariant": "I1", "rule": "AtMostOnceRestore", "namespace": "default", "job": "pmj-redis-0", "reason": "Multiple replacement pods carry snapshot ps-redis-0"}
```

### Step 3: Check Kubernetes Warning Events in Target Namespace

```bash
kubectl get events -n <namespace> --field-selector reason=InvariantViolation -o wide
```

---

## 4. Remediation Procedures by Invariant Category

### A. Split-Brain & Dual Execution Invariants (`I1`, `I11`)

> 🚨 **HIGH SEVERITY:** Immediate action required to prevent dual-writing to databases or shared volumes.

1. **For `I1` (Duplicate Restore Annotations):**
   - Find all pods carrying the snapshot annotation:
     ```bash
     kubectl get pods -n <namespace> -o jsonpath='{range .items[?(@.metadata.annotations.podsnapshot\.gke\.io/ps-name)]}{.metadata.name}{": "}{.metadata.annotations.podsnapshot\.gke\.io/ps-name}{"\n"}{end}'
     ```
   - If two running pods share the same snapshot name, determine which is the authorized replacement.
   - **DO NOT use `--grace-period=0 --force`!** Force-deleting removes the API object immediately while the container process may still be running on the node; for a StatefulSet, the workload controller would immediately recreate the same-name pod, exacerbating the split-brain.
   - Perform a standard graceful deletion:
     ```bash
     kubectl delete pod <unauthorized-pod> -n <namespace>
     ```
   - Confirm the container process has completely stopped on the node before proceeding:
     ```bash
     kubectl get pod <unauthorized-pod> -n <namespace>
     ```

2. **For `I11` (Source Pod Unfenced During Migration):**
   - If a source pod lost its `pod-migration.gke.io/fenced: "true"` label while the PMJ is still in `Snapshotting` or `Restoring`:
     ```bash
     # Re-fence the source pod immediately
     kubectl label pod <source-pod> -n <namespace> pod-migration.gke.io/fenced=true --overwrite
     ```
   - Alternatively, abort the migration by deleting the `PodMigrationJob`.

### B. Stranded Gates & Identity Mismatches (`I3`, `I6`)

1. **For `I3` (Stranded Scheduling Gate):**
   - Follow the step-by-step procedures in [orphaned-scheduling-gate.md](orphaned-scheduling-gate.md).
2. **For `I6` (Revision / Hash Mismatch):**
   - A rolling update occurred during migration. Delete the PMJ so the workload controller can schedule a fresh pod of the new revision:
     ```bash
     kubectl delete pmj <pmj-name> -n <namespace>
     ```

### C. Leaked Snapshot Triggers (`I5`)

1. Check for orphaned `PodSnapshotManualTrigger` objects:
   ```bash
   kubectl get podsnapshotmanualtriggers -A
   ```
2. Delete triggers belonging to completed or failed migrations:
   ```bash
   kubectl delete podsnapshotmanualtrigger <trigger-name> -n <namespace>
   ```

### D. Stuck Deadlines (`I4`)

1. If an active PMJ exceeded its timeout ceiling:
   ```bash
   kubectl delete pmj <pmj-name> -n <namespace>
   ```

---

## 5. Fault-Injection Verification

### Test Case: Simulating an Invariant Violation (`I5` Leaked Trigger)

1. In a cluster configured with `--invariant-mode=observe`, create a completed PMJ fixture:
   ```yaml
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigrationJob
   metadata:
     name: test-i5-violation-pmj
     namespace: default
   spec:
     podRef:
       name: dummy-pod
     targetPodUID: "dummy-uid-12345"
   ```
2. Create an un-deleted `PodSnapshotManualTrigger` owned by the PMJ:
   ```yaml
   apiVersion: podsnapshot.gke.io/v1alpha1
   kind: PodSnapshotManualTrigger
   metadata:
     name: psmt-dummy-pod-manual
     namespace: default
     labels:
       pod-migration.gke.io/job: test-i5-violation-pmj
   spec:
     podName: dummy-pod
     snapshotName: dummy-snap
   ```
3. Trigger reconciliation of `test-i5-violation-pmj`.
4. Check controller metrics via port-forward:
   ```bash
   kubectl port-forward -n pod-migration-system deployment/pod-migration-controller 8080:8080 &
   PF_PID=$!
   curl -s http://localhost:8080/metrics | grep pod_migration_invariant_violations_total
   kill $PF_PID
   ```
5. Verify that `pod_migration_invariant_violations_total{invariant="I5"}` increments and `PodMigrationInvariantViolationDetected` fires immediately.
6. Clean up:
   ```bash
   kubectl delete podsnapshotmanualtrigger psmt-dummy-pod-manual -n default
   kubectl delete pmj test-i5-violation-pmj -n default
   ```
