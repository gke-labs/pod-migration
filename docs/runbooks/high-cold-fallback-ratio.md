# Operator Runbook: High Cold-Start Fallback Ratio & Restore Crashes

## Alerts Covered

- **`PodMigrationHighColdFallbackRatio`** (`severity: warning`): Over 15% of completed pod migrations finished as cold-start fallbacks over the last 15 minutes. Note on ratio sensitivity: with `rate[15m]` and `for: 10m`, 1 fallback out of 3 migrations will page (evaluated when total migration rate exceeds 2 / 900s). Note on restore timeouts: migrations completing as `SucceededWithoutRestore` due to timeouts share `outcome="timeout"` with failure timeouts, and are tracked separately under `PodMigrationJobTimeoutsHigh`.
- **`PodMigrationRestoreCrashFallbackDetected`** (`severity: warning`): A replacement pod crashed during warm restore with a recognized gVisor/OCI restore error signature, triggering an automated fallback to cold start.
- **`PodMigrationRestoreCrashSignatureDrift`** (`severity: info`): A replacement pod failed to start with container exit code 128 (`StartError`), but its error message did not match any known restore crash signature in the controller.

---

## 1. Operational Impact

The primary value of live pod migration is preserving application memory, TCP connections, and cached state across maintenance events. When a migration falls back to a cold start:

- The replacement pod launches with an empty container filesystem and uninitialized heap memory.
- Application cold-start latency (JIT compilation, JVM boot, database connection pool ramp-up) is incurred.
- Stateful workloads may experience session loss or cache invalidation storms.

---

## 2. Root Cause Analysis

A cold fallback is triggered by one of several mechanisms:

1. **Restore Crash Fallback:** The replacement pod scheduled and attempted to restore memory from the snapshot, but the container runtime (gVisor sandbox or CRIU daemon) crashed with an exit code 128 (`StartError`). The controller detected the crash signature, safely terminated the failing pod, and relaunched it cold.
2. **Placement Timeout or Incompatible Nodes (#14):** The snapshot engine has node-level hardware/kernel constraints (e.g. CRIU CPU flags, gVisor machine type), and no compatible destination node could schedule the pod before the placement deadline elapsed.
3. **Snapshot Gone or Deleted (#265, #269):** The underlying snapshot in GCS was garbage collected or deleted before the replacement pod was scheduled.
4. **Signature Drift:** Runtimes occasionally update their error strings (e.g. containerd or runsc version bumps). An unrecognized restore crash will leave the replacement pod stuck in a crash loop instead of triggering an automatic cold fallback.

---

## 3. Diagnostic Workflow

### Step 1: Breakdown Restore Verdicts by Engine

Query controller metrics via port-forward to the leader replica:

```bash
LEADER_POD=$(kubectl get lease pod-migration-controller-leader -n pod-migration-system -o jsonpath='{.spec.holderIdentity}')
LEADER_POD="${LEADER_POD%%_*}"
kubectl port-forward -n pod-migration-system pod/$LEADER_POD 8080:8080 &
PF_PID=$!
curl -s http://localhost:8080/metrics | grep -E "pod_migration_restore_verdicts_total|pod_migration_outcomes_total"
kill $PF_PID
```

Analyze the breakdown of verdicts:

- `verdict="cold_start"`: Intentionally initiated or fallen back cold start.
- `verdict="failed"`: Terminal restore failure.
- `verdict="no_compatible_node"`: Scheduler found no node meeting engine placement constraints.
- `verdict="placement_timeout"`: Placement constraints prevented scheduling within the deadline.
- `verdict="snapshot_deleting"` or `snapshot_gone"`: Storage artifact was cleaned up prematurely.

### Step 2: Identify Crashing Replacement Pods

Find recent pods with exit code 128 or `CrashLoopBackOff`:

```bash
kubectl get pods -A -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,STATUS:.status.containerStatuses[*].state,LAST_STATUS:.status.containerStatuses[*].lastState' | grep -E "StartError|exitCode: 128"
```

Inspect the container termination message:

```bash
kubectl get pod <replacement-pod> -n <namespace> -o jsonpath='{range .status.containerStatuses[*].lastState.terminated}{"ExitCode: "}{.exitCode}{"\tReason: "}{.reason}{"\tMessage: "}{.message}{"\n"}{end}'
```

The controller matches gVisor restore failures defined in `controller/internal/restore/gvisor.go`:

- `"oci runtime restore failed"` (observed gVisor sentry restore error)
- `"restore failed"` (generalized containerd/runc restore error)

### Step 3: Check Node Kernel and Runtime Homogeneity

If using the CRIU engine or gVisor across heterogeneous node pools:

```bash
# Check node OS images and kernel versions
kubectl get nodes -o custom-columns='NAME:.metadata.name,OS:.status.nodeInfo.osImage,KERNEL:.status.nodeInfo.kernelVersion,CONTAINER_RUNTIME:.status.nodeInfo.containerRuntimeVersion'
```

CRIU migrations fail if moving between nodes with mismatched kernel capabilities, missing cgroup v2 controllers, or divergent glibc/seccomp profiles.

---

## 4. Safe Remediation Procedures

### Remediation A: Address Heterogeneous Node Pools

If `verdict="no_compatible_node"` or `placement_timeout` is elevated:

1. Ensure destination node pools have the required CRIU agent daemonset running:
   ```bash
   kubectl get ds -n pod-migration-criu-system -l app=pod-migrate-agent
   ```
2. For GKE Pod Snapshots, verify that all target nodes are running the identical GKE node image and containerd version.
3. Check node labels matching engine placement rules (`kubectl get nodes --show-labels`).

### Remediation B: Handle Unmatched Signature Drift

If `PodMigrationRestoreCrashSignatureDrift` is firing:

1. Extract the exact crash message from `.status.containerStatuses[*].lastState.terminated.message`.
2. Inspect the signature table in `controller/internal/restore/gvisor.go`.
3. If a runtime update modified the message format (e.g. new containerd release), file an issue or submit a PR updating `gvisor.go` with the new substring.
4. For immediate relief on stuck crash-looping replacement pods:
   Manually delete the replacement pod so its controller recreates it without the migration annotation.

---

## 5. Fault-Injection Verification

### Test Case: Simulating a Restore Crash Fallback

To test restore crash classification in a dev/staging cluster:

1. Create a test workload with runtimeClass `gvisor`.
2. Initiate a `PodMigrationJob` targeting the pod.
3. Once the replacement pod is created, inject a mock container termination error matching `"oci runtime restore failed"` into the replacement pod's container status (or configure a mock runtime failure).
4. Observe controller logs:
   ```bash
   kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager | grep "Restore crash detected"
   ```
5. Check controller metrics via port-forward to the leader pod:
   ```bash
   LEADER_POD=$(kubectl get lease pod-migration-controller-leader -n pod-migration-system -o jsonpath='{.spec.holderIdentity}')
   LEADER_POD="${LEADER_POD%%_*}"
   kubectl port-forward -n pod-migration-system pod/$LEADER_POD 8080:8080 &
   PF_PID=$!
   curl -s http://localhost:8080/metrics | grep pod_migration_restore_crash_fallback_total
   kill $PF_PID
   ```
6. Verify that `pod_migration_restore_crash_fallback_total` increments and `PodMigrationRestoreCrashFallbackDetected` fires.
