# Operator Runbook: High Cold-Start Fallback Ratio & Restore Crashes

## Alerts Covered

- **`PodMigrationHighColdFallbackRatio`** (`severity: warning`): Over 15% of pod migrations completed as cold-start fallbacks over the last 15 minutes.
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
2. **Placement Timeout or Incompatible Nodes:** The snapshot engine has node-level hardware/kernel constraints (e.g. CRIU CPU flags, gVisor version), and no compatible destination node could schedule the pod before the placement deadline elapsed.
3. **Snapshot Gone or Deleted:** The underlying snapshot in GCS or the node agent was garbage collected or deleted before the replacement pod was scheduled.
4. **Signature Drift:** Runtimes occasionally update their error strings (e.g. containerd or runsc version bumps). An unrecognized restore crash will leave the replacement pod stuck in a crash loop instead of triggering an automatic cold fallback.

---

## 3. Diagnostic Workflow

### Step 1: Breakdown Restore Verdicts by Engine

Query Prometheus or the controller metrics endpoint directly:

```bash
kubectl exec -n pod-migration-system deployment/pod-migration-controller -c manager -- curl -s http://localhost:8080/metrics | grep -E "pod_migration_restore_verdicts_total|pod_migration_outcomes_total"
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

Common recognized signatures include:

- `failed to restore container: ...`
- `unable to restore process tree: ...`
- `incompatible sandbox architecture: ...`

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
   kubectl get ds -A -l app=criu-node-agent
   ```
2. For GKE Pod Snapshots, verify that all target nodes are running the identical GKE node image and containerd version.
3. Check node labels matching engine placement rules (`kubectl get nodes --show-labels`).

### Remediation B: Handle Unmatched Signature Drift

If `PodMigrationRestoreCrashSignatureDrift` is firing:

1. Extract the exact crash message from `.status.containerStatuses[*].lastState.terminated.message`.
2. Compare against the signature table in `controller/internal/restore/crash_classifier.go`.
3. If a runtime update modified the message format (e.g. new containerd release), file an issue or submit a PR updating `crash_classifier.go` with the new regex.
4. For immediate relief on stuck crash-looping replacement pods:
   Manually delete the replacement pod so its controller recreates it without the migration annotation.

---

## 5. Fault-Injection Verification

### Test Case: Simulating a Restore Crash Fallback

1. In a staging environment, create a pod with an intentionally invalid snapshot reference:
   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: test-restore-crash-pod
     namespace: default
     annotations:
       podsnapshot.gke.io/ps-name: "invalid-corrupted-snapshot-fixture"
   spec:
     runtimeClassName: gvisor
     containers:
       - name: app
         image: busybox
         command: ["sh", "-c", "sleep 3600"]
   ```
2. Observe container startup failure in `kubectl describe pod test-restore-crash-pod`.
3. Check controller logs:
   ```bash
   kubectl logs -n pod-migration-system -l app=pod-migration-controller -c manager | grep "restore crash"
   ```
4. Verify that `pod_migration_restore_crash_fallback_total` increments and `PodMigrationRestoreCrashFallbackDetected` fires.
5. Clean up:
   ```bash
   kubectl delete pod test-restore-crash-pod -n default
   ```
