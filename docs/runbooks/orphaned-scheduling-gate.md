# Operator Runbook: Orphaned or Wedged Scheduling Gates

## Alerts Covered

- **`PodMigrationSchedulingGateWedged`** (`severity: warning`): A pod has carried a scheduling gate (status reason `SchedulingGated`) for more than 10 minutes without being released (requires kube-state-metrics >= v2.19.0). Note that this metric fires for any gated pod (e.g. Kueue, Volcano); operators must inspect `.spec.schedulingGates` to verify if `gke.io/pod-migration-gate` is present.

---

## 1. Operational Impact

Replacement pods injected with `gke.io/pod-migration-gate` are held in the Kubernetes `Pending` state (`SchedulingGated`). If the gate is not released:

- The replacement pod cannot be scheduled by `kube-scheduler`.
- The workload suffers extended downtime or reduced replica capacity.
- Workload autoscalers (HPA) or orchestrators (Deployment/StatefulSet) may become confused by non-starting replicas.

---

## 2. Root Cause Analysis

A scheduling gate remains held due to one of several scenarios:

1. **Legitimate Long Holds:** The gate is legitimately waiting for an active restore workflow:
   - Snapshot upload/verification is still in flight on a large memory footprint.
   - **Terminating Consumer:** A previous pod instance is still terminating and unmounting volumes.
   - _(Note on node compatibility: with `--restore-placement`, node placement requirements are expressed via node affinity and the scheduling gate is released, leaving the replacement in `Unschedulable` rather than `SchedulingGated`)._
2. **Controller Down or Informer Lag:** `pod-migration-controller` is crashing, partitioned, or experiencing API throttling, delaying `PodGateReconciler`.
3. **Abandoned or Deleted PMJ:** A `PodMigrationJob` failed, timed out, or was deleted while the controller was offline.
4. **ReplicaSet Scale-Up Race:** During an active migration, the parent ReplicaSet scaled up. The mutating admission webhook injected `gke.io/pod-migration-gate` onto the new pod, and the controller is resolving the race loser.

---

## 3. Diagnostic Workflow

### Step 1: Enumerate Gated Pods Across All Namespaces

Run the following command to find all pods blocked by `gke.io/pod-migration-gate`:

```bash
kubectl get pods -A -o jsonpath='{range .items[?(@.spec.schedulingGates)]}{"Namespace: "}{.metadata.namespace}{"\tPod: "}{.metadata.name}{"\tPhase: "}{.status.phase}{"\tAssignedPMJ: "}{.metadata.annotations.pod-migration\.gke\.io/assigned-pmj}{"\tGates: "}{.spec.schedulingGates[*].name}{"\n"}{end}'
```

### Step 2: Inspect Gated Pod Conditions

```bash
kubectl describe pod <pod-name> -n <namespace>
```

Look for:

- Conditions: `Type: PodScheduled, Status: False, Reason: SchedulingGated`
- Annotations:
  - `pod-migration.gke.io/assigned-pmj`: The PMJ coordinating this pod's restore.
  - `podsnapshot.gke.io/ps-name`: Snapshot annotation (if already injected).

### Step 3: Inspect Assigned PMJ State

Check if the assigned PMJ exists:

```bash
kubectl get pmj <assigned-pmj> -n <namespace>
```

- **If PMJ exists and is active (`Snapshotting` / `Evicting` / `Restoring`):** The pod gate is legitimately waiting for checkpoint completion or node placement. Inspect the PMJ using [pmj-phase-stuck.md](pmj-phase-stuck.md). Check if destination nodes satisfy engine placement constraints.
- **If PMJ exists and is failed / succeeded:** The reconciler should release the gate. Check controller logs and leader election lease.
- **If PMJ does not exist (`NotFound`):** The PMJ was deleted. Proceed to Section 4.

---

## 4. Safe Remediation Procedures

### Critical Safety Invariants

> ⚠️ **CRITICAL INVARIANT 1 (Never Strip Gate Directly):** When releasing `gke.io/pod-migration-gate` on GKE Pod Snapshots workloads, you **MUST NOT** simply delete the gate from `.spec.schedulingGates` without the cold-start bypass annotation (`podsnapshot.gke.io/ps-name: ""`). Leaving it un-annotated risks stale snapshot resurrection ($I_2$ Silent Cold Start violation).
>
> ⚠️ **CRITICAL INVARIANT 2 (Active PMJ Adoption Path, #25):** If you delete a gated replacement pod while its assigned `PodMigrationJob` is still active, the workload controller creates a replacement, the mutating webhook injects `gke.io/pod-migration-gate` again, and the adoption path from #25 re-adopts the active PMJ. Deleting the pod alone will NOT release the gate while the PMJ is active!

### Remediation Option 1: Let Controller Auto-Recover (Recommended)

The `PodGateReconciler` automatically resolves orphaned gates:

1. Verify that `pod-migration-controller` is running and healthy:
   ```bash
   kubectl rollout status deployment pod-migration-controller -n pod-migration-system
   ```
2. If the PMJ is wedged or failed, deleting the PMJ triggers immediate gate release:
   ```bash
   kubectl delete pmj <assigned-pmj> -n <namespace>
   ```
3. Watch the pod: within seconds, the controller reads the missing PMJ from the API server, annotates the pod with `podsnapshot.gke.io/ps-name: ""`, and removes `gke.io/pod-migration-gate`.

### Remediation Option 2: Delete PMJ First, Then Delete Gated Pod

If the pod belongs to a Deployment, ReplicaSet, or StatefulSet:

1. Delete the stuck PMJ to clear the active migration state:
   ```bash
   kubectl delete pmj <assigned-pmj> -n <namespace>
   ```
2. Delete the gated pod:
   ```bash
   kubectl delete pod <pod-name> -n <namespace>
   ```
3. Because the PMJ is gone, the newly created replica will not be gated, and it will schedule cleanly.

### Remediation Option 3: Emergency Policy Audit Mode

If the controller is offline and you must manually un-gate a pod:

> 🛑 **NOTE ON RESTORE ORDER POLICY:** Direct manual patching of `podsnapshot.gke.io/ps-name` (or `criu.podmigration.gke.io/ps-name`) by human operators is strictly blocked by the `pod-migration-restore-order` ValidatingAdmissionPolicy, which enforces that only the controller service account may write snapshot restore annotations.

To execute an emergency manual release:

1. Temporarily switch `pod-migration-restore-order` binding to Audit mode:
   ```bash
   kubectl patch validatingadmissionpolicybinding pod-migration-restore-order --type=merge -p '{"spec":{"validationActions":["Audit"]}}'
   ```
2. Find the index of `gke.io/pod-migration-gate` so other controllers' gates (e.g. Kueue, Volcano) are preserved:
   ```bash
   GATE_INDEX=$(kubectl get pod <pod-name> -n <namespace> -o json | jq '.spec.schedulingGates | map(.name == "gke.io/pod-migration-gate") | index(true)')
   ```
3. Patch the pod with the cold-start bypass annotation and remove only the `gke.io/pod-migration-gate`:
   - **For GKE Pod Snapshots:**
     ```bash
     kubectl patch pod <pod-name> -n <namespace> --type=json -p "[
       {\"op\": \"add\", \"path\": \"/metadata/annotations/podsnapshot.gke.io~1ps-name\", \"value\": \"\"},
       {\"op\": \"remove\", \"path\": \"/spec/schedulingGates/${GATE_INDEX}\"}
     ]"
     ```
   - **For CRIU Engine:**
     ```bash
     kubectl patch pod <pod-name> -n <namespace> --type=json -p "[
       {\"op\": \"add\", \"path\": \"/metadata/annotations/criu.podmigration.gke.io~1ps-name\", \"value\": \"\"},
       {\"op\": \"remove\", \"path\": \"/spec/schedulingGates/${GATE_INDEX}\"}
     ]"
     ```
4. Immediately restore the policy binding back to `Deny`:
   ```bash
   kubectl patch validatingadmissionpolicybinding pod-migration-restore-order --type=merge -p '{"spec":{"validationActions":["Deny"]}}'
   ```

---

## 5. Fault-Injection Verification

### Test Case: Simulating an Orphaned Scheduling Gate

1. Scale down the controller deployment to halt reconciliation:
   ```bash
   kubectl scale deployment pod-migration-controller -n pod-migration-system --replicas=0
   ```
2. Create a test pod with `gke.io/pod-migration-gate`:
   ```yaml
   apiVersion: v1
   kind: Pod
   metadata:
     name: test-orphaned-gate-pod
     namespace: default
     annotations:
       pod-migration.gke.io/assigned-pmj: non-existent-pmj
   spec:
     schedulingGates:
       - name: gke.io/pod-migration-gate
     containers:
       - name: nginx
         image: nginx:alpine
   ```
   Apply the manifest:
   ```bash
   kubectl apply -f test-gate-pod.yaml
   ```
3. Verify that the pod enters `Pending (SchedulingGated)`:
   ```bash
   kubectl get pod test-orphaned-gate-pod -n default
   ```
4. Observe Prometheus metric: `kube_pod_status_reason{reason="SchedulingGated"}` equals 1.
5. Restore controller:
   ```bash
   kubectl scale deployment pod-migration-controller -n pod-migration-system --replicas=2
   ```
6. Verify that `PodGateReconciler` detects the missing PMJ, injects `podsnapshot.gke.io/ps-name: ""`, removes the gate, and the pod schedules cleanly.
7. Clean up:
   ```bash
   kubectl delete pod test-orphaned-gate-pod -n default
   ```
