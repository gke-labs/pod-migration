# Operator Runbook: Orphaned or Wedged Scheduling Gates

## Alerts Covered

- **`PodMigrationSchedulingGateWedged`** (`severity: warning`): A pod has carried the scheduling gate `gke.io/pod-migration-gate` for more than 10 minutes without being released.

---

## 1. Operational Impact

Replacement pods injected with `gke.io/pod-migration-gate` are held in the Kubernetes `Pending` state (`SchedulingGated`). If the gate is not released:

- The replacement pod cannot be scheduled by `kube-scheduler`.
- The workload suffers extended downtime or reduced replica capacity.
- Workload autoscalers (HPA) or orchestrators (Deployment/StatefulSet) may become confused by non-starting replicas.

---

## 2. Root Cause Analysis

A scheduling gate remains wedged due to one of four scenarios:

1. **Controller Down or Informer Lag:** `pod-migration-controller` is crashing, partitioned, or experiencing massive API throttling (APF), delaying the `PodGateReconciler` run loop.
2. **Abandoned PMJ:** A `PodMigrationJob` failed or timed out, but the status was not synced to the pod gate reconciler.
3. **ReplicaSet Scale-Up Race:** During an active migration, the parent ReplicaSet scaled up. The mutating admission webhook injected `gke.io/pod-migration-gate` onto the new pod, but the pod has no assigned PMJ or was a race loser.
4. **Deleted PMJ Without Reconciler Sweep:** A user or script forcefully deleted the `PodMigrationJob` object using `--grace-period=0` or while the controller was offline.

---

## 3. Diagnostic Workflow

### Step 1: Enumerate Gated Pods Across All Namespaces

Run the following command to find all pods blocked by `gke.io/pod-migration-gate`:

```bash
kubectl get pods -A -o jsonpath='{range .items[?(@.spec.schedulingGates)]}{"Namespace: "}{.metadata.namespace}{"\tPod: "}{.metadata.name}{"\tPhase: "}{.status.phase}{"\tAssignedPMJ: "}{.metadata.annotations.pod-migration\.gke\.io/assigned-pmj}{"\tGates: "}{range .spec.schedulingGates}{.name}{" "}{end}{"\n"}{end}'
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

- **If PMJ exists and is active (`Snapshotting` / `Evicting`):** The pod gate is legitimately waiting for the source snapshot to finish. Inspect the PMJ using [pmj-phase-stuck.md](pmj-phase-stuck.md).
- **If PMJ exists and is failed / succeeded / deleted:** The reconciler should have released the gate. Proceed to Section 4.
- **If PMJ does not exist (`NotFound`):** The PMJ was deleted. Proceed to Section 4.

---

## 4. Safe Remediation Procedures

### Critical Safety Invariant: Never Strip Gate Without Cold-Start Bypass!

> ⚠️ **CRITICAL WARNING:** When manually releasing `gke.io/pod-migration-gate` on GKE Pod Snapshots workloads, you **MUST NOT** simply delete the gate from `.spec.schedulingGates`.
> If you remove the gate while leaving the pod un-annotated, the GKE Pod Snapshots admission webhook or kubelet runtime may implicitly attempt to restore the pod from any leftover or stale snapshot with a matching name ($I_2$ Silent Cold Start violation)!
>
> You must **simultaneously** inject the cold-start bypass annotation:
> `podsnapshot.gke.io/ps-name: ""`

### Remediation Option 1: Let Controller Auto-Recover (Recommended)

The `PodGateReconciler` contains logic to resolve orphaned gates:

1. Verify that `pod-migration-controller` is running and healthy:
   ```bash
   kubectl rollout status deployment pod-migration-controller -n pod-migration-system
   ```
2. If the PMJ is still around in `Failed` phase, deleting it triggers an immediate gate release:
   ```bash
   kubectl delete pmj <assigned-pmj> -n <namespace>
   ```
3. Watch the pod: within 5 seconds, the controller will read the missing PMJ from the API server, annotate the pod with `podsnapshot.gke.io/ps-name: ""`, and strip `gke.io/pod-migration-gate`.

### Remediation Option 2: Delete the Gated Pod (Simplest Workload Recovery)

If the pod belongs to a Deployment, ReplicaSet, or StatefulSet:

```bash
kubectl delete pod <pod-name> -n <namespace>
```

The workload controller will immediately create a brand-new replica. Since no migration is in flight for the new replica, the webhook will not gate it, and it will schedule cleanly.

### Remediation Option 3: Manual Gate Release with Explicit Cold-Start Bypass

If you must preserve the specific pod object (e.g. static pod or bare pod):

Apply a JSON patch that adds the cold-start bypass annotation and removes the scheduling gate in a single atomic update:

```bash
# 1. Inspect the gate index in spec.schedulingGates
kubectl get pod <pod-name> -n <namespace> -o jsonpath='{.spec.schedulingGates}'

# 2. Apply atomic patch (injecting cold-start bypass and removing gate index 0)
kubectl patch pod <pod-name> -n <namespace> --type=json -p '[
  {"op": "add", "path": "/metadata/annotations/podsnapshot.gke.io~1ps-name", "value": ""},
  {"op": "remove", "path": "/spec/schedulingGates/0"}
]'
```

Verify that the pod transitions from `Pending` to `Scheduled`:

```bash
kubectl get pod <pod-name> -n <namespace>
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
4. Observe Prometheus metric: `kube_pod_spec_scheduling_gate{gate_name="gke.io/pod-migration-gate"}` equals 1.
5. Restore controller:
   ```bash
   kubectl scale deployment pod-migration-controller -n pod-migration-system --replicas=1
   ```
6. Verify that `PodGateReconciler` detects the missing PMJ, injects `podsnapshot.gke.io/ps-name: ""`, removes the gate, and the pod schedules cleanly.
7. Clean up:
   ```bash
   kubectl delete pod test-orphaned-gate-pod -n default
   ```
