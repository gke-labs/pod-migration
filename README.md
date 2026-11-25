# Pod Migration Controller

This controller provides **Pod Migration**—the ability to "move" a running Pod from one node to another while preserving its internal runtime state (including memory, CPU registers, and system sockets). It uses [GKE Pod Snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots) to reliably capture the state of the pod and restore it. This feature requires the use of the [GKE Sandbox](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/sandbox-pods), which is based on the [gVisor](https://gvisor.dev/) container runtime.

This controller implements Pod Migration by intercepting eviction requests (e.g. during node drains), triggering manual snapshots with container termination (`postCheckpoint: stop`), and managing the scheduling of replacement pods. This reduces downtime and prevents progress loss from evictions caused by:

- **Node upgrades:** When a node is cordoned and drained, the controller captures the state of the pod, preserving in-progress state for Jobs or reducing warm-restore time for stateful database engines.
- **VPA evictions:** When Vertical Pod Autoscaler evicts pods for resizing, the controller preserves the pod's state prior to eviction so the newly resized pod can resume normal operation faster.
- **Other causes:** Responds to manual evictions and third-party controllers triggering Pod evictions.

While this controller mitigates the effects of eviction, the migration is not completely transparent and the application will experience:
- A change in Pod identity (different Pod name suffix and IP address).
- Closed network connections that must be re-established by client-side retry logic.

---

## How it Works

The controller is implemented as a custom Operator consisting of the following core components:

### 1. Webhooks
- **Eviction Interception Webhook (`/validate-v1-pod-eviction`)**: Handled by `EvictionGate`. Intercepts Pod eviction API calls. If the Pod is opted-in (`pod-migration.gke.io/enabled: "true"`), it denies the eviction request with `429 (Too Many Requests)` to block immediate destruction, and spawns a `PodMigrationJob` (PMJ) CR to orchestrate the migration flow. A replacement Pod that its own migration is still restoring isn't migrated again until that migration concludes: if its new node is also being drained (or for a non-drain eviction), the webhook answers `429` so the evictor retries once restore concludes (#162); if it is a same-name replacement (`StatefulSet`) already off the cordoned origin node and onto an uncordoned node, the webhook answers `404 (Not Found)` so `kubectl drain` on the origin node sees the original Pod is gone and exits cleanly without re-migrating the replacement.
- **Replacement Webhook (`/mutate-v1-pod-scheduling-gate`)**: Handled by `PodGateInjector`. Intercepts Pod creation requests. For replacement pods matched to an active `PodMigrationJob`, it injects the `gke.io/pod-migration-gate` scheduling gate (and scrubs untrusted `<group>/ps-name` restore annotations across registered engines) to hold the pod in a pending state until checkpoint readiness and volume detachment complete, preventing replica collisions; by default (`--shorten-restored-readiness-delay=true`), it also sets `readinessProbe.initialDelaySeconds` to `0` on the replacement's containers and restartable sidecar init containers unless the pod sets `pod-migration.gke.io/preserve-readiness-delay: "true"`, leaving liveness and startup probes untouched (#141). On scale-up pods, it stamps per-engine cold-start bypasses.
- **Status Mutating Webhook (`/mutate-v1-pod-status`)**: Handled by `PodStatusMutator`. Intercepts Pod status updates. For migrating Job-owned Pods, it overrides the final exit code to `137` and status to `Failed`. This allows the Kubernetes Job controller to reschedule the Pod without depleting its `backoffLimit` retry budget (see Job Rescheduling section).
- **Fence Webhook (`/mutate-v1-pod-fence`)**: Handled by `PodFenceMutator`. Intercepts status updates of Pods labeled `pod-migration.gke.io/fenced: "true"` and holds their `Ready` condition at `False` (reason `PodMigrationFenced`), so a fenced migration source stays out of its Services' endpoints (#131). The other conditions, including `ContainersReady`, keep the kubelet's view. It uses `failurePolicy: Fail`: while the webhook is down, a fenced Pod's status writes are rejected, so it stays NotReady, and it can't finish deleting either, because the kubelet writes a Pod's final status before the Pod is removed. That heals when the webhook is back. Pods without the label never reach it. See [Source fence](#3-source-fence).
- **Restore Order Policy (`pod-migration-restore-order`)**: A `ValidatingAdmissionPolicy` in `controller/deploy.yaml`, not a webhook: the API server evaluates it itself, so it doesn't depend on the controller being up. The replacement webhook handles restore orders only when a Pod is created. Afterwards, on a Pod that is opted in, only the controller may set, change or remove a restore order (`<group>/ps-name`, for any engine) (#233). It covers updates of the Pod and of its `status` subresource, which can change annotations too. The policy is skipped for every update that leaves the restore orders as they are, so labels (including removing `pod-migration.gke.io/enabled` to opt out), other annotations, the spec and the kubelet's status writes aren't affected. See [Changing a Restore Order by Hand](#changing-a-restore-order-by-hand).

### 2. Reconcilers
- **`PodMigration` (Config) Reconciler**: Watches `PodMigration` resources. It automatically provisions the required GKE `PodSnapshotStorageConfig` (PSSC) and manual `PodSnapshotPolicy` (PSP) resources with `postCheckpoint: stop` in the target namespace. It also watches those two objects. If someone edits their spec, it changes it back right away. If someone deletes one, it recreates it right away. If one is stuck being deleted, for example because another component's finalizer holds it, the controller doesn't touch it and sets the PodMigration's `Ready` condition to `False` with reason `ManagedObjectTerminating`. Once the object is gone, the controller recreates it (#144).
- **`PodMigrationJob` (Execution) Reconciler**: Coordinates the active migration loop:
  1. Records the source Pod's node on the job (`status.originNodeName`). While the job is `Pending`, before the budget check and the fence, it also records that node's machine type (its `node.kubernetes.io/instance-type` label) in `status.originNodeInstanceType`, and its kubelet version, which on GKE is the node's GKE version, in `status.originNodeKubeletVersion`. A snapshot only restores on a compatible node, and the source node can be gone by the time the replacement is released, so these are captured first. They're written with the rest of the job's status, which by default only the controller can write. This is best effort: if the node can't be read or doesn't carry a value, that field stays empty and the migration goes ahead. A recorded value is never replaced. With `--restore-placement` (on by default), the Pod Gate Reconciler places a replacement that its ReplicaSet or StatefulSet would recreate by the machine type (see below, #14).
  2. Fences the source Pod (see [Source fence](#3-source-fence)), then creates a `PodSnapshotManualTrigger` (PSMT) targeting it to initiate snapshotting and container termination.
  3. Monitors the resulting `PodSnapshot` until it becomes `Ready` (uploaded to GCS). A `PodSnapshot` that's being deleted never counts as `Ready` (#265): the job fails with reason `SnapshotDeleting` instead (see [Source fence](#3-source-fence)). Nor does one that's gone (#269). The trigger can name a snapshot before it's created, so a snapshot that isn't found only counts as gone once the job has recorded seeing it (`SnapshotObserved=True`) and the API server confirms it's missing. The job then fails with reason `SnapshotGone`. A snapshot never seen is waited for, as before, until the migration timeout. A job that times out while its trigger is still unanswered says so in its `Ready` message and points at the trigger's events.
  4. Transitions the job to `Evicting` phase, which signals the Eviction Webhook that it is safe to allow the source Pod deletion.
  5. Waits for the source Pod to be deleted and its volumes to fully detach from the GCE node.
  6. Transitions the job to `Restoring`, signaling the Pod Gate Reconciler that it is safe to release the replacement Pod.
  7. Watches the restore and concludes the job: `Succeeded` once the restore is verified or inferred; `SucceededWithoutRestore` if the replacement runs without its restored state, if the restore isn't confirmed in time (for example after a cold start or the restore timeout), or if no node takes the restore (`PlacementTimeout`, with `--restore-placement`, or `NoCompatibleNode` for a replacement that an earlier version held), or if its `PodSnapshot` is being deleted or is gone before the replacement is released (`SnapshotDeleting`, #265, or `SnapshotGone`, #269); or `Failed` (for example after a restore crash).
  8. Deletes the job's `PodSnapshot` once the job is `Succeeded` or `Failed` and nothing can still restore from it (#107). See [Checkpoint garbage collection](#checkpoint-garbage-collection).
- **Pod Gate Reconciler**: Monitors replacement Pods injected with the `gke.io/pod-migration-gate` scheduling gate. It holds them in a pending state and releases the gate once the matched `PodMigrationJob` reaches `Restoring` (confirming snapshot readiness and volume detachment), then injects the GKE snapshot name annotation to force restore, and removes the scheduling gate to allow startup. It doesn't release a replacement for a restore while the job's `PodSnapshot` is being deleted, or once it's gone. It holds the replacement until the job concludes `SucceededWithoutRestore` (reason `SnapshotDeleting` or `SnapshotGone`), then releases it cold (#265, #269). It bypasses gate injection for normal scale-up pods.
  - **Placement (controller flag `--restore-placement`, on by default, #14; `--restore-placement=false` turns it off):** in the update that removes the gate, it also adds the snapshot engine's node constraints to the Pod's required node affinity, if a ReplicaSet or StatefulSet would recreate the Pod (see below). Kubernetes accepts that change only while the Pod is gated, so the constraints can't be dropped after the release.
    - For GKE Pod Snapshots the constraint is the source node's machine type, `node.kubernetes.io/instance-type In [<type>]`, because a snapshot restored on another machine type can fail and crash-loop. The machine type is the one the job recorded in `status.originNodeInstanceType` while it was `Pending` (step 1 above). If that's empty, because the source node couldn't be read then, had no such label, or the job was past `Pending` when the controller was upgraded, the machine type is unknown, and the replacement gets no constraint and no check, as with the flag off. So does a replacement whose own node selector, or every term of its own required node affinity, already requires the constraint: a Pod recreated in its place would have it too. Its `MigrationRestoreReleased` event then ends with "without adding node constraints". The node's GKE version must match too, but no node label carries it, so it isn't a constraint: a replacement on a node of another GKE version, for example during a node pool upgrade, can still start cold. Nodes of one machine type can also run on different CPU platforms, for example E2 on Intel or AMD hosts, or N1 on several Intel generations, so a restore on a node of the right type can still fail.
    - Only a Pod that its ReplicaSet or StatefulSet would recreate (a Deployment's Pods belong to a ReplicaSet) gets the constraints, because if it can't be scheduled with them, the controller deletes it for its owner to recreate without them (below). Any other Pod, such as a Job's Pod, an operator's Pod or a Pod without a controller, is released without them, as with the flag off: nothing would recreate it without them if it got stuck, and a Job would count a deleted Pod as failed toward its `backoffLimit`. Such a Pod can land on a node of another machine type, where its restore can fail. That's handled as with the flag off: the restore-crash fallback deletes it, so that a Job recreates it cold (see [Job Rescheduling Configuration](#3-job-rescheduling-configuration) for its `backoffLimit`), and a Pod without a controller isn't recreated.
    - At the release, the controller checks whether some node is Ready and not being deleted, isn't cordoned (unless the Pod tolerates `node.kubernetes.io/unschedulable`), matches the Pod's node selector and required node affinity with the constraints added, and has no `NoSchedule` or `NoExecute` taint that the Pod doesn't tolerate. The Ready check is stricter than the scheduler, which leaves not-ready nodes to their taints. If no node passes, the Pod is released with the constraints anyway, so that a node can still come up: one that isn't Ready yet, one being replaced, or one that the cluster autoscaler adds once it sees the Pod, which it doesn't while the Pod is gated. Its `MigrationRestoreReleased` event then says that no node meets the constraints yet. A retry of a release that was already under way, for example because an earlier attempt failed partway through, adds the constraints again without a new check, or none if `--restore-placement` was turned off in between; the job's record follows what it adds.
    - An earlier version of the controller held a replacement to start cold when no node met the constraints, and recorded `RestorePlacement=False` (reason `NoCompatibleNode`) on the job. Such a job still concludes `SucceededWithoutRestore` with that reason, and its Pod is released without the constraints and starts cold, even if a node fits by then or `--restore-placement` is turned off meanwhile.
    - The job records every release with constraints as `RestorePlacement=True` (reason `NodeConstraintsAdded`). Suppose the scheduler still can't place that Pod after `--restore-placement-deadline` (3 minutes by default; it must be more than 0 and at least 1 minute shorter than the restore timeout, which is 5 minutes plus `--restore-stabilization-window`, so at most 4m30s with the default window, or the controller doesn't start). If a node that doesn't meet the constraints could take the Pod without them, judged as its owner would recreate the Pod, with only the constraints the release added taken out (the Pod's `pod-migrate.io/restore-placement-added` annotation records how many went into each term, so a requirement the Pod's own template has, such as `kubernetes.io/arch In [amd64]`, still counts), the job concludes `SucceededWithoutRestore` with reason `PlacementTimeout`, with Warning events on the job and the Pod, and the Pod is deleted so that its owner recreates it. The new Pod starts cold, without the constraints, and can land on that node. If no such node exists, for example in a pool of one machine type that is full until the cluster autoscaler adds a node, the recreated Pod would wait for the same node, so the Pod keeps waiting and can still restore. At the restore timeout, when the job gives up on the restore, a Pod that still can't be scheduled is deleted anyway, also with reason `PlacementTimeout`, because no node of the type may ever come back, and without the constraints the cluster autoscaler can add any node for it, for example in a fallback pool that scales from zero. A Pod that a slower scale-up would have placed later starts cold instead of restoring late. The job concludes first, and the delete only goes ahead while the Pod is the one the job recorded and the scheduler still reports it unschedulable. If the scheduler places the Pod just before the delete, the Pod is kept and may still restore the checkpoint, and a Warning event on the job says so. Since the job has concluded, nothing marks such a late restore as complete: the Pod keeps its restore order, so a sandbox restart later can restore the checkpoint again (#152). If the scheduler nominates a node for it just before the delete, to make room there by preempting other Pods, the Pod is kept for now, also with a Warning event on the job, and looked at again every 15 seconds: it's deleted if it becomes unschedulable again. A delete that fails is retried every 15 seconds too, until the job is garbage collected, 30 minutes after it concluded; it doesn't hold the job's other cleanup.
    - The deadline also covers what the check doesn't look at: resources, the zone of a bound volume, host ports, and pod affinity and topology spread. Like the check, it doesn't look at those when it looks for a node without the constraints, so the recreated Pod may still have to wait. It only acts on a Pod that the scheduler tried and couldn't place (`PodScheduled=False` with reason `Unschedulable`): not on one that another scheduling gate holds, or one with a nominated node, which the scheduler places once it has preempted Pods for it. If such a Pod is still in that state at the restore timeout, it stays `Pending`: the job concludes `SucceededWithoutRestore` with reason `RestoreTimeout`, but the Pod keeps the constraints and the restore order, and can still restore once it's scheduled. Until the job is deleted, 30 minutes after it concluded, the controller keeps checking such a Pod, and deletes it once the scheduler reports that no node can take it, for example after a nominated node fell through. The job keeps reason `RestoreTimeout`, and `PlacementTimeout` Warning events on the job and the Pod say why the Pod was deleted. After the job is deleted, nothing deletes such a Pod. If it still waits with the constraints then, or you delete the job earlier, delete the Pod yourself if a ReplicaSet or StatefulSet owns it, so that the owner recreates it without them. A Pod that an earlier version released with the constraints has no record on its job, so none of this applies to it: if it stays `Pending`, delete it yourself.

### 3. Source Fence

After GKE Pod Snapshots checkpoints the source Pod, it stops it, and the kubelet restarts it in a new sandbox. That instance starts cold, so if its Services still sent it traffic, it would serve without the checkpointed state, and the writes it acknowledged would be lost (#131). So right before the checkpoint trigger, after the [budget check](#poddisruptionbudgets-and-single-replica-minavailable1-workloads), the controller fences the source:

1. It records condition `SourceFenced=True` (reason `Fenced`) on the job.
2. It labels the Pod `pod-migration.gke.io/fenced: "true"`, then sets its `Ready` condition to `False` (reason `PodMigrationFenced`) through `pods/status`. From then on the fence webhook holds `Ready` at `False`, so the Pod leaves its Services' endpoints and doesn't come back, even after the restart.
3. It records the Pod's container IDs and restart counts on the job (annotation `pod-migration.gke.io/fence-baseline`), and emits a `SourceFenced` event. Only then does it create the trigger.

A successful migration deletes the source, and the label goes with it. When a migration fails, the controller decides what happens to the fenced source:

| Situation | What the controller does |
|---|---|
| The budget check refused the disruption | Nothing: the source was never fenced. |
| The trigger was rejected (forbidden, invalid, denied by admission) | The job fails with reason `TriggerCreateFailed`. Nothing was checkpointed, so the source is unfenced (next row). |
| Creating the trigger fails with retryable errors | Retried for up to `--fence-pre-checkpoint-cap` (default 30 s); then the job fails with reason `FenceCapExceeded`, and the next row applies. |
| The job's `PodSnapshot` is being deleted after its checkpoint completed (reason `SnapshotDeleting`, #265) | The source stays fenced (next row but one), recorded in the same status write that fails the job, so the record outlives the snapshot. A snapshot being deleted whose checkpoint hasn't completed is handled as any other failure (next row). |
| The job's `PodSnapshot`, seen before, is gone (reason `SnapshotGone`, #269) | A fenced source stays fenced (next row but one), recorded in the same status write that fails the job. Whether the checkpoint completed can't be read any more, so the fence is kept either way. |
| Any other failure (snapshot failure, timeout, strict-mode invariant failure) | The trigger is deleted, and the controller waits up to 30 s for the snapshot to finish or go away. If the snapshot never completed its checkpoint, and the source still runs the containers it was fenced with, the source can't have stopped: the controller sets `SourceFenced=False` (reason `Unfenced`), removes the label, and emits `SourceUnfenced`. The kubelet's next status write, within about 10 s, sets `Ready` again. |
| Otherwise (the checkpoint completed, a container changed or restarted, or the snapshot didn't finish in time) | The source stays fenced (`SourceFenced=True`, reason `FenceKept`, Warning event `SourceKeptFenced`), and the controller evicts it, honouring its PodDisruptionBudgets and its grace period, so that its workload controller replaces it. A Pod without a controller isn't evicted, because nothing would replace it: delete it to release it. |
| A source returned to service stops anyway | For `--fence-late-stop-watch` (default 5 min) after an unfence, the controller checks the source's containers. If they change, it fences the source again (Warning event `SourceRefencedLateStop`) and evicts it. |

Notes:
- The webhook must be installed: the controller only sets `Ready` to `False` once, and without the webhook the kubelet's next status write sets it back.
- A fenced source counts as unhealthy for its PodDisruptionBudgets, so the budget checks of its siblings see the smaller budget.
- The fence covers Service endpoints only. Services with `publishNotReadyAddresses: true`, clients that connect to the Pod IP directly, and the restarted instance's own outbound work and volume writes aren't fenced.
- Deleting an active job settles its source fence before removing the `pod-migration.gke.io/source-fence` finalizer (unfencing if the checkpoint never ran, or keeping the fence and evicting if it did). If eviction remains blocked past the unwedge timeout (default 2 minutes), the finalizer is removed to avoid wedging deletion, leaving the source fenced (Warning event `SourceFenceDeleteTimedOut`): release the source by deleting the Pod (so its controller replaces it), or remove the `pod-migration.gke.io/fenced` label if you know its checkpoint never ran. If the controller is down or rolling back to a version without the finalizer handler, unwedge a deleting job with: `kubectl patch podmigrationjob <name> --type=merge -p '{"metadata":{"finalizers":null}}'`.
- The fence needs RBAC on `pods/status` (`get`, `patch`), which `deploy.yaml` grants.

### 4. Design Docs & Security
- [Snapshot engine contract](docs/design/engine-contract.md): what the controller requires from a snapshot engine (GKE Pod Snapshots today; CRIU and upstream pod checkpoint/restore next).
- [Safety invariants](docs/invariants.md): the invariants the controller can check after each reconcile (off by default; see `--invariant-mode`).
- [Threat model & security guidance (CRIU engine)](docs/threat-model-criu.md): threat model, node agent privileges, restore path authorization, containerd posture, and GCS storage guidance.
- [Security policy](SECURITY.md): vulnerability reporting and disclosure guidelines.

---

## Prerequisites

Before deploying the Pod Migration Controller, ensure your cluster meets the following requirements:

### 1. GKE Standard Cluster with Pod Snapshots Enabled
A GKE Standard cluster with the GKE Pod Snapshots addon enabled.
- **Minimum GKE Version**: `1.36.0-gke.2253000` or later (required to support GKE Pod Snapshots with VPA and manual triggers).
- **Release Channel**: Depending on availability in your region, you may need to use the `Rapid` release channel to obtain a compatible version.

Example cluster creation command (using Rapid channel):
```bash
gcloud container clusters create pod-migration-cluster \
  --release-channel=rapid \
  --workload-pool=<YOUR_PROJECT>.svc.id.goog \
  --enable-pod-snapshots \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### 2. gVisor Node Pool
At least one node pool must have gVisor sandboxing enabled (`--sandbox=type=gvisor`) to run the sandboxed workloads:
```bash
gcloud container node-pools create gvisor-pool \
  --cluster=pod-migration-cluster \
  --sandbox=type=gvisor \
  --machine-type=n2-standard-4 \
  --num-nodes=2 \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### 3. cert-manager
`cert-manager` must be installed to manage TLS certificates for the admission webhooks:
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=5m -n cert-manager deployment/cert-manager-webhook
```

---

## Installation Guide

Follow these steps to deploy the Pod Migration Controller once the prerequisites are met:

### Step 1: One-Time Snapshot Storage & Workload Identity Configuration
Run the provided setup script to automatically create the GCS bucket, configure IAM permissions for Workload Identity, and create the test Kubernetes Service Account (KSA):

```bash
chmod +x patches/setup-storage.sh
./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]
```

*This script performs the following:*
- Creates a GCS bucket `gs://<BUCKET_NAME>` with uniform bucket-level access and soft-delete disabled.
- Binds IAM roles `roles/storage.objectUser` and `roles/storage.bucketViewer` to all KSAs in the `default` namespace and the GKE container engine robot service account.
- Creates KSA `pm-test-ksa` in the `default` namespace.

This bucket is for GKE Pod Snapshots. The experimental CRIU engine needs a bucket of its own, which grants object access to its two service accounts and to nobody else: an archive is a full memory dump. Set one up with `./patches/setup-criu-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]`, and point only the `PodMigration`s of namespaces that only CRIU serves at it (see "Storage credentials" in `engines/criu/README.md`).

### Step 2: Install CRDs
Install the custom resource definitions for the pod migration controller:
```bash
kubectl apply -f controller/config/crd/bases/
```

### Step 3: Deploy the Pod Migration Controller
The controller manager orchestrates the migration lifecycle and hosts the admission webhooks.

1.  **Build and push the controller image:**
    ```bash
    make -C controller docker-build IMG=<YOUR_REGISTRY>/pod-migration-controller:latest
    docker push <YOUR_REGISTRY>/pod-migration-controller:latest
    ```

2.  **Configure the GCS Bucket:**
    Edit `controller/podmigration-config.yaml` to configure your target GCS bucket for snapshots:
    ```yaml
    spec:
      storage:
        location: gs://<YOUR_BUCKET_NAME>/snapshots
    ```

3.  **Deploy the Controller:**
    Apply the manifests by replacing the `<YOUR_CONTROLLER_IMAGE>` placeholder:
    ```bash
    sed 's|<YOUR_CONTROLLER_IMAGE>|<YOUR_REGISTRY>/pod-migration-controller:latest|' controller/deploy.yaml | kubectl apply -f -
    
    # Apply the storage config
    kubectl apply -f controller/podmigration-config.yaml
    
    # Verify deployment status
    kubectl rollout status deployment/pod-migration-controller -n pod-migration-system
    ```

> [!IMPORTANT]
> **Upgrading from Single-Replica to Multi-Replica HA**:
> The controller runs in High Availability (2 replicas) with active/standby Leader Election enabled by default. When upgrading an existing cluster from the legacy un-elected single-replica deployment, scale down and wait for the old pod to fully terminate before applying the HA manifests:
> ```bash
> kubectl scale deployment/pod-migration-controller -n pod-migration-system --replicas=0
> kubectl wait --for=delete pod -l app=pod-migration-controller -n pod-migration-system --timeout=60s
> sed 's|<YOUR_CONTROLLER_IMAGE>|<YOUR_REGISTRY>/pod-migration-controller:latest|' controller/deploy.yaml | kubectl apply -f -
> ```

### Step 4: Deploy Validating Policies (Optional)
To reject incompatible workloads (e.g. BEAM/fsnotify) at admission time:
```bash
kubectl apply -f patches/gke-pod-snapshot-admission-webhook.yaml
```

---

## 1. Migration Compatibility Matrix (E2E Verified)

Every workload in this matrix has been verified through real E2E eviction migrations on a GKE Standard node pool.

> [!NOTE]
> - **Job Rescheduling**: Workloads of type `Job` (e.g., `go`, `node`) are supported for resilient rescheduling using the Status Mutating Webhook.
> - **NATS Caveat**: `nats` is marked as flaky due to a transient runtime-level deadlock during checkpointing; see troubleshooting notes below.

| Application | Category | Verdict | Required Workarounds & Bypass Configurations |
| :--- | :--- | :--- | :--- |
| **node** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet and Job (rescheduled via webhook). Out-of-the-box support. |
| **go** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet and Job (rescheduled via webhook). Out-of-the-box support. |
| **redis** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Very fast migration. |
| **valkey** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Very fast migration. |
| **mysql** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **mariadb** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **memcached** | datastore | ✅ **SURVIVED** | Pure in-memory cache blocks restored. |
| **dragonfly** | datastore | ✅ **SURVIVED** | **epoll Bypass**: Requires epoll forcing flag (`--force_epoll`). Verified with redis-client. |
| **vault** | secrets | ✅ **SURVIVED** | Dev-mode secrets state restored. |
| **consul** | coordination | ✅ **SURVIVED** | Dev-mode memory key-value databases survive. |
| **etcd** | coordination | ✅ **SURVIVED** | BoltDB storage writes successfully restored. |
| **nats** | streaming | ⚠️ **SURVIVED (FLAKY)** | **Deadlock Flake / Retry Caveat**: Memory jetstream offsets restored. Encountered runtime-level deadlock on 1st run. Succeeded on retry. |
| **zookeeper** | coordination | ✅ **SURVIVED** | **emptyDir Path Redirect**: Redirect `ZOO_DATA_DIR` away from Kubelet `emptyDir` mounts (e.g. to `/tmp/zookeeper`) to prevent walk errors. |
| **kafka** (KRaft) | streaming | ✅ **SURVIVED** | **JVM Metrics Bypass**: Inject environment variable `KAFKA_OPTS="-XX:-UseContainerSupport"` to avoid cgroups mismatch crashes on target nodes. |
| **postgres** | datastore | ✅ **SURVIVED** | Works out-of-the-box (uses guest POSIX shared memory). Requires setting `PGDATA` to container local directories. |
| **minio** | datastore | ✅ **SURVIVED** | Redirect storage paths to container writable layers to avoid emptyDir mount walk failures. |
| **nginx** | proxy | ✅ **SERVED** | Stateless proxies restore and handle reconnected traffic. |
| **haproxy** | proxy | ✅ **SERVED** | Stateless proxies restore and handle reconnected traffic. |
| **traefik** | proxy | ✅ **SERVED** | Stateless routers restore and handle reconnected traffic. |
| **caddy** | proxy | ✅ **SERVED** | Stateless routers restore and handle reconnected traffic. |
| **python** (HTTP) | app | ✅ **SERVED** | Stateless python workers survive. |
| **mongodb** | datastore | ❌ **FAILED** | WiredTiger storage engine locks and blocks on sandboxed `io_uring` seccomp. |
| **cassandra** | datastore | ❌ **FAILED** | Large JVM heaps mismatch host cgroups descriptors post-restore. |
| **cockroachdb** | datastore | ❌ **FAILED** | Raft synchronization timeouts and socket reset crashes post-restore. |
| **clickhouse** | datastore | ❌ **FAILED** | Columnar block datastore sync locks and file descriptor leaks. |
| **rabbitmq / couchdb** | streaming | ❌ **FAILED** | Erlang BEAM runtime epoll and green thread scheduler structures cannot be serialized. |
| **prometheus** | monitoring | ❌ **REFUSED** | Active WAL memory mappings exceed serialization limits under both runtimes. |
| **elasticsearch** | search | ❌ **REFUSED** | Heavy `fsnotify` directory watches cannot be serialized. |

---

## 3. Job Rescheduling Configuration

To support resilient rescheduling of Jobs during migration, the system uses a **Status Mutating Webhook** combined with Kubernetes **Job Pod Failure Policy**.

A migrating Job Pod can terminate for two distinct reasons, and **each needs its own `podFailurePolicy` match**. Cover both, or the Job will burn `backoffLimit` retries on events that are not application failures.

### 3.1 Migration eviction — exit code `137`

1. When a migrating Job Pod is terminated, the `pod-migration-controller`'s status mutating webhook (`/mutate-v1-pod-status`) intercepts the status update.
2. It mutates the Pod phase to `Failed` and sets the container exit code to `137`.
3. The Job controller interprets this exit code according to the Job's `podFailurePolicy`.
4. If configured correctly, the Job controller ignores this failure and recreates the Pod (which then restores from the snapshot) without counting it against the Job's `backoffLimit` retry budget.

### 3.2 Restore-crash cold-start fallback — exit code `128`

When a replacement Pod fails to restore from its snapshot, the controller fails the migration and **deletes the Pod** so its Job recreates it cold. This is a recovery action, not an application failure, but it is **not** covered by the rule above:

- The status webhook only rewrites exit codes on Pods transitioning to `Succeeded` during an active `Snapshotting`/`Evicting` migration. The crashed replacement Pod is in neither state, so it is never touched and never receives the synthetic `137`.
- A container the runtime failed to start terminates with `reason: StartError` and **exit code `128`**.

Without a match for `128`, every cold-start fallback silently consumes one of the Job's `backoffLimit` retries.

> [!IMPORTANT]
> **Precondition:** `onExitCodes` matches the exit code alone — it cannot additionally require `reason: StartError`. An `Ignore` match on `128` therefore assumes **your application never exits `128` itself**. If it can, leave `128` out and treat these as application failures; you will spend one retry per fallback instead.

<details>
<summary>Why not a <code>DisruptionTarget</code> condition rule?</summary>

Kubernetes only adds the `DisruptionTarget` Pod condition for evictions via the Eviction API, kubelet node-pressure and graceful shutdown, the taint manager, PodGC, and scheduler preemption. The fallback issues a direct `DELETE`, so the condition is never set and such a rule would never fire.

Routing the fallback through the Eviction API to obtain the condition would make it PDB-aware — and the crashed Pod is not Ready, so under the default `unhealthyPodEvictionPolicy: IfHealthyBudget` the eviction can be **refused**, wedging the exact Pod the fallback exists to remove. Separately, `onPodConditions` matches on type and status but not `reason`, so the rule would also swallow preemptions and node-pressure evictions. The exit code is both narrower and more reliable here.
</details>

### Job Configuration Example:
To enable this, your Job manifest must:
1. Enable migration via labels.
2. Configure `podFailurePolicy` to `Ignore` exit codes `137` (migration eviction) and `128` (restore-crash fallback).

Here is an example snippet (from `verification-suite/manifests/pm-go-job.yaml`):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pm-go-job
spec:
  backoffLimit: 4
  podFailurePolicy:
    rules:
      - action: Ignore
        onExitCodes:
          operator: In
          # 137: pod evicted by migration (synthesized by the status webhook)
          # 128: container start failure -> restore-crash cold-start fallback
          values: [137, 128]
  template:
    metadata:
      labels:
        pod-migration.gke.io/enabled: "true"
    spec:
      runtimeClassName: gvisor
      restartPolicy: Never
      # ... rest of the spec
```

---

## 4. How to Use Pod Migration

To migrate your workload using this controller:

### Step 1: Opt-in your Workload
Add the label `pod-migration.gke.io/enabled: "true"` to your workload Pod template and ensure the Pod uses the `gvisor` runtime.

The eviction webhook only migrates Pods whose runtime class is listed in the controller flag `--migratable-runtime-classes` (default `gvisor`; the token `@default` matches Pods that set no `runtimeClassName`). Only list a class that your installed snapshot engine supports: GKE Pod Snapshots supports `gvisor`.

A snapshot engine serves each migratable runtime class. By default, the controller registers only GKE Pod Snapshots (`gke-pod-snapshots`, using `podsnapshot.gke.io`), which serves every class in `--migratable-runtime-classes`. When `--runtime-class-engines` routes a class to `criu` (for example `--migratable-runtime-classes=gvisor,@default` and `--runtime-class-engines=gvisor=gke-pod-snapshots,@default=criu`) or `--criu-api-group` is set, the controller also registers the experimental CRIU engine (`criu`), defaulting to `criu.podmigration.gke.io` (`criu.podmigration.gke.io/ps-name`) so both engines coexist in the same cluster without CRD collisions. A `PodMigration` overrides routes for its namespace with the `pod-migration.gke.io/runtime-class-engines` annotation, in the same syntax; its `EngineRoutesValid` condition reports whether the annotation is valid. A Pod whose class no engine serves is evicted without a migration. So is a Pod that its engine can't migrate, for example a Pod the CRIU engine refuses because its container has no explicit `command`: the eviction webhook and preemption controller record a `Warning MigrationSkippedIneligible` event on the Pod that names the engine and its reason (#275). The engine picked for a Pod is recorded on its `PodMigrationJob` (annotation `pod-migration.gke.io/engine`) and serves the job to the end; a job that names an engine the controller doesn't have fails with reason `EngineNotRegistered`.

An engine that does not fence the source Pod after checkpointing (`FencingNone` or unset) leaves the source running alongside the replacement until evicted. To migrate a Pod using an unfenced engine, the Pod must explicitly opt in with the annotation `pod-migration.gke.io/allow-unfenced: "true"`. Without this annotation, the eviction webhook and preemption controller refuse live migration, record a `Warning MigrationSkippedUnfencedEngine` event on the Pod, and fall back to cold eviction.

Example (Deployment):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
        pod-migration.gke.io/enabled: "true" # Enables migration orchestration
    spec:
      runtimeClassName: gvisor # Required for GKE Pod Snapshots
      containers:
        - name: app
          image: my-app-image:latest
```

#### Readiness Probe Initial Delay (#141)

The kubelet treats a restored container as a new container, so a long `readinessProbe.initialDelaySeconds` would delay `Ready` for the full initial delay even when the restored process is serving within seconds. By default (`--shorten-restored-readiness-delay=true`), the replacement webhook sets `readinessProbe.initialDelaySeconds` to `0` on containers and restartable sidecar init containers of gated replacement Pods, while leaving `livenessProbe`, `startupProbe`, and `periodSeconds` untouched so slow cold starts are not restarted.

Because Pod probe specs are immutable after creation, any gated Pod that starts cold keeps `initialDelaySeconds: 0` and joins its Service endpoints as soon as its readiness probe passes (the endpoint fence covers only the source Pod). That applies to Pod gate cold releases (a deleted, failed, snapshot-less, `NoCompatibleNode`, `SnapshotDeleting` or `SnapshotGone` `PodMigrationJob`, or a concurrent-creation tie-break loser released as a normal scale-up Pod), runtime cold fallbacks (`FallbackToColdStart`, `RestoreIncompatibleNode`), and later in-place container restarts after a migration (`#152`). Restore-crash fallbacks delete and recreate the Pod after the job concludes, so the recreated Pod keeps its configured delay.

- **Opt-out:** If a workload's readiness endpoint answers healthy before initialization completes and relies on `initialDelaySeconds` to hold traffic back during a cold start, set the Pod annotation `pod-migration.gke.io/preserve-readiness-delay: "true"` (exact `"true"`) or pass `--shorten-restored-readiness-delay=false` on the controller.
- **Manifest recommendation:** Where you control the Pod manifest, prefer a short-period `startupProbe` with `readinessProbe.initialDelaySeconds: 0` instead of a long `initialDelaySeconds`, which speeds up both warm restores and cold starts without waiting on a long `periodSeconds` interval.

### Step 2: Trigger Migration via Eviction
The controller automatically intercepts standard Kubernetes evictions and orchestrates the stateful migration. You can trigger this manually (e.g. for testing node upgrades) by draining the node the Pod is running on:

1. **Find the node the Pod is running on:**
   ```bash
   kubectl get pod -l app=my-app -o wide
   ```
2. **Drain the node to trigger eviction:**
   ```bash
   kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
   ```
3. **Uncordon the node** once the migration starts to make it available again for rescheduling:
   ```bash
   kubectl uncordon <node-name>
   ```

### Step 3: Check the Restore Verdict
The `PodMigrationJob`'s `Restored` condition records how the replacement Pod came up, and its message names the snapshot engine and the evidence:

```bash
kubectl get podmigrationjob <job-name> -o jsonpath='{.status.conditions[?(@.type=="Restored")]}'
```

| Phase | `Restored` reason | Meaning |
|---|---|---|
| `Succeeded` | `RestoreVerified` | The snapshot engine confirmed the restore with positive evidence. |
| `Succeeded` | `RestoreInferred` | The engine has no positive restore signal and saw no sign of a failed restore, and the replacement stayed Ready without a container restart for the stabilization window (controller flag `--restore-stabilization-window`, default `30s`). GKE Pod Snapshots reports this verdict. |
| `SucceededWithoutRestore` | `RestoreLost` | A container of the replacement restarted before the verdict, so it runs without its restored state. A `RestoreLost` Warning event is recorded on the job and on the Pod. |
| `SucceededWithoutRestore` | `FallbackToColdStart`, `RestoreFailed` | The engine reported a cold start, or a failed restore. |
| `SucceededWithoutRestore` | `RestoreIncompatibleNode` | The engine refused the restore because the replacement's node isn't compatible with the checkpoint's source node (CRIU: a different CPU model), so the replacement started cold and keeps running. `RestoreIncompatibleNode` Warning events are recorded on the job and on the Pod. See [Signals for external controllers](#signals-for-external-controllers). |
| `SucceededWithoutRestore` | `RestoreTimeout` | No verdict within 5 minutes plus the stabilization window. With `--restore-placement`, a replacement released with the node constraints that isn't scheduled yet can still be deleted afterwards, once the scheduler reports that no node can take it, for its ReplicaSet or StatefulSet to recreate. Then `PlacementTimeout` Warning events are recorded on the job and the Pod, and the job keeps this reason. |
| `SucceededWithoutRestore` | `NoCompatibleNode` | Only for a job that an earlier version of the controller held, with `--restore-placement`, because no node met the snapshot engine's node constraints when its replacement was due for release: the replacement started cold, without them. A `NoCompatibleNode` Warning event is recorded on the job. |
| `SucceededWithoutRestore` | `PlacementTimeout` | With `--restore-placement`, the replacement released with the snapshot engine's node constraints wasn't scheduled within `--restore-placement-deadline` while a node without them could take it, or by the restore timeout, so it's deleted for its owner to recreate, and the new Pod starts cold. A `PlacementTimeout` Warning event is recorded on the job and on the Pod. If the scheduler placed or nominated the Pod just before the delete, the Pod is kept (and may still restore), and another `PlacementTimeout` Warning event on the job says so. |
| `SucceededWithoutRestore` | `SnapshotDeleting` | The job was `Restoring`, and its replacement wasn't released yet, when its `PodSnapshot` was found being deleted, so nothing can restore from it (#265). The Pod Gate Reconciler holds the replacement rather than order a restore from that snapshot, and records `SnapshotRestorable=False` (reason `SnapshotDeleting`) on the job. The job then concludes, and the gate releases the replacement cold. A `SnapshotDeleting` Warning event is recorded on the job. |
| `SucceededWithoutRestore` | `SnapshotGone` | As `SnapshotDeleting`, for a `PodSnapshot` that was already gone (#269). A `Restoring` job got there on a `Ready` snapshot, so one the API server doesn't have is gone. The hold is recorded as `SnapshotRestorable=False` with reason `SnapshotGone`, and a `SnapshotGone` Warning event is recorded on the job. |

### Signals for External Controllers

Controllers that place workloads, such as `gke-labs/workload-class`, don't know which snapshot engine serves a Pod, but can tell from these reasons and Warning events when a warm restore wasn't possible on the capacity the Pod got, and why (#294). They're stable: a reason isn't renamed, and the fields below keep their names and order (new fields, if any, are added at the end).

Some messages start with machine-readable fields, then `: ` and the human text. Fields are `key=value`, separated by single spaces. A value never contains a space or `: `, and is empty when it isn't known. A message written by an earlier version of the controller has no fields.

| Reason | Where | When | Message |
|---|---|---|---|
| `PlacementTimeout` | `PodMigrationJob` `Restored` (`False`) and `Ready` conditions; Warning events on the job and on the replacement Pod | With `--restore-placement`, the replacement released with the snapshot engine's node constraints wasn't scheduled in time, so it's deleted for its owner to recreate cold (see the verdict table above). The job concludes `SucceededWithoutRestore`. | `engine=<engine> requirements=<key>[,<key>...]: <text>` |
| `PlacementTimeout` (events only) | Warning events on the job and the Pod | A job that concluded `RestoreTimeout` deletes its unschedulable replacement later; the job keeps `RestoreTimeout`. Also on the job when a placement-timed-out Pod was scheduled or nominated just before the delete and is kept. | Same fields. The kept-Pod events repeat the ones the job's condition recorded. |
| `RestoreIncompatibleNode` | `Restored` (`False`) and `Ready` conditions; Warning events on the job and on the Pod | The engine refused the restore on the replacement's node, which isn't compatible with the checkpoint's source node. The Pod started cold and keeps running. Only the CRIU engine reports it today, for a different CPU model. The job concludes `SucceededWithoutRestore`. | `engine=<engine> fact=<fact> node=<node> sourceNode=<node>: <text>` |
| `MigrationSkippedIneligible` | Warning event on the Pod | The engine can't migrate the Pod at all (for example CRIU and a container without an explicit `command`), so it's evicted, or skipped on a preempted node, without a migration (#275). No job is created. | `Snapshot engine <engine> can't migrate the pod: <reason>; ...` (no fields) |
| `FallbackToColdStart`, `RestoreFailed`, `RestoreLost`, `RestoreTimeout`, `NoCompatibleNode`, `SnapshotDeleting`, `SnapshotGone` | `Restored` and `Ready` conditions (and Warning events where the verdict table above says so) | The other ways a job concludes `SucceededWithoutRestore`, as in the verdict table above. | Human text, no fields |
| `RestoreCrashFallback` | `RestoreCrashed`, `Restored` and `Ready` conditions of a `Failed` job; Warning event on the job | A replacement that crashed restoring was deleted to start cold (see [3.2](#32-restore-crash-cold-start-fallback--exit-code-128)). | Human text, no fields |

The fields:

- `engine`: the snapshot engine that serves the job: `gke-pod-snapshots` or `criu`.
- `requirements`: comma-separated node label keys of the constraints that failed, in the engine's order. These are the constraints the Pod Gate Reconciler added to the replacement (recorded in its `pod-migrate.io/restore-placement-added` annotation) that no node meets among the nodes that would take the Pod without them, checked as the placement deadline checks them (Ready, not cordoned, the Pod's node selector, affinity and tolerations; not resources). When that singles out none (no such node exists, or each constraint is met by some node and only their combination isn't), it lists all the added constraints. For GKE Pod Snapshots that's `node.kubernetes.io/instance-type`. For CRIU, it can be `kubernetes.io/arch` and `criu.pod-migrate.io/*` keys (CPU vendor, model, XSAVE state, kernel, CRIU, runc and containerd versions), and `missing-cpu.criu.pod-migrate.io/<flag>` for instruction flags the node lacks; more than 8 flag keys are listed as one `missing-cpu.criu.pod-migrate.io/*` (see [engines/criu/README.md](engines/criu/README.md) for the labels).
- `fact`: the node fact that didn't match. CRIU: `cpu-model` (the `model name` in `/proc/cpuinfo`, which the node agent compares with the archive's).
- `node`: the replacement's node. `sourceNode`: the node the checkpoint was taken on (`status.originNodeName`).

For example:

```text
Warning  PlacementTimeout  engine=gke-pod-snapshots requirements=node.kubernetes.io/instance-type: Replacement pod web-0, released with the restore's node constraints, wasn't scheduled within 3m0s, and a node without them could take it, so it's deleted for its owner to recreate, and the new pod starts cold
Warning  RestoreIncompatibleNode  engine=criu fact=cpu-model node=pool-b-1 sourceNode=pool-a-3: The replacement's node isn't compatible with the checkpoint's source node (cpu-model differs), so it started cold: ...
```

A controller can match the reason, then the fields with `^engine=(\S+) requirements=(\S*): ` or `^engine=(\S+) fact=(\S+) node=(\S*) sourceNode=(\S*): `.

---

## 5. Automated Verification (E2E Suite)

This repository contains production-ready YAML templates for trying out pod migration on your workloads under the `verification-suite/manifests/` directory.

### Running E2E Verification
You can run automated E2E pod migration verification for any of the 21 pre-configured applications using the driver script `verification-suite/run_app_validation.sh`:
```bash
# Run validation on Valkey
./verification-suite/run_app_validation.sh valkey

# Run validation on MySQL
./verification-suite/run_app_validation.sh mysql
```

The CRIU engine has its own E2E on kind, which needs no GKE cluster or cloud credentials and runs in CI on pull requests: see [End-to-end test on kind](engines/criu/README.md#end-to-end-test-on-kind).

---

## 6. Expected Verification Output

### Expected Log Output from `run_app_validation.sh`

#### Valkey Verification
When running `./verification-suite/run_app_validation.sh valkey`, you should expect logs similar to:
```
[*] Deploying Valkey StatefulSet...
statefulset.apps/pm-valkey created
service/pm-valkey-service created
[*] Waiting for Valkey pod to be Ready...
pod/pm-valkey-0 condition met
[*] Seeding state in Valkey: migkey -> valkey-nonce-1718900000
OK
[*] Pod is running on node: gke-pod-migration-cluster-gvisor-pool-abcdef-1234
[*] Draining node gke-pod-migration-cluster-gvisor-pool-abcdef-1234...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 cordoned
evicting pod pm-system/pm-controller-manager-...
evicting pod default/pm-valkey-0
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 drained
[*] Restoring node gke-pod-migration-cluster-gvisor-pool-abcdef-1234 (uncordon)...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 uncordoned
[*] Waiting for restored Valkey pod to be Ready...
pod/pm-valkey-0 condition met
[*] Verifying state...
[+] Retrieved value: valkey-nonce-1718900000
[SUCCESS] Valkey E2E Live Migration Succeeded. State survived!
```

---

## 7. Troubleshooting & Cleanup

### PodMigration Storage Cleanup Finalizer & Clean Undeploy

`PodMigration` resources use the `podmigration.gke.io/storage-cleanup` finalizer to ensure that cluster-scoped `PodSnapshotStorageConfig` (PSSC) and namespaced `PodSnapshotPolicy` (PSP) resources are cleanly deleted when a `PodMigration` CR is deleted. The finalizer also verifies whether any active migrations (`PodMigrationJob` resources in non-terminal phases) are running in the namespace, postponing deletion until in-flight jobs reach a terminal state (`Succeeded`, `SucceededWithoutRestore`, or `Failed`).

When undeploying the controller, `PodMigration` custom resources must be deleted while the controller is still running so it can process the finalizer:
```bash
# make undeploy handles this automatically by deleting PodMigration CRs first:
make -C controller undeploy
```

If the controller is deleted before the custom resources, `PodMigration` objects will wedge in `Terminating` and block CRD deletion. Similarly, if the controller is downgraded or rolled back to a pre-#35 image that does not support the finalizer, any `PodMigration` deleted during that time will wedge in `Terminating`. To manually unwedge:
```bash
kubectl patch podmigration <name> -n <namespace> --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]'
```

### Bypassing GKE Validating Admission Policy for Snapshot Cleanup

GKE enforces a ValidatingAdmissionPolicy (`gke-pod-snapshot-validating-admission-policy`) that prevents manual edits to `podsnapshots.podsnapshot.gke.io` by anyone other than the GKE snapshot controller and agent. This blocks users from manually removing finalizers from stuck `podsnapshots.podsnapshot.gke.io` (e.g. when GCS upload fails or during test resets).

To override this check and perform cleanup (qualifying the `podsnapshot.gke.io` group explicitly so dual-engine clusters do not hit `podsnapshots.criu.podmigration.gke.io`):
1. **Disable the validation actions temporarily by patching the binding to "Audit" instead of "Deny":**
   ```bash
   kubectl patch validatingadmissionpolicybinding gke-pod-snapshot-vap-binding \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Audit"]}]'
   ```
2. **Remove finalizers and delete the stuck snapshots:**
   ```bash
   kubectl get podsnapshots.podsnapshot.gke.io -o json | jq -r '.items[].metadata.name' | xargs -I {} kubectl patch podsnapshots.podsnapshot.gke.io {} --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]' || true
   kubectl delete podsnapshots.podsnapshot.gke.io --all --timeout=15s
   ```
3. **Restore the validation actions back to "Deny, Audit":**
   ```bash
   kubectl patch validatingadmissionpolicybinding gke-pod-snapshot-vap-binding \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Deny", "Audit"]}]'
   ```

### Checkpoint Garbage Collection

A checkpoint holds the application's memory, so it shouldn't outlive the migration (#107, contract clause C9). The controller deletes the `PodSnapshot` that a job's own trigger created, once the job is `Succeeded` (restore verified or inferred) or `Failed`. Deleting the `PodSnapshot` is the signal to the snapshot engine to delete the archive.

- **Retention:** `--snapshot-retention` (default `0`) keeps the snapshot for that long after the job concluded, for debugging. It must be between `0` and `20m`: concluded jobs are garbage collected 30 minutes after they concluded, the job is the only record of which snapshot is its own, and the deletion needs room to retry before that. Deleting a concluded job by hand within its retention leaves its snapshot to the orphan sweep.
- **Only its own snapshot:** the controller deletes only the snapshot in the job's `status.snapshotRef`, in the job's engine API group. The controller takes that name only from its own trigger's status. If the snapshot carries the `gke-pod-snapshot-triggered-by` label, the label must name the job's trigger (`psmt-<pod>-<uid>`). A snapshot from a Kubeflow pause/resume on the shared policy (#60) is never deleted.
- **Never while a restore may need it.** The controller waits while any of these hold:
  - the source fence is still undecided, because that decision reads the checkpoint's state;
  - the checkpoint hasn't finished and its source Pod still exists;
  - a live Pod carries a restore order (`<group>/ps-name`) naming the snapshot. A restore that concludes `Succeeded` (verified or inferred) replaces the replacement's order with the cold-start bypass before `Succeeded` is recorded (#152), so it doesn't hold the snapshot;
  - a Pod assigned to the job is still gated;
  - another job that names the snapshot isn't `Succeeded` or `Failed`;
  - the job's own trigger still exists (the controller retries deleting it). Otherwise the CRIU engine would answer the trigger with a new work order and checkpoint the source again;
  - another live trigger was answered with the snapshot (the CRIU engine names snapshots after the pod).
- **Engine not registered:** a job whose engine isn't registered any more doesn't know its API group, so its snapshot is left alone.
- **Not deleted:** jobs that concluded `SucceededWithoutRestore` keep their snapshot while the job exists, as before: an unconsumed one can still be claimed, and it stays for recovery. A snapshot that a job failed before recording, and a snapshot still held when the job is garbage collected, are left to the orphan sweep.
- **Orphan sweep:** every `--orphan-snapshot-sweep-interval` (default `10m`; `0` turns it off, otherwise at least `1m`), the leader deletes `PodSnapshot`s whose job is gone. That covers a job deleted by hand, a job that failed before recording its snapshot, a snapshot still held when its job was garbage collected (including `SucceededWithoutRestore` jobs), and jobs garbage collected before this feature. It's on by default, like the release: it only deletes our own objects. It only looks at the API groups of the registered engines. It deletes at most 50 snapshots and makes at most 200 live checks per pass; the rest wait for the next pass. The first pass runs one interval after a replica becomes the leader.
  - **Ownership:** the snapshot's `gke-pod-snapshot-triggered-by` label must have the form of our trigger names, `psmt-<pod>-<first 8 hex characters of the pod UID>`. A snapshot without the label is never deleted, since nothing else records that our trigger made it: older CRIU snapshots, and CRIU snapshots of pods with names over 49 characters, stay for the bucket's lifecycle rule. A Kubeflow pause/resume snapshot carries its own trigger's name, so it's never deleted.
  - **Age:** the snapshot must be older than `--orphan-snapshot-min-age` (default `1h`), counted from its creation. When the sweep is on, it must be at least `--snapshot-retention` plus `10m`.
  - **Kept while any of these hold:** a job in any phase has the snapshot's trigger, has the job name that goes with it (`pmj-<pod>-<uid>`), or names the snapshot in `status.snapshotRef`; a trigger with the label's name exists in any engine group, or any trigger was answered with the snapshot; a Pod that isn't `Succeeded` or `Failed` (terminating Pods count) carries a restore order naming it (any `<group>/ps-name`), `pod-migration.gke.io/restore-completed`, or the CRIU engine's `pod-migrate.io/psengine-snapshot`, or is assigned to the job and still gated; or the snapshot records `SourceFrozen=True` and its source Pod still exists, since the CRIU node agent may still need it to thaw the source. A warm-restored replacement keeps `restore-completed` (and, with CRIU, `psengine-snapshot`) for its lifetime, so the sweep keeps its snapshot until the Pod is gone. A frozen source whose snapshot is kept stays frozen until it's evicted or resumed by hand, as before.
  - **Races:** the checks run against the cache, then again live once per namespace, reading jobs last, since a job is created before its trigger. Right before each delete, the snapshot, its job (`pmj-<pod>-<uid>`) and its trigger in every engine group are read again live. The delete has a UID precondition, and a snapshot that's gone or recreated is skipped.
  - Each deletion is logged and recorded as a Normal `OrphanSnapshotDeleted` event on the `PodSnapshot`.
- **Status:** the job records `SnapshotReleased=True` (reason `SnapshotDeleted`) once its snapshot is deleted or gone, with a Normal `SnapshotDeleted` event when the controller deleted it, or `SnapshotReleased=False` (reason `SnapshotNotOwned`, with a Warning event) when the snapshot belongs to another trigger.
- **GKE Pod Snapshots:** the controller only deletes the `PodSnapshot` CR. GKE's own node agent holds it with the `podsnapshot.gke.io/podsnapshot-finalizer` finalizer and deletes the archive in Cloud Storage before removing the finalizer (verified on GKE 1.37, `pod-snapshot-addon` 1.37.2); still keep a GCS Object Lifecycle Management rule on the bucket as a backstop (for example when `--orphan-snapshot-sweep-interval=0` or for unlabelled archives). On GKE 1.36, the add-on's admission policy `gke-pod-snapshot-validating-admission-policy` only covers `UPDATE` of `podsnapshots.podsnapshot.gke.io` (checked with `kubectl get validatingadmissionpolicy gke-pod-snapshot-validating-admission-policy -o yaml`), so the controller's `delete` is allowed.
- **CRIU engine:** the engine puts a finalizer on its `PodSnapshot`s and deletes the archive from the bucket before it lets one go. See [the engine's README](engines/criu/README.md#archive-garbage-collection).

### PodDisruptionBudgets and Single-Replica (`minAvailable=1`) Workloads

When migrating single-replica workloads (such as a `StatefulSet` with `replicas: 1` or a bare Pod) configured with a strict `PodDisruptionBudget` (`minAvailable: 1` or `maxUnavailable: 0`), Kubernetes PDB admission will prevent eviction of the origin pod because disruptions are not allowed while the single replica is running:

- **Budget Check Before the Checkpoint (#42):** In `Pending`, before anything is checkpointed, the controller sends a dry-run eviction of the origin pod.
  - **Budget exhausted** (a `429` that carries the budget's cause): the job stays `Pending` with condition `DisruptionBudgetChecked=False` (reason `BudgetExhausted`, with a message that names the budget) and emits a `MigrationDeferredByPDB` event on the pod. It checks again after 5 s, backing off to every 30 s (with jitter) the longer it waits, and the eviction webhook keeps answering the drain with `429`. While it waits, the job checks the origin pod on every pass, and fails if the pod is deleted, replaced, or finishes.
  - **Budget allows the eviction:** the job moves on to `Snapshotting`, and its migration timeout starts over there, so the wait doesn't eat into the migration.
  - **Timeout while the budget still refuses:** the job fails with reason `DisruptionBudgetTimeout` (outcome `budget_timeout`) without having checkpointed anything, and the next eviction of the pod starts a fresh job (event `MigrationRestartedAfterBudgetWait`) instead of a cold eviction. So the wait lasts as long as the evictor keeps retrying. With `kubectl drain`'s default of no `--timeout` and a budget that never allows a disruption, it lasts forever, one migration timeout per job: give the drain a `--timeout`, or relax the budget.
  - **Check error** (`DisruptionBudgetChecked=Unknown`, reason `CheckError`, for example a throttled request): the job waits the same way, but its timeout falls back to a cold eviction, like any other failed job.
  - Spot preemption skips the check, because budgets don't cover involuntary disruptions. The result of each check is counted in `pod_migration_budget_checks_total{result}`.
- **Several Replicas Under One Budget (#42):** A dry run doesn't reserve the budget, so when the dry run passes, the controller also counts the other migrations in flight under each PDB that covers the pod. A migration counts from when it passes this check until its source is gone. The job goes ahead only if the PDB still keeps its `desiredHealthy` pods once those sources and this one are counted as disrupted. Otherwise it waits with reason `MigrationsInFlight`, and times out like a job the budget refused. For a source that isn't Ready, the count follows the eviction API: under `unhealthyPodEvictionPolicy: AlwaysAllow` it's skipped, and under `IfHealthyBudget` the other pods must cover `desiredHealthy`. Waiting jobs are let through as each one checks again.
  - **Fewer dry runs:** a Ready pod covered by one PDB whose status in the controller's cache allows no disruption waits without sending a dry run, until the cache shows the budget freed.
  - **Faster wake-up:** a waiting job checks again right away when a PDB in its namespace gains healthy pods or allowed disruptions, or is deleted, instead of waiting out its backed-off recheck.
- **PDB-Safe Eviction Fallback:** The controller respects Kubernetes PDB constraints by issuing eviction requests through the official Kubernetes eviction subresource (`policy/v1 Eviction`) and retrying on `429 Too Many Requests` / `409 Conflict`. This covers the window between the budget check and the eviction.
- **Timeout & Churn Protection:** If the PDB budget does not recover within the 10-minute active migration timeout, the PMJ concludes with status `SucceededWithoutRestore` (Reason: `PDBEvictionTimeout`), and the controller marks the pod with `pod-migration.gke.io/pdb-eviction-timeout: "true"` to prevent repeated snapshot loops during ongoing drain retries.
- **Operator Intervention & Re-arming Migration:** Draining nodes hosting `minAvailable=1` single-replica workloads requires operator intervention (e.g. temporarily updating or relaxing the PDB budget, scaling up the workload, or deleting the pod). Once the PDB is relaxed, to re-arm live migration on the existing pod instance without recreating it, remove the timeout annotation:
  ```bash
  kubectl annotate pod <pod-name> pod-migration.gke.io/pdb-eviction-timeout-
  ```

### Restarts After a Migration

The pod gate sets `podsnapshot.gke.io/ps-name` on the replacement to the migration snapshot, so that GKE Pod Snapshots restores the replacement from it. The agent reads that annotation whenever it creates the pod's sandbox, so a later sandbox restart (a sandbox crash, a containerd restart) would restore the migration snapshot again and roll the app back to its state at migration time (#152). So once the restore is verified, the controller sets the annotation to `""`, the cold-start bypass: later restarts start cold, like any other pod's, and GKE Pod Snapshots emits a `falling back to a cold start` Warning event for each of them.

### Safe RWO PersistentVolume Detachment

Stateful workloads with `ReadWriteOnce` (`RWO`) or `ReadWriteOncePod` (`RWOP`) PersistentVolumes require detachment sequencing to avoid Kubernetes volume attachment conflicts across nodes (#249):

- **Evicting Phase Volume Detachment Wait:** During the `Evicting` phase, after the source Pod is deleted, the controller monitors `VolumeAttachment` resources for all RWO/RWOP volumes (`job.status.pvsToDetach`) and verifies detachment from the origin node before transitioning the job to `Restoring`. Non-RWO volumes (such as `ReadWriteMany` or `ReadOnlyMany`) do not block migration.
- **Volume Detach Timeout & Snapshot Preservation:** If volume detachment from the origin node exceeds the migration timeout, the PMJ concludes as `SucceededWithoutRestore` with reason `VolumeDetachTimeout`. This preserves the durable snapshot reference for inspection and manual recovery while releasing the replacement Pod for a cold start.

### Changing a Restore Order by Hand

On a Pod that is opted into migration, only the controller may set, change or remove a restore order (`<group>/ps-name`). Anyone else, a cluster admin included, gets:

```
ValidatingAdmissionPolicy 'pod-migration-restore-order' with binding 'pod-migration-restore-order' denied request: only the Pod Migration controller may set, change or remove a restore order (<group>/ps-name) on an existing pod that is opted into migration
```

Other updates of the Pod aren't affected, and a Pod without the `pod-migration.gke.io/enabled: "true"` label isn't either. If you must edit a restore order by hand, for example in a test reset, switch the policy's binding from denying to only auditing, and back. Don't delete the binding and don't apply `controller/deploy.yaml` again for this: applying the whole manifest can also roll the controller onto another image and restart it.

> [!WARNING]
> While the binding is set to `Audit`, no opted-in Pod in the cluster is protected: anyone who may update Pods can set a restore order. Keep the window short.

1. **Set the binding to audit only.** The API server stops denying within a few seconds:
   ```bash
   kubectl patch validatingadmissionpolicybinding pod-migration-restore-order \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Audit"]}]'
   ```
2. **Edit the annotation.**
3. **Set the binding back to deny**, and check it:
   ```bash
   kubectl patch validatingadmissionpolicybinding pod-migration-restore-order \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Deny"]}]'
   kubectl get validatingadmissionpolicybinding pod-migration-restore-order -o jsonpath='{.spec.validationActions}'
   ```

The CRIU engine protects its restore outcome (`pod-migrate.io/psengine-restore`, and a failure's reason in `pod-migrate.io/psengine-restore-reason`) the same way, with the `pod-migrate-psengine-restore-status` policy and binding in `engines/criu/deploy/deploy.yaml`. The same steps apply to that binding.

**Custom namespace or service account:** each policy names the one identity it allows, as a literal: `system:serviceaccount:pod-migration-system:pod-migration-controller` in `controller/deploy.yaml`, and `system:serviceaccount:pod-migration-criu-system:pod-migrate-agent` in `engines/criu/deploy/deploy.yaml`. If you install into another namespace or rename a service account, change that line in the policy too. Otherwise the controller is denied whenever it orders a restore, and the node agent whenever it records an outcome. The CRIU engine's bucket names its namespace and service accounts too: it grants `principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/<NAMESPACE>/sa/<SERVICE_ACCOUNT>` for the node agent's and the engine controller's service accounts (`pod-migration-criu-system`, `pod-migrate-agent` and `pod-migrate-psengine-controller` by default). Grant the ones your install uses: `patches/setup-criu-storage.sh` takes them in `CRIU_NAMESPACE`, `CRIU_AGENT_SA` and `CRIU_CONTROLLER_SA`.

## 8. Monitoring & Prometheus Metrics

The controller registers Prometheus metrics on the standard controller-runtime metrics endpoint (`:8080/metrics` by default):

| Metric | Type | Labels | Description |
|---|---|---|---|
| `pod_migration_active` | Gauge | None | Number of currently active pod migrations in flight. |
| `pod_migration_outcomes_total` | Counter | `outcome` (`succeeded`, `failed`, `timeout`, `budget_timeout`, `fallback`, `succeeded_without_restore`, `engine_not_registered`, `restore_lost`, `deleted`) | Total completed migrations by outcome. `timeout` counts jobs that timed out, for example with reason `RestoreTimeout`. `NoCompatibleNode` and `PlacementTimeout` count as `succeeded_without_restore`: they're about placement, not a restore that ran out of time. `RestoreIncompatibleNode` counts as `fallback`, like `FallbackToColdStart` (#294). |
| `pod_migration_restore_verdicts_total` | Counter | `engine`, `verdict` (`confirmed`, `inferred`, `cold_start`, `failed`, `lost`, `timeout`, `no_compatible_node`, `placement_timeout`, `snapshot_deleting`, `snapshot_gone`) | Restore verdicts on replacement Pods, so a restore the engine confirmed can be told apart from one it inferred. A `RestoreIncompatibleNode` conclusion is a `cold_start` verdict. |
| `pod_migration_phase_duration_seconds` | Histogram | `phase` (`pending`, `snapshotting`, `evicting`, `restoring`) | Latency distribution of individual migration phases. |
| `pod_migration_restore_crash_fallback_total` | Counter | None | Total cold-start fallbacks triggered by matched gVisor/OCI restore crashes. |
| `pod_migration_restore_crash_unmatched_total` | Counter | None | Total unrecognised StartError restore-pod crashes with no known signature. |
| `pod_migration_source_fence_total` | Counter | `action` (`fenced`, `unfenced`, `kept`, `refenced`) | Fence actions on migration sources (see [Source fence](#3-source-fence)). |
| `pod_migration_spot_preemption_triggered_total` | Counter | None | Migrations started for Pods on a node that's being preempted. |
| `pod_migration_spot_preemption_skipped_total` | Counter | `reason` (`ineligible`, `unfenced_engine`, `no_policy`, `unknown_memory`, `no_replica_key`, `ambiguous_replica_key`, `budget_exceeded`) | Pods on a node that's being preempted that were skipped for one of these reasons. Each skip also records a Warning event on the Pod, for example `MigrationSkippedIneligible` when the snapshot engine can't migrate the Pod. |
| `pod_migration_source_fenced_seconds` | Histogram | `outcome` (`migrated`, `unfenced`) | Time a source spent fenced: until the migration moved on to restoring, or until a failed migration returned it to service. |
| `pod_migration_application_downtime_seconds` | Histogram | `engine`, `outcome` (`RestoreVerified`, `RestoreInferred`, `ColdStart`) | Application pause duration (traffic stopped on source until replacement became Ready) in seconds (1s resolution, from whole-second RFC3339 timestamps), measuring actual service downtime. `ColdStart` samples are recorded once the replacement pod becomes `Ready` (which can be after job conclusion); cold replacements are observed for up to 5 minutes after job conclusion (checked when the controller reconciles), after which backfill is skipped. |

### High Availability (HA) Scraping Considerations

When running in multi-replica HA mode with leader election enabled:
- The `/metrics` endpoint is served by all controller replicas.
- Only the **leader replica** reconciles migrations and updates `pod_migration_active` in memory; standby replicas serve `pod_migration_active == 0`.
- Prometheus scrape jobs should scrape per-pod endpoints and use `max(pod_migration_active)` when querying or alerting on active in-flight migrations across the cluster.
- For counter and histogram metrics, use `sum(rate(pod_migration_outcomes_total[5m])) by (outcome)` and `sum(rate(pod_migration_phase_duration_seconds_sum[5m])) by (phase) / sum(rate(pod_migration_phase_duration_seconds_count[5m])) by (phase)`.

## 9. Releases & Licensing

- **Release Process**: Step-by-step instructions for building, validating, and publishing official releases are documented in [docs/release-process.md](docs/release-process.md).
- **Third-Party Licenses & GPL Source Availability**: The complete software inventory of published container images, license disclosures, and instructions for obtaining matching GPL/LGPL source code are documented in [docs/third-party-licenses.md](docs/third-party-licenses.md).


