# CRIU Snapshot Engine (`engines/criu`)

> [!WARNING]
> **EXPERIMENTAL — FOR NON-PRODUCTION TEST CLUSTERS ONLY ([#129](https://github.com/gke-labs/pod-migration/issues/129), [#244](https://github.com/gke-labs/pod-migration/issues/244)).**
> `engines/criu` runs a privileged node DaemonSet (`hostPID`, `hostNetwork`,
> host containerd/kubelet mounts). It serves its own dedicated
> `criu.podmigration.gke.io` CRD group so it can coexist alongside the managed
> GKE Pod Snapshots addon (`podsnapshot.gke.io`) in test clusters, and its
> DaemonSet targets Linux nodes (`kubernetes.io/os=linux`) while excluding
> `gVisor` sandbox nodes (`sandbox.gke.io/runtime=gvisor`) and read-only COS
> nodes (`cloud.google.com/gke-os-distribution=cos`, pending [#109](https://github.com/gke-labs/pod-migration/issues/109)).
> Never apply `deploy/crds.yaml` or `deploy/deploy.yaml` to a production
> cluster.
>
> **What remains unverified until [#129](https://github.com/gke-labs/pod-migration/issues/129) lands:**
>
> - **Archive provenance (`R2`) & dedicated GCS credentials (`R5`):** Checkpoint
>   archives in GCS (`manifest.json`, `spec.json`, `checkpoint.tar`) are SHA-256
>   and MD5 integrity-checked against accidental corruption, and restore binds
>   the OCI runtime security posture (`Capabilities`, `User`, `Seccomp`,
>   `MaskedPaths`, `ReadonlyPaths`, `Devices`, `Mounts`, and canonical sandbox
>   `Namespaces`) to the kubelet-created destination container spec. However,
>   CRIU also restores process credentials and seccomp state from the checkpoint
>   image files themselves, archives are **not yet cryptographically signed**,
>   and the `hostNetwork` node agent uses the node's default GCE service account
>   (Workload Identity does not apply to `hostNetwork` pods). Consequently,
>   **anyone who can write to the checkpoint GCS bucket (including workloads on
>   nodes sharing the default GCE service account) can get root on a restoring
>   node** until `R2` (archive provenance) and `R5` (dedicated GCS credentials)
>   land.
> - **Agent RBAC Breadth (`pod-migrate-psengine-agent`):** The node DaemonSet holds
>   cluster-wide `pods` (`get`, `list`, `watch`, `update`, `patch`), `nodes` (`get`,
>   `patch`), and `nodes/checkpoint` (`create`) permissions to annotate restored pods
>   with `pod-migrate.io/psengine-restore` and trigger Kubelet container checkpoints.
> - **Residual Dual-Start Window:** If the holder container downward-API poll times out
>   or falls through to cold-start execution before sibling restore initiates, a residual
>   dual-start race window exists where both cold-started app processes and restored CRIU
>   processes could execute concurrently.
> - **Known follow-ups tracked in [#129](https://github.com/gke-labs/pod-migration/issues/129):**
>   - `cmd/holder`: downward-API volume propagation lag after gate removal can
>     exceed the initial 2s `ps-name` poll window and trigger a cold-start
>     fallback before the annotation projects into `/pminfo/annotations`.
>   - `internal/psengine/restore_node.go`: `fetch()` enforces a 4 MiB download cap on
>     metadata (`manifest.json`, `spec.json`) and an 8 GiB cap on `checkpoint.tar`.
>   - `internal/node/capability.go`: `Bootstrap` installs `/usr/local/sbin/criu`
>     and `/etc/criu/runc.conf` on the host node; removing the DaemonSet does
>     not automatically uninstall those host files.

`engines/criu` is a standalone snapshot engine for the `gke-labs/pod-migration`
controller that implements the snapshot engine CustomResource contract
(`criu.podmigration.gke.io/v1` by default; see [API group](#api-group)) for
single-container `runc` pods using [CRIU](https://criu.org/).

## Architecture

The CRIU snapshot engine consists of three binaries built from this Go module
(`github.com/gke-labs/pod-migration/engines/criu`):

1. **`cmd/engine` (cluster controller + mutating webhook):**
   - Runs a startup preflight (`psengine.Preflight`) before it serves
     anything. It refuses to start if the contract CRDs of its API group are
     missing or owned by another provider (for example, if `--api-group` is
     overridden to `podsnapshot.gke.io` on an addon-enabled cluster).
   - Reconciles `PodSnapshotPolicy` objects labeled
     `pod-migrate.io/engine: criu`, probing the referenced
     `PodSnapshotStorageConfig` GCS bucket and publishing `Ready=True` +
     `ScopeSingleContainer=True`.
   - Reconciles `PodSnapshotManualTrigger` objects for pods matching a `Ready`
     CRIU policy, materializing a `PodSnapshot` work order and setting
     `status.snapshotCreated.name`. It decides eligibility once, when it
     creates the work order: an ineligible pod's order gets no bucket and is
     marked `Refused` (or `Failed` if storage can't be resolved). An order
     with a bucket belongs to the node agent: the controller's later passes
     don't re-check eligibility, and write only the non-terminal `Pending`
     (or, once the trigger is deleted, `CancelRequested`) onto it (#218). A
     trigger re-created for the same pod instance takes over an unfinished
     work order without a new eligibility check.
   - Serves `/psengine-restore-inject` to speculatively inject the `holder`
     initContainer and command wrapper into CRIU-routed gated replacement pods
     at `CREATE` (checking single-container `runc`/default pod shape in memory
     first so `gVisor`, sidecar, `hostNetwork`, and ungated pods short-circuit
     with zero API reads, then reading the assigned `PodMigrationJob` to verify
     `pod-migration.gke.io/engine: criu` so other engines' pods are never touched).
2. **`cmd/agent` (privileged node DaemonSet):**
   - At startup (`node.Bootstrap`), installs a self-contained CRIU >= 3.19
     bundle into `/var/lib/pod-migrate/criu-bundle` and `/usr/local/sbin/criu`,
     writes `/etc/criu/runc.conf`, runs `criu check` in the host namespaces via
     `nsenter`, verifies containerd readiness (inspecting containerd's process
     PATH or shim PATH and ensuring `enable_criu != false` in containerd configuration
     before declaring the node ready), and stamps `pod-migrate.io/criu-ready` and
     `pod-migrate.io/criu-version` on the `Node`.
     If containerd cannot resolve CRIU on its PATH/shim PATH, or CRIU is disabled
     by configuration, or containerd cached a failed CRIU lookup, the agent marks
     the node `pod-migrate.io/criu-ready: false` with reason and remediation
     annotations (`pod-migrate.io/criu-reason` and `pod-migrate.io/criu-remediation`).
     This prevents orchestrators from dispatching checkpoints to nodes where containerd
     would poison its CRIU lookup cache (#130).
     Containerd inspection reads containerd's primary configuration file (from
     `--config` / `-c` or default `/etc/containerd/config.toml`) and does not follow
     multi-file `imports` / `conf.d` drop-ins.
     The agent mounts `/run/containerd` and probes containerd's socket every 10s
     while the node is unready; once containerd restarts, the agent detects the
     new socket inode, re-runs readiness verification and kernel checks, and restores
     `pod-migrate.io/criu-ready: true`.
   - Reconciles `PodSnapshot` work orders scheduled on its node
     (`psengine.NodeSnapshotReconciler`): implements the `Frozen` fencing mode
     (contract clause C4, #148). Pauses (freezes) the source container task via
     containerd before the checkpoint and records `SourceFrozen=True` on the
     `PodSnapshot`. Drives the kubelet `ContainerCheckpoint` API, captures the
     source container's OCI spec, uploads `spec.json`, `checkpoint.tar`, and
     `manifest.json` to GCS with MD5/size verification, and marks the
     `PodSnapshot` `Ready=True` while keeping the source container frozen for
     cold eviction by the controller (eliminating lost acknowledged writes and
     empty-state restarts). Note: the controller does not read `SourceFrozen` yet
     (pending under #148); currently the Stopped rule covers it. While frozen
     through dump, upload, and eviction, liveness probes can fail and trigger container
     restarts, and eviction waits out the termination grace period since `SIGTERM`
     is not delivered to a paused task. If a kubelet checkpoint times out,
     `CheckpointTimedOut` is recorded and the source is not thawed—it stays frozen until
     eviction (C9). On any failure or cancel path, the agent thaws
     (unfreezes) the source container task via containerd using a fresh bounded
     context before reporting terminal `Failed` or `Cancelled`. If thawing
     succeeds, it clears `SourceFrozen` so the workload continues serving. If thawing
     fails, `SourceFrozen=True` remains recorded and the agent retries thawing
     without re-running the checkpoint, ensuring the controller keeps the fence until
     the source is verifiably thawed or cold-evicted. An agent restart mid-checkpoint
     now thaws the source and settles that migration as `Failed` rather than re-running it.
     On startup, the agent sweeps `PodSnapshot`s on its node with `SourceFrozen=True`
     and reconciles any that are not `Ready` or `CheckpointTimedOut` (resuming tasks left paused
     if an agent crashed or restarted, while guarding against racing in-flight work orders).
     If the `PodSnapshotManualTrigger` is deleted first, the engine controller
     sets `Checkpoint=Unknown` with the reason `CancelRequested` (on a work
     order without a bucket, which no agent acts on, it records `Cancelled`
     itself), and the agent
     also reads the trigger itself before the checkpoint and right before
     committing. A cancelled agent starts nothing new. It waits for an in-flight
     kubelet checkpoint to return, because cancelling the call doesn't end the
     freeze. It aborts uploads and thaws the source container. It then
     marks `Checkpoint` and `Ready` `False` with the reason `Cancelled`
     (contract clause C9, #106). The agent needs `get` on
     `podsnapshotmanualtriggers` for this. If the dump outlasts the
     orchestrator's 30 s settle wait, the orchestrator keeps the source fenced
     and cold-evicts it. The agent waits up to 150 s for the kubelet
     checkpoint call, past the kubelet's default 2 min CRI deadline. A call
     that still times out is recorded as `Checkpoint=Unknown` with the reason
     `CheckpointTimedOut`, never `Failed` or `Cancelled`, because the dump may
     still be running, and past the CRI deadline containerd can leave the
     source frozen until it is resumed by hand. The snapshot then never
     settles, so the orchestrator keeps the source fenced and cold-evicts it
     instead of unfencing it. The agent never overwrites
     an outcome already recorded on the `PodSnapshot`. The engine controller
     labels each `PodSnapshot` `gke-pod-snapshot-triggered-by: <trigger>` (when
     the trigger name fits in a label value), so the orchestrator finds
     unfinished snapshots after the trigger is deleted.
   - Records the source node's facts with each checkpoint, so that a later
     change can place restores only on compatible nodes ([#257](https://github.com/gke-labs/pod-migration/issues/257)).
     The agent gathers them just before it freezes the source and writes
     them twice: as `sourceNode` in `manifest.json`, next to the archive, and
     as `status.sourceNode` on the `PodSnapshot`, in the same status write
     that marks it `Ready=True`. The orchestrator can read the `PodSnapshot`
     without access to the bucket. The fields are `cpuModel` (the
     `model name` line of `/proc/cpuinfo`, absent on arm64, which has no such
     line), `cpuFeatures` (the `flags` (x86) or `Features` (arm64) lines, the
     union across processors, sorted, raw: on x86 they include kernel and
     mitigation flags), `kernelVersion` (`/proc/sys/kernel/osrelease`),
     `criuVersion` (the CRIU bundle's `VERSION` file), `runcVersion`
     (`runc --version` in the host mount namespace, on the `PATH` the agent's
     restores use, which may not be the runc containerd is configured with)
     and `containerdVersion` (containerd's version API, without the leading
     `v`). Every probe is best effort and bounded: a fact the agent can't read
     is left out, and never fails the checkpoint. A failed checkpoint records
     nothing on the `PodSnapshot`; one that fails at the commit point has
     already uploaded its manifest, facts included. Nothing reads these facts
     yet: the restore node still checks only `cpuFamily`.
   - Reconciles replacement pods on its node carrying the restore order,
     `<group>/ps-name` (`psengine.NodeRestoreReconciler`): downloads
     and SHA-256 verifies the archive from GCS, checks CPU family compatibility,
     prepares an overlayfs rootfs with `rootfs-diff.tar`, rewrites the OCI spec
     (`node.AdaptSpec`), restores the process tree as a sibling `runc` container
     joining the holder's network/IPC/UTS namespaces, and stamps
     `pod-migrate.io/psengine-restore: restored` (or `failed`).
3. **`cmd/holder` (static scratch binary):**
   - Seeded into replacement pods via an `initContainer` (`/holder seed`).
   - Wraps the replacement container's entrypoint (`/holder wrap`): reads
     the restore order and `pod-migrate.io/psengine-restore` from a
     downward-API volume, holding the pod sandbox alive when `restored` or
     falling through to `exec` the original command on restore failure/timeout.

## API group

The engine serves its four contract CRs (`PodSnapshotStorageConfig`,
`PodSnapshotPolicy`, `PodSnapshotManualTrigger`, `PodSnapshot`) in one API
group, set with `--api-group` on both `cmd/engine` and `cmd/agent`; the two
must match. The restore order follows the group: the agent restores pods
annotated with `<group>/ps-name`, and the restore webhook passes that key to
`holder wrap` as `--ps-name-annotation` whenever it isn't the holder's
built-in default (`podsnapshot.gke.io/ps-name`). Because the default group is
now `criu.podmigration.gke.io`, `--ps-name-annotation=criu.podmigration.gke.io/ps-name`
is passed by default — use a `holder` image from [#221](https://github.com/gke-labs/pod-migration/pull/221) or later (older holder
images reject `--ps-name-annotation`).

The default group is `criu.podmigration.gke.io` ([#135](https://github.com/gke-labs/pod-migration/issues/135)), separate from the
managed GKE Pod Snapshots addon's `podsnapshot.gke.io` group so both engines run
side by side in the same cluster. When `--runtime-class-engines` routes a
runtime class to `criu` (for example `--migratable-runtime-classes=gvisor,@default`
and `--runtime-class-engines=gvisor=gke-pod-snapshots,@default=criu`) or
`--criu-api-group` is set on `pod-migration-controller`, the controller
registers the `criu` engine (defaulting `--criu-api-group` to
`criu.podmigration.gke.io`) and reconciles CRIU storage configs, policies,
triggers, snapshots, and `criu.podmigration.gke.io/ps-name` restore orders in
that group while keeping `gVisor` workloads on `podsnapshot.gke.io`.
`RestoreInjector` checks literal `""` (`@default`) or `"runc"`
`runtimeClassName` (plus single-container / non-sidecar / non-`hostNetwork`
pod shape) in memory before performing a single `get` on the assigned
`PodMigrationJob` to confirm `pod-migration.gke.io/engine: criu`, and
`deploy/deploy.yaml` restricts `pod-migrate-agent` to Linux nodes while
excluding `gVisor` sandbox nodes and read-only COS nodes (pending [#109](https://github.com/gke-labs/pod-migration/issues/109)).

#### Upgrading an existing CRIU install from `podsnapshot.gke.io`

Read this before upgrading: one wrong order makes every migration cold-start
silently.

- **Silent cold starts.** Before [#135](https://github.com/gke-labs/pod-migration/issues/135),
  a CRIU install ran the controller with stock flags, so its jobs are annotated
  with the `gke-pod-snapshots` engine. The restore webhook now wraps a
  replacement pod only when its job's engine is `criu`. If you keep
  `--api-group=podsnapshot.gke.io` without adding a `criu` route to the
  controller, preflight passes and checkpoints are still taken, but no
  replacement pod is wrapped and every migration cold-starts.
- **Crashlooping replacements.** A `holder` image older than
  [#221](https://github.com/gke-labs/pod-migration/pull/221) rejects
  `--ps-name-annotation`, which every wrapped pod now gets. Rolling the engine
  without its matching holder crashloops replacement pods instead of
  cold-starting them.

Upgrade in this order:

1. Let in-flight migrations finish (no `PodMigrationJob` in `Snapshotting`,
   `Evicting` or `Restoring`).
2. Apply the new `deploy/crds.yaml` (`criu.podmigration.gke.io`).
3. Pause node drains and evictions of CRIU workloads until step 4 finishes.
   From step 3 on, the controller sends new CRIU work to the new group while
   the old engine still serves the old one, so a migration that starts in
   between cold-starts.
4. Add a CRIU route to `pod-migration-controller`, for example
   `--runtime-class-engines=@default=criu` (or `<class>=criu` for each class
   CRIU should handle), and keep `--criu-api-group` at its default. Then roll
   `cmd/engine`, `cmd/agent` and the `holder` image together from the same
   release, using the new `deploy/deploy.yaml`. Resume evictions afterwards.
5. Once no migrations are in flight, delete the old CRIU CRDs in
   `podsnapshot.gke.io` (this also removes their objects), then restart
   `pod-migration-controller`. Deleting only the objects isn't enough: the
   controller recreates policy and storage objects in every registered engine's
   group, and once the CRDs are gone its watches on them error until it
   restarts. Only do this on clusters where the GKE Pod Snapshots add-on is
   off: with the add-on on, those CRDs belong to it.

If replacement pods still cold-start after the upgrade, check the engine's
logs: the restore webhook logs each pod it skips because its job isn't routed
to `criu`. It logs at debug level, which is on by default; if you lowered it,
pass `--zap-log-level=debug` to `cmd/engine`.

To stay on `podsnapshot.gke.io` instead (add-on-off clusters only), keep that
group in `deploy/crds.yaml` and `deploy/deploy.yaml`, pass
`--api-group=podsnapshot.gke.io` to `cmd/engine` and `cmd/agent` and
`--criu-api-group=podsnapshot.gke.io` to the controller, and still add the
controller's `criu` route and roll the holder as in step 4.

To run on another custom API group, change the group in
`deploy/crds.yaml` (each CRD's name and `spec.group`) and in the
`deploy/deploy.yaml` RBAC rules of both the engine and the agent (and in
`pod-migration-controller-role` in `controller/deploy.yaml`, which pre-grants
both `criu.podmigration.gke.io` and `podsnapshot.gke.io`), and pass the same
group to `cmd/engine` (`--api-group`), `cmd/agent` (`--api-group`), and
`pod-migration-controller` (`--criu-api-group`). Change the group only when no
migrations are in flight, and roll the controller, the engine, and the agent
together: each wrapped replacement pod has its restore key built into its
command. With `--enable-webhooks=false`, replacement pods aren't wrapped, so
they start cold instead of being restored; also delete the
`pod-migrate-psengine-restore-inject` MutatingWebhookConfiguration then.

## Node & Cluster Requirements

- **Non-production test clusters only (v1):** Coexists with the managed GKE Pod
  Snapshots addon when using the default `criu.podmigration.gke.io` group
  (startup preflight still refuses if `--api-group=podsnapshot.gke.io` is used
  on an addon-enabled cluster, or if CRDs are missing/foreign-owned). Never
  deploy to production clusters until [#244](https://github.com/gke-labs/pod-migration/issues/244) lands.
- **Linux kernel & runtime:** Nodes running `containerd` 2.x + `runc` with
  kubelet `ContainerCheckpoint` enabled and a kernel passing `criu check`.
- **Minimum containerd version (Security Requirement):** containerd `>= 2.2.7`,
  `>= 2.3.4`, or `2.0.x` is strictly required. The agent verifies this at
  bootstrap via `CheckContainerdVersion`. Nodes with containerd in the vulnerable
  range (`>= 2.1.0, < 2.2.7` or `>= 2.3.0, < 2.3.4`) are demoted with
  `pod-migrate.io/criu-ready: false` to protect against GHSA-p7v4-vr35-mj6f
  (CVE-2026-95837, unauthenticated restore-on-create privilege escalation).
- **Workload scope (v1):**
  - Single-container `runc` pods (`len(pod.Spec.Containers) == 1`,
    `runtimeClassName` unset/`""` or literal `"runc"`, and no native sidecar
    `initContainers` with `restartPolicy: Always`).
  - Explicit `command` in the pod spec (required so the `holder` wrapper can
    fall through to a cold start if restore fails).
  - Non-host-network pods (`hostNetwork: false`); pods declaring
    `hostNetwork: true` are refused across `PSMTReconciler`, `RestoreInjector`,
    and `NodeRestoreReconciler`.
  - Homogeneous CPU family between source and target nodes.
  - Single migration per pod instance: migrating an already-restored CRIU pod
    or holder-injected replacement is refused so the orchestrator falls back
    to a clean cold start without checkpointing the holder wrapper ([#121](https://github.com/gke-labs/pod-migration/issues/121)).

## What Installing the Engine Changes on a Node

When `pod-migrate-agent` starts on a node, it applies the following modifications:

| Modification            | Host Location                       | Description                                                                                                                                                      |
| ----------------------- | ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CRIU Bundle**         | `/var/lib/pod-migrate/criu-bundle/` | Directory containing self-contained `criu.bin`, bundled shared libraries, dynamic linker, and `VERSION` file.                                                    |
| **CRIU System Wrapper** | `/usr/local/sbin/criu`              | Executable wrapper script that launches CRIU via the bundled dynamic linker.                                                                                     |
| **Runc Configuration**  | `/etc/criu/runc.conf`               | CRIU global configuration specifying `tcp-close`, `skip-in-flight`, `file-validation buildid`, and `file-locks`.                                                 |
| **Node Annotations**    | Kubernetes `Node` metadata          | Annotated with `pod-migrate.io/criu-ready` (`true` or `false`) and `pod-migrate.io/criu-version`. Stale reason/remediation annotations are cleared on promotion. |
| **Socket Watching**     | `/run/containerd/containerd.sock`   | Monitored for inode changes to detect containerd daemon restarts and re-probe readiness.                                                                         |

## Storage Security & Bucket IAM Guidance

Because CRIU checkpoint archives contain complete, unencrypted process address spaces (including in-memory tokens, TLS keys, and credentials), storage backends must be secured with the same rigor as Kubernetes Secrets:

1. **Workload Identity hostNetwork Limitation & Node SA Exposure:** The node agent runs with `hostNetwork: true` to communicate with the local kubelet and manage network namespaces. On GKE, **Workload Identity does not apply to `hostNetwork: true` pods** (they bypass metadata-server token redirection and inherit the underlying node's GCE service account). Consequently, isolating bucket access from cluster workloads is not achievable today if workloads share nodes with the default node service account. Until R5 ([#129](https://github.com/gke-labs/pod-migration/issues/129)) lands (dedicated secret-mounted or sidecar credentials), run CRIU node pools under a dedicated GSA, isolated from untrusted workloads.
2. **Least Privilege Bucket IAM:**
   - Source node agents: `roles/storage.objectCreator`, `roles/storage.objectViewer`
   - Target node agents: `roles/storage.objectViewer`
   - Engine controller (`cmd/engine`): `storage.objects.create`, `storage.objects.get` and `storage.objects.delete`, for its storage probe and archive garbage collection (#107). Without delete, archives are left behind, reported only by events.
   - Avoid Default Compute Engine SA: Do **not** use the default Compute Engine service account (`<project-number>-compute@developer.gserviceaccount.com`).
3. **CMEK Encryption:** Configure Cloud KMS Customer-Managed Encryption Keys (CMEK) on the checkpoint bucket.
4. **Lifecycle Retention:** Apply GCS Object Lifecycle Management rules to automatically delete checkpoint archives after **1 to 7 days**, preventing cold archives from lingering. The engine deletes a job's archive when its `PodSnapshot` is deleted (see Archive garbage collection below); keep the rule as a backstop for archives it leaves behind.

For full threat analysis, blast radius documentation, and restore path verification details, see [Threat Model and Security Guidance for the CRIU Engine](../../docs/threat-model-criu.md) and [SECURITY.md](../../SECURITY.md).

## Archive garbage collection

The orchestrator deletes a job's `PodSnapshot` once it's done with the
checkpoint: once the job is `Succeeded` (restore verified or inferred) or
`Failed`, after the controller's `--snapshot-retention` (#107, contract clause C9). The engine owns the
archive, so `cmd/engine` deletes it:

- Every CRIU `PodSnapshot` carries the `pod-migrate.io/archive-cleanup`
  finalizer. The engine controller adds it when it creates the work order,
  and adds it to `PodSnapshot`s created before the upgrade, so their archives
  are deleted with them too.
- When a `PodSnapshot` is deleted, the engine deletes `manifest.json` (first,
  since it marks an archive complete), `checkpoint.tar` and `spec.json` under
  `gs://<bucket>/<path>/<snapshot>/`, then removes the finalizer. The bucket
  and path come from the work order itself, so a deleted
  `PodSnapshotStorageConfig` doesn't matter. A missing object or bucket is
  success. A work order without a bucket (refused, or no storage) has no
  archive and is released right away.
- A work order without an outcome may still be uploading. Deleting it cancels
  it (the node agent treats a `PodSnapshot` being deleted as a cancel), and
  the engine waits up to 5 minutes (the agent's 150 s kubelet checkpoint wait
  plus the upload) for the agent to record the outcome before
  it deletes the archive, so an upload in flight can't write objects after
  the delete.
- A delete that fails (for example a missing bucket permission) is retried
  with backoff, from 1 s up to 1 min, with an `ArchiveDeleteFailed` Warning
  event on the `PodSnapshot`. After `--archive-gc-give-up` (default `10m`)
  from the first failed attempt (kept in memory, so an engine restart starts
  it over), the engine removes the finalizer anyway and records an
  `ArchiveDeleteAbandoned` Warning event naming the archive left in the
  bucket. So deleting a namespace never wedges on the finalizer while the
  engine controller runs. Metric: `pm_psengine_archive_gc_total{result}`
  (`deleted`, `no_archive`, `failed`, `abandoned`).
- A trigger re-created for the same pod instance never gets a `PodSnapshot`
  that's being deleted; it waits until it's gone and gets a new work order.
- The engine controller needs `update` and `patch` on `podsnapshots` (in
  `deploy/deploy.yaml`), and `storage.objects.delete` on the bucket, which
  its storage probe already uses.

**Uninstalling the engine:** nothing removes the finalizer once the engine
controller is gone, so once no migration is in flight, delete the CRIU
`PodSnapshot`s while it still runs:

```bash
kubectl delete podsnapshots.criu.podmigration.gke.io -A -l pod-migrate.io/engine=criu --timeout=20m
```

Raise `--timeout` if you raised `--archive-gc-give-up` above 10m; the worst case is the settle wait plus the give-up bound. If the
engine is already gone, remove the finalizers by hand. This removes every
finalizer on each object, and the archives stay in the bucket:

```bash
kubectl get podsnapshots.criu.podmigration.gke.io -A -o jsonpath='{range .items[*]}{.metadata.namespace}{" "}{.metadata.name}{"\n"}{end}' |
  while read -r ns name; do
    kubectl patch podsnapshots.criu.podmigration.gke.io -n "$ns" "$name" --type=json \
      -p='[{"op":"remove","path":"/metadata/finalizers"}]' || true
  done
```

The controller's `make -C controller undeploy` doesn't touch CRIU
`PodSnapshot`s, so it isn't affected.

## Rolling Node-Pool Upgrades & Kernel Compatibility

Rolling node-pool upgrades in GKE cordon and drain nodes one by one, triggering the `EvictionGate` to initiate live migrations for opted-in stateful pods (`pod-migration.gke.io/enabled: "true"`). During a rolling upgrade, pods checkpointed on an older node are restored onto an upgraded node that may run a newer Linux kernel, containerd, runc, or CRIU version.

### Version Compatibility Matrix & Guidelines

| Upgrade Type                   | Example Jump                               | Compatibility Status               | Operational Guidance & Behavior                                                                                                                                                                                                 |
| ------------------------------ | ------------------------------------------ | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **GKE Patch Upgrade**          | `1.34.1-gke.1000` &rarr; `1.34.2-gke.1100` | **Fully Supported**                | Identical kernel major.minor, identical glibc/ABI. In-memory state and socket structures restore with zero degradation.                                                                                                         |
| **GKE Minor Upgrade**          | `1.34.x` &rarr; `1.35.x`                   | **Supported (forward-compatible)** | Newer kernel on destination node. Linux user-space ABI stability ensures task memory, threads, and file descriptors restore cleanly provided CPU features match.                                                                |
| **Minor Downgrade / Rollback** | `1.35.x` &rarr; `1.34.x`                   | **Best-effort / Fallback**         | Checkpoints utilizing newer kernel syscalls or vDSO mappings may fail to restore on older kernels. Handled safely: the replacement pod falls back to a clean cold start with `RestoreCrashFallback`.                            |
| **CPU Architecture Mismatch**  | Intel &rarr; AMD / mixed microarch         | **Refused (fenced)**               | Heterogeneous CPU instruction sets (e.g. missing AVX/AVX-512 flags) cause SIGILL on restore. Filtered via `status.sourceNode` facts recorded at checkpoint time ([#257](https://github.com/gke-labs/pod-migration/issues/257)). |

### Prerequisites for Clean Upgrades

1. **Homogeneous Node Pools:** Node pools participating in rolling upgrades must share CPU family and instruction sets.
2. **Container Runtime Compatibility:** All nodes must run containerd `>= 2.0` (with patch requirements satisfying CVE-2026-95837) and `runc >= 1.2`.
3. **In-Memory Integrity Verification:** Workloads must use explicit entrypoints (`command: ["docker-entrypoint.sh"]`) and opt-in labels.

### Running the Live Upgrade Verification Suite

The repository includes an automated verification script in `verification-suite/run_criu_nodepool_upgrade.sh` that deploys an in-memory stateful workload (Redis or monotonic counter), seeds unpersisted in-memory state, drives/monitors the node pool upgrade, and verifies zero restarts, verified restore outcomes, and data survival:

```bash
# Offline self-test (validates manifests, verifiers, and failure paths):
./verification-suite/run_criu_nodepool_upgrade.sh --self-test

# Live cluster rolling upgrade test:
./verification-suite/run_criu_nodepool_upgrade.sh \
  --cluster kyber-test \
  --location northamerica-northeast2-b \
  --node-pool criu-pool \
  --target-version 1.34.2-gke.1100 \
  --workload redis

# Drain-only verification across existing mixed-version nodes:
./verification-suite/run_criu_nodepool_upgrade.sh \
  --namespace pm-upgrade-test \
  --drain-node \
  --skip-upgrade
```

## Building & Testing

```bash
cd engines/criu
go vet ./...
go build ./...
go test -race ./...
```

To build the three container images:

```bash
cd engines/criu
docker build -f build/engine/Dockerfile -t <YOUR_CRIU_ENGINE_IMAGE> .
docker build -f build/agent/Dockerfile  -t <YOUR_CRIU_AGENT_IMAGE> .
docker build -f build/holder/Dockerfile -t <YOUR_CRIU_HOLDER_IMAGE> .
```

## Installing on a Test Cluster

Apply the `criu.podmigration.gke.io` CRDs and deploy the engine, agent, and
webhook:

```bash
kubectl apply -f engines/criu/deploy/crds.yaml
sed -e "s|<YOUR_CRIU_ENGINE_IMAGE>|${ENGINE_IMG}|g" \
    -e "s|<YOUR_CRIU_AGENT_IMAGE>|${AGENT_IMG}|g" \
    -e "s|<YOUR_CRIU_HOLDER_IMAGE>|${HOLDER_IMG}|g" \
    engines/criu/deploy/deploy.yaml | kubectl apply -f -
```

When creating `PodSnapshotStorageConfig` and `PodSnapshotPolicy` resources for
this engine, label them with `pod-migrate.io/engine: criu` so `cmd/engine`
reconciles them.

## Licensing and Source Code Availability

The CRIU snapshot engine is licensed under Apache-2.0. The bundled CRIU binary (`/opt/criu-bundle/criu.bin`) and certain shared libraries are licensed under GPL-2.0 and LGPL-2.1 and invoked strictly as an external subprocess.

- **Image Inventory & Third-Party Notices**: See [docs/third-party-licenses.md](../../docs/third-party-licenses.md).
- **GPL Source Packaging & Release Process**: See [docs/release-process.md](../../docs/release-process.md) and `tools/release/package_gpl_sources.sh`.
