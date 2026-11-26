# CRIU Snapshot Engine (`engines/criu`)

> [!WARNING]
> **EXPERIMENTAL — FOR NON-PRODUCTION TEST CLUSTERS ONLY: the security requirements R1, R3, R4 and R6 are still open (C10 in [`docs/design/engine-contract.md`](../../docs/design/engine-contract.md)).**
> `engines/criu` runs a privileged node DaemonSet (`hostPID`, `hostNetwork`,
> host containerd/kubelet mounts). It serves its own dedicated
> `criu.podmigration.gke.io` CRD group so it can coexist alongside the managed
> GKE Pod Snapshots addon (`podsnapshot.gke.io`) in test clusters, and its
> DaemonSet targets Linux nodes (`kubernetes.io/os=linux`) while excluding
> `gVisor` sandbox nodes (`sandbox.gke.io/runtime=gvisor`). It runs on both
> Ubuntu and COS node images (see [Node images](#node-images)).
> Never apply `deploy/crds.yaml` or `deploy/deploy.yaml` to a production
> cluster.
>
> **Security status:** archive provenance (R2) and archive handling (R5) are
> in place, with the residual risks in the first item below. The other items
> are open.
>
> - **Archive provenance (`R2`) & dedicated GCS credentials (`R5`) ([#244](https://github.com/gke-labs/pod-migration/issues/244)):**
>   Checkpoint archives in GCS (`manifest.json`, `spec.json`, `checkpoint.tar`)
>   are SHA-256 and MD5 integrity-checked, and restore binds the OCI runtime
>   security posture (`Capabilities`, `User`, `Seccomp`, `MaskedPaths`,
>   `ReadonlyPaths`, `Devices`, `Mounts`, and canonical sandbox `Namespaces`)
>   to the kubelet-created destination container spec. The node agent signs
>   each archive's manifest with a key from the
>   `pod-migrate-archive-signing-keys` Secret, which only the agent mounts, and
>   binds the archive to its snapshot, pod, location, PodMigrationJob and
>   cluster; a restoring agent refuses an archive that doesn't verify, and
>   the replacement cold-starts (see [Archive signing](#archive-signing)). So
>   writing to the checkpoint bucket no longer gets a restoring node to run an
>   attacker's process image. CRIU still restores process credentials and
>   seccomp state from the checkpoint image files themselves, though, so
>   **anyone who can read Secrets in `pod-migration-criu-system`, or who
>   compromises a node agent, can sign an archive that gets root on a
>   restoring node**. The engine uses the bucket as its own two Kubernetes
>   service accounts: the `hostNetwork` node agent exchanges its own
>   projected service account token at Google's Security Token Service
>   instead of using the node's VM service account. R5 holds only for a
>   bucket that only the CRIU engine uses and that grants object access to
>   those two alone (`patches/setup-criu-storage.sh`, see
>   [Storage credentials](#storage-credentials)). A namespace that both
>   engines serve has one storage location for both, on a bucket that also
>   grants GKE Pod Snapshots' workloads, so its CRIU archives don't meet R5.
>   Even on a CRIU-only bucket, anyone who can get a token for either service
>   account (create pods, or workloads that create them, in
>   `pod-migration-criu-system`, exec into the agent's pods, or create
>   `serviceaccounts/token` for them), who has storage access through the
>   project, or who compromises a node, can still read archives, and delete
>   or corrupt them to force cold starts.
> - **Agent RBAC Breadth (`pod-migrate-psengine-agent`):** The node DaemonSet holds
>   cluster-wide `pods` (`get`, `list`, `watch`, `update`, `patch`), `nodes` (`get`,
>   `patch`), and `nodes/checkpoint` (`create`) permissions to annotate restored pods
>   with `pod-migrate.io/psengine-restore` (and, on a failure,
>   `pod-migrate.io/psengine-restore-reason`) and trigger Kubelet container checkpoints.
> - **Residual Dual-Start Window:** If the holder container downward-API poll times out
>   or falls through to cold-start execution before sibling restore initiates, a residual
>   dual-start race window exists where both cold-started app processes and restored CRIU
>   processes could execute concurrently.
> - **Known follow-ups:**
>   - `cmd/holder`: downward-API volume propagation lag after gate removal can
>     exceed the initial 2s `ps-name` poll window and trigger a cold-start
>     fallback before the annotation projects into `/pminfo/annotations`.
>   - `internal/psengine/restore_node.go`: `fetch()` enforces a 4 MiB download cap on
>     metadata (`manifest.json`, `spec.json`) and an 8 GiB cap on `checkpoint.tar`.
>   - `internal/node/node.go`: `Bootstrap` installs `/usr/local/sbin/criu`
>     (on COS, a bind mount over `/usr/local`) and `/etc/criu/runc.conf` on the
>     host node; removing the DaemonSet does not automatically uninstall those
>     host files or remove that bind mount.

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
     work order without a new eligibility check. A work order that an
     earlier instance of the trigger left with an outcome (`Ready`, `Failed`,
     `Refused` or `Cancelled`) is deleted instead, and the trigger gets a new
     one once it's gone (#262). `CheckpointTimedOut` is left as it is. A work
     order that a job or pod still uses is never deleted. The engine checks
     live for a PodMigrationJob whose `status.snapshotRef` names it, and for
     a pod whose restore order names it and hasn't finished restoring. If
     that job owns the trigger (the job created its trigger again), the
     trigger takes the work order over and is answered with it. Otherwise
     the trigger stays unanswered and is looked at again every 30 s, so its
     job waits until its migration timeout and falls back to a cold
     eviction. A work order from before the trigger UID was recorded is
     answered as before in that case. The trigger records why: a
     `WorkOrderInUse` Warning event naming the work order and the job or
     pod that uses it. Look for it with
     `kubectl describe podsnapshotmanualtrigger <name>` when a job timed out
     in `Snapshotting`.
   - Serves `/psengine-restore-inject` to speculatively inject the `holder`
     initContainer and command wrapper into CRIU-routed gated replacement pods
     at `CREATE` (checking single-container `runc`/default pod shape in memory
     first so `gVisor`, sidecar, `hostNetwork`, and ungated pods short-circuit
     with zero API reads, then reading the assigned `PodMigrationJob` to verify
     `pod-migration.gke.io/engine: criu` so other engines' pods are never touched).
2. **`cmd/agent` (privileged node DaemonSet):**
   - At startup (`node.Bootstrap`), installs a self-contained CRIU >= 4.2.1
     bundle (older CRIU fails dumps on Linux >= 6.16; the image builds it from
     Debian forky pinned to a snapshot.debian.org timestamp)
     into `/var/lib/pod-migrate/criu-bundle` and `/usr/local/sbin/criu`
     (on COS, under `/home/kubernetes/bin`; see [Node images](#node-images)),
     writes `/etc/criu/runc.conf`, runs `criu check --all` in the host namespaces via
     `nsenter` (controlled by `PODMIG_CRIU_CHECK`, default: `all`), verifies containerd readiness (inspecting containerd's process
     PATH or shim PATH and ensuring `enable_criu != false` in containerd configuration
     before declaring the node ready), and stamps `pod-migrate.io/criu-ready` and
     `pod-migrate.io/criu-version` on the `Node`.
     The node reaches `pod-migrate.io/criu-ready: true` if core checkpoint/restore features
     pass and all required feature probes pass (`cgroupns`, `fdinfo_lock`, `mem_dirty_track`,
     `seccomp_filters`, `seccomp_suspend`, and `uffd`). Missing optional features (such as
     `timer_cr_ids` or `pagemap_scan_guard_pages` on Linux 6.12) are recorded in
     `pod-migrate.io/criu-reason`. Setting `PODMIG_CRIU_CHECK=core` relaxes the kernel check
     to plain `criu check` (core C/R features only).
     If containerd cannot resolve CRIU on its PATH/shim PATH, or CRIU is disabled
     by configuration, or containerd cached a failed CRIU lookup, or the CRIU
     containerd resolves is older than 4.2.1 (the bundle's minimum: older CRIU
     fails dumps on Linux >= 6.16), or a required kernel feature is missing, the agent marks
     the node `pod-migrate.io/criu-ready: false` with reason and remediation
     annotations (`pod-migrate.io/criu-reason` and `pod-migrate.io/criu-remediation`).
     This prevents orchestrators from dispatching checkpoints to nodes where containerd
     would poison its CRIU lookup cache (#130), or dump with a CRIU too old for
     the kernel.
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
     source container's OCI spec, uploads `spec.json`, `checkpoint.tar`, and a
     signed `manifest.json` (see [Archive signing](#archive-signing)) to GCS
     as its own service account (see [Storage credentials](#storage-credentials))
     with MD5/size verification, and marks the
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
   - Records the source node's facts with each checkpoint, so that the
     orchestrator places the restore only on a compatible node ([#257](https://github.com/gke-labs/pod-migration/issues/257),
     see [Restore placement](#restore-placement)).
     The agent gathers them just before it freezes the source and writes
     them twice: as `sourceNode` in `manifest.json`, next to the archive, and
     as `status.sourceNode` on the `PodSnapshot`, in the same status write
     that marks it `Ready=True`. The orchestrator can read the `PodSnapshot`
     without access to the bucket. The fields are `arch` (the agent's
     architecture as Go names it, `amd64` or `arm64`, matching the node's
     `kubernetes.io/arch` label), `cpuVendor` (the
     `vendor_id` line of `/proc/cpuinfo`, absent on arm64), `cpuModel` (the
     `model name` line of `/proc/cpuinfo`, absent on arm64, which has no such
     line), `cpuFeatures` (the `flags` (x86) or `Features` (arm64) lines, the
     union across processors, sorted, raw: on x86 they include kernel and
     mitigation flags), `kernelVersion` (`/proc/sys/kernel/osrelease`),
     `criuVersion` (the CRIU bundle's `VERSION` file), `runcVersion`
     (`runc --version` in the host mount namespace, on the `PATH` the agent's
     restores use, which may not be the runc containerd is configured with)
     and `containerdVersion` (containerd's version API, without the leading
     `v`), plus `placementLabels`, the node label format the agent publishes
     (`v1`). Every probe is best effort and bounded: a fact the agent can't read
     is left out, and never fails the checkpoint. A failed checkpoint records
     nothing on the `PodSnapshot`; one that fails at the commit point has
     already uploaded its manifest, facts included. The restore node itself
     still checks only `cpuFamily`.
   - Publishes its own node's facts on the `Node`, as labels for restore
     placement and annotations for people (see [Restore placement](#restore-placement)).
   - Reconciles replacement pods on its node carrying the restore order,
     `<group>/ps-name` (`psengine.NodeRestoreReconciler`): downloads the
     archive's `manifest.json` from GCS and verifies its signature and
     bindings before anything else (see [Archive signing](#archive-signing)),
     then downloads and SHA-256 verifies the rest of the archive, checks CPU family compatibility,
     prepares an overlayfs rootfs with `rootfs-diff.tar`, rewrites the OCI spec
     (`node.AdaptSpec`), restores the process tree as a sibling `runc` container
     joining the holder's network/IPC/UTS namespaces, and stamps
     `pod-migrate.io/psengine-restore: restored` (or `failed`, with why in
     `pod-migrate.io/psengine-restore-reason`, in the same update).
     On an existing pod in a managed namespace (`pod-migration.gke.io/managed: "true"`),
     only the agent may write these annotations, and only
     once: the `pod-migrate-psengine-restore-status`
     `ValidatingAdmissionPolicy` in `deploy/deploy.yaml` denies every other
     write, change or removal (#233), and a reason next to anything but
     `failed` (#244). To edit them by hand, set the
     `validationActions` of the `ValidatingAdmissionPolicyBinding` of the
     same name to `["Audit"]`, and back to `["Deny"]` afterwards; while it
     is `Audit`, no pod in the cluster is protected. Don't apply
     `deploy/deploy.yaml` again for this, which would also re-apply the
     agent DaemonSet and the engine Deployment. See
     [Changing a Restore Order by Hand](../../README.md#changing-a-restore-order-by-hand).
     The policy names the agent's identity as a literal
     (`system:serviceaccount:pod-migration-criu-system:pod-migrate-agent`):
     an install with another namespace or service account name must change
     it in the policy too, or the agent's own write is denied.
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
excluding `gVisor` sandbox nodes.

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
  deploy to production clusters until the remaining security requirements
  R1, R3, R4 and R6 (C10 in `docs/design/engine-contract.md`) are met.
- **Workload Identity and a CRIU-only bucket (bucket access):** Workload
  Identity enabled on the cluster, the GKE metadata server
  (`--workload-metadata=GKE_METADATA`) on the node pool that runs the engine
  controller, and a checkpoint bucket that only the CRIU engine uses. The
  node agent and the engine controller use the bucket as their own
  Kubernetes service accounts, never as the nodes' VM service account (see
  [Storage credentials](#storage-credentials)).
- **Node images:** see [Node images](#node-images): Ubuntu and COS
  (`COS_CONTAINERD`) are supported.
- **Linux kernel & runtime:** Nodes running `containerd` 2.x + `runc` with
  kubelet `ContainerCheckpoint` enabled and a kernel passing `criu check`.
- **runc location:** the agent runs the node's own `runc` for restores, looked
  up on `/usr/sbin:/usr/bin:/sbin:/bin:/home/kubernetes/bin:/usr/local/sbin:/usr/local/bin`
  in that order. A node whose `runc` is elsewhere is still marked CRIU-ready,
  but its restores fail and the replacements start cold. Nodes need no CRIU of
  their own: the agent installs its bundled CRIU.
- **CRIU on containerd's path:** if a node has its own CRIU and containerd
  finds it before the agent's `/usr/local/sbin/criu` (earlier on containerd's
  `PATH`, or on the shim `PATH` when one is set), it must be 4.2.1 or newer.
  Otherwise the agent marks the node `pod-migrate.io/criu-ready: false`, and
  its `pod-migrate.io/criu-reason` ends in "older than minimum supported
  version 4.2.1".

  **Upgrading.** An agent from before this requirement accepted any CRIU from
  3.16 on. After the upgrade, a node where containerd finds its own CRIU 3.16
  to 4.2.0 first turns `criu-ready: false`, with a `criu-reason` like this:

  ```text
  criu version 4.1.1 on containerd PATH is older than minimum supported version 4.2.1
  ```

  The controller then stops migrating that node's pods with CRIU, so a drain
  evicts them cold; checkpoints already running and restores aren't affected.
  To list such nodes:

  ```bash
  kubectl get nodes -o custom-columns='NODE:.metadata.name,REASON:.metadata.annotations.pod-migrate\.io/criu-reason' \
    | grep 'older than minimum supported version 4.2.1'
  ```

  To fix a node, make containerd find the agent's `/usr/local/sbin/criu`
  first (put `/usr/local/sbin` earlier on containerd's `PATH`, or on the shim
  `PATH` if one is set) or remove the node's own CRIU, then restart
  containerd. The agent notices the restart and checks the node again.

  Note: `pod-migrate.io/criu-reason` can also appear on ready nodes (`pod-migrate.io/criu-ready: true`)
  when optional kernel features are missing (e.g. `missing optional CRIU features: pagemap_scan_guard_pages, timer_cr_ids`).
  When querying specifically for unready nodes, check `pod-migrate.io/criu-ready: "false"` rather than
  relying solely on the presence of the reason annotation.

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
    fall through to a cold start if restore fails). Most Deployments rely on
    the image's entrypoint and set no `command`: with the CRIU engine, such a
    pod is not migrated. It is evicted cold, with a Warning
    `MigrationSkippedIneligible` event on the pod that says why (#273, #275).
  - Non-host-network pods (`hostNetwork: false`); pods declaring
    `hostNetwork: true` are refused across `PSMTReconciler`, `RestoreInjector`,
    and `NodeRestoreReconciler`.
  - Homogeneous CPU family between source and target nodes.
  - Single migration per pod instance: migrating an already-restored CRIU pod
    or holder-injected replacement is refused so the orchestrator falls back
    to a clean cold start without checkpointing the holder wrapper ([#121](https://github.com/gke-labs/pod-migration/issues/121)).
  - The orchestrator checks the pod-level limits above itself before it starts
    a migration (`CRIU.Eligible` in `controller/internal/engine/criu.go`):
    a pod outside them is evicted without a migration, instead of going
    through a `429` and a refused trigger first (#233). The eviction webhook
    and the preemption controller record a Warning `MigrationSkippedIneligible`
    event on such a pod, with the engine's reason (#275).

## Node images

The agent runs on GKE's Ubuntu (`UBUNTU_CONTAINERD`) and Container-Optimized
OS (`COS_CONTAINERD`) node images, and on kind. It installs CRIU through the
host's root, `/proc/1/root` (its DaemonSet has `hostPID`), and picks where
from the host's `/etc/os-release` when it starts:

- **Ubuntu, kind, any other node:** the bundle in
  `/var/lib/pod-migrate/criu-bundle`, the `criu` wrapper in `/usr/local/sbin`.
- **COS (`ID=cos`):** COS mounts its root (`/usr`, `/opt`) read-only and
  `/var` and `/home` `noexec`, and containerd's `PATH` there is
  `/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin` (the shims
  put `/opt/containerd/bin` first), so no directory on it is writable, and
  `/home/kubernetes/bin`, the one writable host path that allows exec, isn't
  on it. containerd 2.x has no option to point runc at another `criu`. So the
  agent installs the bundle in `/home/kubernetes/bin/pod-migrate/criu-bundle`,
  writes the wrapper to `/home/kubernetes/bin/pod-migrate/usr-local/sbin`, and
  bind-mounts `/home/kubernetes/bin/pod-migrate/usr-local` over `/usr/local`,
  which COS ships empty. containerd then finds `/usr/local/sbin/criu`, as on
  Ubuntu.

On COS:

- If the host's `/usr/local` isn't empty and isn't the agent's bind already,
  the agent doesn't cover it: the node is marked
  `pod-migrate.io/criu-ready: false` with a `criu-reason` that starts with
  `criu bundle install failed: host /usr/local isn't empty`. Empty it (or
  unmount what covers it), then restart that node's `pod-migrate-agent` pod.
- The bind mount lasts until the node reboots, and the agent makes it again
  when it starts. Removing the agent leaves it in place; `umount /usr/local`
  on the node removes it.
- `/etc/criu`, on COS's writable `/etc` overlay, is lost at reboot too, and
  rewritten by the agent at startup.

**Upgrading.** Earlier `deploy/deploy.yaml` kept the agent off COS nodes.
After you apply this one, the agent also starts on every COS node, makes the
bind mount there, and marks the node `criu-ready` once it passes its checks:
drains of eligible pods on those nodes then migrate with CRIU instead of
evicting cold.

## What Installing the Engine Changes on a Node

When `pod-migrate-agent` starts on a node, it applies the following modifications:

| Modification                           | Host Location                                                                              | Description                                                                                                                                                                                                                                    |
| -------------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CRIU Bundle**                        | `/var/lib/pod-migrate/criu-bundle/` (COS: `/home/kubernetes/bin/pod-migrate/criu-bundle/`) | Directory containing self-contained `criu.bin`, bundled shared libraries, dynamic linker, and `VERSION` file.                                                                                                                                  |
| **CRIU System Wrapper**                | `/usr/local/sbin/criu`                                                                     | Executable wrapper script that launches CRIU via the bundled dynamic linker. On COS it is `/home/kubernetes/bin/pod-migrate/usr-local/sbin/criu`, seen at `/usr/local/sbin/criu` through the bind mount below.                                 |
| **`/usr/local` bind mount (COS only)** | `/usr/local`                                                                               | `/home/kubernetes/bin/pod-migrate/usr-local` bind-mounted over COS's empty, read-only `/usr/local`, until the node reboots (see [Node images](#node-images)).                                                                                  |
| **Runc Configuration**                 | `/etc/criu/runc.conf`                                                                      | CRIU global configuration specifying `tcp-close`, `skip-in-flight`, `file-validation buildid`, and `file-locks`.                                                                                                                               |
| **Node Annotations**                   | Kubernetes `Node` metadata                                                                 | Annotated with `pod-migrate.io/criu-ready` (`true` or `false`) and `pod-migrate.io/criu-version`. Stale reason/remediation annotations are cleared on promotion.                                                                               |
| **Node Placement Facts**               | Kubernetes `Node` metadata                                                                 | Labelled with `criu.pod-migrate.io/*` and `missing-cpu.criu.pod-migrate.io/*` placement labels, and annotated with `pod-migrate.io/criu-node-facts` and `pod-migrate.io/criu-node-cpu-features` (see [Restore placement](#restore-placement)). |
| **Socket Watching**                    | `/run/containerd/containerd.sock`                                                          | Monitored for inode changes to detect containerd daemon restarts and re-probe readiness.                                                                                                                                                       |

Uninstalling the engine doesn't remove the `Node` labels and annotations: they
stay on each `Node` until you remove them, for example with
`kubectl label node <node> criu.pod-migrate.io/cpu-vendor-` for each label, or
until the node is replaced.

## Storage Security & Bucket IAM Guidance

Because CRIU checkpoint archives contain complete, unencrypted process address spaces (including in-memory tokens, TLS keys, and credentials), storage backends must be secured with the same rigor as Kubernetes Secrets:

1. **A Bucket for the CRIU Engine Alone:** Use a checkpoint bucket that only the CRIU engine uses, and that grants object access to the engine's two Kubernetes service accounts and to nobody else, not to the nodes' VM service account either (see [Storage credentials](#storage-credentials); `patches/setup-criu-storage.sh` sets one up). Don't share GKE Pod Snapshots' bucket, which grants its workloads' service accounts. The engine controller needs object create, get and delete for its storage probe and archive garbage collection (#107); without delete, archives are left behind, reported only by events.
2. **Avoid the Default Compute Engine SA:** Do **not** run nodes as the default Compute Engine service account (`<project-number>-compute@developer.gserviceaccount.com`), which often carries broad Project Editor permissions, and don't grant it, or any other node service account, on the bucket.
3. **CMEK Encryption:** Configure Cloud KMS Customer-Managed Encryption Keys (CMEK) on the checkpoint bucket.
4. **Lifecycle Retention:** Apply GCS Object Lifecycle Management rules to automatically delete checkpoint archives after **1 to 7 days**, preventing cold archives from lingering. The engine deletes a job's archive when its `PodSnapshot` is deleted (see Archive garbage collection below); keep the rule as a backstop for archives it leaves behind (including temporary composite upload parts matching `checkpoint.tar.part-*`).

For full threat analysis, blast radius documentation, and restore path verification details, see [Threat Model and Security Guidance for the CRIU Engine](../../docs/threat-model-criu.md) and [SECURITY.md](../../SECURITY.md).

## Storage credentials

The node agent runs with `hostNetwork: true`, where the GKE metadata server
can't tell its requests from the node's and answers as the node's VM service
account, which every workload on the node can use too. So the agent doesn't
get its bucket credentials from the metadata server (contract clause C10 R5,
#244):

- Its DaemonSet projects a token of its own service account,
  `pod-migration-criu-system/pod-migrate-agent`, with the cluster's Workload
  Identity pool as its audience, at
  `/var/run/secrets/pod-migrate/gcs-token/token`. The token lives an hour,
  and the kubelet rotates it.
- When it needs an access token, the agent reads that file again and
  exchanges the token at Google's Security Token Service
  (`https://sts.googleapis.com/v1/token`) for an access token of that service
  account's Workload Identity principal, scoped to
  `https://www.googleapis.com/auth/devstorage.read_write`. It reuses the
  access token until 5 minutes before it expires: a request authenticates
  once, when it starts, so 5 minutes covers clock skew and the request's
  trip with room to spare, for about one exchange an hour. There's no Google
  service account and no impersonation: the bucket grants the principal
  itself.
- The engine controller doesn't run with `hostNetwork`, so it keeps getting
  its token from the metadata server, where GKE Workload Identity serves it
  a token of its own service account,
  `pod-migration-criu-system/pod-migrate-psengine-controller`.

**Requirements.** Workload Identity enabled on the cluster, the GKE metadata
server (`--workload-metadata=GKE_METADATA`) on the node pool that runs the
engine controller, and a bucket for the CRIU engine alone (below). On a node
pool without the GKE metadata server, the controller gets its node's
credentials instead, which the bucket shouldn't grant.

**Install substitutions.** Replace these in `deploy/deploy.yaml`, along with
the images (see [Installing on a Test Cluster](#installing-on-a-test-cluster)):

| Placeholder                | Value                                                                                                                                |
| -------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `<YOUR_WORKLOAD_POOL>`     | The cluster's Workload Identity pool: `<PROJECT_ID>.svc.id.goog`                                                                     |
| `<YOUR_IDENTITY_PROVIDER>` | The cluster's identity provider: `https://container.googleapis.com/v1/projects/<PROJECT_ID>/locations/<LOCATION>/clusters/<CLUSTER>` |

`<YOUR_WORKLOAD_POOL>` appears twice, as the agent's `WORKLOAD_POOL` and as
the audience of its projected token, which must be the same.

**A bucket for the CRIU engine alone.** Each archive is a full memory dump of
the migrated container, so the bucket must give object access to the
engine's two service accounts and to nobody else. Signing stops a tampered
archive from being restored (see [Archive signing](#archive-signing)), but
not anyone from reading one. That rules out:

- GKE Pod Snapshots' bucket, as `patches/setup-storage.sh` sets it up: it
  grants every service account in the `default` namespace, and the GKE
  service agent.
- A new bucket's default policy, which grants the project's Owners and
  Editors (`roles/storage.legacyBucketOwner` and
  `roles/storage.legacyObjectOwner`) and its Viewers
  (`roles/storage.legacyBucketReader` and `roles/storage.legacyObjectReader`)
  through the project convenience values.

`patches/setup-criu-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]` sets one
up. It creates the bucket (by default `<PROJECT_ID>-criu-snapshots`, with
uniform bucket-level access and no soft delete), or checks an existing one
first: if that bucket doesn't use uniform bucket-level access (object ACLs
could still grant access), or grants anyone but the two principals below and
the legacy convenience bindings, such as a namespace's service accounts, the
GKE service agent or the nodes' service account, the script says why, prints
what would fix it, and exits without changing anything. It
never strips a shared bucket. Otherwise it grants the two principals,
removes the four legacy convenience bindings after printing them (leaving
for last the one through which project Editors may change the bucket's IAM
policy), checks that the bucket grants nobody else, and warns about what the
bucket's own policy can't keep out (below). Run it as a project Owner or
Storage Admin. Then point the PodMigration of each namespace that only CRIU
serves at the bucket (`spec.storage.location: gs://<BUCKET_NAME>/<namespace>/snapshots`
or another path starting with `<namespace>/`, as required by the controller's multi-tenant scoping guardrails).

A namespace that both engines serve (its runtime classes routed partly to
`gke-pod-snapshots` and partly to `criu`) has one storage location for both
engines, on a bucket that has to grant GKE Pod Snapshots' workloads too. So
its CRIU archives can't meet R5 today: anyone that bucket grants can read
them. Serve such a namespace with one engine, or accept that risk until each
engine can have a storage location of its own (a follow-up).

**Bucket principals.** Grant each of them `roles/storage.objectUser` and
`roles/storage.bucketViewer` on the CRIU bucket (`patches/setup-criu-storage.sh`
does this), with `<PROJECT_NUMBER>` the number of the project that hosts the
cluster:

- `principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/pod-migration-criu-system/sa/pod-migrate-agent`
- `principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/pod-migration-criu-system/sa/pod-migrate-psengine-controller`

An install in another namespace, or with other service account names, grants
those instead (the script takes `CRIU_NAMESPACE`, `CRIU_AGENT_SA` and
`CRIU_CONTROLLER_SA`). The nodes' VM service account needs no access to the
bucket.

**What the bucket's own policy can't keep out.** Roles granted on the
project, or above it, that carry `storage.objects` permissions (Storage
Admin, the other `roles/storage.*` roles, custom roles) reach the archives
whatever the bucket's policy says. Once the convenience bindings are gone,
project Owners and Editors have no object access of their own, but they still
have ways in: an Owner can grant itself such a role, and Owners and Editors
can create pods in `pod-migration-criu-system` through GKE IAM, and a pod can
run as either of the engine's service accounts and get its token. The same
goes for anyone with Kubernetes RBAC to create pods, or workloads that create
them, in that namespace, and for the nodes' service account if it has such a
role: the default Compute Engine service account often has Editor. Run CRIU
nodes as a service account without one.

**Flags.** Each falls back to the environment variable in parentheses:

- `--gcs-credentials` (`GCS_CREDENTIALS`): `workload-identity`, the default,
  or `node`. With `node`, the agent uses the node's VM service account from
  the metadata server, as it did before, and logs a warning at startup.
- `--workload-pool` (`WORKLOAD_POOL`) and `--identity-provider`
  (`IDENTITY_PROVIDER`): the values above. The DaemonSet sets both.
- `--gcs-token-file` (`GCS_TOKEN_FILE`): the projected token, by default
  `/var/run/secrets/pod-migrate/gcs-token/token`.

With `workload-identity`, an agent whose workload pool or identity provider
is empty or still a placeholder, or that can't read its token file, logs why
and exits at startup: it never falls back to the node's service account. It
then gets a first access token in the background, and logs
`checkpoint bucket credentials work: got an access token`, or why not; a
failure doesn't stop it, so an STS outage doesn't crash-loop every agent.
Before each checkpoint freezes its source, the agent makes sure it holds a
token that stays usable for 15 minutes, longer than the orchestrator waits
for a checkpoint (`--migration-timeout`, 10 minutes by default), so no
token is fetched while the source is frozen. If it can't get one, the
checkpoint fails with STS's HTTP status and error in its message, never a
token, and `the source container was not touched`.

`STORAGE_EMULATOR_HOST` is for tests only (the kind E2E runs
`fake-gcs-server`). It names a GCS emulator: `host:port`, as Google's client
libraries take it, or a URL such as `http://host:port`. With it set, the
agent sends its bucket requests to that emulator's GCS JSON API
(`<endpoint>/upload/storage/v1` and `<endpoint>/storage/v1`, over plain HTTP
for a bare `host:port`) instead of GCS, with no credentials at all: no
`Authorization` header, and no request to the metadata server or STS. It
skips the `workload-identity` checks, since a test cluster has no Workload
Identity pool, and logs a `WARNING` at startup. Whoever runs that endpoint
controls the archives, so never set it on a real cluster.

**Upgrading.** On a cluster where the agent used the node's VM service
account, move the CRIU engine to a new bucket:

1. Let in-flight CRIU migrations finish.
2. Set up a bucket for the CRIU engine alone with
   `patches/setup-criu-storage.sh`. For a bucket that only the CRIU engine
   already uses, you can grant the two principals yourself instead, and
   remove its legacy convenience bindings as the script does:
   ```bash
   for sa in pod-migrate-agent pod-migrate-psengine-controller; do
     for role in roles/storage.objectUser roles/storage.bucketViewer; do
       gcloud storage buckets add-iam-policy-binding gs://<BUCKET_NAME> --role="${role}" --condition=None \
         --member="principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/pod-migration-criu-system/sa/${sa}"
     done
   done
   ```
   Make sure the engine controller's node pool runs the GKE metadata server.
3. Point the PodMigration of each namespace that only CRIU serves at the
   bucket (`spec.storage.location`).
4. Apply the new `deploy/deploy.yaml` with both substitutions, and wait for
   `kubectl -n pod-migration-criu-system rollout status ds/pod-migrate-agent`.
5. Check that the new bucket works. It grants neither the nodes nor anyone
   else, so only the engine's own identities can use it:
   - each agent logs `checkpoint bucket credentials work: got an access token`
     at startup, so STS accepted its token;
   - a test migration in a namespace that only CRIU serves checkpoints and
     restores, so the agents read and write the new bucket;
   - once that migration's PodSnapshot is deleted (after the controller's
     `--snapshot-retention`), no `ArchiveDeleteFailed` event follows, so the
     engine controller deletes from the new bucket too.
6. Delete the CRIU archives left in the old bucket by hand. Before the
   switch, list the CRIU PodSnapshots and delete only their prefixes,
   `gs://<OLD_BUCKET>/<OLD_PATH>/<SNAPSHOT>/`: GKE Pod Snapshots' archives
   can share `<OLD_PATH>`. The
   engine deletes an archive from the bucket that its work order recorded,
   as the engine controller, which the old bucket may not grant: the
   archives of PodSnapshots from before the switch can stay behind, and each
   is a memory dump.
7. Only then remove the nodes' access to the bucket the agent used before:
   for each role that `gcloud storage buckets get-iam-policy gs://<OLD_BUCKET>`
   shows for the nodes' service account, run
   `gcloud storage buckets remove-iam-policy-binding gs://<OLD_BUCKET> --member=serviceAccount:<NODE_SERVICE_ACCOUNT> --role=<ROLE> --all`.

## Archive signing

Anyone who can write to the checkpoint bucket could otherwise replace an
archive and its manifest together, or copy a genuine archive into another
snapshot's prefix, and have it restored as root on a node. So the node agent
signs each archive's manifest with a key that workloads can't read, and binds
the archive to the restore it belongs to (contract clause C10 R2, #244):

- `manifest.json` is a [DSSE](https://github.com/secure-systems-lab/dsse)
  envelope (payload type
  `application/vnd.gke-labs.pod-migration.criu-manifest.v1+json`). Its payload
  is the manifest, and its signature is an HMAC-SHA256 over DSSE's
  pre-authentication encoding of the payload, with the key that `keyid` names.
- The manifest carries the SHA-256 and size of `checkpoint.tar` and the
  SHA-256 of `spec.json`, and binds the archive to the snapshot, its
  namespace, the source pod's name and UID, the container, the bucket and
  path, the UID of the PodMigrationJob that controls the work order's
  trigger, and the cluster (the UID of its `kube-system` Namespace, which the
  agent reads at startup; it exits if it can't). It also records the trigger
  (name and UID), which the restore doesn't check: the job UID ties the
  archive to its job, and a job that creates its trigger again takes its own
  work order over (#262).
- An agent that can't sign fails a work order before it freezes the source:
  `Checkpoint=False`, reason `Failed`, with a message that says why
  (`archive signing key not configured on the node agent`, or that the agent
  doesn't know the cluster UID) and ends `the source container was not
touched`. The migration then falls back as for any failed checkpoint.
- The agent never commits an archive that no restore would accept. Right
  before it signs the manifest, it reads the work order and its trigger past
  the cache, and takes the job from the trigger's controller reference. A
  trigger or work order that was deleted, or is being deleted, cancels the
  checkpoint (`Cancelled`). A trigger created again under the same name that
  hasn't taken the work order over yet is read again for a few seconds,
  while the take-over lands (#262). If no job can be bound (the trigger has
  no PodMigrationJob controller, or the take-over never lands), the
  checkpoint fails before `manifest.json` is uploaded. At the commit point,
  the agent reads the trigger again by name: if another PodMigrationJob
  controls it by then, the checkpoint fails rather than commit. A trigger
  that the same job created again commits, taken over or not. Each failure
  or cancel thaws the source.
- The restoring agent downloads `manifest.json` first and verifies its
  signature with its keys. It then compares every binding with its own work
  order, with the PodMigrationJob that the replacement is assigned to
  (`pod-migration.gke.io/assigned-pmj`), and with its own cluster. Only then
  does it download `spec.json` and `checkpoint.tar`, which must match the
  signed digests. An envelope with more than 8 signatures is refused as
  malformed, before any signature is checked.

**Creating the key.** Create it when you install the engine (see
[Installing on a Test Cluster](#installing-on-a-test-cluster)), and before
you apply a `deploy/deploy.yaml` with archive signing to an existing install
(see Upgrading below):

```bash
kubectl create secret generic pod-migrate-archive-signing-keys \
  -n pod-migration-criu-system \
  --from-literal=k1="$(openssl rand -base64 48)"
```

Each entry of the Secret is one key. Its name is the key ID (letters, digits,
`.`, `_` and `-`, up to 63 characters, starting with a letter or digit), and
its value, without surrounding whitespace, is the key: at least 32 bytes. The
DaemonSet mounts the Secret read-only at `/etc/pod-migrate/archive-signing`
(`--archive-signing-keys-dir`), and the agent's pods don't start without it.
The agent signs with the key that `--archive-signing-key-id` names, or with
the only key when the flag is empty (the default), and verifies with every
key. It reads the keys once, at startup: restart the agents after you change
the Secret. Every agent of the cluster shares the keys, so any node can
restore a checkpoint from any other. Only the agent mounts the Secret; keep
`get` on Secrets in `pod-migration-criu-system` away from workloads and
tenants.

**Setting the signing key ID.** The agent's container in `deploy/deploy.yaml`
has no `command` or `args`, and the agent image's entrypoint is the agent
binary (`ENTRYPOINT ["/agent"]`), so arguments you add go to the agent. Set
the flag with a JSON patch, which rolls the DaemonSet (the `add` operation
replaces `args` if there are any, so include any other flags you pass), then
wait for the rollout:

```bash
kubectl -n pod-migration-criu-system patch daemonset pod-migrate-agent --type=json \
  -p='[{"op":"add","path":"/spec/template/spec/containers/0/args","value":["--archive-signing-key-id=k1"]}]'
kubectl -n pod-migration-criu-system rollout status ds/pod-migrate-agent
```

If the flag names a key that isn't in the Secret, the agent still verifies
archives with the keys it has, but it has none to sign with: it logs
`NO ACTIVE ARCHIVE SIGNING KEY` and fails every checkpoint before it freezes
the source (`archive signing key not configured on the node agent`). So does
an agent that starts with several keys and no `--archive-signing-key-id`.

**Rotating the key.** Since the agent reads the keys only at startup, do these
steps in order, and wait for
`kubectl -n pod-migration-criu-system rollout status ds/pod-migrate-agent` to
finish after each one:

1. Pin the current key: set `--archive-signing-key-id=k1` as above, which rolls
   the agents.
2. Add the new key and restart the agents. Each then holds both keys, and
   still signs with `k1`:
   ```bash
   kubectl -n pod-migration-criu-system patch secret pod-migrate-archive-signing-keys \
     --type=merge -p "{\"stringData\":{\"k2\":\"$(openssl rand -base64 48)\"}}"
   kubectl -n pod-migration-criu-system rollout restart ds/pod-migrate-agent
   ```
3. Switch the flag to the new key, which rolls the agents. Every agent already
   holds both keys, so restores keep working while it rolls. The `replace`
   operation overwrites the whole `args` list, so add any other flags you pass
   to the agent (such as `--api-group`) to `value`:
   ```bash
   kubectl -n pod-migration-criu-system patch daemonset pod-migrate-agent --type=json \
     -p='[{"op":"replace","path":"/spec/template/spec/containers/0/args","value":["--archive-signing-key-id=k2"]}]'
   ```
4. Once archives signed with `k1` have expired (their migrations have
   finished, or the bucket's lifecycle rule has deleted them), remove `k1`
   and restart the agents:
   ```bash
   kubectl -n pod-migration-criu-system patch secret pod-migrate-archive-signing-keys \
     --type=json -p='[{"op":"remove","path":"/data/k1"}]'
   kubectl -n pod-migration-criu-system rollout restart ds/pod-migrate-agent
   ```

**What a refused archive looks like.** The replacement starts cold. The agent
stamps `pod-migrate.io/psengine-restore: failed` and, in the same update,
`pod-migrate.io/psengine-restore-reason`, records a Warning `RestoreFailed`
event on the pod with the reason and the details (`kubectl describe pod`), and
the PodMigrationJob concludes `SucceededWithoutRestore` with the reason
`FallbackToColdStart` and a message such as
`CRIU snapshot restore failed; replacement fell back to cold start (annotation pod-migrate.io/psengine-restore=failed (reason ArchiveSignatureInvalid))`.
Neither the event nor the annotation names key IDs or cluster UIDs; the
agent's log does. The reasons:

| Reason                         | Meaning                                                                                                                                                                         |
| ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `ArchiveSigningKeyUnavailable` | The restoring agent has no archive signing keys (see its log).                                                                                                                  |
| `ArchiveSignatureInvalid`      | `manifest.json` is malformed, unsigned (for example written by an agent from before archive signing), signed with a key the agent doesn't hold, or its signature doesn't match. |
| `ArchiveBindingMismatch`       | A genuine archive, bound to another snapshot, pod, location, job or cluster.                                                                                                    |
| `ArchiveDigestMismatch`        | `spec.json` or `checkpoint.tar` doesn't match the digests in the signed manifest.                                                                                               |
| `RestoreFailed`                | Any other failure.                                                                                                                                                              |

**Upgrading.** An agent from before archive signing can't read a signed
manifest, and an agent with it refuses an unsigned one, so a migration whose
checkpoint and restore run on agents of different versions starts cold, and
so does every restore from an archive written before the upgrade. On an
existing install:

1. Let in-flight migrations finish, and don't drain CRIU nodes or start CRIU
   migrations until step 4 is done.
2. Create the `pod-migrate-archive-signing-keys` Secret (see Creating the key)
   before you apply the new `deploy/deploy.yaml`. The agent's pods don't start
   without it, so otherwise the rolling update replaces the agent on the first
   node with a pod that waits for the Secret, and stops there, leaving that
   node without an agent.
3. Apply the new `deploy/deploy.yaml`.
4. Wait for `kubectl -n pod-migration-criu-system rollout status ds/pod-migrate-agent`.

**What it doesn't cover.** Anyone who can read Secrets in
`pod-migration-criu-system`, or who compromises a node agent (each agent holds
the keys), can sign an archive. Signing doesn't stop anyone from reading an
archive, either: that takes a bucket that only the CRIU engine uses and that
grants object access to its two service accounts alone (see
[Storage credentials](#storage-credentials)). Even then, anyone who can get a
token for either of them (create pods, or workloads that create them, in
`pod-migration-criu-system`, exec into the agent's pods, or create
`serviceaccounts/token` for them), who has storage access through the
project, or who compromises a node, can still read archives, and delete or
corrupt them, which forces cold starts.

## Archive garbage collection

The orchestrator deletes a job's `PodSnapshot` once it's done with the
checkpoint: once the job is `Succeeded` (restore verified or inferred) or
`Failed`, after the controller's `--snapshot-retention` (#107, contract clause C9). The engine owns the
archive, so `cmd/engine` deletes it. The controller's orphan sweep also deletes
labelled `PodSnapshot`s whose job is gone (`--orphan-snapshot-sweep-interval`,
`--orphan-snapshot-min-age`), and the engine handles those the same way:

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
  It also never gets one that an earlier instance of the trigger left with an
  outcome: the engine controller deletes that one first (#262). So a bucket
  delete that keeps failing holds up the next migration of the same pod until
  the give-up bound, and that job may time out and fall back to a cold
  eviction. While it waits, the trigger has a `WaitingForWorkOrderDeletion`
  Normal event naming the work order.
- Each of these waits is recorded once per trigger, not on every 2 s or
  30 s look, and again every 10 min while it goes on, so it outlives the
  apiserver's event TTL (`--event-ttl`, 1 h by default). A different work
  order or user, or a trigger created again, records it again at once. The
  engine remembers what it recorded in memory, so a restart records a wait
  once more.
- The engine controller needs `update`, `patch` and `delete` on `podsnapshots`,
  and `list` on `podmigrationjobs` for the in-use check (in
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

## Restore placement

A CRIU checkpoint only restores on a node that is compatible with the one it
was taken on ([#257](https://github.com/gke-labs/pod-migration/issues/257)).
With the controller's `--restore-placement` (on by default), a replacement
that its ReplicaSet or StatefulSet would recreate is released with required
node affinity that only compatible nodes meet, and the controller's usual
placement deadline and cold-start fallback apply when none can take it (engine
contract C8, as for GKE Pod Snapshots). The controller reads the source node's
facts from the `PodSnapshot`'s `status.sourceNode`, and every node's facts
from labels that the node agent publishes on its own `Node`.

**The rule.** A node can restore the checkpoint when, for each fact known on
both nodes:

- the architecture is the same (the node's `kubernetes.io/arch` label, which
  the kubelet sets, against the agent's architecture recorded as `arch`);
- the CPU vendor (`vendor_id`) is the same;
- the CPU model name is the same. The agent's restore refuses a checkpoint
  from another model name (its `cpuFamily` guard), so placement does too;
- on x86, the node has every instruction flag of the source that CRIU itself
  compares, and exactly the source's XSAVE state flags (`avx`, `avx512f`,
  `ospke`, `amx_tile`);
- the kernel, CRIU, runc and containerd versions are the same or newer. The
  leading `major.minor.patch` is compared, so `6.6.72+` and `6.6.72-gke.1`
  are the same version.

A fact that is unknown on either node, or a version that doesn't parse, never
excludes a node: the source then adds no constraint for it, and a node
publishes it as `unknown` (versions as `1000000000`, newer than any real one).
The controller logs the source facts it couldn't use, at verbosity 1. A checkpoint taken by an
agent from before this change carries no `placementLabels` in its facts, and
gets no constraints, so a controller upgraded before the agents doesn't send
restores waiting for labels that no node has yet.

**Which CPU flags count.** The instruction flags are CRIU's own `ins` CPU
capability set (`x86_ins_capability_mask` in `criu/arch/x86/cpu.c`: the
user-visible instruction set extensions, such as `sse4_2`, `avx2`,
`avx512f`, `sha_ni` and `rtm`), under their `/proc/cpuinfo` names, plus
`xsaves` from CRIU's `fpu` check. `osxsave` is in CRIU's set too, but Linux
doesn't show it. CRIU's set predates some user-visible extensions, so these
are compared too: `fma`, `avx512ifma`, `avx512_bf16`, `avx512_fp16`,
`avx_vnni`, `amx_bf16`, `amx_int8`, `movdiri`, `movdir64b`, `serialize`,
`cldemote` and `waitpkg`. Everything else in the `flags` line isn't compared, as in
CRIU: the kernel's mitigation and bug flags (`pti`, `md_clear`, `ibrs`,
`ssbd`, ...), virtualization flags (`hypervisor`, `vmx`, ...), and timer and
power management flags (`constant_tsc`, `arat`, ...), which vary between
hosts and kernels without changing what the application can run. CRIU's `fpu`
check also requires the same XSAVE area size on both nodes, because glibc
caches it at startup, so the XSAVE state flags must match exactly rather than
as a superset. CRIU only applies these checks itself with `--cpu-cap`, which
runc doesn't pass, so placement is the only CPU check before the restore.
The list is in `internal/psengine/contract/node_placement.go`, with a copy in
the controller's `internal/criuplacement`; tests keep them equal. Off x86,
CPU features aren't compared (CRIU doesn't either).

**What the agent publishes.** The agent reads its node's facts once its
bootstrap has run (even a failed one), then every 10 minutes, and patches only what changed; a failed publish is retried after 30 seconds. So is a
publish with a fact still unknown (a version, the CPU features, or on x86 the
vendor or model), for example a containerd version read while containerd
restarts, until the facts are complete or 10 minutes of such retries have
passed. For
its own node, `cpuFeatures` is the intersection across processors. Labels
(all are always present, except the missing-flag ones):

| Label                                                                                                          | Value                                                                                                                        |
| -------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `criu.pod-migrate.io/cpu-vendor`                                                                               | `vendor_id`, or `unknown`                                                                                                    |
| `criu.pod-migrate.io/cpu-model`                                                                                | the first 16 hex digits of the SHA-256 of the model name, or `unknown`                                                       |
| `criu.pod-migrate.io/xstate`                                                                                   | the XSAVE state flags the node has, dot-separated (`avx.avx512f.ospke`), `none`, or `unknown` (features unknown, or not x86) |
| `criu.pod-migrate.io/kernel-version-code`, `criu-version-code`, `runc-version-code`, `containerd-version-code` | `major*1000000 + minor*1000 + patch`, or `1000000000` if unknown                                                             |
| `missing-cpu.criu.pod-migrate.io/<flag>`                                                                       | `true`, for each compared x86 instruction flag the node lacks; none when its features are unknown                            |

The controller requires `kubernetes.io/arch In [<source arch>]`,
`In [<source value>, unknown]` on the vendor, model
and XSAVE labels, `Gt <source code - 1>` on the version labels (none for a code of 0), and
`DoesNotExist` on the missing-flag label of each compared flag the source
had. Annotations `pod-migrate.io/criu-node-facts` (the facts as JSON, without
the CPU features) and `pod-migrate.io/criu-node-cpu-features` (the features,
space-separated) show the same facts to people. The agent removes
`criu.pod-migrate.io/*` and `missing-cpu.criu.pod-migrate.io/*` labels that
it no longer publishes, so don't put your own labels under those prefixes.
A node without the agent carries no version labels, so a restore with known
source versions isn't placed there; it can't restore a CRIU checkpoint
anyway. The engine's readiness on the target (`pod-migrate.io/criu-ready`) isn't
a constraint yet: a node whose agent reports `criu-ready=false` still meets
them, and its restore falls back to a cold start as before.

**Limits.** Node labels only exist once the agent has published them, so:

- During a rolling upgrade of the agent DaemonSet, a checkpoint taken by an
  upgraded agent excludes nodes whose agent hasn't published the labels yet,
  even if they could restore it. If no node fits within
  `--restore-placement-deadline`, the replacement starts cold, as for any
  placement timeout. Upgrade the agents before the window in which you
  drain nodes.
- A new node is `Ready` briefly before its agent has bootstrapped and
  published the labels. If `--restore-placement-deadline` expires in that
  window, the controller counts that unlabelled `Ready` node as a way out,
  a node the replacement could run on without the constraints, so it
  concludes `PlacementTimeout` and the recreated replacement starts cold.
- The cluster autoscaler builds a new node from its node pool's template,
  which doesn't carry these labels, so it doesn't scale a pool up for a
  replacement with these constraints. This changes today's behaviour: in a
  full pool, a replacement with these constraints now waits until the
  restore ceiling (the restore timeout, 5m30s with the default
  `--restore-stabilization-window`) and then starts cold, where without them
  it could be scheduled as soon as the autoscaler added a node. In a pool
  scaled to zero it waits the same way. The workaround is the controller's
  `--restore-placement=false`, which turns restore placement off for every
  engine, GKE Pod Snapshots included. The autoscaler may copy the labels
  from an existing node in a non-empty pool, in which case only pools scaled
  to zero are affected; the #257 live end-to-end run is to settle that, and
  should include a non-empty autoscaled pool.

**Not tested live yet.** The rule and the label format are unit-tested, and
the constraints are checked against a real API server (envtest); an
end-to-end run with an incompatible node, and with a non-empty autoscaled
pool (see Limits), is still to do.

## Rolling Node-Pool Upgrades & Kernel Compatibility

Rolling node-pool upgrades in GKE cordon and drain nodes one by one, triggering the `EvictionGate` to initiate live migrations for opted-in stateful pods (`pod-migration.gke.io/enabled: "true"`). During a rolling upgrade, pods checkpointed on an older node are restored onto an upgraded node that may run a newer Linux kernel, containerd, runc, or CRIU version.

### Version Compatibility Matrix & Guidelines

| Upgrade Type                   | Example Jump                               | Compatibility Status            | Operational Guidance & Behavior                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| ------------------------------ | ------------------------------------------ | ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **GKE Patch Upgrade**          | `1.34.1-gke.1000` &rarr; `1.34.2-gke.1100` | **Expected (untested)**         | Identical kernel major.minor, identical glibc/ABI. In-memory state and socket structures expected to restore with zero degradation.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| **GKE Minor Upgrade**          | `1.34.x` &rarr; `1.35.x`                   | **Expected (untested)**         | Newer kernel on destination node. Linux user-space ABI stability ensures task memory, threads, and file descriptors restore cleanly provided CPU features match.                                                                                                                                                                                                                                                                                                                                                                                                                               |
| **Minor Downgrade / Rollback** | `1.35.x` &rarr; `1.34.x`                   | **Placed away (untested live)** | With `--restore-placement` (on by default), a node with an older kernel, CRIU, runc or containerd than the source doesn't meet the restore's constraints ([Restore placement](#restore-placement)), so a ReplicaSet or StatefulSet replacement isn't restored there. With placement off, or for a replacement that placement doesn't constrain (a Job's or a bare pod), a failed restore falls back to a cold start with `RestoreCrashFallback`.                                                                                                                                               |
| **CPU Architecture Mismatch**  | Intel &rarr; AMD / mixed microarch         | **Placed (untested live)**      | Heterogeneous CPU instruction sets (e.g. missing AVX/AVX-512 flags) cause SIGILL on restore. With `--restore-placement` (on by default), the replacement only goes to a node with the same vendor and model and the source's instruction set ([Restore placement](#restore-placement), [#257](https://github.com/gke-labs/pod-migration/issues/257)); this applies to ReplicaSet and StatefulSet replacements only. If no node meets it, the replacement is recreated to start cold after `--restore-placement-deadline` when another node could take it, or at the restore timeout otherwise. |

### Prerequisites for Clean Upgrades

1. **Homogeneous Node Pools:** Node pools participating in rolling upgrades should share CPU model and instruction sets, and upgrade forward only. Otherwise placement finds no compatible node and the replacement starts cold ([Restore placement](#restore-placement)).
2. **Workload Opt-In & CRIU Routing:** Workloads must carry `pod-migration.gke.io/enabled: "true"`, the namespace must carry `pod-migration.gke.io/managed: "true"` (#303), and the namespace must configure a `PodMigration` routing workloads to CRIU (e.g. `pod-migration.gke.io/runtime-class-engines: "@default=criu"`).

### Running the Live Upgrade Verification Suite

The repository includes an automated verification script in `verification-suite/run_criu_nodepool_upgrade.sh` that deploys an in-memory stateful workload (Redis or monotonic counter), seeds unpersisted in-memory state, drives/monitors the node pool upgrade, and verifies zero restarts, verified restore outcomes, and data survival:

```bash
# Offline self-test (validates manifests, verifiers, and failure paths):
./verification-suite/run_criu_nodepool_upgrade.sh --self-test

# Live cluster rolling upgrade test:
./verification-suite/run_criu_nodepool_upgrade.sh \
  --cluster <CLUSTER> \
  --location <LOCATION> \
  --node-pool <NODE_POOL> \
  --target-version <TARGET_VERSION> \
  --workload redis

# Drain verification across nodes (use --allow-same-version if testing on identical node versions):
./verification-suite/run_criu_nodepool_upgrade.sh \
  --namespace pm-upgrade-test \
  --drain-node \
  --skip-upgrade \
  --allow-same-version
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

### End-to-end test on kind

CI runs real migrations on a local kind cluster
(`.github/workflows/criu-e2e.yaml`,
[#230](https://github.com/gke-labs/pod-migration/issues/230)). The job runs for
every pull request to `main`, so that it can be a required check, but it brings
up the cluster only when the pull request touches `engines/criu/`,
`controller/`, `verification-suite/` or `.github/workflows/`; otherwise it
passes within seconds. Pushes to `main` that touch those paths run it too, and
so does a manual run. The kind
workers run real containerd, runc and CRIU, and `fake-gcs-server` stands in
for the bucket. Two stateful pods, each in its own namespace, are drained to
the other worker: the first must be restored warm; the second holds a file on
an emptyDir, which doesn't move, so its restore must fail and its replacement
must start cold and serve.

A failed run isn't retried. Whether to rerun it is a maintainer's decision:
the job checks correctness, and a pass on a second try can hide a real race.
A failed run uploads the `criu-kind-e2e-logs` artifact (node journals, every
pod's log, the migration objects and events) and keeps it for 7 days.

To run it locally you need an x86-64 Linux host whose kernel passes
`criu check`, with Docker, kind v0.33.0 (the version CI pins), kubectl, make,
curl, jq and openssl. `up.sh` first deletes any kind cluster named `pm-e2e`, and
creates the archive signing key Secret with a random key before it applies
the engine's manifest (see [Archive signing](#archive-signing)). It pins the
kind node image, `fake-gcs-server`, the counter's busybox image and
cert-manager's manifest by digest or checksum (see the top of `up.sh` to change
one). It doesn't pin cert-manager's own images, which that manifest names by
tag, or most of what goes into the four images it builds (base images by tag,
packages from live archives), and CI runs on GitHub's `ubuntu-24.04` runner
image, kernel included, so a run can also fail on a tree that didn't change.
`run_criu_kind_e2e.sh` reads `BUCKET`, `CLUSTER` and `WORKLOAD_IMAGE` too:
export them for both scripts.

```bash
verification-suite/kind/up.sh
verification-suite/run_criu_kind_e2e.sh    # or name scenarios: warm-restore restore-failure
kind delete cluster --name pm-e2e && docker rm -f pm-e2e-gcs    # tear down
```

The engine and the agent use the GCS emulator named by
`STORAGE_EMULATOR_HOST` instead of GCS when it is set: `host:port`, as
Google's client libraries take it (plain HTTP), or a URL such as
`http://host:port`. They then send no credentials and log a `WARNING` line at
startup. `up.sh` sets it; the shipped manifests never do. It is for tests
only: never set it on a real cluster, where whoever runs the emulator could
read, delete or corrupt the checkpoint archives.

## Installing on a Test Cluster

Apply the `criu.podmigration.gke.io` CRDs, create the archive signing key
(see [Archive signing](#archive-signing)), then deploy the engine, agent, and
webhook. On a fresh install, the order of the last two doesn't matter, since
the agent's pods wait for the key. On an existing install, create the key
first (see Upgrading under [Archive signing](#archive-signing)). Set up a
bucket for the CRIU engine alone, which grants the engine's two service
accounts and nobody else, and set `WORKLOAD_POOL` and `IDENTITY_PROVIDER` to
your cluster's values (see [Storage credentials](#storage-credentials)):

```bash
./patches/setup-criu-storage.sh <PROJECT_ID> <BUCKET_NAME> <REGION>
WORKLOAD_POOL="<PROJECT_ID>.svc.id.goog"
IDENTITY_PROVIDER="https://container.googleapis.com/v1/projects/<PROJECT_ID>/locations/<LOCATION>/clusters/<CLUSTER>"
kubectl apply -f engines/criu/deploy/crds.yaml
kubectl create namespace pod-migration-criu-system --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic pod-migrate-archive-signing-keys \
  -n pod-migration-criu-system \
  --from-literal=k1="$(openssl rand -base64 48)"
sed -e "s|<YOUR_CRIU_ENGINE_IMAGE>|${ENGINE_IMG}|g" \
    -e "s|<YOUR_CRIU_AGENT_IMAGE>|${AGENT_IMG}|g" \
    -e "s|<YOUR_CRIU_HOLDER_IMAGE>|${HOLDER_IMG}|g" \
    -e "s|<YOUR_WORKLOAD_POOL>|${WORKLOAD_POOL}|g" \
    -e "s|<YOUR_IDENTITY_PROVIDER>|${IDENTITY_PROVIDER}|g" \
    engines/criu/deploy/deploy.yaml | kubectl apply -f -
kubectl -n pod-migration-criu-system rollout status ds/pod-migrate-agent
```

Then point the PodMigration of each namespace that only CRIU serves at that
bucket (`spec.storage.location: gs://<BUCKET_NAME>/<namespace>/snapshots`
or another path starting with `<namespace>/`).

When creating `PodSnapshotStorageConfig` and `PodSnapshotPolicy` resources for
this engine, label them with `pod-migrate.io/engine: criu` so `cmd/engine`
reconciles them.

## Licensing and Source Code Availability

The CRIU snapshot engine is licensed under Apache-2.0. The bundled CRIU binary (`/opt/criu-bundle/criu.bin`) and certain shared libraries are licensed under GPL-2.0 and LGPL-2.1 and invoked strictly as an external subprocess.

- **Image Inventory & Third-Party Notices**: See [docs/third-party-licenses.md](../../docs/third-party-licenses.md).
- **GPL Source Packaging & Release Process**: See [docs/release-process.md](../../docs/release-process.md) and `tools/release/package_gpl_sources.sh`.
