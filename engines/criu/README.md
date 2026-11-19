# CRIU Snapshot Engine (`engines/criu`)

> [!WARNING]
> **EXPERIMENTAL — FOR ADDON-OFF TEST CLUSTERS ONLY ([#129](https://github.com/gke-labs/pod-migration/issues/129)).**
> `engines/criu` runs a privileged node DaemonSet (`hostPID`, `hostNetwork`,
> host containerd/kubelet mounts) and shares the `podsnapshot.gke.io` CRD group
> with the managed GKE Pod Snapshots addon. Never apply `deploy/crds.yaml` or
> `deploy/deploy.yaml` to a production cluster or to any cluster that has the
> managed GKE Pod Snapshots addon enabled.
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
>   with `pod-migration.gke.io/restore-status` and trigger Kubelet container checkpoints.
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
(`podsnapshot.gke.io/v1` by default; see [API group](#api-group)) for
single-container `runc` pods using [CRIU](https://criu.org/).

## Architecture

The CRIU snapshot engine consists of three binaries built from this Go module
(`github.com/gke-labs/pod-migration/engines/criu`):

1. **`cmd/engine` (cluster controller + mutating webhook):**
   - Runs a startup preflight (`psengine.Preflight`) before it serves
     anything. It refuses to start if the contract CRDs of its API group are
     missing or owned by another provider (on the default group, the managed
     GKE Pod Snapshots addon). On another group with the restore webhook on,
     it also refuses to start next to the addon.
   - Reconciles `PodSnapshotPolicy` objects labeled
     `pod-migrate.io/engine: criu`, probing the referenced
     `PodSnapshotStorageConfig` GCS bucket and publishing `Ready=True` +
     `ScopeSingleContainer=True`.
   - Reconciles `PodSnapshotManualTrigger` objects for pods matching a `Ready`
     CRIU policy, materializing a `PodSnapshot` work order and setting
     `status.snapshotCreated.name`.
   - Serves `/psengine-restore-inject` to speculatively inject the `holder`
     initContainer and command wrapper into gated replacement pods at `CREATE`.
2. **`cmd/agent` (privileged node DaemonSet):**
   - At startup (`node.Bootstrap`), installs a self-contained CRIU >= 3.19
     bundle into `/var/lib/pod-migrate/criu-bundle` and `/usr/local/sbin/criu`,
     writes `/etc/criu/runc.conf`, runs `criu check` in the host namespaces via
     `nsenter`, and stamps `pod-migrate.io/criu-ready` and
     `pod-migrate.io/criu-version` on the `Node`.
     If containerd cached a failed CRIU lookup (e.g. before criu was on its PATH),
     the agent marks the node `pod-migrate.io/criu-ready: false` with reason and
     remediation annotations (`pod-migrate.io/criu-reason` and `pod-migrate.io/criu-remediation`).
     The agent mounts `/run/containerd` and probes containerd's socket every 10s
     while the node is unready; once containerd restarts, the agent detects the
     new socket inode, re-runs `Bootstrap` (`criu check` against the host kernel),
     and restores `pod-migrate.io/criu-ready: true` (verifying containerd's own
     `PATH` lookup remains tracked in `#130`).
   - Reconciles `PodSnapshot` work orders scheduled on its node
     (`psengine.NodeSnapshotReconciler`): drives the kubelet
     `ContainerCheckpoint` API, captures the source container's OCI spec,
     uploads `spec.json`, `checkpoint.tar`, and `manifest.json` to GCS with
     MD5/size verification, stops the source container via containerd (`SIGKILL`),
     and marks the `PodSnapshot` `Ready=True`.
     If the `PodSnapshotManualTrigger` is deleted first, the engine controller
     sets `Checkpoint=Unknown` with the reason `CancelRequested` (on a work
     order without a bucket, which no agent acts on, it records `Cancelled`
     itself), and the agent
     also reads the trigger itself before the checkpoint and right before the
     stop. A cancelled agent starts nothing new. It waits for an in-flight
     kubelet checkpoint to return, because cancelling the call doesn't end the
     freeze. It aborts uploads and doesn't stop the source container. It then
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
default, `podsnapshot.gke.io/ps-name`.

The default group is `podsnapshot.gke.io`, the GKE Pod Snapshots addon's
group. Another group doesn't yet change where the engine can run: the
pod-migration controller still uses only `podsnapshot.gke.io` objects and the
`podsnapshot.gke.io/ps-name` restore order, and the restore webhook rewires
every gated replacement pod, including gVisor pods that the addon restores.
So on another group with the webhook on, the engine refuses to start if the
addon's CRDs are present. Keep the default, on addon-off clusters only, until
the controller can target the engine's group
([#135](https://github.com/gke-labs/pod-migration/issues/135)).
To test another group, change it in `deploy/crds.yaml` (each CRD's name and
`spec.group`) and in the `deploy/deploy.yaml` RBAC rules of both the engine
and the agent (the agent's rules include `get` on `podsnapshotmanualtriggers`),
pass the same `--api-group` to both binaries, and use a holder image from this
release or later (older holders reject `--ps-name-annotation`). Change the
group only when no migrations are in flight, and roll the engine and the agent
together: each wrapped replacement pod has its restore key built into its
command. With `--enable-webhooks=false`, replacement pods aren't wrapped, so
they start cold instead of being restored; also delete the
`pod-migrate-psengine-restore-inject` MutatingWebhookConfiguration then.

## Node & Cluster Requirements

- **Addon-off test clusters only (v1):** Apply `deploy/crds.yaml` only on
  clusters where the managed GKE Pod Snapshots addon is disabled. On the
  default group, the engine's startup preflight refuses to run if
  foreign-owned `podsnapshot.gke.io` CRDs are present; on another group it
  refuses only while the restore webhook is on (the default), since that
  webhook would also rewrite the gVisor pods the addon restores.
- **Linux kernel & runtime:** Nodes running `containerd` 2.x + `runc` with
  kubelet `ContainerCheckpoint` enabled and a kernel passing `criu check`.
- **Workload scope (v1):**
  - Single-container `runc` pods (`len(pod.Spec.Containers) == 1`).
  - Explicit `command` in the pod spec (required so the `holder` wrapper can
    fall through to a cold start if restore fails).
  - Homogeneous CPU family between source and target nodes.

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

## Installing on an Addon-Off Test Cluster

Use `kubectl create` (not `kubectl apply`) for `crds.yaml` so installation
fails immediately if the managed GKE Pod Snapshots addon CRDs already exist on
the cluster:

```bash
kubectl create -f engines/criu/deploy/crds.yaml
sed -e "s|<YOUR_CRIU_ENGINE_IMAGE>|${ENGINE_IMG}|g" \
    -e "s|<YOUR_CRIU_AGENT_IMAGE>|${AGENT_IMG}|g" \
    -e "s|<YOUR_CRIU_HOLDER_IMAGE>|${HOLDER_IMG}|g" \
    engines/criu/deploy/deploy.yaml | kubectl apply -f -
```

When creating `PodSnapshotStorageConfig` and `PodSnapshotPolicy` resources for
this engine, label them with `pod-migrate.io/engine: criu` so `cmd/engine`
reconciles them.
