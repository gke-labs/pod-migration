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
> - **Known follow-ups tracked in [#129](https://github.com/gke-labs/pod-migration/issues/129):**
>   - `cmd/holder`: downward-API volume propagation lag after gate removal can
>     exceed the initial 10s `ps-name` poll window and trigger a cold-start
>     fallback before the annotation projects into `/pminfo/annotations`.
>   - `internal/psengine/restore_node.go`: `fetch()` caps its in-memory return
>     buffer at 4 MiB (`manifest.json` / `spec.json`).
>   - `internal/node/capability.go`: `Bootstrap` installs `/usr/local/sbin/criu`
>     and `/etc/criu/runc.conf` on the host node; removing the DaemonSet does
>     not automatically uninstall those host files.

`engines/criu` is a standalone snapshot engine for the `gke-labs/pod-migration`
controller that implements the `podsnapshot.gke.io/v1` CustomResource contract
for single-container `runc` pods using [CRIU](https://criu.org/).

## Architecture

The CRIU snapshot engine consists of three binaries built from this Go module
(`github.com/gke-labs/pod-migration/engines/criu`):

1. **`cmd/engine` (cluster controller + mutating webhook):**
   - Runs a startup preflight (`psengine.VerifyCRDOwnership`) that refuses to
     start if the `podsnapshot.gke.io` CRDs are missing or owned by the managed
     GKE Pod Snapshots addon.
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
   - Reconciles `PodSnapshot` work orders scheduled on its node
     (`psengine.NodeSnapshotReconciler`): drives the kubelet
     `ContainerCheckpoint` API, captures the source container's OCI spec,
     uploads `spec.json`, `checkpoint.tar`, and `manifest.json` to GCS with
     MD5/size verification, stops the source container via containerd (`SIGKILL`),
     and marks the `PodSnapshot` `Ready=True`.
   - Reconciles replacement pods on its node carrying
     `podsnapshot.gke.io/ps-name` (`psengine.NodeRestoreReconciler`): downloads
     and SHA-256 verifies the archive from GCS, checks CPU family compatibility,
     prepares an overlayfs rootfs with `rootfs-diff.tar`, rewrites the OCI spec
     (`node.AdaptSpec`), restores the process tree as a sibling `runc` container
     joining the holder's network/IPC/UTS namespaces, and stamps
     `pod-migrate.io/psengine-restore: restored` (or `failed`).
3. **`cmd/holder` (static scratch binary):**
   - Seeded into replacement pods via an `initContainer` (`/holder seed`).
   - Wraps the replacement container's entrypoint (`/holder wrap`): reads
     `podsnapshot.gke.io/ps-name` and `pod-migrate.io/psengine-restore` from a
     downward-API volume, holding the pod sandbox alive when `restored` or
     falling through to `exec` the original command on restore failure/timeout.

## Node & Cluster Requirements

- **Addon-off test clusters only (v1):** Apply `deploy/crds.yaml` only on
  clusters where the managed GKE Pod Snapshots addon is disabled. The engine's
  startup preflight refuses to run if foreign-owned `podsnapshot.gke.io` CRDs
  are present.
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
