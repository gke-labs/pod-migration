# Threat Model and Security Guidance for the CRIU Engine

This document defines the threat model, security posture, privilege boundaries, and operational guidance for the CRIU snapshot engine (`engines/criu`) within the `pod-migration` framework.

Security teams and cluster administrators should review these specifications prior to deploying the CRIU snapshot engine to ensure appropriate node posture, storage controls, and IAM safeguards are in place.

---

## 1. System Architecture & Trust Boundaries

The CRIU snapshot engine enables live process checkpoint and restore for `runc` workloads on Kubernetes. It is composed of three components with distinct trust boundaries:

```
+---------------------------------------------------------------------------------------+
| Kubernetes Control Plane                                                              |
|                                                                                       |
|   +--------------------------+          +-----------------------------------------+   |
|   | pod-migration controller |          | Mutating Webhook (/psengine-restore)    |   |
|   |  - Orchestrates PMJs     |          |  - Gated replacement pod injection      |   |
|   +--------------------------+          +-----------------------------------------+   |
+---------------------------------------------------------------------------------------+
                                           |
                                           v
+---------------------------------------------------------------------------------------+
| GKE Worker Node                                                                       |
|                                                                                       |
|   +-------------------------------------------------------------------------------+   |
|   | pod-migrate-agent (DaemonSet)                                                 |   |
|   |   - Privileged host daemon: hostPID=true, hostNetwork=true                    |   |
|   |   - Interacts with kubelet ContainerCheckpoint API and local containerd       |   |
|   |   - Installs CRIU bundle: /var/lib/pod-migrate/criu-bundle/ & /usr/local/sbin |   |
|   |   - Executes runc restore into replacement pod sandbox namespaces             |   |
|   +-------------------------------------------------------------------------------+   |
|                                           |                                           |
|                                           v                                           |
|   +---------------------------------------+---------------------------------------+   |
| Target Pod Sandbox                    | Cloud Object Storage (GCS)            |
|   - holder container (pause sandbox)  |   - Raw checkpoint archives (unencrypted) |
|   - Sibling restored container (runc) |   - Full process memory & metadata    |
+---------------------------------------+---------------------------------------+
+---------------------------------------------------------------------------------------+
```

### Trust Boundary Definitions

1. **Control Plane / API Server**: The primary authority for scheduling, admission validation, and policy orchestration. All restore orders originate from controller-managed `PodMigrationJob` (PMJ) resources.
2. **Node Agent (`pod-migrate-agent`)**: A highly privileged daemon on each node. It has root-equivalent access to the node host OS to freeze container cgroups, manage network namespaces, and execute `runc` in host namespaces.
3. **Target Pod Sandbox**: Sandboxed user workloads running in unprivileged container environments. Workloads must not be able to forge restore orders or escalate privileges during process restoration.
4. **Archive Storage (GCS)**: External repository storing checkpoint archives. Because checkpoint archives contain complete raw memory dumps of user processes (including tokens, TLS keys, and database credentials), the storage backend is classified as **Secret-equivalent**.

---

## 2. Privileged Node Agent Analysis & Blast Radius

The node agent (`pod-migrate-agent`) runs as a Kubernetes `DaemonSet` in the `pod-migration-criu-system` namespace.

### Host Privileges & Justifications

| Configuration                | Setting | Operational Justification                                                                                                                                                                           |
| ---------------------------- | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `securityContext.privileged` | `true`  | Required to invoke `runc` across container namespaces via `nsenter`, manage kernel cgroup hierarchies (`/sys/fs/cgroup`), configure network namespace netfilter rules, and manage overlayfs mounts. |
| `hostPID`                    | `true`  | Required to resolve holder container process IDs (`/proc/<pid>/ns/*`) to target container namespaces during sibling restoration, and to inspect containerd process PID for socket reconnection.     |
| `hostNetwork`                | `true`  | Required to communicate with the node-local kubelet checkpoint API (`http://127.0.0.1:10250` or loopback socket) and manipulate netns rules.                                                        |

### Host Mounts Inventory

The agent mounts host filesystem paths to bridge Kubernetes and container runtime operations:

| Host Path                      | Container Mount Path           | Mount Options                     | Purpose                                                                                                                                          |
| ------------------------------ | ------------------------------ | --------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `/var/lib/pod-migrate`         | `/var/lib/pod-migrate`         | `mountPropagation: Bidirectional` | Houses the CRIU runtime bundle, bundle rootfs directories, runc state, and overlayfs mounts that must persist across container re-execs.         |
| `/run/containerd`              | `/run/containerd`              | `type: Directory`                 | Access to the `containerd.sock` gRPC socket to pause (`Task.Pause`), resume (`Task.Resume`), and inspect pod sandboxes.                          |
| `/var/lib/containerd`          | `/var/lib/containerd`          | `type: Directory`                 | Access to local containerd content stores and snapshot metadata.                                                                                 |
| `/var/lib/kubelet/checkpoints` | `/var/lib/kubelet/checkpoints` | `type: DirectoryOrCreate`         | Staging directory where the local kubelet writes container checkpoint tarballs.                                                                  |
| `/var/lib/kubelet/pods`        | `/var/lib/kubelet/pods`        | `readOnly: true`                  | Reads downward-API volume projections and annotations for target pods.                                                                           |
| `/etc/criu`                    | `/etc/criu`                    | `type: DirectoryOrCreate`         | Hosts global CRIU configuration (`runc.conf`) containing default flags (`tcp-close`, `skip-in-flight`, `file-validation buildid`, `file-locks`). |
| `/usr/local/sbin`              | `/usr/local/sbin`              | `type: DirectoryOrCreate`         | Target directory where the agent installs the atomic `/usr/local/sbin/criu` wrapper script so containerd/runc can resolve `criu` on system PATH. |

### Blast Radius & Mitigations

- **Compromise Impact**: If the `pod-migrate-agent` container is compromised, the attacker achieves root on the host node and access to local containerd sockets.
- **Cluster RBAC Scope**: The agent uses a ClusterRole (`pod-migrate-psengine-agent`) granting cluster-wide permissions:
  - `nodes/checkpoint` (`create`): Triggers kubelet container checkpoints (cluster-wide scope in Kubernetes RBAC).
  - `nodes` (`get`, `patch`): Publishes readiness and capability annotations (`pod-migrate.io/criu-ready`, `pod-migrate.io/criu-version`).
  - `pods` (`get`, `list`, `watch`, `update`, `patch`): Tracks replacement pod lifecycle and stamps `pod-migrate.io/psengine-restore`.
  - `podsnapshots` (`get`, `list`, `watch`) and `podsnapshots/status` (`get`, `update`, `patch`): Read-only work order inspection and status reporting.
  - `podmigrationjobs` (`get`, `list`, `watch`): Validates active migration job bindings.
  - `podsnapshotmanualtriggers` (`get`): Verifies cancellation state.
  - `events` (`create`, `patch`): Emits node restore and failure warning events.
    _Note on RBAC Breadth_: Because Kubernetes RBAC cannot scope `nodes/checkpoint`, `nodes/patch`, or `pods/update` to the DaemonSet's local node, a compromised agent holds broad cluster-wide write privileges against these resources.
- **Engine Controller RBAC Scope (`pod-migrate-psengine-controller`)**: The cluster-level `cmd/engine` controller and `/psengine-restore-inject` webhook run as non-root (`65532:65532`) with narrower RBAC:
  - `podmigrationjobs` (`get` in `podmigration.gke.io`): Single live API `GET` in `RestoreInjector.Handle` (after in-memory pod-shape filtering) to confirm `pod-migration.gke.io/engine: criu` on the assigned `PodMigrationJob` before injecting the holder wrapper (`list`/`watch` are not granted).
  - `customresourcedefinitions` (`get` in `apiextensions.k8s.io`): Startup `VerifyCRDOwnership` preflight check for the configured `--api-group`.
  - `podsnapshotstorageconfigs`, `podsnapshotpolicies`, `podsnapshotmanualtriggers`, `podsnapshots` (`get`, `list`, `watch`, `create`) and `*/status` (`get`, `update`, `patch`) in `criu.podmigration.gke.io` (by default, #135).
  - `podsnapshots` (`update`, `patch`) in `criu.podmigration.gke.io`: adds and removes the `pod-migrate.io/archive-cleanup` finalizer (#107).
  - `pods`, `nodes` (`get`, `list`, `watch`) and `events` (`create`, `patch`).
- **Network Ingress & Health Probes**: The agent listens on host TCP port `:8091` (`hostNetwork: true`) for kubelet liveness (`/healthz`) and readiness (`/readyz`) probes. It does not expose other network services; all other traffic is outbound to the Kubernetes API server, containerd unix sockets, or cloud storage APIs.
- **Resource Constraints**: Default resource requests (`100m` CPU, `128Mi` memory) and memory limits (`512Mi`) prevent memory exhaustion from runaway agent operations.

---

## 3. Restore Path Security & Privilege Boundaries

A central risk in process checkpoint and restore is **unauthorized privilege gain**: restoring an unprivileged container into a higher-privileged configuration, or an archive injecting malicious mounts, devices, or capabilities into a destination host. Process restoration is governed by strict boundaries (tracked in [#129](https://github.com/gke-labs/pod-migration/issues/129)):

### R1. Authorization & Admission Validation

- **Work Order & PMJ Verification**: At `CREATE`, `/psengine-restore-inject` (`RestoreInjector`) injects the holder wrapper only into gated single-container `runc`/default pods (outside `kube-system`, `pod-migration-system`, and `pod-migration-criu-system`) whose assigned `PodMigrationJob` (`get` on `podmigrationjobs`) carries `pod-migration.gke.io/engine: criu` (#135). At restore time, the node agent restores a pod only after verifying that the target pod carries a restore order annotation (`<group>/ps-name`) matching an authorized work order whose referenced `PodMigrationJob` is either actively `Restoring` or `Succeeded` (warm-retry fallback window).
- **Admission Boundaries at `CREATE`**: The replacement webhook (`PodGateInjector`) only intercepts `CREATE` requests. For Kubeflow and third-party orchestration interop, it intentionally preserves pre-existing, non-empty `podsnapshot.gke.io/ps-name` annotations when no matching PMJ is found, rather than unconditionally stripping them.
- **Admission Boundaries on `UPDATE` (#233)**: Two `ValidatingAdmissionPolicy` objects block tampering with restore annotations on existing pods. The API server evaluates them itself, so they don't depend on a webhook being up, and their `matchConditions` skip every update that leaves the protected annotations as they are. They cover updates of a pod and of its `status` subresource, since a status write can change annotations too.
  - `pod-migration-restore-order` (in `controller/deploy.yaml`): on a pod opted into migration (`pod-migration.gke.io/enabled: "true"` before or after the update), only the controller's service account may set, change or remove a restore order (`<group>/ps-name`, for any engine's group).
  - `pod-migrate-psengine-restore-status` (in `engines/criu/deploy/deploy.yaml`): only the node agent's service account may record a restore outcome (`pod-migrate.io/psengine-restore`), only once, and only as `restored` or `failed`. Nobody may change or remove it afterwards.
- **Engine Annotations at `CREATE`**: On a pod opted into migration, the replacement webhook (`PodGateInjector`) removes `pod-migrate.io/psengine-restore` and `pod-migrate.io/psengine-injected` that the pod's template carries, so a pod can't be created with an outcome or a holder marker of its own.
- **Remaining Gaps**:
  - A pod that opts out (its opt-in label is removed, which stays allowed) leaves the scope of `pod-migration-restore-order`. A restore order set on it afterwards is a restore outside a migration, and it stays on the pod if the label is added back later: that last update doesn't change the order, so the policy doesn't see it. The node agent's work order check above still applies to it.
  - The policies see updates of a pod and of its `status`. They don't see a `pods/binding` request, which can also set annotations on a pod when it binds the pod to a node. The built-in `edit` and `admin` roles don't grant `pods/binding`; keep it to schedulers.
  - Whoever may change or delete a `ValidatingAdmissionPolicyBinding` (a cluster admin) can lift a policy. That is also the way to edit these annotations by hand in an emergency: set the binding's `validationActions` to `Audit`, and back to `Deny` afterwards.
  - Each policy names the one service account it allows, namespace included. An install that renames either must change the policy too.

### R2. Archive Integrity vs. Provenance (Integrity Only; see #129 R2)

- **Integrity Against Corruption**: Checkpoint archives in GCS (`manifest.json`, `spec.json`, `checkpoint.tar`) are validated against SHA-256 and MD5 hashes before unpacking.
- **No Provenance or Signatures Today**: SHA-256 and MD5 checks provide integrity against transmission or storage corruption, **not cryptographic provenance**. Archives are not cryptographically signed or MAC'd; an attacker who can write to the GCS bucket can replace the archive and update the digests.
- **Archive Size Bounds**: Strict caps prevent denial-of-service via resource exhaustion:
  - Metadata files (`manifest.json`, `spec.json`): Capped at 4 MiB.
  - Archive payload (`checkpoint.tar`): Capped at 8 GiB.
- **Safe Unpacking**: The filesystem layer (`rootfs-diff.tar`) is applied strictly via containerd's `archive.Apply` into a mounted, isolated snapshot root. Unpacking rejects directory traversal (`..`), path escapes, and tar-slip attempts (regular files and dirs only; sparse files rejected).

### R3. OCI Spec Adaptation & Privilege Boundaries (Partial — see #129 R2/R5)

When restoring a process, security posture is governed at two layers:

1. **OCI Runtime Spec (`node.AdaptSpec`)**: When `node.AdaptSpec` constructs the OCI runtime bundle (`config.json`) for the restored container, security-relevant fields are copied directly from the kubelet-generated destination container spec:
   - `Capabilities` (`CapEff`, `CapPrm`, `CapBnd`, `CapInh`, `CapAmb`)
   - `User` (`UID`, `GID`, `AdditionalGids`)
   - `NoNewPrivileges`
   - `Seccomp` profile configuration
   - `AppArmor` / `SELinux` process labels
   - `MaskedPaths` and `ReadonlyPaths`
   - Container device rules (`Devices`)
   - Linux namespaces (`IPC`, `Network`, `UTS`, `PID`, `Cgroup`)
   - Volume mounts (restricted to destination pod's declared volumes)
2. **CRIU Process State Inherent Privileges**: While `AdaptSpec` governs the OCI container spec, CRIU restores process credentials, capabilities, and seccomp state directly from the checkpoint image files (`core-*.img`). Nothing currently compares in-image process credentials against destination constraints. Consequently, as warned in the project banner: **anyone who can write to the checkpoint GCS bucket (including workloads on nodes sharing the default GCE service account) can get root on a restoring node until R2 (provenance/signatures) and R5 (isolated storage credentials) land**.

- **Pending Host Network Refusal (#229)**: Refusal of pods declaring `hostNetwork: true` across all CRIU boundaries (`PSMTReconciler`, `injectHolder`, and `NodeRestoreReconciler.restore`) is implemented in [#229](https://github.com/gke-labs/pod-migration/pull/229) (pending merge).

### R4. Node Posture & Containerd CVE Defense

Installing `criu` on a node's PATH can interact with container runtime vulnerabilities. Specifically:

- **GHSA-p7v4-vr35-mj6f (CVE-2026-95837)**: Containerd versions `>=2.1.0, <2.2.7` and `>=2.3.0, <2.3.4` contained a critical vulnerability where containerd implicitly restored checkpoint images via `CreateContainer` without validating destination security policies.
- **Enforced Minimums**: The agent inspects `nodeObj.Status.NodeInfo.ContainerRuntimeVersion` at startup (`CheckContainerdVersion`). It refuses to mark the node ready and refuses to install CRIU if containerd is within the vulnerable range:
  - Containerd `>= 2.2.7`, `>= 2.3.4`, or `2.0.x` is strictly required.
  - If an unsupported or vulnerable version is detected, the agent annotates the node with `pod-migrate.io/criu-ready: false`, records the reason, and provides an upgrade remediation message.
- **Operator Warning on Experimental Restore Flag**: On containerd versions where `enable_experimental_restore_via_create = true` is configured in `/etc/containerd/config.toml`, containerd permits untrusted callers to trigger restore on container creation. Cluster administrators must ensure `enable_experimental_restore_via_create` is disabled (set to `false` or omitted).

---

## 4. Archive Storage, IAM, and Data Protection

### Checkpoint Data Sensitivity

When CRIU dumps a container, the resulting archive (`checkpoint.tar`) contains a bit-for-bit snapshot of the application's address space. This includes:

- Plaintext database connection strings and credentials.
- Private encryption keys, TLS certificates, and API tokens loaded in heap memory.
- In-flight session data and customer personally identifiable information (PII).

**Security Classification**: All checkpoint archives in GCS must be treated with the **same classification as Kubernetes Secrets and cryptographic keys**.

### Cloud Storage IAM Hardening & Workload Identity Limitation

To prevent unauthorized access or tampering with checkpoint archives:

1. **Workload Identity hostNetwork Limitation & Node SA Exposure**:
   - The node agent runs with `hostNetwork: true` to communicate with the local kubelet and manage network namespaces.
   - **Workload Identity does not apply to `hostNetwork: true` pods** on GKE (they bypass the GKE metadata server's token interception and receive credentials from the underlying node's GCE service account).
   - Because the agent uses the node's GCE service account, **"workloads must never get bucket access" is not achievable today on nodes where general workloads share the default node service account**.
   - Until R5 (#129) lands (dedicated secret-mounted or sidecar storage credentials), clusters deploying the CRIU engine must run CRIU nodes in dedicated node pools using a dedicated GSA, isolated from untrusted workloads.
2. **Bucket Permissions & Isolation**:
   - Checkpointing nodes require `roles/storage.objectCreator` and `roles/storage.objectViewer`.
   - Restoring nodes require `roles/storage.objectViewer`.
   - The engine controller (`cmd/engine`) needs `storage.objects.create`, `storage.objects.get` and `storage.objects.delete`, for its storage probe and for archive garbage collection (#107). Without delete, archives are left behind, reported only by `ArchiveDeleteFailed` and `ArchiveDeleteAbandoned` events.
   - Avoid Default Compute Engine SA: In GKE, ensure nodes do not use the default Compute Engine service account (`<project-number>-compute@developer.gserviceaccount.com`), which often carries broad Project Editor permissions.
   - Enable Uniform Bucket-Level Access (UBLA) on the checkpoint GCS bucket to prevent ACL-based permission leakage.

### Encryption & CMEK

- **Default Encryption**: GCS encrypts all data at rest using Google-managed encryption keys.
- **Customer-Managed Encryption Keys (CMEK)**: For enterprise deployments, configure Cloud KMS CMEK on the checkpoint bucket. This allows security teams to:
  - Revoke key access instantly in the event of an incident or node compromise.
  - Audit all cryptographic key operations via Cloud KMS audit logs.

### Lifecycle Management & Retention Policy

Checkpoint archives are transient migration artifacts. They should not persist longer than needed to complete the migration.

The engine deletes a job's archive when the controller deletes its `PodSnapshot` (#107, see "Archive garbage collection" in `engines/criu/README.md`). Keep the lifecycle rule as a backstop for archives it leaves behind (`ArchiveDeleteAbandoned`; unlabelled `PodSnapshot`s and those of engines no longer registered, which the controller's orphan sweep never deletes; and clusters that run with `--orphan-snapshot-sweep-interval=0`).

- Configure GCS Object Lifecycle Management rules on the bucket:
  ```json
  {
    "rule": [
      {
        "action": { "type": "Delete" },
        "condition": { "age": 7 }
      }
    ]
  }
  ```
- A retention window of **1 to 7 days** is recommended. This bounds storage costs and automatically purges old memory dumps.

---

## 5. What Installing the Engine Changes on a Node

When `pod-migrate-agent` starts on a node, it applies the following modifications to the host environment:

| Modification            | Host Location                       | Rollback / Cleanup                                                                                                                                  |
| ----------------------- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CRIU Bundle**         | `/var/lib/pod-migrate/criu-bundle/` | Directory containing self-contained `criu.bin`, bundled shared libraries, dynamic linker, and `VERSION` file. Manually removable when uninstalling. |
| **CRIU System Wrapper** | `/usr/local/sbin/criu`              | Executable wrapper script that launches CRIU via the bundled dynamic linker. Overwritten atomically.                                                |
| **Runc Configuration**  | `/etc/criu/runc.conf`               | CRIU global configuration specifying `tcp-close`, `skip-in-flight`, `file-validation buildid`, and `file-locks`.                                    |
| **Node Annotations**    | Kubernetes `Node` metadata          | Stamped with `pod-migrate.io/criu-ready` (`true` or `false`) and `pod-migrate.io/criu-version`. Stale annotations cleared on demotion.              |
| **Socket Watching**     | `/run/containerd/containerd.sock`   | Actively monitored for inode changes to detect containerd daemon restarts and re-probe readiness.                                                   |

---

## 6. Supply Chain & Build Security

The CRIU snapshot engine follows secure container build practices:

- **Minimal Base Images**:
  - `cmd/engine` (Controller): Built on `gcr.io/distroless/static:nonroot`. Runs as non-root UID `65532:65532`.
  - `cmd/holder` (Sandbox Placeholder): Built `FROM scratch`. Static binary, no shell, no package manager, non-root UID `65532:65532`.
  - `cmd/agent` (Node Agent): Built from Debian trixie (for CRIU 4.x package build) into an Ubuntu 24.04 runtime with `util-linux` and `ca-certificates`.
- **Reproducible & Trimmed Binaries**: All Go binaries are compiled with:
  ```bash
  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w"
  ```
  This removes file system paths, debug symbols, and external C-library dependencies.
- **License Hygiene**: The codebase is licensed under Apache-2.0. The bundled CRIU binary is GPL-2.0 and invoked strictly as an external, exec-only subprocess.
