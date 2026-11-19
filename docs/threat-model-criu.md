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
|   | Target Pod Sandbox                    | Cloud Object Storage (GCS)            |   |
|   |   - holder container (pause sandbox)  |   - Encrypted checkpoint archives     |   |
|   |   - Sibling restored container (runc) |   - Full process memory & metadata    |   |
|   +---------------------------------------+---------------------------------------+   |
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
- **Cluster RBAC Minimization**: The agent’s ClusterRole (`pod-migrate-psengine-agent`) is strictly scoped:
  - `nodes/checkpoint` (`create`): Limited to triggering kubelet container checkpoints on its own node.
  - `nodes` (`get`, `patch`): Limited to publishing readiness and demotion annotations (`pod-migrate.io/criu-ready`, `pod-migrate.io/criu-version`).
  - `pods` (`get`, `list`, `watch`, `update`, `patch`): Scoped to tracking pod lifecycle and stamping `pod-migrate.io/psengine-restore`.
  - `podsnapshots` (`get`, `list`, `watch`, `update`, `patch`): Scoped to reconciling assigned checkpoint work orders.
  - `podsnapshotmanualtriggers` (`get`): Read-only validation of cancellation requests.
- **Network Ingress**: The agent does not expose any public or cluster-facing TCP/UDP listening ports. All communication is outbound to the Kubernetes API server, cloud storage endpoints, or node-local unix sockets.
- **Resource Constraints**: Default resource requests (`100m` CPU, `128Mi` memory) and memory limits (`512Mi`) prevent memory exhaustion from runaway agent operations.

---

## 3. Restore Path Security & Privilege Gain Prevention

A central risk in process checkpoint and restore is **unauthorized privilege gain**: restoring an unprivileged container into a higher-privileged configuration, or an archive injecting malicious mounts, devices, or capabilities into a destination host. The CRIU engine enforces strict security invariants (established in [#129](https://github.com/gke-labs/pod-migration/issues/129)):

### R1. Authorization & Admission Validation

- **Controller-Only Orders**: Restore operations are strictly authorized by the controller. The agent restores pods only if they carry an explicit restore order annotation (`<group>/ps-name`) matching an active `PodMigrationJob`.
- **Anti-Spoofing Webhook**: The mutating admission webhook (`PodGateInjector`) strips any user-supplied restore annotations on pod creation, ensuring user clients cannot bind replacement pods to arbitrary snapshot archives.

### R2. Archive Provenance & Integrity

- **Checksum Verification**: Checkpoint archives in GCS (`manifest.json`, `spec.json`, `checkpoint.tar`) are validated against SHA-256 and MD5 hashes before unpacking.
- **Archive Size Bounds**: Strict caps prevent denial-of-service via resource exhaustion:
  - Metadata files (`manifest.json`, `spec.json`): Capped at 4 MiB.
  - Archive payload (`checkpoint.tar`): Capped at 8 GiB.
- **Safe Unpacking**: The filesystem layer (`rootfs-diff.tar`) is applied strictly via containerd's `archive.Apply` into a mounted, isolated snapshot root. Unpacking rejects directory traversal (`..`), path escapes, and tar-slip attempts.

### R3. Strict OCI Runtime Spec Inheritance (No Privilege Gain)

CRIU process dumps record process credentials and capabilities. To guarantee that a restored process **never gains privilege** beyond what the Kubernetes scheduler and destination Pod spec authorize:

- **Destination Pod Authority**: When `node.AdaptSpec` constructs the OCI runtime bundle for the restored container, all security-relevant fields are copied directly from the kubelet-generated destination container spec:
  - `Capabilities` (`CapEff`, `CapPrm`, `CapBnd`, `CapInh`, `CapAmb`)
  - `User` (`UID`, `GID`, `AdditionalGids`)
  - `NoNewPrivileges`
  - `Seccomp` profile configuration
  - `AppArmor` / `SELinux` process labels
  - `MaskedPaths` and `ReadonlyPaths`
  - Container device rules (`Devices`)
  - Linux namespaces (`IPC`, `Network`, `UTS`, `PID`, `Cgroup`)
- **No Arbitrary Host Mounts**: Archives cannot add host volume mounts or device access. Mounts are restricted to the destination pod's declared volumes and canonical container mounts.
- **Host Network Refusal**: Pods declaring `hostNetwork: true` are refused at all CRIU boundaries (`PSMTReconciler`, `injectHolder`, and `NodeRestoreReconciler.restore`). This prevents any restored container from manipulating the host network stack or netfilter tables.

### R4. Node Posture & Containerd CVE Defense

Installing `criu` on a node's PATH can interact with container runtime vulnerabilities. Specifically:

- **GHSA-p7v4-vr35-mj6f (CVE-2026-95837)**: Containerd versions `>=2.1.0, <2.2.7` and `>=2.3.0, <2.3.4` contained a critical vulnerability where containerd implicitly restored checkpoint images via `CreateContainer` without validating destination security policies.
- **Enforced Minimums**: The agent inspects `nodeObj.Status.NodeInfo.ContainerRuntimeVersion` at startup (`CheckContainerdVersion`). It refuses to mark the node ready and refuses to install CRIU if containerd is within the vulnerable range:
  - Containerd `>= 2.2.7`, `>= 2.3.4`, or `2.0.x` is strictly required.
  - If an unsupported or vulnerable version is detected, the agent annotates the node with `pod-migrate.io/criu-ready: false`, records the reason, and provides an upgrade remediation message.

---

## 4. Archive Storage, IAM, and Data Protection

### Checkpoint Data Sensitivity

When CRIU dumps a container, the resulting archive (`checkpoint.tar`) contains a bit-for-bit snapshot of the application's address space. This includes:

- Plaintext database connection strings and credentials.
- Private encryption keys, TLS certificates, and API tokens loaded in heap memory.
- In-flight session data and customer personally identifiable information (PII).

**Security Classification**: All checkpoint archives in GCS must be treated with the **same classification as Kubernetes Secrets and cryptographic keys**.

### Cloud Storage IAM Hardening

To prevent unauthorized access or tampering with checkpoint archives:

1. **Dedicated Service Accounts**: Run the node agent under a dedicated Google Service Account (GSA) with least-privilege bucket access:
   - Checkpointing nodes require `roles/storage.objectCreator` and `roles/storage.objectViewer`.
   - Restoring nodes require `roles/storage.objectViewer`.
   - General cluster workloads must **never** be granted read or write permissions to the checkpoint bucket.
2. **Avoid Default Compute Engine SA**: In GKE, ensure nodes do not use the default Compute Engine service account (`<project-number>-compute@developer.gserviceaccount.com`), which often carries broad Project Editor permissions.
3. **Uniform Bucket-Level Access**: Enable Uniform Bucket-Level Access (UBLA) on the checkpoint GCS bucket to prevent ACL-based permission leakage.

### Encryption & CMEK

- **Default Encryption**: GCS encrypts all data at rest using Google-managed encryption keys.
- **Customer-Managed Encryption Keys (CMEK)**: For enterprise deployments, configure Cloud KMS CMEK on the checkpoint bucket. This allows security teams to:
  - Revoke key access instantly in the event of an incident or node compromise.
  - Audit all cryptographic key operations via Cloud KMS audit logs.

### Lifecycle Management & Retention Policy

Checkpoint archives are transient migration artifacts. They should not persist longer than needed to complete the migration.

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
  - `cmd/agent` (Node Agent): Built from Debian trixie (for CRIU 4.x package build) into an Ubuntu 24.04 minimal runtime with `util-linux` and `ca-certificates`.
- **Reproducible & Trimmed Binaries**: All Go binaries are compiled with:
  ```bash
  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w"
  ```
  This removes file system paths, debug symbols, and external C-library dependencies.
- **License Hygiene**: The codebase is licensed under Apache-2.0. The bundled CRIU binary is GPL-2.0 and invoked strictly as an external, exec-only subprocess.
