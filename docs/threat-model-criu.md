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
|   |     (COS: /home/kubernetes/bin, bound over /usr/local)                        |   |
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

The agent mounts no directory for the `criu` wrapper: it installs the bundle
and the wrapper through the host's root, `/proc/1/root`, which `hostPID`
gives it, in `/var/lib/pod-migrate/criu-bundle` and `/usr/local/sbin`, or on
COS under `/home/kubernetes/bin` with a bind mount over `/usr/local` that it
makes in the host's mount namespace (#109; `engines/criu/README.md`, Node
images). A compromised agent could write anywhere on the host through
`/proc/1/root`, as it already could through `nsenter -t 1`.

### Blast Radius & Mitigations

- **Compromise Impact**: If the `pod-migrate-agent` container is compromised, the attacker achieves root on the host node and access to local containerd sockets, and to the archive signing keys, so it can sign archives that any node restores (R2). It also gets the agent's projected service account token, and with it bucket access as the agent: it can read, delete or corrupt archives (R5).
- **Cluster RBAC Scope**: The agent uses a ClusterRole (`pod-migrate-psengine-agent`) granting cluster-wide permissions:
  - `nodes/checkpoint` (`create`): Triggers kubelet container checkpoints (cluster-wide scope in Kubernetes RBAC).
  - `nodes` (`get`, `patch`): Publishes readiness and capability annotations (`pod-migrate.io/criu-ready`, `pod-migrate.io/criu-version`), and the restore placement facts (#257): the `criu.pod-migrate.io/*` and `missing-cpu.criu.pod-migrate.io/*` labels and the `pod-migrate.io/criu-node-facts` and `pod-migrate.io/criu-node-cpu-features` annotations. They stay on the `Node` after the engine is uninstalled.
  - `pods` (`get`, `list`, `watch`, `update`, `patch`): Tracks replacement pod lifecycle and stamps `pod-migrate.io/psengine-restore` (with `pod-migrate.io/psengine-restore-reason` on a failure).
  - `podsnapshots` (`get`, `list`, `watch`) and `podsnapshots/status` (`get`, `update`, `patch`): Read-only work order inspection and status reporting.
  - `podmigrationjobs` (`get`, `list`, `watch`): Validates active migration job bindings.
  - `podsnapshotmanualtriggers` (`get`): Verifies cancellation state, and reads the trigger's controller reference: the PodMigrationJob whose UID the archive manifest binds (#244).
  - `namespaces` (`get`, on `kube-system` only): Reads the cluster UID that archive manifests bind (#244).
  - `events` (`create`, `patch`): Emits node restore and failure warning events.
    _Note on RBAC Breadth_: Because Kubernetes RBAC cannot scope `nodes/checkpoint`, `nodes/patch`, or `pods/update` to the DaemonSet's local node, a compromised agent holds broad cluster-wide write privileges against these resources.
- **Engine Controller RBAC Scope (`pod-migrate-psengine-controller`)**: The cluster-level `cmd/engine` controller and `/psengine-restore-inject` webhook run as non-root (`65532:65532`) with narrower RBAC:
  - `podmigrationjobs` (`get` in `podmigration.gke.io`): Single live API `GET` in `RestoreInjector.Handle` (after in-memory pod-shape filtering) to confirm `pod-migration.gke.io/engine: criu` on the assigned `PodMigrationJob` before injecting the holder wrapper (`list`/`watch` are not granted).
  - `customresourcedefinitions` (`get` in `apiextensions.k8s.io`): Startup `VerifyCRDOwnership` preflight check for the configured `--api-group`.
  - `podsnapshotstorageconfigs`, `podsnapshotpolicies`, `podsnapshotmanualtriggers`, `podsnapshots` (`get`, `list`, `watch`, `create`) and `*/status` (`get`, `update`, `patch`) in `criu.podmigration.gke.io` (by default, #135).
  - `podsnapshots` (`update`, `patch`) in `criu.podmigration.gke.io`: adds and removes the `pod-migrate.io/archive-cleanup` finalizer (#107).
  - `pods`, `nodes` (`get`, `list`, `watch`) and `events` (`create`, `patch`).
- **Network Ingress & Health Probes**: The agent listens on host TCP port `:8091` (`hostNetwork: true`) for kubelet liveness (`/healthz`) and readiness (`/readyz`) probes. It does not expose other network services; all other traffic is outbound to the Kubernetes API server, containerd unix sockets, cloud storage APIs, or Google's Security Token Service, where it exchanges its service account token for bucket access (R5).
- **Resource Constraints**: Default resource requests (`100m` CPU, `128Mi` memory) and memory limits (`512Mi`) prevent memory exhaustion from runaway agent operations.

---

## 3. Restore Path Security & Privilege Boundaries

A central risk in process checkpoint and restore is **unauthorized privilege gain**: restoring an unprivileged container into a higher-privileged configuration, or an archive injecting malicious mounts, devices, or capabilities into a destination host. Process restoration is governed by strict boundaries (the security requirements of C10 in `docs/design/engine-contract.md`), and operates strictly within namespaces explicitly labeled `pod-migration.gke.io/managed: "true"`: in unmanaged namespaces, admission webhooks (`veviction`, `mpodgate`, and `restore-inject`) and admission policies are inactive, allowing voluntary evictions and replacement pod creations to proceed cold without live migration interception:

### R1. Authorization & Admission Validation

- **Work Order & PMJ Verification**: At `CREATE`, `/psengine-restore-inject` (`RestoreInjector`) intercepts pods in namespaces labeled `pod-migration.gke.io/managed: "true"` and injects the holder wrapper only into gated single-container `runc`/default pods (outside `kube-system`, `pod-migration-system`, and `pod-migration-criu-system`) whose assigned `PodMigrationJob` (`get` on `podmigrationjobs`) carries `pod-migration.gke.io/engine: criu` (#135). At restore time, the node agent restores a pod only after verifying that the target pod carries a restore order annotation (`<group>/ps-name`) matching an authorized work order whose referenced `PodMigrationJob` is either actively `Restoring` or `Succeeded` (warm-retry fallback window).
- **Admission Boundaries at `CREATE`**: The replacement webhook (`PodGateInjector`) only intercepts `CREATE` requests. For Kubeflow and third-party orchestration interop, it intentionally preserves pre-existing, non-empty `podsnapshot.gke.io/ps-name` annotations when no matching PMJ is found, rather than unconditionally stripping them.
- **Admission Boundaries on `UPDATE` (#233)**: Two `ValidatingAdmissionPolicy` objects block tampering with restore annotations on existing pods in namespaces carrying `pod-migration.gke.io/managed: "true"`. The API server evaluates them itself, so they don't depend on a webhook being up, and their `matchConditions` skip every update that leaves the protected annotations as they are. They cover updates of a pod and of its `status` subresource, since a status write can change annotations too.
  - `pod-migration-restore-order` (in `controller/deploy.yaml`): on a pod opted into migration (`pod-migration.gke.io/enabled: "true"` before or after the update), only the controller's service account may set, change or remove a restore order (`<group>/ps-name`, for any engine's group). On `CREATE` in managed namespaces (`pod-migration.gke.io/managed: "true"`), requests carrying non-GKE restore annotations (`<group>/ps-name != podsnapshot.gke.io/ps-name`) are forbidden to prevent malicious restore spoofing during fail-open webhook outages (#303).
  - `pod-migrate-psengine-restore-status` (in `engines/criu/deploy/deploy.yaml`): in managed namespaces (`pod-migration.gke.io/managed: "true"`), only the node agent's service account may record a restore outcome (`pod-migrate.io/psengine-restore`), only once, and only as `restored` or `failed`, with a failure's reason (`pod-migrate.io/psengine-restore-reason`, #244) in the same update and only next to `failed`. Nobody may change or remove either afterwards.
- **Engine Annotations at `CREATE`**: On a pod opted into migration, the replacement webhook (`PodGateInjector`) removes `pod-migrate.io/psengine-restore`, `pod-migrate.io/psengine-restore-reason` and `pod-migrate.io/psengine-injected` that the pod's template carries when the controller is running, so a pod can't be created with an outcome, a reason or a holder marker of its own (during a controller outage, runc pod creation fails open without scrubbing, while gVisor pod creation fails closed).
- **Remaining Gaps**:
  - A pod that opts out (its opt-in label is removed, which stays allowed) leaves the scope of `pod-migration-restore-order` for GKE restore orders, while non-GKE restore annotations remain forbidden on CREATE and UPDATE regardless of the opt-in label. A GKE restore order set on it afterwards is a restore outside a migration, and it stays on the pod if the label is added back later: that last update doesn't change the order, so the policy doesn't see it. The node agent's work order check above still applies to it.
  - The policies see updates of a pod and of its `status`. They don't see a `pods/binding` request, which can also set annotations on a pod when it binds the pod to a node. The built-in `edit` and `admin` roles don't grant `pods/binding`; keep it to schedulers.
  - Whoever may change or delete a `ValidatingAdmissionPolicyBinding` (a cluster admin) can lift a policy. That is also the way to edit these annotations by hand in an emergency: set the binding's `validationActions` to `Audit`, and back to `Deny` afterwards.
  - Each policy names the one service account it allows, namespace included. An install that renames either must change the policy too.

### R2. Archive Integrity & Provenance (in place for CRIU, #244)

- **Integrity Against Corruption**: Checkpoint archives in GCS (`manifest.json`, `spec.json`, `checkpoint.tar`) are validated against SHA-256 and MD5 hashes before unpacking.
- **Signed, Bound Manifests**: The node agent signs each archive's manifest with an HMAC-SHA256 key from the `pod-migrate-archive-signing-keys` Secret in `pod-migration-criu-system`, which only the agent mounts (read-only, mode `0400`). `manifest.json` is a DSSE envelope; its payload carries the SHA-256 of `spec.json` and `checkpoint.tar` and binds the archive to its snapshot, namespace, source pod name and UID, container, bucket and path, the UID of the PodMigrationJob that controls the work order's trigger, and the cluster UID (the `kube-system` Namespace's UID). It also records the trigger's name and UID, which the restore doesn't check: the job UID ties the archive to its job, and a job that creates its trigger again takes its own work order over (#262).
- **Verified Before Anything Else Is Downloaded**: The restoring agent verifies the manifest's signature with its keys, then compares every binding with its own work order, the replacement's assigned PodMigrationJob and its own cluster, before it downloads `spec.json` or `checkpoint.tar`, which must then match the signed digests. A replaced or re-signed archive, a genuine archive copied into another snapshot's prefix, an archive kept from an earlier job and put back, or one from another cluster is refused, and so is an unsigned one. The replacement falls back to a cold start, with the reason in `pod-migrate.io/psengine-restore-reason` and in the PodMigrationJob's message (`engines/criu/README.md`, "Archive signing"). Neither that reason nor the Warning event on the pod names key IDs or cluster UIDs; the agent's log does.
- **Bounded Verification**: A bucket writer can't make verification expensive. An envelope with more than 8 signatures is refused as malformed before any MAC is computed, a signature that isn't 32 bytes never matches, and each key's MAC is computed at most once per envelope.
- **No Key, No Checkpoint**: An agent without a signing key or the cluster UID fails a work order before it freezes the source, and an agent without keys refuses every archive.
- **No Job, No Commit**: An archive that no restore would accept is never committed with its source kept frozen for eviction. Right before signing, the agent reads the work order and its trigger past the cache and takes the job from the trigger's controller reference. It reads again for a few seconds while a trigger that was created again takes the work order over (#262), cancels the checkpoint if the trigger or the work order is gone, and fails it before `manifest.json` is uploaded if no job can be bound. At the commit point it reads the trigger again by name and fails if another PodMigrationJob controls it by then. Each time, the source thaws.
- **Residual Risks**:
  - Anyone who can read Secrets in `pod-migration-criu-system`, or who compromises a node agent, can sign an archive. With a MAC, every agent holds the key, so one compromised node can forge archives that any node restores.
  - Signing doesn't stop anyone from reading an archive. Only a bucket that only the CRIU engine uses, granting object access to its two service accounts alone, keeps others out (R5, section 4). Even then, whoever can get a token for either of them, has storage access through the project, or compromises a node, can read archives, and delete or corrupt them (forcing cold starts).
  - Archives written before signing are refused, and their replacements start cold.
  - The commit point reads the trigger once more, after `manifest.json` is uploaded. A trigger that passes to another PodMigrationJob after that read isn't seen: the snapshot goes Ready, and that job's restore refuses the archive (cold start).
- **Archive Size Bounds**: Strict caps prevent denial-of-service via resource exhaustion:
  - Metadata files (`manifest.json`, `spec.json`): Capped at 4 MiB.
  - Archive payload (`checkpoint.tar`): Capped at 8 GiB.
- **Safe Unpacking**: The filesystem layer (`rootfs-diff.tar`) is applied strictly via containerd's `archive.Apply` into a mounted, isolated snapshot root. Unpacking rejects directory traversal (`..`), path escapes, and tar-slip attempts (regular files and dirs only; sparse files rejected).

### R3. OCI Spec Adaptation & Privilege Boundaries (Partial; R3 is still open, see C10 in `docs/design/engine-contract.md`)

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
2. **CRIU Process State Inherent Privileges**: While `AdaptSpec` governs the OCI container spec, CRIU restores process credentials, capabilities, and seccomp state directly from the checkpoint image files (`core-*.img`). Nothing currently compares in-image process credentials against destination constraints. Signed manifests (R2, #244) mean that writing to the checkpoint GCS bucket no longer gets an attacker's process image restored. But **anyone who can read the archive signing keys (Secrets in `pod-migration-criu-system`) or compromise a node agent can sign an archive that gets root on a restoring node**. The engine uses the bucket as its own service accounts (R5, #244), not as the node's VM service account; on a bucket that only the CRIU engine uses and that grants object access to those two alone, a workload on a CRIU node doesn't reach the archives, as long as the node's service account has no storage access through the project either. Whoever can get a token for those service accounts, or compromises a node, can still delete, corrupt or read archives.

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

### Cloud Storage IAM & Storage Credentials (R5 for CRIU, on a CRIU-only bucket, #244)

To prevent unauthorized access or tampering with checkpoint archives, the engine uses the bucket as its own two identities, and R5 holds for a bucket that only the CRIU engine uses and that grants object access to those two alone:

1. **The Node Agent's Own Service Account, Through STS**:
   - The node agent runs with `hostNetwork: true` to communicate with the local kubelet and manage network namespaces. There, the GKE metadata server can't tell its requests from the node's and answers as the node's VM service account, which every workload on the node can use too. So the agent doesn't use it for the bucket.
   - Its DaemonSet projects a token of its service account (`pod-migration-criu-system/pod-migrate-agent`), with the cluster's Workload Identity pool as its audience, read-only into the agent's container (mode `0400`); it lives an hour and the kubelet rotates it. The agent exchanges it at Google's Security Token Service for an access token of that service account's Workload Identity principal, scoped to `devstorage.read_write`, and reuses it until 5 minutes before it expires. No Google service account, no impersonation. The exchange never follows a redirect, which would resend the service account token.
   - With `--gcs-credentials=workload-identity` (the default), an agent without a workload pool, an identity provider or a readable token, or with an install placeholder left in place, exits at startup: it never falls back to the node's service account. Before each checkpoint freezes its source, the agent gets a token that stays usable for 15 minutes; if it can't, the checkpoint fails with the source untouched. Errors carry STS's HTTP status and error, never a token.
2. **The Engine Controller's Own Service Account, Through GKE Workload Identity**: `cmd/engine` doesn't run with `hostNetwork`, so the GKE metadata server serves it a token of its own service account (`pod-migration-criu-system/pod-migrate-psengine-controller`). Its node pool must run the GKE metadata server (`GKE_METADATA`).
3. **A Bucket for the CRIU Engine Alone**:
   - The bucket grants `roles/storage.objectUser` and `roles/storage.bucketViewer` to the two principals, `principal://iam.googleapis.com/projects/<PROJECT_NUMBER>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/pod-migration-criu-system/sa/<SERVICE_ACCOUNT>`, and object access to nobody else, not to the nodes' VM service account either. The engine controller needs object create, get and delete for its storage probe and archive garbage collection (#107); without delete, archives are left behind, reported only by `ArchiveDeleteFailed` and `ArchiveDeleteAbandoned` events.
   - It can't be GKE Pod Snapshots' bucket: `patches/setup-storage.sh` grants that one to every service account in the `default` namespace and to the GKE service agent. A new bucket's default policy also grants the project's Owners, Editors and Viewers through the legacy convenience bindings (`projectOwner`/`projectEditor` on `roles/storage.legacyBucketOwner` and `roles/storage.legacyObjectOwner`, `projectViewer` on `roles/storage.legacyBucketReader` and `roles/storage.legacyObjectReader`).
   - `patches/setup-criu-storage.sh` creates such a bucket, or first checks an existing one: if it grants anyone but the two principals and the legacy convenience bindings, the script lists them and exits without changing anything, so it never strips a shared bucket. Otherwise it grants the two principals, removes the legacy convenience bindings after printing them (leaving the one through which project Editors may change the bucket's policy for last), checks that nobody else is left, and warns about what the bucket's own policy can't keep out.
   - Avoid Default Compute Engine SA: In GKE, ensure nodes do not use the default Compute Engine service account (`<project-number>-compute@developer.gserviceaccount.com`), which often carries broad Project Editor permissions: an Editor can create pods in `pod-migration-criu-system` through GKE IAM, as either of the engine's service accounts.
   - Enable Uniform Bucket-Level Access (UBLA) on the checkpoint GCS bucket to prevent ACL-based permission leakage.
   - **Multi-Tenancy & Path Scoping**:
     - `tokenSource` in `PodMigration` is GKE Pod Snapshots only; the CRIU engine uses node agent STS credentials (#290) and does not support `tokenSource` in PSSC.
     - Within a shared CRIU bucket, cross-namespace storage separation relies on server-side path scoping enforced by the controller and the `pod-migration-storage-location` ValidatingAdmissionPolicy (`patches/storage-isolation-admission-policy.yaml`), which requires paths to be prefixed with `gs://<bucket>/<namespace>/...`, rejects `.`, `..`, empty segments (`//`), whitespace (space `' '` and tab `'\t'`), and rejects the bucket root even for a namespace bound to its bucket by the controller's `--allow-bucket-root-storage-namespaces` opt-in (only appropriate when exactly one namespace can create PodMigrations on that bucket; the controller reserves a bound bucket for its namespace).
     - Cross-namespace restore isolation is enforced by the `pod-migrate-psengine-restore-status` ValidatingAdmissionPolicy (#287), which blocks unauthorized mutation of restore annotations.
4. **Test-only storage override (`STORAGE_EMULATOR_HOST`)**:
   - When this variable is set on the engine or the node agent, that component sends every archive read and write to the named endpoint instead of GCS, with no credentials (over plain HTTP for a bare `host:port`), and the node agent skips the Workload Identity startup checks. Whoever runs that endpoint can do what a bucket writer can (R2): read, delete or corrupt every archive. Archive signing still keeps it from getting an archive of its own restored.
   - It exists for the kind E2E with `fake-gcs-server` (`verification-suite/kind/up.sh`). The shipped manifests never set it, and setting it takes the same `update` on those workloads that already allows replacing their images. Each component logs a `WARNING` line at startup when it is set. Never set it on a real cluster.
5. **Residual Risks**:
   - A namespace that both engines serve has one storage location for both, on a bucket that also grants GKE Pod Snapshots' workloads, so its CRIU archives can't meet R5 today: anyone that bucket grants can read them. A storage location per engine is a follow-up.
   - Any other principal a bucket grants, such as `setup-storage.sh`'s `default` namespace and the GKE service agent, or the project's convenience bindings left in place, reaches the archives in it.
   - Roles granted on the project, or above it, that carry `storage.objects` permissions (Storage Admin, the other `roles/storage.*` roles, custom roles) reach the archives whatever the bucket's own policy says. Without the convenience bindings, project Owners and Editors have no object access of their own, but an Owner can grant itself such a role, and Owners and Editors can create pods in `pod-migration-criu-system` through GKE IAM as either service account (next item). That includes the nodes' service account if it has such a role, which would let every workload on its nodes reach the archives. Run CRIU nodes as a service account without one.
   - Whoever can get a token for either service account can use the bucket as it: anyone who can create pods, or workloads that create them, in `pod-migration-criu-system` (a pod can name either service account and project its token), whether through Kubernetes RBAC or through GKE IAM (project Owners and Editors can), exec into the agent's pods, or create `serviceaccounts/token` for them. Keep all of these away from workloads and tenants.
   - Whoever compromises a node can read the agent's projected token there, and use the bucket until the tokens it took expire: an access token exchanged from it lasts up to an hour.
   - `--gcs-credentials=node` brings back the node's VM service account, which every workload on the node can use. It is an explicit choice, logged at startup, and needs the node's service account granted on the bucket.

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

| Modification                           | Host Location                                                                              | Rollback / Cleanup                                                                                                                                                                      |
| -------------------------------------- | ------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **CRIU Bundle**                        | `/var/lib/pod-migrate/criu-bundle/` (COS: `/home/kubernetes/bin/pod-migrate/criu-bundle/`) | Directory containing self-contained `criu.bin`, bundled shared libraries, dynamic linker, and `VERSION` file. Manually removable when uninstalling (on COS, `umount /usr/local` first). |
| **CRIU System Wrapper**                | `/usr/local/sbin/criu` (COS: `/home/kubernetes/bin/pod-migrate/usr-local/sbin/criu`)       | Executable wrapper script that launches CRIU via the bundled dynamic linker. Overwritten atomically.                                                                                    |
| **`/usr/local` bind mount (COS only)** | `/usr/local`                                                                               | `/home/kubernetes/bin/pod-migrate/usr-local` bound over COS's empty `/usr/local` so containerd finds `/usr/local/sbin/criu`. Lasts until reboot; `umount /usr/local` removes it.        |
| **Runc Configuration**                 | `/etc/criu/runc.conf`                                                                      | CRIU global configuration specifying `tcp-close`, `skip-in-flight`, `file-validation buildid`, and `file-locks`.                                                                        |
| **Node Annotations**                   | Kubernetes `Node` metadata                                                                 | Stamped with `pod-migrate.io/criu-ready` (`true` or `false`) and `pod-migrate.io/criu-version`. Stale annotations cleared on demotion.                                                  |
| **Socket Watching**                    | `/run/containerd/containerd.sock`                                                          | Actively monitored for inode changes to detect containerd daemon restarts and re-probe readiness.                                                                                       |

---

## 6. Supply Chain & Build Security

The CRIU snapshot engine follows secure container build practices:

- **Minimal Base Images**:
  - `cmd/engine` (Controller): Built on `gcr.io/distroless/static:nonroot`. Runs as non-root UID `65532:65532`.
  - `cmd/holder` (Sandbox Placeholder): Built `FROM scratch`. Static binary, no shell, no package manager, non-root UID `65532:65532`.
  - `cmd/agent` (Node Agent): Bundles CRIU from Debian forky (trixie's CRIU 4.1.1 is older than the 4.2.1 the build requires) into an Ubuntu 24.04 runtime with `util-linux`, `ca-certificates` and `iptables`. Forky is Debian's testing suite, which the Debian security team doesn't cover. The CRIU stage pins the forky base image by digest and apt to one snapshot.debian.org timestamp (`DEBIAN_SNAPSHOT` in `build/agent/Dockerfile`), and fails unless the bundle matches `build/agent/package-versions.lock`, so two builds of the same commit bundle the same packages, and security fixes reach the bundle only when someone bumps that pin. The lock compares source package versions, so it wouldn't notice a rebuild of the same source version (a binNMU); the snapshot pin is what fixes the exact binaries. `/opt/criu-bundle/licenses/package-versions.txt` records exactly what an image carries. The Ubuntu runtime stage isn't pinned: it installs the current packages at build time, and `/licenses/runtime-package-versions.txt` records them.
- **Reproducible & Trimmed Binaries**: All Go binaries are compiled with:
  ```bash
  CGO_ENABLED=0 go build -trimpath -ldflags="-s -w"
  ```
  This removes file system paths, debug symbols, and external C-library dependencies.
- **License Hygiene**: The codebase is licensed under Apache-2.0. The bundled CRIU binary is GPL-2.0 and invoked strictly as an external, exec-only subprocess.
