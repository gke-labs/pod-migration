# Multi-Tenant Snapshot Storage Architecture & Guidance

## Overview

A Kubernetes pod checkpoint taken during live migration contains a full memory dump of application state. Checkpoints frequently hold confidential runtime data, including database passwords, TLS private keys, session tokens, and in-memory caches.

In a multi-tenant or shared-cluster deployment where workloads from different teams or customers share nodes and infrastructure, **snapshot storage must be strictly isolated by access, not merely separated by name or object path prefixes**.

This document defines the supported snapshot storage layouts for multi-tenant clusters, explains the controller's fail-closed collision prevention, and details operator verification procedures (part of issue [#278](https://github.com/gke-labs/pod-migration/issues/278)).

---

## Privileged Field & RBAC Governance

`spec.storage.location` on a `PodMigration` is a **privileged field**. Because the storage location dictates which Cloud Storage bucket and path prefix the snapshot engines read and write checkpoints to:

1. **RBAC Boundaries**: In multi-tenant clusters, tenant workload owners should **not** have unconstrained `create` or `update` RBAC permissions on `podmigrations.podmigration.gke.io`. Cluster operators or GitOps automation should provision and manage `PodMigration` objects.
2. **Admission Enforcement**: When tenants are permitted to self-manage `PodMigration` resources within their namespaces, operators must deploy the `pod-migration-storage-location` ValidatingAdmissionPolicy (`patches/storage-isolation-admission-policy.yaml`). This policy enforces at admission time that any storage path under a bucket begins strictly with the object's `metadata.namespace` (`gs://<bucket>/<namespace>/...`) and contains no `.` or `..` path segments.

---

## Supported Multi-Tenant Storage Layouts

Two secure storage topologies are supported:

### Layout A: Dedicated Cloud Storage Bucket per Tenant (Strongest Isolation)

In Layout A, every tenant namespace is provisioned with its own distinct Google Cloud Storage (GCS) bucket:

- **Bucket Mapping**: Namespace `tenant-a` uses `gs://cluster-tenant-a-snapshots/`, while `tenant-b` uses `gs://cluster-tenant-b-snapshots/`.
- **Identity & Token Source**: `spec.storage.tokenSource: podKSA` (or omitted).
- **IAM Boundaries**: Bucket IAM bindings (`roles/storage.objectUser` and `roles/storage.bucketViewer`) are granted strictly to that tenant's Kubernetes Service Account (`KSA`) or Workload Identity pool principal:
  ```bash
  principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/namespace/${TENANT_NAMESPACE}
  ```
- **Security Guarantee**: Because tenants do not share a storage bucket, a compromised container or tenant in `tenant-b` cannot read, list, or overwrite checkpoints belonging to `tenant-a`, even if they attempt direct GCS API requests.

---

### Layout B: Shared Bucket with Managed Folders and `federatedP4SA`

When deploying a single shared GCS bucket across multiple tenants, GCS **hierarchical namespaces** and **managed folders** paired with GKE Pod Snapshots path-limited tokens (`federatedP4SA`) must be used:

- **Bucket Configuration**: The bucket is created with `--enable-hierarchical-namespace` and `--uniform-bucket-level-access`.
- **Path Separation**: Each tenant namespace configures a distinct subpath matching its namespace:
  - `tenant-a`: `spec.storage.location: "gs://shared-snapshots-bucket/tenant-a"`
  - `tenant-b`: `spec.storage.location: "gs://shared-snapshots-bucket/tenant-b"`
- **Token Source**: `spec.storage.tokenSource: "federatedP4SA"`. (Note: `tokenSource` is GKE Pod Snapshots only; the CRIU engine uses node agent STS credentials, see below).
- **IAM Setup**:
  1. **Zero Bucket-Wide Workload Grants**: Workload Service Accounts (`KSA`) are **never** granted bucket-level IAM roles (`roles/storage.objectUser` or `roles/storage.bucketViewer`) on the shared bucket.
  2. **GKE Node Agent Role**: The GKE node service account is granted `roles/storage.admin` on the bucket:
     ```bash
     gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
       --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com" \
       --role="roles/storage.admin"
     ```
  3. **Per-Namespace Managed Folders**: A managed folder is created for each tenant namespace:
     ```bash
     gcloud storage managed-folders create "gs://${BUCKET_NAME}/tenant-a"
     gcloud storage managed-folders create "gs://${BUCKET_NAME}/tenant-b"
     ```
  4. **Path-Restricted Tokens**: At checkpoint/restore time, the GKE node agent mints a short-lived token restricted specifically to the tenant's managed folder path (`gs://${BUCKET_NAME}/${TENANT_NAMESPACE}/`).
  5. Containers running in `tenant-b` cannot read, list, or overwrite checkpoints under `tenant-a`'s prefix because their issued credentials lack permission outside their own path.

---

## CRIU Engine Storage Isolation Model

The CRIU snapshot engine (`engines/criu`) operates with dedicated node agent credentials rather than per-pod tokens:

1. **Node Agent STS Identity (#290)**: The CRIU daemonset agent obtains scoped Cloud Storage access via STS credentials bound to the CRIU engine service account.
2. **Namespace Path Scoping**: Storage path isolation within the shared bucket is enforced server-side:
   - `util.ParseStorageLocation` rejects dot-dot (`..`), dot (`.`), and empty (`//`) path segments.
   - The controller and `pod-migration-storage-location` ValidatingAdmissionPolicy require `spec.storage.location` paths to be scoped to the namespace (`gs://<bucket>/<namespace>/...`).
   - The CRIU agent structures checkpoint archive keys as `<path>/<pmj-name>/...`, confining objects to the tenant's namespace prefix.
3. **Cross-Namespace Restore Guardrail (#287)**: The `pod-migrate-psengine-restore-status` ValidatingAdmissionPolicy blocks unauthorized mutations to restore annotations (`pod-migrate.io/psengine-restore`), ensuring workloads in namespace B cannot trigger or spoof restores from namespace A checkpoints.

---

## Controller Guardrail: Conflicting & Un-Scoped Storage Locations

The `PodMigrationReconciler` automatically enforces cross-namespace storage location isolation:

1. **Namespace Scoping**: When a storage path is specified, the first path segment must match `metadata.namespace` (`gs://<bucket>/<namespace>/...`). Specifying a path belonging to another namespace is refused with condition `Ready=False` and reason `ConflictingStorageLocation`.
2. **Exact & Prefix Collision Refusal**: If two `PodMigration` resources in different namespaces specify conflicting paths in the same bucket, the younger CR is refused:
   - Identical path: `gs://shared-bucket/snapshots` and `gs://shared-bucket/snapshots`.
   - Prefix containment: `gs://shared-bucket/team-a` and `gs://shared-bucket/team-a/nested`.
   - Bucket root: `gs://shared-bucket` and any subpath in `gs://shared-bucket`.
   - Status: `Ready=False`
   - Reason: `ConflictingStorageLocation`
   - Event: A `Warning` event with reason `ConflictingStorageLocation` is emitted on the refused `PodMigration`.
3. **Deterministic Singleton Precedence**: Only active singleton `PodMigration`s (or ones holding `StorageCleanupFinalizer` during teardown) can block another namespace. Demoted duplicates without finalizers in other namespaces cannot cause cross-namespace denial of service.
4. **Automatic Re-Evaluation**: When an older conflicting `PodMigration` is deleted or changes its storage location, blocked peer `PodMigration`s in other namespaces that share the bucket are automatically re-enqueued and re-evaluated, promoting to `Ready=True`.

---

## Upgrade Considerations & Storage Isolation Mode

When upgrading an existing cluster where multiple namespaces historically configured an un-namespaced shared path (e.g. the legacy README sample `gs://<bucket>/snapshots`):

- **Default Migration Window (`--storage-isolation-mode=warn`)**: Operators upgrading existing clusters with legacy shared paths can rely on `--storage-isolation-mode=warn` (the default). In `warn` mode:
  - Conflicts and un-namespaced paths log warnings and emit Kubernetes `Warning` events with reason `StorageIsolationWarning`.
  - Reconciler does not mark `Ready=False` or delete storage configs, allowing existing workloads to migrate while operators update `PodMigration` manifests to namespace-prefixed paths (`gs://<bucket>/<namespace>/snapshots`).
- **Strict Enforcement (`--storage-isolation-mode=enforce`)**: To fail closed immediately against all un-scoped paths and cross-namespace conflicts, configure `--storage-isolation-mode=enforce`. Newer or conflicting `PodMigration` resources transition to `Ready=False` with `ConflictingStorageLocation` and tear down rendered storage resources once in-flight migrations finish.

---

## Operator Setup & Verification Checklist

Before onboarding multiple tenant namespaces to a shared cluster:

1. **Run `patches/setup-storage.sh`**:
   - For single-tenant dedicated bucket (default):
     ```bash
     ./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION] single-tenant
     ```
   - For multi-tenant shared bucket:
     ```bash
     ./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION] multi-tenant "tenant-a,tenant-b"
     ```
2. **Apply Admission Policy**:
   ```bash
   kubectl apply -f patches/storage-isolation-admission-policy.yaml
   ```
3. **Configure Namespace PodMigrations with Scoped Paths**:
   ```yaml
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigration
   metadata:
     name: tenant-a-migration
     namespace: tenant-a
   spec:
     storage:
       location: gs://my-shared-bucket/tenant-a/snapshots
       tokenSource: federatedP4SA
   ```
4. **Automated Verification**:
   Run the test suite to verify storage isolation:
   ```bash
   ./verification-suite/test_storage_multitenancy.sh --self-test
   ```
