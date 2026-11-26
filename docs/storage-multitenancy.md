# Multi-Tenant Snapshot Storage Architecture & Guidance

## Overview

A Kubernetes pod checkpoint taken during live migration contains a full memory dump of application state. Checkpoints frequently hold confidential runtime data, including database passwords, TLS private keys, session tokens, and in-memory caches.

In a multi-tenant or shared-cluster deployment where workloads from different teams or customers share nodes and infrastructure, **snapshot storage must be strictly isolated by access, not merely separated by name or object path prefixes**.

This document defines the supported snapshot storage layouts for multi-tenant clusters, explains the controller's fail-closed collision prevention, and details operator verification procedures (part of issue [#278](https://github.com/gke-labs/pod-migration/issues/278)).

---

## Privileged Fields, Storage Objects & RBAC Governance

`spec.storage.location` on a `PodMigration`, as well as the managed `PodSnapshotStorageConfig` (PSSC) and `PodSnapshotPolicy` (PSP) resources, are **privileged configurations**:

1. **`spec.storage.location` Privilege & Tenant Boundaries**: Because the storage location dictates which Cloud Storage bucket and path prefix the snapshot engines read and write checkpoints to, tenant workload owners should **not** have unconstrained `create` or `update` RBAC permissions on `podmigrations.podmigration.gke.io`. Cluster operators or GitOps automation should provision and manage `PodMigration` objects.
2. **`PodSnapshotStorageConfig` & `PodSnapshotPolicy` Privileged Status**: In both GKE Pod Snapshots and CRIU environments, `PodSnapshotStorageConfig` defines the physical storage backend, bucket name, and path prefix used for checkpoint storage and path-restricted token minting; `PodSnapshotPolicy` binds checkpoint triggers and stop behaviors. **Only the Pod Migration Controller operator and cluster administrators may create, update, or edit `PodSnapshotStorageConfig` and `PodSnapshotPolicy` resources.** Tenant workloads and non-admin users must be denied `create`, `update`, `patch`, or `delete` RBAC permissions on `podsnapshotstorageconfigs.podsnapshot.gke.io` and `podsnapshotpolicies.podsnapshot.gke.io`. If tenant workloads could create or modify PSSCs directly, they could redirect checkpoint storage to target other tenants' bucket prefixes, bypass path restrictions, or manipulate retention and deletion policies.
3. **Admission Enforcement**: When tenants are permitted to self-manage `PodMigration` resources within their namespaces, operators must deploy the `pod-migration-storage-location` ValidatingAdmissionPolicy (`patches/storage-isolation-admission-policy.yaml`). This policy enforces at admission time that any storage path under a bucket begins strictly with the object's `metadata.namespace` (`gs://<bucket>/<namespace>/...`) and contains no `.`, `..`, empty segments (`//`), or whitespace (space `' '` and tab `'\t'`). Note that the admission policy enforces namespace scoping unconditionally and rejects the bucket root even if the operator runs the controller with `--allow-bucket-root-storage=true`.

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
  2. **GKE Node Agent Role**: The GKE node service account is granted `roles/storage.admin` on the bucket. Per official GKE Pod Snapshots documentation ("Prepare for Pod snapshots"), GKE node agents (`service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com`) require `roles/storage.admin` on the bucket to manage bucket metadata and mint path-restricted session tokens for `federatedP4SA`:
     ```bash
     gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
       --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com" \
       --role="roles/storage.admin"
     ```
  3. **Per-Namespace Managed Folders**: Managed folders are created best-effort for each tenant namespace (creation errors are swallowed, and folders receive no folder-level IAM bindings):
     ```bash
     gcloud storage managed-folders create "gs://${BUCKET_NAME}/tenant-a"
     gcloud storage managed-folders create "gs://${BUCKET_NAME}/tenant-b"
     ```
  4. **Path-Restricted Tokens**: At checkpoint/restore time, "token restricted to the managed folder" rests entirely on GKE's `federatedP4SA` scoping to the PSSC path: the GKE node agent mints a short-lived token restricted specifically to the tenant's configured storage location prefix (`gs://${BUCKET_NAME}/${TENANT_NAMESPACE}/`).
  5. Containers running in `tenant-b` cannot read, list, or overwrite checkpoints under `tenant-a`'s prefix because their issued credentials lack permission outside their own path.

---

## CRIU Engine Storage Isolation Model

The CRIU snapshot engine (`engines/criu`) operates with dedicated node agent credentials rather than per-pod tokens:

1. **Node Agent STS Identity (#290)**: The CRIU daemonset agent obtains scoped Cloud Storage access via STS credentials bound to the CRIU engine service account.
2. **Namespace Path Scoping**: Storage path isolation within the shared bucket is enforced server-side:
   - `util.ParseStorageLocation` rejects dot-dot (`..`), dot (`.`), and empty (`//`) path segments, as well as whitespace inside path segments.
   - The controller and `pod-migration-storage-location` ValidatingAdmissionPolicy require `spec.storage.location` paths to be scoped to the namespace (`gs://<bucket>/<namespace>/...`).
   - The CRIU agent structures checkpoint archive keys as `<path>/<snapshot-name>/...`, confining objects to the tenant's namespace prefix.
3. **Cross-Namespace Restore Guardrail (#287)**: The `pod-migrate-psengine-restore-status` ValidatingAdmissionPolicy blocks unauthorized mutations to restore annotations (`pod-migrate.io/psengine-restore`), ensuring workloads in namespace B cannot trigger or spoof restores from namespace A checkpoints.

---

## Controller Guardrail: Conflicting & Un-Scoped Storage Locations

The `PodMigrationReconciler` automatically enforces cross-namespace storage location isolation:

1. **Namespace Scoping (Always Enforced)**: The controller always requires the storage location's first path segment to match `metadata.namespace` (`gs://<bucket>/<namespace>/...`) and have a non-empty path. Specifying a path belonging to another namespace, or targeting the bucket root without explicit operator opt-in (`--allow-bucket-root-storage`), is refused with condition `Ready=False` and reason `UnscopedStorageLocation` (or `InvalidStorageLocation` for syntax errors).
2. **Cross-Namespace Collision Refusal with Root Storage**: Two properly scoped paths in different namespaces (e.g. `gs://shared-bucket/tenant-a/snapshots` and `gs://shared-bucket/tenant-b/snapshots`) never conflict because their first segments are disjoint. Cross-namespace collision refusal applies when an operator enables `--allow-bucket-root-storage=true`:
   - If one namespace targets the bucket root (`gs://shared-bucket`) and another namespace targets a subpath (`gs://shared-bucket/tenant-a`), or two namespaces both target the root bucket, the younger CR conflicts with the older CR.
   - Status: `Ready=False` (in `enforce` mode) or `Ready=True` with a `StorageIsolationWarning` event (in `warn` mode).
   - Reason: `ConflictingStorageLocation`
   - Event: A `Warning` event with reason `ConflictingStorageLocation` (or `StorageIsolationWarning` in warn mode) is emitted on the conflicting `PodMigration`.
3. **Deterministic Singleton Precedence & Scoping Protection**: Only active singleton `PodMigration`s (or ones holding `StorageCleanupFinalizer` during teardown) that pass their own namespace check can block another namespace. An un-scoped or invalid PodMigration in namespace A cannot cause denial of service against namespace B. Demoted duplicates without finalizers in other namespaces cannot cause cross-namespace denial of service.
4. **Automatic Re-Evaluation**: When an older conflicting `PodMigration` is deleted or changes its storage location, blocked peer `PodMigration`s in other namespaces that share the bucket are automatically re-enqueued and re-evaluated, promoting to `Ready=True`.

---

## Upgrade Considerations & Release Note

### Breaking Change: Mandatory Storage Namespace Scoping

When upgrading to this release:

- **What breaks**: Any existing `PodMigration` resource specifying an un-scoped storage location (such as the legacy documentation sample `gs://<bucket>/snapshots` or a bucket root `gs://<bucket>`) fails namespace scoping validation.
  On upgrade, the controller marks `Ready=False` with reason `UnscopedStorageLocation`. Active in-flight migrations are permitted to drain (up to the deletion deferral window). Once drained, the controller cleans up the managed `PodSnapshotStorageConfig` (PSSC) and `PodSnapshotPolicy` (PSP) in that namespace.
  The eviction interception webhook (`eviction_webhook.go:380`) searches for a `Ready` PodMigration policy matching the pod; finding none, it logs a warning and permits standard cold pod evictions without checkpointing. **Live migration quietly stops for un-scoped namespaces.**
- **How to tell**:
  1. Inspect PodMigration status:
     ```bash
     kubectl get podmigrations -A
     ```
     Check for `READY: False` with reason `UnscopedStorageLocation`.
  2. Inspect Warning events:
     ```bash
     kubectl get events -A --field-selector reason=UnscopedStorageLocation
     ```
  3. Inspect controller logs:
     ```bash
     kubectl logs -n pod-migration-system deployment/pod-migration-controller | grep "Refusing un-scoped storage location"
     ```
- **How to update locations**:
  Update each `PodMigration` manifest so `spec.storage.location` begins with the object's namespace:
  ```yaml
  spec:
    storage:
      location: gs://<bucket>/<namespace>/snapshots
  ```
  (For example, in namespace `default`, change `gs://my-bucket/snapshots` to `gs://my-bucket/default/snapshots`).
  Once updated, the controller re-reconciles immediately, provisions the PSSC and PSP, and restores `Ready=True`.
- **Root Storage Opt-In & `--storage-isolation-mode`**:
  If operators intentionally run a single-tenant cluster where the entire bucket is dedicated to one namespace without path prefixes, `--allow-bucket-root-storage=true` can be passed to the controller.
  When root storage is enabled, `--storage-isolation-mode` controls collision behavior between the root bucket and subpaths:
  - `--storage-isolation-mode=warn` (default): logs warnings and emits `StorageIsolationWarning` events on collisions without setting `Ready=False`, allowing existing migrations to proceed during maintenance windows.
  - `--storage-isolation-mode=enforce`: strictly fails closed on collisions (`Ready=False`, `ConflictingStorageLocation`).
    Note that `--storage-isolation-mode` does NOT permit un-scoped paths (`gs://<bucket>/snapshots`); namespace scoping is enforced in all modes.

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
