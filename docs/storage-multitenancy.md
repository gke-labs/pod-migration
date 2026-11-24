# Multi-Tenant Snapshot Storage Architecture & Guidance

## Overview

A Kubernetes pod checkpoint taken during live migration contains a full memory dump of application state. Checkpoints frequently hold confidential runtime data, including database passwords, TLS private keys, session tokens, and in-memory caches.

In a multi-tenant or shared-cluster deployment where workloads from different teams or customers share nodes and infrastructure, **snapshot storage must be strictly isolated by access, not merely separated by name or object path prefixes**.

This document defines the supported snapshot storage layouts for multi-tenant clusters, explains the controller's fail-closed collision prevention, and details operator verification procedures (closing issue [#278](https://github.com/gke-labs/pod-migration/issues/278)).

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
- **Path Separation**: Each tenant namespace configures a distinct subpath:
  - `tenant-a`: `spec.storage.location: "gs://shared-snapshots-bucket/tenant-a"`
  - `tenant-b`: `spec.storage.location: "gs://shared-snapshots-bucket/tenant-b"`
- **Token Source**: `spec.storage.tokenSource: "federatedP4SA"`.
- **IAM Setup**:
  1. **Zero Bucket-Wide Workload Grants**: Workload Service Accounts (`KSA`) are **never** granted bucket-level IAM roles (`roles/storage.objectUser` or `roles/storage.bucketViewer`) on the shared bucket.
  2. **GKE Node Agent Role**: The GKE node service account is granted `roles/storage.admin` on the bucket:
     ```bash
     gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
       --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com" \
       --role="roles/storage.admin"
     ```
  3. **Path-Restricted Tokens**: At checkpoint/restore time, the GKE node agent mints a short-lived token restricted specifically to the tenant's managed folder path (`gs://${BUCKET_NAME}/${TENANT_PATH}/`).
  4. Containers running in `tenant-b` cannot read, list, or overwrite checkpoints under `tenant-a`'s prefix because their issued credentials lack permission outside their own path.

---

## Controller Guardrail: Conflicting Storage Locations

The `PodMigrationReconciler` automatically enforces cross-namespace storage location isolation:

1. **Exact & Prefix Collision Refusal**: If two `PodMigration` resources in different namespaces specify conflicting paths in the same bucket, the younger CR is refused:
   - Identical path: `gs://shared-bucket/snapshots` and `gs://shared-bucket/snapshots`.
   - Prefix containment: `gs://shared-bucket/team-a` and `gs://shared-bucket/team-a/nested`.
   - Bucket root: `gs://shared-bucket` and any subpath in `gs://shared-bucket`.
   - Status: `Ready=False`
   - Reason: `ConflictingStorageLocation`
   - Message: `Storage location "..." conflicts with active PodMigration <older-namespace>/<older-name>; shared storage locations across namespaces are refused for multi-tenant isolation`
   - Event: A `Warning` event with reason `ConflictingStorageLocation` is emitted on the refused `PodMigration`.
2. **Deterministic Singleton Precedence**: Only active singleton `PodMigration`s (or ones holding `StorageCleanupFinalizer` during teardown) can block another namespace. Demoted duplicates without finalizers in other namespaces cannot cause cross-namespace denial of service.
3. **Automatic Re-Evaluation**: When the older conflicting `PodMigration` is deleted or changes its storage location, blocked `PodMigration`s are automatically re-evaluated via bucket peer watches and a periodic 30-second backstop, unblocking and promoting to `Ready=True`.

---

## Operator Setup & Verification Checklist

Before onboarding multiple tenant namespaces to a shared cluster:

1. **Run `patches/setup-storage.sh` in Multi-Tenant Mode**:
   ```bash
   ./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION] multi-tenant
   ```
   Verify that:
   - The bucket has `--enable-hierarchical-namespace`.
   - The GKE node service account (`service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com`) has `roles/storage.admin`.
   - No `default` or tenant namespace Workload Identity pool has bucket-wide access.
2. **Configure Namespace PodMigrations with Distinct Paths and `federatedP4SA`**:
   ```yaml
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigration
   metadata:
     name: team-a-migration
     namespace: team-a
   spec:
     storage:
       location: gs://my-shared-bucket/team-a
       tokenSource: federatedP4SA
   ```
3. **Verify Refusal on Overlapping Paths**:
   Attempting to create a second `PodMigration` in namespace `team-b` with `location: gs://my-shared-bucket/team-a` must immediately show:
   ```bash
   kubectl get podmigration -n team-b -o jsonpath='{.items[0].status.conditions[?(@.type=="Ready")].reason}'
   # Output: ConflictingStorageLocation
   ```
4. **Automated Verification**:
   Run the test suite to verify storage isolation:
   ```bash
   ./verification-suite/test_storage_multitenancy.sh --self-test
   ```
