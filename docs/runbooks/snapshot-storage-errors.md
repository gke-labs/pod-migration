# Operator Runbook: Snapshot Storage & Retrieval Errors

## Alerts Covered

- **`PodMigrationSnapshotStorageErrors`** (`severity: warning`): A pod restore verdict completed with `snapshot_deleting`, `snapshot_gone`, or `failed` (where `failed` represents engine restore failure).
- **`PodMigrationJobFailuresHigh`** (`severity: warning`): Pod migration jobs are failing terminally (`outcome="failed"`), driven by object storage authentication, quota, engine, PDB refusals, fence failures, or network connectivity failures.

---

## 1. Operational Impact

Snapshot storage is the critical state persistence medium connecting the source pod's checkpoint to the destination pod's restore. When storage fails:

- Checkpoints cannot be uploaded to Google Cloud Storage (GCS), S3, or MinIO.
- Restores cannot download checkpoint archives, causing instant cold-start fallbacks or failed migrations.
- In-flight checkpoint archives may be orphaned in bucket storage, consuming disk quota.

---

## 2. Root Cause Analysis

Storage failures typically fall into four buckets:

1. **IAM & Workload Identity Misconfigurations:** The service account bound to the source pod or CRIU node agent lacks `roles/storage.objectUser` or `roles/storage.bucketViewer` on the target bucket (`gs://<bucket>/path`).
2. **Premature Bucket Lifecycle Garbage Collection:** An aggressive GCS Object Lifecycle Rule (e.g. `Delete after 1 day` or `Age > 0 days`) purged the snapshot file while a pod was queued for restore.
3. **Storage Quota & Rate Limiting:** High-frequency migration drains generated hundreds of concurrent multipart uploads, exceeding GCS bucket QPS or regional API quotas.
4. **Egress NetworkPolicy / Firewall Blocks:** Egress to Google APIs (`storage.googleapis.com:443`) or the cluster's internal storage endpoint was restricted by network policies applied to the namespace.

---

## 3. Diagnostic Workflow

### Step 1: Inspect Failed PMJ Storage Errors

Query failed PMJs using `jsonpath` (field selectors on `status.phase` are not supported on custom resources):

```bash
kubectl get pmj -A -o jsonpath='{range .items[?(@.status.phase=="Failed")]}{"Namespace: "}{.metadata.namespace}{"\tPMJ: "}{.metadata.name}{"\tPod: "}{.spec.podRef.name}{"\tConditions: "}{range .status.conditions}{.type}{"="}{.status}{" ("}{.reason}{": "}{.message}{") "}{end}{"\n"}{end}'
```

Look for explicit storage error messages:

- `403 Forbidden: AccessDeniedException`
- `404 Not Found: Object does not exist`
- `Storage location not reachable`
- `CRIU agent failed to upload checkpoint archive`

### Step 2: Validate Storage Bucket Configuration

Inspect the active `PodMigration` configuration in the affected namespace (`spec.storage.location` is configured on the `PodMigration` resource, which sets storage policies for all PMJs in that namespace):

```bash
kubectl get podmigration -n <namespace> -o yaml
```

Identify `spec.storage.location` (e.g. `gs://my-cluster-snapshots/tenants/team-a`).

### Step 3: Test Object Storage Access from Within the Cluster

Launch a diagnostic pod in the target namespace using the workload's Kubernetes Service Account (KSA):

```bash
KSA=$(kubectl get pod <source-pod> -n <namespace> -o jsonpath='{.spec.serviceAccountName}')

kubectl run storage-canary --rm -it --restart=Never \
  --namespace=<namespace> \
  --serviceaccount="$KSA" \
  --image=google/cloud-sdk:alpine \
  -- gsutil ls gs://<bucket-name>/
```

- If `403 AccessDeniedException`: Workload Identity is not properly bound to the Google Service Account (GSA), or the GSA lacks bucket permissions.
- If network connection times out: Egress to `storage.googleapis.com` is blocked by a `NetworkPolicy`.

### Step 4: Check GCS Bucket Lifecycle Rules

```bash
gsutil lifecycle get gs://<bucket-name>
```

Verify that lifecycle rules do not delete objects with age < 7 days. Pod migrations should own snapshot lifecycle via controller garbage collection, not aggressive bucket TTLs.

---

## 4. Safe Remediation Procedures

### Remediation A: Restore Workload Identity Bucket Permissions

For GKE Workload Identity environments:

1. Grant storage access on the target bucket:
   ```bash
   gcloud storage buckets add-iam-policy-binding gs://<bucket-name> \
     --member="principal://iam.googleapis.com/projects/<PROJECT_NUM>/locations/global/workloadIdentityPools/<PROJECT_ID>.svc.id.goog/subject/ns/<namespace>/sa/<ksa-name>" \
     --role="roles/storage.objectUser"
   ```
2. For quick development/testing cluster fixes, rerun the bundled setup script:
   ```bash
   ./patches/setup-storage.sh <bucket-name>
   ```

### Remediation B: Address NetworkPolicy Egress Blocks

If the namespace enforces default-deny egress, add an egress rule permitting HTTPS traffic to the Google Cloud APIs:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: allow-gcs-egress
  namespace: <namespace>
spec:
  podSelector: {}
  policyTypes:
    - Egress
  egress:
    - to: []
      ports:
        - protocol: TCP
          port: 443
```

### Remediation C: Manage Storage Lifecycle & Orphan Cleanup

If failed migrations left stranded snapshots in object storage:

> ⚠️ **DO NOT run bulk wildcard deletion (`gcloud storage rm gs://...`) during active operations!** Indiscriminate wildcard deletion risks deleting snapshots for migrations currently in flight.

Use the controller's automated snapshot retention and orphan sweep:

- `--snapshot-retention` (default `0`): Controls how long a job that concluded `Succeeded` (restore verified or inferred) or `Failed` keeps the PodSnapshot its trigger created before the controller deletes it. A value of `0` deletes it as soon as nothing can restore from it. Jobs that concluded `SucceededWithoutRestore` keep their snapshot until they are garbage collected (30m after conclusion).
- `--orphan-snapshot-sweep-interval` (default `10m`): Controls how often the leader controller sweeps orphaned PodSnapshots whose PodMigrationJob is gone and that no pod, trigger, or job references. Deleting a PodSnapshot directs its engine to delete the checkpoint archive.
- `--orphan-snapshot-min-age` (default `15m`): Enforces a minimum age from creation before an orphaned snapshot is eligible for sweep deletion.
- To trigger an immediate sweep cycle without waiting for the next periodic interval, restart the controller leader replica (the newly elected leader executes an initial orphan sweep upon startup):
  ```bash
  kubectl rollout restart deployment pod-migration-controller -n pod-migration-system
  ```

---

## 5. Fault-Injection Verification

### Test Case: Simulating Storage Permission Revocation

1. Create a namespace with a `PodMigration` pointing to an inaccessible or non-existent bucket:
   ```yaml
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigration
   metadata:
     name: test-storage-isolation
     namespace: default
   spec:
     storage:
       location: gs://non-existent-permission-denied-bucket/default/fault
   ```
2. Trigger a `PodMigrationJob` for a workload in that namespace.
3. Observe controller execution:
   ```bash
   kubectl describe pmj <pmj-name> -n default
   ```
4. Verify that `pod_migration_outcomes_total{outcome="failed"}` increments and triggers `PodMigrationJobFailuresHigh`.
5. Clean up:
   ```bash
   kubectl delete pmj <pmj-name> -n default
   kubectl delete podmigration test-storage-isolation -n default
   ```
