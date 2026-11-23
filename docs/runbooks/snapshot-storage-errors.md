# Operator Runbook: Snapshot Storage & Retrieval Errors

## Alerts Covered

- **`PodMigrationSnapshotStorageErrors`** (`severity: warning`): A pod restore verdict completed with `snapshot_deleting`, `snapshot_gone`, or `failed` due to storage-layer failures.
- **`PodMigrationSnapshotStoragePermanentFailures`** (`severity: critical`): Pod migration jobs are failing terminally (`outcome="failed"`), frequently driven by object storage authentication, quota, or network connectivity failures.

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

```bash
kubectl get pmj -A --field-selector status.phase=Failed -o custom-columns='NAMESPACE:.metadata.namespace,NAME:.metadata.name,ERROR:.status.error,ENGINE:.spec.engine,LOCATION:.spec.storage.location'
```

Look for explicit storage error messages:

- `403 Forbidden: AccessDeniedException`
- `404 Not Found: Object does not exist`
- `Storage location not reachable`
- `CRIU agent failed to upload checkpoint archive`

### Step 2: Validate Storage Bucket Configuration

Inspect the active `PodMigration` configuration in the affected namespace:

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

### Remediation C: Purge Orphaned Snapshot Files

If active migrations failed but left stranded snapshots in the bucket:

```bash
# List snapshots older than 24 hours under the cluster prefix
gcloud storage rm --recursive gs://<bucket-name>/cluster-prefix/orphaned-pmj-*
```

---

## 5. Fault-Injection Verification

### Test Case: Simulating Storage Permission Revocation

1. Create a test `PodMigrationJob` pointing to an inaccessible or non-existent bucket:
   ```yaml
   apiVersion: podmigration.gke.io/v1alpha1
   kind: PodMigrationJob
   metadata:
     name: test-storage-perm-fault
     namespace: default
   spec:
     sourcePod:
       name: test-workload-0
     storage:
       location: gs://non-existent-permission-denied-bucket/fault
     timeout: 30s
   ```
2. Apply the manifest and observe controller execution:
   ```bash
   kubectl apply -f test-storage-fault.yaml
   ```
3. Check the PMJ error condition:
   ```bash
   kubectl describe pmj test-storage-perm-fault -n default
   ```
4. Verify that `pod_migration_outcomes_total{outcome="failed"}` increments and triggers `PodMigrationSnapshotStoragePermanentFailures`.
5. Clean up:
   ```bash
   kubectl delete pmj test-storage-perm-fault -n default
   ```
