# GKE Pod Migration (Webhook-based Eviction Interception)

This repository contains the blueprints, patches, configuration templates, and E2E verification test suites for GKE Pod Migration. 

Specifically, this implements the **Manual + Stop Webhook Flow** (using GKE Pod Snapshots with a manual trigger policy configured with `postCheckpoint: stop` to intercept Pod evictions, checkpoint RAM state to GCS, and restore statefully on target nodes).

---

## 1. Migration Compatibility Matrix (E2E Verified)

Every workload in this matrix has been verified through real E2E eviction migrations on a GKE Standard node pool.

> [!NOTE]
> - **Job Rescheduling**: Workloads of type `Job` (e.g., `go`, `node`) are now supported for resilient rescheduling using a Status Mutating Webhook.
> - **NATS Caveat**: `nats` is marked as flaky due to a transient runtime-level deadlock during checkpointing; see notes below for mitigation.

| Application | Category | Verdict | Required Workarounds & Bypass Configurations |
| :--- | :--- | :--- | :--- |
| **node** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet (connection tokens survive) and Job (rescheduled via webhook). Out-of-the-box support. |
| **go** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet (counters survive) and Job (rescheduled via webhook). Out-of-the-box support. |
| **redis** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Extremely fast migration. |
| **valkey** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Extremely fast migration. |
| **mysql** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **mariadb** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **memcached** | datastore | ✅ **SURVIVED** | Pure in-memory cache blocks restored. |
| **dragonfly** | datastore | ✅ **SURVIVED** | **epoll Bypass**: Requires epoll forcing flag (`--force_epoll`). Verified with redis-client. |
| **vault** | secrets | ✅ **SURVIVED** | Dev-mode secrets state restored. |
| **consul** | coordination | ✅ **SURVIVED** | Dev-mode memory key-value databases survive. |
| **etcd** | coordination | ✅ **SURVIVED** | BoltDB storage writes successfully restored. |
| **nats** | streaming | ⚠️ **SURVIVED (FLAKY)** | **Deadlock Flake / Retry Caveat**: Memory jetstream offsets restored. Encountered runtime-level deadlock on 1st run (stuck in Terminating). Succeeded on retry. Recommend controller timeout recovery. |
| **zookeeper** | coordination | ✅ **SURVIVED** | **emptyDir Path Redirect**: Redirect `ZOO_DATA_DIR` away from Kubelet `emptyDir` mounts (e.g. to `/tmp/zookeeper`) to prevent walk errors. |
| **kafka** (KRaft) | streaming | ✅ **SURVIVED** | **JVM Metrics Bypass**: Inject environment variable `KAFKA_OPTS="-XX:-UseContainerSupport"` to avoid cgroups mismatch crashes on target nodes. |
| **postgres** | datastore | ✅ **SURVIVED** | Works out-of-the-box (uses guest POSIX shared memory). Requires setting `PGDATA` to container local directories. |
| **minio** | datastore | ✅ **SURVIVED** | Redirect storage paths to container writable layers to avoid emptyDir mount walk failures. |
| **nginx** | proxy | ✅ **SERVED** | Stateless proxies restore and handle reconnected traffic. |
| **haproxy** | proxy | ✅ **SERVED** | Stateless proxies restore and handle reconnected traffic. |
| **traefik** | proxy | ✅ **SERVED** | Stateless routers restore and handle reconnected traffic. |
| **caddy** | proxy | ✅ **SERVED** | Stateless routers restore and handle reconnected traffic. |
| **python** (HTTP) | app | ✅ **SERVED** | Stateless python workers survive. |
| **mongodb** | datastore | ❌ **FAILED** | WiredTiger storage engine locks and blocks on sandboxed `io_uring` seccomp. |
| **cassandra** | datastore | ❌ **FAILED** | Large JVM heaps mismatch host cgroups descriptors post-restore. |
| **cockroachdb** | datastore | ❌ **FAILED** | Raft synchronization timeouts and socket reset crashes post-restore. |
| **clickhouse** | datastore | ❌ **FAILED** | Columnar block datastore sync locks and file descriptor leaks. |
| **rabbitmq / couchdb** | streaming | ❌ **FAILED** | Erlang BEAM runtime epoll and green thread scheduler structures cannot be serialized. |
| **prometheus** | monitoring | ❌ **REFUSED** | Active WAL memory mappings exceed serialization limits under both runtimes. |
| **elasticsearch** | search | ❌ **REFUSED** | Heavy `fsnotify` directory watches cannot be serialized. |

---

## 2. Installation Guide

To deploy this live migration runtime to your GKE test cluster using the native GKE Pod Snapshots:

### Step 1: Create GKE Cluster & Enable Pod Snapshot
Create a GKE Standard cluster on the Rapid release channel with the GKE Pod Snapshots addon enabled:
```bash
gcloud container clusters create pod-migration-cluster \
  --release-channel=rapid \
  --workload-pool=<YOUR_PROJECT>.svc.id.goog \
  --enable-pod-snapshots \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### Step 2: Install cert-manager
Install cert-manager to generate and manage TLS certificates for the admission webhooks:
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=5m -n cert-manager deployment/cert-manager-webhook
```

### Step 3: Provision gVisor Node Pool
Create a GKE node pool with gVisor sandboxing enabled:
```bash
gcloud container node-pools create gvisor-pool \
  --cluster=pod-migration-cluster \
  --sandbox=type=gvisor \
  --machine-type=n2-standard-4 \
  --num-nodes=2 \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### Step 4: Configure Snapshot Storage & Workload Identity
Run the provided setup script to automatically create the GCS bucket, configure IAM permissions for Workload Identity, and create the test Kubernetes Service Account (KSA):

```bash
chmod +x patches/setup-storage.sh
./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]
```

*This script performs the following:*
- Creates a GCS bucket `gs://<BUCKET_NAME>` with uniform bucket-level access and soft-delete disabled.
- Binds IAM roles `roles/storage.objectUser` and `roles/storage.bucketViewer` to all KSAs in the `default` namespace and the GKE container engine robot service account.
- Creates KSA `pm-test-ksa` in the `default` namespace.

*(Note: The script may attempt to apply a static PSSC named `lpm-test-storage`. The Pod Migration Controller will dynamically generate its own PSSC (`pssc-<hash>`) when you deploy the `PodMigration` resource, so the static PSSC is redundant).*

### Step 5: Install CRDs
Install the custom resource definitions for the pod migration controller (PodMigrations and PodMigrationJobs):
```bash
kubectl apply -f controller/config/crd/bases/
```

### Step 6: Deploy the Pod Migration Controller & Webhooks
The controller manager orchestrates the migration lifecycle and hosts the admission webhooks, including the scheduling gate and the status mutating webhook for Job rescheduling.

1.  **Build and push the controller image:**

    ```bash
    docker build -t <YOUR_REGISTRY>/pod-migration-controller:latest -f controller/Dockerfile controller/
    docker push <YOUR_REGISTRY>/pod-migration-controller:latest
    ```

2.  **Configure the GCS Bucket:**

    Edit `controller/podmigration-config.yaml` to configure your target GCS bucket for snapshots:

    ```yaml
    spec:
      storage:
        location: gs://<YOUR_BUCKET_NAME>/snapshots
    ```

3.  **Deploy the Controller:**

    Apply the manifests by replacing the `<YOUR_CONTROLLER_IMAGE>` placeholder:

    ```bash
    sed 's|<YOUR_CONTROLLER_IMAGE>|<YOUR_REGISTRY>/pod-migration-controller:latest|' controller/deploy.yaml | kubectl apply -f -
    
    # Apply the storage config
    kubectl apply -f controller/podmigration-config.yaml
    
    # Verify deployment status
    kubectl rollout status deployment/pod-migration-controller -n pod-migration-system
    ```

### Step 7: Deploy Validating Policies (Optional)
To reject incompatible workloads (e.g. BEAM/fsnotify) at admission time:
```bash
kubectl apply -f patches/gke-pod-snapshot-admission-webhook.yaml
```

---

## 3. Job Rescheduling Configuration

To support resilient rescheduling of Jobs during migration, the system uses a **Status Mutating Webhook** combined with Kubernetes **Job Pod Failure Policy**.

### How it works:
1. When a migrating Job Pod is terminated, the `pod-migration-controller`'s status mutating webhook (`/mutate-v1-pod-status`) intercepts the status update.
2. It mutates the Pod phase to `Failed` and sets the container exit code to `137`.
3. The Job controller interprets this exit code according to the Job's `podFailurePolicy`.
4. If configured correctly, the Job controller ignores this failure and recreates the Pod (which then restores from the snapshot) without counting it against the Job's `backoffLimit` retry budget.

### Job Configuration Example:
To enable this, your Job manifest must:
1. Enable migration via labels.
2. Configure `podFailurePolicy` to `Ignore` exit code `137`.

Here is an example snippet (from `verification-suite/manifests/pm-go-job.yaml`):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: pm-go-job
spec:
  backoffLimit: 4
  podFailurePolicy:
    rules:
      - action: Ignore
        onExitCodes:
          operator: In
          values: [137]
  template:
    metadata:
      labels:
        pod-migration.gke.io/enabled: "true"
        live-migration.gke.io/enabled: "true"
    spec:
      runtimeClassName: gvisor
      restartPolicy: Never
      # ... rest of the spec
```

---

## 4. Workload Verification & Manifest Templates

This repository contains production-ready YAML templates for trying out pod migration on your workloads under the `verification-suite/manifests/` directory:

```
verification-suite/manifests/
├── pm-valkey-statefulset.yaml       # Valkey StatefulSet
├── pm-mysql-statefulset.yaml        # MySQL (with innodb AIO override)
├── pm-zookeeper-statefulset.yaml    # Zookeeper (with path redirection)
├── pm-kafka-statefulset.yaml        # Kafka (with JVM container bypass)
└── ...
```

### Running E2E Verification
You can run E2E live-migration verification for an application using the driver script `verification-suite/run_app_validation.sh`:
```bash
# Run validation on Valkey
./verification-suite/run_app_validation.sh valkey

# Run validation on MySQL
./verification-suite/run_app_validation.sh mysql
```

---

## 5. What to Expect during Verification

### Expected Log Output from `run_app_validation.sh`

#### Valkey Verification
When running `./verification-suite/run_app_validation.sh valkey`, you should expect logs similar to:
```
[*] Deploying Valkey StatefulSet...
statefulset.apps/pm-valkey created
service/pm-valkey-service created
[*] Waiting for Valkey pod to be Ready...
pod/pm-valkey-0 condition met
[*] Seeding state in Valkey: migkey -> valkey-nonce-1718900000
OK
[*] Pod is running on node: gke-pod-migration-cluster-gvisor-pool-abcdef-1234
[*] Draining node gke-pod-migration-cluster-gvisor-pool-abcdef-1234...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 cordoned
evicting pod pm-system/pm-controller-manager-...
evicting pod default/pm-valkey-0
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 drained
[*] Restoring node gke-pod-migration-cluster-gvisor-pool-abcdef-1234 (uncordon)...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 uncordoned
[*] Waiting for restored Valkey pod to be Ready...
pod/pm-valkey-0 condition met
[*] Verifying state...
[+] Retrieved value: valkey-nonce-1718900000
[SUCCESS] Valkey E2E Live Migration Succeeded. State survived!
```

#### MySQL Verification
When running `./verification-suite/run_app_validation.sh mysql`, you should expect logs similar to:
```
[*] Deploying MySQL StatefulSet (Native AIO Disabled)...
statefulset.apps/pm-mysql created
service/pm-mysql-service created
[*] Waiting for MySQL pod to be Ready...
pod/pm-mysql-0 condition met
[*] Seeding state in MySQL: Table durability_test -> mysql-nonce-1718900000
[*] Pod is running on node: gke-pod-migration-cluster-gvisor-pool-abcdef-1234
[*] Draining node gke-pod-migration-cluster-gvisor-pool-abcdef-1234...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 cordoned
evicting pod default/pm-mysql-0
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 drained
[*] Restoring node gke-pod-migration-cluster-gvisor-pool-abcdef-1234 (uncordon)...
node/gke-pod-migration-cluster-gvisor-pool-abcdef-1234 uncordoned
[*] Waiting for restored MySQL pod to be Ready...
pod/pm-mysql-0 condition met
[*] Verifying state...
[+] Retrieved value: mysql-nonce-1718900000
[SUCCESS] MySQL E2E Live Migration Succeeded. State survived!
```

### Manual Verification Guide

If you prefer to verify pod migration manually or to understand the mechanics, you can use `kubectl` commands.

#### Step 1: Deploy a workload (e.g., Valkey)
```bash
kubectl apply -f verification-suite/manifests/pm-valkey-statefulset.yaml
kubectl wait --for=condition=Ready pod/pm-valkey-0 --timeout=120s
```

#### Step 2: Seed State Before Eviction
Exec into the pod to set a value:
```bash
kubectl exec pm-valkey-0 -- valkey-cli set testkey "mkey-data-123"
```
Verify the value is written:
```bash
kubectl exec pm-valkey-0 -- valkey-cli get testkey
# Expected output: "mkey-data-123"
```

#### Step 3: Trigger Node Eviction
Find the node the pod is running on:
```bash
NODE=$(kubectl get pod pm-valkey-0 -o jsonpath='{.spec.nodeName}')
echo "Evicting from node: $NODE"
```
Drain the node to trigger pod eviction (and state snapshotting to GCS):
```bash
kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
```

#### Step 4: Uncordon the node
Make the node schedulable again:
```bash
kubectl uncordon "$NODE"
```

#### Step 5: Verify State After Restoration
Wait for the pod to become Ready again:
```bash
kubectl wait --for=condition=Ready pod/pm-valkey-0 --timeout=120s
```
Exec into the restored pod to fetch the seeded key and confirm state durability:
```bash
kubectl exec pm-valkey-0 -- valkey-cli get testkey
# Expected output: "mkey-data-123"
```

---

## 6. Troubleshooting & Cleanup

### Bypassing GKE Validating Admission Policy for Snapshot Cleanup

GKE enforces a ValidatingAdmissionPolicy (`gke-pod-snapshot-validating-admission-policy`) that prevents manual edits to `podsnapshots` by anyone other than the GKE snapshot controller and agent. This blocks users from manually removing finalizers from stuck `podsnapshots` (e.g. when GCS upload fails or during test resets).

To override this check and perform cleanup:
1. **Disable the validation actions temporarily by patching the binding to "Audit" instead of "Deny":**
   ```bash
   kubectl patch validatingadmissionpolicybinding gke-pod-snapshot-vap-binding \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Audit"]}]'
   ```
2. **Remove finalizers and delete the stuck snapshots:**
   ```bash
   kubectl get podsnapshots -o json | jq -r '.items[].metadata.name' | xargs -I {} kubectl patch podsnapshot {} --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]' || true
   kubectl delete podsnapshots --all --timeout=15s
   ```
3. **Restore the validation actions back to "Deny, Audit":**
   ```bash
   kubectl patch validatingadmissionpolicybinding gke-pod-snapshot-vap-binding \
     --type=json -p='[{"op": "replace", "path": "/spec/validationActions", "value": ["Deny", "Audit"]}]'
   ```
