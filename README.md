# Pod Migration Controller

This controller provides **Pod Migration**—the ability to "move" a running Pod from one node to another while preserving its internal runtime state (including memory, CPU registers, and system sockets). It uses [GKE Pod Snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots) to reliably capture the state of the pod and restore it. This feature requires the use of the [GKE Sandbox](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/sandbox-pods), which is based on the [gVisor](https://gvisor.dev/) container runtime.

This controller implements Pod Migration by intercepting eviction requests (e.g. during node drains), triggering manual snapshots with container termination (`postCheckpoint: stop`), and managing the scheduling of replacement pods. This reduces downtime and prevents progress loss from evictions caused by:

- **Node upgrades:** When a node is cordoned and drained, the controller captures the state of the pod, preserving in-progress state for Jobs or reducing warm-restore time for stateful database engines.
- **VPA evictions:** When Vertical Pod Autoscaler evicts pods for resizing, the controller preserves the pod's state prior to eviction so the newly resized pod can resume normal operation faster.
- **Other causes:** Responds to manual evictions and third-party controllers triggering Pod evictions.

While this controller mitigates the effects of eviction, the migration is not completely transparent and the application will experience:
- A change in Pod identity (different Pod name suffix and IP address).
- Closed network connections that must be re-established by client-side retry logic.

---

## How it Works

The controller is implemented as a custom Operator consisting of the following core components:

### 1. Webhooks
- **Eviction Interception Webhook (`/validate-v1-pod-eviction`)**: Handled by `EvictionGate`. Intercepts Pod eviction API calls. If the Pod is opted-in (`pod-migration.gke.io/enabled: "true"`), it denies the eviction request with `429 (Too Many Requests)` to block immediate destruction, and spawns a `PodMigrationJob` (PMJ) CR to orchestrate the migration flow.
- **Replacement Webhook (`/mutate-v1-pod-scheduling-gate`)**: Handled by `PodGateInjector`. Intercepts Pod creation requests. For replacement pods, it injects the `gke.io/pod-migration-gate` scheduling gate to hold the pod in a pending state until volume detachment is complete, preventing replica collisions.
- **Status Mutating Webhook (`/mutate-v1-pod-status`)**: Handled by `PodStatusMutator`. Intercepts Pod status updates. For migrating Job-owned Pods, it overrides the final exit code to `137` and status to `Failed`. This allows the Kubernetes Job controller to reschedule the Pod without depleting its `backoffLimit` retry budget (see Job Rescheduling section).

### 2. Reconcilers
- **`PodMigration` (Config) Reconciler**: Watches `PodMigration` resources. It automatically provisions the required GKE `PodSnapshotStorageConfig` (PSSC) and manual `PodSnapshotPolicy` (PSP) resources with `postCheckpoint: stop` in the target namespace.
- **`PodMigrationJob` (Execution) Reconciler**: Coordinates the active migration loop:
  1. Creates a `PodSnapshotManualTrigger` (PSMT) targeting the source Pod to initiate snapshotting and container termination.
  2. Monitors the resulting `PodSnapshot` until it becomes `Ready` (uploaded to GCS).
  3. Transitions the job to `Evicting` phase, which signals the Eviction Webhook that it is safe to allow the source Pod deletion.
  4. Waits for the source Pod to be deleted and its volumes to fully detach from the GCE node.
  5. Transitions the job to `Succeeded`, signaling the Pod Gate Reconciler that it is safe to release the replacement Pod.
- **Pod Gate Reconciler**: Monitors replacement Pods injected with the `gke.io/pod-migration-gate` scheduling gate. It holds them in a pending state and releases the gate only after the matched `PodMigrationJob` reaches `Succeeded` (confirming snapshot readiness and volume detachment), then injects the GKE snapshot name annotation to force restore, and removes the scheduling gate to allow startup. It bypasses gate injection for normal scale-up pods.

---

## Prerequisites

Before deploying the Pod Migration Controller, ensure your cluster meets the following requirements:

### 1. GKE Standard Cluster with Pod Snapshots Enabled
A GKE Standard cluster with the GKE Pod Snapshots addon enabled.
- **Minimum GKE Version**: `1.36.0-gke.2253000` or later (required to support GKE Pod Snapshots with VPA and manual triggers).
- **Release Channel**: Depending on availability in your region, you may need to use the `Rapid` release channel to obtain a compatible version.

Example cluster creation command (using Rapid channel):
```bash
gcloud container clusters create pod-migration-cluster \
  --release-channel=rapid \
  --workload-pool=<YOUR_PROJECT>.svc.id.goog \
  --enable-pod-snapshots \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### 2. gVisor Node Pool
At least one node pool must have gVisor sandboxing enabled (`--sandbox=type=gvisor`) to run the sandboxed workloads:
```bash
gcloud container node-pools create gvisor-pool \
  --cluster=pod-migration-cluster \
  --sandbox=type=gvisor \
  --machine-type=n2-standard-4 \
  --num-nodes=2 \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### 3. cert-manager
`cert-manager` must be installed to manage TLS certificates for the admission webhooks:
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=5m -n cert-manager deployment/cert-manager-webhook
```

---

## Installation Guide

Follow these steps to deploy the Pod Migration Controller once the prerequisites are met:

### Step 1: One-Time Snapshot Storage & Workload Identity Configuration
Run the provided setup script to automatically create the GCS bucket, configure IAM permissions for Workload Identity, and create the test Kubernetes Service Account (KSA):

```bash
chmod +x patches/setup-storage.sh
./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]
```

*This script performs the following:*
- Creates a GCS bucket `gs://<BUCKET_NAME>` with uniform bucket-level access and soft-delete disabled.
- Binds IAM roles `roles/storage.objectUser` and `roles/storage.bucketViewer` to all KSAs in the `default` namespace and the GKE container engine robot service account.
- Creates KSA `pm-test-ksa` in the `default` namespace.

### Step 2: Install CRDs
Install the custom resource definitions for the pod migration controller:
```bash
kubectl apply -f controller/config/crd/bases/
```

### Step 3: Deploy the Pod Migration Controller
The controller manager orchestrates the migration lifecycle and hosts the admission webhooks.

1.  **Build and push the controller image:**
    ```bash
    make -C controller docker-build IMG=<YOUR_REGISTRY>/pod-migration-controller:latest
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

### Step 4: Deploy Validating Policies (Optional)
To reject incompatible workloads (e.g. BEAM/fsnotify) at admission time:
```bash
kubectl apply -f patches/gke-pod-snapshot-admission-webhook.yaml
```

---

## 1. Migration Compatibility Matrix (E2E Verified)

Every workload in this matrix has been verified through real E2E eviction migrations on a GKE Standard node pool.

> [!NOTE]
> - **Job Rescheduling**: Workloads of type `Job` (e.g., `go`, `node`) are supported for resilient rescheduling using the Status Mutating Webhook.
> - **NATS Caveat**: `nats` is marked as flaky due to a transient runtime-level deadlock during checkpointing; see troubleshooting notes below.

| Application | Category | Verdict | Required Workarounds & Bypass Configurations |
| :--- | :--- | :--- | :--- |
| **node** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet and Job (rescheduled via webhook). Out-of-the-box support. |
| **go** | app / Job | ✅ **SURVIVED** | Verified both as StatefulSet and Job (rescheduled via webhook). Out-of-the-box support. |
| **redis** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Very fast migration. |
| **valkey** | datastore | ✅ **SURVIVED** | Pure in-memory key-value store. Very fast migration. |
| **mysql** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **mariadb** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **memcached** | datastore | ✅ **SURVIVED** | Pure in-memory cache blocks restored. |
| **dragonfly** | datastore | ✅ **SURVIVED** | **epoll Bypass**: Requires epoll forcing flag (`--force_epoll`). Verified with redis-client. |
| **vault** | secrets | ✅ **SURVIVED** | Dev-mode secrets state restored. |
| **consul** | coordination | ✅ **SURVIVED** | Dev-mode memory key-value databases survive. |
| **etcd** | coordination | ✅ **SURVIVED** | BoltDB storage writes successfully restored. |
| **nats** | streaming | ⚠️ **SURVIVED (FLAKY)** | **Deadlock Flake / Retry Caveat**: Memory jetstream offsets restored. Encountered runtime-level deadlock on 1st run. Succeeded on retry. |
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
    spec:
      runtimeClassName: gvisor
      restartPolicy: Never
      # ... rest of the spec
```

---

## 4. How to Use Pod Migration

To migrate your workload using this controller:

### Step 1: Opt-in your Workload
Add the label `pod-migration.gke.io/enabled: "true"` to your workload Pod template and ensure the Pod uses the `gvisor` runtime.

Example (Deployment):
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
        pod-migration.gke.io/enabled: "true" # Enables migration orchestration
    spec:
      runtimeClassName: gvisor # Required for GKE Pod Snapshots
      containers:
        - name: app
          image: my-app-image:latest
```

### Step 2: Trigger Migration via Eviction
The controller automatically intercepts standard Kubernetes evictions and orchestrates the stateful migration. You can trigger this manually (e.g. for testing node upgrades) by draining the node the Pod is running on:

1. **Find the node the Pod is running on:**
   ```bash
   kubectl get pod -l app=my-app -o wide
   ```
2. **Drain the node to trigger eviction:**
   ```bash
   kubectl drain <node-name> --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
   ```
3. **Uncordon the node** once the migration starts to make it available again for rescheduling:
   ```bash
   kubectl uncordon <node-name>
   ```

---

## 5. Automated Verification (E2E Suite)

This repository contains production-ready YAML templates for trying out pod migration on your workloads under the `verification-suite/manifests/` directory.

### Running E2E Verification
You can run automated E2E pod migration verification for any of the 21 pre-configured applications using the driver script `verification-suite/run_app_validation.sh`:
```bash
# Run validation on Valkey
./verification-suite/run_app_validation.sh valkey

# Run validation on MySQL
./verification-suite/run_app_validation.sh mysql
```

---

## 6. Expected Verification Output

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

---

## 7. Troubleshooting & Cleanup

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
