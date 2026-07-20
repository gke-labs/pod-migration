# GKE Pod Migration (gVisor onDelete Eviction)

This repository contains the blueprints, patches, configuration templates, and E2E verification test suites for GKE Pod Migration. 

Specifically, this implements **Option A: Runtime-layer onDelete Eviction Interception** (using custom container shims to intercept Pod evictions, checkpoint RAM state to GCS, and restore statefully on target nodes).

---

## 1. Live Migration Compatibility Matrix (E2E Verified)

Every workload in this matrix has been verified through real E2E eviction migrations on a GKE Standard node pool.

| Application | Category | Verdict | Required Workarounds & Bypass Configurations |
| :--- | :--- | :--- | :--- |
| **node (Node.js)** | app | ✅ **SURVIVED** | Out-of-the-box support. Memory state and connection tokens survive. |
| **go** | app | ✅ **SURVIVED** | Out-of-the-box support. Active state/counters survived in page cache. |
| **redis / valkey** | datastore | ✅ **SURVIVED** | Pure in-memory key-value stores. Extremely fast migration. |
| **mysql / mariadb** | datastore | ✅ **SURVIVED** | **InnoDB AIO Bypass**: Disable native async I/O (`--innodb_use_native_aio=OFF`) to avoid seccomp blocks on host `io_uring`. |
| **memcached** | datastore | ✅ **SURVIVED** | Pure in-memory cache blocks restored. |
| **dragonfly** | datastore | ✅ **SURVIVED** | **epoll Bypass**: Requires epoll forcing flag (`--force_epoll`). |
| **vault** | secrets | ✅ **SURVIVED** | Both succeed. Memory secrets state restored. |
| **consul** | coordination | ✅ **SURVIVED** | Dev-mode memory key-value databases survive. |
| **etcd** | coordination | ✅ **SURVIVED** | BoltDB storage writes successfully restored. |
| **nats** | streaming | ✅ **SURVIVED** | Memory jetstream offsets restored. |
| **zookeeper** | coordination | ✅ **SURVIVED** | **emptyDir Path Redirect**: Redirect `ZOO_DATA_DIR` away from Kubelet `emptyDir` mounts (e.g. to `/tmp/zookeeper`) to prevent walk errors. |
| **kafka** (KRaft) | streaming | ✅ **SURVIVED** | **JVM Metrics Bypass**: Inject environment variable `KAFKA_OPTS="-XX:-UseContainerSupport"` to avoid cgroups mismatch crashes on target nodes. |
| **postgres** | datastore | ✅ **SURVIVED** | Works out-of-the-box (uses guest POSIX shared memory). Requires setting `PGDATA` to container local directories. |
| **minio** | datastore | ✅ **SURVIVED** | Redirect storage paths to container writable layers to avoid emptyDir mount walk failures. |
| **influxdb** | datastore | ✅ **SURVIVED** | Go TSDB (v1) in-memory state restored. |
| **nginx / haproxy** | proxy | ✅ **SERVED** | Stateless proxies restore and handle reconnected traffic. |
| **traefik / caddy** | proxy | ✅ **SERVED** | Stateless routers restore and handle reconnected traffic. |
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

To deploy this live migration runtime to your GKE test cluster:

### Step 0: Create GKE Cluster (Standard Cluster)
To test this featureset, you need a GKE Standard cluster on the Rapid release channel with Workload Identity enabled:
```bash
gcloud container clusters create pod-migration-cluster \
  --release-channel=rapid \
  --workload-pool=<YOUR_PROJECT>.svc.id.goog \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

#### For Existing Clusters:
If you are bringing an existing GKE Standard cluster that already has GKE Pod Snapshots enabled, you must disable the native addon to avoid host agent socket conflicts:
```bash
gcloud container clusters update <YOUR_CLUSTER> \
  --disable-pod-snapshots \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### Step 0.5: Install cert-manager
Install cert-manager to generate and manage TLS certificates for the admission webhooks:
```bash
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.15.0/cert-manager.yaml
kubectl wait --for=condition=Available --timeout=5m -n cert-manager deployment/cert-manager-webhook
```

### Step 1: Provision gVisor Node Pool
Create a GKE node pool with gVisor sandboxing enabled:
```bash
gcloud container node-pools create gvisor-pool \
  --cluster=<YOUR_CLUSTER> \
  --sandbox=type=gvisor \
  --machine-type=n2-standard-4 \
  --num-nodes=2 \
  --zone=<YOUR_ZONE> \
  --project=<YOUR_PROJECT>
```

### Step 1.2: Install Pod Snapshot CRDs
Install the Pod Snapshot CRDs into the cluster:
```bash
kubectl apply -f patches/crds/
```

### Step 1.5: Configure Snapshot Storage & Workload Identity

Run the provided setup script to automatically create the GCS bucket, configure IAM permissions for Workload Identity, and apply the `PodSnapshotStorageConfig` (PSSC) manifest named `lpm-test-storage`:

```bash
chmod +x patches/setup-storage.sh
./patches/setup-storage.sh
```

You can optionally pass custom values for Project ID, Bucket Name, and Region:
```bash
./patches/setup-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]
```

### Step 2: Deploy containerd-shim patch & Custom Agent (Automated DaemonSets)

Since the GKE native Pod Snapshot addon is disabled, you must deploy both a containerd-shim patch (to intercept OCI lifecycle calls) and our custom host agent (to coordinate the snapshot uploads).

1. **Retrieve the precompiled binary and build the patcher image:**
   Retrieve the precompiled `containerd-shim-runsc-v1` binary (or compile it manually).

   > [!NOTE]
   > `<YOUR_REGISTRY>` refers to your container image registry. If you are using Google Cloud and do not have an Artifact Registry repository created, you can create one (e.g., named `pm-poc` in `us-central1`) using the following command:
   > ```bash
   > gcloud artifacts repositories create pm-poc \
   >   --repository-format=docker \
   >   --location=us-central1 \
   >   --description="Docker repository for Pod Migration"
   > ```
   > The registry path format to use is: `us-central1-docker.pkg.dev/<YOUR_PROJECT>/pm-poc`.

   Then, build the container image using the provided Dockerfile:
   ```bash
   docker build -t <YOUR_REGISTRY>/node-patcher:latest -f patches/Dockerfile.patcher patches/
   docker push <YOUR_REGISTRY>/node-patcher:latest
   ```

2. **Deploy the patcher DaemonSet and verify rollout:**
   Deploy the DaemonSet by replacing the `<PATCHER_IMAGE>` placeholder on the fly:
   ```bash
   sed 's|<PATCHER_IMAGE>|<YOUR_REGISTRY>/node-patcher:latest|' patches/node-patcher-daemonset.yaml | kubectl apply -f -
   kubectl rollout status daemonset/node-patcher -n kube-system
   ```

3. **Deploy the Custom Host Agent:**
   The custom agent needs to run with workload identity permissions to upload snapshots to GCS. 
   
   First, build and push your custom agent image to `<YOUR_REGISTRY>/custom-pod-snapshot-agent:latest`.
   
   Then, edit `patches/custom-agent.yaml` to replace the following placeholders with your actual cluster details:
   *   `<YOUR_AGENT_IMAGE>`: The agent image you just pushed.
   *   `<YOUR_PROJECT_ID>`: Your GCP Project ID.
   *   `<YOUR_PROJECT_NUMBER>`: Your GCP Project Number.
   *   `<YOUR_CLUSTER_ZONE>`: The zone of your GKE cluster.
   *   `<YOUR_CLUSTER_NAME>`: The name of your GKE cluster.
   
   Apply the manifest and wait for rollout:
   ```bash
   kubectl apply -f patches/custom-agent.yaml
   kubectl rollout status daemonset/custom-pod-snapshot-agent -n default
   ```

### Step 3: Register Webhooks & Policies
Deploy the Mutating Webhook (for memory headroom injection and JVM flags) and Validating Policies (to reject incompatible BEAM/fsnotify workloads):
```bash
kubectl apply -f patches/gke-pod-snapshot-admission-webhook.yaml
```

### Step 4: Deploy the Pod Migration Controller
Deploy the Pod Migration Controller manager to handle the lifecycle and coordination of pod migrations:

1. **Build and push the controller image:**
   Build the controller image from the `controller/` directory and push it to your registry:
   ```bash
   docker build -t <YOUR_REGISTRY>/pod-migration-controller:latest -f controller/Dockerfile controller/
   docker push <YOUR_REGISTRY>/pod-migration-controller:latest
   ```

2. **Configure the GCS Bucket:**
   Edit `controller/podmigration-config.yaml` to configure your target GCS bucket for snapshots:
   ```yaml
   spec:
     storage:
       location: gs://<YOUR_BUCKET_NAME>/snapshots
   ```

3. **Deploy the Controller:**
   Apply the CRDs and deploy the controller by replacing the `<YOUR_CONTROLLER_IMAGE>` placeholder:
   ```bash
   # Apply the Custom Resource Definitions first
   kubectl apply -f controller/config/crd/bases/

   # Deploy the controller deployment
   sed 's|<YOUR_CONTROLLER_IMAGE>|<YOUR_REGISTRY>/pod-migration-controller:latest|' controller/deploy.yaml | kubectl apply -f -
   
   # Apply the storage config
   kubectl apply -f controller/podmigration-config.yaml
   
   # Verify deployment status
   kubectl rollout status deployment/pod-migration-controller -n pod-migration-system
   ```

### Patch Dependency & Upstream Progress
> [!NOTE]
> The node-level lifecycle patches required for this live-migration setup are currently pending upstream GKE platform enhancements.
> This precompiled binary DaemonSet setup is a temporary developer preview testing utility that bridges the gap until these enhancements are natively integrated into GKE node images.

---

## 3. Workload Verification & Manifest Templates

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

## 4. Live Migration Controller in Action

Below is an execution trace showing the containerd-shim intercepting a node eviction, snapshotting the active Valkey memory state to GCS, and gracefully restoring it on a target node:

![Live Migration Controller Demo](docs/images/controller-demo.png)

*(Note: Replace `docs/images/controller-demo.png` with a captured screenshot or asciicast of your terminal run).*

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
```