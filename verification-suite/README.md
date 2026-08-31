# GKE gVisor E2E Live Migration Verification Suite

This folder houses the E2E verification manifests, setup runbooks, patches, and
automated driver scripts used to validate the compatibility of 30 common
containerized applications under GKE's gVisor `onDelete` (Option A) runtime
eviction.

--------------------------------------------------------------------------------

## 1. Directory Structure

*   `run_app_validation.sh`: The master CLI driver script to deploy, seed,
    migrate, uncordon, and verify any of the supported workloads.
*   `manifests/`: Cleaned, non-root, gVisor-compatible StatefulSet and Job
    templates.
*   `patches/`: Code patches, configuration tweaks, and runbooks addressing
    specific application runtime gaps.
*   `runbooks/`: Step-by-step text guides for manual verification.
*   `demo/`: Interactive tools (e.g., streaming clients, websockets) to
    demonstrate active state survival.
*   `dashboard/`: Static web UI visualization showing test outcomes.

--------------------------------------------------------------------------------

## 2. Prerequisites

To execute E2E validation runs, ensure the following GKE environment parameters
are met:

1.  **gVisor Runtime**: A GKE cluster node pool configured with
    `runtimeClassName: gvisor`.
2.  **Pod Snapshot Admission Webhook**: The `gke-pod-snapshot-agent` daemonset
    and its associated validating webhook active in the cluster.
3.  **Authentication**: Your local `kubectl` context pointing to the target
    evaluation cluster.

--------------------------------------------------------------------------------

## 3. How to Run Validation Tests

To run the automated E2E migration check for any application:

```bash
# Run validation for a single application
./run_app_validation.sh <app_name>

# Example:
./run_app_validation.sh redis
./run_app_validation.sh kafka
```

### Supported Application Target Names:

*   `redis`, `valkey`, `memcached`, `dragonfly`, `vault`, `etcd`, `consul`,
    `nats`
*   `mysql`, `mariadb`, `postgres`, `zookeeper`, `kafka`
*   `nginx`, `haproxy`, `traefik`, `caddy`, `python`
*   `go` (Job), `node` (Job)

--------------------------------------------------------------------------------

## 4. Key Workarounds Applied

Several critical workarounds are embedded into the manifests and validation
script:

1.  **MySQL / MariaDB**: Disabled InnoDB native AIO
    (`--innodb_use_native_aio=OFF`) to bypass the host-level `io_uring` seccomp
    block.
2.  **ZooKeeper**: Redirected data directories to `/tmp/zookeeper` to bypass
    host-level emptyDir serialization crashes.
3.  **Kafka / JVM**: Added `KAFKA_OPTS="-XX:-UseContainerSupport"` to the
    environment to prevent JDK metrics NullPointerExceptions on restored nodes.
4.  **MinIO**: Overrode data directories to avoid image-declared emptyDir volume
    gaps.
