# Pod Migration Controller - Experimental

> **Proof of Concept**: This controller is a proof of concept and is intended for experimentation only. It should **not** be used in a production environment.

This controller facilitates Pod Migration by intercepting eviction requests, triggering snapshots, and managing pod deletion after snapshots are ready. This enables Pod Migration for any pod evictions including evictions triggered by Vertical Pod Autoscaler.

## How it Works

The controller consists of the following components:

### Webhooks

- **Validating Webhook (`/validate-eviction`)**: Handled by **[EvictionGuard](pkg/webhook/admission.go)**. It intercepts eviction requests for pods with the label `pod-migration.gke.io/enabled: "true"`.
    - If a snapshot is already in progress (annotation `pod-migration.gke.io/snapshot-requested: "true"`), it denies the eviction.
    - If no snapshot is requested, it adds the `pod-migration.gke.io/snapshot-requested: "true"` annotation to the pod to trigger the migration flow and denies the current eviction request.

### Reconcilers

- **[PodReconciler](pkg/controller/pod_controller.go)**: Watches for Pods with the label `pod-migration.gke.io/enabled: "true"` and annotation `pod-migration.gke.io/snapshot-requested: "true"`. When both are present, it creates a `PodSnapshotManualTrigger` resource to initiate a snapshot.
- **[SnapshotReconciler](pkg/controller/snapshot_controller.go)**: Watches for `PodSnapshot` resources. Once a snapshot becomes ready, it deletes the origin pod and the corresponding trigger resource.
- **[DeferredEvictionReconciler](pkg/controller/deferred_eviction_controller.go)**: Watches for Pods with the label `pod-migration.gke.io/enabled: "true"` and condition `PodResizePending` with reason `Deferred`. When a pod is deferred, it acts as follows:
    - It searches for *other* candidate pods on the same node with `pod-migration.gke.io/enabled: "true"` that are not already being deleted.
    - If a candidate pod is found, it issues an eviction request for that *other* pod to free up resources on the node, and adds the label `pod-migration.gke.io/deferred-eviction-processed: "true"` to the deferred pod to prevent redundant actions in subsequent loops.
    - If no candidate is found, it falls back to evicting the deferred pod itself immediately.
    - When a processed pod is no longer deferred, the `deferred-eviction-processed` label is automatically cleaned up.

## Getting Started

### Prerequisites

#### Local Development Tools

- **Go version 1.25+**: Required to compile the controller binary.
- **Docker version 29.3.0+**: Required to build and containerize the controller image.
- **kubectl**: Command-line tool for deploying resources to the cluster.

#### GKE Cluster Requirements

- **GKE Cluster Version**: Minimum version **1.36.0-gke.2253000** or later is required to support GKE Pod Snapshots with VPA.
- **Cert-manager**: Installed on the cluster to handle webhook certificates. See the [cert-manager Installation Guide](https://cert-manager.io/docs/installation/) for details.
- **Pod Snapshot Feature**: Enabled on the cluster. For instructions on enabling this feature (which includes setting up GKE Sandbox and Workload Identity), see [How to enable Pod Snapshots](https://docs.cloud.google.com/kubernetes-engine/docs/how-to/pod-snapshots#enable). Ensure you are also familiar with **Pod Snapshots and their limitations** before deploying; see the [GKE Pod Snapshots Concepts](https://docs.cloud.google.com/kubernetes-engine/docs/concepts/pod-snapshots) for details.
- **Vertical Pod Autoscaler (VPA)**: Enabled on the cluster if you intend to use VPA auto-scaling resize integrations. For a guided demo on testing In-Place Pod Resize (IPPR) with VPA, see the [VPA IPPR Demo Guide](https://github.com/jpawelczak/vpa-demo#introducing-no-to-low-disruptive-vpa-in-gke).

### Deploying the Controller

#### 1. Build and Push the Controller Image

Before deploying, compile the controller binary and build/push the Docker image using the provided Makefile targets (override the `IMAGE` variable to point to your container registry):

```bash
# Compile the local Go binary
make build

# Build the container image
make docker-build IMAGE=<image-registry>/pod-migration:latest

# Push the container image to your repository
make docker-push IMAGE=<image-registry>/pod-migration:latest
```

*Note: Make sure to update the `image` reference in [deploy/deployment.yaml](deploy/deployment.yaml) to match your pushed image.*

#### 2. Deploy Manifests

To deploy the controller and its associated resources, apply the manifests in the **[deploy](deploy)** directory:

```bash
kubectl apply -f deploy/
```

This will deploy:
- RBAC rules (**[rbac.yaml](deploy/rbac.yaml)**)
- Service (**[service.yaml](deploy/service.yaml)**)
- Deployment (**[deployment.yaml](deploy/deployment.yaml)**)
- Cert-manager resources for self-signed certs (**[cert-manager.yaml](deploy/cert-manager.yaml)**)
- Validating Webhook Configuration (**[webhook.yaml](deploy/webhook.yaml)**)

### Setup User Workload

To enable Pod Migration for a user workload, you must add the label `pod-migration.gke.io/enabled: "true"` to the Pod's metadata labels.

Here is an example of a Deployment configuration:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
  namespace: default
spec:
  replicas: 3
  selector:
    matchLabels:
      app: my-app
  template:
    metadata:
      labels:
        app: my-app
        # Enable Pod Migration for pods created by this deployment
        pod-migration.gke.io/enabled: "true"
    spec:
      # Update with the service account that has access to the Cloud Storage bucket used for Pod snapshots
      serviceAccountName: <change-me>
      # The gVisor runtimeClass is required for Pod Snapshotting
      runtimeClassName: gvisor
      containers:
      - name: main
        image: python:3.10-slim
        command: ["python3", "-c"]
        args:
          - |
            import time
            i = 0
            while True:
              print(f"Count: {i}", flush=True)
              i += 1
              time.sleep(1)
        resources:
          requests:
            cpu: "100m"
            memory: "128Mi"
```

> For instructions on how to set up and configure Vertical Pod Autoscaler (VPA) to work with In-Place Pod Resize (IPPR), please refer to the [VPA IPPR Demo Guide](https://github.com/jpawelczak/vpa-demo#introducing-no-to-low-disruptive-vpa-in-gke).

### Triggering Migration (Eviction)

To trigger the migration flow manually, you can issue a Pod eviction request. Evictions are processed as `CREATE` requests on the `eviction` subresource of the target Pod.

First, run `kubectl proxy` to expose the Kubernetes API server locally (e.g., on port `8081`):

```bash
kubectl proxy --port=8081
```

Then, use `curl` to send an eviction request for the target Pod (replace `<pod-name>` and `<namespace>` with your values):

```bash
curl -X POST http://localhost:8081/api/v1/namespaces/<namespace>/pods/<pod-name>/eviction \
-H "Content-Type: application/json" \
-d '{
  "apiVersion": "policy/v1",
  "kind": "Eviction",
  "metadata": {
    "name": "<pod-name>",
    "namespace": "<namespace>"
  }
}'
```

### Uninstallation

To remove the controller and all associated resources:

```bash
kubectl delete -f deploy/
```

## Contributing

This project is licensed under the [Apache 2.0 License](LICENSE).

We welcome contributions! Please see [docs/contributing.md](docs/contributing.md) for more information.

We follow [Google's Open Source Community Guidelines](https://opensource.google.com/conduct/).

## Disclaimer

This is not an officially supported Google product.

This project is not eligible for the Google Open Source Software Vulnerability Rewards Program.
