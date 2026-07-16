# PoC: MicroVM Pod Live Migration (Kata + Cloud Hypervisor)

This runbook outlines how to build and execute a standalone Proof-of-Concept
(PoC) to verify live migration for containers running under a MicroVM runtime
(Kata Containers with Cloud Hypervisor) on a GKE node.

--------------------------------------------------------------------------------

## 1. PoC Architecture Overview

Standard K8s live migration checkpoints process states. In a MicroVM, the state
is managed at the **hypervisor** level:

```
[Host Node A]                                     [Host Node B]
┌─────────────────────────────────┐               ┌─────────────────────────────────┐
│  kata-shim (shim-monitor)       │               │  kata-shim (restore wrapper)    │
│    │                            │               │    │                            │
│    ├──(1) vm.pause & snapshot   │               │    ├──(4) vm.restore & resume   │
│    ▼                            │               │    ▼                            │
│  cloud-hypervisor (Guest RAM)   │               │  cloud-hypervisor (Restored)    │
│    │                            │               │    ▲                            │
│    └──(2) write RAM to GCS ─────┼──► [GCS] ─────┼────┘                            │
│                                 │               │                                 │
│  virtiofsd (VFS daemon)         │               │  virtiofsd (Remapped)           │
│    └──(3) sync host upperdir ───┼──► [GCS] ─────┼────(5) reload new hosts/secret  │
└─────────────────────────────────┘               └─────────────────────────────────┘
```

--------------------------------------------------------------------------------

## 2. Step 1: Standalone Hypervisor Checkpoint & Restore (No K8s)

Before integrating with Kubernetes, you can verify that the Cloud Hypervisor
successfully serializes and resumes your workload's memory state on the node:

### 1. Launch a Guest VM with a Counter Server

Save this script as `launch_counter_vm.sh`:

```bash
#!/bin/bash
RUNTIME_FOLDER="/tmp/my-clh-env"
sudo mkdir -p ${RUNTIME_FOLDER}/snapshot
sudo ip tuntap add name tap0 mode tap
sudo ip addr add 192.168.250.1/24 dev tap0
sudo ip link set dev tap0 up

# Start Cloud Hypervisor running a lightweight Linux image with a counter perl script
sudo /opt/kata/bin/cloud-hypervisor \
  --kernel ./vmlinux \
  --disk path=./ubuntu.img \
  --cpus boot=1 \
  --memory size=1024M \
  --api-socket ${RUNTIME_FOLDER}/counter-api.sock \
  --net tap=tap0,ip=192.168.250.2,mask=255.255.255.0,mac=00:11:22:33:44:55 \
  --cmdline "console=ttyS0 root=/dev/vda1 rw init=/bin/bash" &
```

### 2. Seed the Counter State

Increment the counter a few times via curl:

```bash
curl http://192.168.250.2
# Returns: 0
curl http://192.168.250.2
# Returns: 1
```

### 3. Trigger the VM Checkpoint

Call the Cloud Hypervisor API socket to pause the VM and write its memory dump
to a local folder:

```bash
# Pause guest CPU execution
curl --unix-socket /tmp/my-clh-env/counter-api.sock -X PUT http://localhost/api/v1/vm.pause

# Snapshot RAM state
curl --unix-socket /tmp/my-clh-env/counter-api.sock -X PUT \
  -H "Content-Type: application/json" \
  -d '{"destination_url": "file:///tmp/my-clh-env/counter-snapshot"}' \
  http://localhost/api/v1/vm.snapshot
```

### 4. Restore and Resume the VM

Terminate the running hypervisor instance and launch a new instance pointing to
the snapshot URL:

```bash
sudo pkill -9 -f cloud-hypervisor

# Restore from snapshot
sudo /opt/kata/bin/cloud-hypervisor \
  --api-socket /tmp/my-clh-env/counter-api.sock \
  --snapshot "file:///tmp/my-clh-env/counter-snapshot" &
```

Query the counter again:

```bash
curl http://192.168.250.2
# Returns: 2  (State survived!)
```

--------------------------------------------------------------------------------

## 3. Step 2: Making Kata aware of Checkpoint/Restore (K8s Integration)

To make this compatible with Kubernetes Pod eviction, you must patch the Kata
containerd-shim:

### 1. Embed `shim-monitor` Checkpoint Trigger

In the patched Kata-runtime code, implement an HTTP server listening on the
shim's control socket (`/run/vc/sbs/${SANDBOX_ID}/shim-monitor.sock`):

```go
// Mock Go handler inside Kata shim:
func HandleCheckpointRequest(w http.ResponseWriter, r *http.Request) {
    // 1. Call cloud-hypervisor API: PUT /api/v1/vm.pause
    // 2. Call cloud-hypervisor API: PUT /api/v1/vm.snapshot to local tmpfs
    // 3. Trigger host agent to upload the snapshot folder to GCS
}
```

### 2. Trigger via eviction webhook

When GKE eviction triggers, the webhook calls the shim-monitor endpoint:

```bash
curl -X POST --unix-socket /run/vc/sbs/${SANDBOX_ID}/shim-monitor.sock \
  http://localhost/checkpoint \
  -d '{"path": "/tmp/counter-snapshot", "leave_vm_closed": true}'
```

### 3. Handle `virtiofsd` file descriptor remapping

During pod reschedule on the target node, the containerd-shim must rewrite the
host-path bindings for secret mounts before starting the hypervisor:

*   Read `vm.config` of the checkpointed VM.
*   Scan `/etc/hosts`, `/etc/resolv.conf`, and Kubernetes token paths.
*   Update the config JSON with the new host mount path locations allocated by
    the target Kubelet.
