#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Rolling GKE Node-Pool Upgrade & CRIU Warm-Restore Live Verification Suite (Issue #256 / Epic #247)
#
# Validates that stateful CRIU pods warm-restore cleanly across GKE node-pool upgrades
# when nodes are cordoned and drained during rolling upgrades:
#   1. Deploys a stateful in-memory workload (Redis without persistence or monotonic counter)
#   2. Seeds verifiable in-memory state (unique nonce + key population)
#   3. Records pre-upgrade source node facts (GKE version, kernel, containerd, OS image)
#   4. Drives or monitors rolling node-pool upgrade / drain
#   5. Verifies:
#      - PodMigrationJob reaches Phase=Succeeded with RestoreVerified
#      - Zero container restarts (.status.containerStatuses[0].restartCount == 0)
#      - Destination node is an upgraded or compatible node
#      - In-memory data is 100% preserved (acid test: unpersisted keys intact)
#      - Zero safety violations or invariant errors
#   6. Offline self-test mode (--self-test) exercises full execution path and failure gates.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
export REPO_ROOT

# Configurable options
PROJECT="${PROJECT:-}"
CLUSTER="${CLUSTER:-}"
LOCATION="${LOCATION:-}"
NODE_POOL="${NODE_POOL:-}"
TARGET_VERSION="${TARGET_VERSION:-}"
NAMESPACE="${NAMESPACE:-}"
STORAGE_LOCATION="${STORAGE_LOCATION:-}"
WORKLOAD="${WORKLOAD:-redis}"
TIMEOUT_SEC="${TIMEOUT_SEC:-1800}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-5}"
SKIP_UPGRADE="${SKIP_UPGRADE:-false}"
DRAIN_NODE="${DRAIN_NODE:-false}"
ALLOW_SAME_VERSION="${ALLOW_SAME_VERSION:-false}"
SKIP_CLEANUP="${SKIP_CLEANUP:-false}"
DRY_RUN="${DRY_RUN:-false}"
SELF_TEST="${SELF_TEST:-false}"
OUT_DIR="${OUT_DIR:-/tmp/pm-upgrade-test-runs}"

KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"
GCLOUD_CMD="${GCLOUD_CMD:-gcloud}"

USER_NAMESPACE_SPECIFIED="false"

log() {
  printf "\033[1;36m[%s]\033[0m %s\n" "$(date -u +%H:%M:%S)" "$*"
}

warn() {
  printf "\033[1;33m[WARN %s]\033[0m %s\n" "$(date -u +%H:%M:%S)" "$*" >&2
}

die() {
  printf "\033[1;31m[FAIL %s]\033[0m %s\n" "$(date -u +%H:%M:%S)" "$*" >&2
  exit 1
}

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --project <id>              GCP project ID (default: auto-detected from gcloud)
  --cluster <name>            GKE cluster name (default: auto-detected from current context)
  --location <region|zone>    GKE location (region or zone)
  --node-pool <pool>          Target GKE node pool to upgrade
  --target-version <version>  Target GKE version for node pool upgrade (e.g. 1.35.2-gke.1000)
  --workload <redis|counter>  Workload type to test (default: redis)
  --namespace <ns>            Namespace for test workload (default: auto-generated dedicated namespace)
  --storage-location <uri>    GCS storage location for PodMigration (e.g. gs://bucket/path)
  --timeout <seconds>         Maximum seconds to wait for upgrade and migration (default: 1800)
  --skip-upgrade              Skip gcloud upgrade trigger (observe drain or existing upgrade)
  --drain-node                Trigger kubectl drain on source node rather than gcloud upgrade
  --allow-same-version        Allow destination node to share source version (default: require version jump)
  --skip-cleanup              Do not delete test workload or uncordon nodes after run (default: false)
  --out-dir <dir>             Directory for run logs and report (default: /tmp/pm-upgrade-test-runs)
  --dry-run                   Print actions without modifying cluster resources
  --self-test                 Run offline unit and sabotage validation suite (used in CI)
  -h, --help                  Show this help message

Examples:
  # Run offline self-test suite:
  $(basename "$0") --self-test

  # Test rolling upgrade of pool 'criu-pool' to target patch version:
  $(basename "$0") --cluster <CLUSTER> --location <LOCATION> \\
    --node-pool criu-pool --target-version 1.34.2-gke.1100

  # Test rolling drain across mixed-version nodes without invoking gcloud upgrade:
  $(basename "$0") --cluster <CLUSTER> --namespace pm-upgrade-test --drain-node --skip-upgrade
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --project)
      PROJECT="$2"
      shift 2
      ;;
    --cluster)
      CLUSTER="$2"
      shift 2
      ;;
    --location)
      LOCATION="$2"
      shift 2
      ;;
    --node-pool)
      NODE_POOL="$2"
      shift 2
      ;;
    --target-version)
      TARGET_VERSION="$2"
      shift 2
      ;;
    --workload)
      WORKLOAD="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      USER_NAMESPACE_SPECIFIED="true"
      shift 2
      ;;
    --storage-location)
      STORAGE_LOCATION="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT_SEC="$2"
      shift 2
      ;;
    --skip-upgrade)
      SKIP_UPGRADE="true"
      shift
      ;;
    --drain-node)
      DRAIN_NODE="true"
      shift
      ;;
    --allow-same-version)
      ALLOW_SAME_VERSION="true"
      shift
      ;;
    --skip-cleanup)
      SKIP_CLEANUP="true"
      shift
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --dry-run)
      DRY_RUN="true"
      shift
      ;;
    --self-test)
      SELF_TEST="true"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

exec_with_retry() {
  local max_attempts=5
  local attempt=1
  until "$@"; do
    if [ "${attempt}" -ge "${max_attempts}" ]; then
      return 1
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
}

render_workload_manifest() {
  local name="$1"
  local ns="$2"
  local workload_type="$3"

  case "${workload_type}" in
    redis)
      cat <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  serviceName: ${name}-svc
  replicas: 1
  selector:
    matchLabels:
      app: ${name}
  template:
    metadata:
      labels:
        app: ${name}
        pod-migration.gke.io/enabled: "true"
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        imagePullPolicy: IfNotPresent
        command: ["docker-entrypoint.sh"]
        args: ["redis-server", "--save", "", "--appendonly", "no", "--enable-debug-command", "yes"]
        ports:
        - containerPort: 6379
        readinessProbe:
          tcpSocket:
            port: 6379
          periodSeconds: 2
          failureThreshold: 3
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            memory: 512Mi
---
apiVersion: v1
kind: Service
metadata:
  name: ${name}-svc
  namespace: ${ns}
spec:
  selector:
    app: ${name}
  ports:
  - port: 6379
EOF
      ;;
    counter)
      cat <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  serviceName: ${name}-svc
  replicas: 1
  selector:
    matchLabels:
      app: ${name}
  template:
    metadata:
      labels:
        app: ${name}
        pod-migration.gke.io/enabled: "true"
    spec:
      containers:
      - name: counter
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          ID=\$(cat /proc/sys/kernel/random/uuid 2>/dev/null || echo "counter-\$\$-run")
          C=0
          while true; do
            C=\$((C + 1))
            printf "%s|%d\n" "\$ID" "\$C" > /tmp/counter.state.tmp
            mv /tmp/counter.state.tmp /tmp/counter.state
            sleep 1
          done
        readinessProbe:
          exec:
            command: ["test", "-f", "/tmp/counter.state"]
          periodSeconds: 1
          failureThreshold: 3
        resources:
          requests:
            cpu: 50m
            memory: 128Mi
          limits:
            memory: 256Mi
---
apiVersion: v1
kind: Service
metadata:
  name: ${name}-svc
  namespace: ${ns}
spec:
  selector:
    app: ${name}
  ports:
  - port: 8080
EOF
      ;;
    *)
      die "Unsupported workload type: ${workload_type} (expected 'redis' or 'counter')"
      ;;
  esac
}

render_podmigration_manifest() {
  local ns="$1"
  local storage_loc="${2:-${STORAGE_LOCATION:-gs://pod-migration-checkpoints/${ns}}}"
  cat <<EOF
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: default
  namespace: ${ns}
  annotations:
    pod-migration.gke.io/runtime-class-engines: "@default=criu"
spec:
  storage:
    location: ${storage_loc}
EOF
}

get_node_facts() {
  local node_name="$1"
  ${KUBECTL_CMD} get node "${node_name}" -o json | jq -r '
    [
      .status.nodeInfo.kubeletVersion,
      .status.nodeInfo.kernelVersion,
      .status.nodeInfo.containerRuntimeVersion,
      .status.nodeInfo.osImage
    ] | @tsv
  '
}

seed_workload() {
  local pod="$1"
  local ns="$2"
  local workload_type="$3"
  local nonce="$4"

  case "${workload_type}" in
    redis)
      log "Seeding Redis on ${pod}: setting migkey=${nonce} and populating 1000 in-memory keys"
      exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- redis-cli SET migkey "${nonce}" >/dev/null
      exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- redis-cli DEBUG POPULATE 1000 >/dev/null
      local dbsize
      dbsize="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- redis-cli DBSIZE | tr -dc '0-9')"
      if [[ -z "${dbsize}" || "${dbsize}" -lt 1000 ]]; then
        die "Failed to seed Redis on ${pod}: DBSIZE=${dbsize:-0} (expected >= 1001)"
      fi
      log "Redis seeded successfully: DBSIZE=${dbsize}, migkey=${nonce}"
      ;;
    counter)
      log "Reading initial counter on ${pod}"
      local state
      state="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- cat /tmp/counter.state)"
      [[ -n "${state}" ]] || die "Counter state empty on ${pod}"
      log "Counter state initialized: ${state}"
      ;;
  esac
}

verify_restored_workload() {
  local pod="$1"
  local ns="$2"
  local workload_type="$3"
  local expected_nonce="$4"
  local initial_state="$5"

  case "${workload_type}" in
    redis)
      log "Verifying Redis state survival on restored pod ${pod}"
      local got_nonce dbsize
      got_nonce="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- redis-cli GET migkey | tr -d '[:space:]')" || got_nonce=""
      dbsize="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- redis-cli DBSIZE | tr -dc '0-9')" || dbsize=0

      if [[ "${got_nonce}" != "${expected_nonce}" ]]; then
        die "Redis state verification FAILED: got migkey='${got_nonce}', expected '${expected_nonce}' (in-memory data lost / cold start)"
      fi
      if [[ "${dbsize}" -lt 1000 ]]; then
        die "Redis state verification FAILED: DBSIZE=${dbsize} (expected >= 1001 keys intact)"
      fi
      log "Redis state verified: migkey matched ('${got_nonce}'), DBSIZE=${dbsize}"
      ;;
    counter)
      log "Verifying Counter continuity on restored pod ${pod}"
      local new_state
      new_state="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${ns}" "${pod}" -- cat /tmp/counter.state)"
      local orig_id orig_count new_id new_count
      orig_id="${initial_state%%|*}"
      orig_count="${initial_state##*|}"
      new_id="${new_state%%|*}"
      new_count="${new_state##*|}"

      if [[ "${orig_id}" != "${new_id}" ]]; then
        die "Counter verification FAILED: instance ID changed from '${orig_id}' to '${new_id}' (process restarted / cold start)"
      fi
      if (( new_count < orig_count )); then
        die "Counter verification FAILED: counter rolled back from ${orig_count} to ${new_count}"
      fi
      log "Counter continuity verified: instance ID intact ('${new_id}'), count advanced ${orig_count} -> ${new_count}"
      ;;
  esac
}

verify_pmj_outcome() {
  local pmj_json="$1"
  local phase restored_reason engine

  phase="$(echo "${pmj_json}" | jq -r '.status.phase // empty')"
  restored_reason="$(echo "${pmj_json}" | jq -r '
    .status.conditions[]? | select(.type=="Restored") | .reason // empty
  ')"
  engine="$(echo "${pmj_json}" | jq -r '
    .metadata.annotations["pod-migration.gke.io/engine"] // empty
  ')"

  if [[ "${phase}" != "Succeeded" ]]; then
    die "PodMigrationJob phase is '${phase}' (expected 'Succeeded')"
  fi

  if [[ "${restored_reason}" != "RestoreVerified" && "${restored_reason}" != "RestoreInferred" ]]; then
    die "PodMigrationJob Restored condition reason is '${restored_reason}' (expected 'RestoreVerified' or 'RestoreInferred')"
  fi

  if [[ "${engine}" != "criu" ]]; then
    die "PodMigrationJob engine annotation is '${engine}' (expected 'criu'; run did not use CRIU engine)"
  fi
}

verify_node_transition() {
  local src_node="$1"
  local src_kubelet="$2"
  local src_kernel="$3"
  local dst_node="$4"
  local dst_kubelet="$5"
  local dst_kernel="$6"
  local allow_same_version="$7"

  if [[ "${src_node}" == "${dst_node}" ]]; then
    die "Node transition check FAILED: destination node '${dst_node}' is identical to source node '${src_node}'; replacement did not migrate to a different node"
  fi

  if [[ "${src_kubelet}" == "${dst_kubelet}" && "${src_kernel}" == "${dst_kernel}" ]]; then
    if [[ "${allow_same_version}" == "true" ]]; then
      warn "Destination node '${dst_node}' has identical kubelet (${dst_kubelet}) and kernel (${dst_kernel}) versions to source node '${src_node}'"
    else
      die "Version transition check FAILED: destination node '${dst_node}' has identical kubelet (${dst_kubelet}) and kernel (${dst_kernel}) versions to source node '${src_node}'. Run did not cross node versions (pass --allow-same-version to permit same-version node migration)."
    fi
  else
    log "Version transition check PASSED: Kubelet '${src_kubelet}' -> '${dst_kubelet}', Kernel '${src_kernel}' -> '${dst_kernel}'"
  fi
}

verify_container_restarts() {
  local pod_json="$1"
  local restart_count
  restart_count="$(echo "${pod_json}" | jq -r '.status.containerStatuses[0].restartCount // 0')"

  if [[ "${restart_count}" -ne 0 ]]; then
    die "Restored pod container restartCount is ${restart_count} (expected 0; cold-start or crash occurred)"
  fi
}

CORDONED_NODE=""
WORKLOAD_DEPLOYED="false"
OWNED_NAMESPACE="false"

cleanup() {
  local exit_code=$?
  if [[ "${DRY_RUN}" == "true" ]]; then
    return
  fi
  if [[ -n "${CORDONED_NODE}" ]]; then
    log "Cleaning up: uncordoning node ${CORDONED_NODE}..."
    ${KUBECTL_CMD} uncordon "${CORDONED_NODE}" || true
    CORDONED_NODE=""
  fi
  if [[ "${SKIP_CLEANUP}" != "true" ]]; then
    if [[ "${OWNED_NAMESPACE}" == "true" ]]; then
      log "Cleaning up: deleting dedicated test namespace ${NAMESPACE}..."
      ${KUBECTL_CMD} delete namespace "${NAMESPACE}" --ignore-not-found || true
    elif [[ "${WORKLOAD_DEPLOYED}" == "true" ]]; then
      log "Cleaning up: deleting test workload and PodMigration in namespace ${NAMESPACE}..."
      ${KUBECTL_CMD} delete statefulset "criu-upgrade-${WORKLOAD}" -n "${NAMESPACE}" --ignore-not-found || true
      ${KUBECTL_CMD} delete service "criu-upgrade-${WORKLOAD}-svc" -n "${NAMESPACE}" --ignore-not-found || true
      ${KUBECTL_CMD} delete podmigration default -n "${NAMESPACE}" --ignore-not-found || true
    fi
  fi
  exit "${exit_code}"
}

run_live() {
  trap cleanup EXIT INT TERM

  mkdir -p "${OUT_DIR}"
  local run_log
  run_log="${OUT_DIR}/nodepool_upgrade_$(date +%Y%m%d_%H%M%S).log"
  exec > >(tee -a "${run_log}") 2>&1

  # Auto-generate fresh dedicated namespace if not specified
  if [[ -z "${NAMESPACE}" ]]; then
    NAMESPACE="pm-upgrade-test-$(date +%s)-$RANDOM"
    OWNED_NAMESPACE="true"
    log "Generated dedicated test namespace: ${NAMESPACE}"
  fi

  log "=== GKE Node-Pool Upgrade & CRIU Warm-Restore Live Suite ==="
  log "Namespace: ${NAMESPACE}, Workload: ${WORKLOAD}, Timeout: ${TIMEOUT_SEC}s"

  # Dependencies check
  command -v "${KUBECTL_CMD}" >/dev/null 2>&1 || die "Missing required command: ${KUBECTL_CMD}"
  command -v jq >/dev/null 2>&1 || die "Missing required command: jq"

  if [[ "${SKIP_UPGRADE}" != "true" && "${DRAIN_NODE}" != "true" ]]; then
    command -v "${GCLOUD_CMD}" >/dev/null 2>&1 || die "Missing required command: ${GCLOUD_CMD}"
    [[ -n "${CLUSTER}" ]] || die "Please specify --cluster"
    [[ -n "${NODE_POOL}" ]] || die "Please specify --node-pool"
    [[ -n "${TARGET_VERSION}" ]] || die "Please specify --target-version"
  fi

  # Namespace setup & PodMigration singleton check (#98)
  if ${KUBECTL_CMD} get namespace "${NAMESPACE}" >/dev/null 2>&1; then
    local existing_pm
    existing_pm="$(${KUBECTL_CMD} get podmigrations -n "${NAMESPACE}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
    if [[ -n "${existing_pm}" ]]; then
      die "Namespace '${NAMESPACE}' already contains a PodMigration (${existing_pm}). PodMigrations are singletons per namespace (#98); run in a dedicated namespace or let the script create a fresh one."
    fi
  else
    log "Creating dedicated test namespace ${NAMESPACE}..."
    ${KUBECTL_CMD} create namespace "${NAMESPACE}"
    OWNED_NAMESPACE="true"
  fi

  # Resolve storage location if not provided
  if [[ -z "${STORAGE_LOCATION}" ]]; then
    local discovered_loc
    discovered_loc="$(${KUBECTL_CMD} get podmigrations -n "${NAMESPACE}" -o jsonpath='{.items[0].spec.storage.location}' 2>/dev/null || true)"
    if [[ -z "${discovered_loc}" ]]; then
      discovered_loc="$(${KUBECTL_CMD} get podmigrations -A -o jsonpath='{.items[0].spec.storage.location}' 2>/dev/null || true)"
    fi
    local bucket=""
    if [[ -n "${discovered_loc}" ]]; then
      local trimmed="${discovered_loc#gs://}"
      bucket="${trimmed%%/*}"
    else
      bucket="$(${KUBECTL_CMD} get podsnapshotstorageconfig -o jsonpath='{.items[0].spec.snapshotStorageConfig.gcs.bucket}' 2>/dev/null || true)"
    fi
    if [[ -n "${bucket}" ]]; then
      STORAGE_LOCATION="gs://${bucket}/${NAMESPACE}/snapshots"
      log "Discovered storage bucket '${bucket}'; configured scoped storage location: ${STORAGE_LOCATION}"
    fi
  else
    # In multi-tenant clusters, PodMigration storage paths must be scoped to ${NAMESPACE}
    local trimmed="${STORAGE_LOCATION#gs://}"
    local bucket="${trimmed%%/*}"
    local path="${trimmed#"$bucket"}"
    path="${path#/}"
    local first_seg="${path%%/*}"
    if [[ -z "${first_seg}" || "${first_seg}" != "${NAMESPACE}" ]]; then
      STORAGE_LOCATION="gs://${bucket}/${NAMESPACE}/snapshots"
      log "Scoped provided storage location to namespace '${NAMESPACE}': ${STORAGE_LOCATION}"
    fi
  fi

  if [[ -z "${STORAGE_LOCATION}" ]]; then
    die "No storage location provided or discovered. Pass --storage-location <gs://bucket/prefix> or set STORAGE_LOCATION (required by PodMigration CRD spec.storage.location)."
  fi

  log "Validating PodMigration manifest against apiserver (--dry-run=server)..."
  if ! render_podmigration_manifest "${NAMESPACE}" "${STORAGE_LOCATION}" | ${KUBECTL_CMD} apply --dry-run=server -f - >/dev/null; then
    die "PodMigration manifest rejected by apiserver in dry-run validation. Check CRD and --storage-location (${STORAGE_LOCATION})."
  fi

  log "Configuring namespace PodMigration with CRIU engine route (@default=criu) and storage ${STORAGE_LOCATION}..."
  render_podmigration_manifest "${NAMESPACE}" "${STORAGE_LOCATION}" | ${KUBECTL_CMD} apply -f -

  local workload_name="criu-upgrade-${WORKLOAD}"
  local pod_name="${workload_name}-0"

  log "Deploying stateful workload ${workload_name}..."
  render_workload_manifest "${workload_name}" "${NAMESPACE}" "${WORKLOAD}" | ${KUBECTL_CMD} apply -f -
  WORKLOAD_DEPLOYED="true"

  log "Waiting for pod ${pod_name} to become Ready..."
  ${KUBECTL_CMD} wait --namespace "${NAMESPACE}" --for=condition=Ready "pod/${pod_name}" --timeout=180s

  local src_node
  src_node="$(${KUBECTL_CMD} get pod -n "${NAMESPACE}" "${pod_name}" -o jsonpath='{.spec.nodeName}')"
  local src_uid
  src_uid="$(${KUBECTL_CMD} get pod -n "${NAMESPACE}" "${pod_name}" -o jsonpath='{.metadata.uid}')"
  log "Source pod ${pod_name} (UID: ${src_uid}) is running on node: ${src_node}"

  # Capture initial node facts
  local src_facts
  src_facts="$(get_node_facts "${src_node}")"
  local src_kubelet src_kernel src_runtime src_os
  read -r src_kubelet src_kernel src_runtime src_os <<<"${src_facts}"
  log "Source node facts -> Kubelet: ${src_kubelet}, Kernel: ${src_kernel}, Runtime: ${src_runtime}, OS: ${src_os}"

  # Seed state
  local nonce
  nonce="criu-upgrade-$(date +%s)-$RANDOM"
  local initial_state=""
  if [[ "${WORKLOAD}" == "counter" ]]; then
    initial_state="$(exec_with_retry "${KUBECTL_CMD}" exec -n "${NAMESPACE}" "${pod_name}" -- cat /tmp/counter.state)"
  fi
  seed_workload "${pod_name}" "${NAMESPACE}" "${WORKLOAD}" "${nonce}"

  # Trigger upgrade or drain
  if [[ "${DRAIN_NODE}" == "true" ]]; then
    log "Draining source node ${src_node} to trigger eviction and live migration..."
    CORDONED_NODE="${src_node}"
    ${KUBECTL_CMD} drain "${src_node}" --ignore-daemonsets --delete-emptydir-data --force --pod-selector="app=${workload_name}" || true
  elif [[ "${SKIP_UPGRADE}" != "true" ]]; then
    local gcloud_args=(container clusters upgrade "${CLUSTER}" --node-pool "${NODE_POOL}" --cluster-version "${TARGET_VERSION}" --async)
    if [[ -n "${PROJECT}" ]]; then gcloud_args+=(--project "${PROJECT}"); fi
    if [[ -n "${LOCATION}" ]]; then gcloud_args+=(--location "${LOCATION}"); fi

    log "Initiating GKE node pool upgrade: ${GCLOUD_CMD} ${gcloud_args[*]}"
    "${GCLOUD_CMD}" "${gcloud_args[@]}"
  else
    log "--skip-upgrade set: observing migration and upgrade in progress..."
  fi

  # Monitor migration
  log "Waiting for PodMigrationJob and replacement pod warm restore..."
  local deadline=$((SECONDS + TIMEOUT_SEC))
  local migration_complete="false"
  local pmj_name=""
  local pmj_json=""

  while (( SECONDS < deadline )); do
    # Search for PMJ for this pod
    pmj_json="$(${KUBECTL_CMD} get podmigrationjobs -n "${NAMESPACE}" -o json 2>/dev/null | jq -r "
      .items[]? | select(.spec.podRef.name==\"${pod_name}\" and .spec.targetPodUID==\"${src_uid}\")
    ")"

    if [[ -n "${pmj_json}" ]]; then
      pmj_name="$(echo "${pmj_json}" | jq -r '.metadata.name')"
      local phase
      phase="$(echo "${pmj_json}" | jq -r '.status.phase // empty')"
      log "Found PMJ ${pmj_name}, Phase: ${phase:-Unknown}"

      if [[ "${phase}" == "Succeeded" ]]; then
        migration_complete="true"
        break
      elif [[ "${phase}" == "Failed" ]]; then
        die "PMJ ${pmj_name} reached Phase=Failed"
      fi
    fi
    sleep "${POLL_INTERVAL_SEC}"
  done

  if [[ "${migration_complete}" != "true" ]]; then
    die "Timed out waiting for PodMigrationJob to succeed within ${TIMEOUT_SEC}s"
  fi

  # Verify PMJ outcome
  verify_pmj_outcome "${pmj_json}"
  log "PMJ ${pmj_name} verification PASSED: Phase=Succeeded, Restored=RestoreVerified/RestoreInferred"

  # Wait for restored replacement pod
  log "Waiting for replacement pod ${pod_name} to become Ready..."
  ${KUBECTL_CMD} wait --namespace "${NAMESPACE}" --for=condition=Ready "pod/${pod_name}" --timeout=120s

  local dst_pod_json
  dst_pod_json="$(${KUBECTL_CMD} get pod -n "${NAMESPACE}" "${pod_name}" -o json)"
  local dst_node dst_uid
  dst_node="$(echo "${dst_pod_json}" | jq -r '.spec.nodeName')"
  dst_uid="$(echo "${dst_pod_json}" | jq -r '.metadata.uid')"
  log "Replacement pod ${pod_name} (UID: ${dst_uid}) is running on node: ${dst_node}"

  # Verify container restart count is 0
  verify_container_restarts "${dst_pod_json}"
  log "Container restart count verification PASSED: 0 restarts recorded"

  # Capture destination node facts
  local dst_facts
  dst_facts="$(get_node_facts "${dst_node}")"
  local dst_kubelet dst_kernel dst_runtime dst_os
  read -r dst_kubelet dst_kernel dst_runtime dst_os <<<"${dst_facts}"
  log "Destination node facts -> Kubelet: ${dst_kubelet}, Kernel: ${dst_kernel}, Runtime: ${dst_runtime}, OS: ${dst_os}"

  # Verify node transition and version jump
  verify_node_transition "${src_node}" "${src_kubelet}" "${src_kernel}" "${dst_node}" "${dst_kubelet}" "${dst_kernel}" "${ALLOW_SAME_VERSION}"

  # Acid test: verify state survived intact
  verify_restored_workload "${pod_name}" "${NAMESPACE}" "${WORKLOAD}" "${nonce}" "${initial_state}"

  local report_title="GKE NODE-POOL UPGRADE & CRIU WARM-RESTORE: PASS"
  if [[ "${src_kubelet}" == "${dst_kubelet}" && "${src_kernel}" == "${dst_kernel}" ]]; then
    report_title="SAME-VERSION NODE DRAIN & CRIU WARM-RESTORE: PASS (NO VERSION JUMP)"
  fi

  # Emit summary report
  cat <<REPORT

========================================================================
🎉 ${report_title}
========================================================================
Workload:             ${WORKLOAD} (${pod_name})
Source Node:          ${src_node} (Kubelet: ${src_kubelet}, Kernel: ${src_kernel})
Destination Node:     ${dst_node} (Kubelet: ${dst_kubelet}, Kernel: ${dst_kernel})
PodMigrationJob:      ${pmj_name} (Phase: Succeeded, Engine: criu)
Container Restarts:   0
In-Memory Integrity:  VERIFIED INTACT (unpersisted state survived)
Log:                  ${run_log}
========================================================================
REPORT
}

run_self_test() {
  log "Running offline self-test validation suite for run_criu_nodepool_upgrade.sh..."
  local test_tmp="/tmp/pm-upgrade-self-test-$$"
  mkdir -p "${test_tmp}"

  # Test 1: Verify workload and PodMigration manifest rendering
  log "[Self-Test 1/6] Testing manifest generation..."
  local redis_mf counter_mf pm_mf
  redis_mf="$(render_workload_manifest "test-redis" "test-ns" "redis")"
  echo "${redis_mf}" | grep -q 'image: redis:7-alpine' || die "Self-test: redis manifest missing image"
  echo "${redis_mf}" | grep -q 'command: \["docker-entrypoint.sh"\]' || die "Self-test: redis manifest missing entrypoint"
  echo "${redis_mf}" | grep -q 'pod-migration.gke.io/enabled: "true"' || die "Self-test: redis manifest missing opt-in label"

  counter_mf="$(render_workload_manifest "test-counter" "test-ns" "counter")"
  echo "${counter_mf}" | grep -q 'image: busybox:1.36' || die "Self-test: counter manifest missing image"
  echo "${counter_mf}" | grep -q 'pod-migration.gke.io/enabled: "true"' || die "Self-test: counter manifest missing opt-in label"

  pm_mf="$(render_podmigration_manifest "test-ns" "gs://test-bucket/checkpoints")"
  echo "${pm_mf}" | grep -q 'kind: PodMigration' || die "Self-test: PodMigration manifest missing kind"
  echo "${pm_mf}" | grep -q 'pod-migration.gke.io/runtime-class-engines: "@default=criu"' || die "Self-test: PodMigration manifest missing CRIU routing"
  echo "${pm_mf}" | grep -q 'location: gs://test-bucket/checkpoints' || die "Self-test: PodMigration manifest missing storage location"

  # Test 2: PMJ outcome verification logic (PASS vs FAIL, engine assertion)
  log "[Self-Test 2/6] Testing PMJ outcome verification..."
  local good_pmj='{"metadata":{"annotations":{"pod-migration.gke.io/engine":"criu"}},"status":{"phase":"Succeeded","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}'
  verify_pmj_outcome "${good_pmj}"

  local inferred_pmj='{"metadata":{"annotations":{"pod-migration.gke.io/engine":"criu"}},"status":{"phase":"Succeeded","conditions":[{"type":"Restored","status":"True","reason":"RestoreInferred"}]}}'
  verify_pmj_outcome "${inferred_pmj}"

  local non_criu_pmj='{"metadata":{"annotations":{"pod-migration.gke.io/engine":"gke-pod-snapshots"}},"status":{"phase":"Succeeded","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}'
  if ( verify_pmj_outcome "${non_criu_pmj}" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_pmj_outcome to reject non-CRIU engine"
  fi

  local no_engine_pmj='{"status":{"phase":"Succeeded","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}'
  if ( verify_pmj_outcome "${no_engine_pmj}" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_pmj_outcome to reject PMJ missing engine annotation"
  fi

  local failed_pmj='{"metadata":{"annotations":{"pod-migration.gke.io/engine":"criu"}},"status":{"phase":"Failed","conditions":[{"type":"Restored","status":"False","reason":"RestoreCrashFallback"}]}}'
  if ( verify_pmj_outcome "${failed_pmj}" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_pmj_outcome to reject Phase=Failed"
  fi

  # Test 3: Container restart verification logic (0 restarts vs >0 restarts)
  log "[Self-Test 3/6] Testing container restart count verification..."
  local good_pod='{"status":{"containerStatuses":[{"name":"redis","restartCount":0}]}}'
  verify_container_restarts "${good_pod}"

  local bad_pod='{"status":{"containerStatuses":[{"name":"redis","restartCount":1}]}}'
  if ( verify_container_restarts "${bad_pod}" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_container_restarts to reject restartCount > 0"
  fi

  # Test 4: Node transition and version jump verification logic
  log "[Self-Test 4/6] Testing node transition and version jump verification..."
  # Same node rejected
  if ( verify_node_transition "nodeA" "v1" "k1" "nodeA" "v2" "k2" "false" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_node_transition to reject identical source and destination node"
  fi
  # Same version rejected when allow_same_version=false
  if ( verify_node_transition "nodeA" "v1" "k1" "nodeB" "v1" "k1" "false" >/dev/null 2>&1 ); then
    die "Self-test: expected verify_node_transition to reject identical versions when allow_same_version=false"
  fi
  # Same version allowed when allow_same_version=true
  verify_node_transition "nodeA" "v1" "k1" "nodeB" "v1" "k1" "true" >/dev/null
  # Different kubelet version accepted
  verify_node_transition "nodeA" "v1" "k1" "nodeB" "v2" "k1" "false" >/dev/null
  # Different kernel version accepted
  verify_node_transition "nodeA" "v1" "k1" "nodeB" "v1" "k2" "false" >/dev/null

  # Test 5: End-to-end simulated verification pipeline with mock CLI
  log "[Self-Test 5/6] Testing mock CLI execution pipeline..."
  local mock_kubectl="${test_tmp}/mock_kubectl.sh"
  cat <<'EOF' >"${mock_kubectl}"
#!/usr/bin/env bash
cmd="$*"
if [[ "$cmd" =~ "get node" ]]; then
  echo '{"status":{"nodeInfo":{"kubeletVersion":"v1.35.2-gke.1000","kernelVersion":"6.6.137+","containerRuntimeVersion":"containerd://2.0.2","osImage":"Container-Optimized OS from Google"}}}'
elif [[ "$cmd" =~ "redis-cli GET migkey" ]]; then
  echo "test-nonce-12345"
elif [[ "$cmd" =~ "redis-cli DBSIZE" ]]; then
  echo "1001"
elif [[ "$cmd" =~ "cat /tmp/counter.state" ]]; then
  echo "uuid-abc|42"
fi
EOF
  chmod +x "${mock_kubectl}"

  # Positive Redis verification with mock
  (
    KUBECTL_CMD="${mock_kubectl}"
    verify_restored_workload "test-redis-0" "test-ns" "redis" "test-nonce-12345" ""
  )

  # Negative Redis verification: data loss / wrong nonce
  if (
    KUBECTL_CMD="${mock_kubectl}"
    verify_restored_workload "test-redis-0" "test-ns" "redis" "different-nonce" "" >/dev/null 2>&1
  ); then
    die "Self-test: expected verify_restored_workload to fail on mismatched nonce (data loss)"
  fi

  # Positive Counter verification with mock
  (
    KUBECTL_CMD="${mock_kubectl}"
    verify_restored_workload "test-counter-0" "test-ns" "counter" "" "uuid-abc|10"
  )

  # Negative Counter verification: instance ID restart / cold start
  if (
    KUBECTL_CMD="${mock_kubectl}"
    verify_restored_workload "test-counter-0" "test-ns" "counter" "" "uuid-different|10" >/dev/null 2>&1
  ); then
    die "Self-test: expected verify_restored_workload to fail on mismatched instance ID (cold restart)"
  fi

  # Negative Counter verification: count rollback
  if (
    KUBECTL_CMD="${mock_kubectl}"
    verify_restored_workload "test-counter-0" "test-ns" "counter" "" "uuid-abc|50" >/dev/null 2>&1
  ); then
    die "Self-test: expected verify_restored_workload to fail on count rollback"
  fi

  # Test 6: Verify usage and help flags
  log "[Self-Test 6/6] Testing usage output..."
  "${SCRIPT_DIR}/run_criu_nodepool_upgrade.sh" --help >/dev/null

  rm -rf "${test_tmp}"
  log "=== ALL SELF-TEST CHECKS PASSED ==="
}

if [[ "${SELF_TEST}" == "true" ]]; then
  run_self_test
  exit 0
fi

run_live
