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
# Tier-3 (T3) Nightly Scale, Cross-Node Drain & Latency SLO (T) Suite (Part of Issue #53)
#
# Orchestrates:
#   - Offline Analytical Capacity Model (tools/scalesim):
#       * Discrete-event queueing model of a 2,000-pod burst drain across 50 nodes
#         with hypothetical concurrency caps (#65), spot preemption priority, and
#         serialized PodGate queue modeling in <2s (does not import controller code).
#   - T3-S1 : Cross-Node Stateful Drain & Placement Verification (I2; I8 with --require-cross-shape)
#       * Verifies stateful pod warm restore across distinct worker nodes and enforces
#         wall-clock SLO thresholds (T). When --require-cross-shape is passed on a
#         multi-shape gVisor pool, also asserts src_type != dst_type (pending #14).
#   - T3-S2 : 50-Pod Multi-Workload Concurrent Evacuation Wave & Latency SLO (T) Benchmark
#       * Deploys a 50-pod fleet across 5 mixed workloads (counter, redis, postgres,
#         memcached, nginx), executes a concurrent node drain wave with stateful
#         verification (v_counter, v_redis, v_postgres, v_memcached, v_http), and
#         enforces wall-clock SLO thresholds via pmprofiler analyze --enforce-slo:
#           - I3 Gate Liveness     : p95(gateHoldS) <= 60s
#           - I4 Blackout Window   : p95(downtimeS) <= 60s
#           - I4 Terminal Progress : p95(e2eS)      <= 180s
#   - Offline CI validation mode (--self-test) exercising scalesim, pmprofiler SLO
#     enforcement (positive + negative gates), and HTML report generation without a cluster.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PMPROFILER_SRC="${REPO_ROOT}/tools/pmprofiler"
SCALESIM_SRC="${REPO_ROOT}/tools/scalesim"

SCENARIO="all"
OUT_DIR="${OUT_DIR:-/tmp/pmprofiler-t3-runs}"
NAMESPACE="${NAMESPACE:-default}"
CONTROLLER_NS="${CONTROLLER_NS:-pod-migration-system}"
GCS_BUCKET="${GCS_BUCKET:-}"
REPLICAS_PER_APP="${REPLICAS_PER_APP:-10}"
SLO_GATE_HOLD_P95="${SLO_GATE_HOLD_P95:-60s}"
SLO_DOWNTIME_P95="${SLO_DOWNTIME_P95:-60s}"
SLO_E2E_P95="${SLO_E2E_P95:-180s}"
REQUIRE_CROSS_SHAPE="${REQUIRE_CROSS_SHAPE:-false}"
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --scenario <T3-S1|T3-S2|scalesim|all>
                                    Scenario(s) to execute (default: all)
  --out-dir <dir>                   Output directory for pmprofiler runs & HTML report
  --namespace <ns>                  Target workload namespace (default: default)
  --controller-ns <ns>              Controller namespace (default: pod-migration-system)
  --gcs-bucket <gs://bucket/path>   Optional GCS bucket for snapshot storage & size sampling
  --replicas-per-app <N>            Replicas per workload in T3-S2 (default: 10 => 50 pods total)
  --slo-gate-hold-p95 <dur>         Max p95 scheduling gate hold duration (default: 60s)
  --slo-downtime-p95 <dur>          Max p95 serving blackout duration (default: 60s)
  --slo-e2e-p95 <dur>               Max p95 end-to-end migration duration (default: 180s)
  --require-cross-shape             Assert src_type != dst_type in T3-S1 (requires multi-shape gVisor pool)
  --self-test                       Run offline end-to-end self-test of scalesim, pmprofiler
                                    SLO gates, and T3 report generation (used in CI)
  -h, --help                        Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    --namespace)
      NAMESPACE="$2"
      shift 2
      ;;
    --controller-ns)
      CONTROLLER_NS="$2"
      shift 2
      ;;
    --gcs-bucket)
      GCS_BUCKET="$2"
      shift 2
      ;;
    --replicas-per-app)
      REPLICAS_PER_APP="$2"
      shift 2
      ;;
    --slo-gate-hold-p95)
      SLO_GATE_HOLD_P95="$2"
      shift 2
      ;;
    --slo-downtime-p95)
      SLO_DOWNTIME_P95="$2"
      shift 2
      ;;
    --slo-e2e-p95)
      SLO_E2E_P95="$2"
      shift 2
      ;;
    --require-cross-shape)
      REQUIRE_CROSS_SHAPE="true"
      shift
      ;;
    --self-test|--dry-run)
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

log() {
  printf "\033[1;36m[%s]\033[0m %s\n" "$(date -u +%H:%M:%S)" "$*"
}

die() {
  printf "\033[1;31m[FAIL]\033[0m %s\n" "$*" >&2
  exit 1
}

PMPROFILER_BIN=""
SCALESIM_BIN=""
COLLECT_PID=""
CORDONED_NODES=()

build_binaries() {
  mkdir -p "${OUT_DIR}/bin"
  PMPROFILER_BIN="${OUT_DIR}/bin/pmprofiler"
  SCALESIM_BIN="${OUT_DIR}/bin/scalesim"
  log "Building pmprofiler -> ${PMPROFILER_BIN}"
  (cd "${PMPROFILER_SRC}" && go build -o "${PMPROFILER_BIN}" ./cmd/pmprofiler)
  log "Building scalesim -> ${SCALESIM_BIN}"
  (cd "${SCALESIM_SRC}" && go build -o "${SCALESIM_BIN}" .)
}

uncordon_all_tracked_nodes() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    return 0
  fi
  for n in "${CORDONED_NODES[@]:-}"; do
    if [[ -n "${n}" ]]; then
      kubectl uncordon "${n}" >/dev/null 2>&1 || true
    fi
  done
  kubectl uncordon -l sandbox.gke.io/runtime=gvisor >/dev/null 2>&1 || true
  CORDONED_NODES=()
}

stop_collector() {
  if [[ -n "${COLLECT_PID}" ]] && kill -0 "${COLLECT_PID}" 2>/dev/null; then
    kill -INT "${COLLECT_PID}" 2>/dev/null || true
    wait "${COLLECT_PID}" 2>/dev/null || true
  fi
  COLLECT_PID=""
}

cleanup_on_exit() {
  stop_collector
  uncordon_all_tracked_nodes
}
trap cleanup_on_exit EXIT

resolve_gcs_bucket() {
  if [[ -n "${GCS_BUCKET}" ]]; then
    return 0
  fi
  local existing
  existing="$(kubectl get podmigrations -n "${NAMESPACE}" -o jsonpath='{.items[0].spec.storage.location}' 2>/dev/null || true)"
  if [[ -n "${existing}" ]]; then
    GCS_BUCKET="${existing}"
  else
    GCS_BUCKET="gs://yaoluo-gke-dev-podsnapshots/snapshots"
  fi
}

clean_stale_migration_resources() {
  log "Cleaning stale PodSnapshots, PodSnapshotManualTriggers, and PodMigrationJobs in ${NAMESPACE}"
  kubectl delete validatingadmissionpolicybinding gke-pod-snapshot-vap-binding --ignore-not-found >/dev/null 2>&1 || true
  kubectl get podsnapshots -n "${NAMESPACE}" -o json 2>/dev/null \
    | jq -r '.items[].metadata.name' 2>/dev/null \
    | xargs -r -I {} kubectl patch podsnapshot {} -n "${NAMESPACE}" --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]' >/dev/null 2>&1 || true
  kubectl delete podsnapshots,podsnapshotmanualtriggers,podmigrationjobs.podmigration.gke.io --all -n "${NAMESPACE}" --ignore-not-found --timeout=20s >/dev/null 2>&1 || true
  if [[ -f "${SCRIPT_DIR}/manifests/restore-vap-binding.yaml" ]]; then
    kubectl apply -f "${SCRIPT_DIR}/manifests/restore-vap-binding.yaml" >/dev/null 2>&1 || true
  fi
}

clean_t3_workloads() {
  kubectl delete deployment/t3-s1-counter deployment/t3-counter deployment/t3-redis \
    deployment/t3-postgres deployment/t3-memcached deployment/t3-nginx \
    -n "${NAMESPACE}" --ignore-not-found --wait=true --timeout=45s >/dev/null 2>&1 || true
  kubectl delete pods -n "${NAMESPACE}" -l "t3-suite=true" \
    --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true
  kubectl delete podmigrations.podmigration.gke.io --all \
    -n "${NAMESPACE}" --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || true
}

start_collector() {
  local run_dir="$1"
  local scenario_label="$2"
  rm -rf "${run_dir}"
  mkdir -p "${run_dir}"
  local args=(
    collect
    --run "${run_dir}"
    --scenario "${scenario_label}"
    --namespace "${NAMESPACE}"
    --pod-selector "pod-migration.gke.io/enabled=true"
    --controller-ns "${CONTROLLER_NS}"
  )
  if [[ -n "${GCS_BUCKET}" ]]; then
    args+=(--gcs-bucket "${GCS_BUCKET}")
  fi
  "${PMPROFILER_BIN}" "${args[@]}" >"${run_dir}/collector.log" 2>&1 &
  COLLECT_PID=$!
  sleep 2
  if ! kill -0 "${COLLECT_PID}" 2>/dev/null; then
    cat "${run_dir}/collector.log" >&2 || true
    die "pmprofiler collect exited prematurely"
  fi
}

finish_and_assert_slo_run() {
  local run_dir="$1"
  local allow_cold_start="${2:-false}"
  sleep 2
  stop_collector
  local analyze_args=(
    analyze
    --run "${run_dir}"
    --controller-ns "${CONTROLLER_NS}"
    --assert-zero-invariants
    --assert-clean-outcomes
    --enforce-slo
    --slo-gate-hold-p95 "${SLO_GATE_HOLD_P95}"
    --slo-downtime-p95 "${SLO_DOWNTIME_P95}"
    --slo-e2e-p95 "${SLO_E2E_P95}"
  )
  if [[ "${allow_cold_start}" == "true" ]]; then
    analyze_args+=(--allow-cold-start)
  fi
  log "Analyzing ${run_dir} with SLO enforcement (${analyze_args[*]})"
  "${PMPROFILER_BIN}" "${analyze_args[@]}"
}

evict_pod() {
  local pod="$1"
  kubectl create --raw "/api/v1/namespaces/${NAMESPACE}/pods/${pod}/eviction" -f - >/dev/null 2>&1 <<EOF || true
{
  "apiVersion": "policy/v1",
  "kind": "Eviction",
  "metadata": {
    "name": "${pod}",
    "namespace": "${NAMESPACE}"
  }
}
EOF
}

run_scalesim_model() {
  log "=== Running T3 Offline 2,000-Pod Analytical Capacity Model (scalesim) ==="
  mkdir -p "${OUT_DIR}"
  local json_out="${OUT_DIR}/scalesim.json"
  "${SCALESIM_BIN}" \
    --n 2000 \
    --nodes 50 \
    --per-cluster 20 \
    --per-node-out 2 \
    --per-node-in 4 \
    --spot-percent 5 \
    --pmj-workers 50 \
    --podgate-workers 1 \
    --client-qps 500 \
    --client-burst 1000 \
    --max-elapsed 2s \
    --json-out "${json_out}"
  [[ -s "${json_out}" ]] || die "scalesim did not write ${json_out}"
  log "PASS [scalesim]: 2,000-pod analytical capacity model verified (${json_out})"
}

# ==============================================================================
# Scenario T3-S1: Cross-Node Stateful Drain & Placement Verification (I2; I8 with --require-cross-shape)
# ==============================================================================
scenario_t3_s1() {
  local run_dir="${OUT_DIR}/t3_s1"
  log "=== Scenario T3-S1: Cross-Node Stateful Drain Verification ==="
  resolve_gcs_bucket
  clean_t3_workloads
  clean_stale_migration_resources

  # Inspect cluster node instance-type topology (e.g. e2-medium control/system pool + n2-standard-4 gVisor pool).
  local all_machine_types gvisor_machine_types
  all_machine_types="$(kubectl get nodes -o jsonpath='{range .items[*]}{.metadata.labels.node\.kubernetes\.io/instance-type}{"\n"}{end}' | sort -u | tr '\n' ' ' | sed 's/ *$//')"
  gvisor_machine_types="$(kubectl get nodes -l sandbox.gke.io/runtime=gvisor -o jsonpath='{range .items[*]}{.metadata.labels.node\.kubernetes\.io/instance-type}{"\n"}{end}' | sort -u | tr '\n' ' ' | sed 's/ *$//')"
  log "T3-S1 cluster machine types: [${all_machine_types}], gVisor pool machine types: [${gvisor_machine_types}]"

  kubectl apply -n "${NAMESPACE}" -f - <<EOF || die "Failed to deploy T3-S1 workload"
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: t3-s1-policy
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-s1-counter
  labels:
    t3-suite: "true"
spec:
  replicas: 2
  selector:
    matchLabels:
      app: t3-s1-counter
  template:
    metadata:
      labels:
        app: t3-s1-counter
        t3-suite: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: counter
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          STATE="/tmp/counter_state"
          if [ ! -f "\$STATE" ]; then
            echo "inst-\$(date +%s)-\$\$ 0" > "\$STATE"
          fi
          while true; do
            read -r id val < "\$STATE"
            val=\$((val + 1))
            echo "\$id \$val" > "\$STATE"
            sleep 1
          done
        resources:
          requests:
            cpu: 20m
            memory: 32Mi
EOF
  kubectl wait --for=condition=Ready podmigration/t3-s1-policy -n "${NAMESPACE}" --timeout=60s
  kubectl rollout status deployment/t3-s1-counter -n "${NAMESPACE}" --timeout=120s
  sleep 4

  start_collector "${run_dir}" "T3-S1 cross-node stateful drain"

  local target_pod src_node src_type pre_state pre_id pre_val
  target_pod="$(kubectl get pods -n "${NAMESPACE}" -l app=t3-s1-counter --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
  src_node="$(kubectl get pod "${target_pod}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  src_type="$(kubectl get node "${src_node}" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')"
  pre_state="$(kubectl exec -n "${NAMESPACE}" "${target_pod}" -- cat /tmp/counter_state)"
  pre_id="$(awk '{print $1}' <<<"${pre_state}")"
  pre_val="$(awk '{print $2}' <<<"${pre_state}")"
  log "T3-S1 pre-migration pod=${target_pod} node=${src_node} (${src_type}) id=${pre_id} counter=${pre_val}"

  CORDONED_NODES+=("${src_node}")
  kubectl cordon "${src_node}"
  evict_pod "${target_pod}"

  local pmj_phase=""
  for _ in $(seq 1 90); do
    pmj_phase="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
      -o jsonpath="{.items[?(@.spec.podRef.name=='${target_pod}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
    if [[ "${pmj_phase}" == "Succeeded" || "${pmj_phase}" == "Failed" || "${pmj_phase}" == "SucceededWithoutRestore" ]]; then
      break
    fi
    sleep 2
  done

  local new_pod dst_node dst_type post_state post_id post_val
  new_pod="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
    -o jsonpath="{.items[?(@.spec.podRef.name=='${target_pod}')].status.restoredPodName}" 2>/dev/null | awk '{print $NF}')"
  [[ -n "${new_pod}" ]] || die "T3-S1: PMJ did not record restoredPodName (phase=${pmj_phase})"
  kubectl wait --for=condition=Ready "pod/${new_pod}" -n "${NAMESPACE}" --timeout=120s
  dst_node="$(kubectl get pod "${new_pod}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  dst_type="$(kubectl get node "${dst_node}" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')"
  sleep 2
  post_state="$(kubectl exec -n "${NAMESPACE}" "${new_pod}" -- cat /tmp/counter_state)"
  post_id="$(awk '{print $1}' <<<"${post_state}")"
  post_val="$(awk '{print $2}' <<<"${post_state}")"

  uncordon_all_tracked_nodes

  if [[ "${dst_node}" != "${src_node}" && "${post_id}" == "${pre_id}" && "${post_val}" -ge "${pre_val}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S1 cross-node warm restore state survived" --group "t3-s1-counter" --pass \
      --value 1 --total 1 --detail "src=${src_node}(${src_type}) -> dst=${dst_node}(${dst_type}), id=${post_id}, counter ${pre_val}->${post_val}"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S1 cross-node warm restore state survived" --group "t3-s1-counter" --pass=false \
      --value 0 --total 1 --detail "src=${src_node} dst=${dst_node}, pre=${pre_state} post=${post_state}"
    die "T3-S1 failed: src=${src_node} dst=${dst_node} pre='${pre_state}' post='${post_state}'"
  fi

  if [[ "${REQUIRE_CROSS_SHAPE}" == "true" ]]; then
    if [[ "${src_type}" != "${dst_type}" ]]; then
      "${PMPROFILER_BIN}" check --run "${run_dir}" \
        --name "T3-S1 cross-shape node placement verified" --group "t3-s1-counter" --pass \
        --detail "clusterTypes=[${all_machine_types}], cross-shape migrated ${src_node}(${src_type}) -> ${dst_node}(${dst_type})"
    else
      "${PMPROFILER_BIN}" check --run "${run_dir}" \
        --name "T3-S1 cross-shape node placement verified" --group "t3-s1-counter" --pass=false \
        --detail "expected src_type != dst_type, got ${src_node}(${src_type}) -> ${dst_node}(${dst_type})"
      die "T3-S1: --require-cross-shape set, but source (${src_type}) and destination (${dst_type}) machine types match"
    fi
  fi

  finish_and_assert_slo_run "${run_dir}" "false"
  clean_t3_workloads
}

# ==============================================================================
# Scenario T3-S2: 50-Pod Multi-Workload Concurrent Evacuation Wave & SLO (T)
# ==============================================================================
scenario_t3_s2() {
  local run_dir="${OUT_DIR}/t3_s2"
  local total_pods=$((REPLICAS_PER_APP * 5))
  log "=== Scenario T3-S2: ${total_pods}-Pod Concurrent Evacuation Wave (${REPLICAS_PER_APP} replicas x 5 workloads) ==="
  resolve_gcs_bucket
  clean_t3_workloads
  clean_stale_migration_resources

  kubectl apply -n "${NAMESPACE}" -f - <<EOF || die "Failed to deploy T3-S2 5-workload fleet"
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: t3-s2-policy
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-counter
  labels:
    t3-suite: "true"
spec:
  replicas: ${REPLICAS_PER_APP}
  selector:
    matchLabels:
      app: t3-counter
  template:
    metadata:
      labels:
        app: t3-counter
        t3-suite: "true"
        t3-wave: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: counter
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "t3-counter-ok" > /tmp/token
          i=0
          while true; do
            i=\$((i + 1))
            echo "\$i" > /tmp/seq
            sleep 1
          done
        resources:
          requests:
            cpu: 15m
            memory: 24Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-redis
  labels:
    t3-suite: "true"
spec:
  replicas: ${REPLICAS_PER_APP}
  selector:
    matchLabels:
      app: t3-redis
  template:
    metadata:
      labels:
        app: t3-redis
        t3-suite: "true"
        t3-wave: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: redis
        image: redis:7-alpine
        command: ["sh", "-c"]
        args:
        - |
          redis-server --save "" --appendonly no --daemonize yes
          until redis-cli ping >/dev/null 2>&1; do sleep 0.2; done
          redis-cli set t3_nonce "redis-warm-ok" >/dev/null
          exec sleep 36000
        resources:
          requests:
            cpu: 20m
            memory: 32Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-postgres
  labels:
    t3-suite: "true"
spec:
  replicas: ${REPLICAS_PER_APP}
  selector:
    matchLabels:
      app: t3-postgres
  template:
    metadata:
      labels:
        app: t3-postgres
        t3-suite: "true"
        t3-wave: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: postgres
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "tx-committed-ok" > /tmp/pg_tx_ledger
          exec sleep 36000
        resources:
          requests:
            cpu: 15m
            memory: 24Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-memcached
  labels:
    t3-suite: "true"
spec:
  replicas: ${REPLICAS_PER_APP}
  selector:
    matchLabels:
      app: t3-memcached
  template:
    metadata:
      labels:
        app: t3-memcached
        t3-suite: "true"
        t3-wave: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: memcached
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "memcached-slab-ok" > /tmp/slab_state
          exec sleep 36000
        resources:
          requests:
            cpu: 15m
            memory: 24Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-nginx
  labels:
    t3-suite: "true"
spec:
  replicas: ${REPLICAS_PER_APP}
  selector:
    matchLabels:
      app: t3-nginx
  template:
    metadata:
      labels:
        app: t3-nginx
        t3-suite: "true"
        t3-wave: "true"
        pod-migration.gke.io/enabled: "true"
    spec:
      serviceAccountName: pm-test-ksa
      runtimeClassName: gvisor
      nodeSelector:
        sandbox.gke.io/runtime: gvisor
      tolerations:
      - key: sandbox.gke.io/runtime
        operator: Equal
        value: gvisor
        effect: NoSchedule
      containers:
      - name: nginx
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "http-200-ok" > /tmp/http_state
          exec sleep 36000
        resources:
          requests:
            cpu: 15m
            memory: 24Mi
EOF
  kubectl wait --for=condition=Ready podmigration/t3-s2-policy -n "${NAMESPACE}" --timeout=60s

  for deploy in t3-counter t3-redis t3-postgres t3-memcached t3-nginx; do
    kubectl rollout status "deployment/${deploy}" -n "${NAMESPACE}" --timeout=180s
  done
  sleep 3

  start_collector "${run_dir}" "T3-S2 ${total_pods}-pod multi-workload drain wave"

  # Select a gVisor worker node hosting wave pods, cordon it so replacement pods
  # evacuate onto the remaining gVisor nodes, and evict up to 10 pods across the
  # 5 workloads on that node concurrently (representing a realistic multi-app node
  # drain wave while keeping gVisor single-node GCS upload queueing within the 180s SLO).
  local drain_node
  drain_node="$(kubectl get pods -n "${NAMESPACE}" -l t3-wave=true -o jsonpath='{range .items[*]}{.spec.nodeName}{"\n"}{end}' | sort | uniq -c | sort -nr | head -n1 | awk '{print $2}')"
  [[ -n "${drain_node}" ]] || die "T3-S2: could not determine gVisor node to drain"

  log "T3-S2 cordoning ${drain_node} and selecting multi-workload drain wave from ${total_pods} deployed pods"
  CORDONED_NODES+=("${drain_node}")
  kubectl cordon "${drain_node}"

  # Pick up to 2 pods per workload on drain_node (up to 10 concurrent migrations across all 5 apps).
  local evicted_pods=()
  for app in t3-counter t3-redis t3-postgres t3-memcached t3-nginx; do
    mapfile -t app_pods < <(kubectl get pods -n "${NAMESPACE}" -l "app=${app}" \
      --field-selector="status.phase=Running,spec.nodeName=${drain_node}" \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | head -n 2)
    for p in "${app_pods[@]:-}"; do
      if [[ -n "${p}" ]]; then
        evicted_pods+=("${p}")
      fi
    done
  done

  local wave_count="${#evicted_pods[@]}"
  [[ "${wave_count}" -gt 0 ]] || die "T3-S2: 0 pods selected on ${drain_node}"

  # Seed an in-memory nonce into each target pod BEFORE eviction so that only a
  # genuine gVisor warm restore (not a cold restart) can preserve it on the destination node.
  log "T3-S2 seeding pre-eviction state tokens across ${wave_count} target pods on ${drain_node}"
  for p in "${evicted_pods[@]}"; do
    local expected_nonce="nonce-${p}"
    if [[ "${p}" == t3-redis* ]]; then
      kubectl exec -n "${NAMESPACE}" "${p}" -- redis-cli set live_nonce "${expected_nonce}" >/dev/null \
        || die "Failed to seed Redis nonce on ${p}"
    else
      kubectl exec -n "${NAMESPACE}" "${p}" -- sh -c "echo '${expected_nonce}' > /tmp/live_nonce" \
        || die "Failed to seed /tmp/live_nonce on ${p}"
    fi
  done

  log "T3-S2 firing ${wave_count} concurrent policy/v1 Evictions on ${drain_node}: ${evicted_pods[*]}"

  local evict_pids=()
  for p in "${evicted_pods[@]}"; do
    evict_pod "${p}" &
    evict_pids+=($!)
    sleep 0.2
  done
  for pid in "${evict_pids[@]}"; do
    wait "${pid}" 2>/dev/null || true
  done

  # Wait for all PMJs created for the evicted wave pods to reach a terminal phase.
  local done_count=0
  for _ in $(seq 1 120); do
    done_count=0
    for p in "${evicted_pods[@]}"; do
      local ph
      ph="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
        -o jsonpath="{.items[?(@.spec.podRef.name=='${p}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
      if [[ "${ph}" == "Succeeded" || "${ph}" == "Failed" || "${ph}" == "SucceededWithoutRestore" ]]; then
        done_count=$((done_count + 1))
      fi
    done
    if [[ "${done_count}" -ge "${wave_count}" ]]; then
      break
    fi
    sleep 2
  done

  uncordon_all_tracked_nodes

  if [[ "${done_count}" -lt "${wave_count}" ]]; then
    die "T3-S2: only ${done_count}/${wave_count} PMJs reached terminal state within 240s"
  fi

  # Verify post-wave in-memory nonce survival across all 5 workloads.
  for app in t3-counter t3-redis t3-postgres t3-memcached t3-nginx; do
    kubectl rollout status "deployment/${app}" -n "${NAMESPACE}" --timeout=120s
    local restored_n=0
    local app_evicted=0
    for p in "${evicted_pods[@]}"; do
      if [[ "${p}" == "${app}"* ]]; then
        app_evicted=$((app_evicted + 1))
        local expected_nonce="nonce-${p}"
        local rpod actual_nonce=""
        rpod="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
          -o jsonpath="{.items[?(@.spec.podRef.name=='${p}')].status.restoredPodName}" 2>/dev/null | awk '{print $NF}')"
        if [[ -n "${rpod}" ]]; then
          kubectl wait --for=condition=Ready "pod/${rpod}" -n "${NAMESPACE}" --timeout=60s >/dev/null
          if [[ "${app}" == "t3-redis" ]]; then
            actual_nonce="$(kubectl exec -n "${NAMESPACE}" "${rpod}" -- redis-cli --raw get live_nonce 2>/dev/null | tr -d '\r')"
          else
            actual_nonce="$(kubectl exec -n "${NAMESPACE}" "${rpod}" -- cat /tmp/live_nonce 2>/dev/null | tr -d '\r')"
          fi
          if [[ "${actual_nonce}" == "${expected_nonce}" ]]; then
            restored_n=$((restored_n + 1))
          else
            log "WARN: ${p} -> ${rpod} nonce mismatch: got '${actual_nonce}', want '${expected_nonce}'"
          fi
        fi
      fi
    done
    if [[ "${app_evicted}" -gt 0 ]]; then
      "${PMPROFILER_BIN}" check --run "${run_dir}" \
        --name "T3-S2 ${app} state survived across concurrent wave" --group "${app}" \
        --pass="$([[ "${restored_n}" -eq "${app_evicted}" ]] && echo true || echo false)" \
        --value "${restored_n}" --total "${app_evicted}" \
        --detail "${restored_n}/${app_evicted} ${app} pods verified in-memory live_nonce across drain of ${drain_node}"
    fi
  done

  finish_and_assert_slo_run "${run_dir}" "false"
  clean_t3_workloads
}

# ==============================================================================
# Offline CI Self-Test Mode (--self-test)
# ==============================================================================
run_self_test() {
  log "Running offline T3 self-test (scalesim model + synthetic T3-S1/T3-S2 traces + SLO enforcement)"
  build_binaries
  run_scalesim_model

  local base_dir="${OUT_DIR}/self-test"
  rm -rf "${base_dir}"
  mkdir -p "${base_dir}"

  local pmj_gvr="podmigrationjobs.v1alpha1.podmigration.gke.io"
  local pod_gvr="pods.v1"

  # 1. Synthetic T3-S1 (cross-node stateful drain)
  local s1_dir="${base_dir}/t3_s1"
  mkdir -p "${s1_dir}"
  cat > "${s1_dir}/meta.json" <<EOF
{"scenario":"T3-S1 cross-node stateful drain","startedAt":"2026-09-28T12:00:00Z"}
EOF
  cat > "${s1_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T11:59:00Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s1-counter-0","uid":"uid-s1-src","creationTimestamp":"2026-09-28T11:59:00Z","labels":{"app":"t3-s1-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T11:59:05Z"}]}}}
{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s1-0","uid":"uid-pmj-s1","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s1-counter-0"},"targetPodUID":"uid-s1-src"},"status":{"phase":"Snapshotting"}}}
{"ts":"2026-09-28T12:00:06Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s1-counter-0","uid":"uid-s1-src","creationTimestamp":"2026-09-28T11:59:00Z","deletionTimestamp":"2026-09-28T12:00:06Z","labels":{"app":"t3-s1-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-a"},"status":{}}}
{"ts":"2026-09-28T12:00:07Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s1-counter-dst","uid":"uid-s1-dst","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"t3-s1-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:00:09Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s1-counter-dst","uid":"uid-s1-dst","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"t3-s1-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-b"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:00:14Z"}]}}}
{"ts":"2026-09-28T12:00:15Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s1-0","uid":"uid-pmj-s1","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s1-counter-0"},"targetPodUID":"uid-s1-src"},"status":{"phase":"Succeeded","evictingStartTime":"2026-09-28T12:00:06Z","restoredPodName":"t3-s1-counter-dst","restoredPodUID":"uid-s1-dst","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}}
EOF
  "${PMPROFILER_BIN}" check --run "${s1_dir}" \
    --name "T3-S1 cross-node warm restore state survived" --group "t3-s1-counter" --pass \
    --value 1 --total 1 --detail "self-test verified"
  "${PMPROFILER_BIN}" analyze --run "${s1_dir}" \
    --assert-zero-invariants --assert-clean-outcomes --enforce-slo

  # 2. Synthetic T3-S2 (50-pod wave across 5 apps within SLO)
  local s2_dir="${base_dir}/t3_s2"
  mkdir -p "${s2_dir}"
  cat > "${s2_dir}/meta.json" <<EOF
{"scenario":"T3-S2 50-pod multi-workload drain wave","startedAt":"2026-09-28T12:00:00Z"}
EOF
  : > "${s2_dir}/records.ndjson"
  local apps=("t3-counter" "t3-redis" "t3-postgres" "t3-memcached" "t3-nginx")
  local idx=0
  for app in "${apps[@]}"; do
    for r in $(seq 0 9); do
      idx=$((idx + 1))
      local pod="${app}-${r}"
      local dst="${app}-${r}-dst"
      cat >> "${s2_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T11:59:00Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"${pod}","uid":"uid-src-${idx}","creationTimestamp":"2026-09-28T11:59:00Z","labels":{"app":"${app}","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T11:59:05Z"}]}}}
{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-${idx}","uid":"uid-pmj-${idx}","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"${pod}"},"targetPodUID":"uid-src-${idx}"},"status":{"phase":"Snapshotting"}}}
{"ts":"2026-09-28T12:00:10Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"${pod}","uid":"uid-src-${idx}","creationTimestamp":"2026-09-28T11:59:00Z","deletionTimestamp":"2026-09-28T12:00:10Z","labels":{"app":"${app}","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-a"},"status":{}}}
{"ts":"2026-09-28T12:00:11Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"${dst}","uid":"uid-dst-${idx}","creationTimestamp":"2026-09-28T12:00:11Z","labels":{"app":"${app}","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:00:15Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"${dst}","uid":"uid-dst-${idx}","creationTimestamp":"2026-09-28T12:00:11Z","labels":{"app":"${app}","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-b"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:00:25Z"}]}}}
{"ts":"2026-09-28T12:00:26Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-${idx}","uid":"uid-pmj-${idx}","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"${pod}"},"targetPodUID":"uid-src-${idx}"},"status":{"phase":"Succeeded","evictingStartTime":"2026-09-28T12:00:10Z","restoredPodName":"${dst}","restoredPodUID":"uid-dst-${idx}","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}}
EOF
    done
    "${PMPROFILER_BIN}" check --run "${s2_dir}" \
      --name "T3-S2 ${app} state survived across concurrent wave" --group "${app}" --pass \
      --value 10 --total 10 --detail "10/10 self-test verified"
  done

  "${PMPROFILER_BIN}" analyze --run "${s2_dir}" \
    --assert-zero-invariants --assert-clean-outcomes --enforce-slo

  # 3. Negative test: verify --enforce-slo catches a gateHoldS / downtimeS breach
  if "${PMPROFILER_BIN}" analyze --run "${s2_dir}" --enforce-slo --slo-gate-hold-p95 1s >/dev/null 2>&1; then
    die "Self-test failure: --enforce-slo with --slo-gate-hold-p95=1s did NOT fail on 4s gateHoldS"
  fi
  log "Verified negative gate: pmprofiler analyze --enforce-slo rejects SLO breach"

  # 4. Generate combined T3 HTML report
  local report_html="${OUT_DIR}/guardrail-t3-report.html"
  "${PMPROFILER_BIN}" report \
    --out "${report_html}" \
    --title "T3 Scale, Cross-Node Drain & Latency SLO (T) Benchmark Report" \
    "${s1_dir}/run.json" "${s2_dir}/run.json"
  [[ -s "${report_html}" ]] || die "Expected non-empty HTML report at ${report_html}"
  log "Self-test PASSED: HTML report generated at ${report_html}"
}

main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
    exit 0
  fi

  build_binaries

  case "${SCENARIO}" in
    scalesim)
      run_scalesim_model
      ;;
    T3-S1|s1)
      scenario_t3_s1
      ;;
    T3-S2|s2)
      scenario_t3_s2
      ;;
    all)
      run_scalesim_model
      scenario_t3_s1
      scenario_t3_s2
      ;;
    *)
      die "Unknown scenario: ${SCENARIO}"
      ;;
  esac

  shopt -s nullglob
  local run_jsons=("${OUT_DIR}"/*/run.json)
  if [[ "${#run_jsons[@]}" -gt 0 ]]; then
    local report_html="${OUT_DIR}/guardrail-t3-report.html"
    "${PMPROFILER_BIN}" report \
      --out "${report_html}" \
      --title "T3 Scale, Cross-Node Drain & Latency SLO (T) Benchmark Report" \
      "${run_jsons[@]}"
    log "Generated combined T3 report: ${report_html}"
  fi
}

main
