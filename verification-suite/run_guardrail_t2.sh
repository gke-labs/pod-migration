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
# Tier-2 (T2) Per-Merge Cluster Scenario & Stateful Verifier Suite (Issues #52, #103, #104)
#
# Orchestrates:
#   - 3 Stateful Application Verifiers + Continuous Write-Continuity Probe (I2 / #103):
#       * v_counter    : Monotonic counter + instanceID continuity across warm restore
#       * v_redis      : 50,000 populated keys + per-cycle nonce across warm restore
#       * v_postgres   : Active transactional sequence table without gap/rollback
#       * continuity   : Continuous monotonic sequence writer + reader-identity probe
#                        catching lost acknowledged writes, empty-source serving, and
#                        double-restore rollbacks across migration.
#   - 8 Adversarial T2 Scenarios (S1-S8) exercising invariants I1-I11:
#       * S1 : Deployment rolling upgrade during active node drain (I6, I2)
#       * S2 : PDB minAvailable: 100% block followed by budget release (I7, I2)
#       * S3 : Corrupted checkpoint artifact in GCS -> deterministic I9 cold-start fallback
#       * S4 : Dual-replica simultaneous eviction under serialized PodGate contention (I1, I3, I2)
#       * S5 : Mid-flight PodMigration deletion -> I4/I5 deferral & zero orphan PSSC/PSMT leaks
#       * S6 : Continuous write-continuity probe under live warm migration (#103)
#       * S7 : Verdict-matches-data gate: RestoreVerified vs. live application state (#104)
#       * S8 : Phase-controlled write-continuity scenario (#149) with kube-proxy sync
#              phase anchor, Check (a) no source re-restore after checkpoint (#151),
#              and Check (b) fenced pod never serving in EndpointSlice (#131)
#   - Offline CI validation mode (--self-test) exercising pmprofiler check, continuity,
#     analyze, invariant gates, S7 verdict-matches-data checks, S8 phase-continuity
#     checks, and report generation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PMPROFILER_SRC="${REPO_ROOT}/tools/pmprofiler"

SCENARIO="all"
VERIFIER_ONLY=""
OUT_DIR="${OUT_DIR:-/tmp/pmprofiler-t2-runs}"
NAMESPACE="${NAMESPACE:-default}"
CONTROLLER_NS="${CONTROLLER_NS:-pod-migration-system}"
GCS_BUCKET="${GCS_BUCKET:-}"
RUNTIME_CLASS="${RUNTIME_CLASS:-gvisor}"
NODE_SELECTOR="${NODE_SELECTOR:-sandbox.gke.io/runtime=gvisor}"
CORDON_SELECTOR="${CORDON_SELECTOR:-}"
TOLERATION="${TOLERATION:-sandbox.gke.io/runtime=gvisor:NoSchedule}"
SERVICE_ACCOUNT="${SERVICE_ACCOUNT:-pm-test-ksa}"
SKIP_PODMIGRATION_CREATE="${SKIP_PODMIGRATION_CREATE:-false}"
S8_WINDOW_W="${S8_WINDOW_W:-5}"
S8_ENFORCE_CHECK_A="${S8_ENFORCE_CHECK_A:-true}"
S8_ENFORCE_CHECK_B="${S8_ENFORCE_CHECK_B:-false}"
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --scenario <S1|S2|S3|S4|S5|S6|S7|S8|all>
                                    Scenario(s) to execute (default: all)
  --verifier-only <counter|redis|postgres>
                                    Run a single stateful app verifier on the cluster
  --out-dir <dir>                   Output directory for pmprofiler runs & HTML report
  --namespace <ns>                  Target workload namespace (default: default)
  --controller-ns <ns>              Controller namespace (default: pod-migration-system)
  --gcs-bucket <gs://bucket/path>   GCS bucket for snapshot storage & size sampling
  --runtime-class <name|none>       Pod runtimeClassName (default: gvisor; 'none' or '' omits it)
  --node-selector <k=v|none>        Pod nodeSelector key=value (default: sandbox.gke.io/runtime=gvisor; 'none' or '' omits it)
  --cordon-selector <k=v|all|none>  Node label selector for S3 pool cordon (default: uses --node-selector when set;
                                    requires explicit '<k=v>' or 'all' when --node-selector is 'none'; 'none' skips pool cordon)
  --toleration <k=v:effect|none>    Pod toleration in '<key>[=<value>]:<Effect>' format (default: sandbox.gke.io/runtime=gvisor:NoSchedule; 'none' or '' omits it)
  --service-account <name|none>     Pod serviceAccountName (default: pm-test-ksa; 'none' or '' omits it)
  --skip-podmigration-create        Use a pre-created snapshot policy instead of applying a PodMigration CR
  --s8-window-w <seconds>           Target kube-proxy sync offset W (in seconds after drain, single value or comma-separated list) for S8 phase anchor (default: 5)
  --enforce-s8-check-a              Fail S8 if Check (a) detects a source pod re-restore event after checkpoint (#151; default: true since PR #156)
  --no-enforce-s8-check-a           Downgrade S8 Check (a) to report-only warning
  --enforce-s8-check-b              Fail S8 if Check (b) detects a fenced pod serving in EndpointSlice or cold_served/empty_reads > 0 (#131)
  --enforce-s8-gates                Enable both --enforce-s8-check-a and --enforce-s8-check-b
  --self-test                       Run offline end-to-end self-test of pmprofiler + T2
                                    scenario fixtures and invariant gates (used in CI)
  -h, --help                        Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --scenario)
      SCENARIO="$2"
      shift 2
      ;;
    --verifier-only)
      VERIFIER_ONLY="$2"
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
    --runtime-class)
      RUNTIME_CLASS="$2"
      shift 2
      ;;
    --node-selector)
      NODE_SELECTOR="$2"
      shift 2
      ;;
    --cordon-selector)
      CORDON_SELECTOR="$2"
      shift 2
      ;;
    --toleration)
      TOLERATION="$2"
      shift 2
      ;;
    --service-account)
      SERVICE_ACCOUNT="$2"
      shift 2
      ;;
    --skip-podmigration-create)
      SKIP_PODMIGRATION_CREATE="true"
      shift
      ;;
    --s8-window-w)
      S8_WINDOW_W="$2"
      shift 2
      ;;
    --enforce-s8-check-a)
      S8_ENFORCE_CHECK_A="true"
      shift
      ;;
    --no-enforce-s8-check-a)
      S8_ENFORCE_CHECK_A="false"
      shift
      ;;
    --enforce-s8-check-b)
      S8_ENFORCE_CHECK_B="true"
      shift
      ;;
    --enforce-s8-gates)
      S8_ENFORCE_CHECK_A="true"
      S8_ENFORCE_CHECK_B="true"
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

err() {
  printf "\033[1;33m[WARN]\033[0m %s\n" "$*" >&2
}

now_ms() {
  python3 -c 'import time; print(int(time.time() * 1000))'
}

validate_toleration_format() {
  if [[ -z "${TOLERATION}" || "${TOLERATION}" == "none" ]]; then
    return 0
  fi
  if [[ "${TOLERATION}" != *:* ]]; then
    die "Invalid --toleration '${TOLERATION}': expected '<key>[=<value>]:<Effect>' (missing ':Effect') or 'none'"
  fi
  local tol_kv="${TOLERATION%:*}"
  local tol_effect="${TOLERATION##*:}"
  if [[ -z "${tol_kv}" || -z "${tol_effect}" ]]; then
    die "Invalid --toleration '${TOLERATION}': both key and Effect must be non-empty in '<key>[=<value>]:<Effect>'"
  fi
  case "${tol_effect}" in
    NoSchedule|PreferNoSchedule|NoExecute)
      ;;
    *)
      die "Invalid --toleration Effect '${tol_effect}' in '${TOLERATION}': expected NoSchedule, PreferNoSchedule, or NoExecute"
      ;;
  esac
}

validate_s8_window_w() {
  if [[ -z "${S8_WINDOW_W}" ]]; then
    die "Invalid --s8-window-w '': expected one or more comma-separated numbers in (0, 10)"
  fi
  local item
  local items=()
  IFS=',' read -ra items <<<"${S8_WINDOW_W}"
  for item in "${items[@]}"; do
    if [[ ! "${item}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
      die "Invalid --s8-window-w value '${item}' in '${S8_WINDOW_W}': must be numeric in (0, 10)"
    fi
    if ! awk -v w="${item}" 'BEGIN { exit !(w > 0 && w < 10) }'; then
      die "Invalid --s8-window-w value '${item}' in '${S8_WINDOW_W}': must satisfy 0 < W < 10"
    fi
  done
}

validate_toleration_format
validate_s8_window_w

PMPROFILER_BIN=""
COLLECT_PID=""
CONTINUITY_PID=""
S8_WATCH_PIDS=()
CORDONED_NODES=()
PINGER_NODE=""
KUBECTL_CMD="${KUBECTL_CMD:-kubectl}"

build_pmprofiler() {
  mkdir -p "${OUT_DIR}/bin"
  PMPROFILER_BIN="${OUT_DIR}/bin/pmprofiler"
  log "Building pmprofiler binary -> ${PMPROFILER_BIN}"
  (cd "${PMPROFILER_SRC}" && go build -o "${PMPROFILER_BIN}" ./cmd/pmprofiler)
}

cordon_node() {
  local node="$1"
  [[ -n "${node}" ]] || return 0
  local existing
  for existing in "${CORDONED_NODES[@]:-}"; do
    if [[ "${existing}" == "${node}" ]]; then
      return 0
    fi
  done
  local already_unschedulable
  already_unschedulable="$("${KUBECTL_CMD}" get node "${node}" -o jsonpath='{.spec.unschedulable}' 2>/dev/null || true)"
  if [[ "${already_unschedulable}" != "true" ]]; then
    CORDONED_NODES+=("${node}")
  else
    log "Node ${node} was already unschedulable prior to T2; will not uncordon on cleanup"
  fi
  "${KUBECTL_CMD}" cordon "${node}" >/dev/null
}

cordon_target_pool_nodes() {
  local effective_selector="${CORDON_SELECTOR}"
  if [[ -z "${effective_selector}" && -n "${NODE_SELECTOR}" && "${NODE_SELECTOR}" != "none" ]]; then
    effective_selector="${NODE_SELECTOR}"
  fi
  if [[ -z "${effective_selector}" ]]; then
    die "cordon_target_pool_nodes: refusing to cordon all cluster nodes when --node-selector is 'none' without an explicit --cordon-selector <k=v|all>"
  fi
  if [[ "${effective_selector}" == "none" ]]; then
    log "Skipping pool cordon because --cordon-selector is set to 'none'"
    return 0
  fi

  local nodes=()
  local line
  if [[ "${effective_selector}" == "all" ]]; then
    while IFS= read -r line; do
      [[ -n "${line}" ]] && nodes+=("${line}")
    done < <("${KUBECTL_CMD}" get nodes -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
    log "Cordoning ${#nodes[@]} node(s) across cluster (--cordon-selector=all): ${nodes[*]:-none}"
  else
    while IFS= read -r line; do
      [[ -n "${line}" ]] && nodes+=("${line}")
    done < <("${KUBECTL_CMD}" get nodes -l "${effective_selector}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || true)
    log "Cordoning ${#nodes[@]} node(s) matching selector '${effective_selector}': ${nodes[*]:-none}"
  fi
  local n
  for n in "${nodes[@]:-}"; do
    [[ -n "${n}" ]] && cordon_node "${n}"
  done
}

uncordon_all_tracked_nodes() {
  if [[ "${SELF_TEST}" == "true" && "${T2_TEST_UNCONDITIONAL_UNCORDON:-false}" != "true" ]]; then
    return 0
  fi
  local n
  for n in "${CORDONED_NODES[@]:-}"; do
    if [[ -n "${n}" ]]; then
      "${KUBECTL_CMD}" uncordon "${n}" >/dev/null 2>&1 || true
    fi
  done
  CORDONED_NODES=()
}

stop_collector() {
  if [[ -n "${COLLECT_PID}" ]] && kill -0 "${COLLECT_PID}" 2>/dev/null; then
    kill -INT "${COLLECT_PID}" 2>/dev/null || true
    wait "${COLLECT_PID}" 2>/dev/null || true
  fi
  COLLECT_PID=""
}

stop_continuity_probe() {
  if [[ -n "${CONTINUITY_PID}" ]] && kill -0 "${CONTINUITY_PID}" 2>/dev/null; then
    kill -INT "${CONTINUITY_PID}" 2>/dev/null || true
    wait "${CONTINUITY_PID}" 2>/dev/null || true
  fi
  CONTINUITY_PID=""
}

start_continuity_probe() {
  local trace_path="$1"
  local workload="$2"
  local selector="$3"
  local log_path="${trace_path}.log"
  stop_continuity_probe
  rm -f "${trace_path}" "${log_path}"
  mkdir -p "$(dirname "${trace_path}")"
  # Execute 2 synchronous pre-checkpoint steps before launching the background loop
  "${PMPROFILER_BIN}" continuity \
    --probe \
    --trace "${trace_path}" \
    --workload "${workload}" \
    --namespace "${NAMESPACE}" \
    --pod-selector "${selector}" \
    --kubectl-bin "${KUBECTL_CMD}" \
    --steps 2 \
    --phase "pre-checkpoint" >>"${log_path}" 2>&1
  "${PMPROFILER_BIN}" continuity \
    --probe \
    --trace "${trace_path}" \
    --workload "${workload}" \
    --namespace "${NAMESPACE}" \
    --pod-selector "${selector}" \
    --kubectl-bin "${KUBECTL_CMD}" \
    --interval 250ms \
    --phase "migrating" >>"${log_path}" 2>&1 &
  CONTINUITY_PID=$!
  sleep 0.2
  if ! kill -0 "${CONTINUITY_PID}" 2>/dev/null; then
    err "Background continuity probe for ${workload} exited prematurely (log: ${log_path}):"
    cat "${log_path}" >&2 || true
    return 1
  fi
}

stop_and_verify_continuity_probe() {
  local run_dir="$1"
  local trace_path="$2"
  local workload="$3"
  local selector="$4"
  local log_path="${trace_path}.log"
  stop_continuity_probe
  "${PMPROFILER_BIN}" continuity \
    --probe \
    --trace "${trace_path}" \
    --workload "${workload}" \
    --namespace "${NAMESPACE}" \
    --pod-selector "${selector}" \
    --kubectl-bin "${KUBECTL_CMD}" \
    --steps 2 \
    --phase "post-restore" >>"${log_path}" 2>&1
  if ! "${PMPROFILER_BIN}" continuity \
    --run "${run_dir}" \
    --trace "${trace_path}" \
    --group "${workload}"; then
    err "Continuity verification failed for ${workload} (probe log: ${log_path}):"
    cat "${log_path}" >&2 || true
    return 1
  fi
}

stop_s8_watches() {
  local p
  for p in "${S8_WATCH_PIDS[@]:-}"; do
    if [[ -n "${p}" ]] && kill -0 "${p}" 2>/dev/null; then
      kill -TERM "${p}" 2>/dev/null || true
      wait "${p}" 2>/dev/null || true
    fi
  done
  S8_WATCH_PIDS=()
}

cleanup_on_exit() {
  stop_s8_watches
  stop_continuity_probe
  stop_collector
  uncordon_all_tracked_nodes
}
trap cleanup_on_exit EXIT

resolve_gcs_bucket() {
  if [[ -n "${GCS_BUCKET}" ]]; then
    return 0
  fi
  local existing
  existing="$("${KUBECTL_CMD}" get podmigrations -n "${NAMESPACE}" -o jsonpath='{.items[0].spec.storage.location}' 2>/dev/null || true)"
  if [[ -n "${existing}" ]]; then
    GCS_BUCKET="${existing}"
    return 0
  fi
  local pssc_bucket pssc_path
  pssc_bucket="$("${KUBECTL_CMD}" get podsnapshotstorageconfigs -o jsonpath='{.items[0].spec.snapshotStorageConfig.gcs.bucket}' 2>/dev/null || true)"
  pssc_path="$("${KUBECTL_CMD}" get podsnapshotstorageconfigs -o jsonpath='{.items[0].spec.snapshotStorageConfig.gcs.path}' 2>/dev/null || true)"
  if [[ -n "${pssc_bucket}" ]]; then
    if [[ -n "${pssc_path}" ]]; then
      GCS_BUCKET="gs://${pssc_bucket}/${pssc_path#/}"
    else
      GCS_BUCKET="gs://${pssc_bucket}"
    fi
    return 0
  fi
  die "No GCS bucket could be resolved: pass --gcs-bucket <gs://bucket/path>, set GCS_BUCKET, or ensure a PodMigration / PodSnapshotStorageConfig (.spec.snapshotStorageConfig.gcs.{bucket,path}) exists on the cluster"
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

clean_test_workloads() {
  kubectl delete deployment/t2-counter deployment/t2-bystander statefulset/t2-redis statefulset/t2-postgres \
    service/t2-redis-svc service/t2-postgres-svc service/s8-phase-anchor pdb/t2-redis-pdb \
    pod/pinger-t2-redis pod/observer-t2-redis \
    -n "${NAMESPACE}" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
  kubectl delete pods -n "${NAMESPACE}" -l "app in (t2-counter,t2-bystander,t2-redis,t2-postgres,pinger,observer)" \
    --ignore-not-found --force --grace-period=0 >/dev/null 2>&1 || true
}

get_active_pods() {
  local selector="$1"
  kubectl get pods -n "${NAMESPACE}" -l "${selector}" --field-selector=status.phase=Running -o json \
    | jq -r '.items[] | select(.metadata.deletionTimestamp == null) | .metadata.name'
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

finish_and_assert_run() {
  local run_dir="$1"
  local allow_cold_start="${2:-false}"
  sleep 2
  stop_continuity_probe
  stop_collector
  local analyze_args=(
    analyze
    --run "${run_dir}"
    --controller-ns "${CONTROLLER_NS}"
    --assert-zero-invariants
    --assert-clean-outcomes
    --assert-verdict-matches-data
  )
  if [[ "${allow_cold_start}" == "true" ]]; then
    analyze_args+=(--allow-cold-start)
  fi
  log "Analyzing ${run_dir} (${analyze_args[*]})"
  "${PMPROFILER_BIN}" "${analyze_args[@]}"
}

# Trigger pod eviction via the Kubernetes Eviction API (`pods/eviction` subresource)
# so the PodEvictionHandler admission webhook intercepts it (HTTP 429 TooManyRequests).
evict_pod() {
  local pod="$1"
  log "Sending policy/v1 Eviction for pod ${NAMESPACE}/${pod}"
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

exec_with_retry() {
  local max_attempts=8
  local attempt=1
  until "$@"; do
    if [[ ${attempt} -ge ${max_attempts} ]]; then
      return 1
    fi
    sleep 2
    attempt=$((attempt + 1))
  done
}

# ==============================================================================
# Stateful Application Verifiers (I2: No Silent Cold-Start / No Rollback)
# ==============================================================================

v_counter_capture() {
  local pod="$1"
  local raw
  raw="$(exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- sh -c '
    if [ -f /tmp/counter.state ]; then
      cat /tmp/counter.state
    else
      exit 1
    fi
  ')" || return 1
  echo "${raw}" | tr -d '[:space:]'
}

v_counter_verify() {
  local run_dir="$1"
  local pod="$2"
  local before_state="$3"
  local id_before="${before_state%%|*}"
  local count_before="${before_state##*|}"

  local after_state
  after_state="$(v_counter_capture "${pod}")" || {
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "counter" \
      --pass=false --value 0 --total 1 \
      --detail "failed to read counter state from restored pod ${pod}"
    return 1
  }
  local id_after="${after_state%%|*}"
  local count_after="${after_state##*|}"

  if [[ -n "${id_before}" && "${id_after}" == "${id_before}" && "${count_after}" -ge "${count_before}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "counter" \
      --pass --value 1 --total 1 \
      --detail "instanceID=${id_after} preserved, count advanced ${count_before} -> ${count_after}"
    log "v_counter PASS: instanceID=${id_after}, count ${count_before} -> ${count_after}"
    return 0
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "counter" \
      --pass=false --value 0 --total 1 \
      --detail "state mismatch: before=${id_before}|${count_before}, after=${id_after}|${count_after}"
    die "v_counter FAIL: before=${id_before}|${count_before}, after=${id_after}|${count_after}"
  fi
}

v_redis_seed() {
  local pod="$1"
  local nonce="$2"
  exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- redis-cli debug populate 50000 >/dev/null
  exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- redis-cli SET migkey "${nonce}" >/dev/null
  local dbsize
  dbsize="$(exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- redis-cli DBSIZE | tr -dc '0-9')"
  if [[ "${dbsize:-0}" -lt 50000 ]]; then
    die "v_redis_seed: expected DBSIZE >= 50000 on ${pod}, got ${dbsize:-0}"
  fi
  log "v_redis seeded on ${pod}: DBSIZE=${dbsize}, migkey=${nonce}"
}

v_redis_verify() {
  local run_dir="$1"
  local pod="$2"
  local expected_nonce="$3"

  local got_nonce dbsize
  got_nonce="$(exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- redis-cli GET migkey | tr -d '[:space:]')" || got_nonce=""
  dbsize="$(exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- redis-cli DBSIZE | tr -dc '0-9')" || dbsize=0

  if [[ "${got_nonce}" == "${expected_nonce}" && "${dbsize:-0}" -ge 50000 ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "redis" \
      --pass --value 50000 --total 50000 \
      --detail "migkey=${got_nonce} matched and DBSIZE=${dbsize} (>= 50000)"
    log "v_redis PASS: migkey=${got_nonce}, DBSIZE=${dbsize}"
    return 0
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "redis" \
      --pass=false --value "${dbsize:-0}" --total 50000 \
      --detail "migkey=${got_nonce} (expected ${expected_nonce}), DBSIZE=${dbsize:-0}"
    die "v_redis FAIL: migkey=${got_nonce} (expected ${expected_nonce}), DBSIZE=${dbsize:-0}"
  fi
}

v_postgres_seed() {
  local pod="$1"
  local nonce="$2"
  local target_rows="${3:-200}"
  exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c "
    CREATE TABLE IF NOT EXISTS mig_seq (
      seq INT PRIMARY KEY,
      nonce TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    TRUNCATE TABLE mig_seq;
    INSERT INTO mig_seq (seq, nonce)
    SELECT g, '${nonce}' FROM generate_series(1, ${target_rows}) AS g;
  " >/dev/null
  log "v_postgres seeded on ${pod}: ${target_rows} contiguous sequence rows (nonce=${nonce})"
}

v_postgres_verify() {
  local run_dir="$1"
  local pod="$2"
  local expected_nonce="$3"
  local min_rows="${4:-200}"

  local query_out
  query_out="$(exec_with_retry kubectl exec -n "${NAMESPACE}" "${pod}" -- psql -U postgres -d postgres -t -A -c "
    SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0),
           COALESCE((SELECT DISTINCT nonce FROM mig_seq LIMIT 1), '')
    FROM mig_seq;
  " | tr -d '[:space:]')" || query_out="0|0|0|"

  IFS='|' read -r row_count min_seq max_seq got_nonce <<<"${query_out}"
  if [[ "${row_count}" -ge "${min_rows}" && "${min_seq}" -eq 1 && "${max_seq}" -eq "${row_count}" && "${got_nonce}" == "${expected_nonce}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "postgres" \
      --pass --value "${row_count}" --total "${row_count}" \
      --detail "contiguous sequence 1..${max_seq} (${row_count} rows) and nonce=${got_nonce} preserved"
    log "v_postgres PASS: 1..${max_seq} contiguous (${row_count} rows), nonce=${got_nonce}"
    return 0
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "postgres" \
      --pass=false --value "${row_count:-0}" --total "${min_rows}" \
      --detail "sequence check failed: count=${row_count}, min=${min_seq}, max=${max_seq}, nonce=${got_nonce} (expected ${expected_nonce})"
    die "v_postgres FAIL: ${query_out}"
  fi
}

# ==============================================================================
# Workload & Policy Helpers for Live Cluster Scenarios
# ==============================================================================

preflight_snapshot_policy() {
  local psp_json
  psp_json="$("${KUBECTL_CMD}" get podsnapshotpolicies -n "${NAMESPACE}" -o json 2>/dev/null || true)"
  if [[ -z "${psp_json}" ]]; then
    die "Preflight failed: unable to query PodSnapshotPolicies in namespace '${NAMESPACE}'"
  fi
  local usable_count
  usable_count="$(jq -r '
    [ (.items // [])[]
      | select(
          .metadata.deletionTimestamp == null
          and (
            ((.status.conditions // []) | map(select(.type == "Ready" and .status == "True")) | length) > 0
          )
        )
    ] | length
  ' <<<"${psp_json}" 2>/dev/null || echo 0)"
  if [[ "${usable_count:-0}" -lt 1 ]]; then
    die "Preflight failed: no Ready PodSnapshotPolicy found in namespace '${NAMESPACE}' (require status.conditions Ready=True)"
  fi
}

ensure_podmigration_policy() {
  local name="${1:-diskless-migration}"
  local force_create="${2:-false}"
  resolve_gcs_bucket
  if [[ "${SKIP_PODMIGRATION_CREATE}" == "true" && "${force_create}" != "true" ]]; then
    log "Skipping PodMigration CR creation (--skip-podmigration-create; using pre-created policy with GCS_BUCKET=${GCS_BUCKET})"
    preflight_snapshot_policy
    return 0
  fi
  "${KUBECTL_CMD}" apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
EOF
  "${KUBECTL_CMD}" wait --for=condition=Ready "podmigration/${name}" -n "${NAMESPACE}" --timeout=60s >/dev/null
  preflight_snapshot_policy
}

render_pod_runtime_spec() {
  if [[ -n "${SERVICE_ACCOUNT}" && "${SERVICE_ACCOUNT}" != "none" ]]; then
    printf "      serviceAccountName: %s\n" "${SERVICE_ACCOUNT}"
  fi
  if [[ -n "${RUNTIME_CLASS}" && "${RUNTIME_CLASS}" != "none" ]]; then
    printf "      runtimeClassName: %s\n" "${RUNTIME_CLASS}"
  fi
  if [[ -n "${NODE_SELECTOR}" && "${NODE_SELECTOR}" != "none" ]]; then
    local ns_key="${NODE_SELECTOR%%=*}"
    local ns_val="${NODE_SELECTOR#*=}"
    printf "      nodeSelector:\n        %s: %s\n" "${ns_key}" "${ns_val}"
  fi
  if [[ -n "${TOLERATION}" && "${TOLERATION}" != "none" ]]; then
    validate_toleration_format
    local tol_kv="${TOLERATION%:*}"
    local tol_effect="${TOLERATION##*:}"
    if [[ "${tol_kv}" == *"="* ]]; then
      local tol_key="${tol_kv%%=*}"
      local tol_val="${tol_kv#*=}"
      cat <<EOF
      tolerations:
      - key: ${tol_key}
        operator: Equal
        value: ${tol_val}
        effect: ${tol_effect}
EOF
    else
      cat <<EOF
      tolerations:
      - key: ${tol_kv}
        operator: Exists
        effect: ${tol_effect}
EOF
    fi
  fi
}

render_counter_manifest() {
  local name="${1:-t2-counter}"
  local replicas="${2:-1}"
  cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
spec:
  replicas: ${replicas}
  selector:
    matchLabels:
      app: ${name}
  template:
    metadata:
      labels:
        app: ${name}
        pod-migration.gke.io/enabled: "true"
    spec:
$(render_pod_runtime_spec)
      containers:
      - name: counter
        image: busybox:1.36
        imagePullPolicy: IfNotPresent
        command:
        - /bin/sh
        - -c
        - |
          ID=\$(cat /proc/sys/kernel/random/uuid)
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
EOF
}

deploy_counter_workload() {
  local name="${1:-t2-counter}"
  local replicas="${2:-1}"
  render_counter_manifest "${name}" "${replicas}" | kubectl apply -n "${NAMESPACE}" -f - >/dev/null
  kubectl rollout status deployment/"${name}" -n "${NAMESPACE}" --timeout=180s >/dev/null
}

render_redis_manifest() {
  local name="${1:-t2-redis}"
  cat <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
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
$(render_pod_runtime_spec)
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
  namespace: ${NAMESPACE}
spec:
  selector:
    app: ${name}
  ports:
  - port: 6379
    targetPort: 6379
EOF
}

deploy_redis_workload() {
  local name="${1:-t2-redis}"
  render_redis_manifest "${name}" | kubectl apply -n "${NAMESPACE}" -f - >/dev/null
  kubectl rollout status statefulset/"${name}" -n "${NAMESPACE}" --timeout=180s >/dev/null
}

render_postgres_manifest() {
  local name="${1:-t2-postgres}"
  cat <<EOF
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
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
$(render_pod_runtime_spec)
      containers:
      - name: postgres
        image: postgres:16-alpine
        imagePullPolicy: IfNotPresent
        command: ["docker-entrypoint.sh"]
        args: ["postgres"]
        env:
        - name: POSTGRES_HOST_AUTH_METHOD
          value: trust
        - name: POSTGRES_PASSWORD
          value: pw
        - name: PGDATA
          value: /var/lib/postgresql/pgdata
        ports:
        - containerPort: 5432
        readinessProbe:
          tcpSocket:
            port: 5432
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
  namespace: ${NAMESPACE}
spec:
  selector:
    app: ${name}
  ports:
  - port: 5432
    targetPort: 5432
EOF
}

deploy_postgres_workload() {
  local name="${1:-t2-postgres}"
  render_postgres_manifest "${name}" | kubectl apply -n "${NAMESPACE}" -f - >/dev/null
  kubectl rollout status statefulset/"${name}" -n "${NAMESPACE}" --timeout=180s >/dev/null
}

wait_for_pmj_terminal() {
  local pod_name="$1"
  local expected_phases="${2:-Succeeded}"
  local timeout_s="${3:-240}"
  local deadline=$((SECONDS + timeout_s))
  while [[ ${SECONDS} -lt ${deadline} ]]; do
    local phase
    phase="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath="{.items[?(@.spec.podRef.name=='${pod_name}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
    if [[ -n "${phase}" && ",${expected_phases}," == *",${phase},"* ]]; then
      log "PodMigrationJob for ${pod_name} reached terminal phase: ${phase}"
      return 0
    fi
    if [[ "${phase}" == "Failed" && ",${expected_phases}," != *",Failed,"* ]]; then
      kubectl get podmigrationjobs -n "${NAMESPACE}" -o yaml >&2 || true
      die "PodMigrationJob for ${pod_name} entered Failed phase (expected ${expected_phases})"
    fi
    sleep 2
  done
  kubectl get podmigrationjobs -n "${NAMESPACE}" -o wide >&2 || true
  die "Timed out after ${timeout_s}s waiting for PodMigrationJob of ${pod_name} to reach ${expected_phases}"
}

# ==============================================================================
# 7 Adversarial T2 Scenarios (S1 - S7)
# ==============================================================================

# S1: Deployment rolling upgrade during active node drain (I6: Zero False Positive Intercepts)
run_scenario_s1() {
  local run_dir="${OUT_DIR}/s1-rolling-upgrade-during-drain"
  local trace_path="${run_dir}/continuity.ndjson"
  log "=== Running Scenario S1: Deployment Rolling Upgrade During Active Node Drain ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_counter_workload "t2-counter" 1
  deploy_counter_workload "t2-bystander" 1
  sleep 2

  local src_pod src_node before_state
  src_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
  before_state="$(v_counter_capture "${src_pod}")"

  start_collector "${run_dir}" "S1: Deployment rolling upgrade during active node drain"
  start_continuity_probe "${trace_path}" "counter" "app=t2-counter"

  # Cordon source node and trigger eviction of t2-counter while simultaneously
  # rolling-upgrading t2-bystander (proving I6: rolling upgrade does not trigger a false PMJ).
  cordon_node "${src_node}"

  evict_pod "${src_pod}"
  kubectl patch deployment t2-bystander -n "${NAMESPACE}" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"rollout-ts\":\"$(date +%s)\"}}}}}" >/dev/null

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null
  kubectl rollout status deployment/t2-bystander -n "${NAMESPACE}" --timeout=180s >/dev/null

  local dst_pod
  dst_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  v_counter_verify "${run_dir}" "${dst_pod}" "${before_state}"
  stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "counter" "app=t2-counter"

  # Verify zero PMJs were created for t2-bystander's rolling upgrade
  local bystander_pmjs
  bystander_pmjs="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath='{.items[*].spec.podRef.name}' | tr ' ' '\n' | grep -c '^t2-bystander-' || true)"
  if [[ "${bystander_pmjs}" -ne 0 ]]; then
    die "S1 FAIL (I6): rolling upgrade of t2-bystander triggered ${bystander_pmjs} unexpected PodMigrationJobs"
  fi

  uncordon_all_tracked_nodes
  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S2: PDB minAvailable: 100% temporary block followed by budget release (I7: PDB & Quota Fidelity)
run_scenario_s2() {
  local run_dir="${OUT_DIR}/s2-pdb-temporary-block"
  local trace_path="${run_dir}/continuity.ndjson"
  log "=== Running Scenario S2: PDB minAvailable: 100% Temporary Block & Release ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_redis_workload "t2-redis"
  sleep 2

  local src_pod nonce
  src_pod="t2-redis-0"
  nonce="nonce-s2-$(date +%s)"
  v_redis_seed "${src_pod}" "${nonce}"

  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: t2-redis-pdb
  namespace: ${NAMESPACE}
spec:
  minAvailable: "100%"
  selector:
    matchLabels:
      app: t2-redis
EOF

  start_collector "${run_dir}" "S2: PDB minAvailable 100% block followed by budget release"
  start_continuity_probe "${trace_path}" "redis" "app=t2-redis"

  local src_node
  src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
  cordon_node "${src_node}"

  # Trigger migration while PDB blocks origin eviction; wait until PMJ reaches Evicting (BlockedByPDB),
  # then relax PDB to minAvailable: 0 so eviction and warm restore complete cleanly.
  evict_pod "${src_pod}"

  local wait_evicting=$((SECONDS + 90))
  while [[ ${SECONDS} -lt ${wait_evicting} ]]; do
    local p_phase
    p_phase="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath="{.items[?(@.spec.podRef.name=='${src_pod}')].status.phase}" 2>/dev/null || true)"
    if [[ "${p_phase}" == "Evicting" ]]; then
      break
    fi
    sleep 2
  done
  sleep 3
  log "Releasing PDB t2-redis-pdb (minAvailable: 100% -> 0)"
  kubectl patch pdb t2-redis-pdb -n "${NAMESPACE}" --type=merge -p '{"spec":{"minAvailable":0}}' >/dev/null

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  uncordon_all_tracked_nodes
  kubectl rollout status statefulset/t2-redis -n "${NAMESPACE}" --timeout=180s >/dev/null

  v_redis_verify "${run_dir}" "${src_pod}" "${nonce}"
  stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "redis" "app=t2-redis"

  kubectl delete pdb t2-redis-pdb -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S3: Corrupted checkpoint artifact in GCS triggering deterministic I9 cold-start fallback
run_scenario_s3() {
  local run_dir="${OUT_DIR}/s3-corrupted-checkpoint-cold-start-fallback"
  log "=== Running Scenario S3: Corrupted Checkpoint Artifact in GCS -> I9 Cold-Start Fallback ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_counter_workload "t2-counter" 1
  sleep 2

  local src_pod before_state
  src_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  before_state="$(v_counter_capture "${src_pod}")"
  start_collector "${run_dir}" "S3: Corrupted checkpoint artifact -> I9 cold-start fallback"

  # Cordon target node pool before triggering eviction so the replacement pod is held
  # Unschedulable after snapshot upload + source eviction while we corrupt checkpoint.img in GCS.
  log "Cordoning target pool nodes (CORDON_SELECTOR=${CORDON_SELECTOR:-${NODE_SELECTOR}}) to hold replacement pod Pending during GCS checkpoint corruption"
  cordon_target_pool_nodes

  evict_pod "${src_pod}"

  # Wait until PodMigrationJob reaches Evicting or Restoring (meaning PodSnapshot upload succeeded)
  local snap_name=""
  local deadline=$((SECONDS + 120))
  while [[ ${SECONDS} -lt ${deadline} ]]; do
    snap_name="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath="{.items[?(@.spec.podRef.name=='${src_pod}')].status.snapshotRef}" 2>/dev/null || true)"
    local phase
    phase="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath="{.items[?(@.spec.podRef.name=='${src_pod}')].status.phase}" 2>/dev/null || true)"
    if [[ -n "${snap_name}" && ( "${phase}" == "Evicting" || "${phase}" == "Restoring" ) ]]; then
      break
    fi
    sleep 1
  done
  [[ -n "${snap_name}" ]] || die "S3: timed out waiting for PodSnapshot reference on PMJ for ${src_pod}"

  # Poll until the replacement pod is created and held Pending on the cordoned nodes
  local repl_pod=""
  local repl_deadline=$((SECONDS + 60))
  while [[ ${SECONDS} -lt ${repl_deadline} ]]; do
    repl_pod="$(kubectl get pods -n "${NAMESPACE}" -l app=t2-counter -o json 2>/dev/null \
      | jq -r --arg src "${src_pod}" '.items[] | select(.metadata.name != $src and .metadata.deletionTimestamp == null) | .metadata.name' | head -n 1)"
    if [[ -n "${repl_pod}" ]]; then
      break
    fi
    sleep 1
  done
  [[ -n "${repl_pod}" ]] || die "S3: timed out waiting for replacement pod after checkpoint of ${src_pod}"

  # Remove the newly uploaded checkpoint artifacts in GCS so restore fails and triggers
  # deterministic I9 cold-start fallback (SucceededWithoutRestore / FallbackToColdStart).
  log "Removing GCS checkpoint artifacts at ${GCS_BUCKET}/${snap_name} (replacement pod ${repl_pod} held Pending)"
  if ! gcloud storage rm -r "${GCS_BUCKET}/${snap_name}" >/dev/null; then
    die "S3: failed to remove GCS checkpoint artifacts at ${GCS_BUCKET}/${snap_name}"
  fi

  log "Uncordoning tracked nodes so replacement pod attempts restore from missing/corrupted checkpoint"
  uncordon_all_tracked_nodes

  # Wait for PMJ to conclude via either SucceededWithoutRestore (FallbackToColdStart)
  # or Failed (RestoreCrashFallback where controller deletes the crashed pod for cold start).
  wait_for_pmj_terminal "${src_pod}" "SucceededWithoutRestore,Failed" 240
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null

  local reason
  reason="$(kubectl get podmigrationjobs -n "${NAMESPACE}" -o jsonpath="{.items[?(@.spec.podRef.name=='${src_pod}')].status.conditions[?(@.type=='Restored')].reason}" 2>/dev/null || true)"
  if [[ "${reason}" != "FallbackToColdStart" && "${reason}" != "RestoreCrashFallback" ]]; then
    die "S3: expected Restored condition reason FallbackToColdStart or RestoreCrashFallback, got '${reason}'"
  fi

  local dst_pod after_state id_before id_after
  dst_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  after_state="$(v_counter_capture "${dst_pod}")"
  id_before="${before_state%%|*}"
  id_after="${after_state%%|*}"
  if [[ "${id_after}" == "${id_before}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "counter" \
      --pass --value 1 --total 1 \
      --detail "unexpectedly preserved pre-checkpoint instanceID=${id_after} despite ${reason}"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "state survived (token-verified)" --group "counter" \
      --pass=false --value 0 --total 1 \
      --detail "cold-start instanceID=${id_after} replaced pre-checkpoint instanceID=${id_before} (consistent with ${reason})"
  fi

  "${PMPROFILER_BIN}" check --run "${run_dir}" \
    --name "I9 deterministic cold-start fallback (${reason})" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "Corrupted GCS checkpoint.img triggered ${reason} and workload recovered via cold start"

  finish_and_assert_run "${run_dir}" "true"
  clean_test_workloads
}

# S4: Dual-replica simultaneous eviction under serialized PodGate contention (I1, I3, I2)
run_scenario_s4() {
  local run_dir="${OUT_DIR}/s4-dual-replica-podgate-contention"
  local trace_path="${run_dir}/continuity.ndjson"
  log "=== Running Scenario S4: Dual-Replica Simultaneous Eviction Under Serialized PodGate Contention ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_counter_workload "t2-counter" 2
  deploy_postgres_workload "t2-postgres"
  sleep 5

  local pg_pod pg_nonce
  pg_pod="t2-postgres-0"
  pg_nonce="nonce-s4-$(date +%s)"
  v_postgres_seed "${pg_pod}" "${pg_nonce}" 200

  start_collector "${run_dir}" "S4: Dual-replica simultaneous eviction under serialized PodGate contention"
  start_continuity_probe "${trace_path}" "postgres" "app=t2-postgres"

  local c_pods=()
  local c_pod_line
  while IFS= read -r c_pod_line; do
    [[ -n "${c_pod_line}" ]] && c_pods+=("${c_pod_line}")
  done < <(get_active_pods "app=t2-counter")
  for p in "${c_pods[@]}" "${pg_pod}"; do
    evict_pod "${p}"
  done

  for p in "${c_pods[@]}" "${pg_pod}"; do
    wait_for_pmj_terminal "${p}" "Succeeded" 300
  done
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null
  kubectl rollout status statefulset/t2-postgres -n "${NAMESPACE}" --timeout=180s >/dev/null

  v_postgres_verify "${run_dir}" "${pg_pod}" "${pg_nonce}" 200
  stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "postgres" "app=t2-postgres"

  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S5: Mid-flight PodMigration deletion verifying I4/I5 deferral and zero orphan PSSC/PSMT leaks
run_scenario_s5() {
  local run_dir="${OUT_DIR}/s5-midflight-podmigration-deletion"
  local trace_path="${run_dir}/continuity.ndjson"
  log "=== Running Scenario S5: Mid-Flight PodMigration Deletion (I4/I5 Deferral & Zero Orphans) ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_counter_workload "t2-counter" 1
  sleep 2

  local src_pod before_state pm_uid
  src_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  before_state="$(v_counter_capture "${src_pod}")"

  start_collector "${run_dir}" "S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)"
  start_continuity_probe "${trace_path}" "counter" "app=t2-counter"

  evict_pod "${src_pod}"
  sleep 1
  # Ensure a PodMigration CR exists to exercise I4/I5 deletion deferral even when
  # --skip-podmigration-create was used prior to eviction.
  if ! kubectl get podmigration diskless-migration -n "${NAMESPACE}" >/dev/null 2>&1; then
    ensure_podmigration_policy "diskless-migration" "true"
  fi
  pm_uid="$(kubectl get podmigration diskless-migration -n "${NAMESPACE}" -o jsonpath='{.metadata.uid}')"
  # Delete PodMigration while PMJ is in-flight; I4/I5 finalizer defers PSSC/PSP cleanup until PMJ finishes.
  kubectl delete podmigration diskless-migration -n "${NAMESPACE}" --wait=false >/dev/null

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  kubectl wait --for=delete podmigration/diskless-migration -n "${NAMESPACE}" --timeout=120s >/dev/null 2>&1 || true
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null

  local dst_pod
  dst_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  v_counter_verify "${run_dir}" "${dst_pod}" "${before_state}"
  stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "counter" "app=t2-counter"

  local leaked_pssc
  leaked_pssc="$(kubectl get podsnapshotstorageconfigs -l "podmigration.gke.io/owner-uid=${pm_uid}" -o name 2>/dev/null || true)"
  if [[ -n "${leaked_pssc}" ]]; then
    ensure_podmigration_policy "diskless-migration"
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "I4 zero orphan PSSC after PodMigration deletion" --group "controller" \
      --pass=false --value 0 --total 1 \
      --detail "leaked PSSC found for owner-uid=${pm_uid}: ${leaked_pssc}"
    die "S5 FAIL: leaked PSSC after PodMigration deletion: ${leaked_pssc}"
  fi
  "${PMPROFILER_BIN}" check --run "${run_dir}" \
    --name "I4 zero orphan PSSC after PodMigration deletion" --group "controller" \
    --pass --value 1 --total 1 \
    --detail "0 orphan PodSnapshotStorageConfigs (owner-uid=${pm_uid}) after PodMigration deletion"

  # Restore baseline PodMigration CR on the cluster unless --skip-podmigration-create is set
  ensure_podmigration_policy "diskless-migration"
  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S6: Continuous write-continuity probe under live Redis warm migration (#103)
run_scenario_s6() {
  local run_dir="${OUT_DIR}/s6-write-continuity-probe"
  local trace_path="${run_dir}/continuity.ndjson"
  log "=== Running Scenario S6: Continuous Write-Continuity Probe Across Warm Migration ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_redis_workload "t2-redis"
  sleep 2

  local src_pod nonce src_node
  src_pod="t2-redis-0"
  nonce="nonce-s6-$(date +%s)"
  v_redis_seed "${src_pod}" "${nonce}"

  start_collector "${run_dir}" "S6: Continuous write-continuity probe across warm migration"
  start_continuity_probe "${trace_path}" "redis" "app=t2-redis"

  src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
  cordon_node "${src_node}"
  evict_pod "${src_pod}"

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  uncordon_all_tracked_nodes
  kubectl rollout status statefulset/t2-redis -n "${NAMESPACE}" --timeout=180s >/dev/null

  v_redis_verify "${run_dir}" "${src_pod}" "${nonce}"
  stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "redis" "app=t2-redis"

  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S7: Verdict-matches-data enforcement (RestoreVerified vs. application state, #104)
run_scenario_s7() {
  local run_dir="${OUT_DIR}/s7-verdict-matches-data"
  local s6_dir="${OUT_DIR}/s6-write-continuity-probe"
  log "=== Running Scenario S7: Verdict Matches Data (RestoreVerified vs. Live Application State) ==="
  rm -rf "${run_dir}"
  mkdir -p "${run_dir}"

  if [[ -f "${s6_dir}/records.ndjson" && -f "${s6_dir}/run.json" ]]; then
    log "Reusing S6 live migration trace for S7 verdict-matches-data positive and negative verification"
    cp "${s6_dir}/records.ndjson" "${run_dir}/records.ndjson"
    cp "${s6_dir}/prom_before.txt" "${run_dir}/prom_before.txt" 2>/dev/null || true
    cp "${s6_dir}/prom_after.txt" "${run_dir}/prom_after.txt" 2>/dev/null || true
    cp "${s6_dir}/run.json" "${run_dir}/run.json"
    "${PMPROFILER_BIN}" analyze --run "${run_dir}" --assert-verdict-matches-data >/dev/null
  else
    local trace_path="${run_dir}/continuity.ndjson"
    clean_test_workloads
    clean_stale_migration_resources
    ensure_podmigration_policy "diskless-migration"
    deploy_counter_workload "t2-counter"
    sleep 2

    local src_pod nonce src_node
    src_pod="t2-counter-0"
    nonce="nonce-s7-$(date +%s)"
    v_counter_seed "${src_pod}" "${nonce}"

    start_collector "${run_dir}" "S7: Verdict matches data (RestoreVerified vs. application state)"
    start_continuity_probe "${trace_path}" "counter" "app=t2-counter"

    src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
    cordon_node "${src_node}"
    evict_pod "${src_pod}"

    wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
    uncordon_all_tracked_nodes
    kubectl rollout status statefulset/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null

    v_counter_verify "${run_dir}" "${src_pod}" "${nonce}"
    stop_and_verify_continuity_probe "${run_dir}" "${trace_path}" "counter" "app=t2-counter"

    finish_and_assert_run "${run_dir}" "false"
    clean_test_workloads
  fi

  # Verify bidirectional S7 negative gates against mutated copies of the live run:
  local neg_false_pos="${run_dir}/neg-false-restore-verified"
  mkdir -p "${neg_false_pos}"
  cp "${run_dir}/records.ndjson" "${neg_false_pos}/records.ndjson"
  cp "${run_dir}/prom_before.txt" "${neg_false_pos}/prom_before.txt" 2>/dev/null || true
  cp "${run_dir}/prom_after.txt" "${neg_false_pos}/prom_after.txt" 2>/dev/null || true
  python3 -c '
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
d["stateSurvivalChecks"] = [{"group": "s7-neg", "name": "in-memory state", "value": 0, "total": 1, "unit": "checks", "pass": False, "detail": "simulated state loss"}]
with open(sys.argv[2], "w") as f:
    json.dump(d, f, indent=2)
' "${run_dir}/run.json" "${neg_false_pos}/run.json"
  if "${PMPROFILER_BIN}" analyze --run "${neg_false_pos}" --assert-verdict-matches-data >/dev/null 2>&1; then
    err "S7 FAIL: --assert-verdict-matches-data did not reject RestoreVerified with failed state check"
    exit 1
  fi
  log "S7 PASS: Positive and negative verdict-matches-data checks verified"
}

# ==============================================================================
# S8: Phase-Controlled Write-Continuity Scenario & Checks (a)/(b) (#149, #151, #131)
# ==============================================================================

deploy_s8_pinger_and_observer() {
  local target_svc="$1"
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pinger-t2-redis
  namespace: ${NAMESPACE}
  labels:
    app: pinger
spec:
  containers:
  - name: pinger
    image: python:3.12-alpine
    imagePullPolicy: IfNotPresent
    command: ["python3", "-u", "-c"]
    args:
    - |
      import socket, time
      host, last_ok, last_val, acked = "${target_svc}", None, 0, {}
      hms = lambda t: time.strftime("%H:%M:%S", time.gmtime(t))
      print("START", hms(time.time()), flush=True)
      while True:
          t = time.time()
          val = None
          try:
              s = socket.create_connection((host, 6379), timeout=0.5)
              s.settimeout(0.5)
              s.sendall(b"INCR ackctr\r\n")
              r = s.recv(64)
              s.close()
              if r.startswith(b":"):
                  val = int(r[1:].strip())
          except Exception:
              val = None
          if val is not None:
              if last_ok is not None and t - last_ok > 0.5:
                  print("GAP %.3f %s" % (t - last_ok, hms(t)), flush=True)
              if val <= last_val:
                  first_t = acked.get(val)
                  win = acked[last_val] - first_t if first_t is not None else -1.0
                  print("LOST %d first=%d last=%d window>=%.3f %s" % (last_val - val + 1, val, last_val, win, hms(t)), flush=True)
              acked[val] = t
              print("A %d %d" % (int(t * 1000), val), flush=True)
              if len(acked) > 6000:
                  for k in sorted(acked)[:1000]:
                      del acked[k]
              last_val, last_ok = val, t
          time.sleep(0.1)
    resources:
      requests:
        cpu: 10m
        memory: 32Mi
EOF
  kubectl wait -n "${NAMESPACE}" --for=condition=Ready pod/pinger-t2-redis --timeout=180s >/dev/null
  PINGER_NODE="$(kubectl get pod -n "${NAMESPACE}" pinger-t2-redis -o jsonpath='{.spec.nodeName}')"
  [[ -n "${PINGER_NODE}" ]] || die "S8: failed to resolve nodeName for pinger-t2-redis"

  kubectl delete service s8-sync-probe -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: s8-sync-probe
  namespace: ${NAMESPACE}
spec:
  selector:
    s8-sync-probe: active
  ports:
  - name: probe
    port: 6380
    targetPort: 6380
EOF
  local sync_probe_ip
  sync_probe_ip="$(kubectl get service -n "${NAMESPACE}" s8-sync-probe -o jsonpath='{.spec.clusterIP}')"
  [[ -n "${sync_probe_ip}" ]] || die "S8: failed to resolve clusterIP for s8-sync-probe"

  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: observer-t2-redis
  namespace: ${NAMESPACE}
  labels:
    app: observer
spec:
  nodeName: ${PINGER_NODE}
  containers:
  - name: observer
    image: python:3.12-alpine
    imagePullPolicy: IfNotPresent
    command: ["python3", "-u", "-c"]
    args:
    - |
      import socket, threading, time
      ip = socket.gethostbyname("${target_svc}")
      probe_ip = "${sync_probe_ip}"
      print("START", int(time.time() * 1000), ip, "v2", probe_ip, flush=True)
      lock = threading.Lock()
      def serve_probe():
          srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
          srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
          srv.bind(("0.0.0.0", 6380))
          srv.listen(32)
          while True:
              try:
                  conn, _ = srv.accept()
                  conn.sendall(b"ok\n")
                  conn.close()
              except Exception:
                  pass
      threading.Thread(target=serve_probe, daemon=True).start()
      def poll_sync_probe():
          seen = 0
          while seen < 5:
              t = time.time()
              try:
                  s = socket.create_connection((probe_ip, 6380), timeout=0.1)
                  s.settimeout(0.1)
                  d = s.recv(16)
                  s.close()
                  if d.startswith(b"ok"):
                      with lock:
                          print(int(t * 1000), int((time.time() - t) * 1000), "sync_probe_ok :0", probe_ip, flush=True)
                      seen += 1
                      time.sleep(0.2)
                      continue
              except Exception:
                  pass
              time.sleep(0.04)
      threading.Thread(target=poll_sync_probe, daemon=True).start()
      def attempt(t):
          try:
              s = socket.create_connection((ip, 6379), timeout=0.5)
              s.settimeout(0.5)
              s.sendall(b"DBSIZE\r\nCLIENT INFO\r\n")
              data = b""
              while b"\n" not in data or (data.count(b"\n") < 2 and b"laddr=" not in data):
                  chunk = s.recv(512)
                  if not chunk:
                      break
                  data += chunk
              s.close()
              lines = [ln.strip() for ln in data.decode(errors="replace").splitlines() if ln.strip()]
              db_line = lines[0] if lines else ""
              laddr = ""
              for ln in lines[1:]:
                  for tok in ln.split():
                      if tok.startswith("laddr="):
                          laddr = tok.split("=", 1)[1].split(":", 1)[0]
                          break
              out = ("ok " + db_line + (" " + laddr if laddr else "")).strip()
          except ConnectionRefusedError:
              out = "refused"
          except socket.timeout:
              out = "timeout"
          except ConnectionResetError:
              out = "reset"
          except Exception as e:
              out = "err " + type(e).__name__
          with lock:
              print(int(t * 1000), int((time.time() - t) * 1000), out, flush=True)
      while True:
          threading.Thread(target=attempt, args=(time.time(),), daemon=True).start()
          time.sleep(0.02)
    resources:
      requests:
        cpu: 10m
        memory: 32Mi
EOF
  kubectl wait -n "${NAMESPACE}" --for=condition=Ready pod/observer-t2-redis --timeout=180s >/dev/null
}

start_s8_watches() {
  local run_dir="$1"
  local pod_selector="$2"
  local svc_name="$3"
  stop_s8_watches

  python3 - "${NAMESPACE}" "${pod_selector}" "${run_dir}/pods.ndjson" "${KUBECTL_CMD}" <<'PY' 2>"${run_dir}/podwatch.err" &
import json, signal, subprocess, sys, time
ns, selector, out_path, kubectl_bin = sys.argv[1:5]
proc = subprocess.Popen(
    [kubectl_bin, "-n", ns, "get", "pods", "-l", selector, "--watch", "-o", "json"],
    stdout=subprocess.PIPE,
    text=True,
)
def _stop(signum, frame):
    try:
        proc.terminate()
    except Exception:
        pass
    sys.exit(0)
signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)
buf = []
depth = 0
try:
    with open(out_path, "w") as out:
        for line in proc.stdout:
            s = line.strip()
            if not s and depth == 0:
                continue
            buf.append(line)
            depth += line.count("{") - line.count("}")
            if depth <= 0 and buf:
                raw = "".join(buf)
                buf = []
                depth = 0
                try:
                    obj = json.loads(raw)
                    rec = {"t_ms": int(time.time() * 1000), "ev": {"type": "WATCH", "object": obj}}
                    out.write(json.dumps(rec) + "\n")
                    out.flush()
                except Exception:
                    pass
finally:
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        pass
PY
  S8_WATCH_PIDS+=($!)

  # Seed the target service's current EndpointSlice before starting the cluster-wide watch, then use
  # --watch-only so kubectl fetches the current list resourceVersion internally and watches only subsequent
  # changes without dumping an initial LIST burst of existing slices into the 12s quiet window.
  python3 - "${NAMESPACE}" "${svc_name}" "${run_dir}/endpointslices.ndjson" "${KUBECTL_CMD}" <<'PY' 2>"${run_dir}/epwatch.err" &
import json, signal, subprocess, sys, time
ns, svc_name, out_path, kubectl_bin = sys.argv[1:5]
with open(out_path, "w") as out:
    try:
        seed_raw = subprocess.check_output(
            [kubectl_bin, "-n", ns, "get", "endpointslices", "-l", f"kubernetes.io/service-name={svc_name}", "-o", "json"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        seed_doc = json.loads(seed_raw)
        now_t = int(time.time() * 1000)
        for item in seed_doc.get("items") or []:
            out.write(json.dumps({"t_ms": now_t, "ev": {"type": "SEED", "object": item}}) + "\n")
        out.flush()
    except Exception:
        pass
cmd = [kubectl_bin, "get", "endpointslices", "-A", "--watch-only", "-o", "json"]
proc = subprocess.Popen(
    cmd,
    stdout=subprocess.PIPE,
    text=True,
)
def _stop(signum, frame):
    try:
        proc.terminate()
    except Exception:
        pass
    sys.exit(0)
signal.signal(signal.SIGTERM, _stop)
signal.signal(signal.SIGINT, _stop)
buf = []
depth = 0
try:
    with open(out_path, "a") as out:
        for line in proc.stdout:
            s = line.strip()
            if not s and depth == 0:
                continue
            buf.append(line)
            depth += line.count("{") - line.count("}")
            if depth <= 0 and buf:
                raw = "".join(buf)
                buf = []
                depth = 0
                try:
                    obj = json.loads(raw)
                    rec = {"t_ms": int(time.time() * 1000), "ev": {"type": "WATCH", "object": obj}}
                    out.write(json.dumps(rec) + "\n")
                    out.flush()
                except Exception:
                    pass
finally:
    try:
        proc.terminate()
        proc.wait(timeout=2)
    except Exception:
        pass
PY
  S8_WATCH_PIDS+=($!)
  sleep 1
}

# s8_phase_analyze <run_dir> [enforce_check_a] [enforce_check_b]
# Analyzes a phase-anchored S8 run directory, writes <run_dir>/s8_phase_continuity.json,
# records pmprofiler checks, and enforces Check (a) (#151) / Check (b) (#131) when requested.
s8_phase_analyze() {
  local run_dir="$1"
  local enforce_a="${2:-${S8_ENFORCE_CHECK_A}}"
  local enforce_b="${3:-${S8_ENFORCE_CHECK_B}}"

  if ! python3 - "${run_dir}" "${enforce_a}" "${enforce_b}" <<'PY'; then
import datetime, json, os, sys

run_dir, enforce_a_str, enforce_b_str = sys.argv[1:4]
enforce_a = enforce_a_str.lower() == "true"
enforce_b = enforce_b_str.lower() == "true"

def parse_iso_ms(ts):
    if not ts:
        return None
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return int(datetime.datetime.fromisoformat(ts).timestamp() * 1000)
    except Exception:
        return None

def load_ndjson(path):
    items = []
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        items.append(json.loads(line))
                    except Exception:
                        pass
    return items

src_pod = "t2-redis-0"
if os.path.exists(f"{run_dir}/source_pod.txt"):
    src_pod = open(f"{run_dir}/source_pod.txt").read().strip() or src_pod

src_uid = ""
if os.path.exists(f"{run_dir}/source_pod_uid.txt"):
    src_uid = open(f"{run_dir}/source_pod_uid.txt").read().strip()

drain_ms = 0
if os.path.exists(f"{run_dir}/drain_ms.txt"):
    try:
        drain_ms = int(open(f"{run_dir}/drain_ms.txt").read().strip())
    except Exception:
        drain_ms = 0

phase_kv = {}
if os.path.exists(f"{run_dir}/phase.txt"):
    with open(f"{run_dir}/phase.txt") as f:
        for line in f:
            for tok in line.strip().split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    phase_kv[k] = v

window_w = float(phase_kv.get("W", "5"))
dataplane_mode = phase_kv.get("dataplane_mode", "kube-proxy")
target_ns = phase_kv.get("namespace", "")
target_service = phase_kv.get("target_service", "t2-redis-svc")
if not target_ns and os.path.exists(f"{run_dir}/meta.json"):
    try:
        target_ns = (json.load(open(f"{run_dir}/meta.json")) or {}).get("namespace") or ""
    except Exception:
        pass
if not target_ns:
    target_ns = "default"
quiet_start_ms = int(phase_kv["quiet_start_ms"]) if "quiet_start_ms" in phase_kv and phase_kv["quiet_start_ms"].isdigit() else None
anchor_created_ms = int(phase_kv["anchor_created_ms"]) if "anchor_created_ms" in phase_kv and phase_kv["anchor_created_ms"].isdigit() else None

# 1. Parse pinger.log -> cold_served, lost_after_ckpt using M0 segment analysis
acks = []
if os.path.exists(f"{run_dir}/pinger.log"):
    with open(f"{run_dir}/pinger.log") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 3 and parts[0] == "A":
                try:
                    acks.append((int(parts[1]), int(parts[2])))
                except Exception:
                    pass

JUMP_TOL = 5
segs = []
for t, v in acks:
    s = segs[-1] if segs else None
    if s and s["last"] < v <= s["last"] + JUMP_TOL:
        s.update(last=v, t_last=t, n=s["n"] + 1)
        s["times"][v] = t
    else:
        segs.append({"first": v, "last": v, "t_first": t, "t_last": t, "n": 1, "times": {v: t}})
for i, s in enumerate(segs):
    s["kind"] = "source" if i == 0 else ("cold" if s["first"] == 1 else "restored")

src_seg = segs[0] if segs else None
restored = [s for s in segs[1:] if s["kind"] == "restored"]
lost_after_ckpt = 0
if src_seg and restored:
    c = restored[0]["first"] - 1
    lost_after_ckpt = max(0, src_seg["last"] - c)
cold_served = sum(s["n"] for s in segs[1:] if s["kind"] == "cold")

# 2. Load pod, EndpointSlice, and collector records
pod_records = load_ndjson(f"{run_dir}/pods.ndjson")
ep_records = load_ndjson(f"{run_dir}/endpointslices.ndjson")
collector_records = load_ndjson(f"{run_dir}/records.ndjson")

ckpt_ms = None
restored_pod_uids = set()
earliest_src_pod_uid = ""
earliest_src_pod_ms = None
initial_src_ip = ""

for r in collector_records:
    gvr = r.get("gvr", "")
    obj = r.get("obj") or {}
    md = obj.get("metadata") or {}
    st = obj.get("status") or {}
    t_ms = parse_iso_ms(r.get("ts"))
    if gvr.startswith("podsnapshots.") or gvr.startswith("podsnapshotmanualtriggers."):
        spec = obj.get("spec") or {}
        if spec.get("podName") == src_pod or spec.get("targetPod") == src_pod or md.get("name", "").find(src_pod) >= 0:
            c_ms = parse_iso_ms(md.get("creationTimestamp")) or t_ms
            if c_ms is not None and (ckpt_ms is None or c_ms < ckpt_ms):
                ckpt_ms = c_ms
    elif gvr.startswith("podmigrationjobs."):
        spec = obj.get("spec") or {}
        if (spec.get("podRef") or {}).get("name") == src_pod:
            if not src_uid and spec.get("targetPodUID"):
                src_uid = spec["targetPodUID"]
            if st.get("restoredPodUID"):
                restored_pod_uids.add(st["restoredPodUID"])
            ev_ms = parse_iso_ms(st.get("evictingStartTime"))
            if ev_ms is not None and (ckpt_ms is None or ev_ms < ckpt_ms):
                ckpt_ms = ev_ms
    elif gvr == "pods.v1":
        if md.get("name") == src_pod and md.get("uid"):
            c_ms = parse_iso_ms(md.get("creationTimestamp")) or t_ms
            if earliest_src_pod_ms is None or (c_ms is not None and c_ms < earliest_src_pod_ms):
                earliest_src_pod_ms = c_ms
                earliest_src_pod_uid = md["uid"]
                if st.get("podIP"):
                    initial_src_ip = st["podIP"]

if not src_uid and earliest_src_pod_uid and earliest_src_pod_uid not in restored_pod_uids:
    src_uid = earliest_src_pod_uid

# 3. Parse observer.log -> empty_reads (DBSIZE < 50000 after drain), sync_probe_ok, backend cutover, and kube_proxy_sync_s
empty_reads = 0
sync_probe_ms = None
backend_cutover_ms = None
fallback_empty_ms = None
obs_before_drain = 0
obs_after_drain = 0
initial_backend_ip = initial_src_ip

if os.path.exists(f"{run_dir}/observer.log"):
    with open(f"{run_dir}/observer.log") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) >= 3 and parts[0] != "START":
                try:
                    t_ms = int(parts[0])
                except Exception:
                    continue
                status = parts[2]
                backend_ip = parts[4] if (status == "ok" and len(parts) >= 5) else ""
                if t_ms < drain_ms:
                    obs_before_drain += 1
                    if status == "ok" and backend_ip and not initial_backend_ip:
                        initial_backend_ip = backend_ip
                else:
                    obs_after_drain += 1
                    if status == "sync_probe_ok":
                        if sync_probe_ms is None:
                            sync_probe_ms = t_ms
                    elif status == "ok" and len(parts) >= 4 and parts[3].startswith(":"):
                        try:
                            dbsize = int(parts[3][1:])
                            if dbsize < 50000:
                                empty_reads += 1
                                if not initial_backend_ip and fallback_empty_ms is None:
                                    fallback_empty_ms = t_ms
                        except Exception:
                            pass
                        if backend_ip and initial_backend_ip and backend_ip != initial_backend_ip:
                            if backend_cutover_ms is None:
                                backend_cutover_ms = t_ms

kp_sync_ms = sync_probe_ms if sync_probe_ms is not None else (backend_cutover_ms if backend_cutover_ms is not None else fallback_empty_ms)

PHASE_TOLERANCE_S = 2.0
kp_sync_s = round((kp_sync_ms - drain_ms) / 1000.0, 3) if (kp_sync_ms is not None and drain_ms > 0) else None
backend_cutover_s = round((backend_cutover_ms - drain_ms) / 1000.0, 3) if (backend_cutover_ms is not None and drain_ms > 0) else None
phase_applicable = dataplane_mode == "kube-proxy"
phase_hit = bool(phase_applicable and kp_sync_s is not None and abs(kp_sync_s - window_w) <= PHASE_TOLERANCE_S)

def is_target_service_ep_slice(obj):
    md = obj.get("metadata") or {}
    ns = md.get("namespace") or ""
    labels = md.get("labels") or {}
    svc_label = labels.get("kubernetes.io/service-name") or ""
    slice_name = md.get("name") or ""
    if ns and ns != target_ns:
        return False
    if svc_label and svc_label != target_service:
        return False
    if slice_name and not svc_label and not slice_name.startswith(target_service):
        return False
    return True

all_ep_events = []
for r in ep_records:
    all_ep_events.append((r.get("t_ms"), (r.get("ev") or {}).get("object") or {}))
for r in collector_records:
    if r.get("gvr", "").startswith("endpointslices."):
        all_ep_events.append((parse_iso_ms(r.get("ts")), r.get("obj") or {}))
all_ep_events.sort(key=lambda x: x[0] if x[0] is not None else 0)
target_ep_events = [(t_ms, obj) for (t_ms, obj) in all_ep_events if is_target_service_ep_slice(obj)]

# Validate minimum non-empty inputs (fail closed if pinger/observer/watches/checkpoint/events failed to record)
acks_before_drain = [a for a in acks if a[0] < drain_ms]
acks_after_drain = [a for a in acks if a[0] >= drain_ms]
total_pod_obs = len(pod_records) + sum(1 for r in collector_records if r.get("gvr") == "pods.v1")
total_ep_obs = len(target_ep_events)

inconclusive_reasons = []
if drain_ms <= 0:
    inconclusive_reasons.append("missing or invalid drain_ms")
if ckpt_ms is None:
    inconclusive_reasons.append("missing checkpoint timestamp (no PodSnapshot/PSMT/PMJ evictingStartTime found)")
if len(acks_before_drain) == 0:
    inconclusive_reasons.append("0 pinger acks before drain_ms")
if len(acks_after_drain) == 0:
    inconclusive_reasons.append("0 pinger acks after drain_ms")
if obs_after_drain == 0:
    inconclusive_reasons.append("0 observer samples after drain_ms")
if total_pod_obs == 0:
    inconclusive_reasons.append("0 pod watch/collector records")
if total_ep_obs == 0:
    inconclusive_reasons.append("0 EndpointSlice watch/collector records")

# Quiet period EndpointSlice churn check (#149): ep_records watches EndpointSlices cluster-wide (-A)
quiet_period_ep_churn_count = 0
if quiet_start_ms is not None and anchor_created_ms is not None:
    for r in ep_records:
        ev = r.get("ev") or {}
        if ev.get("type") == "SEED":
            continue
        rt = r.get("t_ms")
        if rt is not None and quiet_start_ms <= rt < anchor_created_ms:
            quiet_period_ep_churn_count += 1
quiet_period_clean = quiet_period_ep_churn_count == 0

phase_inconclusive_reasons = []
if dataplane_mode == "dataplane-v2":
    phase_status = "skipped"
    phase_inconclusive = False
elif not phase_applicable:
    phase_status = "inconclusive"
    phase_inconclusive_reasons.append(f"dataplane_mode={dataplane_mode} (could not verify kube-proxy vs dataplane-v2)")
    phase_inconclusive = True
else:
    if not phase_hit:
        phase_inconclusive_reasons.append(f"phase_hit=false (kp_sync_s={kp_sync_s}, W={window_w}s, tolerance=+-{PHASE_TOLERANCE_S}s)")
    if not quiet_period_clean:
        phase_inconclusive_reasons.append(f"quiet_period_clean=false (quiet_period_ep_churn_count={quiet_period_ep_churn_count})")
    phase_inconclusive = len(phase_inconclusive_reasons) > 0
    phase_status = "inconclusive" if phase_inconclusive else "pass"

# Check (a) (#151): No source pod emits "Successfully restored the pod from PodSnapshot" after its checkpoint.
# Match by source pod UID (src_uid) so a StatefulSet replacement pod with the same name (e.g. t2-redis-0)
# and a different UID is never misidentified as the source pod re-restoring.
re_restore_violations = []
events_list = []
if os.path.exists(f"{run_dir}/events.json"):
    try:
        with open(f"{run_dir}/events.json") as ef:
            ev_doc = json.load(ef)
        if not isinstance(ev_doc, dict) or not isinstance(ev_doc.get("items"), list):
            inconclusive_reasons.append("invalid events.json structure (missing items list)")
        else:
            events_list.extend(ev_doc["items"])
    except Exception:
        inconclusive_reasons.append("unreadable or invalid events.json")
for r in collector_records:
    if r.get("gvr") in ("events.v1", "events.v1beta1", "events"):
        events_list.append(r.get("obj") or {})

seen_ev_uids = set()
restored_pod_restore_events = 0
for ev in events_list:
    md = ev.get("metadata") or {}
    uid = md.get("uid") or md.get("name")
    if uid and uid in seen_ev_uids:
        continue
    if uid:
        seen_ev_uids.add(uid)
    inv = ev.get("involvedObject") or ev.get("regarding") or {}
    inv_uid = inv.get("uid") or ""
    inv_name = inv.get("name") or ""
    msg = ev.get("message") or ev.get("note") or ""
    if "Successfully restored the pod from PodSnapshot" not in msg:
        continue
    if inv_uid and inv_uid in restored_pod_uids:
        restored_pod_restore_events += 1
        continue
    if src_uid and inv_uid:
        if inv_uid != src_uid:
            continue
    else:
        if inv_name != src_pod:
            continue
    ev_ms = (
        parse_iso_ms(ev.get("lastTimestamp"))
        or parse_iso_ms(ev.get("eventTime"))
        or parse_iso_ms(md.get("creationTimestamp"))
    )
    count = int(ev.get("count") or 1)
    if ckpt_ms is not None and ev_ms is not None and ev_ms >= ckpt_ms:
        re_restore_violations.append({
            "pod": inv_name or src_pod,
            "pod_uid": inv_uid or src_uid,
            "event_ms": ev_ms,
            "checkpoint_ms": ckpt_ms,
            "count": count,
            "message": msg,
        })

if len(events_list) == 0 or (len(restored_pod_uids) > 0 and restored_pod_restore_events == 0):
    inconclusive_reasons.append("0 replacement pod restore events observed in events.json/collector")

inputs_valid = len(inconclusive_reasons) == 0
check_a_pass = inputs_valid and (len(re_restore_violations) == 0)

# Check (b) (#131): A pod labeled as fenced (pod-migration.gke.io/fenced=true) is never serving in any EndpointSlice
# for the target Service/namespace. Key fenced_pods by Pod UID so a StatefulSet replacement pod with the same name
# does not collide with the fenced source pod.
fenced_pods = {}  # pod_uid -> {"uid": str, "name": str, "fence_ms": int, "ready_false_ms": int, "first_non_serving_ep_ms": int, "ips": set()}

def inspect_pod_obj(t_ms, obj):
    md = obj.get("metadata") or {}
    st = obj.get("status") or {}
    name = md.get("name")
    if not name:
        return
    p_uid = md.get("uid") or ""
    if not p_uid:
        if name == src_pod and src_uid:
            p_uid = src_uid
        else:
            p_uid = f"name:{name}"
    labels = md.get("labels") or {}
    ip = st.get("podIP")
    if labels.get("pod-migration.gke.io/fenced") == "true":
        entry = fenced_pods.setdefault(
            p_uid,
            {"uid": p_uid, "name": name, "fence_ms": t_ms, "ready_false_ms": None, "first_non_serving_ep_ms": None, "ips": set()},
        )
        if t_ms is not None and (entry["fence_ms"] is None or t_ms < entry["fence_ms"]):
            entry["fence_ms"] = t_ms
        if ip:
            entry["ips"].add(ip)
        for c in st.get("conditions") or []:
            if c.get("type") == "Ready" and c.get("status") == "False":
                if entry["ready_false_ms"] is None or (t_ms is not None and t_ms < entry["ready_false_ms"]):
                    entry["ready_false_ms"] = t_ms

for r in pod_records:
    t_ms = r.get("t_ms")
    obj = (r.get("ev") or {}).get("object") or {}
    inspect_pod_obj(t_ms, obj)

for r in collector_records:
    if r.get("gvr") == "pods.v1":
        inspect_pod_obj(parse_iso_ms(r.get("ts")), r.get("obj") or {})

fenced_serving_violations = []

def endpoint_matches_fenced_pod(ep, fp_uid, fp_info):
    tref = ep.get("targetRef") or {}
    t_uid = tref.get("uid") or ""
    if t_uid and t_uid in restored_pod_uids:
        return False
    if t_uid and not fp_uid.startswith("name:"):
        return t_uid == fp_uid
    addrs = set(ep.get("addresses") or [])
    if addrs and fp_info["ips"]:
        return bool(addrs & fp_info["ips"])
    return (not t_uid) and (tref.get("name") == fp_info["name"])

for t_ms, obj in target_ep_events:
    endpoints = obj.get("endpoints") or []
    for fp_uid, fp_info in fenced_pods.items():
        anchor_ms = fp_info["ready_false_ms"] or fp_info["fence_ms"]
        matched_eps = [ep for ep in endpoints if endpoint_matches_fenced_pod(ep, fp_uid, fp_info)]
        is_serving = any((ep.get("conditions") or {}).get("serving") is True for ep in matched_eps)
        if not is_serving:
            if t_ms is not None and anchor_ms is not None and t_ms >= anchor_ms:
                if fp_info["first_non_serving_ep_ms"] is None or t_ms < fp_info["first_non_serving_ep_ms"]:
                    fp_info["first_non_serving_ep_ms"] = t_ms
        else:
            # Violation if:
            #  (1) fenced pod UID was already observed non-serving in the target Service's EndpointSlice and became serving=true again, OR
            #  (2) fenced pod UID remained serving=true more than 1500ms after fence/Ready=False timestamp.
            non_serving_ms = fp_info["first_non_serving_ep_ms"]
            re_served = non_serving_ms is not None and t_ms is not None and t_ms > non_serving_ms
            stale_serving = anchor_ms is not None and t_ms is not None and t_ms > anchor_ms + 1500
            if re_served or stale_serving:
                addrs = sorted({a for ep in matched_eps for a in (ep.get("addresses") or [])})
                fenced_serving_violations.append({
                    "pod": fp_info["name"],
                    "pod_uid": fp_uid,
                    "addresses": addrs,
                    "t_ms": t_ms,
                    "fence_ms": anchor_ms,
                    "first_non_serving_ep_ms": non_serving_ms,
                    "offset_after_fence_ms": (t_ms - anchor_ms) if (t_ms is not None and anchor_ms is not None) else None,
                })

fenced_pods_seen = len(fenced_pods)
no_fenced_pods_seen = fenced_pods_seen == 0
if not inputs_valid:
    check_b_pass = False
elif no_fenced_pods_seen:
    # Never pass vacuously when enforcement is requested (#131); in report-only mode record status explicitly.
    check_b_pass = False if enforce_b else True
else:
    check_b_pass = len(fenced_serving_violations) == 0

summary = {
    "source_pod": src_pod,
    "source_pod_uid": src_uid,
    "restored_pod_uids": sorted(restored_pod_uids),
    "inputs_valid": inputs_valid,
    "inconclusive_reasons": inconclusive_reasons,
    "dataplane_mode": dataplane_mode,
    "phase_applicable": phase_applicable,
    "phase_status": phase_status,
    "phase_tolerance_s": PHASE_TOLERANCE_S,
    "window_w_s": window_w,
    "drain_ms": drain_ms,
    "checkpoint_ms": ckpt_ms,
    "kube_proxy_sync_s": kp_sync_s,
    "backend_cutover_s": backend_cutover_s,
    "phase_hit": phase_hit,
    "quiet_period_ep_churn_count": quiet_period_ep_churn_count,
    "quiet_period_clean": quiet_period_clean,
    "phase_inconclusive": phase_inconclusive,
    "phase_inconclusive_reasons": phase_inconclusive_reasons,
    "cold_served": cold_served,
    "empty_reads": empty_reads,
    "lost_after_ckpt": lost_after_ckpt,
    "fenced_pods_seen": fenced_pods_seen,
    "no_fenced_pods_seen": no_fenced_pods_seen,
    "fenced_pods_observed": sorted(fenced_pods.keys()),
    "check_a_source_no_rerestore": {
        "issue": "#151",
        "pass": check_a_pass,
        "enforced": enforce_a,
        "violations": re_restore_violations,
    },
    "check_b_fenced_never_serving": {
        "issue": "#131",
        "pass": check_b_pass,
        "enforced": enforce_b,
        "fenced_pods_seen": fenced_pods_seen,
        "status": "no_fenced_pods_seen" if no_fenced_pods_seen else ("pass" if len(fenced_serving_violations) == 0 else "fail"),
        "violations": fenced_serving_violations,
    },
}

with open(f"{run_dir}/s8_phase_continuity.json", "w") as f:
    json.dump(summary, f, indent=2)

print(
    f"S8 summary: W={window_w}s dataplane={dataplane_mode} kp_sync_s={kp_sync_s} phase_hit={phase_hit} "
    f"phase_status={phase_status} phase_inconclusive={phase_inconclusive} "
    f"cold_served={cold_served} empty_reads={empty_reads} lost_after_ckpt={lost_after_ckpt} "
    f"fenced_pods_seen={fenced_pods_seen} "
    f"check_a_pass={check_a_pass}(enforced={enforce_a}) "
    f"check_b_pass={check_b_pass}(enforced={enforce_b})"
)

if not inputs_valid:
    print(f"S8 INCONCLUSIVE / INVALID INPUTS: {', '.join(inconclusive_reasons)}", file=sys.stderr)
    sys.exit(1)
PY
    die "S8 FAIL: phase analysis inputs were empty or inconclusive (${run_dir}/s8_phase_continuity.json)"
  fi

  local summary_json="${run_dir}/s8_phase_continuity.json"
  local check_a_pass check_b_pass cold_served empty_reads lost_after_ckpt kp_sync_s fenced_pods_seen
  local dataplane_mode phase_applicable phase_status phase_hit quiet_period_clean phase_inconclusive phase_inconclusive_reasons
  check_a_pass="$(jq -r '.check_a_source_no_rerestore.pass' "${summary_json}")"
  check_b_pass="$(jq -r '.check_b_fenced_never_serving.pass' "${summary_json}")"
  cold_served="$(jq -r '.cold_served' "${summary_json}")"
  empty_reads="$(jq -r '.empty_reads' "${summary_json}")"
  lost_after_ckpt="$(jq -r '.lost_after_ckpt' "${summary_json}")"
  kp_sync_s="$(jq -r '.kube_proxy_sync_s // "n/a"' "${summary_json}")"
  fenced_pods_seen="$(jq -r '.fenced_pods_seen' "${summary_json}")"
  dataplane_mode="$(jq -r '.dataplane_mode' "${summary_json}")"
  phase_applicable="$(jq -r '.phase_applicable' "${summary_json}")"
  phase_status="$(jq -r '.phase_status' "${summary_json}")"
  phase_hit="$(jq -r '.phase_hit' "${summary_json}")"
  quiet_period_clean="$(jq -r '.quiet_period_clean' "${summary_json}")"
  phase_inconclusive="$(jq -r '.phase_inconclusive' "${summary_json}")"
  phase_inconclusive_reasons="$(jq -r '.phase_inconclusive_reasons | join("; ")' "${summary_json}")"

  # Record Check (a) (#151) in pmprofiler: never record a fake green --pass when a violation occurred in report-only mode
  if [[ "${check_a_pass}" == "true" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "S8 Check (a): no source re-restore after checkpoint (#151)" --group "redis" \
      --pass --value 1 --total 1 \
      --detail "0 source re-restore events after checkpoint (source_uid=$(jq -r '.source_pod_uid' "${summary_json}"))" >/dev/null || true
  elif [[ "${enforce_a}" == "true" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "S8 Check (a): no source re-restore after checkpoint (#151)" --group "redis" \
      --pass=false --value 0 --total 1 \
      --detail "VIOLATION (#151): source pod emitted 'Successfully restored the pod from PodSnapshot' after checkpoint" >/dev/null || true
    die "S8 FAIL (Check a / #151): source pod re-restored from PodSnapshot after checkpoint (${summary_json})"
  else
    err "S8 Check (a) [REPORT-ONLY WARNING]: source pod emitted 'Successfully restored the pod from PodSnapshot' after checkpoint (#151)"
  fi

  # Record Check (b) (#131) + phase-anchored serving continuity (#149) in pmprofiler.
  # Phase validity (phase_hit / quiet_period_clean) governs the phase-anchored continuity part of Check (b)
  # behind enforce_b (never enforce_a), and on Dataplane V2 (phase_status=skipped) the kube-proxy phase anchor
  # is skipped rather than failed.
  local check_b_violation="false"
  if [[ "${check_b_pass}" != "true" || "${cold_served}" -gt 0 || "${empty_reads}" -gt 0 || "${fenced_pods_seen}" -eq 0 ]]; then
    check_b_violation="true"
  fi
  local check_b_detail="dataplane_mode=${dataplane_mode}, phase_status=${phase_status}, phase_applicable=${phase_applicable}, phase_hit=${phase_hit}, quiet_period_clean=${quiet_period_clean}, fenced_pods_seen=${fenced_pods_seen}, fenced_serving_pass=${check_b_pass}, cold_served=${cold_served}, empty_reads=${empty_reads}, lost_after_ckpt=${lost_after_ckpt}, kp_sync_s=${kp_sync_s}"
  if [[ "${check_b_violation}" == "false" && "${phase_inconclusive}" == "false" ]]; then
    local check_b_title="S8 Check (b): fenced pod never serving & zero cold/empty reads (#131)"
    if [[ "${phase_status}" == "skipped" ]]; then
      check_b_title="S8 Check (b) [DPv2 phase anchor skipped]: fenced pod never serving & zero cold/empty reads (#131)"
    fi
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "${check_b_title}" --group "redis" \
      --pass --value 1 --total 1 \
      --detail "${check_b_detail}" >/dev/null || true
  elif [[ "${enforce_b}" == "true" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "S8 Check (b): fenced pod never serving & zero cold/empty reads (#131)" --group "redis" \
      --pass=false --value 0 --total 1 \
      --detail "VIOLATION / INCONCLUSIVE (#131/#149): ${check_b_detail}" >/dev/null || true
    die "S8 FAIL (Check b / #131 / #149): fenced pod serving, no fenced pods seen, phase window missed, or cold/empty reads observed: ${check_b_detail} (${summary_json})"
  elif [[ "${check_b_violation}" == "false" && "${phase_inconclusive}" == "true" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "S8 Check (b) [INCONCLUSIVE]: phase window missed or noisy (#149)" --group "redis" \
      --pass --value 0 --total 0 \
      --detail "INCONCLUSIVE (#149): ${phase_inconclusive_reasons}; ${check_b_detail}" >/dev/null || true
    err "S8 Check (b) / Phase [INCONCLUSIVE]: ${phase_inconclusive_reasons}; ${check_b_detail} (#149)"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "S8 Check (b) [REPORT-ONLY WARNING]: fenced pod serving or cold/empty reads (#131)" --group "redis" \
      --pass --value 0 --total 0 \
      --detail "WARNING (#131): ${check_b_detail}" >/dev/null || true
    err "S8 Check (b) [REPORT-ONLY WARNING]: ${check_b_detail} (#131)"
  fi
}

detect_s8_dataplane_mode() {
  if ! "${KUBECTL_CMD}" get pods -n kube-system -o name >/dev/null 2>&1; then
    echo "unknown"
    return 0
  fi
  local dpv2_pods dpv2_desired kp_running kp_ds_desired
  dpv2_pods="$("${KUBECTL_CMD}" get pods -n kube-system -l k8s-app=cilium --field-selector=status.phase=Running -o name 2>/dev/null || true)"
  if [[ -n "${dpv2_pods}" ]]; then
    echo "dataplane-v2"
    return 0
  fi
  dpv2_desired="$("${KUBECTL_CMD}" get daemonset -n kube-system anetd cilium -o jsonpath='{range .items[*]}{.status.desiredNumberScheduled}{"\n"}{end}' 2>/dev/null | awk '$1 > 0 { found=1 } END { print (found ? "1" : "0") }')"
  if [[ "${dpv2_desired}" == "1" ]]; then
    echo "dataplane-v2"
    return 0
  fi
  kp_running="$("${KUBECTL_CMD}" get pods -n kube-system -l component=kube-proxy --field-selector=status.phase=Running -o name 2>/dev/null || true)"
  if [[ -z "${kp_running}" ]]; then
    kp_running="$("${KUBECTL_CMD}" get pods -n kube-system -l k8s-app=kube-proxy --field-selector=status.phase=Running -o name 2>/dev/null || true)"
  fi
  if [[ -n "${kp_running}" ]]; then
    echo "kube-proxy"
    return 0
  fi
  kp_ds_desired="$("${KUBECTL_CMD}" get daemonset -n kube-system kube-proxy -o jsonpath='{.status.desiredNumberScheduled}' 2>/dev/null || true)"
  if [[ "${kp_ds_desired}" =~ ^[0-9]+$ ]] && [[ "${kp_ds_desired}" -gt 0 ]]; then
    echo "kube-proxy"
  else
    echo "unknown"
  fi
}

run_scenario_s8_single_w() {
  local w_val="$1"
  local run_dir="$2"
  log "=== Running Scenario S8: Phase-Controlled Write-Continuity (W=${w_val}s, enforce_a=${S8_ENFORCE_CHECK_A}, enforce_b=${S8_ENFORCE_CHECK_B}) ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_redis_workload "t2-redis"
  sleep 2

  local src_pod src_uid nonce src_node dataplane_mode
  src_pod="t2-redis-0"
  src_uid="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.metadata.uid}')"
  nonce="nonce-s8-$(date +%s)"
  v_redis_seed "${src_pod}" "${nonce}"
  src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
  deploy_s8_pinger_and_observer "t2-redis-svc"
  dataplane_mode="$(detect_s8_dataplane_mode)"
  if [[ "${dataplane_mode}" != "kube-proxy" ]]; then
    log "S8 note: no kube-proxy detected in kube-system (dataplane_mode=${dataplane_mode}); kube-proxy 10s sync phase anchor is not applicable on Dataplane V2"
  fi
  log "S8 source pod ${src_pod} (uid=${src_uid}) on ${src_node}; pinger + 20ms observer co-located on ${PINGER_NODE} (dataplane=${dataplane_mode})"

  # Clean up any leftover anchor Service and cordon the source node BEFORE starting the EndpointSlice
  # watch so neither cleanup nor cordon API latency lands inside the 12s quiet window.
  local anchor_svc="s8-phase-anchor"
  kubectl delete service "${anchor_svc}" -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true
  cordon_node "${src_node}"

  start_collector "${run_dir}" "S8: Phase-controlled write-continuity (W=${w_val}s)"
  start_s8_watches "${run_dir}" "app=t2-redis" "t2-redis-svc"

  local quiet_start_ms quiet_end_ms a_ms d_sec target_ms cur_ms sleep_s drain_ms
  quiet_start_ms="$(now_ms)"
  log "S8 phase anchor: waiting 12s quiet period before creating throwaway ClusterIP Service ${anchor_svc}"
  sleep 12
  quiet_end_ms="$(now_ms)"
  kubectl create service clusterip "${anchor_svc}" -n "${NAMESPACE}" --tcp=1:1 >/dev/null
  a_ms="$(now_ms)"
  d_sec="$(awk -v w="${w_val}" 'BEGIN { x = 10 - w; if (x < 0) x = 0; printf "%.3f", x }')"
  target_ms="$(awk -v a="${a_ms}" -v d="${d_sec}" 'BEGIN { printf "%d", a + d * 1000 }')"
  cur_ms="$(now_ms)"
  sleep_s="$(awk -v t="${target_ms}" -v n="${cur_ms}" 'BEGIN { x = (t - n) / 1000; if (x < 0) x = 0; printf "%.3f", x }')"
  sleep "${sleep_s}"

  drain_ms="$(now_ms)"
  # Activate s8-sync-probe endpoint right at drain_ms so observer-t2-redis directly measures
  # the client node's first post-drain kube-proxy iptables sync time (kp_sync_s).
  kubectl label pod -n "${NAMESPACE}" observer-t2-redis s8-sync-probe=active --overwrite >/dev/null 2>&1 &
  local label_pid=$!
  evict_pod "${src_pod}"
  wait "${label_pid}" || true

  echo "${drain_ms}" >"${run_dir}/drain_ms.txt"
  printf '%s\n' "${src_pod}" >"${run_dir}/source_pod.txt"
  printf '%s\n' "${src_uid}" >"${run_dir}/source_pod_uid.txt"
  printf 'quiet_start_ms=%s anchor_created_ms=%s W=%s D=%s drain_ms=%s drain_minus_anchor_ms=%s pinger_node=%s dataplane_mode=%s namespace=%s target_service=t2-redis-svc\n' \
    "${quiet_start_ms}" "${quiet_end_ms}" "${w_val}" "${d_sec}" "${drain_ms}" "$((drain_ms - a_ms))" "${PINGER_NODE}" "${dataplane_mode}" "${NAMESPACE}" >"${run_dir}/phase.txt"

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  uncordon_all_tracked_nodes
  kubectl rollout status statefulset/t2-redis -n "${NAMESPACE}" --timeout=180s >/dev/null

  # Allow post-restore kube-proxy dataplane sync and pinger/observer tail samples
  sleep 10
  stop_s8_watches
  kubectl logs -n "${NAMESPACE}" pod/pinger-t2-redis >"${run_dir}/pinger.log" 2>&1 || true
  kubectl logs -n "${NAMESPACE}" pod/observer-t2-redis >"${run_dir}/observer.log" 2>&1 || true
  kubectl get events -n "${NAMESPACE}" -o json >"${run_dir}/events.json" 2>&1 || true
  kubectl delete service "${anchor_svc}" s8-sync-probe -n "${NAMESPACE}" --ignore-not-found >/dev/null 2>&1 || true

  v_redis_verify "${run_dir}" "${src_pod}" "${nonce}"
  s8_phase_analyze "${run_dir}" "${S8_ENFORCE_CHECK_A}" "${S8_ENFORCE_CHECK_B}"

  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S8: Phase-controlled write-continuity scenario (#149, #151, #131)
run_scenario_s8() {
  local w_list=()
  IFS=',' read -ra w_list <<<"${S8_WINDOW_W}"
  if [[ "${#w_list[@]}" -le 1 ]]; then
    run_scenario_s8_single_w "${w_list[0]}" "${OUT_DIR}/s8-phase-controlled-continuity"
  else
    local w_val
    for w_val in "${w_list[@]}"; do
      local w_slug="${w_val//./_}"
      run_scenario_s8_single_w "${w_val}" "${OUT_DIR}/s8-phase-controlled-continuity-w${w_slug}"
    done
  fi
}

# ==============================================================================
# Offline CI Self-Test Mode (--self-test)
# ==============================================================================
run_self_test() {
  log "Running offline T2 guardrail self-test in ${OUT_DIR}"
  rm -rf "${OUT_DIR}"
  mkdir -p "${OUT_DIR}"
  build_pmprofiler

  local prom_clean='# HELP pod_migration_invariant_violations_total Total number of runtime invariant violations
# TYPE pod_migration_invariant_violations_total counter
pod_migration_invariant_violations_total{invariant="I1_DoubleExecution"} 0
pod_migration_invariant_violations_total{invariant="I2_RestoreVerification"} 0
pod_migration_invariant_violations_total{invariant="I3_VolumeSafeUngate"} 0
pod_migration_invariant_violations_total{invariant="I4_OrphanArtifact"} 0
pod_migration_invariant_violations_total{invariant="I5_BoundedTermination"} 0
pod_migration_invariant_violations_total{invariant="I6_InterceptScope"} 0
pod_migration_invariant_violations_total{invariant="I7_PDBBudget"} 0
pod_migration_invariant_violations_total{invariant="I8_DeterministicLeader"} 0
pod_migration_invariant_violations_total{invariant="I9_ColdStartTransparency"} 0'

  emit_synthetic_scenario() {
    local dir="$1"
    local scenario_name="$2"
    local app="$3"
    local pmj_name="$4"
    local src_pod="$5"
    local dst_pod="$6"
    local phase="$7"
    local restored_status="$8"
    local restored_reason="$9"
    local src_uid="${10:-}"
    local dst_uid="${11:-}"
    if [[ -z "${src_uid}" ]]; then
      if [[ "${src_pod}" == "${dst_pod}" ]]; then
        src_uid="uid-${src_pod}-src"
      else
        src_uid="uid-${src_pod}"
      fi
    fi
    if [[ -z "${dst_uid}" ]]; then
      if [[ "${src_pod}" == "${dst_pod}" ]]; then
        dst_uid="uid-${dst_pod}-dst"
      else
        dst_uid="uid-${dst_pod}"
      fi
    fi

    mkdir -p "${dir}"
    cat >"${dir}/meta.json" <<EOF
{"scenario":"${scenario_name}","namespace":"default","startedAt":"2026-09-27T10:00:00Z","serverVersion":"v1.32.2-gke.1200"}
EOF
    python3 - "${dir}/records.ndjson" "${app}" "${pmj_name}" "${src_pod}" "${dst_pod}" "${src_uid}" "${dst_uid}" "${phase}" "${restored_status}" "${restored_reason}" "${prom_clean}" <<'PY'
import json, sys

out_path, app, pmj_name, src_pod, dst_pod, src_uid, dst_uid, phase, r_status, r_reason, prom_body = sys.argv[1:]

records = [
    {
        "ts": "2026-09-27T09:59:00Z",
        "type": "list",
        "gvr": "pods.v1",
        "obj": {
            "metadata": {
                "name": src_pod,
                "uid": src_uid,
                "creationTimestamp": "2026-09-27T09:59:00Z",
                "labels": {"app": app, "pod-migration.gke.io/enabled": "true"},
            },
            "spec": {"nodeName": "gke-node-a"},
            "status": {
                "podIP": "10.24.1.10",
                "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-09-27T09:59:10Z"}],
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:01Z",
        "type": "add",
        "gvr": "podmigrations.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": "diskless-migration", "creationTimestamp": "2026-09-27T10:00:01Z"},
            "spec": {"storage": {"location": "gs://example-podsnapshots/snapshots"}},
            "status": {"conditions": [{"type": "Ready", "status": "True", "reason": "Reconciled"}]},
        },
    },
    {
        "ts": "2026-09-27T10:00:02Z",
        "type": "add",
        "gvr": "podmigrationjobs.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": pmj_name, "uid": f"uid-{pmj_name}", "creationTimestamp": "2026-09-27T10:00:02Z"},
            "spec": {"podRef": {"name": src_pod}, "targetPodUID": src_uid},
            "status": {"phase": "Snapshotting"},
        },
    },
    {
        "ts": "2026-09-27T10:00:08Z",
        "type": "update",
        "gvr": "podsnapshots.v1alpha1.podsnapshot.gke.io",
        "obj": {
            "metadata": {
                "name": f"snap-{src_pod}",
                "creationTimestamp": "2026-09-27T10:00:03Z",
                "labels": {"pod-migrate.io/engine": "gvisor-snapshot"},
            },
            "spec": {"podName": src_pod},
            "status": {
                "conditions": [{"type": "Triggered", "status": "True", "lastTransitionTime": "2026-09-27T10:00:08Z"}]
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:09Z",
        "type": "update",
        "gvr": "pods.v1",
        "obj": {
            "metadata": {
                "name": src_pod,
                "uid": src_uid,
                "creationTimestamp": "2026-09-27T09:59:00Z",
                "deletionTimestamp": "2026-09-27T10:00:09Z",
                "labels": {"app": app, "pod-migration.gke.io/enabled": "true"},
            },
            "spec": {"nodeName": "gke-node-a"},
            "status": {},
        },
    },
    {
        "ts": "2026-09-27T10:00:10Z",
        "type": "add",
        "gvr": "pods.v1",
        "obj": {
            "metadata": {
                "name": dst_pod,
                "uid": dst_uid,
                "creationTimestamp": "2026-09-27T10:00:10Z",
                "labels": {"app": app, "pod-migration.gke.io/enabled": "true"},
            },
            "spec": {"nodeName": "gke-node-b"},
            "status": {
                "podIP": "10.24.2.20",
                "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-09-27T10:00:14Z"}],
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:15Z",
        "type": "update",
        "gvr": "podmigrationjobs.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": pmj_name, "uid": f"uid-{pmj_name}", "creationTimestamp": "2026-09-27T10:00:02Z"},
            "spec": {"podRef": {"name": src_pod}, "targetPodUID": src_uid},
            "status": {
                "phase": phase,
                "snapshotRef": f"snap-{src_pod}",
                "restoredPodName": dst_pod,
                "restoredPodUID": dst_uid,
                "evictingStartTime": "2026-09-27T10:00:08Z",
                "completionTime": "2026-09-27T10:00:15Z",
                "conditions": [
                    {
                        "type": "Restored",
                        "status": r_status,
                        "reason": r_reason,
                        "lastTransitionTime": "2026-09-27T10:00:15Z",
                    }
                ],
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:16Z",
        "type": "prommetrics",
        "gvr": "",
        "obj": {
            "pod": "pod-migration-controller-0",
            "body": prom_body,
        },
    },
]

with open(out_path, "w") as f:
    for r in records:
        f.write(json.dumps(r) + "\n")
PY
  }

  emit_clean_continuity_trace() {
    local trace_file="$1"
    local src_pod="$2"
    local dst_pod="$3"
    local inst_id="$4"
    cat >"${trace_file}" <<EOF
{"ts":"2026-09-27T10:00:01Z","op":"step","pod":"${src_pod}","instanceId":"${inst_id}","ackedSeq":1,"observedMaxSeq":1,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:05Z","op":"step","pod":"${src_pod}","instanceId":"${inst_id}","ackedSeq":2,"observedMaxSeq":2,"phase":"migrating"}
{"ts":"2026-09-27T10:00:09Z","op":"step","pod":"${src_pod}","phase":"migrating","error":"connection reset during handoff"}
{"ts":"2026-09-27T10:00:14Z","op":"step","pod":"${dst_pod}","instanceId":"${inst_id}","ackedSeq":3,"observedMaxSeq":3,"phase":"post-restore"}
{"ts":"2026-09-27T10:00:15Z","op":"final","pod":"${dst_pod}","instanceId":"${inst_id}","ackedSeq":4,"observedMaxSeq":4,"phase":"post-restore"}
EOF
  }

  # S1: Counter rolling upgrade during active node drain
  emit_synthetic_scenario "${OUT_DIR}/s1" "S1: Deployment rolling upgrade during active node drain" \
    "counter" "pmj-s1-counter" "counter-0" "counter-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s1" \
    --name "state survived (token-verified)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "instanceID=7f9c-s1 preserved, count advanced 42 -> 51"
  emit_clean_continuity_trace "${OUT_DIR}/s1/continuity.ndjson" "counter-0" "counter-0-restored" "7f9c-s1"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s1" --trace "${OUT_DIR}/s1/continuity.ndjson" --group "counter"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s1" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S2: Redis PDB temporary block followed by release (50,000 keys + nonce)
  emit_synthetic_scenario "${OUT_DIR}/s2" "S2: PDB minAvailable 100% block followed by budget release" \
    "redis" "pmj-s2-redis" "redis-0" "redis-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s2" \
    --name "state survived (token-verified)" --group "redis" \
    --pass --value 50000 --total 50000 \
    --detail "migkey=nonce-s2 matched and DBSIZE=50001 (>= 50000)"
  emit_clean_continuity_trace "${OUT_DIR}/s2/continuity.ndjson" "redis-0" "redis-0-restored" "runid-redis-s2"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s2" --trace "${OUT_DIR}/s2/continuity.ndjson" --group "redis"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s2" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S3: Corrupted checkpoint -> deterministic I9 cold-start fallback (FallbackToColdStart)
  emit_synthetic_scenario "${OUT_DIR}/s3" "S3: Corrupted checkpoint artifact -> I9 cold-start fallback" \
    "counter" "pmj-s3-counter" "counter-corrupt-0" "counter-corrupt-0-fallback" \
    "SucceededWithoutRestore" "False" "FallbackToColdStart"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s3" \
    --name "state survived (token-verified)" --group "counter" \
    --pass=false --value 0 --total 1 \
    --detail "cold-start instanceID=fresh-uuid replaced pre-checkpoint instanceID=old-uuid (consistent with FallbackToColdStart)"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s3" \
    --name "I9 deterministic cold-start fallback (FallbackToColdStart)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "PMJ transitioned to SucceededWithoutRestore with Restored=False (Reason=FallbackToColdStart)"
  if "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s3" --assert-zero-invariants --assert-clean-outcomes >/dev/null 2>&1; then
    die "Expected S3 cold-start run to fail --assert-clean-outcomes when --allow-cold-start is not set"
  fi
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s3" --assert-zero-invariants --assert-clean-outcomes --allow-cold-start --assert-verdict-matches-data

  # S4: Postgres + Counter dual-replica contention
  emit_synthetic_scenario "${OUT_DIR}/s4" "S4: Dual-replica simultaneous eviction under serialized PodGate contention" \
    "postgres" "pmj-s4-postgres" "postgres-0" "postgres-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s4" \
    --name "state survived (token-verified)" --group "postgres" \
    --pass --value 200 --total 200 \
    --detail "contiguous sequence 1..200 (200 rows) and nonce=nonce-s4 preserved"
  emit_clean_continuity_trace "${OUT_DIR}/s4/continuity.ndjson" "postgres-0" "postgres-0-restored" "pg-start-s4"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s4" --trace "${OUT_DIR}/s4/continuity.ndjson" --group "postgres"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s4" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)
  emit_synthetic_scenario "${OUT_DIR}/s5" "S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)" \
    "counter" "pmj-s5-counter" "counter-s5-0" "counter-s5-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s5" \
    --name "state survived (token-verified)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "instanceID=7f9c-s5 preserved across mid-flight PodMigration deletion"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s5" \
    --name "I4 zero orphan PSSC after PodMigration deletion" --group "controller" \
    --pass --value 1 --total 1 \
    --detail "0 orphan PodSnapshotStorageConfigs and clean Undeploying deferral"
  emit_clean_continuity_trace "${OUT_DIR}/s5/continuity.ndjson" "counter-s5-0" "counter-s5-0-restored" "7f9c-s5"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s5" --trace "${OUT_DIR}/s5/continuity.ndjson" --group "counter"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s5" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S6: Continuous write-continuity probe under live warm migration (#103)
  emit_synthetic_scenario "${OUT_DIR}/s6" "S6: Continuous write-continuity probe across warm migration" \
    "redis" "pmj-s6-redis" "redis-s6-0" "redis-s6-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s6" \
    --name "state survived (token-verified)" --group "redis" \
    --pass --value 50000 --total 50000 \
    --detail "migkey=nonce-s6 matched and DBSIZE=50004 (>= 50000)"
  emit_clean_continuity_trace "${OUT_DIR}/s6/continuity.ndjson" "redis-s6-0" "redis-s6-0-restored" "runid-redis-s6"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s6" --trace "${OUT_DIR}/s6/continuity.ndjson" --group "redis"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s6" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S7: Verdict matches data (RestoreVerified vs. application state, #104)
  emit_synthetic_scenario "${OUT_DIR}/s7" "S7: Verdict matches data (RestoreVerified vs. application state)" \
    "redis" "pmj-s7-redis" "redis-s7-0" "redis-s7-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s7" \
    --name "state survived (token-verified)" --group "redis" \
    --pass --value 50000 --total 50000 \
    --detail "migkey=nonce-s7 matched and DBSIZE=50004 (>= 50000)"
  emit_clean_continuity_trace "${OUT_DIR}/s7/continuity.ndjson" "redis-s7-0" "redis-s7-0-restored" "runid-redis-s7"
  "${PMPROFILER_BIN}" continuity --run "${OUT_DIR}/s7" --trace "${OUT_DIR}/s7/continuity.ndjson" --group "redis"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s7" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # S8: Phase-controlled write-continuity scenario (#149, #151, #131) - Clean fenced StatefulSet run
  # Use the SAME pod name ("t2-redis-0") for both source (uid-t2-redis-0-src) and replacement (uid-t2-redis-0-dst)
  # to verify that Check (a) and Check (b) correlate by Pod UID rather than colliding on StatefulSet pod names.
  emit_synthetic_scenario "${OUT_DIR}/s8" "S8: Phase-controlled write-continuity (W=5s)" \
    "redis" "pmj-s8-redis" "t2-redis-0" "t2-redis-0" "Succeeded" "True" "RestoreVerified" \
    "uid-t2-redis-0-src" "uid-t2-redis-0-dst"
  echo "1758967203000" >"${OUT_DIR}/s8/drain_ms.txt"
  printf '%s\n' "t2-redis-0" >"${OUT_DIR}/s8/source_pod.txt"
  printf '%s\n' "uid-t2-redis-0-src" >"${OUT_DIR}/s8/source_pod_uid.txt"
  printf 'quiet_start_ms=1758967186000 quiet_end_ms=1758967198000 anchor_created_ms=1758967198000 W=5 D=5.000 drain_ms=1758967203000 drain_minus_anchor_ms=5000 pinger_node=gke-node-b dataplane_mode=kube-proxy service_name=t2-redis-svc namespace=default\n' >"${OUT_DIR}/s8/phase.txt"
  cat >"${OUT_DIR}/s8/pinger.log" <<'EOF'
START 10:00:00
A 1758967201000 1
A 1758967202000 2
GAP 14.200 10:00:17
A 1758967217200 3
A 1758967217300 4
EOF
  cat >"${OUT_DIR}/s8/observer.log" <<'EOF'
1758967202000 2 ok :50002 10.24.1.10
1758967205120 1 refused
1758967208020 2 sync_probe_ok :0 10.24.0.99
1758967217200 2 ok :50004 10.24.2.20
EOF
  cat >"${OUT_DIR}/s8/pods.ndjson" <<'EOF'
{"t_ms":1758967202500,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src","labels":{"app":"redis","pod-migration.gke.io/fenced":"true"}},"status":{"podIP":"10.24.1.10","conditions":[{"type":"Ready","status":"False","reason":"PodMigrationFenced"}]}}}}
{"t_ms":1758967217100,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-0","uid":"uid-t2-redis-0-dst","labels":{"app":"redis"}},"status":{"podIP":"10.24.2.20","conditions":[{"type":"Ready","status":"True"}]}}}}
EOF
  # Include an unrelated kube-system EndpointSlice event at +550ms after fence followed by a target-service
  # EndpointSlice event at +800ms (inside the 1500ms endpoint-controller grace window) where t2-redis-0 is still
  # transiently serving=true before endpoint-controller removes it at +950ms. Because Check (b) filters to the
  # target service (kubernetes.io/service-name=t2-redis-svc), the unrelated kube-system event does NOT set
  # first_non_serving_ep_ms and does not trigger a false re_served violation.
  cat >"${OUT_DIR}/s8/endpointslices.ndjson" <<'EOF'
{"t_ms":1758967203050,"ev":{"type":"WATCH","object":{"metadata":{"name":"kube-dns-abc","namespace":"kube-system","labels":{"kubernetes.io/service-name":"kube-dns"}},"endpoints":[{"addresses":["10.24.0.10"],"conditions":{"ready":true,"serving":true,"terminating":false}}]}}}
{"t_ms":1758967203300,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-svc-xyz","namespace":"default","labels":{"kubernetes.io/service-name":"t2-redis-svc"}},"endpoints":[{"addresses":["10.24.1.10"],"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src"}}]}}}
{"t_ms":1758967203450,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-svc-xyz","namespace":"default","labels":{"kubernetes.io/service-name":"t2-redis-svc"}},"endpoints":[{"addresses":["10.24.1.10"],"conditions":{"ready":false,"serving":false,"terminating":false},"targetRef":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src"}}]}}}
{"t_ms":1758967217000,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-svc-xyz","namespace":"default","labels":{"kubernetes.io/service-name":"t2-redis-svc"}},"endpoints":[{"addresses":["10.24.2.20"],"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"name":"t2-redis-0","uid":"uid-t2-redis-0-dst"}}]}}}
EOF
  cat >"${OUT_DIR}/s8/events.json" <<'EOF'
{"items":[{"metadata":{"uid":"ev-restored-dst","creationTimestamp":"2026-09-27T10:00:17Z"},"involvedObject":{"name":"t2-redis-0","uid":"uid-t2-redis-0-dst"},"lastTimestamp":"2026-09-27T10:00:17Z","message":"Successfully restored the pod from PodSnapshot snap-t2-redis-0","count":1}]}
EOF
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s8" \
    --name "state survived (token-verified)" --group "redis" \
    --pass --value 50000 --total 50000 \
    --detail "migkey=nonce-s8 matched and DBSIZE=50004 (>= 50000)"
  s8_phase_analyze "${OUT_DIR}/s8" "true" "true"
  [[ "$(jq -r '.inputs_valid' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "true" ]] || die "Expected S8 clean fixture inputs_valid == true"
  [[ "$(jq -r '.cold_served' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "0" ]] || die "Expected S8 clean fixture cold_served == 0"
  [[ "$(jq -r '.empty_reads' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "0" ]] || die "Expected S8 clean fixture empty_reads == 0"
  [[ "$(jq -r '.lost_after_ckpt' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "0" ]] || die "Expected S8 clean fixture lost_after_ckpt == 0"
  [[ "$(jq -r '.kube_proxy_sync_s' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "5.02" ]] || die "Expected S8 clean fixture kube_proxy_sync_s == 5.02 (from sync_probe_ok)"
  [[ "$(jq -r '.backend_cutover_s' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "14.2" ]] || die "Expected S8 clean fixture backend_cutover_s == 14.2 (from replacement pod Ready)"
  [[ "$(jq -r '.phase_status' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "pass" ]] || die "Expected S8 clean fixture phase_status == pass"
  [[ "$(jq -r '.phase_hit' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "true" ]] || die "Expected S8 clean fixture phase_hit == true"
  [[ "$(jq -r '.phase_inconclusive' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "false" ]] || die "Expected S8 clean fixture phase_inconclusive == false"
  [[ "$(jq -r '.quiet_period_clean' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "true" ]] || die "Expected S8 clean fixture quiet_period_clean == true"
  [[ "$(jq -r '.fenced_pods_seen' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "1" ]] || die "Expected S8 clean fixture fenced_pods_seen == 1"
  [[ "$(jq -r '.check_a_source_no_rerestore.pass' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "true" ]] || die "Expected S8 clean fixture Check (a) pass == true"
  [[ "$(jq -r '.check_b_fenced_never_serving.pass' "${OUT_DIR}/s8/s8_phase_continuity.json")" == "true" ]] || die "Expected S8 clean fixture Check (b) pass == true"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s8" --assert-zero-invariants --assert-clean-outcomes --assert-verdict-matches-data

  # Negative tests for S8 Check (a) (#151), Check (b) (#131), no-fenced-pods vacuous guard, phase miss/DPv2, and empty/corrupt inputs:
  # 1) Source pod UID re-restores after checkpoint (#151): warns in report-only mode (without fake green check), fails when enforced
  local neg_s8_a="${OUT_DIR}/neg-s8-check-a"
  cp -r "${OUT_DIR}/s8" "${neg_s8_a}"
  rm -f "${neg_s8_a}/checks.json" "${neg_s8_a}/checks.ndjson" "${neg_s8_a}/run.json"
  cat >"${neg_s8_a}/events.json" <<'EOF'
{"items":[
  {"metadata":{"uid":"ev-restored-dst","creationTimestamp":"2026-09-27T10:00:07Z"},"involvedObject":{"name":"t2-redis-0","uid":"uid-t2-redis-0-dst"},"lastTimestamp":"2026-09-27T10:00:07Z","message":"Successfully restored the pod from PodSnapshot snap-t2-redis-0","count":1},
  {"metadata":{"uid":"ev-rerestore-src","creationTimestamp":"2026-09-27T10:00:05Z"},"involvedObject":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src"},"lastTimestamp":"2026-09-27T10:00:05Z","message":"Successfully restored the pod from PodSnapshot snap-t2-redis-0","count":1}
]}
EOF
  s8_phase_analyze "${neg_s8_a}" "false" "false"
  [[ "$(jq -r '.check_a_source_no_rerestore.pass' "${neg_s8_a}/s8_phase_continuity.json")" == "false" ]] || die "Expected Check (a) pass == false on source re-restore fixture"
  if [[ -f "${neg_s8_a}/checks.ndjson" ]] && grep -q 'S8 Check (a)' "${neg_s8_a}/checks.ndjson"; then
    die "Expected report-only Check (a) violation NOT to record a misleading green PASS check in checks.ndjson"
  fi
  if (s8_phase_analyze "${neg_s8_a}" "true" "false" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze with enforce_a=true to fail on source re-restore after checkpoint (#151)"
  fi
  rm -rf "${neg_s8_a}"

  # 2) Fenced pod UID serving in EndpointSlice + cold_served + empty_reads (#131): warns in report-only mode, fails when enforced
  local neg_s8_b="${OUT_DIR}/neg-s8-check-b"
  cp -r "${OUT_DIR}/s8" "${neg_s8_b}"
  rm -f "${neg_s8_b}/checks.json" "${neg_s8_b}/checks.ndjson" "${neg_s8_b}/run.json"
  cat >"${neg_s8_b}/pinger.log" <<'EOF'
START 10:00:00
A 1758967201000 10
A 1758967202000 11
LOST 11 first=1 last=11 window>=1.000 10:00:05
A 1758967205000 1
A 1758967205100 2
A 1758967208000 10
EOF
  cat >"${neg_s8_b}/observer.log" <<'EOF'
1758967202000 2 ok :50011 10.24.1.10
1758967205050 2 ok :2 10.24.1.10
1758967208020 2 sync_probe_ok :0 10.24.0.99
1758967208200 2 ok :50010 10.24.2.20
EOF
  cat >"${neg_s8_b}/endpointslices.ndjson" <<'EOF'
{"t_ms":1758967205000,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-svc-xyz","namespace":"default","labels":{"kubernetes.io/service-name":"t2-redis-svc"}},"endpoints":[{"addresses":["10.24.1.10"],"conditions":{"ready":true,"serving":true,"terminating":false},"targetRef":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src"}}]}}}
EOF
  s8_phase_analyze "${neg_s8_b}" "false" "false"
  [[ "$(jq -r '.check_b_fenced_never_serving.pass' "${neg_s8_b}/s8_phase_continuity.json")" == "false" ]] || die "Expected Check (b) pass == false when fenced pod is serving in EndpointSlice"
  [[ "$(jq -r '.cold_served' "${neg_s8_b}/s8_phase_continuity.json")" == "2" ]] || die "Expected cold_served == 2 on cold-serving fixture"
  [[ "$(jq -r '.empty_reads' "${neg_s8_b}/s8_phase_continuity.json")" == "1" ]] || die "Expected empty_reads == 1 on cold-serving fixture"
  if (s8_phase_analyze "${neg_s8_b}" "false" "true" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze with enforce_b=true to fail when fenced pod is serving in EndpointSlice (#131)"
  fi
  rm -rf "${neg_s8_b}"

  # 3) Zero fenced pods observed: records no_fenced_pods_seen=true and fails closed when enforce_b=true
  local neg_s8_nofence="${OUT_DIR}/neg-s8-no-fenced"
  cp -r "${OUT_DIR}/s8" "${neg_s8_nofence}"
  rm -f "${neg_s8_nofence}/checks.json" "${neg_s8_nofence}/checks.ndjson" "${neg_s8_nofence}/run.json"
  cat >"${neg_s8_nofence}/pods.ndjson" <<'EOF'
{"t_ms":1758967202500,"ev":{"type":"WATCH","object":{"metadata":{"name":"t2-redis-0","uid":"uid-t2-redis-0-src","labels":{"app":"redis"}},"status":{"podIP":"10.24.1.10","conditions":[{"type":"Ready","status":"True"}]}}}}
EOF
  s8_phase_analyze "${neg_s8_nofence}" "false" "false"
  [[ "$(jq -r '.fenced_pods_seen' "${neg_s8_nofence}/s8_phase_continuity.json")" == "0" ]] || die "Expected fenced_pods_seen == 0 when no pod has fenced label"
  [[ "$(jq -r '.no_fenced_pods_seen' "${neg_s8_nofence}/s8_phase_continuity.json")" == "true" ]] || die "Expected no_fenced_pods_seen == true when no pod has fenced label"
  [[ "$(jq -r '.check_b_fenced_never_serving.status' "${neg_s8_nofence}/s8_phase_continuity.json")" == "no_fenced_pods_seen" ]] || die "Expected check_b status == no_fenced_pods_seen"
  if (s8_phase_analyze "${neg_s8_nofence}" "false" "true" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze with enforce_b=true to fail closed when 0 fenced pods are observed"
  fi
  rm -rf "${neg_s8_nofence}"

  # 4) Phase window missed (kp_sync_s = 9.0 at W = 5, outside +-2.0s tolerance): marks phase_hit=false, phase_inconclusive=true,
  # records an explicit [INCONCLUSIVE] Check (b) entry in checks.ndjson in report-only mode, allows Check (a) enforcement
  # (enforce_a=true, enforce_b=false) to pass independently (#151), and fails closed when enforce_b=true (#149)
  local neg_s8_miss="${OUT_DIR}/neg-s8-phase-miss"
  cp -r "${OUT_DIR}/s8" "${neg_s8_miss}"
  rm -f "${neg_s8_miss}/checks.json" "${neg_s8_miss}/checks.ndjson" "${neg_s8_miss}/run.json"
  cat >"${neg_s8_miss}/observer.log" <<'EOF'
1758967202000 2 ok :50002 10.24.1.10
1758967205120 1 refused
1758967212000 2 sync_probe_ok :0 10.24.0.99
1758967217200 2 ok :50004 10.24.2.20
EOF
  s8_phase_analyze "${neg_s8_miss}" "true" "false"
  [[ "$(jq -r '.kube_proxy_sync_s' "${neg_s8_miss}/s8_phase_continuity.json")" == "9.0" ]] || die "Expected kube_proxy_sync_s == 9.0 on phase-miss fixture"
  [[ "$(jq -r '.phase_status' "${neg_s8_miss}/s8_phase_continuity.json")" == "inconclusive" ]] || die "Expected phase_status == inconclusive when kp_sync_s=9.0 at W=5"
  [[ "$(jq -r '.phase_hit' "${neg_s8_miss}/s8_phase_continuity.json")" == "false" ]] || die "Expected phase_hit == false when kp_sync_s=9.0 at W=5"
  [[ "$(jq -r '.phase_inconclusive' "${neg_s8_miss}/s8_phase_continuity.json")" == "true" ]] || die "Expected phase_inconclusive == true when phase_hit=false"
  grep -q 'S8 Check (b) \[INCONCLUSIVE\]' "${neg_s8_miss}/checks.ndjson" || die "Expected report-only phase-miss run to record an explicit [INCONCLUSIVE] Check (b) entry in checks.ndjson"
  if (s8_phase_analyze "${neg_s8_miss}" "false" "true" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze with enforce_b=true to fail closed when phase_hit=false (#149)"
  fi
  rm -rf "${neg_s8_miss}"

  # 5) Dataplane V2 (dataplane_mode=dataplane-v2 -> phase_applicable=false): skips kube-proxy phase assertion
  # (phase_status="skipped", phase_inconclusive=false) while still enforcing Check (a) and Check (b) (#149)
  local neg_s8_dpv2="${OUT_DIR}/neg-s8-dpv2"
  cp -r "${OUT_DIR}/s8" "${neg_s8_dpv2}"
  rm -f "${neg_s8_dpv2}/checks.json" "${neg_s8_dpv2}/checks.ndjson" "${neg_s8_dpv2}/run.json"
  printf 'quiet_start_ms=1758967186000 quiet_end_ms=1758967198000 anchor_created_ms=1758967198000 W=5 D=5.000 drain_ms=1758967203000 drain_minus_anchor_ms=5000 pinger_node=gke-node-b dataplane_mode=dataplane-v2 service_name=t2-redis-svc namespace=default\n' >"${neg_s8_dpv2}/phase.txt"
  s8_phase_analyze "${neg_s8_dpv2}" "true" "true"
  [[ "$(jq -r '.phase_applicable' "${neg_s8_dpv2}/s8_phase_continuity.json")" == "false" ]] || die "Expected phase_applicable == false on dataplane-v2 fixture"
  [[ "$(jq -r '.phase_status' "${neg_s8_dpv2}/s8_phase_continuity.json")" == "skipped" ]] || die "Expected phase_status == skipped on dataplane-v2 fixture"
  [[ "$(jq -r '.phase_hit' "${neg_s8_dpv2}/s8_phase_continuity.json")" == "false" ]] || die "Expected phase_hit == false on dataplane-v2 fixture"
  [[ "$(jq -r '.phase_inconclusive' "${neg_s8_dpv2}/s8_phase_continuity.json")" == "false" ]] || die "Expected phase_inconclusive == false on dataplane-v2 fixture"
  rm -rf "${neg_s8_dpv2}"

  # 5b) Unknown dataplane (dataplane_mode=unknown): marks phase_inconclusive=true and fails closed when enforce_b=true
  local neg_s8_unknown_dp="${OUT_DIR}/neg-s8-unknown-dp"
  cp -r "${OUT_DIR}/s8" "${neg_s8_unknown_dp}"
  rm -f "${neg_s8_unknown_dp}/checks.json" "${neg_s8_unknown_dp}/checks.ndjson" "${neg_s8_unknown_dp}/run.json"
  printf 'quiet_start_ms=1758967186000 quiet_end_ms=1758967198000 anchor_created_ms=1758967198000 W=5 D=5.000 drain_ms=1758967203000 drain_minus_anchor_ms=5000 pinger_node=gke-node-b dataplane_mode=unknown service_name=t2-redis-svc namespace=default\n' >"${neg_s8_unknown_dp}/phase.txt"
  if (s8_phase_analyze "${neg_s8_unknown_dp}" "false" "true" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze with enforce_b=true to fail closed when dataplane_mode=unknown"
  fi
  [[ "$(jq -r '.phase_inconclusive' "${neg_s8_unknown_dp}/s8_phase_continuity.json")" == "true" ]] || die "Expected phase_inconclusive == true on unknown dataplane fixture"
  rm -rf "${neg_s8_unknown_dp}"

  # 6) Corrupt events.json: must fail closed with inputs_valid=false
  local neg_s8_badev="${OUT_DIR}/neg-s8-bad-events"
  cp -r "${OUT_DIR}/s8" "${neg_s8_badev}"
  echo "Error from server (Forbidden): events is forbidden" >"${neg_s8_badev}/events.json"
  if (s8_phase_analyze "${neg_s8_badev}" "false" "false" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze to fail closed when events.json is corrupt/unreadable"
  fi
  [[ "$(jq -r '.inputs_valid' "${neg_s8_badev}/s8_phase_continuity.json")" == "false" ]] || die "Expected inputs_valid == false on corrupt events.json"
  rm -rf "${neg_s8_badev}"

  # 7) Empty or missing inputs: must fail closed even in report-only mode (enforce_a=false, enforce_b=false)
  local neg_s8_empty="${OUT_DIR}/neg-s8-empty"
  mkdir -p "${neg_s8_empty}"
  echo "1758967203000" >"${neg_s8_empty}/drain_ms.txt"
  if (s8_phase_analyze "${neg_s8_empty}" "false" "false" >/dev/null 2>&1); then
    die "Expected s8_phase_analyze to fail closed when pinger/observer/watch/checkpoint inputs are missing"
  fi
  [[ "$(jq -r '.inputs_valid' "${neg_s8_empty}/s8_phase_continuity.json")" == "false" ]] || die "Expected inputs_valid == false on empty S8 directory"
  rm -rf "${neg_s8_empty}"

  # Negative tests for Issue #103: verify all 3 stateful failure modes fail pmprofiler continuity
  local neg_cont_dir="${OUT_DIR}/neg-continuity"
  mkdir -p "${neg_cont_dir}"

  # Failure mode 1: Lost acknowledged writes (source acks seq 3..4 after checkpoint at seq 2; restored pod only has 1..2)
  cat >"${neg_cont_dir}/lost-acks.ndjson" <<'EOF'
{"ts":"2026-09-27T10:00:01Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":1,"observedMaxSeq":1,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:02Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":2,"observedMaxSeq":2,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:05Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":3,"observedMaxSeq":3,"phase":"migrating"}
{"ts":"2026-09-27T10:00:06Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":4,"observedMaxSeq":4,"phase":"migrating"}
{"ts":"2026-09-27T10:00:14Z","op":"final","pod":"redis-0-restored","instanceId":"run-a","observedMaxSeq":2,"missingSeqs":[3,4],"phase":"post-restore"}
EOF
  local mode1_out=""
  if mode1_out="$("${PMPROFILER_BIN}" continuity --trace "${neg_cont_dir}/lost-acks.ndjson" --group "redis" 2>&1)"; then
    die "Expected pmprofiler continuity to fail on lost acknowledged writes (#103 failure mode 1)"
  fi
  grep -q 'lost acknowledged writes' <<<"${mode1_out}" || die "Expected mode 1 output to report 'lost acknowledged writes', got: ${mode1_out}"
  if grep -q 'double-restore' <<<"${mode1_out}"; then
    die "Expected single-restore mode 1 output NOT to report 'double-restore', got: ${mode1_out}"
  fi

  # Failure mode 2: Empty-source serving (source container restarted empty with new instanceId and observedMaxSeq=0 before eviction)
  cat >"${neg_cont_dir}/empty-source.ndjson" <<'EOF'
{"ts":"2026-09-27T10:00:01Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":1,"observedMaxSeq":1,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:02Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":2,"observedMaxSeq":2,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:06Z","op":"step","pod":"redis-0","instanceId":"run-restarted-empty","observedMaxSeq":0,"phase":"migrating"}
{"ts":"2026-09-27T10:00:14Z","op":"final","pod":"redis-0-restored","instanceId":"run-a","ackedSeq":3,"observedMaxSeq":3,"phase":"post-restore"}
EOF
  local mode2_out=""
  if mode2_out="$("${PMPROFILER_BIN}" continuity --trace "${neg_cont_dir}/empty-source.ndjson" --group "redis" 2>&1)"; then
    die "Expected pmprofiler continuity to fail on empty-source serving (#103 failure mode 2)"
  fi
  grep -q 'empty-source serving' <<<"${mode2_out}" || die "Expected mode 2 output to report 'empty-source serving', got: ${mode2_out}"

  # Failure mode 3: Double restore / rollback (replacement restores at seq 2, acks seq 3..4, then is restored a second time back to seq 2)
  cat >"${neg_cont_dir}/double-restore.ndjson" <<'EOF'
{"ts":"2026-09-27T10:00:01Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":1,"observedMaxSeq":1,"phase":"pre-checkpoint"}
{"ts":"2026-09-27T10:00:02Z","op":"step","pod":"redis-0","instanceId":"run-a","ackedSeq":2,"observedMaxSeq":2,"phase":"migrating"}
{"ts":"2026-09-27T10:00:12Z","op":"step","pod":"redis-0-restored-1","instanceId":"run-a","ackedSeq":3,"observedMaxSeq":3,"phase":"post-restore"}
{"ts":"2026-09-27T10:00:13Z","op":"step","pod":"redis-0-restored-1","instanceId":"run-a","ackedSeq":4,"observedMaxSeq":4,"phase":"post-restore"}
{"ts":"2026-09-27T10:00:18Z","op":"final","pod":"redis-0-restored-2","instanceId":"run-a","observedMaxSeq":2,"missingSeqs":[3,4],"phase":"post-restore"}
EOF
  local mode3_out=""
  if mode3_out="$("${PMPROFILER_BIN}" continuity --trace "${neg_cont_dir}/double-restore.ndjson" --group "redis" 2>&1)"; then
    die "Expected pmprofiler continuity to fail on double restore / sequence rollback (#103 failure mode 3)"
  fi
  grep -q 'double-restore sequence rollback' <<<"${mode3_out}" || die "Expected mode 3 output to report 'double-restore sequence rollback', got: ${mode3_out}"
  rm -rf "${neg_cont_dir}"

  # Negative tests for Issue #104 (S7: the verdict matches the data):
  # 1) Migration reports RestoreVerified (Restored=True) while app is empty (state check Pass=false) -> must fail --assert-verdict-matches-data
  local neg_s7_fp="${OUT_DIR}/neg-s7-false-positive"
  emit_synthetic_scenario "${neg_s7_fp}" "Negative Test: S7 RestoreVerified with Empty App" \
    "redis" "pmj-s7-fp" "redis-fp-0" "redis-fp-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${neg_s7_fp}" \
    --name "state survived (token-verified)" --group "redis" \
    --pass=false --value 0 --total 50000 \
    --detail "migkey='' (expected nonce), DBSIZE=0"
  if "${PMPROFILER_BIN}" analyze --run "${neg_s7_fp}" --assert-verdict-matches-data >/dev/null 2>&1; then
    die "Expected pmprofiler analyze --assert-verdict-matches-data to fail when migration reports RestoreVerified while app is empty"
  fi
  rm -rf "${neg_s7_fp}"

  # 2) Reverse direction: migration reports FallbackToColdStart (Restored=False) while app check says state survived (Pass=true) -> must fail --assert-verdict-matches-data
  local neg_s7_rev="${OUT_DIR}/neg-s7-reverse"
  emit_synthetic_scenario "${neg_s7_rev}" "Negative Test: S7 ColdStart Verdict with Survived State" \
    "counter" "pmj-s7-rev" "counter-rev-0" "counter-rev-0-fallback" "SucceededWithoutRestore" "False" "FallbackToColdStart"
  "${PMPROFILER_BIN}" check --run "${neg_s7_rev}" \
    --name "state survived (token-verified)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "instanceID preserved despite FallbackToColdStart"
  if "${PMPROFILER_BIN}" analyze --run "${neg_s7_rev}" --allow-cold-start --assert-verdict-matches-data >/dev/null 2>&1; then
    die "Expected pmprofiler analyze --assert-verdict-matches-data to fail when migration reports FallbackToColdStart while app state check passed"
  fi
  rm -rf "${neg_s7_rev}"

  # Negative test: verify that a non-zero pod_migration_invariant_violations_total metric fails analyze
  local neg_dir="${OUT_DIR}/neg-invariant-check"
  emit_synthetic_scenario "${neg_dir}" "Negative Test: Invariant Violation Gate" \
    "counter" "pmj-neg" "neg-0" "neg-0-restored" "Succeeded" "True" "RestoreVerified"
  cat >"${neg_dir}/dirty.prom" <<'EOF'
pod_migration_invariant_violations_total{invariant="I3_VolumeSafeUngate"} 1
EOF
  if "${PMPROFILER_BIN}" analyze --run "${neg_dir}" --metrics-file "${neg_dir}/dirty.prom" --assert-zero-invariants >/dev/null 2>&1; then
    die "Expected pmprofiler analyze --assert-zero-invariants to fail on non-zero invariant counter"
  fi
  rm -rf "${neg_dir}"

  local report_html="${OUT_DIR}/guardrail-t2-report.html"
  "${PMPROFILER_BIN}" report \
    --out "${report_html}" \
    --title "GKE Live Pod Migration — T2 Guardrail Scenarios (S1–S8)" \
    "${OUT_DIR}/s1/run.json" \
    "${OUT_DIR}/s2/run.json" \
    "${OUT_DIR}/s3/run.json" \
    "${OUT_DIR}/s4/run.json" \
    "${OUT_DIR}/s5/run.json" \
    "${OUT_DIR}/s6/run.json" \
    "${OUT_DIR}/s7/run.json" \
    "${OUT_DIR}/s8/run.json"

  [[ -s "${report_html}" ]] || die "Expected non-empty HTML report at ${report_html}"

  # Verify runtime-agnostic manifest rendering & entrypoint command arrays (#82, #104)
  local gvisor_counter gvisor_redis gvisor_pg
  gvisor_counter="$(RUNTIME_CLASS=gvisor NODE_SELECTOR="sandbox.gke.io/runtime=gvisor" TOLERATION="sandbox.gke.io/runtime=gvisor:NoSchedule" SERVICE_ACCOUNT="pm-test-ksa" render_counter_manifest)"
  gvisor_redis="$(RUNTIME_CLASS=gvisor NODE_SELECTOR="sandbox.gke.io/runtime=gvisor" TOLERATION="sandbox.gke.io/runtime=gvisor:NoSchedule" SERVICE_ACCOUNT="pm-test-ksa" render_redis_manifest)"
  gvisor_pg="$(RUNTIME_CLASS=gvisor NODE_SELECTOR="sandbox.gke.io/runtime=gvisor" TOLERATION="sandbox.gke.io/runtime=gvisor:NoSchedule" SERVICE_ACCOUNT="pm-test-ksa" render_postgres_manifest)"
  grep -q 'runtimeClassName: gvisor' <<<"${gvisor_counter}" || die "Expected runtimeClassName: gvisor in default counter manifest"
  grep -q 'command: \["docker-entrypoint.sh"\]' <<<"${gvisor_redis}" || die "Expected command: [\"docker-entrypoint.sh\"] in redis manifest (#104)"
  grep -q 'command: \["docker-entrypoint.sh"\]' <<<"${gvisor_pg}" || die "Expected explicit command: [\"docker-entrypoint.sh\"] in postgres manifest"

  local runc_counter runc_redis runc_pg
  runc_counter="$(RUNTIME_CLASS=none NODE_SELECTOR=none TOLERATION=none SERVICE_ACCOUNT=none render_counter_manifest)"
  runc_redis="$(RUNTIME_CLASS=none NODE_SELECTOR=none TOLERATION=none SERVICE_ACCOUNT=none render_redis_manifest)"
  runc_pg="$(RUNTIME_CLASS=none NODE_SELECTOR=none TOLERATION=none SERVICE_ACCOUNT=none render_postgres_manifest)"
  if grep -qE 'runtimeClassName:|nodeSelector:|tolerations:|serviceAccountName:' <<<"${runc_counter}${runc_redis}${runc_pg}"; then
    die "Expected runtime-agnostic ('none') manifests to omit runtimeClassName, nodeSelector, tolerations, and serviceAccountName"
  fi
  grep -q 'command: \["docker-entrypoint.sh"\]' <<<"${runc_redis}" || die "Expected docker-entrypoint.sh in runtime-agnostic redis manifest (#104)"
  grep -q 'command: \["docker-entrypoint.sh"\]' <<<"${runc_pg}" || die "Expected explicit command in runtime-agnostic postgres manifest"

  # Verify --toleration format validation rejects missing ':Effect', empty Effect, or unknown Effect
  if (TOLERATION="foo=bar" validate_toleration_format >/dev/null 2>&1); then
    die "Expected validate_toleration_format to reject --toleration without ':Effect'"
  fi
  if (TOLERATION="foo:" validate_toleration_format >/dev/null 2>&1); then
    die "Expected validate_toleration_format to reject --toleration with empty Effect"
  fi
  if (TOLERATION=":NoSchedule" validate_toleration_format >/dev/null 2>&1); then
    die "Expected validate_toleration_format to reject --toleration with empty key"
  fi
  if (TOLERATION="foo=bar:bogus" validate_toleration_format >/dev/null 2>&1); then
    die "Expected validate_toleration_format to reject --toleration with invalid Effect"
  fi

  # Verify --s8-window-w validation rejects non-numeric or out-of-range values and accepts single or comma-separated W
  if (S8_WINDOW_W="bogus" validate_s8_window_w >/dev/null 2>&1); then
    die "Expected validate_s8_window_w to reject non-numeric --s8-window-w"
  fi
  if (S8_WINDOW_W="0" validate_s8_window_w >/dev/null 2>&1); then
    die "Expected validate_s8_window_w to reject --s8-window-w <= 0"
  fi
  if (S8_WINDOW_W="10" validate_s8_window_w >/dev/null 2>&1); then
    die "Expected validate_s8_window_w to reject --s8-window-w >= 10"
  fi
  (S8_WINDOW_W="5" validate_s8_window_w)
  (S8_WINDOW_W="3,5,7" validate_s8_window_w)

  # Verify detect_s8_dataplane_mode distinguishes GKE Standard static kube-proxy pods (component=kube-proxy),
  # GKE Dataplane V2 clusters (where daemonset/anetd and daemonset/kube-proxy have desiredNumberScheduled=0 and
  # anetd-m runs as -l k8s-app=cilium), and unknown/unreachable clusters
  local fake_dpv2_kubectl="${OUT_DIR}/fake-dpv2-kubectl.sh"
  cat >"${fake_dpv2_kubectl}" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"get daemonset -n kube-system anetd cilium"* ]]; then
  printf "0\n"
  exit 0
fi
if [[ "$*" == *"get pods -n kube-system -l k8s-app=cilium --field-selector=status.phase=Running -o name"* ]]; then
  printf "pod/anetd-gke-node-a\n"
  exit 0
fi
if [[ "$*" == *"get daemonset -n kube-system kube-proxy"* ]]; then
  printf "0"
  exit 0
fi
exit 0
EOF
  chmod +x "${fake_dpv2_kubectl}"
  [[ "$(KUBECTL_CMD="${fake_dpv2_kubectl}" detect_s8_dataplane_mode)" == "dataplane-v2" ]] || die "Expected detect_s8_dataplane_mode == dataplane-v2 on GKE DPv2 (anetd DS desired=0, running k8s-app=cilium pod)"

  local fake_kp0_kubectl="${OUT_DIR}/fake-kp0-kubectl.sh"
  cat >"${fake_kp0_kubectl}" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"get daemonset -n kube-system kube-proxy"* ]]; then
  printf "0"
  exit 0
fi
if [[ "$*" == *"get pods -n kube-system"* ]]; then
  exit 0
fi
exit 0
EOF
  chmod +x "${fake_kp0_kubectl}"
  [[ "$(KUBECTL_CMD="${fake_kp0_kubectl}" detect_s8_dataplane_mode)" == "unknown" ]] || die "Expected detect_s8_dataplane_mode == unknown when daemonset/kube-proxy has desired=0 and neither cilium nor kube-proxy pods are running"

  local fake_gke_std_kubectl="${OUT_DIR}/fake-gke-std-kubectl.sh"
  cat >"${fake_gke_std_kubectl}" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"get pods -n kube-system -l component=kube-proxy --field-selector=status.phase=Running -o name"* ]]; then
  printf "pod/kube-proxy-gke-node-a\n"
  exit 0
fi
exit 0
EOF
  chmod +x "${fake_gke_std_kubectl}"
  [[ "$(KUBECTL_CMD="${fake_gke_std_kubectl}" detect_s8_dataplane_mode)" == "kube-proxy" ]] || die "Expected detect_s8_dataplane_mode == kube-proxy when running component=kube-proxy static pods exist"
  rm -f "${fake_dpv2_kubectl}" "${fake_kp0_kubectl}" "${fake_gke_std_kubectl}"

  # Verify tracked node cordon/uncordon only uncordons nodes cordoned during the run
  # and skips nodes that were already unschedulable prior to T2 (#82)
  local kubectl_log="${OUT_DIR}/kubectl-calls.log"
  local fake_kubectl="${OUT_DIR}/fake-kubectl.sh"
  cat >"${fake_kubectl}" <<EOF
#!/usr/bin/env bash
if [[ "\$*" == "get node pre-cordoned-node -o jsonpath={.spec.unschedulable}" ]]; then
  printf "true"
  exit 0
fi
if [[ "\$*" == "get node "* ]]; then
  exit 0
fi
if [[ "\$*" == "get nodes -l pool=runc -o jsonpath="* ]]; then
  printf "runc-pool-node-1\nrunc-pool-node-2\n"
  exit 0
fi
if [[ "\$*" == "get nodes -o jsonpath="* ]]; then
  printf "single-pool-node-1\n"
  exit 0
fi
echo "\$*" >> "${kubectl_log}"
EOF
  chmod +x "${fake_kubectl}"
  CORDONED_NODES=()
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_node "runc-pool-node-1"
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_node "runc-pool-node-2"
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_node "runc-pool-node-1"
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_node "pre-cordoned-node"
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" uncordon_all_tracked_nodes
  grep -q '^uncordon runc-pool-node-1$' "${kubectl_log}" || die "Expected tracked node runc-pool-node-1 to be uncordoned"
  grep -q '^uncordon runc-pool-node-2$' "${kubectl_log}" || die "Expected tracked node runc-pool-node-2 to be uncordoned"
  if grep -q '^uncordon pre-cordoned-node$' "${kubectl_log}"; then
    die "Expected pre-existing unschedulable node pre-cordoned-node NOT to be uncordoned"
  fi
  if grep -q 'sandbox.gke.io/runtime=gvisor' "${kubectl_log}"; then
    die "Expected uncordon_all_tracked_nodes not to hardcode sandbox.gke.io/runtime=gvisor"
  fi

  # Verify cordon_target_pool_nodes refuses NODE_SELECTOR=none without explicit --cordon-selector,
  # and honors explicit --cordon-selector <k=v> and --cordon-selector all
  if (KUBECTL_CMD="${fake_kubectl}" NODE_SELECTOR="none" CORDON_SELECTOR="" cordon_target_pool_nodes >/dev/null 2>&1); then
    die "Expected cordon_target_pool_nodes to fail closed when NODE_SELECTOR=none and CORDON_SELECTOR is unset"
  fi
  : >"${kubectl_log}"
  CORDONED_NODES=()
  KUBECTL_CMD="${fake_kubectl}" NODE_SELECTOR="none" CORDON_SELECTOR="pool=runc" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_target_pool_nodes >/dev/null
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" uncordon_all_tracked_nodes
  grep -q '^cordon runc-pool-node-1$' "${kubectl_log}" || die "Expected cordon_target_pool_nodes with CORDON_SELECTOR=pool=runc to cordon runc-pool-node-1"
  grep -q '^uncordon runc-pool-node-2$' "${kubectl_log}" || die "Expected cordon_target_pool_nodes with CORDON_SELECTOR=pool=runc to track and uncordon runc-pool-node-2"

  : >"${kubectl_log}"
  CORDONED_NODES=()
  KUBECTL_CMD="${fake_kubectl}" NODE_SELECTOR="none" CORDON_SELECTOR="all" T2_TEST_UNCONDITIONAL_UNCORDON="true" cordon_target_pool_nodes >/dev/null
  KUBECTL_CMD="${fake_kubectl}" T2_TEST_UNCONDITIONAL_UNCORDON="true" uncordon_all_tracked_nodes
  grep -q '^cordon single-pool-node-1$' "${kubectl_log}" || die "Expected cordon_target_pool_nodes with CORDON_SELECTOR=all to cordon single-pool-node-1"

  # Verify resolve_gcs_bucket reads .spec.snapshotStorageConfig.gcs.{bucket,path} on PodSnapshotStorageConfig
  # and fails closed (no personal bucket fallback) when no bucket is configured (#104)
  local fake_pssc_kubectl="${OUT_DIR}/fake-pssc-kubectl.sh"
  cat >"${fake_pssc_kubectl}" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"get podsnapshotstorageconfigs -o jsonpath={.items[0].spec.snapshotStorageConfig.gcs.bucket}"* ]]; then
  printf "custom-pssc-bucket"
  exit 0
fi
if [[ "$*" == *"get podsnapshotstorageconfigs -o jsonpath={.items[0].spec.snapshotStorageConfig.gcs.path}"* ]]; then
  printf "/custom-pssc-path"
  exit 0
fi
exit 0
EOF
  chmod +x "${fake_pssc_kubectl}"
  local resolved_pssc_bucket
  resolved_pssc_bucket="$(
    GCS_BUCKET=""
    SKIP_PODMIGRATION_CREATE="true"
    KUBECTL_CMD="${fake_pssc_kubectl}"
    resolve_gcs_bucket
    echo "${GCS_BUCKET}"
  )"
  if [[ "${resolved_pssc_bucket}" != "gs://custom-pssc-bucket/custom-pssc-path" ]]; then
    die "Expected resolve_gcs_bucket to resolve 'gs://custom-pssc-bucket/custom-pssc-path' from PodSnapshotStorageConfig, got '${resolved_pssc_bucket}'"
  fi

  local fake_empty_kubectl="${OUT_DIR}/fake-empty-kubectl.sh"
  cat >"${fake_empty_kubectl}" <<'EOF'
#!/usr/bin/env bash
if [[ "$*" == *"get podsnapshotpolicies"* ]]; then
  printf '{"items":[]}'
  exit 0
fi
exit 0
EOF
  chmod +x "${fake_empty_kubectl}"
  if (GCS_BUCKET="" SKIP_PODMIGRATION_CREATE="true" KUBECTL_CMD="${fake_empty_kubectl}" resolve_gcs_bucket >/dev/null 2>&1); then
    die "Expected resolve_gcs_bucket to fail closed when --skip-podmigration-create is set and no bucket is found"
  fi
  if (GCS_BUCKET="" SKIP_PODMIGRATION_CREATE="false" KUBECTL_CMD="${fake_empty_kubectl}" resolve_gcs_bucket >/dev/null 2>&1); then
    die "Expected resolve_gcs_bucket to fail closed when no bucket is passed or found on the cluster (#104)"
  fi

  # Verify preflight_snapshot_policy (#104):
  #   - fails when no PodSnapshotPolicy exists in the namespace
  #   - fails when PodSnapshotPolicy has empty status.conditions (never reconciled)
  #   - fails when the only PodSnapshotPolicy has Ready=False
  #   - passes when an active PodSnapshotPolicy with Ready=True exists
  if (KUBECTL_CMD="${fake_empty_kubectl}" preflight_snapshot_policy >/dev/null 2>&1); then
    die "Expected preflight_snapshot_policy to fail when no PodSnapshotPolicy exists in namespace"
  fi
  local fake_unreconciled_psp="${OUT_DIR}/fake-unreconciled-psp.sh"
  cat >"${fake_unreconciled_psp}" <<'EOF'
#!/usr/bin/env bash
printf '{"items":[{"metadata":{"name":"psp-unreconciled"},"status":{"conditions":[]}}]}'
EOF
  chmod +x "${fake_unreconciled_psp}"
  if (KUBECTL_CMD="${fake_unreconciled_psp}" preflight_snapshot_policy >/dev/null 2>&1); then
    die "Expected preflight_snapshot_policy to fail when PodSnapshotPolicy has empty status.conditions (never reconciled)"
  fi
  local fake_unready_psp="${OUT_DIR}/fake-unready-psp.sh"
  cat >"${fake_unready_psp}" <<'EOF'
#!/usr/bin/env bash
printf '{"items":[{"metadata":{"name":"psp-unready"},"status":{"conditions":[{"type":"Ready","status":"False","reason":"StorageNotReady"}]}}]}'
EOF
  chmod +x "${fake_unready_psp}"
  if (KUBECTL_CMD="${fake_unready_psp}" preflight_snapshot_policy >/dev/null 2>&1); then
    die "Expected preflight_snapshot_policy to fail when PodSnapshotPolicy has Ready=False"
  fi
  local fake_ready_psp="${OUT_DIR}/fake-ready-psp.sh"
  cat >"${fake_ready_psp}" <<'EOF'
#!/usr/bin/env bash
printf '{"items":[{"metadata":{"name":"psp-ready"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}]}'
EOF
  chmod +x "${fake_ready_psp}"
  KUBECTL_CMD="${fake_ready_psp}" preflight_snapshot_policy

  rm -f "${kubectl_log}" "${fake_kubectl}" "${fake_pssc_kubectl}" "${fake_empty_kubectl}" "${fake_unreconciled_psp}" "${fake_unready_psp}" "${fake_ready_psp}"

  log "Self-test PASSED: S1-S8 analyzed with 0 invariant violations, continuity failure modes (#103), S7 verdict-matches-data & preflight gates (#104), S8 phase-continuity checks (#149/#151/#131), runtime-agnostic templates, and report generated at ${report_html}"
}

# ==============================================================================
# Entrypoint Dispatch
# ==============================================================================

if [[ "${SELF_TEST}" == "true" ]]; then
  run_self_test
  exit 0
fi

build_pmprofiler
resolve_gcs_bucket

if [[ -n "${VERIFIER_ONLY}" ]]; then
  run_dir="${OUT_DIR}/verifier-${VERIFIER_ONLY}"
  mkdir -p "${run_dir}"
  case "${VERIFIER_ONLY}" in
    counter)
      pod="$(kubectl get pods -n "${NAMESPACE}" -l app=t2-counter -o jsonpath='{.items[0].metadata.name}')"
      state="$(v_counter_capture "${pod}")"
      sleep 2
      v_counter_verify "${run_dir}" "${pod}" "${state}"
      ;;
    redis)
      pod="t2-redis-0"
      nonce="nonce-$(date +%s)"
      v_redis_seed "${pod}" "${nonce}"
      v_redis_verify "${run_dir}" "${pod}" "${nonce}"
      ;;
    postgres)
      pod="t2-postgres-0"
      nonce="nonce-$(date +%s)"
      v_postgres_seed "${pod}" "${nonce}" 200
      v_postgres_verify "${run_dir}" "${pod}" "${nonce}" 200
      ;;
    *)
      die "Unknown --verifier-only target: ${VERIFIER_ONLY} (expected counter, redis, or postgres)"
      ;;
  esac
  exit 0
fi

case "${SCENARIO}" in
  S1|s1)
    run_scenario_s1
    ;;
  S2|s2)
    run_scenario_s2
    ;;
  S3|s3)
    run_scenario_s3
    ;;
  S4|s4)
    run_scenario_s4
    ;;
  S5|s5)
    run_scenario_s5
    ;;
  S6|s6)
    run_scenario_s6
    ;;
  S7|s7)
    run_scenario_s7
    ;;
  S8|s8)
    run_scenario_s8
    ;;
  all|ALL)
    run_scenario_s1
    run_scenario_s2
    run_scenario_s3
    run_scenario_s4
    run_scenario_s5
    run_scenario_s6
    run_scenario_s7
    run_scenario_s8
    "${PMPROFILER_BIN}" report \
      --out "${OUT_DIR}/guardrail-t2-report.html" \
      --title "GKE Live Pod Migration — T2 Guardrail Scenarios (S1–S8)" \
      "${OUT_DIR}"/s*/run.json
    log "All T2 scenarios (S1-S8) PASSED. Report: ${OUT_DIR}/guardrail-t2-report.html"
    ;;
  *)
    die "Unknown --scenario value: ${SCENARIO} (expected S1, S2, S3, S4, S5, S6, S7, S8, or all)"
    ;;
esac

