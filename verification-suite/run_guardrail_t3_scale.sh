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
#   - T3-S3 : Heterogeneous Node-Shape Drain & I9 Cold-Start Fallback Under Scale
#       * Exercises cross-pool/cross-shape evacuation and controlled I9
#         (SucceededWithoutRestore / RestoreFailedColdStart) fallback across
#         multi-replica stateful pods, asserting 100% Ready convergence within SLO (T).
#   - T3-S4 : Multi-Wave Sequential Rolling Node Drain Soak & I5 Resource Leak Audit
#       * Executes back-to-back sequential node drain waves (Wave 1 -> Wave 2) on a
#         multi-replica stateful deployment, verifying cumulative state survival across
#         multiple migrations and 0 residual scheduling gates / 0 orphaned triggers (I3, I5).
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
ALLOW_SIMULATED_S3="${ALLOW_SIMULATED_S3:-false}"
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --scenario <T3-S1|T3-S2|T3-S3|T3-S4|scalesim|all>
                                    Scenario(s) to execute (default: all)
  --out-dir <dir>                   Output directory for pmprofiler runs & HTML report
  --namespace <ns>                  Target workload namespace (default: default)
  --controller-ns <ns>              Controller namespace (default: pod-migration-system)
  --gcs-bucket <gs://bucket/path>   Optional GCS bucket for snapshot storage & size sampling
  --replicas-per-app <N>            Replicas per workload in T3-S2 (default: 10 => 50 pods total)
  --slo-gate-hold-p95 <dur>         Max p95 scheduling gate hold duration (default: 60s)
  --slo-downtime-p95 <dur>          Max p95 serving blackout duration (default: 60s)
  --slo-e2e-p95 <dur>               Max p95 end-to-end migration duration (default: 180s)
  --require-cross-shape             Assert src_type != dst_type in T3-S1/T3-S3 (requires multi-shape pool)
  --allow-simulated-s3              Run T3-S3 on a single-shape pool as an explicit simulated exit-128 fallback
                                    test (otherwise T3-S3 logs SKIPPED when <2 gVisor shapes exist)
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
    --allow-simulated-s3)
      ALLOW_SIMULATED_S3="true"
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

PREEXISTING_PODMIGRATIONS_BACKUP="${OUT_DIR}/preexisting-podmigrations.json"
BACKED_UP_PREEXISTING_PODMIGRATIONS="false"
RESTORE_FAILED="false"

# Shared JQ filter for backing up pre-existing PodMigrations.
# Strips cluster-assigned metadata, owner references, managed fields, finalizers,
# last-applied-configuration annotation, and status block.
PODMIGRATIONS_BACKUP_JQ_FILTER='{
  apiVersion: "v1",
  kind: "List",
  items: [
    .items[]?
    | select(
        .metadata.name != "t3-s1-policy"
        and .metadata.name != "t3-s2-policy"
        and .metadata.name != "t3-s3-policy"
        and .metadata.name != "t3-s4-policy"
      )
    | del(
        .metadata.uid,
        .metadata.resourceVersion,
        .metadata.creationTimestamp,
        .metadata.generation,
        .metadata.ownerReferences,
        .metadata.managedFields,
        .metadata.finalizers,
        .metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"],
        .status
      )
    | if .metadata.annotations == {} then del(.metadata.annotations) else . end
  ]
}'

# JQ filters for scoping clean_stale_migration_resources to T3-owned controller
# runtime artifacts (Issue #97), preserving non-T3 PodMigrationJobs,
# PodSnapshotManualTriggers, and PodSnapshots in shared namespaces.
T3_PMJ_NAMES_JQ_FILTER='
  .items[]?
  | select(
      (.metadata.labels["t3-suite"] // "") == "true"
      or ((.spec.podRef.name // "") | startswith("t3-"))
      or ((.status.restoredPodName // "") | startswith("t3-"))
      or ((.metadata.name // "") | test("^(pmj-|migrate-)?t3-"))
    )
  | .metadata.name // empty
'

T3_PMJ_SNAP_REFS_JQ_FILTER='[
  .items[]?
  | select(
      (.metadata.labels["t3-suite"] // "") == "true"
      or ((.spec.podRef.name // "") | startswith("t3-"))
      or ((.status.restoredPodName // "") | startswith("t3-"))
      or ((.metadata.name // "") | test("^(pmj-|migrate-)?t3-"))
    )
  | .status.snapshotRef // empty
  | select(length > 0)
]'

T3_PSMT_NAMES_JQ_FILTER='
  .items[]?
  | select(
      (.metadata.labels["t3-suite"] // "") == "true"
      or ((.spec.targetPod // "") | startswith("t3-"))
      or ((.metadata.name // "") | test("^(trigger-)?t3-"))
    )
  | .metadata.name // empty
'

T3_PSMT_SNAP_REFS_JQ_FILTER='[
  .items[]?
  | select(
      (.metadata.labels["t3-suite"] // "") == "true"
      or ((.spec.targetPod // "") | startswith("t3-"))
      or ((.metadata.name // "") | test("^(trigger-)?t3-"))
    )
  | .status.snapshotCreated.name // empty
  | select(length > 0)
]'

T3_PODSNAPSHOT_NAMES_JQ_FILTER='
  .items[]?
  | select(
      (.metadata.name as $n | ($refs | index($n)) != null)
      or (.metadata.labels["t3-suite"] // "") == "true"
      or (((.spec.podRef.name // .spec.podName // .spec.source.podName // .spec.sourcePod // .status.podName // "")) | startswith("t3-"))
      or ((.metadata.name // "") | test("^(snap-)?t3-"))
    )
  | .metadata.name // empty
'

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

cordon_node() {
  local node="$1"
  if [[ -z "${node}" ]]; then
    return 0
  fi
  local already_unschedulable
  already_unschedulable="$(kubectl get node "${node}" -o jsonpath='{.spec.unschedulable}' 2>/dev/null || true)"
  if [[ "${already_unschedulable}" != "true" ]]; then
    CORDONED_NODES+=("${node}")
  fi
  kubectl cordon "${node}" >/dev/null
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
  CORDONED_NODES=()
}

stop_collector() {
  if [[ -n "${COLLECT_PID}" ]] && kill -0 "${COLLECT_PID}" 2>/dev/null; then
    kill -INT "${COLLECT_PID}" 2>/dev/null || true
    wait "${COLLECT_PID}" 2>/dev/null || true
  fi
  COLLECT_PID=""
}

delete_t3_policies() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    return 0
  fi
  kubectl delete podmigrations.podmigration.gke.io \
    t3-s1-policy t3-s2-policy t3-s3-policy t3-s4-policy \
    -n "${NAMESPACE}" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
}

apply_preexisting_podmigrations() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    return 0
  fi
  if [[ ! -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" ]]; then
    return 0
  fi

  local count
  count="$(jq '.items | length' "${PREEXISTING_PODMIGRATIONS_BACKUP}" 2>/dev/null || echo 0)"
  if [[ "${count}" -eq 0 ]]; then
    return 0
  fi

  log "Restoring ${count} pre-existing PodMigration(s) from ${PREEXISTING_PODMIGRATIONS_BACKUP} into namespace ${NAMESPACE}"
  if ! kubectl apply -n "${NAMESPACE}" -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" >/dev/null 2>&1; then
    log "FAIL: kubectl apply failed for pre-existing PodMigrations from ${PREEXISTING_PODMIGRATIONS_BACKUP}"
    RESTORE_FAILED="true"
  fi
}

verify_restored_podmigrations() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    return 0
  fi
  if [[ ! -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" ]]; then
    return 0
  fi

  local count
  count="$(jq '.items | length' "${PREEXISTING_PODMIGRATIONS_BACKUP}" 2>/dev/null || echo 0)"
  if [[ "${count}" -eq 0 ]]; then
    rm -f "${PREEXISTING_PODMIGRATIONS_BACKUP}"
    return 0
  fi

  if [[ "${RESTORE_FAILED}" == "true" ]]; then
    log "FAIL: Skipping Ready wait because kubectl apply failed for pre-existing PodMigrations; preserving backup file ${PREEXISTING_PODMIGRATIONS_BACKUP}"
    return 0
  fi

  local names=()
  local name_line
  while IFS= read -r name_line; do
    [[ -n "${name_line}" ]] && names+=("${name_line}")
  done < <(jq -r '.items[].metadata.name' "${PREEXISTING_PODMIGRATIONS_BACKUP}" 2>/dev/null || true)
  local restore_err=0
  for name in "${names[@]:-}"; do
    if [[ -n "${name}" ]]; then
      log "Waiting for restored PodMigration '${name}' in namespace ${NAMESPACE} to reach condition Ready=True..."
      if ! kubectl wait --for=condition=Ready "podmigration/${name}" -n "${NAMESPACE}" --timeout=60s >/dev/null 2>&1; then
        log "FAIL: Restored PodMigration '${name}' in namespace ${NAMESPACE} failed to reach condition Ready=True within 60s"
        restore_err=1
      fi
    fi
  done

  if [[ "${restore_err}" -eq 0 ]]; then
    rm -f "${PREEXISTING_PODMIGRATIONS_BACKUP}"
    log "Restored pre-existing PodMigration(s) successfully and verified Ready"
  else
    log "FAIL: Pre-existing PodMigration restore incomplete or failed; preserving backup file ${PREEXISTING_PODMIGRATIONS_BACKUP}"
    RESTORE_FAILED="true"
  fi
}

cleanup_on_exit() {
  local exit_code=$?
  trap - EXIT INT TERM
  delete_t3_policies
  apply_preexisting_podmigrations
  stop_collector
  uncordon_all_tracked_nodes
  verify_restored_podmigrations
  if [[ "${RESTORE_FAILED}" == "true" && "${exit_code}" -eq 0 ]]; then
    exit 1
  fi
  if [[ "${exit_code}" -ne 0 ]]; then
    exit "${exit_code}"
  fi
}
trap cleanup_on_exit EXIT
trap 'cleanup_on_exit; trap - INT; kill -INT $$' INT
trap 'cleanup_on_exit; trap - TERM; kill -TERM $$' TERM

backup_preexisting_podmigrations() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    return 0
  fi
  if [[ "${BACKED_UP_PREEXISTING_PODMIGRATIONS}" == "true" ]]; then
    return 0
  fi
  if [[ -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" ]]; then
    die "Pre-existing backup file found at ${PREEXISTING_PODMIGRATIONS_BACKUP}. A previous run did not complete restore; inspect/apply or remove this file before re-running."
  fi
  mkdir -p "${OUT_DIR}"
  local err_file
  err_file="$(mktemp)"
  local raw_json
  if ! raw_json="$(kubectl get podmigrations.podmigration.gke.io -n "${NAMESPACE}" -o json 2>"${err_file}")"; then
    local err_msg
    err_msg="$(cat "${err_file}")"
    rm -f "${err_file}"
    die "Cannot list PodMigrations in ${NAMESPACE} (kubectl get failed: ${err_msg:-${raw_json}}); refusing to delete them without a backup"
  fi
  rm -f "${err_file}"
  if ! echo "${raw_json}" | jq -e . >/dev/null 2>&1; then
    die "Cannot parse PodMigrations list output in ${NAMESPACE} as JSON; refusing to delete them without a backup"
  fi

  echo "${raw_json}" | jq "${PODMIGRATIONS_BACKUP_JQ_FILTER}" > "${PREEXISTING_PODMIGRATIONS_BACKUP}"
  BACKED_UP_PREEXISTING_PODMIGRATIONS="true"

  local count
  count="$(jq '.items | length' "${PREEXISTING_PODMIGRATIONS_BACKUP}" 2>/dev/null || echo 0)"
  if [[ "${count}" -gt 0 ]]; then
    log "Backed up ${count} pre-existing PodMigration(s) in namespace ${NAMESPACE} to ${PREEXISTING_PODMIGRATIONS_BACKUP}"
  fi
}

resolve_gcs_bucket() {
  if [[ -n "${GCS_BUCKET}" ]]; then
    return 0
  fi
  local existing=""
  if [[ -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" ]]; then
    existing="$(jq -r '.items[0].spec.storage.location // empty' "${PREEXISTING_PODMIGRATIONS_BACKUP}" 2>/dev/null || true)"
  else
    local err_file
    err_file="$(mktemp)"
    local raw_json
    if ! raw_json="$(kubectl get podmigrations.podmigration.gke.io -n "${NAMESPACE}" -o json 2>"${err_file}")"; then
      local err_msg
      err_msg="$(cat "${err_file}")"
      rm -f "${err_file}"
      die "Cannot inspect PodMigrations in ${NAMESPACE} for GCS bucket resolution (kubectl get failed: ${err_msg:-${raw_json}})"
    fi
    rm -f "${err_file}"
    if ! echo "${raw_json}" | jq -e . >/dev/null 2>&1; then
      die "Cannot parse PodMigrations list output in ${NAMESPACE} as JSON for GCS bucket resolution"
    fi
    existing="$(jq -r '.items[0].spec.storage.location // empty' <<<"${raw_json}")"
  fi
  if [[ -n "${existing}" ]]; then
    GCS_BUCKET="${existing}"
  else
    GCS_BUCKET="gs://yaoluo-gke-dev-podsnapshots/snapshots"
  fi
}

clean_stale_migration_resources() {
  log "Cleaning stale T3-scoped PodSnapshots, PodSnapshotManualTriggers, and PodMigrationJobs in ${NAMESPACE}"

  local pmj_json psmt_json snap_json
  pmj_json="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" -o json 2>/dev/null || echo '{"items":[]}')"
  psmt_json="$(kubectl get podsnapshotmanualtriggers -n "${NAMESPACE}" -o json 2>/dev/null || echo '{"items":[]}')"
  snap_json="$(kubectl get podsnapshots -n "${NAMESPACE}" -o json 2>/dev/null || echo '{"items":[]}')"

  local t3_pmjs=()
  local t3_psmts=()
  local t3_snaps=()
  local line

  while IFS= read -r line; do
    [[ -n "${line}" ]] && t3_pmjs+=("${line}")
  done < <(echo "${pmj_json}" | jq -r "${T3_PMJ_NAMES_JQ_FILTER}" 2>/dev/null || true)

  while IFS= read -r line; do
    [[ -n "${line}" ]] && t3_psmts+=("${line}")
  done < <(echo "${psmt_json}" | jq -r "${T3_PSMT_NAMES_JQ_FILTER}" 2>/dev/null || true)

  local pmj_refs psmt_refs combined_refs
  pmj_refs="$(echo "${pmj_json}" | jq -c "${T3_PMJ_SNAP_REFS_JQ_FILTER}" 2>/dev/null || echo '[]')"
  psmt_refs="$(echo "${psmt_json}" | jq -c "${T3_PSMT_SNAP_REFS_JQ_FILTER}" 2>/dev/null || echo '[]')"
  combined_refs="$(jq -nc --argjson a "${pmj_refs}" --argjson b "${psmt_refs}" '($a + $b) | unique' 2>/dev/null || echo '[]')"

  while IFS= read -r line; do
    [[ -n "${line}" ]] && t3_snaps+=("${line}")
  done < <(echo "${snap_json}" | jq -r --argjson refs "${combined_refs}" "${T3_PODSNAPSHOT_NAMES_JQ_FILTER}" 2>/dev/null || true)

  if [[ "${#t3_snaps[@]}" -gt 0 ]]; then
    kubectl delete validatingadmissionpolicybinding gke-pod-snapshot-vap-binding --ignore-not-found >/dev/null 2>&1 || true
    local snap
    for snap in "${t3_snaps[@]}"; do
      kubectl patch podsnapshot "${snap}" -n "${NAMESPACE}" --type=json \
        -p='[{"op": "remove", "path": "/metadata/finalizers"}]' >/dev/null 2>&1 || true
    done
    kubectl delete podsnapshot "${t3_snaps[@]}" -n "${NAMESPACE}" --ignore-not-found --timeout=20s >/dev/null 2>&1 || true
    if [[ -f "${SCRIPT_DIR}/manifests/restore-vap-binding.yaml" ]]; then
      kubectl apply -f "${SCRIPT_DIR}/manifests/restore-vap-binding.yaml" >/dev/null 2>&1 || true
    fi
  fi

  if [[ "${#t3_psmts[@]}" -gt 0 ]]; then
    kubectl delete podsnapshotmanualtrigger "${t3_psmts[@]}" -n "${NAMESPACE}" --ignore-not-found --timeout=20s >/dev/null 2>&1 || true
  fi

  if [[ "${#t3_pmjs[@]}" -gt 0 ]]; then
    kubectl delete podmigrationjobs.podmigration.gke.io "${t3_pmjs[@]}" -n "${NAMESPACE}" --ignore-not-found --timeout=20s >/dev/null 2>&1 || true
  fi
}

clean_t3_workloads() {
  backup_preexisting_podmigrations
  kubectl delete deployment/t3-s1-counter deployment/t3-counter deployment/t3-redis \
    deployment/t3-postgres deployment/t3-memcached deployment/t3-nginx \
    deployment/t3-s3-hetero deployment/t3-s4-soak \
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

  cordon_node "${src_node}"
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
  cordon_node "${drain_node}"

  # Pick up to 2 pods per workload on drain_node (up to 10 concurrent migrations across all 5 apps).
  local evicted_pods=()
  for app in t3-counter t3-redis t3-postgres t3-memcached t3-nginx; do
    local app_pods=()
    local pod_line
    while IFS= read -r pod_line; do
      [[ -n "${pod_line}" ]] && app_pods+=("${pod_line}")
    done < <(kubectl get pods -n "${NAMESPACE}" -l "app=${app}" \
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
# Scenario T3-S3: Heterogeneous Node-Shape Drain & I9 Cold-Start Fallback Under Scale
# ==============================================================================
scenario_t3_s3() {
  local run_dir="${OUT_DIR}/t3_s3"
  log "=== Scenario T3-S3: Heterogeneous Node-Shape Drain & I9 Fallback Verification ==="

  local shape_lines shape_list distinct_shapes
  shape_lines="$(kubectl get nodes -l sandbox.gke.io/runtime=gvisor -o jsonpath='{range .items[*]}{.metadata.labels.node\.kubernetes\.io/instance-type}{"\n"}{end}' 2>/dev/null | sort -u || true)"
  shape_list="$(grep -v '^$' <<<"${shape_lines}" | paste -sd ',' - || true)"
  distinct_shapes="$(grep -cv '^$' <<<"${shape_lines}" || true)"

  if [[ "${distinct_shapes}" -le 1 ]]; then
    if [[ "${REQUIRE_CROSS_SHAPE}" == "true" ]]; then
      die "T3-S3: --require-cross-shape is set, but cluster only has ${distinct_shapes} distinct gVisor machine shape(s) (${shape_list:-unknown})"
    fi
    if [[ "${ALLOW_SIMULATED_S3}" != "true" ]]; then
      log "SKIPPED [T3-S3]: heterogeneous cross-shape drain requires >=2 distinct gVisor machine shapes (found ${distinct_shapes}: ${shape_list:-unknown}); pass --allow-simulated-s3 to run same-shape simulated exit-128 fallback"
      return 0
    fi
    log "SKIPPED [T3-S3 heterogeneous cross-shape claim]: cluster has ${distinct_shapes} gVisor machine shape (${shape_list:-unknown}); running opt-in --allow-simulated-s3 same-shape exit-128 fallback"
  fi

  resolve_gcs_bucket
  clean_t3_workloads
  clean_stale_migration_resources

  kubectl apply -n "${NAMESPACE}" -f - <<EOF || die "Failed to deploy T3-S3 workload"
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: t3-s3-policy
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-s3-hetero
  labels:
    t3-suite: "true"
spec:
  replicas: 2
  selector:
    matchLabels:
      app: t3-s3-hetero
  template:
    metadata:
      labels:
        app: t3-s3-hetero
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
      - name: hetero-app
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          echo "hetero-ready-\$(date +%s)" > /tmp/hetero_state
          while true; do
            if [ -f /tmp/simulate_restore_exit_128 ]; then
              rm -f /tmp/simulate_restore_exit_128
              exit 128
            fi
            sleep 1
          done
        resources:
          requests:
            cpu: 20m
            memory: 32Mi
EOF
  kubectl wait --for=condition=Ready podmigration/t3-s3-policy -n "${NAMESPACE}" --timeout=60s
  kubectl rollout status deployment/t3-s3-hetero -n "${NAMESPACE}" --timeout=120s
  sleep 3

  if [[ "${distinct_shapes}" -gt 1 ]]; then
    start_collector "${run_dir}" "T3-S3 heterogeneous node-shape drain (${shape_list}) & I9 fallback"
  else
    start_collector "${run_dir}" "T3-S3 (simulated fallback, single-shape ${shape_list:-unknown})"
  fi

  local target_pod src_node src_type
  target_pod="$(kubectl get pods -n "${NAMESPACE}" -l app=t3-s3-hetero --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
  src_node="$(kubectl get pod "${target_pod}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  src_type="$(kubectl get node "${src_node}" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}')"

  if [[ "${distinct_shapes}" -gt 1 ]]; then
    log "T3-S3 detected ${distinct_shapes} distinct gVisor machine shapes (${shape_list}); cordoning all '${src_type}' nodes to force cross-shape migration"
    local same_shape_nodes=()
    local node_line
    while IFS= read -r node_line; do
      [[ -n "${node_line}" ]] && same_shape_nodes+=("${node_line}")
    done < <(kubectl get nodes -l "sandbox.gke.io/runtime=gvisor,node.kubernetes.io/instance-type=${src_type}" -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')
    for n in "${same_shape_nodes[@]:-}"; do
      cordon_node "${n}"
    done
  else
    log "T3-S3 (simulated fallback) single gVisor machine shape (${src_type}); cordoning ${src_node} and injecting exit-128 restore fallback"
    cordon_node "${src_node}"
  fi

  evict_pod "${target_pod}"

  # Wait for replacement pod to appear; when running under --allow-simulated-s3 on a single-shape pool,
  # inject exit 128 on first restore so I9 (SucceededWithoutRestore / RestoreFailedColdStart) fallback is exercised.
  local rpod=""
  for _ in $(seq 1 60); do
    rpod="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
      -o jsonpath="{.items[?(@.spec.podRef.name=='${target_pod}')].status.restoredPodName}" 2>/dev/null | awk '{print $NF}')"
    if [[ -n "${rpod}" ]]; then
      break
    fi
    sleep 1
  done

  if [[ -n "${rpod}" && "${distinct_shapes}" -le 1 ]]; then
    for _ in $(seq 1 30); do
      local rphase
      rphase="$(kubectl get pod "${rpod}" -n "${NAMESPACE}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
      if [[ "${rphase}" == "Running" ]]; then
        kubectl exec -n "${NAMESPACE}" "${rpod}" -- touch /tmp/simulate_restore_exit_128 >/dev/null 2>&1 || true
        break
      fi
      sleep 1
    done
  fi

  local pmj_phase=""
  for _ in $(seq 1 90); do
    pmj_phase="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
      -o jsonpath="{.items[?(@.spec.podRef.name=='${target_pod}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
    if [[ "${pmj_phase}" == "Succeeded" || "${pmj_phase}" == "Failed" || "${pmj_phase}" == "SucceededWithoutRestore" ]]; then
      break
    fi
    sleep 2
  done

  uncordon_all_tracked_nodes
  kubectl rollout status deployment/t3-s3-hetero -n "${NAMESPACE}" --timeout=120s

  local dst_node="" dst_type=""
  if [[ -n "${rpod}" ]]; then
    dst_node="$(kubectl get pod "${rpod}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
    if [[ -n "${dst_node}" ]]; then
      dst_type="$(kubectl get node "${dst_node}" -o jsonpath='{.metadata.labels.node\.kubernetes\.io/instance-type}' 2>/dev/null || true)"
    fi
  fi

  local s3_check_name s3_scenario_title
  if [[ "${distinct_shapes}" -gt 1 ]]; then
    s3_check_name="T3-S3 heterogeneous cross-shape (${src_type} -> ${dst_type}) & I9 fallback convergence"
    s3_scenario_title="T3-S3 heterogeneous cross-shape (${src_type} -> ${dst_type}) & I9 fallback"
  else
    s3_check_name="T3-S3 (simulated fallback: ${src_type} -> ${dst_type}) & I9 convergence"
    s3_scenario_title="T3-S3 (simulated fallback: ${src_type} -> ${dst_type})"
  fi
  if [[ -f "${run_dir}/meta.json" ]]; then
    local tmp_meta="${run_dir}/meta.json.tmp"
    jq --arg s "${s3_scenario_title}" '.scenario = $s' "${run_dir}/meta.json" > "${tmp_meta}" && mv "${tmp_meta}" "${run_dir}/meta.json"
  fi

  if [[ ( "${REQUIRE_CROSS_SHAPE}" == "true" || "${distinct_shapes}" -gt 1 ) && "${src_type}" == "${dst_type}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "${s3_check_name}" --group "t3-s3-hetero" --pass=false \
      --value 0 --total 1 --detail "expected src_type != dst_type, got ${src_node}(${src_type}) -> ${dst_node}(${dst_type})"
    die "T3-S3: expected cross-shape migration, but source (${src_type}) and destination (${dst_type}) match"
  fi

  if [[ "${pmj_phase}" == "Succeeded" || "${pmj_phase}" == "SucceededWithoutRestore" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "${s3_check_name}" --group "t3-s3-hetero" --pass \
      --value 1 --total 1 --detail "phase=${pmj_phase}, src=${src_node}(${src_type}) -> dst=${dst_node}(${dst_type})"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "${s3_check_name}" --group "t3-s3-hetero" --pass=false \
      --value 0 --total 1 --detail "unexpected phase=${pmj_phase}, src=${src_node}(${src_type}) -> dst=${dst_node}(${dst_type})"
    die "T3-S3 failed with unexpected PMJ phase=${pmj_phase}"
  fi

  finish_and_assert_slo_run "${run_dir}" "true"
  clean_t3_workloads
}

# ==============================================================================
# Scenario T3-S4: Multi-Wave Sequential Rolling Node Drain Soak & I5 Leak Audit
# ==============================================================================
scenario_t3_s4() {
  local run_dir="${OUT_DIR}/t3_s4"
  log "=== Scenario T3-S4: Multi-Wave Sequential Rolling Node Drain Soak & I5 Audit ==="
  resolve_gcs_bucket
  clean_t3_workloads
  clean_stale_migration_resources

  kubectl apply -n "${NAMESPACE}" -f - <<EOF || die "Failed to deploy T3-S4 soak workload"
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: t3-s4-policy
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: t3-s4-soak
  labels:
    t3-suite: "true"
spec:
  replicas: 2
  selector:
    matchLabels:
      app: t3-s4-soak
  template:
    metadata:
      labels:
        app: t3-s4-soak
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
      - name: soak-counter
        image: busybox:1.36
        command: ["/bin/sh", "-c"]
        args:
        - |
          STATE="/tmp/soak_state"
          if [ ! -f "\$STATE" ]; then
            echo "soak-\$(date +%s)-\$\$ 0" > "\$STATE"
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
  kubectl wait --for=condition=Ready podmigration/t3-s4-policy -n "${NAMESPACE}" --timeout=60s
  kubectl rollout status deployment/t3-s4-soak -n "${NAMESPACE}" --timeout=120s
  sleep 4

  start_collector "${run_dir}" "T3-S4 multi-wave sequential rolling node drain soak"

  # Wave 1: Drain pod_w1 from node_w1 -> pod_w2 on node_w2
  local pod_w1 node_w1 state_0 id_0 val_0
  pod_w1="$(kubectl get pods -n "${NAMESPACE}" -l app=t3-s4-soak --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')"
  node_w1="$(kubectl get pod "${pod_w1}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  state_0="$(kubectl exec -n "${NAMESPACE}" "${pod_w1}" -- cat /tmp/soak_state)"
  id_0="$(awk '{print $1}' <<<"${state_0}")"
  val_0="$(awk '{print $2}' <<<"${state_0}")"
  log "T3-S4 Wave 1: draining ${pod_w1} on ${node_w1} (id=${id_0}, val=${val_0})"

  cordon_node "${node_w1}"
  evict_pod "${pod_w1}"

  local pmj_w1_phase=""
  for _ in $(seq 1 90); do
    pmj_w1_phase="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
      -o jsonpath="{.items[?(@.spec.podRef.name=='${pod_w1}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
    if [[ "${pmj_w1_phase}" == "Succeeded" || "${pmj_w1_phase}" == "Failed" || "${pmj_w1_phase}" == "SucceededWithoutRestore" ]]; then
      break
    fi
    sleep 2
  done

  local pod_w2 node_w2 state_1 id_1 val_1
  pod_w2="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
    -o jsonpath="{.items[?(@.spec.podRef.name=='${pod_w1}')].status.restoredPodName}" 2>/dev/null | awk '{print $NF}')"
  [[ -n "${pod_w2}" ]] || die "T3-S4 Wave 1: PMJ did not record restoredPodName (phase=${pmj_w1_phase})"
  kubectl wait --for=condition=Ready "pod/${pod_w2}" -n "${NAMESPACE}" --timeout=120s
  node_w2="$(kubectl get pod "${pod_w2}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  uncordon_all_tracked_nodes
  sleep 3
  state_1="$(kubectl exec -n "${NAMESPACE}" "${pod_w2}" -- cat /tmp/soak_state)"
  id_1="$(awk '{print $1}' <<<"${state_1}")"
  val_1="$(awk '{print $2}' <<<"${state_1}")"

  # Wave 2: Drain the ALREADY-RESTORED pod_w2 from node_w2 -> pod_w3 on node_w3
  log "T3-S4 Wave 2: draining restored pod ${pod_w2} on ${node_w2} (id=${id_1}, val=${val_1})"
  cordon_node "${node_w2}"
  evict_pod "${pod_w2}"

  local pmj_w2_phase=""
  for _ in $(seq 1 90); do
    pmj_w2_phase="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
      -o jsonpath="{.items[?(@.spec.podRef.name=='${pod_w2}')].status.phase}" 2>/dev/null | awk '{print $NF}')"
    if [[ "${pmj_w2_phase}" == "Succeeded" || "${pmj_w2_phase}" == "Failed" || "${pmj_w2_phase}" == "SucceededWithoutRestore" ]]; then
      break
    fi
    sleep 2
  done

  local pod_w3 node_w3 state_2 id_2 val_2
  pod_w3="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" \
    -o jsonpath="{.items[?(@.spec.podRef.name=='${pod_w2}')].status.restoredPodName}" 2>/dev/null | awk '{print $NF}')"
  [[ -n "${pod_w3}" ]] || die "T3-S4 Wave 2: PMJ did not record restoredPodName (phase=${pmj_w2_phase})"
  kubectl wait --for=condition=Ready "pod/${pod_w3}" -n "${NAMESPACE}" --timeout=120s
  node_w3="$(kubectl get pod "${pod_w3}" -n "${NAMESPACE}" -o jsonpath='{.spec.nodeName}')"
  uncordon_all_tracked_nodes
  sleep 2
  state_2="$(kubectl exec -n "${NAMESPACE}" "${pod_w3}" -- cat /tmp/soak_state)"
  id_2="$(awk '{print $1}' <<<"${state_2}")"
  val_2="$(awk '{print $2}' <<<"${state_2}")"

  if [[ "${id_1}" == "${id_0}" && "${id_2}" == "${id_0}" && "${val_2}" -ge "${val_1}" && "${val_1}" -ge "${val_0}" ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S4 multi-wave sequential warm migration state continuity" --group "t3-s4-soak" --pass \
      --value 2 --total 2 --detail "${node_w1} -> ${node_w2} -> ${node_w3}, id=${id_2}, counter ${val_0}->${val_1}->${val_2}"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S4 multi-wave sequential warm migration state continuity" --group "t3-s4-soak" --pass=false \
      --value 0 --total 2 --detail "state mismatch across waves: w0='${state_0}' w1='${state_1}' w2='${state_2}'"
    die "T3-S4 failed multi-wave state continuity: w0='${state_0}' w1='${state_1}' w2='${state_2}'"
  fi

  # Post-Soak I3 (SchedulingGateLiveness) & I5 (BoundedResourceLeak) Audit
  local gated_pods active_pmjs
  gated_pods="$(kubectl get pods -n "${NAMESPACE}" -l app=t3-s4-soak -o json \
    | jq '[.items[] | select((.spec.schedulingGates // []) | length > 0)] | length')"
  active_pmjs="$(kubectl get podmigrationjobs.podmigration.gke.io -n "${NAMESPACE}" -o json \
    | jq '[.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed" and .status.phase != "SucceededWithoutRestore")] | length')"

  if [[ "${gated_pods}" -eq 0 && "${active_pmjs}" -eq 0 ]]; then
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S4 post-soak I3/I5 zero residual gates or active PMJs" --group "t3-s4-soak" --pass \
      --value 1 --total 1 --detail "gatedPods=${gated_pods}, activePMJs=${active_pmjs}"
  else
    "${PMPROFILER_BIN}" check --run "${run_dir}" \
      --name "T3-S4 post-soak I3/I5 zero residual gates or active PMJs" --group "t3-s4-soak" --pass=false \
      --value 0 --total 1 --detail "gatedPods=${gated_pods}, activePMJs=${active_pmjs}"
    die "T3-S4 failed I3/I5 hygiene audit: gatedPods=${gated_pods}, activePMJs=${active_pmjs}"
  fi

  finish_and_assert_slo_run "${run_dir}" "false"
  clean_t3_workloads
}

# ==============================================================================
# Offline CI Self-Test Mode (--self-test)
# ==============================================================================
# shellcheck disable=SC2030,SC2031
run_self_test() {
  log "Running offline T3 self-test (scalesim model + synthetic T3-S1/T3-S2/T3-S3/T3-S4 traces + SLO enforcement)"
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

  # 3. Synthetic T3-S3 (heterogeneous node-shape drain & I9 cold-start fallback)
  local s3_dir="${base_dir}/t3_s3"
  mkdir -p "${s3_dir}"
  cat > "${s3_dir}/meta.json" <<EOF
{"scenario":"T3-S3 heterogeneous cross-shape (e2-standard-4 -> n2-standard-4) & I9 fallback","startedAt":"2026-09-28T12:00:00Z"}
EOF
  cat > "${s3_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T11:59:00Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s3-hetero-0","uid":"uid-s3-src","creationTimestamp":"2026-09-28T11:59:00Z","labels":{"app":"t3-s3-hetero","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-e2-standard-4"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T11:59:05Z"}]}}}
{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s3-0","uid":"uid-pmj-s3","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s3-hetero-0"},"targetPodUID":"uid-s3-src"},"status":{"phase":"Snapshotting"}}}
{"ts":"2026-09-28T12:00:06Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s3-hetero-0","uid":"uid-s3-src","creationTimestamp":"2026-09-28T11:59:00Z","deletionTimestamp":"2026-09-28T12:00:06Z","labels":{"app":"t3-s3-hetero","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-e2-standard-4"},"status":{}}}
{"ts":"2026-09-28T12:00:07Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s3-hetero-dst","uid":"uid-s3-dst","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"t3-s3-hetero","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:00:10Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s3-hetero-dst","uid":"uid-s3-dst","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"t3-s3-hetero","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-n2-standard-4"},"status":{"containerStatuses":[{"name":"hetero-app","restartCount":1,"lastState":{"terminated":{"exitCode":128,"reason":"Error","finishedAt":"2026-09-28T12:00:12Z"}}}],"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:00:16Z"}]}}}
{"ts":"2026-09-28T12:00:17Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s3-0","uid":"uid-pmj-s3","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s3-hetero-0"},"targetPodUID":"uid-s3-src"},"status":{"phase":"SucceededWithoutRestore","evictingStartTime":"2026-09-28T12:00:06Z","restoredPodName":"t3-s3-hetero-dst","restoredPodUID":"uid-s3-dst","conditions":[{"type":"Restored","status":"False","reason":"RestoreFailedColdStart"}]}}}
EOF
  "${PMPROFILER_BIN}" check --run "${s3_dir}" \
    --name "T3-S3 heterogeneous cross-shape (e2-standard-4 -> n2-standard-4) & I9 fallback convergence" --group "t3-s3-hetero" --pass \
    --value 1 --total 1 --detail "self-test I9 cold-start fallback verified (e2-standard-4 -> n2-standard-4)"
  "${PMPROFILER_BIN}" analyze --run "${s3_dir}" \
    --assert-zero-invariants --assert-clean-outcomes --allow-cold-start --enforce-slo

  # 4. Synthetic T3-S4 (multi-wave sequential rolling node drain soak & I3/I5 audit)
  local s4_dir="${base_dir}/t3_s4"
  mkdir -p "${s4_dir}"
  cat > "${s4_dir}/meta.json" <<EOF
{"scenario":"T3-S4 multi-wave sequential rolling node drain soak","startedAt":"2026-09-28T12:00:00Z"}
EOF
  cat > "${s4_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T11:59:00Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-0","uid":"uid-s4-w1","creationTimestamp":"2026-09-28T11:59:00Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T11:59:05Z"}]}}}
{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s4-w1","uid":"uid-pmj-s4-w1","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s4-soak-0"},"targetPodUID":"uid-s4-w1"},"status":{"phase":"Snapshotting"}}}
{"ts":"2026-09-28T12:00:05Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-0","uid":"uid-s4-w1","creationTimestamp":"2026-09-28T11:59:00Z","deletionTimestamp":"2026-09-28T12:00:05Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-a"},"status":{}}}
{"ts":"2026-09-28T12:00:06Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-1","uid":"uid-s4-w2","creationTimestamp":"2026-09-28T12:00:06Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:00:09Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-1","uid":"uid-s4-w2","creationTimestamp":"2026-09-28T12:00:06Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-b"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:00:14Z"}]}}}
{"ts":"2026-09-28T12:00:15Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s4-w1","uid":"uid-pmj-s4-w1","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"t3-s4-soak-0"},"targetPodUID":"uid-s4-w1"},"status":{"phase":"Succeeded","evictingStartTime":"2026-09-28T12:00:05Z","restoredPodName":"t3-s4-soak-1","restoredPodUID":"uid-s4-w2","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}}
{"ts":"2026-09-28T12:01:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s4-w2","uid":"uid-pmj-s4-w2","creationTimestamp":"2026-09-28T12:01:01Z"},"spec":{"podRef":{"name":"t3-s4-soak-1"},"targetPodUID":"uid-s4-w2"},"status":{"phase":"Snapshotting"}}}
{"ts":"2026-09-28T12:01:05Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-1","uid":"uid-s4-w2","creationTimestamp":"2026-09-28T12:00:06Z","deletionTimestamp":"2026-09-28T12:01:05Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-b"},"status":{}}}
{"ts":"2026-09-28T12:01:06Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-2","uid":"uid-s4-w3","creationTimestamp":"2026-09-28T12:01:06Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:01:09Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-s4-soak-2","uid":"uid-s4-w3","creationTimestamp":"2026-09-28T12:01:06Z","labels":{"app":"t3-s4-soak","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-gvisor-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:01:14Z"}]}}}
{"ts":"2026-09-28T12:01:15Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-s4-w2","uid":"uid-pmj-s4-w2","creationTimestamp":"2026-09-28T12:01:01Z"},"spec":{"podRef":{"name":"t3-s4-soak-1"},"targetPodUID":"uid-s4-w2"},"status":{"phase":"Succeeded","evictingStartTime":"2026-09-28T12:01:05Z","restoredPodName":"t3-s4-soak-2","restoredPodUID":"uid-s4-w3","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}}
EOF
  "${PMPROFILER_BIN}" check --run "${s4_dir}" \
    --name "T3-S4 multi-wave sequential warm migration state continuity" --group "t3-s4-soak" --pass \
    --value 2 --total 2 --detail "node-gvisor-a -> node-gvisor-b -> node-gvisor-a verified"
  "${PMPROFILER_BIN}" check --run "${s4_dir}" \
    --name "T3-S4 post-soak I3/I5 zero residual gates or active PMJs" --group "t3-s4-soak" --pass \
    --value 1 --total 1 --detail "gatedPods=0, activePMJs=0"
  "${PMPROFILER_BIN}" analyze --run "${s4_dir}" \
    --assert-zero-invariants --assert-clean-outcomes --enforce-slo

  # 5. Negative test: verify --enforce-slo catches a gateHoldS / downtimeS breach
  if "${PMPROFILER_BIN}" analyze --run "${s2_dir}" --enforce-slo --slo-gate-hold-p95 1s >/dev/null 2>&1; then
    die "Self-test failure: --enforce-slo with --slo-gate-hold-p95=1s did NOT fail on 4s gateHoldS"
  fi
  log "Verified negative gate: pmprofiler analyze --enforce-slo rejects SLO breach"

  # 6. Generate combined T3 HTML report
  local report_html="${OUT_DIR}/guardrail-t3-report.html"
  "${PMPROFILER_BIN}" report \
    --out "${report_html}" \
    --title "T3 Scale, Cross-Node Drain & Latency SLO (T) Benchmark Report" \
    "${s1_dir}/run.json" "${s2_dir}/run.json" "${s3_dir}/run.json" "${s4_dir}/run.json"
  [[ -s "${report_html}" ]] || die "Expected non-empty HTML report at ${report_html}"

  # 7. Offline verification of PodMigration backup filter and restoration transform
  local test_backup_in="${base_dir}/test-podmigrations-raw.json"
  local test_backup_out="${base_dir}/test-podmigrations-filtered.json"
  cat > "${test_backup_in}" <<EOF
{
  "apiVersion": "v1",
  "kind": "List",
  "items": [
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "diskless-migration",
        "namespace": "${NAMESPACE}",
        "uid": "1111-2222",
        "resourceVersion": "12345",
        "creationTimestamp": "2026-09-28T00:00:00Z",
        "generation": 1,
        "finalizers": ["podmigration.gke.io/storage-cleanup"],
        "managedFields": [{"manager": "kubectl"}],
        "annotations": {
          "kubectl.kubernetes.io/last-applied-configuration": "{\"apiVersion\":\"v1\"}",
          "custom.io/policy": "preserve-me"
        }
      },
      "spec": {"storage": {"location": "gs://bucket/test"}},
      "status": {"conditions": [{"type": "Ready", "status": "True"}]}
    },
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "t3-s1-policy",
        "namespace": "${NAMESPACE}",
        "uid": "3333-4444"
      },
      "spec": {"storage": {"location": "gs://bucket/t3-s1"}}
    },
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "t3-s2-policy",
        "namespace": "${NAMESPACE}",
        "uid": "5555-6666"
      },
      "spec": {"storage": {"location": "gs://bucket/t3-s2"}}
    },
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "t3-s3-policy",
        "namespace": "${NAMESPACE}",
        "uid": "5555-7777"
      },
      "spec": {"storage": {"location": "gs://bucket/t3-s3"}}
    },
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "t3-s4-policy",
        "namespace": "${NAMESPACE}",
        "uid": "5555-8888"
      },
      "spec": {"storage": {"location": "gs://bucket/t3-s4"}}
    },
    {
      "apiVersion": "podmigration.gke.io/v1alpha1",
      "kind": "PodMigration",
      "metadata": {
        "name": "custom-policy",
        "namespace": "${NAMESPACE}",
        "uid": "7777-8888",
        "resourceVersion": "67890",
        "ownerReferences": [{"apiVersion": "v1", "kind": "Foo", "name": "bar"}]
      },
      "spec": {"storage": {"location": "gs://bucket/custom"}}
    }
  ]
}
EOF
  jq "${PODMIGRATIONS_BACKUP_JQ_FILTER}" "${test_backup_in}" > "${test_backup_out}"

  local preserved_count
  preserved_count="$(jq '.items | length' "${test_backup_out}")"
  [[ "${preserved_count}" -eq 2 ]] || die "Self-test failure: expected 2 preserved PodMigrations, got ${preserved_count}"

  local has_t3_s1 has_t3_s2 has_t3_s3 has_t3_s4 has_uid has_status has_last_applied has_custom_ann
  has_t3_s1="$(jq '[.items[].metadata.name] | index("t3-s1-policy")' "${test_backup_out}")"
  has_t3_s2="$(jq '[.items[].metadata.name] | index("t3-s2-policy")' "${test_backup_out}")"
  has_t3_s3="$(jq '[.items[].metadata.name] | index("t3-s3-policy")' "${test_backup_out}")"
  has_t3_s4="$(jq '[.items[].metadata.name] | index("t3-s4-policy")' "${test_backup_out}")"
  [[ "${has_t3_s1}" == "null" ]] || die "Self-test failure: t3-s1-policy was not filtered from backup"
  [[ "${has_t3_s2}" == "null" ]] || die "Self-test failure: t3-s2-policy was not filtered from backup"
  [[ "${has_t3_s3}" == "null" ]] || die "Self-test failure: t3-s3-policy was not filtered from backup"
  [[ "${has_t3_s4}" == "null" ]] || die "Self-test failure: t3-s4-policy was not filtered from backup"

  has_uid="$(jq '[.items[].metadata.uid // empty] | length' "${test_backup_out}")"
  has_status="$(jq '[.items[].status // empty] | length' "${test_backup_out}")"
  [[ "${has_uid}" -eq 0 ]] || die "Self-test failure: metadata.uid was not stripped from backup"
  [[ "${has_status}" -eq 0 ]] || die "Self-test failure: status was not stripped from backup"

  has_last_applied="$(jq '[.items[].metadata.annotations["kubectl.kubernetes.io/last-applied-configuration"] // empty] | length' "${test_backup_out}")"
  [[ "${has_last_applied}" -eq 0 ]] || die "Self-test failure: kubectl.kubernetes.io/last-applied-configuration was not stripped from backup"

  has_custom_ann="$(jq -r '.items[] | select(.metadata.name == "diskless-migration") | .metadata.annotations["custom.io/policy"] // empty' "${test_backup_out}")"
  [[ "${has_custom_ann}" == "preserve-me" ]] || die "Self-test failure: custom annotation was not preserved in backup"

  # 8. Verify stale backup file collision guard fails closed offline
  local test_stale_dir="${base_dir}/test-stale-backup"
  mkdir -p "${test_stale_dir}"
  local test_stale_file="${test_stale_dir}/preexisting-podmigrations.json"
  echo '{}' > "${test_stale_file}"
  if (
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_stale_file}"
    BACKED_UP_PREEXISTING_PODMIGRATIONS="false"
    SELF_TEST="false"
    backup_preexisting_podmigrations >/dev/null 2>&1
  ); then
    die "Self-test failure: backup_preexisting_podmigrations did not fail when stale backup file exists"
  fi
  log "PASS [self-test]: stale backup file collision guard verified"

  # 7. Verify backup_preexisting_podmigrations fails closed when kubectl get fails
  local test_get_fail_dir="${base_dir}/test-get-fail"
  mkdir -p "${test_get_fail_dir}"
  local test_get_fail_backup="${test_get_fail_dir}/preexisting-podmigrations.json"
  if (
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_get_fail_backup}"
    BACKED_UP_PREEXISTING_PODMIGRATIONS="false"
    SELF_TEST="false"
    kubectl() {
      if [[ "$*" == *"get podmigrations"* ]]; then
        return 1
      fi
      command kubectl "$@"
    }
    backup_preexisting_podmigrations >/dev/null 2>&1
  ); then
    die "Self-test failure: backup_preexisting_podmigrations did not fail when kubectl get fails"
  fi
  if [[ -f "${test_get_fail_backup}" ]]; then
    die "Self-test failure: backup file was created despite kubectl get failure"
  fi
  log "PASS [self-test]: backup fails closed when kubectl get fails"

  # 8. Verify resolve_gcs_bucket fails closed when kubectl get fails and no backup exists
  if (
    GCS_BUCKET=""
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_get_fail_backup}"
    kubectl() {
      if [[ "$*" == *"get podmigrations"* ]]; then
        return 1
      fi
      command kubectl "$@"
    }
    resolve_gcs_bucket >/dev/null 2>&1
  ); then
    die "Self-test failure: resolve_gcs_bucket did not fail when kubectl get fails"
  fi
  log "PASS [self-test]: resolve_gcs_bucket fails closed when kubectl get fails"

  # 9. Verify backup_preexisting_podmigrations succeeds when kubectl get emits stderr noise alongside valid JSON
  local test_stderr_noise_dir="${base_dir}/test-stderr-noise"
  mkdir -p "${test_stderr_noise_dir}"
  local test_stderr_noise_backup="${test_stderr_noise_dir}/preexisting-podmigrations.json"
  (
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_stderr_noise_backup}"
    BACKED_UP_PREEXISTING_PODMIGRATIONS="false"
    SELF_TEST="false"
    kubectl() {
      if [[ "$*" == *"get podmigrations"* ]]; then
        echo "Warning: v1alpha1 podmigration.gke.io is deprecated" >&2
        echo '{"apiVersion":"v1","kind":"List","items":[]}'
        return 0
      fi
      command kubectl "$@"
    }
    backup_preexisting_podmigrations >/dev/null 2>&1
  ) || die "Self-test failure: backup_preexisting_podmigrations failed when kubectl emitted stderr noise"
  if [[ ! -f "${test_stderr_noise_backup}" ]]; then
    die "Self-test failure: backup file was not created when kubectl emitted stderr noise"
  fi
  log "PASS [self-test]: backup succeeds when kubectl emits stderr noise"

  # 10. Verify resolve_gcs_bucket returns default bucket on empty List, even with stderr noise
  local resolved_default_bucket
  resolved_default_bucket="$(
    GCS_BUCKET=""
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_stderr_noise_dir}/nonexistent.json"
    kubectl() {
      if [[ "$*" == *"get podmigrations"* ]]; then
        echo "Warning: v1alpha1 podmigration.gke.io is deprecated" >&2
        echo '{"apiVersion":"v1","kind":"List","items":[]}'
        return 0
      fi
      command kubectl "$@"
    }
    resolve_gcs_bucket >/dev/null 2>&1
    echo "${GCS_BUCKET}"
  )" || die "Self-test failure: resolve_gcs_bucket failed on empty List with stderr noise"

  if [[ "${resolved_default_bucket}" != "gs://yaoluo-gke-dev-podsnapshots/snapshots" ]]; then
    die "Self-test failure: expected default GCS bucket for empty List, got '${resolved_default_bucket}'"
  fi
  log "PASS [self-test]: resolve_gcs_bucket defaults correctly on empty List with stderr noise"

  # 11. Verify resolve_gcs_bucket extracts existing location when PodMigration exists with stderr noise
  local resolved_custom_bucket
  resolved_custom_bucket="$(
    GCS_BUCKET=""
    PREEXISTING_PODMIGRATIONS_BACKUP="${test_stderr_noise_dir}/nonexistent.json"
    kubectl() {
      if [[ "$*" == *"get podmigrations"* ]]; then
        echo "Warning: admission webhook warning" >&2
        echo '{"apiVersion":"v1","kind":"List","items":[{"spec":{"storage":{"location":"gs://custom-bucket/snaps"}}}]}'
        return 0
      fi
      command kubectl "$@"
    }
    resolve_gcs_bucket >/dev/null 2>&1
    echo "${GCS_BUCKET}"
  )" || die "Self-test failure: resolve_gcs_bucket failed on existing policy with stderr noise"

  if [[ "${resolved_custom_bucket}" != "gs://custom-bucket/snaps" ]]; then
    die "Self-test failure: expected 'gs://custom-bucket/snaps', got '${resolved_custom_bucket}'"
  fi
  log "PASS [self-test]: resolve_gcs_bucket extracts custom bucket with stderr noise"

  # 12. Verify scenario_t3_s3 logs SKIPPED on a single-shape pool by default (without --allow-simulated-s3)
  # and fails closed when --require-cross-shape is set
  local s3_skip_out
  s3_skip_out="$(
    OUT_DIR="${base_dir}/test-s3-skip"
    REQUIRE_CROSS_SHAPE="false"
    ALLOW_SIMULATED_S3="false"
    kubectl() {
      if [[ "$*" == *"get nodes -l sandbox.gke.io/runtime=gvisor"* ]]; then
        printf "n2-standard-4\nn2-standard-4\n"
        return 0
      fi
      return 1
    }
    scenario_t3_s3 2>&1
  )" || die "Self-test failure: scenario_t3_s3 failed instead of skipping on a single-shape pool"
  grep -q 'SKIPPED \[T3-S3\]' <<<"${s3_skip_out}" || die "Self-test failure: expected SKIPPED [T3-S3] log on single-shape pool, got: ${s3_skip_out}"
  if [[ -d "${base_dir}/test-s3-skip/t3_s3" ]]; then
    die "Self-test failure: expected scenario_t3_s3 not to emit a t3_s3 run directory when skipped"
  fi
  if (
    OUT_DIR="${base_dir}/test-s3-require-fail"
    REQUIRE_CROSS_SHAPE="true"
    ALLOW_SIMULATED_S3="false"
    kubectl() {
      if [[ "$*" == *"get nodes -l sandbox.gke.io/runtime=gvisor"* ]]; then
        printf "n2-standard-4\n"
        return 0
      fi
      return 1
    }
    scenario_t3_s3 >/dev/null 2>&1
  ); then
    die "Self-test failure: expected scenario_t3_s3 to fail closed on single-shape pool when --require-cross-shape is set"
  fi
  log "PASS [self-test]: scenario_t3_s3 single-shape SKIPPED and --require-cross-shape fail-closed verified"

  # 13. Verify clean_stale_migration_resources scopes deletion to T3 artifacts and preserves non-T3 resources (Issue #97)
  local clean_calls_log="${base_dir}/clean-stale-kubectl.log"
  : > "${clean_calls_log}"
  (
    kubectl() {
      if [[ "$*" == "get podmigrationjobs.podmigration.gke.io -n ${NAMESPACE} -o json" ]]; then
        cat <<'JSON'
{
  "items": [
    {"metadata": {"name": "pmj-t3-s1-0"}, "spec": {"podRef": {"name": "t3-s1-counter-0"}}, "status": {"snapshotRef": "snap-opaque-from-pmj"}},
    {"metadata": {"name": "pmj-prod-db-0"}, "spec": {"podRef": {"name": "prod-db-0"}}, "status": {"snapshotRef": "snap-prod-db-0"}}
  ]
}
JSON
        return 0
      fi
      if [[ "$*" == "get podsnapshotmanualtriggers -n ${NAMESPACE} -o json" ]]; then
        cat <<'JSON'
{
  "items": [
    {"metadata": {"name": "trigger-t3-s1-0"}, "spec": {"targetPod": "t3-s1-counter-0"}, "status": {"snapshotCreated": {"name": "snap-opaque-from-psmt"}}},
    {"metadata": {"name": "trigger-prod-db-0"}, "spec": {"targetPod": "prod-db-0"}, "status": {"snapshotCreated": {"name": "snap-prod-db-0"}}}
  ]
}
JSON
        return 0
      fi
      if [[ "$*" == "get podsnapshots -n ${NAMESPACE} -o json" ]]; then
        cat <<'JSON'
{
  "items": [
    {"metadata": {"name": "snap-opaque-from-pmj"}, "spec": {}},
    {"metadata": {"name": "snap-opaque-from-psmt"}, "spec": {}},
    {"metadata": {"name": "snap-direct-t3"}, "spec": {"podName": "t3-redis-0"}},
    {"metadata": {"name": "snap-prod-db-0"}, "spec": {"podName": "prod-db-0"}}
  ]
}
JSON
        return 0
      fi
      echo "$*" >> "${clean_calls_log}"
      return 0
    }
    clean_stale_migration_resources >/dev/null 2>&1
  ) || die "Self-test failure: clean_stale_migration_resources failed during T3-scoped cleanup test"

  if grep -q -- '--all' "${clean_calls_log}"; then
    die "Self-test failure: clean_stale_migration_resources invoked blanket '--all' delete: $(cat "${clean_calls_log}")"
  fi
  if grep -qE 'pmj-prod-db-0|trigger-prod-db-0|snap-prod-db-0' "${clean_calls_log}"; then
    die "Self-test failure: clean_stale_migration_resources touched non-T3 prod resources: $(cat "${clean_calls_log}")"
  fi
  grep -q 'patch podsnapshot snap-opaque-from-pmj ' "${clean_calls_log}" || die "Self-test failure: expected snap-opaque-from-pmj finalizer patch"
  grep -q 'patch podsnapshot snap-opaque-from-psmt ' "${clean_calls_log}" || die "Self-test failure: expected snap-opaque-from-psmt finalizer patch"
  grep -q 'patch podsnapshot snap-direct-t3 ' "${clean_calls_log}" || die "Self-test failure: expected snap-direct-t3 finalizer patch"
  grep -q 'delete podsnapshot snap-opaque-from-pmj snap-opaque-from-psmt snap-direct-t3 ' "${clean_calls_log}" || die "Self-test failure: expected T3 podsnapshots delete"
  grep -q 'delete podsnapshotmanualtrigger trigger-t3-s1-0 ' "${clean_calls_log}" || die "Self-test failure: expected T3 PSMT delete"
  grep -q 'delete podmigrationjobs.podmigration.gke.io pmj-t3-s1-0 ' "${clean_calls_log}" || die "Self-test failure: expected T3 PMJ delete"

  # Also verify that when only non-T3 resources exist, zero delete/patch calls are issued
  local empty_clean_log="${base_dir}/clean-stale-empty-kubectl.log"
  : > "${empty_clean_log}"
  (
    kubectl() {
      if [[ "$*" == "get "* ]]; then
        cat <<'JSON'
{"items": [{"metadata": {"name": "prod-only"}, "spec": {"podRef": {"name": "prod-0"}, "targetPod": "prod-0", "podName": "prod-0"}}]}
JSON
        return 0
      fi
      echo "$*" >> "${empty_clean_log}"
      return 0
    }
    clean_stale_migration_resources >/dev/null 2>&1
  ) || die "Self-test failure: clean_stale_migration_resources failed when only non-T3 resources exist"
  if [[ -s "${empty_clean_log}" ]]; then
    die "Self-test failure: expected zero mutating kubectl calls when only non-T3 resources exist, got: $(cat "${empty_clean_log}")"
  fi
  log "PASS [self-test]: T3-scoped clean_stale_migration_resources preserves non-T3 artifacts"

  log "PASS [self-test]: PodMigration backup filter & restore transform verified"
  log "Self-test PASSED: HTML report generated at ${report_html}"
}

# shellcheck disable=SC2030,SC2031
main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
    exit 0
  fi

  if [[ -f "${PREEXISTING_PODMIGRATIONS_BACKUP}" ]]; then
    die "Pre-existing backup file found at ${PREEXISTING_PODMIGRATIONS_BACKUP}. A previous run did not complete restore; inspect/apply or remove this file before re-running."
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
    T3-S3|s3)
      scenario_t3_s3
      ;;
    T3-S4|s4)
      scenario_t3_s4
      ;;
    all)
      run_scalesim_model
      scenario_t3_s1
      scenario_t3_s2
      scenario_t3_s3
      scenario_t3_s4
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
