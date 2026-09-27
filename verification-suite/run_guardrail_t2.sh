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
# Tier-2 (T2) Per-Merge Cluster Scenario & Stateful Verifier Suite (Issue #52)
#
# Orchestrates:
#   - 3 Stateful Application Verifiers (I2: No Silent Cold-Start / No Rollback):
#       * v_counter  : Monotonic counter + instanceID continuity across warm restore
#       * v_redis    : 50,000 populated keys + per-cycle nonce across warm restore
#       * v_postgres : Active transactional sequence table without gap/rollback
#   - 5 Adversarial T2 Scenarios (S1-S5) exercising invariants I1-I9:
#       * S1 : Deployment rolling upgrade during active node drain (I6, I2)
#       * S2 : PDB minAvailable: 100% block followed by budget release (I7, I2)
#       * S3 : Corrupted checkpoint artifact in GCS -> deterministic I9 cold-start fallback
#       * S4 : Dual-replica simultaneous eviction under serialized PodGate contention (I1, I3, I2)
#       * S5 : Mid-flight PodMigration deletion -> I4/I5 deferral & zero orphan PSSC/PSMT leaks
#   - Offline CI validation mode (--self-test) exercising pmprofiler check, analyze,
#     invariant gates, and report generation end-to-end without requiring a live cluster.

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
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --scenario <S1|S2|S3|S4|S5|all>   Scenario(s) to execute (default: all)
  --verifier-only <counter|redis|postgres>
                                    Run a single stateful app verifier on the cluster
  --out-dir <dir>                   Output directory for pmprofiler runs & HTML report
  --namespace <ns>                  Target workload namespace (default: default)
  --controller-ns <ns>              Controller namespace (default: pod-migration-system)
  --gcs-bucket <gs://bucket/path>   Optional GCS bucket for snapshot storage & size sampling
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
COLLECT_PID=""
CORDONED_NODES=()

build_pmprofiler() {
  mkdir -p "${OUT_DIR}/bin"
  PMPROFILER_BIN="${OUT_DIR}/bin/pmprofiler"
  log "Building pmprofiler binary -> ${PMPROFILER_BIN}"
  (cd "${PMPROFILER_SRC}" && go build -o "${PMPROFILER_BIN}" ./cmd/pmprofiler)
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

clean_test_workloads() {
  kubectl delete deployment/t2-counter deployment/t2-bystander statefulset/t2-redis statefulset/t2-postgres \
    service/t2-redis-svc service/t2-postgres-svc pdb/t2-redis-pdb \
    -n "${NAMESPACE}" --ignore-not-found --wait=true --timeout=30s >/dev/null 2>&1 || true
  kubectl delete pods -n "${NAMESPACE}" -l "app in (t2-counter,t2-bystander,t2-redis,t2-postgres)" \
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
  stop_collector
  local analyze_args=(
    analyze
    --run "${run_dir}"
    --controller-ns "${CONTROLLER_NS}"
    --assert-zero-invariants
    --assert-clean-outcomes
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

ensure_podmigration_policy() {
  local name="${1:-diskless-migration}"
  resolve_gcs_bucket
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: ${name}
  namespace: ${NAMESPACE}
spec:
  storage:
    location: ${GCS_BUCKET}
EOF
  kubectl wait --for=condition=Ready "podmigration/${name}" -n "${NAMESPACE}" --timeout=60s >/dev/null
}

deploy_counter_workload() {
  local name="${1:-t2-counter}"
  local replicas="${2:-1}"
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
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
  kubectl rollout status deployment/"${name}" -n "${NAMESPACE}" --timeout=180s >/dev/null
}

deploy_redis_workload() {
  local name="${1:-t2-redis}"
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
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
        imagePullPolicy: IfNotPresent
        args: ["--save", "", "--appendonly", "no", "--enable-debug-command", "yes"]
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
  kubectl rollout status statefulset/"${name}" -n "${NAMESPACE}" --timeout=180s >/dev/null
}

deploy_postgres_workload() {
  local name="${1:-t2-postgres}"
  kubectl apply -n "${NAMESPACE}" -f - >/dev/null <<EOF
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
        image: postgres:16-alpine
        imagePullPolicy: IfNotPresent
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
# 5 Adversarial T2 Scenarios (S1 - S5)
# ==============================================================================

# S1: Deployment rolling upgrade during active node drain (I6: Zero False Positive Intercepts)
run_scenario_s1() {
  local run_dir="${OUT_DIR}/s1-rolling-upgrade-during-drain"
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

  # Cordon source node and trigger eviction of t2-counter while simultaneously
  # rolling-upgrading t2-bystander (proving I6: rolling upgrade does not trigger a false PMJ).
  kubectl cordon "${src_node}" >/dev/null
  CORDONED_NODES+=("${src_node}")

  evict_pod "${src_pod}"
  kubectl patch deployment t2-bystander -n "${NAMESPACE}" --type=merge \
    -p "{\"spec\":{\"template\":{\"metadata\":{\"annotations\":{\"rollout-ts\":\"$(date +%s)\"}}}}}" >/dev/null

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null
  kubectl rollout status deployment/t2-bystander -n "${NAMESPACE}" --timeout=180s >/dev/null

  local dst_pod
  dst_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  v_counter_verify "${run_dir}" "${dst_pod}" "${before_state}"

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

  local src_node
  src_node="$(kubectl get pod -n "${NAMESPACE}" "${src_pod}" -o jsonpath='{.spec.nodeName}')"
  kubectl cordon "${src_node}" >/dev/null
  CORDONED_NODES+=("${src_node}")

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

  local src_pod
  src_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  start_collector "${run_dir}" "S3: Corrupted checkpoint artifact -> I9 cold-start fallback"

  # Cordon all gVisor nodes before triggering eviction so the replacement pod is held
  # Unschedulable after snapshot upload + source eviction while we corrupt checkpoint.img in GCS.
  log "Cordoning gVisor nodes to hold replacement pod Pending during GCS checkpoint corruption"
  kubectl cordon -l sandbox.gke.io/runtime=gvisor >/dev/null

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

  # Wait until source pod is deleted and replacement pod is created (held Pending on cordoned nodes)
  sleep 3

  # Remove the newly uploaded checkpoint artifacts in GCS so restore fails and triggers
  # deterministic I9 cold-start fallback (SucceededWithoutRestore / FallbackToColdStart).
  log "Removing GCS checkpoint artifacts at ${GCS_BUCKET}/${snap_name}"
  gcloud storage rm -r "${GCS_BUCKET}/${snap_name}" >/dev/null 2>&1

  log "Uncordoning gVisor nodes so replacement pod attempts restore from missing/corrupted checkpoint"
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

  local c_pods
  mapfile -t c_pods < <(get_active_pods "app=t2-counter")
  for p in "${c_pods[@]}" "${pg_pod}"; do
    evict_pod "${p}"
  done

  for p in "${c_pods[@]}" "${pg_pod}"; do
    wait_for_pmj_terminal "${p}" "Succeeded" 300
  done
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null
  kubectl rollout status statefulset/t2-postgres -n "${NAMESPACE}" --timeout=180s >/dev/null

  v_postgres_verify "${run_dir}" "${pg_pod}" "${pg_nonce}" 200

  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
}

# S5: Mid-flight PodMigration deletion verifying I4/I5 deferral and zero orphan PSSC/PSMT leaks
run_scenario_s5() {
  local run_dir="${OUT_DIR}/s5-midflight-podmigration-deletion"
  log "=== Running Scenario S5: Mid-Flight PodMigration Deletion (I4/I5 Deferral & Zero Orphans) ==="
  clean_test_workloads
  clean_stale_migration_resources
  ensure_podmigration_policy "diskless-migration"
  deploy_counter_workload "t2-counter" 1
  sleep 2

  local src_pod before_state pm_uid
  src_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  before_state="$(v_counter_capture "${src_pod}")"
  pm_uid="$(kubectl get podmigration diskless-migration -n "${NAMESPACE}" -o jsonpath='{.metadata.uid}')"

  start_collector "${run_dir}" "S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)"

  evict_pod "${src_pod}"
  sleep 1
  # Delete PodMigration while PMJ is in-flight; I4/I5 finalizer defers PSSC/PSP cleanup until PMJ finishes.
  kubectl delete podmigration diskless-migration -n "${NAMESPACE}" --wait=false >/dev/null

  wait_for_pmj_terminal "${src_pod}" "Succeeded" 240
  kubectl wait --for=delete podmigration/diskless-migration -n "${NAMESPACE}" --timeout=120s >/dev/null 2>&1 || true
  kubectl rollout status deployment/t2-counter -n "${NAMESPACE}" --timeout=180s >/dev/null

  local dst_pod
  dst_pod="$(get_active_pods "app=t2-counter" | head -n 1)"
  v_counter_verify "${run_dir}" "${dst_pod}" "${before_state}"

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

  # Restore baseline PodMigration CR on the cluster
  ensure_podmigration_policy "diskless-migration"
  finish_and_assert_run "${run_dir}" "false"
  clean_test_workloads
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

    mkdir -p "${dir}"
    cat >"${dir}/meta.json" <<EOF
{"scenario":"${scenario_name}","namespace":"default","startedAt":"2026-09-27T10:00:00Z","serverVersion":"v1.32.2-gke.1200"}
EOF
    python3 - "${dir}/records.ndjson" "${app}" "${pmj_name}" "${src_pod}" "${dst_pod}" "${phase}" "${restored_status}" "${restored_reason}" "${prom_clean}" <<'PY'
import json, sys

out_path, app, pmj_name, src_pod, dst_pod, phase, r_status, r_reason, prom_body = sys.argv[1:]

records = [
    {
        "ts": "2026-09-27T09:59:00Z",
        "type": "list",
        "gvr": "pods.v1",
        "obj": {
            "metadata": {
                "name": src_pod,
                "uid": f"uid-{src_pod}",
                "creationTimestamp": "2026-09-27T09:59:00Z",
                "labels": {"app": app, "pod-migration.gke.io/enabled": "true"},
            },
            "spec": {"nodeName": "gke-node-a"},
            "status": {
                "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-09-27T09:59:10Z"}]
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:01Z",
        "type": "add",
        "gvr": "podmigrations.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": "diskless-migration", "creationTimestamp": "2026-09-27T10:00:01Z"},
            "spec": {"storage": {"location": "gs://yaoluo-gke-dev-podsnapshots/snapshots"}},
            "status": {"conditions": [{"type": "Ready", "status": "True", "reason": "Reconciled"}]},
        },
    },
    {
        "ts": "2026-09-27T10:00:02Z",
        "type": "add",
        "gvr": "podmigrationjobs.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": pmj_name, "uid": f"uid-{pmj_name}", "creationTimestamp": "2026-09-27T10:00:02Z"},
            "spec": {"podRef": {"name": src_pod}, "targetPodUID": f"uid-{src_pod}"},
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
                "uid": f"uid-{src_pod}",
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
                "uid": f"uid-{dst_pod}",
                "creationTimestamp": "2026-09-27T10:00:10Z",
                "labels": {"app": app, "pod-migration.gke.io/enabled": "true"},
            },
            "spec": {"nodeName": "gke-node-b"},
            "status": {
                "conditions": [{"type": "Ready", "status": "True", "lastTransitionTime": "2026-09-27T10:00:14Z"}]
            },
        },
    },
    {
        "ts": "2026-09-27T10:00:15Z",
        "type": "update",
        "gvr": "podmigrationjobs.v1alpha1.podmigration.gke.io",
        "obj": {
            "metadata": {"name": pmj_name, "uid": f"uid-{pmj_name}", "creationTimestamp": "2026-09-27T10:00:02Z"},
            "spec": {"podRef": {"name": src_pod}, "targetPodUID": f"uid-{src_pod}"},
            "status": {
                "phase": phase,
                "snapshotRef": f"snap-{src_pod}",
                "restoredPodName": dst_pod,
                "restoredPodUID": f"uid-{dst_pod}",
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

  # S1: Counter rolling upgrade during active node drain
  emit_synthetic_scenario "${OUT_DIR}/s1" "S1: Deployment rolling upgrade during active node drain" \
    "counter" "pmj-s1-counter" "counter-0" "counter-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s1" \
    --name "state survived (token-verified)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "instanceID=7f9c-s1 preserved, count advanced 42 -> 51"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s1" --assert-zero-invariants --assert-clean-outcomes

  # S2: Redis PDB temporary block followed by release (50,000 keys + nonce)
  emit_synthetic_scenario "${OUT_DIR}/s2" "S2: PDB minAvailable 100% block followed by budget release" \
    "redis" "pmj-s2-redis" "redis-0" "redis-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s2" \
    --name "state survived (token-verified)" --group "redis" \
    --pass --value 50000 --total 50000 \
    --detail "migkey=nonce-s2 matched and DBSIZE=50001 (>= 50000)"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s2" --assert-zero-invariants --assert-clean-outcomes

  # S3: Corrupted checkpoint -> deterministic I9 cold-start fallback (FallbackToColdStart)
  emit_synthetic_scenario "${OUT_DIR}/s3" "S3: Corrupted checkpoint artifact -> I9 cold-start fallback" \
    "counter" "pmj-s3-counter" "counter-corrupt-0" "counter-corrupt-0-fallback" \
    "SucceededWithoutRestore" "False" "FallbackToColdStart"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s3" \
    --name "I9 deterministic cold-start fallback (FallbackToColdStart)" --group "counter" \
    --pass --value 1 --total 1 \
    --detail "PMJ transitioned to SucceededWithoutRestore with Restored=False (Reason=FallbackToColdStart)"
  if "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s3" --assert-zero-invariants --assert-clean-outcomes >/dev/null 2>&1; then
    die "Expected S3 cold-start run to fail --assert-clean-outcomes when --allow-cold-start is not set"
  fi
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s3" --assert-zero-invariants --assert-clean-outcomes --allow-cold-start

  # S4: Postgres + Counter dual-replica contention
  emit_synthetic_scenario "${OUT_DIR}/s4" "S4: Dual-replica simultaneous eviction under serialized PodGate contention" \
    "postgres" "pmj-s4-postgres" "postgres-0" "postgres-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s4" \
    --name "state survived (token-verified)" --group "postgres" \
    --pass --value 200 --total 200 \
    --detail "contiguous sequence 1..200 (200 rows) and nonce=nonce-s4 preserved"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s4" --assert-zero-invariants --assert-clean-outcomes

  # S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)
  emit_synthetic_scenario "${OUT_DIR}/s5" "S5: Mid-flight PodMigration deletion (I4/I5 deferral & zero orphan leaks)" \
    "counter" "pmj-s5-counter" "counter-s5-0" "counter-s5-0-restored" "Succeeded" "True" "RestoreVerified"
  "${PMPROFILER_BIN}" check --run "${OUT_DIR}/s5" \
    --name "I4 zero orphan PSSC after PodMigration deletion" --group "controller" \
    --pass --value 1 --total 1 \
    --detail "0 orphan PodSnapshotStorageConfigs and clean Undeploying deferral"
  "${PMPROFILER_BIN}" analyze --run "${OUT_DIR}/s5" --assert-zero-invariants --assert-clean-outcomes

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
    --title "GKE Live Pod Migration — T2 Guardrail Scenarios (S1–S5)" \
    "${OUT_DIR}/s1/run.json" \
    "${OUT_DIR}/s2/run.json" \
    "${OUT_DIR}/s3/run.json" \
    "${OUT_DIR}/s4/run.json" \
    "${OUT_DIR}/s5/run.json"

  [[ -s "${report_html}" ]] || die "Expected non-empty HTML report at ${report_html}"
  log "Self-test PASSED: S1-S5 analyzed with 0 invariant violations and report generated at ${report_html}"
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
  all|ALL)
    run_scenario_s1
    run_scenario_s2
    run_scenario_s3
    run_scenario_s4
    run_scenario_s5
    "${PMPROFILER_BIN}" report \
      --out "${OUT_DIR}/guardrail-t2-report.html" \
      --title "GKE Live Pod Migration — T2 Guardrail Scenarios (S1–S5)" \
      "${OUT_DIR}"/s*/run.json
    log "All T2 scenarios (S1-S5) PASSED. Report: ${OUT_DIR}/guardrail-t2-report.html"
    ;;
  *)
    die "Unknown --scenario value: ${SCENARIO} (expected S1, S2, S3, S4, S5, or all)"
    ;;
esac
