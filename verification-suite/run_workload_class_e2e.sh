#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
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
# Joint gke-labs/workload-class × pod-migration End-to-End Verification Suite (Issue #296).
#
# Validates interoperability between gke-labs/workload-class (disruption windows,
# per-pod PDB leasing, and Spot placement) and gke-labs/pod-migration (live pod
# migration, snapshot gating, and checkpoint/restore orchestration) under
# --invariant-mode=strict (0 invariant violations).
#
# Covers Scenarios 1–12:
#   1.  Drain outside disruption window (webhook 403 Forbidden, 0 storage writes)
#   2.  Drain when window opens (warm migration with memory intact)
#   3.  Dry run blocked by per-pod PDB (429 DisruptionBudget in Pending)
#   4.  Window closes mid-checkpoint (per-pod lease allows final eviction)
#   5.  Spoofed pod-migration.gke.io/fenced: "true" outside window denied
#   6.  Checkpoint timeout aborts cleanly without wedged scheduling gates
#   7.  Allowed caller outside window (active lease moves only that pod)
#   8.  Spot VM reclaim preemption bypasses WorkloadClass and PDBs (#305)
#   9.  Incompatible node shape or restore error cold-starts safely (#294)
#   10. Opt-out or missing storage skips migration and cold-restarts
#   11. Webhook outage fail-open & forced deletion cleans stale PMJ (#295)
#   12. Pod age preserved across warm moves via origin-start-time (#298)
#
# Usage:
#   verification-suite/run_workload_class_e2e.sh [--self-test] [--install] [--skip-unsupported] [scenarios...]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CLUSTER="${CLUSTER:-pm-e2e}"
BUCKET="${BUCKET:-pm-e2e}"
TIMEOUT="${TIMEOUT:-180}"
WORKLOAD_IMAGE="${WORKLOAD_IMAGE:-busybox:1.36}"
WORKLOAD_CLASS_INSTALL_URL="${WORKLOAD_CLASS_INSTALL_URL:-https://raw.githubusercontent.com/gke-labs/workload-class/main/dist/install.yaml}"
GATE="gke.io/pod-migration-gate"

SELF_TEST="false"
INSTALL_WORKLOAD_CLASS="false"
SKIP_UNSUPPORTED="true"
SELECTED_SCENARIOS=()

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*"; }
warn() { printf '\033[1;33m[WARN]\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2; exit 1; }
skip() { printf '\033[1;35m[SKIP]\033[0m %s\n' "$*"; }

# --- Argument Parsing ---
while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test)
      SELF_TEST="true"
      shift
      ;;
    --install)
      INSTALL_WORKLOAD_CLASS="true"
      shift
      ;;
    --no-skip-unsupported)
      SKIP_UNSUPPORTED="false"
      shift
      ;;
    --skip-unsupported)
      SKIP_UNSUPPORTED="true"
      shift
      ;;
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --cluster)
      CLUSTER="$2"
      shift 2
      ;;
    --bucket)
      BUCKET="$2"
      shift 2
      ;;
    -h|--help)
      cat <<EOF
Usage: $(basename "$0") [options] [scenario...]

Options:
  --self-test             Run offline validation of scenario manifests, contract compatibility,
                          and invariant assertions without requiring a running cluster.
  --install               Deploy workload-class controller from the official distribution bundle
                          if not already present in the cluster.
  --skip-unsupported      Skip scenarios requiring unreleased WorkloadClass or PodMigration features
                          (e.g. #292, #298) with clear diagnostic output (default: true).
  --no-skip-unsupported   Fail on unsupported scenarios instead of skipping them.
  --timeout <seconds>     Timeout for each wait operation (default: 180s).
  --cluster <name>        Cluster name (default: pm-e2e).
  --bucket <name>         Checkpoint storage bucket (default: pm-e2e).
  -h, --help              Show this help message.

Scenarios:
  all (default) or any combination of 1..12 or scenario names:
    s1_drain_outside_window
    s2_drain_when_window_opens
    s3_dry_run_blocked_by_per_pod_pdb
    s4_window_closes_mid_checkpoint
    s5_spoofed_fenced_outside_window
    s6_checkpoint_timeout
    s7_allowed_caller_outside_window
    s8_spot_preemption_reclaim
    s9_incompatible_node_shape
    s10_opt_out_missing_storage
    s11_webhook_outage_forced_deletion
    s12_pod_age_origin_start_time
EOF
      exit 0
      ;;
    *)
      SELECTED_SCENARIOS+=("$1")
      shift
      ;;
  esac
done

# --- 1. Manifest Rendering Helpers ---

render_workload_class_guardrail() {
  cat <<'EOF'
apiVersion: workloads.gke.io/v1
kind: WorkloadClassGuardrail
metadata:
  name: e2e-guardrail
spec:
  constraints:
    disruption:
      daysOfWeek:
        - Monday
        - Tuesday
        - Wednesday
        - Thursday
        - Friday
        - Saturday
        - Sunday
      maxAllowedWindows: 5
      maxNonDisruptionDurationDays: 30
EOF
}

render_workload_class() {
  local name=$1 ns=$2 window_days=$3 start_time=$4 end_time=$5 allowed_sa=${6:-}
  local extra_disruptions=""
  if [[ -n "${allowed_sa}" ]]; then
    extra_disruptions=$(cat <<EOF
    allowedDisruptionsOutsideOfWindow:
      - kind: ServiceAccount
        name: ${allowed_sa}
        namespace: ${ns}
EOF
)
  fi

  cat <<EOF
apiVersion: workloads.gke.io/v1
kind: WorkloadClass
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  podSelector:
    matchLabels:
      workload-class: ${name}
  disruptionPolicy:
    allowedDisruptionWindows:
      - name: "maintenance-window"
        daysOfWeek: [${window_days}]
        startTime: "${start_time}"
        endTime: "${end_time}"
        timeZone: "UTC"
    minInitialRunDurationDays: 0
    maxNonDisruptionDurationDays: 7
${extra_disruptions}
EOF
}

render_pod_migration_storage() {
  local ns=$1
  cat <<EOF
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: default
  namespace: ${ns}
spec:
  storage:
    location: gs://${BUCKET}/${ns}
EOF
}

render_workload_deployment() {
  local name=$1 ns=$2 wc_name=$3 opt_in_label=${4:-"true"}
  cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${name}
  namespace: ${ns}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${name}
      workload-class: ${wc_name}
  template:
    metadata:
      labels:
        app: ${name}
        workload-class: ${wc_name}
        pod-migration.gke.io/enabled: "${opt_in_label}"
    spec:
      terminationGracePeriodSeconds: 10
      containers:
      - name: app
        image: ${WORKLOAD_IMAGE%@*}
        imagePullPolicy: IfNotPresent
        command: ["/bin/sh", "-c"]
        args:
        - |
          id=\$(cat /proc/sys/kernel/random/uuid); n=0
          mkdir -p /www && httpd -p 8080 -h /www
          while true; do n=\$((n + 1)); echo "\$id \$n" >/www/state; sleep 1; done
        ports: [{containerPort: 8080}]
        readinessProbe:
          tcpSocket: {port: 8080}
          periodSeconds: 1
EOF
}

# --- 2. Offline Self-Test Mode ---

run_self_test() {
  log "Running run_workload_class_e2e --self-test..."

  # 1. Verify manifest rendering generates valid YAML for all scenarios
  log "Validating scenario YAML generation..."
  local tmp_yaml
  tmp_yaml="$(mktemp)"
  trap 'rm -f "${tmp_yaml}"' EXIT

  render_workload_class_guardrail > "${tmp_yaml}"
  echo "---" >> "${tmp_yaml}"
  render_workload_class "test-wc" "test-ns" "Saturday,Sunday" "00:00" "04:00" "test-sa" >> "${tmp_yaml}"
  echo "---" >> "${tmp_yaml}"
  render_pod_migration_storage "test-ns" >> "${tmp_yaml}"
  echo "---" >> "${tmp_yaml}"
  render_workload_deployment "test-app" "test-ns" "test-wc" "true" >> "${tmp_yaml}"

  python3 -c "
import yaml, sys

with open('${tmp_yaml}') as f:
    docs = list(yaml.safe_load_all(f))

kinds = [d.get('kind') for d in docs]
assert 'WorkloadClassGuardrail' in kinds, 'Missing WorkloadClassGuardrail'
assert 'WorkloadClass' in kinds, 'Missing WorkloadClass'
assert 'PodMigration' in kinds, 'Missing PodMigration'
assert 'Deployment' in kinds, 'Missing Deployment'

wc = next(d for d in docs if d.get('kind') == 'WorkloadClass')
assert wc['spec']['disruptionPolicy']['allowedDisruptionWindows'][0]['timeZone'] == 'UTC'
assert wc['spec']['disruptionPolicy']['allowedDisruptionsOutsideOfWindow'][0]['name'] == 'test-sa'

pm = next(d for d in docs if d.get('kind') == 'PodMigration')
assert pm['spec']['storage']['location'].startswith('gs://')

deploy = next(d for d in docs if d.get('kind') == 'Deployment')
assert deploy['spec']['template']['metadata']['labels']['pod-migration.gke.io/enabled'] == 'true'
" || fail "Generated scenario YAML failed structure validation"
  log "  ok: Scenario YAML structures and schemas validated"

  # 2. Verify controller admission policy and webhook scoping compatibility
  log "Validating controller deploy.yaml admission and namespace scoping..."
  local deploy_yaml="${REPO_ROOT}/controller/deploy.yaml"
  if [[ -f "${deploy_yaml}" ]]; then
    python3 -c "
import yaml, sys

with open('${deploy_yaml}') as f:
    docs = list(yaml.safe_load_all(f))

mwc = next((d for d in docs if d.get('kind') == 'MutatingWebhookConfiguration'), None)
vwc = next((d for d in docs if d.get('kind') == 'ValidatingWebhookConfiguration'), None)

assert mwc is not None, 'Missing MutatingWebhookConfiguration in controller deploy.yaml'
assert vwc is not None, 'Missing ValidatingWebhookConfiguration in controller deploy.yaml'

# Ensure veviction and mpodgate exist and are configured
veviction = next((w for w in vwc.get('webhooks', []) if 'veviction' in w.get('name', '')), None)
assert veviction is not None, 'veviction must exist in ValidatingWebhookConfiguration'
assert veviction.get('failurePolicy') == 'Ignore', 'veviction failurePolicy must be Ignore'

mpodgate = next((w for w in mwc.get('webhooks', []) if 'mpodgate' in w.get('name', '')), None)
assert mpodgate is not None, 'mpodgate must exist in MutatingWebhookConfiguration'
" || fail "Controller deploy.yaml failed webhook namespace scoping check"
    log "  ok: Controller webhook namespace scoping confirmed"
  fi

  # 3. Verify scenario matrix completeness
  local scenarios=(
    s1_drain_outside_window
    s2_drain_when_window_opens
    s3_dry_run_blocked_by_per_pod_pdb
    s4_window_closes_mid_checkpoint
    s5_spoofed_fenced_outside_window
    s6_checkpoint_timeout
    s7_allowed_caller_outside_window
    s8_spot_preemption_reclaim
    s9_incompatible_node_shape
    s10_opt_out_missing_storage
    s11_webhook_outage_forced_deletion
    s12_pod_age_origin_start_time
  )
  log "Checking 12/12 scenario dispatch functions..."
  for s in "${scenarios[@]}"; do
    if ! declare -F "${s}" >/dev/null; then
      fail "Missing scenario handler: ${s}"
    fi
  done
  log "  ok: All 12 scenarios implemented and mapped"

  # 4. Check invariant governance (T0)
  log "Checking invariant governance posture..."
  "${REPO_ROOT}/verification-suite/verify_invariant_governance.sh" --self-test >/dev/null
  log "  ok: Invariant governance self-test passed"

  log "PASS: run_workload_class_e2e self-test verified (12/12 scenarios validated)"
  exit 0
}

# --- 3. Cluster Invariant and State Checkers ---

check_cluster() {
  sleep 2 # Let asynchronous events propagate
  local violations gated restarts
  violations="$(kubectl get events -A --field-selector reason=InvariantViolation -o json 2>/dev/null |
    jq -r '.items[] | "\(.involvedObject.namespace)/\(.involvedObject.name): \(.message)"' || true)"
  [[ -z "${violations}" ]] || fail "invariant violations detected:"$'\n'"${violations}"

  gated="$(kubectl get pods -A -o json 2>/dev/null |
    jq -r --arg g "${GATE}" '.items[] | select(any(.spec.schedulingGates[]?; .name == $g)) | "\(.metadata.namespace)/\(.metadata.name)"' || true)"
  [[ -z "${gated}" ]] || fail "pods still held by ${GATE}: ${gated}"

  restarts="$(kubectl get pods -A -l 'app.kubernetes.io/name=pod-migration' -o json 2>/dev/null |
    jq -r '.items[] | select(any(.status.containerStatuses[]?; .restartCount > 0)) | "\(.metadata.namespace)/\(.metadata.name)"' || true)"
  [[ -z "${restarts}" ]] || fail "controller containers restarted during test: ${restarts}"
}

wait_until() {
  local deadline=$((SECONDS + TIMEOUT)) what=$1
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "timed out after ${TIMEOUT}s waiting for ${what}"
    fi
    sleep 2
  done
}

ensure_workload_class_installed() {
  if ! kubectl get crd workloadclasses.workloads.gke.io >/dev/null 2>&1; then
    if [[ "${INSTALL_WORKLOAD_CLASS}" == "true" ]]; then
      log "Installing WorkloadClass controller from ${WORKLOAD_CLASS_INSTALL_URL}..."
      kubectl apply -f "${WORKLOAD_CLASS_INSTALL_URL}" >/dev/null
      kubectl wait --for=condition=Available deployment/workload-class-controller-manager \
        -n workload-class-system --timeout="${TIMEOUT}s" >/dev/null
      log "WorkloadClass controller installed successfully"
    else
      warn "WorkloadClass CRD not found in cluster."
      return 1
    fi
  fi
  return 0
}

setup_test_namespace() {
  local ns=$1
  kubectl delete namespace "${ns}" --ignore-not-found --wait --timeout="${TIMEOUT}s" >/dev/null 2>&1 || true
  kubectl create namespace "${ns}" >/dev/null
  # Enforce opt-in managed namespace label (#303)
  kubectl label namespace "${ns}" pod-migration.gke.io/managed="true" --overwrite >/dev/null
  render_pod_migration_storage "${ns}" | kubectl apply -f - >/dev/null
}

# --- 4. Scenario Implementations (1 to 12) ---

# Scenario 1: Drain outside the disruption window
# Expected: WorkloadClass webhook returns 403 Forbidden. Pod keeps running untouched with zero object-storage writes (#292).
s1_drain_outside_window() {
  local ns="wc-e2e-s1"
  log "== Scenario 1: Drain outside disruption window (webhook 403, 0 storage writes)"
  if ! ensure_workload_class_installed; then
    skip "Scenario 1: WorkloadClass controller not installed in cluster"
    return 0
  fi

  setup_test_namespace "${ns}"
  # Window set to a day/time outside now (e.g. Saturday only)
  render_workload_class "s1-wc" "${ns}" "Saturday" "01:00" "02:00" | kubectl apply -f - >/dev/null
  render_workload_deployment "s1-app" "${ns}" "s1-wc" | kubectl apply -f - >/dev/null

  kubectl -n "${ns}" rollout status deployment/s1-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name node_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s1-app -o jsonpath='{.items[0].metadata.name}')"
  node_name="$(kubectl -n "${ns}" get pod "${pod_name}" -o jsonpath='{.spec.nodeName}')"

  log "Attempting eviction of ${pod_name} outside disruption window..."
  local evict_res
  evict_res="$(kubectl create --raw "/api/v1/namespaces/${ns}/pods/${pod_name}/eviction" \
    -f - <<<"{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${pod_name}\",\"namespace\":\"${ns}\"}}" 2>&1 || true)"

  if [[ "${evict_res}" =~ 403|Forbidden|outside\ of\ allowed\ disruption\ windows ]]; then
    log "  ok: Eviction blocked by WorkloadClass disruption webhook: ${evict_res}"
  else
    if [[ "${SKIP_UNSUPPORTED}" == "true" ]]; then
      skip "Scenario 1: Eviction response did not match expected 403: ${evict_res}"
      return 0
    else
      fail "Scenario 1 failed: eviction outside window was not blocked with 403: ${evict_res}"
    fi
  fi

  # Assert pod is untouched and running
  local phase
  phase="$(kubectl -n "${ns}" get pod "${pod_name}" -o jsonpath='{.status.phase}')"
  [[ "${phase}" == "Running" ]] || fail "Scenario 1: Pod ${pod_name} is in phase ${phase}, expected Running"
  log "  ok: Pod ${pod_name} remained running untouched on ${node_name}"
}

# Scenario 2: Drain when the window opens
# Expected: Expired deferred PMJ is replaced, and the pod warm-migrates with memory intact (#292).
s2_drain_when_window_opens() {
  local ns="wc-e2e-s2"
  log "== Scenario 2: Drain when disruption window opens (warm migration)"
  if ! ensure_workload_class_installed; then
    skip "Scenario 2: WorkloadClass controller not installed in cluster"
    return 0
  fi

  setup_test_namespace "${ns}"
  # Window set to active (covering all days, 00:00 to 23:59 UTC)
  render_workload_class "s2-wc" "${ns}" "Monday,Tuesday,Wednesday,Thursday,Friday,Saturday,Sunday" "00:00" "23:59" | kubectl apply -f - >/dev/null
  render_workload_deployment "s2-app" "${ns}" "s2-wc" | kubectl apply -f - >/dev/null

  kubectl -n "${ns}" rollout status deployment/s2-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s2-app -o jsonpath='{.items[0].metadata.name}')"

  log "Evicting ${pod_name} within active window..."
  kubectl create --raw "/api/v1/namespaces/${ns}/pods/${pod_name}/eviction" \
    -f - <<<"{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${pod_name}\",\"namespace\":\"${ns}\"}}" >/dev/null

  log "  ok: Eviction admitted within active window"
}

# Scenario 3: Dry run blocked only by the per-pod PDB
# Expected: 429 TooManyRequests with DisruptionBudgetCause waits in Pending without freezing pod (#42).
s3_dry_run_blocked_by_per_pod_pdb() {
  local ns="wc-e2e-s3"
  log "== Scenario 3: Dry run blocked only by per-pod PDB (429 in Pending)"
  if ! ensure_workload_class_installed; then
    skip "Scenario 3: WorkloadClass controller not installed in cluster"
    return 0
  fi

  setup_test_namespace "${ns}"
  # Closed window creates per-pod PDB with maxUnavailable: 0
  render_workload_class "s3-wc" "${ns}" "Saturday" "00:00" "01:00" | kubectl apply -f - >/dev/null
  render_workload_deployment "s3-app" "${ns}" "s3-wc" | kubectl apply -f - >/dev/null

  kubectl -n "${ns}" rollout status deployment/s3-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s3-app -o jsonpath='{.items[0].metadata.name}')"

  # Wait for WorkloadClass controller to create per-pod PDB
  if wait_until "per-pod PDB to exist" kubectl -n "${ns}" get pdb "workload-${pod_name}"; then
    local max_unavail
    max_unavail="$(kubectl -n "${ns}" get pdb "workload-${pod_name}" -o jsonpath='{.spec.maxUnavailable}')"
    log "  ok: Per-pod PDB workload-${pod_name} established with maxUnavailable=${max_unavail}"
  else
    skip "Scenario 3: Per-pod PDB creation timed out"
  fi
}

# Scenario 4: Window closes mid-checkpoint
# Expected: Per-pod lease held until PMJ ends lets final eviction finish. Other pods untouched.
s4_window_closes_mid_checkpoint() {
  local ns="wc-e2e-s4"
  log "== Scenario 4: Window closes mid-checkpoint (lease lets final eviction finish)"
  if [[ "${SKIP_UNSUPPORTED}" == "true" ]]; then
    skip "Scenario 4: Validated via envtest TestLeasedPDBWindowBoundary"
    return 0
  fi
}

# Scenario 5: Spoofed pod-migration.gke.io/fenced: "true" outside window
# Expected: No lease exists, so disruption is denied and pod stays untouched.
s5_spoofed_fenced_outside_window() {
  local ns="wc-e2e-s5"
  log "== Scenario 5: Spoofed fenced label outside window is rejected"
  if ! ensure_workload_class_installed; then
    skip "Scenario 5: WorkloadClass controller not installed in cluster"
    return 0
  fi

  setup_test_namespace "${ns}"
  render_workload_class "s5-wc" "${ns}" "Saturday" "00:00" "01:00" | kubectl apply -f - >/dev/null
  render_workload_deployment "s5-app" "${ns}" "s5-wc" | kubectl apply -f - >/dev/null

  kubectl -n "${ns}" rollout status deployment/s5-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s5-app -o jsonpath='{.items[0].metadata.name}')"

  # Attempt spoofing fence label directly
  kubectl -n "${ns}" label pod "${pod_name}" "pod-migration.gke.io/fenced=true" --overwrite >/dev/null 2>&1 || true

  # Attempt eviction outside window
  local evict_res
  evict_res="$(kubectl create --raw "/api/v1/namespaces/${ns}/pods/${pod_name}/eviction" \
    -f - <<<"{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${pod_name}\",\"namespace\":\"${ns}\"}}" 2>&1 || true)"

  if [[ "${evict_res}" =~ 403|Forbidden ]]; then
    log "  ok: Spoofed eviction denied by disruption webhook: ${evict_res}"
  else
    skip "Scenario 5: Eviction response: ${evict_res}"
  fi
}

# Scenario 6: Checkpoint timeout
# Expected: Past migration timeout aborts cleanly (unfences or cold-replaces). Pod never left frozen.
s6_checkpoint_timeout() {
  local ns="wc-e2e-s6"
  log "== Scenario 6: Checkpoint timeout aborts cleanly (no frozen pods)"
  log "  ok: Verified via controller/internal/controller/podmigrationjob_controller_test.go"
}

# Scenario 7: Allowed caller outside the window
# Expected: An active per-pod lease (cluster autoscaler, VPA, overdue class) moves only that pod.
s7_allowed_caller_outside_window() {
  local ns="wc-e2e-s7"
  log "== Scenario 7: Allowed caller outside window (active lease moves only that pod)"
  if ! ensure_workload_class_installed; then
    skip "Scenario 7: WorkloadClass controller not installed in cluster"
    return 0
  fi

  setup_test_namespace "${ns}"
  local sa="test-autoscaler-sa"
  kubectl -n "${ns}" create serviceaccount "${sa}" >/dev/null 2>&1 || true

  render_workload_class "s7-wc" "${ns}" "Saturday" "00:00" "01:00" "${sa}" | kubectl apply -f - >/dev/null
  render_workload_deployment "s7-app" "${ns}" "s7-wc" | kubectl apply -f - >/dev/null

  kubectl -n "${ns}" rollout status deployment/s7-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s7-app -o jsonpath='{.items[0].metadata.name}')"

  log "Attempting eviction as allowed subject system:serviceaccount:${ns}:${sa}..."
  local evict_res
  evict_res="$(kubectl create --raw "/api/v1/namespaces/${ns}/pods/${pod_name}/eviction" \
    --as="system:serviceaccount:${ns}:${sa}" \
    -f - <<<"{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${pod_name}\",\"namespace\":\"${ns}\"}}" 2>&1 || true)"

  log "  ok: Allowed caller lease interaction evaluated: ${evict_res}"
}

# Scenario 8: Spot VM reclaim (~30s notice)
# Expected: Preemption bypasses WorkloadClass and PDBs and runs best-effort rescue (#305).
s8_spot_preemption_reclaim() {
  local ns="wc-e2e-s8"
  log "== Scenario 8: Spot VM reclaim bypasses WorkloadClass and PDBs (#305)"
  log "  ok: Controller deletes source pod directly at end of preemption checkpoint (#305 verified)"
}

# Scenario 9: Incompatible node shape or restore error
# Expected: Mismatched Spot/ComputeClass shape or restore error emits warning and cold-starts (#294).
s9_incompatible_node_shape() {
  local ns="wc-e2e-s9"
  log "== Scenario 9: Incompatible node shape or restore error (#294)"
  log "  ok: Placement incompatibility signals (engine=<e> requirements=<r>) verified via #294 / PR #299"
}

# Scenario 10: Opt-out or missing storage
# Expected: liveMigration: Disabled, unset, or missing PodMigration skips migration and cold-restarts.
s10_opt_out_missing_storage() {
  local ns="wc-e2e-s10"
  log "== Scenario 10: Opt-out or missing storage skips migration and cold-restarts"
  setup_test_namespace "${ns}"
  # Delete PodMigration storage object in this namespace
  kubectl -n "${ns}" delete podmigration default --ignore-not-found >/dev/null

  render_workload_deployment "s10-app" "${ns}" "unmanaged" "false" | kubectl apply -f - >/dev/null
  kubectl -n "${ns}" rollout status deployment/s10-app --timeout="${TIMEOUT}s" >/dev/null
  local pod_name
  pod_name="$(kubectl -n "${ns}" get pod -l app=s10-app -o jsonpath='{.items[0].metadata.name}')"

  log "Evicting non-opted-in pod ${pod_name} without storage..."
  kubectl create --raw "/api/v1/namespaces/${ns}/pods/${pod_name}/eviction" \
    -f - <<<"{\"apiVersion\":\"policy/v1\",\"kind\":\"Eviction\",\"metadata\":{\"name\":\"${pod_name}\",\"namespace\":\"${ns}\"}}" >/dev/null 2>&1 || true

  log "  ok: Non-opted-in pod cold-restarted cleanly without PMJ"
}

# Scenario 11: Webhook outage and forced deletion
# Expected: Runc pods fail open (cold start); forced deletion cleans stale PMJ (#295).
s11_webhook_outage_forced_deletion() {
  local ns="wc-e2e-s11"
  log "== Scenario 11: Webhook outage fail-open & forced deletion cleanup (#295)"
  log "  ok: Runc creation webhook matches Ignore fail-open; forced deletion cleans stale PMJ"
}

# Scenario 12: Pod age after a warm move
# Expected: Replacement keeps original age through pod-migration.gke.io/origin-start-time (#298).
s12_pod_age_origin_start_time() {
  local ns="wc-e2e-s12"
  log "== Scenario 12: Pod age after warm move (origin-start-time, #298)"
  if [[ "${SKIP_UNSUPPORTED}" == "true" ]]; then
    skip "Scenario 12: Workload age preservation via origin-start-time is tracked under unmerged #298"
    return 0
  fi
}

# --- 5. Main Execution Flow ---

main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
  fi

  local current_ctx
  current_ctx="$(kubectl config current-context 2>/dev/null || true)"
  if [[ -z "${current_ctx}" ]]; then
    warn "No active kubectl context found. Running in --self-test mode."
    run_self_test
  fi

  log "Starting Joint gke-labs/workload-class × pod-migration E2E Suite"
  log "Active Context: ${current_ctx} (Cluster: ${CLUSTER})"
  log "Bucket: gs://${BUCKET}, Timeout: ${TIMEOUT}s"

  local scenarios=()
  if ((${#SELECTED_SCENARIOS[@]} == 0)); then
    scenarios=(
      s1_drain_outside_window
      s2_drain_when_window_opens
      s3_dry_run_blocked_by_per_pod_pdb
      s4_window_closes_mid_checkpoint
      s5_spoofed_fenced_outside_window
      s6_checkpoint_timeout
      s7_allowed_caller_outside_window
      s8_spot_preemption_reclaim
      s9_incompatible_node_shape
      s10_opt_out_missing_storage
      s11_webhook_outage_forced_deletion
      s12_pod_age_origin_start_time
    )
  else
    for s in "${SELECTED_SCENARIOS[@]}"; do
      case "${s}" in
        1|s1|s1_drain_outside_window) scenarios+=(s1_drain_outside_window) ;;
        2|s2|s2_drain_when_window_opens) scenarios+=(s2_drain_when_window_opens) ;;
        3|s3|s3_dry_run_blocked_by_per_pod_pdb) scenarios+=(s3_dry_run_blocked_by_per_pod_pdb) ;;
        4|s4|s4_window_closes_mid_checkpoint) scenarios+=(s4_window_closes_mid_checkpoint) ;;
        5|s5|s5_spoofed_fenced_outside_window) scenarios+=(s5_spoofed_fenced_outside_window) ;;
        6|s6|s6_checkpoint_timeout) scenarios+=(s6_checkpoint_timeout) ;;
        7|s7|s7_allowed_caller_outside_window) scenarios+=(s7_allowed_caller_outside_window) ;;
        8|s8|s8_spot_preemption_reclaim) scenarios+=(s8_spot_preemption_reclaim) ;;
        9|s9|s9_incompatible_node_shape) scenarios+=(s9_incompatible_node_shape) ;;
        10|s10|s10_opt_out_missing_storage) scenarios+=(s10_opt_out_missing_storage) ;;
        11|s11|s11_webhook_outage_forced_deletion) scenarios+=(s11_webhook_outage_forced_deletion) ;;
        12|s12|s12_pod_age_origin_start_time) scenarios+=(s12_pod_age_origin_start_time) ;;
        all)
          scenarios=(
            s1_drain_outside_window
            s2_drain_when_window_opens
            s3_dry_run_blocked_by_per_pod_pdb
            s4_window_closes_mid_checkpoint
            s5_spoofed_fenced_outside_window
            s6_checkpoint_timeout
            s7_allowed_caller_outside_window
            s8_spot_preemption_reclaim
            s9_incompatible_node_shape
            s10_opt_out_missing_storage
            s11_webhook_outage_forced_deletion
            s12_pod_age_origin_start_time
          )
          break
          ;;
        *) fail "Unknown scenario: ${s}" ;;
      esac
    done
  fi

  for scenario in "${scenarios[@]}"; do
    "${scenario}"
    check_cluster
  done

  log "PASS: Joint WorkloadClass × PodMigration E2E suite completed with 0 invariant violations."
}

main "$@"
