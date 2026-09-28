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
# Track 5 (T5) Automated Invariant Evolution Pipeline Suite (Issue #54 / Epic #49)
#
# Orchestrates:
#   1. T0 Two-Lane CODEOWNERS Governance Gate (verification-suite/verify_invariant_governance.sh)
#   2. Trigger A (Blind-Spot Detector): pmprofiler analyze -> invariant-gen pipeline
#   3. Trigger B / C (/extract-invariant & bugfix PR extraction)
#   4. Automated RED / GREEN Replay Verification against captured records.ndjson traces

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
PMPROFILER_SRC="${REPO_ROOT}/tools/pmprofiler"
INVARIANT_GEN_SRC="${REPO_ROOT}/tools/invariant-gen"

OUT_DIR="${OUT_DIR:-/tmp/pmprofiler-t5-runs}"
RUN_DIR=""
GREEN_RECORDS=""
INVARIANT_ID="I10"
COMMENT_BODY=""
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --run <dir>                 pmprofiler run directory to inspect for blind spots (Trigger A)
  --green-records <csv>       Comma-separated clean records.ndjson paths for GREEN replay
  --comment <text>            Extract candidate invariant from /extract-invariant comment (Trigger C)
  --invariant-id <I_N>        Candidate invariant ID (default: I10)
  --out-dir <dir>             Output directory for generated invariant artifacts
  --self-test                 Run offline end-to-end self-test (T0 governance + Trigger A/C + RED/GREEN gates)
  -h, --help                  Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --run)
      RUN_DIR="$2"
      shift 2
      ;;
    --green-records)
      GREEN_RECORDS="$2"
      shift 2
      ;;
    --comment)
      COMMENT_BODY="$2"
      shift 2
      ;;
    --invariant-id)
      INVARIANT_ID="$2"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="$2"
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
INVARIANT_GEN_BIN=""

build_binaries() {
  mkdir -p "${OUT_DIR}/bin"
  PMPROFILER_BIN="${OUT_DIR}/bin/pmprofiler"
  INVARIANT_GEN_BIN="${OUT_DIR}/bin/invariant-gen"
  log "Building pmprofiler -> ${PMPROFILER_BIN}"
  (cd "${PMPROFILER_SRC}" && go build -o "${PMPROFILER_BIN}" ./cmd/pmprofiler)
  log "Building invariant-gen -> ${INVARIANT_GEN_BIN}"
  (cd "${INVARIANT_GEN_SRC}" && go build -o "${INVARIANT_GEN_BIN}" .)
}

run_self_test() {
  log "=== Running Track 5 Invariant Evolution Pipeline Self-Test ==="

  # 1. Verify T0 Two-Lane CODEOWNERS Governance self-test
  "${SCRIPT_DIR}/verify_invariant_governance.sh" --self-test

  build_binaries

  local base_dir="${OUT_DIR}/self-test"
  rm -rf "${base_dir}"
  mkdir -p "${base_dir}"

  local pmj_gvr="podmigrationjobs.v1alpha1.podmigration.gke.io"
  local pod_gvr="pods.v1"
  local ps_gvr="podsnapshots.v1alpha1.podsnapshot.gke.io"

  # 2. Construct a clean baseline trace (5 healthy migrations) and verify 0 blind spots
  local clean_dir="${base_dir}/clean_run"
  mkdir -p "${clean_dir}"
  cat > "${clean_dir}/meta.json" <<EOF
{"scenario":"T5 clean baseline run","startedAt":"2026-09-28T12:00:00Z"}
EOF
  : > "${clean_dir}/records.ndjson"
  for i in $(seq 1 5); do
    cat >> "${clean_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T11:59:00Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"app-${i}","uid":"uid-src-${i}","creationTimestamp":"2026-09-28T11:59:00Z","labels":{"app":"counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T11:59:05Z"}]}}}
{"ts":"2026-09-28T12:00:01Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-clean-${i}","uid":"uid-pmj-${i}","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"app-${i}"},"targetPodUID":"uid-src-${i}"},"status":{"phase":"Snapshotting","snapshotName":"ps-clean-${i}"}}}
{"ts":"2026-09-28T12:00:02Z","type":"add","gvr":"${ps_gvr}","obj":{"metadata":{"name":"ps-clean-${i}","namespace":"default"},"spec":{"podName":"app-${i}"},"status":{"conditions":[{"type":"Checkpoint","status":"False","reason":"InProgress"},{"type":"StorageReplicated","status":"False","reason":"AwaitingCheckpoint"},{"type":"Ready","status":"False","reason":"InProgress"}]}}}
{"ts":"2026-09-28T12:00:05Z","type":"update","gvr":"${ps_gvr}","obj":{"metadata":{"name":"ps-clean-${i}","namespace":"default"},"spec":{"podName":"app-${i}"},"status":{"conditions":[{"type":"Checkpoint","status":"True","reason":"Succeeded"},{"type":"StorageReplicated","status":"True","reason":"Succeeded"},{"type":"Ready","status":"True","reason":"AllSnapshotsAvailable"}]}}}
{"ts":"2026-09-28T12:00:06Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"app-${i}","uid":"uid-src-${i}","creationTimestamp":"2026-09-28T11:59:00Z","deletionTimestamp":"2026-09-28T12:00:06Z","labels":{"app":"counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-a"},"status":{}}}
{"ts":"2026-09-28T12:00:07Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"app-${i}-dst","uid":"uid-dst-${i}","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"counter","pod-migration.gke.io/enabled":"true"},"annotations":{"podsnapshot.gke.io/ps-name":"ps-clean-${i}"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{}}}
{"ts":"2026-09-28T12:00:09Z","type":"update","gvr":"${pod_gvr}","obj":{"metadata":{"name":"app-${i}-dst","uid":"uid-dst-${i}","creationTimestamp":"2026-09-28T12:00:07Z","labels":{"app":"counter","pod-migration.gke.io/enabled":"true"},"annotations":{"podsnapshot.gke.io/ps-name":"ps-clean-${i}"}},"spec":{"nodeName":"node-b"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T12:00:13Z"}]}}}
{"ts":"2026-09-28T12:00:14Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-clean-${i}","uid":"uid-pmj-${i}","creationTimestamp":"2026-09-28T12:00:01Z"},"spec":{"podRef":{"name":"app-${i}"},"targetPodUID":"uid-src-${i}"},"status":{"phase":"Succeeded","snapshotName":"ps-clean-${i}","evictingStartTime":"2026-09-28T12:00:06Z","restoredPodName":"app-${i}-dst","restoredPodUID":"uid-dst-${i}","conditions":[{"type":"Restored","status":"True","reason":"RestoreVerified"}]}}}
EOF
  done
  "${PMPROFILER_BIN}" analyze --run "${clean_dir}" --assert-zero-invariants --assert-clean-outcomes
  "${INVARIANT_GEN_BIN}" --mode detect --run "${clean_dir}" --out-dir "${clean_dir}/detect"
  local clean_count
  clean_count="$(jq 'length' "${clean_dir}/detect/blindspots.json")"
  [[ "${clean_count}" -eq 0 ]] || die "Expected 0 blind spots on clean run, got ${clean_count}"
  log "Verified clean trace produces 0 blind spots"

  # 3. Trigger A (Blind-Spot Detector): Synthetic self-test for wedged-restoring-orphan
  local bs1_dir="${base_dir}/blindspot_wedged_restoring"
  mkdir -p "${bs1_dir}"
  cat > "${bs1_dir}/meta.json" <<EOF
{"scenario":"T5 synthetic self-test: wedged-restoring-orphan blind spot","startedAt":"2026-09-28T00:26:55Z"}
EOF
  cat > "${bs1_dir}/records.ndjson" <<EOF
{"ts":"2026-09-28T00:26:50Z","type":"list","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-counter-0","uid":"uid-wedge-src","creationTimestamp":"2026-09-28T00:25:00Z","labels":{"app":"t3-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"nodeName":"node-a"},"status":{"conditions":[{"type":"Ready","status":"True","lastTransitionTime":"2026-09-28T00:25:05Z"}]}}}
{"ts":"2026-09-28T00:26:58Z","type":"add","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-wedge-0","uid":"uid-pmj-wedge","creationTimestamp":"2026-09-28T00:26:58Z"},"spec":{"podRef":{"name":"t3-counter-0"},"targetPodUID":"uid-wedge-src"},"status":{"phase":"Snapshotting","snapshotRef":"ps-wedge-0"}}}
{"ts":"2026-09-28T00:27:05Z","type":"update","gvr":"${pmj_gvr}","obj":{"metadata":{"name":"pmj-wedge-0","uid":"uid-pmj-wedge","creationTimestamp":"2026-09-28T00:26:58Z"},"spec":{"podRef":{"name":"t3-counter-0"},"targetPodUID":"uid-wedge-src"},"status":{"phase":"Restoring","snapshotRef":"ps-wedge-0","restoredPodName":"t3-counter-0-dst","restoringStartTime":"2026-09-28T00:27:05Z"}}}
{"ts":"2026-09-28T00:27:06Z","type":"add","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-counter-0-dst","uid":"uid-wedge-dst","creationTimestamp":"2026-09-28T00:27:06Z","labels":{"app":"t3-counter","pod-migration.gke.io/enabled":"true"}},"spec":{"schedulingGates":[{"name":"pod-migration.gke.io/restoring"}]},"status":{"phase":"Pending"}}}
{"ts":"2026-09-28T00:38:00Z","type":"delete","gvr":"${pod_gvr}","obj":{"metadata":{"name":"t3-counter-0-dst","uid":"uid-wedge-dst"}}}
EOF
  "${PMPROFILER_BIN}" analyze --run "${bs1_dir}" --assert-zero-invariants
  local i10_out="${base_dir}/out_i10"
  "${INVARIANT_GEN_BIN}" \
    --mode pipeline \
    --run "${bs1_dir}" \
    --green-records "${clean_dir}/records.ndjson" \
    --invariant-id "I10" \
    --out-dir "${i10_out}"
  [[ "$(jq -r '.compiledAndTested' "${i10_out}/proof.json")" == "true" ]] || die "I10 was not compiled and tested via go test"
  [[ "$(jq -r '.redPassed' "${i10_out}/proof.json")" == "true" ]] || die "I10 RED proof did not pass"
  [[ "$(jq -r '.greenPassed' "${i10_out}/proof.json")" == "true" ]] || die "I10 GREEN proof did not pass"
  [[ -s "${i10_out}/testdata/i10_wedged_restoring_orphan_snapshot.json" ]] || die "Missing I10 snapshot fixture"
  [[ -s "${i10_out}/testdata/i10_wedged_restoring_orphan_green_snapshots.json" ]] || die "Missing I10 green snapshots fixture"
  [[ -s "${i10_out}/i10_wedged_restoring_orphan_rule.go" ]] || die "Missing I10 rule.go"
  [[ -s "${i10_out}/i10_wedged_restoring_orphan_test.go" ]] || die "Missing I10 test.go"
  [[ -s "${i10_out}/pr_body.md" ]] || die "Missing I10 pr_body.md"
  log "Verified Trigger A (wedged-restoring-orphan blind spot) -> I10 compiled go test RED/GREEN proof PASSED"

  # 4. Trigger C (/extract-invariant slash command) + Negative Honesty Gate
  local i11_out="${base_dir}/out_i11"
  "${INVARIANT_GEN_BIN}" \
    --mode extract-comment \
    --comment "/extract-invariant I11 unintended-cold-start-active-pmj Replacement pod cold-started while PMJ remained in PhaseRestoring" \
    --green-records "${clean_dir}/records.ndjson" \
    --out-dir "${i11_out}"
  [[ "$(jq -r '.compiledAndTested' "${i11_out}/proof.json")" == "true" ]] || die "I11 was not compiled and tested via go test"
  [[ "$(jq -r '.redPassed' "${i11_out}/proof.json")" == "true" ]] || die "I11 RED proof did not pass"
  [[ "$(jq -r '.greenPassed' "${i11_out}/proof.json")" == "true" ]] || die "I11 GREEN proof did not pass"

  local i12_custom_out="${base_dir}/out_i12_custom"
  if "${INVARIANT_GEN_BIN}" \
    --mode extract-comment \
    --comment "/extract-invariant I12 custom-unauthored-check requires manual predicate" \
    --require-red-green=true \
    --out-dir "${i12_custom_out}" >/dev/null 2>&1; then
    die "Negative gate failed: custom scaffold without predicate should fail --require-red-green=true"
  fi
  log "Verified negative gate: unauthored custom scaffold fails compiled go test RED proof under --require-red-green=true"
  log "=== Track 5 Invariant Evolution Pipeline Self-Test PASSED ==="
}

main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
    exit 0
  fi

  build_binaries

  if [[ -n "${COMMENT_BODY}" ]]; then
    "${INVARIANT_GEN_BIN}" \
      --mode extract-comment \
      --comment "${COMMENT_BODY}" \
      --green-records "${GREEN_RECORDS}" \
      --invariant-id "${INVARIANT_ID}" \
      --out-dir "${OUT_DIR}"
    exit 0
  fi

  if [[ -n "${RUN_DIR}" ]]; then
    "${INVARIANT_GEN_BIN}" \
      --mode pipeline \
      --run "${RUN_DIR}" \
      --green-records "${GREEN_RECORDS}" \
      --invariant-id "${INVARIANT_ID}" \
      --out-dir "${OUT_DIR}"
    exit 0
  fi

  usage >&2
  exit 2
}

main
