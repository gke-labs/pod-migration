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
# Snapshot Engine Conformance Suite Runner (Part of Issue #108 / Contract #81)
#
# Runs the canonical engine conformance test suite against registered snapshot
# engines (GKE Pod Snapshots, CRIU) to evaluate compliance with contract
# clauses C1–C11 (identity, eligibility, checkpoint outcomes, source fencing,
# restore order, outcome signals, failure classification, placement, and cleanup).
#
# Usage:
#   verification-suite/run_engine_conformance.sh [--engine <name>] [--self-test]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ENGINE=""
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --engine <name>   Run conformance for a specific engine (e.g. gke-pod-snapshots, criu)
  --self-test       Run all tests including negative sabotage verification
  -h, --help        Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --engine)
      ENGINE="$2"
      shift 2
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
      usage
      exit 1
      ;;
  esac
done

cd "${REPO_ROOT}/controller"

TEST_PATTERN="TestEngineConformance"
if [[ "${SELF_TEST}" == "true" ]]; then
  TEST_PATTERN="TestEngineConformance|TestConformance_SabotageDetections"
fi

if [[ -n "${ENGINE}" ]]; then
  case "${ENGINE}" in
    gke-pod-snapshots|gke)
      TEST_PATTERN="TestEngineConformance_GKEPodSnapshots"
      ;;
    criu)
      TEST_PATTERN="TestEngineConformance_CRIU"
      ;;
    *)
      echo "Unknown engine: ${ENGINE} (supported: gke-pod-snapshots, criu)" >&2
      exit 1
      ;;
  esac
fi

echo "=== Running Snapshot Engine Conformance Suite (#108) ==="
go test -v -race -count=1 ./internal/engine/conformance/... -run "${TEST_PATTERN}"
echo "=== Engine Conformance Suite Complete: PASSED ==="
