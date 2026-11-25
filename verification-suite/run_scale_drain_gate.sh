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
# 100-Pod Concurrent Drain Scale Regression Gate Runner (Part of Issue #251)
#
# Exercises an automated 100-pod concurrent eviction/drain wave under
# --invariant-mode=strict, asserting hard SLO gates on completion time,
# controller heap/RSS memory, zero OOMs, zero wedged scheduling gates,
# and zero uncached API storms.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONTROLLER_DIR="${REPO_ROOT}/controller"

SUMMARY_PATH="${SCALE_SUMMARY_PATH:-/tmp/scale-100-drain-summary.json}"
POD_COUNT="${SCALE_DRAIN_POD_COUNT:-100}"

echo "=== Running 100-Pod Concurrent Drain Scale Regression Gate (#251) ==="
echo "Target Pod Count: ${POD_COUNT}"
echo "Summary Output:   ${SUMMARY_PATH}"

# Ensure KUBEBUILDER_ASSETS is set via setup-envtest if not already present
if [[ -z "${KUBEBUILDER_ASSETS:-}" ]]; then
  SETUP_ENVTEST="${CONTROLLER_DIR}/bin/setup-envtest-v0.25.0"
  if [[ ! -x "${SETUP_ENVTEST}" ]]; then
    make -C "${CONTROLLER_DIR}" envtest
  fi
  KUBEBUILDER_ASSETS="$("${SETUP_ENVTEST}" use 1.36.2 -p path || "${SETUP_ENVTEST}" use -p path)"
  export KUBEBUILDER_ASSETS
fi

export SCALE_SUMMARY_PATH="${SUMMARY_PATH}"
export SCALE_DRAIN_POD_COUNT="${POD_COUNT}"
export RUN_SCALE_DRAIN_GATE=1

# Clean up stale summary file from previous runs
rm -f "${SUMMARY_PATH}"

# Run the envtest scale regression test
(
  cd "${CONTROLLER_DIR}"
  go test -v -timeout 120s -count=1 -run TestScale100PodConcurrentDrain ./internal/controller/...
)

if [[ ! -f "${SUMMARY_PATH}" ]]; then
  echo "Error: scale regression gate did not produce summary artifact at ${SUMMARY_PATH}" >&2
  exit 1
fi

echo "=== Scale Regression Gate Summary Artifact ==="
cat "${SUMMARY_PATH}"
echo ""
echo "PASS: 100-Pod Concurrent Drain Scale Regression Gate verified."
