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
# Unit test for package_gpl_sources.sh script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_SCRIPT="${SCRIPT_DIR}/package_gpl_sources.sh"

echo "Running package_gpl_sources unit tests..."

# 1. Test --help exits 0
"${TARGET_SCRIPT}" --help >/dev/null

# 2. Test --dry-run exits 0 and prints release target
OUT="$("${TARGET_SCRIPT}" --dry-run --version v1.2.3)"
if [[ "${OUT}" != *"v1.2.3"* ]]; then
  echo "FAIL: expected v1.2.3 in output, got: ${OUT}" >&2
  exit 1
fi

# 3. Test invalid option fails
if "${TARGET_SCRIPT}" --invalid-flag >/dev/null 2>&1; then
  echo "FAIL: expected failure on unknown flag" >&2
  exit 1
fi

echo "PASS: all package_gpl_sources tests passed."
