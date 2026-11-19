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

TMP_TEST_DIR="$(mktemp -d "${TMPDIR:-/tmp}/pkg-gpl-test.XXXXXX")"
trap 'rm -rf "${TMP_TEST_DIR}"' EXIT

# 1. Test --help exits 0
"${TARGET_SCRIPT}" --help >/dev/null

# 2. Test --dry-run exits 0 and prints release target
OUT="$("${TARGET_SCRIPT}" --dry-run --version v1.2.3 --output-dir "${TMP_TEST_DIR}")"
if [[ "${OUT}" != *"v1.2.3"* ]]; then
  echo "FAIL: expected v1.2.3 in output, got: ${OUT}" >&2
  exit 1
fi

# Verify generated manifest in dry-run
MANIFEST="${TMP_TEST_DIR}/GPL_SOURCE_MANIFEST_v1.2.3.txt"
if [[ ! -f "${MANIFEST}" ]]; then
  echo "FAIL: expected manifest file at ${MANIFEST}" >&2
  exit 1
fi
if ! grep -q "WRITTEN OFFER FOR SOURCE CODE" "${MANIFEST}"; then
  echo "FAIL: expected written offer in manifest" >&2
  exit 1
fi
if ! grep -q "snapshot.debian.org" "${MANIFEST}"; then
  echo "FAIL: expected snapshot.debian.org in manifest" >&2
  exit 1
fi

# 3. Test with explicit --package-versions and --runtime-package-versions files
DEB_VERSIONS="${TMP_TEST_DIR}/debian_pkgs.txt"
UBUNTU_VERSIONS="${TMP_TEST_DIR}/ubuntu_pkgs.txt"
cat <<'EOF' > "${DEB_VERSIONS}"
criu 4.0-2
libnl-3-200 3.7.0-0.3+b1
nftables 1.1.1-1
EOF
cat <<'EOF' > "${UBUNTU_VERSIONS}"
util-linux 2.39.3-9ubuntu6.2
coreutils 9.4-3ubuntu6
bash 5.2.21-2ubuntu4
EOF

OUT2="$("${TARGET_SCRIPT}" --dry-run \
  --version v2.0.0 \
  --output-dir "${TMP_TEST_DIR}" \
  --package-versions "${DEB_VERSIONS}" \
  --runtime-package-versions "${UBUNTU_VERSIONS}")"

MANIFEST2="${TMP_TEST_DIR}/GPL_SOURCE_MANIFEST_v2.0.0.txt"
if [[ ! -f "${MANIFEST2}" ]]; then
  echo "FAIL: expected manifest file at ${MANIFEST2}" >&2
  exit 1
fi
if ! grep -q "criu 4.0-2" "${MANIFEST2}"; then
  echo "FAIL: expected criu 4.0-2 in manifest" >&2
  exit 1
fi
if ! grep -q "util-linux 2.39.3-9ubuntu6.2" "${MANIFEST2}"; then
  echo "FAIL: expected util-linux in manifest" >&2
  exit 1
fi

# 4. Test missing versions file fails cleanly
if "${TARGET_SCRIPT}" --package-versions "/nonexistent/path/pkg.txt" >/dev/null 2>&1; then
  echo "FAIL: expected failure on nonexistent package versions file" >&2
  exit 1
fi

# 5. Test invalid option fails
if "${TARGET_SCRIPT}" --invalid-flag >/dev/null 2>&1; then
  echo "FAIL: expected failure on unknown flag" >&2
  exit 1
fi

echo "PASS: all package_gpl_sources tests passed."
