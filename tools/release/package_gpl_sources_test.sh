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

# 6. Test --internal-fetch-distro executes exact pkg=ver apt-get calls
MOCK_BIN_DIR="${TMP_TEST_DIR}/mock-bin"
mkdir -p "${MOCK_BIN_DIR}"
MOCK_LOG="${TMP_TEST_DIR}/mock_apt.log"

cat <<EOF > "${MOCK_BIN_DIR}/apt-get"
#!/usr/bin/env bash
echo "apt-get \$*" >> "${MOCK_LOG}"
exit 0
EOF
chmod +x "${MOCK_BIN_DIR}/apt-get"

TWO_PKG_MANIFEST="${TMP_TEST_DIR}/two_pkgs.txt"
cat <<'EOF' > "${TWO_PKG_MANIFEST}"
criu 4.0-2
libnl-3-200 3.7.0-0.3+b1
EOF

MOCK_OUT_DIR="${TMP_TEST_DIR}/mock_out"
mkdir -p "${MOCK_OUT_DIR}"

PATH="${MOCK_BIN_DIR}:${PATH}" "${TARGET_SCRIPT}" \
  --internal-fetch-distro debian \
  --manifest "${TWO_PKG_MANIFEST}" \
  --out-dir "${MOCK_OUT_DIR}"

if ! grep -q "apt-get source -d criu=4.0-2" "${MOCK_LOG}"; then
  echo "FAIL: expected 'apt-get source -d criu=4.0-2' in log: $(cat "${MOCK_LOG}")" >&2
  exit 1
fi
if ! grep -q "apt-get source -d libnl-3-200=3.7.0-0.3+b1" "${MOCK_LOG}"; then
  echo "FAIL: expected 'apt-get source -d libnl-3-200=3.7.0-0.3+b1' in log: $(cat "${MOCK_LOG}")" >&2
  exit 1
fi

# 7. Test snapshot fallback is invoked when apt-get source fails, and fails cleanly when package is missing
MOCK_FAIL_BIN="${TMP_TEST_DIR}/mock-fail-bin"
mkdir -p "${MOCK_FAIL_BIN}"
MOCK_FAIL_LOG="${TMP_TEST_DIR}/mock_fail_apt.log"

cat <<EOF > "${MOCK_FAIL_BIN}/apt-get"
#!/usr/bin/env bash
echo "apt-get \$*" >> "${MOCK_FAIL_LOG}"
exit 1
EOF
chmod +x "${MOCK_FAIL_BIN}/apt-get"

MOCK_FAIL_MANIFEST="${TMP_TEST_DIR}/missing_pkg.txt"
cat <<'EOF' > "${MOCK_FAIL_MANIFEST}"
nonexistent-pkg 999.0.0
EOF

if PATH="${MOCK_FAIL_BIN}:${PATH}" "${TARGET_SCRIPT}" \
  --internal-fetch-distro debian \
  --manifest "${MOCK_FAIL_MANIFEST}" \
  --out-dir "${MOCK_OUT_DIR}" >/dev/null 2>&1; then
  echo "FAIL: expected fetch to fail when both apt-get and snapshot fail" >&2
  exit 1
fi

if ! grep -q "apt-get source -d nonexistent-pkg=999.0.0" "${MOCK_FAIL_LOG}"; then
  echo "FAIL: expected apt-get call before fallback" >&2
  exit 1
fi

echo "PASS: all package_gpl_sources tests passed."
