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
# Verifies that the Go third-party notice collection blocks in the shipped
# Dockerfiles stay in sync and fail closed on missing/unreadable licenses,
# source-code false positives (license.go), NOTICE-only modules, and nested
# subpackage licenses.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "Running Dockerfile notice-collection unit tests..."

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/dockerfile-notices-test.XXXXXX")"
trap 'chmod -R u+w "${TMP_DIR}" 2>/dev/null || true; rm -rf "${TMP_DIR}"' EXIT

extract_block() {
  local df="$1"
  python3 - "${df}" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
m = re.search(r"(sh -eu -c '.*?'\s+_\s+\S+)", text, re.S)
if not m:
    sys.exit(f"notice block not found in {sys.argv[1]}")
block = m.group(1).replace("\\\n", "")
# Normalize trailing target package argument for sync comparison
block = re.sub(r"'\s+_\s+\S+$", "' _ TARGET_PKG", block)
print(block)
PY
}

B_CTRL="$(extract_block "${REPO_ROOT}/controller/Dockerfile")"
B_ENG="$(extract_block "${REPO_ROOT}/engines/criu/build/engine/Dockerfile")"
B_AGT="$(extract_block "${REPO_ROOT}/engines/criu/build/agent/Dockerfile")"

if [[ "${B_CTRL}" != "${B_ENG}" ]] || [[ "${B_CTRL}" != "${B_AGT}" ]]; then
  echo "FAIL: notice-collection blocks drifted across Dockerfiles" >&2
  diff -u <(printf '%s\n' "${B_CTRL}") <(printf '%s\n' "${B_ENG}") >&2 || true
  diff -u <(printf '%s\n' "${B_CTRL}") <(printf '%s\n' "${B_AGT}") >&2 || true
  exit 1
fi

# Build a synthetic fixture with mock `go` binary so tests run fast & offline.
MOCK_BIN="${TMP_DIR}/bin"
FIX="${TMP_DIR}/mods"
mkdir -p "${MOCK_BIN}" "${FIX}/goroot"
echo "BSD-3-Clause Go stdlib" > "${FIX}/goroot/LICENSE"

# mod1: Apache-2.0 + NOTICE + shared read-only ancestor BSD LICENSE across two sibling subpackages + license.go
mkdir -p "${FIX}/example.com/mod1/pkg/internal/sub/a" "${FIX}/example.com/mod1/pkg/internal/sub/b"
echo "Apache License Version 2.0" > "${FIX}/example.com/mod1/LICENSE"
echo "Copyright Notice" > "${FIX}/example.com/mod1/NOTICE"
echo "package mod1" > "${FIX}/example.com/mod1/license.go"
echo "BSD Nested Copyright" > "${FIX}/example.com/mod1/pkg/internal/sub/LICENSE"
chmod 0444 "${FIX}/example.com/mod1/LICENSE" "${FIX}/example.com/mod1/pkg/internal/sub/LICENSE"

# mod2: MPL-2.0 reciprocal
mkdir -p "${FIX}/example.com/mod2"
echo "Mozilla Public License Version 2.0" > "${FIX}/example.com/mod2/LICENSE.MPL-2.0"
echo "package mod2" > "${FIX}/example.com/mod2/mod2.go"

# mod3: Apache-2.0 code + CC-BY-SA LICENSE.docs (docs-only, should NOT trigger reciprocal)
mkdir -p "${FIX}/example.com/mod3"
echo "Apache License Version 2.0" > "${FIX}/example.com/mod3/LICENSE"
echo "Attribution-ShareAlike 4.0 International" > "${FIX}/example.com/mod3/LICENSE.docs"

# mod4: EPL-2.0 reciprocal
mkdir -p "${FIX}/example.com/mod4"
echo "Eclipse Public License - v 2.0" > "${FIX}/example.com/mod4/LICENSE"

DEPS_FILE="${TMP_DIR}/mock_deps.txt"
cat > "${DEPS_FILE}" <<EOF
example.com/mod1 v1.0.0 ${FIX}/example.com/mod1 ${FIX}/example.com/mod1/pkg/internal/sub/a
example.com/mod1 v1.0.0 ${FIX}/example.com/mod1 ${FIX}/example.com/mod1/pkg/internal/sub/b
example.com/mod2 v0.5.1 ${FIX}/example.com/mod2 ${FIX}/example.com/mod2
example.com/mod3 v1.0.0 ${FIX}/example.com/mod3 ${FIX}/example.com/mod3
example.com/mod4 v2.0.0 ${FIX}/example.com/mod4 ${FIX}/example.com/mod4
EOF

cat > "${MOCK_BIN}/go" <<EOF
#!/usr/bin/env bash
if [[ "\$1" == "env" && "\$2" == "GOROOT" ]]; then
  echo "${FIX}/goroot"
  exit 0
fi
if [[ "\$1" == "list" ]]; then
  cat "\${MOCK_DEPS_FILE}"
  exit 0
fi
exit 1
EOF
chmod +x "${MOCK_BIN}/go"

run_notice_block() {
  local out_dir="$1"
  local deps_file="$2"
  local script="${B_CTRL//\/third-party-notices/${out_dir}}"
  script="${script//\/tmp\/go-deps-pkgs.txt/${TMP_DIR}/pkgs.txt}"
  script="${script//\/tmp\/go-deps-mods.txt/${TMP_DIR}/mods.txt}"
  script="${script//\/tmp\/go-deps-subdirs.txt/${TMP_DIR}/subdirs.txt}"
  script="${script//TARGET_PKG/./cmd}"
  PATH="${MOCK_BIN}:${PATH}" MOCK_DEPS_FILE="${deps_file}" eval "${script}"
}

# 1. Happy path: collects root + nested subpackage licenses, skips license.go,
#    bundles reciprocal source for MPL & EPL but not LICENSE.docs.
OUT1="${TMP_DIR}/out1"
run_notice_block "${OUT1}" "${DEPS_FILE}"

[[ -f "${OUT1}/go-stdlib/LICENSE" ]] || { echo "FAIL: missing stdlib LICENSE" >&2; exit 1; }
[[ -f "${OUT1}/example.com/mod1/LICENSE" ]] || { echo "FAIL: missing mod1 LICENSE" >&2; exit 1; }
[[ -f "${OUT1}/example.com/mod1/NOTICE" ]] || { echo "FAIL: missing mod1 NOTICE" >&2; exit 1; }
[[ ! -e "${OUT1}/example.com/mod1/license.go" ]] || { echo "FAIL: license.go should not be copied" >&2; exit 1; }
[[ -f "${OUT1}/example.com/mod1/pkg/internal/sub/LICENSE" ]] || { echo "FAIL: missing nested subpackage LICENSE" >&2; exit 1; }
[[ -f "${OUT1}/reciprocal-source/example.com/mod2/mod2.go" ]] || { echo "FAIL: missing MPL reciprocal source" >&2; exit 1; }
[[ -f "${OUT1}/reciprocal-source/example.com/mod4/LICENSE" ]] || { echo "FAIL: missing EPL reciprocal source" >&2; exit 1; }
[[ ! -d "${OUT1}/reciprocal-source/example.com/mod3" ]] || { echo "FAIL: LICENSE.docs should not trigger reciprocal source" >&2; exit 1; }

# 2. Sabotage: read-only target directory causes cp failure -> must exit non-zero!
OUT_RO="${TMP_DIR}/out_ro"
mkdir -p "${OUT_RO}/example.com/mod1"
chmod 0500 "${OUT_RO}/example.com/mod1"
if run_notice_block "${OUT_RO}" "${DEPS_FILE}" >/dev/null 2>&1; then
  echo "FAIL: expected non-zero exit when cp fails on read-only target dir" >&2
  exit 1
fi
chmod -R u+w "${OUT_RO}"

# 3. Sabotage: module with NOTICE + license.go only (no real LICENSE) -> must exit non-zero!
mkdir -p "${FIX}/example.com/noticeonly"
echo "Notice only" > "${FIX}/example.com/noticeonly/NOTICE"
echo "package noticeonly" > "${FIX}/example.com/noticeonly/license.go"
BAD_DEPS="${TMP_DIR}/bad_deps.txt"
echo "example.com/noticeonly v0.1.0 ${FIX}/example.com/noticeonly ${FIX}/example.com/noticeonly" > "${BAD_DEPS}"
if run_notice_block "${TMP_DIR}/out_bad" "${BAD_DEPS}" >/dev/null 2>&1; then
  echo "FAIL: expected non-zero exit on NOTICE + license.go without LICENSE" >&2
  exit 1
fi

echo "PASS: all Dockerfile notice-collection tests passed."
