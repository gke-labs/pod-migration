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
# Fails if any Go file in the repo lacks the Apache-2.0 license header (#133).
#
# The expected header is the one engines/criu set and controller/hack/
# boilerplate.go.txt carries (controller-gen copies it into zz_generated files):
#
#   // SPDX-License-Identifier: Apache-2.0
#   // Copyright <year> Google LLC
#   //
#   // Licensed under the Apache License, Version 2.0 (the "License");
#   // ... rest of the standard Apache-2.0 notice ...
#
# Build constraints (//go:build, // +build) may sit above the header, followed
# by a blank line, as Go requires.  A blank line must also follow the header. Any four-digit year is accepted.
#
# Checks tracked and untracked (not ignored) files, so a new file fails locally
# before it is committed. Portable to macOS bash 3.2 and BSD/GNU userlands.
#
# Usage: verification-suite/verify_license_headers.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

# Paths (prefixes, relative to the repo root) that are not checked.
EXEMPT_PREFIXES="
vendor/
third_party/
controller/internal/invariants/
"
# controller/internal/invariants/: governance lock, not a licensing decision.
#   CODEOWNERS locks this directory and verification-suite/
#   verify_invariant_governance.sh fails any non-invariant PR that edits it
#   without the maintainer-applied 'allow-invariant-cochange' label, so the
#   header sweep for #133 left it alone. A follow-up PR carrying that label
#   should add the header to these files (and to tools/invariant-gen's output
#   templates) and then delete this entry.
# vendor/, third_party/: copied third-party code keeps its own headers. None
#   exists today; listed so a future import doesn't trip the check.

EXPECTED_HEADER='// SPDX-License-Identifier: Apache-2.0
// Copyright YEAR Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.'

is_exempt() {
  local path="$1" prefix
  for prefix in ${EXEMPT_PREFIXES}; do
    case "${path}" in
      "${prefix}"*|*/"${prefix}"*) return 0 ;;
    esac
  done
  return 1
}

# Prints nothing and exits 0 if the file starts with the expected header
# (after optional build constraints and the blank line that must follow them).
# Otherwise prints the first mismatching line number and exits 1.
# BSD awk rejects newlines in -v values, so the header is read from a file.
HEADER_FILE="$(mktemp "${TMPDIR:-/tmp}/license-header.XXXXXX")"
trap 'rm -f "${HEADER_FILE}"' EXIT
printf '%s\n' "${EXPECTED_HEADER}" > "${HEADER_FILE}"

check_file() {
  awk '
    BEGIN { i = 1; inbuild = 1 }
    FNR == NR { want[++n] = $0; next }
    {
      if (inbuild) {
        if ($0 ~ /^\/\/go:build / || $0 ~ /^\/\/ \+build /) { sawbuild = 1; next }
        inbuild = 0
        if (sawbuild) {
          if ($0 != "") { print FNR; bad = 1; exit 1 }
          next
        }
      }
      line = $0
      if (i == 2) { sub(/^\/\/ Copyright [0-9][0-9][0-9][0-9] /, "// Copyright YEAR ", line) }
      if (i > n) {
        # A blank line must follow the header, or the license becomes the
        # package doc comment.
        if ($0 != "") { print FNR; bad = 1; exit 1 }
        done = 1; exit 0
      }
      if (line != want[i]) { print FNR; bad = 1; exit 1 }
      i++
    }
    END { if (!bad && !done && i <= n) { print FNR + 1; exit 1 } }
  ' "${HEADER_FILE}" "$1"
}

# quotePath=false: otherwise non-ASCII names come back quoted and get skipped.
files="$(git -c core.quotePath=false ls-files --cached --others --exclude-standard -- '*.go')
controller/hack/boilerplate.go.txt"

checked=0
missing=0
while IFS= read -r f; do
  [ -n "${f}" ] || continue
  [ -f "${f}" ] || continue # deleted in the working tree
  if is_exempt "${f}"; then
    continue
  fi
  checked=$((checked + 1))
  if ! where="$(check_file "${f}")"; then
    echo "missing or malformed license header: ${f} (first mismatch at line ${where})" >&2
    missing=$((missing + 1))
  fi
done <<EOF
${files}
EOF

if [ "${missing}" -ne 0 ]; then
  echo "FAIL: ${missing} of ${checked} file(s) lack the Apache-2.0 header. Copy it from controller/hack/boilerplate.go.txt." >&2
  exit 1
fi
echo "PASS: all ${checked} checked file(s) carry the Apache-2.0 header (exempt: $(echo ${EXEMPT_PREFIXES}))"
