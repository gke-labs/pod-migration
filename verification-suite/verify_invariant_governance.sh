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
# Tier-0 (T0) Two-Lane CODEOWNERS Governance Gate (Issue #54 / Epic #49)
#
# Enforces Two-Lane Governance for controller/internal/invariants/**:
#   1. Verifies .github/CODEOWNERS locks /controller/internal/invariants/ and
#      /.github/CODEOWNERS to human code owners (@yaoluo @bnaylor).
#   2. Lane 1 (Feature & Bugfix PRs — branch does NOT match ^invariant/):
#      Forbids modifying any file under controller/internal/invariants/** or
#      .github/CODEOWNERS so feature/bugfix branches cannot weaken or delete
#      invariant rules to turn a failing test green.
#   3. Lane 2 (Dedicated Invariant PRs — branch matches ^invariant/):
#      Requires all modified files to be strictly scoped to
#      controller/internal/invariants/** so human review is a self-contained,
#      1-click audit of the invariant rule, test, and snapshot fixture.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CODEOWNERS_FILE="${REPO_ROOT}/.github/CODEOWNERS"
BRANCH=""
BASE_REF=""
CHANGED_FILES_OVERRIDE=""
HAS_CHANGED_OVERRIDE="false"
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --branch <name>            Branch name to evaluate (default: GITHUB_HEAD_REF or current git branch)
  --base <git-ref>           Base git ref for diffing changed files (default: auto-detect main/HEAD^1)
  --codeowners <path>        Path to CODEOWNERS file (default: .github/CODEOWNERS)
  --changed-files <list>     Explicit newline/space-separated changed file list (used in tests)
  --self-test                Run offline self-test covering Lane 1 and Lane 2 governance rules
  -h, --help                 Show this help message
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --branch)
      BRANCH="$2"
      shift 2
      ;;
    --base)
      BASE_REF="$2"
      shift 2
      ;;
    --codeowners)
      CODEOWNERS_FILE="$2"
      shift 2
      ;;
    --changed-files)
      CHANGED_FILES_OVERRIDE="$2"
      HAS_CHANGED_OVERRIDE="true"
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
      usage >&2
      exit 2
      ;;
  esac
done

log() {
  printf "\033[1;36m[T0-governance]\033[0m %s\n" "$*"
}

fail() {
  printf "\033[1;31m[FAIL][T0-governance]\033[0m %s\n" "$*" >&2
  return 1
}

verify_codeowners_file() {
  local co_file="$1"
  if [[ ! -f "${co_file}" ]]; then
    fail "Missing CODEOWNERS file at ${co_file}"
    return 1
  fi
  if ! grep -Eq '^/controller/internal/invariants/[[:space:]]+.*@yaoluo' "${co_file}"; then
    fail "${co_file} must lock /controller/internal/invariants/ to @yaoluo"
    return 1
  fi
  if ! grep -Eq '^/\.github/CODEOWNERS[[:space:]]+.*@yaoluo' "${co_file}"; then
    fail "${co_file} must lock /.github/CODEOWNERS to @yaoluo"
    return 1
  fi
  return 0
}

resolve_branch() {
  if [[ -n "${BRANCH}" ]]; then
    echo "${BRANCH}"
    return 0
  fi
  if [[ -n "${GITHUB_HEAD_REF:-}" ]]; then
    echo "${GITHUB_HEAD_REF}"
    return 0
  fi
  git -C "${REPO_ROOT}" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD"
}

resolve_changed_files() {
  if [[ "${HAS_CHANGED_OVERRIDE}" == "true" ]]; then
    tr ' ' '\n' <<<"${CHANGED_FILES_OVERRIDE}" | sed '/^$/d'
    return 0
  fi

  if [[ -n "${BASE_REF}" ]]; then
    git -C "${REPO_ROOT}" diff --name-only "${BASE_REF}...HEAD"
    return 0
  fi

  # In GitHub Actions pull_request checkouts, HEAD is a merge commit whose first
  # parent (HEAD^1) is the target base branch commit.
  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^2 >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" diff --name-only HEAD^1 HEAD
    return 0
  fi

  local candidate
  for candidate in "upstream/main" "origin/main" "main"; do
    if git -C "${REPO_ROOT}" rev-parse --verify "${candidate}" >/dev/null 2>&1; then
      git -C "${REPO_ROOT}" diff --name-only "${candidate}...HEAD"
      return 0
    fi
  done

  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^1 >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" diff --name-only HEAD^1 HEAD
    return 0
  fi

  return 0
}

evaluate_governance() {
  local branch="$1"
  local co_file="$2"
  local changed_raw="$3"

  verify_codeowners_file "${co_file}" || return 1

  # Direct push/merge commits on main only validate CODEOWNERS file integrity.
  if [[ "${branch}" == "main" || "${branch}" == "HEAD" ]]; then
    log "PASS: branch '${branch}' CODEOWNERS integrity verified"
    return 0
  fi

  local files=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && files+=("${line}")
  done < <(tr ' ' '\n' <<<"${changed_raw}" | sed '/^$/d')

  if [[ "${branch}" =~ ^invariant/ ]]; then
    # Lane 2: Dedicated invariant/* branch.
    if [[ "${#files[@]}" -eq 0 ]]; then
      fail "Lane 2 violation: invariant branch '${branch}' has no modified files under controller/internal/invariants/"
      return 1
    fi
    local touched_invariants=0
    for f in "${files[@]}"; do
      if [[ "${f}" =~ ^controller/internal/invariants/ ]]; then
        touched_invariants=$((touched_invariants + 1))
      else
        fail "Lane 2 violation: invariant branch '${branch}' modified non-invariant path '${f}'. Dedicated invariant PRs must touch ONLY controller/internal/invariants/**."
        return 1
      fi
    done
    if [[ "${touched_invariants}" -eq 0 ]]; then
      fail "Lane 2 violation: invariant branch '${branch}' did not modify any file under controller/internal/invariants/"
      return 1
    fi
    log "PASS (Lane 2): invariant branch '${branch}' touches only controller/internal/invariants/** (${touched_invariants} file(s))"
    return 0
  fi

  # Lane 1: Feature / bugfix / chore branch (not matching ^invariant/).
  for f in "${files[@]:-}"; do
    [[ -z "${f}" ]] && continue
    if [[ "${f}" =~ ^controller/internal/invariants/ ]]; then
      fail "Lane 1 violation: non-invariant branch '${branch}' modified protected path '${f}'. Feature/bugfix PRs cannot modify controller/internal/invariants/**; use a dedicated 'invariant/i<N>-<slug>' branch."
      return 1
    fi
    if [[ "${f}" == ".github/CODEOWNERS" ]]; then
      fail "Lane 1 violation: branch '${branch}' modified protected file '.github/CODEOWNERS'."
      return 1
    fi
  done

  log "PASS (Lane 1): branch '${branch}' leaves controller/internal/invariants/** and .github/CODEOWNERS untouched (${#files[@]} changed file(s))"
  return 0
}

run_self_test() {
  log "Running T0 Two-Lane CODEOWNERS Governance self-test..."
  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local valid_co="${tmp_dir}/CODEOWNERS.valid"
  cat > "${valid_co}" <<'EOF'
/controller/internal/invariants/ @yaoluo @bnaylor
/.github/CODEOWNERS @yaoluo @bnaylor
EOF

  local invalid_co="${tmp_dir}/CODEOWNERS.invalid"
  cat > "${invalid_co}" <<'EOF'
# Missing invariant lock
/.github/CODEOWNERS @yaoluo
EOF

  # 1. Negative: malformed CODEOWNERS must be rejected
  if evaluate_governance "feature/test" "${invalid_co}" "README.md" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: malformed CODEOWNERS was not rejected"
    exit 1
  fi

  # 2. Positive (Lane 1): feature branch modifying controller/tools files outside invariants
  evaluate_governance "feature/guardrail-invariant-evolution-pipeline" "${valid_co}" \
    "tools/invariant-gen/main.go .github/workflows/ci.yaml" >/dev/null

  # 3. Negative (Lane 1): feature branch modifying controller/internal/invariants/rules.go
  if evaluate_governance "feature/weaken-invariant" "${valid_co}" \
    "controller/internal/controller/podmigrationjob_controller.go controller/internal/invariants/rules.go" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Lane 1 allowed feature branch to modify controller/internal/invariants/rules.go"
    exit 1
  fi

  # 4. Negative (Lane 1): bugfix branch modifying .github/CODEOWNERS
  if evaluate_governance "fix/bypass-owners" "${valid_co}" ".github/CODEOWNERS" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Lane 1 allowed fix branch to modify .github/CODEOWNERS"
    exit 1
  fi

  # 5. Positive (Lane 2): invariant/* branch modifying only controller/internal/invariants/**
  evaluate_governance "invariant/i10-premature-snapshot-failed" "${valid_co}" \
    "controller/internal/invariants/rules.go controller/internal/invariants/invariants_test.go controller/internal/invariants/testdata/i10_snapshot.json" >/dev/null

  # 6. Negative (Lane 2): invariant/* branch sneaking in a controller change outside invariants/
  if evaluate_governance "invariant/i10-mixed-change" "${valid_co}" \
    "controller/internal/invariants/rules.go controller/internal/controller/podmigrationjob_controller.go" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Lane 2 allowed invariant/* branch to modify files outside controller/internal/invariants/**"
    exit 1
  fi

  rm -rf "${tmp_dir}"
  log "Self-test PASSED: all 6 Lane 1 / Lane 2 CODEOWNERS governance cases verified"
}

main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
    exit 0
  fi

  local branch changed
  branch="$(resolve_branch)"
  changed="$(resolve_changed_files)"
  evaluate_governance "${branch}" "${CODEOWNERS_FILE}" "${changed}"
}

main
