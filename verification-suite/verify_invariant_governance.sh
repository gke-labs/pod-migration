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
# Tier-0 (T0) Two-Lane CODEOWNERS CI Governance Check (Part of Issue #54 / Epic #49)
#
# Note: main does not currently have server-side GitHub branch protection rules
# enabled; this script acts as a CI guardrail alongside .github/CODEOWNERS review
# routing rather than a server-enforced branch lock.
#
# Enforces Two-Lane CI Governance for controller/internal/invariants/**:
#   1. Verifies .github/CODEOWNERS assigns at least one @owner to
#      /controller/internal/invariants/, /.github/CODEOWNERS, and
#      /verification-suite/verify_invariant_governance.sh (without hardcoding
#      specific usernames in CI).
#   2. Governance-File Protection (.github/CODEOWNERS and
#      verification-suite/verify_invariant_governance.sh):
#      Forbids modifying either file on any non-main branch unless a
#      maintainer-applied PR label (`allow-invariant-cochange` ->
#      ALLOW_INVARIANT_COCHANGE=true) or local `--allow-cochange` flag is set.
#      Author-controlled branch names and commit trailers cannot bypass this check.
#   3. Lane 1 (Feature & Bugfix PRs — branch does NOT match ^invariant/):
#      Forbids modifying any file under controller/internal/invariants/**
#      unless an explicit co-change override is present:
#        - PR label `allow-invariant-cochange` (ALLOW_INVARIANT_COCHANGE=true),
#        - Commit trailer `Invariant-Cochange: <reason>` in any commit on the PR, or
#        - CLI flag `--allow-cochange`.
#   4. Lane 2 (Dedicated Invariant PRs — branch matches ^invariant/):
#      Requires all modified files to be strictly scoped to
#      controller/internal/invariants/** unless a co-change override is present.
#   5. Never exempts detached HEAD (`HEAD` / `DETACHED_HEAD`); only direct
#      commits on `main` skip per-PR lane diff enforcement.
#   6. Fails closed if the git diff against a base ref cannot be resolved.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CODEOWNERS_FILE="${REPO_ROOT}/.github/CODEOWNERS"
BRANCH=""
BASE_REF=""
CHANGED_FILES_OVERRIDE=""
HAS_CHANGED_OVERRIDE="false"
ALLOW_COCHANGE="${ALLOW_INVARIANT_COCHANGE:-false}"
COMMIT_MSG_OVERRIDE=""
SELF_TEST="false"

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Options:
  --branch <name>            Branch name to evaluate (default: GITHUB_HEAD_REF or current git branch)
  --base <git-ref>           Base git ref for diffing changed files (default: auto-detect main/HEAD^1)
  --codeowners <path>        Path to CODEOWNERS file (default: .github/CODEOWNERS)
  --changed-files <list>     Explicit newline/space-separated changed file list (used in tests)
  --allow-cochange           Allow governance-file or reconciler+invariant co-changes (matches PR label allow-invariant-cochange)
  --commit-msg <text>        Override commit message text when checking for Invariant-Cochange trailer
  --self-test                Run offline self-test covering Lane 1, Lane 2, detached HEAD, fail-closed, and co-change cases
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
    --allow-cochange)
      ALLOW_COCHANGE="true"
      shift
      ;;
    --commit-msg)
      COMMIT_MSG_OVERRIDE="$2"
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
  if ! grep -Eq '^/controller/internal/invariants/[[:space:]]+@[^[:space:]]+' "${co_file}"; then
    fail "${co_file} must assign at least one @owner to /controller/internal/invariants/"
    return 1
  fi
  if ! grep -Eq '^/\.github/CODEOWNERS[[:space:]]+@[^[:space:]]+' "${co_file}"; then
    fail "${co_file} must assign at least one @owner to /.github/CODEOWNERS"
    return 1
  fi
  if ! grep -Eq '^/verification-suite/verify_invariant_governance\.sh[[:space:]]+@[^[:space:]]+' "${co_file}"; then
    fail "${co_file} must assign at least one @owner to /verification-suite/verify_invariant_governance.sh"
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
  if [[ "${GITHUB_REF:-}" == "refs/heads/main" ]] || [[ "${GITHUB_EVENT_NAME:-}" == "push" && "${GITHUB_REF_NAME:-}" == "main" ]]; then
    echo "main"
    return 0
  fi
  git -C "${REPO_ROOT}" symbolic-ref --quiet --short HEAD 2>/dev/null || echo "DETACHED_HEAD"
}

resolve_base_ref() {
  if [[ -n "${BASE_REF}" ]]; then
    if ! git -C "${REPO_ROOT}" rev-parse --verify "${BASE_REF}" >/dev/null 2>&1; then
      fail "Configured --base ref '${BASE_REF}' cannot be resolved in git repository (failing closed)"
      return 1
    fi
    echo "${BASE_REF}"
    return 0
  fi

  # Only use HEAD^1 on GitHub Actions synthetic PR merge commits (refs/pull/<pr>/merge),
  # never on local feature branch merges (`git merge main`), where HEAD^1 is the
  # feature branch and HEAD^2 is main.
  if [[ "${GITHUB_ACTIONS:-}" == "true" && "${GITHUB_REF:-}" =~ ^refs/pull/ ]]; then
    if git -C "${REPO_ROOT}" rev-parse --verify HEAD^2 >/dev/null 2>&1; then
      echo "HEAD^1"
      return 0
    fi
  fi

  local candidate
  if [[ -n "${GITHUB_BASE_REF:-}" ]]; then
    for candidate in "upstream/${GITHUB_BASE_REF}" "origin/${GITHUB_BASE_REF}" "${GITHUB_BASE_REF}"; do
      if git -C "${REPO_ROOT}" rev-parse --verify "${candidate}" >/dev/null 2>&1; then
        echo "${candidate}"
        return 0
      fi
    done
  fi

  for candidate in "upstream/main" "origin/main" "main"; do
    if git -C "${REPO_ROOT}" rev-parse --verify "${candidate}" >/dev/null 2>&1; then
      echo "${candidate}"
      return 0
    fi
  done

  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^1 >/dev/null 2>&1; then
    echo "HEAD^1"
    return 0
  fi

  fail "Could not resolve any base git ref (tried GITHUB_BASE_REF, upstream/main, origin/main, main, HEAD^1) to compute changed files; failing closed"
  return 1
}

resolve_changed_files() {
  if [[ "${HAS_CHANGED_OVERRIDE}" == "true" ]]; then
    tr ' ' '\n' <<<"${CHANGED_FILES_OVERRIDE}" | sed '/^$/d'
    return 0
  fi

  local base
  base="$(resolve_base_ref)" || return 1

  if [[ "${base}" == "HEAD^1" ]]; then
    git -C "${REPO_ROOT}" diff --name-only HEAD^1 HEAD
    return 0
  fi

  git -C "${REPO_ROOT}" diff --name-only "${base}...HEAD"
}

collect_pr_commit_messages() {
  local base=""
  base="$(resolve_base_ref 2>/dev/null || true)"
  if [[ -n "${base}" ]]; then
    git -C "${REPO_ROOT}" log "${base}..HEAD" --format=%B 2>/dev/null || true
  fi
  if git -C "${REPO_ROOT}" rev-parse --verify HEAD^2 >/dev/null 2>&1; then
    git -C "${REPO_ROOT}" log -1 --format=%B HEAD^2 2>/dev/null || true
  fi
  git -C "${REPO_ROOT}" log -1 --format=%B HEAD 2>/dev/null || true
}

has_cochange_trailer() {
  local commit_msg="$1"
  if [[ -z "${commit_msg}" ]]; then
    commit_msg="$(collect_pr_commit_messages)"
  fi
  if grep -Eiq '^[[:space:]]*Invariant-Cochange:[[:space:]]*[^[:space:]]+' <<<"${commit_msg}"; then
    return 0
  fi
  return 1
}

evaluate_governance() {
  local branch="$1"
  local co_file="$2"
  local changed_raw="$3"
  local allow_cochange="${4:-false}"
  local commit_msg="${5:-}"

  verify_codeowners_file "${co_file}" || return 1

  # Only direct push/merge commits on `main` skip per-PR lane diff enforcement.
  # Detached HEAD (`HEAD` / `DETACHED_HEAD`) is NEVER exempted.
  if [[ "${branch}" == "main" ]]; then
    log "PASS: branch 'main' CODEOWNERS structure verified"
    return 0
  fi

  local files=()
  while IFS= read -r line; do
    [[ -n "${line}" ]] && files+=("${line}")
  done < <(tr ' ' '\n' <<<"${changed_raw}" | sed '/^$/d')

  # 1. Governance-file protection (.github/CODEOWNERS and
  #    verification-suite/verify_invariant_governance.sh):
  #    Only a maintainer-applied PR label (ALLOW_INVARIANT_COCHANGE=true) or
  #    local --allow-cochange flag can authorize edits to these two files.
  #    Author-typed commit trailers and branch names NEVER bypass this check.
  for f in "${files[@]:-}"; do
    [[ -z "${f}" ]] && continue
    if [[ "${f}" == ".github/CODEOWNERS" || "${f}" == "verification-suite/verify_invariant_governance.sh" ]]; then
      if [[ "${allow_cochange}" != "true" ]]; then
        fail "Branch '${branch}' modified protected governance file '${f}'. Modifying .github/CODEOWNERS or verification-suite/verify_invariant_governance.sh requires the 'allow-invariant-cochange' PR label (or --allow-cochange for local runs); commit trailers and branch names do not authorize governance-file edits."
        return 1
      fi
    fi
  done

  if [[ "${allow_cochange}" == "true" ]]; then
    log "PASS (Label / CLI override): branch '${branch}' authorized via allow-invariant-cochange (${#files[@]} changed file(s))"
    return 0
  fi

  # 2. Reconciler + Invariant co-change trailer check (applies only when
  #    governance files are untouched).
  if has_cochange_trailer "${commit_msg}"; then
    log "PASS (Co-change trailer override): branch '${branch}' authorized via Invariant-Cochange commit trailer (${#files[@]} changed file(s))"
    return 0
  fi

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
        fail "Lane 2 violation: invariant branch '${branch}' modified non-invariant path '${f}'. Dedicated invariant PRs must touch ONLY controller/internal/invariants/** (or set 'Invariant-Cochange: <reason>' / label 'allow-invariant-cochange' for co-dependent changes)."
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

  # Lane 1: Feature / bugfix / chore / detached-HEAD branch (not matching ^invariant/).
  for f in "${files[@]:-}"; do
    [[ -z "${f}" ]] && continue
    if [[ "${f}" =~ ^controller/internal/invariants/ ]]; then
      fail "Lane 1 violation: non-invariant branch '${branch}' modified '${f}'. Feature/bugfix PRs must not modify controller/internal/invariants/** without 'Invariant-Cochange: <reason>' trailer or 'allow-invariant-cochange' PR label."
      return 1
    fi
  done

  log "PASS (Lane 1): branch '${branch}' leaves controller/internal/invariants/** and governance files untouched (${#files[@]} changed file(s))"
  return 0
}

run_self_test() {
  log "Running T0 Two-Lane CODEOWNERS CI Governance self-test..."
  local tmp_dir
  tmp_dir="$(mktemp -d)"

  local valid_co="${tmp_dir}/CODEOWNERS.valid"
  cat > "${valid_co}" <<'EOF'
/controller/internal/invariants/ @some-owner
/.github/CODEOWNERS @some-owner
/verification-suite/verify_invariant_governance.sh @some-owner
EOF

  local invalid_co="${tmp_dir}/CODEOWNERS.invalid"
  cat > "${invalid_co}" <<'EOF'
# Missing verify_invariant_governance.sh owner entry
/controller/internal/invariants/ @some-owner
/.github/CODEOWNERS @some-owner
EOF

  # 1. Negative: incomplete CODEOWNERS must be rejected
  if evaluate_governance "feature/test" "${invalid_co}" "README.md" "false" "" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: incomplete CODEOWNERS was not rejected"
    exit 1
  fi

  # 2. Positive (Lane 1): feature branch modifying controller/tools files outside invariants and governance files
  evaluate_governance "feature/guardrail-invariant-evolution-pipeline" "${valid_co}" \
    "tools/invariant-gen/main.go .github/workflows/ci.yaml" "false" "chore: regular commit" >/dev/null

  # 3. Negative (Lane 1): feature branch modifying controller/internal/invariants/rules.go without co-change override
  if evaluate_governance "feature/weaken-invariant" "${valid_co}" \
    "controller/internal/controller/podmigrationjob_controller.go controller/internal/invariants/rules.go" "false" "feat: normal commit" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Lane 1 allowed feature branch to modify controller/internal/invariants/rules.go"
    exit 1
  fi

  # 4. Negative (Governance-file protection): neither normal commits, Invariant-Cochange trailers, nor admin/* branch names may modify .github/CODEOWNERS or verify_invariant_governance.sh
  if evaluate_governance "fix/bypass-owners" "${valid_co}" ".github/CODEOWNERS" "false" "fix: normal commit" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: allowed fix branch to modify .github/CODEOWNERS"
    exit 1
  fi
  if evaluate_governance "feature/anyone" "${valid_co}" ".github/CODEOWNERS" "false" $'chore: x\n\nInvariant-Cochange: because' >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Invariant-Cochange trailer bypassed .github/CODEOWNERS protection"
    exit 1
  fi
  if evaluate_governance "admin/whatever" "${valid_co}" "verification-suite/verify_invariant_governance.sh .github/CODEOWNERS" "false" "chore: y" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: admin/* branch name bypassed governance-file protection"
    exit 1
  fi

  # 5. Positive (Lane 2): invariant/* branch modifying only controller/internal/invariants/**
  evaluate_governance "invariant/i10-wedged-restoring-orphan" "${valid_co}" \
    "controller/internal/invariants/rules.go controller/internal/invariants/invariants_test.go controller/internal/invariants/testdata/i10_snapshot.json" "false" "feat: add I10" >/dev/null

  # 6. Negative (Lane 2): invariant/* branch sneaking in a controller change outside invariants/
  if evaluate_governance "invariant/i10-mixed-change" "${valid_co}" \
    "controller/internal/invariants/rules.go controller/internal/controller/podmigrationjob_controller.go" "false" "feat: mixed change" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: Lane 2 allowed invariant/* branch to modify files outside controller/internal/invariants/**"
    exit 1
  fi

  # 7. Negative (Fail-closed diff resolution): unresolvable --base ref must fail closed
  if "${BASH_SOURCE[0]}" --branch "feature/test" --base "refs/heads/nonexistent-ref-99999" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: unresolvable --base ref did not fail closed"
    exit 1
  fi

  # 8. Positive (Co-change trailer for reconciler + invariants): Invariant-Cochange trailer allows co-dependent reconciler + invariant edits when governance files are untouched
  evaluate_governance "fix/reconciler-and-invariant-together" "${valid_co}" \
    "controller/internal/controller/podmigrationjob_controller.go controller/internal/invariants/rules.go" \
    "false" $'fix(controller): update state transition and I3 together\n\nInvariant-Cochange: Reconciler state transition and I3 predicate updated atomically' >/dev/null

  # 9. Positive (--allow-cochange / ALLOW_INVARIANT_COCHANGE=true): exercises the CLI flag and env var path for governance files and co-changes
  "${BASH_SOURCE[0]}" --branch "feature/governance-bootstrap" --codeowners "${valid_co}" \
    --changed-files ".github/CODEOWNERS verification-suite/verify_invariant_governance.sh" \
    --commit-msg "feat: bootstrap governance" --allow-cochange >/dev/null
  ALLOW_INVARIANT_COCHANGE=true "${BASH_SOURCE[0]}" --branch "feature/governance-bootstrap" --codeowners "${valid_co}" \
    --changed-files ".github/CODEOWNERS verification-suite/verify_invariant_governance.sh" \
    --commit-msg "feat: bootstrap governance" >/dev/null

  # 10. Negative (Detached HEAD): HEAD / DETACHED_HEAD must NEVER bypass Lane 1 checks
  if evaluate_governance "HEAD" "${valid_co}" \
    "controller/internal/invariants/rules.go" "false" "feat: detached head edit" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: detached HEAD ('HEAD') bypassed Lane 1 governance check"
    exit 1
  fi
  if evaluate_governance "DETACHED_HEAD" "${valid_co}" \
    "controller/internal/invariants/rules.go" "false" "feat: detached head edit" >/dev/null 2>&1; then
    rm -rf "${tmp_dir}"
    fail "Self-test failed: detached HEAD ('DETACHED_HEAD') bypassed Lane 1 governance check"
    exit 1
  fi

  rm -rf "${tmp_dir}"
  log "Self-test PASSED: all 10 Lane 1, Lane 2, governance-file, detached-HEAD, fail-closed, and co-change cases verified"
}

main() {
  if [[ "${SELF_TEST}" == "true" ]]; then
    run_self_test
    exit 0
  fi

  local branch changed
  branch="$(resolve_branch)"
  changed="$(resolve_changed_files)"
  evaluate_governance "${branch}" "${CODEOWNERS_FILE}" "${changed}" "${ALLOW_COCHANGE}" "${COMMIT_MSG_OVERRIDE}"
}

main
