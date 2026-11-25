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
# Validates multi-tenant snapshot storage isolation and setup guardrails (Part of Issue #278).
#
# Ensures:
# 1. patches/setup-storage.sh defaults to single-tenant dedicated bucket (podKSA) for clean
#    out-of-the-box compatibility, and configures multi-tenant federatedP4SA with per-namespace
#    managed folders and legacy grant revocation.
# 2. PodMigration storage reconciliation enforces cross-namespace location isolation
#    (ConflictingStorageLocation), namespace path scoping, and dot-dot segment rejection.
# 3. Pinned peer enqueuing, resource deletion, and warn-mode upgrade paths pass.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SELF_TEST="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --self-test)
      SELF_TEST="true"
      shift
      ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--self-test]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 2
      ;;
  esac
done

log() {
  echo "[test_storage_multitenancy] $*"
}

die() {
  echo "[test_storage_multitenancy] ERROR: $*" >&2
  exit 1
}

# --- 1. Audit patches/setup-storage.sh defaults and multi-tenant setup ---
log "Auditing patches/setup-storage.sh configuration postures..."

SETUP_SCRIPT="${REPO_ROOT}/patches/setup-storage.sh"
if [[ ! -f "${SETUP_SCRIPT}" ]]; then
  die "Missing ${SETUP_SCRIPT}"
fi

# Ensure default mode is single-tenant for backward-compatible podKSA setup
if ! grep -q 'MODE=\${4:-"single-tenant"}' "${SETUP_SCRIPT}"; then
  die "setup-storage.sh does not default to single-tenant mode"
fi

# Ensure multi-tenant mode configures federatedP4SA and manages per-tenant folders
if ! grep -q 'TOKEN_SOURCE="federatedP4SA"' "${SETUP_SCRIPT}"; then
  die "setup-storage.sh does not set TOKEN_SOURCE='federatedP4SA' for multi-tenant mode"
fi

# Ensure multi-tenant mode cleans up legacy KSA bindings
if ! grep -q 'remove-iam-policy-binding' "${SETUP_SCRIPT}"; then
  die "setup-storage.sh does not revoke legacy KSA bindings when switching to multi-tenant mode"
fi

# Ensure setup script no longer creates orphaned lpm-test-storage PSSC
if grep -q 'name: lpm-test-storage' "${SETUP_SCRIPT}"; then
  die "setup-storage.sh should not create orphaned lpm-test-storage PSSC manifest"
fi

log "PASS: setup-storage.sh defaults to single-tenant mode and cleans legacy grants in multi-tenant mode"

# --- 2. Audit ValidatingAdmissionPolicy manifest ---
log "Auditing patches/storage-isolation-admission-policy.yaml..."
VAP_MANIFEST="${REPO_ROOT}/patches/storage-isolation-admission-policy.yaml"
if [[ ! -f "${VAP_MANIFEST}" ]]; then
  die "Missing ${VAP_MANIFEST}"
fi
if ! grep -q 'name: pod-migration-storage-location' "${VAP_MANIFEST}"; then
  die "VAP manifest missing pod-migration-storage-location policy"
fi
log "PASS: storage isolation ValidatingAdmissionPolicy manifest present and valid"

# --- 3. Run controller storage isolation unit & conflict tests ---
log "Running controller storage location conflict, isolation, and pinning tests..."
cd "${REPO_ROOT}/controller"
go test -v -run "TestPodMigrationReconciler_ConflictingStorageLocationAcrossNamespaces|TestPodMigrationReconciler_DistinctStorageLocationsAcrossNamespaces|TestPodMigrationReconciler_EnqueuePeers_CrossNamespaceBucketSharing|TestPodMigrationReconciler_ConflictingStorageLocation_DeletesNamespaceResources|TestPodMigrationReconciler_InvalidStorageLocation_Syntax|TestPodMigrationReconciler_StorageIsolationMode_Warn|TestParseStorageLocation|TestValidateStorageLocationForNamespace|TestStorageLocationsConflict" ./internal/controller/... ./internal/util/... >/dev/null

log "PASS: Controller rejects cross-namespace collisions, enforces scoping, and pins peer/cleanup paths"

log "ALL MULTI-TENANT STORAGE ISOLATION CHECKS PASSED!"
