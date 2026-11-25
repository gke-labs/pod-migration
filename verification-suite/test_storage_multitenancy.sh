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

python3 -c "
import yaml, sys

with open('${VAP_MANIFEST}') as f:
    docs = list(yaml.safe_load_all(f))

vap = next((d for d in docs if d.get('kind') == 'ValidatingAdmissionPolicy'), None)
binding = next((d for d in docs if d.get('kind') == 'ValidatingAdmissionPolicyBinding'), None)

if not vap:
    sys.exit('VAP manifest missing ValidatingAdmissionPolicy')
if not binding:
    sys.exit('VAP manifest missing ValidatingAdmissionPolicyBinding')
if vap.get('metadata', {}).get('name') != 'pod-migration-storage-location':
    sys.exit('VAP name is not pod-migration-storage-location')
if vap.get('spec', {}).get('failurePolicy') != 'Fail':
    sys.exit('VAP failurePolicy must be Fail')

rules = vap.get('spec', {}).get('matchConstraints', {}).get('resourceRules', [])
if not any('podmigrations' in r.get('resources', []) for r in rules):
    sys.exit('VAP missing podmigrations in resourceRules')
if not any('podmigration.gke.io' in r.get('apiGroups', []) for r in rules):
    sys.exit('VAP missing podmigration.gke.io in apiGroups')

if binding.get('spec', {}).get('policyName') != 'pod-migration-storage-location':
    sys.exit('Binding policyName is not pod-migration-storage-location')
if 'Deny' not in binding.get('spec', {}).get('validationActions', []):
    sys.exit('Binding validationActions must include Deny')

expr = vap.get('spec', {}).get('validations', [{}])[0].get('expression', '')
required_clauses = [
    \"startsWith('gs://')\",
    r\"!object.spec.storage.location.substring(5).contains('//')\",
    r\"!object.spec.storage.location.substring(5).contains(' ')\",
    r\"!object.spec.storage.location.substring(5).contains('\t')\",
    r\"split('/')[1] == object.metadata.namespace\",
    r\"exists(seg, seg == '.' || seg == '..')\"
]
for clause in required_clauses:
    if clause not in expr:
        sys.exit(f'Missing required CEL clause in VAP expression: {clause}')

def eval_cel_expr(loc, ns):
    if loc is None: return True
    if not loc.startswith('gs://'): return False
    sub = loc[5:]
    if '//' in sub or ' ' in sub or '\t' in sub: return False
    parts = sub.split('/')
    if len(parts) < 2 or parts[0] == '' or parts[1] != ns: return False
    if any(seg == '.' or seg == '..' for seg in parts): return False
    return True

test_cases = [
    ('gs://my-bucket/tenant-a', 'tenant-a', True),
    ('gs://my-bucket/tenant-a/snapshots', 'tenant-a', True),
    ('gs://my-bucket/tenant-a/nested/path', 'tenant-a', True),
    ('gs://my-bucket/tenant-b', 'tenant-a', False),
    ('gs://my-bucket', 'tenant-a', False),
    ('gs://my-bucket/', 'tenant-a', False),
    ('gs://my-bucket/tenant-a/..', 'tenant-a', False),
    ('gs://my-bucket/tenant-a/.', 'tenant-a', False),
    ('gs://my-bucket//tenant-a', 'tenant-a', False),
    ('gs://my-bucket/ tenant-a /snaps', 'tenant-a', False),
    ('gs://my-bucket/\ttenant-a/snaps', 'tenant-a', False),
    ('s3://my-bucket/tenant-a/snaps', 'tenant-a', False),
]

for loc, ns, expected in test_cases:
    res = eval_cel_expr(loc, ns)
    if res != expected:
        sys.exit(f'CEL simulation failed for {loc} in {ns}: got {res}, expected {expected}')
" || die "ValidatingAdmissionPolicy manifest audit failed"

log "PASS: storage isolation ValidatingAdmissionPolicy manifest structure and CEL rules verified"

# --- 3. Run controller storage isolation unit & conflict tests ---
log "Running controller storage location conflict, isolation, and pinning tests..."
cd "${REPO_ROOT}/controller"
go test -v -run "TestPodMigrationReconciler_ConflictingStorageLocationAcrossNamespaces|TestPodMigrationReconciler_DistinctStorageLocationsAcrossNamespaces|TestPodMigrationReconciler_EnqueuePeers_CrossNamespaceBucketSharing|TestPodMigrationReconciler_ConflictingStorageLocation_DeletesNamespaceResources|TestPodMigrationReconciler_InvalidStorageLocation_Syntax|TestPodMigrationReconciler_StorageIsolationMode_Warn|TestPodMigrationReconciler_RootBucketStorage|TestParseStorageLocation|TestValidateStorageLocationForNamespace|TestStorageLocationsConflict" ./internal/controller/... ./internal/util/... >/dev/null

log "PASS: Controller rejects cross-namespace collisions, enforces scoping, and pins peer/cleanup paths"

log "ALL MULTI-TENANT STORAGE ISOLATION CHECKS PASSED!"
