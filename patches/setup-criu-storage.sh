#!/bin/bash
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

# Sets up a checkpoint bucket that only the CRIU engine (engines/criu) uses
# (contract clause C10 R5, #244). A CRIU archive is a full memory dump of the
# migrated container, so the bucket grants object access to the engine's two
# Kubernetes service accounts and to nobody else:
#
#   1. Creates the bucket if it doesn't exist: uniform bucket-level access,
#      hierarchical namespace, no soft delete. An existing bucket is checked
#      before anything changes: if it doesn't use uniform bucket-level access
#      (object ACLs could still grant access), or if it grants anyone but the
#      engine's two principals and the legacy project convenience bindings
#      (for example a namespace's principalSet, the GKE service agent, or the
#      nodes' service account), the script says why, prints what would fix
#      it, and exits 1 without changing anything. It never strips a shared
#      bucket.
#   2. Grants roles/storage.objectUser and roles/storage.bucketViewer to the
#      Workload Identity principals of the engine's node agent and controller.
#   3. Removes the bucket's legacy project convenience bindings (projectOwner,
#      projectEditor and projectViewer on the roles/storage.legacy* roles),
#      after printing them. The projectEditor binding on legacyBucketOwner,
#      which is how a project Editor may change the bucket's IAM policy, goes
#      last, so the policy isn't left half done.
#   4. Checks that the bucket grants the two principals and nobody else.
#   5. Warns that roles granted on the project, and GKE access to the
#      engine's namespace, still reach the archives.
#
# This bucket isn't for GKE Pod Snapshots (patches/setup-storage.sh), which
# needs its workloads' own service accounts on its bucket. Point only the
# PodMigrations of namespaces that only CRIU serves at it: a namespace that
# both engines serve has one storage location for both.
#
# Usage: patches/setup-criu-storage.sh [PROJECT_ID] [BUCKET_NAME] [REGION]
#   CRIU_NAMESPACE, CRIU_AGENT_SA and CRIU_CONTROLLER_SA name the engine's
#   namespace and service accounts, for an install that changed them.
#
# Run it as a project Owner or Storage Admin.

set -e
set -o pipefail

PROJECT_ID=${1:-$(gcloud config get-value project 2>/dev/null)}
BUCKET_NAME=${2:-"${PROJECT_ID}-criu-snapshots"}
REGION=${3:-"us-central1"}
CRIU_NAMESPACE=${CRIU_NAMESPACE:-pod-migration-criu-system}
CRIU_AGENT_SA=${CRIU_AGENT_SA:-pod-migrate-agent}
CRIU_CONTROLLER_SA=${CRIU_CONTROLLER_SA:-pod-migrate-psengine-controller}

if [ -z "${PROJECT_ID}" ]; then
  echo "Error: Project ID is required. Please set it via argument or configure gcloud: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

echo "Using Project ID: ${PROJECT_ID}"
echo "Using Bucket Name: ${BUCKET_NAME}"
echo "Using Region: ${REGION}"

PROJECT_NUMBER=$(gcloud projects describe "${PROJECT_ID}" --format='value(projectNumber)')
PRINCIPAL_PREFIX="principal://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/subject/ns/${CRIU_NAMESPACE}/sa"
AGENT="${PRINCIPAL_PREFIX}/${CRIU_AGENT_SA}"
CONTROLLER="${PRINCIPAL_PREFIX}/${CRIU_CONTROLLER_SA}"
COMPUTE_SA="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"
LEGACY_ROLE='^roles/storage[.]legacy(Bucket|Object)(Owner|Reader)$'
LEGACY_MEMBER='^project(Owner|Editor|Viewer):'

# bindings prints every role and member the bucket grants, as "role,member"
# lines.
bindings() {
  gcloud storage buckets get-iam-policy "gs://${BUCKET_NAME}" \
    --flatten="bindings[].members" \
    --format="csv[no-heading](bindings.role,bindings.members)"
}

# require_criu_only exits 1, listing them, if the bucket grants anyone but
# the engine's two principals and the legacy convenience bindings. $1 says
# what has been changed so far.
require_criu_only() {
  local others
  if ! others=$(bindings | awk -F, -v agent="${AGENT}" -v controller="${CONTROLLER}" \
      -v lrole="${LEGACY_ROLE}" -v lmember="${LEGACY_MEMBER}" \
      '$2 != agent && $2 != controller && !($1 ~ lrole && $2 ~ lmember)'); then
    echo "Error: couldn't read the IAM policy of gs://${BUCKET_NAME}; $1" >&2
    exit 1
  fi
  if [ -n "${others}" ]; then
    echo "Error: gs://${BUCKET_NAME} also grants these principals, so it isn't a bucket for the CRIU engine alone; $1:" >&2
    echo "${others}" | while IFS=, read -r role member; do
      note=""
      if [ "${member}" = "${COMPUTE_SA}" ]; then
        note=" (the nodes' default Compute Engine service account)"
      fi
      echo "  ${member} on ${role}${note}" >&2
    done
    echo "Use another bucket (by default <PROJECT_ID>-criu-snapshots), or, once nothing needs them, remove them with:" >&2
    echo "${others}" | while IFS=, read -r role member; do
      echo "  gcloud storage buckets remove-iam-policy-binding gs://${BUCKET_NAME} --member=${member} --role=${role} --all" >&2
    done
    exit 1
  fi
}

# 1. The bucket.
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Bucket gs://${BUCKET_NAME} already exists; checking that only the CRIU engine uses it..."
  # Without uniform bucket-level access, object ACLs (usually the default ACL,
  # which gives project Viewers READER on every new object) can still grant
  # access that the IAM policy below doesn't show.
  if ! ubla=$(gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" \
      --format='value(uniform_bucket_level_access)'); then
    echo "Error: couldn't read the settings of gs://${BUCKET_NAME}; nothing was changed." >&2
    exit 1
  fi
  if [ "${ubla}" != "True" ]; then
    echo "Error: gs://${BUCKET_NAME} doesn't use uniform bucket-level access, so object ACLs can still let others read" >&2
    echo "the archives; nothing was changed. Use another bucket (by default <PROJECT_ID>-criu-snapshots), or turn it on with:" >&2
    echo "  gcloud storage buckets update gs://${BUCKET_NAME} --uniform-bucket-level-access" >&2
    exit 1
  fi
  require_criu_only "nothing was changed"
else
  echo "Creating bucket gs://${BUCKET_NAME}..."
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace

  echo "Disabling soft-delete policy on gs://${BUCKET_NAME}..."
  gcloud storage buckets update "gs://${BUCKET_NAME}" \
    --clear-soft-delete
  require_criu_only "the bucket was created, and nothing else was changed"
fi

# 2. The engine's two principals.
for member in "${AGENT}" "${CONTROLLER}"; do
  for role in roles/storage.objectUser roles/storage.bucketViewer; do
    echo "Granting ${role} to ${member}..."
    gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
      --member="${member}" \
      --role="${role}" \
      --condition=None \
      --quiet >/dev/null
  done
done

# 3. The legacy project convenience bindings, in an order that leaves the
# projectEditor binding on legacyBucketOwner, through which a project Editor
# may change the policy, for last.
if ! legacy=$(bindings | awk -F, -v lrole="${LEGACY_ROLE}" -v lmember="${LEGACY_MEMBER}" '
    $1 ~ lrole && $2 ~ lmember {
      rank = 0
      if ($1 == "roles/storage.legacyObjectReader") rank = 1
      else if ($1 == "roles/storage.legacyObjectOwner") rank = 2
      else if ($1 == "roles/storage.legacyBucketReader") rank = 3
      else if ($2 ~ /^projectEditor:/) rank = 5
      else rank = 4
      print rank "," $0
    }' | sort -t, -k1,1n | cut -d, -f2-); then
  echo "Error: couldn't read the IAM policy of gs://${BUCKET_NAME}; the two principals are granted, but its legacy project bindings are still in place." >&2
  exit 1
fi
if [ -n "${legacy}" ]; then
  echo "Removing these legacy project convenience bindings, which let the project's Owners, Editors and Viewers read or write the archives:"
  echo "${legacy}" | while IFS=, read -r role member; do
    echo "  ${member} on ${role}"
  done
  echo "${legacy}" | while IFS=, read -r role member; do
    if ! gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
        --member="${member}" \
        --role="${role}" \
        --all \
        --quiet >/dev/null; then
      echo "Error: couldn't remove ${member} from ${role}; rerun this script as a project Owner or Storage Admin to finish." >&2
      exit 1
    fi
  done
fi

# 4. Only the engine's two principals now.
if ! remaining=$(bindings | awk -F, -v agent="${AGENT}" -v controller="${CONTROLLER}" '$2 != agent && $2 != controller'); then
  echo "Error: couldn't read the IAM policy of gs://${BUCKET_NAME} to check it. Once the legacy bindings are gone, project" >&2
  echo "Editors can't read the bucket's policy, so the bucket is most likely correct; a project Owner or Storage Admin can" >&2
  echo "confirm with: gcloud storage buckets get-iam-policy gs://${BUCKET_NAME}" >&2
  exit 1
fi
if [ -n "${remaining}" ]; then
  echo "Error: gs://${BUCKET_NAME} still grants principals other than the CRIU engine's:" >&2
  echo "${remaining}" | sed 's/^\([^,]*\),\(.*\)$/  \2 on \1/' >&2
  exit 1
fi

# 5. What the bucket's own policy can't keep out.
echo "WARNING: the bucket now grants only the CRIU engine's two service accounts, but these still reach the archives:" >&2
echo "  - roles granted on the project ${PROJECT_ID}, or above it, that carry storage.objects permissions (Storage Admin," >&2
echo "    the other roles/storage.* roles, custom roles), whatever the bucket's own policy says;" >&2
echo "  - project Owners, who can grant themselves such a role;" >&2
echo "  - anyone who can create pods in ${CRIU_NAMESPACE} through Kubernetes RBAC or GKE IAM (project Owners and" >&2
echo "    Editors can), since a pod can run as either service account and get its token. That includes the nodes'" >&2
echo "    service account if it has such a role: the default Compute Engine service account often has Editor." >&2
echo "  List the project-level grants with:" >&2
echo "  gcloud projects get-iam-policy ${PROJECT_ID} --flatten='bindings[].members' --format='table(bindings.role,bindings.members)' --filter='bindings.role~^roles/(owner|editor|storage[.]|container[.])'" >&2

echo "CRIU storage setup complete. Point the PodMigration of each namespace that only CRIU serves at it, for example"
echo "spec.storage.location: gs://${BUCKET_NAME}/<namespace>/snapshots (see engines/criu/README.md, Storage credentials)."
