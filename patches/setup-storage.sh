#!/bin/bash
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

set -e
set -o pipefail

# Accept Project ID, Bucket Name, Region, Mode, and Tenant Namespaces as optional arguments
PROJECT_ID=${1:-$(gcloud config get-value project 2>/dev/null)}
BUCKET_NAME=${2:-"${PROJECT_ID}-podsnapshots"}
REGION=${3:-"us-central1"}
MODE=${4:-"single-tenant"} # "single-tenant" (default, podKSA on dedicated bucket) or "multi-tenant" (federatedP4SA + managed folders)
TENANT_NAMESPACES=${5:-"default"} # Comma-separated list of tenant namespaces for multi-tenant managed folders

if [ -z "$PROJECT_ID" ]; then
  echo "Error: Project ID is required. Please set it via argument or configure gcloud: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

echo "Using Project ID: ${PROJECT_ID}"
echo "Using Bucket Name: ${BUCKET_NAME}"
echo "Using Region: ${REGION}"
echo "Using Storage Mode: ${MODE}"

# 1. Create the GCS bucket if it does not exist
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Bucket gs://${BUCKET_NAME} already exists."
else
  echo "Creating bucket gs://${BUCKET_NAME} with hierarchical namespace..."
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access \
    --enable-hierarchical-namespace
  
  echo "Disabling soft-delete policy on gs://${BUCKET_NAME}..."
  gcloud storage buckets update "gs://${BUCKET_NAME}" \
    --clear-soft-delete
fi

# Get project number
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

# 2. Bind IAM roles based on storage isolation mode (#278)
GKE_ROBOT_SA="serviceAccount:service-${PROJECT_NUMBER}@container-engine-robot.iam.gserviceaccount.com"

echo "Binding IAM roles for GKE robot service account..."
for role in "roles/storage.objectUser" "roles/storage.bucketViewer"; do
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="${GKE_ROBOT_SA}" \
    --role="${role}" \
    --quiet
done

if [ "$MODE" = "multi-tenant" ]; then
  echo "Configuring multi-tenant storage isolation (federatedP4SA + managed folders)..."
  # In multi-tenant mode, grant storage.admin to the GKE node service account.
  # Per official GKE Pod Snapshots documentation ("Prepare for Pod snapshots"), federatedP4SA
  # requires roles/storage.admin for the GKE node service agent (gcp-sa-gkenode) on the bucket
  # to manage bucket metadata and mint path-restricted session tokens for workloads.
  # Workload KSAs receive NO bucket-wide IAM grants.
  GKE_NODE_SA="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com"
  echo "Granting roles/storage.admin to GKE node service account (${GKE_NODE_SA})..."
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="${GKE_NODE_SA}" \
    --role="roles/storage.admin" \
    --quiet

  # Revoke legacy bucket-wide workload KSA bindings if present from prior single-tenant runs
  KSA_PRINCIPAL_SET="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/namespace/default"
  echo "Revoking legacy bucket-wide KSA IAM bindings if present..."
  for role in "roles/storage.objectUser" "roles/storage.bucketViewer"; do
    gcloud storage buckets remove-iam-policy-binding "gs://${BUCKET_NAME}" \
      --member="${KSA_PRINCIPAL_SET}" \
      --role="${role}" \
      --quiet 2>/dev/null || true
  done

  # Create managed folders for each tenant namespace
  IFS=',' read -ra NAMESPACES <<< "${TENANT_NAMESPACES}"
  for ns in "${NAMESPACES[@]}"; do
    ns_clean=$(echo "$ns" | tr -d ' ')
    if [ -n "$ns_clean" ]; then
      echo "Creating managed folder for namespace '${ns_clean}': gs://${BUCKET_NAME}/${ns_clean}..."
      gcloud storage managed-folders create "gs://${BUCKET_NAME}/${ns_clean}" --project="${PROJECT_ID}" 2>/dev/null || true
      kubectl label namespace "${ns_clean}" pod-migration.gke.io/managed="true" --overwrite 2>/dev/null || true
    fi
  done

  TOKEN_SOURCE="federatedP4SA"
else
  echo "Configuring single-tenant dedicated bucket storage (podKSA)..."
  # In single-tenant mode, access is bound directly to the default namespace KSA.
  # This mode must only be used when the bucket is dedicated to a single tenant namespace.
  KSA_PRINCIPAL_SET="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/namespace/default"
  echo "Granting storage permissions to namespace/default Workload Identity..."
  for role in "roles/storage.objectUser" "roles/storage.bucketViewer"; do
    gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
      --member="${KSA_PRINCIPAL_SET}" \
      --role="${role}" \
      --quiet
  done
  kubectl label namespace default pod-migration.gke.io/managed="true" --overwrite 2>/dev/null || true
  TOKEN_SOURCE="podKSA"
fi

# Create the default workload KSA referenced by manifests
echo "Creating Kubernetes Service Account pm-test-ksa..."
kubectl create serviceaccount pm-test-ksa --namespace default --dry-run=client -o yaml | kubectl apply -f -

echo "Storage setup complete (${MODE} mode, tokenSource: ${TOKEN_SOURCE})!"
if [ "$MODE" = "multi-tenant" ]; then
  echo "Next steps for multi-tenancy:"
  echo "1. Apply controller/deploy.yaml to deploy the controller."
  echo "2. Configure PodMigration in each tenant namespace with:"
  echo "     spec.storage.tokenSource: 'federatedP4SA'"
  echo "     spec.storage.location: 'gs://${BUCKET_NAME}/<namespace>/snapshots'"
else
  echo "Next steps for single-tenant mode:"
  echo "1. Apply controller/deploy.yaml to deploy the controller."
  echo "2. Apply controller/podmigration-config.yaml configured with gs://${BUCKET_NAME}/default/snapshots."
fi
