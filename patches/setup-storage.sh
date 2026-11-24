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

# Accept Project ID, Bucket Name, Region, and Mode as optional arguments
PROJECT_ID=${1:-$(gcloud config get-value project 2>/dev/null)}
BUCKET_NAME=${2:-"${PROJECT_ID}-podsnapshots"}
REGION=${3:-"us-central1"}
MODE=${4:-"multi-tenant"} # "multi-tenant" (default, federatedP4SA + managed folders) or "single-tenant" (podKSA)

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
  # The node service account mints short-lived, path-restricted tokens on demand for the exact folder path,
  # ensuring workloads in one namespace cannot access or overwrite checkpoints belonging to another namespace.
  # Workload KSAs receive NO bucket-wide IAM grants.
  GKE_NODE_SA="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-gkenode.iam.gserviceaccount.com"
  echo "Granting roles/storage.admin to GKE node service account (${GKE_NODE_SA})..."
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="${GKE_NODE_SA}" \
    --role="roles/storage.admin" \
    --quiet

  echo "Creating managed folder for namespace 'snapshots'..."
  gcloud storage managed-folders create "gs://${BUCKET_NAME}/snapshots" --project="${PROJECT_ID}" 2>/dev/null || true

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
  TOKEN_SOURCE="podKSA"
fi

# Create the default workload KSA referenced by manifests
echo "Creating Kubernetes Service Account pm-test-ksa..."
kubectl create serviceaccount pm-test-ksa --namespace default --dry-run=client -o yaml | kubectl apply -f -

# 3. Generate and apply the PodSnapshotStorageConfig manifest
echo "Applying PodSnapshotStorageConfig with tokenSource: ${TOKEN_SOURCE}..."
cat <<EOF | kubectl apply -f -
apiVersion: podsnapshot.gke.io/v1
kind: PodSnapshotStorageConfig
metadata:
  name: lpm-test-storage
spec:
  snapshotStorageConfig:
    gcs:
      bucket: ${BUCKET_NAME}
      path: snapshots
      tokenSource: ${TOKEN_SOURCE}
EOF

echo "Storage setup complete (${MODE} mode, tokenSource: ${TOKEN_SOURCE})!"
