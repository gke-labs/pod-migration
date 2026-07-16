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

# Accept Project ID, Bucket Name, and Region as optional arguments
PROJECT_ID=${1:-$(gcloud config get-value project 2>/dev/null)}
BUCKET_NAME=${2:-"${PROJECT_ID}-podsnapshots"}
REGION=${3:-"us-central1"}

if [ -z "$PROJECT_ID" ]; then
  echo "Error: Project ID is required. Please set it via argument or configure gcloud: gcloud config set project <PROJECT_ID>" >&2
  exit 1
fi

echo "Using Project ID: ${PROJECT_ID}"
echo "Using Bucket Name: ${BUCKET_NAME}"
echo "Using Region: ${REGION}"

# 1. Create the GCS bucket if it does not exist
if gcloud storage buckets describe "gs://${BUCKET_NAME}" --project="${PROJECT_ID}" >/dev/null 2>&1; then
  echo "Bucket gs://${BUCKET_NAME} already exists."
else
  echo "Creating bucket gs://${BUCKET_NAME}..."
  gcloud storage buckets create "gs://${BUCKET_NAME}" \
    --project="${PROJECT_ID}" \
    --location="${REGION}" \
    --uniform-bucket-level-access
  
  echo "Disabling soft-delete policy on gs://${BUCKET_NAME}..."
  gcloud storage buckets update "gs://${BUCKET_NAME}" \
    --clear-soft-delete
fi

# Get project number
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

# 2. Bind IAM roles for Workload Identity and GKE engine robot SA
KSA_PRINCIPAL_SET="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${PROJECT_ID}.svc.id.goog/namespace/default"
GKE_ROBOT_SA="serviceAccount:service-${PROJECT_NUMBER}@container-engine-robot.iam.gserviceaccount.com"

echo "Binding IAM roles..."
for member in "${KSA_PRINCIPAL_SET}" "${GKE_ROBOT_SA}"; do
  echo "Binding roles for ${member}..."
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="${member}" \
    --role="roles/storage.objectUser" \
    --quiet
  gcloud storage buckets add-iam-policy-binding "gs://${BUCKET_NAME}" \
    --member="${member}" \
    --role="roles/storage.bucketViewer" \
    --quiet
done

# Create the default workload KSA referenced by manifests
echo "Creating Kubernetes Service Account pm-test-ksa..."
kubectl create serviceaccount pm-test-ksa --namespace default --dry-run=client -o yaml | kubectl apply -f -

# 3. Generate and apply the PodSnapshotStorageConfig manifest
echo "Applying PodSnapshotStorageConfig..."
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
      tokenSource: podKSA
EOF

echo "Storage setup complete!"
