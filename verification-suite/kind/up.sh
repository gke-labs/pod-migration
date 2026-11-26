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
# Brings up the cluster the CRIU engine E2E runs on (#230): a kind cluster
# whose two workers run real containerd, runc and CRIU, fake-gcs-server as the
# checkpoint bucket, cert-manager, and the shipped controller and CRIU engine
# manifests with images built from this checkout. Any existing cluster of the
# same name is deleted first.
#
# Usage:     verification-suite/kind/up.sh
# Then:      verification-suite/run_criu_kind_e2e.sh
# Tear down: kind delete cluster --name pm-e2e && docker rm -f pm-e2e-gcs
#
# Needs docker, kind, kubectl, make, curl and openssl. What the E2E changes in
# the shipped manifests is in deploy_controller and deploy_criu_engine.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${CLUSTER:-pm-e2e}"
# The images and the cert-manager manifest this script pulls are pinned by
# digest or checksum, so a re-pushed tag or release asset can't change them.
# Not pinned: cert-manager's own images, which its manifest names by tag, and
# most of what goes into the four images built from this checkout (base images
# by tag, packages from live archives). Override a version together with its
# digest or checksum.
#
# kind v0.33.0 node images ship containerd 2.3.4: older kind releases ship a
# containerd that the CRIU agent refuses (CheckContainerdVersion).
NODE_IMAGE="${NODE_IMAGE:-kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.3}"
# sha256 of that release's cert-manager.yaml.
CERT_MANAGER_SHA256="${CERT_MANAGER_SHA256:-c04a51295adda2b1a775146751f973e787190778c7f35180861d9678876e5c74}"
FAKE_GCS_IMAGE="${FAKE_GCS_IMAGE:-fsouza/fake-gcs-server:1.56.1@sha256:797ce226d62f947c009dc40246b30cfb456b8473d8241407f9d6f2c04e4d69ef}"
BUCKET="${BUCKET:-pm-e2e}"
# The counter's image. It's pulled by digest and loaded into kind under its tag
# alone, which is what the counter names and never pulls: a pod that names a
# digest pulls from the registry instead of using the loaded image.
# run_criu_kind_e2e.sh reads WORKLOAD_IMAGE too, so export it for both scripts.
WORKLOAD_IMAGE="${WORKLOAD_IMAGE:-busybox:1.36@sha256:73aaf090f3d85aa34ee199857f03fa3a95c8ede2ffd4cc2cdb5b94e566b11662}"
LOG_DIR="${LOG_DIR:-/tmp/${CLUSTER}}"
GCS_CONTAINER="${CLUSTER}-gcs"
IMAGES=(controller criu-engine criu-agent criu-holder)

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# wait_until <seconds> <what> <command...>: runs the command every 2s until it
# succeeds.
wait_until() {
  local deadline=$((SECONDS + $1)) what=$2
  shift 2
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      echo "timed out waiting for ${what}" >&2
      return 1
    fi
    sleep 2
  done
}

# build_image <name>: builds pm-e2e/<name>:e2e from its shipped Dockerfile, and
# prints the build log only if the build fails.
build_image() {
  local name=$1
  if [[ "${name}" == controller ]]; then
    make -C "${REPO_ROOT}/controller" docker-build IMG="pm-e2e/controller:e2e"
  else
    docker build -f "${REPO_ROOT}/engines/criu/build/${name#criu-}/Dockerfile" \
      -t "pm-e2e/${name}:e2e" "${REPO_ROOT}/engines/criu"
  fi >"${LOG_DIR}/build-${name}.log" 2>&1 || {
    cat "${LOG_DIR}/build-${name}.log" >&2
    return 1
  }
}

# cert-manager's webhook can refuse Issuers and Certificates for a little
# while after its Deployment is Available.
apply_with_retry() {
  local i
  for i in 1 2 3 4 5 6 7 8 9 10; do
    kubectl apply -k "$1" >/dev/null && return 0
    log "kubectl apply -k $1 failed (attempt ${i}), retrying"
    sleep 6
  done
  return 1
}

# The shipped controller, with the flags the E2E needs: migrate pods without a
# runtimeClassName (runc) with CRIU, and no other class, so the controller
# doesn't manage GKE Pod Snapshots objects that kind has no CRDs for; strict
# invariants; and keep each PodSnapshot 10m after its job, for the test to
# read. The image is loaded into kind, so it must not be pulled.
deploy_controller() {
  local dir=$1/controller
  mkdir -p "${dir}"
  sed "s|<YOUR_CONTROLLER_IMAGE>|pm-e2e/controller:e2e|g" \
    "${REPO_ROOT}/controller/deploy.yaml" >"${dir}/deploy.yaml"
  cat >"${dir}/kustomization.yaml" <<'EOF'
resources: [deploy.yaml]
patches:
- target: {kind: Deployment, name: pod-migration-controller}
  patch: |-
    - {op: replace, path: /spec/template/spec/containers/0/imagePullPolicy, value: IfNotPresent}
    - {op: add, path: /spec/template/spec/containers/0/args/-, value: "--migratable-runtime-classes=@default"}
    - {op: add, path: /spec/template/spec/containers/0/args/-, value: "--runtime-class-engines=@default=criu"}
    - {op: add, path: /spec/template/spec/containers/0/args/-, value: "--invariant-mode=strict"}
    - {op: add, path: /spec/template/spec/containers/0/args/-, value: "--snapshot-retention=10m"}
EOF
  kubectl apply -f "${REPO_ROOT}/controller/config/crd/bases/" >/dev/null
  apply_with_retry "${dir}"
}

# The shipped CRIU engine, with the engine and the agent pointed at the GCS
# emulator, and a random archive signing key.
deploy_criu_engine() {
  local dir=$1/criu gcs_host=$2 key
  mkdir -p "${dir}"
  sed -e "s|<YOUR_CRIU_ENGINE_IMAGE>|pm-e2e/criu-engine:e2e|g" \
    -e "s|<YOUR_CRIU_AGENT_IMAGE>|pm-e2e/criu-agent:e2e|g" \
    -e "s|<YOUR_CRIU_HOLDER_IMAGE>|pm-e2e/criu-holder:e2e|g" \
    "${REPO_ROOT}/engines/criu/deploy/deploy.yaml" >"${dir}/deploy.yaml"
  # Strategic merge patches: they add the variable to each container's env and
  # keep whatever env the shipped manifest has.
  cat >"${dir}/kustomization.yaml" <<EOF
resources: [deploy.yaml]
patches:
- patch: |-
    {kind: Deployment, apiVersion: apps/v1, metadata: {name: pod-migrate-psengine, namespace: pod-migration-criu-system},
     spec: {template: {spec: {containers: [{name: engine, env: [{name: STORAGE_EMULATOR_HOST, value: "${gcs_host}"}]}]}}}}
- patch: |-
    {kind: DaemonSet, apiVersion: apps/v1, metadata: {name: pod-migrate-agent, namespace: pod-migration-criu-system},
     spec: {template: {spec: {containers: [{name: agent, env: [{name: STORAGE_EMULATOR_HOST, value: "${gcs_host}"}]}]}}}}
EOF
  # The agent signs and verifies archive manifests with the keys in this
  # Secret, and its pods don't start without it (engines/criu/README.md,
  # Archive signing).
  key="$(openssl rand -base64 48)"
  kubectl create namespace pod-migration-criu-system --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  kubectl -n pod-migration-criu-system create secret generic pod-migrate-archive-signing-keys \
    --from-literal=k1="${key}" >/dev/null
  kubectl apply -f "${REPO_ROOT}/engines/criu/deploy/crds.yaml" >/dev/null
  apply_with_retry "${dir}"
}

has_ca_bundle() {
  [[ -n "$(kubectl get "$1" -o jsonpath='{.webhooks[0].clientConfig.caBundle}')" ]]
}

criu_ready() {
  [[ "$(kubectl get "$1" -o jsonpath='{.metadata.annotations.pod-migrate\.io/criu-ready}')" == true ]]
}

main() {
  mkdir -p "${LOG_DIR}"
  WORK_DIR="$(mktemp -d)"
  trap 'rm -rf "${WORK_DIR}"' EXIT

  log "Building ${IMAGES[*]} in the background (logs in ${LOG_DIR})"
  local pids=() name
  for name in "${IMAGES[@]}"; do
    build_image "${name}" &
    pids+=("$!")
  done
  docker pull -q "${WORKLOAD_IMAGE}" >/dev/null
  docker tag "${WORKLOAD_IMAGE}" "${WORKLOAD_IMAGE%@*}"
  curl -fsSLo "${WORK_DIR}/cert-manager.yaml" \
    "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
  echo "${CERT_MANAGER_SHA256}  ${WORK_DIR}/cert-manager.yaml" | sha256sum --quiet -c - || {
    echo "cert-manager ${CERT_MANAGER_VERSION}'s cert-manager.yaml doesn't match CERT_MANAGER_SHA256" >&2
    exit 1
  }

  log "Creating kind cluster ${CLUSTER}"
  kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  kind create cluster --name "${CLUSTER}" --image "${NODE_IMAGE}" \
    --config "${REPO_ROOT}/verification-suite/kind/config.yaml" --wait 120s

  log "Starting fake-gcs-server with bucket ${BUCKET}"
  docker rm -f "${GCS_CONTAINER}" >/dev/null 2>&1 || true
  docker run -d --name "${GCS_CONTAINER}" --network kind "${FAKE_GCS_IMAGE}" \
    -scheme http -port 4443 -backend memory >/dev/null
  local gcs_host
  gcs_host="$(docker inspect -f '{{(index .NetworkSettings.Networks "kind").IPAddress}}' "${GCS_CONTAINER}"):4443"
  # Created from a node: the network path the engine and the agent use.
  wait_until 60 "fake-gcs-server" docker exec "${CLUSTER}-control-plane" \
    curl -fsS -X POST "http://${gcs_host}/storage/v1/b" -d "{\"name\":\"${BUCKET}\"}"

  log "Installing cert-manager ${CERT_MANAGER_VERSION}"
  kubectl apply -f "${WORK_DIR}/cert-manager.yaml" >/dev/null
  kubectl -n cert-manager wait --for=condition=Available deployment --all --timeout=180s

  log "Waiting for the image builds"
  local pid refs=()
  for pid in "${pids[@]}"; do
    wait "${pid}"
  done
  for name in "${IMAGES[@]}"; do
    refs+=("pm-e2e/${name}:e2e")
  done
  kind load docker-image --name "${CLUSTER}" "${refs[@]}" "${WORKLOAD_IMAGE%@*}"

  log "Deploying the controller and the CRIU engine"
  deploy_controller "${WORK_DIR}"
  deploy_criu_engine "${WORK_DIR}" "${gcs_host}"
  kubectl -n pod-migration-system rollout status deployment/pod-migration-controller --timeout=180s
  kubectl -n pod-migration-criu-system rollout status deployment/pod-migrate-psengine --timeout=180s
  kubectl -n pod-migration-criu-system rollout status daemonset/pod-migrate-agent --timeout=180s

  # The gate webhook fails closed and the restore webhook fails open (a
  # replacement it misses starts cold), so neither may be missing its CA.
  local wh
  for wh in mutatingwebhookconfiguration/pod-migration-mutating-webhook-configuration \
    validatingwebhookconfiguration/pod-migration-validating-webhook-configuration \
    mutatingwebhookconfiguration/pod-migrate-psengine-restore-inject; do
    wait_until 120 "the CA bundle of ${wh}" has_ca_bundle "${wh}"
  done

  local node
  for node in $(kubectl get nodes -l '!node-role.kubernetes.io/control-plane' -o name); do
    if ! wait_until 180 "${node} to be CRIU-ready" criu_ready "${node}"; then
      kubectl get "${node}" -o jsonpath='{.metadata.annotations}' >&2
      echo >&2
      return 1
    fi
  done
  log "Ready: kubectl context kind-${CLUSTER}, checkpoints in gs://${BUCKET} on fake-gcs-server at ${gcs_host}"
}

main "$@"
