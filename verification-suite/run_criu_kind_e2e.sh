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
# CRIU engine E2E on the kind cluster from verification-suite/kind/up.sh
# (#230). Each scenario drains the worker that runs a stateful counter pod,
# so the pod migrates to the other worker:
#
#   warm-restore     The replacement is restored by CRIU and keeps counting
#                    from where the source stopped, in the same process.
#   restore-failure  The pod also holds an open file on an emptyDir, which
#                    doesn't move with it, so CRIU can't restore it. The
#                    replacement must start cold and serve: the restore
#                    failure leaves no CRIU network lock behind (#126).
#
# The controller runs with --invariant-mode=strict: any InvariantViolation
# event, or a pod still held by the migration scheduling gate (I3), fails the
# run.
#
# Usage: verification-suite/run_criu_kind_e2e.sh [warm-restore|restore-failure]...
# Needs kubectl and jq, and the cluster from verification-suite/kind/up.sh.

set -euo pipefail

BUCKET="${BUCKET:-pm-e2e}"
CLUSTER="${CLUSTER:-pm-e2e}"
# The counter's image, as up.sh loaded it into kind: by tag, so any @digest is
# dropped. The counter never pulls it, so a missing image fails the run.
WORKLOAD_IMAGE="${WORKLOAD_IMAGE:-busybox:1.36}"
TIMEOUT="${TIMEOUT:-300}"
GATE="gke.io/pod-migration-gate"

CORDONED=""
trap 'if [[ -n "${CORDONED}" ]]; then kubectl uncordon "${CORDONED}" >/dev/null || true; fi' EXIT

log() { printf '\033[1;36m[%s]\033[0m %s\n' "$(date -u +%H:%M:%S)" "$*"; }
fail() {
  printf '\033[1;31m[FAIL]\033[0m %s\n' "$*" >&2
  exit 1
}

# expect <what> <got> <want>
expect() {
  [[ "$2" == "$3" ]] || fail "$1: got '$2', want '$3'"
  log "  ok: $1 = $3"
}

# render <namespace> <scenario> <hold-file>: the namespace's PodMigration and
# a counter that keeps its state only in memory and serves it at :8080/state.
# A cold start shows as a new id; a warm restore keeps the id and keeps
# counting.
render() {
  local ns=$1 scenario=$2 hold=$3 open_file="" mounts="" volumes=""
  if [[ "${hold}" == true ]]; then
    open_file='exec 3>/scratch/held'
    mounts='        volumeMounts: [{name: scratch, mountPath: /scratch}]'
    volumes='      volumes: [{name: scratch, emptyDir: {}}]'
  fi
  cat <<EOF
apiVersion: podmigration.gke.io/v1alpha1
kind: PodMigration
metadata:
  name: default
  namespace: ${ns}
spec:
  storage:
    location: gs://${BUCKET}/${ns}
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: counter
  namespace: ${ns}
spec:
  serviceName: counter
  replicas: 1
  selector:
    matchLabels: {app: counter}
  template:
    metadata:
      labels:
        app: counter
        e2e-scenario: ${scenario}
        pod-migration.gke.io/enabled: "true"
    spec:
      # Short, for the deletions the controller doesn't cut to 1s: the
      # namespace cleanup when the scenarios run again, and the holder's wait.
      terminationGracePeriodSeconds: 5
      containers:
      - name: counter
        image: ${WORKLOAD_IMAGE%@*}
        imagePullPolicy: Never
        command: ["/bin/sh", "-c"]
        args:
        - |
          ${open_file}
          id=\$(cat /proc/sys/kernel/random/uuid); n=0
          mkdir -p /www && httpd -p 8080 -h /www
          while true; do n=\$((n + 1)); echo "\$id \$n" >/www/state; sleep 1; done
        ports: [{containerPort: 8080}]
        readinessProbe:
          tcpSocket: {port: 8080}
          periodSeconds: 1
${mounts}
${volumes}
EOF
}

# state <namespace>: "<id> <count>". The restored process runs in its own
# mount namespace, next to the container kubectl exec enters, but in the same
# network namespace: read the state over the network, not from a file.
state() {
  kubectl -n "$1" exec counter-0 -c counter -- wget -qO- http://127.0.0.1:8080/state
}

# pid1 <namespace>: the command of the container's PID 1. The holder stays PID
# 1 while the restored process runs next to it; when the restore fails, or the
# holder gives up waiting for it, the holder execs the app's own command.
pid1() {
  kubectl -n "$1" exec counter-0 -c counter -- cat /proc/1/cmdline | tr '\0' '\n' | head -1
}

# job <namespace> <source pod uid>: the PodMigrationJob of that pod instance.
job() {
  kubectl -n "$1" get podmigrationjobs -o json |
    jq -c --arg uid "$2" '.items[] | select(.spec.targetPodUID == $uid)'
}

job_concluded() {
  [[ "$(job "$1" "$2" | jq -r '.status.phase // ""')" =~ ^(Succeeded|SucceededWithoutRestore|Failed)$ ]]
}

replacement_ready() {
  [[ "$(kubectl -n "$1" get pod counter-0 -o json | jq -r --arg uid "$2" \
    'select(.metadata.uid != $uid) | .status.conditions[]? | select(.type == "Ready") | .status')" == True ]]
}

# wait_until <what> <command...>: runs the command every 2s until it succeeds.
wait_until() {
  local deadline=$((SECONDS + TIMEOUT)) what=$1
  shift
  until "$@" >/dev/null 2>&1; do
    ((SECONDS < deadline)) || fail "timed out after ${TIMEOUT}s waiting for ${what}"
    sleep 2
  done
}

condition() { # <json> <type> -> "<status>/<reason>"
  jq -r --arg t "$2" '.status.conditions[]? | select(.type == $t) | "\(.status)/\(.reason)"' <<<"$1"
}

# migrate <scenario> <hold-file>: deploys the counter in its own namespace,
# drains its node and waits for the job and the replacement. Sets NS, SRC_NODE,
# BEFORE, JOB and POD for the checks.
migrate() {
  NS="pm-e2e-$1"
  log "== $1: deploying the counter in namespace ${NS}"
  kubectl delete namespace "${NS}" --ignore-not-found --wait --timeout="${TIMEOUT}s" >/dev/null
  kubectl create namespace "${NS}" >/dev/null
  render "${NS}" "$1" "$2" | kubectl apply -f - >/dev/null
  kubectl -n "${NS}" wait podmigration/default --for=condition=Ready --timeout="${TIMEOUT}s" >/dev/null
  kubectl -n "${NS}" rollout status statefulset/counter --timeout="${TIMEOUT}s" >/dev/null
  sleep 3 # let the counter count
  local src_uid
  SRC_NODE="$(kubectl -n "${NS}" get pod counter-0 -o jsonpath='{.spec.nodeName}')"
  src_uid="$(kubectl -n "${NS}" get pod counter-0 -o jsonpath='{.metadata.uid}')"
  BEFORE="$(state "${NS}")"
  log "counter-0 on ${SRC_NODE} is at '${BEFORE}'; draining ${SRC_NODE}"

  CORDONED="${SRC_NODE}"
  local drain_log
  drain_log="$(mktemp)"
  kubectl drain "${SRC_NODE}" --pod-selector="e2e-scenario=$1" --delete-emptydir-data \
    --timeout="${TIMEOUT}s" >"${drain_log}" 2>&1 || {
    cat "${drain_log}" >&2
    fail "the drain of ${SRC_NODE} didn't finish"
  }
  rm -f "${drain_log}"
  wait_until "the migration job to conclude" job_concluded "${NS}" "${src_uid}"
  wait_until "the replacement counter-0 to be Ready" replacement_ready "${NS}" "${src_uid}"
  kubectl uncordon "${SRC_NODE}" >/dev/null
  CORDONED=""

  JOB="$(job "${NS}" "${src_uid}")"
  POD="$(kubectl -n "${NS}" get pod counter-0 -o json)"
  expect "job engine" "$(jq -r '.metadata.annotations["pod-migration.gke.io/engine"]' <<<"${JOB}")" criu
  expect "replacement moved off ${SRC_NODE}" "$(jq -r --arg n "${SRC_NODE}" '.spec.nodeName != $n' <<<"${POD}")" true
}

scenario_warm_restore() {
  migrate warm-restore false
  expect "job phase" "$(jq -r .status.phase <<<"${JOB}")" Succeeded
  expect "job Restored" "$(condition "${JOB}" Restored)" True/RestoreVerified
  local snapshot
  snapshot="$(kubectl -n "${NS}" get podsnapshots.criu.podmigration.gke.io \
    "$(jq -r .status.snapshotRef <<<"${JOB}")" -o json)"
  expect "PodSnapshot Ready" "$(condition "${snapshot}" Ready | cut -d/ -f1)" True
  expect "PodSnapshot SourceFrozen" "$(condition "${snapshot}" SourceFrozen)" True/Frozen
  expect "restore outcome" "$(jq -r '.metadata.annotations["pod-migrate.io/psengine-restore"]' <<<"${POD}")" restored
  expect "container restarts" "$(jq -r '.status.containerStatuses[0].restartCount' <<<"${POD}")" 0
  # A holder that gave up waiting would have started a cold copy next to the
  # restored process.
  expect "replacement PID 1" "$(pid1 "${NS}")" /pmhold/holder
  local after later
  after="$(state "${NS}")"
  expect "process id after restore" "${after% *}" "${BEFORE% *}"
  sleep 2
  later="$(state "${NS}")"
  if [[ "${later% *}" != "${BEFORE% *}" ]] || ! ((${after#* } >= ${BEFORE#* } && ${later#* } > ${after#* })); then
    fail "the restored counter isn't counting on: ${BEFORE#* }, then ${after#* }, then '${later}'"
  fi
  log "  ok: the restored process counts on: ${BEFORE#* } -> ${after#* } -> ${later#* }"
}

scenario_restore_failure() {
  migrate restore-failure true
  expect "job phase" "$(jq -r .status.phase <<<"${JOB}")" SucceededWithoutRestore
  expect "job Restored" "$(condition "${JOB}" Restored)" False/FallbackToColdStart
  expect "restore outcome" "$(jq -r '.metadata.annotations["pod-migrate.io/psengine-restore"]' <<<"${POD}")" failed
  expect "container restarts" "$(jq -r '.status.containerStatuses[0].restartCount' <<<"${POD}")" 0
  expect "replacement PID 1" "$(pid1 "${NS}")" /bin/sh
  # The failure must be the one this scenario sets up: CRIU itself failed on
  # the missing file, after the restore had locked the pod's network.
  local event
  event="$(kubectl -n "${NS}" get events --field-selector reason=RestoreFailed -o jsonpath='{.items[*].message}')"
  [[ "${event}" == *"runc restore failed"* && "${event}" == *"scratch/held"* ]] ||
    fail "expected a RestoreFailed event for CRIU failing on scratch/held, got: '${event:0:400}'"
  log "  ok: CRIU failed the restore on the missing scratch/held"
  local after
  after="$(state "${NS}")"
  [[ "${after% *}" != "${BEFORE% *}" ]] || fail "the replacement kept the source's process; expected a cold start"
  log "  ok: the replacement started cold and serves (Ready, '${after}')"
}

# Checked after each scenario, across the whole cluster.
check_cluster() {
  sleep 3 # events from the last reconciles land asynchronously
  local violations gated restarts
  violations="$(kubectl get events -A --field-selector reason=InvariantViolation -o json |
    jq -r '.items[] | "\(.involvedObject.namespace)/\(.involvedObject.name): \(.message)"')"
  [[ -z "${violations}" ]] || fail "invariant violations:"$'\n'"${violations}"
  log "  ok: no InvariantViolation events"
  gated="$(kubectl get pods -A -o json |
    jq -r --arg g "${GATE}" '.items[] | select(any(.spec.schedulingGates[]?; .name == $g)) | "\(.metadata.namespace)/\(.metadata.name)"')"
  [[ -z "${gated}" ]] || fail "pods still held by ${GATE}: ${gated}"
  log "  ok: no pod held by ${GATE}"
  # A component that crashed and retried its way to a pass is still a failure.
  restarts="$(kubectl get pods -A -l 'app in (pod-migration-controller,pod-migrate-psengine,pod-migrate-agent)' -o json |
    jq -r '.items[] | select(any(.status.containerStatuses[]?; .restartCount > 0)) | "\(.metadata.namespace)/\(.metadata.name)"')"
  [[ -z "${restarts}" ]] || fail "controller or engine containers restarted: ${restarts}"
  log "  ok: no controller, engine or agent restarts"
}

main() {
  # The scenarios drain nodes: never run them against another cluster.
  [[ "$(kubectl config current-context)" == "kind-${CLUSTER}" ]] ||
    fail "kubectl context is '$(kubectl config current-context)', want kind-${CLUSTER} (run verification-suite/kind/up.sh)"
  local scenarios=("$@") s
  ((${#scenarios[@]})) || scenarios=(warm-restore restore-failure)
  for s in "${scenarios[@]}"; do
    case "${s}" in
      warm-restore) scenario_warm_restore ;;
      restore-failure) scenario_restore_failure ;;
      *) fail "unknown scenario ${s} (want warm-restore or restore-failure)" ;;
    esac
    check_cluster
  done
  log "PASS: ${scenarios[*]}"
}

main "$@"
