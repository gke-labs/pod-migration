#!/bin/bash
# Script to verify Case B: Normal pod deletion does not trigger pod snapshot.
set -e

CORPUS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MANIFEST="$CORPUS_DIR/manifests/lpm-redis-statefulset.yaml"
if [ ! -f "$MANIFEST" ]; then
  # Fallback to pm- if lpm- doesn't exist
  MANIFEST="$CORPUS_DIR/manifests/pm-redis-statefulset.yaml"
fi

POD_NAME="pm-redis-0"

echo "[*] Cleaning up potential residue..."
kubectl delete statefulset/pm-redis service/pm-redis-service --ignore-not-found || true
kubectl delete podsnapshots --all || true

echo "[*] Deploying Redis StatefulSet using $MANIFEST..."
kubectl apply -f "$MANIFEST"

echo "[*] Waiting for Redis pod to be Ready..."
kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s

NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
echo "[*] Pod is running on node: $NODE"

# Find the agent pod on this node
AGENT_POD=$(kubectl get pods -n default -o wide | grep "$NODE" | grep custom-pod-snapshot-agent | awk '{print $1}')
echo "[*] Agent pod on node $NODE is: $AGENT_POD"

# Get current log size or timestamp to filter new logs
START_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "[*] Deleting pod $POD_NAME normally (user deletion)..."
kubectl delete pod "$POD_NAME" --timeout=60s

echo "[*] Verifying no PodSnapshot CR was created..."
snapshots=$(kubectl get podsnapshots -o jsonpath='{.items[*].metadata.name}')
if [ -n "$snapshots" ]; then
  echo "[FAIL] PodSnapshot CRs were found: $snapshots"
  exit 1
fi
echo "[+] No PodSnapshot CR created. Passed."

echo "[*] Verifying agent logs for user deletion skip message..."
# Wait a bit for logs to propagate
sleep 5

if kubectl logs "$AGENT_POD" --since-time="$START_TIME" 2>&1 | grep -q "skipping checkpoint (user deletion)"; then
  echo "[+] Found skip message in agent logs. Passed."
else
  echo "[FAIL] Skip message not found in agent logs!"
  echo "--- Agent Logs since $START_TIME ---"
  kubectl logs "$AGENT_POD" --since-time="$START_TIME"
  exit 1
fi

echo "[SUCCESS] Case B verification passed!"
# Cleanup
kubectl delete statefulset/pm-redis service/pm-redis-service --ignore-not-found || true
