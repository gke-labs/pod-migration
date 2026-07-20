#!/bin/bash
# run_progressive_validation.sh

CONV_ID="157318f7-34fd-42aa-818b-ecffe95214d9"
APPS=(
  "nats"
  "nginx"
  "caddy"
)

LOG_DIR="validation_logs"
mkdir -p "$LOG_DIR"
SUMMARY_FILE="$LOG_DIR/summary.txt"
echo "E2E Validation Summary - $(date)" > "$SUMMARY_FILE"
echo "==========================================" >> "$SUMMARY_FILE"

cleanup() {
  local app=$1
  echo "[*] Post-app cleanup for $app..."
  kubectl delete statefulset pm-$app --ignore-not-found --timeout=30s || true
  kubectl delete job pm-$app --ignore-not-found --timeout=30s || true
  kubectl delete pod -l pod-migration.gke.io/enabled=true --ignore-not-found --timeout=30s || true
  kubectl delete pvc -l app=pm-$app --ignore-not-found --timeout=30s || true
  kubectl delete podmigrationjob,podsnapshotmanualtrigger --all --timeout=15s || true
  kubectl get nodes -l sandbox.gke.io/runtime=gvisor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | xargs -I {} kubectl uncordon {} 2>/dev/null || true
  # Clear webhook metadata database to avoid restored pod mismatch (Rule 2)
  kubectl delete podsnapshots,podsnapshotmanualtriggers --all --ignore-not-found --timeout=15s || true
}

# Ensure clean start
for app in "${APPS[@]}"; do
  kubectl delete statefulset pm-$app --ignore-not-found || true
  kubectl delete job pm-$app --ignore-not-found || true
  kubectl delete pvc -l app=pm-$app --ignore-not-found || true
done
kubectl delete pod -l pod-migration.gke.io/enabled=true --ignore-not-found || true
kubectl delete podmigrationjob,podsnapshotmanualtrigger --all --ignore-not-found || true

for app in "${APPS[@]}"; do
  # Send start notification
  agentapi send-message "$CONV_ID" "[PROGRESS] Starting E2E validation for: **$app**"
  
  LOG_FILE="$LOG_DIR/${app}.log"
  
  # Run validation
  set +e
  ./verification-suite/run_app_validation.sh "$app" > "$LOG_FILE" 2>&1
  STATUS=$?
  set -e
  
  if [ $STATUS -eq 0 ]; then
    echo "$app: SUCCESS" >> "$SUMMARY_FILE"
    agentapi send-message "$CONV_ID" "[PROGRESS] ✅ **$app** validation **PASSED**."
    # Perform cleanup only on success
    cleanup "$app"
    sleep 5
  else
    echo "$app: FAILED" >> "$SUMMARY_FILE"
    # Extract the failure error lines for context
    ERRORS=$(grep -i -E "error|failed|command failed" "$LOG_FILE" | tail -n 5)
    agentapi send-message "$CONV_ID" "[PROGRESS] ❌ **$app** validation **FAILED** (Exit code: $STATUS). Errors:
\`\`\`
$ERRORS
\`\`\`
**Validation aborted to preserve state for debugging.**"
    exit 1
  fi
done

# Send final summary
SUMMARY_CONTENT=$(cat "$SUMMARY_FILE")
agentapi send-message "$CONV_ID" "[PROGRESS] 🏁 **E2E Validation Loop Finished!** Final Summary:
\`\`\`
$SUMMARY_CONTENT
\`\`\`"
