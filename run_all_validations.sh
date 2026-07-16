#!/bin/bash
# Wrapper to run all E2E validations sequentially
set -o pipefail

APPS=(redis dragonfly vault minio nginx haproxy traefik caddy python consul mysql mariadb zookeeper kafka memcached valkey etcd nats postgres node go)

log_file="/usr/local/google/home/yaoluo/testing-pod-migration/gke-pod-migration/validation_summary.log"
echo "E2E Validation Start: $(date)" > "$log_file"

cleanup() {
  local app=$1
  echo "[*] Cleaning up after $app..." | tee -a "$log_file"
  kubectl delete statefulset,pvc,job --all --ignore-not-found --timeout=30s || true
  kubectl delete pod -l 'app!=custom-pod-snapshot-agent' --all --ignore-not-found --timeout=30s || true
  kubectl delete podmigrationjob,podsnapshotmanualtrigger --all --timeout=15s || true
  kubectl get nodes -l sandbox.gke.io/runtime=gvisor -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | xargs -I {} kubectl uncordon {} || true
}

failed_apps=()
passed_apps=()

for app in "${APPS[@]}"; do
  echo "==============================================" | tee -a "$log_file"
  echo "Running E2E Validation for: $app" | tee -a "$log_file"
  echo "==============================================" | tee -a "$log_file"
  
  # Run validation
  if ./verification-suite/run_app_validation.sh "$app" 2>&1 | tee -a "$log_file"; then
    echo "[SUCCESS] $app validation passed!" | tee -a "$log_file"
    passed_apps+=("$app")
  else
    echo "[ERROR] $app validation failed!" | tee -a "$log_file"
    failed_apps+=("$app")
  fi
  
  # Cleanup
  cleanup "$app"
done

echo "==============================================" | tee -a "$log_file"
echo "E2E Validation Finished: $(date)" | tee -a "$log_file"
echo "Passed apps: ${passed_apps[*]}" | tee -a "$log_file"
echo "Failed apps: ${failed_apps[*]}" | tee -a "$log_file"
echo "==============================================" | tee -a "$log_file"

if [ ${#failed_apps[@]} -ne 0 ]; then
  echo "Some applications failed validation!"
  exit 1
else
  echo "All applications passed validation!"
  exit 0
fi
