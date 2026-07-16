#!/bin/bash
set -e

APP=$1
CORPUS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "$APP" ]; then
  echo "Usage: $0 [redis|node|go|memcached|valkey|etcd|nats|postgres|dragonfly|vault|minio|nginx|haproxy|traefik|caddy|python|consul|mysql|mariadb|zookeeper|kafka]"
  exit 1
fi

exec_with_retry() {
  local max_attempts=5
  local attempt=1
  until "$@"; do
    if [ $attempt -eq $max_attempts ]; then
      echo "[ERROR] Command failed after $max_attempts attempts." >&2
      exit 1
    fi
    echo "[*] Connection failed, retrying in 3 seconds (Attempt $attempt/$max_attempts)..." >&2
    sleep 3
    attempt=$((attempt + 1))
  done
}

# Cleanup old podsnapshots before starting (Rule 2)
echo "[*] Cleaning up old podsnapshots..."
# Temporarily delete VAP to allow finalizer removal
kubectl delete validatingadmissionpolicybinding gke-pod-snapshot-vap-binding --ignore-not-found || true
# Remove finalizers using JSON patch
kubectl get podsnapshots -o json | jq -r '.items[].metadata.name' | xargs -I {} kubectl patch podsnapshot {} --type=json -p='[{"op": "remove", "path": "/metadata/finalizers"}]' || true
# Delete them
kubectl delete podsnapshots,podsnapshotmanualtriggers,podmigrationjobs.podmigration.gke.io --all --timeout=15s || true
# Restore VAP binding
kubectl apply -f "$CORPUS_DIR/manifests/restore-vap-binding.yaml" || true

case "$APP" in
  redis)
    MANIFEST="$CORPUS_DIR/manifests/pm-redis-statefulset.yaml"
    POD_NAME="pm-redis-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-redis service/pm-redis-service --ignore-not-found || true
    
    echo "[*] Deploying Redis StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Redis pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="redis-nonce-$(date +%s)"
    echo "[*] Seeding state in Redis: migkey -> $NONCE"
    kubectl exec "$POD_NAME" -- redis-cli set migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Redis pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(kubectl exec "$POD_NAME" -- redis-cli get migkey)
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Redis E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Redis state verification failed! Expected $NONCE, got $VAL"
      exit 1
    fi
    ;;

  dragonfly)
    MANIFEST="$CORPUS_DIR/manifests/pm-dragonfly-statefulset.yaml"
    POD_NAME="pm-dragonfly-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-dragonfly service/pm-dragonfly-service pod/redis-client --ignore-not-found || true

    echo "[*] Deploying Dragonfly StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Dragonfly pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Launching redis-client helper pod..."
    kubectl run redis-client --image=redis:7-alpine --restart=Never -- sleep 3600
    kubectl wait --for=condition=Ready "pod/redis-client" --timeout=60s
    
    NONCE="df-nonce-$(date +%s)"
    echo "[*] Seeding state in Dragonfly: migkey -> $NONCE"
    exec_with_retry kubectl exec redis-client -- redis-cli -h pm-dragonfly-service set migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Dragonfly pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec redis-client -- redis-cli -h pm-dragonfly-service get migkey)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    echo "[*] Cleaning up redis-client helper pod..."
    kubectl delete pod redis-client --wait=false
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Dragonfly E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Dragonfly state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  vault)
    MANIFEST="$CORPUS_DIR/manifests/pm-vault-statefulset.yaml"
    POD_NAME="pm-vault-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-vault service/pm-vault-service --ignore-not-found || true
    
    echo "[*] Deploying Vault StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Vault pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="vault-nonce-$(date +%s)"
    echo "[*] Seeding state in Vault: secret/migkey -> $NONCE"
    # Vault might take a second to initialize dev engine
    sleep 3
    exec_with_retry kubectl exec "$POD_NAME" -- vault kv put secret/migkey val="$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Vault pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- vault kv get -field=val secret/migkey)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Vault E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Vault state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  minio)
    MANIFEST="$CORPUS_DIR/manifests/pm-minio-statefulset.yaml"
    POD_NAME="pm-minio-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-minio service/pm-minio-service pod/minio-client --ignore-not-found || true
    
    echo "[*] Deploying MinIO StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for MinIO pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Launching minio-client helper pod..."
    kubectl run minio-client --image=minio/mc --restart=Never --overrides='{"spec":{"containers":[{"name":"minio-client","image":"minio/mc","command":["sh","-c","sleep 3600"]}]}}'
    kubectl wait --for=condition=Ready "pod/minio-client" --timeout=60s
    
    NONCE="minio-nonce-$(date +%s)"
    echo "[*] Seeding state in MinIO: bucket/object -> $NONCE"
    exec_with_retry kubectl exec minio-client -- mc alias set myminio http://pm-minio-service:9000 minioadmin minioadmin
    exec_with_retry kubectl exec minio-client -- mc mb myminio/migbucket
    # Pipe seed content to object
    exec_with_retry kubectl exec -i minio-client -- mc pipe myminio/migbucket/migobject <<< "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored MinIO pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec minio-client -- mc cat myminio/migbucket/migobject)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    echo "[*] Cleaning up minio-client helper pod..."
    kubectl delete pod minio-client --wait=false
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] MinIO E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] MinIO state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  nginx)
    MANIFEST="$CORPUS_DIR/manifests/pm-nginx-statefulset.yaml"
    POD_NAME="pm-nginx-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-nginx service/pm-nginx-service --ignore-not-found || true
    
    echo "[*] Deploying Nginx StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Nginx pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="nginx-nonce-$(date +%s)"
    echo "[*] Seeding state in Nginx: Overwriting index.html -> $NONCE"
    # Write directly to rootfs HTML directory
    exec_with_retry kubectl exec "$POD_NAME" -- sh -c "echo '$NONCE' > /usr/share/nginx/html/index.html"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Nginx pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    # Query Nginx service port 80 directly
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- wget -qO- http://localhost:80)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Nginx E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Nginx state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  haproxy)
    MANIFEST="$CORPUS_DIR/manifests/pm-haproxy-statefulset.yaml"
    POD_NAME="pm-haproxy-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-haproxy service/pm-haproxy-service configmap/pm-haproxy-cfg --ignore-not-found || true
    
    echo "[*] Deploying HAProxy StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for HAProxy pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored HAProxy pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying HAProxy port response..."
    # Query port 8080. It should return "ok"
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- wget -qO- http://localhost:8080)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved response: $VAL"
    
    if [ "$VAL" == "ok" ]; then
      echo "[SUCCESS] HAProxy E2E Live Migration Succeeded. Served traffic!"
    else
      echo "[ERROR] HAProxy response validation failed! Expected 'ok', got '$VAL'"
      exit 1
    fi
    ;;

  traefik)
    MANIFEST="$CORPUS_DIR/manifests/pm-traefik-statefulset.yaml"
    POD_NAME="pm-traefik-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-traefik service/pm-traefik-service --ignore-not-found || true
    
    echo "[*] Deploying Traefik StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Traefik pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Traefik pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying Traefik ping response..."
    # Traefik's default ping returns "OK"
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- wget -qO- http://localhost:8080/ping)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved response: $VAL"
    
    if [ "$VAL" == "OK" ]; then
      echo "[SUCCESS] Traefik E2E Live Migration Succeeded. Served traffic!"
    else
      echo "[ERROR] Traefik ping validation failed! Expected 'OK', got '$VAL'"
      exit 1
    fi
    ;;

  caddy)
    MANIFEST="$CORPUS_DIR/manifests/pm-caddy-statefulset.yaml"
    POD_NAME="pm-caddy-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-caddy service/pm-caddy-service --ignore-not-found || true
    
    echo "[*] Deploying Caddy StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Caddy pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Caddy pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying Caddy response..."
    # Default Caddy welcome page contains "Caddy" in its HTML title
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- wget -qO- http://localhost:80)
    if echo "$VAL" | grep -q "Caddy"; then
      echo "[SUCCESS] Caddy E2E Live Migration Succeeded. Served traffic!"
    else
      echo "[ERROR] Caddy validation failed! Welcome page title not found."
      exit 1
    fi
    ;;

  python)
    MANIFEST="$CORPUS_DIR/manifests/pm-python-statefulset.yaml"
    POD_NAME="pm-python-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-python service/pm-python-service --ignore-not-found || true
    
    echo "[*] Deploying Python StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Python pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Python pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying Python HTTP Server response..."
    # Python http.server returns a Directory listing containing "Directory listing" in its HTML
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- python3 -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8080').read().decode())")
    if echo "$VAL" | grep -q "Directory listing"; then
      echo "[SUCCESS] Python HTTP Server E2E Live Migration Succeeded. Served traffic!"
    else
      echo "[ERROR] Python validation failed! Directory listing title not found. Got: '$VAL'"
      exit 1
    fi
    ;;

  consul)
    MANIFEST="$CORPUS_DIR/manifests/pm-consul-statefulset.yaml"
    POD_NAME="pm-consul-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-consul service/pm-consul-service --ignore-not-found || true
    
    echo "[*] Deploying Consul StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Consul pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="consul-nonce-$(date +%s)"
    echo "[*] Seeding state in Consul: Key 'migkey' -> $NONCE"
    exec_with_retry kubectl exec "$POD_NAME" -- consul kv put migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Consul pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- consul kv get migkey)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Consul E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Consul state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  mysql)
    MANIFEST="$CORPUS_DIR/manifests/pm-mysql-statefulset.yaml"
    POD_NAME="pm-mysql-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-mysql service/pm-mysql-service --ignore-not-found || true
    
    echo "[*] Deploying MySQL StatefulSet (Native AIO Disabled)..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for MySQL pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="mysql-nonce-$(date +%s)"
    echo "[*] Seeding state in MySQL: Table durability_test -> $NONCE"
    exec_with_retry kubectl exec "$POD_NAME" -- mysql -u root -e "CREATE DATABASE IF NOT EXISTS migdb; USE migdb; CREATE TABLE IF NOT EXISTS dur_test (val varchar(255)); TRUNCATE dur_test; INSERT INTO dur_test VALUES ('$NONCE');"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored MySQL pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- mysql -u root -e "SELECT val FROM migdb.dur_test;" -N -s)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] MySQL E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] MySQL state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  mariadb)
    MANIFEST="$CORPUS_DIR/manifests/pm-mariadb-statefulset.yaml"
    POD_NAME="pm-mariadb-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-mariadb service/pm-mariadb-service --ignore-not-found || true
    
    echo "[*] Deploying MariaDB StatefulSet (Native AIO Disabled)..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for MariaDB pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="mariadb-nonce-$(date +%s)"
    echo "[*] Seeding state in MariaDB: Table durability_test -> $NONCE"
    exec_with_retry kubectl exec "$POD_NAME" -- mariadb -u root -e "CREATE DATABASE IF NOT EXISTS migdb; USE migdb; CREATE TABLE IF NOT EXISTS dur_test (val varchar(255)); TRUNCATE dur_test; INSERT INTO dur_test VALUES ('$NONCE');"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored MariaDB pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- mariadb -u root -e "SELECT val FROM migdb.dur_test;" -N -s)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] MariaDB E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] MariaDB state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  zookeeper)
    MANIFEST="$CORPUS_DIR/manifests/pm-zookeeper-statefulset.yaml"
    POD_NAME="pm-zookeeper-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-zookeeper service/pm-zookeeper-service --ignore-not-found || true
    
    echo "[*] Deploying ZooKeeper StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for ZooKeeper pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="zookeeper-nonce-$(date +%s)"
    echo "[*] Seeding state in ZooKeeper: Node /migkey -> $NONCE"
    exec_with_retry kubectl exec "$POD_NAME" -- zkCli.sh -server localhost:2181 create /migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored ZooKeeper pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    RAW_VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- zkCli.sh -server localhost:2181 get /migkey 2>/dev/null)
    VAL=$(echo "$RAW_VAL" | grep -o -E "zookeeper-nonce-[0-9]+")
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] ZooKeeper E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] ZooKeeper state verification failed! Expected $NONCE, got '$VAL' (Raw: '$RAW_VAL')"
      exit 1
    fi
    ;;

  kafka)
    MANIFEST="$CORPUS_DIR/manifests/pm-kafka-statefulset.yaml"
    POD_NAME="pm-kafka-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-kafka service/pm-kafka-service --ignore-not-found || true
    
    echo "[*] Deploying Kafka StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Kafka pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s
    
    NONCE="kafka-nonce-$(date +%s)"
    echo "[*] Seeding state in Kafka: Topic migtopic -> $NONCE"
    exec_with_retry kubectl exec "$POD_NAME" -- sh -c "export KAFKA_OPTS=\"-XX:-UseContainerSupport\" && echo '$NONCE' | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic migtopic"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Kafka pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=180s
    
    echo "[*] Verifying state..."
    RAW_VAL=$(exec_with_retry kubectl exec "$POD_NAME" -- sh -c "export KAFKA_OPTS=\"-XX:-UseContainerSupport\" && /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic migtopic --from-beginning --max-messages 1 --timeout-ms 10000 2>/dev/null")
    VAL=$(echo "$RAW_VAL" | grep -o -E "kafka-nonce-[0-9]+")
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Kafka E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Kafka state verification failed! Expected $NONCE, got '$VAL' (Raw: '$RAW_VAL')"
      exit 1
    fi
    ;;

  memcached)
    MANIFEST="$CORPUS_DIR/manifests/pm-memcached-statefulset.yaml"
    POD_NAME="pm-memcached-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-memcached service/pm-memcached-service --ignore-not-found || true
    
    echo "[*] Deploying Memcached StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Memcached pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="memcached-nonce-$(date +%s)"
    LEN=${#NONCE}
    echo "[*] Seeding state in Memcached: key 'migkey' -> $NONCE (length $LEN)"
    # Using busybox nc inside container to set value via telnet protocol
    kubectl exec "$POD_NAME" -c memcached -- sh -c "printf 'set migkey 0 0 $LEN\r\n$NONCE\r\n' | nc localhost 11211"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Memcached pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(kubectl exec "$POD_NAME" -c memcached -- sh -c "printf 'get migkey\r\n' | nc localhost 11211 | sed -n 2p")
    # Clean output (strip trailing carriage returns if any)
    VAL=$(echo "$VAL" | tr -d '\r')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Memcached E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Memcached state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  valkey)
    MANIFEST="$CORPUS_DIR/manifests/pm-valkey-statefulset.yaml"
    POD_NAME="pm-valkey-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-valkey service/pm-valkey-service --ignore-not-found || true
    
    echo "[*] Deploying Valkey StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Valkey pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="valkey-nonce-$(date +%s)"
    echo "[*] Seeding state in Valkey: migkey -> $NONCE"
    kubectl exec "$POD_NAME" -- valkey-cli set migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Valkey pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(kubectl exec "$POD_NAME" -- valkey-cli get migkey)
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Valkey E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Valkey state verification failed! Expected $NONCE, got $VAL"
      exit 1
    fi
    ;;

  etcd)
    MANIFEST="$CORPUS_DIR/manifests/pm-etcd-statefulset.yaml"
    POD_NAME="pm-etcd-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-etcd service/pm-etcd-service --ignore-not-found || true
    
    echo "[*] Deploying etcd StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for etcd pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="etcd-nonce-$(date +%s)"
    echo "[*] Seeding state in etcd: migkey -> $NONCE"
    kubectl exec "$POD_NAME" -- etcdctl put migkey "$NONCE"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored etcd pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(kubectl exec "$POD_NAME" -- etcdctl get migkey --print-value-only)
    # Clean output
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] etcd E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] etcd state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  nats)
    MANIFEST="$CORPUS_DIR/manifests/pm-nats-statefulset.yaml"
    POD_NAME="pm-nats-0"
    CONFIG_JSON="$CORPUS_DIR/manifests/nats-stream-config.json"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-nats service/pm-nats-service pvc/dummy-vol-pm-nats-0 --ignore-not-found || true
    
    echo "[*] Deploying nats StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for nats pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Launching nats-client helper pod..."
    kubectl run nats-client --image=synadia/nats-box:latest --restart=Never --command -- sleep 3600
    kubectl wait --for=condition=Ready pod/nats-client --timeout=60s
    
    echo "[*] Waiting for NATS server startup settlement (10s)..."
    sleep 10
    
    echo "[*] Copying stream configuration to nats-client..."
    kubectl cp "$CONFIG_JSON" nats-client:/tmp/nats-stream-config.json
    
    echo "[*] Creating JetStream stream..."
    kubectl exec nats-client -- nats stream add --config /tmp/nats-stream-config.json --server nats://pm-nats-service:4222
    
    NONCE="nats-nonce-$(date +%s)"
    echo "[*] Seeding state in NATS: test-subject -> $NONCE"
    kubectl exec nats-client -- nats pub test-subject "$NONCE" --server nats://pm-nats-service:4222
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored nats pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL_JSON=$(exec_with_retry kubectl exec nats-client -- nats stream get my-stream 1 --server nats://pm-nats-service:4222 -j)
    VAL_B64=$(echo "$VAL_JSON" | jq -r '.data')
    VAL=$(echo "$VAL_B64" | base64 -d)
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    echo "[*] Cleaning up nats-client helper pod..."
    kubectl delete pod nats-client --wait=false
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] NATS E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] NATS state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  postgres)
    MANIFEST="$CORPUS_DIR/manifests/pm-postgres-statefulset.yaml"
    POD_NAME="pm-postgres-0"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete statefulset/pm-postgres service/pm-postgres-service --ignore-not-found || true
    
    echo "[*] Deploying Postgres StatefulSet..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Waiting for Postgres pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    NONCE="pg-nonce-$(date +%s)"
    echo "[*] Seeding state in Postgres: Table 'migtest' -> val = $NONCE"
    # Wait for postgres port to accept connections
    sleep 5
    kubectl exec "$POD_NAME" -- psql -U postgres -d postgres -c "CREATE TABLE migtest (val text); INSERT INTO migtest VALUES ('$NONCE');"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Waiting for restored Postgres pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    VAL=$(kubectl exec "$POD_NAME" -- psql -U postgres -d postgres -t -A -c "SELECT val FROM migtest;")
    VAL=$(echo "$VAL" | tr -d '\r\n')
    echo "[+] Retrieved value: $VAL"
    
    if [ "$VAL" == "$NONCE" ]; then
      echo "[SUCCESS] Postgres E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Postgres state verification failed! Expected $NONCE, got '$VAL'"
      exit 1
    fi
    ;;

  node)
    MANIFEST="$CORPUS_DIR/manifests/pm-node-job.yaml"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete job/pm-node-job service/pm-node-service --ignore-not-found || true
    
    echo "[*] Deploying Node.js Job..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Resolving pod name..."
    sleep 3
    POD_NAME=$(kubectl get pods -l app=pm-node-job -o jsonpath='{.items[0].metadata.name}')
    
    echo "[*] Waiting for Node.js pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=120s
    
    echo "[*] Getting initial instance ID..."
    INITIAL_ID=$(kubectl exec "$POD_NAME" -c node -- wget -qO- http://localhost:8080)
    echo "[+] Initial Instance ID: $INITIAL_ID"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Resolving new pod name..."
    sleep 5
    NEW_POD_NAME=$(kubectl get pods -l app=pm-node-job -o jsonpath='{.items[0].metadata.name}')
    
    echo "[*] Waiting for new Node.js pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$NEW_POD_NAME" --timeout=120s
    
    echo "[*] Verifying state..."
    RESTORED_ID=$(kubectl exec "$NEW_POD_NAME" -c node -- wget -qO- http://localhost:8080)
    echo "[+] Restored Instance ID: $RESTORED_ID"
    
    if [ "$RESTORED_ID" == "$INITIAL_ID" ]; then
      echo "[SUCCESS] Node.js E2E Live Migration Succeeded. Instance ID survived!"
    else
      echo "[ERROR] Node.js state verification failed! Expected $INITIAL_ID, got $RESTORED_ID (Cold Start)"
      exit 1
    fi
    ;;

  go)
    MANIFEST="$CORPUS_DIR/manifests/pm-go-job.yaml"
    
    echo "[*] Cleaning up potential residue..."
    kubectl delete job/pm-go-job service/pm-go-service configmap/pm-go-source --ignore-not-found || true
    
    echo "[*] Deploying Go Counter Job..."
    kubectl apply -f "$MANIFEST"
    
    echo "[*] Resolving pod name..."
    sleep 3
    POD_NAME=$(kubectl get pods -l app=pm-go-job -o jsonpath='{.items[0].metadata.name}')
    
    echo "[*] Waiting for Go pod to compile and be Ready..."
    kubectl wait --for=condition=Ready "pod/$POD_NAME" --timeout=240s
    
    echo "[*] Seeding state (incrementing counter)..."
    kubectl exec "$POD_NAME" -c go-counter -- wget -qO- --post-data="" http://localhost:8080/incr
    kubectl exec "$POD_NAME" -c go-counter -- wget -qO- --post-data="" http://localhost:8080/incr
    
    HEALTH_VAL=$(kubectl exec "$POD_NAME" -c go-counter -- wget -qO- http://localhost:8080/healthz)
    INITIAL_COUNT=$(echo "$HEALTH_VAL" | grep -o 'counter=[0-9]*' | cut -d= -f2)
    INITIAL_INST_ID=$(echo "$HEALTH_VAL" | grep -o 'instance=[a-z0-9]*' | cut -d= -f2)
    echo "[+] Initial count: $INITIAL_COUNT, Instance ID: $INITIAL_INST_ID"
    
    NODE=$(kubectl get pod "$POD_NAME" -o jsonpath='{.spec.nodeName}')
    echo "[*] Pod is running on node: $NODE"
    
    echo "[*] Draining node $NODE..."
    kubectl drain "$NODE" --ignore-daemonsets --delete-emptydir-data --force --grace-period=30
    
    echo "[*] Restoring node $NODE (uncordon)..."
    kubectl uncordon "$NODE"
    
    echo "[*] Resolving new pod name..."
    sleep 5
    NEW_POD_NAME=$(kubectl get pods -l app=pm-go-job -o jsonpath='{.items[0].metadata.name}')
    
    echo "[*] Waiting for new Go pod to be Ready..."
    kubectl wait --for=condition=Ready "pod/$NEW_POD_NAME" --timeout=240s
    
    echo "[*] Verifying state..."
    HEALTH_VAL_RESTORED=$(kubectl exec "$NEW_POD_NAME" -c go-counter -- wget -qO- http://localhost:8080/healthz)
    RESTORED_COUNT=$(echo "$HEALTH_VAL_RESTORED" | grep -o 'counter=[0-9]*' | cut -d= -f2)
    RESTORED_INST_ID=$(echo "$HEALTH_VAL_RESTORED" | grep -o 'instance=[a-z0-9]*' | cut -d= -f2)
    echo "[+] Restored count: $RESTORED_COUNT, Instance ID: $RESTORED_INST_ID"
    
    if [ "$RESTORED_INST_ID" == "$INITIAL_INST_ID" ] && [ "$RESTORED_COUNT" -ge "$INITIAL_COUNT" ]; then
      echo "[SUCCESS] Go E2E Live Migration Succeeded. State survived!"
    else
      echo "[ERROR] Go state verification failed! Expected $INITIAL_INST_ID / >=$INITIAL_COUNT, got $RESTORED_INST_ID / $RESTORED_COUNT"
      exit 1
    fi
    ;;

  *)
    echo "Unknown app: $APP"
    exit 1
    ;;
esac
