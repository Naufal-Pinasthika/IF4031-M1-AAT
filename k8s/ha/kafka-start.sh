#!/usr/bin/env bash
set -euo pipefail
ordinal="${POD_NAME##*-}"
export KAFKA_NODE_ID="$((ordinal + 1))"
export KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://${POD_NAME}.kafka:9092"
properties=/opt/kafka/config/server.properties
: > "$properties"
while IFS= read -r name; do
  case "$name" in
    KAFKA_HEAP_OPTS|KAFKA_JVM_PERFORMANCE_OPTS|KAFKA_OPTS) continue ;;
  esac
  key="${name#KAFKA_}"
  key="${key,,}"
  key="${key//_/.}"
  printf '%s=%s\n' "$key" "${!name}" >> "$properties"
done < <(compgen -v KAFKA_)
if [[ ! -f /var/lib/kafka/data/meta.properties ]]; then
  # Existing quorum is authoritative. Empty disks join as observers, never
  # initialize a competing metadata log or invent a new cluster identity.
  /opt/kafka/bin/kafka-storage.sh format --cluster-id "$CLUSTER_ID" --config "$properties" --no-initial-controllers
fi
exec /opt/kafka/bin/kafka-server-start.sh "$properties"
