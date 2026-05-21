#!/usr/bin/env bash
# FICHIER GÉNÉRÉ par tools/topics.py à partir de governance/topics/catalog.yaml. Ne pas modifier.
# Idempotent : crée les topics absents, aligne la configuration des topics existants.
# Le nombre de partitions n'est jamais réduit (impossible dans Kafka) ; il peut être augmenté.
set -euo pipefail
BOOTSTRAP="${BOOTSTRAP:-broker-1:19092}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
CMD_CONFIG="${CMD_CONFIG:-}"
extra=()
if [[ -n "$CMD_CONFIG" ]]; then extra=(--command-config "$CMD_CONFIG"); fi

ensure_topic() {
  local name="$1" partitions="$2" replicas="$3" config="$4"
  if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --describe --topic "$name" >/dev/null 2>&1; then
    current=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --describe --topic "$name" | grep -o "PartitionCount: *[0-9]*" | grep -o "[0-9]*")
    if (( partitions > current )); then
      echo "[topics] $name : $current -> $partitions partitions"
      "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --alter --topic "$name" --partitions "$partitions"
    fi
    "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --alter --entity-type topics --entity-name "$name" --add-config "$config" >/dev/null
    echo "[topics] $name : configuration alignée"
  else
    echo "[topics] $name : création ($partitions partitions, RF=$replicas)"
    local cfg_args=()
    IFS="," read -ra pairs <<< "$config"
    for p in "${pairs[@]}"; do cfg_args+=(--config "$p"); done
    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --create --if-not-exists \
      --topic "$name" --partitions "$partitions" --replication-factor "$replicas" "${cfg_args[@]}"
  fi
}

ensure_topic "orders.evt.order-placed.v1" 12 3 "cleanup.policy=delete,compression.type=producer,max.message.bytes=1048576,min.insync.replicas=2,retention.ms=604800000,unclean.leader.election.enable=false"
ensure_topic "orders.evt.order-enriched.v1" 12 3 "cleanup.policy=delete,compression.type=producer,min.insync.replicas=2,retention.ms=1209600000,unclean.leader.election.enable=false"
ensure_topic "orders.dlq.order-placed.v1" 3 3 "cleanup.policy=delete,compression.type=producer,min.insync.replicas=2,retention.ms=2592000000,unclean.leader.election.enable=false"
ensure_topic "crm.cdc.customers.v1" 12 3 "cleanup.policy=compact,compression.type=producer,delete.retention.ms=86400000,min.compaction.lag.ms=3600000,min.insync.replicas=2,segment.ms=86400000,unclean.leader.election.enable=false"
ensure_topic "analytics.agg.revenue-by-country-1m.v1" 6 3 "cleanup.policy=delete,compression.type=producer,min.insync.replicas=2,retention.ms=2592000000,unclean.leader.election.enable=false"
ensure_topic "platform.dlq.hdfs-sink.v1" 3 3 "cleanup.policy=delete,compression.type=producer,min.insync.replicas=2,retention.ms=2592000000,unclean.leader.election.enable=false"
ensure_topic "_platform.connect.configs" 1 3 "cleanup.policy=compact,compression.type=producer,min.insync.replicas=2,unclean.leader.election.enable=false"
ensure_topic "_platform.connect.offsets" 25 3 "cleanup.policy=compact,compression.type=producer,min.insync.replicas=2,unclean.leader.election.enable=false"
ensure_topic "_platform.connect.status" 5 3 "cleanup.policy=compact,compression.type=producer,min.insync.replicas=2,unclean.leader.election.enable=false"
echo "[topics] catalogue appliqué : 9 topics"
