#!/usr/bin/env bash
# PROC-03 (lab) — Relance complète ordonnée, contrôlée à chaque étape.
source "$(dirname "$0")/lib.sh"

step "0/7 Supervision et servitudes (pour voir la relance dans Grafana)"
"${COMPOSE[@]}" up -d prometheus alertmanager grafana node-exporter blackbox-exporter postgres namenode datanode
wait_healthy postgres 120; wait_healthy namenode 180

step "1/7 Contrôleurs KRaft (ensemble : il faut une majorité)"
"${COMPOSE[@]}" up -d controller-1 controller-2 controller-3
for c in controller-1 controller-2 controller-3; do wait_healthy "$c" 120; done
leader=$(quorum_leader || echo "")
[[ -n "$leader" && "$leader" != "-1" ]] && c_ok "leader du quorum : nœud $leader" || { c_ko "pas de leader KRaft"; exit 1; }

step "2/7 Brokers"
"${COMPOSE[@]}" up -d broker-1 broker-2 broker-3
for b in broker-1 broker-2 broker-3; do wait_healthy "$b" 240; done

step "3/7 Réplication revenue avant de laisser revenir les clients"
offline=$(count_partitions --unavailable-partitions)
[[ "$offline" == "0" ]] && c_ok "aucune partition hors ligne" || { c_ko "$offline partition(s) hors ligne : runbook RB-03"; exit 1; }
wait_replication 300

step "4/7 Schema Registry"
"${COMPOSE[@]}" up -d schema-registry && wait_healthy schema-registry 180

step "5/7 Kafka Connect et reprise des connecteurs"
"${COMPOSE[@]}" up -d connect && wait_healthy connect 240
connectors_action resume

step "6/7 Applications"
"${COMPOSE[@]}" up -d order-enrichment ingestion-api kafka-exporter kafka-ui && c_ok "applications démarrées"

step "7/7 Contrôle de santé"
sleep 15
"$(dirname "$0")/healthcheck.sh"
