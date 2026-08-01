#!/usr/bin/env bash
# PROC-02 (lab) — Arrêt complet ordonné du socle. Usage : stop-platform.sh [--with-monitoring]
source "$(dirname "$0")/lib.sh"

step "1/6 Applications (arrêt propre : transactions terminées, groupes quittés)"
"${COMPOSE[@]}" stop -t 45 ingestion-api order-enrichment && c_ok "applications arrêtées"

step "2/6 Kafka Connect : pause des connecteurs puis arrêt du worker"
connectors_action pause
"${COMPOSE[@]}" stop -t 60 connect && c_ok "connect arrêté"

step "3/6 Schema Registry"
"${COMPOSE[@]}" stop -t 30 schema-registry && c_ok "schema-registry arrêté"

step "4/6 Brokers un par un (arrêt contrôlé : transfert des leaderships)"
for b in broker-3 broker-2 broker-1; do
  "${COMPOSE[@]}" stop -t 180 "$b" && c_ok "$b arrêté"
done

step "5/6 Contrôleurs : suiveurs d'abord, leader du quorum en dernier"
leader=$(quorum_leader || echo "")
leader_svc="controller-${leader:-1}"
for c in controller-1 controller-2 controller-3; do
  [[ "$c" == "$leader_svc" ]] && continue
  "${COMPOSE[@]}" stop -t 60 "$c" && c_ok "$c (suiveur) arrêté"
done
"${COMPOSE[@]}" stop -t 60 "$leader_svc" && c_ok "$leader_svc (leader) arrêté"

step "6/6 Supervision et outils"
if [[ "${1:-}" == "--with-monitoring" ]]; then
  "${COMPOSE[@]}" stop && c_ok "tout est arrêté"
else
  "${COMPOSE[@]}" stop kafka-ui kafka-exporter connectors-init 2>/dev/null || true
  c_warn "supervision laissée active (Prometheus, Grafana) : les alertes de disponibilité sont attendues"
fi
echo; echo "Socle arrêté. Relance : make start (ou scripts/ops/start-platform.sh)"
