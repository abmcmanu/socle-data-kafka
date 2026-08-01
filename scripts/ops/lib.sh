#!/usr/bin/env bash
# Fonctions communes aux scripts d'exploitation du lab Docker.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/platform/docker-compose/docker-compose.yml" --env-file "$ROOT/platform/docker-compose/.env")
KBIN=/opt/kafka/bin
BOOT=broker-1:19092
CONNECT_URL=${CONNECT_URL:-http://localhost:8083}

c_ok()   { printf '  \033[32m[OK]\033[0m  %s\n' "$*"; }
c_ko()   { printf '  \033[31m[KO]\033[0m  %s\n' "$*"; }
c_warn() { printf '  \033[33m[!!]\033[0m  %s\n' "$*"; }
step()   { printf '\n\033[1m== %s\033[0m\n' "$*"; }

kexec() { "${COMPOSE[@]}" exec -T "${KEXEC_HOST:-broker-1}" "$@"; }

# Nombre de lignes renvoyées par kafka-topics --describe <filtre> (0 = sain).
count_partitions() { kexec "$KBIN/kafka-topics.sh" --bootstrap-server "$BOOT" --describe "$1" 2>/dev/null | grep -c 'Partition:' || true; }

wait_healthy() {
  local svc="$1" timeout="${2:-180}" waited=0
  while [[ "$("${COMPOSE[@]}" ps --format '{{.Health}}' "$svc" 2>/dev/null)" != "healthy" ]]; do
    (( waited >= timeout )) && { c_ko "$svc pas healthy après ${timeout}s"; return 1; }
    sleep 5; waited=$((waited + 5))
  done
  c_ok "$svc healthy"
}

wait_replication() {
  local timeout="${1:-300}" waited=0 urp
  while true; do
    urp=$(count_partitions --under-replicated-partitions)
    [[ "$urp" == "0" ]] && { c_ok "réplication complète (0 partition sous-répliquée)"; return 0; }
    (( waited >= timeout )) && { c_ko "$urp partition(s) encore sous-répliquée(s)"; return 1; }
    sleep 5; waited=$((waited + 5))
  done
}

quorum_leader() {
  KEXEC_HOST=controller-1 kexec "$KBIN/kafka-metadata-quorum.sh" --bootstrap-controller controller-1:9093 describe --status 2>/dev/null \
    | awk '/LeaderId:/ {print $2}'
}

connectors_action() {   # pause | resume
  local action="$1" list
  list=$(curl -fs "$CONNECT_URL/connectors" 2>/dev/null | tr -d '[]"' | tr ',' ' ') || return 0
  for c in $list; do curl -fs -X PUT "$CONNECT_URL/connectors/$c/$action" >/dev/null && c_ok "connecteur $c : $action"; done
}
