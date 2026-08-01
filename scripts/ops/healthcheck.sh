#!/usr/bin/env bash
# PROC-01 (lab) — Contrôle de santé complet. Code retour 1 si un point critique est KO.
source "$(dirname "$0")/lib.sh"
critical=0

step "Quorum KRaft"
leader=$(quorum_leader || echo "")
if [[ -n "$leader" && "$leader" != "-1" ]]; then c_ok "leader actif : nœud $leader"; else c_ko "aucun leader"; critical=1; fi

step "Brokers"
n=$(kexec "$KBIN/kafka-broker-api-versions.sh" --bootstrap-server "$BOOT" 2>/dev/null | grep -c ' (id: ' || true)
[[ "$n" == "3" ]] && c_ok "3/3 brokers enregistrés" || { c_ko "$n/3 brokers"; critical=1; }

step "Partitions"
for f in --unavailable-partitions --under-min-isr-partitions --under-replicated-partitions; do
  v=$(count_partitions "$f")
  if [[ "$v" == "0" ]]; then c_ok "$f : 0"
  elif [[ "$f" == "--under-replicated-partitions" ]]; then c_warn "$f : $v (runbook RB-04)"
  else c_ko "$f : $v"; critical=1; fi
done

step "Consumer groups critiques"
for g in order-enrichment connect-hdfs-sink; do
  out=$(kexec "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOT" --describe --group "$g" 2>/dev/null || true)
  lag=$(awk 'NR>1 && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}' <<<"$out")
  members=$(awk 'NR>1 && $7 !~ /^-$/ && $7 != "" {print $7}' <<<"$out" | sort -u | grep -vc '^$' || true)
  if [[ -z "$out" ]]; then c_warn "$g : groupe absent (pas encore démarré ?)"
  elif (( lag > 10000 )); then c_warn "$g : lag $lag (runbook RB-07)"
  else c_ok "$g : lag $lag, $members membre(s)"; fi
done

step "Schema Registry"
if subjects=$(curl -fs http://localhost:8081/subjects); then c_ok "$(tr -cd ',' <<<"$subjects" | wc -c | awk '{print $1+1}') subjects enregistrés"; else c_ko "injoignable"; critical=1; fi

step "Kafka Connect"
if status=$(curl -fs "$CONNECT_URL/connectors?expand=status"); then
  if grep -q FAILED <<<"$status"; then c_ko "connecteur ou tâche FAILED (runbook RB-08)"; critical=1
  else c_ok "connecteurs : $(grep -o '"state":"RUNNING"' <<<"$status" | wc -l | tr -d ' ') état(s) RUNNING"; fi
else c_ko "API REST injoignable"; critical=1; fi

step "Data lake HDFS"
if "${COMPOSE[@]}" exec -T namenode hdfs dfs -test -d /data/raw 2>/dev/null; then
  c_ok "$("${COMPOSE[@]}" exec -T namenode hdfs dfs -count /data/raw 2>/dev/null | awk '{print $2" fichier(s) dans /data/raw"}')"
else c_warn "/data/raw absent (normal pendant les 10 premières minutes)"; fi

step "Supervision"
if alerts=$(curl -fs 'http://localhost:9090/api/v1/alerts'); then
  firing=$(grep -o '"state":"firing"' <<<"$alerts" | wc -l | tr -d ' ')
  [[ "$firing" == "0" ]] && c_ok "aucune alerte active" || c_warn "$firing alerte(s) active(s) : http://localhost:9093"
else c_warn "Prometheus injoignable"; fi

echo
if (( critical )); then echo "RÉSULTAT : NON SAIN — voir docs/exploitation/04-runbooks.md"; exit 1; fi
echo "RÉSULTAT : SAIN"
