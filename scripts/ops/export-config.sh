#!/usr/bin/env bash
# PROC-07 (lab) — Export de la configuration logique du socle dans exports/<date>/.
source "$(dirname "$0")/lib.sh"
out="$ROOT/exports/$(date +%Y%m%dT%H%M%S)"; mkdir -p "$out"
kexec "$KBIN/kafka-topics.sh" --bootstrap-server "$BOOT" --describe > "$out/topics.txt"
kexec "$KBIN/kafka-configs.sh" --bootstrap-server "$BOOT" --describe --entity-type topics > "$out/topic-configs.txt"
kexec "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOT" --describe --all-groups > "$out/groups.txt" 2>/dev/null || true
curl -fs "$SCHEMA_REGISTRY_URL/schemas" > "$out/schemas.json"
curl -fs "$CONNECT_URL/connectors?expand=info&expand=status" > "$out/connectors.json"
c_ok "export dans $out"
