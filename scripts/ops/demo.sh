#!/usr/bin/env bash
# Démonstration de bout en bout : commandes -> enrichissement -> agrégat -> DLQ -> HDFS.
source "$(dirname "$0")/lib.sh"
API=${API:-http://localhost:$(env_port INGESTION_API_PORT 8090)/api/v1/orders}

step "Envoi de 6 commandes valides et 1 incohérente"
for i in 1 2 3 4 5 6; do
  cust="C-000$(( (i % 6) + 1 ))"
  curl -fs -X POST "$API" -H 'Content-Type: application/json' -d "{
    \"customerId\":\"$cust\",\"channel\":\"WEB\",\"currency\":\"EUR\",
    \"items\":[{\"sku\":\"SKU-$i\",\"quantity\":$i,\"unitPrice\":12.50}]}" && echo
done
# Commande au client inconnu : enrichissement partiel (pays UNKNOWN), pas de rejet.
curl -fs -X POST "$API" -H 'Content-Type: application/json' -d '{
  "customerId":"C-9999","channel":"MOBILE","currency":"EUR","items":[{"sku":"SKU-X","quantity":1,"unitPrice":3.00}]}' && echo

step "Commande incohérente publiée directement (contourne l'API) -> doit partir en DLQ"
# total 99.00 pour une ligne à 10.00 : order-enrichment la rejette avec dlq.reason=total_mismatch.
# En JSON Avro, un decimal est un « bytes » : 10.00 -> 1000 = 0x03E8, 99.00 -> 9900 = 0x26AC.
schema_id=$(curl -fs "$SCHEMA_REGISTRY_URL"/subjects/orders.evt.order-placed.v1-value/versions/latest | grep -o '"id":[0-9]*' | cut -d: -f2)
now_ms=$(( $(date +%s) * 1000 ))
printf 'O-DLQ-1|{"eventId":"dlq-demo-1","orderId":"O-DLQ-1","customerId":"C-0001","channel":"WEB","items":[{"sku":"SKU-BAD","quantity":1,"unitPrice":"\\u0003\\u00e8"}],"totalAmount":"\\u0026\\u00ac","currency":"EUR","placedAt":%s}\n' "$now_ms" \
  | "${COMPOSE[@]}" exec -T schema-registry kafka-avro-console-producer --bootstrap-server "$BOOT" \
      --topic orders.evt.order-placed.v1 --property schema.registry.url=http://localhost:8081 \
      --property value.schema.id="$schema_id" --property parse.key=true --property key.separator='|' \
      --property key.serializer=org.apache.kafka.common.serialization.StringSerializer \
  && c_ok "commande O-DLQ-1 publiée"
sleep 5
kexec "$KBIN/kafka-console-consumer.sh" --bootstrap-server "$BOOT" --topic orders.dlq.order-placed.v1 \
  --from-beginning --timeout-ms 5000 --property print.headers=true --property print.key=true 2>/dev/null | cut -c1-120 || true

step "Lecture du topic enrichi (5 secondes)"
kexec "$KBIN/kafka-console-consumer.sh" --bootstrap-server "$BOOT" --topic orders.evt.order-enriched.v1 \
  --from-beginning --timeout-ms 5000 --property print.key=true --isolation-level read_committed 2>/dev/null | cat -v | cut -c1-160 || true

step "Lag du groupe order-enrichment"
kexec "$KBIN/kafka-consumer-groups.sh" --bootstrap-server "$BOOT" --describe --group order-enrichment 2>/dev/null | head -8 || true

step "Agrégats par pays : émis 1 min 30 après la fin de chaque fenêtre (grâce + suppress)"
echo "Grafana : http://localhost:3000  ·  Kafka UI : http://localhost:$(env_port KAFKA_UI_PORT 8080)  ·  HDFS : http://localhost:9870"
