# Services Java

| Module | Rôle | Garanties |
| --- | --- | --- |
| `events` | Classes Avro générées depuis `governance/schemas/avro` | Un seul contrat partagé par tous les services |
| `ingestion-api` | REST → `orders.evt.order-placed.v1` (Spring Boot 3, Java 21) | `acks=all`, idempotence, réponse après acquittement |
| `order-enrichment` | Validation, DLQ, jointure client, agrégat 1 min (Kafka Streams 4.1) | `exactly_once_v2`, standby replicas, noms d'opérateurs stables |

```bash
mvn -f services/pom.xml verify          # compile + tests (TopologyTestDriver, MockMvc, MockProducer)
mvn -f services/pom.xml package -DskipTests && make images
```

## Points de conception à retenir (CCDAK)

- **Clé = identité métier** : `orderId` pour l'ordre par commande ; re-clé `customerId` explicite
  (`repartition("orders-by-customer")`) pour la jointure co-partitionnée avec la KTable clients.
- **Event time** : `OrderTimestampExtractor` lit `placedAt` ; l'API écrit aussi ce timestamp dans le record.
- **Fenêtres** : tumbling 1 min, grâce 30 s, `suppress(untilWindowCloses)` → un résultat définitif par fenêtre.
- **Erreurs** : commande invalide → DLQ avec en-tête `dlq.reason` ; message illisible → journalisé et compté
  (`LogAndContinueExceptionHandler`) ; erreur fatale → arrêt du client, relance par l'orchestrateur.
- **Serdes publics / internes** : `auto.register.schemas=false` pour les topics gouvernés, `true` pour
  les topics internes de Kafka Streams (voir `AvroSerdes`).
- **Producer de l'API** : chaque paramètre est commenté dans `KafkaConfig.java`.

## Tests

| Test | Ce qu'il prouve |
| --- | --- |
| `EnrichmentTopologyTest` | Jointure, client inconnu, DLQ avec raison, agrégat émis seulement à la fermeture de la fenêtre, tombstone |
| `OrderValidatorTest` | Règles métier |
| `OrderPublisherTest` | Clé, timestamp, échelle décimale, erreur retriable → 503 |
| `OrderControllerTest` | Validation HTTP 400, 202, 503 |
