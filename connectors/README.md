# Connecteurs Kafka Connect

| Connecteur | Type | Source → cible | Points clés |
| --- | --- | --- | --- |
| `crm-customers-cdc` | Source Debezium PostgreSQL | `public.customers` → `crm.cdc.customers.v1` | WAL logique (`pgoutput`), slot `debezium_crm`, colonnes sensibles exclues, clé = id client, tombstone sur suppression |
| `hdfs-sink` | Sink HDFS | 3 topics → `hdfs:///data/raw/<topic>/dt=AAAA-MM-JJ` | Avro, rotation 10 min, `read_committed`, e-mail masqué, tombstones filtrés, DLQ |

- `lab/` : configurations du lab Docker (un fichier = un connecteur, nom du fichier = nom du connecteur).
- Les secrets ne sont jamais dans ces fichiers : `${file:/opt/secrets/connect-secrets.properties:crm.password}`
  est résolu par le `FileConfigProvider` du worker.
- Déploiement idempotent : `scripts/ops/deploy-connectors.sh <url-connect> connectors/lab`
  (`PUT /connectors/<nom>/config` crée ou met à jour).
- Kubernetes : mêmes configurations sous forme de `KafkaConnector` dans `platform/strimzi/connect/`.
- `Dockerfile` : image Connect = Debezium + HDFS Sink + convertisseur Avro.

## Pourquoi ces SMT

| SMT | Rôle |
| --- | --- |
| `ExtractNewRecordState` | Remplace l'enveloppe Debezium (`before`/`after`/`op`) par l'état après modification : la KTable lit directement l'état du client |
| `RegexRouter` | Renomme `crm.public.customers` selon la convention du catalogue |
| `ExtractField$Key` | Clé = `id` texte (et non une structure) : co-partitionnement avec la re-clé `customerId` de order-enrichment |
| `Filter` + `RecordIsTombstone` | Le format Avro HDFS ne stocke pas de valeur nulle : les suppressions restent dans Kafka |
| `MaskField` + `TopicNameMatches` | Minimisation : l'e-mail n'arrive jamais dans le data lake |

Attention : un SMT ne fait ni jointure ni agrégat. C'est le rôle de Kafka Streams (`services/order-enrichment`).
