# ADR 0004 — Dépôt dans HDFS par Kafka Connect

**Statut** : acceptée

## Contexte

Le data lake Hadoop reste la référence analytique. Il faut y déposer les flux en moins de 15 minutes,
sans perte ni doublon, et sans charger la base CRM.

Options : job Spark Structured Streaming, connecteur HDFS de Kafka Connect, Apache Flume (abandonné).

## Décision

- Connecteur **HDFS Sink** (Confluent, licence communautaire), format Avro, partitionnement
  **journalier sur l'heure de l'événement** (`TimeBasedPartitioner`, `dt=AAAA-MM-JJ`).
- Rotation des fichiers : `flush.size=10000` ou `rotate.interval.ms=600000` (10 min).
- Tables Hive **externes** sur ces répertoires (DDL dans `hadoop/hive`).
- Capture des tables CRM par **Debezium** (CDC sur le WAL) plutôt que par Sqoop.

## Conséquences

- Exactly-once vers HDFS : le connecteur encode les offsets dans les noms de fichiers et reprend
  exactement là où il s'était arrêté.
- Plus aucun export nocturne sur la base CRM (P4 résolu).
- Petits fichiers : 10 min × 12 partitions = jusqu'à 1 700 fichiers par jour et par topic. Un job de
  compaction Hive/Spark hebdomadaire les regroupe (hors périmètre, documenté dans le DEX).
- Alternative à réévaluer : Apache Iceberg (tables ACID, compaction intégrée) si le data lake migre.
