-- Tables Hive EXTERNES sur les fichiers Avro déposés par le connecteur HDFS Sink.
-- Supprimer une table externe ne supprime pas les fichiers.
-- Les schémas Avro sont lus dans les fichiers eux-mêmes (avro.schema.literal facultatif).
-- Exécution : beeline -u jdbc:hive2://<hiveserver2>:10000 -f hadoop/hive/01-raw-tables.sql

CREATE DATABASE IF NOT EXISTS raw COMMENT 'Données brutes issues du socle Kafka';

CREATE EXTERNAL TABLE IF NOT EXISTS raw.orders_enriched (
  eventId         STRING,
  orderId         STRING,
  customerId      STRING,
  channel         STRING,
  itemCount       INT,
  totalAmount     DECIMAL(12,2),
  currency        STRING,
  placedAt        TIMESTAMP,
  customerSegment STRING,
  customerCountry STRING,
  enrichedAt      TIMESTAMP
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION 'hdfs:///data/raw/orders.evt.order-enriched.v1'
TBLPROPERTIES ('socle.source.topic' = 'orders.evt.order-enriched.v1');

CREATE EXTERNAL TABLE IF NOT EXISTS raw.revenue_by_country_1m (
  country     STRING,
  currency    STRING,
  windowStart TIMESTAMP,
  windowEnd   TIMESTAMP,
  orderCount  BIGINT,
  revenue     DECIMAL(14,2)
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION 'hdfs:///data/raw/analytics.agg.revenue-by-country-1m.v1'
TBLPROPERTIES ('socle.source.topic' = 'analytics.agg.revenue-by-country-1m.v1');

-- Historique des états clients (e-mail masqué par le connecteur, données sensibles exclues par Debezium).
CREATE EXTERNAL TABLE IF NOT EXISTS raw.customers_history (
  id             STRING,
  full_name      STRING,
  email          STRING,
  country        STRING,
  segment        STRING,
  updated_at     TIMESTAMP,
  `__op`         STRING,
  `__source_ts_ms` BIGINT
)
PARTITIONED BY (dt STRING)
STORED AS AVRO
LOCATION 'hdfs:///data/raw/crm.cdc.customers.v1'
TBLPROPERTIES ('socle.source.topic' = 'crm.cdc.customers.v1');

-- Déclare les nouvelles partitions dt=... (à planifier chaque jour, ou activer hive.integration du connecteur).
MSCK REPAIR TABLE raw.orders_enriched;
MSCK REPAIR TABLE raw.revenue_by_country_1m;
MSCK REPAIR TABLE raw.customers_history;
