# 01 — Architecture cible

Le socle s'organise en quatre couches : **production** (sources), **transport** (Kafka et ses
composants), **traitement** (applications de flux), **restitution** (data lake et consommateurs).
Une cinquième couche transverse, **exploitation**, supervise et pilote l'ensemble.

## Vue de contexte

```mermaid
flowchart TB
  CLI["Clients web / mobile / magasins"] --> API
  subgraph SI["Système d'information"]
    API["ingestion-api"]
    CRM[("PostgreSQL CRM")]
  end
  subgraph SOCLE["Socle Data Kafka"]
    K["Cluster Kafka"]
    SR["Schema Registry"]
    KC["Kafka Connect"]
    KS["Applications Kafka Streams"]
  end
  subgraph LAKE["Data lake"]
    HDFS[("HDFS")]
    HIVE["Hive"]
  end
  subgraph OPS["Exploitation"]
    MON["Prometheus / Grafana / Alertmanager"]
    ANS["Ansible / Strimzi"]
    GIT["Git (topics, schémas, ACL as code)"]
  end
  API --> K
  CRM --> KC --> K
  K --> KS --> K
  K --> KC --> HDFS --> HIVE
  SOCLE -.-> MON
  GIT --> ANS --> SOCLE
```

## Vue des conteneurs

| Composant | Rôle | Instances (prod) | Technologie |
| --- | --- | --- | --- |
| Contrôleurs KRaft | Quorum des métadonnées, élections de leaders | 3 (un par zone) | Apache Kafka 4.1 |
| Brokers | Stockage et service des partitions | 3 minimum, un par zone | Apache Kafka 4.1 |
| Schema Registry | Contrats Avro, contrôle de compatibilité | 2 (actif / secours) | Confluent Schema Registry 8.0 |
| Kafka Connect | CDC Debezium, HDFS Sink | 2 workers distribués | Kafka Connect + plugins |
| ingestion-api | Point d'entrée REST des commandes | 2+ (sans état) | Spring Boot 3, Java 21 |
| order-enrichment | Validation, jointure client, agrégats | 2+ (état local RocksDB) | Kafka Streams 4.1 |
| Prometheus, Alertmanager, Grafana | Supervision | 1 + 1 + 1 | Prometheus stack |
| kafka-exporter | Lag des consumer groups | 1 | danielqsj/kafka_exporter |
| HDFS / Hive | Data lake | Existant | Apache Hadoop 3 |

## Vue de déploiement

```mermaid
flowchart LR
  subgraph AZ1["Zone A"]
    C1["controller-1"]
    B1["broker-1"]
  end
  subgraph AZ2["Zone B"]
    C2["controller-2"]
    B2["broker-2"]
  end
  subgraph AZ3["Zone C"]
    C3["controller-3"]
    B3["broker-3"]
  end
  B1 <--> B2
  B2 <--> B3
  B1 <--> B3
  C1 --- C2 --- C3
```

- `broker.rack` = zone : chaque partition a une réplica par zone. La perte d'une zone entière ne
  perd aucune donnée et laisse 2 réplicas sur 3, donc `min.insync.replicas=2` reste satisfait.
- Les contrôleurs sont séparés des brokers (pas de mode combiné en production) pour isoler le
  quorum des pics de charge.
- Deux cibles de déploiement, **même configuration logique** :
  - VM : `platform/ansible` (inventaire `prod`) ;
  - Kubernetes : `platform/strimzi` (opérateur Strimzi ≥ 0.49).

## Choix structurants

| Décision | Justification | ADR |
| --- | --- | --- |
| KRaft, sans ZooKeeper | Seul mode supporté depuis Kafka 4.0 ; une seule technologie à exploiter | [0001](../adr/0001-kraft-sans-zookeeper.md) |
| Avro + Schema Registry, `BACKWARD_TRANSITIVE` | Contrats vérifiés avant production, évolution sans casse | [0002](../adr/0002-avro-et-schema-registry.md) |
| Ansible maintenant, Strimzi en cible | Respecte l'existant, prépare Kubernetes | [0003](../adr/0003-ansible-et-strimzi.md) |
| Dépôt HDFS par Kafka Connect | Pas de code, reprise sur offset, partitionnement par date | [0004](../adr/0004-depot-hdfs-par-kafka-connect.md) |
| Exactly-once dans Kafka Streams, at-least-once ailleurs | Exactitude des agrégats, idempotence côté puits | [0005](../adr/0005-semantiques-de-livraison.md) |
| Topics, schémas et ACL « as code » | Revue par pull request, reproductible, auditable | [0006](../adr/0006-gouvernance-as-code.md) |
