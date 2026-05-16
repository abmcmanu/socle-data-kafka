# Décisions d'architecture (ADR)

Format : contexte, décision, conséquences. Une ADR acceptée n'est jamais modifiée : on la remplace
par une nouvelle ADR qui la cite.

| N° | Décision | Statut |
| --- | --- | --- |
| [0001](0001-kraft-sans-zookeeper.md) | KRaft avec contrôleurs dédiés | Acceptée |
| [0002](0002-avro-et-schema-registry.md) | Avro + Schema Registry, compatibilité BACKWARD_TRANSITIVE | Acceptée |
| [0003](0003-ansible-et-strimzi.md) | Ansible sur VM aujourd'hui, Strimzi sur Kubernetes en cible | Acceptée |
| [0004](0004-depot-hdfs-par-kafka-connect.md) | Dépôt dans HDFS par Kafka Connect | Acceptée |
| [0005](0005-semantiques-de-livraison.md) | Exactly-once dans Kafka Streams, at-least-once + idempotence ailleurs | Acceptée |
| [0006](0006-gouvernance-as-code.md) | Topics, schémas et ACL gérés en Git | Acceptée |
