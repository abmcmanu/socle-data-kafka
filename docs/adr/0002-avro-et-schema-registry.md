# ADR 0002 — Avro et Schema Registry

**Statut** : acceptée

## Contexte

Le problème P3 (ruptures silencieuses de format) vient de l'absence de contrat. Options étudiées :
JSON sans schéma, JSON Schema, Protobuf, Avro.

## Décision

- **Avro** pour tous les topics métier, schémas versionnés dans `governance/schemas/avro`.
- **Schema Registry**, stratégie `TopicNameStrategy` (un subject `<topic>-value`).
- Compatibilité **BACKWARD_TRANSITIVE** : un nouveau schéma doit pouvoir lire *toutes* les versions
  précédentes, ce qui protège les rejeus de l'historique.
- `auto.register.schemas=false` dans les applications : seul le pipeline CI enregistre les schémas.

## Conséquences

- Les consommateurs sont mis à jour **avant** les producteurs lors d'une évolution.
- Ajout de champ : valeur par défaut obligatoire. Suppression : seulement si le champ avait un défaut.
- Avro est natif dans Kafka Connect et Kafka Streams, et lisible par Hive et Spark dans HDFS.
- Le Schema Registry devient une dépendance critique (servitude S-12) : 2 instances, et les
  clients mettent les schémas en cache.
