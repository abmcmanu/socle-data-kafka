# ADR 0003 — Ansible sur VM aujourd'hui, Strimzi sur Kubernetes en cible

**Statut** : acceptée

## Contexte

L'exploitation maîtrise les VM et Ansible ; la DSI vise Kubernetes à 18 mois.

## Décision

- **Ansible** : rôles `kafka_controller`, `kafka_broker`, `schema_registry`, `kafka_connect`,
  `monitoring`, et playbooks d'exploitation (arrêt, relance, rolling restart, contrôle).
- **Strimzi** (opérateur Kubernetes, CNCF) : ressources `Kafka`, `KafkaNodePool`, `KafkaTopic`,
  `KafkaUser`, `KafkaConnect`, `KafkaConnector`.
- La configuration logique (topics, ACL, règles JMX, alertes) est **partagée** : générée depuis
  `governance/` et `monitoring/` par les scripts de `tools/`.

## Conséquences

- La migration VM → Kubernetes se fera par MirrorMaker 2 (ou réplication de cluster), sans changer
  les clients, puisque topics, schémas et ACL sont identiques.
- Deux procédures d'exploitation à maintenir pendant la transition.
- Strimzi gère seul les rolling restarts, les certificats et les mises à jour : moins de procédures
  manuelles en cible.
