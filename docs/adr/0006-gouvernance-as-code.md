# ADR 0006 : Gouvernance « as code »

**Statut** : acceptée

## Contexte

Les topics créés à la main dérivent : partitions et rétentions incohérentes, propriétaire inconnu,
`auto.create.topics.enable=true` qui crée des topics à la moindre faute de frappe.

## Décision

- `auto.create.topics.enable=false` sur tous les brokers.
- Le catalogue `governance/topics/catalog.yaml` décrit chaque topic : propriétaire, description,
  classification, clé, schéma, partitions, configuration.
- `tools/topics.py` valide la convention de nommage et génère le script d'application (VM, lab)
  et les `KafkaTopic` (Strimzi). La CI vérifie que les fichiers générés sont à jour.
- Les schémas (`governance/schemas`) sont testés en compatibilité par la CI avant fusion.
- Les ACL sont déclarées dans les `KafkaUser` (Strimzi) et l'inventaire Ansible.

## Conséquences

- Une demande de nouveau flux = une pull request, revue par l'équipe plateforme : traçable et < 1 jour.
- Pas de modification manuelle en production : toute dérive est écrasée au prochain déploiement.
