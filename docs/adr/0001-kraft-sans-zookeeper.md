# ADR 0001 — KRaft avec contrôleurs dédiés

**Statut** : acceptée

## Contexte

Kafka 4.0 a supprimé ZooKeeper : les métadonnées sont gérées par un quorum Raft interne (KRaft),
stocké dans le topic `__cluster_metadata`. Un nœud peut être broker, contrôleur, ou les deux.

## Décision

- 3 contrôleurs **dédiés** (`process.roles=controller`), un par zone, et des brokers dédiés.
- Le mode combiné (`broker,controller`) est réservé aux environnements de développement.
- Le lab Docker utilise aussi des rôles séparés, pour que l'apprentissage reflète la production.

## Conséquences

- Le quorum tolère la perte d'un contrôleur sur trois ; on passera à 5 si le cluster dépasse ~20 brokers.
- Un pic de charge sur un broker ne ralentit pas les élections de leaders.
- Ordre d'arrêt et de relance imposé (contrôleurs en premier à la relance) : voir
  [la procédure](../exploitation/02-procedures-arret-relance.md).
- Coût : 3 petites machines de plus.
