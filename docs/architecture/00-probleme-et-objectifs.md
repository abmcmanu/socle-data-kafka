# 00 — Problème et objectifs

## Contexte

L'enseigne vend sur trois canaux (web, mobile, magasins). Ses données vivent dans :

- une base **PostgreSQL CRM** (clients, segments, consentements) ;
- un **back-office commandes** exposé par API ;
- un **data lake Hadoop** (HDFS + Hive) alimenté chaque nuit par Sqoop et des scripts shell ;
- une dizaine d'applications qui s'appellent directement entre elles.

## Problèmes constatés

| # | Problème | Impact métier |
| --- | --- | --- |
| P1 | Données disponibles à J+1 dans Hadoop | Pilotage des ventes et détection de fraude en retard d'un jour |
| P2 | Intégrations point à point (N × M) | Chaque nouveau besoin = un nouveau développement ; une panne se propage |
| P3 | Aucun contrat de données | Ruptures silencieuses des traitements en aval |
| P4 | Exports nocturnes lourds sur la base CRM | Ralentissements de production, fenêtres de batch saturées |
| P5 | Supervision éclatée, pas de procédures | Incidents remontés par les utilisateurs, temps de résolution long |

## Objectifs mesurables

| Objectif | Indicateur (SLI) | Cible (SLO) |
| --- | --- | --- |
| Fraîcheur des commandes enrichies | Délai `placedAt` → écriture sur `orders.evt.order-enriched.v1`, p99 | < 5 s |
| Fraîcheur du data lake | Délai événement → fichier visible dans HDFS | < 15 min |
| Durabilité | Événements acquittés puis perdus | 0 |
| Disponibilité en écriture | Requêtes Produce réussies / total, sur 30 jours | ≥ 99,9 % |
| Exactitude des agrégats | Écart entre agrégat temps réel et recalcul batch | 0 (exactly-once) |
| Détection d'incident | Délai entre défaillance et alerte | < 5 min |
| Délai de mise à disposition d'un nouveau flux | Demande → topic créé, sécurisé, supervisé | < 1 jour (GitOps) |

## Périmètre

**Dans le périmètre** : cluster Kafka et ses composants (Schema Registry, Kafka Connect), deux
services de référence (ingestion et enrichissement), dépôt dans HDFS, gouvernance, supervision,
exploitation, déploiement VM et Kubernetes.

**Hors périmètre** : migration des traitements Hadoop existants, outil de BI, réplication
multi-région active-active (préparée dans le PRA mais non déployée).

## Contraintes

- L'exploitation actuelle est sur **VM Linux** gérées par Ansible ; la cible à 18 mois est
  **Kubernetes**. Le socle doit donc être déployable des deux façons, avec la même configuration.
- Le data lake **Hadoop reste** la référence analytique : le socle l'alimente, il ne le remplace pas.
- Composants open source ou sous licence communautaire uniquement.

## Ce que démontre ce projet

| Compétence | Où la voir |
| --- | --- |
| Conception de topics (clés, partitions, rétention, compaction) | [03-conception-des-topics.md](03-conception-des-topics.md), `governance/topics/catalog.yaml` |
| Garanties de livraison de bout en bout | [02-flux-de-donnees.md](02-flux-de-donnees.md), services Java |
| Contrats de données et évolution de schémas | `governance/schemas/avro`, ADR 0002 |
| Exploitation d'un cluster | `docs/exploitation`, `platform/ansible`, `scripts/ops` |
| Sécurité | [04-securite.md](04-securite.md), `platform/strimzi/users` |
| Observabilité | `monitoring/` |
| Dimensionnement | [05-dimensionnement.md](05-dimensionnement.md) |
