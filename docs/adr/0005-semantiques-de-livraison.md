# ADR 0005 — Sémantiques de livraison

**Statut** : acceptée

## Contexte

Les agrégats de chiffre d'affaires doivent être exacts (P1, pilotage) ; les autres flux tolèrent un
doublon s'il est absorbé en aval.

## Décision

| Segment | Sémantique | Mécanisme |
| --- | --- | --- |
| Client → ingestion-api | Au moins une fois | Le client réessaie ; `eventId` unique |
| ingestion-api → Kafka | Pas de doublon sur retry | Producer idempotent, `acks=all` |
| order-enrichment | Exactly-once | `exactly_once_v2` (transactions + `read_committed`) |
| Kafka → HDFS | Exactly-once | Offsets dans les noms de fichiers |
| Consommateurs externes | Au moins une fois | Idempotence côté cible (upsert par `eventId` / `orderId`) |

## Conséquences

- Latence ajoutée par les transactions ≈ `commit.interval.ms` (100 ms en EOS) : compatible avec le SLO de 5 s.
- Les consommateurs des topics produits par order-enrichment doivent lire en `read_committed`,
  sinon ils voient les messages des transactions annulées.
- Il faut le droit Write sur le `TransactionalId` dans les ACL.
