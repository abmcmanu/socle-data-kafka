# Conception des topics

Le catalogue [`governance/topics/catalog.yaml`](../../governance/topics/catalog.yaml) est la seule
source de vérité. Ce document explique **pourquoi** chaque paramètre a cette valeur.

## Convention de nommage

`<domaine>.<type>.<nom>.v<version>`

| Segment | Valeurs | Exemple |
| --- | --- | --- |
| domaine | Domaine métier propriétaire | `orders`, `crm`, `analytics`, `platform` |
| type | `evt` événement métier · `cdc` changement de base · `agg` agrégat · `dlq` rejets | `evt` |
| nom | kebab-case, au passé pour un événement | `order-placed` |
| version | Incrémentée seulement en cas de rupture de compatibilité | `v1` |

Les topics préfixés `_` sont techniques (internes à Kafka Connect). La convention permet des ACL par
préfixe : l'équipe commandes reçoit `Write` sur `orders.evt.` et rien d'autre.

## Choix des clés

| Topic | Clé | Raison |
| --- | --- | --- |
| `orders.evt.order-placed.v1` | `orderId` | Tous les événements d'une commande dans la même partition, donc ordonnés |
| `crm.cdc.customers.v1` | `id` client | La compaction garde le dernier état **par clé** : la clé doit être l'identité de l'entité |
| `analytics.agg.revenue-by-country-1m.v1` | `pays:devise` | Un consommateur lit toutes les fenêtres d'un pays dans l'ordre |

Une clé nulle aurait réparti les messages sans ordre ; elle est interdite sur ces topics.

## Nombre de partitions

Formule utilisée : `partitions = max(débit_cible / débit_par_consommateur, parallélisme_souhaité)`,
arrondie à un multiple du nombre de brokers pour équilibrer les leaders.

| Topic | Débit de pointe estimé | Débit d'un consommateur | Calcul | Retenu |
| --- | --- | --- | --- | --- |
| order-placed | 2 000 msg/s (soldes) | ~300 msg/s (jointure + écriture transactionnelle) | 2000 / 300 ≈ 7 | **12** (marge 18 mois, multiple de 3) |
| customers (CDC) | 50 msg/s | — | Co-partitionnement avec order-placed pour la jointure | **12** |
| revenue 1 min | ~200 msg/min | — | Faible volume | **6** |
| DLQ | Très faible | — | — | **3** |

On peut ajouter des partitions mais jamais en retirer, et l'ajout change la partition de certaines
clés. On dimensionne donc large dès le départ.

## Rétention et compaction

| Topic | Politique | Valeur | Raison |
| --- | --- | --- | --- |
| order-placed | delete | 7 jours | Couvre un week-end prolongé d'incident aval + rejeu |
| order-enriched | delete | 14 jours | Permet de reconstruire un consommateur aval ; l'historique long est dans HDFS |
| customers | compact | — | État courant de chaque client, rejouable intégralement |
| DLQ | delete | 30 jours | Temps d'analyse et de correction |

Pour `crm.cdc.customers.v1` :

- `min.compaction.lag.ms=1h` : un consommateur en léger retard voit quand même les états intermédiaires ;
- `delete.retention.ms=24h` : les tombstones restent un jour, le temps que tous les consommateurs les lisent ;
- `segment.ms=24h` : force la fermeture quotidienne du segment actif, sinon la compaction ne
  l'atteint jamais sur un topic peu alimenté.

## Paramètres de durabilité (tous les topics)

| Paramètre | Valeur | Effet |
| --- | --- | --- |
| replicas | 3 | Une copie par zone |
| `min.insync.replicas` | 2 | Une écriture `acks=all` exige 2 copies ; tolère la perte d'un broker |
| `unclean.leader.election.enable` | false | Jamais de leader hors ISR, donc jamais de perte silencieuse |
| `compression.type` | producer | Le broker garde la compression choisie par le producer (zstd), sans recompresser |

## Versionner un topic

Une évolution **compatible** (ajout d'un champ avec valeur par défaut) se fait sur le même topic,
contrôlée par le Schema Registry. Une rupture (changement de clé, suppression d'un champ obligatoire)
impose un nouveau topic `v2` : le producteur écrit sur les deux pendant la migration, les
consommateurs basculent un à un, puis `v1` est retiré après sa rétention.
