# Dimensionnement

Calcul fait pour la charge de pointe (soldes), avec une marge de croissance de 18 mois.

## Hypothèses

| Grandeur | Valeur |
| --- | --- |
| Commandes en pointe | 2 000 /s |
| Taille moyenne d'un événement (Avro, avant compression) | 1 Ko |
| Ratio de compression zstd | ~3 |
| Topics alimentés par commande (placed, enriched, repartition, changelog) | ~4 écritures |
| Facteur de réplication | 3 |
| Rétention moyenne pondérée | 10 jours |
| Croissance prévue | × 2 |

## Débit réseau

- Écriture applicative : 2 000 × 1 Ko × 4 topics ÷ 3 (compression) ≈ **2,7 Mo/s**
- Réplication : × (RF − 1) = × 2 → **5,4 Mo/s** entrant supplémentaire réparti entre brokers
- Lecture : chaque topic lu par 2 consommateurs en moyenne → ≈ **5,4 Mo/s** sortant
- Avec croissance × 2 : environ **27 Mo/s** au total sur le cluster, soit < 10 Mo/s par broker.

Une carte 10 Gb/s (≈ 1,2 Go/s) est très largement suffisante ; c'est la **latence disque** et le
nombre de partitions qui dimensionnent, pas la bande passante.

## Stockage

`débit écrit × rétention × RF × croissance`

2,7 Mo/s × 864 000 s (10 j) × 3 × 2 ≈ **14 To** au total, soit **~4,7 To par broker**.

On ajoute 30 % de marge (pic, réassignation, compaction) → **6 To utiles par broker**, en SSD/NVMe,
système de fichiers XFS. Avec le **tiered storage**, on pourrait ne garder que 2 jours en local
(≈ 1,3 To par broker) et le reste en stockage objet.

## Mémoire et CPU

| Rôle | vCPU | RAM | Heap JVM | Disque |
| --- | --- | --- | --- | --- |
| Broker | 8 | 64 Go | 6 Go (le reste sert de page cache) | 6 To NVMe |
| Contrôleur | 2 | 8 Go | 2 Go | 50 Go SSD |
| Kafka Connect worker | 4 | 16 Go | 8 Go | 50 Go |
| Schema Registry | 2 | 4 Go | 1 Go | 20 Go |
| order-enrichment (par instance) | 4 | 8 Go | 2 Go + RocksDB hors heap | 100 Go SSD (state store) |

## Nombre de partitions du cluster

Environ 60 partitions applicatives × 3 réplicas = 180 réplicas, plus les topics internes
(`__consumer_offsets` 50 × 3, `__transaction_state` 50 × 3). On reste très en dessous des
limites pratiques (plusieurs milliers de réplicas par broker avec KRaft).

## Quand ajouter un broker

| Signal (tableau de bord « Cluster Kafka ») | Seuil |
| --- | --- |
| `RequestHandlerAvgIdlePercent` | < 30 % durablement |
| Disque | > 70 % après purge de rétention |
| Latence p99 Produce | > 100 ms hors incident |

Un broker ajouté ne reçoit **aucune partition existante** : il faut lancer une réassignation
(procédure [MNT-03](../exploitation/06-maintenance.md)).
