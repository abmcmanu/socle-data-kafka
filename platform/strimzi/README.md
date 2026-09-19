# Déploiement Kubernetes avec Strimzi

Prérequis : opérateur **Strimzi ≥ 0.49** (API `kafka.strimzi.io/v1beta2`, supportée jusqu'à Strimzi 1.0),
**kube-prometheus-stack** pour la supervision, une `StorageClass` nommée `ssd`, des nœuds répartis
sur trois zones (`topology.kubernetes.io/zone`).

```bash
helm install strimzi oci://quay.io/strimzi-helm/strimzi-kafka-operator -n strimzi --create-namespace \
  --set watchNamespaces="{socle-data}"
kubectl apply -k platform/strimzi
kubectl -n socle-data wait kafka/socle --for=condition=Ready --timeout=15m
```

| Dossier | Ressources | Généré ? |
| --- | --- | --- |
| `cluster/` | `KafkaNodePool` contrôleurs et brokers, `Kafka` (listeners mTLS/SCRAM, ACL, rack, Cruise Control, Kafka Exporter), `KafkaRebalance` d'exemple | Non |
| `topics/` | Un `KafkaTopic` par entrée du catalogue | **Oui** : `tools/topics.py` |
| `users/` | `KafkaUser` avec ACL au moindre privilège et quotas | Non |
| `schema-registry/` | Deployment, Service, PodDisruptionBudget | Non |
| `connect/` | `KafkaConnect` (image construite avec les plugins), `KafkaConnector` CDC et HDFS, RBAC des secrets | Non |
| `monitoring/` | ConfigMap JMX, `PodMonitor`, `PrometheusRule`, tableaux de bord Grafana | **Oui** (sauf PodMonitor) : `tools/sync_strimzi_monitoring.py` |

Les manifestes sont validés par `kubeconform` contre les schémas des CRD (CI).

## Ce que Strimzi fait à la place des procédures manuelles

| Opération | VM (Ansible) | Strimzi |
| --- | --- | --- |
| Redémarrage roulant | `rolling-restart.yml` | Automatique à chaque changement ; manuel : `kubectl annotate strimzipodset socle-brokers strimzi.io/manual-rolling-update=true` |
| Certificats | PKI, renouvellement manuel | Autorités et certificats générés et renouvelés par l'opérateur |
| Ajout de broker | Réassignation manuelle | `replicas: 4` sur le pool + `KafkaRebalance` (Cruise Control) |
| Topics et ACL | Script généré / `kafka-acls.sh` | Topic Operator et User Operator (réconciliation continue) |
| Montée de version | Binaires + `metadata.version` | Changer `spec.kafka.version`, puis `metadataVersion` |

Procédures d'arrêt et de relance propres à Kubernetes : [PROC-05 et PROC-06](../../docs/exploitation/02-procedures-arret-relance.md).
