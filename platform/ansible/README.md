# Déploiement sur VM avec Ansible

| Playbook | Usage | Procédure |
| --- | --- | --- |
| `site.yml` | Installation ou mise en conformité complète | — |
| `healthcheck.yml` | Contrôle de santé (quorum, brokers, partitions, lag, connecteurs, NTP, disque, certificats) | PROC-01 |
| `stop.yml` | Arrêt complet ordonné, avec confirmation | PROC-02 |
| `start.yml` | Relance complète ordonnée, contrôlée à chaque étape | PROC-03 |
| `rolling-restart.yml` | Redémarrage roulant sans interruption (configuration, correctif, montée de version) | PROC-04 |
| `apply-topics.yml` | Application du catalogue des topics | MNT-01 |
| `export-config.yml` | Export daté de la configuration (topics, ACL, groupes, schémas, connecteurs) | PROC-07 |

```bash
pip install ansible-core && ansible-galaxy collection install -r requirements.yml
ansible-playbook -i inventories/prod/hosts.yml playbooks/healthcheck.yml --ask-vault-pass
```

## Rôles

| Rôle | Contenu |
| --- | --- |
| `common` | Java 21, sysctl (`vm.swappiness=1`, `vm.max_map_count`), limites de fichiers, NTP (chrony), utilisateur `kafka` |
| `monitoring_agent` | Agent JMX Exporter (mêmes règles que le lab et Strimzi), node_exporter |
| `kafka` | Binaires vérifiés (SHA-512), `server.properties` KRaft (contrôleur ou broker selon le groupe), formatage idempotent, systemd avec arrêt contrôlé |
| `schema_registry` | Paquet Confluent, compatibilité `backward_transitive` |
| `kafka_connect` | Worker distribué, plugins Debezium / HDFS / Avro, secrets via `FileConfigProvider` |
| `monitoring_stack` | Prometheus, Alertmanager, Grafana, blackbox, kafka-exporter (Docker Compose), configuration générée depuis l'inventaire |

## Principes

- **Un changement de configuration ne redémarre jamais tout le cluster** : le handler du rôle `kafka`
  signale seulement ; l'application se fait par `rolling-restart.yml`.
- **Contrôleurs avant brokers**, brokers en `serial: 1`, **leader du quorum en dernier**.
- **On n'avance que si la réplication est revenue** (`--under-replicated-partitions` vide).
- Secrets dans Ansible Vault (`inventories/prod/group_vars/vault.yml`, modèle fourni).
- `ansible-lint` (profil `production`) passe sans erreur ; la CI le vérifie.
