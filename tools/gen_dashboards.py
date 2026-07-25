#!/usr/bin/env python3
"""Génère les tableaux de bord Grafana du socle (« dashboards as code »).

Sortie : monitoring/grafana/dashboards/*.json, chargés par le provisioning Grafana (lab, Ansible)
et par les ConfigMap Grafana sur Kubernetes.

Usage :
  python3 tools/gen_dashboards.py          # régénère
  python3 tools/gen_dashboards.py --check  # CI : échoue si les JSON ne sont pas à jour
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / "monitoring" / "grafana" / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}


class Board:
    def __init__(self, uid: str, title: str, description: str, tags: list[str]):
        self.uid, self.title, self.description, self.tags = uid, title, description, tags
        self.panels: list[dict] = []
        self.y = 0
        self.x = 0
        self.row_h = 0
        self.next_id = 1
        self.variables: list[dict] = []

    def _place(self, w: int, h: int) -> dict:
        if self.x + w > 24:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def row(self, title: str) -> None:
        if self.x:
            self.x, self.y, self.row_h = 0, self.y + self.row_h, 0
        self.panels.append({"type": "row", "title": title, "id": self._id(), "collapsed": False,
                            "gridPos": {"x": 0, "y": self.y, "w": 24, "h": 1}, "panels": []})
        self.y += 1

    def _id(self) -> int:
        self.next_id += 1
        return self.next_id - 1

    def stat(self, title: str, expr: str, desc: str, w: int = 4, unit: str = "short",
             steps: list[tuple[str, float | None]] | None = None, decimals: int | None = None) -> None:
        steps = steps or [("green", None)]
        panel = {
            "type": "stat", "title": title, "description": desc, "id": self._id(),
            "datasource": DS, "gridPos": self._place(w, 4),
            "targets": [{"refId": "A", "expr": expr, "datasource": DS, "instant": True}],
            "fieldConfig": {"defaults": {
                "unit": unit,
                "thresholds": {"mode": "absolute",
                               "steps": [{"color": c, "value": v} for c, v in steps]},
                "color": {"mode": "thresholds"}},
                "overrides": []},
            "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                        "colorMode": "background", "graphMode": "none", "textMode": "value",
                        "justifyMode": "center", "orientation": "auto"},
        }
        if decimals is not None:
            panel["fieldConfig"]["defaults"]["decimals"] = decimals
        self.panels.append(panel)

    def series(self, title: str, targets: list[tuple[str, str]], desc: str, w: int = 12, h: int = 8,
               unit: str = "short", threshold: float | None = None, stack: bool = False) -> None:
        defaults = {
            "unit": unit,
            "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 10 if not stack else 40,
                       "showPoints": "never", "spanNulls": True,
                       "stacking": {"mode": "normal" if stack else "none", "group": "A"},
                       "thresholdsStyle": {"mode": "line" if threshold is not None else "off"}},
            "color": {"mode": "palette-classic"},
        }
        if threshold is not None:
            defaults["thresholds"] = {"mode": "absolute", "steps": [
                {"color": "green", "value": None}, {"color": "red", "value": threshold}]}
        self.panels.append({
            "type": "timeseries", "title": title, "description": desc, "id": self._id(),
            "datasource": DS, "gridPos": self._place(w, h),
            "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": legend, "datasource": DS}
                        for i, (e, legend) in enumerate(targets)],
            "fieldConfig": {"defaults": defaults, "overrides": []},
            "options": {"legend": {"displayMode": "table", "placement": "bottom",
                                   "calcs": ["lastNotNull", "max"]},
                        "tooltip": {"mode": "multi", "sort": "desc"}},
        })

    def table(self, title: str, expr: str, desc: str, w: int = 24, h: int = 8) -> None:
        self.panels.append({
            "type": "table", "title": title, "description": desc, "id": self._id(),
            "datasource": DS, "gridPos": self._place(w, h),
            "targets": [{"refId": "A", "expr": expr, "format": "table", "instant": True, "datasource": DS}],
            "transformations": [{"id": "organize", "options": {"excludeByName": {"Time": True}}}],
            "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True, "sortBy": [{"displayName": "Value", "desc": True}]},
        })

    def text(self, content: str, w: int = 24, h: int = 3) -> None:
        self.panels.append({"type": "text", "title": "", "id": self._id(), "gridPos": self._place(w, h),
                            "options": {"mode": "markdown", "content": content}})

    def variable(self, name: str, label: str, query: str) -> None:
        self.variables.append({
            "name": name, "label": label, "type": "query", "datasource": DS,
            "query": {"query": query, "refId": "var"}, "definition": query,
            "refresh": 2, "includeAll": True, "multi": True, "allValue": ".*",
            "current": {"selected": True, "text": "All", "value": "$__all"}, "sort": 1,
        })

    def to_json(self) -> dict:
        return {
            "uid": self.uid, "title": self.title, "description": self.description, "tags": self.tags,
            "timezone": "browser", "schemaVersion": 39, "version": 1, "editable": True,
            "graphTooltip": 1, "refresh": "30s", "time": {"from": "now-3h", "to": "now"},
            "templating": {"list": self.variables}, "annotations": {"list": []},
            "links": [{"title": "Tableaux de bord du socle", "type": "dashboards", "tags": ["socle"],
                       "asDropdown": True}],
            "panels": self.panels,
        }


RED_IF_POSITIVE = [("green", None), ("red", 1)]


def overview() -> Board:
    b = Board("socle-overview", "Socle Data — Vue d'ensemble",
              "État global du socle et SLO. Point d'entrée de l'astreinte.", ["socle", "slo"])
    b.text("**Lecture** : tout doit être vert. Une case rouge renvoie à l'alerte du même nom et à son "
           "runbook (`docs/exploitation/04-runbooks.md`). Le détail est dans les autres tableaux de bord.", h=2)
    b.row("Santé du cluster")
    b.stat("Brokers en ligne", 'count(up{job="kafka-brokers"} == 1)', "Nombre de brokers collectés.",
           steps=[("red", None), ("orange", 2), ("green", 3)])
    b.stat("Contrôleurs en ligne", 'count(up{job="kafka-controllers"} == 1)', "Quorum KRaft : 2 minimum.",
           steps=[("red", None), ("orange", 2), ("green", 3)])
    b.stat("Contrôleur actif", 'sum(kafka_controller_kafkacontroller_activecontrollercount)',
           "Doit valoir exactement 1.", steps=[("red", None), ("green", 1), ("red", 2)])
    b.stat("Partitions hors ligne", 'sum(kafka_controller_kafkacontroller_offlinepartitionscount) or vector(0)',
           "Partitions sans leader.", steps=RED_IF_POSITIVE)
    b.stat("Sous min ISR", 'sum(kafka_server_replicamanager_underminisrpartitioncount) or vector(0)',
           "Écritures acks=all refusées sur ces partitions.", steps=RED_IF_POSITIVE)
    b.stat("Sous-répliquées", 'sum(kafka_server_replicamanager_underreplicatedpartitions) or vector(0)',
           "Partitions avec au moins une réplica hors ISR.", steps=[("green", None), ("orange", 1)])
    b.row("Flux et SLO")
    b.stat("Disponibilité ingestion (5 min)", 'socle:ingestion_availability:ratio_rate5m',
           "Part des requêtes ingestion-api sans erreur 5xx. SLO : 99,9 %.", unit="percentunit", w=6,
           steps=[("red", None), ("orange", 0.99), ("green", 0.999)], decimals=2)
    b.stat("Messages entrants / s", 'socle:messages_in:rate5m', "Tous topics confondus.", w=6, unit="ops")
    b.stat("Lag order-enrichment", 'sum(kafka_consumergroup_lag{consumergroup="order-enrichment"}) or vector(0)',
           "Retard du traitement d'enrichissement.", w=6,
           steps=[("green", None), ("orange", 1000), ("red", 10000)])
    b.stat("Alertes actives", 'count(ALERTS{alertstate="firing"}) or vector(0)',
           "Alertes en cours dans Prometheus.", w=6, steps=[("green", None), ("orange", 1)])
    b.series("Débit entrant par topic", [
        ('sum by (topic) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic!~"__.*|_.*", topic!=""}[5m]))', "{{topic}}")],
        "Messages par seconde écrits dans chaque topic métier.", unit="ops")
    b.series("Lag par consumer group", [('socle:consumer_lag:sum', "{{consumergroup}}")],
             "Messages en attente de traitement.", threshold=10000)
    b.series("Commandes rejetées (DLQ)", [
        ('sum(increase(kafka_server_brokertopicmetrics_messagesin_total{topic=~".*dlq.*"}[5m])) by (topic)', "{{topic}}")],
        "Messages arrivés dans les topics DLQ par tranche de 5 minutes.")
    b.series("Disponibilité des points d'entrée", [('probe_success', "{{instance}}")],
             "1 = disponible. Servitudes et API sondées par blackbox-exporter.")
    return b


def cluster() -> Board:
    b = Board("socle-kafka-cluster", "Socle Data — Cluster Kafka",
              "Brokers et contrôleurs : réplication, charge, latences, ressources.", ["socle", "kafka"])
    b.variable("instance", "Broker", 'label_values(up{job="kafka-brokers"}, instance)')
    b.row("Réplication")
    b.series("Partitions sous-répliquées", [
        ('kafka_server_replicamanager_underreplicatedpartitions{instance=~"$instance"}', "{{instance}}")],
        "Doit rester à 0. Une valeur durable signale un broker ou un disque lent.", w=8, threshold=1)
    b.series("ISR : réductions / élargissements", [
        ('sum by (instance) (rate(kafka_server_replicamanager_isrshrinks_total{instance=~"$instance"}[5m]))', "shrink {{instance}}"),
        ('sum by (instance) (rate(kafka_server_replicamanager_isrexpands_total{instance=~"$instance"}[5m]))', "expand {{instance}}")],
        "Des oscillations répétées indiquent un réseau ou un GC instable.", w=8)
    b.series("Leaders et partitions par broker", [
        ('kafka_server_replicamanager_leadercount{instance=~"$instance"}', "leaders {{instance}}"),
        ('kafka_server_replicamanager_partitioncount{instance=~"$instance"}', "partitions {{instance}}")],
        "Un déséquilibre des leaders appelle une élection du preferred leader.", w=8)
    b.row("Charge et latences")
    b.series("Inactivité des threads I/O", [
        ('kafka_server_kafkarequesthandlerpool_requesthandleravgidlepercent{instance=~"$instance"}', "{{instance}}")],
        "Sous 30 % : le broker sature.", w=8, unit="percentunit", threshold=0.3)
    b.series("Inactivité des threads réseau", [
        ('kafka_network_socketserver_networkprocessoravgidlepercent{instance=~"$instance"}', "{{instance}}")],
        "Sous 30 % : trop de connexions ou de requêtes.", w=8, unit="percentunit", threshold=0.3)
    b.series("File d'attente des requêtes", [
        ('kafka_network_requestchannel_requestqueuesize{instance=~"$instance"}', "{{instance}}")],
        "Requêtes en attente d'un thread I/O.", w=8)
    b.series("Latence totale p99 par requête", [
        ('kafka_network_requestmetrics_totaltimems{quantile="0.99", instance=~"$instance"}', "{{request}} {{instance}}")],
        "Produce, FetchConsumer, FetchFollower. Seuil d'alerte Produce : 200 ms.", unit="ms", threshold=200)
    b.series("Décomposition de la latence Produce p99", [
        ('kafka_network_requestmetrics_requestqueuetimems{request="Produce", quantile="0.99", instance=~"$instance"}', "file {{instance}}"),
        ('kafka_network_requestmetrics_localtimems{request="Produce", quantile="0.99", instance=~"$instance"}', "écriture locale {{instance}}"),
        ('kafka_network_requestmetrics_remotetimems{request="Produce", quantile="0.99", instance=~"$instance"}', "attente réplication {{instance}}")],
        "RemoteTime élevé = followers lents (acks=all). LocalTime élevé = disque.", unit="ms")
    b.row("Débits")
    b.series("Octets entrants / sortants", [
        ('sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic="", instance=~"$instance"}[5m]))', "in {{instance}}"),
        ('sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesout_total{topic="", instance=~"$instance"}[5m]))', "out {{instance}}")],
        "Trafic client (hors réplication).", unit="Bps")
    b.series("Requêtes par seconde", [
        ('sum by (request) (rate(kafka_network_requestmetrics_requests_total{instance=~"$instance", request=~"Produce|Fetch|Metadata|OffsetCommit|JoinGroup|Heartbeat"}[5m]))', "{{request}}")],
        "Un pic de Metadata ou JoinGroup trahit des clients mal configurés.", unit="reqps")
    b.row("Ressources")
    b.series("Heap JVM", [
        ('jvm_memory_used_bytes{area="heap", job=~"kafka-brokers|kafka-controllers"}', "{{instance}}")],
        "Heap utilisée. Le reste de la RAM sert de page cache.", w=8, unit="bytes")
    b.series("Temps de GC", [
        ('sum by (instance) (rate(jvm_gc_collection_seconds_sum{job=~"kafka-brokers|kafka-controllers"}[5m]))', "{{instance}}")],
        "Secondes de GC par seconde. Des pauses longues font sortir le broker de l'ISR.", w=8, unit="s")
    b.series("Espace disque utilisé", [
        ('1 - node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|squashfs"} / node_filesystem_size_bytes{fstype!~"tmpfs|overlay|squashfs"}', "{{mountpoint}}")],
        "Alerte à 75 % (warning) et 85 % (critique).", w=8, unit="percentunit", threshold=0.75)
    b.table("Taille des topics sur disque (par broker)",
            'sum by (topic, instance) (kafka_log_log_size{topic!~"__.*"})',
            "Octets occupés par chaque topic, toutes partitions, sur chaque broker.")
    return b


def consumers() -> Board:
    b = Board("socle-consumers", "Socle Data — Consommateurs et lag",
              "Retard des consumer groups, membres actifs, applications Kafka Streams.", ["socle", "consumers"])
    b.variable("group", "Consumer group", "label_values(kafka_consumergroup_lag, consumergroup)")
    b.row("Lag")
    b.series("Lag total par groupe", [
        ('sum by (consumergroup) (kafka_consumergroup_lag{consumergroup=~"$group"})', "{{consumergroup}}")],
        "Messages produits mais pas encore consommés. Alerte au-delà de 10 000 pendant 10 min.", threshold=10000)
    b.series("Lag par partition", [
        ('kafka_consumergroup_lag{consumergroup=~"$group"}', "{{consumergroup}} {{topic}}-{{partition}}")],
        "Une seule partition en retard = clé « chaude » ou consommateur bloqué.")
    b.series("Vitesse de consommation", [
        ('sum by (consumergroup) (rate(kafka_consumergroup_current_offset_sum{consumergroup=~"$group"}[5m]))', "{{consumergroup}}")],
        "Offsets committés par seconde.", unit="ops")
    b.series("Membres actifs par groupe", [
        ('kafka_consumergroup_members{consumergroup=~"$group"}', "{{consumergroup}}")],
        "0 membre sur un groupe critique = traitement arrêté.")
    b.row("Kafka Streams — order-enrichment")
    b.series("État du client Streams", [
        ('kafka_streams_client_state', "{{state}}")],
        "RUNNING attendu. REBALANCING bref acceptable, ERROR = alerte.", w=8)
    b.series("Débit de traitement", [
        ('sum(kafka_streams_thread_process_rate)', "records/s")],
        "Enregistrements traités par seconde, tous threads.", w=8, unit="ops")
    b.series("Latence de commit (transactions)", [
        ('avg(kafka_streams_thread_commit_latency_avg)', "commit ms")],
        "Durée moyenne d'un commit exactly-once.", w=8, unit="ms")
    b.table("Lag courant (instantané)",
            'sum by (consumergroup, topic) (kafka_consumergroup_lag{consumergroup=~"$group"})',
            "Vue tabulaire triée par lag décroissant.")
    return b


def connect_apps() -> Board:
    b = Board("socle-connect-apps", "Socle Data — Kafka Connect et applications",
              "Connecteurs CDC et HDFS, erreurs, DLQ, API d'ingestion, Schema Registry.", ["socle", "connect"])
    b.row("Kafka Connect")
    b.stat("Connecteurs RUNNING", 'count(kafka_connect_connector_status{status="running"}) or vector(0)',
           "Connecteurs démarrés.", w=6, steps=[("red", None), ("green", 1)])
    b.stat("Connecteurs FAILED", 'count(kafka_connect_connector_status{status="failed"}) or vector(0)',
           "Connecteurs en échec.", w=6, steps=RED_IF_POSITIVE)
    b.stat("Tâches FAILED", 'count(kafka_connect_connector_task_status{status="failed"}) or vector(0)',
           "Une tâche en échec ne redémarre pas seule.", w=6, steps=RED_IF_POSITIVE)
    b.stat("Messages en DLQ (24 h)", 'sum(delta(kafka_connect_task_error_deadletterqueue_produce_requests[24h])) or vector(0)',
           "Messages envoyés en DLQ par les connecteurs sink.", w=6, steps=[("green", None), ("orange", 1)])
    b.table("État des tâches", 'kafka_connect_connector_task_status == 1',
            "Une ligne par tâche et par état.", h=7)
    b.series("Débit des connecteurs source (CDC)", [
        ('sum by (connector) (kafka_connect_source_task_source_record_poll_rate)', "{{connector}}")],
        "Changements lus dans la base par seconde.", unit="ops")
    b.series("Débit des connecteurs sink (HDFS)", [
        ('sum by (connector) (kafka_connect_sink_task_sink_record_read_rate)', "{{connector}}")],
        "Messages lus depuis Kafka par seconde.", unit="ops")
    b.row("ingestion-api")
    b.series("Requêtes HTTP par statut", [
        ('sum by (status) (rate(http_server_requests_seconds_count{job="ingestion-api", uri!~"/actuator.*"}[5m]))', "{{status}}")],
        "202 = commande acceptée, 400 = invalide, 503 = Kafka indisponible.", w=8, unit="reqps")
    b.series("Latence HTTP p99", [
        ('histogram_quantile(0.99, sum by (le) (rate(http_server_requests_seconds_bucket{job="ingestion-api", uri="/api/v1/orders"}[5m])))', "p99")],
        "Inclut l'attente de l'acquittement Kafka (acks=all).", w=8, unit="s")
    b.series("Envois Kafka de l'API", [
        ('sum(rate(socle_orders_published_total{job="ingestion-api"}[5m]))', "publiées"),
        ('sum(rate(socle_orders_publish_failed_total{job="ingestion-api"}[5m]))', "échecs")],
        "Compteurs métier exposés par l'application.", w=8, unit="ops")
    b.row("Schema Registry")
    b.series("Requêtes et erreurs", [
        ('sum(schema_registry_jersey_request_rate)', "requêtes/s"),
        ('sum(schema_registry_jersey_request_error_rate)', "erreurs/s")],
        "Les clients mettent les schémas en cache : le trafic doit rester faible.", w=12, unit="reqps")
    b.series("Schémas enregistrés", [('schema_registry_registered_count', "{{instance}}")],
             "Nombre de schémas connus.", w=12)
    return b


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    boards = [overview(), cluster(), consumers(), connect_apps()]
    stale = []
    for b in boards:
        path = OUT / f"{b.uid}.json"
        content = json.dumps(b.to_json(), indent=2, ensure_ascii=False) + "\n"
        if args.check:
            if not path.exists() or path.read_text(encoding="utf-8") != content:
                stale.append(path)
        else:
            OUT.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
    if args.check:
        if stale:
            sys.exit("Tableaux de bord désynchronisés : " + ", ".join(p.name for p in stale))
        print(f"OK : {len(boards)} tableaux de bord à jour")
    else:
        print(f"{len(boards)} tableaux de bord générés dans {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
