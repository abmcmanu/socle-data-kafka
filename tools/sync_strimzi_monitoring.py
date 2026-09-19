#!/usr/bin/env python3
"""Génère les ressources de supervision Strimzi à partir de monitoring/ (source unique) :

  platform/strimzi/monitoring/kafka-metrics-configmap.yaml   règles JMX Exporter (brokers, Connect)
  platform/strimzi/monitoring/prometheus-rules.yaml          PrometheusRule (alertes + règles SLI)
  platform/strimzi/monitoring/grafana-dashboards.yaml        ConfigMap des tableaux de bord (sidecar Grafana)

Usage : python3 tools/sync_strimzi_monitoring.py [--check]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
MON = ROOT / "monitoring"
OUT = ROOT / "platform" / "strimzi" / "monitoring"
HEADER = "# FICHIER GÉNÉRÉ par tools/sync_strimzi_monitoring.py à partir de monitoring/. Ne pas modifier.\n"


class Literal(str):
    pass


def literal_representer(dumper, data):
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")


yaml.add_representer(Literal, literal_representer, Dumper=yaml.SafeDumper)


def dump(doc: dict) -> str:
    return HEADER + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=200)


def render() -> dict[Path, str]:
    metrics = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "socle-kafka-metrics", "labels": {"app.kubernetes.io/part-of": "socle-data"}},
        "data": {
            "kafka-metrics-config.yml": Literal((MON / "jmx-exporter" / "kafka-broker.yml").read_text(encoding="utf-8")),
            "connect-metrics-config.yml": Literal((MON / "jmx-exporter" / "kafka-connect.yml").read_text(encoding="utf-8")),
        },
    }
    rules_src = yaml.safe_load((MON / "prometheus" / "rules" / "kafka-alerts.yml").read_text(encoding="utf-8"))
    rules = {
        "apiVersion": "monitoring.coreos.com/v1",
        "kind": "PrometheusRule",
        "metadata": {"name": "socle-kafka-alerts", "labels": {"release": "kube-prometheus-stack"}},
        "spec": {"groups": rules_src["groups"]},
    }
    dashboards = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "socle-grafana-dashboards",
                     "labels": {"grafana_dashboard": "1"},
                     "annotations": {"grafana_folder": "Socle Data"}},
        "data": {p.name: Literal(p.read_text(encoding="utf-8"))
                 for p in sorted((MON / "grafana" / "dashboards").glob("*.json"))},
    }
    return {
        OUT / "kafka-metrics-configmap.yaml": dump(metrics),
        OUT / "prometheus-rules.yaml": dump(rules),
        OUT / "grafana-dashboards.yaml": dump(dashboards),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    files = render()
    if args.check:
        stale = [p for p, c in files.items() if not p.exists() or p.read_text(encoding="utf-8") != c]
        if stale:
            sys.exit("Supervision Strimzi désynchronisée : " + ", ".join(p.name for p in stale)
                     + "\nLancez : python3 tools/sync_strimzi_monitoring.py")
        print("OK : supervision Strimzi synchronisée avec monitoring/")
        return
    OUT.mkdir(parents=True, exist_ok=True)
    for path, content in files.items():
        path.write_text(content, encoding="utf-8")
    print(f"{len(files)} fichiers générés dans {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
