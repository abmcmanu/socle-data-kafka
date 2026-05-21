#!/usr/bin/env python3
"""Génère les artefacts de déploiement des topics à partir de governance/topics/catalog.yaml.

Sorties :
  governance/generated/create-topics.sh  script idempotent (kafka-topics.sh / kafka-configs.sh)
  platform/strimzi/topics/<nom>.yaml     ressources KafkaTopic pour Strimzi

Usage :
  python3 tools/topics.py            # régénère
  python3 tools/topics.py --check    # échoue si les fichiers générés ne sont pas à jour (CI)
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
CATALOG = ROOT / "governance" / "topics" / "catalog.yaml"
SHELL_OUT = ROOT / "governance" / "generated" / "create-topics.sh"
STRIMZI_DIR = ROOT / "platform" / "strimzi" / "topics"
STRIMZI_CLUSTER = "socle"

NAME_RE = re.compile(r"^_?[a-z0-9]+(\.[a-z0-9-]+)+(\.v\d+)?$")
REQUIRED = ("name", "owner", "description", "partitions")


def load_catalog() -> tuple[dict, list[dict]]:
    data = yaml.safe_load(CATALOG.read_text(encoding="utf-8"))
    defaults = data.get("defaults", {})
    topics = data["topics"]
    errors = []
    seen = set()
    for t in topics:
        for field in REQUIRED:
            if field not in t:
                errors.append(f"{t.get('name', '?')}: champ obligatoire manquant '{field}'")
        name = t.get("name", "")
        if not NAME_RE.match(name):
            errors.append(f"{name}: ne respecte pas la convention <domaine>.<type>.<nom>.v<N>")
        if name in seen:
            errors.append(f"{name}: déclaré deux fois")
        seen.add(name)
        if not name.startswith("_") and not name.endswith(tuple(f".v{i}" for i in range(1, 100))):
            errors.append(f"{name}: version obligatoire (.vN) pour un topic métier")
    if errors:
        sys.exit("Catalogue invalide :\n  " + "\n  ".join(errors))
    return defaults, topics


def merged(defaults: dict, topic: dict) -> tuple[int, dict]:
    replicas = int(topic.get("replicas", defaults.get("replicas", 3)))
    config = dict(defaults.get("config", {}))
    config.update(topic.get("config", {}))
    return replicas, {k: str(v) for k, v in config.items()}


def render_shell(defaults: dict, topics: list[dict]) -> str:
    lines = [
        "#!/usr/bin/env bash",
        "# FICHIER GÉNÉRÉ par tools/topics.py à partir de governance/topics/catalog.yaml. Ne pas modifier.",
        "# Idempotent : crée les topics absents, aligne la configuration des topics existants.",
        "# Le nombre de partitions n'est jamais réduit (impossible dans Kafka) ; il peut être augmenté.",
        "set -euo pipefail",
        'BOOTSTRAP="${BOOTSTRAP:-broker-1:19092}"',
        'KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"',
        'CMD_CONFIG="${CMD_CONFIG:-}"',
        'extra=()',
        'if [[ -n "$CMD_CONFIG" ]]; then extra=(--command-config "$CMD_CONFIG"); fi',
        "",
        "ensure_topic() {",
        '  local name="$1" partitions="$2" replicas="$3" config="$4"',
        '  if "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --describe --topic "$name" >/dev/null 2>&1; then',
        '    current=$("$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --describe --topic "$name" | grep -o "PartitionCount: *[0-9]*" | grep -o "[0-9]*")',
        '    if (( partitions > current )); then',
        '      echo "[topics] $name : $current -> $partitions partitions"',
        '      "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --alter --topic "$name" --partitions "$partitions"',
        "    fi",
        '    "$KAFKA_BIN/kafka-configs.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --alter --entity-type topics --entity-name "$name" --add-config "$config" >/dev/null',
        '    echo "[topics] $name : configuration alignée"',
        "  else",
        '    echo "[topics] $name : création ($partitions partitions, RF=$replicas)"',
        '    local cfg_args=()',
        '    IFS="," read -ra pairs <<< "$config"',
        '    for p in "${pairs[@]}"; do cfg_args+=(--config "$p"); done',
        '    "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "${extra[@]}" --create --if-not-exists \\',
        '      --topic "$name" --partitions "$partitions" --replication-factor "$replicas" "${cfg_args[@]}"',
        "  fi",
        "}",
        "",
    ]
    for t in topics:
        replicas, config = merged(defaults, t)
        cfg = ",".join(f"{k}={v}" for k, v in sorted(config.items()))
        lines.append(f'ensure_topic "{t["name"]}" {int(t["partitions"])} {replicas} "{cfg}"')
    lines.append('echo "[topics] catalogue appliqué : %d topics"' % len(topics))
    return "\n".join(lines) + "\n"


def k8s_name(topic_name: str) -> str:
    name = topic_name.lstrip("_").replace("_", "-").lower()
    return re.sub(r"[^a-z0-9.-]", "-", name)


def render_strimzi(defaults: dict, topic: dict) -> str:
    replicas, config = merged(defaults, topic)
    doc = {
        "apiVersion": "kafka.strimzi.io/v1beta2",
        "kind": "KafkaTopic",
        "metadata": {
            "name": k8s_name(topic["name"]),
            "labels": {"strimzi.io/cluster": STRIMZI_CLUSTER},
            "annotations": {
                "socle.io/owner": topic["owner"],
                "socle.io/description": topic["description"],
            },
        },
        "spec": {
            "topicName": topic["name"],
            "partitions": int(topic["partitions"]),
            "replicas": replicas,
            "config": config,
        },
    }
    header = "# FICHIER GÉNÉRÉ par tools/topics.py — modifier governance/topics/catalog.yaml\n"
    return header + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="vérifie sans écrire")
    args = parser.parse_args()

    defaults, topics = load_catalog()
    expected: dict[Path, str] = {SHELL_OUT: render_shell(defaults, topics)}
    for t in topics:
        expected[STRIMZI_DIR / f"{k8s_name(t['name'])}.yaml"] = render_strimzi(defaults, t)

    stale = [p for p, c in expected.items() if not p.exists() or p.read_text(encoding="utf-8") != c]
    orphans = [p for p in STRIMZI_DIR.glob("*.yaml") if p not in expected and p.name != "kustomization.yaml"]

    if args.check:
        if stale or orphans:
            for p in stale + orphans:
                print(f"désynchronisé : {p.relative_to(ROOT)}")
            sys.exit("Lancez : python3 tools/topics.py")
        print(f"OK : {len(topics)} topics, fichiers générés à jour")
        return

    STRIMZI_DIR.mkdir(parents=True, exist_ok=True)
    SHELL_OUT.parent.mkdir(parents=True, exist_ok=True)
    for p in orphans:
        p.unlink()
    for path, content in expected.items():
        path.write_text(content, encoding="utf-8")
    SHELL_OUT.chmod(0o755)
    kustom = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": sorted(p.name for p in expected if p.parent == STRIMZI_DIR),
    }
    (STRIMZI_DIR / "kustomization.yaml").write_text(
        "# FICHIER GÉNÉRÉ par tools/topics.py\n" + yaml.safe_dump(kustom, sort_keys=False), encoding="utf-8"
    )
    print(f"{len(topics)} topics générés")


if __name__ == "__main__":
    main()
