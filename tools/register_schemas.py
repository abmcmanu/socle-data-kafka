#!/usr/bin/env python3
"""Enregistre les schémas Avro de governance/schemas/avro dans le Schema Registry.

- Les types nommés partagés (Channel.avsc) sont intégrés dans chaque schéma à leur première
  utilisation, comme le fait le compilateur Avro : le schéma enregistré est donc identique à celui
  des classes Java générées, ce qui permet auto.register.schemas=false côté applications.
- Fixe le niveau de compatibilité de chaque subject.
- Teste la compatibilité AVANT d'enregistrer : un schéma incompatible fait échouer la commande.

Bibliothèque standard uniquement (exécutable dans une image python:3-alpine).

Usage :
  python3 tools/register_schemas.py --url http://localhost:8081            # enregistre
  python3 tools/register_schemas.py --url http://localhost:8081 --dry-run  # teste seulement
  python3 tools/register_schemas.py --resolve-only                         # affiche les schémas résolus
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_DIR = ROOT / "governance" / "schemas" / "avro"
CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"


def load_subjects() -> dict:
    # Mini-analyse du YAML pour rester sans dépendance (format volontairement simple).
    result = {"defaultCompatibility": "BACKWARD", "references": [], "subjects": []}
    current = None
    section = None
    for raw in (SCHEMA_DIR / "subjects.yaml").read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].rstrip()
        if not line.strip():
            continue
        if line.startswith("defaultCompatibility:"):
            result["defaultCompatibility"] = line.split(":", 1)[1].strip()
        elif line.startswith("references:"):
            section = "references"
        elif line.startswith("subjects:"):
            section = "subjects"
        elif section == "references" and line.strip().startswith("- "):
            result["references"].append(line.strip()[2:].strip())
        elif section == "subjects" and line.strip().startswith("- subject:"):
            current = {"subject": line.split(":", 1)[1].strip()}
            result["subjects"].append(current)
        elif section == "subjects" and current is not None and ":" in line:
            key, value = line.strip().split(":", 1)
            current[key.strip()] = value.strip()
    return result


def resolve(schema, named: dict, defined: set):
    """Remplace la première référence à un type nommé par sa définition complète."""
    if isinstance(schema, str):
        if schema in named and schema not in defined:
            defined.add(schema)
            return resolve(named[schema], named, defined)
        return schema
    if isinstance(schema, list):
        return [resolve(s, named, defined) for s in schema]
    if isinstance(schema, dict):
        out = {}
        for key, value in schema.items():
            if key in ("type", "items", "values"):
                out[key] = resolve(value, named, defined)
            elif key == "fields":
                out[key] = [{**f, "type": resolve(f["type"], named, defined)} for f in value]
            else:
                out[key] = value
        if out.get("name") and out.get("type") in ("record", "enum", "fixed"):
            ns = out.get("namespace", "")
            defined.add(f"{ns}.{out['name']}" if ns else out["name"])
        return out
    return schema


def named_types(references: list[str]) -> dict:
    named = {}
    for ref in references:
        s = json.loads((SCHEMA_DIR / ref).read_text(encoding="utf-8"))
        named[f"{s['namespace']}.{s['name']}"] = s
    return named


def call(url: str, method: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read() or b"{}")
    except urllib.error.HTTPError as err:
        payload = err.read()
        try:
            return err.code, json.loads(payload)
        except json.JSONDecodeError:
            return err.code, {"message": payload.decode(errors="replace")}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8081")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resolve-only", action="store_true")
    args = parser.parse_args()

    cfg = load_subjects()
    named = named_types(cfg["references"])
    failures = 0
    for entry in cfg["subjects"]:
        subject = entry["subject"]
        raw = json.loads((SCHEMA_DIR / entry["schema"]).read_text(encoding="utf-8"))
        schema = json.dumps(resolve(raw, named, set()), separators=(",", ":"))
        if args.resolve_only:
            print(f"--- {subject}\n{schema}")
            continue
        level = entry.get("compatibility", cfg["defaultCompatibility"])
        base = args.url.rstrip("/")

        status, _ = call(f"{base}/subjects/{subject}/versions/latest", "GET")
        if status == 200:
            status, body = call(f"{base}/compatibility/subjects/{subject}/versions/latest?verbose=true",
                                "POST", {"schema": schema})
            if status != 200 or not body.get("is_compatible", False):
                print(f"[KO] {subject} : incompatible ({level}) -> {body.get('messages', body)}")
                failures += 1
                continue
        if args.dry_run:
            print(f"[OK] {subject} : compatible")
            continue
        call(f"{base}/config/{subject}", "PUT", {"compatibility": level})
        status, body = call(f"{base}/subjects/{subject}/versions", "POST", {"schema": schema})
        if status != 200:
            print(f"[KO] {subject} : {status} {body}")
            failures += 1
        else:
            print(f"[OK] {subject} -> id {body.get('id')} ({level})")
    if failures:
        sys.exit(f"{failures} schéma(s) en échec")


if __name__ == "__main__":
    main()
