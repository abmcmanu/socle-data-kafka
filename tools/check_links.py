#!/usr/bin/env python3
"""Vérifie les liens Markdown internes (fichiers et ancres) et les runbooks cités par les alertes."""
from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LINK = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
HEADING = re.compile(r"^#{1,6}\s+(.+?)\s*$", re.M)


def slug(title: str) -> str:
    """Algorithme d'ancre de GitHub : minuscules, ponctuation retirée, espaces -> tirets."""
    title = re.sub(r"`|\*\*?|_", "", title).strip().lower()
    out = []
    for ch in title:
        cat = unicodedata.category(ch)
        if ch in " -":
            out.append("-" if ch == " " else ch)
        elif cat[0] in ("L", "N") or ch == "_":
            out.append(ch)
    return "".join(out)


def anchors(path: Path) -> set[str]:
    text = re.sub(r"```.*?```", "", path.read_text(encoding="utf-8"), flags=re.S)
    return {slug(h) for h in HEADING.findall(text)}


def main() -> None:
    errors = []
    for md in ROOT.rglob("*.md"):
        if "target" in md.parts or ".git" in md.parts:
            continue
        text = re.sub(r"```.*?```", "", md.read_text(encoding="utf-8"), flags=re.S)
        for target in LINK.findall(text):
            if target.startswith(("http://", "https://", "mailto:")):
                continue
            file_part, _, anchor = target.partition("#")
            dest = (md.parent / file_part).resolve() if file_part else md
            if not dest.exists():
                errors.append(f"{md.relative_to(ROOT)} -> {target} : fichier absent")
            elif anchor and dest.suffix == ".md" and anchor not in anchors(dest):
                errors.append(f"{md.relative_to(ROOT)} -> {target} : ancre absente")
    rules = (ROOT / "monitoring/prometheus/rules/kafka-alerts.yml").read_text(encoding="utf-8")
    for ref in re.findall(r"runbook: (\S+)", rules):
        file_part, _, anchor = ref.partition("#")
        dest = ROOT / file_part
        if not dest.exists() or (anchor and anchor not in anchors(dest)):
            errors.append(f"alerte -> {ref} : runbook introuvable")
    if errors:
        sys.exit("Liens cassés :\n  " + "\n  ".join(errors))
    print("OK : liens internes et runbooks valides")


if __name__ == "__main__":
    main()
