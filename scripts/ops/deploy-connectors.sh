#!/bin/sh
# Déploie (crée ou met à jour) tous les connecteurs d'un répertoire, puis vérifie leur état.
# Usage : deploy-connectors.sh <url-connect> <répertoire>
# Compatible sh POSIX (image curlimages/curl).
set -eu
CONNECT_URL="${1:-http://localhost:8083}"
DIR="${2:-connectors/lab}"

for file in "$DIR"/*.json; do
  name=$(basename "$file" .json)
  printf '[connect] %s : ' "$name"
  code=$(curl -s -o /tmp/resp.json -w '%{http_code}' -X PUT -H 'Content-Type: application/json' \
         --data @"$file" "$CONNECT_URL/connectors/$name/config")
  case "$code" in
    200) echo "mis à jour" ;;
    201) echo "créé" ;;
    *)   echo "ERREUR HTTP $code"; cat /tmp/resp.json; echo; exit 1 ;;
  esac
done

# Laisse le temps aux tâches de démarrer, puis contrôle qu'aucune n'est en échec.
sleep 10
failed=0
for file in "$DIR"/*.json; do
  name=$(basename "$file" .json)
  status=$(curl -fsS "$CONNECT_URL/connectors/$name/status")
  echo "[connect] $name : $status"
  case "$status" in *FAILED*) failed=1 ;; esac
done
[ "$failed" -eq 0 ] || { echo "[connect] au moins un connecteur est en échec (voir runbook RB-08)"; exit 1; }
