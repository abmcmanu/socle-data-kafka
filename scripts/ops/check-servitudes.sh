#!/usr/bin/env bash
# PROC-08 — Contrôle des servitudes (dépendances d'infrastructure) depuis une machine du socle.
# Usage : check-servitudes.sh [fichier de cibles]   (défaut : servitudes.conf à côté du script)
# Format du fichier : TYPE NOM CIBLE   (types : dns, tcp, ntp, cert, disk, http)
set -uo pipefail
CONF="${1:-$(dirname "$0")/servitudes.conf}"
ko=0
ok()  { printf '  [OK] %-28s %s\n' "$1" "$2"; }
nok() { printf '  [KO] %-28s %s\n' "$1" "$2"; ko=1; }

while read -r type name target extra; do
  [[ -z "${type:-}" || "$type" == \#* ]] && continue
  case "$type" in
    dns)  getent hosts "$target" >/dev/null && ok "$name" "$target résolu" || nok "$name" "$target non résolu" ;;
    tcp)  timeout 5 bash -c "</dev/tcp/${target%:*}/${target#*:}" 2>/dev/null && ok "$name" "$target ouvert" || nok "$name" "$target fermé" ;;
    http) curl -fs -m 5 -o /dev/null "$target" && ok "$name" "$target répond" || nok "$name" "$target ne répond pas" ;;
    ntp)  off=$(chronyc tracking 2>/dev/null | awk '/System time/ {print $4}')
          if [[ -n "$off" ]] && awk -v o="$off" -v m="${target}" 'BEGIN {exit !(o < m)}'; then ok "$name" "dérive ${off}s"; else nok "$name" "dérive ${off:-inconnue}s (max ${target}s)"; fi ;;
    cert) if openssl x509 -checkend $(( ${extra:-21} * 86400 )) -noout -in "$target" >/dev/null 2>&1; then
            ok "$name" "valide > ${extra:-21} j ($(openssl x509 -enddate -noout -in "$target" | cut -d= -f2))"
          else nok "$name" "expire dans moins de ${extra:-21} j ou illisible"; fi ;;
    disk) pct=$(df --output=pcent "$target" 2>/dev/null | tail -1 | tr -dc '0-9')
          if [[ -n "$pct" ]] && (( pct < ${extra:-75} )); then ok "$name" "$target à ${pct}%"; else nok "$name" "$target à ${pct:-?}% (seuil ${extra:-75}%)"; fi ;;
    *)    nok "$name" "type inconnu : $type" ;;
  esac
done < "$CONF"

exit $ko
