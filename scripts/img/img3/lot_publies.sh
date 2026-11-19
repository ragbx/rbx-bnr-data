#!/usr/bin/env bash
# Corpus publies, TIFF sans JPEG (manifestes_publies.py) : publies_mets.py, avec validation XSD et
# ENVOI RÉEL sur S3. À lancer depuis la racine du dépôt, env vips-convert actif, dans tmux ou screen :
#   bash scripts/img/img3/lot_publies.sh                     # lot : 15 873 TIFF, 7 657 METS, 463 Go
#   MANIFESTE=results/img/manifeste_corpus_publies_partielles.csv.gz OVERWRITE=1 bash scripts/img/img3/lot_publies.sh
#                                                            # notices partielles : 9 868 TIFF, 115 METS,
#                                                            # leurs JPEG et METS deja sur S3 sont REMPLACES
#
# Avant de lancer, les TIFF doivent être sous <SOURCE>/<path>/<name> : la liste est
# <manifeste>_tiff.txt ; les télécharger depuis S3 avec
#   python scripts/img/img3/tiff_depuis_s3.py <manifeste> --source <SOURCE>
# (une partie n'a pas de chemin sur le partage). Le script la contrôle et s'arrête si des fichiers manquent (SKIP_CHECK=1
# pour passer outre : les METS concernés seront alors en erreur, le lot continue). Il contrôle aussi
# les groupes (publies_mets.py --verifier) et s'arrête si l'un d'eux est refusé.
#
# Réglages (variables d'environnement) :
#   SOURCE=/home/kibini/bnr  préfixe des chemins du ref
#   OUT=~                    sortie sous <OUT>/lot_publies (ou lot_publies_partielles)
#   WORKERS=4                processus de conversion (pic de 1,9 Go chacun au lot des 6 corpus)
#   UPLOAD=1                 0 : conversion et METS seulement, sans envoi sur S3
#   OVERWRITE=0              1 : --overwrite (manifeste des notices partielles uniquement)
#   QUALITY=80               qualité JPEG
#   PY=python                interpréteur
#   MANIFESTE=results/img/manifeste_corpus_publies_lot.csv.gz
#   SKIP_CHECK=0             1 : ne pas contrôler la présence des TIFF
#
# Les METS déjà sur S3 sont sautés (sauf OVERWRITE=1) : une relance reprend là où le lot en était.
# Sortie avec le code 2 si un METS n'est pas ok (voir med_mets_*.csv et med_jpeg_*.csv sous la
# sortie, à conserver).
set -uo pipefail

SOURCE=${SOURCE:-/home/kibini/bnr}
OUT=${OUT:-$HOME}
WORKERS=${WORKERS:-4}
UPLOAD=${UPLOAD:-1}
OVERWRITE=${OVERWRITE:-0}
QUALITY=${QUALITY:-80}
PY=${PY:-python}
MANIFESTE=${MANIFESTE:-results/img/manifeste_corpus_publies_lot.csv.gz}
SKIP_CHECK=${SKIP_CHECK:-0}
# sortie de Python ligne a ligne : sinon, derriere tee, le journal n'avance que par blocs
export PYTHONUNBUFFERED=1

if [ ! -f "$MANIFESTE" ]; then
    echo "manifeste introuvable : $MANIFESTE" >&2
    exit 1
fi
case "$MANIFESTE" in
    *partielles*) [ "$OVERWRITE" = 1 ] || { echo "notices partielles : relancer avec OVERWRITE=1 (sinon elles sont sautees, METS deja sur S3)" >&2; exit 1; } ;;
    *) [ "$OVERWRITE" = 1 ] && { echo "OVERWRITE=1 est reserve au manifeste des notices partielles" >&2; exit 1; } ;;
esac

liste="${MANIFESTE%.csv.gz}_tiff.txt"
PAGES=$(wc -l < "$liste")   # une vue par TIFF du manifeste
if [ "$SKIP_CHECK" != 1 ]; then
    manquants=$(while IFS= read -r f; do [ -f "$SOURCE/$f" ] || echo "$f"; done < "$liste")
    if [ -n "$manquants" ]; then
        echo "$(printf '%s\n' "$manquants" | wc -l) TIFF de $liste absents sous $SOURCE, par exemple :" >&2
        printf '%s\n' "$manquants" | head -5 >&2
        echo "Copier les TIFF sous $SOURCE, ou relancer avec SKIP_CHECK=1." >&2
        exit 1
    fi
fi
$PY scripts/img/img3/publies_mets.py --verifier "$MANIFESTE" || { echo "groupes refuses : voir ci-dessus" >&2; exit 1; }

options=()
[ "$UPLOAD" = 1 ] && options+=(--upload)
[ "$OVERWRITE" = 1 ] && options+=(--overwrite)
stamp=$(date +%Y%m%d%H%M%S)
nom=$(basename "$MANIFESTE" .csv.gz); nom=lot_publies${nom#manifeste_corpus_publies}; nom=${nom%_lot}
sortie="$OUT/$nom"
mkdir -p "$sortie"
journal="$sortie/${nom}_${stamp}.log"

echo "[$(date '+%F %T')] debut ($MANIFESTE -> $sortie, $WORKERS processus, Q=$QUALITY ${options[*]:-sans envoi})" | tee -a "$journal"
$PY scripts/img/img3/publies_mets.py "$MANIFESTE" --source "$SOURCE" --out-dir "$sortie" \
    --workers "$WORKERS" --quality "$QUALITY" --validate "${options[@]}" --estimer-pages "$PAGES" 2>&1 | tee -a "$journal"
code=${PIPESTATUS[0]}
echo "[$(date '+%F %T')] fin, code $code (journal $journal)" | tee -a "$journal"
echo "Code 0 = tous les METS ok ; 2 = METS en echec ou refuses, voir les recapitulatifs sous $sortie."
exit "$code"
