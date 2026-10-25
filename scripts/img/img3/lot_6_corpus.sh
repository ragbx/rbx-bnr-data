#!/usr/bin/env bash
# Lot de six corpus convertibles sans probleme connu (MED_IMA, MED_CP, MED_AFF, MUS_VAI,
# MED_MON, MED_PUB ; 18 174 TIFF, 529 Go) : lot_mets.py sur results/img/manifeste_6_corpus.csv.gz,
# avec validation XSD et ENVOI RÉEL sur S3. À lancer depuis la racine du dépôt, env vips-convert
# actif, dans tmux ou screen (3 h 05 avec 4 processus au lot du 25/10/2026 : 172 Go de TIFF/h,
# 7 891 notices ok, aucun échec) :
#   bash scripts/img/img3/lot_6_corpus.sh
#
# Avant de lancer, les TIFF doivent être sous <SOURCE>/<path>/<name> : la liste est
# results/img/manifeste_6_corpus_tiff.txt (BNR_SAUV/MED, BNR_VERIF/MED, BNR_SAUV/MUS).
# Le script la contrôle et s'arrête si des fichiers manquent (SKIP_CHECK=1 pour passer outre :
# les notices concernées seront alors en erreur, le lot continue).
#
# Réglages (variables d'environnement) :
#   SOURCE=/home/kibini/bnr  préfixe des chemins du ref
#   OUT=~                    sortie sous <OUT>/lot_6_corpus
#   WORKERS=4                processus de conversion (pic de 1,9 Go chacun, soit 7,6 Go pour 4 : les
#                            affiches MED_AFF vont jusqu'à 324 Mpx et 1,1 Go par TIFF)
#   UPLOAD=1                 0 : conversion et METS seulement, sans envoi sur S3
#   QUALITY=80               qualité JPEG
#   PY=python                interpréteur
#   MANIFESTE=results/img/manifeste_6_corpus.csv.gz
#   SKIP_CHECK=0             1 : ne pas contrôler la présence des TIFF
#
# Les notices dont le METS est déjà sur S3 sont sautées : une relance reprend là où le lot en
# était. Sortie avec le code 2 si une notice n'est pas ok (voir med_mets_*.csv et
# med_jpeg_*.csv sous <OUT>/lot_6_corpus, à conserver ; scripts/img/img3/med_relance.py lit le journal).
set -uo pipefail

SOURCE=${SOURCE:-/home/kibini/bnr}
OUT=${OUT:-$HOME}
WORKERS=${WORKERS:-4}
UPLOAD=${UPLOAD:-1}
QUALITY=${QUALITY:-80}
PY=${PY:-python}
MANIFESTE=${MANIFESTE:-results/img/manifeste_6_corpus.csv.gz}
SKIP_CHECK=${SKIP_CHECK:-0}
PAGES=18174   # une vue par TIFF du manifeste
# sortie de Python ligne a ligne : sinon, derriere tee, le journal n'avance que par blocs
export PYTHONUNBUFFERED=1

if [ ! -f "$MANIFESTE" ]; then
    echo "manifeste introuvable : $MANIFESTE" >&2
    exit 1
fi

liste="${MANIFESTE%.csv.gz}_tiff.txt"
if [ "$SKIP_CHECK" != 1 ] && [ -f "$liste" ]; then
    manquants=$(while IFS= read -r f; do [ -f "$SOURCE/$f" ] || echo "$f"; done < "$liste")
    if [ -n "$manquants" ]; then
        echo "$(printf '%s\n' "$manquants" | wc -l) TIFF de $liste absents sous $SOURCE, par exemple :" >&2
        printf '%s\n' "$manquants" | head -5 >&2
        echo "Copier les TIFF sous $SOURCE, ou relancer avec SKIP_CHECK=1." >&2
        exit 1
    fi
fi

envoi=()
[ "$UPLOAD" = 1 ] && envoi=(--upload)
stamp=$(date +%Y%m%d%H%M%S)
sortie="$OUT/lot_6_corpus"
mkdir -p "$sortie"
journal="$sortie/lot_6_corpus_${stamp}.log"

echo "[$(date '+%F %T')] debut ($MANIFESTE -> $sortie, $WORKERS processus, Q=$QUALITY${envoi:+, ENVOI REEL})" | tee -a "$journal"
$PY scripts/img/img3/lot_mets.py "$MANIFESTE" --source "$SOURCE" --out-dir "$sortie" \
    --workers "$WORKERS" --quality "$QUALITY" --validate "${envoi[@]}" --estimer-pages "$PAGES" 2>&1 | tee -a "$journal"
code=${PIPESTATUS[0]}
echo "[$(date '+%F %T')] fin, code $code (journal $journal)" | tee -a "$journal"
echo "Code 0 = toutes les notices ok ; 2 = notices en echec ou refusees, voir les recapitulatifs sous $sortie."
exit "$code"
