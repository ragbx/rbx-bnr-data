#!/usr/bin/env bash
# Lot complet des fonds de la médiathèque (MED_MS, puis MED_VDM, puis MED_PAR) :
# med_mets.py sur chaque manifeste, avec validation XSD et ENVOI RÉEL sur S3.
# À lancer depuis la racine du dépôt, env vips-convert actif, dans tmux ou screen
# (environ 12 h avec 4 processus) :
#   bash scripts/img/img3/med_lot.sh
#
# Réglages (variables d'environnement) :
#   SOURCE=/home/kibini/bnr  préfixe des chemins du ref (TIFF lus sous <SOURCE>/<path>/<name>)
#   OUT=~                    sortie de chaque fonds sous <OUT>/med_ms, med_vdm, med_par
#   WORKERS=4                processus de conversion (690 Mo de pic chacun à l'essai)
#   UPLOAD=1                 0 : conversion et METS seulement, sans envoi sur S3
#   PY=python                interpréteur
#   FONDS="MED_MS MED_VDM MED_PAR"  fonds à traiter, dans cet ordre
#
# Un fonds en échec n'arrête pas les suivants : med_mets.py sort avec le code 2
# dès qu'une notice n'est pas ok (9 notices MED_VDM sont refusées d'emblée), et
# une relance reprend un fonds là où il en était (les notices dont le METS est
# déjà sur S3 sont sautées). Le journal de chaque fonds est <OUT>/<fonds>/med_lot_<date>.log ;
# les récapitulatifs med_mets_*.csv et med_jpeg_*.csv sont à conserver.
set -uo pipefail

SOURCE=${SOURCE:-/home/kibini/bnr}
OUT=${OUT:-$HOME}
WORKERS=${WORKERS:-4}
UPLOAD=${UPLOAD:-1}
PY=${PY:-python}
FONDS=${FONDS:-MED_MS MED_VDM MED_PAR}
# sortie de Python ligne a ligne : sinon, derriere tee, le journal n'avance que par blocs
export PYTHONUNBUFFERED=1

# vues à convertir par fonds (manifestes du ref 20261023, notices refusées exclues)
declare -A PAGES=([MED_MS]=32410 [MED_VDM]=30952 [MED_PAR]=16299)

envoi=()
[ "$UPLOAD" = 1 ] && envoi=(--upload)
stamp=$(date +%Y%m%d%H%M%S)
bilan=()
pire=0

for fonds in $FONDS; do
    manifeste="results/${fonds,,}/manifeste_${fonds}.csv.gz"
    sortie="$OUT/${fonds,,}"
    if [ ! -f "$manifeste" ]; then
        echo "$fonds : manifeste introuvable ($manifeste), fonds saute" >&2
        bilan+=("$fonds : manifeste introuvable")
        pire=1
        continue
    fi
    mkdir -p "$sortie"
    journal="$sortie/med_lot_${stamp}.log"
    echo "[$(date '+%F %T')] $fonds : debut ($manifeste -> $sortie, $WORKERS processus${envoi:+, ENVOI REEL})" | tee -a "$journal"
    $PY scripts/img/img3/med_mets.py "$manifeste" --source "$SOURCE" --out-dir "$sortie" \
        --workers "$WORKERS" --validate "${envoi[@]}" --estimer-pages "${PAGES[$fonds]:-0}" 2>&1 | tee -a "$journal"
    code=${PIPESTATUS[0]}
    echo "[$(date '+%F %T')] $fonds : fin, code $code" | tee -a "$journal"
    bilan+=("$fonds : code $code (journal $journal)")
    [ "$code" -gt "$pire" ] && pire=$code
done

echo
echo "Bilan du lot (0 = toutes les notices ok ; 2 = notices en echec ou refusees, voir les recapitulatifs) :"
printf '  %s\n' "${bilan[@]}"
exit "$pire"
