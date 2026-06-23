#!/usr/bin/env bash
#
# run_tif_convert.sh — Lanceur de tests pour tif_convert.py.
#
# Pour chaque dossier d'entrée, convertit les TIF rangés sous
#   <entree>/conservation/<type>/...
# vers
#   <entree>/diffusion/<type>/...
# en appliquant un FACTEUR de réduction PROPRE À CHAQUE TYPE de document
# (cf. FACTEUR_PAR_TYPE) et en balayant plusieurs qualités (Q) et plusieurs
# seuils de résolution minimale (--resolution-min).
#
# Q, facteur et seuil étant inscrits dans le nom de sortie
# (ex. page001_q85_f65_rmin2000.jpg), toutes les combinaisons cohabitent dans le
# même dossier sans s'écraser, et une relance saute ce qui est déjà produit.
#
# Usage :
#   ./run_tif_convert.sh
#
# Prérequis : env conda « rbx-bnr-data » (env unifié du projet, support JP2).

set -euo pipefail

# --------------------------------------------------------------------------- #
# Configuration — à adapter
# --------------------------------------------------------------------------- #

# Dossiers d'entrée à traiter (chacun contient un sous-dossier conservation/).
INPUT_DIRS=(
  "/media/fpichenot/ragbx512/bnr-images/test1"
  "/media/fpichenot/ragbx512/bnr-images/test2"
)

# Sous-dossier source (où sont les TIF) et sous-dossier de sortie (miroir).
SOURCE_SUBDIR="conservation"
OUT_SUBDIR="diffusion"

# Facteur de réduction par type de document (= nom du sous-dossier sous conservation/).
# Reprend les niveaux du script : manuscrits/plans=haut, iconographie=moyen, presse=bas.
declare -A FACTEUR_PAR_TYPE=(
  ["manuscrits_plans"]=0.80
  ["iconographie"]=0.65
  ["presse"]=0.50
)

# Format de sortie : jp2 | jpeg | ptiff
FORMAT="jpeg"

# Qualités (Q) à tester.
QUALITIES=(80 85 90)

# Seuils de résolution minimale (largeur en px) à tester.
RESMINS=(2000 2500 3000)

# Nombre de processus parallèles (vide = nb de cœurs).
WORKERS=""

# Env conda du projet (unifié, inclut libvips/pyvips avec support JP2).
CONDA_ENV="rbx-bnr-data"

# Chemin du script Python (à côté de ce lanceur par défaut).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="$SCRIPT_DIR/tif_convert.py"

# --------------------------------------------------------------------------- #
# Exécution
# --------------------------------------------------------------------------- #

if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "Script introuvable : $PY_SCRIPT" >&2
  exit 1
fi

# Décompte des essais réellement présents (types existants × Q × résolution-min).
total=0
for in_dir in "${INPUT_DIRS[@]}"; do
  for type in "${!FACTEUR_PAR_TYPE[@]}"; do
    [[ -d "$in_dir/$SOURCE_SUBDIR/$type" ]] || continue
    total=$(( total + ${#QUALITIES[@]} * ${#RESMINS[@]} ))
  done
done
n=0
echo "=== $total essai(s) à lancer (format=$FORMAT) ==="

for in_dir in "${INPUT_DIRS[@]}"; do
  if [[ ! -d "$in_dir" ]]; then
    echo "!! Dossier d'entrée introuvable, ignoré : $in_dir" >&2
    continue
  fi

  for type in "${!FACTEUR_PAR_TYPE[@]}"; do
    src="$in_dir/$SOURCE_SUBDIR/$type"
    if [[ ! -d "$src" ]]; then
      echo "!! Type absent, ignoré : $src" >&2
      continue
    fi
    out="$in_dir/$OUT_SUBDIR/$type"
    facteur="${FACTEUR_PAR_TYPE[$type]}"

    for q in "${QUALITIES[@]}"; do
      for rmin in "${RESMINS[@]}"; do
        n=$(( n + 1 ))
        echo
        echo "--- [$n/$total] $(basename "$in_dir")/$type : Q=$q, f=$facteur, resolution-min=$rmin"
        echo "    $src  ->  $out"

        # CSV récapitulatif distinct par essai (sinon ils s'écraseraient dans le
        # dossier de sortie commun au type). Nom horodaté à la milliseconde.
        stamp="$(date +%Y%m%d_%H%M%S_%3N)"
        cmd=(conda run -n "$CONDA_ENV" python "$PY_SCRIPT"
             "$src" "$out"
             --format "$FORMAT"
             --quality "$q"
             --facteur "$facteur"
             --resolution-min "$rmin"
             --csv "$out/recap_q${q}_rmin${rmin}_${stamp}.csv")
        if [[ -n "$WORKERS" ]]; then
          cmd+=(--workers "$WORKERS")
        fi

        # On n'interrompt pas le balayage si un essai échoue.
        if ! "${cmd[@]}"; then
          echo "!! Essai en erreur ($type, Q=$q, resolution-min=$rmin) — on continue." >&2
        fi
      done
    done
  done
done

echo
echo "=== Terminé. Un CSV horodaté par essai à la racine de chaque dossier de sortie. ==="
