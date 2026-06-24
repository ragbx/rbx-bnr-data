#!/usr/bin/env bash
#
# run_tif_convert.sh — Lanceur de tests pour tif_convert.py.
#
# Pour chaque dossier de corpus, convertit RÉCURSIVEMENT les TIF rangés sous
#   <corpus>/conservation/...
# vers
#   <corpus>/diffusion/...        (arborescence reproduite à l'identique)
# Le TYPE de document — donc le FACTEUR de réduction (cf. FACTEUR_PAR_TYPE) — est
# DÉDUIT DU NOM du dossier de corpus (ex. corpus_presse_..._1 -> presse), et on
# balaie plusieurs qualités (Q) et plusieurs seuils de résolution minimale
# (--resolution-min).
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
  "/media/fpichenot/ragbx512/corpus/bnr-images/corpus_presse_20260502_1"
  "/media/fpichenot/ragbx512/corpus/bnr-images/corpus_presse_20260502_2"
  "/media/fpichenot/ragbx512/corpus/bnr-images/corpus_presse_20260502_3"
)

# Sous-dossier source (où sont les TIF) et sous-dossier de sortie (miroir).
SOURCE_SUBDIR="conservation"
OUT_SUBDIR="diffusion"

# Facteur de réduction par type de document. Le type est déduit du nom du dossier
# de corpus, qui doit contenir l'une de ces clés (ex. « corpus_presse_… » -> presse).
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

# Déduit le type de document à partir du nom du dossier de corpus : renvoie la
# première clé de FACTEUR_PAR_TYPE contenue dans ce nom (ex. « corpus_presse_..._1 »
# -> « presse »). Échoue (code 1) si aucune clé ne correspond.
type_de() {
  local base="$1" t
  for t in "${!FACTEUR_PAR_TYPE[@]}"; do
    [[ "$base" == *"$t"* ]] && { printf '%s\n' "$t"; return 0; }
  done
  return 1
}

# Décompte des essais réellement lançables (corpus dont le type est reconnu et qui
# possèdent un sous-dossier conservation/) × Q × résolution-min.
total=0
for in_dir in "${INPUT_DIRS[@]}"; do
  [[ -d "$in_dir/$SOURCE_SUBDIR" ]] || continue
  type_de "$(basename "$in_dir")" >/dev/null || continue
  total=$(( total + ${#QUALITIES[@]} * ${#RESMINS[@]} ))
done
n=0
echo "=== $total essai(s) à lancer (format=$FORMAT) ==="

for in_dir in "${INPUT_DIRS[@]}"; do
  if [[ ! -d "$in_dir" ]]; then
    echo "!! Dossier d'entrée introuvable, ignoré : $in_dir" >&2
    continue
  fi

  src="$in_dir/$SOURCE_SUBDIR"
  if [[ ! -d "$src" ]]; then
    echo "!! Sous-dossier source absent, ignoré : $src" >&2
    continue
  fi

  base="$(basename "$in_dir")"
  if ! doctype="$(type_de "$base")"; then
    echo "!! Type indéterminé pour « $base » (aucune clé parmi : ${!FACTEUR_PAR_TYPE[*]}), ignoré." >&2
    continue
  fi

  out="$in_dir/$OUT_SUBDIR"
  facteur="${FACTEUR_PAR_TYPE[$doctype]}"

  for q in "${QUALITIES[@]}"; do
    for rmin in "${RESMINS[@]}"; do
      n=$(( n + 1 ))
      echo
      echo "--- [$n/$total] $base [$doctype] : Q=$q, f=$facteur, resolution-min=$rmin"
      echo "    $src  ->  $out"

      # CSV récapitulatif distinct par essai (sinon ils s'écraseraient dans le
      # dossier de sortie commun aux essais). Nom horodaté à la milliseconde.
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
        echo "!! Essai en erreur ($doctype, Q=$q, resolution-min=$rmin) — on continue." >&2
      fi
    done
  done
done

echo
echo "=== Terminé. Un CSV horodaté par essai à la racine de chaque dossier de sortie. ==="
