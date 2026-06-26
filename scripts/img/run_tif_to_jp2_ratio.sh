#!/usr/bin/env bash
#
# run_tif_to_jp2_ratio.sh — Lanceur de tests pour tif_to_jp2_ratio.py.
#
# Variante de run_tif_convert.sh : au lieu de balayer des qualités Q fixes, on
# balaie des RATIOS de compression cibles (Q est calibré par image pour chaque ratio).
#
# Pour chaque dossier de corpus, convertit RÉCURSIVEMENT les TIF rangés sous
#   <corpus>/conservation/...
# vers
#   <corpus>/diffusion/...        (arborescence reproduite à l'identique)
# Le TYPE de document — donc le FACTEUR de réduction (cf. FACTEUR_PAR_TYPE) — est
# DÉDUIT DU NOM du dossier de corpus (ex. corpus_presse_..._1 -> presse), et on
# balaie plusieurs ratios cibles et plusieurs seuils de résolution minimale
# (--resolution-min).
#
# Ratio, facteur et seuil étant inscrits dans le nom de sortie
# (ex. page001_r15_f50_rmin2000.jp2), toutes les combinaisons cohabitent dans le
# même dossier sans s'écraser, et une relance saute ce qui est déjà produit.
#
# Usage :
#   ./run_tif_to_jp2_ratio.sh                       # traite la liste DEFAULT_INPUT_DIRS
#   ./run_tif_to_jp2_ratio.sh DIR1 DIR2 ...         # traite les dossiers passés en argument
#
# Les dossiers passés en argument (utilisés par chaine_corpus.sh) priment sur la
# liste DEFAULT_INPUT_DIRS codée plus bas.
#
# Prérequis : env conda « rbx-bnr-data » (env unifié du projet, support JP2).

set -euo pipefail

# --------------------------------------------------------------------------- #
# Configuration — à adapter
# --------------------------------------------------------------------------- #

# Dossiers d'entrée par défaut (chacun contient un sous-dossier conservation/).
# Utilisés seulement si aucun dossier n'est passé en argument (cf. plus bas).
DEFAULT_INPUT_DIRS=(
    #"/media/fpichenot/ragbx512/corpus/corpus_iconographie_20260502_1"
    #"/media/fpichenot/ragbx512/corpus/corpus_manuscrits_plans_20260502_1"
    #"/media/fpichenot/ragbx512/corpus/corpus_presse_20260502_1"
    "/media/fpichenot/ragbx512/corpus/corpus_iconographie_20260502_2"
    "/media/fpichenot/ragbx512/corpus/corpus_manuscrits_plans_20260502_2"
    "/media/fpichenot/ragbx512/corpus/corpus_presse_20260502_2"
    "/media/fpichenot/ragbx512/corpus/corpus_iconographie_20260502_3"
    "/media/fpichenot/ragbx512/corpus/corpus_manuscrits_plans_20260502_3"
    "/media/fpichenot/ragbx512/corpus/corpus_presse_20260502_3"
    "/media/fpichenot/ragbx512/corpus/corpus_iconographie_20260502_4"
    "/media/fpichenot/ragbx512/corpus/corpus_manuscrits_plans_20260502_4"
    "/media/fpichenot/ragbx512/corpus/corpus_presse_20260502_4"
)

# Sous-dossier source (où sont les TIF) et sous-dossier de sortie (miroir).
SOURCE_SUBDIR="conservation"
OUT_SUBDIR="jp2"

# Facteur de réduction par type de document. Le type est déduit du nom du dossier
# de corpus, qui doit contenir l'une de ces clés (ex. « corpus_presse_… » -> presse).
# Reprend les niveaux du script : manuscrits/plans=haut, iconographie=moyen, presse=bas.
declare -A FACTEUR_PAR_TYPE=(
  ["manuscrits_plans"]=1 #0.80
  ["iconographie"]=1 #0.65
  ["presse"]=1 #0.50
)

# Ratios de compression cibles à tester (Q calibré par image pour chacun).
RATIOS=(15)

# Tolérance relative sur le ratio (arrêt anticipé de la dichotomie). Vide = défaut du script.
TOL=""

# Seuils de résolution minimale (largeur en px) à tester.
RESMINS=(2000) # 2500 3000)

# Nombre de processus parallèles (vide = nb de cœurs).
WORKERS="8"

# Env conda du projet (unifié, inclut libvips/pyvips avec support JP2).
CONDA_ENV="rbx-bnr-data"

# Chemin du script Python (à côté de ce lanceur par défaut).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY_SCRIPT="$SCRIPT_DIR/tif_to_jp2_ratio.py"

# --------------------------------------------------------------------------- #
# Exécution
# --------------------------------------------------------------------------- #

if [[ ! -f "$PY_SCRIPT" ]]; then
  echo "Script introuvable : $PY_SCRIPT" >&2
  exit 1
fi

# Dossiers à traiter : arguments positionnels s'il y en a, sinon la liste par défaut.
if [[ $# -gt 0 ]]; then
  INPUT_DIRS=("$@")
else
  INPUT_DIRS=("${DEFAULT_INPUT_DIRS[@]}")
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
# possèdent un sous-dossier conservation/) × ratio × résolution-min.
total=0
for in_dir in "${INPUT_DIRS[@]}"; do
  [[ -d "$in_dir/$SOURCE_SUBDIR" ]] || continue
  type_de "$(basename "$in_dir")" >/dev/null || continue
  total=$(( total + ${#RATIOS[@]} * ${#RESMINS[@]} ))
done
n=0
echo "=== $total essai(s) à lancer (JP2, ratio cible) ==="

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

  for ratio in "${RATIOS[@]}"; do
    for rmin in "${RESMINS[@]}"; do
      n=$(( n + 1 ))
      echo
      echo "--- [$n/$total] $base [$doctype] : ratio=1:$ratio, f=$facteur, resolution-min=$rmin"
      echo "    $src  ->  $out"

      # CSV récapitulatif distinct par essai (sinon ils s'écraseraient dans le
      # dossier de sortie commun aux essais). Nom horodaté à la milliseconde.
      stamp="$(date +%Y%m%d_%H%M%S_%3N)"
      cmd=(conda run -n "$CONDA_ENV" python "$PY_SCRIPT"
           "$src" "$out"
           --ratio "$ratio"
           --facteur "$facteur"
           --resolution-min "$rmin"
           --csv "$out/recap_r${ratio}_rmin${rmin}_${stamp}.csv")
      if [[ -n "$TOL" ]]; then
        cmd+=(--tol "$TOL")
      fi
      if [[ -n "$WORKERS" ]]; then
        cmd+=(--workers "$WORKERS")
      fi

      # On n'interrompt pas le balayage si un essai échoue.
      if ! "${cmd[@]}"; then
        echo "!! Essai en erreur ($doctype, ratio=1:$ratio, resolution-min=$rmin) — on continue." >&2
      fi
    done
  done
done

echo
echo "=== Terminé. Un CSV horodaté par essai à la racine de chaque dossier de sortie. ==="
