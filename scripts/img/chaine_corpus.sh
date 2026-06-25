#!/usr/bin/env bash
#
# chaine_corpus.sh — Chaîne complète : TÉLÉCHARGEMENT puis CONVERSION.
#
# 1) Télécharge TOUS les corpus listés dans CSV_DIR (manifestes .csv/.csv.gz)
#    via telechargement_corpus.py, vers <DEST>/<stem>/conservation/...
# 2) SEULEMENT une fois tous les téléchargements terminés, lance la conversion
#    via run_tif_convert.sh sur ces mêmes dossiers (<DEST>/<stem>), qui produit
#    <DEST>/<stem>/diffusion/...
#
# Les dossiers de conversion sont DÉDUITS des manifestes (stem du CSV ->
# <DEST>/<stem>) : téléchargement et conversion traitent donc exactement les
# mêmes corpus, sans liste à maintenir en double.
#
# Usage :
#   ./chaine_corpus.sh
#
# Prérequis : env conda « rbx-bnr-data » (env unifié du projet, support JP2).

set -euo pipefail

# --------------------------------------------------------------------------- #
# Configuration — à adapter
# --------------------------------------------------------------------------- #

# Env conda du projet (unifié, inclut libvips/pyvips avec support JP2).
CONDA_ENV="rbx-bnr-data"

# Répertoire des manifestes à traiter (défaut du téléchargeur : results/corpus/tif2dl).
CSV_DIR="results/corpus/tif2dl"

# Racine du stockage source (où path+name se résolvent). À ADAPTER au montage local.
SOURCE="\\srvbnr.ntrbx.local\BNR"

# Racine de destination sur le disque dur. C'est aussi la racine lue par la
# conversion : <DEST>/<stem>/conservation -> <DEST>/<stem>/diffusion.
DEST="E:\corpus"

# --------------------------------------------------------------------------- #
# Exécution
# --------------------------------------------------------------------------- #

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DL_SCRIPT="$SCRIPT_DIR/telechargement_corpus.py"
CONV_SCRIPT="$SCRIPT_DIR/run_tif_convert.sh"

for f in "$DL_SCRIPT" "$CONV_SCRIPT"; do
  [[ -f "$f" ]] || { echo "Script introuvable : $f" >&2; exit 1; }
done
[[ -d "$CSV_DIR" ]] || { echo "Répertoire de manifestes introuvable : $CSV_DIR" >&2; exit 1; }

# --- Étape 1 : télécharger TOUS les corpus avant toute conversion ----------- #
echo "=== [1/2] Téléchargement de tous les corpus de $CSV_DIR vers $DEST ==="
conda run -n "$CONDA_ENV" python "$DL_SCRIPT" \
  --csv-dir "$CSV_DIR" --source "$SOURCE" --dest "$DEST"

# --- Dossiers à convertir : déduits des manifestes (stem -> <DEST>/<stem>) --- #
INPUT_DIRS=()
shopt -s nullglob
for manifest in "$CSV_DIR"/*.csv "$CSV_DIR"/*.csv.gz; do
  stem="$(basename "$manifest")"
  stem="${stem%.gz}"
  stem="${stem%.csv}"
  INPUT_DIRS+=("$DEST/$stem")
done
shopt -u nullglob

# Dédoublonnage (un même corpus présent en .csv et .csv.gz, par sécurité).
mapfile -t INPUT_DIRS < <(printf '%s\n' "${INPUT_DIRS[@]}" | awk '!seen[$0]++')

if [[ ${#INPUT_DIRS[@]} -eq 0 ]]; then
  echo "Aucun manifeste (.csv/.csv.gz) dans $CSV_DIR : rien à convertir." >&2
  exit 1
fi

# --- Étape 2 : conversion des corpus téléchargés ---------------------------- #
echo
echo "=== [2/2] Conversion de ${#INPUT_DIRS[@]} corpus téléchargé(s) ==="
"$CONV_SCRIPT" "${INPUT_DIRS[@]}"

echo
echo "=== Chaîne terminée. ==="
