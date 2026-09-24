#!/usr/bin/env bash
# Chaîne complète stagemel : DAO -> extraction par corpus -> cas1/cas2/cas3 -> recap Excel.
# À relancer tel quel après toute nouvelle notice EAD et/ou nouvelle version de REF :
# chaque étape auto-détecte ses sources les plus récentes, rien à éditer.
# À lancer depuis la racine du dépôt : bash scripts/corpus/stagemel_pipeline.sh
set -euo pipefail

# Interpréteur surchargeable si l'env rbx-bnr-data n'existe pas sur le poste :
#   PY=/chemin/vers/python bash scripts/corpus/stagemel_pipeline.sh
PY=${PY:-conda run -n rbx-bnr-data python}

# Une seule date pour toutes les étapes (un lancement qui passe minuit ne doit pas
# décaler les noms de fichiers d'une étape à l'autre), et une seule liste de corpus
# (CORPUS_CODES de l'extraction) : sans argument, cas_merge prendrait tous les
# *_files_<date> présents, y compris ceux d'un corpus retiré de la liste.
DATE=$(date +%Y%m%d)
CORPUS=$($PY -c "import sys; sys.path.insert(0, 'scripts/corpus'); from stagemel_extraction_corpus import CORPUS_CODES; print(' '.join(CORPUS_CODES))" | tr -d '\r')

# A. DAO à jour depuis les notices EAD (data/ead/bnr) -> results/ead/ead_cor/dao_ref_link_brut.csv
$PY scripts/ead/dao_ref_link.py

# B. extraction REF + DAO par corpus -> results/corpus/stagemel/
$PY scripts/corpus/stagemel_extraction_corpus.py $CORPUS --date "$DATE"

# C. merge cas1/cas2/cas3 par corpus -> results/corpus/stagemel/cas/
$PY scripts/corpus/stagemel_cas_merge.py $CORPUS --date "$DATE"

# D. recap Excel par corpus -> results/corpus/stagemel/recap/
for corpus in $CORPUS; do
    $PY scripts/corpus/stagemel_recap.py "$corpus" --date "$DATE"
done
