#!/usr/bin/env bash
# Chaîne complète stagemel : DAO -> extraction par corpus -> cas1/cas2/cas3.
# À relancer tel quel après toute nouvelle notice EAD et/ou nouvelle version de REF :
# chaque étape auto-détecte ses sources les plus récentes, rien à éditer.
# À lancer depuis la racine du dépôt : bash scripts/corpus/stagemel_pipeline.sh
set -euo pipefail

# A. DAO à jour depuis les notices EAD (data/ead/bnr) -> results/ead/ead_cor/dao_ref_link_brut.csv
conda run -n rbx-bnr-data python scripts/ead/dao_ref_link.py

# B. extraction REF + DAO par corpus -> results/corpus/stagemel/
conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py

# C. merge cas1/cas2/cas3 par corpus -> results/corpus/stagemel/cas/
conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py
