#!/usr/bin/env bash
# Lance la chaîne d'appariement des DAO (hors ead_bnr2mnesys.py).
# Suppose les IR déjà transformés dans results/ead/ead_cor/bnr2mnesys/*.xml.
# Voir documentation/files/scripts/dao_appariement.md pour le détail du flux.
# À lancer depuis la racine du dépôt : bash scripts/ead/dao_appariement.sh
set -euo pipefail

# A. liens isolés : orphelins puis appariement au référentiel
conda run -n rbx-bnr-data python scripts/ead/dao_sans_conservation.py
conda run -n rbx-bnr-data python scripts/ead/dao_appariement_conservation.py

# B. plages first/last : développement, vérification, puis analyses
conda run -n rbx-bnr-data python scripts/ead/dao_first_last_developpe.py
conda run -n rbx-bnr-data python scripts/ead/dao_first_last_verif_ref.py
conda run -n rbx-bnr-data python scripts/ead/dao_first_last_plages_lacunaires.py
conda run -n rbx-bnr-data python scripts/ead/dao_first_last_access_sans_conservation.py
