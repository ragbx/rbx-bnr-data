"""Extraction du corpus VAH_PUB depuis les sources consolidées.

Produit deux fichiers datés dans results/corpus/ :
  - vah_pub_files_{date}.csv.gz : fichiers du corpus, extraits du DERNIER
    fichier de référence (filtre corpus_code == VAH_PUB) ;
  - vah_pub_dao_{date}.csv.gz : liens dao du corpus, extraits de
    results/ead/ead_cor/dao_ref_link_brut.csv (filtre VAH_PUB dans href_base).

Remplace l'ancienne source results/dao/liste_dao_flat_{date}.csv.gz, produite
par dao_liste_flat.py (supprimé le 13/06/2026) : dao_ref_link_brut.csv porte la
même information (finding_aid, unitid, href développés plage par plage), avec
en plus source, position et taille_plage.

À lancer depuis la racine du dépôt.
"""

import re
from datetime import datetime
from glob import glob
from os.path import basename, join

import pandas as pd

today = datetime.now().strftime("%Y%m%d")


def dernier_ref():
    """Chemin du référentiel de fichiers le plus récent (_ref_files_AAAAMMJJ)."""
    refs = [
        p
        for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


ref_path = dernier_ref()
print(f"référentiel : {ref_path}")
ref = pd.read_csv(ref_path, low_memory=False)

v_files = ref[ref["corpus_code"] == "VAH_PUB"]
sortie_files = join("results", "corpus", f"vah_pub_files_{today}.csv.gz")
v_files.to_csv(sortie_files, index=False)
print(f"{len(v_files)} fichiers VAH_PUB -> {sortie_files}")

dao = pd.read_csv(join("results", "ead", "ead_cor", "dao_ref_link_brut.csv"),
                  low_memory=False)
v_dao = dao[dao["href_base"].fillna("").str.contains("VAH_PUB")]
sortie_dao = join("results", "corpus", f"vah_pub_dao_{today}.csv.gz")
v_dao.to_csv(sortie_dao, index=False)
print(f"{len(v_dao)} liens dao VAH_PUB -> {sortie_dao}")
