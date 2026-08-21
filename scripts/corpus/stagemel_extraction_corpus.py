#!/usr/bin/env python3
r"""
stagemel_extraction_corpus.py — Extraction REF + DAO par corpus.

Version scriptée de la méthodologie du stage de Mélanie (2026), cf.
stage/Méthodologie/Méthodologie.docx et stage/Méthodologie/extraction_corpus.py
(dont ce script reprend la logique, en pointant vers les sources auto-détectées
les plus récentes plutôt que des chemins figés — cf. extraction_vah_pub.py).

Pour chaque corpus_code, extrait :
  - les lignes du DERNIER results/ref/_ref_files_AAAAMMJJ.csv.gz (auto-détecté,
    comme dans extraction_vah_pub.py) dont corpus_code == code
    -> results/corpus/stagemel/<code>_files_<today>.csv.gz
  - les lignes de results/ead/ead_cor/dao_ref_link_brut.csv (généré depuis
    data/ead/bnr par scripts/ead/dao_ref_link.py — relancer ce script après
    tout ajout/modif de notice EAD, avant de relancer celui-ci) dont
    nom_fichier_base contient le code -> results/corpus/stagemel/<code>_dao_<today>.csv.gz

Ces deux fichiers sont l'entrée attendue par stagemel_cas_merge.py. Sans argument,
le script prend systématiquement le ref le plus récent et l'état actuel des DAO :
relancer tel quel après une nouvelle version de REF ou de nouvelles notices EAD.

Usage
-----
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py MED_AFF AMR_LEB
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_extraction_corpus.py --ref-path results/ref/_ref_files_20260630.csv.gz
"""
import argparse
import re
from datetime import datetime
from glob import glob
from os.path import basename, join
from pathlib import Path

import pandas as pd

# Liste des 37 corpus suivis par la stagiaire (stage/Méthodologie/extraction_corpus.py).
CORPUS_CODES = [
    "MED_AFF", "MED_AVI", "MED_CHA", "MED_CP", "MED_DIL", "MED_EPH", "MED_FAN",
    "MED_FLR", "MED_FOO", "MED_IMA", "MED_JOU", "MED_LET", "MED_MAR", "MED_MON",
    "MED_MS", "MED_NPT", "MED_PAR", "MED_PBI", "MED_PER", "MED_PHD", "MED_PHO",
    "MED_PLA", "MED_PUB", "MED_VAH", "MED_VAI", "MED_VDM", "MED_VID", "LAI",
    "AMR_AFF", "AMR_CAD", "AMR_LEB", "AMR_OBJ", "ARA_CPS", "CSV_PAL", "LAR_PUB",
    "MDF_MTX", "MUS_ARC", "OBS_JOU",
]

OUT_DIR = Path("results/corpus/stagemel")
DAO_PATH = join("results", "ead", "ead_cor", "dao_ref_link_brut.csv")


def dernier_ref():
    """Chemin du référentiel de fichiers le plus récent (_ref_files_AAAAMMJJ), comme extraction_vah_pub.py."""
    refs = [
        p
        for p in glob(join("results", "ref", "_ref_files_*.csv.gz"))
        if re.fullmatch(r"_ref_files_\d{8}\.csv\.gz", basename(p))
    ]
    return max(refs)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("corpus_codes", nargs="*", help="Codes corpus à traiter (défaut : les 37 du stage)")
    parser.add_argument("--ref-path", default=None, help="Chemin d'un _ref_files_ précis (défaut : le plus récent, auto-détecté)")
    args = parser.parse_args()

    codes = args.corpus_codes or CORPUS_CODES
    today = datetime.now().strftime("%Y%m%d")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    ref_path = args.ref_path or dernier_ref()
    print(f"référentiel : {ref_path}")
    print(f"dao         : {DAO_PATH}")
    ref = pd.read_csv(ref_path, low_memory=False)
    dao = pd.read_csv(DAO_PATH, low_memory=False)
    dao = dao[~dao["nom_fichier_base"].isna()]

    for corpus_code in codes:
        print(f"Traitement du corpus {corpus_code}")
        v_files = ref[ref["corpus_code"] == corpus_code]
        v_files.to_csv(OUT_DIR / f"{corpus_code}_files_{today}.csv.gz", index=False)
        print(f"-- {len(v_files)} fichiers")

        v_dao = dao[dao["nom_fichier_base"].str.contains(corpus_code, regex=False)]
        v_dao.to_csv(OUT_DIR / f"{corpus_code}_dao_{today}.csv.gz", index=False)
        print(f"-- {len(v_dao)} dao")
        if v_dao.empty:
            print(f"-- attention : aucune DAO pour {corpus_code} (corpus sans notice EAD ?)")


if __name__ == "__main__":
    main()
