#!/usr/bin/env python3
r"""
stagemel_cas_merge.py — Répartition REF/DAO en cas1/cas2/cas3, par corpus.

Version scriptée de l'étape "merge" refaite à la main dans chaque notebook du
stage de Mélanie (2026), cf. stage/Méthodologie/Méthodologie.docx et
stage/Notebook/<corpus>/*.ipynb. Prend en entrée les fichiers produits par
stagemel_extraction_corpus.py.

Méthode (identique à la méthodologie du stage, reproduite telle quelle) :
  1. clé 'key' = nom de fichier sans extension, côté REF (colonne name) et côté
     DAO (colonne nom_fichier_base) ;
  2. merge outer des deux tables sur 'key' ;
  3. cas1 = uuid ET key présents (fichier dans REF et DAO) ;
     cas2 = uuid manquant, key présent (fichier seulement dans DAO -> probablement
            manquant/non numérisé, ou erreur de nommage à rapprocher d'un cas1/cas3) ;
     cas3 = uuid présent, key manquant.
  cas3 ne peut survenir que si le calcul de 'key' échoue d'un côté du merge (ex.
  'name' absent) ; en pratique il est donc quasi toujours vide, comme dans les
  notebooks d'origine (peu de corpus ont un fichier cas3 non vide).

Usage
-----
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MED_AFF
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MED_AFF AMR_LEB --date 20260821
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py --date 20260821
      (sans code : traite tous les corpus dont les fichiers _files_/_dao_ existent pour cette date)
"""
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd

IN_DIR = Path("results/corpus/stagemel")
OUT_DIR = IN_DIR / "cas"


def discover_corpus_codes(date):
    return sorted(
        p.name[: -len(f"_files_{date}.csv.gz")]
        for p in IN_DIR.glob(f"*_files_{date}.csv.gz")
    )


def merge_corpus(corpus_code, date):
    ref_path = IN_DIR / f"{corpus_code}_files_{date}.csv.gz"
    dao_path = IN_DIR / f"{corpus_code}_dao_{date}.csv.gz"
    if not ref_path.exists() or not dao_path.exists():
        raise FileNotFoundError(
            f"{ref_path} ou {dao_path} introuvable — lancer d'abord stagemel_extraction_corpus.py"
        )

    ref = pd.read_csv(ref_path)
    dao = pd.read_csv(dao_path)

    ref["key"] = ref["name"].apply(lambda x: Path(x).stem)
    dao["key"] = dao["nom_fichier_base"].apply(lambda x: Path(x).stem)

    merged = pd.merge(ref, dao, on="key", how="outer")

    cas1 = merged[merged["uuid"].notna() & merged["key"].notna()]
    cas2 = merged[merged["uuid"].isna() & merged["key"].notna()]
    cas3 = merged[merged["uuid"].notna() & merged["key"].isna()]
    return cas1, cas2, cas3


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("corpus_codes", nargs="*", help="Codes corpus à traiter (défaut : auto-détectés pour --date)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="Date des fichiers _files_/_dao_ en entrée (défaut : aujourd'hui)")
    args = parser.parse_args()

    codes = args.corpus_codes or discover_corpus_codes(args.date)
    if not codes:
        raise SystemExit(f"Aucun fichier {IN_DIR}/*_files_{args.date}.csv.gz trouvé — lancer stagemel_extraction_corpus.py, ou préciser --date")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for corpus_code in codes:
        print(f"Traitement du corpus {corpus_code}")
        cas1, cas2, cas3 = merge_corpus(corpus_code, args.date)
        print(f"-- cas1: {len(cas1)}  cas2: {len(cas2)}  cas3: {len(cas3)}")

        cas1.to_csv(OUT_DIR / f"{corpus_code}_cas1_{args.date}.csv.gz", index=False)
        cas2.to_csv(OUT_DIR / f"{corpus_code}_cas2_{args.date}.csv.gz", index=False)
        if len(cas3):
            cas3.to_csv(OUT_DIR / f"{corpus_code}_cas3_{args.date}.csv.gz", index=False)


if __name__ == "__main__":
    main()
