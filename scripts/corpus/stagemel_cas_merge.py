#!/usr/bin/env python3
r"""
stagemel_cas_merge.py — Répartition REF/DAO en cas1/cas2/cas3, par corpus.

Version scriptée de l'étape "merge" refaite à la main dans chaque notebook du
stage de Mélanie (2026), cf. stage/Méthodologie/Méthodologie.docx et
stage/Notebook/<corpus>/*.ipynb. Prend en entrée les fichiers produits par
stagemel_extraction_corpus.py.

Méthode (reprise de la méthodologie du stage, corrigée sur deux points) :
  1. clé 'key' = nom de fichier sans extension, côté REF (colonne name) et côté
     DAO (colonne nom_fichier_base) ;
  2. DAO dédoublonnées sur 'key' : un même fichier référencé par plusieurs
     notices ne doit donner qu'une ligne (sinon les lignes REF sont dupliquées
     par le merge et gonflent les comptes du recap) ;
  3. merge outer des deux tables sur 'key' ;
  4. cas1 = uuid ET nom_fichier_base présents (fichier dans REF et DAO) ;
     cas2 = uuid manquant (fichier seulement dans DAO -> probablement
            manquant/non numérisé, ou erreur de nommage à rapprocher d'un cas1/cas3) ;
     cas3 = uuid présent, nom_fichier_base manquant (fichier dans REF, référencé
            par aucune notice EAD).
  La présence côté DAO se teste sur nom_fichier_base et non sur 'key' : 'key' est
  la colonne de jointure, donc jamais vide après le merge — les notebooks du stage
  testaient 'key', ce qui classait tous les fichiers REF sans DAO en cas1 et
  laissait cas3 toujours vide.

Usage
-----
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MED_AFF
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py MED_AFF AMR_LEB --date 20260821
  conda run -n rbx-bnr-data python scripts/corpus/stagemel_cas_merge.py --date 20260821
      (sans code : traite tous les corpus dont les fichiers _files_/_dao_ existent pour cette date)
"""
import argparse
from datetime import datetime
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
from stagemel_extraction_corpus import dossier_travail  # noqa: E402


def discover_corpus_codes(date):
    return sorted(
        p.name[: -len(f"_files_{date}.csv.gz")]
        for p in dossier_travail(date).glob(f"*_files_{date}.csv.gz")
    )


def merge_corpus(corpus_code, date):
    ref_path = dossier_travail(date) / f"{corpus_code}_files_{date}.csv.gz"
    dao_path = dossier_travail(date) / f"{corpus_code}_dao_{date}.csv.gz"
    if not ref_path.exists() or not dao_path.exists():
        raise FileNotFoundError(
            f"{ref_path} ou {dao_path} introuvable — lancer d'abord stagemel_extraction_corpus.py"
        )

    ref = pd.read_csv(ref_path)
    dao = pd.read_csv(dao_path)

    ref["key"] = ref["name"].apply(lambda x: Path(x).stem)
    dao["key"] = dao["nom_fichier_base"].apply(lambda x: Path(x).stem)
    dao = dao.drop_duplicates(subset="key")

    merged = pd.merge(ref, dao, on="key", how="outer")

    dans_dao = merged["nom_fichier_base"].notna()
    cas1 = merged[merged["uuid"].notna() & dans_dao]
    cas2 = merged[merged["uuid"].isna()]
    cas3 = merged[merged["uuid"].notna() & ~dans_dao]
    return cas1, cas2, cas3


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("corpus_codes", nargs="*", help="Codes corpus à traiter (défaut : auto-détectés pour --date)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y%m%d"), help="Date des fichiers _files_/_dao_ en entrée (défaut : aujourd'hui)")
    args = parser.parse_args()

    codes = args.corpus_codes or discover_corpus_codes(args.date)
    if not codes:
        raise SystemExit(f"Aucun fichier {dossier_travail(args.date)}/*_files_{args.date}.csv.gz trouvé — lancer stagemel_extraction_corpus.py, ou préciser --date")

    out_dir = dossier_travail(args.date)

    for corpus_code in codes:
        print(f"Traitement du corpus {corpus_code}")
        cas1, cas2, cas3 = merge_corpus(corpus_code, args.date)
        print(f"-- cas1: {len(cas1)}  cas2: {len(cas2)}  cas3: {len(cas3)}")

        cas1.to_csv(out_dir / f"{corpus_code}_cas1_{args.date}.csv.gz", index=False)
        cas2.to_csv(out_dir / f"{corpus_code}_cas2_{args.date}.csv.gz", index=False)
        # toujours écrit, même vide : un cas3 d'un lancement antérieur à la même date
        # serait sinon relu par stagemel_recap.py
        cas3.to_csv(out_dir / f"{corpus_code}_cas3_{args.date}.csv.gz", index=False)


if __name__ == "__main__":
    main()
