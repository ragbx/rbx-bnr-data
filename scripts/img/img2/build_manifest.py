#!/usr/bin/env python3
r"""build_manifest.py — Construit un manifeste CSV pour download.py/convert.py a partir
du referentiel complet (results/ref/_ref_files_<date>.csv.gz).

Le referentiel contient aussi des JPG, PDF, XML (OCR), audio/video, etc. -
on ne garde que les fichiers TIFF (file_type == "tiff"). Pour chaque
corpus_code restant, tire aleatoirement au plus N fichiers parmi ceux
effectivement deposes sur S3 (s3_uploaded=True), avec une exception pour
MED_PLA (jamais depose sur S3) : on garde ses fichiers dont source2s3='az'.
Un uuid = un tif unique (le referentiel contient des doublons, dedoublonnes
avant tirage).

Colonnes de sortie : uuid, path, name, s3_key, corpus_code, taux (valeur fixe
passee en argument, identique pour toutes les lignes).

Usage :
  python build_manifest.py ../../../results/ref/_ref_files_20260630.csv.gz \
      --n-par-corpus 20 --taux 80;85;90;95 --output manifest.csv
"""

import argparse
from pathlib import Path

import pandas as pd

USECOLS = ["uuid", "path", "name", "corpus_code", "s3_key", "s3_uploaded", "source2s3", "file_type"]


def main():
    parser = argparse.ArgumentParser(
        description="Construit un manifeste (uuid, path, name, s3_key, corpus_code, taux) "
                    "a partir du referentiel complet, N fichiers tires au hasard par corpus_code."
    )
    parser.add_argument("ref_csv", type=Path, help="referentiel source (_ref_files_<date>.csv.gz)")
    parser.add_argument("--n-par-corpus", type=int, default=20,
                        help="nb de fichiers a tirer par corpus_code (defaut : 20)")
    parser.add_argument("--taux", default="80;85;90;95",
                        help="valeur de la colonne taux, identique pour toutes les lignes "
                             "(defaut : 80;85;90;95)")
    parser.add_argument("--seed", type=int, default=5,
                        help="graine du tirage aleatoire (defaut : 5, reproductible)")
    parser.add_argument("--no-filtre-s3", action="store_true",
                        help="ne pas filtrer sur s3_uploaded=True (garde tout le referentiel)")
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV de sortie (defaut : manifest_AAAAMMJJHHMMSS.csv)")
    args = parser.parse_args()

    if not args.ref_csv.is_file():
        parser.error(f"Referentiel introuvable : {args.ref_csv}")

    ref = pd.read_csv(args.ref_csv, usecols=USECOLS, low_memory=False)
    avant = len(ref)
    ref = ref.drop_duplicates(subset="uuid")
    ref = ref[ref["file_type"] == "tiff"]
    print(f"{avant} ligne(s) dans le referentiel, {len(ref)} apres dedoublonnage sur uuid et "
          f"filtre file_type=tiff.")

    if not args.no_filtre_s3:
        deposes = ref["s3_uploaded"].astype(str) == "True"
        med_pla = (ref["corpus_code"] == "MED_PLA") & (ref["source2s3"] == "az")
        ref = ref[deposes | med_pla]
        print(f"{len(ref)} ligne(s) apres filtre s3_uploaded=True (+ exception MED_PLA).")

    tirages = [
        g.sample(n=min(len(g), args.n_par_corpus), random_state=args.seed)
        for _, g in ref.groupby("corpus_code")
    ]
    echantillon = pd.concat(tirages, ignore_index=True)
    echantillon = echantillon[["uuid", "path", "name", "s3_key", "corpus_code"]].copy()
    echantillon["taux"] = args.taux
    echantillon = echantillon.sort_values(["corpus_code", "name"])

    output = args.output or Path(f"manifest_{pd.Timestamp.now():%Y%m%d%H%M%S}.csv")
    echantillon.to_csv(output, index=False)

    print(f"{echantillon['corpus_code'].nunique()} corpus_code, {len(echantillon)} fichier(s) au total.")
    print(f"Manifeste ecrit : {output}")


if __name__ == "__main__":
    main()
