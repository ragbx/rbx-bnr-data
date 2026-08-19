#!/usr/bin/env python3
r"""manifest_from_disk.py — Reconstitue un manifeste CSV a partir des TIFF deja
presents sur disque (plutot que d'un echantillon tire du referentiel).

Utile quand le disque contient un melange de telechargements (differents runs,
differentes epoques) et qu'on veut un manifeste couvrant tout ce qui est
reellement present, pour pouvoir lancer convert.py dessus.

Les metadonnees (uuid, path, s3_key, corpus_code) ne sont PAS deduites du nom
des dossiers sur disque : sur un disque NTFS (insensible a la casse), un nom de
dossier peut avoir une casse differente du vrai corpus_code (ex. dossier
"AMR_Obj" pour le corpus_code "AMR_OBJ"), ce qui rendrait la deduction
incorrecte. On recoupe a la place chaque fichier trouve avec le referentiel
complet, via le meme calcul de chemin relatif (common.relkey) que celui utilise
par download.py pour ranger les fichiers - ca donne des metadonnees fiables
pour tout fichier reellement issu du referentiel.

Usage :
  python manifest_from_disk.py /media/fpichenot/Elements/corpus \
      ../../../results/ref/_ref_files_20260630.csv.gz \
      --taux "80;85;90;95" --output manifest.csv
"""

import argparse
from pathlib import Path

import pandas as pd

from common import relkey

TIFF_EXTS = {".tif", ".tiff"}
USECOLS = ["uuid", "path", "name", "corpus_code", "s3_key", "file_type"]


def main():
    parser = argparse.ArgumentParser(
        description="Reconstitue un manifeste a partir des TIFF presents sur disque, "
                    "recoupes avec le referentiel complet."
    )
    parser.add_argument("dest", type=Path, help="racine ou chercher les TIFF deja presents")
    parser.add_argument("ref_csv", type=Path, help="referentiel source (_ref_files_<date>.csv.gz)")
    parser.add_argument("--taux", default="80;85;90;95",
                        help="valeur de la colonne taux, identique pour toutes les lignes "
                             "(defaut : 80;85;90;95)")
    parser.add_argument("--output", type=Path, default=None,
                        help="CSV de sortie (defaut : manifest_disque_AAAAMMJJHHMMSS.csv)")
    args = parser.parse_args()

    if not args.dest.is_dir():
        parser.error(f"Dossier introuvable : {args.dest}")
    if not args.ref_csv.is_file():
        parser.error(f"Referentiel introuvable : {args.ref_csv}")

    print(f"Recherche des TIFF sous {args.dest} ...")
    disk_files = [p for p in args.dest.rglob("*") if p.is_file() and p.suffix.lower() in TIFF_EXTS]
    disk_relpaths = {p.relative_to(args.dest).as_posix() for p in disk_files}
    print(f"{len(disk_relpaths)} TIFF trouve(s) sur disque.")

    print(f"Chargement du referentiel {args.ref_csv} ...")
    ref = pd.read_csv(args.ref_csv, usecols=USECOLS, low_memory=False)
    ref = ref[ref["file_type"] == "tiff"].drop_duplicates(subset="uuid")
    print(f"{len(ref)} ligne(s) TIFF dans le referentiel (dedoublonne sur uuid).")

    # Index referentiel par le chemin relatif qu'aurait produit download.py pour
    # chaque ligne (meme calcul que celui utilise pour ranger les fichiers).
    index = {}
    doublons_relkey = 0
    for row in ref.to_dict("records"):
        rel = relkey(row)
        if rel in index:
            doublons_relkey += 1
        else:
            index[rel] = row
    if doublons_relkey:
        print(f"Attention : {doublons_relkey} chemin(s) relatif(s) en double dans le referentiel "
              f"(premiere occurrence conservee).")

    matches = []
    non_trouves = []
    for rel in sorted(disk_relpaths):
        row = index.get(rel)
        if row is None:
            non_trouves.append(rel)
        else:
            matches.append(row)

    print(f"{len(matches)} fichier(s) du disque retrouve(s) dans le referentiel.")
    if non_trouves:
        print(f"{len(non_trouves)} fichier(s) du disque SANS correspondance dans le referentiel "
              f"(exclus du manifeste) :")
        for rel in non_trouves[:10]:
            print(f"  - {rel}")
        if len(non_trouves) > 10:
            print(f"  ... et {len(non_trouves) - 10} autre(s).")

    manifest = pd.DataFrame(matches)[["uuid", "path", "name", "s3_key", "corpus_code"]]
    manifest["taux"] = args.taux
    manifest = manifest.sort_values(["corpus_code", "name"])

    output = args.output or Path(f"manifest_disque_{pd.Timestamp.now():%Y%m%d%H%M%S}.csv")
    manifest.to_csv(output, index=False)

    print(f"{manifest['corpus_code'].nunique()} corpus_code, {len(manifest)} fichier(s) au total.")
    print(f"Manifeste ecrit : {output}")


if __name__ == "__main__":
    main()
