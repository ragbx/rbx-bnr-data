"""Concatène en un seul fichier daté les tranches d'un listing S3 (list9.py, list8.py...).

Lit data/s3/{listing}-{bucket}_{préfixe}_{n}.csv.gz (les tranches de 10 000 objets) et écrit
data/s3/{listing}-{bucket}_{AAAAMMJJ}.csv.gz : mêmes colonnes, une ligne par objet, triée par
clé. Les valeurs restent du texte, tel qu'écrit par le listing. Les tranches ne sont pas
supprimées. Le nom ne commence pas par « s3_listing_ » : s3_join_ref.py y prendrait un
listing partiel pour le listing complet du bucket.

Usage (depuis la racine du projet) :
  python scripts/s3/concat_listing.py listing9
  python scripts/s3/concat_listing.py listing8 --date 20260630
"""

import argparse
import re
from datetime import date
from glob import glob
from os.path import basename, join

import pandas as pd

BUCKET = "mediatheque-patarch-communicable"
COLUMNS = ["key", "last_modified", "size", "storage_class", "uuid", "checksum_md5"]


def main():
    ap = argparse.ArgumentParser(description="Concatène les tranches d'un listing S3 en un fichier daté.")
    ap.add_argument("listing", help="préfixe des tranches, par exemple listing9")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--date", default=f"{date.today():%Y%m%d}", help="date du fichier de sortie (défaut : aujourd'hui)")
    ap.add_argument("--data-dir", default=join("data", "s3"))
    args = ap.parse_args()

    stem = f"{args.listing}-{args.bucket}_"
    # tranche : ..._{préfixe}_{n}.csv.gz ; le fichier daté se termine par une date, pas par un n
    tranches = sorted(f for f in glob(join(args.data_dir, f"{stem}*.csv.gz"))
                      if re.search(r"_\d{1,3}\.csv\.gz$", f))
    if not tranches:
        raise SystemExit(f"Aucune tranche {stem}*_{{n}}.csv.gz sous {args.data_dir}")

    frames = []
    for f in tranches:
        df = pd.read_csv(f, dtype=str)
        if list(df.columns) != COLUMNS:
            raise SystemExit(f"Colonnes inattendues dans {f} : {list(df.columns)}")
        df["_tranche"] = re.sub(r"_\d{1,3}$", "", basename(f)[len(stem):-len(".csv.gz")])
        frames.append(df)
    listing = pd.concat(frames, ignore_index=True)

    doublons = listing["key"].duplicated(keep=False)
    if doublons.any():
        print(f"ATTENTION : {int(doublons.sum())} ligne(s) à clé en double (préfixes qui se recouvrent) ; "
              f"première occurrence gardée.")
        listing = listing.drop_duplicates("key", keep="first")

    par_prefixe = listing.groupby("_tranche").agg(
        objets=("key", "count"), volume_go=("size", lambda s: round(s.astype(float).sum() / 1e9, 1)))
    listing = listing[COLUMNS].sort_values("key")
    sortie = join(args.data_dir, f"{stem}{args.date}.csv.gz")
    listing.to_csv(sortie, index=False)

    print(par_prefixe.to_string())
    print(f"\n{len(tranches)} tranches → {sortie} : {len(listing)} objets, "
          f"{listing['size'].astype(float).sum() / 1e9:.1f} Go, "
          f"uuid renseigné pour {int(listing['uuid'].notna().sum())}, "
          f"checksum_md5 pour {int(listing['checksum_md5'].notna().sum())}")


if __name__ == "__main__":
    main()
