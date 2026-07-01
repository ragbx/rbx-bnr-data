"""Jointure S3 → fichiers : (ré)écrit l'état S3 au niveau fichier depuis le listing.

Le listing du bucket (data/s3/s3_listing_{date}.csv.gz) porte, pour chaque objet
versé, son `uuid` et son `checksum_md5` en métadonnées, ainsi que sa clé (`key`),
sa date (`last_modified`) et son bucket (`s3_bucket`). On reporte donc l'état S3
sur chaque fichier du référentiel par **jointure sur uuid** (uuid unique des deux
côtés, aucun checksum discordant vérifié sur le run 20260630).

Le listing courant est **autoritaire** : on écrase les colonnes s3 du référentiel
(pas d'héritage de l'ancien ref). Les quatre colonnes s3 sont remplies directement,
sans suffixe :
    - s3_key            ← key du listing              (NaN si absent du listing)
    - s3_uploaded_date  ← last_modified au format AAAAMMJJ  (NaN si absent)
    - s3_bucket         ← s3_bucket du listing  (NaN si absent)
    - s3_uploaded       = True si l'uuid est dans le listing, sinon False (100% rempli)

NB : ces colonnes plein-nom alimentent aussi ead_bnr2mnesys.py, qui indexe les
fichiers de conservation par `s3_key`. (L'ancien contrat de 60_merge lisait
s3_key_ / s3_uploaded_date_ suffixés ; le bloc s3 de 60_merge est à adapter en
conséquence.)

Usage :
    python scripts/s3/s3_join_ref.py --input <fichiers.csv[.gz]> --output <sortie.csv.gz> \
        [--s3-date AAAAMMJJ]

`--input` doit contenir une colonne `uuid`. Sortie = l'entrée avec s3_key,
s3_uploaded_date, s3_bucket et s3_uploaded réécrites.

Importable : `from s3_join_ref import charger_s3, ajouter_s3`.
"""

import argparse
from glob import glob
from os.path import join

import pandas as pd


def charger_s3(s3_date=None):
    """Charge le listing S3 réduit à (uuid, key, last_modified, s3_bucket).

    s3_date=None → listing s3_listing le plus récent disponible.
    Retourne (chemin_utilisé, dataframe[uuid, key, last_modified, s3_bucket]).
    """
    if s3_date:
        path = join("data", "s3", f"s3_listing_{s3_date}.csv.gz")
    else:
        cands = sorted(glob(join("data", "s3", "s3_listing_*.csv.gz")))
        if not cands:
            raise FileNotFoundError("Aucun data/s3/s3_listing_*.csv.gz trouvé")
        path = cands[-1]

    s3 = pd.read_csv(path, low_memory=False)
    s3 = s3[s3["uuid"].notna()]

    # un uuid = un objet ; on signale toute collision et on garde le premier
    dup = s3["uuid"].duplicated(keep=False)
    if dup.any():
        print(f"[s3] {int(dup.sum())} uuid en collision (plusieurs objets) "
              f"→ on garde le premier")
        s3 = s3.drop_duplicates("uuid", keep="first")

    return path, s3[["uuid", "key", "last_modified", "s3_bucket"]]


def ajouter_s3(df, s3):
    """Réécrit s3_key, s3_uploaded_date, s3_bucket, s3_uploaded depuis le listing."""
    df = df.copy()
    m = df.merge(
        s3.rename(
            columns={
                "key": "_s3_key",
                "last_modified": "_s3_date",
                "s3_bucket": "_s3_bucket",
            }
        ),
        on="uuid",
        how="left",
    )
    m["s3_uploaded"] = m["_s3_key"].notna()
    m["s3_key"] = m["_s3_key"]
    # last_modified (« 2025-12-24 20:35:27+00:00 ») -> AAAAMMJJ entier (20251224),
    # en Int64 nullable pour écrire « 20251224 » sans .0 (vide si non versé).
    d = pd.to_datetime(m["_s3_date"], utc=True, errors="coerce")
    m["s3_uploaded_date"] = (
        d.dt.strftime("%Y%m%d").pipe(pd.to_numeric, errors="coerce").astype("Int64")
    )
    m["s3_bucket"] = m["_s3_bucket"]
    return m.drop(columns=["_s3_key", "_s3_date", "_s3_bucket"])


if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Jointure S3 → fichiers (s3_key, s3_uploaded_date, s3_bucket, s3_uploaded)")
    p.add_argument("--input", required=True,
                   help="CSV de fichiers contenant une colonne uuid")
    p.add_argument("--output", required=True, help="CSV de sortie (.csv.gz)")
    p.add_argument("--s3-date", default=None,
                   help="Horodatage de s3_listing (défaut: le plus récent)")
    a = p.parse_args()

    s3_path, s3 = charger_s3(a.s3_date)
    print(f"[s3] listing : {s3_path} ({len(s3)} objets avec uuid)")

    df = pd.read_csv(a.input, low_memory=False)
    out = ajouter_s3(df, s3)
    n = int(out["s3_uploaded"].sum())
    print(f"[s3] {n}/{len(out)} fichiers versés sur S3 ({n / len(out) * 100:.1f}%)")
    out.to_csv(a.output, index=False)
    print(f"[s3] écrit : {a.output}")
