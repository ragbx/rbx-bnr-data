#!/usr/bin/env python3
r"""download.py — Copie les fichiers d'un manifeste CSV vers <dest>/conservation/.

Colonnes utilisees du CSV : path, name, s3_key, corpus_code (+ uuid pour le
journal). La source de chaque fichier est <source>/<path>/<name> ; la
destination est calquee sur la cle S3 (cf. common.relkey).

La copie est reprenable : un fichier deja present a la bonne taille est saute.
Un fichier source absent ou une erreur de copie n'interrompent pas le lot.

Usage :
  python download.py manifest.csv \
      --source \\srvbnr.ntrbx.local\BNR --dest E:\corpus\corpus_presse_1
"""

import argparse
import csv
import os
import shutil
import sys
from datetime import datetime
from os.path import dirname, exists, getsize, join

import pandas as pd

from common import relkey

try:
    from tqdm import tqdm
except ImportError:  # tqdm est optionnel
    def tqdm(it, **kwargs):
        return it


def long_path(p: str) -> str:
    """Sous Windows, prefixe le chemin pour lever la limite des 260 caracteres.

    Local : C:\\... -> \\\\?\\C:\\...  UNC : \\\\srv\\... -> \\\\?\\UNC\\srv\\...
    Ailleurs (Linux/macOS) : chemin inchange.
    """
    if os.name != "nt":
        return p
    p = os.path.abspath(p)
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):
        return "\\\\?\\UNC\\" + p[2:]
    return "\\\\?\\" + p


def copy_one(src: str, dst: str) -> str:
    """Copie src -> dst. Retourne le statut : copie / deja_present / absent."""
    src_fs, dst_fs = long_path(src), long_path(dst)
    if not exists(src_fs):
        return "absent"
    if exists(dst_fs) and getsize(dst_fs) == getsize(src_fs):
        return "deja_present"
    os.makedirs(dirname(dst_fs), exist_ok=True)
    shutil.copy2(src_fs, dst_fs)
    return "copie"


def main():
    parser = argparse.ArgumentParser(
        description="Copie les fichiers d'un manifeste CSV vers <dest>/conservation/."
    )
    parser.add_argument("csv_path", help="manifeste CSV (.csv ou .csv.gz)")
    parser.add_argument("--source", required=True,
                        help="racine du stockage source (path+name s'y resolvent)")
    parser.add_argument("--dest", required=True,
                        help="racine de destination (recevra conservation/)")
    parser.add_argument("--log", default=None,
                        help="journal CSV (defaut : <dest>/telechargement_AAAAMMJJHHMMSS.csv)")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path, low_memory=False)
    print(f"=== {len(df)} fichier(s) a copier ===")

    log_path = args.log or join(args.dest, f"telechargement_{datetime.now():%Y%m%d%H%M%S}.csv")
    os.makedirs(dirname(log_path) or ".", exist_ok=True)

    totaux = {"copie": 0, "deja_present": 0, "absent": 0, "erreur": 0}
    with open(log_path, "w", newline="", encoding="utf-8") as logf:
        writer = csv.DictWriter(logf, fieldnames=["uuid", "name", "statut", "src", "dst", "erreur"])
        writer.writeheader()

        for row in tqdm(df.to_dict("records"), unit="f"):
            rel_src = str(row["path"]).replace("/", os.sep)
            src = join(args.source, rel_src, str(row["name"]))
            dst = join(args.dest, "conservation", relkey(row).replace("/", os.sep))
            erreur = ""
            try:
                statut = copy_one(src, dst)
            except OSError as e:
                statut, erreur = "erreur", str(e)
            totaux[statut] = totaux.get(statut, 0) + 1
            writer.writerow({
                "uuid": row.get("uuid", ""), "name": row["name"], "statut": statut,
                "src": src, "dst": dst, "erreur": erreur,
            })

    print(f"Bilan : {totaux['copie']} copies, {totaux['deja_present']} deja presents, "
          f"{totaux['absent']} absents, {totaux['erreur']} erreurs")
    print(f"Journal : {log_path}")
    if totaux["absent"] or totaux["erreur"]:
        sys.exit(2)


if __name__ == "__main__":
    main()
