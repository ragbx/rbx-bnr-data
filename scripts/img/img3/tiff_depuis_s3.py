#!/usr/bin/env python3
r"""tiff_depuis_s3.py - Telecharge depuis S3 les TIFF d'un manifeste, sous <source>/<path>/<name>.

C'est l'emplacement ou publies_mets.py (et med_mets.py, presse_mets.py) lit les TIFF. Le
telechargement remplace la copie depuis le partage : une partie des TIFF n'ont pas de chemin
sur le partage au ref (3 446 TIFF MED_MON dont le path est un dossier de livraison relatif,
38 TIFF sous E:/RBX_NUM_MEL), mais tous sont sur S3 (statut TRANSFERT_S3_OK).

Chaque fichier est telecharge de s3_bucket/s3_key dans un fichier .part, controle (taille et
MD5 du ref), puis renomme. Un fichier deja present a la bonne taille et au bon MD5 est saute :
une relance reprend la ou le lot en etait. Lecture seule sur S3 (profil user_ro).

Journal : <manifeste>_telechargement_<horodatage>.csv sous --journal (defaut : <source>),
une ligne par TIFF (statut telecharge / deja_present / erreur).

Usage (depuis la racine du depot, sur le serveur) :
  python scripts/img/img3/tiff_depuis_s3.py results/img/manifeste_corpus_publies_lot.csv.gz --source /home/kibini/bnr
  python scripts/img/img3/tiff_depuis_s3.py <manifeste> --source <dossier> --workers 8 --limite 20   # essai
"""

import argparse
import csv
import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os.path import basename, exists, getsize, join
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts" / "s3"))


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for bloc in iter(lambda: f.read(1 << 20), b""):
            h.update(bloc)
    return h.hexdigest()


def conforme(path: str, size: int, md5: str) -> bool:
    return exists(path) and getsize(path) == size and md5_file(path) == md5


def un_tiff(client, row: dict, source: str) -> dict:
    dest = join(source, *row["path"].split("/"), row["name"])
    size, md5 = int(float(row["size"])), row["checksum_md5"].strip().lower()
    res = {"s3_key": row["s3_key"], "dest": dest, "size": size, "statut": "", "erreur": ""}
    try:
        if conforme(dest, size, md5):
            res["statut"] = "deja_present"
            return res
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        client.download_file(row["s3_bucket"], row["s3_key"], dest + ".part")
        if getsize(dest + ".part") != size:
            raise ValueError(f"taille {getsize(dest + '.part')} au lieu de {size}")
        if md5_file(dest + ".part") != md5:
            raise ValueError("MD5 different du ref")
        os.replace(dest + ".part", dest)
        res["statut"] = "telecharge"
    except Exception as e:  # le lot continue ; le fichier .part est retire
        res["statut"], res["erreur"] = "erreur", f"{type(e).__name__}: {e}"
        if exists(dest + ".part"):
            os.remove(dest + ".part")
    return res


def main():
    ap = argparse.ArgumentParser(description="Telecharge depuis S3 les TIFF d'un manifeste.")
    ap.add_argument("manifeste")
    ap.add_argument("--source", required=True, help="racine ou deposer <path>/<name>")
    ap.add_argument("--workers", type=int, default=8, help="telechargements en parallele (defaut 8)")
    ap.add_argument("--limite", type=int, default=0, help="seulement les N premiers TIFF (essai)")
    ap.add_argument("--journal", help="dossier du journal (defaut : --source)")
    args = ap.parse_args()

    df = pd.read_csv(args.manifeste, dtype=str, low_memory=False,
                     usecols=["path", "name", "size", "checksum_md5", "s3_key", "s3_bucket"])
    if args.limite:
        df = df.head(args.limite)
    manquant = df[["path", "name", "size", "checksum_md5", "s3_key", "s3_bucket"]].isna().any(axis=1)
    if manquant.any():
        sys.exit(f"{manquant.sum()} lignes sans path, name, size, checksum_md5, s3_key ou s3_bucket : arret")
    dests = df.path + "/" + df.name
    if dests.duplicated().any():
        sys.exit(f"{dests.duplicated().sum()} chemins <path>/<name> en double dans le manifeste : arret")

    from rbx_s3 import Rbx_client
    client = Rbx_client(user="user_ro").s3_client
    go = df["size"].astype(float).sum() / 1e9
    print(f"{len(df)} TIFF, {go:.1f} Go -> {args.source} ({args.workers} en parallele)", flush=True)

    stamp = f"{datetime.now():%Y%m%d%H%M%S}"
    nom = basename(args.manifeste).removesuffix(".gz").removesuffix(".csv")
    journal = join(args.journal or args.source, f"{nom}_telechargement_{stamp}.csv")
    os.makedirs(os.path.dirname(journal), exist_ok=True)
    compte, octets = {}, 0
    with open(journal, "w", newline="", encoding="utf-8") as f, ThreadPoolExecutor(args.workers) as ex:
        w = csv.DictWriter(f, fieldnames=["s3_key", "dest", "size", "statut", "erreur"])
        w.writeheader()
        futures = [ex.submit(un_tiff, client, row, args.source) for row in df.to_dict("records")]
        for i, fut in enumerate(as_completed(futures), 1):
            res = fut.result()
            w.writerow(res)
            compte[res["statut"]] = compte.get(res["statut"], 0) + 1
            octets += res["size"] if res["statut"] != "erreur" else 0
            if res["statut"] == "erreur":
                print(f"ERREUR {res['s3_key']} : {res['erreur']}", flush=True)
            if i % 500 == 0 or i == len(futures):
                print(f"[{datetime.now():%H:%M:%S}] {i}/{len(futures)} ({octets / 1e9:.1f} Go) "
                      + ", ".join(f"{n} {s}" for s, n in sorted(compte.items())), flush=True)
    print(f"Journal : {journal}")
    sys.exit(2 if compte.get("erreur") else 0)


if __name__ == "__main__":
    main()
