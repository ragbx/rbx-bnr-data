#!/usr/bin/env python3
r"""presse_download.py — Telecharge depuis S3 les JPEG de diffusion et les ALTO des
fascicules de presse d'un ou plusieurs manifestes (ni TIFF, ni texte, ni PDF, ni METS).

Entree : le ou les manifestes passes a presse_mets.py (extraits du ref, .csv ou
.csv.gz), lus comme lui (build_jobs). Pour chaque page :
- JPEG : la s3_key du TIFF, extension .jpg (la cle sous laquelle presse_mets.py
  le depose) ;
- ALTO : la s3_key du .xml au ref.

Chaque fichier est ecrit sous <out-dir>/<s3_key> (arborescence de S3). Pour chacun :
1. un fichier deja present est garde s'il a la taille attendue (celle du ref pour
   l'ALTO, sans appel a S3 ; celle de l'objet sur S3 pour le JPEG), sauf --overwrite ;
2. un objet absent de S3 est signale (statut absent) : JPEG d'un fascicule pas
   encore converti, par exemple ;
3. telechargement sous <nom>.part, controle, puis renommage : taille de l'objet,
   et empreinte (colonne controle) : MD5 du ref pour l'ALTO ; pour le JPEG, MD5 de
   l'ETag (md5), ou, s'il a ete envoye en plusieurs parties (8 Mio et plus), ETag
   recalcule sur le fichier (etag_multipart) ; a defaut la taille seule (taille).
   L'etiquette checksum_md5 de l'objet n'est pas lue : user_ro n'y a pas acces.

L'acces S3 est celui de scripts/s3/rbx_s3.py (conf.yml), en lecture seule
(utilisateur user_ro).

Le resultat (une ligne par fichier : telecharge, deja_present, absent, erreur ;
simule avec --simulation) est ecrit au fil de l'eau.

Usage :
  python presse_download.py results/presse/manifeste_PRA_RTG.csv results/presse/manifeste_PRA_CTG.csv --out-dir /chemin/sortie
  python presse_download.py results/presse/manifeste_PRA_RTG.csv --out-dir /chemin/sortie --types alto
  python presse_download.py results/presse/manifeste_PRA_RTG.csv --out-dir /chemin/sortie --simulation   # aucun appel a S3
"""

import argparse
import csv
import hashlib
import os
import posixpath
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os.path import dirname, exists, getsize, join

import pandas as pd

from presse_mets import BUCKET, ROOT, SETS_PATH, build_jobs, load_extract, load_titles

USER = "user_ro"
TYPES = ("jpeg", "alto")
PART_SIZE = 8 << 20  # taille des parties d'un envoi boto3 par defaut (rbx_s3.upload), des 8 Mio
FIELDS = ["fascicule", "type", "s3_key", "size", "checksum_md5", "controle", "status", "error"]


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def multipart_etag(path: str) -> str:
    """ETag S3 d'un objet envoye en plusieurs parties de PART_SIZE : MD5 des MD5
    des parties, suivi de leur nombre."""
    digests = []
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(PART_SIZE), b""):
            digests.append(hashlib.md5(chunk).digest())
    return f"{hashlib.md5(b''.join(digests)).hexdigest()}-{len(digests)}"


def targets(jobs: list, types: list) -> list:
    """Fichiers a telecharger : {fascicule, type, s3_key, size, md5} ; taille et MD5
    connus d'avance pour l'ALTO seulement (ref)."""
    out = []
    for job in jobs:
        for page in sorted(job["pages"]):
            files = job["pages"][page]
            if "jpeg" in types and "tif" in files:
                out.append({"fascicule": job["fasc"], "type": "jpeg", "size": None, "md5": "",
                            "s3_key": posixpath.splitext(files["tif"]["key"])[0] + ".jpg"})
            if "alto" in types and "xml" in files:
                out.append({"fascicule": job["fasc"], "type": "alto", "size": files["xml"]["size"],
                            "md5": files["xml"]["md5"], "s3_key": files["xml"]["key"]})
    return out


def fetch_one(t: dict, out_dir: str, client, bucket: str, overwrite: bool) -> dict:
    """Telecharge et controle un fichier ; client=None : simulation, aucun appel a S3."""
    from botocore.exceptions import ClientError

    res = {"fascicule": t["fascicule"], "type": t["type"], "s3_key": t["s3_key"], "size": t["size"],
           "checksum_md5": t["md5"], "controle": "", "status": "erreur", "error": None}
    dst = join(out_dir, *t["s3_key"].split("/"))
    try:
        if not overwrite and t["size"] is not None and exists(dst) and getsize(dst) == t["size"]:
            res["status"] = "deja_present"  # ALTO : taille du ref, sans appel a S3
            return res
        if client is None:
            res["status"] = "simule"
            return res
        try:
            head = client.head_object(Bucket=bucket, Key=t["s3_key"])
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                res["status"] = "absent"
                return res
            raise
        size = head["ContentLength"]
        if t["size"] is not None and size != t["size"]:
            raise ValueError(f"taille sur S3 {size} differente du ref ({t['size']})")
        res["size"] = size
        if not overwrite and exists(dst) and getsize(dst) == size:
            res["status"] = "deja_present"
            return res
        etag = head.get("ETag", "").strip('"').lower()
        os.makedirs(dirname(dst), exist_ok=True)
        tmp = dst + ".part"
        try:
            client.download_file(bucket, t["s3_key"], tmp)
            if getsize(tmp) != size:
                raise ValueError(f"taille telechargee {getsize(tmp)} differente de S3 ({size})")
            md5 = md5_file(tmp)
            parts = re.fullmatch(r"[0-9a-f]{32}-(\d+)", etag)
            if t["md5"] or re.fullmatch(r"[0-9a-f]{32}", etag):
                if md5 != (t["md5"] or etag):
                    raise ValueError(f"MD5 telecharge {md5} different de l'attendu ({t['md5'] or etag})")
                res["controle"] = "md5"
            elif parts and int(parts[1]) == -(-size // PART_SIZE):
                if multipart_etag(tmp) != etag:
                    raise ValueError(f"ETag recalcule different de celui de S3 ({etag})")
                res["controle"] = "etag_multipart"
            else:  # ETag d'une autre forme, ou autre taille de partie a l'envoi
                res["controle"] = "taille"
            os.replace(tmp, dst)
        finally:
            if exists(tmp):
                os.remove(tmp)
        res.update(status="telecharge", checksum_md5=md5)
    except Exception as e:  # erreur isolee par fichier
        res["error"] = str(e)
    return res


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("manifestes", nargs="+", help="manifestes passes a presse_mets.py (.csv ou .csv.gz)")
    ap.add_argument("--out-dir", required=True, help="racine des fichiers telecharges (arborescence des s3_key)")
    ap.add_argument("--types", nargs="+", choices=TYPES, default=list(TYPES), help="defaut : jpeg alto")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--sets", default=SETS_PATH, help="sets OAI (titres des journaux)")
    ap.add_argument("--overwrite", action="store_true", help="retelecharger les fichiers deja presents")
    ap.add_argument("--simulation", action="store_true", help="lister sans telecharger (aucun appel a S3)")
    ap.add_argument("--workers", type=int, default=8, help="telechargements simultanes")
    ap.add_argument("--csv-out", help="resultat (defaut : <out-dir>/presse_download_AAAAMMJJHHMMSS.csv)")
    args = ap.parse_args()

    ref = pd.concat([load_extract(m) for m in args.manifestes]).drop_duplicates("s3_key")
    jobs, refused, _ = build_jobs(ref, "", load_titles(args.sets))
    todo = targets(jobs, args.types)
    par_type = {k: sum(t["type"] == k for t in todo) for k in args.types}
    print(f"{len(jobs)} fascicule(s), {len(refused)} refuse(s) ; "
          + ", ".join(f"{n} {k}" for k, n in par_type.items()) + f" a telecharger sous {args.out_dir}"
          + (" (SIMULATION, aucun appel a S3)" if args.simulation else ""))

    client = None
    if not args.simulation:
        sys.path.insert(0, join(ROOT, "scripts", "s3"))
        from rbx_s3 import Rbx_client

        client = Rbx_client(user=USER).s3_client
    os.makedirs(args.out_dir, exist_ok=True)
    out_path = args.csv_out or join(args.out_dir, f"presse_download_{datetime.now():%Y%m%d%H%M%S}.csv")
    counts, octets = {}, 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        f.flush()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(fetch_one, t, args.out_dir, client, args.bucket, args.overwrite) for t in todo]
            for n, fut in enumerate(as_completed(futures), 1):
                res = fut.result()
                counts[res["status"]] = counts.get(res["status"], 0) + 1
                if res["status"] == "telecharge":
                    octets += res["size"]
                elif res["status"] == "erreur":
                    print(f"ECHEC {res['s3_key']} : {res['error']}", file=sys.stderr)
                w.writerow(res)
                f.flush()
                if n % 1000 == 0:
                    print(f"[{datetime.now():%H:%M:%S}] {n}/{len(todo)} fichiers", file=sys.stderr)
    print("Termine : " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items()))
          + f" ; {octets / 1e9:.2f} Go telecharges. Resultat : {out_path}")
    if counts.get("erreur") or counts.get("absent"):
        sys.exit(2)


if __name__ == "__main__":
    main()
