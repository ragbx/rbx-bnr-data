#!/usr/bin/env python3
r"""presse_upload.py — Envoie sur S3 les JPEG produits par presse_mets.py.

Entree : le recapitulatif par JPEG de presse_mets.py (presse_jpeg_*.csv, une
ligne par JPEG des fascicules au statut ok : name, path, size, checksum_md5,
uuid, s3_key). Chaque JPEG est lu sous <source>/<path>/<name>, ou <source> est
le --out-dir de presse_mets.py. Les METS ne sont pas envoyes.

Pour chaque JPEG :
1. controle local : le fichier existe, sa taille et son MD5 sont ceux du
   recapitulatif (sinon erreur, rien n'est envoye) ;
2. sans --execute, on s'arrete la (simulation : aucun appel a S3) ;
3. un objet deja present a cette s3_key n'est pas ecrase (sauf --overwrite) ;
4. envoi a sa s3_key, avec les etiquettes uuid et checksum_md5 comme
   scripts/s3/upload.py, et le type image/jpeg ; la taille de l'objet depose est
   comparee a celle du fichier.

L'acces S3 est celui de scripts/s3/rbx_s3.py (conf.yml, utilisateur user_rw).

Le resultat (une ligne par JPEG, colonnes de scripts/s3/upload.py + status :
simule, envoye, deja_present, erreur) est ecrit au fil de l'eau.

Usage :
  python presse_upload.py sortie/presse_jpeg_AAAAMMJJHHMMSS.csv --source sortie            # simulation
  python presse_upload.py sortie/presse_jpeg_AAAAMMJJHHMMSS.csv --source sortie --execute  # envoi reel
"""

import argparse
import csv
import hashlib
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os.path import dirname, exists, getsize, join, splitext

BUCKET = "mediatheque-patarch-communicable"  # celui de tous les fichiers presse du ref
USER = "user_rw"
ROOT = dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))  # racine du depot
FIELDS = ["name", "path", "checksum_md5", "uuid", "size", "key", "uploaded", "uploaded_file_size",
          "uploaded_file_lastmodified", "error", "status"]


def make_client():
    """Client S3 du depot (scripts/s3/rbx_s3.py), importe seulement pour un envoi reel."""
    sys.path.insert(0, join(ROOT, "scripts", "s3"))
    from rbx_s3 import Rbx_client

    return Rbx_client(user=USER)


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def remote_exists(client, bucket: str, key: str) -> bool:
    from botocore.exceptions import ClientError

    try:
        client.s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def send_one(row: dict, source: str, client, bucket: str, overwrite: bool) -> dict:
    """Controle puis envoie un JPEG ; client=None : simulation, aucun appel a S3."""
    res = {"name": row["name"], "path": row["path"], "checksum_md5": row["checksum_md5"], "uuid": row["uuid"],
           "size": row["size"], "key": row["s3_key"], "uploaded": False, "uploaded_file_size": None,
           "uploaded_file_lastmodified": None, "error": None, "status": "erreur"}
    try:
        local = join(source, *row["path"].split("/"), row["name"])
        if not exists(local):
            raise ValueError(f"fichier absent : {local}")
        if getsize(local) != int(row["size"]):
            raise ValueError(f"taille locale {getsize(local)} differente du recapitulatif")
        if md5_file(local) != row["checksum_md5"]:
            raise ValueError("MD5 local different du recapitulatif")
        if not row["uuid"]:
            raise ValueError("uuid vide au recapitulatif")
        if client is None:
            res["status"] = "simule"
            return res
        if not overwrite and remote_exists(client, bucket, row["s3_key"]):
            res["status"] = "deja_present"
            return res
        tags = f"uuid={row['uuid']}&checksum_md5={row['checksum_md5']}"
        up = client.upload(local, bucket, row["s3_key"], ExtraArgs={"Tagging": tags, "ContentType": "image/jpeg"})
        res.update(uploaded=bool(up.get("result")), uploaded_file_size=up.get("size"),
                   uploaded_file_lastmodified=up.get("LastModified"), error=up.get("error"))
        if res["uploaded"] and res["uploaded_file_size"] != int(row["size"]):
            res["error"] = "cohérence tailles"
        if res["uploaded"] and not res["error"]:
            res["status"] = "envoye"
    except Exception as e:  # erreur isolee par fichier
        res["error"] = str(e)
    return res


def run(rows: list, source: str, client, bucket: str, overwrite: bool, workers: int, out_path: str) -> dict:
    """Traite toutes les lignes ; ecrit le resultat au fil de l'eau ; retourne le decompte par status."""
    counts = {}
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        f.flush()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(send_one, row, source, client, bucket, overwrite) for row in rows]
            for fut in as_completed(futures):
                res = fut.result()
                counts[res["status"]] = counts.get(res["status"], 0) + 1
                if res["status"] == "erreur":
                    print(f"ECHEC {res['key']} : {res['error']}", file=sys.stderr)
                w.writerow(res)
                f.flush()
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("jpeg_csv", help="recapitulatif par JPEG de presse_mets.py (presse_jpeg_*.csv)")
    ap.add_argument("--source", required=True, help="racine des JPEG : le --out-dir de presse_mets.py")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--execute", action="store_true", help="envoyer reellement (defaut : simulation)")
    ap.add_argument("--overwrite", action="store_true", help="ecraser un objet deja present sur S3")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--csv-out", help="resultat (defaut : <jpeg_csv sans extension>_upload_AAAAMMJJHHMMSS.csv)")
    args = ap.parse_args()

    with open(args.jpeg_csv, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    doublons = len(rows) - len({r["s3_key"] for r in rows})
    if doublons:
        sys.exit(f"{doublons} s3_key en double dans {args.jpeg_csv} : rien n'est envoye.")
    out_path = args.csv_out or f"{splitext(args.jpeg_csv)[0]}_upload_{datetime.now():%Y%m%d%H%M%S}.csv"
    mode = f"ENVOI REEL vers {args.bucket}" if args.execute else "SIMULATION (aucun appel a S3, ajouter --execute pour envoyer)"
    print(f"{len(rows)} JPEG, {mode}")

    client = make_client() if args.execute else None
    counts = run(rows, args.source, client, args.bucket, args.overwrite, args.workers, out_path)
    print("Termine : " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())) + f". Resultat : {out_path}")
    if counts.get("erreur"):
        sys.exit(2)


if __name__ == "__main__":
    main()
