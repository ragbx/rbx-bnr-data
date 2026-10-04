#!/usr/bin/env python3
r"""presse_upload.py — Envoie sur S3 les JPEG produits par presse_mets.py.

Entree : le recapitulatif de presse_mets.py (presse_jpeg_*.csv, une ligne par
JPEG et par METS des fascicules au statut ok : type, name, path, size,
checksum_md5, uuid, s3_key). Chaque fichier est lu sous <source>/<path>/<name>,
ou <source> est le --out-dir de presse_mets.py.

Les JPEG partent d'abord, les METS ensuite : le METS d'un fascicule n'est envoye
que si tous ses JPEG sont sur S3 (envoyes ou deja presents), puisqu'il decrit
ces fichiers. Un fascicule dont un JPEG echoue reste sans METS.

Pour chaque fichier :
1. controle local : le fichier existe, sa taille et son MD5 sont ceux du
   recapitulatif (sinon erreur, rien n'est envoye) ;
2. sans --execute, on s'arrete la (simulation : aucun appel a S3) ;
3. un objet deja present a cette s3_key n'est pas ecrase (sauf --overwrite) :
   il doit avoir la taille et le MD5 du fichier local, sinon erreur ;
4. envoi a sa s3_key, avec les etiquettes uuid et checksum_md5 comme
   scripts/s3/upload.py, et le type image/jpeg (METS : memes etiquettes, type
   application/xml) ; l'objet depose est ensuite controle : taille
   et MD5 (ETag, ou relecture de l'objet s'il a ete envoye en plusieurs parties).

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
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os.path import dirname, exists, getsize, join, splitext

BUCKET = "mediatheque-patarch-communicable"  # celui de tous les fichiers presse du ref
USER = "user_rw"
ROOT = dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))  # racine du depot
CONTENT_TYPES = {"jpeg": "image/jpeg", "mets": "application/xml"}
FIELDS = ["fascicule", "type", "name", "path", "checksum_md5", "uuid", "size", "key", "uploaded", "uploaded_file_size",
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


def remote_head(client, bucket: str, key: str):
    """head_object de la cle, None si l'objet n'existe pas."""
    from botocore.exceptions import ClientError

    try:
        return client.s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
            return None
        raise


def remote_exists(client, bucket: str, key: str) -> bool:
    return remote_head(client, bucket, key) is not None


def verify_remote(client, bucket: str, key: str, size: int, md5: str, head=None) -> dict:
    """Controle l'objet depose : taille et MD5 du fichier local. Le MD5 est lu dans
    l'ETag quand c'en est un (envoi en une partie) ; sinon (envoi en plusieurs
    parties) l'objet est relu en entier. Leve ValueError au moindre ecart."""
    head = head or remote_head(client, bucket, key)
    if head is None:
        raise ValueError("objet absent de S3 apres l'envoi")
    if head["ContentLength"] != size:
        raise ValueError(f"taille sur S3 {head['ContentLength']} differente du fichier ({size})")
    etag = head.get("ETag", "").strip('"').lower()
    if not re.fullmatch(r"[0-9a-f]{32}", etag):
        h = hashlib.md5()
        for chunk in client.s3_client.get_object(Bucket=bucket, Key=key)["Body"].iter_chunks(1 << 20):
            h.update(chunk)
        etag = h.hexdigest()
    if etag != md5:
        raise ValueError(f"MD5 sur S3 {etag} different du fichier ({md5})")
    return head


def send_one(row: dict, source: str, client, bucket: str, overwrite: bool) -> dict:
    """Controle puis envoie un fichier ; client=None : simulation, aucun appel a S3.
    Statut envoye ou deja_present : l'objet sur S3 a la taille et le MD5 du fichier."""
    kind = row.get("type") or "jpeg"  # anciens recapitulatifs : JPEG seuls
    res = {"fascicule": row.get("fascicule", ""), "type": kind, "name": row["name"], "path": row["path"],
           "checksum_md5": row["checksum_md5"], "uuid": row["uuid"],
           "size": row["size"], "key": row["s3_key"], "uploaded": False, "uploaded_file_size": None,
           "uploaded_file_lastmodified": None, "error": None, "status": "erreur"}
    try:
        size = int(row["size"])
        local = join(source, *row["path"].split("/"), row["name"])
        if not exists(local):
            raise ValueError(f"fichier absent : {local}")
        if getsize(local) != size:
            raise ValueError(f"taille locale {getsize(local)} differente du recapitulatif")
        if md5_file(local) != row["checksum_md5"]:
            raise ValueError("MD5 local different du recapitulatif")
        if not row["uuid"]:
            raise ValueError("uuid vide au recapitulatif")
        if client is None:
            res["status"] = "simule"
            return res
        head = None if overwrite else remote_head(client, bucket, row["s3_key"])
        if head is not None:
            try:
                verify_remote(client, bucket, row["s3_key"], size, row["checksum_md5"], head)
            except ValueError as e:
                raise ValueError(f"objet deja present et different, relancer avec --overwrite ({e})") from e
            res.update(status="deja_present", uploaded_file_size=head["ContentLength"],
                       uploaded_file_lastmodified=head["LastModified"].isoformat())
            return res
        tags = f"checksum_md5={row['checksum_md5']}"
        if row["uuid"]:
            tags = f"uuid={row['uuid']}&{tags}"
        up = client.upload(local, bucket, row["s3_key"], ExtraArgs={"Tagging": tags, "ContentType": CONTENT_TYPES[kind]})
        if not up.get("result"):
            raise ValueError(up.get("error") or "envoi en echec")
        head = verify_remote(client, bucket, row["s3_key"], size, row["checksum_md5"])
        res.update(uploaded=True, uploaded_file_size=head["ContentLength"],
                   uploaded_file_lastmodified=head["LastModified"].isoformat(), status="envoye")
    except Exception as e:  # erreur isolee par fichier
        res["error"] = str(e)
    return res


def run(rows: list, source: str, client, bucket: str, overwrite: bool, workers: int, out_path: str) -> dict:
    """JPEG d'abord, METS ensuite (seulement ceux dont tous les JPEG sont sur S3) ; ecrit le
    resultat au fil de l'eau ; retourne le decompte par status."""
    counts = {}
    echecs = set()  # fascicules ayant un JPEG en erreur
    jpegs = [r for r in rows if (r.get("type") or "jpeg") == "jpeg"]
    metss = [r for r in rows if r.get("type") == "mets"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        f.flush()

        def phase(batch):
            with ThreadPoolExecutor(max_workers=workers) as pool:
                futures = [pool.submit(send_one, row, source, client, bucket, overwrite) for row in batch]
                for fut in as_completed(futures):
                    res = fut.result()
                    counts[res["status"]] = counts.get(res["status"], 0) + 1
                    if res["status"] == "erreur":
                        echecs.add(res["fascicule"])
                        print(f"ECHEC {res['key']} : {res['error']}", file=sys.stderr)
                    w.writerow(res)
                    f.flush()

        phase(jpegs)
        a_envoyer = []
        for row in metss:
            if row["fascicule"] in echecs:
                counts["non_envoye"] = counts.get("non_envoye", 0) + 1
                print(f"METS NON ENVOYE {row['s3_key']} : JPEG du fascicule en erreur", file=sys.stderr)
                w.writerow({"fascicule": row["fascicule"], "type": "mets", "name": row["name"], "path": row["path"],
                            "checksum_md5": row["checksum_md5"], "size": row["size"], "key": row["s3_key"],
                            "uploaded": False, "error": "JPEG du fascicule en erreur", "status": "non_envoye"})
                f.flush()
            else:
                a_envoyer.append(row)
        phase(a_envoyer)
    return counts


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("jpeg_csv", help="recapitulatif de presse_mets.py (presse_jpeg_*.csv)")
    ap.add_argument("--source", required=True, help="racine des fichiers : le --out-dir de presse_mets.py")
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
    print(f"{len(rows)} fichiers (JPEG + METS), {mode}")

    client = make_client() if args.execute else None
    counts = run(rows, args.source, client, args.bucket, args.overwrite, args.workers, out_path)
    print("Termine : " + ", ".join(f"{n} {s}" for s, n in sorted(counts.items())) + f". Resultat : {out_path}")
    if counts.get("erreur") or counts.get("non_envoye"):
        sys.exit(2)


if __name__ == "__main__":
    main()
