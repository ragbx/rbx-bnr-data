"""Liste sur le S3 les objets déposés depuis quelques jours, et les corpus concernés.

Sur le modèle de list8.py (même listing : key, last_modified, size, storage_class, uuid,
checksum_md5 ; tranches de 10 000 objets dans data/s3/), mais filtré sur la date de dépôt
(last_modified) et, par défaut, sur ce que produit la conversion TIFF → JPEG : les JPEG de
diffusion et les METS (scripts/img/img3/). S3 ne sait pas filtrer sur la date : tout le
préfixe est parcouru, puis les objets récents sont gardés.

Sorties (la date est celle du jour) :
  data/s3/listing_recents-{bucket}_{AAAAMMJJ}_{n}.csv.gz   objets récents, colonnes de list8.py
  results/s3/listing_recents_resume.csv                     un corpus par ligne : nombre de JPEG,
      de METS, volume, répertoires, premier et dernier dépôt (UTC) ; réécrit à chaque lancement
Le nom du listing ne commence pas par « s3_listing_ » : s3_join_ref.py y prendrait un listing
partiel pour le listing complet du bucket.

Le corpus est déduit de la clé (MED/MED_MS/... → MED_MS ; MED/MED_PRA/PRA_RTG/... → PRA_RTG ;
PAR/PAR_*/... et AMR/PAR_LAB/... → MED_PAR). AMR/PAR_LAB mêle MED_PAR et AMR_PAR au fichier de
référence (231 fichiers AMR_PAR) : ils sont comptés avec MED_PAR.

Usage (depuis la racine du projet) :
  python scripts/s3/list_recent.py                          # 7 derniers jours, tout le bucket
  python scripts/s3/list_recent.py --jours 3
  python scripts/s3/list_recent.py --depuis 2026-10-24 --prefix MED/MED_AFF --prefix MUS/MUS_VAI
  python scripts/s3/list_recent.py --types tous --sans-tags

Tout le bucket compte plusieurs millions d'objets (une requête par millier : quelques minutes
de parcours) ; --prefix le restreint. Les étiquettes uuid et checksum_md5 coûtent une requête par objet récent
(--sans-tags pour les omettre ; --workers requêtes en parallèle).
"""

import argparse
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from os import makedirs
from os.path import dirname, join

import pandas as pd
from botocore.exceptions import ClientError
from rbx_s3 import Rbx_client, Rbx_resource

BUCKET = "mediatheque-patarch-communicable"
CHUNK = 10000
COLUMNS = ["key", "last_modified", "size", "storage_class", "uuid", "checksum_md5"]
# dossier S3 → corpus_code du fichier de référence, quand ils diffèrent
RENOMMES = {"MED_FLRS": "MED_FLR", "TCG_PRA": "PRA_BDR", "AMR_Obj": "AMR_OBJ", "CSV": "CSV_PAL"}


def get_Tagset(s3_client, bucket_name, key):
    response = s3_client.get_object_tagging(Bucket=bucket_name, Key=key)

    tagset = {}
    for tag in response["TagSet"]:
        if tag["Key"] == "uuid":
            tagset["uuid"] = tag["Value"]
        if tag["Key"] == "checksum_md5":
            tagset["checksum_md5"] = tag["Value"]
    return tagset


def type_of(key):
    k = key.lower()
    if k.endswith("_mets.xml"):
        return "mets"
    if k.endswith((".jpg", ".jpeg")):
        return "jpeg"
    if k.endswith((".tif", ".tiff")):
        return "tiff"
    return "autre"


def corpus_of(key):
    parts = key.split("/")
    if len(parts) <= 2:  # fichier posé directement sous le premier niveau
        return RENOMMES.get(parts[0], parts[0])
    if parts[1] == "MED_PRA":
        return parts[2]
    if parts[1].startswith("PAR_"):
        return "MED_PAR"
    return RENOMMES.get(parts[1], re.sub(r"_a_traiter$", "", parts[1]))


def recents(bucket, prefixes, depuis, types):
    """Objets déposés à partir de `depuis` (datetime UTC), des types demandés, sans doublon."""
    trouves, vus, scannes = {}, 0, 0
    for prefix in prefixes:
        for obj in bucket.objects.filter(Prefix=prefix):
            scannes += 1
            if scannes % 100000 == 0:
                print(f"{scannes} objets parcourus, {len(trouves)} récents", flush=True)
            if obj.last_modified >= depuis and (types is None or type_of(obj.key) in types):
                trouves[obj.key] = {"key": obj.key, "last_modified": obj.last_modified,
                                    "size": obj.size, "storage_class": obj.storage_class}
    print(f"{scannes} objets parcourus, {len(trouves)} récents", flush=True)
    return sorted(trouves.values(), key=lambda r: r["key"])


def resume(df):
    df = df.assign(corpus=df["key"].map(corpus_of), type=df["key"].map(type_of),
                   repertoire=df["key"].str.rsplit("/", n=1).str[0],
                   last_modified=pd.to_datetime(df["last_modified"], utc=True))
    comptes = df.pivot_table(index="corpus", columns="type", values="key", aggfunc="count", fill_value=0)
    out = comptes.join(df.groupby("corpus").agg(
        volume_go=("size", lambda s: round(s.sum() / 1e9, 2)),
        repertoires=("repertoire", "nunique"),
        premier_depot=("last_modified", "min"), dernier_depot=("last_modified", "max")))
    for col in ("premier_depot", "dernier_depot"):
        out[col] = out[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return out.reset_index()


def main():
    ap = argparse.ArgumentParser(description="Objets S3 déposés depuis quelques jours, par corpus.")
    ap.add_argument("--jours", type=int, default=7, help="déposés depuis N jours (défaut : 7)")
    ap.add_argument("--depuis", help="déposés depuis cette date AAAA-MM-JJ (UTC) ; remplace --jours")
    ap.add_argument("--prefix", action="append", help="préfixe à parcourir (répétable ; défaut : tout le bucket)")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--user", default="user_ro", help="profil de conf.yml (défaut : user_ro)")
    ap.add_argument("--types", default="jpeg,mets",
                    help="types gardés parmi jpeg, mets, tiff, autre, ou « tous » (défaut : jpeg,mets)")
    ap.add_argument("--sans-tags", action="store_true", help="ne pas lire les étiquettes uuid et checksum_md5")
    ap.add_argument("--workers", type=int, default=16, help="lectures d'étiquettes en parallèle (défaut : 16)")
    ap.add_argument("--out-dir", default=join("data", "s3"))
    ap.add_argument("--resume", default=join("results", "s3", "listing_recents_resume.csv"))
    args = ap.parse_args()

    maintenant = datetime.now(timezone.utc)
    if args.depuis:
        depuis = datetime.strptime(args.depuis, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        depuis = maintenant - timedelta(days=args.jours)
    types = None if args.types == "tous" else set(args.types.split(","))
    print(f"Objets de {args.bucket} déposés depuis le {depuis:%Y-%m-%d %H:%M} UTC, types : {args.types}", flush=True)

    s3_resource = Rbx_resource(user=args.user).s3_resource
    s3_client = Rbx_client(user=args.user).s3_client
    objets = recents(s3_resource.Bucket(args.bucket), args.prefix or [""], depuis, types)
    if not objets:
        print("Aucun objet récent.")
        return

    def tags(obj):
        return get_Tagset(s3_client, args.bucket, obj["key"])

    makedirs(args.out_dir, exist_ok=True)
    stamp = f"{maintenant:%Y%m%d}"
    lignes = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for j, debut in enumerate(range(0, len(objets), CHUNK), 1):
            tranche = objets[debut:debut + CHUNK]
            if not args.sans_tags:
                try:
                    etiquettes = list(pool.map(tags, tranche))
                except ClientError as e:
                    sys.exit(f"Lecture des étiquettes refusée ({e.response['Error']['Code']}) : "
                             f"essayer --user user_rw ou --sans-tags.")
                tranche = [{**o, **t} for o, t in zip(tranche, etiquettes)]
            df = pd.DataFrame(tranche, columns=COLUMNS)
            fichier = join(args.out_dir, f"listing_recents-{args.bucket}_{stamp}_{j}.csv.gz")
            df.to_csv(fichier, index=False)
            print(f"{fichier} : {len(df)} objets", flush=True)
            lignes.append(df)

    synthese = resume(pd.concat(lignes, ignore_index=True))
    makedirs(dirname(args.resume) or ".", exist_ok=True)
    synthese.to_csv(args.resume, index=False)
    print()
    print(synthese.to_string(index=False))
    print(f"\nRésumé : {args.resume}")


if __name__ == "__main__":
    main()
