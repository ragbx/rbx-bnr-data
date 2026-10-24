"""Rechargement sur s3 des 10 JPG AMR_EC dont la taille a changé sur az (ref 20261023).

Ces fichiers (même name+path qu'au ref 20260630, taille différente) ont été recompressés
sur az. Le ref 20261023 porte déjà leur nouveau checksum_md5 (calculé sur az par
scripts/azrael/35_checksum_absents.py) et garde leur uuid et leur s3_key : l'objet s3
existant, qui contient encore l'ancienne version, est écrasé à la même clé, dans le
bucket du ref (incommunicable), avec les étiquettes uuid + checksum_md5.

Contrôles :
    - avant envoi : le MD5 du fichier local doit être égal au checksum_md5 du ref, et
      la taille locale égale à la taille du ref (sinon le fichier n'est pas envoyé) ;
    - après envoi : taille (ContentLength), ETag (= MD5, envoi en une partie sous
      8 Mio) et étiquettes relues (GetObjectTagging, droit de user_rw).

Par défaut, simple vérification (rien n'est envoyé) ; --apply pour envoyer.

Usage (depuis la racine du dépôt) :
    python scripts/s3/reupload_amr_ec_taille_20261023.py [--root /home/kibini/bnr] [--apply]

Sortie : results/s3/result/reupload_amr_ec_taille_20261023_{dt}.csv
"""

import argparse
import csv
import hashlib
import sys
from datetime import datetime
from os.path import dirname, exists, getsize, join

import pandas as pd

sys.path.insert(0, dirname(__file__))
from rbx_s3 import Rbx_client  # noqa: E402

REF = join("results", "ref", "_ref_files_20261023.csv.gz")
ABSENTS = join("results", "ref", "tmp", "az_absents_ref_20261023_cs.csv.gz")
MULTIPART_SEUIL = 8 * 1024 * 1024


def md5(path):
    h = hashlib.md5()
    with open(path, "rb") as f:
        for bloc in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloc)
    return h.hexdigest()


p = argparse.ArgumentParser()
p.add_argument("--root", default="/home/kibini/bnr", help="racine des fichiers azrael")
p.add_argument("--apply", action="store_true", help="envoyer réellement sur s3")
a = p.parse_args()

cles = ["name", "path"]
cibles = pd.read_csv(ABSENTS)
cibles = cibles.loc[cibles["motif"] == "taille_differente", cles]
ref = pd.read_csv(REF, dtype=str, keep_default_na=False,
                  usecols=cles + ["size", "checksum_md5", "uuid", "s3_key", "s3_bucket"])
files = cibles.merge(ref, on=cles, how="left")
assert len(files) == 10 and (files["s3_key"] != "").all() and (files["s3_bucket"] != "").all()

client = Rbx_client(user="user_rw").s3_client if a.apply else None
dt = datetime.now().strftime("%Y%m%d%H%M%S")
log_path = join("results", "s3", "result", f"reupload_amr_ec_taille_20261023_{dt}.csv")
champs = ["name", "path", "uuid", "key", "bucket", "size", "checksum_md5", "md5_local",
          "uploaded", "uploaded_file_size", "etag", "tag_uuid", "tag_checksum_md5",
          "uploaded_file_lastmodified", "error"]

with open(log_path, "w", newline="", encoding="utf-8") as logfile:
    writer = csv.DictWriter(logfile, fieldnames=champs)
    writer.writeheader()
    for f in files.to_dict(orient="records"):
        fichier = join(a.root, f["path"], f["name"])
        size = int(float(f["size"]))
        log = {"name": f["name"], "path": f["path"], "uuid": f["uuid"], "key": f["s3_key"],
               "bucket": f["s3_bucket"], "size": size, "checksum_md5": f["checksum_md5"],
               "uploaded": False}
        if not exists(fichier):
            log["error"] = "fichier absent"
        else:
            log["md5_local"] = md5(fichier)
            if log["md5_local"] != f["checksum_md5"]:
                log["error"] = "md5 local != checksum_md5 du ref"
            elif getsize(fichier) != size:
                log["error"] = "taille locale != taille du ref"
            elif size >= MULTIPART_SEUIL:
                log["error"] = "fichier >= 8 Mio : ETag non comparable, à traiter à part"
        if a.apply and "error" not in log:
            tags = f"uuid={f['uuid']}&checksum_md5={f['checksum_md5']}"
            try:
                client.upload_file(fichier, f["s3_bucket"], f["s3_key"], ExtraArgs={"Tagging": tags})
                head = client.head_object(Bucket=f["s3_bucket"], Key=f["s3_key"])
                tagset = client.get_object_tagging(Bucket=f["s3_bucket"], Key=f["s3_key"])["TagSet"]
                tagset = {t["Key"]: t["Value"] for t in tagset}
                log.update(uploaded=True, uploaded_file_size=head["ContentLength"],
                           etag=head["ETag"].strip('"'),
                           uploaded_file_lastmodified=head["LastModified"].isoformat(),
                           tag_uuid=tagset.get("uuid"), tag_checksum_md5=tagset.get("checksum_md5"))
                erreurs = []
                if log["uploaded_file_size"] != size:
                    erreurs.append("cohérence tailles")
                if log["etag"] != f["checksum_md5"]:
                    erreurs.append("ETag != checksum_md5")
                if log["tag_uuid"] != f["uuid"] or log["tag_checksum_md5"] != f["checksum_md5"]:
                    erreurs.append("étiquettes")
                if erreurs:
                    log["error"] = " ; ".join(erreurs)
            except Exception as e:
                log["error"] = f"envoi : {e}"
        writer.writerow(log)
        print(f["s3_key"], "|", "envoyé" if log["uploaded"] else "non envoyé", "|", log.get("error", "ok"))

print("journal :", log_path, "| mode :", "ENVOI" if a.apply else "VÉRIFICATION (--apply pour envoyer)")
