#!/usr/bin/env python3
r"""presse_bilan.py — Bilan a posteriori de la chaine presse : ou en est chaque
fascicule de l'extrait (envoye, en erreur et pourquoi, jamais traite).

Entrees :
- l'extrait du ref passe a presse_mets.py (.csv ou .csv.gz) : il donne la liste
  des fascicules attendus, lue comme le fait presse_mets.py (build_jobs) ;
- les recapitulatifs par fascicule de presse_mets.py (presse_mets_*.csv), de
  tous les lancements : fichiers, ou dossiers (les --out-dir) ou ils sont
  cherches. Ils sont lus du plus ancien au plus recent (horodatage du nom, a
  defaut date du fichier) : l'etat d'un fascicule est celui de sa derniere ligne ;
- --uploads (facultatif) : les resultats de presse_upload.py --execute
  (*_upload_*.csv), si l'envoi a ete fait a part ; les simulations sont ignorees ;
- --s3 (facultatif) : interroge S3 (lecture seule, utilisateur user_ro) sur la
  presence du METS de chaque fascicule. Le METS part en dernier : sa presence
  vaut fascicule complet, et S3 fait alors foi sur les recapitulatifs.

Les recapitulatifs par fichier (presse_jpeg_*.csv) ne sont pas lus : ils ne
portent que les fascicules au statut ok.

Etat d'un fascicule :
  envoye               METS sur S3 (--s3), ou a defaut envoi reussi aux recapitulatifs
  a_verifier           envoye d'apres les recapitulatifs, mais METS absent de S3
  erreur_envoi         l'envoi a echoue (cause : cle fautive et message)
  erreur, invalide     conversion ou METS en echec, validation XSD en echec
  refuse               ecarte d'emblee (aucun TIFF, titre inconnu)
  converti_non_envoye  JPEG et METS produits, sans envoi connu
  jamais_traite        aucune ligne : lot arrete (--max-echecs-envoi) ou interrompu

Sortie : un CSV, une ligne par fascicule de l'extrait (etat, mets_s3,
dernier_status, dernier_recap, passages, cause), et le decompte par etat.

Usage :
  python presse_bilan.py extrait_ref.csv sortie/                       # d'apres les recapitulatifs
  python presse_bilan.py extrait_ref.csv sortie/ autre/presse_mets_x.csv --s3
  python presse_bilan.py extrait_ref.csv sortie/ --uploads sortie/presse_jpeg_*_upload_*.csv --s3
"""

import argparse
import csv
import glob
import re
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from os.path import basename, dirname, getmtime, isdir, join

from presse_mets import BUCKET, ROOT, SETS_PATH, build_jobs, load_extract, load_titles

USER = "user_ro"
FIELDS = ["fascicule", "corpus_code", "pages", "etat", "mets_s3", "dernier_status", "dernier_recap", "passages",
          "cause"]
ENVOYES = ("envoye", "deja_present")  # statuts de presse_upload.py valant objet sur S3


def stamp(path: str) -> str:
    """Horodatage AAAAMMJJHHMMSS du recapitulatif : le dernier de son nom, a defaut la date du fichier."""
    found = re.findall(r"\d{14}", basename(path))
    return found[-1] if found else f"{datetime.fromtimestamp(getmtime(path)):%Y%m%d%H%M%S}"


def recap_files(targets: list) -> list:
    """Recapitulatifs par fascicule, du plus ancien au plus recent ; un dossier donne ses presse_mets_*.csv."""
    files = []
    for target in targets:
        files += glob.glob(join(target, "presse_mets_*.csv")) if isdir(target) else [target]
    return sorted(set(files), key=stamp)


def read_recaps(files: list) -> dict:
    """fascicule -> {status, msg, envoi (bool), recap, passages} : derniere ligne connue."""
    last = {}
    for path in files:
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                prev = last.get(row["fascicule"], {})
                last[row["fascicule"]] = {
                    "status": row["status"], "msg": (row.get("msg") or "").strip(),
                    # le recapitulatif ne dit pas si --upload etait pose : un envoi a une duree
                    "envoi": float(row.get("t_envoi_s") or 0) > 0,
                    "recap": basename(path), "passages": prev.get("passages", 0) + 1}
    return last


def read_uploads(files: list) -> dict:
    """fascicule -> (etat, cause) d'apres le dernier resultat de presse_upload.py qui le porte."""
    out = {}
    for path in sorted(files, key=stamp):
        rows = defaultdict(list)
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("fascicule") and row["status"] != "simule":
                    rows[row["fascicule"]].append(row)
        for fasc, lignes in rows.items():
            echecs = [r for r in lignes if r["status"] not in ENVOYES]
            if echecs:
                premier = next((r for r in echecs if r["status"] == "erreur"), echecs[0])
                out[fasc] = ("erreur_envoi", f"{premier['key']} : {premier['error']} "
                                             f"({len(echecs)} fichier(s) non envoye(s), {basename(path)})")
            elif any(r["type"] == "mets" for r in lignes):
                out[fasc] = ("envoye", "")
            else:
                out[fasc] = ("erreur_envoi", f"METS absent de {basename(path)}")
    return out


def mets_on_s3(keys: list, bucket: str, workers: int) -> dict:
    """cle -> True si l'objet existe (head_object, lecture seule)."""
    from botocore.exceptions import ClientError

    sys.path.insert(0, join(ROOT, "scripts", "s3"))
    from rbx_s3 import Rbx_client

    client = Rbx_client(user=USER).s3_client

    def present(key):
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey", "NotFound"):
                return False
            raise

    with ThreadPoolExecutor(max_workers=workers) as pool:
        return dict(zip(keys, pool.map(present, keys)))


def etat(recap: dict, upload: tuple, on_s3) -> tuple:
    """(etat, cause) d'un fascicule ; on_s3 : True, False, ou None sans --s3."""
    if on_s3:
        return "envoye", ""
    if not recap:
        e, cause = "jamais_traite", ""
    elif recap["status"] == "deja_envoye" or (recap["status"] == "ok" and recap["envoi"]):
        e, cause = "envoye", ""
    elif recap["status"] == "ok":
        e, cause = "converti_non_envoye", recap["msg"]
    else:
        e, cause = recap["status"], recap["msg"]
    if upload and e in ("converti_non_envoye", "jamais_traite"):
        e, cause = upload
    if e == "envoye" and on_s3 is False:
        return "a_verifier", "envoye d'apres les recapitulatifs, METS absent de S3"
    return e, cause


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("extrait", help="CSV extrait du ref passe a presse_mets.py (.csv ou .csv.gz)")
    ap.add_argument("recaps", nargs="+",
                    help="recapitulatifs presse_mets_*.csv, ou dossiers (--out-dir) ou les chercher")
    ap.add_argument("--uploads", nargs="*", default=[], help="resultats de presse_upload.py (*_upload_*.csv)")
    ap.add_argument("--s3", action="store_true", help="verifier sur S3 la presence du METS de chaque fascicule")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--sets", default=SETS_PATH, help="sets OAI (titres des journaux)")
    ap.add_argument("--workers", type=int, default=16, help="appels simultanes a S3")
    ap.add_argument("--csv-out", help="bilan (defaut : <dossier du dernier recapitulatif>/presse_bilan_AAAAMMJJHHMMSS.csv)")
    args = ap.parse_args()

    files = recap_files(args.recaps)
    if not files:
        sys.exit(f"Aucun recapitulatif presse_mets_*.csv sous : {', '.join(args.recaps)}")
    jobs, refused, _ = build_jobs(load_extract(args.extrait), "", load_titles(args.sets))
    recaps = read_recaps(files)
    uploads = read_uploads([p for u in args.uploads for p in (glob.glob(u) or [u])])
    print(f"{len(jobs) + len(refused)} fascicule(s) a l'extrait, {len(files)} recapitulatif(s) "
          f"du {stamp(files[0])} au {stamp(files[-1])}.")
    hors = set(recaps) - {j["fasc"] for j in jobs} - {f for f, _, _ in refused}
    if hors:
        print(f"{len(hors)} fascicule(s) des recapitulatifs absents de l'extrait : ignores "
              f"(ex. {sorted(hors)[0]}).", file=sys.stderr)

    s3 = {}
    if args.s3:
        s3 = mets_on_s3([f"{j['prefix']}_mets.xml" for j in jobs], args.bucket, args.workers)

    rows = []
    for job in jobs:
        recap = recaps.get(job["fasc"], {})
        on_s3 = s3.get(f"{job['prefix']}_mets.xml") if args.s3 else None
        e, cause = etat(recap, uploads.get(job["fasc"]), on_s3)
        rows.append({"fascicule": job["fasc"], "corpus_code": job["corpus"], "pages": len(job["pages"]),
                     "etat": e, "mets_s3": {True: "oui", False: "non", None: ""}[on_s3],
                     "dernier_status": recap.get("status", ""), "dernier_recap": recap.get("recap", ""),
                     "passages": recap.get("passages", 0), "cause": cause.splitlines()[0] if cause else ""})
    for fasc, corpus, msg in refused:
        recap = recaps.get(fasc, {})
        rows.append({"fascicule": fasc, "corpus_code": corpus, "etat": "refuse", "dernier_status": recap.get("status", ""),
                     "dernier_recap": recap.get("recap", ""), "passages": recap.get("passages", 0), "cause": msg})

    out = args.csv_out or join(dirname(files[-1]), f"presse_bilan_{datetime.now():%Y%m%d%H%M%S}.csv")
    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: r["fascicule"]))

    counts = Counter(r["etat"] for r in rows)
    pages = Counter()
    for r in rows:
        pages[r["etat"]] += r.get("pages") or 0
    for e, n in sorted(counts.items(), key=lambda c: -c[1]):
        print(f"  {e:<20} {n:>7} fascicule(s) {pages[e]:>9} page(s)")
    if not args.s3:
        print("Sans --s3 : etat d'apres les seuls recapitulatifs, presence sur S3 non verifiee.")
    print(f"Bilan : {out}")
    if counts.keys() - {"envoye"}:
        sys.exit(2)


if __name__ == "__main__":
    main()
