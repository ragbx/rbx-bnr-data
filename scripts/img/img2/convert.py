#!/usr/bin/env python3
r"""convert.py — Convertit en JPEG les fichiers deja copies par download.py,
a un ou plusieurs taux de compression par fichier (colonne `taux` du CSV,
qualites separees par ';', ex. "80;85;90").

Resolution native : aucun redimensionnement, seule la compression varie.
Chaque taux produit son propre fichier, depose a cote du TIFF source (meme
repertoire, calque sur la cle S3) : page001.tif -> page001_q80.jpg,
page001_q85.jpg, ... afin de pouvoir comparer les compressions apres coup.

Les TIFF source portent souvent un profil ICC propre au scanner (pas du sRGB
standard) : sans conversion, les couleurs ressortent trop saturees en JPEG
(rouges trop vifs, etc.). On convertit donc explicitement vers sRGB via
icc_transform avant l'enregistrement (uniquement pour les images couleur
avec profil embarque ; les images en niveaux de gris ne sont pas concernees).
Le chargement + la conversion ICC ne sont faits qu'une fois par fichier
source, meme si plusieurs taux sont demandes (evite de redecoder le TIFF
une fois par taux).

Colonnes utilisees du CSV : path (non utilise ici), name, s3_key, corpus_code
(pour retrouver le fichier source, cf. common.relkey), taux.

La conversion est reprenable (fichier de sortie deja present -> saute, sauf
--overwrite) et isole les erreurs par fichier (et par taux).

Usage :
  python convert.py manifest.csv --dest E:\corpus\corpus_presse_1
"""

import argparse
import csv
import os
import sys
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from os.path import dirname, exists, getsize, join, splitext

import pandas as pd

from common import relkey

# Sous Windows, libvips est fournie comme DLL autonome : on ajoute son dossier au PATH
# AVANT d'importer pyvips. Sur Linux/macOS, libvips est installee par le gestionnaire
# de paquets (ou conda) et trouvee automatiquement : rien a faire.
if os.name == "nt":
    vips_bin = os.environ.get(
        "VIPS_BIN",
        r"C:\Users\pichenotf\vips-dev-x64-all-8.18.3\vips-dev-8.18\bin",
    )
    if os.path.isdir(vips_bin):
        os.environ["PATH"] = vips_bin + os.pathsep + os.environ["PATH"]
        os.add_dll_directory(vips_bin)

# 1 seul thread libvips par processus : on parallelise au niveau des fichiers
# (ProcessPoolExecutor). Doit etre pose avant tout import de pyvips.
os.environ.setdefault("VIPS_CONCURRENCY", "1")

try:
    from tqdm import tqdm
except ImportError:  # tqdm est optionnel
    tqdm = None


def convert_group(src_str: str, corpus_code: str, outputs: list) -> list:
    """Convertit un fichier source vers plusieurs qualites JPEG (outputs = [(dst, quality), ...]).

    Charge et transforme (ICC -> sRGB) le TIFF une seule fois, puis encode
    chaque qualite demandee a partir de cette meme image en memoire.
    """
    import pyvips  # reimport local (process pool)

    try:
        image = pyvips.Image.new_from_file(src_str, access="sequential")
        # Conversion vers sRGB uniquement si l'image a un profil embarque et
        # au moins 3 bandes (une image en niveaux de gris n'a pas de teinte
        # a corriger, et la conversion la ferait passer inutilement en RGB).
        if image.bands >= 3 and image.get_typeof("icc-profile-data") != 0:
            image = image.icc_transform("srgb")
        if len(outputs) > 1:
            # Materialise les pixels en memoire : en access="sequential", l'image
            # ne peut etre lue qu'une fois en flux, donc un 2e jpegsave planterait
            # ("out of order read") sans ce cache complet.
            image = image.copy_memory()
    except Exception:  # noqa: BLE001 — echec de lecture/transformation : tous les taux echouent
        msg = traceback.format_exc()
        return [{"src": src_str, "corpus_code": corpus_code, "dst": dst_str, "quality": q,
                "status": "erreur", "msg": msg}
                for dst_str, q in outputs]

    results = []
    for dst_str, quality in outputs:
        try:
            os.makedirs(dirname(dst_str), exist_ok=True)
            # JPEG progressif (meilleur rendu au chargement web).
            image.jpegsave(dst_str, Q=quality, interlace=True)
            results.append({
                "src": src_str, "corpus_code": corpus_code, "dst": dst_str, "quality": quality,
                "status": "ok", "msg": "",
                "src_size": getsize(src_str), "dst_size": getsize(dst_str),
                "width": image.width, "height": image.height,
            })
        except Exception:  # noqa: BLE001 — on attrape tout pour ne pas tuer le lot
            results.append({"src": src_str, "corpus_code": corpus_code, "dst": dst_str,
                            "quality": quality, "status": "erreur", "msg": traceback.format_exc()})
    return results


def dst_path(dest_root: str, rel: str, quality: int) -> str:
    """Chemin de sortie, dans le meme repertoire que le TIFF source."""
    stem, _ = splitext(rel)
    return join(dest_root, f"{stem}_q{quality}.jpg")


def build_jobs(df, dest_root: str, overwrite: bool):
    """Liste les conversions a faire, groupees par fichier source : une ligne
    du CSV peut produire plusieurs sorties (une par taux de la colonne `taux`),
    traitees ensemble pour ne charger/transformer le TIFF qu'une seule fois.

    Retourne une liste de (src, corpus_code, [(dst, quality), ...])."""
    groups = {}
    skipped, absents = 0, 0
    for row in df.to_dict("records"):
        rel = relkey(row).replace("/", os.sep)
        src = join(dest_root, rel)
        if not exists(src):
            absents += 1
            continue
        taux_raw = str(row.get("taux", "")).strip()
        if not taux_raw or taux_raw.lower() == "nan":
            continue
        for q in taux_raw.split(";"):
            q = q.strip()
            if not q:
                continue
            dst = dst_path(dest_root, rel, int(q))
            if exists(dst) and not overwrite:
                skipped += 1
                continue
            group = groups.setdefault(src, {"corpus_code": row.get("corpus_code", ""), "outputs": []})
            group["outputs"].append((dst, int(q)))
    jobs = [(src, g["corpus_code"], g["outputs"]) for src, g in groups.items()]
    return jobs, skipped, absents


def main():
    parser = argparse.ArgumentParser(
        description="Convertit en JPEG (taux de la colonne `taux`) les fichiers deja "
                    "copies par download.py, a cote de leur TIFF source."
    )
    parser.add_argument("csv_path", help="manifeste CSV (.csv ou .csv.gz)")
    parser.add_argument("--dest", required=True,
                        help="racine contenant les TIFF (deja copies par download.py)")
    parser.add_argument("--workers", type=int, default=os.cpu_count())
    parser.add_argument("--overwrite", action="store_true",
                        help="reconvertir meme si le fichier de sortie existe")
    parser.add_argument("--csv-out", default=None,
                        help="recapitulatif CSV (defaut : <dest>/conversion_AAAAMMJJHHMMSS.csv)")
    args = parser.parse_args()

    df = pd.read_csv(args.csv_path, low_memory=False)

    jobs, skipped, absents = build_jobs(df, args.dest, args.overwrite)
    n_outputs = sum(len(outputs) for _, _, outputs in jobs)
    print(f"{len(jobs)} fichier(s) source a traiter ({n_outputs} sortie(s) JPEG au total), "
          f"{skipped} deja presente(s) sautee(s), {absents} source(s) TIFF introuvable(s) sous {args.dest}.")
    if not jobs:
        return

    progress = tqdm(total=n_outputs, unit="img") if tqdm else None

    ok = err = 0
    rows = []
    t0 = time.time()
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(convert_group, src, corpus_code, outputs) for src, corpus_code, outputs in jobs]
        for fut in as_completed(futures):
            for res in fut.result():
                if res["status"] == "ok":
                    ok += 1
                    rows.append(res)
                else:
                    err += 1
                    print(f"ECHEC {res['src']} (q={res['quality']}) :\n{res['msg']}", file=sys.stderr)
                if progress:
                    progress.update(1)
    if progress:
        progress.close()

    csv_out = args.csv_out or join(args.dest, f"conversion_{datetime.now():%Y%m%d%H%M%S}.csv")
    if rows:
        rows.sort(key=lambda r: (r["src"], r["quality"]))
        with open(csv_out, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["src", "corpus_code", "src_size", "dst", "quality", "dst_size", "width", "height"]
            )
            writer.writeheader()
            for r in rows:
                writer.writerow({
                    "src": r["src"], "corpus_code": r["corpus_code"], "src_size": r["src_size"],
                    "dst": r["dst"], "quality": r["quality"], "dst_size": r["dst_size"],
                    "width": r["width"], "height": r["height"],
                })
        print(f"Recapitulatif : {csv_out}")

    dt = time.time() - t0
    print(f"Termine : {ok} reussis, {err} en erreur, en {dt:.1f}s.")
    if err:
        sys.exit(2)


if __name__ == "__main__":
    main()
