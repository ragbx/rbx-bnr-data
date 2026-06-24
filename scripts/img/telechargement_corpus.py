"""Copie sur disque dur des fichiers d'un corpus d'images.

A partir d'un manifeste produit par extraction_corpus_tif.py
(results/corpus/corpus_<nom>_<date>_<seed>.csv.gz), copie chaque fichier depuis
son emplacement d'origine (colonnes `path` + `name`) vers un disque dur.

Les fichiers ne sont PAS recuperes depuis S3 : la source est le stockage
d'origine, ou chaque fichier se trouve a <source>/<path>/<name>.

Usage (Windows) :
    conda run -n rbx-bnr-data python scripts\\img\\telechargement_corpus.py ^
        --csv results\\corpus\\corpus_presse_20260502_1.csv.gz ^
        --source \\\\srvbnr.ntrbx.local\\BNR --dest E:\\corpus

Les separateurs de `path` (/ dans le referentiel) sont convertis vers ceux du
systeme, et les chemins longs Windows (> 260 caracteres) sont geres via le
prefixe \\\\?\\ : le script fonctionne aussi bien sous Windows que sous Linux.

Arborescence de sortie : <dest>/<nom_du_csv>/conservation/<path>/<name>
ou <nom_du_csv> est le nom du fichier CSV sans extension
(ex. corpus_presse_20260502_1).

Un journal CSV par execution est ecrit dans results/corpus/ (fichiers copies,
ignores, en erreur). La copie est reprenable : un fichier deja present a la
bonne taille est saute.
"""

import argparse
import csv
import os
import shutil
from datetime import datetime
from os import makedirs
from os.path import basename, dirname, exists, getsize, join

import pandas as pd

try:
    from tqdm import tqdm
except ImportError:  # tqdm est optionnel
    def tqdm(it, **kwargs):
        return it


def manifest_stem(csv_path: str) -> str:
    """Nom du manifeste sans extension (corpus_presse_..._1.csv.gz -> ...1)."""
    stem = basename(csv_path)
    for suffix in (".gz", ".csv"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return stem


def long_path(p: str) -> str:
    """Sous Windows, prefixe le chemin pour lever la limite des 260 caracteres.

    Local  : C:\\... -> \\\\?\\C:\\...
    UNC    : \\\\srv\\share\\... -> \\\\?\\UNC\\srv\\share\\...
    Ailleurs (Linux/macOS) : chemin inchange.
    """
    if os.name != "nt":
        return p
    p = os.path.abspath(p)  # normalise les separateurs et resout . / ..
    if p.startswith("\\\\?\\"):
        return p
    if p.startswith("\\\\"):  # chemin UNC
        return "\\\\?\\UNC\\" + p[2:]
    return "\\\\?\\" + p


def copy_one(src: str, dst: str) -> tuple[str, str]:
    """Copie src -> dst. Retourne (statut, message)."""
    # Chemins prefixes pour les operations disque (longs chemins Windows) ;
    # on conserve src/dst non prefixes pour le journal.
    src_fs, dst_fs = long_path(src), long_path(dst)
    if not exists(src_fs):
        return "absent", src
    if exists(dst_fs) and getsize(dst_fs) == getsize(src_fs):
        return "deja_present", dst
    makedirs(dirname(dst_fs), exist_ok=True)
    shutil.copy2(src_fs, dst_fs)
    return "copie", dst


def main():
    parser = argparse.ArgumentParser(description="Copie d'un corpus d'images sur disque dur.")
    parser.add_argument("--csv", required=True,
                        help="Manifeste CSV (.csv ou .csv.gz) listant les fichiers a copier.")
    parser.add_argument("--source", required=True,
                        help="Racine du stockage source (path+name s'y resolvent).")
    parser.add_argument("--dest", required=True,
                        help="Racine de destination sur le disque dur.")
    args = parser.parse_args()

    if not exists(args.csv):
        parser.error(f"Manifeste introuvable : {args.csv}")

    stem = manifest_stem(args.csv)

    today = datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = join("results", "corpus", f"telechargement_{stem}_{today}.csv")
    totaux = {"copie": 0, "deja_present": 0, "absent": 0, "erreur": 0}

    df = pd.read_csv(args.csv, low_memory=False)
    print(f"=== Manifeste '{stem}' : {len(df)} fichiers ===")

    with open(log_path, "w", newline="", encoding="utf-8") as logf:
        writer = csv.DictWriter(
            logf, fieldnames=["corpus_code", "name", "statut", "src", "dst", "erreur"]
        )
        writer.writeheader()

        for row in tqdm(df.to_dict("records"), desc=stem, unit="f"):
            # `path` utilise / dans le referentiel : on convertit vers le
            # separateur du systeme (indispensable sous Windows).
            rel_path = str(row["path"]).replace("/", os.sep)
            src = join(args.source, rel_path, str(row["name"]))
            dst = join(args.dest, stem, "conservation", rel_path, str(row["name"]))
            erreur = ""
            try:
                statut, _ = copy_one(src, dst)
            except OSError as e:
                statut, erreur = "erreur", str(e)
            totaux[statut] = totaux.get(statut, 0) + 1
            writer.writerow({
                "corpus_code": row.get("corpus_code", ""),
                "name": row["name"], "statut": statut,
                "src": src, "dst": dst, "erreur": erreur,
            })

    print(f"\nBilan : {totaux['copie']} copies, {totaux['deja_present']} deja presents, "
          f"{totaux['absent']} absents, {totaux['erreur']} erreurs")
    print(f"Journal : {log_path}")


if __name__ == "__main__":
    main()
