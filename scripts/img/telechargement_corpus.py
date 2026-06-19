"""Copie sur disque dur des fichiers des trois corpus d'images.

A partir des manifestes produits par extraction_corpus_tif.py
(results/corpus/corpus_<nom>_<date>.csv.gz), copie chaque fichier depuis son
emplacement d'origine (colonnes `path` + `name`) vers un disque dur.

Les fichiers ne sont PAS recuperes depuis S3 : la source est le stockage
d'origine, ou chaque fichier se trouve a <racine_source>/<path>/<name>.

Usage (Windows) :
    conda run -n ds python scripts\\img\\telechargement_corpus.py ^
        --source \\\\srvbnr.ntrbx.local\\BNR --dest E:\\corpus [--date 20260619]

Les separateurs de `path` (/ dans le referentiel) sont convertis vers ceux du
systeme, et les chemins longs Windows (> 260 caracteres) sont geres via le
prefixe \\\\?\\ : le script fonctionne aussi bien sous Windows que sous Linux.

Arborescence de sortie : <dest>/<corpus>/<corpus_code>/<name>

Un journal CSV par execution est ecrit dans results/corpus/ (fichiers copies,
ignores, en erreur). La copie est reprenable : un fichier deja present a la
bonne taille est saute.
"""

import argparse
import csv
import glob
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

CORPORA = ["presse", "iconographie", "manuscrits_plans"]


def find_manifest(corpus: str, date: str | None) -> str | None:
    """Chemin du manifeste le plus recent pour un corpus (ou date donnee)."""
    if date:
        path = join("results", "corpus", f"corpus_{corpus}_{date}.csv.gz")
        return path if exists(path) else None
    matches = sorted(glob.glob(join("results", "corpus", f"corpus_{corpus}_*.csv.gz")))
    return matches[-1] if matches else None


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
    parser = argparse.ArgumentParser(description="Copie des corpus d'images sur disque dur.")
    parser.add_argument("--source", required=True,
                        help="Racine du stockage source (path+name s'y resolvent).")
    parser.add_argument("--dest", required=True,
                        help="Racine de destination sur le disque dur.")
    parser.add_argument("--date", default=None,
                        help="Date des manifestes (AAAAMMJJ). Defaut : le plus recent.")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = join("results", "corpus", f"telechargement_{today}.csv")
    totaux = {"copie": 0, "deja_present": 0, "absent": 0, "erreur": 0}

    with open(log_path, "w", newline="", encoding="utf-8") as logf:
        writer = csv.DictWriter(
            logf, fieldnames=["corpus", "corpus_code", "name", "statut", "src", "dst", "erreur"]
        )
        writer.writeheader()

        for corpus in CORPORA:
            manifest = find_manifest(corpus, args.date)
            if not manifest:
                print(f"[!] Aucun manifeste pour le corpus '{corpus}' — ignore.")
                continue

            df = pd.read_csv(manifest, low_memory=False)
            print(f"\n=== Corpus '{corpus}' : {len(df)} fichiers ({basename(manifest)}) ===")

            for row in tqdm(df.to_dict("records"), desc=corpus, unit="f"):
                # `path` utilise / dans le referentiel : on convertit vers le
                # separateur du systeme (indispensable sous Windows).
                rel_path = str(row["path"]).replace("/", os.sep)
                src = join(args.source, rel_path, str(row["name"]))
                dst = join(args.dest, corpus, str(row["corpus_code"]), str(row["name"]))
                erreur = ""
                try:
                    statut, _ = copy_one(src, dst)
                except OSError as e:
                    statut, erreur = "erreur", str(e)
                totaux[statut] = totaux.get(statut, 0) + 1
                writer.writerow({
                    "corpus": corpus, "corpus_code": row["corpus_code"],
                    "name": row["name"], "statut": statut,
                    "src": src, "dst": dst, "erreur": erreur,
                })

    print(f"\nBilan : {totaux['copie']} copies, {totaux['deja_present']} deja presents, "
          f"{totaux['absent']} absents, {totaux['erreur']} erreurs")
    print(f"Journal : {log_path}")


if __name__ == "__main__":
    main()
