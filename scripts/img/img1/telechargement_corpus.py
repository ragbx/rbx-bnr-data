"""Copie sur disque dur des fichiers d'un ou plusieurs corpus d'images.

A partir d'un (ou plusieurs) manifeste(s) produit(s) par extraction_corpus_tif.py
(results/corpus/corpus_<nom>_<date>_<seed>.csv.gz), copie chaque fichier depuis
son emplacement d'origine (colonnes `path` + `name`) vers un disque dur.

Les fichiers ne sont PAS recuperes depuis S3 : la source est le stockage
d'origine, ou chaque fichier se trouve a <source>/<path>/<name>.

Usage (Windows) — manifestes explicites (--csv) et/ou repertoire (--csv-dir) :
    conda run -n rbx-bnr-data python scripts\\img\\telechargement_corpus.py ^
        --csv results\\corpus\\corpus_presse_20260502_1.csv.gz ^
               results\\corpus\\corpus_iconographie_20260502_1.csv.gz ^
        --source \\\\srvbnr.ntrbx.local\\BNR --dest E:\\corpus

    conda run -n rbx-bnr-data python scripts\\img\\telechargement_corpus.py ^
        --source \\\\srvbnr.ntrbx.local\\BNR --dest E:\\corpus

Sans argument de selection, on traite tous les manifestes du repertoire par
defaut results/corpus/tif2dl. --csv-dir traite tous les .csv / .csv.gz d'un
repertoire (defaut : results/corpus/tif2dl) ; il est cumulable avec --csv (les
doublons sont elimines). Chaque manifeste est traite independamment, dans son
propre sous-dossier de destination et avec son propre journal.

Les separateurs de `path` (/ dans le referentiel) sont convertis vers ceux du
systeme, et les chemins longs Windows (> 260 caracteres) sont geres via le
prefixe \\\\?\\ : le script fonctionne aussi bien sous Windows que sous Linux.

Arborescence de sortie : <dest>/<nom_du_csv>/conservation/<s3_key>
ou <nom_du_csv> est le nom du fichier CSV sans extension
(ex. corpus_presse_20260502_1) et <s3_key> est la cle S3 du fichier, qui
inclut deja son nom (ex. MED/MED_CP/MED_CP_XXXXXXX.tif). Les fichiers sans
s3_key (cas MED_PLA, non deposes sur S3) suivent la meme convention,
reconstruite a partir du corpus_code : <prefixe>/<corpus_code>/<name>.

Un journal CSV par execution est ecrit dans results/corpus/ (fichiers copies,
ignores, en erreur). La copie est reprenable : un fichier deja present a la
bonne taille est saute.
"""

import argparse
import csv
import os
import shutil
from datetime import datetime
from glob import glob
from os import makedirs
from os.path import basename, dirname, exists, getsize, isdir, join

import pandas as pd

DEFAULT_CSV_DIR = join("results", "corpus", "tif2dl")

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


def dest_relkey(row: dict) -> str:
    """Chemin relatif de destination, calque sur la cle S3.

    s3_key (ex. MED/MED_CP/RBX_MED_CP_0001.tif) inclut deja le nom du fichier.
    Pour les fichiers sans s3_key (cas MED_PLA, non deposes sur S3), on
    reconstruit la meme convention a partir du corpus_code :
    <prefixe>/<corpus_code>/<name> (ex. MED/MED_PLA/<name>), le prefixe etant
    le segment avant le premier « _ » du corpus_code (MED, AMR, ...).
    """
    s3_key = row.get("s3_key")
    if pd.notna(s3_key) and str(s3_key).strip():
        return str(s3_key)
    code = str(row["corpus_code"])
    return f"{code.split('_')[0]}/{code}/{row['name']}"


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


def copy_manifest(csv_path: str, source: str, dest: str) -> dict:
    """Copie tous les fichiers d'un manifeste vers <dest>/<stem>/conservation/.

    Retourne le decompte des statuts. Chaque manifeste a son propre sous-dossier
    de destination (le stem) et son propre journal horodate.
    """
    stem = manifest_stem(csv_path)

    today = datetime.now().strftime("%Y%m%d%H%M%S")
    log_path = join("results", "corpus", f"telechargement_{stem}_{today}.csv")
    totaux = {"copie": 0, "deja_present": 0, "absent": 0, "erreur": 0}

    df = pd.read_csv(csv_path, low_memory=False)
    print(f"=== Manifeste '{stem}' : {len(df)} fichiers ===")

    with open(log_path, "w", newline="", encoding="utf-8") as logf:
        writer = csv.DictWriter(
            logf, fieldnames=["corpus_code", "name", "statut", "src", "dst", "erreur"]
        )
        writer.writeheader()

        for row in tqdm(df.to_dict("records"), desc=stem, unit="f"):
            # Source : emplacement d'origine reel (<source>/<path>/<name>).
            # `path` utilise / dans le referentiel : on convertit vers le
            # separateur du systeme (indispensable sous Windows).
            rel_src = str(row["path"]).replace("/", os.sep)
            src = join(source, rel_src, str(row["name"]))
            # Destination : arborescence calquee sur la cle S3 (s3_key inclut
            # deja le nom du fichier ; repli reconstruit pour MED_PLA).
            rel_dst = dest_relkey(row).replace("/", os.sep)
            dst = join(dest, stem, "conservation", rel_dst)
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

    print(f"Bilan '{stem}' : {totaux['copie']} copies, {totaux['deja_present']} deja "
          f"presents, {totaux['absent']} absents, {totaux['erreur']} erreurs")
    print(f"Journal : {log_path}")
    return totaux


def main():
    parser = argparse.ArgumentParser(description="Copie d'un ou plusieurs corpus d'images sur disque dur.")
    parser.add_argument("--csv", nargs="+", default=[],
                        help="Un ou plusieurs manifestes CSV (.csv ou .csv.gz) a copier. "
                             "Chacun est traite dans son propre sous-dossier de destination.")
    parser.add_argument("--csv-dir", default=DEFAULT_CSV_DIR,
                        help=f"Repertoire de manifestes : tous les .csv et .csv.gz qu'il "
                             f"contient sont traites. Cumulable avec --csv. "
                             f"Defaut : {DEFAULT_CSV_DIR}.")
    parser.add_argument("--source", required=True,
                        help="Racine du stockage source (path+name s'y resolvent).")
    parser.add_argument("--dest", required=True,
                        help="Racine de destination sur le disque dur.")
    args = parser.parse_args()

    # Liste des manifestes : ceux passes explicitement (--csv) + tous ceux du
    # repertoire (--csv-dir), dedoublonnes en conservant l'ordre. Le repertoire
    # par defaut absent est simplement ignore (pas d'erreur) ; un --csv-dir
    # explicite mais introuvable est en revanche signale.
    csv_files = list(args.csv)
    if isdir(args.csv_dir):
        csv_files += sorted(
            glob(join(args.csv_dir, "*.csv")) + glob(join(args.csv_dir, "*.csv.gz"))
        )
    elif args.csv_dir != DEFAULT_CSV_DIR:
        parser.error(f"Repertoire introuvable : {args.csv_dir}")
    csv_files = list(dict.fromkeys(csv_files))

    if not csv_files:
        parser.error(
            "Aucun manifeste a copier : passer des fichiers via --csv, ou placer "
            f"des .csv/.csv.gz dans {args.csv_dir} (ou un autre --csv-dir)."
        )

    manquants = [c for c in csv_files if not exists(c)]
    if manquants:
        parser.error("Manifeste(s) introuvable(s) : " + ", ".join(manquants))

    grand_total = {"copie": 0, "deja_present": 0, "absent": 0, "erreur": 0}
    for csv_path in csv_files:
        totaux = copy_manifest(csv_path, args.source, args.dest)
        for k, v in totaux.items():
            grand_total[k] += v
        print()

    if len(csv_files) > 1:
        print(f"=== Total des {len(csv_files)} manifestes : {grand_total['copie']} copies, "
              f"{grand_total['deja_present']} deja presents, {grand_total['absent']} absents, "
              f"{grand_total['erreur']} erreurs ===")


if __name__ == "__main__":
    main()
