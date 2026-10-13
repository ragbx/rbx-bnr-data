#!/usr/bin/env python3
r"""ram_mets.py — Rapports du maire (AMR_RAM) : TIFF -> JPEG de diffusion, et un
METS par rapport.

Meme chaine que presse_mets.py, dont ce script reprend tout le traitement
(controle MD5 des TIFF, encodage JPEG controle et trace, MIX, validation XSD,
envoi S3 controle, recapitulatifs, mesure des durees), et meme METS que
del_mets.py (build_mets) : voir leur documentation. Seuls changent ici :
- la qualite JPEG : 80, comme pour la presse (deliberations : 75) ;
- le perimetre : lignes AMR_RAM de l'extrait dont la s3_key est sous
  AMR/AMR_RAM/, TIFF et ALTO (.xml), hors « A SUPPRIMER ». Le PDF de chaque
  volume (RBX_AMR_3D_<n>_<annees>.pdf) n'est ni lu ni decrit : il porte sur le
  volume entier, pas sur un rapport ;
- l'unite : le rapport, c'est-a-dire, dans un repertoire (le volume,
  3D001_1862-1870), les fichiers de meme prefixe RBX_AMR_RAM_<cote>_<annee> :
  la numerotation des vues repart a 0001 a chaque annee. Un volume d'une seule
  annee (3D017_1911) ou d'une plage non decoupee (3D020_1914-1930) ne compte
  qu'un rapport. Ce decoupage est celui de l'EAD
  results/ead/corpus_ocr/RBX_AMR_RAM.xml (59 notices) ;
- le METS : <repertoire>/<prefixe>_mets.xml, par exemple
  AMR/AMR_RAM/3D001_1862-1870/RBX_AMR_RAM_3D001_1862_mets.xml ;
  - dmdSec : DMD_SERIE (l'ensemble des rapports) et DMD_RAPPORT (cote et ARK
    actuel et ancien d'apres l'EAD, ou la notice est retrouvee par la cle de sa
    premiere image ; annee ou plage d'annees lue dans le nom des fichiers) ;
  - fileSec : fileGrp master (TIFF), access (JPEG), alto ;
- les pages incompletes : le TIFF et l'ALTO sont attendus ;
- les recapitulatifs : ram_mets_*.csv et ram_jpeg_*.csv. Leur colonne
  « fascicule » et les messages du lot (« fascicule(s) ») designent ici un
  rapport.

Un rapport compte de 71 a 609 vues : il est traite d'un bloc (un TIFF absent,
different du ref ou illisible met tout le rapport en erreur).

Usage :
  python ram_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie [--workers 4] [--validate]
  python ram_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie --validate --upload   # envoi reel
"""

import argparse
import multiprocessing
import posixpath
import re
import sys
from collections import defaultdict
from datetime import datetime
from os.path import basename, exists, join

import pandas as pd
from lxml import etree

import del_mets as dm
import presse_mets as pm

QUALITY = 80
CORPUS = "AMR_RAM"
# intitule de l'ensemble, d'apres l'instrument de recherche FRAC512_D_01 (3 D 1-26)
TITRE = "Administration de la commune. Rapports du maire"
EAD_PATH = join(pm.EAD_OCR_DIR, f"RBX_{CORPUS}.xml")
LIBELLES = {"ensemble": "Ensemble des rapports", "unite": "Rapport", "dmd": "DMD_RAPPORT"}

KEY_RE = re.compile(r"^AMR/AMR_RAM/(?P<dir>[^/]+)/(?P<stem>[^/]+?)\s*\.(?P<ext>\w+)$")
# RBX_AMR_RAM_3D006_1889_0339 ; RBX_AMR_RAM_3D020_1914-1930_0454
STEM_RE = re.compile(r"^(?P<fasc>RBX_AMR_RAM_(?P<cote>\d+[A-Z]+\d+)_(?P<an>\d{4}(?:-\d{4})?))_(?P<vue>\d+(?:_\d+)?)$")
REF_EXT = {"tif": "tif", "tiff": "tif", "xml": "xml"}


def load_extract(path: str) -> pd.DataFrame:
    """Extrait du ref (.csv ou .csv.gz) : lignes AMR_RAM, hors « A SUPPRIMER »."""
    cols = ["name", "path", "s3_key", "size", "checksum_md5", "uuid", "corpus_code", "conservation_statut",
            "mix_dateTimeCreated"]
    df = pd.read_csv(path, dtype=str, usecols=cols, low_memory=False)
    df = df[(df.corpus_code == CORPUS) & df.s3_key.str.match(KEY_RE.pattern, na=False)]
    return df[~df.conservation_statut.str.startswith("À SUPPRIMER", na=False)]


def load_notices() -> dict:
    """debut de s3_key du rapport (.../RBX_AMR_RAM_3D001_1862) -> {cote, current,
    previous}, d'apres l'EAD corpus_ocr : la notice est reperee par la cle de sa
    premiere image de conservation."""
    notices = {}
    if exists(EAD_PATH):
        for c in etree.parse(EAD_PATH).iter("c"):
            first = c.xpath('string((dao|daogrp/daoloc)[@role="preservation:image:first"]/@href)')
            m = STEM_RE.match(posixpath.splitext(posixpath.basename(first))[0]) if first else None
            if not m:
                continue
            notice = {role: c.xpath(f'string((dao|daogrp/daoloc)[@role="publication:{role}"]/@href)')
                      for role in ("current", "previous")}
            notice["cote"] = " ".join((c.findtext("did/unitid") or "").split())
            notices[f"{posixpath.dirname(first)}/{m['fasc']}"] = notice
    return notices


def build_jobs(ref: pd.DataFrame, source: str, titles: dict):
    """Un job par rapport de l'extrait ; ses TIFF sont attendus sous
    <source>/<path>/<name> (chemin du ref). titles (sets OAI) n'est pas utilise."""
    rapports = defaultdict(lambda: defaultdict(dict))
    infos = {}
    anomalies = []
    for row in ref.itertuples(index=False):
        k = KEY_RE.match(row.s3_key)  # la cle fait foi, pas le nom
        ext = REF_EXT.get(k["ext"].lower()) if k else None
        if not ext:
            continue  # dont le PDF du volume
        m = STEM_RE.match(k["stem"])
        if not m or not k["dir"].startswith(m["cote"] + "_"):
            anomalies.append(f"{row.s3_key} : nom hors du schema RBX_{CORPUS}_<cote>_<annee>_<vue>, ligne ignoree")
            continue
        prefix = f"{posixpath.dirname(row.s3_key)}/{m['fasc']}"
        infos[prefix] = m
        page = rapports[prefix][m["vue"]]
        if ext in page:
            anomalies.append(f"{m['fasc']} vue {m['vue']} : plusieurs .{ext} au ref")
        page[ext] = {"key": row.s3_key, "size": int(float(row.size)), "md5": row.checksum_md5,
                     "uuid": row.uuid if isinstance(row.uuid, str) else ""}
        if ext == "tif":
            rel = row.path.split("/") if isinstance(row.path, str) else []
            page[ext]["local"] = join(source, *rel, row.name)
            page[ext]["created_ref"] = row.mix_dateTimeCreated if isinstance(row.mix_dateTimeCreated, str) else ""

    jobs, refused = [], []
    notices = load_notices()
    for prefix, pages in rapports.items():
        m = infos[prefix]
        if not any("tif" in f for f in pages.values()):
            refused.append((m["fasc"], CORPUS, "aucun TIFF pour ce rapport dans l'extrait"))
            continue
        notice = notices.get(prefix, {})
        jobs.append({"fasc": m["fasc"], "corpus": CORPUS, "title": TITRE, "serie": CORPUS,
                     "cote": notice.get("cote") or dm.cote_label(m["cote"]),
                     "notice": {"date": m["an"], "normal": m["an"].replace("-", "/")},  # 1862 ; 1914/1930
                     "arks": {r: notice.get(r, "") for r in ("current", "previous")},
                     "pages": {p: dict(f) for p, f in pages.items()},
                     "prefix": prefix})
    return jobs, refused, anomalies


def incomplete_pages(pages: dict) -> dict:
    """{vue: [types absents du ref]} parmi le TIFF et l'ALTO."""
    out = {}
    for page in sorted(pages):
        missing = [pm.GROUPS[ext][1].lower() for ext in ("tif", "xml") if ext not in pages[page]]
        if missing:
            out[page] = missing
    return out


def main():
    # ce qui est propre aux rapports remplace son equivalent presse et
    # deliberations ; les processus du lot heritent de ces remplacements (fork)
    multiprocessing.set_start_method("fork")
    dm.SCRIPT = basename(__file__)
    dm.LIBELLES = LIBELLES
    pm.__doc__ = __doc__
    pm.QUALITY = QUALITY
    pm.load_extract = load_extract
    pm.build_jobs = build_jobs
    pm.build_mets = dm.build_mets
    pm.incomplete_pages = incomplete_pages

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--out-dir")
    pre.add_argument("--csv-out")
    pre.add_argument("--jpeg-csv-out")
    args, _ = pre.parse_known_args()
    if args.out_dir:
        stamp = f"{datetime.now():%Y%m%d%H%M%S}"
        if not args.csv_out:
            sys.argv += ["--csv-out", join(args.out_dir, f"ram_mets_{stamp}.csv")]
        if not args.jpeg_csv_out:
            sys.argv += ["--jpeg-csv-out", join(args.out_dir, f"ram_jpeg_{stamp}.csv")]
    pm.main()


if __name__ == "__main__":
    main()
