#!/usr/bin/env python3
r"""med_mets.py — Fonds de la mediatheque (MED_MS, MED_VDM, MED_PAR) : TIFF -> JPEG
de diffusion, et un METS par notice de l'instrument de recherche.

Meme chaine que presse_mets.py, dont ce script reprend tout le traitement
(controle MD5 des TIFF, encodage JPEG controle et trace, MIX, validation XSD,
envoi S3 controle, recapitulatifs, mesure des durees), et meme METS que
del_mets.py (build_mets) : voir leur documentation. Seuls changent ici :
- la qualite JPEG : 80, comme pour la presse ;
- le perimetre : lignes MED_MS, MED_VDM et MED_PAR de l'extrait, TIFF seuls
  (ces fonds n'ont ni ALTO, ni texte, ni PDF par vue), pourvues d'une s3_key et
  d'un unitid, hors « A SUPPRIMER ». Un extrait peut melanger les trois fonds ;
- l'unite : la notice de l'instrument de recherche, c'est-a-dire les TIFF de
  meme unitid au ref (colonne remplie par le maillon dao). Une notice compte de
  1 a 934 vues ; MED_PAR a surtout des notices d'une seule image. Une notice
  dont les TIFF sont dans plusieurs repertoires de s3_key est refusee : c'est le
  cas de 9 notices MED_VDM de VDM_a_traiter/, qui melangent un original et sa
  photocopie (memes noms de fichiers, contenus differents) ;
- les vues : numerotees 0001, 0002... dans l'ordre naturel des s3_key, les noms
  de fichiers etant irreguliers (_160a, _109d, « _001 .tif », « _320..tif ») ;
  ORDERLABEL porte la fin du nom propre a la vue (160a), ou le dernier element
  du nom (109 pour ..._SPO_109, suivi de ..._SPO_109a) ;
- le METS : <repertoire>/RBX_<fonds>_<unitid>_mets.xml, le premier element de
  l'unitid etant omis s'il repete le fonds (Ms_009 -> RBX_MED_MS_009 ;
  VDM_ADP1_D01 -> RBX_MED_VDM_ADP1_D01 ; BOH_SPO_001 -> RBX_MED_PAR_BOH_SPO_001),
  par exemple MED/MED_MS/RBX_MED_MS_009_mets.xml ;
  - dmdSec : DMD_SERIE (le fonds : intitule du set OAI RBX_<fonds>, code) et
    DMD_NOTICE (intitule, cote, dates, nombre de vues, ARK actuel et ancien,
    d'apres l'IR results/ead/ead_cor/bnr2mnesys/FR595126101_<fonds>.xml, ou la
    notice est retrouvee par son unitid). Une date « sans date » n'est pas
    reprise, meme si l'IR lui donne une forme normalisee. typeOfResource
    text manuscript="yes" pour MED_MS seulement ; pas de langue (latin, francais) ;
  - fileSec : fileGrp master (TIFF) et access (JPEG) ;
  - structMap physique : document > page ;
- les recapitulatifs : med_mets_*.csv et med_jpeg_*.csv. Leur colonne
  « fascicule » et les messages du lot (« fascicule(s) ») designent ici une
  notice.

Le JPEG est depose a la cle du TIFF, extension .jpg, comme l'attendent les roles
access:image* de l'IR ; les espaces et points parasites avant l'extension
(76 cles MED_VDM : « _001 .tif », « _320..tif ») sont retires (jpeg_key).

Usage :
  python med_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie [--workers 4] [--validate]
  python med_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie --validate --upload   # envoi reel
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
from presse_mets import NS, q, sub

QUALITY = 80
CORPORA = ("MED_MS", "MED_VDM", "MED_PAR")
MANUSCRITS = "MED_MS"
IR_DIR = join(pm.ROOT, "results", "ead", "ead_cor", "bnr2mnesys")
LIBELLES = {"ensemble": "Fonds", "unite": "Notice", "dmd": "DMD_NOTICE"}
SANS_DATE = re.compile(r"^\s*(sans date|s\.\s*d\.?)\s*$", re.I)


def load_extract(path: str) -> pd.DataFrame:
    """Extrait du ref (.csv ou .csv.gz) : TIFF MED_MS, MED_VDM, MED_PAR pourvus
    d'une s3_key et d'un unitid, hors « A SUPPRIMER »."""
    cols = ["name", "path", "s3_key", "size", "checksum_md5", "uuid", "corpus_code", "conservation_statut",
            "mix_dateTimeCreated", "unitid"]
    df = pd.read_csv(path, dtype=str, usecols=cols, low_memory=False)
    df = df[df.corpus_code.isin(CORPORA) & df.s3_key.str.match(r".+\.tiff?$", case=False, na=False)
            & (df.unitid.fillna("").str.strip() != "")]
    return df[~df.conservation_statut.str.startswith("À SUPPRIMER", na=False)]


def load_notices(corpus: str, anomalies: list) -> dict:
    """unitid -> {titre, date, normal, current, previous}, d'apres l'IR bnr2mnesys
    du fonds."""
    notices, doubles = {}, set()
    path = join(IR_DIR, f"FR595126101_{corpus}.xml")
    if not exists(path):
        anomalies.append(f"IR introuvable : {path} (METS sans intitule, date ni ARK)")
        return notices
    for c in etree.parse(path).iter("c"):
        unitid = (c.findtext("did/unitid") or "").strip()
        if not unitid:
            continue
        if unitid in notices:
            doubles.add(unitid)
        date = " ".join((c.findtext("did/unitdate") or "").split())
        notice = {role: c.xpath(f'string((dao|daogrp/daoloc)[@role="publication:{role}"]/@href)')
                  for role in ("current", "previous")}
        notice.update(titre=" ".join((c.findtext("did/unittitle") or "").split()),
                      date="" if SANS_DATE.match(date) else date,
                      normal="" if SANS_DATE.match(date) else c.xpath("string(did/unitdate/@normal)"))
        notices[unitid] = notice
    for unitid in sorted(doubles):  # notice ambigue : ni intitule, ni date, ni ARK
        anomalies.append(f"{corpus} {unitid} : plusieurs notices de meme unitid dans l'IR, ARK non repris")
        notices[unitid] = {}
    return notices


def unit_id(corpus: str, unitid: str) -> str:
    """Ms_009 -> RBX_MED_MS_009 ; VDM_ADP1_D01 -> RBX_MED_VDM_ADP1_D01 ;
    BOH_SPO_001 -> RBX_MED_PAR_BOH_SPO_001"""
    parts = unitid.split("_")
    if len(parts) > 1 and parts[0].upper() == corpus.split("_", 1)[1]:
        parts = parts[1:]
    return f"RBX_{corpus}_{'_'.join(parts)}"


def natural(key: str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", key)]


def view_label(stem: str, fasc: str) -> str:
    """Fin du nom propre a la vue : RBX_MED_MS_009_001 -> 001 ; ..._SPO_109d -> d ;
    dernier element du nom s'il ne reste rien (..._SPO_109 -> 109) ou si le nom
    ne commence pas par l'identifiant de la notice."""
    stem = stem.strip(" ._")
    rest = stem[len(fasc):].strip(" ._") if stem.lower().startswith(fasc.lower()) else ""
    return rest or stem.rsplit("_", 1)[-1]


def jpeg_key(tif_key: str) -> str:
    """s3_key du JPEG : celle du TIFF sans espace ni point parasite avant
    l'extension, extension .jpg (« _001 .tif » -> « _001.jpg », « _320..tif » ->
    « _320.jpg »), comme l'attendent les roles access:image* de l'IR."""
    return posixpath.splitext(tif_key)[0].rstrip(" .") + ".jpg"


def build_jobs(ref: pd.DataFrame, source: str, titles: dict):
    """Un job par notice de l'extrait ; ses TIFF sont attendus sous
    <source>/<path>/<name> (chemin du ref)."""
    notes = defaultdict(list)
    for row in ref.itertuples(index=False):
        notes[(row.corpus_code, row.unitid.strip())].append(row)

    jobs, refused, anomalies, notices, prefixes = [], [], [], {}, {}
    for (corpus, unitid), rows in sorted(notes.items()):
        fasc = unit_id(corpus, unitid)
        dirs = sorted({posixpath.dirname(r.s3_key) for r in rows})
        if len(dirs) > 1:
            refused.append((fasc, corpus, f"TIFF dans {len(dirs)} repertoires de s3_key ({', '.join(dirs)})"))
            continue
        if corpus not in titles:
            refused.append((fasc, corpus, f"titre inconnu pour {corpus} (pas de set OAI)"))
            continue
        prefix = f"{dirs[0]}/{fasc}"
        if prefix in prefixes:
            refused.append((fasc, corpus, f"meme METS que la notice {prefixes[prefix]} ({prefix}_mets.xml)"))
            continue
        prefixes[prefix] = unitid
        if corpus not in notices:
            notices[corpus] = load_notices(corpus, anomalies)
        notice = notices[corpus].get(unitid)
        if notice is None:
            anomalies.append(f"{corpus} {unitid} : notice absente de l'IR, METS sans intitule, date ni ARK")
            notice = {}

        pages, labels = {}, {}
        rows = sorted(rows, key=lambda r: natural(r.s3_key))
        for seq, row in enumerate(rows, 1):
            page = f"{seq:04d}"
            rel = row.path.split("/") if isinstance(row.path, str) else []
            pages[page] = {"tif": {
                "key": row.s3_key, "size": int(float(row.size)), "md5": row.checksum_md5,
                "uuid": row.uuid if isinstance(row.uuid, str) else "", "local": join(source, *rel, row.name),
                "created_ref": row.mix_dateTimeCreated if isinstance(row.mix_dateTimeCreated, str) else ""}}
            labels[page] = view_label(posixpath.splitext(posixpath.basename(row.s3_key))[0], fasc)
        jobs.append({"fasc": fasc, "corpus": corpus, "title": titles[corpus], "serie": corpus,
                     "cote": unitid, "notice": notice,
                     "arks": {r: notice.get(r, "") for r in ("current", "previous")},
                     "pages": pages, "labels": labels, "prefix": prefix})
    return jobs, refused, anomalies


def incomplete_pages(pages: dict) -> dict:
    """{vue: [types absents du ref]} : seul le TIFF est attendu."""
    return {p: ["tif"] for p in sorted(pages) if "tif" not in pages[p]}


# --------------------------------------------------------------------------- METS


def type_of_resource(root, serie: str):
    if serie == MANUSCRITS:
        sub(root, "mods", "typeOfResource", "text", manuscript="yes")


def mods_notice(job: dict, n_pages: int):
    """MODS de la notice : intitule, cote, dates et ARK de l'IR."""
    root = etree.Element(q("mods", "mods"), nsmap={"mods": NS["mods"]})
    sub(sub(root, "mods", "titleInfo"), "mods", "title", job["notice"].get("titre") or job["title"])
    type_of_resource(root, job["serie"])
    bornes = [b for b in job["notice"].get("normal", "").split("/") if re.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", b)]
    if bornes or job["notice"].get("date"):
        origin = sub(root, "mods", "originInfo")
        if job["notice"].get("date"):
            sub(origin, "mods", "dateCreated", job["notice"]["date"])
        if len(bornes) == 2 and bornes[0] != bornes[1]:
            sub(origin, "mods", "dateCreated", bornes[0], encoding="w3cdtf", keyDate="yes", point="start")
            sub(origin, "mods", "dateCreated", bornes[1], encoding="w3cdtf", point="end")
        elif bornes:
            sub(origin, "mods", "dateCreated", bornes[0], encoding="w3cdtf", keyDate="yes")
    sub(sub(root, "mods", "physicalDescription"), "mods", "extent", f"{n_pages} vue{'s' if n_pages > 1 else ''}")
    host = sub(root, "mods", "relatedItem", type="host")
    sub(sub(host, "mods", "titleInfo"), "mods", "title", job["title"])
    sub(root, "mods", "identifier", job["cote"], type="local", displayLabel="cote")
    sub(root, "mods", "identifier", job["fasc"], type="local")
    for role in ("current", "previous"):  # roles publication:* de l'IR
        if job["arks"].get(role):
            sub(root, "mods", "identifier", job["arks"][role], type="uri", displayLabel=role)
    sub(sub(root, "mods", "recordInfo"), "mods", "recordIdentifier", job["prefix"], source="s3_key")
    return root


def build_mets(job: dict, pages: dict, version: str, uuid: str):
    """METS de del_mets.py, puis : division « document » et ORDERLABEL = fin du
    nom de la vue (les vues sont numerotees 0001... par ce script)."""
    tree = dm.build_mets(job, pages, version, uuid)
    for div in tree.getroot().iter(q("mets", "div")):
        if div.get("TYPE") == "volume":
            div.set("TYPE", "document")
        elif div.get("TYPE") == "page":
            div.set("ORDERLABEL", job["labels"][div.get("ID")[len("PAGE_"):]])
    return tree


# ---------------------------------------------------------------------- lancement


def main():
    # ce qui est propre a ces fonds remplace son equivalent presse et
    # deliberations ; les processus du lot heritent de ces remplacements (fork)
    multiprocessing.set_start_method("fork")
    dm.SCRIPT = basename(__file__)
    dm.LIBELLES = LIBELLES
    dm.type_of_resource = type_of_resource
    dm.mods_registre = mods_notice
    pm.__doc__ = __doc__
    pm.QUALITY = QUALITY
    pm.load_extract = load_extract
    pm.build_jobs = build_jobs
    pm.build_mets = build_mets
    pm.incomplete_pages = incomplete_pages
    pm.jpeg_key = jpeg_key

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--out-dir")
    pre.add_argument("--csv-out")
    pre.add_argument("--jpeg-csv-out")
    args, _ = pre.parse_known_args()
    if args.out_dir:
        stamp = f"{datetime.now():%Y%m%d%H%M%S}"
        if not args.csv_out:
            sys.argv += ["--csv-out", join(args.out_dir, f"med_mets_{stamp}.csv")]
        if not args.jpeg_csv_out:
            sys.argv += ["--jpeg-csv-out", join(args.out_dir, f"med_jpeg_{stamp}.csv")]
    pm.main()


if __name__ == "__main__":
    main()
