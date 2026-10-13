#!/usr/bin/env python3
r"""del_mets.py — Registres de deliberations (AMR_DEL, series DEL_MS et DEL_TAP) :
TIFF -> JPEG de diffusion, et un METS par registre.

Meme chaine que presse_mets.py, dont ce script reprend tout le traitement
(controle MD5 des TIFF, encodage JPEG controle et trace, MIX, validation XSD,
envoi S3 controle, recapitulatifs, mesure des durees) : voir sa documentation.
Seuls changent ici :
- la qualite JPEG : 75 (presse : 80) ;
- le perimetre : lignes AMR_DEL de l'extrait dont la s3_key est sous
  AMR/AMR_DEL/DEL_MS/ (registres manuscrits, TIFF seuls) ou AMR/AMR_DEL/DEL_TAP/
  (registres tapuscrits, TIFF + ALTO), hors « A SUPPRIMER ». Les seances
  499W (1983-2002), sans TIFF, ne sont pas concernees ;
- l'unite : le registre, c'est-a-dire le repertoire de la s3_key
  (1D144_1889). La vue est ce qui suit, dans le nom, le prefixe commun du
  registre : RBX_AMR_DEL_1D144_1889_0152 -> 0152 ; RBX_AMR_DEL_1D001_0096 ->
  0096 (manuscrits : pas d'annee dans le nom) ; RBX_AMR_DEL_1D008_0001_0694 ->
  0001_0694 ; RBX_AMR_DEL_1D225_1972_0022_1 -> 0022_1 ;
- le METS : <repertoire>/<prefixe commun>_mets.xml, par exemple
  AMR/AMR_DEL/DEL_TAP/1D144_1889/RBX_AMR_DEL_1D144_1889_mets.xml et
  AMR/AMR_DEL/DEL_MS/1D001_1790-1795/RBX_AMR_DEL_1D001_mets.xml ;
  - dmdSec : deux MODS minimaux, DMD_SERIE (l'ensemble des registres : titre,
    code du corpus) et DMD_REGISTRE (cote, dates extremes et ARK actuel et
    ancien d'apres results/ead/corpus_ocr/RBX_AMR_DEL.xml, langue, nombre de
    vues, identifiant local, recordIdentifier = debut de s3_key commun aux
    fichiers) ; typeOfResource text, manuscript="yes" pour DEL_MS ; pas de
    genre ;
  - fileSec : fileGrp master (TIFF), access (JPEG) et, pour DEL_TAP, alto ;
  - structMap physique : volume > page.
  Le reste (OBJID, uuid en altRecordID, MIX des TIFF et JPEG, OWNERID) est
  celui de la presse ;
- les pages incompletes : seuls le TIFF et, pour un registre qui a des ALTO,
  l'ALTO sont attendus (ni texte ni PDF par vue dans ces series) ;
- les recapitulatifs : del_mets_*.csv et del_jpeg_*.csv. Leur colonne
  « fascicule » et les messages du lot (« fascicule(s) ») designent ici un
  registre.

Un registre compte plusieurs centaines de vues : il est traite d'un bloc (un
TIFF absent, different du ref ou illisible met tout le registre en erreur), et
l'avancement n'evolue qu'a la fin de chaque registre.

Les MIX des JPEG citent presse_mets.py comme logiciel de traitement : c'est lui
qui porte le code de conversion. L'en-tete du METS cite del_mets.py.

Usage :
  python del_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie [--workers 4] [--validate]
  python del_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie --validate --upload   # envoi reel
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

import presse_mets as pm
from presse_mets import GROUPS, NS, q, sub

QUALITY = 75
CORPUS = "AMR_DEL"
# intitule de l'ensemble, d'apres l'instrument de recherche FRAC512_D_01 (1 D 1-129)
TITRE = "Conseil municipal. Registres des procès-verbaux des délibérations"
EAD_PATH = join(pm.EAD_OCR_DIR, f"RBX_{CORPUS}.xml")
MANUSCRITS = "DEL_MS"
# nom du script cite par le METS et libelles de ses deux dmdSec : remplaces par
# les scripts qui reprennent build_mets pour un autre corpus (ram_mets.py)
SCRIPT = basename(__file__)
LIBELLES = {"ensemble": "Ensemble des registres", "unite": "Registre", "dmd": "DMD_REGISTRE"}

KEY_RE = re.compile(r"^AMR/AMR_DEL/(?P<serie>DEL_MS|DEL_TAP)/(?P<dir>[^/]+)/(?P<stem>[^/]+?)\s*\.(?P<ext>\w+)$")
COTE_RE = re.compile(r"^(\d+)\s*([A-Z]+)\s*0*(\d+)$")  # 1D144 -> 1 D 144 ; 1D001 -> 1 D 1
REF_EXT = {"tif": "tif", "tiff": "tif", "xml": "xml"}


def load_extract(path: str) -> pd.DataFrame:
    """Extrait du ref (.csv ou .csv.gz) : lignes AMR_DEL des series DEL_MS et
    DEL_TAP, hors « A SUPPRIMER »."""
    cols = ["name", "path", "s3_key", "size", "checksum_md5", "uuid", "corpus_code", "conservation_statut",
            "mix_dateTimeCreated"]
    df = pd.read_csv(path, dtype=str, usecols=cols, low_memory=False)
    df = df[(df.corpus_code == CORPUS) & df.s3_key.str.match(KEY_RE.pattern, na=False)]
    return df[~df.conservation_statut.str.startswith("À SUPPRIMER", na=False)]


def cote_label(cote: str) -> str:
    """Cote telle que dans l'EAD : 1D144 -> 1 D 144 ; 1D001 -> 1 D 1."""
    m = COTE_RE.match(cote.strip())
    return " ".join(m.groups()) if m else cote.strip()


def load_notices() -> dict:
    """cote (1 D 144) -> {date, normal, current, previous} : dates extremes et
    ARK actuel et ancien du registre, d'apres l'EAD corpus_ocr."""
    notices = {}
    if exists(EAD_PATH):
        for c in etree.parse(EAD_PATH).iter("c"):
            unitid = c.findtext("did/unitid")
            if not unitid:
                continue
            notice = {role: c.xpath(f'string((dao|daogrp/daoloc)[@role="publication:{role}"]/@href)')
                      for role in ("current", "previous")}
            notice["date"] = (c.findtext("did/unitdate") or "").strip()
            notice["normal"] = c.xpath("string(did/unitdate/@normal)")
            notices[cote_label(unitid)] = notice
    return notices


def split_stem(stem: str, directory: str):
    """(prefixe commun du registre, vue) d'un nom de fichier sans extension ;
    (None, None) si le nom ne commence ni par le repertoire ni par la cote."""
    for unit in (directory, directory.split("_")[0]):
        prefix = f"RBX_{CORPUS}_{unit}"
        if stem.startswith(prefix + "_") and len(stem) > len(prefix) + 1:
            return prefix, stem[len(prefix) + 1:]
    return None, None


def build_jobs(ref: pd.DataFrame, source: str, titles: dict):
    """Un job par registre de l'extrait ; ses TIFF sont attendus sous
    <source>/<path>/<name> (chemin du ref). titles (sets OAI) n'est pas utilise."""
    registres = defaultdict(lambda: defaultdict(dict))
    infos = {}
    anomalies = []
    for row in ref.itertuples(index=False):
        m = KEY_RE.match(row.s3_key)  # la cle fait foi, pas le nom
        ext = REF_EXT.get(m["ext"].lower()) if m else None
        if not ext:
            continue
        directory = posixpath.dirname(row.s3_key)
        prefix, vue = split_stem(m["stem"], m["dir"])
        if prefix is None:
            anomalies.append(f"{row.s3_key} : nom hors du schema RBX_{CORPUS}_<registre>_<vue>, ligne ignoree")
            continue
        info = infos.setdefault(directory, {"fasc": prefix, "serie": m["serie"],
                                            "cote": cote_label(m["dir"].split("_")[0])})
        if info["fasc"] != prefix:
            anomalies.append(f"{row.s3_key} : prefixe {prefix} different de {info['fasc']}, ligne ignoree")
            continue
        page = registres[directory][vue]
        if ext in page:
            anomalies.append(f"{prefix} vue {vue} : plusieurs .{ext} au ref")
        page[ext] = {"key": row.s3_key, "size": int(float(row.size)), "md5": row.checksum_md5,
                     "uuid": row.uuid if isinstance(row.uuid, str) else ""}
        if ext == "tif":
            rel = row.path.split("/") if isinstance(row.path, str) else []
            page[ext]["local"] = join(source, *rel, row.name)
            page[ext]["created_ref"] = row.mix_dateTimeCreated if isinstance(row.mix_dateTimeCreated, str) else ""

    jobs, refused = [], []
    notices = load_notices()
    for directory, pages in registres.items():
        info = infos[directory]
        if not any("tif" in f for f in pages.values()):
            refused.append((info["fasc"], CORPUS, "aucun TIFF pour ce registre dans l'extrait"))
            continue
        notice = notices.get(info["cote"], {})
        jobs.append({"fasc": info["fasc"], "corpus": CORPUS, "title": TITRE, "serie": info["serie"],
                     "cote": info["cote"], "notice": notice,
                     "arks": {r: notice.get(r, "") for r in ("current", "previous")},
                     "pages": {p: dict(f) for p, f in pages.items()},
                     "prefix": f"{directory}/{info['fasc']}"})
    return jobs, refused, anomalies


def incomplete_pages(pages: dict) -> dict:
    """{vue: [types absents du ref]} : le TIFF, et l'ALTO si le registre en a."""
    attendus = ("tif", "xml") if any("xml" in f for f in pages.values()) else ("tif",)
    out = {}
    for page in sorted(pages):
        missing = [GROUPS[ext][1].lower() for ext in attendus if ext not in pages[page]]
        if missing:
            out[page] = missing
    return out


# --------------------------------------------------------------------------- METS


def type_of_resource(root, serie: str):
    el = sub(root, "mods", "typeOfResource", "text")
    if serie == MANUSCRITS:
        el.set("manuscript", "yes")


def mods_serie(job: dict):
    """MODS de l'ensemble des registres (commun a tous)."""
    root = etree.Element(q("mods", "mods"), nsmap={"mods": NS["mods"]})
    sub(sub(root, "mods", "titleInfo"), "mods", "title", job["title"])
    type_of_resource(root, job["serie"])
    sub(root, "mods", "identifier", job["corpus"], type="local")
    return root


def mods_registre(job: dict, n_pages: int):
    """MODS du registre : cote, dates extremes et ARK de l'EAD."""
    root = etree.Element(q("mods", "mods"), nsmap={"mods": NS["mods"]})
    info = sub(root, "mods", "titleInfo")
    sub(info, "mods", "title", job["title"])
    sub(info, "mods", "partNumber", job["cote"])
    type_of_resource(root, job["serie"])
    sub(sub(root, "mods", "language"), "mods", "languageTerm", pm.LANGUE, type="code", authority="iso639-2b")
    bornes = [b for b in job["notice"].get("normal", "").split("/") if re.fullmatch(r"\d{4}(-\d{2}(-\d{2})?)?", b)]
    if bornes:
        origin = sub(root, "mods", "originInfo")
        if len(bornes) == 2 and bornes[0] != bornes[1]:
            sub(origin, "mods", "dateCreated", bornes[0], encoding="w3cdtf", keyDate="yes", point="start")
            sub(origin, "mods", "dateCreated", bornes[1], encoding="w3cdtf", point="end")
        else:
            sub(origin, "mods", "dateCreated", bornes[0], encoding="w3cdtf", keyDate="yes")
    sub(sub(root, "mods", "physicalDescription"), "mods", "extent", f"{n_pages} vues")
    sub(root, "mods", "identifier", job["cote"], type="local", displayLabel="cote")
    sub(root, "mods", "identifier", job["fasc"], type="local")
    for role in ("current", "previous"):  # roles publication:* de l'EAD
        if job["arks"].get(role):
            sub(root, "mods", "identifier", job["arks"][role], type="uri", displayLabel=role)
    sub(sub(root, "mods", "recordInfo"), "mods", "recordIdentifier", job["prefix"], source="s3_key")
    return root


def build_mets(job: dict, pages: dict, version: str, uuid: str):
    """pages : {vue: {ext: {key, size, md5, uuid, admid?, mix?, created?}}} ; uuid : celui du METS"""
    mets = etree.Element(q("mets", "mets"), nsmap={k: NS[k] for k in ("mets", "mods", "mix", "xlink", "xsi")})
    mets.set(q("xsi", "schemaLocation"), pm.SCHEMA_LOCATION)
    mets.set("OBJID", f"{job['prefix']}_mets")  # s3_key du METS sans l'extension
    date = job["notice"].get("date", "")
    mets.set("LABEL", f"{job['title']}, {job['cote']}" + (f" ({date})" if date else ""))

    hdr = sub(mets, "mets", "metsHdr", CREATEDATE=datetime.now().astimezone().isoformat(timespec="seconds"))
    sub(sub(hdr, "mets", "agent", ROLE="CUSTODIAN", TYPE="ORGANIZATION"), "mets", "name", pm.AGENT)
    agent = sub(hdr, "mets", "agent", ROLE="CREATOR", TYPE="OTHER", OTHERTYPE="SOFTWARE")
    sub(agent, "mets", "name", SCRIPT)
    sub(agent, "mets", "note", f"version {version}")
    sub(hdr, "mets", "altRecordID", uuid, TYPE="uuid")

    dmd = sub(mets, "mets", "dmdSec", ID="DMD_SERIE")
    sub(sub(dmd, "mets", "mdWrap", MDTYPE="MODS", LABEL=LIBELLES["ensemble"]), "mets", "xmlData").append(
        mods_serie(job))
    dmd = sub(mets, "mets", "dmdSec", ID=LIBELLES["dmd"])
    sub(sub(dmd, "mets", "mdWrap", MDTYPE="MODS", LABEL=LIBELLES["unite"]), "mets", "xmlData").append(
        mods_registre(job, len(pages)))

    amd = sub(mets, "mets", "amdSec", ID="AMD")
    for page in sorted(pages):
        for ext in ("tif", "jpg"):
            f = pages[page].get(ext)
            if f:
                tech = sub(amd, "mets", "techMD", ID=f["admid"])
                sub(sub(tech, "mets", "mdWrap", MDTYPE="NISOIMG", LABEL=f"MIX {basename(f['key'])}"),
                    "mets", "xmlData").append(f["mix"])

    file_sec = sub(mets, "mets", "fileSec")
    for ext, (use, prefix, mime) in GROUPS.items():
        grp = None
        for seq, page in enumerate(sorted(pages), 1):
            f = pages[page].get(ext)
            if not f:
                continue
            if grp is None:
                grp = sub(file_sec, "mets", "fileGrp", USE=use)
            attrs = {"ID": f"{prefix}_{page}", "MIMETYPE": mime, "SEQ": seq, "SIZE": f["size"],
                     "GROUPID": f"PAGE_{page}", "CHECKSUM": f["md5"], "CHECKSUMTYPE": "MD5"}
            if f.get("created"):
                attrs["CREATED"] = f["created"]
            if f.get("admid"):
                attrs["ADMID"] = f["admid"]
            if f.get("uuid"):  # uuid du ref ; pour le JPEG, celui de sa trace XMP (aussi au MIX)
                attrs["OWNERID"] = f["uuid"]
            el = sub(grp, "mets", "file", **attrs)
            loc = sub(el, "mets", "FLocat", LOCTYPE="OTHER", OTHERLOCTYPE="SYSTEM")
            loc.set(q("xlink", "href"), f["key"])

    smap = sub(mets, "mets", "structMap", TYPE="physical")
    volume = sub(smap, "mets", "div", TYPE="volume", DMDID=f"DMD_SERIE {LIBELLES['dmd']}", LABEL=job["fasc"])
    for order, page in enumerate(sorted(pages), 1):
        label = int(page) if page.isdigit() else page  # 0152 -> 152 ; 0001_0694 et 0022_1 tels quels
        div = sub(volume, "mets", "div", ID=f"PAGE_{page}", TYPE="page", ORDER=order, ORDERLABEL=label)
        for ext, (_, prefix, _) in GROUPS.items():
            if ext in pages[page]:
                sub(div, "mets", "fptr", FILEID=f"{prefix}_{page}")
    return etree.ElementTree(mets)


# ---------------------------------------------------------------------- lancement


def main():
    # ce qui est propre aux registres remplace son equivalent presse ; les
    # processus du lot heritent de ces remplacements (demarrage par fork)
    multiprocessing.set_start_method("fork")
    pm.__doc__ = __doc__
    pm.QUALITY = QUALITY
    pm.load_extract = load_extract
    pm.build_jobs = build_jobs
    pm.build_mets = build_mets
    pm.incomplete_pages = incomplete_pages

    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--out-dir")
    pre.add_argument("--csv-out")
    pre.add_argument("--jpeg-csv-out")
    args, _ = pre.parse_known_args()
    if args.out_dir:
        stamp = f"{datetime.now():%Y%m%d%H%M%S}"
        if not args.csv_out:
            sys.argv += ["--csv-out", join(args.out_dir, f"del_mets_{stamp}.csv")]
        if not args.jpeg_csv_out:
            sys.argv += ["--jpeg-csv-out", join(args.out_dir, f"del_jpeg_{stamp}.csv")]
    pm.main()


if __name__ == "__main__":
    main()
