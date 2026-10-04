#!/usr/bin/env python3
r"""presse_mets.py — Chaine presse ancienne : TIFF -> JPEG de diffusion, et un
METS par fascicule (PRA_XXX_AAAAMMJJ[...]) decrivant ses cinq types de fichiers.

Entrees :
- un CSV extrait du ref (memes colonnes ; .csv ou .csv.gz) portant TOUTES les
  lignes des fascicules a traiter : il donne, par page, la s3_key, la taille et
  le MD5 du TIFF, de l'ALTO (.xml), de l'OCR texte (.txt) et du PDF. Seules les
  lignes PRA_* pourvues d'une s3_key et hors « A SUPPRIMER » sont retenues.
  Fascicule et page sont lus dans la s3_key, qui fait foi quand elle differe du
  nom (PRA_JRX_19420431 -> 19420430) ;
- --source : le prefixe sous lequel se resolvent les chemins du ref ; chaque TIFF
  est lu sous <source>/<path>/<name>.

Pour chaque fascicule de l'extrait :
1. controle : tous ses TIFF sont presents sous <source> et leur MD5 est celui
   du ref (sinon le fascicule est refuse, rien n'est ecrit) ;
2. JPEG Q=80 a resolution native (jpeg_encode.py : load_master, un TIFF au
   decodage en erreur est refuse ; prepare_for_jpeg ; jpegsave_checked, trace
   XMP, ecriture sous .part, redecodage de controle puis renommage) depose sous
   <out-dir>/<repertoire de la s3_key>/<stem>.jpg (meme cle que le TIFF,
   extension .jpg, comme l'attendent les EAD corpus_ocr) ; un JPEG deja present
   est repris (sauf --overwrite) s'il passe le meme controle (redecodage,
   dimensions du TIFF) et porte la trace XMP a la qualite demandee, sinon le
   fascicule est en erreur. Chaque JPEG recoit a sa creation un uuid (32 hexa,
   comme au ref), inscrit dans son XMP (dc:identifier) : une reprise le relit
   au lieu d'en creer un autre ;
3. METS <out-dir>/<repertoire>/<fascicule>_mets.xml :
   - dmdSec : deux MODS minimaux, l'un pour le titre de presse (titre d'apres le
     set OAI RBX_<corpus>, code du corpus), l'autre pour le fascicule (langue,
     date d'emission tiree de l'identifiant, identifiant local, recordIdentifier
     = debut de s3_key commun aux fichiers du fascicule, ARK actuel et ancien
     s'ils sont dans l'EAD results/ead/corpus_ocr/RBX_<corpus>.xml) ; l'OBJID
     du METS est sa propre s3_key sans l'extension (.../<fascicule>_mets) ;
   - metsHdr : le METS recoit a sa creation un uuid (32 hexa, comme au ref),
     inscrit en altRecordID TYPE="uuid" ; un METS regenere reprend l'uuid du
     METS deja present sous <out-dir> au lieu d'en creer un autre ;
   - amdSec : un techMD MIX 2.0 par TIFF (relu dans le fichier : tags TIFF, sans
     decodage des pixels) et par JPEG (jpeg_to_mix.build_mix : uuid et trace
     XMP, sourceData = s3_key du TIFF, ce script et sa version git en
     ProcessingSoftware) ; pas de metadonnees techniques pour ALTO/TXT/PDF ;
   - fileSec : fileGrp master (TIFF), access (JPEG), alto, text, pdf ; MD5 et
     taille du ref (TIFF verifie, JPEG calcule) ; FLocat = s3_key complete ;
     OWNERID = uuid du fichier (celui du ref ; pour le JPEG, celui de sa trace
     XMP), repris aussi au MIX des TIFF et JPEG ;
   - structMap physique : issue > page (ORDER), un fptr par fichier de la page.

Le recapitulatif CSV (une ligne par fascicule : ok, erreur, refuse, invalide) est
ecrit au fil de l'eau, puis reecrit trie en fin de lot. Sa colonne
couleur_sans_profil compte les TIFF couleur sans profil ICC : leurs pixels passent
tels quels et le MIX du JPEG declare RGB, et non sRGB. Les colonnes
pages_incompletes et manques signalent les pages auxquelles il manque au ref un
TIFF, un ALTO, un texte ou un PDF (ex. « 003: alto, txt ; 007: pdf ») : le METS
est produit avec ce qui existe, le statut reste ok.

Un second recapitulatif (presse_jpeg_*.csv) donne une ligne par fichier a
deposer des fascicules au statut ok, JPEG (type jpeg) et METS (type mets) : name, path (relatif a <out-dir>), size, checksum_md5, uuid, s3_key. C'est
l'entree de presse_upload.py, qui envoie sur S3 les JPEG puis le METS.

Avec --upload (ENVOI REEL, pour une machine a peu d'espace disque), chaque
fascicule va au bout avant le suivant (cf. run_fascicule) : JPEG et METS,
validation, envoi sur S3 des JPEG puis du METS (presse_upload.send_one), controle
de chaque objet depose (taille et MD5), puis suppression des fichiers locaux
(--keep-mets garde les METS). Le disque ne porte donc que les fascicules en
cours (--workers). Rien n'est supprime si un envoi echoue (statut erreur_envoi,
fichiers laisses sur place) ; apres --max-echecs-envoi echecs, le lot s'arrete.
Une relance saute les fascicules dont le METS est deja sur S3 (statut
deja_envoye) : le METS part en dernier, sa presence vaut fascicule complet. Les
recapitulatifs sont alors la seule trace locale des uuid et MD5 des JPEG (colonnes
envoi et envoi_date en plus) : les conserver.

Durees : le recapitulatif par fascicule porte duree_s (fascicule entier, dans
son processus), le detail t_controle_s (MD5 des TIFF), t_conversion_s (lecture
du TIFF, JPEG et son controle), t_mets_s (METS et validation), t_envoi_s (envoi,
controle sur S3, suppression), et tiff_octets / jpeg_octets. Pendant le lot, une
ligne d'avancement (--avancement, 60 s) donne l'ecoule et le reste estime. En fin
de lot, le bilan donne la cadence (s/page, pages/h, Go de TIFF/h) et, avec
--estimer-pages N, la duree estimee pour N pages a cette cadence : elle ne vaut
que pour la meme machine, le meme --workers et les memes options.

Un fascicule qui echoue en cours de conversion ne laisse rien de la tentative : ses
JPEG deja ecrits sont supprimes (cf. process_fascicule). Un arret brutal du
traitement echappe a ce nettoyage : le METS fait foi, ne deposer que les
fascicules au statut ok.

Les suffixes d'edition du Journal de Roubaix (_M, _S, _ES) restent dans
l'identifiant, sans interpretation. Une plage AAAAMMJJ-JJ donne une date de debut
et de fin. PRA_BDR (pas de set OAI, ALTO/TXT/PDF sans s3_key) n'est pas couvert.

Usage :
  python presse_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie [--workers 8] [--validate]
  python presse_mets.py extrait_ref.csv --source /mnt/bnr --out-dir /chemin/sortie --validate --upload   # envoi reel
"""

import argparse
import csv
import hashlib
import os
import posixpath
import re
import subprocess
import sys
import time
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from os.path import basename, dirname, exists, getsize, join, relpath
from uuid import uuid4

import pandas as pd
from lxml import etree

from jpeg_encode import check_jpeg, jpegsave_checked, load_master, prepare_for_jpeg

# 1 seul thread libvips par processus : on parallelise au niveau des fascicules.
os.environ.setdefault("VIPS_CONCURRENCY", "1")

REF_DATE = "20260630"
ROOT = dirname(dirname(dirname(dirname(os.path.abspath(__file__)))))  # racine du depot
SETS_PATH = join(ROOT, "results", "oai", f"bnr_sets_{REF_DATE}.csv")
EAD_OCR_DIR = join(ROOT, "results", "ead", "corpus_ocr")
XSD_DIR = join(ROOT, "data", "xsd")  # copies locales des schemas loc.gov
QUALITY = 80
BUCKET = "mediatheque-patarch-communicable"  # celui de tous les fichiers presse du ref
T_PHASES = ("t_controle_s", "t_conversion_s", "t_mets_s", "t_envoi_s")
LIBELLES = {"invalide": "INVALIDE", "erreur_envoi": "ECHEC ENVOI"}
# titres corriges la ou le setName OAI s'ecarte du titre du journal
TITRES = {"PRA_AVE": "L’Avenir de Roubaix-Tourcoing"}
AGENT = "Médiathèque et Archives de Roubaix"
LANGUE = "fre"  # iso639-2b

NS = {
    "mets": "http://www.loc.gov/METS/",
    "mods": "http://www.loc.gov/mods/v3",
    "mix": "http://www.loc.gov/mix/v20",
    "xlink": "http://www.w3.org/1999/xlink",
    "xsi": "http://www.w3.org/2001/XMLSchema-instance",
}
SCHEMA_LOCATION = " ".join([
    NS["mets"], "http://www.loc.gov/standards/mets/mets.xsd",
    NS["mods"], "http://www.loc.gov/standards/mods/v3/mods-3-8.xsd",
    NS["mix"], "http://www.loc.gov/standards/mix/mix.xsd",
])

# extension -> (fileGrp USE, prefixe d'ID, MIMETYPE)
GROUPS = {
    "tif": ("master", "TIF", "image/tiff"),
    "jpg": ("access", "JPG", "image/jpeg"),
    "xml": ("alto", "ALTO", "text/xml"),
    "txt": ("text", "TXT", "text/plain"),
    "pdf": ("pdf", "PDF", "application/pdf"),
}
REF_EXT = {"tif": "tif", "tiff": "tif", "xml": "xml", "txt": "txt", "pdf": "pdf"}

# nom de fichier : <fascicule>_<page>.<ext> (espace parasite toleree avant l'extension)
PAGE_RE = re.compile(r"^(?P<fasc>PRA_[A-Z]+_\S+?)_(?P<page>\d+)\s*\.(?P<ext>\w+)$")
DATE_RE = re.compile(r"^PRA_[A-Z]+_(\d{4})(\d{2})(\d{2})(?:-(\d{2}))?")
UUID_RE = re.compile(r"[0-9a-f]{32}")
# terme de forme RAMEAU (data.bnf.fr, vocabulaire rameau/form, notice FRBNF11933071)
GENRE = "Périodique"
GENRE_URI = "http://data.bnf.fr/ark:/12148/cb11933071q"

PHOTOMETRIC = {0: "WhiteIsZero", 1: "BlackIsZero", 2: "RGB", 3: "PaletteColor",
               4: "TransparencyMask", 5: "CMYK", 6: "YCbCr", 8: "CIELab"}
COMPRESSION = {1: "Uncompressed", 2: "CCITT 1D", 3: "CCITT Group 3", 4: "CCITT Group 4",
               5: "LZW", 7: "JPEG", 8: "Deflate", 32773: "PackBits", 32946: "Deflate"}
RES_UNIT = {1: "no absolute unit of measurement", 2: "in.", 3: "cm"}
MOIS = ["janvier", "février", "mars", "avril", "mai", "juin",
        "juillet", "août", "septembre", "octobre", "novembre", "décembre"]


def q(prefix, tag):
    return f"{{{NS[prefix]}}}{tag}"


def sub(parent, prefix, tag, text=None, **attrs):
    el = etree.SubElement(parent, q(prefix, tag), {k: str(v) for k, v in attrs.items()})
    if text is not None:
        el.text = str(text)
    return el


def script_version() -> str:
    """Commit git de la chaine (suffixe -modifie si un .py de ce dossier, suivi
    ou non, differe du commit : l'encodage et le MIX sont dans des modules voisins)."""
    here = dirname(os.path.abspath(__file__))
    try:
        rev = subprocess.run(["git", "-C", here, "rev-parse", "--short", "HEAD"],
                             capture_output=True, text=True, check=True).stdout.strip()
        dirty = subprocess.run(["git", "-C", here, "status", "--porcelain", "--untracked-files=all",
                                "--", "*.py"], capture_output=True, text=True).stdout.strip()
        return rev + ("-modifie" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "inconnue"


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


# --------------------------------------------------------------------------- MIX


def tiff_mix(path: str, size: int, uuid: str, ref_created: str = ""):
    """MIX 2.0 du TIFF master, d'apres ses tags (pixels non decodes). Sans tag
    DateTime, dateTimeCreated est repris du ref (mix_dateTimeCreated)."""
    from PIL import Image, ImageCms

    with open(path, "rb") as f:
        byte_order = {b"II": "little endian", b"MM": "big endian"}.get(f.read(2))
    with Image.open(path) as im:
        tags = dict(im.tag_v2)
    root = etree.Element(q("mix", "mix"), nsmap={"mix": NS["mix"]})

    bdoi = sub(root, "mix", "BasicDigitalObjectInformation")
    if uuid:
        oid = sub(bdoi, "mix", "ObjectIdentifier")
        sub(oid, "mix", "objectIdentifierType", "uuid")
        sub(oid, "mix", "objectIdentifierValue", uuid)
    sub(bdoi, "mix", "fileSize", size)
    sub(sub(bdoi, "mix", "FormatDesignation"), "mix", "formatName", "image/tiff")
    if byte_order:
        sub(bdoi, "mix", "byteOrder", byte_order)
    comp = tags.get(259, 1)
    sub(sub(bdoi, "mix", "Compression"), "mix", "compressionScheme", COMPRESSION.get(comp, str(comp)))

    bic = sub(sub(root, "mix", "BasicImageInformation"), "mix", "BasicImageCharacteristics")
    sub(bic, "mix", "imageWidth", tags[256])
    sub(bic, "mix", "imageHeight", tags[257])
    photo = sub(bic, "mix", "PhotometricInterpretation")
    if 262 in tags:
        sub(photo, "mix", "colorSpace", PHOTOMETRIC.get(tags[262], str(tags[262])))
    if 34675 in tags:  # profil ICC embarque
        try:
            from io import BytesIO
            desc = ImageCms.getProfileDescription(ImageCms.ImageCmsProfile(BytesIO(tags[34675]))).strip()
            sub(sub(sub(photo, "mix", "ColorProfile"), "mix", "IccProfile"), "mix", "iccProfileName", desc)
        except (OSError, ImageCms.PyCMSError):
            pass

    # Z39.87 : DateTime (306) -> dateTimeCreated, Artist (315) -> imageProducer,
    # Make/Model (271/272) -> scanner, Software (305) -> scanningSoftwareName
    capture = etree.Element(q("mix", "ImageCaptureMetadata"))
    general = etree.Element(q("mix", "GeneralCaptureInformation"))
    m = re.match(r"^(\d{4}):(\d{2}):(\d{2}) (\d{2}:\d{2}:\d{2})$", str(tags.get(306, "")).strip())
    if not m:  # ref : « 2011:05:04 12:42:30+02:00 »
        m = re.match(r"^(\d{4}):(\d{2}):(\d{2}) (\d{2}:\d{2}:\d{2}(?:[+-]\d{2}:\d{2}|Z)?)$", ref_created.strip())
    if m:
        sub(general, "mix", "dateTimeCreated", f"{m[1]}-{m[2]}-{m[3]}T{m[4]}")
    if str(tags.get(315, "")).strip():
        sub(general, "mix", "imageProducer", str(tags[315]).strip())
    scanner = etree.Element(q("mix", "ScannerCapture"))
    if str(tags.get(271, "")).strip():
        sub(scanner, "mix", "scannerManufacturer", str(tags[271]).strip())
    if str(tags.get(272, "")).strip():
        sub(sub(scanner, "mix", "ScannerModel"), "mix", "scannerModelName", str(tags[272]).strip())
    if str(tags.get(305, "")).strip():
        sub(sub(scanner, "mix", "ScanningSystemSoftware"), "mix", "scanningSoftwareName",
            str(tags[305]).strip())
    for part in (general, scanner):  # sequence MIX : General avant Scanner
        if len(part):
            capture.append(part)
    if len(capture):
        root.append(capture)

    assess = sub(root, "mix", "ImageAssessmentMetadata")
    if 282 in tags and 283 in tags:
        spatial = sub(assess, "mix", "SpatialMetrics")
        sub(spatial, "mix", "samplingFrequencyUnit", RES_UNIT.get(tags.get(296, 2), "in."))
        for tag, name in ((282, "xSamplingFrequency"), (283, "ySamplingFrequency")):
            r = sub(spatial, "mix", name)
            sub(r, "mix", "numerator", tags[tag].numerator)
            sub(r, "mix", "denominator", tags[tag].denominator)
    enc = sub(assess, "mix", "ImageColorEncoding")
    bps = sub(enc, "mix", "BitsPerSample")
    bits = tags.get(258, (1,))
    for b in (bits if isinstance(bits, tuple) else (bits,)):
        sub(bps, "mix", "bitsPerSampleValue", b)
    sub(bps, "mix", "bitsPerSampleUnit", "integer")
    sub(enc, "mix", "samplesPerPixel", tags.get(277, 1))
    return root


def jpeg_mix(path: str, source_data: str, software: tuple, quality: int):
    """MIX du JPEG d'apres sa trace XMP, qui doit porter la qualite demandee et
    l'uuid du JPEG."""
    from xml.etree import ElementTree as ET

    from jpeg_to_mix import build_mix, read_trace

    t = read_trace(path)
    if "quality" not in t:
        raise ValueError(f"JPEG sans trace XMP conforme, relancer avec --overwrite : {path}")
    if t["quality"] != quality:
        raise ValueError(f"JPEG a Q={t['quality']} alors que Q={quality} est demande, "
                         f"relancer avec --overwrite : {path}")
    if not t["uuid"]:
        raise ValueError(f"JPEG sans uuid dans sa trace XMP, relancer avec --overwrite : {path}")
    tree = build_mix(t, source_data=source_data, extra_software=[software])
    # sans les blancs d'ET.indent, pour que pretty_print reindente dans le METS
    return etree.fromstring(ET.tostring(tree.getroot()), etree.XMLParser(remove_blank_text=True))


# -------------------------------------------------------------------------- METS


def mods_title(corpus: str, title: str):
    """MODS du titre de presse (commun a tous ses fascicules)."""
    root = etree.Element(q("mods", "mods"), nsmap={"mods": NS["mods"]})
    sub(sub(root, "mods", "titleInfo"), "mods", "title", title)
    sub(root, "mods", "typeOfResource", "text")
    sub(root, "mods", "genre", GENRE, authority="rameau", valueURI=GENRE_URI)
    sub(root, "mods", "identifier", corpus, type="local")
    return root


def mods(fasc: str, title: str, arks: dict, n_pages: int, prefix: str):
    """MODS du fascicule. prefix : debut de s3_key commun a ses fichiers."""
    root = etree.Element(q("mods", "mods"), nsmap={"mods": NS["mods"]})
    sub(sub(root, "mods", "titleInfo"), "mods", "title", title)
    sub(root, "mods", "typeOfResource", "text")
    sub(root, "mods", "genre", GENRE, authority="rameau", valueURI=GENRE_URI)
    sub(sub(root, "mods", "language"), "mods", "languageTerm", LANGUE, type="code", authority="iso639-2b")
    m = DATE_RE.match(fasc)
    if m:
        origin = sub(root, "mods", "originInfo")
        start = f"{m[1]}-{m[2]}-{m[3]}"
        if m[4]:
            sub(origin, "mods", "dateIssued", start, encoding="w3cdtf", keyDate="yes", point="start")
            sub(origin, "mods", "dateIssued", f"{m[1]}-{m[2]}-{m[4]}", encoding="w3cdtf", point="end")
        else:
            sub(origin, "mods", "dateIssued", start, encoding="w3cdtf", keyDate="yes")
    sub(sub(root, "mods", "physicalDescription"), "mods", "extent", f"{n_pages} p.")
    sub(root, "mods", "identifier", fasc, type="local")
    for role in ("current", "previous"):  # roles publication:* de l'EAD
        if arks.get(role):
            sub(root, "mods", "identifier", arks[role], type="uri", displayLabel=role)
    sub(sub(root, "mods", "recordInfo"), "mods", "recordIdentifier", prefix, source="s3_key")
    return root


def read_mets_uuid(path: str) -> str:
    """uuid d'un METS deja ecrit (metsHdr/altRecordID TYPE="uuid") ; '' si absent ou illisible."""
    try:
        found = etree.parse(path).findtext("mets:metsHdr/mets:altRecordID[@TYPE='uuid']", namespaces=NS)
    except (OSError, etree.XMLSyntaxError):
        return ""
    return found.strip() if found and UUID_RE.fullmatch(found.strip()) else ""


def build_mets(job: dict, pages: dict, version: str, uuid: str):
    """pages : {page: {ext: {key, size, md5, admid?, created?}}} ; uuid : celui du METS"""
    fasc = job["fasc"]
    mets = etree.Element(q("mets", "mets"), nsmap={k: NS[k] for k in ("mets", "mods", "mix", "xlink", "xsi")})
    mets.set(q("xsi", "schemaLocation"), SCHEMA_LOCATION)
    mets.set("OBJID", f"{job['prefix']}_mets")  # s3_key du METS sans l'extension
    m = DATE_RE.match(fasc)
    date = f"{int(m[3])}{'-' + str(int(m[4])) if m[4] else ''} {MOIS[int(m[2]) - 1]} {m[1]}" if m else fasc
    mets.set("LABEL", f"{job['title']}, {date}")

    hdr = sub(mets, "mets", "metsHdr", CREATEDATE=datetime.now().astimezone().isoformat(timespec="seconds"))
    sub(sub(hdr, "mets", "agent", ROLE="CUSTODIAN", TYPE="ORGANIZATION"), "mets", "name", AGENT)
    agent = sub(hdr, "mets", "agent", ROLE="CREATOR", TYPE="OTHER", OTHERTYPE="SOFTWARE")
    sub(agent, "mets", "name", basename(__file__))
    sub(agent, "mets", "note", f"version {version}")
    sub(hdr, "mets", "altRecordID", uuid, TYPE="uuid")

    dmd = sub(mets, "mets", "dmdSec", ID="DMD_TITLE")
    sub(sub(dmd, "mets", "mdWrap", MDTYPE="MODS", LABEL="Titre de presse"), "mets", "xmlData").append(
        mods_title(job["corpus"], job["title"]))
    dmd = sub(mets, "mets", "dmdSec", ID="DMD_ISSUE")
    sub(sub(dmd, "mets", "mdWrap", MDTYPE="MODS"), "mets", "xmlData").append(
        mods(fasc, job["title"], job["arks"], len(pages), job["prefix"]))

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
        for page in sorted(pages):
            f = pages[page].get(ext)
            if not f:
                continue
            if grp is None:
                grp = sub(file_sec, "mets", "fileGrp", USE=use)
            attrs = {"ID": f"{prefix}_{page}", "MIMETYPE": mime, "SEQ": int(page), "SIZE": f["size"],
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
    issue = sub(smap, "mets", "div", TYPE="issue", DMDID="DMD_TITLE DMD_ISSUE", LABEL=fasc)
    for order, page in enumerate(sorted(pages), 1):
        div = sub(issue, "mets", "div", ID=f"PAGE_{page}", TYPE="page", ORDER=order, ORDERLABEL=int(page))
        for ext, (_, prefix, _) in GROUPS.items():
            if ext in pages[page]:
                sub(div, "mets", "fptr", FILEID=f"{prefix}_{page}")
    return etree.ElementTree(mets)


# -------------------------------------------------------------------- validation


class _LocalXsd(etree.Resolver):
    """Sert les schemas importes (xlink.xsd, xml.xsd) depuis XSD_DIR, sans reseau."""

    def resolve(self, url, pubid, context):
        local = join(XSD_DIR, basename(url))
        if exists(local):
            return self.resolve_filename(local, context)


def load_schema():
    """Schema unique METS + MODS + MIX : les declarations MODS et MIX etant
    connues, le contenu des xmlData (processContents lax) est valide lui aussi."""
    imports = "".join(f'<xs:import namespace="{NS[p]}" schemaLocation="{join(XSD_DIR, f)}"/>'
                      for p, f in (("mets", "mets.xsd"), ("mods", "mods-3-8.xsd"), ("mix", "mix.xsd")))
    parser = etree.XMLParser()
    parser.resolvers.add(_LocalXsd())
    wrapper = f'<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">{imports}</xs:schema>'
    return etree.XMLSchema(etree.fromstring(wrapper, parser))


def validation_errors(schema, path: str) -> str:
    if schema.validate(etree.parse(path)):
        return ""
    return " | ".join(f"l.{e.line} {e.message}" for e in list(schema.error_log)[:5])


# ---------------------------------------------------------------------- travail


def incomplete_pages(pages: dict) -> dict:
    """{page: [types absents du ref]} parmi tif, alto, txt, pdf (le JPEG, lui, est
    produit ici). Le METS decrit ce qui existe ; ces pages sont signalees au
    recapitulatif."""
    out = {}
    for page in sorted(pages):
        missing = [GROUPS[ext][1].lower() for ext in ("tif", "xml", "txt", "pdf") if ext not in pages[page]]
        if missing:
            out[page] = missing
    return out


def process_fascicule(job: dict, out_dir: str, quality: int, overwrite: bool, version: str) -> dict:
    """Controle, conversion et METS d'un fascicule. Rien n'est ecrit si un TIFF
    manque ou differe du ref. Si le fascicule echoue ensuite, les JPEG ecrits
    pendant la tentative sont supprimes, ainsi que le METS d'un lot precedent
    (il ne decrit plus les JPEG presents) ; les JPEG deja la avant sont laisses."""
    ecrits = []  # JPEG ecrits pendant cette tentative
    mets_path = join(out_dir, *f"{job['prefix']}_mets.xml".split("/"))
    incompletes = incomplete_pages(job["pages"])
    res = {"fascicule": job["fasc"], "corpus_code": job["corpus"], "pages": len(job["pages"]),
           "jpeg_crees": 0, "couleur_sans_profil": 0, "pages_incompletes": len(incompletes),
           "manques": " ; ".join(f"{p}: {', '.join(m)}" for p, m in incompletes.items()),
           "mets": "", "status": "ok", "msg": "", "jpegs": [],
           "tiff_octets": sum(f["tif"]["size"] for f in job["pages"].values() if "tif" in f), "jpeg_octets": 0,
           "t_controle_s": 0.0, "t_conversion_s": 0.0, "t_mets_s": 0.0, "t_envoi_s": 0.0}
    t = time.perf_counter()
    try:
        manquants = [p for p, f in job["pages"].items() if "tif" in f and not exists(f["tif"]["local"])]
        if manquants:
            raise ValueError(f"TIFF absents sous la source pour les pages {', '.join(sorted(manquants))} "
                             f"(ex. {job['pages'][sorted(manquants)[0]]['tif']['local']})")
        for page, files in job["pages"].items():
            tif = files.get("tif")
            if tif and md5_file(tif["local"]) != tif["md5"]:
                raise ValueError(f"MD5 du TIFF local different du ref : {tif['local']}")

        res["t_controle_s"], t = time.perf_counter() - t, time.perf_counter()
        software = (basename(__file__), version)
        for page in sorted(job["pages"]):
            files = job["pages"][page]
            tif = files.get("tif")
            if not tif:
                continue
            tif["admid"] = f"TECH_TIF_{page}"
            tif["mix"] = tiff_mix(tif["local"], tif["size"], tif["uuid"], tif["created_ref"])

            jpg_key = posixpath.splitext(tif["key"])[0] + ".jpg"
            dst = join(out_dir, *jpg_key.split("/"))
            image = load_master(tif["local"])
            if image.bands >= 3 and not image.get_typeof("icc-profile-data"):
                res["couleur_sans_profil"] += 1  # pas de conversion possible, MIX du JPEG : RGB
            if overwrite or not exists(dst):
                os.makedirs(dirname(dst), exist_ok=True)
                image, steps = prepare_for_jpeg(image)
                jpegsave_checked(image, dst, quality, steps, uuid4().hex)
                ecrits.append(dst)
                res["jpeg_crees"] += 1
            else:  # JPEG repris : meme controle qu'un JPEG neuf
                if exists(dst + ".part"):  # reste d'un lot --overwrite interrompu brutalement
                    os.remove(dst + ".part")
                try:
                    check_jpeg(dst, image.width, image.height)
                except Exception as e:
                    raise ValueError(f"JPEG deja present invalide, relancer avec --overwrite : {dst} "
                                     f"({str(e).strip().splitlines()[-1].strip()})") from e
            mix = jpeg_mix(dst, tif["key"], software, quality)
            created = mix.findtext(".//mix:dateTimeProcessed", namespaces=NS)
            files["jpg"] = {"key": jpg_key, "size": getsize(dst), "md5": md5_file(dst),
                            "admid": f"TECH_JPG_{page}", "mix": mix, "created": created,
                            "uuid": mix.findtext(".//mix:objectIdentifierValue", namespaces=NS)}
            res["jpegs"].append({
                "fascicule": job["fasc"], "corpus_code": job["corpus"], "type": "jpeg",
                "name": posixpath.basename(jpg_key), "path": posixpath.dirname(jpg_key),
                "size": files["jpg"]["size"], "checksum_md5": files["jpg"]["md5"],
                "uuid": files["jpg"]["uuid"], "s3_key": jpg_key})

        res["jpeg_octets"] = sum(j["size"] for j in res["jpegs"])
        res["t_conversion_s"], t = time.perf_counter() - t, time.perf_counter()
        # le METS est reecrit a chaque lot : il garde l'uuid de sa premiere creation
        tree = build_mets(job, job["pages"], version, read_mets_uuid(mets_path) or uuid4().hex)
        os.makedirs(dirname(mets_path), exist_ok=True)
        tree.write(mets_path + ".part", encoding="utf-8", xml_declaration=True, pretty_print=True)
        os.replace(mets_path + ".part", mets_path)
        res["mets"] = mets_path
        res["t_mets_s"] = time.perf_counter() - t
    except Exception as e:  # erreur isolee par fascicule
        res.update(status="erreur", msg=f"{e}\n{traceback.format_exc()}" if not isinstance(e, ValueError) else str(e),
                   jpegs=[])
        if ecrits:  # ne rien laisser d'un fascicule incomplet
            supprimes, restes = [], []
            for path in [*ecrits, mets_path, mets_path + ".part"]:
                if exists(path):
                    try:
                        os.remove(path)
                        supprimes.append(path)
                    except OSError:
                        restes.append(path)
            bilan = [f"{sum(p in supprimes for p in ecrits)} JPEG de cette tentative supprimes"]
            if mets_path in supprimes:
                bilan.append("METS du lot precedent supprime")
            if restes:
                bilan.append("suppression impossible : " + ", ".join(restes))
            res["jpeg_crees"] = 0
            res["msg"] += f" [nettoyage : {' ; '.join(bilan)}]"
    return res


_SCHEMA = None  # par processus de travail : schema XSD et client S3, crees au premier besoin
_CLIENT = None


def remove_sent(paths: list, out_dir: str):
    """Supprime les fichiers envoyes et leurs repertoires devenus vides (sous out_dir)."""
    for path in paths:
        os.remove(path)
    for folder in {dirname(p) for p in paths}:
        while folder != out_dir and folder.startswith(out_dir):
            try:
                os.rmdir(folder)  # echoue si non vide (autre fascicule en cours) : on s'arrete la
            except OSError:
                break
            folder = dirname(folder)


def run_fascicule(job: dict, out_dir: str, quality: int, overwrite: bool, version: str, validate: bool,
                  upload: dict) -> dict:
    """Un fascicule de bout en bout : conversion et METS (process_fascicule),
    validation XSD, puis, si upload (bucket, keep_mets) est fourni, envoi sur S3
    des JPEG puis du METS, controle des objets deposes et suppression des
    fichiers locaux. Le METS part en dernier : sa presence sur S3 signifie que
    le fascicule y est complet, et une relance le saute (statut deja_envoye)."""
    global _SCHEMA, _CLIENT
    t0 = time.perf_counter()
    res = _run_fascicule(job, out_dir, quality, overwrite, version, validate, upload)
    res["duree_s"] = time.perf_counter() - t0
    for k in ("duree_s", *T_PHASES):
        if k in res:
            res[k] = round(res[k], 2)
    return res


def _run_fascicule(job: dict, out_dir: str, quality: int, overwrite: bool, version: str, validate: bool,
                   upload: dict) -> dict:
    global _SCHEMA, _CLIENT
    mets_key = f"{job['prefix']}_mets.xml"
    if upload:
        import presse_upload

        try:
            if _CLIENT is None:
                _CLIENT = presse_upload.make_client()
            if not overwrite and presse_upload.remote_exists(_CLIENT, upload["bucket"], mets_key):
                return {"fascicule": job["fasc"], "corpus_code": job["corpus"], "pages": len(job["pages"]),
                        "status": "deja_envoye", "mets": mets_key, "msg": "METS deja sur S3", "jpegs": []}
        except Exception as e:  # S3 injoignable : ne rien convertir
            return {"fascicule": job["fasc"], "corpus_code": job["corpus"], "pages": len(job["pages"]),
                    "status": "erreur_envoi", "msg": f"S3 : {e}", "jpegs": []}

    res = process_fascicule(job, out_dir, quality, overwrite, version)
    if validate and res["status"] == "ok":
        t = time.perf_counter()
        if _SCHEMA is None:
            _SCHEMA = load_schema()
        msg = validation_errors(_SCHEMA, res["mets"])
        res["t_mets_s"] += time.perf_counter() - t
        if msg:
            res.update(status="invalide", msg=msg, jpegs=[])
    if res["status"] != "ok":
        return res
    res["jpegs"].append(mets_row(res, out_dir))
    if not upload:
        return res

    t = time.perf_counter()
    for row in res["jpegs"]:  # JPEG d'abord, METS en dernier ; arret au premier echec
        sent = presse_upload.send_one(row, out_dir, _CLIENT, upload["bucket"], overwrite)
        row.update(envoi=sent["status"], envoi_date=sent["uploaded_file_lastmodified"])
        if sent["status"] not in ("envoye", "deja_present"):
            res["t_envoi_s"] = time.perf_counter() - t
            res.update(status="erreur_envoi", jpegs=[],
                       msg=f"{sent['key']} : {sent['error']} [fichiers du fascicule laisses sous {out_dir}]")
            return res
    # tout le fascicule est sur S3, taille et MD5 controles : les fichiers locaux peuvent partir
    locaux = [join(out_dir, *r["s3_key"].split("/")) for r in res["jpegs"]
              if r["type"] == "jpeg" or not upload["keep_mets"]]
    try:
        remove_sent(locaux, out_dir)
    except OSError as e:
        res["msg"] = f"envoye, mais suppression locale impossible : {e}"
    if not upload["keep_mets"]:
        res["mets"] = mets_key
    res["t_envoi_s"] = time.perf_counter() - t  # envoi, controle sur S3 et suppression locale
    return res


def hms(seconds: float) -> str:
    """Duree lisible : 45 s, 12 min 05 s, 3 h 20 min, 4 j 07 h."""
    s = int(round(seconds))
    if s < 60:
        return f"{s} s"
    if s < 3600:
        return f"{s // 60} min {s % 60:02d} s"
    if s < 86400:
        return f"{s // 3600} h {s % 3600 // 60:02d} min"
    return f"{s // 86400} j {s % 86400 // 3600:02d} h"


def bilan_durees(rows: list, elapsed: float, workers: int, total_pages: int) -> str:
    """Bilan des durees du lot et cadence, d'apres les fascicules au statut ok ;
    avec total_pages, duree estimee pour ce nombre de pages a la meme cadence
    (meme machine, meme --workers, memes options)."""
    ok = [r for r in rows if r["status"] == "ok"]
    pages = sum(r["pages"] for r in ok)
    if not pages:
        return "Durees : aucun fascicule au statut ok, pas de cadence mesuree."
    go = sum(r["tiff_octets"] for r in ok) / 1e9
    travail = sum(r["duree_s"] for r in ok)
    lines = [f"Durees : {hms(elapsed)} pour {len(ok)} fascicule(s), {pages} page(s), {go:.2f} Go de TIFF, "
             f"{sum(r['jpeg_octets'] for r in ok) / 1e9:.2f} Go de JPEG ({workers} processus).",
             f"  cadence du lot : {elapsed / pages:.2f} s/page, {pages / elapsed * 3600:.0f} pages/h, "
             f"{go / elapsed * 3600:.1f} Go de TIFF/h",
             f"  travail par page (un processus) : {travail / pages:.2f} s, dont "
             + ", ".join(f"{lib} {sum(r[k] for r in ok) / pages:.2f} s"
                         for k, lib in zip(T_PHASES, ("controle MD5 du TIFF", "conversion", "METS", "envoi S3")))]
    if total_pages:
        lines.append(f"  estimation pour {total_pages} pages a cette cadence : {hms(elapsed / pages * total_pages)}")
    return "\n".join(lines)


def mets_row(res: dict, out_dir: str) -> dict:
    """Ligne du METS d'un fascicule ok, au recapitulatif a deposer (uuid relu dans le METS)."""
    rel = relpath(res["mets"], out_dir).replace(os.sep, "/")
    return {"fascicule": res["fascicule"], "corpus_code": res["corpus_code"], "type": "mets",
            "name": posixpath.basename(rel), "path": posixpath.dirname(rel), "size": getsize(res["mets"]),
            "checksum_md5": md5_file(res["mets"]), "uuid": read_mets_uuid(res["mets"]), "s3_key": rel}


def load_extract(path: str) -> pd.DataFrame:
    """Extrait du ref (.csv ou .csv.gz) : lignes PRA_* pourvues d'une s3_key, hors
    « A SUPPRIMER »."""
    cols = ["name", "path", "s3_key", "size", "checksum_md5", "uuid", "corpus_code", "conservation_statut",
            "mix_dateTimeCreated"]
    df = pd.read_csv(path, dtype=str, usecols=cols, low_memory=False)
    df = df[df.corpus_code.str.startswith("PRA_", na=False) & df.s3_key.notna()]
    return df[~df.conservation_statut.str.startswith("À SUPPRIMER", na=False)]


def load_titles(path: str) -> dict:
    sets = pd.read_csv(path, dtype=str)
    titles = {s.removeprefix("RBX_"): n for s, n in zip(sets.setSpec, sets.setName)}
    return {**titles, **{c: n for c, n in TITRES.items() if c in titles}}


def load_arks(corpus: str) -> dict:
    """unitid -> {current, previous} : ARK actuel et ancien du fascicule, d'apres
    les roles publication:* de l'EAD corpus_ocr du titre."""
    path = join(EAD_OCR_DIR, f"RBX_{corpus}.xml")
    arks = {}
    if exists(path):
        for c in etree.parse(path).iter("c"):
            unitid = c.findtext("did/unitid")
            found = {role: c.xpath(f'string((dao|daogrp/daoloc)[@role="publication:{role}"]/@href)')
                     for role in ("current", "previous")}
            if unitid and any(found.values()):
                arks[unitid] = found
    return arks


def build_jobs(ref: pd.DataFrame, source: str, titles: dict):
    """Un job par fascicule de l'extrait ; ses TIFF sont attendus sous
    <source>/<path>/<name> (chemin du ref)."""
    fascs = defaultdict(lambda: defaultdict(dict))
    corpus_of = {}
    anomalies = []
    for row in ref.itertuples(index=False):
        m = PAGE_RE.match(posixpath.basename(row.s3_key))  # la cle fait foi, pas le nom
        ext = REF_EXT.get(m["ext"].lower()) if m else None
        if not ext:
            continue
        page = fascs[m["fasc"]][m["page"]]
        if ext in page:
            anomalies.append(f"{m['fasc']} page {m['page']} : plusieurs .{ext} au ref")
        page[ext] = {"key": row.s3_key, "size": int(float(row.size)), "md5": row.checksum_md5,
                     "uuid": row.uuid if isinstance(row.uuid, str) else ""}
        if ext == "tif":
            rel = row.path.split("/") if isinstance(row.path, str) else []
            page[ext]["local"] = join(source, *rel, row.name)
            page[ext]["created_ref"] = row.mix_dateTimeCreated if isinstance(row.mix_dateTimeCreated, str) else ""
        corpus_of[m["fasc"]] = row.corpus_code

    jobs, refused, arks = [], [], {}
    for fasc, pages in fascs.items():
        corpus = corpus_of[fasc]
        if not any("tif" in f for f in pages.values()):
            refused.append((fasc, corpus, "aucun TIFF pour ce fascicule dans l'extrait"))
            continue
        if corpus not in titles:
            refused.append((fasc, corpus, f"titre inconnu pour {corpus} (pas de set OAI)"))
            continue
        if corpus not in arks:
            arks[corpus] = load_arks(corpus)
        some_key = next(f["key"] for p in pages.values() for f in p.values())
        jobs.append({"fasc": fasc, "corpus": corpus, "title": titles[corpus],
                     "arks": arks[corpus].get(fasc, {}), "pages": {p: dict(f) for p, f in pages.items()},
                     "prefix": f"{posixpath.dirname(some_key)}/{fasc}"})
    return jobs, refused, anomalies


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("extrait", help="CSV extrait du ref (.csv ou .csv.gz) : toutes les lignes des fascicules")
    ap.add_argument("--source", required=True,
                    help="prefixe des chemins du ref : un TIFF est lu sous <source>/<path>/<name>")
    ap.add_argument("--out-dir", required=True, help="racine des JPEG et METS (arborescence des s3_key)")
    ap.add_argument("--sets", default=SETS_PATH, help="sets OAI (titres des journaux)")
    ap.add_argument("--quality", type=int, default=QUALITY)
    ap.add_argument("--workers", type=int, default=os.cpu_count())
    ap.add_argument("--overwrite", action="store_true", help="regenerer les JPEG deja presents")
    ap.add_argument("--validate", action="store_true",
                    help="valider chaque METS contre les XSD METS/MODS/MIX de data/xsd")
    ap.add_argument("--upload", action="store_true",
                    help="ENVOI REEL : envoyer chaque fascicule sur S3 des qu'il est pret (JPEG puis METS), "
                         "controler les objets deposes, puis supprimer les fichiers locaux")
    ap.add_argument("--bucket", default=BUCKET)
    ap.add_argument("--keep-mets", action="store_true", help="avec --upload : garder les METS sous --out-dir")
    ap.add_argument("--max-echecs-envoi", type=int, default=5,
                    help="avec --upload : arreter le lot apres ce nombre de fascicules en echec d'envoi")
    ap.add_argument("--avancement", type=float, default=60,
                    help="secondes entre deux lignes d'avancement (ecoule, reste estime du lot)")
    ap.add_argument("--estimer-pages", type=int, default=0,
                    help="nombre total de pages a convertir : la duree en est estimee en fin de lot, "
                         "a la cadence mesuree")
    ap.add_argument("--csv-out", help="recapitulatif par fascicule "
                                      "(defaut : <out-dir>/presse_mets_AAAAMMJJHHMMSS.csv)")
    ap.add_argument("--jpeg-csv-out", help="recapitulatif par JPEG, entree de presse_upload.py "
                                           "(defaut : <out-dir>/presse_jpeg_AAAAMMJJHHMMSS.csv)")
    args = ap.parse_args()

    out_dir = os.path.abspath(args.out_dir)
    jobs, refused, anomalies = build_jobs(load_extract(args.extrait), args.source, load_titles(args.sets))
    for a in anomalies:
        print(f"ANOMALIE ref : {a}", file=sys.stderr)
    print(f"{len(jobs)} fascicule(s) a traiter, {len(refused)} refuse(s) d'emblee.")

    version = script_version()
    if args.validate:
        load_schema()  # echoue ici, avant le lot, si les XSD manquent
    upload = {"bucket": args.bucket, "keep_mets": args.keep_mets} if args.upload else None
    if upload:
        print(f"ENVOI REEL vers {args.bucket} : chaque fascicule est envoye, controle sur S3 puis supprime "
              f"de {args.out_dir}.")
    os.makedirs(args.out_dir, exist_ok=True)
    stamp = f"{datetime.now():%Y%m%d%H%M%S}"
    csv_out = args.csv_out or join(args.out_dir, f"presse_mets_{stamp}.csv")
    jpeg_csv_out = args.jpeg_csv_out or join(args.out_dir, f"presse_jpeg_{stamp}.csv")
    fields = ["fascicule", "corpus_code", "status", "pages", "jpeg_crees", "couleur_sans_profil",
              "pages_incompletes", "manques", "mets", "tiff_octets", "jpeg_octets", "duree_s", *T_PHASES, "msg"]
    jpeg_fields = ["fascicule", "corpus_code", "type", "name", "path", "size", "checksum_md5", "uuid", "s3_key"]
    if upload:
        jpeg_fields += ["envoi", "envoi_date"]
    jpeg_rows = []
    rows = [{"fascicule": f, "corpus_code": c, "status": "refuse", "msg": m} for f, c, m in refused]
    echecs_envoi = 0
    total = sum(len(j["pages"]) for j in jobs)  # pages du lot, pour l'avancement
    faites, dernier = 0, 0.0
    t0 = time.time()
    # recapitulatifs ecrits au fil de l'eau, a chaque fascicule termine : une
    # interruption du lot en laisse l'etat sur disque
    with open(csv_out, "w", newline="", encoding="utf-8") as f, \
            open(jpeg_csv_out, "w", newline="", encoding="utf-8") as fj:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)
        f.flush()
        wj = csv.DictWriter(fj, fieldnames=jpeg_fields)
        wj.writeheader()
        fj.flush()
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(run_fascicule, j, out_dir, args.quality, args.overwrite, version,
                                   args.validate, upload) for j in jobs]
            for fut in as_completed(futures):
                if fut.cancelled():
                    continue
                res = fut.result()
                jpegs = res.pop("jpegs")
                if res["status"] not in ("ok", "deja_envoye"):
                    print(f"{LIBELLES.get(res['status'], 'ECHEC')} {res['fascicule']} : {res['msg']}",
                          file=sys.stderr)
                rows.append(res)
                w.writerow(res)
                f.flush()
                faites += res["pages"]
                if time.time() - dernier >= args.avancement or faites == total:
                    dernier, ecoule = time.time(), time.time() - t0
                    print(f"[{datetime.now():%H:%M:%S}] {len(rows) - len(refused)}/{len(jobs)} fascicules, "
                          f"{faites}/{total} pages, ecoule {hms(ecoule)}, "
                          f"reste estime {hms(ecoule / faites * (total - faites))}", file=sys.stderr)
                if res["status"] == "ok":  # seuls les fascicules complets et valides sont a deposer
                    jpeg_rows.extend(jpegs)
                    wj.writerows(jpegs)
                    fj.flush()
                if res["status"] == "erreur_envoi":
                    echecs_envoi += 1
                    if echecs_envoi == args.max_echecs_envoi:  # ne pas remplir le disque si S3 ne repond plus
                        print(f"{echecs_envoi} fascicules en echec d'envoi : arret du lot, les fascicules "
                              f"non commences sont abandonnes.", file=sys.stderr)
                        for other in futures:
                            other.cancel()

    # fin de lot : memes recapitulatifs, tries
    for path, names, data, key in ((csv_out, fields, rows, lambda r: r["fascicule"]),
                                   (jpeg_csv_out, jpeg_fields, jpeg_rows, lambda r: r["s3_key"])):
        with open(path + ".part", "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=names)
            w.writeheader()
            w.writerows(sorted(data, key=key))
        os.replace(path + ".part", path)
    ok = sum(r["status"] == "ok" for r in rows)
    deja = sum(r["status"] == "deja_envoye" for r in rows)
    print(f"Termine : {ok} METS{' envoyes avec leurs JPEG' if upload else ''}, "
          + (f"{deja} fascicule(s) deja sur S3, " if deja else "")
          + f"{len(rows) - ok - deja} fascicule(s) en echec/refuse(s)/invalide(s), "
          f"{len(jobs) + len(refused) - len(rows)} abandonne(s), "
          f"en {time.time() - t0:.1f}s. Recapitulatif : {csv_out}")
    elapsed = time.time() - t0
    print(bilan_durees(rows, elapsed, args.workers, args.estimer_pages))
    print(f"{len(jpeg_rows)} fichiers (JPEG + METS) {'deposes' if upload else 'a deposer'}, decrits dans {jpeg_csv_out}")
    incomplets = sum(bool(r.get("pages_incompletes")) for r in rows)
    if incomplets:
        print(f"{incomplets} fascicule(s) avec des pages incompletes (colonnes pages_incompletes et manques).")
    if ok < len(rows):
        sys.exit(2)


if __name__ == "__main__":
    main()
