"""Génération d'instruments de recherche EAD pour les corpus OCR.

À partir de results/ref/_ref_files_<date>.csv.gz, génère un fichier EAD par
corpus_code dans results/ead/corpus_ocr/ :

- corpus de presse (PRA_*) : hiérarchie année (series) / mois (subseries) /
  numéro (file), un <c level="file"> par unitid (PRA_XXX_YYYYMMDD).
- corpus de registres (AMR_DEL, AMR_RAM) : dsc plat, un <c level="file"> par
  unitid (cote).

Chaque <c level="file"> contient un <daogrp> avec une <daoloc
role="preservation:image"> et une <daoloc role="preservation:ocr"> par page
(fichiers .tif et .xml ALTO), triées par numéro de page.

Usage
-----
    python scripts/ead/corpusocr2ead.py
"""

import re
from os.path import join

import pandas as pd
from lxml import etree

from mnesys_id import ids_existants, nouvel_id

REF_DATE = "20260502"

CORPUS_PRESSE = ["PRA_AVE", "PRA_CRT", "PRA_CTG", "PRA_ERT", "PRA_IND", "PRA_JRX", "PRA_RTG"]
CORPUS_REGISTRE = ["AMR_DEL", "AMR_RAM"]

MOIS = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]

PAGE_RE = re.compile(r"_(\d+)\.\w+$")
PRA_UNITID_RE = re.compile(r"_(\d{4})(\d{2})(\d{2})$")
DATE_RANGE_RE = re.compile(r"\((\d{4})(?:-(\d{4}))?\)")


def normaliser_unitid(unitid):
    """Normalise une cote : "_" -> " ", espaces multiples réduits à un seul."""
    return " ".join(str(unitid).replace("_", " ").split())


def charger_fichiers(corpus_codes):
    """Lignes .tif et ocr xml du ref pour les corpus demandés, dédupliquées."""
    df = pd.read_csv(
        join("results", "ref", f"_ref_files_{REF_DATE}.csv.gz"),
        usecols=["corpus_code", "unitid", "name", "extension", "path", "s3_key"],
        low_memory=False,
    )
    df = df[df["corpus_code"].isin(corpus_codes) & df["extension"].isin([".tif", ".xml"])]
    df = df.dropna(subset=["unitid"])

    registres = df["corpus_code"].isin(CORPUS_REGISTRE)
    df.loc[registres, "unitid"] = df.loc[registres, "unitid"].apply(normaliser_unitid)

    return df.drop_duplicates(subset=["corpus_code", "unitid", "name", "extension"])


def numero_page(name):
    """Numéro de page (entier) extrait de la fin du nom de fichier."""
    m = PAGE_RE.search(name)
    return int(m.group(1)) if m else 0


def href(row):
    """Chemin de la <daoloc> : s3_key si connu, sinon chemin local path/name."""
    if pd.notna(row["s3_key"]):
        return row["s3_key"]
    return f"{row['path']}/{row['name']}"


def role(extension):
    return "preservation:image" if extension == ".tif" else "preservation:ocr"


def ajouter_daogrp(c, fichiers):
    """Ajoute un <daogrp> à <c>, une <daoloc> par page (image puis ocr)."""
    daogrp = etree.SubElement(c, "daogrp")
    fichiers = fichiers.sort_values(
        by=["page", "extension"], key=lambda s: s if s.name != "extension" else s.map({".tif": 0, ".xml": 1})
    )
    for _, row in fichiers.iterrows():
        daoloc = etree.SubElement(daogrp, "daoloc")
        daoloc.set("href", href(row))
        daoloc.set("role", role(row["extension"]))


def nouvel_element_c(parent, level, deja_pris):
    c = etree.SubElement(parent, "c")
    c.set("id", nouvel_id(deja_pris))
    c.set("level", level)
    return c


def ajouter_did(c, unittitle, unitid=None, unitdate_normal=None, unitdate_texte=None):
    did = etree.SubElement(c, "did")
    etree.SubElement(did, "unittitle").text = unittitle
    if unitid is not None:
        etree.SubElement(did, "unitid").text = unitid
    if unitdate_normal is not None:
        unitdate = etree.SubElement(did, "unitdate")
        unitdate.set("normal", unitdate_normal)
        unitdate.text = unitdate_texte


def squelette_ead(eadid, titre):
    """Squelette d'un EAD : eadheader + archdesc/did, retourne (tree, dsc)."""
    ead = etree.Element("ead")
    ead.set("audience", "external")

    eadheader = etree.SubElement(ead, "eadheader")
    etree.SubElement(eadheader, "eadid").text = eadid
    filedesc = etree.SubElement(eadheader, "filedesc")
    titlestmt = etree.SubElement(filedesc, "titlestmt")
    etree.SubElement(titlestmt, "titleproper").text = titre
    publicationstmt = etree.SubElement(filedesc, "publicationstmt")
    etree.SubElement(publicationstmt, "publisher").text = "Bibliothèque numérique de Roubaix"
    profiledesc = etree.SubElement(eadheader, "profiledesc")
    etree.SubElement(profiledesc, "creation").text = (
        "Instrument de recherche généré automatiquement à partir des "
        "métadonnées de référencement (scripts/ead/corpusocr2ead.py)"
    )
    langusage = etree.SubElement(profiledesc, "langusage")
    langusage.text = "Instrument de recherche rédigé en "
    language = etree.SubElement(langusage, "language")
    language.set("langcode", "fre")
    language.text = "français"

    archdesc = etree.SubElement(ead, "archdesc")
    archdesc.set("level", "collection")
    did = etree.SubElement(archdesc, "did")
    etree.SubElement(did, "unittitle").text = titre
    unitid = etree.SubElement(did, "unitid")
    unitid.set("identifier", eadid)
    unitid.text = eadid

    dsc = etree.SubElement(archdesc, "dsc")
    dsc.set("type", "in-depth")

    tree = etree.ElementTree(ead)
    return tree, archdesc, dsc


def date_francaise(annee, mois, jour):
    return f"{jour} {MOIS[mois - 1]} {annee}"


def construire_ead_presse(corpus_code, df, eadid, titre, deja_pris):
    df = df.copy()
    df["page"] = df["name"].apply(numero_page)

    dates = df["unitid"].str.extract(PRA_UNITID_RE)
    df["annee"] = dates[0]
    df["mois"] = dates[1]
    df["jour"] = dates[2]
    df = df.dropna(subset=["annee", "mois", "jour"])

    tree, archdesc, dsc = squelette_ead(eadid, titre)
    archdesc.set("id", nouvel_id(deja_pris))

    for annee, df_annee in df.groupby("annee"):
        c_annee = nouvel_element_c(dsc, "series", deja_pris)
        ajouter_did(c_annee, annee, unitdate_normal=f"{annee}-01-01/{annee}-12-31", unitdate_texte=annee)

        for mois, df_mois in df_annee.groupby("mois"):
            c_mois = nouvel_element_c(c_annee, "subseries", deja_pris)
            titre_mois = f"{MOIS[int(mois) - 1]} {annee}"
            ajouter_did(
                c_mois, titre_mois,
                unitdate_normal=f"{annee}-{mois}",
                unitdate_texte=titre_mois,
            )

            for unitid, df_numero in df_mois.groupby("unitid"):
                jour = df_numero["jour"].iloc[0]
                c_numero = nouvel_element_c(c_mois, "file", deja_pris)
                ajouter_did(
                    c_numero,
                    date_francaise(int(annee), int(mois), int(jour)),
                    unitid=unitid,
                    unitdate_normal=f"{annee}-{mois}-{jour}",
                    unitdate_texte=date_francaise(int(annee), int(mois), int(jour)),
                )
                ajouter_daogrp(c_numero, df_numero)

    return tree


def numero_registre(unitid):
    """Premier nombre trouvé dans la cote, pour le tri (ex. '1 D 10' -> 10)."""
    m = re.search(r"\d+", unitid)
    return int(m.group()) if m else 0


def plage_dates(path):
    m = DATE_RANGE_RE.search(path)
    if not m:
        return None, None
    debut, fin = m.group(1), m.group(2) or m.group(1)
    return debut, fin


def construire_ead_registre(corpus_code, df, eadid, titre, deja_pris):
    df = df.copy()
    df["page"] = df["name"].apply(numero_page)

    tree, archdesc, dsc = squelette_ead(eadid, titre)
    archdesc.set("id", nouvel_id(deja_pris))

    unitids = sorted(df["unitid"].unique(), key=numero_registre)
    for unitid in unitids:
        df_registre = df[df["unitid"] == unitid]
        c = nouvel_element_c(dsc, "file", deja_pris)

        debut, fin = plage_dates(df_registre["path"].iloc[0])
        if debut:
            ajouter_did(
                c, unitid, unitid=unitid,
                unitdate_normal=f"{debut}-01-01/{fin}-12-31",
                unitdate_texte=debut if debut == fin else f"{debut}-{fin}",
            )
        else:
            ajouter_did(c, unitid, unitid=unitid)

        ajouter_daogrp(c, df_registre)

    return tree


def main():
    corpus_info = pd.read_excel(join("data", "corpus_liste", "bnr_corpus.xlsx"))
    corpus_info = corpus_info.set_index("corpus_code")
    corpus_info["collection_bnr"] = corpus_info["collection_bnr"].str.strip()
    corpus_info["corpus"] = corpus_info["corpus"].str.strip()

    corpus_codes = CORPUS_PRESSE + CORPUS_REGISTRE
    df = charger_fichiers(corpus_codes)
    deja_pris = ids_existants()

    for corpus_code in corpus_codes:
        eadid = corpus_info.loc[corpus_code, "collection_bnr"]
        titre = corpus_info.loc[corpus_code, "corpus"]
        df_corpus = df[df["corpus_code"] == corpus_code]

        if corpus_code in CORPUS_REGISTRE:
            tree = construire_ead_registre(corpus_code, df_corpus, eadid, titre, deja_pris)
        else:
            tree = construire_ead_presse(corpus_code, df_corpus, eadid, titre, deja_pris)

        out_path = join("results", "ead", "corpus_ocr", f"{eadid}.xml")
        tree.write(out_path, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        print(f"{corpus_code} -> {out_path}")


if __name__ == "__main__":
    main()
