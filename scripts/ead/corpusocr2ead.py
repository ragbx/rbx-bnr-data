"""Génération d'instruments de recherche EAD pour les corpus OCR.

À partir de results/ref/_ref_files_<date>.csv.gz, génère un fichier EAD par
corpus_code dans results/ead/corpus_ocr/ :

- corpus de presse (PRA_*) : hiérarchie année (series) / mois (subseries) /
  numéro (file), un <c level="file"> par unitid (PRA_XXX_YYYYMMDD).
- corpus de registres (AMR_DEL, AMR_RAM) : dsc plat, un <c level="file"> par
  unitid (cote).

Chaque <c level="file"> contient un <daogrp> avec, pour chaque rôle
(preservation/access x image/ocr), une <daoloc role="{rôle}:first"> et une
<daoloc role="{rôle}:last"> délimitant la plage de pages (une seule <daoloc
role="{rôle}"> si une seule page).

Les <c> et <archdesc> reçoivent un id (format Mnesys, préfixe "m0"), stable
d'une exécution à l'autre pour un même unitid grâce à la concordance
results/ead/ead_cor/concordance_id_corpusocr.csv (cf. generateur_id). Cet id
sert de base au lien ARK actuel (role="publication:current"), ajouté à chaque
<archdesc>/<c> ; l'ancien ARK (role="publication:previous") est ajouté aux <c>
dont l'unitid est connu de l'ancien référencement OAI (colonne osiros_id du
ref, cf. dict_osiros et ajouter_ark).

TODO : ajouter AMR_PUV, AMR_PVC, MED_PER ?

Usage
-----
    python scripts/ead/corpusocr2ead.py
"""

import re
from os.path import exists, join

import pandas as pd
from lxml import etree

from dao_ark import add_ark_links
from mnesys_id import ids_existants, nouvel_id

REF_DATE = "20260630"

CONCORDANCE_PATH = join("results", "ead", "ead_cor", "concordance_id_corpusocr.csv")

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
        usecols=["corpus_code", "unitid", "name", "extension", "path", "s3_key", "osiros_id"],
        low_memory=False,
    )
    df = df[df["corpus_code"].isin(corpus_codes) & df["extension"].isin([".tif", ".xml"])]
    df = df.dropna(subset=["unitid"])

    registres = df["corpus_code"].isin(CORPUS_REGISTRE)
    df.loc[registres, "unitid"] = df.loc[registres, "unitid"].apply(normaliser_unitid)

    return df.drop_duplicates(subset=["corpus_code", "unitid", "name", "extension"])


def dict_osiros(df):
    """Correspondance unitid -> osiros_id (ancien identifiant OAI, déjà préfixé
    "BNR"), pour les lignes où il est connu."""
    connus = df.dropna(subset=["osiros_id"])
    return dict(zip(connus["unitid"], connus["osiros_id"]))


def numero_page(name):
    """Numéro de page (entier) extrait de la fin du nom de fichier."""
    m = PAGE_RE.search(name)
    return int(m.group(1)) if m else 0


def href(row):
    """Chemin de la <daoloc> : s3_key si connu, sinon chemin local path/name."""
    if pd.notna(row["s3_key"]):
        return row["s3_key"]
    return f"{row['path']}/{row['name']}"


def href_jpg(row):
    """Chemin de la <daoloc> d'accès : href avec l'extension TIFF remplacée par .jpg."""
    return re.sub(r"\.tiff?$", ".jpg", href(row), flags=re.IGNORECASE)


def entrees_daoloc(fichiers):
    """Une entrée (rôle, page, href) par page et par rôle : conservation et accès,
    pour l'image (.tif) comme pour l'OCR (.xml)."""
    entrees = []
    for _, row in fichiers.iterrows():
        if row["extension"] == ".tif":
            entrees.append(("preservation:image", row["page"], href(row)))
            entrees.append(("access:image", row["page"], href_jpg(row)))
        elif row["extension"] == ".xml":
            entrees.append(("preservation:ocr", row["page"], href(row)))
            entrees.append(("access:ocr", row["page"], href(row)))
    return pd.DataFrame(entrees, columns=["role", "page", "href"])


def ajouter_daogrp(c, fichiers):
    """Ajoute un <daogrp> à <c> avec, pour chaque rôle (conservation/accès,
    image/ocr), une <daoloc role="{rôle}:first"> et une <daoloc role="{rôle}:last">
    délimitant la plage de pages (une seule <daoloc role="{rôle}"> si une seule
    page)."""
    daogrp = etree.SubElement(c, "daogrp")
    entrees = entrees_daoloc(fichiers)

    for role_nom, groupe in entrees.groupby("role", sort=False):
        groupe = groupe.sort_values("page")
        if len(groupe) == 1:
            paires = ((None, groupe.iloc[0]),)
        else:
            paires = (("first", groupe.iloc[0]), ("last", groupe.iloc[-1]))

        for suffixe, ligne in paires:
            daoloc = etree.SubElement(daogrp, "daoloc")
            daoloc.set("href", ligne["href"])
            daoloc.set("role", f"{role_nom}:{suffixe}" if suffixe else role_nom)


def charger_concordance(csv_path):
    """Charge la concordance ir / unitid / id des exécutions précédentes."""
    if not exists(csv_path):
        return []
    return pd.read_csv(csv_path, dtype=str).to_dict(orient="records")


def sauvegarder_concordance(concordance, csv_path):
    """Écrit la concordance ir / unitid / id, rechargée aux exécutions suivantes."""
    pd.DataFrame(concordance, columns=["ir", "unitid", "id"]).to_csv(csv_path, index=False)


def generateur_id(concordance, deja_pris, ir_id):
    """Générateur d'id (format Mnesys, préfixe "m0") pour les <c> et <archdesc> d'un
    ir : si la concordance contient une entrée pour (ir_id, unitid), l'id qu'elle
    contient est repris ; sinon un nouvel id est généré et consigné dans la
    concordance. Les unitid en doublon dans un même ir sont appariés dans l'ordre
    du document. Les appels sans unitid reçoivent un id nouveau à chaque fois (pas
    de clé de concordance) : c'est le cas des <c level="series"/"subseries">."""
    disponibles = {}
    for ligne in concordance:
        if ligne["ir"] == ir_id:
            disponibles.setdefault(ligne["unitid"], []).append(ligne["id"])

    def id_pour(unitid=None):
        if unitid and disponibles.get(unitid):
            return disponibles[unitid].pop(0)
        nouveau = nouvel_id(deja_pris, prefixe="m0")
        if unitid:
            concordance.append({"ir": ir_id, "unitid": unitid, "id": nouveau})
        return nouveau

    return id_pour


def nouvel_element_c(parent, level, id_pour, unitid=None):
    c = etree.SubElement(parent, "c")
    c.set("id", id_pour(unitid))
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


def construire_ead_presse(corpus_code, df, eadid, titre, id_pour):
    df = df.copy()
    df["page"] = df["name"].apply(numero_page)

    dates = df["unitid"].str.extract(PRA_UNITID_RE)
    df["annee"] = dates[0]
    df["mois"] = dates[1]
    df["jour"] = dates[2]
    df = df.dropna(subset=["annee", "mois", "jour"])

    tree, archdesc, dsc = squelette_ead(eadid, titre)
    archdesc.set("id", id_pour(eadid))

    for annee, df_annee in df.groupby("annee"):
        c_annee = nouvel_element_c(dsc, "series", id_pour)
        ajouter_did(c_annee, annee, unitdate_normal=f"{annee}-01-01/{annee}-12-31", unitdate_texte=annee)

        for mois, df_mois in df_annee.groupby("mois"):
            c_mois = nouvel_element_c(c_annee, "subseries", id_pour)
            titre_mois = f"{MOIS[int(mois) - 1]} {annee}"
            ajouter_did(
                c_mois, titre_mois,
                unitdate_normal=f"{annee}-{mois}",
                unitdate_texte=titre_mois,
            )

            for unitid, df_numero in df_mois.groupby("unitid"):
                jour = df_numero["jour"].iloc[0]
                c_numero = nouvel_element_c(c_mois, "file", id_pour, unitid=unitid)
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


def construire_ead_registre(corpus_code, df, eadid, titre, id_pour):
    df = df.copy()
    df["page"] = df["name"].apply(numero_page)

    tree, archdesc, dsc = squelette_ead(eadid, titre)
    archdesc.set("id", id_pour(eadid))

    unitids = sorted(df["unitid"].unique(), key=numero_registre)
    for unitid in unitids:
        df_registre = df[df["unitid"] == unitid]
        c = nouvel_element_c(dsc, "file", id_pour, unitid=unitid)

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


def ajouter_ark(element, oai_dict):
    """Ajoute à chaque <archdesc>/<c> les liens ARK BnR sous forme de <dao>/<daoloc>
    (cf. dao_ark.add_ark_links pour la mécanique d'insertion, même logique que
    ead_bnr2mnesys._add_dao_ark) :

    - l'ARK actuel (role="publication:current"), construit à partir de l'id de
      l'élément : https://www.bn-r.fr/ark:/20179/BNR<id> ;
    - l'ancien ARK (role="publication:previous") pour les <c> dont le <unitid>
      figure dans oai_dict (correspondance unitid -> osiros_id de l'ancien
      référencement) : https://www.bn-r.fr/ark:/20179/<osiros_id>.
    """
    def link_builder(el):
        liens = []
        if el.get("id"):
            liens.append(
                (f"https://www.bn-r.fr/ark:/20179/BNR{el.get('id')}", "publication:current")
            )
        unitid = el.find("./did/unitid")
        if el.tag == "c" and unitid is not None and unitid.text in oai_dict:
            liens.append(
                (f"https://www.bn-r.fr/ark:/20179/{oai_dict[unitid.text]}", "publication:previous")
            )
        return liens

    add_ark_links(element, link_builder)


def main():
    corpus_info = pd.read_excel(join("data", "corpus_liste", "bnr_corpus.xlsx"))
    corpus_info = corpus_info.set_index("corpus_code")
    corpus_info["collection_bnr"] = corpus_info["collection_bnr"].str.strip()
    corpus_info["corpus"] = corpus_info["corpus"].str.strip()

    corpus_codes = CORPUS_PRESSE + CORPUS_REGISTRE
    df = charger_fichiers(corpus_codes)
    oai_dict = dict_osiros(df)

    concordance = charger_concordance(CONCORDANCE_PATH)
    deja_pris = ids_existants() | {ligne["id"] for ligne in concordance}

    for corpus_code in corpus_codes:
        eadid = corpus_info.loc[corpus_code, "collection_bnr"]
        titre = corpus_info.loc[corpus_code, "corpus"]
        df_corpus = df[df["corpus_code"] == corpus_code]
        id_pour = generateur_id(concordance, deja_pris, eadid)

        if corpus_code in CORPUS_REGISTRE:
            tree = construire_ead_registre(corpus_code, df_corpus, eadid, titre, id_pour)
        else:
            tree = construire_ead_presse(corpus_code, df_corpus, eadid, titre, id_pour)

        ajouter_ark(tree.getroot(), oai_dict)

        out_path = join("results", "ead", "corpus_ocr", f"{eadid}.xml")
        tree.write(out_path, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        print(f"{corpus_code} -> {out_path}")

    sauvegarder_concordance(concordance, CONCORDANCE_PATH)


if __name__ == "__main__":
    main()
