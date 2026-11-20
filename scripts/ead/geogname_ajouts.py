"""
Ajout de <geogname> de sites validés notice par notice, commun aux deux chaînes d'application :
geogname_applique_concordance.py (IR BnR, arbre lxml) et mnesys_geo_applique.py (IR Mnesys, édition du texte).

Ces deux scripts ne font que réécrire les <geogname> existants. Les sites validés notice par notice (recherche
dédiée, décision par notice) n'ont pas de <geogname> dans les IR : ils sont ajoutés ici, à chaque exécution,
d'après les fichiers de décisions (décisions utilisateur du 2026-11-20) :

  - results/ead/indexation/hotel_des_postes_20261120.csv : notices « validé », colonne batiment (A, B, C, ou
    deux lettres « B+C » : la notice reçoit les deux lieux) ; lettres → lieux : POSTES de sites_candidats.py ;
    notices BnR repérées par unitid, notices Mnesys par IR + id du <c> ;
  - data/geo/geogname_ajouts_manuels.csv, tenu à la main : une ligne par notice et par lieu (colonnes source
    « bnr » ou « mnesys », ir, unitid, c_id, lieu_id, libelle, titre, remarque) ; lieu_id d'un lieu du référentiel
    ou d'un territoire (ex. 59299 = Hem (Nord) pour les sites de Hem retirés du référentiel).

Le <geogname> ajouté suit la convention de la réécriture : texte = libellé du lieu, @source = thésaurus du lieu,
@coord = géométrie WKT. Il va dans le <controlaccess> propre au <c> (pas celui d'un <c> enfant), créé s'il
n'existe pas (avant le premier <c> enfant, sinon à la fin du <c>). Un lieu déjà indexé dans ce <controlaccess>
(même libellé) n'est pas ajouté une seconde fois.

Ensuite (2026-11-20), dans chaque <controlaccess> : quartier déduit d'un site ou d'une rue située dans un seul des
31 quartiers, commune déduite d'un site hors Roubaix (data/geo/sites_communes.csv) ; rien pour une rue ambiguë ;
jamais de doublon (voir regles_deduction, deduire_arbre, deduire_texte).
"""
import csv
import re
import sys
from collections import defaultdict
from os.path import basename, dirname, exists, join
from xml.sax.saxutils import escape, quoteattr

sys.path.insert(0, dirname(__file__))
from sites_candidats import POSTES  # noqa: E402

POSTES_MANUEL = join("results", "ead", "indexation", "hotel_des_postes_20261120.csv")
AJOUTS_MANUELS = join("data", "geo", "geogname_ajouts_manuels.csv")
ACTION = "ajouté (validé par notice)"


def charger_ajouts():
    """({unitid: [lieu_id]}, {(ir Mnesys sans préfixe, c_id): [lieu_id]}) des notices validées."""
    par_unitid, par_id = defaultdict(list), defaultdict(list)
    if exists(AJOUTS_MANUELS):
        with open(AJOUTS_MANUELS, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                if r["source"] == "mnesys" and r["c_id"].strip():
                    cle, cible = (r["ir"].strip().removeprefix("FRAM59100_"), r["c_id"].strip()), par_id
                elif r["unitid"].strip():
                    cle, cible = r["unitid"].strip(), par_unitid
                else:
                    continue
                if r["lieu_id"].strip() and r["lieu_id"].strip() not in cible[cle]:
                    cible[cle].append(r["lieu_id"].strip())
    if not exists(POSTES_MANUEL):
        return par_unitid, par_id
    with open(POSTES_MANUEL, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if r["decision"].strip() != "validé":
                continue
            lieux = [POSTES[x.strip()][1] for x in r["batiment"].split("+") if x.strip() in POSTES]
            if r["source"] == "mnesys" and r["id"].strip():
                cle, cible = (basename(r["ir"])[:-4].removeprefix("FRAM59100_"), r["id"].strip()), par_id
            elif r["unitid"].strip():
                cle, cible = r["unitid"].strip(), par_unitid
            else:
                continue
            for lieu in lieux:
                if lieu not in cible[cle]:
                    cible[cle].append(lieu)
    return par_unitid, par_id


def _ligne(ir, c_id, lieu, source, type_coord):
    return {"ir": ir, "c_id": c_id, "texte_avant": "", "texte_apres": lieu["libelle"], "source_apres": source,
            "lieu_id": lieu["lieu_id"], "thesaurus": lieu["thesaurus"], "coord_apres": type_coord,
            "action": ACTION, "doublon": ""}


def ajouter_arbre(root, ir, par_unitid, lieux, coords, sources):
    """IR BnR (arbre lxml, sans espace de noms) : ajoute les <geogname> ; renvoie (lignes du rapport, unitid trouvés)."""
    from lxml import etree
    lignes, trouves = [], set()
    for c in root.iter("c"):
        unitid = c.find("did/unitid")
        cle = "".join(unitid.itertext()).strip() if unitid is not None else ""
        if cle not in par_unitid:
            continue
        trouves.add(cle)
        ca = c.find("controlaccess")
        if ca is None:
            ca = etree.Element("controlaccess")
            enfant = c.find("c")
            if enfant is not None:
                enfant.addprevious(ca)
            else:
                c.append(ca)
        deja = {"".join(g.itertext()).strip() for g in ca.findall("geogname")}
        for lieu_id in par_unitid[cle]:
            lieu = lieux[lieu_id]
            if lieu["libelle"] in deja:
                continue
            coord, type_coord = coords[lieu_id]
            g = etree.SubElement(ca, "geogname", source=sources[lieu["thesaurus"]])
            if coord:
                g.set("coord", coord)
            g.text = lieu["libelle"]
            deja.add(lieu["libelle"])
            lignes.append(_ligne(ir, c.get("id", ""), lieu, g.get("source"), type_coord))
    return lignes, trouves


BALISE_C = re.compile(r"<c\b[^>]*>|</c>")


def ajouter_texte(texte_ir, ir, par_id, lieux, coords, sources):
    """IR Mnesys (texte, conservé à l'octet près ailleurs) : renvoie (texte, lignes du rapport, introuvables)."""
    cibles = {c_id: l for (i, c_id), l in par_id.items() if i == ir}
    insertions, lignes, introuvables = [], [], []
    for c_id, lieux_ids in cibles.items():
        m = re.search(r'<c\b[^>]*\bid="%s"[^>]*>' % re.escape(c_id), texte_ir)
        if m is None:
            introuvables.append(c_id)
            continue
        # fin de la partie propre au <c> : premier <c> enfant, sinon sa balise fermante (la première rencontrée)
        fin_propre = BALISE_C.search(texte_ir, m.end()).start()
        propre = texte_ir[m.end():fin_propre]
        fermante = propre.rfind("</controlaccess>")
        deja = set(re.findall(r"<geogname\b[^>]*>([^<]*)</geogname>", propre[:fermante] if fermante >= 0 else ""))
        nouveaux = []
        for lieu_id in lieux_ids:
            lieu = lieux[lieu_id]
            if lieu["libelle"] in deja or escape(lieu["libelle"]) in deja:
                continue
            coord, type_coord = coords[lieu_id]
            source = sources[lieu["thesaurus"]]
            attrs = f" source={quoteattr(source)}" + (f" coord={quoteattr(coord)}" if coord else "")
            nouveaux.append(f"<geogname{attrs}>{escape(lieu['libelle'])}</geogname>")
            deja.add(lieu["libelle"])
            lignes.append(_ligne(ir, c_id, lieu, source, type_coord))
        if not nouveaux:
            continue
        if fermante >= 0:
            insertions.append((m.end() + fermante, "".join(nouveaux)))
        else:
            insertions.append((fin_propre, "<controlaccess>" + "".join(nouveaux) + "</controlaccess>"))
    for position, ajout in sorted(insertions, reverse=True):
        texte_ir = texte_ir[:position] + ajout + texte_ir[position:]
    return texte_ir, lignes, introuvables


# --- Quartier ou commune déduits (décision utilisateur du 2026-11-20) ---------------------------------------
# Dans chaque <controlaccess>, après les réécritures et les ajouts ci-dessus :
#   - un site (thésaurus des points) situé dans un seul quartier de Roubaix → <geogname> de ce quartier ;
#     un site hors Roubaix → <geogname> de sa commune (data/geo/sites_communes.csv, commune du point) ;
#   - une rue située dans un seul quartier (colonne quartiers du référentiel, 31 quartiers) → ce quartier ;
#     une rue sur plusieurs quartiers (ambiguë) ou sans quartier → rien.
# Jamais de doublon : un lieu déjà présent dans le <controlaccess> (même libellé) n'est pas ajouté, et un
# quartier cité par plusieurs rues de la notice n'est ajouté qu'une fois.
SITES_COMMUNES = join("data", "geo", "sites_communes.csv")
ACTION_QUARTIER = "ajouté (quartier déduit)"
ACTION_COMMUNE = "ajouté (commune déduite)"


def regles_deduction(lieux):
    """{(thésaurus, libellé du lieu cité): (lieu_id à ajouter, action)} pour les rues et les sites."""
    quartiers = {l["libelle"]: l["lieu_id"] for l in lieux.values() if l.get("thesaurus") == "quartiers"}
    communes = {}
    if exists(SITES_COMMUNES):
        with open(SITES_COMMUNES, encoding="utf-8") as f:
            communes = {r["lieu_id"]: r["commune_id"] for r in csv.DictReader(f)}
    regles = {}
    for l in lieux.values():
        thesaurus = l.get("thesaurus")
        if thesaurus not in ("rues", "points"):
            continue
        qs = [q for q in (l.get("quartiers") or "").split(" | ") if q]
        if len(qs) == 1 and qs[0] in quartiers:
            regles[(thesaurus, l["libelle"])] = (quartiers[qs[0]], ACTION_QUARTIER)
        elif thesaurus == "points" and not qs and communes.get(l["lieu_id"]) in lieux:
            regles[(thesaurus, l["libelle"])] = (communes[l["lieu_id"]], ACTION_COMMUNE)
    return regles


def deduire_arbre(root, ir, regles, lieux, coords, sources):
    """IR BnR (arbre lxml) : ajoute les quartiers ou communes déduits ; renvoie les lignes du rapport."""
    from lxml import etree
    thesaurus_de = {v: k for k, v in sources.items()}
    lignes = []
    for ca in root.iter("controlaccess"):
        cites = ca.findall("geogname")
        presents = {"".join(g.itertext()).strip() for g in cites}
        for g in cites:
            regle = regles.get((thesaurus_de.get(g.get("source", "")), "".join(g.itertext()).strip()))
            if regle is None:
                continue
            lieu_id, action = regle
            lieu = lieux[lieu_id]
            if lieu["libelle"] in presents:
                continue
            coord, type_coord = coords[lieu_id]
            nouveau = etree.SubElement(ca, "geogname", source=sources[lieu["thesaurus"]])
            if coord:
                nouveau.set("coord", coord)
            nouveau.text = lieu["libelle"]
            presents.add(lieu["libelle"])
            c = next(ca.iterancestors("c"), None)
            ligne = _ligne(ir, c.get("id", "") if c is not None else "", lieu, nouveau.get("source"), type_coord)
            ligne.update(action=action, texte_avant="".join(g.itertext()).strip())
            lignes.append(ligne)
    return lignes


CONTROLACCESS = re.compile(r"<controlaccess\b[^>]*>(.*?)</controlaccess>", re.S)
GEOGNAME_TEXTE = re.compile(r"<geogname\b([^>]*)>(.*?)</geogname>", re.S)
SOURCE_ATTR = re.compile(r'\bsource="([^"]*)"')
C_ID = re.compile(r'<c\b[^>]*\bid="([^"]+)"')


def deduire_texte(texte_ir, ir, regles, lieux, coords, sources):
    """IR Mnesys (texte) : ajoute les quartiers ou communes déduits avant </controlaccess> ; renvoie (texte, lignes)."""
    from bisect import bisect_right
    from html import unescape
    thesaurus_de = {v: k for k, v in sources.items()}
    debuts_c = [(m.start(), m.group(1)) for m in C_ID.finditer(texte_ir)]
    positions = [d for d, _ in debuts_c]
    insertions, lignes = [], []
    for m in CONTROLACCESS.finditer(texte_ir):
        cites = [(SOURCE_ATTR.search(a), unescape(re.sub(r"<[^>]+>", "", t)).strip())
                 for a, t in GEOGNAME_TEXTE.findall(m.group(1))]
        presents = {t for _, t in cites}
        nouveaux = []
        i = bisect_right(positions, m.start()) - 1
        c_id = debuts_c[i][1] if i >= 0 else ""
        for source, texte in cites:
            regle = regles.get((thesaurus_de.get(source.group(1) if source else ""), texte))
            if regle is None:
                continue
            lieu_id, action = regle
            lieu = lieux[lieu_id]
            if lieu["libelle"] in presents:
                continue
            coord, type_coord = coords[lieu_id]
            src = sources[lieu["thesaurus"]]
            attrs = f" source={quoteattr(src)}" + (f" coord={quoteattr(coord)}" if coord else "")
            nouveaux.append(f"<geogname{attrs}>{escape(lieu['libelle'])}</geogname>")
            presents.add(lieu["libelle"])
            ligne = _ligne(ir, c_id, lieu, src, type_coord)
            ligne.update(action=action, texte_avant=texte)
            lignes.append(ligne)
        if nouveaux:
            insertions.append((m.end() - len("</controlaccess>"), "".join(nouveaux)))
    for position, ajout in sorted(insertions, reverse=True):
        texte_ir = texte_ir[:position] + ajout + texte_ir[position:]
    return texte_ir, lignes


# --- Doublons de quartier ou de commune (décision utilisateur du 2026-11-20) ---------------------------------
# Deux formes d'un même quartier ou d'une même commune dans un <controlaccess> (ex. « Fresnoy » et « quartier du
# Fresnoy ») sont réécrites en deux <geogname> identiques : le second et les suivants sont retirés (même texte,
# même @source, thésaurus des quartiers ou des territoires). Les doublons de voies (ancien et nouveau nom) restent
# gardés (décision du 2026-10-27).
ACTION_DOUBLON = "retiré (doublon de quartier ou de commune)"


def _sources_dedoublonnees(sources):
    return {sources[t] for t in ("quartiers", "communes_pays") if t in sources}


def dedoublonner_arbre(root, ir, sources):
    """IR BnR (arbre lxml) : retire les doublons de quartier ou de commune ; renvoie les lignes du rapport."""
    cibles = _sources_dedoublonnees(sources)
    lignes = []
    for ca in root.iter("controlaccess"):
        vus = set()
        for g in ca.findall("geogname"):
            if g.get("source", "") not in cibles:
                continue
            cle = ("".join(g.itertext()).strip(), g.get("source"))
            if cle in vus:
                c = next(ca.iterancestors("c"), None)
                lignes.append({"ir": ir, "c_id": c.get("id", "") if c is not None else "", "texte_avant": cle[0],
                               "texte_apres": "", "source_apres": "", "action": ACTION_DOUBLON, "doublon": "oui"})
                precedent = g.getprevious()
                if precedent is not None:  # garder la mise en forme : l'élément précédent prend la fin de ligne
                    precedent.tail = g.tail
                ca.remove(g)
            else:
                vus.add(cle)
    return lignes


def dedoublonner_texte(texte_ir, ir, sources):
    """IR Mnesys (texte) : retire les doublons de quartier ou de commune ; renvoie (texte, lignes du rapport)."""
    from bisect import bisect_right
    from html import unescape
    cibles = _sources_dedoublonnees(sources)
    debuts_c = [(m.start(), m.group(1)) for m in C_ID.finditer(texte_ir)]
    positions = [d for d, _ in debuts_c]
    retraits, lignes = [], []
    for m in CONTROLACCESS.finditer(texte_ir):
        vus = set()
        for g in GEOGNAME_TEXTE.finditer(texte_ir, m.start(1), m.end(1)):
            source = SOURCE_ATTR.search(g.group(1))
            if not source or source.group(1) not in cibles:
                continue
            cle = (unescape(re.sub(r"<[^>]+>", "", g.group(2))).strip(), source.group(1))
            if cle in vus:
                i = bisect_right(positions, m.start()) - 1
                retraits.append((g.start(), g.end()))
                lignes.append({"ir": ir, "c_id": debuts_c[i][1] if i >= 0 else "", "texte_avant": cle[0],
                               "texte_apres": "", "source_apres": "", "action": ACTION_DOUBLON, "doublon": "oui"})
            else:
                vus.add(cle)
    for debut, fin in sorted(retraits, reverse=True):
        texte_ir = texte_ir[:debut] + texte_ir[fin:]
    return texte_ir, lignes
