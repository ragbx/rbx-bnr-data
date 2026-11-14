"""Thésaurus SKOS de test pour Mnesys : rues de Roubaix -> quartiers (31 MEL).

Un seul fichier, un seul schéma : les rues et les 31 quartiers y sont des
concepts de tête. Chaque rue porte un skos:related vers son ou ses quartiers
de rattachement ; les quartiers n'en portent pas.

Les quartiers doivent être dans le même fichier : à l'import, Mnesys
transforme skos:related en <relation type="TA" ref="<schéma>#<id>"/> et ne
résout ref que vers un terme du même thésaurus (essai du 14/11/2026 avec des
liens vers geo_rbx_quartiers : quartiers absents des fiches).

Rattachement d'une rue :
  - rue ambiguë (data/geo/rues_rattachement.csv) : colonne retenu_31 ;
  - sinon rue « sûre » : q31_principal de data/geo/rues_quartiers_13_repartition.csv ;
  - rue sans tracé : aucun quartier.

Libellés, notations, anciens noms et formes des IR sont repris des thésaurus
bnr_geo_rbx_voies.xml et bnr_geo_rbx_quartiers.xml (geogname2skos.py).

    python scripts/ead/rues_quartiers2skos.py
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

from lxml import etree

RACINE = Path(__file__).resolve().parents[2]
THES = RACINE / "results/ead/indexation/thesaurus"
# --quartiers-hors-tete : les quartiers restent des concepts du schéma (cibles
# des related) mais ne sont plus concepts de tête, pour qu'ils n'apparaissent
# pas comme entrées du thésaurus dans Mnesys (essai du 14/11/2026).
HORS_TETE = "--quartiers-hors-tete" in sys.argv
# --deux-branches : deux concepts de tête « Rues » et « Quartiers » ; rues et
# quartiers rangés dessous (broader / narrower), pour ne pas les mélanger dans
# la liste des entrées de Mnesys, qui affiche tous les concepts (essai du
# 14/11/2026 : --quartiers-hors-tete ne les masque pas).
BRANCHES = "--deux-branches" in sys.argv
# --quartiers-en-note : rues seules (pas de concept quartier, pas de related) ;
# les quartiers de rattachement sont écrits en texte dans skos:scopeNote,
# « Quartier(s) : A, B ». Essai du 14/11/2026 : des trois notes testées
# (scopeNote, definition, note), Mnesys n'affiche que scopeNote, en « note ».
EN_NOTE = "--quartiers-en-note" in sys.argv
SORTIE = RACINE / "results/ead/indexation/mnesys_test" / (
    "bnr_geo_rbx_rues_quartiers_note.xml" if EN_NOTE
    else "bnr_geo_rbx_rues_quartiers_branches.xml" if BRANCHES
    else "bnr_geo_rbx_rues_quartiers_hors_tete.xml" if HORS_TETE
    else "bnr_geo_rbx_rues_quartiers.xml")
BRANCHE = {"rue": ("rues", "Rues"), "quartier": ("quartiers", "Quartiers")}
SORTIE_MNESYS = SORTIE.with_name("bnr_geo_rbx_rues_quartiers_mnesys.xml")
SCHEMA = "https://www.bn-r.fr/thesaurus/geo_rbx_rues_quartiers"

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
SKOS = "http://www.w3.org/2004/02/skos/core#"
DCT = "http://purl.org/dc/terms/"
NS = {"rdf": RDF, "skos": SKOS, "dct": DCT}
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"


def lire_concepts(fichier):
    """Concepts d'un thésaurus geogname2skos : liste de dicts."""
    concepts = []
    for d in etree.parse(str(fichier)).getroot():
        if d.find("skos:prefLabel", NS) is None:
            continue
        uri = d.get(f"{{{RDF}}}about")
        concepts.append({
            "frag": uri.split("#", 1)[1],
            "pref": d.findtext("skos:prefLabel", namespaces=NS),
            "notation": d.findtext("skos:notation", namespaces=NS),
            "type": d.findtext("dct:type", namespaces=NS),
            "alt": [e.text for e in d.findall("skos:altLabel", NS)],
            "hidden": [e.text for e in d.findall("skos:hiddenLabel", NS)],
        })
    return concepts


def ecrire_mnesys(voies, liens):
    """Même contenu au format natif des thésaurus Mnesys (FRAM59100_THES_*.xml).

    Format relevé sur l'export du 14/11/2026 : UTF-8 sans BOM, fins de ligne LF,
    tabulations ; un <terme> par rue, trié par id ; TA (quartiers) puis EP
    (anciens noms). Les TA portent le libellé du quartier, pas un ref.
    """
    from xml.sax.saxutils import escape
    lignes = ['<?xml version="1.0" encoding="utf-8"?>', "<!DOCTYPE thesaurus>",
              "<thesaurus>", "\t<title/>"]
    for v in sorted(voies, key=lambda v: v["frag"]):
        lignes.append(f'\t<terme id="geo_rbx_rues_quartiers#rue_{v["frag"]}">')
        lignes.append(f"\t\t<title>{escape(v['pref'])}</title>")
        for n in liens[v["notation"]][0]:
            lignes.append(f'\t\t<relation type="TA">{escape(n)}</relation>')
        for a in v["alt"]:
            lignes.append(f'\t\t<relation type="EP">{escape(a)}</relation>')
        lignes.append("\t</terme>")
    lignes.append("</thesaurus>")
    SORTIE_MNESYS.write_bytes(("\n".join(lignes) + "\n").encode("utf-8"))
    print(f"{SORTIE_MNESYS.relative_to(RACINE)}")


def lire_csv(fichier):
    with open(fichier, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def main():
    voies = lire_concepts(THES / "bnr_geo_rbx_voies.xml")
    quartiers = lire_concepts(THES / "bnr_geo_rbx_quartiers.xml")
    q_par_libelle = {q["pref"]: q for q in quartiers}

    repartition = {r["lieu_id"]: r for r in lire_csv(RACINE / "data/geo/rues_quartiers_13_repartition.csv")}
    rattachement = {r["lieu_id"]: r for r in lire_csv(RACINE / "data/geo/rues_rattachement.csv")}

    erreurs = []
    liens = {}          # notation rue -> (liste de libellés de quartier, règle)
    for v in voies:
        lid = v["notation"]
        if lid in rattachement:
            r = rattachement[lid]
            noms = [n.strip() for n in r["retenu_31"].split("|") if n.strip()]
            regle = "rue ambiguë, " + ("décision manuelle" if r["quartier_31_manuel"].strip() else r["regle_31"])
        elif lid in repartition and repartition[lid]["q31_principal"]:
            noms = [repartition[lid]["q31_principal"]]
            regle = "rue sûre ({} % du tracé)".format(repartition[lid]["q31_pct_principal"])
        else:
            noms, regle = [], "sans tracé : quartier inconnu"
        for n in noms:
            if n not in q_par_libelle:
                erreurs.append(f"{v['pref']} : quartier inconnu « {n} »")
        liens[lid] = ([n for n in noms if n in q_par_libelle], regle)
    if erreurs:
        sys.exit("\n".join(erreurs))

    root = etree.Element(f"{{{RDF}}}RDF", nsmap=NS)

    def sous(parent, tag, texte=None, ressource=None, lang=None):
        e = etree.SubElement(parent, tag)
        if texte is not None:
            e.text = texte
        if ressource is not None:
            e.set(f"{{{RDF}}}resource", ressource)
        if lang:
            e.set(XML_LANG, lang)
        return e

    def uri(frag_type, frag):
        return f"{SCHEMA}#{frag_type}_{frag}"

    tous = [("quartier", q) for q in sorted(quartiers, key=lambda q: q["frag"])] + \
           [("rue", v) for v in sorted(voies, key=lambda v: v["frag"])]

    s = etree.SubElement(root, f"{{{RDF}}}Description", {f"{{{RDF}}}about": SCHEMA})
    sous(s, f"{{{RDF}}}type", ressource=SKOS + "ConceptScheme")
    sous(s, f"{{{DCT}}}title", "Thésaurus BnR — rues et quartiers de Roubaix (test)", lang="fr")
    sous(s, f"{{{DCT}}}description",
         "Rues de Roubaix reliées (skos:related) à leur quartier de rattachement "
         "parmi les 31 quartiers MEL ; relation écrite du seul côté de la rue.", lang="fr")
    def en_tete(genre):
        if BRANCHES:
            return genre == "branche"
        return genre == "rue" or not HORS_TETE

    if EN_NOTE:
        tous = [(g, c) for g, c in tous if g == "rue"]

    if BRANCHES:
        branches = [("branche", {"frag": frag, "pref": libelle, "notation": None, "type": None,
                                 "alt": [], "hidden": [], "genre": g})
                    for g, (frag, libelle) in BRANCHE.items()]
        tous = branches + tous

    for genre, c in tous:
        if en_tete(genre):
            sous(s, f"{{{SKOS}}}hasTopConcept", ressource=uri(genre, c["frag"]))

    for genre, c in tous:
        d = etree.SubElement(root, f"{{{RDF}}}Description", {f"{{{RDF}}}about": uri(genre, c["frag"])})
        sous(d, f"{{{RDF}}}type", ressource=SKOS + "Concept")
        sous(d, f"{{{SKOS}}}prefLabel", c["pref"], lang="fr")
        sous(d, f"{{{SKOS}}}inScheme", ressource=SCHEMA)
        if en_tete(genre):
            sous(d, f"{{{SKOS}}}topConceptOf", ressource=SCHEMA)
        if BRANCHES and genre == "branche":
            for g2, c2 in tous:
                if g2 == c["genre"]:
                    sous(d, f"{{{SKOS}}}narrower", ressource=uri(g2, c2["frag"]))
        elif BRANCHES:
            sous(d, f"{{{SKOS}}}broader", ressource=uri("branche", BRANCHE[genre][0]))
        if c["notation"]:
            sous(d, f"{{{SKOS}}}notation", c["notation"])
        if c["type"]:
            sous(d, f"{{{DCT}}}type", c["type"], lang="fr")
        for a in c["alt"]:
            sous(d, f"{{{SKOS}}}altLabel", a, lang="fr")
        for h in c["hidden"]:
            sous(d, f"{{{SKOS}}}hiddenLabel", h, lang="fr")
        if genre == "rue":
            noms, regle = liens[c["notation"]]
            if EN_NOTE:
                if noms:
                    sous(d, f"{{{SKOS}}}scopeNote", "Quartier(s) : " + ", ".join(noms), lang="fr")
            else:
                for n in noms:
                    sous(d, f"{{{SKOS}}}related", ressource=uri("quartier", q_par_libelle[n]["frag"]))
            sous(d, f"{{{SKOS}}}editorialNote", "Rattachement : " + regle + ".", lang="fr")

    SORTIE.parent.mkdir(parents=True, exist_ok=True)
    etree.ElementTree(root).write(str(SORTIE), xml_declaration=True, encoding="utf-8", pretty_print=True)

    if not (HORS_TETE or BRANCHES or EN_NOTE):
        ecrire_mnesys(voies, liens)

    nb = defaultdict(int)
    for noms, _ in liens.values():
        nb[len(noms)] += 1
    print(f"{SORTIE.relative_to(RACINE)} : {len(voies)} rues, {len(quartiers)} quartiers")
    for k in sorted(nb):
        print(f"  rues à {k} quartier(s) : {nb[k]}")


if __name__ == "__main__":
    main()
