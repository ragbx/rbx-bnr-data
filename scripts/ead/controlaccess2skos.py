"""
Extrait les accès indexés des IR et en génère les thésaurus SKOS de la BnR.

Fusionne les anciens controlaccess_extraction.py et csv2skos.py : tout part
des mêmes fichiers results/ead/ead_cor/bnr2mnesys/*.xml, en un seul passage,
ce qui évite que le CSV intermédiaire ne se désynchronise du corpus.

Deux sorties :
  - results/ead/indexation/controlaccess_extraction.csv : inventaire complet de
    l'indexation (une ligne par enfant de <controlaccess>), conservé pour
    l'analyse — colonnes eadid, titleproper, balise, concept, source, role, normal ;
  - results/ead/indexation/thesaurus/bnr_<nom>.xml : un thésaurus SKOS par valeur de
    l'attribut source normalisé par ead_bnr2mnesys.py
    ("thesaurus--SLASH--bnr_<nom>.xml"). Le thésaurus cible est porté par la
    donnée : pas de filtrage en dur par balise ni par valeur. Chaque fichier
    contient un skos:ConceptScheme et, par valeur distincte, un skos:Concept
    (skos:prefLabel @fr, skos:inScheme).

Les accès sans source de thésaurus (geogname rue/quartier désactivés, valeurs
sans source ou hors vocabulaire) sont ignorés pour le SKOS, mais figurent dans
le CSV.

À lancer depuis la racine du dépôt.
"""
import unicodedata
from glob import glob
from os import makedirs
from os.path import basename, join

import pandas as pd
from lxml import etree
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, SKOS

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
CSV_SORTIE = join("results", "ead", "indexation", "controlaccess_extraction.csv")
DEST_THESAURUS = join("results", "ead", "indexation", "thesaurus")
PREFIXE_SOURCE = "thesaurus--SLASH--"


def extraire_controlaccess():
    """Parcourt les IR et renvoie un DataFrame des enfants de <controlaccess>."""
    lignes = []
    for path in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        root = etree.parse(path).getroot()
        eadid = root.xpath("//eadheader/eadid/text()")
        titleproper = root.xpath(
            "//eadheader/filedesc/titlestmt/titleproper/text()"
        )
        eadid = eadid[0] if eadid else None
        titleproper = titleproper[0] if titleproper else None

        for controlaccess in root.xpath("//controlaccess"):
            for child in controlaccess:
                lignes.append(
                    {
                        "eadid": eadid,
                        "titleproper": titleproper,
                        "balise": child.tag,
                        "concept": child.text.strip() if child.text else None,
                        "source": child.get("source"),
                        "role": child.get("role"),
                        "normal": child.get("normal"),
                    }
                )
    return pd.DataFrame(lignes)


def normalize_for_uri(text):
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace(" ", "_").replace("-", "_").lower()
    return "".join(c for c in text if c.isalnum() or c == "_")


def create_skos(concepts, fichier):
    """Écrit un thésaurus SKOS dans results/ead/indexation/thesaurus/<fichier> :
    un ConceptScheme et un Concept par libellé. `fichier` est de la forme
    bnr_<nom>.xml ; le nom court (sans préfixe bnr_) sert au namespace."""
    nom_court = fichier[:-4].removeprefix("bnr_")
    ns_uri = f"https://www.bn-r.fr/thesaurus/{nom_court}#"
    ex = Namespace(ns_uri)
    g = Graph()
    g.bind("skos", SKOS)
    g.bind("dct", DCTERMS)
    g.bind(nom_court, ex)
    scheme = URIRef(ns_uri.rstrip("#"))
    g.add((scheme, RDF.type, SKOS.ConceptScheme))
    g.add((scheme, DCTERMS.title, Literal(f"Thésaurus BnR — {nom_court}", lang="fr")))
    for concept in concepts:
        uri = ex[normalize_for_uri(concept)]
        g.add((uri, RDF.type, SKOS.Concept))
        g.add((uri, SKOS.prefLabel, Literal(concept, lang="fr")))
        g.add((uri, SKOS.inScheme, scheme))
    g.serialize(destination=join(DEST_THESAURUS, fichier), format="xml")


df = extraire_controlaccess()
df.to_csv(CSV_SORTIE, index=False)
print(f"{len(df)} accès indexés → {CSV_SORTIE}")

makedirs(DEST_THESAURUS, exist_ok=True)
skos = df[df["source"].str.startswith(PREFIXE_SOURCE, na=False)].copy()
skos = skos[skos["concept"].notna()]
skos["concept"] = skos["concept"].str.replace("\\", "", regex=False)
skos = skos[skos["concept"].str.strip() != ""]

print("\nthésaurus SKOS :")
for source, sous_df in skos.groupby("source"):
    fichier = basename(source.replace("--SLASH--", "/"))  # bnr_<nom>.xml
    concepts = sorted(sous_df["concept"].drop_duplicates())
    create_skos(concepts, fichier)
    print(f"{len(concepts):6d} concepts  {fichier}")
