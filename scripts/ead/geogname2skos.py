"""
Génère les quatre thésaurus SKOS géographiques de la BnR à partir des
référentiels (et non des IR) :

  - data/geo/referentiel_lieux_roubaix.csv : quartiers, voies (thesaurus
    « rues »), sites (thesaurus « points ») ;
  - data/geo/referentiel_communes_pays.csv : territoires (thesaurus
    « communes_pays ») ;
  - data/geo/concordance_geogname_lieux.csv : formes des IR alignées.

Sorties : results/ead/indexation/thesaurus/bnr_geo_rbx_quartiers.xml,
bnr_geo_rbx_voies.xml, bnr_geo_rbx_sites.xml, bnr_geo_territoires.xml (RDF/XML),
même convention que controlaccess2skos.py : un skos:ConceptScheme
https://www.bn-r.fr/thesaurus/<nom> et des concepts <nom>#<libellé normalisé>.

Par concept (décisions du 2026-11-06) :
  - skos:prefLabel@fr = libellé (le texte des <geogname> des IR) ;
  - skos:notation = lieu_id ; dct:type = catégorie (rue, cour, commune…) ;
  - skos:altLabel@fr = anciens noms (colonne nom_ancien) ;
  - skos:hiddenLabel@fr = formes des IR alignées sur le lieu, différentes du
    libellé et des anciens noms ;
  - geo:hasGeometry <concept>_geometrie / geo:asWKT (GeoSPARQL) = coord_manuel
    sinon coord_wkt,
    WKT longitude latitude (CRS84, le CRS par défaut de geo:wktLiteral),
    6 décimales, comme le @coord des IR ;
  - voies et sites : skos:related vers les quartiers traversés (colonne
    quartiers ; écrit d'un seul côté, la relation est symétrique) ;
  - lieu disparu : skos:historyNote (dates) et dct:isReplacedBy ;
  - territoires : skos:broader / skos:narrower d'après la colonne parent,
    skos:exactMatch vers Wikidata et data.bnf.fr ;
  - skos:topConceptOf : tous les lieux de Roubaix, les territoires sans parent.

Tous les lieux des référentiels sont exportés, cités ou non dans les IR ; les
lieux écartés (referentiel_lieux_roubaix_ecartes.csv) ne le sont pas.

À lancer depuis la racine du dépôt, dans l'environnement rbx-bnr-data :
    python scripts/ead/geogname2skos.py
"""
import csv
import sys
import unicodedata
from collections import defaultdict
from os import makedirs
from os.path import basename, dirname, join

from lxml import etree
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, SKOS

sys.path.insert(0, dirname(__file__))
from geogname_applique_concordance import SOURCES, coord_ir, normaliser  # noqa: E402
from geogname_concordance_lieux import COMMUNES_PAYS, SORTIE as CONCORDANCE  # noqa: E402
from referentiel_lieux_roubaix import SORTIE as REFERENTIEL  # noqa: E402

DEST_THESAURUS = join("results", "ead", "indexation", "thesaurus")
GEO = Namespace("http://www.opengis.net/ont/geosparql#")
WIKIDATA = "http://www.wikidata.org/entity/"
BNF = "https://data.bnf.fr/ark:/12148/cb"
SEP = "|"


def normalize_for_uri(text):
    """Même règle que controlaccess2skos.py."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.replace(" ", "_").replace("-", "_").lower()
    return "".join(c for c in text if c.isalnum() or c == "_")


def lire(chemin):
    with open(chemin, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def valeurs(texte):
    return [v.strip() for v in texte.split(SEP) if v.strip()]


def ecrire_trie(g, fichier):
    """RDF/XML avec les rdf:Description triées par URI : rdflib les sort dans un
    ordre qui varie d'une exécution à l'autre, le fichier serait sinon différent
    à chaque fois."""
    racine = etree.fromstring(g.serialize(format="xml", encoding="utf-8"))
    racine[:] = sorted(racine, key=lambda e: e.get(f"{{{RDF}}}about", ""))
    racine.text = "\n  "
    for i, e in enumerate(racine):
        e.tail = "\n  " if i < len(racine) - 1 else "\n"
    with open(fichier, "wb") as f:
        f.write(etree.tostring(racine, xml_declaration=True, encoding="utf-8") + b"\n")


def main():
    lieux = lire(REFERENTIEL) + lire(COMMUNES_PAYS)
    par_thesaurus = defaultdict(list)
    for l in lieux:
        par_thesaurus[l["thesaurus"]].append(l)

    # URI de chaque lieu : <espace du thésaurus>#<libellé normalisé>, unique par thésaurus
    espaces, uri, uri_par_libelle = {}, {}, {}
    for thesaurus, liste in par_thesaurus.items():
        nom_court = basename(SOURCES[thesaurus].replace("--SLASH--", "/"))[:-4].removeprefix("bnr_")
        espaces[thesaurus] = (nom_court, f"https://www.bn-r.fr/thesaurus/{nom_court}")
        vus = {}
        for l in liste:
            fragment = normalize_for_uri(l["libelle"])
            if fragment in vus or fragment.endswith("_geometrie"):
                sys.exit(f"{thesaurus} : « {l['libelle']} » et « {vus[fragment]} » donnent la même URI #{fragment}")
            vus[fragment] = l["libelle"]
            uri[l["lieu_id"]] = URIRef(f"{espaces[thesaurus][1]}#{fragment}")
            uri_par_libelle[(thesaurus, l["libelle"])] = uri[l["lieu_id"]]

    formes = defaultdict(set)
    for f in lire(CONCORDANCE):
        if f["statut"] == "aligné" and f["lieu_id"]:
            formes[f["lieu_id"]].add(normaliser(f["forme"]))

    makedirs(DEST_THESAURUS, exist_ok=True)
    for thesaurus, liste in sorted(par_thesaurus.items()):
        nom_court, uri_schema = espaces[thesaurus]
        schema = URIRef(uri_schema)
        g = Graph()
        g.bind("skos", SKOS)
        g.bind("dct", DCTERMS)
        g.bind("geo", GEO)
        g.bind(nom_court, Namespace(uri_schema + "#"))
        g.add((schema, RDF.type, SKOS.ConceptScheme))
        g.add((schema, DCTERMS.title, Literal(f"Thésaurus BnR — {nom_court}", lang="fr")))
        compte = defaultdict(int)
        for l in liste:
            c = uri[l["lieu_id"]]
            g.add((c, RDF.type, SKOS.Concept))
            g.add((c, SKOS.prefLabel, Literal(l["libelle"], lang="fr")))
            g.add((c, SKOS.inScheme, schema))
            g.add((c, SKOS.notation, Literal(l["lieu_id"])))
            if l["categorie"]:
                g.add((c, DCTERMS.type, Literal(l["categorie"], lang="fr")))

            anciens = valeurs(l.get("nom_ancien", ""))
            for a in anciens:
                g.add((c, SKOS.altLabel, Literal(a, lang="fr")))
                compte["altLabel"] += 1
            for forme in sorted(formes[l["lieu_id"]] - {l["libelle"], *anciens}):
                g.add((c, SKOS.hiddenLabel, Literal(forme, lang="fr")))
                compte["hiddenLabel"] += 1

            geometrie = l["coord_manuel"] or l["coord_wkt"]
            if geometrie:
                noeud = URIRef(f"{c}_geometrie")  # URI stable, pas de nœud anonyme renuméroté à chaque exécution
                g.add((c, RDF.type, GEO.Feature))
                g.add((c, GEO.hasGeometry, noeud))
                g.add((noeud, RDF.type, GEO.Geometry))
                g.add((noeud, GEO.asWKT, Literal(coord_ir(geometrie)[0], datatype=GEO.wktLiteral)))
                compte["géométrie"] += 1

            for q in valeurs(l.get("quartiers", "")) if thesaurus != "quartiers" else []:
                g.add((c, SKOS.related, uri_par_libelle[("quartiers", q)]))
                compte["related"] += 1

            if l.get("date_debut") or l.get("date_fin"):
                g.add((c, SKOS.historyNote, Literal(
                    f"Dates : {l.get('date_debut') or '?'} – {l.get('date_fin') or '?'}.", lang="fr")))
                compte["historyNote"] += 1
            if l.get("remplacee_par"):
                g.add((c, DCTERMS.isReplacedBy, uri[l["remplacee_par"]]))
                compte["isReplacedBy"] += 1

            parent = l.get("parent", "")
            if parent:
                p = uri_par_libelle[(thesaurus, parent)]
                g.add((c, SKOS.broader, p))
                g.add((p, SKOS.narrower, c))
                compte["broader"] += 1
            else:
                g.add((c, SKOS.topConceptOf, schema))
                g.add((schema, SKOS.hasTopConcept, c))
            if l.get("wikidata"):
                g.add((c, SKOS.exactMatch, URIRef(WIKIDATA + l["wikidata"])))
                compte["exactMatch"] += 1
            if l.get("bnf"):
                g.add((c, SKOS.exactMatch, URIRef(BNF + l["bnf"])))
                compte["exactMatch"] += 1

        fichier = join(DEST_THESAURUS, f"bnr_{nom_court}.xml")
        ecrire_trie(g, fichier)
        print(f"{len(liste):5d} concepts  {fichier}  {dict(compte)}")


if __name__ == "__main__":
    main()
