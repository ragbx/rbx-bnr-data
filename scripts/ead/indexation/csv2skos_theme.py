import unicodedata
from os.path import join

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, SKOS


def normalize_for_uri(text):
    text = unicodedata.normalize("NFKD", text)
    text = "".join([c for c in text if not unicodedata.combining(c)])
    text = text.replace(" ", "_").replace("-", "_")
    text = text.lower()
    text = "".join(c for c in text if c.isalnum() or c == "_")
    return text


df = pd.read_csv(join("results", "ead", "indexation", "ref_indexation_theme.csv"))


g = Graph()
ex_ns = Namespace("https://www.bn-r.fr/thesaurus/theme#")


for _, row in df.iterrows():
    concept = row["concept"]
    normalized_concept = normalize_for_uri(concept)
    concept_uri = ex_ns[normalized_concept]

    g.add((concept_uri, RDF.type, SKOS.Concept))
    g.add((concept_uri, SKOS.prefLabel, Literal(concept, lang="fr")))


g.serialize(
    destination=join("results", "ead", "indexation", "thesaurus_bnr_theme.xml"),
    format="xml",
)
