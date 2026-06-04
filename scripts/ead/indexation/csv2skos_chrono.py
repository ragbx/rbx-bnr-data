from os.path import join

import pandas as pd
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, SKOS

df = pd.read_csv(join("results", "ead", "indexation", "ref_indexation_chrono.csv"))


g = Graph()
ex_ns = Namespace("https://www.bn-r.fr/thesaurus/chrono#")


for _, row in df.iterrows():
    concept = row["concept"]
    concept_uri = ex_ns[concept.replace("-", "_")]  # Remplacer les tirets pour l'URI

    g.add((concept_uri, RDF.type, SKOS.Concept))
    g.add((concept_uri, SKOS.prefLabel, Literal(concept, lang="fr")))


g.serialize(
    destination=join("results", "ead", "indexation", "thesaurus_bnr_chrono.xml"),
    format="xml",
)
