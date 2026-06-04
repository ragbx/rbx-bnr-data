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


def create_skos(df, thesaurus_name=None):
    g = Graph()
    ns_uri = f"https://www.bn-r.fr/thesaurus/{thesaurus_name}#"
    ex_ns = Namespace(ns_uri)
    for _, row in df.iterrows():
        concept = row["concept"]
        normalized_concept = normalize_for_uri(concept)
        concept_uri = ex_ns[normalized_concept]

        g.add((concept_uri, RDF.type, SKOS.Concept))
        g.add((concept_uri, SKOS.prefLabel, Literal(concept, lang="fr")))

    g.serialize(
        destination=join(
            "results", "ead", "indexation", f"thesaurus_bnr_{thesaurus_name}.xml"
        ),
        format="xml",
    )


df = pd.read_csv(join("results", "ead", "indexation", "controlaccess_extraction.csv"))
df = df[["balise", "source", "concept"]]
df["concept"] = df["concept"].str.replace("\\", "")
df = df.drop_duplicates()

# on crée le thésaurus genreform
df_ = df[df["balise"] == "genreform"]
thesaurus_name = "genreform"
create_skos(df_, thesaurus_name=thesaurus_name)


subject = df[df["balise"] == "subject"]
# on crée le thésaurus chrono
df_ = subject[subject["source"] == "chrono"]
thesaurus_name = "chrono"
create_skos(df_, thesaurus_name=thesaurus_name)

# on crée le thésaurus theme
df_ = subject[subject["source"] == "theme"]
thesaurus_name = "theme"
create_skos(df_, thesaurus_name=thesaurus_name)

# on crée le thésaurus rameau
df_ = subject[subject["source"] == "Rameau"]
thesaurus_name = "rameau"
create_skos(df_, thesaurus_name=thesaurus_name)

# on crée le thésaurus persname
df_ = df[df["balise"] == "persname"]
thesaurus_name = "persname"
create_skos(df_, thesaurus_name=thesaurus_name)

# on crée le thésaurus corpname
df_ = df[df["balise"] == "corpname"]
thesaurus_name = "corpname"
create_skos(df_, thesaurus_name=thesaurus_name)
