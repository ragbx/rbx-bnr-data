"""
Éléments communs des scripts mnesys_geo_* (alignement des <geogname> des inventaires Mnesys) : séries d'IR,
noms des fichiers de propositions, chaîne des familles. Aucun traitement ici ; module à importer.

Séries (paramètre --serie des scripts) :
  - fi     : séries Fi (data/ead/mnesys/FRAM59100_*Fi*.xml) ; IR corrigés dans results/ead/ead_cor/fi_geo/ ;
  - mnesys : tous les IR (data/ead/mnesys/FRAM59100_*.xml) ; IR corrigés dans results/ead/ead_cor/mnesys_geo/.

Fichiers de propositions : results/ead/indexation/<série>_<famille>_propositions_<date>.csv, avec les familles
« quartiers », « reprise » (concordance BnR et territoires), « voies », « sites » et « territoires » (communes et
pays absents du thésaurus des territoires), dans cet ordre : chaque famille exclut les formes des familles
précédentes et de toutes les séries précédentes (fi avant mnesys) ; « territoires » reprend en outre les formes
de communes ou de pays que les fichiers « sites » ont listées sans proposition (voir propositions_a_exclure).
Les décisions (colonne decision) sont globales : mnesys_geo_applique.py les applique à tous les IR où la forme apparaît.

Table de décisions cumulative : data/geo/concordance_mnesys_lieux.csv, alimentée en plusieurs fois par
mnesys_geo_verse.py à partir des décisions saisies dans les fichiers de propositions. Elle prime sur ces fichiers
(mnesys_geo_applique.py) et ses formes sont exclues des propositions régénérées : on peut donc régénérer des
propositions (après un réimport Mnesys par exemple) sans perdre les décisions versées.
"""
import csv
import glob
import re
from collections import Counter, defaultdict
from os.path import basename, exists, join

from lxml import etree

MNESYS = join("data", "ead", "mnesys")
INDEXATION = join("results", "ead", "indexation")
FAMILLES = ["quartiers", "reprise", "voies", "sites", "territoires"]
SERIES = {
    "fi": {
        "ir": join(MNESYS, "FRAM59100_*Fi*.xml"),
        "destination": join("results", "ead", "ead_cor", "fi_geo"),
        "rapport": join(INDEXATION, "fi_geo_rapport.csv"),
    },
    "mnesys": {
        "ir": join(MNESYS, "FRAM59100_*.xml"),
        "destination": join("results", "ead", "ead_cor", "mnesys_geo"),
        "rapport": join(INDEXATION, "mnesys_geo_rapport.csv"),
    },
}
QUARTIER = re.compile(r",\s*quartier\b.*$|^quartier\s+", re.I)  # « X, quartier de… » ou « Quartier X »


def lire_formes(serie="fi"):
    """forme -> {occ, ir, src} pour tous les <geogname> non vides des IR de la série ; les IR illisibles
    (esperluettes non échappées) sont ignorés : ils n'ont aucun <geogname>."""
    formes = defaultdict(lambda: {"occ": 0, "ir": Counter(), "src": Counter()})
    for f in sorted(glob.glob(SERIES[serie]["ir"])):
        ir = basename(f).replace("FRAM59100_", "").replace(".xml", "")
        try:
            arbre = etree.parse(f)
        except etree.XMLSyntaxError:
            continue
        for g in arbre.iter("geogname"):
            t = "".join(g.itertext()).strip()
            if t:
                formes[t]["occ"] += 1
                formes[t]["ir"][ir] += 1
                formes[t]["src"][g.get("source", "(aucune)")] += 1
    return formes


def chemin_propositions(serie, famille, date):
    """Fichier de propositions à écrire."""
    return join(INDEXATION, f"{serie}_{famille}_propositions_{date}.csv")


def dernier(serie, famille):
    """Fichier de propositions le plus récent d'une série et d'une famille (ou None)."""
    return max(glob.glob(join(INDEXATION, f"{serie}_{famille}_propositions_*.csv")), default=None)


def propositions_a_exclure(serie, famille):
    """Fichiers dont les formes sont exclues des propositions de (série, famille) : toutes les familles des séries
    précédentes et les familles précédentes de la série."""
    noms = list(SERIES)
    fichiers = [dernier(s, f) for s in noms[:noms.index(serie)] for f in FAMILLES]
    fichiers += [dernier(serie, f) for f in FAMILLES[:FAMILLES.index(famille)]]
    return [f for f in fichiers if f]


def formes_exclues(fichiers):
    """Formes des fichiers de propositions, sauf les lignes inertes des communes ou pays : lignes d'un fichier
    « sites » (nature « commune ou pays »), sans lieu_id ni décision, que la famille « territoires » reprend."""
    exclues = set()
    for fichier in fichiers:
        with open(fichier, encoding="utf-8") as f:
            for r in csv.DictReader(f):
                inerte = r.get("nature") == "commune ou pays" and not r["lieu_id"] and not r["decision"].strip()
                if not inerte:
                    exclues.add(r["forme"])
    return exclues


def propositions_appliquees():
    """Fichiers de propositions à appliquer : le plus récent de chaque série et famille."""
    return [f for s in SERIES for fam in FAMILLES for f in [dernier(s, fam)] if f]


# Table de décisions cumulative (mnesys_geo_verse.py) : une ligne par forme décidée (« oui » ou « non »), alimentée
# en plusieurs fois ; elle l'emporte sur les fichiers de propositions et ses formes en sont exclues.
TABLE = join("data", "geo", "concordance_mnesys_lieux.csv")
COLONNES_TABLE = ["forme", "decision", "lieu_id", "libelle", "thesaurus", "lieu_rejete", "remarque", "validation", "origine",
                  "date_versement", "date_maj"]


def lire_table(chemin=TABLE):
    """forme -> ligne de la table de décisions ({} si la table n'existe pas encore)."""
    if not exists(chemin):
        return {}
    with open(chemin, encoding="utf-8") as f:
        return {r["forme"]: r for r in csv.DictReader(f)}


def formes_decidees():
    """Formes déjà décidées dans la table : exclues des nouveaux fichiers de propositions."""
    return set(lire_table())
