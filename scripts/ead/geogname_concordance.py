"""
Concordance des noms de voies de Roubaix (anciens noms -> voie actuelle) et
repérage des appariements geogname <-> voie actuelle potentiellement
anachroniques.

Sources (copies locales dans data/geo/, téléchargées si absentes ou avec
--maj) :
  - topo_roubaix.csv : fichier TOPO de la DGFiP (successeur de FANTOIR),
    entrées de Roubaix (code_dep 59, code_commune 512), via l'API de
    data.economie.gouv.fr. Donne le code voie (identifiant 59512_<code>,
    commun à la BAN et à la BD TOPO) et la date de création de l'article.
  - osm_roubaix_noms.json : voies nommées d'OpenStreetMap à Roubaix, avec
    leurs old_name / was:name (renommages) et alt_name (variantes), via
    l'API Overpass.

Sorties :
  - data/geo/concordance_renommages.csv : table tenue à la main. Le script
    n'y ajoute que les candidats absents (clé : ancien nom + voie actuelle,
    normalisés), avec statut "à vérifier" ; il ne modifie jamais une ligne
    existante.
  - results/ead/indexation/rbx-bnr_appariements_a_verifier.csv : formes du
    référentiel (data/geo/referentiel_geogname.csv) dont le rattachement à
    une voie actuelle est douteux, avec les années des documents indexés :
      * "ancien nom" : la forme correspond à l'ancien nom d'une voie de la
        concordance (la voie actuelle homonyme n'est peut-être pas la bonne) ;
      * "voie créée en <année>" : la voie actuelle homonyme a été créée dans
        TOPO après le chargement initial de 1987, alors que des documents
        sont plus anciens.

À lancer depuis la racine du dépôt :
    python scripts/ead/geogname_concordance.py [--maj]
"""
import csv
import json
import re
import sys
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from glob import glob
from os.path import exists, join

from lxml import etree

GEO = join("data", "geo")
TOPO_CSV = join(GEO, "topo_roubaix.csv")
OSM_JSON = join(GEO, "osm_roubaix_noms.json")
CONCORDANCE_CSV = join(GEO, "concordance_renommages.csv")
REFERENTIEL_CSV = join(GEO, "referentiel_geogname.csv")
GEOGNAME_CSV = join("results", "ead", "indexation", "rbx-bnr_geogname.csv")
EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
SORTIE_CSV = join("results", "ead", "indexation", "rbx-bnr_appariements_a_verifier.csv")

TOPO_URL = "https://data.economie.gouv.fr/api/explore/v2.1/catalog/datasets/topo-fichier-des-entites-topographiques/exports/csv"
OVERPASS_URLS = ["https://overpass-api.de/api/interpreter", "https://overpass.kumi.systems/api/interpreter"]
OVERPASS_REQUETE = """[out:json][timeout:120];
area["ref:INSEE"="59512"]["boundary"="administrative"]->.r;
(
  way(area.r)["highway"]["name"];
  relation(area.r)["type"~"^(street|associatedStreet)$"]["name"];
);
out tags;"""
USER_AGENT = "rbx-bnr-data (referentiel geogname)"
CREATION_INITIALE_TOPO = "19870101"

NATURES_TOPO = {
    "RUE": "rue", "AV": "avenue", "BD": "boulevard", "PL": "place", "IMP": "impasse", "CHE": "chemin",
    "SQ": "square", "ALL": "allee", "QUAI": "quai", "CRS": "cours", "CR": "cour", "CITE": "cite",
    "RES": "residence", "PAS": "passage", "RPT": "rond point", "PRV": "parvis", "MAIL": "mail",
    "PARC": "parc", "SEN": "sentier", "ESP": "esplanade", "PROM": "promenade", "CAR": "carrefour",
    "PONT": "pont", "VC": "",
}
CONCORDANCE_COLONNES = [
    "ancien_nom", "voie_actuelle", "voie_actuelle_id", "type", "date_creation_topo", "source", "statut", "remarque",
]


def reconstruire(nom_brut):
    """'Achille Screpel, rue' -> 'Rue Achille Screpel' (forme BnR à l'envers)."""
    nom_brut = nom_brut.replace("\\'", "'")
    if "," not in nom_brut:
        return nom_brut.strip()
    nom, type_part = (p.strip() for p in nom_brut.split(",", 1))
    if not type_part:
        return nom
    return f"{type_part}{nom}" if type_part.endswith("'") else f"{type_part} {nom}"


def cle(nom):
    """Clé de comparaison : sans accents, casse, ponctuation ni articles."""
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c)).lower()
    nom = re.sub(r"[^a-z0-9]+", " ", nom)
    nom = re.sub(r"\bst\b", "saint", nom)
    nom = re.sub(r"\bste\b", "sainte", nom)
    nom = re.sub(r"\b(de|du|des|la|le|les|l|d)\b", " ", nom)
    return re.sub(r"\s+", " ", nom).strip()


def telecharger(url, donnees=None):
    requete = urllib.request.Request(url, data=donnees, headers={"User-Agent": USER_AGENT})
    return urllib.request.urlopen(requete, timeout=300).read()


def charger_sources(maj):
    if maj or not exists(TOPO_CSV):
        params = urllib.parse.urlencode({"where": "code_dep='59' and code_commune='512'", "delimiter": ";"})
        with open(TOPO_CSV, "wb") as f:
            f.write(telecharger(f"{TOPO_URL}?{params}"))
        print(f"téléchargé : {TOPO_CSV}")
    if maj or not exists(OSM_JSON):
        donnees = urllib.parse.urlencode({"data": OVERPASS_REQUETE}).encode()
        for tentative, url in enumerate(OVERPASS_URLS * 2):
            try:
                brut = json.loads(telecharger(url, donnees))
                break
            except urllib.error.URLError as e:
                print(f"Overpass indisponible ({url} : {e}), nouvel essai")
                time.sleep(10 * (tentative + 1))
        else:
            sys.exit("Overpass injoignable : relancer plus tard.")
        elements = [{"type": e["type"], "id": e["id"], "tags": e["tags"]} for e in brut["elements"]]
        with open(OSM_JSON, "w", encoding="utf-8") as f:
            json.dump({"source": url, "osm3s": brut.get("osm3s", {}), "elements": elements}, f, ensure_ascii=False, indent=1)
        print(f"téléchargé : {OSM_JSON}")

    with open(TOPO_CSV, encoding="utf-8-sig") as f:
        topo = list(csv.DictReader(f, delimiter=";"))
    with open(OSM_JSON, encoding="utf-8") as f:
        osm = json.load(f)["elements"]
    return topo, osm


def index_topo(topo):
    """clé(nom complet) -> [(identifiant 59512_<code>, nom TOPO, date de création)]."""
    index = defaultdict(list)
    for t in topo:
        nature = NATURES_TOPO.get(t["nature_de_voie"], t["nature_de_voie"].lower())
        nom = f"{t['nature_de_voie']} {t['libelle']}".strip()
        index[cle(f"{nature} {t['libelle']}")].append(
            (f"59512_{t['code_voie']}", nom, t["date_creation_de_article"])
        )
    return index


def candidats_osm(osm, topo_index):
    candidats = []
    for e in osm:
        tags = e["tags"]
        nom = tags.get("name")
        if not nom:
            continue
        for attribut, type_ in (("old_name", "renommage"), ("was:name", "renommage"), ("alt_name", "variante")):
            for ancien in (v.strip() for v in tags.get(attribut, "").split(";")):
                if not ancien or cle(ancien) == cle(nom):
                    continue
                topo_voies = topo_index.get(cle(nom), [])
                candidats.append(
                    {
                        "ancien_nom": ancien,
                        "voie_actuelle": nom,
                        "voie_actuelle_id": " | ".join(sorted({v[0] for v in topo_voies})),
                        "type": type_,
                        "date_creation_topo": " | ".join(sorted({v[2] for v in topo_voies})),
                        "source": f"OSM {attribut} ({e['type']}/{e['id']})",
                        "statut": "à vérifier",
                        "remarque": "",
                    }
                )
    return candidats


def maj_concordance(candidats):
    existantes = []
    if exists(CONCORDANCE_CSV):
        with open(CONCORDANCE_CSV, encoding="utf-8") as f:
            existantes = list(csv.DictReader(f))
    vues = {(cle(l["ancien_nom"]), cle(l["voie_actuelle"])) for l in existantes}
    ajouts = []
    for c in candidats:
        k = (cle(c["ancien_nom"]), cle(c["voie_actuelle"]))
        if k not in vues:
            vues.add(k)
            ajouts.append(c)
    with open(CONCORDANCE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CONCORDANCE_COLONNES)
        writer.writeheader()
        writer.writerows(existantes + ajouts)
    return existantes + ajouts, len(ajouts)


def annees_par_composant():
    """(eadid, c_id) -> années citées dans unitdate (@normal, sinon texte)."""
    annees = {}
    for path in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        root = etree.parse(path).getroot()
        eadid = (root.findtext("eadheader/eadid") or "").strip()
        for c in root.iter("c"):
            u = c.find("did/unitdate")
            if c.get("id") and u is not None:
                texte = u.get("normal") or u.text or ""
                annees[(eadid, c.get("id"))] = [int(a) for a in re.findall(r"\b(1[5-9]\d\d|20\d\d)\b", texte)]
    return annees


def appariements_a_verifier(concordance, topo_index):
    anciens = defaultdict(list)
    for l in concordance:
        if l["type"] == "renommage":
            anciens[cle(l["ancien_nom"])].append(l["voie_actuelle"])

    annees = annees_par_composant()
    docs = defaultdict(list)
    with open(GEOGNAME_CSV, encoding="utf-8") as f:
        for l in csv.DictReader(f):
            docs[(l["geogname_origine"], l["source_origine"])].append(annees.get((l["eadid"], l["c_id"]), []))

    with open(REFERENTIEL_CSV, encoding="utf-8") as f:
        referentiel = list(csv.DictReader(f))

    lignes = []
    for r in referentiel:
        k = cle(reconstruire(r["forme_bnr"]))
        voie = r["nom_officiel"]
        topo_voies = topo_index.get(cle(voie) if voie else k, [])
        if not voie and topo_voies:
            voie = " | ".join(sorted({v[1] for v in topo_voies}))
        occ = docs.get((r["forme_bnr"], r["source_bnr"]), [])
        toutes = [a for liste in occ for a in liste]
        debut = min(toutes) if toutes else None

        motifs = []
        if k in anciens:
            motifs.append("ancien nom de : " + " | ".join(sorted(set(anciens[k]))))
        creations = sorted({v[2] for v in topo_voies if v[2] > CREATION_INITIALE_TOPO})
        if creations and (debut is None or debut < int(creations[0][:4])):
            motifs.append(f"voie actuelle créée en {creations[0][:4]} (TOPO)")
        if not motifs:
            continue
        lignes.append(
            {
                "forme_bnr": r["forme_bnr"],
                "source_bnr": r["source_bnr"],
                "occurrences": r["occurrences"],
                "voie_actuelle_homonyme": voie,
                "voie_actuelle_id": " | ".join(sorted({v[0] for v in topo_voies})),
                "motif": " ; ".join(motifs),
                "annee_min": debut or "",
                "annee_max": max(toutes) if toutes else "",
                "documents_sans_annee": sum(1 for liste in occ if not liste),
            }
        )
    lignes.sort(key=lambda l: -int(l["occurrences"]))
    with open(SORTIE_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(lignes[0]) if lignes else ["forme_bnr"])
        writer.writeheader()
        writer.writerows(lignes)
    return lignes


if __name__ == "__main__":
    topo, osm = charger_sources("--maj" in sys.argv)
    topo_index = index_topo(topo)
    concordance, n_ajouts = maj_concordance(candidats_osm(osm, topo_index))
    print(f"{len(topo)} entrées TOPO, {len(osm)} objets OSM")
    print(f"{len(concordance)} lignes dans {CONCORDANCE_CSV} ({n_ajouts} ajoutées)")
    lignes = appariements_a_verifier(concordance, topo_index)
    print(f"{len(lignes)} formes à vérifier → {SORTIE_CSV}")
