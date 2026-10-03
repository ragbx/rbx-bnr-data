"""
Extrait tous les <geogname> des IR dans un CSV unique, une ligne par élément.

Colonnes : eadid (identifiant de l'IR), c_id (@id du <c> ancêtre le plus
proche, vide si le geogname est indexé au niveau de l'archdesc), geogname
(texte), source (@source), coord (@coord), puis geogname_origine,
source_origine et coord_origine : les mêmes valeurs dans l'état de
référence d'avant les corrections (cf. ORIGINE_CSV). Les attributs absents
donnent une cellule vide.

Un geogname est rapproché de sa version d'origine par (eadid, c_id, texte,
rang du texte dans le composant). Si le texte a été corrigé, repli : dans
un composant où il reste exactement un geogname sans correspondance de
chaque côté, les deux sont appariés.

Dernières colonnes, nom_officiel et filaire_id : pour une rue dont coord
est le tracé d'une voie du filaire officiel, le nom_de_la_rue de cette voie
et les id de ses tronçons (séparés par " | "). Déduits du tracé lui-même
(comparaison des coordonnées), pas du libellé.

Sortie : results/ead/indexation/rbx-bnr_geogname.csv

À lancer depuis la racine du dépôt.
"""
import ast
import csv
import json
from collections import defaultdict
from glob import glob
from os.path import join

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
FILAIRE_SOURCE = join("data", "geo", "filaire_voies_roubaix.geojson")
CSV_SORTIE = join("results", "ead", "indexation", "rbx-bnr_geogname.csv")
# Extraction figée des IR au commit d6ea6ad (2026-09-26), avant l'ajout
# automatique de source="rue"/coord : ne pas régénérer.
ORIGINE_CSV = join("results", "ead", "indexation", "rbx-bnr_geogname_origine.csv")
COLONNES = ["eadid", "c_id", "geogname", "source", "coord"]


def extraire(dossier):
    lignes = []
    for path in sorted(glob(join(dossier, "*.xml"))):
        root = etree.parse(path).getroot()
        eadid = root.xpath("//eadheader/eadid/text()")
        eadid = eadid[0].strip() if eadid else ""
        for geogname in root.iter("geogname"):
            composant = geogname.xpath("ancestor::c[1]")
            lignes.append(
                {
                    "eadid": eadid,
                    "c_id": composant[0].get("id", "") if composant else "",
                    "geogname": "".join(geogname.itertext()).strip(),
                    "source": geogname.get("source", ""),
                    "coord": geogname.get("coord", ""),
                }
            )
    return lignes


def voies_par_trace():
    """Tracé de chaque voie du filaire, au format des coord des IR ([lat, lon],
    un tronçon ou une liste de tronçons) -> (nom_de_la_rue, id des features
    de la voie)."""
    with open(FILAIRE_SOURCE, encoding="utf-8") as f:
        filaire = json.load(f)
    segments, ids = defaultdict(list), defaultdict(list)
    for feature in filaire["features"]:
        nom = feature["properties"]["nom_de_la_rue"]
        g = feature["geometry"]
        parts = g["coordinates"] if g["type"] == "MultiLineString" else [g["coordinates"]]
        segments[nom].extend(tuple((lat, lon) for lon, lat in p) for p in parts)
        ids[nom].append(feature["id"])
    return {tuple(segments[nom]): (nom, " | ".join(ids[nom])) for nom in segments}


def trace(coord):
    """coord de rue -> tuple de tronçons ((lat, lon), ...), None si point."""
    valeurs = ast.literal_eval(coord)
    if isinstance(valeurs[0], (int, float)):
        return None
    if isinstance(valeurs[0][0], (int, float)):
        valeurs = [valeurs]
    return tuple(tuple(tuple(p) for p in seg) for seg in valeurs)


def cles(lignes):
    rangs = defaultdict(int)
    for ligne in lignes:
        base = (ligne["eadid"], ligne["c_id"], ligne["geogname"])
        yield ligne, base + (rangs[base],)
        rangs[base] += 1


if __name__ == "__main__":
    lignes = extraire(EAD_FOLDER)

    with open(ORIGINE_CSV, encoding="utf-8") as f:
        origine = {cle: ligne for ligne, cle in cles(list(csv.DictReader(f)))}

    appariement = {}
    restes_actuels = defaultdict(list)
    for ligne, cle in cles(lignes):
        if cle in origine:
            appariement[id(ligne)] = origine[cle]
        else:
            restes_actuels[cle[:2]].append(ligne)
    utilisees = {id(a) for a in appariement.values()}
    restes_origine = defaultdict(list)
    for avant in origine.values():
        if id(avant) not in utilisees:
            restes_origine[(avant["eadid"], avant["c_id"])].append(avant)
    for composant, actuels in restes_actuels.items():
        if len(actuels) == 1 and len(restes_origine.get(composant, [])) == 1:
            appariement[id(actuels[0])] = restes_origine[composant][0]

    sans_origine = 0
    for ligne in lignes:
        avant = appariement.get(id(ligne))
        if avant is None:
            sans_origine += 1
        ligne["geogname_origine"] = avant["geogname"] if avant else ""
        ligne["source_origine"] = avant["source"] if avant else ""
        ligne["coord_origine"] = avant["coord"] if avant else ""

    traces = voies_par_trace()
    for ligne in lignes:
        voie = None
        if ligne["source"] == "rue" and ligne["coord"]:
            voie = traces.get(trace(ligne["coord"]))
        ligne["nom_officiel"], ligne["filaire_id"] = voie or ("", "")

    with open(CSV_SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=COLONNES + ["geogname_origine", "source_origine", "coord_origine", "nom_officiel", "filaire_id"],
        )
        writer.writeheader()
        writer.writerows(lignes)

    corriges = sum(
        1
        for l in lignes
        if (l["geogname"], l["source"], l["coord"]) != (l["geogname_origine"], l["source_origine"], l["coord_origine"])
    )
    print(f"{len(lignes)} geogname → {CSV_SORTIE}")
    print(f"{corriges} corrigés par rapport à l'origine, {sans_origine} sans correspondance dans l'origine")
    print(f"{sum(1 for l in lignes if l['filaire_id'])} rues rattachées au filaire (filaire_id)")
