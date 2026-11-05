"""
Applique la concordance des formes avec les lieux aux <geogname> des IR, à
titre de test : lit les IR de results/ead/ead_cor/bnr2mnesys/ (non modifiés)
et écrit les IR corrigés dans results/ead/ead_cor/bnr2mnesysgeo/ (dossier
recréé à chaque exécution).

Pour chaque <geogname>, la forme est retrouvée dans
data/geo/concordance_geogname_lieux.csv par son texte et son @source (texte
normalisé : « \\' » → « ' », espaces réduits), puis :

  - forme « aligné » : le <geogname> est réécrit d'après son lieu dans
    data/geo/referentiel_lieux_roubaix.csv ou data/geo/referentiel_communes_pays.csv :
      - texte : libellé du lieu (seul lien du <geogname> au concept du
        thésaurus) ;
      - @source : thésaurus du lieu (bnr_geo_rbx_quartiers.xml,
        bnr_geo_rbx_voies.xml, bnr_geo_rbx_sites.xml ou
        bnr_geo_territoires.xml) ;
      - @coord : géométrie complète du lieu (coord_manuel sinon coord_wkt) en
        WKT, longitude puis latitude, 6 décimales : « POINT (lon lat) »,
        « LINESTRING (…) », « MULTILINESTRING ((…), (…)) », « POLYGON ((…)) »
        (contour refermé) — décision du 2026-11-05. Un lieu sans géométrie
        perd le @coord existant (les @coord des IR ne font pas foi) ;
  - forme « hors référentiel » ou « sans lieu » : <geogname> laissé tel quel,
    sauf son @coord éventuel, converti de l'ancien format JSON [lat, lon] en
    WKT pour que tous les @coord des IR soient au même format ;
  - forme « à retirer » : <geogname> retiré (et son <controlaccess> s'il en
    est vidé) ;
  - forme absente de la concordance (corrigée dans l'IR après l'extraction
    du 2026-09-26) dont le texte est déjà le libellé exact d'un lieu :
    réécrite comme une forme « aligné » sur ce lieu ;
  - <geogname> vide, chronologique ou introuvable : laissé tel quel.

Les autres attributs (role, normal…) sont conservés. Deux formes d'un même
lieu dans un même <controlaccess> (ancien et nouveau nom d'une voie) donnent
deux <geogname> identiques : ils sont gardés (décision du 2026-10-27) et
signalés dans le rapport.

Rapport : results/ead/indexation/<dossier de sortie>_rapport.csv, une ligne
par <geogname> (IR, composant, avant / après, lieu, action).

À lancer depuis la racine du dépôt, dans l'environnement rbx-bnr-data, après
geogname_concordance_lieux.py :
    python scripts/ead/geogname_applique_concordance.py
    python scripts/ead/geogname_applique_concordance.py --ir MED_PHO MED_CP
"""
import argparse
import csv
import json
import shutil
import sys
from collections import Counter
from glob import glob
from os import makedirs
from os.path import abspath, basename, dirname, exists, join

from lxml import etree
from shapely import wkt
from shapely.geometry import LineString, MultiLineString, Point, Polygon

sys.path.insert(0, dirname(__file__))
from geogname_concordance_lieux import A_RETIRER, COMMUNES_PAYS, SORTIE as CONCORDANCE  # noqa: E402
from referentiel_lieux_roubaix import SORTIE as REFERENTIEL  # noqa: E402

csv.field_size_limit(sys.maxsize)

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
DESTINATION = join("results", "ead", "ead_cor", "bnr2mnesysgeo")
INDEXATION = join("results", "ead", "indexation")

SOURCES = {
    "quartiers": "thesaurus--SLASH--bnr_geo_rbx_quartiers.xml",
    "rues": "thesaurus--SLASH--bnr_geo_rbx_voies.xml",
    "points": "thesaurus--SLASH--bnr_geo_rbx_sites.xml",
    "communes_pays": "thesaurus--SLASH--bnr_geo_territoires.xml",
}
COLONNES_RAPPORT = [
    "ir", "c_id", "texte_avant", "source_avant", "coord_avant", "statut_forme", "lieu_id", "thesaurus",
    "texte_apres", "source_apres", "coord_apres", "action", "doublon",
]


def normaliser(texte):
    return " ".join(texte.replace("\\'", "'").split())


def charger():
    """(forme normalisée, @source) -> ligne de la concordance ; lieu_id -> ligne du référentiel ou de communes_pays."""
    with open(CONCORDANCE, encoding="utf-8") as f:
        formes = {(normaliser(l["forme"]), l["source"]): l for l in csv.DictReader(f)}
    lieux = {}
    for fichier in (REFERENTIEL, COMMUNES_PAYS):
        with open(fichier, encoding="utf-8") as f:
            lieux.update({l["lieu_id"]: l for l in csv.DictReader(f)})
    return formes, lieux


TYPES = {"Point": "point", "LineString": "tracé", "MultiLineString": "tracé multiple", "Polygon": "contour"}


def coord_ir(texte_wkt):
    """WKT (lon lat) du référentiel -> (@coord en WKT arrondi à 6 décimales, type)."""
    geo = wkt.loads(texte_wkt)
    if geo.geom_type not in TYPES:
        raise ValueError(f"géométrie non prévue : {geo.geom_type}")
    return wkt.dumps(geo, rounding_precision=6, trim=True), TYPES[geo.geom_type]


def json_vers_wkt(coord, source):
    """Ancien @coord JSON ([lat, lon] imbriqués, contour de quartier non refermé) -> WKT ; None si illisible."""
    try:
        c = json.loads(coord)
    except ValueError:
        return None
    lon_lat = lambda suite: [(lon, lat) for lat, lon in suite]  # noqa: E731
    if not isinstance(c[0], list):
        geo = Point(c[1], c[0])
    elif not isinstance(c[0][0], list):
        geo = Polygon(lon_lat(c)) if source == "quartier" else LineString(lon_lat(c))
    else:
        geo = MultiLineString([lon_lat(l) for l in c])
    return wkt.dumps(geo, rounding_precision=6, trim=True)


def type_coord(coord):
    """Type d'un @coord existant, d'après sa profondeur d'imbrication."""
    if not coord:
        return ""
    profondeur = len(coord.replace(" ", "")) - len(coord.replace(" ", "").lstrip("["))
    return {1: "point", 2: "tracé ou contour"}.get(profondeur, "tracé multiple")


def retirer(element):
    """Retire element de l'arbre en gardant la mise en forme (son tail passe au précédent)."""
    parent, precedent = element.getparent(), element.getprevious()
    if precedent is not None:
        precedent.tail = element.tail
    else:
        parent.text = element.tail
    parent.remove(element)


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("--ir", nargs="*", help="IR à traiter (ex. MED_PHO) ; par défaut tous")
    parser.add_argument("--sortie", default=DESTINATION, help=f"dossier des IR corrigés (défaut : {DESTINATION})")
    args = parser.parse_args()
    if abspath(args.sortie) == abspath(EAD_FOLDER):
        sys.exit("La sortie ne peut pas être le dossier source bnr2mnesys.")
    rapport_csv = join(INDEXATION, f"{basename(args.sortie.rstrip('/'))}_rapport.csv")

    formes, lieux = charger()
    par_libelle = Counter(normaliser(l["libelle"]) for l in lieux.values())
    par_libelle = {normaliser(l["libelle"]): l for l in lieux.values() if par_libelle[normaliser(l["libelle"])] == 1}
    coords = {}
    for lieu_id, lieu in lieux.items():
        geo = lieu["coord_manuel"] or lieu["coord_wkt"]
        coords[lieu_id] = coord_ir(geo) if geo else ("", "")

    if exists(args.sortie):
        shutil.rmtree(args.sortie)
    makedirs(args.sortie)
    chemins = sorted(glob(join(EAD_FOLDER, "*.xml")))
    if args.ir:
        chemins = [c for c in chemins if basename(c)[:-4].removeprefix("FR595126101_") in args.ir]

    actions, par_thesaurus, rapport = Counter(), Counter(), []
    for chemin in chemins:
        tree = etree.parse(chemin)
        root = tree.getroot()
        ir = basename(chemin)[:-4].removeprefix("FR595126101_")
        vus = Counter()
        for g in list(root.iter("geogname")):
            c = next(g.iterancestors("c"), None)
            texte, source, coord = g.text or "", g.get("source", ""), g.get("coord", "")
            ligne = {"ir": ir, "c_id": c.get("id", "") if c is not None else "", "texte_avant": texte,
                     "source_avant": source, "coord_avant": type_coord(coord), "statut_forme": "",
                     "lieu_id": "", "thesaurus": "", "texte_apres": texte, "source_apres": source,
                     "coord_apres": type_coord(coord), "action": "laissé", "doublon": ""}
            rapport.append(ligne)
            forme = formes.get((normaliser(texte), source))
            if forme is None and normaliser(texte) in par_libelle:
                forme = {"statut": "aligné", "lieu_id": par_libelle[normaliser(texte)]["lieu_id"]}
            if not texte.strip() or source == "chrono":
                ligne["action"] = "ignoré (vide ou chrono)"
            elif forme is None:
                ligne["action"] = "laissé (introuvable)"
            elif forme["statut"] == A_RETIRER:
                ligne.update(statut_forme=forme["statut"], texte_apres="", source_apres="", coord_apres="",
                             action="retiré")
                controlaccess = g.getparent()
                retirer(g)
                if controlaccess.tag == "controlaccess" and len(controlaccess) == 0 \
                        and not (controlaccess.text or "").strip():
                    retirer(controlaccess)
            elif forme["statut"] != "aligné":
                ligne["statut_forme"] = forme["statut"]
                if coord and (converti := json_vers_wkt(coord, source)):
                    g.set("coord", converti)
                    ligne["action"] = "laissé (@coord converti en WKT)"
            else:
                lieu = lieux[forme["lieu_id"]]
                nouveau_coord, type_nouveau = coords[lieu["lieu_id"]]
                g.text = lieu["libelle"]
                g.set("source", SOURCES[lieu["thesaurus"]])
                if nouveau_coord:
                    g.set("coord", nouveau_coord)
                elif "coord" in g.attrib:
                    del g.attrib["coord"]
                ligne.update(statut_forme=forme["statut"], lieu_id=lieu["lieu_id"], thesaurus=lieu["thesaurus"],
                             texte_apres=g.text, source_apres=g.get("source"), coord_apres=type_nouveau,
                             action="réécrit" if nouveau_coord else "réécrit sans géométrie")
                par_thesaurus[lieu["thesaurus"]] += 1
                cle = (g.getparent(), lieu["lieu_id"])
                vus[cle] += 1
                if vus[cle] > 1:
                    ligne["doublon"] = "oui"
            actions[ligne["action"]] += 1
        tree.write(join(args.sortie, basename(chemin)), encoding="UTF-8", xml_declaration=True,
                   pretty_print=True, with_comments=True)

    with open(rapport_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_RAPPORT, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rapport)

    print(f"{len(rapport)} geogname dans {len(chemins)} IR → {args.sortie}")
    print(f"actions : {dict(actions.most_common())}")
    print(f"réécrits par thésaurus : {dict(par_thesaurus.most_common())}")
    print(f"doublons dans un même controlaccess : {sum(1 for l in rapport if l['doublon'])}")
    print(f"rapport → {rapport_csv}")


if __name__ == "__main__":
    main()
