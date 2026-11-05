"""
Exporte en GeoJSON les <geogname> géoréférencés d'un ou plusieurs IR, pour les
contrôler dans un SIG (QGIS…).

Une entité par toponyme distinct (même texte, même @source, même @coord) :
toutes ses occurrences dans les IR y sont regroupées. Le @coord des IR, en
[lat, lon], est retourné en [lon, lat] (ordre GeoJSON, WGS84 / EPSG:4326) :

  - [lat, lon]                    -> Point ;
  - [[lat, lon], …]               -> Polygon pour un quartier (contour refermé
                                     au besoin), LineString sinon (une rue en
                                     boucle reste une ligne) ;
  - [[[lat, lon], …], …]          -> MultiLineString.

Propriétés : libelle, source (@source, sans le préfixe thesaurus--SLASH--),
thesaurus (quartiers, rues, points, communes_pays, ou vide si le <geogname>
n'est pas encore rattaché à un thésaurus), occurrences, unitid (cotes des
composants, séparées par « | »), ir.

Les <geogname> sans @coord (communes et pays…) ou au @coord illisible sont
comptés et listés à l'écran, pas exportés.

QGIS charge un fichier à géométries mixtes en proposant une couche par type
de géométrie (points, lignes, polygones).

À lancer depuis la racine du dépôt :
    python scripts/ead/geogname_ir2geojson.py <IR.xml> [<IR.xml> …] -o <sortie.geojson>
"""
import argparse
import json
from collections import Counter, defaultdict
from os.path import basename

from lxml import etree

PREFIXE = "thesaurus--SLASH--"
THESAURUS = {
    "bnr_geo_rbx_quartiers.xml": "quartiers",
    "bnr_geo_rbx_rues.xml": "rues",
    "bnr_geo_rbx_points.xml": "points",
    "bnr_geo_communes_pays.xml": "communes_pays",
}


def geometrie(coord, quartier):
    """@coord des IR -> géométrie GeoJSON, ou None si illisible."""
    try:
        v = json.loads(coord)
    except ValueError:
        return None
    lon_lat = lambda p: [p[1], p[0]]
    if isinstance(v[0], (int, float)):
        return {"type": "Point", "coordinates": lon_lat(v)}
    if isinstance(v[0][0], (int, float)):
        points = [lon_lat(p) for p in v]
        if quartier:
            if points[0] != points[-1]:
                points.append(points[0])
            return {"type": "Polygon", "coordinates": [points]}
        return {"type": "LineString", "coordinates": points}
    return {"type": "MultiLineString", "coordinates": [[lon_lat(p) for p in t] for t in v]}


def main():
    parser = argparse.ArgumentParser(description=__doc__.strip().split("\n\n")[0])
    parser.add_argument("ir", nargs="+", help="fichiers EAD")
    parser.add_argument("-o", "--sortie", required=True, help="fichier GeoJSON à écrire")
    args = parser.parse_args()

    groupes = defaultdict(lambda: {"unitid": set(), "ir": set(), "occurrences": 0})
    sans_coord, illisibles = Counter(), Counter()
    for chemin in args.ir:
        root = etree.parse(chemin).getroot()
        ir = (root.findtext("eadheader/eadid") or basename(chemin)).strip().removeprefix("FR595126101_")
        for g in root.iter("geogname"):
            texte, source, coord = (g.text or "").strip(), g.get("source", ""), g.get("coord", "")
            if not texte:
                continue
            if not coord:
                sans_coord[source.removeprefix(PREFIXE) or "(aucune)"] += 1
                continue
            c = next(g.iterancestors("c"), None)
            groupe = groupes[(texte, source, coord)]
            groupe["occurrences"] += 1
            groupe["ir"].add(ir)
            if c is not None and c.findtext("did/unitid"):
                groupe["unitid"].add(c.findtext("did/unitid").strip())

    entites, types = [], Counter()
    for (texte, source, coord), groupe in groupes.items():
        court = source.removeprefix(PREFIXE)
        thesaurus = THESAURUS.get(court, "")
        geo = geometrie(coord, quartier=thesaurus == "quartiers" or court == "quartier")
        if geo is None:
            illisibles[texte] += groupe["occurrences"]
            continue
        types[geo["type"]] += 1
        entites.append({
            "type": "Feature",
            "geometry": geo,
            "properties": {
                "libelle": texte, "source": court, "thesaurus": thesaurus,
                "occurrences": groupe["occurrences"], "unitid": " | ".join(sorted(groupe["unitid"])),
                "ir": ", ".join(sorted(groupe["ir"])),
            },
        })
    entites.sort(key=lambda e: (e["properties"]["thesaurus"] == "", e["properties"]["thesaurus"], e["properties"]["libelle"]))
    with open(args.sortie, "w", encoding="utf-8") as f:
        json.dump({"type": "FeatureCollection", "name": basename(args.sortie).rsplit(".", 1)[0],
                   "features": entites}, f, ensure_ascii=False)

    print(f"{len(entites)} entités ({sum(e['properties']['occurrences'] for e in entites)} geogname) → {args.sortie}")
    print(f"géométries : {dict(types)}")
    print(f"par thésaurus : {dict(Counter(e['properties']['thesaurus'] or '(non rattaché)' for e in entites))}")
    print(f"sans @coord (non exportés) : {dict(sans_coord)}")
    if illisibles:
        print(f"@coord illisible (non exportés) : {dict(illisibles)}")


if __name__ == "__main__":
    main()
