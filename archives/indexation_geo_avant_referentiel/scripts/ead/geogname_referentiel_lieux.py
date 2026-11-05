"""
Construit le référentiel géographique des lieux : une ligne par lieu, futur
concept d'un des quatre thésaurus géographiques de la BnR.

Source : la concordance formes ↔ lieux (data/geo/concordance_geogname_lieux.csv,
produite par geogname_concordance_lieux.py), qui relie chaque forme trouvée
dans les IR à un lieu. Les formes d'un même lieu sont regroupées :

  - lieu rattaché à une référence : par son identifiant (ref_id : code voie
    TOPO 59512_<code>, quartier historique MEL, objet BD TOPO, code INSEE…) ;
  - lieu sans référence (voie disparue, lieu hors Nord, pays, à identifier) :
    par son libellé normalisé, avec un identifiant BnR provisoire
    "bnr:<thésaurus>:<libellé normalisé>".

Thésaurus (colonne thesaurus) : quartiers (quartiers de Roubaix), rues (voies de
Roubaix), points (adresses et espaces publics de Roubaix), communes_pays (communes,
départements, régions, pays… hors Roubaix ; seul thésaurus hiérarchisé, par la
colonne parent).

Mnesys relie un <geogname> à un concept par le libellé seul : le libellé doit
être unique dans chaque thésaurus. La colonne coord_wkt donne la géométrie
(coord_manuel, sinon coord) en WKT, WGS84, ordre lon lat, avec son type
(POINT, LINESTRING, MULTILINESTRING, POLYGON) ; coord garde le format des IR
lu par Mnesys ([lat, lon], sans type). Le libellé d'un lieu est celui de ses formes
(libelle_retenu de la concordance) ; si elles divergent, celui de la forme la
plus fréquente, avec un signalement.

La colonne a_traiter liste ce qui reste à régler sur le lieu (libellés
divergents ou en double, pas de géométrie, pas de référence actuelle, formes
encore à traiter dans la concordance…) ; elle se vide dès que validation est
remplie. Les colonnes validation, remarque, coord_manuel et parent, remplies à
la main, sont conservées d'une exécution à l'autre (clé : lieu_id).

Sortie : data/geo/referentiel_geogname.csv.

À lancer depuis la racine du dépôt, après geogname_concordance_lieux.py :
    python scripts/ead/geogname_referentiel_lieux.py
"""
import csv
import json
import sys
from collections import Counter, defaultdict
from os.path import dirname, exists, join

sys.path.insert(0, dirname(__file__))
from geogname_concordance import GEO, cle  # noqa: E402
from geogname_concordance_lieux import libelle_affiche, sans_accents  # noqa: E402
from shapely import wkt as shapely_wkt  # noqa: E402
from shapely.geometry import LineString, MultiLineString, Point, Polygon  # noqa: E402

CONCORDANCE = join(GEO, "concordance_geogname_lieux.csv")
SORTIE = join(GEO, "referentiel_geogname.csv")

THESAURUS = {
    "quartier": "quartiers",
    "voie": "rues",
    "espace public": "points",
    "adresse": "points",
    "commune": "communes_pays",
    "lieu hors Nord": "communes_pays",
    "pays": "communes_pays",
    "à identifier": "communes_pays",
}
ORDRE = ["quartiers", "rues", "points", "communes_pays"]
MANUELLES = ["validation", "remarque", "coord_manuel", "parent"]
COLONNES = [
    "lieu_id", "thesaurus", "libelle", "a_traiter", "nom_ancien", "variantes", "categorie",
    "ref_nom", "ref_origine", "geometrie", "coord", "coord_manuel", "coord_wkt", "parent",
    "occurrences", "ir", "validation", "remarque",
]


def coord_wkt(coord, thesaurus):
    """@coord des IR ([lat, lon]) -> WKT en WGS84, ordre lon lat, type explicite.

    Le type découle du thésaurus et de la structure, pas de la forme des
    coordonnées : un quartier est un POLYGON ; une rue en boucle reste un
    LINESTRING ; plusieurs tronçons donnent un MULTILINESTRING."""
    if not coord:
        return ""
    try:
        v = json.loads(coord)
    except ValueError:
        return ""
    lon_lat = lambda p: (p[1], p[0])
    if isinstance(v[0], (int, float)):
        geo = Point(lon_lat(v))
    elif isinstance(v[0][0], (int, float)):
        points = [lon_lat(p) for p in v]
        geo = Polygon(points) if thesaurus == "quartiers" else LineString(points)
    else:
        geo = MultiLineString([[lon_lat(p) for p in t] for t in v])
    return shapely_wkt.dumps(geo, trim=True)


def lieu_id(forme, thesaurus):
    if forme["ref_id"]:
        return " | ".join(i.strip() for i in forme["ref_id"].split("|"))
    return f"bnr:{thesaurus}:{cle(forme['libelle_retenu']).replace(' ', '_')}"


def construire(formes):
    """Regroupe les formes de la concordance en lieux."""
    groupes = defaultdict(list)
    for f in formes:
        thesaurus = THESAURUS.get(f["categorie"])
        if thesaurus:
            groupes[(thesaurus, lieu_id(f, thesaurus))].append(f)
    # Une forme sans référence dont le libellé est exactement celui d'un lieu rattaché
    # du même thésaurus rejoint ce lieu (signalé dans a_traiter).
    rattaches = {(t, cle(f["libelle_retenu"])): (t, i) for (t, i), liste in groupes.items()
                 if not i.startswith("bnr:") for f in liste}
    par_libelle = {}
    for (t, i) in [g for g in groupes if g[1].startswith("bnr:")]:
        cible = rattaches.get((t, cle(groupes[(t, i)][0]["libelle_retenu"])))
        if cible:
            for f in groupes.pop((t, i)):
                f["par_libelle"] = True
                groupes[cible].append(f)

    lieux = []
    for (thesaurus, ident), liste in groupes.items():
        occ = Counter()
        for f in liste:
            occ[f["libelle_retenu"]] += int(f["occurrences"])
        libelle = occ.most_common(1)[0][0]
        if len({sans_accents(x).lower() for x in occ}) == 1:
            # Variantes de casse ou d'accents seulement : la mieux écrite.
            libelle = max(occ, key=lambda x: (len(x.encode()), sum(c.isupper() for c in x), occ[x]))
        principale = max(liste, key=lambda f: (f["libelle_retenu"] == libelle, int(f["occurrences"])))
        avec_coord = [f for f in sorted(liste, key=lambda f: -int(f["occurrences"])) if f["coord"]]
        anciens = sorted({f["nom_ancien"] for f in liste if f["nom_ancien"]})
        variantes = sorted(
            ({libelle_affiche(f["forme_bnr"]) for f in liste} | set(occ)) - {libelle} - set(anciens)
        )
        lieux.append({
            "lieu_id": ident, "thesaurus": thesaurus, "libelle": libelle,
            "nom_ancien": " | ".join(anciens), "variantes": " | ".join(variantes),
            "categorie": principale["categorie"], "ref_nom": principale["ref_nom"],
            "ref_origine": principale["ref_origine"], "geometrie": principale["geometrie"],
            "coord": (avec_coord[0]["coord"] if avec_coord else ""),
            "coord_manuel_formes": next((f["coord_manuel"] for f in liste if f["coord_manuel"]), ""),
            "occurrences": sum(int(f["occurrences"]) for f in liste),
            "ir": ", ".join(sorted({i.strip() for f in liste for i in f["ir"].split(",") if i.strip()})),
            "libelles_formes": occ, "formes_a_traiter": sum(1 for f in liste if f["a_traiter"]),
            "par_libelle": sum(1 for f in liste if f.get("par_libelle")),
            "cles_formes": [(f["forme_bnr"], f["source_bnr"]) for f in liste],
        })
    return lieux


def a_traiter(lieu, doublons):
    """Ce qui reste à régler sur le lieu ; vide une fois le lieu validé."""
    if lieu["validation"].strip():
        return ""
    motifs = []
    if len({sans_accents(x).lower() for x in lieu["libelles_formes"]}) > 1:
        motifs.append("libellés divergents entre formes : " + " | ".join(sorted(lieu["libelles_formes"])))
    if doublons[(lieu["thesaurus"], lieu["libelle"])] > 1:
        motifs.append("libellé en double dans le thésaurus")
    if lieu["thesaurus"] != "communes_pays" and not (lieu["coord"] or lieu["coord_manuel"]):
        motifs.append("pas de géométrie : saisir coord_manuel")
    if lieu["lieu_id"].startswith("bnr:") and lieu["thesaurus"] != "communes_pays":
        motifs.append("pas de référence actuelle (nom ancien ?)")
    if lieu["categorie"] == "à identifier":
        motifs.append("lieu à identifier")
    if lieu["par_libelle"]:
        motifs.append(f"{lieu['par_libelle']} forme(s) sans référence rattachée(s) au lieu par son seul libellé : vérifier")
    if lieu["formes_a_traiter"]:
        motifs.append(f"{lieu['formes_a_traiter']} forme(s) à traiter dans la concordance")
    return " ; ".join(motifs)


def main():
    with open(CONCORDANCE, encoding="utf-8") as f:
        lieux = construire(list(csv.DictReader(f)))

    manuel = {}
    if exists(SORTIE):
        with open(SORTIE, encoding="utf-8") as f:
            manuel = {l["lieu_id"]: l for l in csv.DictReader(f)}
    for l in lieux:
        garde = manuel.get(l["lieu_id"], {})
        coord_formes = l.pop("coord_manuel_formes")
        for c in MANUELLES:
            l[c] = garde.get(c, "")
        l["coord_manuel"] = l["coord_manuel"] or coord_formes
        l["coord_wkt"] = coord_wkt(l["coord_manuel"] or l["coord"], l["thesaurus"])

    doublons = Counter((l["thesaurus"], l["libelle"]) for l in lieux)
    for l in lieux:
        l["a_traiter"] = a_traiter(l, doublons)

    lieux.sort(key=lambda l: (ORDRE.index(l["thesaurus"]), l["libelle"].lower()))
    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        writer.writerows(lieux)

    print(f"{len(lieux)} lieux → {SORTIE}")
    print(f"{'thésaurus':14s} {'lieux':>6s} {'geogname':>9s} {'géométrie':>10s} {'à traiter':>10s}")
    for t in ORDRE:
        sel = [l for l in lieux if l["thesaurus"] == t]
        print(f"{t:14s} {len(sel):6d} {sum(l['occurrences'] for l in sel):9d} "
              f"{sum(1 for l in sel if l['coord'] or l['coord_manuel']):10d} {sum(1 for l in sel if l['a_traiter']):10d}")


if __name__ == "__main__":
    main()
