"""Sites sans point : propositions de localisation pour les sites candidats que le référentiel
ne connaît pas (méthode « sites », suite de sites_candidats.py ; 2026-11-14).

Entrées :
  - data/geo/propositions_sites_20261112.csv et _notices.csv (sites_candidats.py) : sites
    « sans point » ayant au moins un lien retenu (sujet ou lieu) ; rues indexées dans leurs
    notices ; adresses géocodées des notes ;
  - data/geo/referentiel_lieux_roubaix.csv et _ecartes.csv : rues et points ;
  - data/geo/osm_roubaix_lieux.json : lieux nommés d'OpenStreetMap dans Roubaix (Overpass,
    --extraire-osm pour le refaire).

Pour chaque site :
  1. groupe : sites de même famille dont les mots distinctifs sont égaux, ou inclus les uns
     dans les autres (« usine motte bossut » / « usine motte ») — inclusion à vérifier ;
  2. candidats, du plus sûr au moins sûr :
       - rue du référentiel au même nom (« Square Pierre Catteau » : déjà une rue) ;
       - point du référentiel ou écarté dont le nom contient les mots distinctifs du site ;
       - lieu OSM dont le nom contient les mots distinctifs du site ;
       - adresse « n° + voie » des notes, géocodée par la BAN (sites_candidats.py) ;
       - rue la plus indexée avec le site dans ses notices : point au milieu de la rue,
         **approximatif** (la rue n'est qu'un indice) ;
  3. proposition = premier candidat ; alerte si un candidat est à plus de 500 m de la rue
     co-indexée.

Sorties (data/geo/) : propositions_sites_sans_point_<date>.csv (une ligne par site, colonnes
manuelles decision, lieu_manuel, coord_manuel, remarque conservées d'une exécution à l'autre,
clé site_cle) et .gpkg (couches propositions, osm_candidats).

    python scripts/ead/sites_sans_point.py [--extraire-osm] [--date AAAAMMJJ]
"""
import argparse
import csv
import json
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from glob import glob
from os.path import dirname, exists, join, realpath

from lxml import etree
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point
from shapely.ops import transform

sys.path.insert(0, dirname(realpath(__file__)))
from sites_candidats import cle_site, distinctifs, famille  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
GEO = join(RACINE, "data/geo")
PROPOSITIONS = join(GEO, "propositions_sites_20261112.csv")
NOTICES = join(GEO, "propositions_sites_20261112_notices.csv")
REFERENTIEL = join(GEO, "referentiel_lieux_roubaix.csv")
ECARTES = join(GEO, "referentiel_lieux_roubaix_ecartes.csv")
OSM = join(GEO, "osm_roubaix_lieux.json")
IR_ACTUELS = join(RACINE, "results/ead/ead_cor/bnr2mnesys")  # IR tels qu'en ligne : liens ARK des notices
NB_EXEMPLES = 3
OVERPASS = "https://overpass-api.de/api/interpreter"
REQUETE_OSM = """[out:json][timeout:180];
area["ref:INSEE"="59512"]["boundary"="administrative"]->.r;
nwr(area.r)["name"][!"highway"][!"route"][!"public_transport"][!"bus"]["railway"!="tram_stop"];
out center tags;"""
DISTANCE_ALERTE_M = 500
# Même site sous deux noms (site_cle → site_cle du nom principal), décisions de l'utilisateur
# du 2026-11-14 : regroupés, et le groupe partage sa meilleure proposition.
MEMES_SITES = {
    "mediatheque grand plage": "mediatheque",            # La Grand'Plage = Médiathèque de Roubaix
    "chateau congo": "chateau vaissier",                # Château du Congo = Château Vaissier (savon du Congo)
    "chateau princes congo": "chateau vaissier",
}
TYPES = ["rue", "point", "OSM", "adresse", "milieu"]  # ordre de préférence des propositions
MANUELLES = ["decision", "lieu_manuel", "coord_manuel", "remarque"]
VERS_L93 = Transformer.from_crs(4326, 2154, always_xy=True).transform
VERS_WGS84 = Transformer.from_crs(2154, 4326, always_xy=True).transform


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def l93(w):
    return transform(VERS_L93, wkt.loads(w)) if w and w.strip() else None


def extraire_osm():
    req = urllib.request.Request(OVERPASS, data=urllib.parse.urlencode({"data": REQUETE_OSM}).encode(),
                                 headers={"User-Agent": "rbx-bnr-data (indexation geographique)"})
    with open(OSM, "w", encoding="utf-8") as f:
        f.write(urllib.request.urlopen(req, timeout=240).read().decode("utf-8"))


def charger_osm():
    lieux = []
    for e in json.load(open(OSM, encoding="utf-8"))["elements"]:
        c = e.get("center") or ({"lat": e["lat"], "lon": e["lon"]} if "lat" in e else None)
        if not c:
            continue
        t = e["tags"]
        nature = next((f"{k}={t[k]}" for k in ("amenity", "historic", "building", "leisure", "tourism", "shop",
                                                "office", "man_made", "landuse", "railway", "place") if k in t), "")
        k = cle_site(t["name"])
        lieux.append({"id": f"osm:{e['type']}/{e['id']}", "nom": t["name"], "nature": nature, "cle": k,
                      "mots": set(k.split()), "point": transform(VERS_L93, Point(c["lon"], c["lat"]))})
    return lieux


def charger_referentiel():
    rues, points = [], []
    for f, statut in ((REFERENTIEL, "référentiel"), (ECARTES, "écarté")):
        for r in lire(f):
            lib = r.get("libelle_manuel") or r["libelle"]
            g = l93(r.get("coord_manuel") or r.get("coord_wkt"))
            k = cle_site(lib)
            l = {"lieu_id": r["lieu_id"], "libelle": lib, "statut": statut, "categorie": r["categorie"],
                 "cle": k, "mots": set(k.split()), "geom": g}
            if r["thesaurus"] == "rues" and statut == "référentiel":
                rues.append(l)
            elif r["thesaurus"] == "points":
                points.append(l)
    return rues, points


def grouper(sites):
    """Union des sites de même famille aux mots distinctifs égaux ou inclus (≥ 1 mot)."""
    parent = {s["site_cle"]: s["site_cle"] for s in sites}

    def racine(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x
    inclus = set()
    for i, a in enumerate(sites):
        for b in sites[i + 1:]:
            da, db = a["dist"], b["dist"]
            # inclusion seulement si le nom le plus court garde 2 mots distinctifs (« usine motte »
            # ne doit pas absorber Motte-Porisse, Rossel-Motte…)
            if a["famille"] == b["famille"] and da and db and (da == db or min(len(da), len(db)) >= 2 and (da <= db or db <= da)):
                parent[racine(b["site_cle"])] = racine(a["site_cle"])
                if da != db:
                    inclus.update((a["site_cle"], b["site_cle"]))
    # Mêmes sites sous des noms sans mot commun (décisions de l'utilisateur)
    for a, b in MEMES_SITES.items():
        if a in parent and b in parent:
            parent[racine(a)] = racine(b)
    groupes = defaultdict(list)
    for s in sites:
        groupes[racine(s["site_cle"])].append(s)
    return groupes, inclus


VOIE = re.compile(r"^(rue|place|avenue|boulevard|quai|chemin|impasse|allee|passage|sentier|cour|cite|residence|"
                  r"parvis|promenade|esplanade|clos|hameau|contour|mail|villa|square) ")


def compatible(site, cle_cand, nature):
    """Candidat acceptable pour un site : pas une voie ni un quartier ; même famille (une église
    pour une église) ou, si la famille du candidat est inconnue, au moins 2 mots distinctifs."""
    if VOIE.match(cle_cand) and site["famille"] != "parc" or nature.startswith(("place=", "landuse=", "shop=")):
        return False  # voies, quartiers, zones ; commerces (« Chapellerie des Beaux Arts »)
    fam = famille(cle_cand) or famille(cle_site(nature.replace("=", " ").replace("_", " ")))
    if fam:
        return fam == site["famille"]
    return len(site["dist"]) >= 2


def milieu(g):
    """Point au milieu d'une ligne (ou de la plus longue partie d'une ligne multiple)."""
    if g.geom_type.startswith("Multi"):
        g = max(g.geoms, key=lambda p: p.length)
    return g.interpolate(0.5, normalized=True) if g.length else g.centroid


def arks():
    """{(IR court, c_id): lien ARK publication:previous} des IR actuels (comme quartiers_31_rues_notices.py)."""
    out = {}
    for f in sorted(glob(join(IR_ACTUELS, "*.xml"))):
        ir = re.sub(r"^FR\d+_", "", f.replace("\\", "/").rsplit("/", 1)[1][:-4])
        for c in etree.parse(f).iter("c"):
            for d in c.findall("dao") + c.findall("daogrp/daoloc"):
                if d.get("role") == "publication:previous" and "ark:/" in (d.get("href") or ""):
                    out[(ir, c.get("id"))] = d.get("href")
                    break
    return out


def exemples(notices, ark):
    """Jusqu'à NB_EXEMPLES notices du site, liens retenus d'abord : {exemple_i, ark_i}."""
    tri = sorted(notices, key=lambda n: (n["lien"] not in ("sujet", "lieu"), n["ir"], n["c_id"]))
    vus, out = set(), {}
    for n in tri:
        if (n["ir"], n["c_id"]) in vus:
            continue
        vus.add((n["ir"], n["c_id"]))
        i = len(vus)
        out[f"exemple_{i}"] = f"{n['ir']} ({n['lien']}) : {n['unittitle'][:150]}"
        out[f"ark_{i}"] = ark.get((n["ir"], n["c_id"]), "")
        if i == NB_EXEMPLES:
            break
    return {**{f"{k}_{i}": "" for i in range(1, NB_EXEMPLES + 1) for k in ("exemple", "ark")}, **out}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--extraire-osm", action="store_true", help="refaire l'extrait OSM (Overpass)")
    p.add_argument("--date", default=time.strftime("%Y%m%d"))
    p.add_argument("--reporter", nargs="?", const="", metavar="GPKG",
                   help="reporter les saisies du projet QGIS (validation_sites_sans_point_<date>.gpkg) dans le CSV")
    a = p.parse_args()
    if a.reporter is not None:
        return reporter(a.reporter or join(GEO, f"validation_sites_sans_point_{a.date}.gpkg"),
                        join(GEO, f"propositions_sites_sans_point_{a.date}.csv"))
    if a.extraire_osm or not exists(OSM):
        extraire_osm()
    sortie = join(GEO, f"propositions_sites_sans_point_{a.date}.csv")

    props = [r for r in lire(PROPOSITIONS) if r["proposition"] == "sans point" and int(r["nb_retenus"] or 0) > 0]
    for s in props:
        s["dist"] = distinctifs(s["site_cle"])
    ark = arks()
    notices = defaultdict(list)
    for n in lire(NOTICES):
        notices[n["site_cle"]].append(n)
    rues, points = charger_referentiel()
    rue_par_libelle = {r["libelle"]: r for r in rues}
    osm = charger_osm()
    groupes, inclus = grouper(props)
    anciennes = {r["site_cle"]: r for r in lire(sortie)} if exists(sortie) else {}

    lignes = []
    for chef, membres in groupes.items():
        for s in membres:
            d = s["dist"]
            # rues co-indexées dans les notices du site (retenues d'abord)
            compte = Counter()
            for n in notices[s["site_cle"]]:
                for r in [x for x in n["rues"].split(" | ") if x]:
                    compte[r] += 2 if n["lien"] in ("sujet", "lieu") else 1
            rue_co, poids = (compte.most_common(1)[0] if compte else ("", 0))
            g_rue = rue_par_libelle[rue_co]["geom"] if rue_co in rue_par_libelle else None

            candidats = []  # (type, libellé, identifiant, géométrie L93)
            meme_rue = [r for r in rues if r["cle"] == s["site_cle"] or (d and r["mots"] >= d and
                        s["famille"] in ("parc",) and re.match(r"(square|jardin|parc) ", r["cle"]))]
            for r in meme_rue:
                candidats.append(("rue du référentiel", r["libelle"], r["lieu_id"], r["geom"]))
            if d:
                for pt in points:
                    if pt["mots"] >= d and pt["geom"] is not None and compatible(s, pt["cle"], pt["categorie"]):
                        candidats.append((f"point {pt['statut']} ({pt['categorie']})", pt["libelle"], pt["lieu_id"], pt["geom"]))
                for o in osm:
                    if o["mots"] >= d and compatible(s, o["cle"], o["nature"]):
                        candidats.append((f"OSM ({o['nature']})", o["nom"], o["id"], o["point"]))
            else:
                # site sans nom propre (« Médiathèque de Roubaix » → « mediatheque ») : nom identique
                for o in osm:
                    if o["cle"] == s["site_cle"] and not o["nature"].startswith(("place=", "shop=")):
                        candidats.append((f"OSM ({o['nature']})", o["nom"], o["id"], o["point"]))
            if s["adresse_wkt"]:
                candidats.append(("adresse des notes (BAN)", s["adresse_note"], "", l93(s["adresse_wkt"])))
            if g_rue is not None:
                candidats.append(("milieu de la rue co-indexée (approximatif)", rue_co, rue_par_libelle[rue_co]["lieu_id"], milieu(g_rue)))
            # plus d'un candidat point/OSM : celui le plus proche de la rue co-indexée d'abord
            if g_rue is not None:
                candidats.sort(key=lambda c: (["rue", "point", "OSM", "adresse", "milieu"].index(
                    next(t for t in ["rue", "point", "OSM", "adresse", "milieu"] if c[0].startswith(t))),
                    c[3].distance(g_rue) if c[3] is not None else 1e9))
            choix = candidats[0] if candidats else None
            alertes = []
            if s["site_cle"] in inclus:
                alertes.append("groupe par inclusion de noms : vérifier qu'il s'agit du même site")
            if choix and g_rue is not None and choix[3] is not None and choix[3].distance(g_rue) > DISTANCE_ALERTE_M:
                alertes.append(f"proposition à {choix[3].distance(g_rue):.0f} m de la rue co-indexée ({rue_co})")
            if len({c[2] for c in candidats if c[0].startswith(("point", "OSM"))}) > 1:
                alertes.append("plusieurs points ou lieux OSM possibles")
            pt = choix[3] if choix else None
            if pt is not None and pt.geom_type != "Point":
                pt = pt.representative_point() if pt.geom_type.endswith("Polygon") else milieu(pt)
            ancien = anciennes.get(s["site_cle"], {})
            lignes.append({
                "groupe": chef, "site_cle": s["site_cle"], "famille": s["famille"], "forme": s["forme"],
                "nb_notices": s["nb_notices"], "nb_retenus": s["nb_retenus"], "variantes": s["variantes"],
                "rue_coindexee": rue_co, "poids_rue": poids,
                "proposition": choix[0] if choix else "aucune piste",
                "libelle_propose": choix[1] if choix else "", "id_propose": choix[2] if choix else "",
                "coord_wkt": wkt.dumps(transform(VERS_WGS84, pt), rounding_precision=6, trim=True) if pt is not None else "",
                "autres_candidats": " | ".join(f"{c[0]} : {c[1]}" for c in candidats[1:6]),
                "alertes": " ; ".join(alertes), "dates_note": s["dates_note"], "exemple": s["exemple"], **exemples(notices[s["site_cle"]], ark),
                **{m: ancien.get(m, "") for m in MANUELLES},
            })
    # Un groupe = un site : chaque membre reçoit la meilleure proposition du groupe (type le plus
    # sûr, puis le plus de liens retenus) ; sa propre piste reste dans autres_candidats.
    def priorite(l):
        t = next((i for i, x in enumerate(TYPES) if l["proposition"].startswith(x)), len(TYPES))
        return (t, -int(l["nb_retenus"]))
    par_groupe = defaultdict(list)
    for l in lignes:
        par_groupe[l["groupe"]].append(l)
    for membres in par_groupe.values():
        meilleur = min(membres, key=priorite)
        for l in membres:
            if l is not meilleur and meilleur["proposition"] != "aucune piste" and priorite(l)[0] > priorite(meilleur)[0]:
                propre = f"{l['proposition']} : {l['libelle_propose']}" if l["libelle_propose"] else ""
                l.update({k: meilleur[k] for k in ("proposition", "libelle_propose", "id_propose", "coord_wkt")})
                l["proposition"] += " — reprise du groupe"
                l["autres_candidats"] = " | ".join(x for x in (propre, l["autres_candidats"]) if x)
    rang = {g: -sum(int(x["nb_retenus"]) for x in m) for g, m in groupes.items()}
    lignes.sort(key=lambda l: (rang[l["groupe"]], l["groupe"], -int(l["nb_retenus"])))
    with open(sortie, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(lignes[0]))
        w.writeheader()
        w.writerows(lignes)
    ecrire_gpkg(sortie.replace(".csv", ".gpkg"), lignes, osm, props)

    par = Counter(l["proposition"].split(" (")[0] for l in lignes)
    print(f"{len(lignes)} sites sans point (liens retenus : {sum(int(l['nb_retenus']) for l in lignes)}), "
          f"{len(groupes)} groupes → {sortie}")
    for k, v in par.most_common():
        print(f"  {v:4d}  {k}")
    print(f"  alertes : {sum(1 for l in lignes if l['alertes'])}")


def lire_point_gpkg(blob):
    """Géométrie d'un blob GeoPackage (en-tête GP, enveloppe facultative) → shapely."""
    from shapely import wkb
    if not blob:
        return None
    taille_env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(blob[3] >> 1) & 0b111]
    return wkb.loads(bytes(blob[8 + taille_env:]))


def reporter(gpkg, csv_chemin):
    """Saisies QGIS → colonnes manuelles du CSV : decision, lieu_manuel, remarque ; coord_manuel si le
    point a été déplacé (plus d'environ 1 m de coord_origine). Sauvegarde du CSV avant écriture."""
    import shutil
    con = sqlite3.connect(gpkg)
    saisies = {r[0]: r[1:] for r in con.execute(
        "SELECT site_cle, decision, lieu_manuel, remarque, coord_origine, geom FROM sites_a_valider")}
    con.close()
    lignes = lire(csv_chemin)
    shutil.copy(csv_chemin, csv_chemin.replace(".csv", time.strftime(".backup_%Y%m%d_%H%M%S_avant_report.csv")))
    n_dec = n_dep = 0
    for l in lignes:
        if l["site_cle"] not in saisies:
            continue
        decision, lieu, remarque, origine, blob = saisies[l["site_cle"]]
        for col, v in (("decision", decision), ("lieu_manuel", lieu), ("remarque", remarque)):
            if v is not None and v != "" and v != l[col]:
                l[col] = v
        n_dec += bool(decision)
        g = lire_point_gpkg(blob)
        if g is not None and origine and g.distance(wkt.loads(origine)) > 1e-5:
            l["coord_manuel"] = wkt.dumps(g, rounding_precision=6, trim=True)
            n_dep += 1
    with open(csv_chemin, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(lignes[0]))
        w.writeheader()
        w.writerows(lignes)
    print(f"report → {csv_chemin} : {n_dec} décisions, {n_dep} points déplacés (coord_manuel)")


def ecrire_gpkg(chemin, lignes, osm, props):
    """GeoPackage minimal (sans GDAL) : couche propositions (points WGS84) et osm_candidats."""
    import os
    if exists(chemin):
        os.remove(chemin)
    from shapely import wkb
    con = sqlite3.connect(chemin)
    c = con.cursor()
    c.execute("PRAGMA application_id = 1196444487")
    c.execute("PRAGMA user_version = 10200")
    c.executescript("""
    CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT NOT NULL, srs_id INTEGER PRIMARY KEY, organization TEXT NOT NULL,
      organization_coordsys_id INTEGER NOT NULL, definition TEXT NOT NULL, description TEXT);
    CREATE TABLE gpkg_contents (table_name TEXT PRIMARY KEY, data_type TEXT NOT NULL, identifier TEXT UNIQUE,
      description TEXT DEFAULT '', last_change DATETIME NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE, srs_id INTEGER);
    CREATE TABLE gpkg_geometry_columns (table_name TEXT NOT NULL, column_name TEXT NOT NULL, geometry_type_name TEXT NOT NULL,
      srs_id INTEGER NOT NULL, z TINYINT NOT NULL, m TINYINT NOT NULL, CONSTRAINT pk_geom_cols PRIMARY KEY (table_name, column_name));
    """)
    c.execute("INSERT INTO gpkg_spatial_ref_sys VALUES ('Undefined cartesian SRS',-1,'NONE',-1,'undefined',NULL)")
    c.execute("INSERT INTO gpkg_spatial_ref_sys VALUES ('Undefined geographic SRS',0,'NONE',0,'undefined',NULL)")
    c.execute("INSERT INTO gpkg_spatial_ref_sys VALUES ('WGS 84',4326,'EPSG',4326,"
              "'GEOGCS[\"WGS 84\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],"
              "UNIT[\"degree\",0.0174532925199433],AUTHORITY[\"EPSG\",\"4326\"]]',NULL)")

    def blob(g):
        if g is None:
            return None
        return b"GP" + bytes([0, 1]) + (4326).to_bytes(4, "little") + wkb.dumps(g)

    def couche(nom, colonnes, rangs):
        c.execute(f'CREATE TABLE "{nom}" (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom POINT, '
                  + ", ".join(f'"{k}" TEXT' for k in colonnes) + ")")
        c.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, srs_id) VALUES (?, 'features', ?, 4326)", (nom, nom))
        c.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', 'POINT', 4326, 0, 0)", (nom,))
        for g, valeurs in rangs:
            c.execute(f'INSERT INTO "{nom}" (geom, ' + ", ".join(f'"{k}"' for k in colonnes) + ") VALUES (?, "
                      + ", ".join("?" for _ in colonnes) + ")", [blob(g)] + [str(valeurs.get(k, "")) for k in colonnes])

    colonnes = [k for k in lignes[0] if k != "coord_wkt"]
    couche("propositions", colonnes, [(wkt.loads(l["coord_wkt"]) if l["coord_wkt"] else None, l) for l in lignes])
    mots_sites = [s["dist"] for s in props if s["dist"]]
    utiles = [o for o in osm if any(o["mots"] >= d for d in mots_sites)]
    couche("osm_candidats", ["id", "nom", "nature"],
           [(transform(VERS_WGS84, o["point"]), o) for o in utiles])
    con.commit()
    con.close()


if __name__ == "__main__":
    main()
