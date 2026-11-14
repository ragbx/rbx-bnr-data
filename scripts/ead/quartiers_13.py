"""
Découpage de Roubaix en 13 quartiers : chaque quartier est l'union de
quartiers MEL (les 31 « quartiers historiques » du référentiel des lieux),
selon les regroupements de l'ancienne liste des quartiers de la BnR
(data/geo/bnr_liste_quartiers_20261112.csv, export du 2026-11-12).

Analyse dérivée : le référentiel des lieux n'est ni lu en écriture ni
modifié. Les polygones de l'ancienne liste servent seulement de contrôle
(chaque polygone doit recouvrir un quartier MEL, IoU ≥ 0,95) ; la géométrie
des 13 quartiers est construite à partir des polygones MEL du référentiel.

Le script répartit aussi chaque rue du référentiel entre les 13 quartiers
et entre les 31 (part de la longueur du tracé dans chaque quartier, sur la
longueur dans Roubaix) et la compare à l'ancienne liste des rues de la BnR
(data/geo/bnr_liste_rues_20261112.csv), où une rue figure dans un ou
plusieurs des 13 quartiers.

Sorties (data/geo/) :
  - quartiers_13.csv : un quartier par ligne, WKT WGS84 ; colonnes
    manuelles libelle_manuel et remarque conservées (clé : lieu_id) ;
  - quartiers_13.gpkg : couches quartiers_13 et quartiers_31 (avec le
    quartier de rattachement) ;
  - rues_quartiers_13_repartition.csv : une ligne par rue du référentiel.

À lancer depuis la racine du dépôt :
    python scripts/ead/quartiers_13.py
"""
import csv
import re
import sqlite3
import sys
from collections import Counter
from os import remove, replace
from os.path import dirname, exists, join

from shapely import make_valid, wkt
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import linemerge, transform, unary_union

sys.path.insert(0, dirname(__file__))
from referentiel_lieux_roubaix import GEO, SORTIE as REFERENTIEL, VERS_L93, VERS_WGS84, WGS84_WKT, blob_gpkg, cle  # noqa: E402

LISTE_QUARTIERS = join(GEO, "bnr_liste_quartiers_20261112.csv")
LISTE_RUES = join(GEO, "bnr_liste_rues_20261112.csv")
SORTIE = join(GEO, "quartiers_13.csv")
SORTIE_GPKG = join(GEO, "quartiers_13.gpkg")
SORTIE_RUES = join(GEO, "rues_quartiers_13_repartition.csv")
IR = join("results", "ead", "ead_cor", "bnr2mnesysgeo")

# Les 13 quartiers : libellé proposé (graphies du référentiel) → quartiers MEL
# du référentiel. Ordre et regroupements = ancienne liste de la BnR ; Espérance
# est rangée avec Anseele… dans les deux listes, mais n'apparaît dans le nom que
# dans la liste des rues ; Hutin et Oran Cartigny sont un seul quartier MEL.
QUARTIERS_13 = [
    ("Alma Gare - Fosse-aux-Chênes", ["Alma Gare", "Fosse-aux-Chênes"]),
    ("Anseele - Centre - Nations Unies - Crouy - Espérance",
     ["Anseele Motte-Bossut", "Centre-Ville", "Nations Unies", "Crouy", "Espérance"]),
    ("Édouard Vaillant - Vauban - Barbieux", ["Édouard Vaillant", "Vauban", "Barbieux"]),
    ("Entrepont - Hutin - Oran Cartigny - Cul-de-Four", ["Entrepont", "Hutin Oran Cartigny", "Cul-de-Four"]),
    ("Épeule - Trichon", ["Épeule", "Trichon"]),
    ("Fresnoy-Mackellerie - Armentières", ["Fresnoy-Mackellerie", "Armentières"]),
    ("Hauts Champs - Nouveau Roubaix - Petites Haies", ["Hauts Champs", "Nouveau Roubaix", "Petites Haies"]),
    ("Hommelet", ["Hommelet"]),
    ("Justice - Fraternité - Linné - Chemin Neuf", ["Justice", "Fraternité", "Linné", "Chemin Neuf"]),
    ("Moulin - Potennerie", ["Moulin", "Potennerie"]),
    ("Pile", ["Pile"]),
    ("Sainte-Élisabeth", ["Sainte-Élisabeth"]),
    ("Sartel Carihem - Trois Ponts", ["Sartel Carihem", "Trois Ponts"]),
]
COLONNES = ["lieu_id", "libelle", "libelle_bnr", "quartiers_31", "nb_quartiers_31", "surface_ha", "coord_wkt",
            "libelle_manuel", "remarque"]
MANUELLES = ["libelle_manuel", "remarque"]
COLONNES_RUES = ["lieu_id", "libelle", "longueur_m", "nb_geogname",
                 "q13_principal", "q13_pct_principal", "q13_nb", "q13_repartition",
                 "q31_principal", "q31_pct_principal", "q31_nb", "q31_repartition",
                 "pct_hors_roubaix", "liste_bnr_libelle", "liste_bnr_quartiers", "accord_liste_bnr"]
# Noms de l'ancienne liste BnR (clé) → clé du quartier MEL du référentiel.
ALIAS_BNR = {"anseele": cle("Anseele Motte-Bossut"), "centre": cle("Centre-Ville"),
             "hutin": cle("Hutin Oran Cartigny"), "oran cartigny": cle("Hutin Oran Cartigny")}
SEUIL_CERTAIN = 95.0   # part de la longueur au-delà de laquelle la rue est « dans » un quartier
SEUIL_TRAVERSE = 20.0  # longueur (m) au-delà de laquelle un quartier est compté comme traversé
POINT = re.compile(r"\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]")


def lire_csv(f, **kw):
    with open(f, encoding=kw.pop("encoding", "utf-8"), newline="") as fh:
        return list(csv.DictReader(fh, **kw))


def mel_de(nom, mel):
    """Quartier MEL du référentiel désigné par un nom de l'ancienne liste BnR (quartier, ou
    regroupement par son premier nom : « Moulin - Potennerie » → Moulin)."""
    k = cle(nom.split(" - ")[0])
    k = ALIAS_BNR.get(k, k)
    return next((lib for lib in mel if cle(lib) == k), None)


def inverser(libelle):
    """« Pierre Motte, rue » → « rue Pierre Motte » (forme de l'ancien thésaurus)."""
    m = re.match(r"^(.*?),\s*(.*)$", libelle.strip())
    return f"{m.group(2)} {m.group(1)}" if m else libelle.strip()


def quartiers_mel():
    out = {}
    for r in lire_csv(REFERENTIEL):
        if r["thesaurus"] == "quartiers":
            out[r.get("libelle_manuel") or r["libelle"]] = (r["lieu_id"], make_valid(transform(VERS_L93, wkt.loads(
                r.get("coord_manuel") or r["coord_wkt"]))))
    return out


def controler_liste(mel, membres):
    """Chaque polygone de l'ancienne liste doit recouvrir le quartier MEL attendu (IoU ≥ 0,95)
    et être rangé dans le même regroupement que lui."""
    anomalies = []
    for r in lire_csv(LISTE_QUARTIERS, encoding="latin-1", delimiter="\t"):
        pts = [(float(b), float(a)) for a, b in POINT.findall(r["Valeur"])]
        g = make_valid(transform(VERS_L93, Polygon(pts)))
        meilleur = max(mel, key=lambda m: g.intersection(mel[m][1]).area)
        iou = g.intersection(mel[meilleur][1]).area / g.union(mel[meilleur][1]).area
        tete = mel_de(r["Catégorie"], mel)
        if iou < 0.95 or meilleur != mel_de(r["Libellé"], mel) or not any(
                tete in qs and meilleur in qs for qs in membres.values()):
            anomalies.append(f"{r['Catégorie']} / {r['Libellé']} → {meilleur} (IoU {iou:.3f})")
    return anomalies


def repartition(g, polygones, roubaix):
    """{quartier: part en %} de la longueur du tracé g dans Roubaix, et part hors Roubaix."""
    total = g.length
    dedans = g.intersection(roubaix).length
    parts = {q: g.intersection(p).length for q, p in polygones.items() if g.intersects(p)}
    parts = {q: v for q, v in parts.items() if v > 0}
    pct = {q: round(100 * v / dedans, 1) for q, v in sorted(parts.items(), key=lambda x: -x[1])} if dedans else {}
    return pct, parts, round(100 * (total - dedans) / total, 1) if total else 0.0


def compter_geogname():
    """Nombre de geogname « voies » par libellé dans les IR de test (citations)."""
    from glob import glob
    from lxml import etree
    n = Counter()
    for f in glob(join(IR, "*.xml")):
        for e in etree.parse(f).iter("geogname"):
            if "bnr_geo_rbx_voies" in (e.get("source") or ""):
                n[" ".join("".join(e.itertext()).split())] += 1
    return n


def ecrire_gpkg(lignes, q31):
    temporaire = SORTIE_GPKG + ".tmp"
    if exists(temporaire):
        remove(temporaire)
    con = sqlite3.connect(temporaire)
    con.executescript("""
        PRAGMA application_id = 1196444487;
        PRAGMA user_version = 10300;
        CREATE TABLE gpkg_spatial_ref_sys (srs_name TEXT NOT NULL, srs_id INTEGER PRIMARY KEY,
            organization TEXT NOT NULL, organization_coordsys_id INTEGER NOT NULL, definition TEXT NOT NULL,
            description TEXT);
        CREATE TABLE gpkg_contents (table_name TEXT NOT NULL PRIMARY KEY, data_type TEXT NOT NULL,
            identifier TEXT UNIQUE, description TEXT DEFAULT '', last_change DATETIME NOT NULL DEFAULT
            (strftime('%Y-%m-%dT%H:%M:%fZ','now')), min_x DOUBLE, min_y DOUBLE, max_x DOUBLE, max_y DOUBLE,
            srs_id INTEGER REFERENCES gpkg_spatial_ref_sys(srs_id));
        CREATE TABLE gpkg_geometry_columns (table_name TEXT NOT NULL, column_name TEXT NOT NULL,
            geometry_type_name TEXT NOT NULL, srs_id INTEGER NOT NULL, z TINYINT NOT NULL, m TINYINT NOT NULL,
            PRIMARY KEY (table_name, column_name));
    """)
    con.executemany("INSERT INTO gpkg_spatial_ref_sys VALUES (?, ?, ?, ?, ?, ?)", [
        ("Undefined cartesian SRS", -1, "NONE", -1, "undefined", ""),
        ("Undefined geographic SRS", 0, "NONE", 0, "undefined", ""),
        ("WGS 84 geodetic", 4326, "EPSG", 4326, WGS84_WKT, "longitude/latitude coordinates in decimal degrees"),
    ])
    for table, colonnes, rangs in (("quartiers_13", [c for c in COLONNES if c != "coord_wkt"], lignes),
                                   ("quartiers_31", ["lieu_id", "libelle", "quartier_13", "lieu_id_13"], q31)):
        con.execute(f'CREATE TABLE "{table}" (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom MULTIPOLYGON, '
                    + ", ".join(f'"{c}" TEXT' for c in colonnes) + ")")
        emprises = []
        for r in rangs:
            g = wkt.loads(r["coord_wkt"])
            g = g if g.geom_type == "MultiPolygon" else MultiPolygon([g])
            emprises.append(g.bounds)
            con.execute(f'INSERT INTO "{table}" VALUES (NULL, ?, {", ".join("?" * len(colonnes))})',
                        [blob_gpkg(g)] + [str(r[c]) if r[c] not in ("", None) else None for c in colonnes])
        minx, miny = min(e[0] for e in emprises), min(e[1] for e in emprises)
        maxx, maxy = max(e[2] for e in emprises), max(e[3] for e in emprises)
        con.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, "
                    "srs_id) VALUES (?, 'features', ?, ?, ?, ?, ?, 4326)", (table, table, minx, miny, maxx, maxy))
        con.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', 'MULTIPOLYGON', 4326, 0, 0)", (table,))
    con.commit()
    con.close()
    replace(temporaire, SORTIE_GPKG)


def sans_interstices(g, aire_max=100.0):
    """Bouche les trous de moins de aire_max m² laissés entre deux polygones MEL voisins
    (un seul aujourd'hui : 2 m² entre les sous-quartiers de Hauts Champs)."""
    def boucher(p):
        return Polygon(p.exterior, [i for i in p.interiors if Polygon(i).area >= aire_max])
    if g.geom_type == "Polygon":
        return boucher(g)
    return MultiPolygon([boucher(p) for p in g.geoms])


def wkt_wgs84(g):
    return wkt.dumps(transform(VERS_WGS84, g), rounding_precision=6)


def main():
    mel = quartiers_mel()
    membres = dict(QUARTIERS_13)
    manquants = sorted(set(mel) - {q for _, qs in QUARTIERS_13 for q in qs})
    inconnus = sorted({q for _, qs in QUARTIERS_13 for q in qs} - set(mel))
    if manquants or inconnus:
        sys.exit(f"regroupements incomplets : non rangés {manquants}, inconnus {inconnus}")
    anomalies = controler_liste(mel, membres)
    for a in anomalies:
        print("contrôle ancienne liste :", a)

    anciennes = {r["lieu_id"]: r for r in lire_csv(SORTIE)} if exists(SORTIE) else {}
    lignes, q31, poly13 = [], [], {}
    for i, (lib13, qs) in enumerate(QUARTIERS_13, 1):
        g = sans_interstices(make_valid(unary_union([mel[q][1] for q in qs]).buffer(0.01).buffer(-0.01)))
        lieu_id = f"bnr:quartiers_13:{i:02d}"
        poly13[lib13] = g
        r = {"lieu_id": lieu_id, "libelle": lib13, "libelle_bnr": "", "quartiers_31": " | ".join(qs),
             "nb_quartiers_31": len(qs), "surface_ha": round(g.area / 10000, 1), "coord_wkt": wkt_wgs84(g)}
        r.update({c: anciennes.get(lieu_id, {}).get(c, "") for c in MANUELLES})
        lignes.append(r)
        for q in qs:
            q31.append({"lieu_id": mel[q][0], "libelle": q, "quartier_13": lib13, "lieu_id_13": lieu_id,
                        "coord_wkt": wkt_wgs84(mel[q][1])})
    du_mel = {q: lib13 for lib13, qs in QUARTIERS_13 for q in qs}
    noms_bnr = {}
    for f in (LISTE_QUARTIERS, LISTE_RUES):
        for r in lire_csv(f, encoding="latin-1", delimiter="\t"):
            if r["Catégorie"].strip():
                noms_bnr.setdefault(du_mel[mel_de(r["Catégorie"], mel)], set()).add(r["Catégorie"].strip())
    for r in lignes:
        r["libelle_bnr"] = " | ".join(sorted(noms_bnr.get(r["libelle"], [])))

    with open(SORTIE, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, COLONNES)
        w.writeheader()
        w.writerows(lignes)
    ecrire_gpkg(lignes, q31)

    # Rues : répartition 13 / 31 et comparaison avec l'ancienne liste des rues.
    roubaix = unary_union(list(poly13.values()))
    p31 = {q: g for q, (_, g) in mel.items()}
    liste = {}
    for r in lire_csv(LISTE_RUES, encoding="latin-1", delimiter="\t"):
        e = liste.setdefault(cle(inverser(r["Libellé"])), {"libelle": r["Libellé"].strip(), "quartiers": set()})
        if r["Catégorie"].strip():
            e["quartiers"].add(du_mel[mel_de(r["Catégorie"], mel)])
    citations = compter_geogname()
    rues, vues = [], set()
    for r in lire_csv(REFERENTIEL):
        if r["thesaurus"] != "rues":
            continue
        lib = r.get("libelle_manuel") or r["libelle"]
        w = r.get("coord_manuel") or r["coord_wkt"]
        out = {c: "" for c in COLONNES_RUES}
        out.update(lieu_id=r["lieu_id"], libelle=lib, nb_geogname=citations.get(lib, 0))
        if w:
            g = transform(VERS_L93, wkt.loads(w))
            g = linemerge(g) if g.geom_type == "MultiLineString" else g
            out["longueur_m"] = round(g.length)
            for prefixe, polys in (("q13", poly13), ("q31", p31)):
                pct, longueurs, hors = repartition(g, polys, roubaix)
                if pct:
                    q0 = next(iter(pct))
                    out.update({f"{prefixe}_principal": q0, f"{prefixe}_pct_principal": pct[q0],
                                f"{prefixe}_nb": sum(v > SEUIL_TRAVERSE for v in longueurs.values()) or 1,
                                f"{prefixe}_repartition": " | ".join(f"{q} {p} %" for q, p in pct.items())})
                out["pct_hors_roubaix"] = hors
            k = cle(lib)
            if k in liste:
                vues.add(k)
                anc = liste[k]["quartiers"]
                traverses = {q for q, v in repartition(g, poly13, roubaix)[1].items() if v > SEUIL_TRAVERSE}
                out["liste_bnr_libelle"] = liste[k]["libelle"]
                out["liste_bnr_quartiers"] = " | ".join(sorted(anc))
                out["accord_liste_bnr"] = ("sans quartier" if not anc else "identique" if anc == traverses
                                           else "liste incomplète" if anc < traverses
                                           else "liste en trop" if traverses < anc else "différent")
        rues.append(out)
    with open(SORTIE_RUES, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, COLONNES_RUES)
        w.writeheader()
        w.writerows(rues)

    # Bilan.
    print(f"{len(lignes)} quartiers (13) → {SORTIE}, {SORTIE_GPKG}")
    print(f"{len(rues)} rues → {SORTIE_RUES} ; ancienne liste : {len(liste)} rues, "
          f"{len(vues)} retrouvées dans le référentiel")
    print("accord avec l'ancienne liste :", dict(Counter(r["accord_liste_bnr"] for r in rues if r["accord_liste_bnr"])))
    avec = [r for r in rues if r["q13_pct_principal"] != ""]
    for prefixe in ("q31", "q13"):
        sures = [r for r in avec if float(r[f"{prefixe}_pct_principal"]) >= SEUIL_CERTAIN]
        n_tot = sum(r["nb_geogname"] for r in avec)
        n_sur = sum(r["nb_geogname"] for r in sures)
        print(f"{prefixe} : {len(sures)}/{len(avec)} rues à ≥ {SEUIL_CERTAIN:.0f} % dans un quartier "
              f"({100 * len(sures) / len(avec):.1f} %) ; citations : {n_sur}/{n_tot} ({100 * n_sur / n_tot:.1f} %)")


if __name__ == "__main__":
    main()
