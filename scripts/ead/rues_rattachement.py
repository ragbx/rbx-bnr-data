"""
Table de rattachement des rues ambiguës à un quartier, à 31 et à 13 quartiers.

Une rue est ambiguë quand moins de 95 % de sa longueur est dans un seul
quartier (data/geo/rues_quartiers_13_repartition.csv, écrit par
quartiers_13.py). Pour chacune, le script propose un rattachement par
convention, selon ces règles, dans cet ordre :
  1. repère : la rue mène à un site repère, elle va dans le quartier du site
     (REPERES : avenue Jean Lebas → quartier de la gare) ;
  2. sinon : tous les quartiers traversés (5 % ou plus de la longueur).
Règles retirées le 2026-11-13, jugées mauvaises par l'utilisateur :
« éponyme » (rue qui porte le nom d'un quartier → ce quartier) et
« majoritaire » (75 % ou plus de la longueur dans un quartier → ce quartier).

Ordre de priorité décidé le 2026-11-13, pour une notice :
  1. un point (site validé ou adresse géocodée) → le quartier du point,
     toujours, même contre une rue indexée ;
  2. une rue indexée sûre (≥ 95 %) → son quartier ;
  3. sinon la convention de cette table.
La table ne sert donc qu'aux notices sans point. Elle compte, pour chaque
rue, les notices concernées (rue ambiguë, ni point ni autre rue sûre).

Points d'une notice :
  - site : lien sujet ou lieu vers un site validé (decision oui / autre
    point dans propositions_sites_20261112.csv), quartier = quartier_point ;
  - adresse : « n° + voie » du titre ou de la présentation, dont la voie est
    une rue indexée de la notice, géocodée par la BAN (numéro exact, sinon
    numéro voisin de même parité à ±10), point à ≤ 50 m du tracé.

Analyse dérivée : ni le référentiel ni les IR ne sont modifiés.

Sorties (data/geo/) :
  - rues_rattachement.csv : une ligne par rue ambiguë ; colonnes manuelles
    decision (oui / non / à revoir), quartier_31_manuel, quartier_13_manuel,
    remarque, conservées (clé : lieu_id) ; retenu_31 / retenu_13 = saisie
    manuelle, sinon (à 13) quartiers de la saisie à 31, sinon proposition si
    decision = oui ;
  - rues_rattachement.gpkg : couche rues_rattachement (tracés, mêmes
    colonnes), où l'on corrige dans QGIS (projet quartiers_13.qgz). Le script
    relit les colonnes manuelles du CSV puis du GeoPackage (une valeur
    renseignée dans le GeoPackage l'emporte) avant de tout réécrire ; les
    quartiers y sont au format liste de QGIS {"A","B"}, « A | B » dans le CSV ;
  - rues_rattachement_conflits_points.csv : notices dont le point est hors du
    quartier d'une rue sûre indexée (le point l'emporte ; liste pour contrôle).

À lancer depuis la racine du dépôt, après quartiers_13.py :
    python scripts/ead/rues_rattachement.py
"""
import csv
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from glob import glob
from os import remove, replace
from os.path import basename, dirname, exists, join

from lxml import etree
from shapely import wkt
from shapely.geometry import MultiLineString, Point
from shapely.ops import transform, unary_union

sys.path.insert(0, dirname(__file__))
from referentiel_lieux_roubaix import (BAN_CSV, GEO, SORTIE as REFERENTIEL, VERS_L93, WGS84_WKT,  # noqa: E402
                                       blob_gpkg, cle, cle_voie)
from sites_candidats import ADRESSE  # noqa: E402

REPARTITION = join(GEO, "rues_quartiers_13_repartition.csv")
QUARTIERS_13 = join(GEO, "quartiers_13.csv")
SITES = join(GEO, "propositions_sites_20261112.csv")
SITES_NOTICES = join(GEO, "propositions_sites_20261112_notices.csv")
IR = join("results", "ead", "ead_cor", "bnr2mnesysgeo")
SORTIE = join(GEO, "rues_rattachement.csv")
SORTIE_GPKG = join(GEO, "rues_rattachement.gpkg")
SORTIE_CONFLITS = join(GEO, "rues_rattachement_conflits_points.csv")

SEUIL_SURE = 95.0       # part (%) au-delà de laquelle la rue est dans un seul quartier
SEUIL_CANDIDAT = 5.0    # part (%) minimale d'un quartier traversé
DISTANCE_MAX_M = 50     # point d'adresse au-delà : géocodage rejeté
DECISIONS_SITES = {"oui", "autre point"}
LIENS_SITES = {"sujet", "lieu"}
# Rue → site repère (libellé du point dans le référentiel) dont elle prend le quartier.
REPERES = {"Avenue Jean Lebas": "Gare de Roubaix"}
MANUELLES = ["decision", "quartier_31_manuel", "quartier_13_manuel", "remarque"]
COLONNES = ["lieu_id", "libelle", "longueur_m", "nb_geogname", "notices_sans_point_31", "notices_sans_point_13",
            "type_31", "repartition_31", "regle_31", "proposition_31",
            "type_13", "repartition_13", "regle_13", "proposition_13"] + MANUELLES + ["retenu_31", "retenu_13"]
DECISIONS = {"oui", "non", "à revoir"}


def lire_csv(f, **kw):
    with open(f, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh, **kw))


def parts(repartition):
    """« A 52.1 % | B 47.9 % » → [(A, 52.1), (B, 47.9)]."""
    out = []
    for p in repartition.split(" | ") if repartition else []:
        q, pct, _ = p.rsplit(" ", 2)
        out.append((q, float(pct)))
    return out


def candidats(repartition):
    return [q for q, pct in parts(repartition) if pct >= SEUIL_CANDIDAT]


def texte(e):
    return " ".join("".join(e.itertext()).split())


def charger():
    ref = lire_csv(REFERENTIEL)
    rues = {(r.get("libelle_manuel") or r["libelle"]): r for r in ref if r["thesaurus"] == "rues"}
    traces = {k: transform(VERS_L93, wkt.loads(r.get("coord_manuel") or r["coord_wkt"]))
              for k, r in rues.items() if r.get("coord_manuel") or r["coord_wkt"]}
    q31 = {(r.get("libelle_manuel") or r["libelle"]): transform(VERS_L93, wkt.loads(r["coord_wkt"]))
           for r in ref if r["thesaurus"] == "quartiers"}
    points = {(r.get("libelle_manuel") or r["libelle"]): transform(VERS_L93, wkt.loads(
        r.get("coord_manuel") or r["coord_wkt"])) for r in ref if r["thesaurus"] == "points"}
    du13 = {q: r["libelle"] for r in lire_csv(QUARTIERS_13) for q in r["quartiers_31"].split(" | ")}
    cles_rue = {}
    for k, r in rues.items():
        s = {cle_voie(k)}
        for v in [r.get("nom_ban", "")] + (r.get("variantes") or "").split("|"):
            if v.strip():
                s.add(cle_voie(v.strip()))
        cles_rue[k] = s
    ban = defaultdict(dict)
    with open(BAN_CSV, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter=";"):
            ban[cle_voie(r["nom_voie"])][(r["numero"], r["rep"].lower())] = (float(r["x"]), float(r["y"]))
    sites = {r["site_cle"]: r["quartier_point"] for r in lire_csv(SITES) if r["decision"] in DECISIONS_SITES}
    sites_notice = defaultdict(set)
    for r in lire_csv(SITES_NOTICES):
        if r["lien"] in LIENS_SITES and sites.get(r["site_cle"]):
            sites_notice[(r["ir"], r["c_id"])].add(sites[r["site_cle"]])
    return rues, traces, q31, points, du13, cles_rue, ban, sites_notice


def geocoder(num, rep, cles, ban):
    for k in cles:
        numeros = ban.get(k)
        if numeros is None:
            continue
        xy = numeros.get((num, rep)) or numeros.get((num, ""))
        if xy:
            return Point(xy)
        n = int(num)
        voisins = [(abs(int(a) - n), v) for (a, _), v in numeros.items()
                   if a.isdigit() and int(a) % 2 == n % 2 and abs(int(a) - n) <= 10]
        return Point(min(voisins, key=lambda x: x[0])[1]) if voisins else None
    return None


def quartiers_adresses(c, voies, traces, q31, cles_rue, ban):
    """Quartiers (31) des adresses « n° + voie » de la notice portant sur une de ses rues indexées."""
    t = " ".join(texte(x) for x in c.iter("unittitle", "scopecontent"))
    out = set()
    for num, rep, voie in ADRESSE.findall(t):
        voie = re.split(r"\s+(?:à|a|et|puis|en|dans|qui|où|ou|par|pour|avec|sur|entre|jusqu\S*|depuis|-)\s+",
                        voie.strip())[0].strip(" -")
        ka = cle_voie(voie)
        rue = next((v for v in voies if any(ka == k or ka.startswith(k + " ") for k in cles_rue.get(v, ()))), None)
        if rue is None or rue not in traces:
            continue
        pt = geocoder(num, (rep or "").lower(), cles_rue[rue], ban)
        if pt is None or pt.distance(traces[rue]) > DISTANCE_MAX_M:
            continue
        q = next((q for q, g in q31.items() if g.contains(pt)), None)
        if q:
            out.add(q)
    return out


def type_rue(g, limites):
    """« limite » si la moitié au moins du tracé longe (≤ 15 m) une limite de quartier."""
    return "limite" if g.intersection(limites).length / g.length >= 0.5 else "traversante"


def proposer(lib, repartition, niveau, du13, points, q31):
    """(règle, proposition) pour une rue ambiguë à un niveau (31 ou 13)."""
    cands = candidats(repartition)
    if lib in REPERES and REPERES[lib] in points:
        pt = points[REPERES[lib]]
        q = next((q for q, g in q31.items() if g.contains(pt)), None)
        q = q if niveau == "31" else du13.get(q)
        if q in cands:
            return f"repère : {REPERES[lib]}", q
    return "tous traversés", " | ".join(cands)


def liste_quartiers(valeur):
    """Quartiers saisis : « A | B » (CSV) ou « {A,B} » / « {"A","B"} » (liste à cocher de QGIS)."""
    v = (valeur or "").strip()
    if v.startswith("{") and v.endswith("}"):
        return [x.strip() for x in next(csv.reader([v[1:-1]], skipinitialspace=True)) if x.strip()]
    return [x.strip() for x in v.split("|") if x.strip()]


def format_qgis(quartiers):
    return "{" + ",".join(f'"{q}"' for q in quartiers) + "}" if quartiers else ""


def lire_saisies():
    """Colonnes manuelles, par lieu_id : celles du CSV, remplacées par celles du GeoPackage quand elles y
    sont renseignées (saisie dans QGIS). Les quartiers sont rendus en liste."""
    saisies = {r["lieu_id"]: {c: r.get(c, "") for c in MANUELLES} for r in lire_csv(SORTIE)} if exists(SORTIE) else {}
    if exists(SORTIE_GPKG):
        con = sqlite3.connect(f"file:{SORTIE_GPKG}?mode=ro", uri=True)
        for row in con.execute(f'SELECT lieu_id, {", ".join(MANUELLES)} FROM rues_rattachement'):
            d = saisies.setdefault(row[0], {c: "" for c in MANUELLES})
            for c, v in zip(MANUELLES, row[1:]):
                if v not in (None, ""):
                    d[c] = str(v).strip()
        con.close()
    for d in saisies.values():
        for n in ("31", "13"):
            d[f"quartier_{n}_manuel"] = liste_quartiers(d.get(f"quartier_{n}_manuel"))
    return saisies


def ecrire_gpkg(lignes, traces):
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
    entiers = {"longueur_m", "nb_geogname", "notices_sans_point_31", "notices_sans_point_13"}
    con.execute('CREATE TABLE rues_rattachement (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom MULTILINESTRING, '
                + ", ".join(f'"{c}" {"INTEGER" if c in entiers else "TEXT"}' for c in COLONNES) + ")")
    emprises = []
    for r in lignes:
        g = traces[r["libelle"]]
        g = g if g.geom_type == "MultiLineString" else MultiLineString([g])
        emprises.append(g.bounds)
        valeurs = [format_qgis(r[c]) if c in ("quartier_31_manuel", "quartier_13_manuel") else r[c] for c in COLONNES]
        con.execute(f'INSERT INTO rues_rattachement VALUES (NULL, ?, {", ".join("?" * len(COLONNES))})',
                    [blob_gpkg(g)] + [v if v != "" else None for v in valeurs])
    minx, miny = min(e[0] for e in emprises), min(e[1] for e in emprises)
    maxx, maxy = max(e[2] for e in emprises), max(e[3] for e in emprises)
    con.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, srs_id) "
                "VALUES ('rues_rattachement', 'features', 'rues_rattachement', ?, ?, ?, ?, 4326)",
                (minx, miny, maxx, maxy))
    con.execute("INSERT INTO gpkg_geometry_columns VALUES ('rues_rattachement', 'geom', 'MULTILINESTRING', 4326, 0, 0)")
    con.commit()
    con.close()
    replace(temporaire, SORTIE_GPKG)


def main():
    rues, traces, q31, points, du13, cles_rue, ban, sites_notice = charger()
    rep = {r["libelle"]: r for r in lire_csv(REPARTITION)}
    q13 = {r["libelle"]: transform(VERS_L93, wkt.loads(r["coord_wkt"])) for r in lire_csv(QUARTIERS_13)}
    niveaux = {"31": ("q31_repartition", lambda q: q), "13": ("q13_repartition", lambda q: du13[q])}

    # Notices : point, rue sûre, ou reste à la convention.
    sans_point = {n: Counter() for n in niveaux}
    bilan = Counter()
    conflits = []
    for f in sorted(glob(join(IR, "*.xml"))):
        ir = basename(f).replace("FR595126101_", "").replace(".xml", "")
        for c in etree.parse(f).iter("c"):
            voies = [texte(g) for g in c.findall("controlaccess/geogname")
                     if "bnr_geo_rbx_voies" in (g.get("source") or "")]
            voies = [v for v in voies if v in rep and rep[v]["q31_repartition"]]
            if not voies:
                continue
            bilan["notices avec rue"] += 1
            pts31 = sites_notice.get((ir, c.get("id")), set()) | quartiers_adresses(c, voies, traces, q31, cles_rue, ban)
            for n, (col, vers) in niveaux.items():
                cands = {v: set(candidats(rep[v][col])) for v in voies}
                sures = {v: next(iter(s)) for v, s in cands.items() if len(s) == 1}
                if pts31:
                    bilan[(n, "point")] += 1
                    pts = {vers(q) for q in pts31}
                    for v, q in sures.items():
                        if q not in pts:
                            bilan[(n, "point contre rue sûre")] += 1
                            conflits.append({"niveau": n, "ir": ir, "c_id": c.get("id"), "rue": v, "quartier_rue": q,
                                             "quartiers_point": " | ".join(sorted(pts)),
                                             "unittitle": texte(c.find("did/unittitle"))
                                             if c.find("did/unittitle") is not None else ""})
                            break
                elif sures:
                    bilan[(n, "rue sûre")] += 1
                elif len(set.intersection(*cands.values())) == 1:
                    bilan[(n, "croisement de rues")] += 1
                else:
                    bilan[(n, "convention")] += 1
                    for v in voies:
                        sans_point[n][v] += 1

    # Table des rues ambiguës (à 31 ; toute rue ambiguë à 13 l'est aussi à 31).
    saisies = lire_saisies()
    noms = {"31": set(q31), "13": set(q13)}
    alertes = []
    limites = {"31": unary_union([g.boundary for g in q31.values()]).buffer(15),
               "13": unary_union([g.boundary for g in q13.values()]).buffer(15)}
    lignes = []
    for lib, r in rep.items():
        if not r["q31_pct_principal"] or float(r["q31_pct_principal"]) >= SEUIL_SURE or lib not in traces:
            continue
        out = {"lieu_id": r["lieu_id"], "libelle": lib, "longueur_m": int(float(r["longueur_m"])),
               "nb_geogname": int(r["nb_geogname"])}
        for n, (col, _) in niveaux.items():
            out[f"notices_sans_point_{n}"] = sans_point[n][lib]
            out[f"repartition_{n}"] = r[col]
            if float(r[f"q{n}_pct_principal"]) >= SEUIL_SURE:
                out.update({f"type_{n}": "", f"regle_{n}": "sûre", f"proposition_{n}": r[f"q{n}_principal"]})
            else:
                regle, prop = proposer(lib, r[col], n, du13, points, q31)
                out.update({f"type_{n}": type_rue(traces[lib], limites[n]), f"regle_{n}": regle,
                            f"proposition_{n}": prop})
        d = saisies.get(r["lieu_id"], {})
        out.update({"decision": d.get("decision", ""), "remarque": d.get("remarque", ""),
                    "quartier_31_manuel": d.get("quartier_31_manuel", []),
                    "quartier_13_manuel": d.get("quartier_13_manuel", [])})
        if out["decision"] and out["decision"] not in DECISIONS:
            alertes.append(f"{lib} : décision « {out['decision']} » inconnue (oui / non / à revoir)")
        for n in niveaux:
            inconnus = [q for q in out[f"quartier_{n}_manuel"] if q not in noms[n]]
            if inconnus:
                alertes.append(f"{lib} : quartier(s) {n} inconnu(s) {inconnus}")
        # Retenu : saisie manuelle, sinon (à 13) quartiers de la saisie à 31, sinon proposition acceptée.
        m31 = [q for q in out["quartier_31_manuel"] if q in noms["31"]]
        m13 = [q for q in out["quartier_13_manuel"] if q in noms["13"]]
        out["retenu_31"] = " | ".join(m31) if m31 else (out["proposition_31"] if out["decision"] == "oui" else "")
        out["retenu_13"] = (" | ".join(m13) if m13 else
                            " | ".join(dict.fromkeys(du13[q] for q in m31 if q in du13)) if m31 else
                            out["proposition_13"] if out["decision"] == "oui" or out["regle_13"] == "sûre" else "")
        lignes.append(out)
    lignes.sort(key=lambda x: (-x["notices_sans_point_31"], -x["nb_geogname"], x["libelle"]))

    with open(SORTIE, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, COLONNES)
        w.writeheader()
        w.writerows({**r, "quartier_31_manuel": " | ".join(r["quartier_31_manuel"]),
                     "quartier_13_manuel": " | ".join(r["quartier_13_manuel"])} for r in lignes)
    ecrire_gpkg(lignes, {r["libelle"]: wkt.loads(rues[r["libelle"]].get("coord_manuel")
                                                 or rues[r["libelle"]]["coord_wkt"]) for r in lignes})
    with open(SORTIE_CONFLITS, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, ["niveau", "ir", "c_id", "rue", "quartier_rue", "quartiers_point", "unittitle"])
        w.writeheader()
        w.writerows(conflits)

    print(f"{len(lignes)} rues ambiguës → {SORTIE}, {SORTIE_GPKG}")
    print(f"{bilan['notices avec rue']} notices avec rue")
    for n in niveaux:
        print(f"  {n} : " + ", ".join(f"{k[1]} {v}" for k, v in sorted(bilan.items(), key=str)
                                      if isinstance(k, tuple) and k[0] == n))
        regles = Counter(r[f"regle_{n}"] for r in lignes)
        notices = Counter()
        for r in lignes:
            notices[r[f"regle_{n}"]] += r[f"notices_sans_point_{n}"]
        print(f"  {n} : règles (rues) {dict(regles)} ; notices concernées {dict(notices)}")
    print(f"conflits point / rue sûre → {SORTIE_CONFLITS} ({len(conflits)})")
    print("saisies : décisions " + str(dict(Counter(r["decision"] or "(vide)" for r in lignes)))
          + f", rues avec quartier retenu à 31 : {sum(1 for r in lignes if r['retenu_31'])}, "
          f"à 13 : {sum(1 for r in lignes if r['retenu_13'])}")
    for a in alertes:
        print("ALERTE", a)


if __name__ == "__main__":
    main()
