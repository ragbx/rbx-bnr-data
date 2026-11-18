"""Liste des noms des points de sites à fixer avant leur ajout au référentiel (2026-11-17).

    conda run -n rbx-bnr-data python scripts/ead/noms_points_sites.py

Lecture seule. Réunit les sites validés qui deviendront des points du thésaurus des sites :
  - validation du 12/11 : propositions_sites_20261112.csv, proposition « validé » (points du référentiel ou
    points écartés à réintégrer) ;
  - sites sans point : validation_sites_sans_point_20261114.gpkg, décision « oui » ou « autre point » ;
  - vélodromes : groupes `velodrome <nom> (choix)` des propositions.

Un lieu = un même point (identifiant du référentiel, des écartés ou d'OSM, sinon position saisie arrondie au
mètre). Pour chaque lieu : formes des IR avec leur nombre, nom de la source du point (BD TOPO, OSM), libellé
proposé (forme la plus citée, nettoyée), et les cas à trancher (plusieurs noms, nom de la source différent
des IR, lieu proche d'un autre de la liste, hors Roubaix).

Sortie : data/geo/noms_points_sites_<date>.xlsx
  - « à nommer » : lieux qui ne sont pas encore au référentiel ; colonnes à remplir en jaune (libellé
    retenu, variantes retenues, actuel / disparu, remarque) ;
  - « déjà au référentiel » : lieux dont le nom est déjà fixé, pour information (formes des IR).
"""
import csv
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import date
from os.path import dirname, join

from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from shapely import wkb, wkt
from shapely.ops import transform

sys.path.insert(0, dirname(__file__))
from sites_candidats import cle_site, distinctifs, geometries_thesaurus  # noqa: E402
from referentiel_lieux_roubaix import GEO, SORTIE as REFERENTIEL, SORTIE_ECARTES, VERS_L93  # noqa: E402

PROPOSITIONS = join(GEO, "propositions_sites_20261112.csv")
SANS_POINT = join(GEO, "validation_sites_sans_point_20261114.gpkg")
SORTIE = join(GEO, f"noms_points_sites_{date.today():%Y%m%d}.xlsx")
PROCHE_M = 100
POINT = re.compile(r"^\s*point\s*\(\s*([-\d.]+)\s*[\s,]\s*([-\d.]+)\s*\)\s*$", re.I)
VARIANTE = re.compile(r"^(.*) \((\d+)\)$")


def lire(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def formes(variantes):
    """« forme (n) | forme (n) » → Counter."""
    c = Counter()
    for v in variantes.split(" | "):
        m = VARIANTE.match(v.strip())
        if m:
            c[m.group(1)] += int(m.group(2))
    return c


def nettoyer(forme):
    f = re.sub(r"\s*\((?:Roubaix|Hem|Croix|Wattrelos|Tourcoing)[^)]*\)\s*$", "", forme.strip())
    f = re.sub(r"^(?:E|É)glise\b", "Église", f[0].upper() + f[1:])
    f = re.sub(r"^Ecole\b", "École", f)
    return f


def proposer(compte, nom_source):
    """Libellé proposé : le nom le plus cité des IR (une forme tronquée, « lycée Maxence Van », compte pour
    la forme complète ; un mot de type seul, « vélodrome », ne compte pas), dans la graphie de la source du
    point quand c'est le même nom, sinon dans sa graphie la plus citée, nettoyée."""
    par_cle = Counter()
    for f, n in compte.items():
        if distinctifs(cle_site(f)):
            par_cle[cle_site(f)] += n
    for k in list(par_cle):
        longues = [k2 for k2 in par_cle if k2 != k and k2.startswith(k + " ")]
        if longues:
            par_cle[max(longues, key=lambda k2: par_cle[k2])] += par_cle.pop(k)
    if not par_cle:
        return nettoyer(nom_source) if nom_source else nettoyer(compte.most_common(1)[0][0])
    k = par_cle.most_common(1)[0][0]
    if nom_source and cle_site(nom_source) == k:
        return nettoyer(nom_source)
    return nettoyer(max((f for f in compte if cle_site(f) == k), key=lambda f: (compte[f], f[:1].isupper())))


def point_wkt(texte):
    m = POINT.match(texte or "")
    return f"POINT ({float(m.group(1)):.6f} {float(m.group(2)):.6f})" if m else ""


def geom_gpkg(blob):
    taille = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(blob[3] >> 1) & 0b111]
    return wkb.loads(bytes(blob[8 + taille:]))


def est_point(lieu_id):
    """Identifiant d'un point (référentiel, BD TOPO, OSM), pas d'une voie."""
    return bool(lieu_id) and not re.match(r"^(59512_|mel:|bdtopo:(VOIE|TRONCON))", lieu_id)


def main():
    lieux_ref = {r["lieu_id"]: (r, "référentiel") for r in lire(REFERENTIEL)}
    lieux_ref.update({r["lieu_id"]: (r, "écarté") for r in lire(SORTIE_ECARTES) if r["lieu_id"] not in lieux_ref})
    lieux = defaultdict(lambda: {"formes": Counter(), "notices": 0, "sources": set(), "decisions": Counter(),
                                 "remarques": [], "nom_source": "", "statut": "", "wkt": "", "lieu_id": ""})

    def ajouter(cle, lieu_id, statut, nom_source, w, variantes, nb, source, decision="", remarque=""):
        l = lieux[cle]
        l["formes"].update(variantes)
        l["notices"] += nb
        l["sources"].add(source)
        if decision:
            l["decisions"][decision] += 1
        if remarque and remarque not in l["remarques"]:
            l["remarques"].append(remarque)
        for k, v in (("lieu_id", lieu_id), ("statut", statut), ("nom_source", nom_source), ("wkt", w)):
            if v and not l[k]:
                l[k] = v

    # 1. validation du 12/11 et vélodromes (points du référentiel ou écartés)
    for p in lire(PROPOSITIONS):
        velo = p["site_cle"].startswith("velodrome") and p["site_cle"].endswith("(choix)")
        if p["proposition"] != "validé" or not p["lieu_id_propose"]:
            continue
        r, statut = lieux_ref.get(p["lieu_id_propose"], ({}, ""))
        ajouter(p["lieu_id_propose"], p["lieu_id_propose"], statut, p["libelle_propose"],
                r.get("coord_manuel") or r.get("coord_wkt", ""), formes(p["variantes"]), int(p["nb_retenus"] or 0),
                "vélodromes 17/11" if velo else "validation 12/11", p["decision"] or ("choix par notice" if velo else ""),
                p["remarque"])

    # 2. sites sans point (décisions oui / autre point)
    con = sqlite3.connect(f"file:{SANS_POINT}?mode=ro", uri=True)
    con.row_factory = sqlite3.Row
    for s in con.execute("SELECT * FROM sites_a_valider WHERE decision IN ('oui', 'autre point')"):
        manuel = point_wkt(s["lieu_manuel"])
        if s["decision"] == "oui" and est_point(s["id_propose"]) and not manuel:
            lieu_id = s["id_propose"]
            r, statut = lieux_ref.get(lieu_id, ({}, "OSM" if lieu_id.startswith("osm:") else "écarté"))
            cle, w, nom = lieu_id, geom_gpkg(s["geom"]).wkt, s["libelle_propose"]  # point de la couche (déplacé ou non)
        else:
            couche = geom_gpkg(s["geom"])
            deplace = s["coord_origine"] and couche.distance(wkt.loads(s["coord_origine"])) > 1e-5
            w = manuel or couche.wkt
            g = wkt.loads(w)
            cle, lieu_id, nom = f"point {g.x:.5f} {g.y:.5f}", "", ""
            statut = "point saisi" if manuel or deplace else f"point proposé ({s['proposition']})"
        remarque = " ".join((s["remarque"] or "").split())
        ajouter(cle, lieu_id, statut, nom, w, formes(s["variantes"]) or Counter({s["forme"]: int(s["nb_retenus"] or 0)}),
                int(s["nb_retenus"] or 0), "sans point 14/11", s["decision"], remarque)
    con.close()

    # Points disparus ajoutés avec un libellé provisoire (vélodromes) : à nommer comme les autres.
    provisoires = {r["lieu_id"] for r in lire(join(GEO, "lieux_disparus.csv")) if "libellé provisoire" in r["remarque"]}
    quartiers = geometries_thesaurus("quartiers")
    rangs = []
    for cle, l in lieux.items():
        if not l["wkt"]:
            continue
        g = transform(VERS_L93, wkt.loads(l["wkt"]))
        q = [lib for lib, qg in quartiers.items() if qg.covers(g)]
        tri = l["formes"].most_common()
        if l["lieu_id"] in provisoires:
            l["statut"] = "référentiel (libellé provisoire)"
        deja = l["statut"] == "référentiel"
        propose = l["nom_source"] if deja or not tri else proposer(l["formes"], l["nom_source"])
        # variantes : un nom distinct par clé (pas les graphies du même nom, ni un mot de type seul)
        vus, variantes = {cle_site(propose)}, []
        for f, _ in tri:
            k = cle_site(f)
            if k not in vus and distinctifs(k):
                vus.add(k)
                variantes.append(nettoyer(f))
        rangs.append({**l, "cle": cle, "g": g, "quartier": " | ".join(q) or "hors Roubaix", "propose": propose,
                      "variantes": variantes, "deja": deja})
    rangs.sort(key=lambda r: (r["deja"], -r["notices"], r["propose"]))
    for i, r in enumerate(rangs, 1):
        r["n"] = i
    for r in rangs:
        a = []
        noms = {cle_site(f) for f in r["formes"]}
        if len(noms) > 1:
            a.append(f"{len(noms)} noms différents dans les IR")
        if r["nom_source"] and cle_site(r["nom_source"]) not in noms:
            a.append("nom de la source différent des IR")
        if r["quartier"] == "hors Roubaix":
            a.append("hors Roubaix")
        proches = sorted((r["g"].distance(o["g"]), o["n"]) for o in rangs if o is not r and r["g"].distance(o["g"]) <= PROCHE_M)
        if proches:
            a.append("proche de " + ", ".join(f"n° {n} ({round(d)} m)" for d, n in proches))
        r["a_trancher"] = " ; ".join(a)

    wb = Workbook()
    jaune = PatternFill("solid", fgColor="FFF2B3")
    entetes = [("n°", 5), ("libellé proposé", 34), ("libellé retenu", 34), ("variantes retenues", 34),
               ("actuel / disparu", 12), ("remarque", 30), ("à trancher", 40), ("formes des IR (nombre)", 60),
               ("notices", 8), ("nom de la source du point", 34), ("point", 22), ("identifiant", 30),
               ("position", 30), ("quartier", 18), ("validation", 22), ("décisions", 18), ("remarques de validation", 50)]
    a_remplir = {"libellé retenu", "variantes retenues", "actuel / disparu", "remarque"}
    for titre, deja in (("à nommer", False), ("déjà au référentiel", True)):
        ws = wb.active if not deja else wb.create_sheet()
        ws.title = titre
        ws.append([e for e, _ in entetes])
        for r in (x for x in rangs if x["deja"] == deja):
            ws.append([r["n"], r["propose"], r["propose"] if not deja else "", " | ".join(r["variantes"]) if not deja else "",
                       "", "", r["a_trancher"], " | ".join(f"{f} ({n})" for f, n in r["formes"].most_common()),
                       r["notices"], r["nom_source"], r["statut"], r["lieu_id"], r["wkt"], r["quartier"],
                       ", ".join(sorted(r["sources"])), ", ".join(f"{d} {n}" for d, n in r["decisions"].items()),
                       " | ".join(r["remarques"])])
        for j, (e, largeur) in enumerate(entetes, 1):
            col = ws.cell(1, j).column_letter
            ws.column_dimensions[col].width = largeur
            ws.cell(1, j).font = Font(bold=True)
            if e in a_remplir and not deja:
                ws.cell(1, j).fill = jaune
                for i in range(2, ws.max_row + 1):
                    ws.cell(i, j).fill = jaune
        for ligne in ws.iter_rows(min_row=2):
            for c in ligne:
                c.alignment = Alignment(wrap_text=True, vertical="top")
        ws.freeze_panes = "C2"
        ws.auto_filter.ref = ws.dimensions
    wb.save(SORTIE)
    a_nommer = [r for r in rangs if not r["deja"]]
    print(f"{len(a_nommer)} lieux à nommer ({sum(r['notices'] for r in a_nommer)} liens retenus), "
          f"{len(rangs) - len(a_nommer)} déjà au référentiel → {SORTIE}")
    print("  à trancher :", dict(Counter("proche d'un autre lieu" if x.startswith("proche") else re.sub(r"^\d+ ", "", x)
                                         for r in a_nommer for x in r["a_trancher"].split(" ; ") if x)))
    print("  statut du point :", dict(Counter(r["statut"].split(" (")[0] for r in a_nommer)))


if __name__ == "__main__":
    main()
