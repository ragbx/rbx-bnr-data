"""Sites validés pour la carte « Bn-r : accès par plan » (carte_leaflet.py), sans passer par les IR (2026-11-16).

La carte ne montrait comme sites que les points indexés en geogname dans les IR (2 au 16/11). Ce script
rassemble les points validés par la méthode « sites » et la liste de leurs notices, en lecture seule :

  - data/geo/propositions_sites_<date>.csv (sites_candidats.py) : sites « validé » ; point = lieu_manuel
    s'il est renseigné, sinon lieu_id_propose ; coordonnées lues dans le référentiel ou ses écartés ;
  - data/geo/validation_sites_sans_point_<date2>.gpkg, couche sites_a_valider (saisies QGIS) : décision
    « oui » ou « autre point » ; position = le POINT saisi dans lieu_manuel, sinon le point de la couche ;
    les points provisoires « à placer » (a_placer = oui sans lieu_manuel) sont laissés de côté ;
  - data/geo/propositions_sites_<date>_notices.csv : notices liées à chaque site (liens sujet et lieu) ;
  - results/ead/ead_cor/bnr2mnesys/*.xml (IR tels qu'en ligne) : titre, date et ARK publication:previous.

Les formes qui tombent au même endroit (≈ 15 m) sont réunies en un seul point. Sorties :

  - data/geo/sites_carte.csv : un point par ligne ; colonnes manuelles **nom** (nom affiché, vide = nom
    proposé), **afficher** (« non » pour masquer) et **remarque**, conservées d'une exécution à l'autre
    (clé point_id) ; colonne alerte (hors Roubaix, point à moins de 15 m d'un autre…) ;
  - results/ead/indexation/sites_carte_notices.csv : point_id, ir, c_id, date, titre, ark.

Ni les IR ni le référentiel ne sont modifiés.

    python scripts/ead/sites_carte.py      (env conda rbx-bnr-data)
    python scripts/ead/carte_leaflet.py
"""
import csv
import json
import re
import sqlite3
import struct
import unicodedata
from collections import Counter, defaultdict
from glob import glob
from math import cos, hypot, radians
from os.path import dirname, exists, join, realpath

from lxml import etree
from shapely.geometry import Point, shape

RACINE = dirname(dirname(dirname(realpath(__file__))))
GEO = join(RACINE, "data/geo")
PROPOSITIONS = join(GEO, "propositions_sites_20261112.csv")
NOTICES = join(GEO, "propositions_sites_20261112_notices.csv")
SANS_POINT = join(GEO, "validation_sites_sans_point_20261114.gpkg")
REFERENTIELS = [join(GEO, "referentiel_lieux_roubaix.csv"), join(GEO, "referentiel_lieux_roubaix_ecartes.csv")]
UMAP = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.umap")
IR_ACTUELS = join(RACINE, "results/ead/ead_cor/bnr2mnesys")
SORTIE = join(GEO, "sites_carte.csv")
# Liste des noms (noms_points_sites.py, relue par l'utilisateur) : le libellé retenu devient le nom proposé
# (2026-11-17) ; la colonne manuelle « nom » de SORTIE garde la priorité.
NOMS = join(GEO, "noms_points_sites_20261117.xlsx")
SORTIE_NOTICES = join(RACINE, "results/ead/indexation/sites_carte_notices.csv")
# Noms d'un même site (tenu à la main, 2026-11-19) : colonnes nom, site_cle_cible, remarque. Une notice dont le titre
# est l'un de ces noms rejoint le site cible (lien « sujet ») ; les liens retenus de la clé du nom (forme des
# propositions) aussi. Ex. Hôpital Napoléon, Hôtel-Dieu → hospice blanchemaille.
FUSIONS = join(GEO, "sites_fusions.csv")

LIENS_RETENUS = {"sujet", "lieu"}
DECISIONS_SANS_POINT = {"oui", "autre point"}
MANUELLES = ["nom", "afficher", "remarque"]
COLONNES = ["point_id", "nom_propose", "nom", "afficher", "categorie", "quartier", "lon", "lat", "nb_notices",
            "nb_ark", "formes", "site_cles", "source", "alerte", "remarque"]
RAYON_M = 15
POINT_WKT = re.compile(r"POINT\s*\(\s*(-?[\d.]+)[\s,]+(-?[\d.]+)\s*\)", re.I)
FAMILLES = {"eglise": "Église", "usine": "Usine", "ecole": "École", "college": "Collège", "lycee": "Lycée",
            "hopital": "Hôpital", "hospice": "Hospice", "parc": "Parc", "square": "Square", "theatre": "Théâtre",
            "brasserie": "Brasserie", "chateau": "Château", "cimetiere": "Cimetière", "couvent": "Couvent",
            "gare": "Gare", "stade": "Stade", "galerie": "Galerie", "bibliotheque": "Bibliothèque",
            "mediatheque": "Médiathèque", "clinique": "Clinique", "maternite": "Maternité", "temple": "Temple",
            "chapelle": "Chapelle", "institut": "Institut", "musee": "Musée", "salle": "Salle"}


def lire_csv(f):
    with open(f, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def xy(w):
    m = POINT_WKT.search(w or "")
    return (float(m.group(1)), float(m.group(2))) if m else None


def distance_m(a, b):
    return hypot((a[0] - b[0]) * 111320 * cos(radians(a[1])), (a[1] - b[1]) * 110540)


def points_valides():
    """[{point_id, nom_propose, categorie, quartier, xy, formes: Counter, site_cles, source}] avant fusion."""
    coords, categories = {}, {}
    for f in REFERENTIELS:
        for r in lire_csv(f):
            if r["thesaurus"] == "points":
                coords[r["lieu_id"]] = xy(r.get("coord_manuel") or r.get("coord_wkt"))
                categories[r["lieu_id"]] = r["categorie"]
    out = {}
    for r in lire_csv(PROPOSITIONS):
        if r["proposition"] != "validé":
            continue
        pid = r["lieu_manuel"].strip() or r["lieu_id_propose"]
        if not coords.get(pid):
            print(f"  ! {r['site_cle']} : pas de coordonnées pour {pid}")
            continue
        p = out.setdefault(pid, {"point_id": pid, "nom_propose": r["libelle_propose"], "categorie": categories.get(pid, ""),
                                 "quartier": r["quartier_point"], "xy": coords[pid], "formes": Counter(),
                                 "site_cles": [], "source": "points"})
        p["formes"][r["forme"]] += int(r["nb_retenus"] or 0)
        p["site_cles"].append(r["site_cle"])

    con = sqlite3.connect(SANS_POINT)
    con.row_factory = sqlite3.Row
    sans_point = []
    for r in con.execute("SELECT * FROM sites_a_valider"):  # lecture seule ; géométrie = blob GPKG (blob_point)
        if (r["decision"] or "").strip() not in DECISIONS_SANS_POINT:
            continue
        pos = xy(r["lieu_manuel"])
        if not pos:
            if (r["a_placer"] or "").strip() == "oui":
                print(f"  ! {r['forme']} : encore à placer, laissé de côté")
                continue
            pos = blob_point(r["geom"])
        sans_point.append({"forme": r["forme"], "site_cle": r["site_cle"], "famille": r["famille"] or "",
                           "nb": int(r["nb_retenus"] or 0), "xy": pos, "decision": r["decision"],
                           "id_propose": r["id_propose"] or "", "libelle_propose": r["libelle_propose"] or ""})
    con.close()

    # regroupement des formes sans point au même endroit ; nom proposé = forme la plus citée
    groupes = []
    for s in sorted(sans_point, key=lambda s: -s["nb"]):
        g = next((g for g in groupes if distance_m(g[0]["xy"], s["xy"]) <= RAYON_M), None)
        (g.append(s) if g else groupes.append([s]))
    for g in groupes:
        tete = g[0]
        # même endroit qu'un point validé avec point (« Conservatoire de Musique » → CRD) : on le complète
        deja = next((p for p in out.values() if p["source"] == "points" and distance_m(p["xy"], tete["xy"]) <= RAYON_M), None)
        if deja:
            for s in g:
                deja["formes"][s["forme"]] += s["nb"]
                deja["site_cles"].append(s["site_cle"])
            continue
        pid = (tete["id_propose"] if tete["decision"] == "oui" and re.match(r"(osm|bdtopo|bnr):", tete["id_propose"])
               else "sp:" + tete["site_cle"])
        nom = (tete["libelle_propose"] if pid == tete["id_propose"] and tete["libelle_propose"]
               else re.sub(r"\s*\(Roubaix, Nord\)$", "", tete["forme"]))
        p = out.setdefault(pid, {"point_id": pid, "nom_propose": nom, "categorie": "" if tete["famille"].startswith("(") else FAMILLES.get(tete["famille"], tete["famille"].capitalize()),
                                 "quartier": "", "xy": tete["xy"], "formes": Counter(), "site_cles": [], "source": "sans point"})
        for s in g:
            p["formes"][s["forme"]] += s["nb"]
            p["site_cles"].append(s["site_cle"])
    return list(out.values())


def blob_point(b):
    """Point d'un blob géométrique GeoPackage (en-tête GP + WKB point, petit-boutiste)."""
    drapeaux = b[3]
    env = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(drapeaux >> 1) & 7]
    wkb = b[8 + env:]
    fmt = "<" if wkb[0] == 1 else ">"
    return struct.unpack(fmt + "dd", wkb[5:21])


def quartiers():
    with open(UMAP, encoding="utf-8") as f:
        umap = json.load(f)
    calque = next(l for l in umap["layers"] if l["_umap_options"]["name"] == "Quartiers")
    return [(f["properties"]["name"], shape(f["geometry"])) for f in calque["features"]]


def notices_ir():
    """{(IR court, c_id): (date, titre, ark previous)} des IR actuels."""
    out = {}
    for f in sorted(glob(join(IR_ACTUELS, "*.xml"))):
        ir = re.sub(r"^FR\d+_", "", f.replace("\\", "/").rsplit("/", 1)[1][:-4])
        for c in etree.parse(f).iter("c"):
            did = c.find("did")
            titre = " ".join("".join(did.find("unittitle").itertext()).split()) if did is not None and did.find("unittitle") is not None else ""
            ud = did.find("unitdate") if did is not None else None
            date = " ".join("".join(ud.itertext()).split()) if ud is not None else ""
            ark = next((d.get("href") for d in c.findall("dao") + c.findall("daogrp/daoloc")
                        if d.get("role") == "publication:previous" and "ark:/" in (d.get("href") or "")), "")
            out[(ir, c.get("id"))] = (date, titre, ark, (ud.get("normal") or "") if ud is not None else "")
    return out


def noms_retenus(points):
    """Nom proposé = libellé retenu de la liste des noms : même identifiant, sinon même endroit (≤ RAYON_M) ;
    plusieurs lieux de la liste au même endroit : celui qui a le plus de notices."""
    if not exists(NOMS):
        return
    from openpyxl import load_workbook
    wb = load_workbook(NOMS, read_only=True)
    lignes = list(wb["à nommer"].values)
    deja = {r[list(lignes[0]).index("identifiant")] for r in list(wb["déjà au référentiel"].values)[1:]}
    h = {k: i for i, k in enumerate(lignes[0])}
    liste = [{"id": r[h["identifiant"]] or "", "nom": r[h["libellé retenu"]], "xy": xy(r[h["position"]]),
              "notices": r[h["notices"]] or 0} for r in lignes[1:] if r[h["libellé retenu"]]]
    for p in points:
        memes = [l for l in liste if l["id"] == p["point_id"]] or \
                [l for l in liste if l["xy"] and distance_m(l["xy"], p["xy"]) <= RAYON_M]
        if memes:
            p["nom_propose"] = max(memes, key=lambda l: l["notices"])["nom"]
        elif p["point_id"] not in deja:  # nom déjà fixé au référentiel : rien à signaler
            print(f"  ! {p['nom_propose']} : absent de la liste des noms")


def normal(texte):
    """« Hôtel-Dieu » → « hotel dieu » (casse, accents, ponctuation)."""
    t = unicodedata.normalize("NFD", texte.lower()).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def fusionner_noms(par_site, ir):
    """Applique sites_fusions.csv : notices au titre égal à un nom, et liens de la clé du nom, vers le site cible."""
    if not exists(FUSIONS):
        return
    cle_de_forme = {normal(r["forme"]): r["site_cle"] for r in lire_csv(PROPOSITIONS)}
    for f in lire_csv(FUSIONS):
        nom, cible = normal(f["nom"]), f["site_cle_cible"].strip()
        deja = {(n["ir"], n["c_id"]) for n in par_site[cible]}
        ajout = 0
        cle = cle_de_forme.get(nom)
        if cle and cle != cible:
            for n in par_site.pop(cle, []):
                if (n["ir"], n["c_id"]) not in deja:
                    par_site[cible].append(n)
                    deja.add((n["ir"], n["c_id"]))
                    ajout += 1
        for (ir_court, c_id), (date, titre, ark, _) in ir.items():
            if normal(titre) == nom and (ir_court, c_id) not in deja:
                par_site[cible].append({"ir": ir_court, "c_id": c_id, "unittitle": titre, "lien": "sujet"})
                deja.add((ir_court, c_id))
                ajout += 1
        print(f"  fusion « {f['nom']} » → {cible} : {ajout} notice(s)")


def main():
    points = points_valides()
    noms_retenus(points)
    qs = quartiers()
    ir = notices_ir()
    par_site = defaultdict(list)
    for n in lire_csv(NOTICES):
        if n["lien"] in LIENS_RETENUS:
            par_site[n["site_cle"]].append(n)
    fusionner_noms(par_site, ir)
    anciens = {r["point_id"]: r for r in lire_csv(SORTIE)} if exists(SORTIE) else {}

    lignes, lignes_notices = [], []
    for p in points:
        x, y = p["xy"]
        if not p["quartier"]:
            p["quartier"] = next((nom for nom, g in qs if g.contains(Point(x, y))), "")
        vus = {}
        for k in p["site_cles"]:
            for n in par_site.get(k, []):
                cle = (n["ir"], n["c_id"])
                if cle not in vus:
                    date, titre, ark, normal = ir.get(cle, ("", n["unittitle"], "", ""))
                    vus[cle] = {"point_id": p["point_id"], "ir": n["ir"], "c_id": n["c_id"], "date": date,
                                "titre": titre or n["unittitle"], "ark": ark, "_tri": normal or date}
        notices = sorted(vus.values(), key=lambda v: (v["_tri"] or "9999", v["titre"]))
        for v in notices:
            v.pop("_tri")
        lignes_notices += notices
        alertes = []
        if not p["quartier"]:
            alertes.append("hors des quartiers de Roubaix")
        proches = [q["nom_propose"] for q in points if q is not p and distance_m(q["xy"], p["xy"]) <= RAYON_M]
        if proches:
            alertes.append("même endroit que " + " ; ".join(proches))
        if not notices:
            alertes.append("aucune notice retenue")
        a = anciens.get(p["point_id"], {})
        lignes.append({"point_id": p["point_id"], "nom_propose": p["nom_propose"], "categorie": p["categorie"],
                       "quartier": p["quartier"], "lon": round(x, 6), "lat": round(y, 6),
                       "nb_notices": len(notices), "nb_ark": sum(1 for v in notices if v["ark"]),
                       "formes": " | ".join(f"{f} ({n})" for f, n in p["formes"].most_common()),
                       "site_cles": " | ".join(p["site_cles"]), "source": p["source"], "alerte": " ; ".join(alertes),
                       **{k: a.get(k, "") for k in MANUELLES}})
    lignes.sort(key=lambda r: (-r["nb_notices"], r["nom_propose"]))

    with open(SORTIE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, COLONNES)
        w.writeheader()
        w.writerows(lignes)
    with open(SORTIE_NOTICES, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, ["point_id", "ir", "c_id", "date", "titre", "ark"])
        w.writeheader()
        w.writerows(lignes_notices)

    print(f"→ {SORTIE} : {len(lignes)} points ({Counter(r['source'] for r in lignes)})")
    print(f"→ {SORTIE_NOTICES} : {len(lignes_notices)} liens point × notice, "
          f"{sum(1 for v in lignes_notices if v['ark'])} avec ARK")
    for r in lignes:
        if r["alerte"]:
            print(f"  ! {r['nom_propose']} : {r['alerte']}")
    disparus = set(anciens) - {r["point_id"] for r in lignes}
    if disparus:
        print(f"  ! points de l'exécution précédente absents : {sorted(disparus)}")


if __name__ == "__main__":
    main()
