"""Carte « public » en Leaflet (titre « Bn-r : accès par plan », choisi par l'utilisateur le 2026-11-14) : page HTML autonome des lieux de Roubaix dans les IR.

Données : la carte uMap « public » (results/ead/indexation/bnr2mnesysgeo_lieux.umap, produite
par geogname_ir2kml.py : quartiers colorés, voies et sites cités dans les IR, nombre
d'occurrences, lien vers la recherche de la BN-R), complétée par le référentiel des lieux
(anciens noms) et le rattachement des rues aux quartiers (même règle que
rues_quartiers2skos.py et quartiers2kml.py).

Page : results/ead/indexation/carte_bnr/index.html, un seul fichier (données incluses).
Leaflet 1.9.4 (cdnjs) ; fonds Plan IGN et photographies aériennes (Géoplateforme de l'IGN,
sans clé), OpenStreetMap en secours. Les voies sont dessinées en canvas avec une tolérance
de clic (pas de zones de clic « fantômes ») ; recherche par nom actuel ou ancien ; choix
d'un quartier (zoom, ses voies mises en avant) ; lien direct #voie=<id> / #quartier=<id> / #site=<id>.
Sites (2026-11-16) : les sites validés de data/geo/sites_carte.csv (sites_carte.py, hors IR), chacun avec la
liste de ses notices (lien ARK publication:previous), puis les sites de l'uMap qui n'y sont pas déjà.
Rues et quartiers (2026-11-16) : même liste des notices, d'après results/ead/indexation/lieux_carte_notices.csv
(lieux_carte_notices.py, <geogname> des IR) ; à défaut, nombre de mentions et lien de recherche de l'uMap.
Plans anciens (ajoutés le 2026-11-16) : plans de Roubaix de 1899, 1889, 1855 et vers 1811 géoréférencés,
superposables aux fonds dans la boîte des couches (œil, opacité, ordre par glisser-déposer, centrer, source) ;
les fonds de carte sont présentés de la même façon (liste repliée sauf le Plan IGN et les fonds affichés),
en tuiles XYZ WebP lues dans plans/<année>/ à côté de la page ; ces tuiles ne sont
pas écrites par ce script, elles se copient depuis data/geo/plans_anciens/leaflet_1899
(tuiles → 1899, tuiles_1889 → 1889, tuiles_1855 → 1855, tuiles_1811 → 1811), cf. README de carte-bnr.
Archives municipales (2026-11-19) : notices des inventaires Mnesys par lieu, d'après
results/ead/indexation/archives_carte_notices.csv (archives_carte_notices.py : formes alignées sur le référentiel ou
validées) ; ajoutées aux listes des fiches, groupées par inventaire (« Archives municipales · <cote du fonds> · titre »),
sans lien (pas d'ARK) : titre, cote et date. Les rues et sites cités seulement par les archives sont ajoutés à la
carte avec la géométrie du référentiel des lieux.
Coordonnées (2026-11-17) : petit widget en bas à gauche pour copier le point cliqué (lat, lon ou WKT POINT)
ou la géométrie WKT du lieu dont la fiche est ouverte.

    python scripts/ead/geogname_ir2kml.py   # d'abord, si les IR ont changé
    python scripts/ead/lieux_carte_notices.py   # notices des rues et quartiers (IR)
    python scripts/ead/sites_carte.py   # sites validés et leurs notices
    python scripts/ead/archives_carte_notices.py   # notices des Archives municipales (IR Mnesys)
    python scripts/ead/carte_leaflet.py
"""
import csv
import json
import re
import sys
import unicodedata
from collections import defaultdict
from glob import glob
from math import cos, hypot, radians
from os import makedirs
from os.path import dirname, exists, join, realpath

from lxml import etree
from shapely import wkt
from shapely.geometry import mapping

sys.path.insert(0, dirname(realpath(__file__)))
from geogname_ir2kml import ROUGE  # noqa: E402
from quartiers2kml import quartiers_des_rues  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
UMAP = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.umap")
REFERENTIEL = join(RACINE, "data/geo/referentiel_lieux_roubaix.csv")
SORTIE = join(RACINE, "results/ead/indexation/carte_bnr/index.html")
SITES = join(RACINE, "data/geo/sites_carte.csv")   # sites validés, hors IR (sites_carte.py, 2026-11-16)
SITES_NOTICES = join(RACINE, "results/ead/indexation/sites_carte_notices.csv")
IR_ACTUELS = join(RACINE, "results/ead/ead_cor/bnr2mnesys")   # titres des IR (tels qu'en ligne)
LIEUX_NOTICES = join(RACINE, "results/ead/indexation/lieux_carte_notices.csv")   # rues, quartiers (lieux_carte_notices.py)
ARCHIVES = join(RACINE, "results/ead/indexation/archives_carte_notices.csv")   # Archives municipales (archives_carte_notices.py)
IR_ARCHIVES = join(RACINE, "data/ead/mnesys")   # titres des IR des Archives municipales
AM = "AM_"   # préfixe des codes d'IR des Archives municipales dans la page (notices sans lien)
ARK = "https://www.bn-r.fr/ark:/20179/"   # préfixe retiré des liens dans la page (allègement), remis en JS
LIEN = re.compile(r"\[\[(.+?)\|[^\]]*\]\]")
OCC = re.compile(r"^(\d+) occurrence")


def ident(texte):
    """« Rue de l'Épeule » → « rue_de_l_epeule » (lien direct)."""
    t = unicodedata.normalize("NFD", texte.lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "_", t).strip("_")


def arrondir(geom):
    """Coordonnées GeoJSON arrondies à 5 décimales (≈ 1 m) pour alléger la page."""
    def r(c):
        return [round(c[0], 5), round(c[1], 5)] if isinstance(c[0], (int, float)) else [r(x) for x in c]
    return {"type": geom["type"], "coordinates": r(geom["coordinates"])}


def lire_umap_publique():
    with open(UMAP, encoding="utf-8") as f:
        umap = json.load(f)
    calques = {l["_umap_options"]["name"]: l["features"] for l in umap["layers"]}
    return calques["Quartiers"], calques["Voies"], calques["Sites"]


def fiche_umap(feature):
    """description uMap « N occurrences\\n[[url|Voir sur la BN-R]] » → (N, url)."""
    d = feature["properties"].get("description", "")
    n, lien = OCC.search(d), LIEN.search(d)
    return (int(n.group(1)) if n else 0), (lien.group(1).replace("%5B", "%5B") if lien else "")


def distance_m(a, b):
    return hypot((a[0] - b[0]) * 111320 * cos(radians(a[1])), (a[1] - b[1]) * 110540)


def notice(r):
    """Ligne de notice → [titre, date, fin de l'ARK] (ARK vide : pas encore en ligne)."""
    return [r["titre"], r["date"], r["ark"][len(ARK):] if r["ark"].startswith(ARK) else r["ark"], r["ir"]]


def titres_ir():
    """{code court de l'IR (MED_CP): titre (unittitle de l'archdesc)} des IR tels qu'en ligne."""
    out = {}
    for f in sorted(glob(join(IR_ACTUELS, "*.xml"))):
        ut = etree.parse(f).getroot().find("{*}archdesc/{*}did/{*}unittitle")
        code = re.sub(r"^FR\d+_", "", f.replace("\\", "/").rsplit("/", 1)[1][:-4])
        out[code] = " ".join("".join(ut.itertext()).split()) if ut is not None else code
    return out


def titres_archives(codes):
    """{AM_<code>: « Archives municipales · <fonds> · <titre> »} : unittitle de l'archdesc, sinon titleproper."""
    out = {}
    for code in codes:
        racine = etree.parse(join(IR_ARCHIVES, f"FRAM59100_{code}.xml")).getroot()
        ut = racine.find("archdesc/did/unittitle")
        ut = ut if ut is not None else racine.find(".//titleproper")
        titre = " ".join("".join(ut.itertext()).split()) if ut is not None else ""
        titre = re.sub(r"^\d+\s*[A-Z]+\s*-\s*", "", titre)   # « 1 N - Biens communaux. »
        titre = re.split(r"(?<!\b[A-Z])\.\s+(?=[A-ZÉ])", titre)[0].rstrip(" .")   # 1re phrase (360W) ; « M. Lepoutre » gardé
        fonds = code.split("_")[0]   # 3Fi_20210427_import → 3Fi
        out[AM + code] = f"Archives municipales · {fonds}" + (f" · {titre}" if titre else "")
    return out


def notices_archives():
    """{lieu_id: [[titre, date, "", AM_ir, cote], …]} des Archives municipales (pas d'ARK : pas de lien)."""
    out = defaultdict(list)
    if not exists(ARCHIVES):
        print(f"  ! {ARCHIVES} absent : pas de notices des Archives municipales (archives_carte_notices.py)")
        return out
    with open(ARCHIVES, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            out[r["lieu_id"]].append([r["titre"], r["date"], "", AM + r["ir"], r["cote"]])
    return out


def geom_referentiel(r, point=False):
    """Géométrie GeoJSON d'un lieu du référentiel (coord_manuel sinon coord_wkt) ; point=True : un point."""
    texte = (r.get("coord_manuel") or r.get("coord_wkt") or "").strip()
    if not texte:
        return None
    g = wkt.loads(texte)
    if point and g.geom_type != "Point":
        g = g.representative_point()
    return arrondir(mapping(g))


def par_ir(liste):
    """[[titre, date, ark, ir(, cote)], …] → [[ir, [[titre, date, ark(, cote)], …]], …] : IR les plus fournis
    d'abord, notices dans l'ordre reçu (date puis titre) à l'intérieur de chaque IR."""
    groupes = defaultdict(list)
    for n in liste:
        groupes[n[3]].append(n[:3] + n[4:])
    return [[ir, l] for ir, l in sorted(groupes.items(), key=lambda x: (-len(x[1]), x[0]))]


def notices_lieux():
    """{(thésaurus, libellé): [[titre, date, ark], …]} des rues, quartiers et sites indexés dans les IR."""
    out = defaultdict(list)
    if exists(LIEUX_NOTICES):
        with open(LIEUX_NOTICES, encoding="utf-8", newline="") as f:
            for r in csv.DictReader(f):
                out[(r["thesaurus"], r["libelle"])].append(notice(r))
    else:
        print(f"  ! {LIEUX_NOTICES} absent : fiches des rues et quartiers avec lien de recherche (lieux_carte_notices.py)")
    return out


def avec_notices(o, liste):
    """Liste des notices si on l'a (n = nombre de notices), sinon nombre de mentions et lien de recherche uMap."""
    if liste:
        o.update(n=len(liste), notices=par_ir(liste))
        o.pop("lien", None)
    return o


def sites_valides():
    """Sites validés (sites_carte.py) : nom = colonne nom si remplie, sinon nom proposé ; afficher = non → absent.
    Chaque site porte la liste de ses notices [titre, date, ark] (ark vide : pas encore en ligne)."""
    if not exists(SITES):
        return []
    notices = defaultdict(list)
    with open(SITES_NOTICES, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            notices[r["point_id"]].append(notice(r))
    sites = []
    with open(SITES, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            if r["afficher"].strip().lower() == "non":
                continue
            nom = r["nom"].strip() or r["nom_propose"]
            sites.append({"id": ident(nom), "nom": nom, "categorie": r["categorie"], "point_id": r["point_id"],
                          "_liste": notices[r["point_id"]],
                          "quartiers": [r["quartier"]] if r["quartier"] else [],
                          "n": len(notices[r["point_id"]]), "notices": par_ir(notices[r["point_id"]]),
                          "geom": {"type": "Point", "coordinates": [round(float(r["lon"]), 5), round(float(r["lat"]), 5)]}})
    return sites


def main():
    quartiers_f, voies_f, sites_f = lire_umap_publique()
    ref = {}
    with open(REFERENTIEL, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            ref[(r["thesaurus"], r["libelle_manuel"].strip() or r["libelle"])] = r
    q_rues = quartiers_des_rues()
    par_lieu = notices_lieux()
    archives = notices_archives()
    par_id = {r["lieu_id"]: r for r in ref.values()}
    places = set()   # lieux du référentiel déjà sur la carte

    def toutes(cle, lieu_id):
        """Notices de la BN-R (par libellé) puis des Archives municipales (par lieu_id)."""
        if lieu_id:
            places.add(lieu_id)
        return par_lieu.get(cle, []) + archives.get(lieu_id, [])

    voies = []
    for f in voies_f:
        nom = f["properties"]["name"]
        r = ref.get(("rues", nom), {})
        n, lien = fiche_umap(f)
        voies.append(avec_notices({"id": ident(nom), "nom": nom,
                                   "anciens": [a for a in r.get("nom_ancien", "").split(" | ") if a],
                                   "quartiers": q_rues.get(r.get("lieu_id"), []),
                                   "n": n, "lien": lien, "geom": arrondir(f["geometry"])}, toutes(("rues", nom), r.get("lieu_id"))))
    for lieu_id, liste in archives.items():   # rues citées seulement par les archives : tracé du référentiel
        r = par_id.get(lieu_id)
        if lieu_id in places or not r or r["thesaurus"] != "rues" or not (geom := geom_referentiel(r)):
            continue
        nom = r["libelle_manuel"].strip() or r["libelle"]
        places.add(lieu_id)
        voies.append(avec_notices({"id": ident(nom), "nom": nom,
                                   "anciens": [a for a in r.get("nom_ancien", "").split(" | ") if a],
                                   "quartiers": q_rues.get(lieu_id, []), "n": 0, "lien": "", "geom": geom}, liste))

    quartiers = []
    for f in quartiers_f:
        nom = f["properties"]["name"]
        n, lien = fiche_umap(f)
        quartiers.append(avec_notices({"id": ident(nom), "nom": nom, "couleur": f["properties"]["_umap_options"]["color"],
                                       "n": n, "lien": lien, "geom": arrondir(f["geometry"]),
                                       "voies": sum(1 for v in voies if nom in v["quartiers"])},
                                      toutes(("quartiers", nom), ref.get(("quartiers", nom), {}).get("lieu_id"))))

    sites = sites_valides()
    for s in sites:   # sites validés : point_id = lieu_id du référentiel
        if s.get("point_id") in archives:
            s["notices"] = par_ir(s.pop("_liste") + archives[s["point_id"]])
            s["n"] = sum(len(l) for _, l in s["notices"])
        s.pop("_liste", None)
        places.add(s.get("point_id"))
    for f in sites_f:   # sites indexés dans les IR, sauf s'ils sont déjà parmi les sites validés
        nom = f["properties"]["name"]
        lon, lat = f["geometry"]["coordinates"]
        if any(ident(nom) == s["id"] or distance_m((lon, lat), s["geom"]["coordinates"]) <= 15 for s in sites):
            continue
        r = ref.get(("points", nom), {})
        n, lien = fiche_umap(f)
        sites.append(avec_notices({"id": ident(nom), "nom": nom, "categorie": r.get("categorie", ""),
                                   "quartiers": [r["quartiers"]] if r.get("quartiers") else [],
                                   "n": n, "lien": lien, "geom": arrondir(f["geometry"])}, toutes(("points", nom), r.get("lieu_id"))))
    for lieu_id, liste in archives.items():   # sites cités seulement par les archives : point du référentiel
        r = par_id.get(lieu_id)
        if lieu_id in places or not r or r["thesaurus"] != "points" or not (geom := geom_referentiel(r, point=True)):
            continue
        nom = r["libelle_manuel"].strip() or r["libelle"]
        places.add(lieu_id)
        sites.append(avec_notices({"id": ident(nom), "nom": nom, "categorie": r.get("categorie", ""),
                                   "quartiers": [r["quartiers"]] if r.get("quartiers") else [],
                                   "n": 0, "lien": "", "geom": geom}, liste))
    sans_place = {i for i in archives if i not in places}
    if sans_place:
        print(f"  ! {len(sans_place)} lieu(x) des archives absents de la carte (sans géométrie ou hors thésaurus) : "
              f"{sorted(sans_place)[:10]}")
    for s in sites:
        s.pop("point_id", None)


    donnees = json.dumps({"quartiers": quartiers, "voies": voies, "sites": sites, "rouge": ROUGE, "ark": ARK,
                          "ir": {**titres_ir(), **titres_archives({i[len(AM):] for o in quartiers + voies + sites
                                                                   for i, _ in o.get("notices", []) if i.startswith(AM)})}},
                         ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    makedirs(dirname(SORTIE), exist_ok=True)
    with open(SORTIE, "w", encoding="utf-8", newline="\n") as f:
        f.write(PAGE.replace("__DONNEES__", donnees))

    print(f"→ {SORTIE}")
    print(f"  {len(quartiers)} quartiers, {len(voies)} voies, {len(sites)} sites")
    print(f"  Archives municipales : {sum(len(l) for l in archives.values())} lignes lieu × notice, "
          f"{len(archives)} lieux, dont {len(archives) - len(sans_place)} sur la carte")
    print(f"  voies avec ancien(s) nom(s) : {sum(1 for v in voies if v['anciens'])} ; "
          f"sans quartier : {sum(1 for v in voies if not v['quartiers'])}")


PAGE = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Bn-r : accès par plan</title>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.css">
<style>
:root {
  --turquoise: #2FAAAF; --jaune: #E9B400; --violet: #9460A4; --gris: #ADA796; --rouge: #C03730;
  --encre: #1d1d1b; --doux: #5f5b52; --fond: #ffffff; --trait: #d9d5cb; --survol: #f3f1ec;
}
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body { font: 15px/1.4 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--encre); background: var(--fond); }
#carte { position: absolute; inset: 0; }
.panneau { position: absolute; z-index: 1000; top: 12px; left: 12px; width: 340px; max-width: calc(100% - 24px);
  background: var(--fond); border-radius: 10px; box-shadow: 0 4px 18px rgba(0,0,0,.2); }
.bandeau { display: flex; height: 6px; border-radius: 10px 10px 0 0; overflow: hidden; }
.bandeau span { flex: 1; }
.panneau header { position: relative; padding: 12px 44px 10px 16px; }
.panneau h1 { font-size: 17px; margin: 0; line-height: 1.25; }
.panneau header p { margin: 4px 0 0; color: var(--doux); font-size: 13px; }
#replier { position: absolute; top: 10px; right: 10px; width: 30px; height: 30px; border: 0; border-radius: 6px;
  background: none; color: var(--doux); font-size: 16px; cursor: pointer; }
#replier:hover, #replier:focus-visible { background: var(--survol); outline: none; }
.panneau.replie .corps, .panneau.replie header p, .panneau.replie footer { display: none; }
.panneau .corps { padding: 2px 16px 12px; border-top: 1px solid var(--trait); }
.panneau label { display: block; font-size: 12px; font-weight: 600; color: var(--doux); margin: 12px 0 5px; text-transform: uppercase; letter-spacing: .03em; }
input, select { width: 100%; font: inherit; padding: 8px 10px; border: 1px solid var(--trait); border-radius: 6px; background: #fff; color: var(--encre); }
#cherche { padding-left: 34px; background: #fff url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' fill='none' stroke='%235f5b52' stroke-width='2'%3E%3Ccircle cx='7' cy='7' r='5'/%3E%3Cpath d='m11 11 4 4'/%3E%3C/svg%3E") no-repeat 10px center; }
input:focus, select:focus { outline: 2px solid var(--turquoise); outline-offset: 1px; }
.ligne-quartier { display: flex; gap: 6px; }
#tous { flex: none; font: inherit; font-size: 13px; padding: 0 10px; border: 1px solid var(--trait); border-radius: 6px;
  background: #fff; color: var(--doux); cursor: pointer; }
#tous:hover { background: var(--survol); }
#tous[hidden] { display: none; }
#aucun { margin: 6px 0 0; font-size: 13px; color: var(--doux); }
#ir { text-overflow: ellipsis; }
#ir-compte { margin: 4px 0 0; font-size: 13px; color: var(--doux); }
.fiche .filtre { margin: 0 0 4px; padding: 4px 8px; border-radius: 5px; background: var(--survol); }
.panneau footer { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 8px 16px;
  border-top: 1px solid var(--trait); background: #faf9f6; font-size: 12px; color: var(--doux); border-radius: 0 0 10px 10px; }
.panneau footer[hidden] { display: none; }
.panneau footer a { color: #1f7a7e; font-weight: 600; text-decoration: none; white-space: nowrap; }
.panneau footer a:hover { text-decoration: underline; }
.recherche { position: relative; }
#suggestions { list-style: none; margin: 2px 0 0; padding: 0; position: absolute; left: 0; right: 0; z-index: 2;
  background: #fff; border: 1px solid var(--trait); border-radius: 6px; max-height: min(260px, 50vh); overflow-y: auto;
  overscroll-behavior: contain; scrollbar-width: thin; scrollbar-color: var(--gris) transparent; box-shadow: 0 4px 12px rgba(0,0,0,.12); }
#suggestions::-webkit-scrollbar { width: 8px; }
#suggestions::-webkit-scrollbar-thumb { background: var(--gris); border-radius: 4px; }
#suggestions:empty { display: none; }
#suggestions li { padding: 7px 10px; cursor: pointer; }
#suggestions li small { color: var(--doux); display: block; font-size: 12px; }
#suggestions li[aria-selected="true"], #suggestions li:hover { background: var(--survol); }
.legende { display: flex; flex-wrap: wrap; gap: 6px 14px; margin-top: 12px; font-size: 13px; color: var(--doux); }
.legende span { display: inline-flex; align-items: center; gap: 6px; }
.legende i { display: inline-block; width: 18px; height: 3px; background: var(--encre); }
.legende i.site { width: 11px; height: 11px; border-radius: 50%; background: var(--rouge); border: 2px solid #fff; box-shadow: 0 0 0 1px var(--rouge); }
.legende i.quartier { width: 14px; height: 12px; background: linear-gradient(90deg, var(--turquoise) 25%, var(--jaune) 25% 50%, var(--violet) 50% 75%, var(--gris) 75%); opacity: .7; }
/* Boîte des fonds et couches */
.leaflet-control-layers { border: 0 !important; border-radius: 10px !important; box-shadow: 0 4px 18px rgba(0,0,0,.2) !important; }
.leaflet-control-layers-expanded { padding: 10px 14px 10px 12px; font: 14px/1.3 system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--encre); min-width: 210px; }
.leaflet-control-layers-scrollbar { overflow-y: auto; }
.leaflet-control-layers .intitule { margin: 0 0 4px; font-size: 12px; font-weight: 600; color: var(--doux); text-transform: uppercase; letter-spacing: .03em; }
.leaflet-control-layers label { margin: 0; padding: 4px 6px; border-radius: 6px; cursor: pointer; }
.leaflet-control-layers label:hover { background: var(--survol); }
.leaflet-control-layers label > span { display: flex; align-items: center; gap: 8px; }
.leaflet-control-layers label > span > span { display: inline-flex; align-items: center; gap: 8px; }
.leaflet-control-layers-selector { margin: 0; accent-color: var(--turquoise); width: 16px; height: 16px; top: 0; }
.leaflet-control-layers-separator { margin: 8px -14px 8px -12px; border-top-color: var(--trait); }
/* boîte dépliée au-dessus du panneau (téléphone : le panneau couvre le haut de l'écran) */
.leaflet-top.leaflet-right:has(.leaflet-control-layers-expanded) { z-index: 1001; }
/* Fonds de carte et plans anciens : une fiche par couche (poignée, vignette, nom, œil, chevron ; réglages dépliables) */
.section-fiches { width: 330px; max-width: 100%; }
.liste-fiches { list-style: none; margin: 0 -6px; padding: 0; }
.liste-fiches li { border-radius: 8px; background: var(--fond); }
.liste-fiches li + li { margin-top: 2px; }
.liste-fiches.replie li:not(.principal):not(.actif) { display: none; }   /* fonds repliés : Plan IGN et fonds affichés */
.liste-fiches li.ouvert { background: #faf9f6; box-shadow: inset 0 0 0 1px var(--trait); }
.liste-fiches li.glisse { position: relative; z-index: 1; background: var(--fond); box-shadow: 0 6px 18px rgba(0,0,0,.2); }
.liste-fiches .tete { display: flex; align-items: center; gap: 8px; padding: 6px 4px 6px 0; }
.liste-fiches .poignee { flex: none; width: 22px; height: 36px; padding: 0; border: 0; border-radius: 5px; background: none;
  color: var(--gris); cursor: grab; touch-action: none; display: grid; place-items: center; }
.liste-fiches li.glisse .poignee { cursor: grabbing; color: var(--encre); }
.liste-fiches .vignette { flex: none; width: 44px; height: 44px; border-radius: 6px; object-fit: cover;
  border: 1px solid var(--trait); background: var(--survol); }
.liste-fiches li:not(.actif) .vignette { filter: saturate(.35); opacity: .75; }
.liste-fiches .titre { flex: 1; min-width: 0; display: flex; flex-direction: column; gap: 1px; }
.liste-fiches .nom { font-size: 14px; font-weight: 600; color: var(--encre); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
/* fonds datés (photographies aériennes) : la date en grand, le type de fond dessous */
.liste-fiches .date { font-size: 16px; font-weight: 700; color: var(--encre); font-variant-numeric: tabular-nums; white-space: nowrap; }
.liste-fiches .type { font-size: 12px; color: var(--doux); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.liste-fiches .oeil, .liste-fiches .deplier { flex: none; width: 30px; height: 30px; padding: 0; border: 0; border-radius: 6px;
  background: none; color: var(--doux); cursor: pointer; display: grid; place-items: center; }
.liste-fiches .tete button:hover { background: var(--survol); color: var(--encre); }
.liste-fiches li.actif .oeil { color: #1f7a7e; }
.liste-fiches li.actif .oeil .barre { display: none; }
.liste-fiches .deplier svg { transition: transform .15s; }
.liste-fiches li.ouvert .deplier svg { transform: rotate(180deg); }
.liste-fiches .reglages { padding: 0 10px 10px 30px; }
.liste-fiches .opacite { display: flex; align-items: center; gap: 10px; }
.liste-fiches input[type=range] { flex: 1; min-width: 0; width: auto; padding: 0; border: 0; accent-color: var(--turquoise); }
.liste-fiches output { width: 3.2em; text-align: right; font-size: 12px; color: var(--doux); font-variant-numeric: tabular-nums; }
.liste-fiches .actions { display: flex; flex-wrap: wrap; gap: 2px 14px; margin-top: 6px; }
.liste-fiches .actions button { display: inline-flex; align-items: center; gap: 5px; padding: 3px 2px; border: 0; border-radius: 5px;
  background: none; font: 13px system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: #1f7a7e; cursor: pointer; }
.liste-fiches .actions button:hover { text-decoration: underline; }
.liste-fiches .source { margin: 6px 0 0; font-size: 12px; line-height: 1.4; color: var(--doux); }
.liste-fiches .source a { color: #1f7a7e; }
.liste-fiches button:focus-visible { outline: 2px solid var(--turquoise); outline-offset: 1px; }
.autres-fonds { display: inline-flex; align-items: center; gap: 4px; margin: 2px 0 0; padding: 4px 6px; border: 0; border-radius: 6px;
  background: none; font: 13px system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: #1f7a7e; cursor: pointer; }
.autres-fonds:hover { background: var(--survol); }
.autres-fonds:focus-visible { outline: 2px solid var(--turquoise); outline-offset: 1px; }
.autres-fonds svg { width: 16px; height: 16px; transition: transform .15s; }
.autres-fonds[aria-expanded="true"] svg { transform: rotate(180deg); }
.leg { display: inline-block; flex: none; }
.leg.quartier { width: 16px; height: 12px; border-radius: 2px; opacity: .75;
  background: linear-gradient(90deg, var(--turquoise) 25%, var(--jaune) 25% 50%, var(--violet) 50% 75%, var(--gris) 75%); }
.leg.rue { width: 16px; height: 3px; background: var(--encre); }
.leg.site { width: 11px; height: 11px; margin: 0 2px; border-radius: 50%; background: var(--rouge); box-shadow: 0 0 0 2px #fff, 0 0 0 3px var(--rouge); }
.leaflet-popup-content { margin: 12px 14px; font-size: 14px; }
.fiche h2 { font-size: 16px; margin: 0 0 6px; }
.fiche p { margin: 3px 0; }
.fiche .meta { color: var(--doux); font-size: 13px; }
.fiche a.bouton { display: inline-block; margin-top: 8px; padding: 6px 10px; background: var(--turquoise); color: #fff;
  border-radius: 5px; text-decoration: none; font-weight: 600; }
.fiche a.bouton:hover { background: #24888c; }
.fiche .notices { margin: 6px 0 0; padding-right: 4px; max-height: 260px; overflow: auto; font-size: 13px; }
.fiche .notices summary { cursor: pointer; font-weight: 600; margin: 4px 0; }
.fiche .notices h3 { font-size: 13px; margin: 4px 0; }
.fiche ol.notices, .fiche .notices ol { margin: 2px 0 6px; padding: 0 0 0 20px; max-height: none; }
.fiche ol.notices li { margin: 2px 0; }
.fiche ol.notices a { color: #1f7a7e; }
.fiche button.lien { font: inherit; background: none; border: 0; padding: 0; color: #1f7a7e; text-decoration: underline; cursor: pointer; }
.fermer-couches { display: none; }
/* Coordonnées du point cliqué ou du lieu sélectionné (en bas à gauche, discret) */
.coord { display: flex; align-items: center; gap: 2px; max-width: 340px; padding: 2px 2px 2px 4px; border-radius: 6px;
  background: rgba(255,255,255,.85); box-shadow: 0 1px 4px rgba(0,0,0,.18); font: 12px system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--doux); }
.coord select { flex: none; width: auto; padding: 2px 2px; border: 0; border-radius: 4px; background: transparent; font-size: 12px; color: var(--doux); cursor: pointer; }
.coord select:hover { background: var(--survol); }
.coord output { flex: 1; min-width: 0; padding: 0 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
  font: 12px ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; color: var(--encre); }
.coord output.vide { font: italic 12px system-ui, -apple-system, "Segoe UI", Roboto, sans-serif; color: var(--doux); }
.coord button { flex: none; width: 26px; height: 26px; padding: 0; border: 0; border-radius: 5px; background: none; color: var(--doux);
  cursor: pointer; display: grid; place-items: center; }
.coord button:hover:not(:disabled) { background: var(--survol); color: var(--encre); }
.coord button:disabled { opacity: .35; cursor: default; }
.coord button.ok { color: #1f7a7e; }
.coord button:focus-visible, .coord select:focus-visible { outline: 2px solid var(--turquoise); outline-offset: 0; }
@media (max-width: 600px) {   /* téléphone : au-dessus de la ligne d'attribution, à gauche de l'échelle */
  .leaflet-bottom.leaflet-left .coord { margin-bottom: 26px; max-width: calc(100vw - 104px); }
}
@media (max-width: 600px) {
  .panneau { top: 8px; left: 8px; right: 8px; width: auto; }
  .panneau h1 { font-size: 16px; }
  .panneau header p { font-size: 12px; }
  /* zoom et couches en bas à droite, au-dessus de l'échelle et de l'attribution */
  .leaflet-top.leaflet-right { top: auto; bottom: 46px; }
  /* boîte des couches dépliée : volet en bas de l'écran, défilant ; le zoom s'efface le temps qu'elle est ouverte */
  .leaflet-control-layers-expanded { position: fixed; left: 0; right: 0; bottom: 0; margin: 0 !important;
    max-height: 75vh; overflow-y: auto; border-radius: 12px 12px 0 0 !important; padding-top: 6px; }
  .leaflet-control-layers-expanded .leaflet-control-layers-list { height: auto !important; overflow: visible; }
  .leaflet-control-layers-expanded .section-fiches { width: auto; }
  .leaflet-top.leaflet-right:has(.leaflet-control-layers-expanded) .leaflet-control-zoom { visibility: hidden; }
  .leaflet-control-layers-expanded .fermer-couches { display: block; position: sticky; top: 0; z-index: 2; margin: 0 -8px 4px auto;
    width: 36px; height: 36px; border: 0; border-radius: 8px; background: #fff; color: var(--doux); font-size: 22px; line-height: 1; cursor: pointer; }
}
/* écran bas (téléphone en paysage) : listes de notices plus courtes pour que la fiche tienne */
@media (max-height: 500px) {
  .fiche .notices { max-height: 130px; }
}
</style>
</head>
<body>
<div id="carte" role="application" aria-label="Carte des lieux de Roubaix"></div>
<section class="panneau" id="panneau" aria-label="Recherche">
  <div class="bandeau" aria-hidden="true"><span style="background:var(--turquoise)"></span><span style="background:var(--jaune)"></span><span style="background:var(--violet)"></span><span style="background:var(--gris)"></span></div>
  <header>
    <h1>Bn-r : accès par plan</h1>
    <p>Cliquez sur une rue, un quartier ou un site pour voir ses documents.</p>
    <button id="replier" aria-expanded="true" aria-controls="corps" title="Replier le panneau">▴</button>
  </header>
  <div class="corps" id="corps">
    <label for="cherche">Rechercher une rue ou un site</label>
    <div class="recherche">
      <input id="cherche" type="search" autocomplete="off" placeholder="Nom actuel ou ancien…"
             role="combobox" aria-expanded="false" aria-controls="suggestions" aria-autocomplete="list">
      <ul id="suggestions" role="listbox"></ul>
    </div>
    <p id="aucun" role="status" hidden>Aucun lieu trouvé.</p>
    <label for="quartier">Quartier</label>
    <div class="ligne-quartier">
      <select id="quartier"><option value="">Tous les quartiers</option></select>
      <button id="tous" type="button" hidden title="Revoir toute la ville">Toute la ville</button>
    </div>
    <label for="ir">Instrument de recherche</label>
    <select id="ir"><option value="">Tous les instruments de recherche</option></select>
    <p id="ir-compte" role="status" hidden></p>
    <div class="legende" aria-hidden="true">
      <span><i class="quartier"></i>Quartiers</span><span><i></i>Rues</span><span><i class="site"></i>Sites</span>
    </div>
  </div>
  <footer hidden>
    <a id="pleinecran" href="./" target="_blank" rel="noopener" hidden>Ouvrir en plein écran ↗</a>
  </footer>
</section>
<script src="https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.9.4/leaflet.min.js"></script>
<script>
const D = __DONNEES__;

const echap = s => String(s).replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
const sansAccents = s => s.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();
const mentions = n => n + (n > 1 ? " mentions" : " mention") + " dans les inventaires";

const carte = L.map("carte", { zoomControl: false, preferCanvas: true }).setView([50.6905, 3.176], 14);
L.control.zoom({ position: "topright" }).addTo(carte);
L.control.scale({ imperial: false, position: "bottomright" }).addTo(carte);
const rendu = L.canvas({ tolerance: 8 });   // une rue se clique à 8 px près, sans zone fantôme

// Fonds de la Géoplateforme (WMTS, sans clé) ; maxNative = dernier niveau publié (au-delà, agrandi)
// (les photographies 1965-1980 n'ont que le style BDORTHOHISTORIQUE ; celles de 1980-1995
// ne couvrent pas Roubaix : 404 à tous les niveaux, vérifié le 14/11/2026)
const geopf = (couche, format, maxNative = 19, style = "normal") => L.tileLayer(
  "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&STYLE=" + style + "&TILEMATRIXSET=PM" +
  "&LAYER=" + couche + "&FORMAT=" + format + "&TILEMATRIX={z}&TILEROW={y}&TILECOL={x}",
  { maxZoom: 19, maxNativeZoom: maxNative, attribution: '&copy; <a href="https://www.ign.fr/">IGN</a> – Géoplateforme' });
// Vignette d'un fond = sa tuile z15 du centre de Roubaix
const VIGNETTE = { z: 15, x: 16673, y: 11017 };
const vignetteGeopf = (couche, format, style = "normal") =>
  "https://data.geopf.fr/wmts?SERVICE=WMTS&REQUEST=GetTile&VERSION=1.0.0&STYLE=" + style + "&TILEMATRIXSET=PM&LAYER=" + couche +
  "&FORMAT=" + format + "&TILEMATRIX=" + VIGNETTE.z + "&TILEROW=" + VIGNETTE.y + "&TILECOL=" + VIGNETTE.x;
const SOURCE_IGN = '&copy; <a href="https://www.ign.fr/">IGN</a> – Géoplateforme';
// Fonds de carte : superposables comme les plans anciens (toujours dessinés dessous) ; seul le Plan IGN
// est affiché au départ et reste visible dans la liste repliée (principal)
const fondsCarte = [
  { nom: "Plan IGN", principal: true, actif: true, couche: geopf("GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2", "image/png"),
    vignette: vignetteGeopf("GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2", "image/png"), source: "Plan IGN v2 – " + SOURCE_IGN },
  { nom: "Photographies aériennes actuelles", date: "Actuelles", type: "Photographies aériennes", couche: geopf("ORTHOIMAGERY.ORTHOPHOTOS", "image/jpeg"),
    vignette: vignetteGeopf("ORTHOIMAGERY.ORTHOPHOTOS", "image/jpeg"), source: "Photographies aériennes actuelles (BD ORTHO) – " + SOURCE_IGN },
  { nom: "Photographies aériennes 1965-1980", date: "1965 – 1980", type: "Photographies aériennes", couche: geopf("ORTHOIMAGERY.ORTHOPHOTOS.1965-1980", "image/png", 18, "BDORTHOHISTORIQUE"),
    vignette: vignetteGeopf("ORTHOIMAGERY.ORTHOPHOTOS.1965-1980", "image/png", "BDORTHOHISTORIQUE"), source: "Photographies aériennes 1965-1980 (BD ORTHO historique) – " + SOURCE_IGN },
  { nom: "Photographies aériennes 1950-1965", date: "1950 – 1965", type: "Photographies aériennes", couche: geopf("ORTHOIMAGERY.ORTHOPHOTOS.1950-1965", "image/png", 18),
    vignette: vignetteGeopf("ORTHOIMAGERY.ORTHOPHOTOS.1950-1965", "image/png"), source: "Photographies aériennes 1950-1965 (BD ORTHO historique) – " + SOURCE_IGN },
  { nom: "Carte de 1950 (1/50 000)", couche: geopf("GEOGRAPHICALGRIDSYSTEMS.MAPS.SCAN50.1950", "image/jpeg", 15),
    vignette: vignetteGeopf("GEOGRAPHICALGRIDSYSTEMS.MAPS.SCAN50.1950", "image/jpeg"), source: "Carte de France au 1/50 000, type 1922, vers 1950 (SCAN 50 historique) – " + SOURCE_IGN },
  { nom: "OpenStreetMap", couche: L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",
      { maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' }),
    vignette: `https://tile.openstreetmap.org/${VIGNETTE.z}/${VIGNETTE.x}/${VIGNETTE.y}.png`,
    source: '&copy; contributeurs <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' },
];
fondsCarte.filter(f => f.actif).forEach(f => f.couche.addTo(carte));

// Plans anciens de Roubaix géoréférencés : tuiles XYZ WebP servies avec la page (plans/<année>/),
// copiées depuis rbx-bnr-data/data/geo/plans_anciens/leaflet_1899. Superposables au fond choisi
// (voir la section « Plans anciens » de la boîte des couches) ; vignette = un carré pris au milieu du plan
// (plans/<année>/vignette.webp, produite par scripts/ead/vignettes_plans_anciens.py).
// max = niveau de pleine résolution des tuiles (au-delà, agrandi).
const plansAnciens = [   // ordre chronologique par défaut (en haut de la liste = dessiné au-dessus)
  { nom: "Roubaix vers 1811", dossier: "1811",
    sw: [50.6628, 3.1378], ne: [50.7145, 3.2249], max: 17,
    source: "Roubaix il y a un siècle (vers 1811), plan d'Édouard Verscheure pour l'Exposition de 1911 – BN-R, PLA_B_033" },
  { nom: "Roubaix en 1855", dossier: "1855",
    sw: [50.6838, 3.1573], ne: [50.7025, 3.1908], max: 18,
    source: 'Plan de Roubaix, 1855, E. Barbotin – <a href="https://gallica.bnf.fr/">BnF</a>, Ge C 3003' },
  { nom: "Roubaix en 1889", dossier: "1889",
    sw: [50.6683, 3.1502], ne: [50.7109, 3.2202], max: 18,
    source: "Plan de la ville de Roubaix dressé en 1889 par le service de la voirie – Archives municipales de Roubaix, 6Fi022" },
  { nom: "Roubaix en 1899", dossier: "1899",
    sw: [50.6698, 3.1501], ne: [50.7111, 3.2188], max: 17,
    source: "Plan de la ville de Roubaix, 1899, P. Pachy – Archives municipales de Roubaix, 6Fi024" },
];
plansAnciens.forEach(p => {
  p.vignette = "plans/" + p.dossier + "/vignette.webp";
  p.couche = L.tileLayer("plans/" + p.dossier + "/{z}/{x}/{y}.webp",
    { minZoom: 12, maxZoom: 19, maxNativeZoom: p.max, bounds: L.latLngBounds(p.sw, p.ne), attribution: p.source });
});

function fiche(o, type) {
  const lignes = [`<div class="fiche"><h2>${echap(o.nom)}</h2>`];
  if (type === "site" && o.categorie) lignes.push(`<p class="meta">${echap(o.categorie)}</p>`);
  if (o.anciens && o.anciens.length) lignes.push(`<p><b>Ancien${o.anciens.length > 1 ? "s noms" : " nom"} :</b> ${o.anciens.map(echap).join(", ")}</p>`);
  if (o.quartiers && o.quartiers.length) lignes.push(`<p><b>Quartier${o.quartiers.length > 1 ? "s" : ""} :</b> ` +
    o.quartiers.map(q => `<button class="lien" data-quartier="${echap(q)}">${echap(q)}</button>`).join(", ") + "</p>");
  if (type === "quartier") lignes.push(`<p>${o.voies} rue${o.voies > 1 ? "s" : ""} citée${o.voies > 1 ? "s" : ""} dans ce quartier</p>`);
  if (o.notices) {   // rue, quartier ou site : la liste de ses documents, chacun vers sa notice sur la BN-R
    // classées par inventaire (IR) : les plus fournis d'abord ; repliés s'il y en a plusieurs et plus de 8 documents.
    // Filtre par instrument de recherche : seulement ses documents (tous, avec une mention, si le lieu n'en a aucun)
    let notices = o.notices, n = o.n, filtre = false;
    if (irChoisi) {
      const titreIr = "« " + echap(D.ir[irChoisi] || irChoisi) + " »";
      const garde = o.notices.filter(([ir]) => ir === irChoisi), k = garde.reduce((s, [, l]) => s + l.length, 0);
      if (garde.length) {
        notices = garde; n = k; filtre = true;
        if (o.n > k) lignes.push(`<p class="meta filtre">Documents de ${titreIr} seulement (${o.n - k} autre${o.n - k > 1 ? "s" : ""} dans d'autres inventaires)</p>`);
      } else lignes.push(`<p class="meta filtre">Aucun document de ${titreIr} : documents des autres inventaires</p>`);
    }
    // Archives municipales (IR « AM_… ») : pas de lien, la cote suit le titre
    const ligne = (ir, [titre, date, ark, cote]) => "<li>" + (ark ? `<a href="${echap(D.ark + ark)}" target="_blank" rel="noopener">${echap(titre)}</a>`
      : ir.startsWith("AM_") ? `${echap(titre)} <span class="meta">${echap(cote || "")}</span>`
      : `${echap(titre)} <span class="meta">(pas encore en ligne)</span>`) + (date ? ` <span class="meta">${echap(date)}</span>` : "") + "</li>";
    const ouvert = notices.length === 1 || n <= 8;
    lignes.push((filtre ? "" : `<p class="meta">${n} document${n > 1 ? "s" : ""} dans ${notices.length > 1 ? notices.length + " inventaires" : "les inventaires"}</p>`) +
      '<div class="notices">' + notices.map(([ir, l]) => {
        const titreIr = `${echap(D.ir[ir] || ir)} <span class="meta">(${l.length})</span>`;
        const items = l.map(x => ligne(ir, x)).join("");
        return ouvert ? `<h3>${titreIr}</h3><ol>${items}</ol>`
                      : `<details><summary>${titreIr}</summary><ol>${items}</ol></details>`;
      }).join("") + "</div>");
    return lignes.join("") + "</div>";
  }
  lignes.push(`<p class="meta">${mentions(o.n)}</p>`);
  if (o.lien) lignes.push(`<a class="bouton" href="${echap(o.lien)}" target="_blank" rel="noopener">Voir les documents sur la BN-R</a>`);
  else lignes.push(`<p class="meta"><i>Pas encore de document en ligne sur la BN-R.</i></p>`);
  return lignes.join("") + "</div>";
}

let lieuChoisi = null;   // rue, quartier ou site dont la fiche est ouverte (widget des coordonnées)
let irChoisi = "";       // instrument de recherche choisi dans le panneau ("" : tous)
const dansIr = o => !irChoisi || (o.notices || []).some(([ir]) => ir === irChoisi);

// Quartiers
const parQuartier = {};
const coucheQuartiers = L.layerGroup().addTo(carte);
const styleQuartier = (o, etat) => ({ renderer: rendu, color: o.couleur, weight: etat === "choisi" ? 3 : 1.5,
  fillColor: o.couleur, fillOpacity: etat === "choisi" ? 0.55 : etat === "estompe" ? 0.12 : 0.4, opacity: etat === "estompe" ? 0.5 : 1 });
D.quartiers.forEach(o => {
  o.couche = L.geoJSON(o.geom, { style: () => styleQuartier(o) })
    .bindTooltip(o.nom, { sticky: true, direction: "top" })
    .bindPopup(() => fiche(o, "quartier"))
    .on("popupopen", () => { lieuChoisi = o; history.replaceState(null, "", "#quartier=" + o.id); })
    .addTo(coucheQuartiers);
  parQuartier[o.nom] = o;
  const opt = document.createElement("option"); opt.value = o.nom; opt.textContent = o.nom;
  document.getElementById("quartier").append(opt);
});
[...document.getElementById("quartier").options].slice(1).sort((a, b) => a.text.localeCompare(b.text, "fr"))
  .forEach(o => document.getElementById("quartier").append(o));

// Voies
const parVoie = {};
const coucheVoies = L.layerGroup().addTo(carte);
const styleVoie = etat => ({ renderer: rendu, color: etat === "survol" ? "#2FAAAF" : "#1d1d1b",
  weight: etat === "survol" ? 6 : etat === "choisi" ? 4 : 2.5, opacity: etat === "estompe" ? 0.25 : 1 });
let quartierChoisi = "";
const etatVoie = v => quartierChoisi ? (v.quartiers.includes(quartierChoisi) ? "choisi" : "estompe") : "";
let voieOuverte = null;   // voie dont la fiche est ouverte : reste en surbrillance
D.voies.forEach(v => {
  v.couche = L.geoJSON(v.geom, { style: () => styleVoie(etatVoie(v)) })
    .bindTooltip(v.nom, { sticky: true })
    .bindPopup(() => fiche(v, "voie"))
    .on("mouseover", () => v.couche.setStyle(styleVoie("survol")))
    .on("mouseout", () => { if (voieOuverte !== v) v.couche.setStyle(styleVoie(etatVoie(v))); })
    .on("popupopen", () => { voieOuverte = lieuChoisi = v; v.couche.setStyle(styleVoie("survol")); v.couche.bringToFront();
                             history.replaceState(null, "", "#voie=" + v.id); })
    .on("popupclose", () => { voieOuverte = null; v.couche.setStyle(styleVoie(etatVoie(v))); })
    .addTo(coucheVoies);
  parVoie[v.id] = v;
});

// Sites
const coucheSites = L.layerGroup().addTo(carte);
const parSite = {};
D.sites.forEach(s => {
  const [lon, lat] = s.geom.coordinates;
  s.couche = L.circleMarker([lat, lon], { renderer: rendu, radius: 8, color: "#fff", weight: 2, fillColor: D.rouge, fillOpacity: 1 })
    .bindTooltip(s.nom, { direction: "top" }).bindPopup(() => fiche(s, "site"))
    .on("popupopen", () => { lieuChoisi = s; history.replaceState(null, "", "#site=" + s.id); })
    .addTo(coucheSites);
  parSite[s.id] = s;
});

// Boîte des fonds et couches : fonds de carte et plans anciens en fiches (ci-dessous), couches vectorielles
// par le contrôle de Leaflet, en français, avec une petite légende devant chaque couche
const choixCouches = L.control.layers({}, {
  '<i class="leg quartier"></i>Quartiers': coucheQuartiers,
  '<i class="leg rue"></i>Rues': coucheVoies,
  '<i class="leg site"></i>Sites': coucheSites,
}, { position: "topright" }).addTo(carte);
{
  const boite = choixCouches.getContainer();
  const bouton = boite.querySelector(".leaflet-control-layers-toggle");
  // téléphone : bouton de fermeture du volet des couches (ailleurs, la boîte se ferme en quittant la souris)
  const fermer = Object.assign(document.createElement("button"), { type: "button", className: "fermer-couches", textContent: "×",
    title: "Fermer", ariaLabel: "Fermer le choix des fonds et couches" });
  fermer.addEventListener("click", ev => { ev.stopPropagation(); choixCouches.collapse(); });
  bouton.title = "Fond de carte et couches"; bouton.setAttribute("aria-label", "Fond de carte et couches");
  const intitule = texte => Object.assign(document.createElement("p"), { className: "intitule", textContent: texte });
  boite.querySelector(".leaflet-control-layers-overlays").prepend(intitule("Couches"));

  // Sections « Plans anciens » puis « Fond de carte », sous les couches de la BN-R (même ordre que le dessin :
  // couches, puis plans anciens, puis fonds), sur le modèle du gestionnaire
  // de couches du Géoportail : une fiche par couche avec poignée (glisser, ou flèches haut/bas au clavier,
  // pour l'ordre d'affichage : en haut de la liste = dessiné au-dessus), vignette, nom, œil (afficher/masquer)
  // et chevron (réglages : opacité, « Centrer sur le plan » pour les plans anciens, source).
  // Les plans anciens sont toujours au-dessus des fonds (zIndex 100-… contre 50-…), sous les couches vectorielles.
  const svg = (vb, corps, t = 18) => `<svg viewBox="${vb}" width="${t}" height="${t}" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${corps}</svg>`;
  const ico = {
    poignee: '<svg viewBox="0 0 10 16" width="10" height="16" aria-hidden="true" fill="currentColor"><circle cx="2" cy="3" r="1.4"/><circle cx="8" cy="3" r="1.4"/><circle cx="2" cy="8" r="1.4"/><circle cx="8" cy="8" r="1.4"/><circle cx="2" cy="13" r="1.4"/><circle cx="8" cy="13" r="1.4"/></svg>',
    oeil: svg("0 0 24 24", '<path d="M2 12s3.6-7 10-7 10 7 10 7-3.6 7-10 7S2 12 2 12z"/><circle cx="12" cy="12" r="3"/><path class="barre" d="M3 3l18 18"/>', 20),
    chevron: svg("0 0 24 24", '<path d="M6 9l6 6 6-6"/>'),
    centrer: svg("0 0 24 24", '<circle cx="11" cy="11" r="7"/><path d="M21 21l-5-5M11 8v6M8 11h6"/>', 16),
  };
  const listeFiches = (titre, elements, zHaut, centrer) => {
    const section = Object.assign(document.createElement("div"), { className: "section-fiches" });
    section.append(intitule(titre));
    const liste = Object.assign(document.createElement("ol"), { className: "liste-fiches" });
    section.append(liste);
    const ordonner = () => [...liste.children].forEach((li, i) => li.element.couche.setZIndex(zHaut - i));
    const visible = x => x.offsetParent !== null;   // fiches masquées par le repli : ignorées pour l'ordre
    const voisin = (li, sens) => { let x = li; do { x = sens < 0 ? x.previousElementSibling : x.nextElementSibling; } while (x && !visible(x)); return x; };
    elements.forEach(p => {
      const li = document.createElement("li"); li.element = p;
      if (p.principal) li.classList.add("principal");
      const nom = echap(p.nom);
      li.innerHTML = `<div class="tete">
          <button type="button" class="poignee" title="Glisser pour changer l'ordre d'affichage" aria-label="Ordre d'affichage : ${nom} (flèches haut et bas)">${ico.poignee}</button>
          <img class="vignette" src="${echap(p.vignette)}" alt="" width="44" height="44">
          <div class="titre">${p.date ? `<span class="date">${echap(p.date)}</span><span class="type">${echap(p.type)}</span>` : `<span class="nom">${nom}</span>`}</div>
          <button type="button" class="oeil" aria-pressed="false" title="Afficher" aria-label="Afficher : ${nom}">${ico.oeil}</button>
          <button type="button" class="deplier" aria-expanded="false" title="Réglages" aria-label="Réglages : ${nom}">${ico.chevron}</button>
        </div>
        <div class="reglages" hidden>
          <div class="opacite"><input type="range" min="0" max="100" step="5" value="100" aria-label="Opacité : ${nom}"><output>100 %</output></div>
          ${centrer ? `<div class="actions"><button type="button" class="act-centrer" aria-label="Centrer sur le plan : ${nom}">${ico.centrer}Centrer sur le plan</button></div>` : ""}
          <p class="source">${p.source}</p>
        </div>`;
      const q = sel => li.querySelector(sel);
      const marquer = oui => {
        li.classList.toggle("actif", oui);
        q(".oeil").setAttribute("aria-pressed", oui); q(".oeil").title = oui ? "Masquer" : "Afficher";
        q(".oeil").setAttribute("aria-label", (oui ? "Masquer : " : "Afficher : ") + p.nom);
      };
      const afficher = oui => { if (oui) p.couche.addTo(carte); else p.couche.remove(); marquer(oui); };
      marquer(carte.hasLayer(p.couche));
      q(".oeil").addEventListener("click", () => afficher(!carte.hasLayer(p.couche)));
      q(".deplier").addEventListener("click", () => {
        const ouvrir = q(".reglages").hidden;
        q(".reglages").hidden = !ouvrir; q(".deplier").setAttribute("aria-expanded", ouvrir); li.classList.toggle("ouvert", ouvrir);
      });
      q("input[type=range]").addEventListener("input", e => {
        p.couche.setOpacity(e.target.value / 100); q("output").textContent = e.target.value + " %";
      });
      if (centrer) q(".act-centrer").addEventListener("click", () => {
        afficher(true); carte.fitBounds(L.latLngBounds(p.sw, p.ne), marges());
      });
      // ordre : glisser la poignée (souris ou doigt) ou flèches haut/bas quand elle a le focus
      const poignee = q(".poignee");
      poignee.addEventListener("keydown", e => {
        const v = e.key === "ArrowUp" ? voisin(li, -1) : e.key === "ArrowDown" ? voisin(li, 1) : undefined;
        if (v === undefined) return;
        e.preventDefault();
        if (!v) return;
        if (e.key === "ArrowUp") liste.insertBefore(li, v); else liste.insertBefore(li, v.nextElementSibling);
        ordonner(); poignee.focus();
      });
      poignee.addEventListener("pointerdown", e => {
        if (e.button !== 0) return;
        // suivi au niveau de la fenêtre : déplacer l'élément dans la liste ferait perdre une capture du pointeur
        e.preventDefault(); li.classList.add("glisse");
        const bouger = ev => {
          const suivant = [...liste.children].filter(x => x !== li && visible(x))
            .find(x => { const r = x.getBoundingClientRect(); return ev.clientY < r.top + r.height / 2; });
          if (suivant) { if (li.nextElementSibling !== suivant) liste.insertBefore(li, suivant); }
          else if (liste.lastElementChild !== li) liste.append(li);
        };
        const finir = () => {
          li.classList.remove("glisse"); window.removeEventListener("pointermove", bouger);
          window.removeEventListener("pointerup", finir); window.removeEventListener("pointercancel", finir); ordonner();
        };
        window.addEventListener("pointermove", bouger);
        window.addEventListener("pointerup", finir); window.addEventListener("pointercancel", finir);
      });
      liste.append(li);
    });
    ordonner();
    return { section, liste };
  };
  const separateur = () => Object.assign(document.createElement("div"), { className: "leaflet-control-layers-separator" });

  // Fond de carte : liste repliée au départ (Plan IGN et fonds affichés seulement) ; « Autres fonds de carte » la déplie
  const fondsUI = listeFiches("Fond de carte", fondsCarte, 50, false);
  fondsUI.liste.classList.add("replie");
  const autres = fondsCarte.filter(f => !f.principal).length;
  const plus = Object.assign(document.createElement("button"), { type: "button", className: "autres-fonds" });
  const majPlus = () => {
    const replie = fondsUI.liste.classList.contains("replie");
    plus.innerHTML = (replie ? `Autres fonds de carte (${autres})` : "Moins de fonds de carte") + ico.chevron;
    plus.setAttribute("aria-expanded", !replie);
  };
  plus.addEventListener("click", () => { fondsUI.liste.classList.toggle("replie"); majPlus(); });
  majPlus();
  fondsUI.section.append(plus);

  const plansUI = listeFiches("Plans anciens", plansAnciens, 100, true);
  const listeBoite = boite.querySelector(".leaflet-control-layers-list");
  listeBoite.prepend(fermer);
  listeBoite.append(separateur(), plansUI.section, separateur(), fondsUI.section);
}

// Fiches (popups) sur téléphone ou écran bas (paysage) : le panneau se replie quand une fiche s'ouvre, et Leaflet
// la cadre sous lui (lu par Leaflet au moment de cadrer la fiche ; ailleurs, réglage par défaut)
Object.defineProperty(L.Popup.prototype.options, "autoPanPaddingTopLeft", { configurable: true, get() {
  if (!surTelephone() && window.innerHeight > 500) return null;
  replier(true);
  return [10, Math.round(document.getElementById("panneau").getBoundingClientRect().bottom) + 10];
} });

// Marges de cadrage : laisser la place du panneau (à gauche sur grand écran, en haut sur téléphone)
const marges = extra => window.innerWidth > 600
  ? { paddingTopLeft: [360, 30], paddingBottomRight: [40, 30], ...extra }
  : { paddingTopLeft: [20, 90], paddingBottomRight: [20, 20], ...extra };   // panneau replié

// Choix d'un quartier : zoom, ses voies en avant, les autres estompées
function choisirQuartier(nom) {
  quartierChoisi = nom;
  document.getElementById("quartier").value = nom;
  D.quartiers.forEach(o => o.couche.setStyle(styleQuartier(o, etatQuartier(o))));
  D.voies.forEach(v => v.couche.setStyle(styleVoie(etatVoie(v))));
  document.getElementById("tous").hidden = !nom;
  if (nom) { carte.fitBounds(parQuartier[nom].couche.getBounds(), marges());
             history.replaceState(null, "", "#quartier=" + parQuartier[nom].id); }
  else { carte.fitBounds(ville, marges()); history.replaceState(null, "", location.pathname + location.search); }
}
const ville = L.featureGroup(D.quartiers.map(o => o.couche)).getBounds();
const etatQuartier = o => quartierChoisi ? (o.nom === quartierChoisi ? "choisi" : "estompe") : dansIr(o) ? "" : "estompe";
document.getElementById("tous").addEventListener("click", () => { carte.closePopup(); choisirQuartier(""); });

// Filtre par instrument de recherche : rues et sites sans document de cet IR masqués, quartiers estompés,
// fiches et recherche limitées à ses documents ; la vue se cadre sur les lieux concernés
{
  const selIr = document.getElementById("ir"), compte = document.getElementById("ir-compte");
  const docs = {};
  [...D.quartiers, ...D.voies, ...D.sites].forEach(o => (o.notices || []).forEach(([ir, l]) => { docs[ir] = (docs[ir] || 0) + l.length; }));
  Object.keys(docs).sort((a, b) => (D.ir[a] || a).localeCompare(D.ir[b] || b, "fr")).forEach(ir => {
    const opt = document.createElement("option"); opt.value = ir;
    opt.textContent = `${D.ir[ir] || ir} (${docs[ir].toLocaleString("fr-FR")})`; opt.title = opt.textContent;
    selIr.append(opt);
  });
  window.choisirIr = (ir, cadrer = true) => {
    irChoisi = ir; selIr.value = ir; carte.closePopup();
    D.voies.forEach(v => dansIr(v) ? coucheVoies.addLayer(v.couche) : coucheVoies.removeLayer(v.couche));
    D.sites.forEach(s => dansIr(s) ? coucheSites.addLayer(s.couche) : coucheSites.removeLayer(s.couche));
    D.quartiers.forEach(o => o.couche.setStyle(styleQuartier(o, etatQuartier(o))));
    const n = t => D[t].filter(o => ir && dansIr(o)).length;
    const pl = (k, un, plusieurs) => k + " " + (k > 1 ? plusieurs : un);
    compte.hidden = !ir;
    compte.textContent = ir ? [pl(n("voies"), "rue", "rues"), pl(n("sites"), "site", "sites"), pl(n("quartiers"), "quartier", "quartiers")].join(", ") : "";
    champ.value = ""; liste.innerHTML = ""; document.getElementById("aucun").hidden = true;
    if (!cadrer) return;
    const lieux = [...D.voies, ...D.sites, ...D.quartiers].filter(o => ir && dansIr(o)).map(o => o.couche);
    if (quartierChoisi) return;   // quartier choisi : la vue reste sur lui
    carte.fitBounds(lieux.length ? L.featureGroup(lieux).getBounds() : ville, marges({ maxZoom: 17 }));
  };
  selIr.addEventListener("change", e => { choisirIr(e.target.value); if (surTelephone()) replier(true); });
}
document.getElementById("quartier").addEventListener("change", e => { carte.closePopup(); choisirQuartier(e.target.value); });
carte.on("popupopen", e => e.popup.getElement().querySelectorAll("button[data-quartier]")
  .forEach(b => b.addEventListener("click", () => { carte.closePopup(); choisirQuartier(b.dataset.quartier); })));

function ouvrirVoie(v) {
  carte.fitBounds(v.couche.getBounds(), marges({ maxZoom: 17, animate: false }));   // sans animation : la fiche se cadre sur la vue finale
  const c = v.couche.getBounds().getCenter();
  v.couche.openPopup(c);
}

// Recherche : nom actuel ou ancien, sans tenir compte des accents
const index = [];
D.voies.forEach(v => { index.push({ v, cle: sansAccents(v.nom), texte: v.nom });
  v.anciens.forEach(a => index.push({ v, cle: sansAccents(a), texte: a, ancien: true })); });
D.sites.forEach(s => index.push({ v: s, cle: sansAccents(s.nom), texte: s.nom, site: true }));
const champ = document.getElementById("cherche"), liste = document.getElementById("suggestions");
let resultats = [], actif = -1;
function proposer() {
  const q = sansAccents(champ.value.trim());
  resultats = q.length < 2 ? [] : index.filter(e => e.cle.includes(q) && dansIr(e.v))
    .sort((a, b) => (b.cle.startsWith(q) - a.cle.startsWith(q)) || a.texte.localeCompare(b.texte, "fr")).slice(0, 50);  // liste défilante (ascenseur)
  actif = -1;
  liste.innerHTML = resultats.map((e, i) => `<li role="option" id="s${i}" data-i="${i}">${echap(e.texte)}` +
    (e.ancien ? `<small>ancien nom de ${echap(e.v.nom)}</small>` : e.site ? "<small>site</small>" : "") + "</li>").join("");
  champ.setAttribute("aria-expanded", resultats.length > 0);
  document.getElementById("aucun").hidden = !(q.length >= 2 && !resultats.length);
}
function choisir(i) {
  const e = resultats[i]; if (!e) return;
  champ.value = e.v.nom; liste.innerHTML = ""; champ.setAttribute("aria-expanded", false);
  if (e.site) { carte.setView(e.v.couche.getLatLng(), 17, { animate: false }); e.v.couche.openPopup(); } else ouvrirVoie(e.v);
}
champ.addEventListener("input", proposer);
champ.addEventListener("keydown", ev => {
  if (!resultats.length) return;
  if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
    ev.preventDefault();
    actif = (actif + (ev.key === "ArrowDown" ? 1 : -1) + resultats.length) % resultats.length;
    liste.querySelectorAll("li").forEach((li, i) => li.setAttribute("aria-selected", i === actif));
    liste.querySelector(`li[aria-selected="true"]`)?.scrollIntoView({ block: "nearest" });
    champ.setAttribute("aria-activedescendant", "s" + actif);
  } else if (ev.key === "Enter") { ev.preventDefault(); choisir(actif < 0 ? 0 : actif); }
  else if (ev.key === "Escape") { liste.innerHTML = ""; }
});
liste.addEventListener("mousedown", ev => { const li = ev.target.closest("li"); if (li) choisir(+li.dataset.i); });

// Dans un iframe seulement : pied du panneau avec le lien « Ouvrir en plein écran » (adresse courante, avec le lien direct)
const pleinEcran = document.getElementById("pleinecran");
let dansIframe = false;
try { dansIframe = window.self !== window.top; } catch (e) { dansIframe = true; }
if (dansIframe) {
  pleinEcran.hidden = false; pleinEcran.closest("footer").hidden = false;
  pleinEcran.addEventListener("click", () => { pleinEcran.href = location.href; });
}

// Panneau repliable (partout) ; sur téléphone, replié tout seul après une recherche ou un choix de quartier
const bouton = document.getElementById("replier");
function replier(oui) {
  const replie = document.getElementById("panneau").classList.toggle("replie", oui);
  bouton.textContent = replie ? "▾" : "▴"; bouton.setAttribute("aria-expanded", !replie);
}
bouton.addEventListener("click", () => replier(!document.getElementById("panneau").classList.contains("replie")));
const surTelephone = () => window.innerWidth <= 600;
document.getElementById("quartier").addEventListener("change", () => { if (surTelephone()) replier(true); });
liste.addEventListener("mousedown", () => { if (surTelephone()) replier(true); });
champ.addEventListener("keydown", ev => { if (ev.key === "Enter" && surTelephone()) replier(true); });

// Coordonnées (WGS 84) du dernier point cliqué, ou de l'ancrage de la fiche ouverte, à copier en un clic :
// « lat, lon » (6 décimales), WKT du point (lon lat) ou WKT du lieu sélectionné (géométrie complète, 5 décimales) ;
// le format choisi est retenu par le navigateur
{
  const FORMATS = { latlon: "Lat, lon", point: "WKT point", lieu: "WKT lieu" };
  const ICO_COPIER = '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="12" height="12" rx="2"/><path d="M5 15H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v1"/></svg>';
  const ICO_OK = '<svg viewBox="0 0 24 24" width="15" height="15" aria-hidden="true" fill="none" stroke="currentColor" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12l5 5 9-10"/></svg>';
  const wkt = g => {
    const pt = c => c[0] + " " + c[1], ligne = l => "(" + l.map(pt).join(", ") + ")", poly = p => "(" + p.map(ligne).join(", ") + ")";
    return { Point: () => "POINT (" + pt(g.coordinates) + ")", LineString: () => "LINESTRING " + ligne(g.coordinates),
      MultiLineString: () => "MULTILINESTRING (" + g.coordinates.map(ligne).join(", ") + ")",
      Polygon: () => "POLYGON " + poly(g.coordinates), MultiPolygon: () => "MULTIPOLYGON (" + g.coordinates.map(poly).join(", ") + ")" }[g.type]();
  };
  let point = null, format = "latlon";
  try { if (FORMATS[localStorage.getItem("carte-bnr-coord")]) format = localStorage.getItem("carte-bnr-coord"); } catch (e) {}

  const boite = L.DomUtil.create("div", "coord leaflet-control");
  boite.innerHTML = '<select aria-label="Format des coordonnées">' +
    Object.entries(FORMATS).map(([k, t]) => `<option value="${k}">${t}</option>`).join("") + "</select>" +
    '<output aria-live="polite"></output><button type="button" title="Copier" aria-label="Copier les coordonnées">' + ICO_COPIER + "</button>";
  L.DomEvent.disableClickPropagation(boite); L.DomEvent.disableScrollPropagation(boite);
  const choix = boite.querySelector("select"), sortie = boite.querySelector("output"), copier = boite.querySelector("button");
  choix.value = format;

  const texte = () => {
    if (format === "lieu") return lieuChoisi ? wkt(lieuChoisi.geom) : "";
    if (!point) return "";
    const lat = point.lat.toFixed(6), lon = point.lng.toFixed(6);
    return format === "latlon" ? lat + ", " + lon : "POINT (" + lon + " " + lat + ")";
  };
  const maj = () => {
    const t = texte();
    const attente = format === "lieu" ? "Ouvrez la fiche d'un lieu" : "Cliquez sur la carte";
    sortie.textContent = t || attente; sortie.classList.toggle("vide", !t);
    sortie.title = format === "lieu" && lieuChoisi ? lieuChoisi.nom : "";
    copier.disabled = !t;
  };
  choix.addEventListener("change", () => {
    format = choix.value; maj();
    try { localStorage.setItem("carte-bnr-coord", format); } catch (e) {}
  });
  copier.addEventListener("click", async () => {
    const t = texte(); if (!t) return;
    try { await navigator.clipboard.writeText(t); }
    catch (e) {   // presse-papiers refusé (iframe sans permission, http) : copie à l'ancienne
      const zone = Object.assign(document.createElement("textarea"), { value: t });
      zone.style.cssText = "position:fixed;opacity:0"; document.body.append(zone); zone.select();
      document.execCommand("copy"); zone.remove();
    }
    copier.classList.add("ok"); copier.innerHTML = ICO_OK; copier.title = "Copié";
    setTimeout(() => { copier.classList.remove("ok"); copier.innerHTML = ICO_COPIER; copier.title = "Copier"; }, 1200);
  });

  const Coord = L.Control.extend({ options: { position: "bottomleft" }, onAdd: () => boite });
  new Coord().addTo(carte);
  carte.on("click", e => { point = e.latlng; maj(); });
  // la carte reçoit popupopen avant le lieu (qui renseigne lieuChoisi) : mise à jour au tour suivant
  carte.on("popupopen", e => { point = e.popup.getLatLng(); setTimeout(maj); });
  carte.on("popupclose", () => { lieuChoisi = null; maj(); });
  maj();
}

// Lien direct : #voie=<id>, #quartier=<id> ou #site=<id>
function suivreLien() {
  const m = location.hash.match(/^#(voie|quartier|site)=(.+)$/); if (!m) return;
  const cible = m[1] === "site" ? parSite[m[2]] : m[1] === "voie" ? parVoie[m[2]] : null;
  if (cible && !dansIr(cible)) choisirIr("", false);
  if (m[1] === "site" && parSite[m[2]]) { const s = parSite[m[2]]; carte.setView(s.couche.getLatLng(), 17, { animate: false }); s.couche.openPopup(); }
  if (m[1] === "voie" && parVoie[m[2]]) ouvrirVoie(parVoie[m[2]]);
  if (m[1] === "quartier") { const o = D.quartiers.find(q => q.id === m[2]); if (o) choisirQuartier(o.nom); }
}
suivreLien();
window.addEventListener("hashchange", suivreLien);
</script>
</body>
</html>
"""

if __name__ == "__main__":
    main()
