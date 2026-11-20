"""Tracés (lignes) des rues repérées sur le plan de 1889 (6Fi022).

Entrées :
  results/ead/indexation/plan_1889/rues_1889_propositions.csv   (64 propositions, non modifié)
  results/ead/indexation/plan_1889/traces_1889_releves.json     (sommets L93 relevés à la main
      sur plan_roubaix_1889_georef.tif pour les voies disparues / projetées / sans géométrie)
  data/geo/referentiel_lieux_roubaix.csv                        (lu seulement)
Sorties :
  results/ead/indexation/plan_1889/rues_1889_traces.csv  (+ .gpkg, couche rues_1889_traces)

Règle : voie relevée sur 1889 -> tracé relevé ; sinon voie renommée -> tracé actuel du référentiel.
Voie disparue ou projetée : pas de libellé actuel (elle n'a pas été renommée) ; la voie qui occupe aujourd'hui
son emplacement, s'il y en a une, va dans emprise_actuelle (ex. Rue de Vaucanson -> Avenue des Nations Unies).
"""
import csv, json, subprocess
from pathlib import Path
from pyproj import Transformer

D = Path("results/ead/indexation/plan_1889")
tr = Transformer.from_crs(2154, 4326, always_xy=True).transform
rel = json.load(open(D / "traces_1889_releves.json"))
ref = {r["lieu_id"]: r for r in csv.DictReader(open("data/geo/referentiel_lieux_roubaix.csv"))}

PRECISION = {
    "Rue d'Alexandrie": "approximatif : voie projetée en tirets",
    "Rue d'Aboukir": "approximatif : voie projetée en tirets",
    "Rue Saint-Denis / Parvis Saint-Denis": "approximatif : axe du parvis projeté, entre Rue d'Aboukir et Rue d'Alexandrie",
    "Rue Jean de Melun": "voie projetée (lotissement du Nouveau Roubaix), axe entre les deux traits",
    "Rue Montyon": "extrémités incertaines",
    "Rue Marc Seguin": "extrémité est sur une voie du lotissement projeté",
    "Rue Pellart (tronçon ouest)": "sur le plan, le libellé « Rue … Pellart » court de la Rue du Curé à la Rue Saint-Antoine",
    "Rue de la Planche Trouée": "relevé sur 1889 (relecture du 20/11 : ne correspond pas à la Rue Saint-Exupéry)",
    "Rue Nicolas Leblanc": "relevé sur 1889 (relecture du 20/11 : la Rue Berthollet existe déjà, à côté)",
    "Rue de la Redoute": "relevé sur 1889 (relecture du 20/11 : c'est la Rue Émile Moreau, mais le tracé diffère)",
    "Rue des Filatures": "relevé sur 1889, du boulevard à la Rue Saint-Jean (relecture du 20/11 : pas exactement la Rue Winston Churchill)",
    "Rue du Ballon": "de la Rue Saint-Antoine à la Rue de l'Hommelet par le carrefour de la Rue du Collège (libellé répété deux fois)",
}

def wkt(coords):
    return "LINESTRING (" + ", ".join(f"{x:.7f} {y:.7f}" for x, y in coords) + ")"

def longueur(l93):
    return round(sum(((b[0] - a[0]) ** 2 + (b[1] - a[1]) ** 2) ** 0.5 for a, b in zip(l93, l93[1:])), 1)

rows = []
for r in csv.DictReader(open(D / "rues_1889_propositions.csv")):
    nom = r["nom_1889"]
    o = {k: r[k] for k in ("nom_1889", "constat", "libelle_actuel", "lieu_id_actuel", "certitude", "secteur")}
    o["emprise_actuelle"] = ""
    if not r["constat"].startswith(("renommée", "conflit", "à vérifier")):  # disparue / projetée
        o["emprise_actuelle"], o["libelle_actuel"], o["lieu_id_actuel"] = r["libelle_actuel"], "", ""
    if nom in rel:
        l93 = rel[nom]["l93"]
        o.update(source_trace="relevé sur le plan de 1889", longueur_m=longueur(l93),
                 precision=PRECISION.get(nom, "relevé manuel sur le plan géoréférencé (calage ~5-10 m)"),
                 coord_wkt=wkt([tr(x, y) for x, y in l93]))
    elif o["lieu_id_actuel"] and ref.get(r["lieu_id_actuel"], {}).get("coord_wkt"):
        o.update(source_trace="référentiel (voie actuelle)", longueur_m="",
                 precision="tracé actuel de " + ref[r["lieu_id_actuel"]]["libelle"],
                 coord_wkt=ref[r["lieu_id_actuel"]]["coord_wkt"])
    else:
        o.update(source_trace="", longueur_m="", precision="aucun tracé", coord_wkt="")
    rows.append(o)

out = D / "rues_1889_traces.csv"
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
gpkg = out.with_suffix(".gpkg"); gpkg.unlink(missing_ok=True)
subprocess.run(["ogr2ogr", "-q", "-f", "GPKG", str(gpkg), str(out), "-nln", "rues_1889_traces", "-nlt", "MULTILINESTRING",
                "-oo", "GEOM_POSSIBLE_NAMES=coord_wkt", "-oo", "KEEP_GEOM_COLUMNS=NO", "-a_srs", "EPSG:4326"], check=True)
from collections import Counter
print(len(rows), "lignes ->", out, Counter(o["source_trace"] for o in rows))
