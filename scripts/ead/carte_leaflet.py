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
d'un quartier (zoom, ses voies mises en avant) ; lien direct #voie=<id> / #quartier=<id>.

    python scripts/ead/geogname_ir2kml.py   # d'abord, si les IR ont changé
    python scripts/ead/carte_leaflet.py
"""
import csv
import json
import re
import sys
import unicodedata
from os import makedirs
from os.path import dirname, join, realpath

sys.path.insert(0, dirname(realpath(__file__)))
from geogname_ir2kml import ROUGE  # noqa: E402
from quartiers2kml import quartiers_des_rues  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
UMAP = join(RACINE, "results/ead/indexation/bnr2mnesysgeo_lieux.umap")
REFERENTIEL = join(RACINE, "data/geo/referentiel_lieux_roubaix.csv")
SORTIE = join(RACINE, "results/ead/indexation/carte_bnr/index.html")
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


def main():
    quartiers_f, voies_f, sites_f = lire_umap_publique()
    ref = {}
    with open(REFERENTIEL, encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            ref[(r["thesaurus"], r["libelle_manuel"].strip() or r["libelle"])] = r
    q_rues = quartiers_des_rues()

    voies = []
    for f in voies_f:
        nom = f["properties"]["name"]
        r = ref.get(("rues", nom), {})
        n, lien = fiche_umap(f)
        voies.append({"id": ident(nom), "nom": nom,
                      "anciens": [a for a in r.get("nom_ancien", "").split(" | ") if a],
                      "quartiers": q_rues.get(r.get("lieu_id"), []),
                      "n": n, "lien": lien, "geom": arrondir(f["geometry"])})

    quartiers = []
    for f in quartiers_f:
        nom = f["properties"]["name"]
        n, lien = fiche_umap(f)
        quartiers.append({"id": ident(nom), "nom": nom, "couleur": f["properties"]["_umap_options"]["color"],
                          "n": n, "lien": lien, "geom": arrondir(f["geometry"]),
                          "voies": sum(1 for v in voies if nom in v["quartiers"])})

    sites = []
    for f in sites_f:
        nom = f["properties"]["name"]
        r = ref.get(("points", nom), {})
        n, lien = fiche_umap(f)
        sites.append({"id": ident(nom), "nom": nom, "categorie": r.get("categorie", ""),
                      "quartiers": [r["quartiers"]] if r.get("quartiers") else [],
                      "n": n, "lien": lien, "geom": arrondir(f["geometry"])})

    donnees = json.dumps({"quartiers": quartiers, "voies": voies, "sites": sites, "rouge": ROUGE},
                         ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
    makedirs(dirname(SORTIE), exist_ok=True)
    with open(SORTIE, "w", encoding="utf-8", newline="\n") as f:
        f.write(PAGE.replace("__DONNEES__", donnees))

    print(f"→ {SORTIE}")
    print(f"  {len(quartiers)} quartiers, {len(voies)} voies, {len(sites)} sites")
    print(f"  voies avec ancien(s) nom(s) : {sum(1 for v in voies if v['anciens'])} ; "
          f"sans quartier : {sum(1 for v in voies if not v['quartiers'])}")


PAGE = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
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
  background: var(--fond); border-radius: 10px; box-shadow: 0 4px 18px rgba(0,0,0,.2); overflow: hidden; }
.bandeau { display: flex; height: 6px; }
.bandeau span { flex: 1; }
.panneau header { position: relative; padding: 12px 44px 10px 16px; }
.panneau h1 { font-size: 17px; margin: 0; line-height: 1.25; }
.panneau header p { margin: 4px 0 0; color: var(--doux); font-size: 13px; }
.chiffres { display: flex; gap: 6px; flex-wrap: wrap; margin-top: 8px; }
.chiffres span { font-size: 12px; background: var(--survol); border-radius: 999px; padding: 2px 9px; color: var(--doux); }
.chiffres b { color: var(--encre); }
#replier { position: absolute; top: 10px; right: 10px; width: 30px; height: 30px; border: 0; border-radius: 6px;
  background: none; color: var(--doux); font-size: 16px; cursor: pointer; }
#replier:hover, #replier:focus-visible { background: var(--survol); outline: none; }
.panneau.replie .corps, .panneau.replie header p, .panneau.replie .chiffres, .panneau.replie footer { display: none; }
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
.panneau footer { display: flex; justify-content: space-between; align-items: center; gap: 8px; padding: 8px 16px;
  border-top: 1px solid var(--trait); background: #faf9f6; font-size: 12px; color: var(--doux); }
.panneau footer a { color: #1f7a7e; font-weight: 600; text-decoration: none; white-space: nowrap; }
.panneau footer a:hover { text-decoration: underline; }
.recherche { position: relative; }
#suggestions { list-style: none; margin: 2px 0 0; padding: 0; position: absolute; left: 0; right: 0; z-index: 2;
  background: #fff; border: 1px solid var(--trait); border-radius: 6px; max-height: 260px; overflow: auto; box-shadow: 0 4px 12px rgba(0,0,0,.12); }
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
.fiche button.lien { font: inherit; background: none; border: 0; padding: 0; color: #1f7a7e; text-decoration: underline; cursor: pointer; }
@media (max-width: 600px) {
  .panneau { top: 8px; left: 8px; right: 8px; width: auto; }
  .panneau h1 { font-size: 16px; }
  .panneau header p { font-size: 12px; }
  .leaflet-top.leaflet-right { top: auto; bottom: 28px; }
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
    <div class="chiffres" id="chiffres"></div>
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
    <div class="legende" aria-hidden="true">
      <span><i class="quartier"></i>Quartiers</span><span><i></i>Rues</span><span><i class="site"></i>Sites</span>
    </div>
  </div>
  <footer>
    <span>Sources : BN-R · fond IGN</span>
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
const fonds = {
  "Plan IGN": geopf("GEOGRAPHICALGRIDSYSTEMS.PLANIGNV2", "image/png"),
  "Photographies aériennes": geopf("ORTHOIMAGERY.ORTHOPHOTOS", "image/jpeg"),
  "Photographies aériennes 1965-1980": geopf("ORTHOIMAGERY.ORTHOPHOTOS.1965-1980", "image/png", 18, "BDORTHOHISTORIQUE"),
  "Photographies aériennes 1950-1965": geopf("ORTHOIMAGERY.ORTHOPHOTOS.1950-1965", "image/png", 18),
  "Carte de 1950 (1/50 000)": geopf("GEOGRAPHICALGRIDSYSTEMS.MAPS.SCAN50.1950", "image/jpeg", 15),
  "Carte de l'état-major (1820-1866)": geopf("GEOGRAPHICALGRIDSYSTEMS.ETATMAJOR40", "image/jpeg", 15),
  "Carte de Cassini (XVIIIe siècle)": geopf("AN-IGNF_GEOGRAPHICALGRIDSYSTEMS.CASSINI", "image/jpeg", 14),
  "OpenStreetMap": L.tileLayer("https://tile.openstreetmap.org/{z}/{x}/{y}.png",
    { maxZoom: 19, attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>' }),
};
fonds["Plan IGN"].addTo(carte);

function fiche(o, type) {
  const lignes = [`<div class="fiche"><h2>${echap(o.nom)}</h2>`];
  if (type === "site" && o.categorie) lignes.push(`<p class="meta">${echap(o.categorie)}</p>`);
  if (o.anciens && o.anciens.length) lignes.push(`<p><b>Ancien${o.anciens.length > 1 ? "s noms" : " nom"} :</b> ${o.anciens.map(echap).join(", ")}</p>`);
  if (o.quartiers && o.quartiers.length) lignes.push(`<p><b>Quartier${o.quartiers.length > 1 ? "s" : ""} :</b> ` +
    o.quartiers.map(q => `<button class="lien" data-quartier="${echap(q)}">${echap(q)}</button>`).join(", ") + "</p>");
  if (type === "quartier") lignes.push(`<p>${o.voies} rue${o.voies > 1 ? "s" : ""} citée${o.voies > 1 ? "s" : ""} dans ce quartier</p>`);
  lignes.push(`<p class="meta">${mentions(o.n)}</p>`);
  if (o.lien) lignes.push(`<a class="bouton" href="${echap(o.lien)}" target="_blank" rel="noopener">Voir les documents sur la BN-R</a>`);
  else lignes.push(`<p class="meta"><i>Pas encore de document en ligne sur la BN-R.</i></p>`);
  return lignes.join("") + "</div>";
}

// Quartiers
const parQuartier = {};
const coucheQuartiers = L.layerGroup().addTo(carte);
const styleQuartier = (o, etat) => ({ renderer: rendu, color: o.couleur, weight: etat === "choisi" ? 3 : 1.5,
  fillColor: o.couleur, fillOpacity: etat === "choisi" ? 0.55 : etat === "estompe" ? 0.12 : 0.4, opacity: etat === "estompe" ? 0.5 : 1 });
D.quartiers.forEach(o => {
  o.couche = L.geoJSON(o.geom, { style: () => styleQuartier(o) })
    .bindTooltip(o.nom, { sticky: true, direction: "top" })
    .bindPopup(() => fiche(o, "quartier"))
    .on("popupopen", () => { history.replaceState(null, "", "#quartier=" + o.id); })
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
    .on("popupopen", () => { voieOuverte = v; v.couche.setStyle(styleVoie("survol")); v.couche.bringToFront();
                             history.replaceState(null, "", "#voie=" + v.id); })
    .on("popupclose", () => { voieOuverte = null; v.couche.setStyle(styleVoie(etatVoie(v))); })
    .addTo(coucheVoies);
  parVoie[v.id] = v;
});

// Sites
const coucheSites = L.layerGroup().addTo(carte);
D.sites.forEach(s => {
  const [lon, lat] = s.geom.coordinates;
  s.couche = L.circleMarker([lat, lon], { renderer: rendu, radius: 8, color: "#fff", weight: 2, fillColor: D.rouge, fillOpacity: 1 })
    .bindTooltip(s.nom, { direction: "top" }).bindPopup(() => fiche(s, "site")).addTo(coucheSites);
});

// Boîte des fonds et couches : en français, deux intitulés, une petite légende devant chaque couche
const choixCouches = L.control.layers(fonds, {
  '<i class="leg quartier"></i>Quartiers': coucheQuartiers,
  '<i class="leg rue"></i>Rues': coucheVoies,
  '<i class="leg site"></i>Sites': coucheSites,
}, { position: "topright" }).addTo(carte);
{
  const boite = choixCouches.getContainer();
  const bouton = boite.querySelector(".leaflet-control-layers-toggle");
  bouton.title = "Fond de carte et couches"; bouton.setAttribute("aria-label", "Fond de carte et couches");
  const intitule = texte => Object.assign(document.createElement("p"), { className: "intitule", textContent: texte });
  boite.querySelector(".leaflet-control-layers-base").prepend(intitule("Fond de carte"));
  boite.querySelector(".leaflet-control-layers-overlays").prepend(intitule("Couches"));
}

// Marges de cadrage : laisser la place du panneau (à gauche sur grand écran, en haut sur téléphone)
const marges = extra => window.innerWidth > 600
  ? { paddingTopLeft: [360, 30], paddingBottomRight: [40, 30], ...extra }
  : { paddingTopLeft: [20, 90], paddingBottomRight: [20, 20], ...extra };   // panneau replié

// Choix d'un quartier : zoom, ses voies en avant, les autres estompées
function choisirQuartier(nom) {
  quartierChoisi = nom;
  document.getElementById("quartier").value = nom;
  D.quartiers.forEach(o => o.couche.setStyle(styleQuartier(o, !nom ? "" : o.nom === nom ? "choisi" : "estompe")));
  D.voies.forEach(v => v.couche.setStyle(styleVoie(etatVoie(v))));
  document.getElementById("tous").hidden = !nom;
  if (nom) { carte.fitBounds(parQuartier[nom].couche.getBounds(), marges());
             history.replaceState(null, "", "#quartier=" + parQuartier[nom].id); }
  else { carte.fitBounds(ville, marges()); history.replaceState(null, "", location.pathname + location.search); }
}
const ville = L.featureGroup(D.quartiers.map(o => o.couche)).getBounds();
document.getElementById("tous").addEventListener("click", () => { carte.closePopup(); choisirQuartier(""); });
document.getElementById("quartier").addEventListener("change", e => { carte.closePopup(); choisirQuartier(e.target.value); });
carte.on("popupopen", e => e.popup.getElement().querySelectorAll("button[data-quartier]")
  .forEach(b => b.addEventListener("click", () => { carte.closePopup(); choisirQuartier(b.dataset.quartier); })));

function ouvrirVoie(v) {
  carte.fitBounds(v.couche.getBounds(), marges({ maxZoom: 17 }));
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
  resultats = q.length < 2 ? [] : index.filter(e => e.cle.includes(q))
    .sort((a, b) => (b.cle.startsWith(q) - a.cle.startsWith(q)) || a.texte.localeCompare(b.texte, "fr")).slice(0, 12);
  actif = -1;
  liste.innerHTML = resultats.map((e, i) => `<li role="option" id="s${i}" data-i="${i}">${echap(e.texte)}` +
    (e.ancien ? `<small>ancien nom de ${echap(e.v.nom)}</small>` : e.site ? "<small>site</small>" : "") + "</li>").join("");
  champ.setAttribute("aria-expanded", resultats.length > 0);
  document.getElementById("aucun").hidden = !(q.length >= 2 && !resultats.length);
}
function choisir(i) {
  const e = resultats[i]; if (!e) return;
  champ.value = e.v.nom; liste.innerHTML = ""; champ.setAttribute("aria-expanded", false);
  if (e.site) { carte.setView(e.v.couche.getLatLng(), 17); e.v.couche.openPopup(); } else ouvrirVoie(e.v);
}
champ.addEventListener("input", proposer);
champ.addEventListener("keydown", ev => {
  if (!resultats.length) return;
  if (ev.key === "ArrowDown" || ev.key === "ArrowUp") {
    ev.preventDefault();
    actif = (actif + (ev.key === "ArrowDown" ? 1 : -1) + resultats.length) % resultats.length;
    liste.querySelectorAll("li").forEach((li, i) => li.setAttribute("aria-selected", i === actif));
    champ.setAttribute("aria-activedescendant", "s" + actif);
  } else if (ev.key === "Enter") { ev.preventDefault(); choisir(actif < 0 ? 0 : actif); }
  else if (ev.key === "Escape") { liste.innerHTML = ""; }
});
liste.addEventListener("mousedown", ev => { const li = ev.target.closest("li"); if (li) choisir(+li.dataset.i); });

// Chiffres du panneau
const pluriel = (n, mot) => `<span><b>${n}</b> ${mot}${n > 1 ? "s" : ""}</span>`;
document.getElementById("chiffres").innerHTML =
  pluriel(D.voies.length, "rue") + pluriel(D.quartiers.length, "quartier") + pluriel(D.sites.length, "site");

// Dans un iframe : lien « Ouvrir en plein écran » (adresse courante, avec le lien direct)
const pleinEcran = document.getElementById("pleinecran");
let dansIframe = false;
try { dansIframe = window.self !== window.top; } catch (e) { dansIframe = true; }
if (dansIframe) {
  pleinEcran.hidden = false;
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

// Lien direct : #voie=<id> ou #quartier=<id>
function suivreLien() {
  const m = location.hash.match(/^#(voie|quartier)=(.+)$/); if (!m) return;
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
