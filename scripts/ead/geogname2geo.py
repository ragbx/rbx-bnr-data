"""
Extrait les <geogname> géoréférencés des IR et en génère un GeoJSON.

Parcourt les mêmes fichiers results/ead/ead_cor/bnr2mnesys/*.xml que
controlaccess2skos.py. Seuls les geogname portant un attribut coord (issu du
géoréférencement rue/quartier/adresse) sont retenus ; les autres (sans
géoréférencement) sont ignorés.

Un même toponyme (texte + source) revient dans de nombreux IR avec
exactement le même coord (vérifié : aucune incohérence de géocodage dans le
corpus) — les sorties ne contiennent donc qu'une entité par toponyme unique,
avec deux compteurs en propriétés plutôt qu'une entité par occurrence :
documents (nombre de balises <geogname> pour ce toponyme, tous IR confondus)
et instruments_de_recherche (nombre d'IR distincts où il apparaît).

coord est soit un point [lat, lon] (source rue/adresse), soit un polygone
[[lat, lon], ...] (source quartier).

Alignement des rues sur le filaire officiel
--------------------------------------------
Pour la source rue, le point geocodé est remplacé par la géométrie réelle de
la voie (LineString, ou MultiLineString si plusieurs tronçons partagent le
même nom) — data/geo/filaire_voies_roubaix.geojson, export WFS de Lille
Métropole (couche ville_roubaix:filaire_des_voies_de_la_ville_de_roubaix,
récupéré le 2026-09-22 :
https://data.lillemetropole.fr/geoserver/wfs?SERVICE=WFS&REQUEST=GetFeature&VERSION=2.0.0&TYPENAMES=ville_roubaix%3Afilaire_des_voies_de_la_ville_de_roubaix&OUTPUTFORMAT=application%2Fgeo%2Bjson).

Les noms de rue de la BnR sont à l'envers ("Achille Screpel, rue" au lieu de
"Rue Achille Screpel") ; reconstruire() les remet à l'endroit avant
comparaison avec nom_de_la_rue du filaire (normalise() pour la casse/accents).

Sur les 405 toponymes rue : 352 correspondances exactes après reconstruction
et normalisation. Les 53 restants ont été vérifiés à la main (voir
CORRECTIONS_RUE) : 36 corrigent une coquille ou une variante mineure côté
BnR (accent, article manquant, type de voie différent — ex. "Menin" est une
fausse piste automatique vers "Denain", mais "Alsace" est bien "Avenue
d'Alsace" et pas une "Rue d'Alsace" distincte) ; les 17 restants n'ont
aucune correspondance fiable dans le filaire actuel (rue disparue/renommée,
ou toponyme trop ambigu comme "Notre-Dame") et gardent leur point géocodé.

Pour une rue appariée, name porte le nom officiel du filaire (cohérent avec
la géométrie alignée) et non plus le texte brut de l'IR ; ce dernier reste
disponible dans rue_autre_forme, toujours renseignée pour la source rue
(même identique à name en substance, juste réordonnée). Pour une rue non
appariée, name retombe sur le texte BnR reconstruit à l'endroit (pas de nom
officiel disponible) et rue_autre_forme reste le texte brut d'origine.
Inchangé pour quartier/adresse : name = texte brut, rue_autre_forme absente
(null).

Chaque feature du GeoJSON porte aussi une propriété url : lien vers la
recherche « Plan de Roubaix » de bn-r.fr, filtrée sur le texte brut de
l'IR dans l'index correspondant à la source (rue/quartier/adresse —
vérifié manuellement sur le site, ce sont des index distincts qui
correspondent exactement aux trois valeurs de source), et une propriété
geometrie_officielle (bool) indiquant si la géométrie vient du filaire ou
reste un point geocodé.

Sorties :
  - results/ead/indexation/rbx-bnr.geojson, une FeatureCollection, en
    WGS84 (EPSG:4326, la seule référence prévue par le format GeoJSON) ;
  - results/ead/indexation/rbx-bnr_epsg27561.geojson, la même
    FeatureCollection reprojetée en Lambert Nord France (EPSG:27561, la
    projection utilisée pour la vérification dans QGIS), via ogr2ogr — la
    conversion NTF/WGS84 passe par une grille officielle PROJ, pas une
    formule écrite à la main ;
  - results/ead/indexation/rbx-bnr_<source>.csv (rue/quartier/adresse) :
    une extraction de rbx-bnr.geojson, régénérée à chaque exécution — mêmes
    propriétés que le GeoJSON (hors source, constante dans chaque fichier),
    plus coordinates, le contenu brut de geometry.coordinates (JSON), donc
    la géométrie complète, pas un point simplifié.

À lancer depuis la racine du dépôt.
"""
import ast
import csv
import json
import re
import subprocess
import unicodedata
from collections import defaultdict
from glob import glob
from os import remove
from os.path import exists, join
from urllib.parse import urlencode

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
FILAIRE_SOURCE = join("data", "geo", "filaire_voies_roubaix.geojson")
GEOJSON_SORTIE = join("results", "ead", "indexation", "rbx-bnr.geojson")
GEOJSON_27561_SORTIE = join("results", "ead", "indexation", "rbx-bnr_epsg27561.geojson")
RECHERCHE_URL = "https://www.bn-r.fr/resultat.php"

# Corrections vérifiées à la main (cf. docstring) : nom_bnr brut -> nom_de_la_rue
# du filaire. Complète l'appariement automatique (reconstruire + normalise)
# pour les cas qu'il rate ou dont il choisirait une mauvaise cible.
CORRECTIONS_RUE = {
    "Alexandre Fleming, rue": "Rue Alexander Fleming",
    "Alsace, rue d\\'": "Avenue d'Alsace",
    "Barbe d\\'or, rue": "Rue de la Barbe d'Or",
    "Beaurepaire, boulevard": "Boulevard de Beaurepaire",
    "Beaurewaert, rue": "Rue de Beaurewaert",
    "Belfort, allée": "Allée de Belfort",
    "Belfort, boulevard": "Boulevard de Belfort",
    "Blanchemaille, rue": "Rue de Blanchemaille",
    "Blanqui, rue": "Rue Auguste Blanqui",
    "Cambai, boulevard de": "Boulevard de Cambrai",
    "Charles Louis Spriet, place": "Place Charles Spriet",
    "Charpentiers, rue": "Rue des Charpentiers",
    "Choiseul, rue": "Rue de Choiseul",
    "Dammartin, rue": "Rue de Dammartin",
    "Derègnaucourt, rue": "Rue Jules Deregnaucourt",
    "Dunant, rue": "Rue Henri Dunant",
    "Favreuil, rue du": "Rue de Favreuil",
    "Fondeur, rue des": "Rue des Fondeurs",
    "Frasez, rue": "Avenue Frasez",
    "Halluin, boulevard d\\'": "Cour d'Halluin",
    "Henri Carette, rue": "Rue Henri Carrette",
    "Inkermann, rue d\\'": "Rue Inkermann",
    "Jean Baptiste Vercoutère, rue": "Rue J-B. Vercoutère",
    "La Rochefoucault, rue": "Rue Larochefoucauld",
    "Malplaquet, rue de": "Passage Malplaquet",
    "Maxenxe Van Der Meersch, avenue": "Avenue Maxence Van der Meersch",
    "Mulbhouse, boulevard de": "Boulevard de Mulhouse",
    "Pays, rue de": "Rue du Pays",
    "Président  Auriol, rue du": "Rue du Président Vincent Auriol",
    "Rouget De L\\'isle, rue": "Rue Rouget de Lisle",
    "Saint Hubert, rue": "Rue de Saint Hubert",
    "Saint-Roch, rue": "Rue St Roch",
    "Sainte Elisabeth, place": "Place Ste Elisabeth",
    "Salomon De Caux, rue": "Rue Salomon de Caus",
    "Sept ponts, rue des": "Rue des 7 Ponts",
    "Sergent Betremieux, rue": "Rue du Sergent Louis Bettremieux",
}


def reconstruire(nom_brut):
    """Remet à l'endroit un nom de rue BnR : 'Achille Screpel, rue' ->
    'Rue Achille Screpel' ; "Alger, rue d\\'" -> "Rue d'Alger"."""
    nom_brut = nom_brut.replace("\\'", "'")
    if "," not in nom_brut:
        return nom_brut.strip()
    nom, type_part = nom_brut.split(",", 1)
    nom = nom.strip()
    type_part = type_part.strip()
    type_part = type_part[0].upper() + type_part[1:]
    if type_part.endswith("'"):
        return f"{type_part}{nom}"
    return f"{type_part} {nom}"


def normalise(nom):
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c))
    nom = nom.lower()
    nom = re.sub(r"[’']", " ", nom)
    nom = re.sub(r"[^a-z0-9 ]", " ", nom)
    return re.sub(r"\s+", " ", nom).strip()


def charger_filaire():
    """Charge le filaire officiel : nom_de_la_rue -> liste de LineString
    (coordinates), et un index normalise(nom) -> nom_de_la_rue pour
    l'appariement automatique."""
    with open(FILAIRE_SOURCE, encoding="utf-8") as f:
        filaire = json.load(f)
    segments_par_nom = defaultdict(list)
    for feature in filaire["features"]:
        nom = feature["properties"]["nom_de_la_rue"]
        geometrie = feature["geometry"]
        if geometrie["type"] == "MultiLineString":
            segments_par_nom[nom].extend(geometrie["coordinates"])
        else:
            segments_par_nom[nom].append(geometrie["coordinates"])
    index_normalise = defaultdict(list)
    for nom in segments_par_nom:
        index_normalise[normalise(nom)].append(nom)
    return segments_par_nom, index_normalise


def apparier_rue(texte_brut, index_normalise):
    """Nom officiel du filaire correspondant à ce toponyme rue de la BnR,
    ou None si aucune correspondance fiable (cf. docstring du module)."""
    if texte_brut in CORRECTIONS_RUE:
        return CORRECTIONS_RUE[texte_brut]
    candidats = index_normalise.get(normalise(reconstruire(texte_brut)))
    return candidats[0] if candidats else None


def geometrie_rue_officielle(nom_officiel, segments_par_nom):
    segments = segments_par_nom[nom_officiel]
    if len(segments) == 1:
        return {"type": "LineString", "coordinates": segments[0]}
    return {"type": "MultiLineString", "coordinates": segments}


def extraire_geogname():
    """Parcourt les IR et renvoie, par (texte, source), coord + eadid vus.

    occurrences : nombre de balises <geogname> pour cette clé, tous IR
    confondus (plusieurs par IR si le toponyme est indexé sur plusieurs <c>).
    instruments : ensemble des eadid des IR où la clé apparaît au moins une
    fois (un même IR n'y compte qu'une fois, quel que soit son nombre
    d'occurrences internes)."""
    toponymes = {}
    occurrences = defaultdict(int)
    instruments = defaultdict(set)
    ignores = []
    for path in sorted(glob(join(EAD_FOLDER, "*.xml"))):
        root = etree.parse(path).getroot()
        eadid = root.xpath("//eadheader/eadid/text()")
        eadid = eadid[0] if eadid else None

        for geogname in root.xpath("//controlaccess/geogname[@coord]"):
            texte = geogname.text.strip() if geogname.text else None
            source = geogname.get("source")
            coord = geogname.get("coord")
            if not texte or not coord:
                continue
            try:
                ast.literal_eval(coord)
            except (ValueError, SyntaxError):
                ignores.append((path, texte, source))
                continue
            cle = (texte, source)
            toponymes.setdefault(cle, coord)
            occurrences[cle] += 1
            instruments[cle].add(eadid)
    if ignores:
        print(f"{len(ignores)} geogname au coord illisible (ignorés) :")
        for path, texte, source in ignores:
            print(f"  {path} | {texte} | {source}")
    return toponymes, occurrences, instruments


def coord_en_geometrie(coord):
    """Convertit un coord [lat, lon] ou [[lat, lon], ...] en géométrie
    GeoJSON (dict), en refermant l'anneau du polygone si besoin."""
    valeurs = ast.literal_eval(coord)
    if isinstance(valeurs[0], (int, float)):
        lat, lon = valeurs
        return {"type": "Point", "coordinates": [lon, lat]}
    anneau = [[lon, lat] for lat, lon in valeurs]
    if anneau[0] != anneau[-1]:
        anneau.append(anneau[0])
    return {"type": "Polygon", "coordinates": [anneau]}


def construire_geometries(toponymes):
    """Géométrie GeoJSON finale de chaque toponyme (le filaire officiel pour
    les rues appariées, sinon le point/polygone geocodé d'origine), et le
    nom officiel retenu pour les rues appariées."""
    segments_par_nom, index_normalise = charger_filaire()
    geometries = {}
    officielles = {}
    noms_officiels = {}
    appariements = 0
    for (texte, source), coord in toponymes.items():
        nom_officiel = None
        if source == "rue":
            nom_officiel = apparier_rue(texte, index_normalise)
        if nom_officiel:
            geometries[(texte, source)] = geometrie_rue_officielle(nom_officiel, segments_par_nom)
            officielles[(texte, source)] = True
            noms_officiels[(texte, source)] = nom_officiel
            appariements += 1
        else:
            geometries[(texte, source)] = coord_en_geometrie(coord)
            officielles[(texte, source)] = False
            noms_officiels[(texte, source)] = None
    print(f"{appariements}/{sum(1 for _, s in toponymes if s == 'rue')} rues appariées au filaire officiel")
    return geometries, officielles, noms_officiels


def url_recherche(toponyme, source):
    """URL de recherche bn-r.fr (Plan de Roubaix), filtrée sur ce toponyme
    dans l'index de sa source (rue/quartier/adresse)."""
    params = urlencode(
        {
            "type_rech": "pl",
            "index[]": source,
            "value[]": f'"{toponyme}"',
            "decouvrir_par_terme": toponyme,
        }
    )
    return f"{RECHERCHE_URL}?{params}"


def creer_geojson(toponymes, geometries, officielles, noms_officiels, occurrences, instruments):
    features = []
    for cle in sorted(toponymes):
        texte, source = cle
        if source == "rue":
            nom = noms_officiels[cle] or reconstruire(texte)
            rue_autre_forme = texte
        else:
            nom = texte
            rue_autre_forme = None
        properties = {
            "name": nom,
            "source": source,
            "documents": occurrences[cle],
            "instruments_de_recherche": len(instruments[cle]),
            "geometrie_officielle": officielles[cle],
            "rue_autre_forme": rue_autre_forme,
            "url": url_recherche(texte, source),
        }
        features.append({"type": "Feature", "geometry": geometries[cle], "properties": properties})
    return {"type": "FeatureCollection", "features": features}


def creer_csv(geojson):
    """Un CSV par source, dérivé des features du GeoJSON : mêmes propriétés
    (hors source, constante dans chaque fichier), plus coordinates — le
    contenu brut de geometry.coordinates (JSON), donc la géométrie complète
    (point, polygone, ligne ou multi-ligne selon la source), pas un point
    représentatif simplifié."""
    colonnes = ["name", "rue_autre_forme", "documents", "instruments_de_recherche", "geometrie_officielle", "url", "coordinates"]
    lignes_par_source = defaultdict(list)
    for feature in geojson["features"]:
        p = feature["properties"]
        ligne = {**p, "coordinates": json.dumps(feature["geometry"]["coordinates"])}
        lignes_par_source[p["source"]].append(ligne)

    fichiers = []
    for source, lignes in lignes_par_source.items():
        chemin = join("results", "ead", "indexation", f"rbx-bnr_{source}.csv")
        with open(chemin, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=colonnes, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(lignes)
        fichiers.append((chemin, len(lignes)))
    return fichiers


toponymes, occurrences, instruments = extraire_geogname()
geometries, officielles, noms_officiels = construire_geometries(toponymes)

geojson = creer_geojson(toponymes, geometries, officielles, noms_officiels, occurrences, instruments)
with open(GEOJSON_SORTIE, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

if exists(GEOJSON_27561_SORTIE):
    remove(GEOJSON_27561_SORTIE)
subprocess.run(
    [
        "ogr2ogr",
        "-f", "GeoJSON",
        "-s_srs", "EPSG:4326",
        "-t_srs", "EPSG:27561",
        GEOJSON_27561_SORTIE,
        GEOJSON_SORTIE,
    ],
    check=True,
)

csv_fichiers = creer_csv(geojson)

print(f"{len(toponymes)} toponymes uniques → {GEOJSON_SORTIE}, {GEOJSON_27561_SORTIE}")
for source in sorted({s for _, s in toponymes}):
    n = sum(1 for (_, s) in toponymes if s == source)
    print(f"{n:6d}  {source}")
for chemin, n in sorted(csv_fichiers):
    print(f"{n:6d}  {chemin}")
