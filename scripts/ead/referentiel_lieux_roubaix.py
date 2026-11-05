"""
Construit le référentiel des lieux de Roubaix à partir des seules sources de
référence, sans regarder les instruments de recherche : une ligne par lieu,
futur concept d'un des thésaurus géographiques de la BnR. L'alignement des
<geogname> des IR sur ce référentiel est une étape ultérieure.

Sources (data/geo/) :
  - ban_roubaix.csv : Base Adresse Nationale, lignes de Roubaix (59512) du
    fichier adresses-59.csv.gz (adresse.data.gouv.fr) ; une voie par
    identifiant BAN, avec son nom certifié par la commune et ses adresses ;
  - bdtopo_roubaix_extrait.csv : objets nommés de la BD TOPO IGN (voies
    nommées, zones d'activité ou d'intérêt, zones d'habitation, équipements de
    transport, constructions, cours d'eau, commune) qui touchent Roubaix, en WKT
    Lambert 93 ; produit par l'option --extraire <GeoPackage BD TOPO du Nord> ;
  - topo_roubaix.csv : fichier TOPO de la DGFiP (ex-FANTOIR), sans géométrie ;
  - filaire_voies_roubaix.geojson : filaire des voies de la ville (MEL) ;
  - quartiers_historiques_mel.geojson : quartiers historiques (MEL) ;
  - osm_roubaix_noms.json : noms OpenStreetMap (old_name, alt_name).

Thésaurus (colonne thesaurus) :
  - quartiers : les quartiers historiques de la MEL ; les quartiers de même nom
    de la BD TOPO leur sont rattachés ;
  - rues : les voies de la BAN, rapprochées par identifiant des voies nommées
    de la BD TOPO (tracé) et de TOPO, par le nom du filaire MEL et d'OSM ; puis
    les voies de la BD TOPO, du filaire ou de TOPO absentes de la BAN. Les
    courées, cités, résidences, pavillons… de TOPO ne sont retenus que s'ils
    sont attestés par la BAN ou la BD TOPO (décision du 2026-10-27) ; les
    places, squares et parcs de la BD TOPO qui portent le nom d'une voie lui
    sont rattachés ;
  - points : les autres lieux nommés de la BD TOPO (espaces publics sans voie
    de même nom, équipements, stations, ponts, écluses, cours d'eau, zones
    d'habitation qui ne sont pas des quartiers de la MEL).

Géométrie : tracé BD TOPO, sinon filaire MEL, sinon centre des adresses BAN
(rues) ; contour MEL (quartiers) ; points : un point pris dans l'emprise BD
TOPO (point_on_surface), l'emprise elle-même allant dans emprise_wkt.
coord_wkt la donne en WKT, WGS84, ordre lon lat, arrondie à 6 décimales ;
coord_manuel (WKT, même convention), si remplie, la remplace.

La colonne a_traiter liste ce qui reste à régler sur le lieu (libellé en double
dans le thésaurus, noms différents selon les sources, abréviation, lieu attesté
par une seule source secondaire, pas de géométrie, zone d'habitation à
classer…) ; elle se vide dès que validation est remplie. Les colonnes
libelle_manuel, coord_manuel, validation et remarque, remplies à la main, sont
conservées d'une exécution à l'autre (clé : lieu_id). Un lieu validé « ne pas
conserver » sort du référentiel et est mis de côté, avec ses colonnes, dans
referentiel_lieux_roubaix_ecartes.csv ; changer sa validation dans ce fichier
le fait revenir à l'exécution suivante.

Sorties, toujours produites ensemble : data/geo/referentiel_lieux_roubaix.csv
et data/geo/referentiel_lieux_roubaix_ecartes.csv (les colonnes manuelles se
saisissent là), et data/geo/referentiel_lieux_roubaix.gpkg (WGS84, mêmes
colonnes, une couche par thésaurus : quartiers, rues, points, plus
ecartes_rues et ecartes_points ; un lieu sans géométrie y a une géométrie
vide). Le GeoPackage est réécrit à chaque exécution : ne pas y saisir.

À lancer depuis la racine du dépôt :
    python scripts/ead/referentiel_lieux_roubaix.py --extraire <gpkg BD TOPO D059>   # une fois par édition
    python scripts/ead/referentiel_lieux_roubaix.py
"""
import argparse
import csv
import json
import re
import sqlite3
import struct
import sys
import unicodedata
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from os import remove, replace
from os.path import basename, dirname, exists, join

from pyproj import CRS, Transformer
from shapely import force_2d, get_dimensions, make_valid, wkb, wkt
from shapely.geometry import MultiLineString, MultiPoint, MultiPolygon, shape
from shapely.ops import transform, unary_union

csv.field_size_limit(sys.maxsize)

GEO = join("data", "geo")
BAN_CSV = join(GEO, "ban_roubaix.csv")
TOPO_CSV = join(GEO, "topo_roubaix.csv")
EXTRAIT_BDTOPO = join(GEO, "bdtopo_roubaix_extrait.csv")
FILAIRE_MEL = join(GEO, "filaire_voies_roubaix.geojson")
QUARTIERS_MEL = join(GEO, "quartiers_historiques_mel.geojson")
OSM_JSON = join(GEO, "osm_roubaix_noms.json")
SORTIE = join(GEO, "referentiel_lieux_roubaix.csv")
SORTIE_GPKG = join(GEO, "referentiel_lieux_roubaix.gpkg")
SORTIE_ECARTES = join(GEO, "referentiel_lieux_roubaix_ecartes.csv")
# Couche historique, tenue à la main : anciens noms des lieux, et voies disparues (dates,
# voie qui les a remplacées, tracé historique).
NOMS_ANCIENS = join(GEO, "noms_anciens.csv")
LIEUX_DISPARUS = join(GEO, "lieux_disparus.csv")
# Valeur de validation qui retire un lieu du référentiel (mis de côté dans SORTIE_ECARTES).
NE_PAS_CONSERVER = "ne pas conserver"

INSEE = "59512"
VERS_WGS84 = Transformer.from_crs(2154, 4326, always_xy=True).transform
WGS84_WKT = CRS.from_epsg(4326).to_wkt("WKT1_GDAL")
VERS_L93 = Transformer.from_crs(4326, 2154, always_xy=True).transform

# Tables de la BD TOPO extraites : (table, colonne du nom, filtre SQL). Les tables
# sans code INSEE sont filtrées par intersection avec la commune.
TABLES_BDTOPO = {
    "voie_nommee": ("nom_voie_ban", f"insee_commune = '{INSEE}'"),
    "zone_d_activite_ou_d_interet": ("toponyme", f"insee_commune = '{INSEE}' and toponyme is not null"),
    "zone_d_habitation": ("toponyme", f"insee_commune = '{INSEE}' and toponyme is not null"),
    "equipement_de_transport": ("toponyme", f"insee_commune = '{INSEE}' and toponyme is not null and nature != 'Parking'"),
    "construction_lineaire": ("toponyme", "toponyme is not null"),
    "construction_surfacique": ("toponyme", "toponyme is not null"),
    "construction_ponctuelle": ("toponyme", "toponyme is not null"),
    "cours_d_eau": ("toponyme", "toponyme is not null"),
}
COLONNES_EXTRAIT = [
    "table", "cleabs", "nom", "nom_collaboratif", "identifiant_voie_ban", "type_voie", "nature",
    "nature_detaillee", "date_creation", "wkt",
]

# Natures de voie TOPO → type développé, pour la clé de comparaison des noms.
NATURES_TOPO = {
    "RUE": "rue", "AV": "avenue", "BD": "boulevard", "PL": "place", "IMP": "impasse", "CHE": "chemin",
    "SQ": "square", "ALL": "allee", "QUAI": "quai", "CRS": "cours", "CR": "cour", "CITE": "cite",
    "RES": "residence", "PAS": "passage", "RPT": "rond point", "PRV": "parvis", "MAIL": "mail",
    "PARC": "parc", "SEN": "sentier", "ESP": "esplanade", "PROM": "promenade", "CAR": "carrefour",
    "PONT": "pont", "VC": "",
}
# Abréviation de type en tête d'un nom normalisé BD TOPO ('R DE MENIN'), pour la clé.
ABREVIATIONS = {
    "r": "rue", "av": "avenue", "bd": "boulevard", "pl": "place", "imp": "impasse", "che": "chemin", "sq": "square",
    "all": "allee", "qu": "quai", "pas": "passage", "rpt": "rond point", "sen": "sentier", "crs": "cours",
    "cr": "cour", "prv": "parvis", "esp": "esplanade", "prom": "promenade",
}

# Types de voie abrégés (TOPO, noms en majuscules de la BD TOPO), pour mettre en
# forme un libellé qui n'existe qu'en majuscules.
TYPES_ABREGES = {
    "R": "Rue", "RUE": "Rue", "QU": "Quai", "AV": "Avenue", "AVE": "Avenue", "BD": "Boulevard", "BRD": "Boulevard",
    "PL": "Place", "IMP": "Impasse", "CHE": "Chemin", "SQ": "Square", "ALL": "Allée", "QUAI": "Quai",
    "RPT": "Rond-Point", "PRV": "Parvis", "PAS": "Passage", "PROM": "Promenade", "SEN": "Sentier", "MAIL": "Mail",
    "PARC": "Parc", "ESP": "Esplanade", "PONT": "Pont", "GPL": "Grand'Place", "CTR": "Contour", "PTTE": "Placette",
    "ENC": "Enclos", "HAM": "Hameau", "IMM": "Immeuble", "PAV": "Pavillon", "JARD": "Jardin", "VOI": "Voie",
    "VOIE": "Voie", "CITE": "Cité", "COUR": "Cour", "RES": "Résidence",
}
# Ensembles d'habitation de TOPO (courées, cités, résidences, pavillons, groupes,
# bâtiments, forts, rangées) : par leur nature, ou par leur libellé, qui commence
# par leur type, porte un numéro, ou cite la voie où ils se trouvent (« DELBAR
# RUE MARCEAU », « RUE DE TUNIS COUR SEYS »).
NATURES_ENSEMBLES = {"CRS", "VC", "CITE", "RES", "TOUR", "LOT", "CR", "COUR", "COTE", "CTRE"}
ENSEMBLE_LIBELLE = re.compile(
    r"^(PAV|GR|FORT|RANGEE|CRS|CR|C|LDT|RES|MAISONS)\b|\d|\bBAT\b"
    r"|.\b(R|RUE|RD|BD|BRD|BOULEVARD|AVE|AV|QUAI|GRANDE RUE|COUR|CR|IMP|IMPASSE|SQUARE|CLOS|HAUTS? CHAMPS?)\b"
)
# Date dans un nom de voie (« SQ DU 8 MAI 1945 ») : ses chiffres ne sont pas un numéro
# d'ensemble d'habitation.
DATE_LIBELLE = re.compile(
    r"\b\d{1,2} (JANVIER|FEVRIER|MARS|AVRIL|MAI|JUIN|JUILLET|AOUT|SEPTEMBRE|OCTOBRE|NOVEMBRE|DECEMBRE) \d{4}\b"
)
# Types de voie qu'une source peut donner pour un autre (« Avenue » / « Rue du Onze
# Novembre », « Cour » / « Impasse Poissonnier ») ; un pont, une place… restent distincts.
TYPES_INTERCHANGEABLES = [
    {"rue", "avenue", "boulevard", "chemin", "allee"},
    {"cour", "impasse", "cite", "lotissement", "rangee", "residence"},
    {"voie", "contour"},
]
COMMUNE_VOISINE = re.compile(r"\((Tourcoing|Croix|Wattrelos|Hem|Lys-lez-Lannoy|Mouvaux|Wasquehal|Lannoy|Leers)\)")
ABREVIATION = re.compile(r"(^|\s)([A-Z]|[A-Z]\.[A-Z]|St|Ste|Bd|Av|Gal|Mal|Pdt|Dr|Ph|Grd)\.?\s")
RESIDENCE = re.compile(r"^(Résidence|Pavillon|Tour|Immeuble|Cour|Clos|Hameau|Enclos|Foyer)\b", re.I)
# Quartiers MEL sans zone BD TOPO qui donne leur graphie : libellé fixé
# (décision du 2026-10-27, d'après les voies du même nom dans la BAN).
LIBELLES_QUARTIERS = {
    "CHEMIN NEUF": "Chemin Neuf", "CROUY": "Crouy", "EDOUARD VAILLANT": "Édouard Vaillant", "LINNE": "Linné",
    "STE ELISABETH": "Sainte-Élisabeth",
}
# Zones BD TOPO de nature « Quartier » dont le nom diffère de celui du quartier MEL
# (clé normalisée → clé du quartier MEL) : « Sainte-Élizabeth » (sic) → STE ELISABETH.
ALIAS_QUARTIERS = {"sainte elizabeth": "sainte elisabeth"}
# Doublons fusionnés (décision du 2026-10-28) : lieu absorbé → lieu gardé (de
# préférence celui qui a un identifiant officiel BAN/TOPO). Les places et squares
# restent des rues (lignes) : d'un espace public BD TOPO absorbé, on ne garde que
# l'identifiant et le nom (en variante), jamais l'emprise.
FUSIONS = {
    "mel:place martyrs resistance": "59512_3610",
    "mel:square savonnerie victor vaissier": "59512_7564",
    "59512_2886": "59512_vr9qln",  # Square Jean Pennel Rés. les Muséal → Square Jean Pennel
    "59512_4992": "59512_4983",  # Parvis St François Rés. la Bruyère → Parvis Saint François
    "bdtopo:SURFACTI0000000056485543": "59512_3203",  # Square Louis Suzanne et Andrée Herbeaux
    "mel:place 3 ponts": "59512_5430",
    "bdtopo:SURFACTI0000000056485487": "59512_5430",  # Place des Trois Ponts
    "bdtopo:SURFACTI0000000259289709": "mel:place sainte elisabeth",  # Place Saint-Élisabeth
    # Propositions « sûr » du 2026-10-28 (propositions_rues_a_traiter_20261028.csv).
    "mel:impasse a dumas": "59512_0073",  # Impasse A. Dumas → Impasse Alexandre Dumas
    "59512_2005": "59512_wkq87f",  # Rue Eugene Motte Prolong → Rue Eugène Motte Prolongée
    "bdtopo:VOIE_NOM0000002339446801": "bdtopo:VOIE_NOM0000002339446776",  # Rue Flechier → Rue Fléchier
    "mel:rue j b vercoutere": "59512_2820",  # Rue J-B. Vercoutère → Rue Jean Baptiste Vercoutère
    "mel:rue m p curie": "59512_3575",  # Rue M&P. Curie → Rue Marie et Pierre Curie
    "mel:rue ph comines": "59512_4420",  # Rue Ph. de Comines → Rue Philippe de Comines
    "mel:rue 7 ponts": "59512_5210",  # Rue des 7 Ponts → Rue des sept Ponts
    "59512_7563": "59512_0136",  # Rue du Peignage Amedee Prouvos → Rue du peignage Amédée Prouvost
    "bdtopo:VOIE_NOM0000002499984170": "59512_ku33lp",  # Contour du Fer à Cheval (tronçon BD TOPO de 53 m)
    # Décisions saisies dans QGIS le 2026-10-28 (validation_rues_20261028.gpkg).
    "bdtopo:VOIE_NOM0000002339437429": "59512_21g0u1",  # Cité Saint-Louis Rue Mackellerie → Cité Saint-Louis
    "bdtopo:VOIE_NOM0000002339446762": "59512_nvlu1q",  # Impasse Petit Bd Montesquieu → Impasse Petit
    "mel:rangee duretete": "59512_3ihwlf",  # Rangée Duretête → Cité Duretête Éloi
    "bdtopo:VOIE_NOM0000002339446764": "59512_4470",  # Rue Pierre Brosselette → Rue Pierre Brossolette
    "mel:rue vedrines": "bdtopo:VOIE_NOM0000002339446802",  # Rue Védrines (MEL) → Passage Védrines
    "59512_1274": "59512_7ugo48",  # Rue Clos Gustave Eiffel (code TOPO sans voie) → Clos Gustave Eiffel
    # Cas de fond tranchés dans QGIS le 2026-10-28 (validation_fond_20261028.gpkg).
    "bdtopo:VOIE_NOM0000002489055025": "59512_4082",  # Mail Notre-Dame (Rue du Pays) → Mail Notre-Dame
    "bdtopo:VOIE_NOM0000002489055019": "59512_0111",  # Place de l'Alma → Place de l'Alma Gare
    "mel:cour demarcq": "59512_gbach0",  # Cour Demarcq → Cour Vroman
    "mel:cour dilles": "59512_vmt56k",  # Cour Dilles → Cité Dillies Piat
    # Cas de fond TOPO seul : recommandations appliquées (accord utilisateur du 2026-10-28).
    "59512_3501": "59512_3670",  # Allée Maertens → Allée Maurice Maertens
    "59512_4385": "bdtopo:VOIE_NOM0000002339446742",  # Avenue du Peuple → Avenue du Peuple Belge
    "59512_3805": "59512_mp1zzn",  # Impasse Miribel → Impasse Miribel Jacob
    "59512_0075": "59512_a6pfk9",  # Rue Alexandre Fleming → Rue Alexander Fleming
    "59512_0130": "59512_u2qo37",  # Rue d'Alsace → Avenue d'Alsace
    "59512_0251": "59512_0250",  # Rue d'Anzin Prolongée → Rue d'Anzin
    "59512_3510": "59512_03i9k3",  # Rue de Malplaquet → Passage Malplaquet
    "59512_1941": "59512_omkpe1",  # Rue Hameau d'Épinal → Hameau d'Épinal
    "59512_3502": "59512_qxwigz",  # Rue Mail de Lannoy → Mail de Lannoy
    "59512_3640": "59512_3xw3jb",  # Rue Massillon → Impasse Massillon
    "59512_4900": "59512_i04h54",  # Rue Rouget de l'Isle → Rue Rouget de Lisle
    "59512_5315": "59512_5310",  # Rue Thècle Prolongée → Rue Thècle
    "59512_5682": "59512_5680",  # Rue Wagram Prolongée → Rue Wagram
    # Contrôle des libellés (2026-10-28) : même courée, deux voies BAN à 11 m ; TOPO ne connaît que « Duchatelet ».
    "59512_p1ci61": "59512_o5s38d",  # Cour du Châtelet → Cour Duchatelet
    # Squares traités comme des rues (décision du 2026-10-29) : l'espace public BD TOPO
    # passe dans la voie TOPO (identifiant_voie_ban de la zone : 59512_2686).
    "bdtopo:SURFACTI0000002333918786": "59512_2686",  # Square Armistice 8 Mai 1945 → Square du 8 Mai 1945
}
# Espaces publics de la BD TOPO sans voie dans aucune source, traités comme des rues
# (les squares sont des rues, décision du 2026-10-29) : cleabs. Square Pierre Catteau
# en est sorti le 2026-10-30 (voir ESPACES_EN_POINTS).
ESPACES_EN_RUES = set()
# Squares sans tracé de voie traités comme des points (décision du 2026-10-30) : le
# lieu gardé par FUSIONS (voie TOPO, dont il garde l'identifiant) passe dans le
# thésaurus des points avec l'emprise de l'espace public BD TOPO qu'il absorbe.
# Square Pierre Catteau, connu de la seule BD TOPO, y est déjà en tant qu'espace public.
ESPACES_EN_POINTS = {
    "59512_3203",  # Square Louis Suzanne et Andrée Herbeaux
    "59512_2686",  # Square de l'Armistice du 8 Mai 1945
}
# Catégories fixées à la main (décisions du 2026-10-28), là où ni la BD TOPO ni le
# premier mot du libellé ne donnent la bonne.
CATEGORIES_FORCEES = {
    "59512_2370": "place",  # Grand Place
    "59512_2380": "rue",  # Grande Rue
    "59512_hfk2od": "rue",  # La Closeraie
    "59512_2162": "rue",  # Le Frome
    "bdtopo:VOIE_NOM0000002339446752": "cour",  # Deuxième Cour Jean Ferret
    "bdtopo:VOIE_NOM0000002339446776": "rue",  # Rue Fléchier (« PRue » dans la BD TOPO)
    "mel:contre allee quai boulogne": "allée",
    "mel:rond point limite": "rond-point",
    "bdtopo:EQ_RESEA0000000056480057": "Gare",  # Gare de Roubaix
}
PARTICULES = {"de", "du", "des", "la", "le", "les", "et", "aux", "au", "sur", "en", "a"}
ARTICLE_INITIAL = re.compile(r"^(l'|le |la |les )", re.I)

COLONNES = [
    "lieu_id", "thesaurus", "categorie", "libelle", "a_traiter", "variantes", "nom_ancien", "date_debut", "date_fin",
    "remplacee_par", "quartiers", "sources",
    "nom_ban", "nom_bdtopo", "nom_mel", "nom_topo", "id_ban", "id_topo", "cleabs_bdtopo", "id_mel", "nb_adresses",
    "date_creation_topo", "source_geometrie", "geometrie_type", "coord_wkt", "emprise_wkt",
    "libelle_manuel", "coord_manuel", "validation", "remarque",
]
MANUELLES = ["libelle_manuel", "coord_manuel", "validation", "remarque"]


def geometrie_gpkg(blob):
    """Géométrie 2D d'un blob GeoPackage (en-tête GP + WKB ; l'altitude de la BD TOPO est ignorée)."""
    enveloppe = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(blob[3] >> 1) & 0b111]
    return force_2d(wkb.loads(bytes(blob[8 + enveloppe:])))


def extraire(gpkg):
    """Écrit bdtopo_roubaix_extrait.csv depuis le GeoPackage BD TOPO du département."""
    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    fid, blob = con.execute(f"select fid, geometrie from commune where code_insee = '{INSEE}'").fetchone()
    commune = geometrie_gpkg(blob)
    minx, miny, maxx, maxy = commune.bounds
    lignes = [{"table": "commune", "cleabs": con.execute(f"select cleabs from commune where fid = {fid}").fetchone()[0],
               "nom": "Roubaix", "wkt": commune.wkt}]
    for table, (nom, filtre) in TABLES_BDTOPO.items():
        colonnes = {r[1] for r in con.execute(f"pragma table_info({table})")}
        champs = ", ".join(
            f"t.{c}" if c in colonnes else "null"
            for c in ("cleabs", nom, "nom_collaboratif", "identifiant_voie_ban", "type_voie", "nature",
                      "nature_detaillee", "date_creation")
        )
        requete = (f"select {champs}, t.geometrie from {table} t join rtree_{table}_geometrie r on r.id = t.fid "
                   f"where r.maxx >= ? and r.minx <= ? and r.maxy >= ? and r.miny <= ? and {filtre}")
        n = 0
        for valeurs in con.execute(requete, (minx, maxx, miny, maxy)):
            g = geometrie_gpkg(valeurs[-1])
            if not g.intersects(commune):
                continue
            lignes.append(dict(zip(COLONNES_EXTRAIT, (table, *valeurs[:-1], wkt.dumps(g, rounding_precision=2)))))
            n += 1
        print(f"  {table} : {n}")
    with open(EXTRAIT_BDTOPO, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES_EXTRAIT, lineterminator="\n")
        writer.writeheader()
        writer.writerows(lignes)
    print(f"extrait BD TOPO ({basename(gpkg)}) : {len(lignes)} objets → {EXTRAIT_BDTOPO}")


def cle(nom):
    """Clé de comparaison : sans accents, casse, ponctuation ni articles."""
    nom = unicodedata.normalize("NFKD", nom)
    nom = "".join(c for c in nom if not unicodedata.combining(c)).lower()
    nom = re.sub(r"[^a-z0-9]+", " ", nom)
    nom = re.sub(r"\bst\b", "saint", nom)
    nom = re.sub(r"\bste\b", "sainte", nom)
    nom = re.sub(r"\b(de|du|des|la|le|les|l|d)\b", " ", nom)
    return re.sub(r"\s+", " ", nom).strip()


def developper(nom):
    """Développe l'abréviation de type d'un nom normalisé BD TOPO ('R DE MENIN')."""
    mots = nom.split()
    if mots and mots[0].lower() in ABREVIATIONS and nom.isupper():
        mots[0] = ABREVIATIONS[mots[0].lower()]
    return " ".join(mots)


def mettre_en_forme(nom):
    """'QUAI D ANVERS' -> 'Quai d'Anvers', 'R BLIDA' -> 'Rue Blida' (nom en majuscules,
    sans accents ni apostrophes, de TOPO ou de la BD TOPO)."""
    mots = nom.replace("'", "' ").split()
    if mots and mots[0] in TYPES_ABREGES:
        if TYPES_ABREGES[mots[0]] == "Grand'Place":
            return "Grand'Place"
        mots[0] = TYPES_ABREGES[mots[0]]
    resultat = []
    for i, mot in enumerate(mots):
        bas = mot.lower()
        if i == 0:
            resultat.append(mot if mot in TYPES_ABREGES.values() else bas.capitalize())
        elif bas in ("d", "l", "d'", "l'") and i < len(mots) - 1:
            resultat.append(bas.rstrip("'") + "'")
        elif bas in PARTICULES:
            resultat.append(bas)
        else:
            resultat.append("-".join(m.capitalize() for m in bas.split("-")))
    return re.sub(r"' ", "'", " ".join(resultat))


def cle_voie(nom):
    return cle(developper(nom))


def nouveau_lieu(lieu_id, thesaurus):
    return {
        "lieu_id": lieu_id, "thesaurus": thesaurus, "categorie": "", "noms": {}, "ids": defaultdict(set),
        "geoms": defaultdict(list), "nb_adresses": 0, "date_creation_topo": "", "nom_ancien": set(),
        "variantes": set(), "motifs": [], "sources": set(), "date_debut": "", "date_fin": "", "remplacee_par": "",
    }


def charger_ban(lieux):
    with open(BAN_CSV, encoding="utf-8") as f:
        adresses = list(csv.DictReader(f, delimiter=";"))
    fantoir = {}
    for a in adresses:
        lieu_id = "_".join(a["id"].split("_")[:2])
        l = lieux.get(lieu_id) or lieux.setdefault(lieu_id, nouveau_lieu(lieu_id, "rues"))
        l["noms"]["ban"] = a["nom_voie"]
        l["ids"]["ban"].add(lieu_id)
        l["sources"].add("BAN")
        l["nb_adresses"] += 1
        l["geoms"]["ban"].append((float(a["x"]), float(a["y"])))
        if a["id_fantoir"]:
            l["ids"]["topo"].add(a["id_fantoir"])
            fantoir[a["id_fantoir"]] = lieu_id
    return fantoir


def plus_proche(candidats, g, lieux, distance_max):
    """Parmi des lieux homonymes, celui dont la géométrie est la plus proche de g (sinon None)."""
    if g is None:
        return None
    distances = sorted((geometrie_l93(lieux[c])[0].distance(g), c) for c in candidats if geometrie_l93(lieux[c])[0])
    return distances[0][1] if distances and distances[0][0] <= distance_max else None


def geometrie_l93(l):
    """(géométrie Lambert 93, source) d'un lieu : tracé ou emprise, sinon centre des adresses BAN."""
    for source, libelle in (("historique", "tracé historique"), ("bdtopo", "BD TOPO"), ("mel", "MEL"),
                            ("zone", "BD TOPO (emprise)")):
        if l["geoms"][source]:
            g = make_valid(unary_union(l["geoms"][source]))
            if g.geom_type == "GeometryCollection":
                # make_valid peut laisser des morceaux de ligne ou des points
                # (contour auto-sécant) : on ne garde que la dimension principale.
                dimension = max(get_dimensions(p) for p in g.geoms)
                g = unary_union([p for p in g.geoms if get_dimensions(p) == dimension])
            return g, libelle
    if l["geoms"]["ban"]:
        return MultiPoint(l["geoms"]["ban"]).centroid, "BAN (centre des adresses)"
    return None, ""


def index_noms(lieux, thesaurus):
    index = defaultdict(list)
    for l in lieux.values():
        if l["thesaurus"] != thesaurus:
            continue
        for nom in l["noms"].values():
            for k in {cle_voie(nom), cle_voie(mettre_en_forme(nom)) if nom.isupper() else ""} - {""}:
                if l["lieu_id"] not in index[k]:
                    index[k].append(l["lieu_id"])
    return index


def rattacher_par_nom(nom, g, lieux, index, distance_max=150):
    """Lieu unique (ou le plus proche des homonymes) dont un nom a la même clé que nom."""
    candidats = index.get(cle_voie(nom), [])
    if len(candidats) == 1:
        return candidats[0]
    return plus_proche(candidats, g, lieux, distance_max)


def noms_proches(a, b):
    """Deux noms de voie qui s'écrivent autrement mais désignent sans doute la même :
    même type et nom semblable (coquille, abréviation, prénom en plus), ou nom
    quasi identique avec un autre type (« Avenue » / « Rue du Onze Novembre »)."""
    type_a, _, reste_a = cle_voie(a).partition(" ")
    type_b, _, reste_b = cle_voie(b).partition(" ")
    mots_a, mots_b = set(reste_a.split()), set(reste_b.split())
    ressemblance = SequenceMatcher(None, reste_a, reste_b).ratio()
    if type_a == type_b:
        return ressemblance >= 0.75 or bool(mots_a and mots_b and (mots_a <= mots_b or mots_b <= mots_a))
    return ressemblance >= 0.9 and any({type_a, type_b} <= groupe for groupe in TYPES_INTERCHANGEABLES)


def rattacher_par_trace(nom, g, lieux, tampons, recouvrement_min=0.6, distance_max=50):
    """Voie à laquelle rattacher un tracé dont le nom s'écrit autrement (noms_proches) :
    celle qui en recouvre la plus grande part (tampon de 15 m autour de son tracé
    BD TOPO), sinon la plus proche, à distance_max près."""
    candidats = [l["lieu_id"] for l in lieux.values()
                 if l["thesaurus"] == "rues" and any(noms_proches(nom, n) for n in l["noms"].values())]
    if g.length:
        parts = sorted(((g.intersection(tampons[c]).length / g.length, c) for c in candidats
                        if c in tampons and tampons[c].intersects(g)), reverse=True)
        if parts and parts[0][0] >= recouvrement_min:
            return parts[0][1]
    return plus_proche(candidats, g, lieux, distance_max)


def charger_voies_bdtopo(lieux, fantoir, objets):
    voies = [o for o in objets if o["table"] == "voie_nommee"]
    orphelines = []
    for v in voies:
        ident = v["identifiant_voie_ban"]
        lieu_id = ident if ident in lieux else fantoir.get(ident)
        if lieu_id is None:
            orphelines.append(v)
            continue
        ajouter_voie_bdtopo(lieux[lieu_id], v)
    index = index_noms(lieux, "rues")
    for v in orphelines:
        nom = v["nom"] or v["nom_collaboratif"]
        lieu_id = rattacher_par_nom(nom, v["geom"], lieux, index) if nom else None
        if lieu_id is None:
            lieu_id = v["identifiant_voie_ban"] or f"bdtopo:{v['cleabs']}"
            l = lieux.get(lieu_id) or lieux.setdefault(lieu_id, nouveau_lieu(lieu_id, "rues"))
            if v["identifiant_voie_ban"].startswith(f"{INSEE}_"):
                l["ids"]["topo" if re.fullmatch(r"\d{5}_[0-9A-Z]\d{3}", lieu_id) else "ban"].add(lieu_id)
            for k in {cle_voie(n) for n in (v["nom"], v["nom_collaboratif"]) if n}:
                index[k].append(lieu_id)
        ajouter_voie_bdtopo(lieux[lieu_id], v)


def ajouter_voie_bdtopo(l, v):
    if (v["nom"] or v["nom_collaboratif"]) and "bdtopo" not in l["noms"]:
        l["noms"]["bdtopo"] = v["nom"] or v["nom_collaboratif"]
    if v["nom"] and v["nom_collaboratif"] and v["nom_collaboratif"] != v["nom"]:
        l["variantes"].add(v["nom_collaboratif"])
    if v["type_voie"] and not l["categorie"]:
        l["categorie"] = v["type_voie"]
    l["ids"]["bdtopo"].add(v["cleabs"])
    l["geoms"]["bdtopo"].append(v["geom"])
    l["sources"].add("BD TOPO")


def charger_topo(lieux, fantoir):
    """Voies et lieux-dits de TOPO : rattachés par identifiant à une voie de la BAN ou
    de la BD TOPO, sinon par le nom à une voie ou à un point ; sinon voie connue de
    TOPO seul, sauf les ensembles d'habitation (non attestés) et les voies annulées."""
    with open(TOPO_CSV, encoding="utf-8-sig") as f:
        topo = [t for t in csv.DictReader(f, delimiter=";") if t["code_voie"]]
    index_rues, index_points = index_noms(lieux, "rues"), index_noms(lieux, "points")
    ecartes = Counter()
    for t in topo:
        ident = f"{INSEE}_{t['code_voie']}"
        ensemble = t["nature_de_voie"] in NATURES_ENSEMBLES or ENSEMBLE_LIBELLE.search(DATE_LIBELLE.sub("", t["libelle"]))
        nom = f"{t['nature_de_voie']} {t['libelle']}".strip()
        lieu_id = ident if ident in lieux else fantoir.get(ident)
        if lieu_id is None and not ensemble:
            nature = NATURES_TOPO.get(t["nature_de_voie"], t["nature_de_voie"].lower())
            for nom_long in (f"{nature} {t['libelle']}", mettre_en_forme(nom)):
                lieu_id = rattacher_par_nom(nom_long, None, lieux, index_rues)
                if lieu_id is None and len(index_points.get(cle_voie(nom_long), [])) == 1:
                    lieu_id = index_points[cle_voie(nom_long)][0]
                if lieu_id:
                    break
        if lieu_id is None:
            if ensemble:
                ecartes["ensemble d'habitation non attesté"] += 1
                continue
            if t["caractere_voie"] == "1" or t["annulation"]:
                ecartes["annulée"] += 1
                continue
            lieu_id = ident
            lieux[lieu_id] = nouveau_lieu(lieu_id, "rues")
        l = lieux[lieu_id]
        l["noms"].setdefault("topo", nom)
        l["ids"]["topo"].add(ident)
        l["sources"].add("TOPO")
        if t["date_creation_de_article"] not in ("", "19870101"):
            l["date_creation_topo"] = t["date_creation_de_article"]
    return ecartes


def charger_mel(lieux):
    with open(FILAIRE_MEL, encoding="utf-8") as f:
        par_nom = defaultdict(list)
        for ft in json.load(f)["features"]:
            par_nom[ft["properties"]["nom_de_la_rue"]].append((ft["id"], transform(VERS_L93, shape(ft["geometry"]))))
    index = index_noms(lieux, "rues")
    tampons = {l["lieu_id"]: unary_union(l["geoms"]["bdtopo"]).buffer(15)
               for l in lieux.values() if l["thesaurus"] == "rues" and l["geoms"]["bdtopo"]}
    for nom, troncons in par_nom.items():
        g = unary_union([t[1] for t in troncons])
        lieu_id = rattacher_par_nom(nom, g, lieux, index) or rattacher_par_trace(nom, g, lieux, tampons)
        if lieu_id is None:
            lieu_id = f"mel:{cle_voie(nom)}"
            lieux.setdefault(lieu_id, nouveau_lieu(lieu_id, "rues"))
        l = lieux[lieu_id]
        l["noms"].setdefault("mel", nom)
        l["ids"]["mel"].update(t[0] for t in troncons)
        l["geoms"]["mel"].extend(t[1] for t in troncons)
        l["sources"].add("MEL")


def charger_osm(lieux):
    with open(OSM_JSON, encoding="utf-8") as f:
        elements = json.load(f)["elements"]
    index = index_noms(lieux, "rues")
    for e in elements:
        tags = e["tags"]
        candidats = index.get(cle_voie(tags["name"]), [])
        if len(candidats) != 1:
            continue
        l = lieux[candidats[0]]
        for ancien in tags.get("old_name", "").split(";"):
            if ancien.strip():
                l["nom_ancien"].add(ancien.strip())
                l["sources"].add("OSM")
        for autre in tags.get("alt_name", "").split(";"):
            if autre.strip():
                l["variantes"].add(autre.strip())
                l["sources"].add("OSM")


def charger_quartiers(lieux):
    with open(QUARTIERS_MEL, encoding="utf-8") as f:
        features = json.load(f)["features"]
    for ft in features:
        p = ft["properties"]
        lieu_id = f"mel_quartier:{p['id']}"
        l = lieux[lieu_id] = nouveau_lieu(lieu_id, "quartiers")
        l["categorie"] = "quartier"
        l["noms"]["mel"] = p["Nom_du_quartier"]
        l["ids"]["mel"].add(str(p["id"]))
        l["geoms"]["mel"].append(make_valid(transform(VERS_L93, shape(ft["geometry"]))))
        l["sources"].add("MEL")


def retirer_hors_roubaix(lieux, commune):
    """Retire les voies inconnues de la BAN et de TOPO de Roubaix dont le tracé est
    surtout hors de la commune (voies des communes voisines dans la BD TOPO ou le
    filaire MEL) ; renvoie leurs libellés."""
    retirees = []
    for lieu_id, l in list(lieux.items()):
        if l["thesaurus"] != "rues" or l["sources"] & {"BAN", "TOPO"}:
            continue
        g = geometrie_l93(l)[0]
        if any(COMMUNE_VOISINE.search(n) for n in l["noms"].values()) or (
            g is not None and g.length and g.intersection(commune).length / g.length < 0.5
        ):
            retirees.append(next(iter(l["noms"].values())))
            del lieux[lieu_id]
    return retirees


def sans_article(nom):
    return ARTICLE_INITIAL.sub("", nom).strip()


def charger_zones(lieux, objets):
    """Zones, équipements, constructions et cours d'eau de la BD TOPO : rattachés à un
    quartier ou à une voie de même nom, sinon lieux du thésaurus points."""
    quartiers = defaultdict(list)
    for l in lieux.values():
        if l["thesaurus"] == "quartiers":
            quartiers[cle(sans_article(l["noms"]["mel"]))].append(l["lieu_id"])
    index_rues = index_noms(lieux, "rues")
    points = defaultdict(list)
    for o in objets:
        if o["table"] in ("commune", "voie_nommee") or o["nom"] == "Roubaix":
            continue
        nom, g = o["nom"], o["geom"]
        k_quartier = ALIAS_QUARTIERS.get(cle(sans_article(nom)), cle(sans_article(nom)))
        if o["table"] == "zone_d_habitation" and len(quartiers.get(k_quartier, [])) == 1:
            l = lieux[quartiers[k_quartier][0]]
            l["noms"].setdefault("bdtopo", nom)
            l["ids"]["bdtopo"].add(o["cleabs"])
            l["sources"].add("BD TOPO")
            continue
        if o["cleabs"] in ESPACES_EN_RUES:
            # Square connu de la seule BD TOPO : une rue, sans géométrie (l'emprise
            # n'est jamais reprise pour une rue).
            l = lieux.setdefault(f"bdtopo:{o['cleabs']}", nouveau_lieu(f"bdtopo:{o['cleabs']}", "rues"))
            l["categorie"] = (o["nature_detaillee"] or "square").lower()
            l["noms"]["bdtopo"] = nom
            l["ids"]["bdtopo"].add(o["cleabs"])
            l["sources"].add("BD TOPO")
            continue
        if o["table"] in ("zone_d_activite_ou_d_interet", "zone_d_habitation"):
            lieu_id = rattacher_par_nom(nom, g, lieux, index_rues, distance_max=50)
            if lieu_id and plus_proche([lieu_id], g, lieux, 50):
                l = lieux[lieu_id]
                l["ids"]["bdtopo"].add(o["cleabs"])
                if nom != l["noms"].get("ban") and nom != l["noms"].get("bdtopo"):
                    l["variantes"].add(nom)
                continue
        categorie = o["nature_detaillee"] or o["nature"] or o["table"].replace("_", " ")
        if o["table"] == "zone_d_habitation":
            # Hors quartiers MEL, les zones d'habitation de la BD TOPO sont des
            # ensembles de logements (les Aloès, la Grive, Georges Feydeau…) :
            # décision du 2026-10-27.
            categorie = "résidence" if RESIDENCE.match(nom) else "ensemble d'habitation"
        if o["table"] == "cours_d_eau":
            g = g.intersection(o["commune"])
        # Objets de même nom à moins de 100 m (bâtiments d'un même établissement,
        # tronçons d'un même canal) : un seul lieu.
        proches = [l for l in points[cle(nom)] if unary_union(l["geoms"]["zone"]).distance(g) <= 100]
        if proches:
            l = proches[0]
        else:
            l = nouveau_lieu(f"bdtopo:{o['cleabs']}", "points")
            points[cle(nom)].append(l)
            l["categorie"] = categorie
            l["noms"]["bdtopo"] = nom
            l["sources"].add("BD TOPO")
        l["ids"]["bdtopo"].add(o["cleabs"])
        l["geoms"]["zone"].append(g)
    for homonymes in points.values():
        for l in homonymes:
            lieux[l["lieu_id"]] = l


def fusionner(lieux):
    """Applique FUSIONS : identifiants, sources et noms du lieu absorbé passent au lieu gardé.
    Un lieu de rue apporte aussi son tracé ; un espace public (thésaurus points) seulement
    son nom, en variante, sauf si le lieu gardé est dans ESPACES_EN_POINTS : il devient
    alors un point et reçoit l'emprise."""
    for absorbe_id, garde_id in FUSIONS.items():
        if absorbe_id not in lieux or garde_id not in lieux:
            print(f"  fusion impossible, lieu absent des sources : {absorbe_id} → {garde_id}")
            continue
        a, l = lieux.pop(absorbe_id), lieux[garde_id]
        for source, ids in a["ids"].items():
            l["ids"][source] |= ids
        l["sources"] |= a["sources"]
        l["variantes"] |= a["variantes"]
        l["nom_ancien"] |= a["nom_ancien"]
        l["nb_adresses"] += a["nb_adresses"]
        l["date_creation_topo"] = l["date_creation_topo"] or a["date_creation_topo"]
        if a["thesaurus"] == "points":
            l["variantes"] |= set(a["noms"].values())
            if garde_id in ESPACES_EN_POINTS:
                l["thesaurus"] = "points"
                l["geoms"]["zone"].extend(a["geoms"]["zone"])
            continue
        for source, nom in a["noms"].items():
            if l["noms"].setdefault(source, nom) != nom:
                l["variantes"].add(mettre_en_forme(nom) if nom.isupper() else nom)
        for source, geoms in a["geoms"].items():
            l["geoms"][source].extend(geoms)


def charger_historique(lieux):
    """Couche historique : voies disparues de LIEUX_DISPARUS (créées si aucune source ne les
    connaît ; dates, voie de remplacement et tracé historique, qui prime sur les autres
    géométries), puis anciens noms de NOMS_ANCIENS, ajoutés à nom_ancien."""
    if exists(LIEUX_DISPARUS):
        with open(LIEUX_DISPARUS, encoding="utf-8") as f:
            for d in csv.DictReader(f):
                l = lieux.get(d["lieu_id"])
                if l is None:
                    l = lieux[d["lieu_id"]] = nouveau_lieu(d["lieu_id"], "rues")
                    l["noms"]["historique"] = d["libelle"]
                    l["categorie"] = d["categorie"]
                l["sources"].add("historique")
                l["date_debut"], l["date_fin"], l["remplacee_par"] = d["date_debut"], d["date_fin"], d["remplacee_par"]
                if d["remplacee_par"] and d["remplacee_par"] not in lieux:
                    print(f"  lieu de remplacement inconnu : {d['lieu_id']} → {d['remplacee_par']}")
                if d["trace_wkt"]:
                    l["geoms"]["historique"] = [transform(VERS_L93, wkt.loads(d["trace_wkt"]))]
    if exists(NOMS_ANCIENS):
        with open(NOMS_ANCIENS, encoding="utf-8") as f:
            for n in csv.DictReader(f):
                if n["lieu_id"] not in lieux:
                    print(f"  ancien nom d'un lieu inconnu : {n['nom_ancien']} → {n['lieu_id']}")
                    continue
                lieux[n["lieu_id"]]["nom_ancien"].add(n["nom_ancien"])


def choisir_libelle(l):
    """(libellé, reconstitué ?) : nom de la BAN, sinon de la BD TOPO, du filaire MEL ou
    de TOPO ; un nom qui n'existe qu'en majuscules est remis en forme. Quartiers :
    graphie de la BD TOPO (accents, casse) sans article initial, sinon nom MEL."""
    noms = l["noms"]
    if l["thesaurus"] == "quartiers" and noms["mel"] in LIBELLES_QUARTIERS:
        return LIBELLES_QUARTIERS[noms["mel"]], False
    if l["thesaurus"] == "quartiers" and "bdtopo" in noms:
        libelle = sans_article(noms["bdtopo"])
        return libelle[0].upper() + libelle[1:], False
    for source in ("ban", "bdtopo", "mel", "topo", "historique"):
        if source in noms:
            nom = noms[source]
            return (mettre_en_forme(nom), True) if nom.isupper() else (nom, False)


def rattacher_quartiers(g, quartiers):
    """Quartiers MEL qui contiennent au moins 10 % du lieu (longueur, surface), sinon le principal."""
    if g is None:
        return []
    parts = []
    for nom, q in quartiers:
        if not q.intersects(g):
            continue
        inter = q.intersection(g)
        mesure = inter.area / g.area if g.area else inter.length / g.length if g.length else 1
        parts.append((mesure, nom))
    parts.sort(reverse=True)
    retenus = [nom for mesure, nom in parts if mesure >= 0.1]
    return retenus or [nom for _, nom in parts[:1]]


def finaliser(lieux):
    quartiers = []
    for l in lieux.values():
        l["libelle"], l["reconstitue"] = choisir_libelle(l)
        if l["thesaurus"] == "quartiers":
            quartiers.append((l["libelle"], geometrie_l93(l)[0]))
    lignes = []
    for l in lieux.values():
        noms = l["noms"]
        g, source_geometrie = geometrie_l93(l)
        motifs = list(l["motifs"])
        if g is None:
            motifs.append("pas de géométrie")
        if l["reconstitue"]:
            motifs.append("libellé reconstitué d'un nom en majuscules (accents à vérifier)")
        if l["thesaurus"] == "rues":
            if l["sources"] <= {"TOPO", "OSM"}:
                motifs.append("connue de TOPO seul (voie disparue ou non ouverte ?)")
            elif l["sources"] <= {"MEL", "OSM"}:
                motifs.append("connue du filaire MEL seul")
            elif l["sources"] <= {"BD TOPO", "OSM"}:
                motifs.append("connue de la BD TOPO seule")
            cles = {cle_voie(noms[s]) for s in ("ban", "bdtopo", "mel") if s in noms}
            if len(cles) > 1:
                motifs.append("noms différents selon les sources")
            if ABREVIATION.search(l["libelle"] + " "):
                motifs.append("abréviation dans le libellé")
        if l["lieu_id"] in CATEGORIES_FORCEES:
            l["categorie"] = CATEGORIES_FORCEES[l["lieu_id"]]
        if not l["categorie"]:
            l["categorie"] = l["libelle"].split()[0].lower() if l["libelle"] else ""
        variantes ={n for s, n in noms.items() if s != "topo"} | l["variantes"]
        variantes -= {l["libelle"]}
        coord = transform(VERS_WGS84, g) if g is not None else None
        # Thésaurus des points : un point dans l'emprise (point_on_surface, toujours
        # sur l'objet, contrairement au centroïde) ; l'emprise est gardée à part.
        emprise = None
        if l["thesaurus"] == "points" and coord is not None:
            emprise, coord = coord, transform(VERS_WGS84, g.point_on_surface())
        lignes.append({
            "lieu_id": l["lieu_id"], "thesaurus": l["thesaurus"], "categorie": l["categorie"],
            "libelle": l["libelle"], "motifs": motifs,
            "variantes": " | ".join(sorted(variantes)), "nom_ancien": " | ".join(sorted(l["nom_ancien"])),
            "date_debut": l["date_debut"], "date_fin": l["date_fin"], "remplacee_par": l["remplacee_par"],
            "quartiers": " | ".join(rattacher_quartiers(g, quartiers)) if l["thesaurus"] != "quartiers" else "",
            "sources": ", ".join(s for s in ("BAN", "BD TOPO", "TOPO", "MEL", "OSM", "historique") if s in l["sources"]),
            "nom_ban": noms.get("ban", ""), "nom_bdtopo": noms.get("bdtopo", ""), "nom_mel": noms.get("mel", ""),
            "nom_topo": noms.get("topo", ""),
            "id_ban": " | ".join(sorted(l["ids"]["ban"])), "id_topo": " | ".join(sorted(l["ids"]["topo"])),
            "cleabs_bdtopo": " | ".join(sorted(l["ids"]["bdtopo"])),
            "id_mel": " | ".join(sorted(l["ids"]["mel"], key=lambda x: (len(x), x))),
            "nb_adresses": l["nb_adresses"] or "", "date_creation_topo": l["date_creation_topo"],
            "source_geometrie": source_geometrie, "geometrie_type": coord.geom_type.upper() if coord else "",
            "coord_wkt": wkt.dumps(coord, rounding_precision=6, trim=True) if coord else "",
            "emprise_wkt": wkt.dumps(emprise, rounding_precision=6, trim=True) if emprise else "",
        })
    return lignes


def ecrire(lignes):
    """Écrit le référentiel et, à part, les lieux validés « ne pas conserver » ; renvoie (conservés, écartés).
    Les colonnes manuelles sont relues dans les deux fichiers : un lieu écarté dont on change
    la validation revient dans le référentiel."""
    manuelles = {}
    for fichier in (SORTIE_ECARTES, SORTIE):
        verrou = join(dirname(fichier), f".~lock.{basename(fichier)}#")
        if exists(verrou):
            sys.exit(f"{fichier} est ouvert (verrou {verrou}) : le fermer avant de relancer.")
        if exists(fichier):
            with open(fichier, encoding="utf-8") as f:
                manuelles.update({r["lieu_id"]: r for r in csv.DictReader(f)})
    for r in lignes:
        r["libelle_source"] = r["libelle"]
        for c in MANUELLES:
            r[c] = manuelles.get(r["lieu_id"], {}).get(c, "")
        if r["libelle_manuel"]:
            r["libelle"] = r["libelle_manuel"]
    # Le libellé manuel d'un quartier vaut aussi dans la colonne quartiers des autres lieux.
    quartiers = {r["libelle_source"]: r["libelle"] for r in lignes if r["thesaurus"] == "quartiers"}
    for r in lignes:
        r.pop("libelle_source")
        r["quartiers"] = " | ".join(quartiers.get(q, q) for q in r["quartiers"].split(" | ")) if r["quartiers"] else ""
    ecartes =[r for r in lignes if r["validation"].strip().lower() == NE_PAS_CONSERVER]
    lignes = [r for r in lignes if r["validation"].strip().lower() != NE_PAS_CONSERVER]
    # Mnesys relie un <geogname> à un concept par le libellé : il doit être unique
    # dans chaque thésaurus (lieux conservés, libellés manuels compris).
    doubles = Counter((r["thesaurus"], r["libelle"]) for r in lignes)
    for r in lignes:
        n = doubles[(r["thesaurus"], r["libelle"])]
        if n > 1:
            r["motifs"].append(f"libellé en double dans le thésaurus ({n})")
    ordre = {"quartiers": 0, "rues": 1, "points": 2}
    for r in lignes + ecartes:
        motifs = r.pop("motifs")
        r["a_traiter"] = "" if r["validation"] else " ; ".join(motifs)
    for liste in (lignes, ecartes):
        liste.sort(key=lambda r: (ordre[r["thesaurus"]], r["libelle"].lower(), r["lieu_id"]))
    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(lignes)
    with open(SORTIE_ECARTES, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(ecartes)
    ecrire_gpkg(lignes, ecartes)
    return lignes, ecartes


# Type de géométrie de chaque couche du GeoPackage (toujours multiple, pour accueillir
# les tracés en plusieurs morceaux) et classe shapely correspondante.
TYPES_GPKG = {"quartiers": ("MULTIPOLYGON", MultiPolygon), "rues": ("MULTILINESTRING", MultiLineString),
              "points": ("MULTIPOINT", MultiPoint)}


def blob_gpkg(geometrie):
    """Blob GeoPackage (en-tête GP, enveloppe xy, WKB petit-boutiste) d'une géométrie WGS84."""
    minx, miny, maxx, maxy = geometrie.bounds
    return (b"GP" + struct.pack("<BBi4d", 0, 0b011, 4326, minx, maxx, miny, maxy)
            + wkb.dumps(geometrie, byte_order=1, output_dimension=2))


def ecrire_gpkg(lignes, ecartes):
    """Écrit le GeoPackage du référentiel (mêmes lignes et colonnes que les CSV), une couche
    par thésaurus ; réécrit en entier dans un fichier temporaire puis mis en place."""
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
    colonnes = ", ".join(f'"{c}" {"INTEGER" if c == "nb_adresses" else "TEXT"}' for c in COLONNES)
    for prefixe, liste in (("", lignes), ("ecartes_", ecartes)):
        for thesaurus, (type_gpkg, multiple) in TYPES_GPKG.items():
            du_thesaurus = [r for r in liste if r["thesaurus"] == thesaurus]
            if not du_thesaurus:
                continue
            table = prefixe + thesaurus
            con.execute(f'CREATE TABLE "{table}" (fid INTEGER PRIMARY KEY AUTOINCREMENT, geom {type_gpkg}, {colonnes})')
            emprises = []
            for r in du_thesaurus:
                geometrie = r["coord_manuel"] or r["coord_wkt"]
                blob = None
                if geometrie:
                    g = wkt.loads(geometrie)
                    g = g if g.geom_type.startswith("Multi") else multiple([g])
                    blob = blob_gpkg(g)
                    emprises.append(g.bounds)
                valeurs = [int(r[c]) if c == "nb_adresses" and r[c] else (r[c] or None) for c in COLONNES]
                con.execute(f'INSERT INTO "{table}" VALUES (NULL, ?, {", ".join("?" * len(COLONNES))})',
                            [blob] + valeurs)
            minx, miny = min(e[0] for e in emprises), min(e[1] for e in emprises)
            maxx, maxy = max(e[2] for e in emprises), max(e[3] for e in emprises)
            con.execute("INSERT INTO gpkg_contents (table_name, data_type, identifier, min_x, min_y, max_x, max_y, "
                        "srs_id) VALUES (?, 'features', ?, ?, ?, ?, ?, 4326)", (table, table, minx, miny, maxx, maxy))
            con.execute("INSERT INTO gpkg_geometry_columns VALUES (?, 'geom', ?, 4326, 0, 0)", (table, type_gpkg))
    con.commit()
    con.close()
    replace(temporaire, SORTIE_GPKG)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--extraire", metavar="GPKG", help="GeoPackage BD TOPO du Nord à extraire d'abord")
    args = parser.parse_args()
    if args.extraire:
        extraire(args.extraire)

    with open(EXTRAIT_BDTOPO, encoding="utf-8") as f:
        objets = list(csv.DictReader(f))
    commune = next(wkt.loads(o["wkt"]) for o in objets if o["table"] == "commune")
    for o in objets:
        o["geom"] = wkt.loads(o["wkt"])
        o["commune"] = commune

    lieux = {}
    fantoir = charger_ban(lieux)
    charger_voies_bdtopo(lieux, fantoir, objets)
    charger_mel(lieux)
    charger_osm(lieux)
    charger_quartiers(lieux)
    charger_zones(lieux, objets)
    ecartes = charger_topo(lieux, fantoir)
    hors_roubaix = retirer_hors_roubaix(lieux, commune)
    fusionner(lieux)
    charger_historique(lieux)
    lignes, ne_pas_conserver = ecrire(finaliser(lieux))

    print(f"{len(lignes)} lieux → {SORTIE} (+ {basename(SORTIE_GPKG)})")
    print(f"{len(ne_pas_conserver)} lieux « {NE_PAS_CONSERVER} » mis de côté → {SORTIE_ECARTES} :",
          dict(Counter(r["thesaurus"] for r in ne_pas_conserver)))
    for thesaurus in ("quartiers", "rues", "points"):
        du_thesaurus = [r for r in lignes if r["thesaurus"] == thesaurus]
        print(f"  {thesaurus} : {len(du_thesaurus)}, dont {sum(1 for r in du_thesaurus if r['a_traiter'])} à traiter")
    print("  sources des rues :", dict(Counter(r["sources"] for r in lignes if r["thesaurus"] == "rues").most_common()))
    print("  TOPO écartés :", dict(ecartes))
    print(f"  voies hors Roubaix retirées : {len(hors_roubaix)} ({', '.join(sorted(hors_roubaix))})")
    motifs = Counter(re.sub(r" \(\d+\)$", "", m) for r in lignes for m in r["a_traiter"].split(" ; ") if m)
    for motif, n in motifs.most_common():
        print(f"  {n:5}  {motif}")


if __name__ == "__main__":
    main()
