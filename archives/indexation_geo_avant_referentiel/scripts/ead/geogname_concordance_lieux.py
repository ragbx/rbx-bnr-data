"""
Propose la concordance des geogname entre les formes trouvées dans les IR et
les lieux : une ligne par forme BnR (libellé + @source d'origine), rattachée
quand c'est possible à un objet de référence actuel — voie (TOPO / BD TOPO),
espace public, quartier ou commune. Cette concordance sert à nourrir le
référentiel des lieux (data/geo/referentiel_geogname.csv, construit par
geogname_referentiel_lieux.py), d'où
sortiront les thésaurus géographiques.

Formes : état de référence d'avant corrections
(results/ead/indexation/rbx-bnr_geogname_origine.csv), hors geogname vides
et hors @source="chrono".

Sources :
  - data/geo/topo_roubaix.csv, data/geo/concordance_renommages.csv : cf.
    geogname_concordance.py (à lancer avant) ;
  - CORRECTIONS_RUE de geogname2geo.py (correspondances validées à la main) ;
  - data/geo/filaire_voies_roubaix.geojson (MEL), en secours quand une voie
    n'est trouvée ni dans TOPO ni dans la BD TOPO ;
  - extraits de la BD TOPO IGN (édition 2026-06-15), en WGS84, créés par
    --bdtopo <chemin du .gpkg du Nord> puis réutilisés :
      data/geo/bdtopo_roubaix_voies.geojson   (voie_nommee, INSEE 59512)
      data/geo/bdtopo_roubaix_zones.geojson   (zone_d_activite_ou_d_interet
          nature "Espace public" et zone_d_habitation, INSEE 59512)
      data/geo/bdtopo_nord_communes.csv       (commune : INSEE, nom)

Ordre de rattachement : décision manuelle, quartier (pour une forme indexée
comme quartier), concordance (renommage / variante), CORRECTIONS_RUE, voie par
nom normalisé (TOPO + BD TOPO), espace public, commune, quartier. Les
quartiers sont ceux des quartiers historiques de la MEL
(data/geo/quartiers_historiques_mel.geojson, couche WFS
ville_roubaix:quartiers_historiques de data.lillemetropole.fr). Les alertes signalent les cas à trancher
(renommage, voie actuelle plus récente que les documents…).

Sortie : data/geo/concordance_geogname_lieux.csv. Les colonnes validation et
remarque, remplies à la main, sont conservées d'une exécution à l'autre. La
première colonne, a_traiter, liste les motifs d'intervention manuelle de la
ligne ; elle se vide dès que validation est remplie.

À lancer depuis la racine du dépôt :
    python scripts/ead/geogname_concordance_lieux.py [--bdtopo <fichier.gpkg>]
"""
import ast
import csv
import json
import re
import sqlite3
import sys
import unicodedata
from collections import Counter, defaultdict
from os.path import dirname, exists, join

from pyproj import Transformer
from shapely import wkb
from shapely.geometry import mapping, shape
from shapely.validation import make_valid
from shapely.ops import linemerge, transform, unary_union

sys.path.insert(0, dirname(__file__))
from geogname_concordance import (  # noqa: E402
    CONCORDANCE_CSV,
    CREATION_INITIALE_TOPO,
    GEO,
    NATURES_TOPO,
    TOPO_CSV,
    annees_par_composant,
    cle,
    reconstruire,
)

ORIGINE_CSV = join("results", "ead", "indexation", "rbx-bnr_geogname_origine.csv")
GEOGNAME2GEO = join("scripts", "ead", "geogname2geo.py")
VOIES_GEOJSON = join(GEO, "bdtopo_roubaix_voies.geojson")
ZONES_GEOJSON = join(GEO, "bdtopo_roubaix_zones.geojson")
COMMUNES_CSV = join(GEO, "bdtopo_nord_communes.csv")
FILAIRE_MEL = join(GEO, "filaire_voies_roubaix.geojson")
QUARTIERS_MEL = join(GEO, "quartiers_historiques_mel.geojson")
SORTIE = join(GEO, "concordance_geogname_lieux.csv")

TYPE_VOIE = re.compile(
    r"\b(rue|boulevard|avenue|place|quai|impasse|chemin|square|parc|all[ée]e?|cour|passage|route|pont|sentier|"
    r"contour|cit[ée]|carrefour|rond[- ]point|parvis|mail|esplanade|promenade|ruelle|grand[' ]place|grande rue)\b",
    re.I,
)
PAYS = {"belgique", "pologne", "france", "allemagne", "angleterre", "italie", "espagne", "pays bas", "suisse", "algerie", "maroc"}

ESPACE_PUBLIC = re.compile(r"\b(parc|square|jardin)\b", re.I)

# Décisions prises à la main (2026-09-26), prioritaires sur tout rattachement.
POINT_PARC_BARBIEUX = "point manuel [50.67722273760695, 3.1625329346302813]"
PARC_BARBIEUX = {
    "categorie": "espace public", "source_cible": "adresse", "zone": "Parc Barbieux", "geometrie": POINT_PARC_BARBIEUX,
}
DECISIONS = {
    ("Barbieux, parc", "rue"): PARC_BARBIEUX,
    ("Barbieux, parc", ""): PARC_BARBIEUX,
    ("Parc Barbieux", "rue"): PARC_BARBIEUX,
    ("Parc Barbieux", ""): PARC_BARBIEUX,
    ("Barbieux", "rue"): {
        **PARC_BARBIEUX, "libelle_retenu": "Parc Barbieux",
        "remarque_auto": "carte postale « Café - Laiterie - Parc Barbieux »",
    },
    ("Pas de mention d'adresse", "rue"): {"categorie": "non toponyme", "source_cible": ""},
}
# Libellés que la règle de libelle_affiche() ne sait pas remettre à l'endroit.
LIBELLES_FORCES = {
    "Clément, Jean-Baptiste place": "Place Jean-Baptiste Clément",
    "Verte, rue de la place": "Rue de la Place Verte",
}
# Libellés finaux corrigés à la main (faute de la forme BnR reprise par accentuer()).
LIBELLES_FINAUX = {
    "Rue Ingrès": "Rue Ingres",
}
# Libellés finaux imparfaits dans toutes les sources : signalés dans a_traiter.
LIBELLES_A_VERIFIER = {
    "Rue J-B. Vercoutère": "nom abrégé dans les sources (Jean-Baptiste ?)",
    "Rue de Sevigné": "accent manquant dans les sources (Sévigné ?)",
    "Avenue André Chenier": "accent manquant dans les sources (Chénier ?)",
    "Parvis du Saint Rédempteur": "tiret manquant dans la référence (Saint-Rédempteur ?)",
}
# Mots laissés tels quels dans le type de voie remis en tête ("rue de la", "rue St").
MOTS_TYPE = {"de", "du", "des", "la", "le", "les", "l'", "d'", "et", "st", "ste", "point"}
# Mots en minuscule dans un nom de référence TOPO remis en casse ("PL DE LA PATRIE").
PARTICULES = {"de", "du", "des", "la", "le", "les", "et", "aux", "au", "sur", "en"}
# Mots qu'harmoniser() ne recapitalise jamais d'après la forme BnR ("Pierre De Roubaix").
MINUSCULES = {"de", "du", "des", "d", "l", "et", "aux", "au", "sur", "en", "van", "der", "von", "ten"}
# Départements présents dans l'extrait BD TOPO des communes (le Nord et ses voisins).
DEPARTEMENTS = {"59": "Nord", "62": "Pas-de-Calais", "02": "Aisne", "80": "Somme"}
ALERTE_TYPE = "type de voie différent de la référence (cas douteux, libellé non corrigé)"

COLONNES = [
    "a_traiter", "forme_bnr", "source_bnr", "occurrences", "ir", "annee_min", "annee_max", "coord_origine",
    "categorie", "source_cible", "libelle_retenu", "nom_ancien", "ref_nom", "ref_id", "ref_origine", "geometrie",
    "coord", "coord_manuel", "methode", "alerte", "validation", "remarque",
]


def extraire_bdtopo(gpkg):
    """Extrait de la BD TOPO (Lambert 93) les objets utiles, reprojetés en WGS84."""
    vers_wgs84 = Transformer.from_crs(2154, 4326, always_xy=True).transform

    def geom(blob):
        enveloppe = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}[(blob[3] >> 1) & 0b111]
        g = transform(vers_wgs84, wkb.loads(bytes(blob[8 + enveloppe:])))
        return json.loads(json.dumps(mapping(g)), parse_float=lambda x: round(float(x), 7))

    con = sqlite3.connect(f"file:{gpkg}?mode=ro", uri=True)
    voies = [
        {"type": "Feature", "geometry": geom(g),
         "properties": {"cleabs": c, "identifiant_voie_ban": i or "", "nom": n or "", "nom_normalise": nn or "", "type_voie": t or ""}}
        for c, i, n, nn, t, g in con.execute(
            "select cleabs, identifiant_voie_ban, coalesce(nullif(nom_voie_ban, ''), nom_collaboratif), nom_normalise, type_voie, geometrie "
            "from voie_nommee where insee_commune = '59512'"
        )
    ]
    zones = [
        {"type": "Feature", "geometry": geom(g),
         "properties": {"cleabs": c, "table": tab, "nature": n or "", "nature_detaillee": nd or "", "toponyme": t}}
        for tab, requete in (
            ("zone_d_activite_ou_d_interet", "select cleabs, nature, nature_detaillee, toponyme, geometrie from zone_d_activite_ou_d_interet "
             "where insee_commune = '59512' and nature = 'Espace public' and toponyme is not null"),
            ("zone_d_habitation", "select cleabs, nature, nature_detaillee, toponyme, geometrie from zone_d_habitation "
             "where insee_commune = '59512' and toponyme is not null"),
        )
        for c, n, nd, t, g in con.execute(requete)
    ]
    for chemin, features in ((VOIES_GEOJSON, voies), (ZONES_GEOJSON, zones)):
        with open(chemin, "w", encoding="utf-8") as f:
            json.dump({"type": "FeatureCollection", "source": "BD TOPO IGN, édition 2026-06-15", "features": features}, f, ensure_ascii=False)
    with open(COMMUNES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["code_insee", "nom_officiel"])
        writer.writerows(con.execute("select code_insee, nom_officiel from commune order by code_insee"))
    print(f"extrait BD TOPO : {len(voies)} voies, {len(zones)} zones → {GEO}")


def charger_references():
    with open(TOPO_CSV, encoding="utf-8-sig") as f:
        topo = [t for t in csv.DictReader(f, delimiter=";") if t["nature_de_voie"]]
    topo_par_cle, topo_par_id = defaultdict(list), {}
    for t in topo:
        ident = f"59512_{t['code_voie']}"
        nature = NATURES_TOPO.get(t["nature_de_voie"], t["nature_de_voie"].lower())
        topo_par_cle[cle(f"{nature} {t['libelle']}")].append(ident)
        topo_par_id[ident] = {"nom": f"{t['nature_de_voie']} {t['libelle']}", "creation": t["date_creation_de_article"]}

    with open(VOIES_GEOJSON, encoding="utf-8") as f:
        voies = [v["properties"] for v in json.load(f)["features"]]
    bd_par_cle, bd_par_ban = defaultdict(list), defaultdict(list)
    for v in voies:
        for nom in {v["nom"], v["nom_normalise"]}:
            if nom:
                bd_par_cle[cle(developper(nom))].append(v)
        if v["identifiant_voie_ban"]:
            bd_par_ban[v["identifiant_voie_ban"]].append(v)

    with open(ZONES_GEOJSON, encoding="utf-8") as f:
        zones = [z["properties"] for z in json.load(f)["features"]]
    espaces, quartiers = defaultdict(list), defaultdict(list)
    for z in zones:
        cible = espaces if z["table"] == "zone_d_activite_ou_d_interet" else quartiers
        if z["table"] == "zone_d_habitation" and z["nature"] != "Quartier":
            continue
        cible[cle(z["toponyme"])].append(z)

    with open(COMMUNES_CSV, encoding="utf-8") as f:
        communes = {cle(c["nom_officiel"]): c for c in csv.DictReader(f)}

    with open(FILAIRE_MEL, encoding="utf-8") as f:
        mel = defaultdict(list)
        for feature in json.load(f)["features"]:
            mel[feature["properties"]["nom_de_la_rue"]].append(feature["id"])
    mel_par_cle = defaultdict(list)
    for nom, ids in mel.items():
        mel_par_cle[cle(nom)].append((nom, ids))

    with open(QUARTIERS_MEL, encoding="utf-8") as f:
        quartiers_mel = [
            {"id": ft["properties"]["id"], "nom": ft["properties"]["Nom_du_quartier"],
             "cle": cle(ft["properties"]["Nom_du_quartier"])}
            for ft in json.load(f)["features"]
        ]

    with open(CONCORDANCE_CSV, encoding="utf-8") as f:
        concordance = defaultdict(list)
        for c in csv.DictReader(f):
            concordance[cle(c["ancien_nom"])].append(c)

    src = open(GEOGNAME2GEO, encoding="utf-8").read()
    corrections = ast.literal_eval(re.search(r"CORRECTIONS_RUE = (\{.*?\n\})", src, re.S).group(1))
    return {
        "topo_par_cle": topo_par_cle, "topo_par_id": topo_par_id, "bd_par_cle": bd_par_cle, "bd_par_ban": bd_par_ban,
        "espaces": espaces, "quartiers": quartiers, "communes": communes, "concordance": concordance,
        "corrections": corrections, "mel_par_cle": mel_par_cle, "quartiers_mel": quartiers_mel,
    }


ABREVIATIONS = {
    "r": "rue", "av": "avenue", "bd": "boulevard", "pl": "place", "imp": "impasse", "che": "chemin", "sq": "square",
    "all": "allee", "qu": "quai", "pas": "passage", "rpt": "rond point", "sen": "sentier", "crs": "cours",
    "cr": "cour", "prv": "parvis", "esp": "esplanade", "prom": "promenade",
}


def developper(nom):
    """Développe l'abréviation de type d'un nom normalisé BD TOPO ('R DE MENIN')."""
    mots = nom.split()
    if mots and mots[0].lower() in ABREVIATIONS and nom.isupper():
        mots[0] = ABREVIATIONS[mots[0].lower()]
    return " ".join(mots)


def voie(k, refs):
    """Voie actuelle de clé k : (id, nom, cleabs BD TOPO, date de création TOPO) ou None."""
    ids = refs["topo_par_cle"].get(k, [])
    bd = list(refs["bd_par_cle"].get(k, []))
    for i in ids:
        bd += [v for v in refs["bd_par_ban"].get(i, []) if v not in bd]
    if not ids:
        ids = sorted({v["identifiant_voie_ban"] for v in bd if v["identifiant_voie_ban"]})
    if not ids and not bd:
        mel = refs["mel_par_cle"].get(k)
        if not mel:
            return None
        ids_mel = " | ".join(i for _, liste in mel for i in liste)
        return {
            "ref_id": ids_mel, "ref_nom": " | ".join(n for n, _ in mel),
            "creation": "", "geometrie": "filaire MEL " + ids_mel, "ref_origine": "filaire MEL",
        }
    noms = (
        sorted({v["nom"] for v in bd if v["identifiant_voie_ban"] in ids and v["nom"]})
        or [refs["topo_par_id"][i]["nom"] for i in ids if i in refs["topo_par_id"]]
        or sorted({v["nom"] for v in bd})
    )
    creation = min((refs["topo_par_id"][i]["creation"] for i in ids if i in refs["topo_par_id"]), default="")
    ref_id = " | ".join(ids) or " | ".join(v["cleabs"] for v in bd)
    mel = [] if bd else refs["mel_par_cle"].get(k, [])
    if bd:
        geometrie = "BD TOPO " + " | ".join(sorted(v["cleabs"] for v in bd))
    elif mel:
        geometrie = "filaire MEL " + " | ".join(i for _, ids_mel in mel for i in ids_mel)
    else:
        geometrie = ""
    return {
        "ref_id": ref_id, "ref_nom": " | ".join(noms), "creation": creation, "geometrie": geometrie,
        "ref_origine": " + ".join(o for o, ok in (
            ("TOPO", ids and ids[0] in refs["topo_par_id"]), ("BD TOPO voie_nommee", bd), ("filaire MEL", mel)) if ok),
    }


def rattacher_espace(p, cles, source_bnr, refs):
    """Rattache p à un espace public BD TOPO (parc, square, place aménagée…)."""
    for cle_z in cles:
        z = refs["espaces"].get(cle_z)
        if z:
            p.update(categorie="espace public", source_cible="adresse", ref_nom=" | ".join(x["toponyme"] for x in z),
                     ref_id=" | ".join(x["cleabs"] for x in z), ref_origine="BD TOPO zone_d_activite",
                     geometrie="BD TOPO " + " | ".join(x["cleabs"] for x in z), methode="nom normalisé")
            if source_bnr != "adresse":
                p["alerte"].append("espace public : source adresse proposée (comme le parc Barbieux)")
            return True
    return False


def libelle_affiche(forme):
    """Libellé à afficher, dans l'ordre normal : 'Gare, rue de la' -> 'Rue de la Gare'.

    Apostrophes déséchappées, retours chariot et espaces multiples supprimés,
    qualificatif entre parenthèses renvoyé en fin ('Royale, rue (Lille)' ->
    'Rue Royale (Lille)'), prénoms et titres du type de voie capitalisés
    ('Bernard, rue claude' -> 'Rue Claude Bernard').
    """
    texte = " ".join(forme.replace("\\'", "'").split())
    texte = re.sub(r"\(\s+", "(", re.sub(r"\s+\)", ")", texte))
    if texte in LIBELLES_FORCES:
        return LIBELLES_FORCES[texte]
    if "," in texte:
        nom, type_part = (p.strip() for p in texte.rsplit(",", 1))
        qualif = re.search(r"\s*(\([^()]*\))$", type_part)
        if qualif:
            type_part = type_part[:qualif.start()]
        mots = type_part.split()
        mots[1:] = [m if m.lower() in MOTS_TYPE or TYPE_VOIE.fullmatch(m) else m[:1].upper() + m[1:] for m in mots[1:]]
        type_part = " ".join(mots)
        nom = nom[:1].upper() + nom[1:]
        texte = f"{type_part}{nom}" if type_part.endswith("'") else f"{type_part} {nom}".strip()
        if qualif:
            texte += " " + qualif.group(1)
    return texte[:1].upper() + texte[1:]


def sans_accents(texte):
    return "".join(c for c in unicodedata.normalize("NFKD", texte) if not unicodedata.combining(c))


def accentuer(cible, modele):
    """Reprend dans cible les accents des mots de modele ("Chat Botte" + "chat Botté" -> "Chat Botté").
    Un mot déjà accentué dans cible est laissé tel quel."""
    accentues = {sans_accents(m).lower(): m for m in re.findall(r"\w+", modele) if sans_accents(m) != m}
    def remplacer(mo):
        m = accentues.get(mo.group(0).lower())
        if m is None or sans_accents(mo.group(0)) != mo.group(0):
            return mo.group(0)
        return (m[:1].upper() if mo.group(0)[:1].isupper() else m[:1].lower()) + m[1:]
    return re.sub(r"\w+", remplacer, cible)


def harmoniser(ref, bnr):
    """Graphie de référence, réparée d'après la forme BnR : mot en minuscule dans la
    référence mais capitalisé dans la BnR ("Rue racine" -> "Rue Racine"), accents
    manquants ("Avenue Le Notre" -> "Avenue Le Nôtre")."""
    capitales = {sans_accents(m).lower() for m in re.findall(r"\w+", bnr) if m[:1].isupper()}
    def capitaliser(mo):
        m = mo.group(0)
        if m[:1].islower() and m not in MINUSCULES and sans_accents(m).lower() in capitales:
            return m[:1].upper() + m[1:]
        return m
    ref = re.sub(r"(?<!^)\b\w+", capitaliser, ref)
    return accentuer(ref, bnr)


def graphie_reference(ref_nom, modele):
    """Graphie à retenir pour un nom de référence ; un nom TOPO en capitales ('PL D AUDENAERDE') est
    développé, remis en casse ('Place d'Audenaerde') et accentué d'après la forme BnR."""
    nom = ref_nom.split(" | ")[0].strip()
    if not nom.isupper():
        return nom
    mots = developper(nom).lower().split()
    sortie = []
    for i, m in enumerate(mots):
        if m in ("d", "l") and i + 1 < len(mots):
            sortie.append(m + "'")
        elif i and m in PARTICULES:
            sortie.append(m)
        else:
            sortie.append("-".join(p[:1].upper() + p[1:] for p in m.split("-")))
    return accentuer(re.sub(r"([dl]') ", r"\1", " ".join(sortie)), modele)


def type_de_voie(nom):
    mo = TYPE_VOIE.match(nom)
    return re.sub(r"[- ]", " ", mo.group(0).lower()) if mo else ""


def finaliser(texte):
    """Retouches communes : St/Ste développés, Rond-Point, Allé -> Allée."""
    texte = re.sub(r"\bSte\.? (?=[A-ZÉ])", "Sainte-", re.sub(r"\bSt\.? (?=[A-ZÉ])", "Saint-", texte))
    texte = re.sub(r"\bRond[- ][Pp]oint\b", "Rond-Point", texte)
    texte = re.sub(r"^Allé\b", "Allée", texte)
    return LIBELLES_FINAUX.get(texte, texte)


def libelle_final(forme, p, refs):
    """(libelle_retenu, nom_ancien) selon les règles décidées le 2026-10-27.

    - voie rattachée : graphie de la référence (accents, casse, articles, nom
      officiel complet), réparée par harmoniser() quand la référence est moins
      bien écrite que la BnR ; CORRECTIONS_RUE : forme de CORRECTIONS_RUE ;
      renommage : nom actuel, l'ancien nom gardé dans nom_ancien ; type de voie
      différent de la référence : libellé BnR conservé et alerte ;
    - espace public : graphie de la référence si elle ne diffère que par les
      accents, la casse ou la ponctuation ;
    - quartier : graphie de la référence, sans article ;
    - commune : nom officiel suivi de son département, tiré du code INSEE
      (" (Nord)", " (Pas-de-Calais)"…).
    """
    bnr = libelle_affiche(p["libelle_retenu"])
    if p["methode"] == "décision manuelle" or not p["ref_nom"]:
        return finaliser(bnr), ""
    ref = graphie_reference(p["ref_nom"], bnr)
    if p["categorie"] == "commune":
        return f"{ref} ({DEPARTEMENTS[p['ref_id'].removeprefix('INSEE ')[:2]]})", ""
    if p["categorie"] == "quartier":
        if p["ref_origine"] == "MEL quartiers historiques":
            # Nom MEL en capitales sans accents : graphie de la BD TOPO pour le même nom,
            # sinon nom MEL remis en casse et accentué d'après la forme BnR.
            bd = refs["quartiers"].get(cle(p["ref_nom"]))
            if bd:
                ref = bd[0]["toponyme"]
            else:
                ref = harmoniser(graphie_reference(re.sub(r"\bSTE\b", "SAINTE", p["ref_nom"]).replace(" - ", " "), bnr), bnr)
                p["alerte"].append("libellé de quartier construit d'après le nom MEL : à valider")
        ref = re.sub(r"^(l'|la |le |les )", "", ref, flags=re.I)
        return ref[:1].upper() + ref[1:], ""
    if p["categorie"] == "espace public":
        simple = lambda s: re.sub(r"[^a-z0-9]", "", sans_accents(s).lower())
        return finaliser(ref if simple(ref) == simple(bnr) else bnr), ""
    if p["methode"] == "CORRECTIONS_RUE":
        ref = refs["corrections"][forme]
    else:
        ref = harmoniser(ref, bnr)
    if type_de_voie(bnr) and type_de_voie(ref) and type_de_voie(bnr) != type_de_voie(ref):
        p["alerte"].append(ALERTE_TYPE)
        return finaliser(bnr), ""
    ancien = bnr if p["methode"].startswith("concordance (ancien nom") else ""
    return finaliser(ref), ancien


def charger_geometries():
    """cleabs BD TOPO ou id du filaire MEL -> géométrie shapely (WGS84, x = lon, y = lat)."""
    geos = {}
    for chemin in (VOIES_GEOJSON, ZONES_GEOJSON):
        with open(chemin, encoding="utf-8") as f:
            geos.update({ft["properties"]["cleabs"]: shape(ft["geometry"]) for ft in json.load(f)["features"]})
    with open(FILAIRE_MEL, encoding="utf-8") as f:
        geos.update({ft["id"]: shape(ft["geometry"]) for ft in json.load(f)["features"]})
    with open(QUARTIERS_MEL, encoding="utf-8") as f:
        geos.update({f"MEL quartier {ft['properties']['id']}": shape(ft["geometry"]) for ft in json.load(f)["features"]})
    return geos


def lat_lon(coords):
    return [[round(y, 7), round(x, 7)] for x, y in coords]


def coordonnees(l, coord_origine, geos):
    """@coord à écrire, au format des IR ([lat, lon], l'inverse du GeoJSON).

    Voie : tracé, [[lat, lon], …] pour un tronçon, [[[lat, lon], …], …] pour
    plusieurs (tronçons contigus fusionnés). Quartier : contour du polygone.
    Espace public : point central. Point manuel, point ou polygone d'origine :
    repris tels quels. Sans géométrie : vide. Un polygone source invalide est
    réparé (partie surfacique de make_valid) et signalé dans la remarque.
    """
    g = l["geometrie"]
    if g.startswith("point manuel "):
        return g[len("point manuel "):].strip()
    if g in ("point d'origine", "polygone d'origine"):
        try:
            return json.dumps(json.loads(coord_origine))
        except ValueError:
            return coord_origine.strip()
    if g.startswith("MEL quartier "):
        ids = [g]
    elif g.startswith(("BD TOPO ", "filaire MEL ")):
        ids = dict.fromkeys(i.strip() for i in re.sub(r"^(BD TOPO|filaire MEL) ", "", g).split("|"))
    else:
        return ""
    objet = unary_union([geos[i] for i in ids])
    if not objet.is_valid and objet.geom_type.endswith("Polygon"):
        # Polygone source invalide (ex. contour MEL qui se recoupe) : on garde la partie
        # surfacique de make_valid() et on le note en remarque.
        objet = unary_union([g for g in getattr(make_valid(objet), "geoms", [make_valid(objet)])
                             if g.geom_type.endswith("Polygon")])
        l["remarque_auto"] = (l["remarque_auto"] + " ; " if l["remarque_auto"] else "") + \
            "géométrie source invalide, réparée (make_valid)"
    if l["categorie"] == "espace public":
        point = objet.centroid if objet.contains(objet.centroid) else objet.representative_point()
        return json.dumps([round(point.y, 7), round(point.x, 7)])
    if objet.geom_type == "MultiLineString":
        objet = linemerge(objet)
    parties = list(getattr(objet, "geoms", [objet]))
    if objet.geom_type.endswith("Polygon"):
        traces = [lat_lon(p.exterior.coords) for p in parties]
    else:
        traces = [lat_lon(p.coords) for p in parties]
    return json.dumps(traces[0] if len(traces) == 1 else traces)


def a_traiter(l):
    """Motifs d'intervention manuelle d'une ligne, vides une fois la ligne validée."""
    if l["validation"].strip():
        return ""
    motifs = [a for a in l["alerte"].split(" ; ") if a]
    if l["categorie"] == "à identifier":
        motifs.append("lieu à identifier")
    if l["nom_ancien"]:
        motifs.append("renommage : nom actuel à confirmer")
    if l["methode"] == "CORRECTIONS_RUE" and ALERTE_TYPE not in motifs:
        ref = graphie_reference(l["ref_nom"], l["libelle_retenu"])
        simple = lambda s: re.sub(r"[^a-z]", "", sans_accents(s).lower())
        if simple(ref) != simple(l["libelle_retenu"]):
            motifs.append(f"CORRECTIONS_RUE et référence divergent (référence : {ref})")
    if l["categorie"] == "voie" and l["ref_id"] and not l["geometrie"]:
        motifs.append("voie sans géométrie (référence TOPO seule)")
    if l["libelle_retenu"] in LIBELLES_A_VERIFIER:
        motifs.append(LIBELLES_A_VERIFIER[l["libelle_retenu"]])
    if l["coord"] and not l["coord_manuel"]:
        try:
            json.loads(l["coord"])
        except ValueError:
            motifs.append("coordonnées d'origine illisibles (tronquées ?) : saisir coord_manuel")
    return " ; ".join(motifs)


def rattacher_quartier(p, forme, refs, inclusion):
    """Rattache p à un quartier historique de la MEL : par nom normalisé, ou — si
    inclusion — à l'unique quartier dont le nom contient tous les mots de la forme
    ("Centre" -> CENTRE VILLE), avec une alerte."""
    k = cle(re.sub(r"^(l'|la |le |les )", "", forme, flags=re.I))
    if not k:
        return False
    exacts = [q for q in refs["quartiers_mel"] if q["cle"] == k]
    inclus = [q for q in refs["quartiers_mel"] if set(k.split()) <= set(q["cle"].split())] if inclusion else []
    q = exacts[0] if exacts else (inclus[0] if len(inclus) == 1 else None)
    if q is None:
        return False
    p.update(categorie="quartier", source_cible="quartier", ref_nom=q["nom"], ref_id=f"MEL quartier {q['id']}",
             ref_origine="MEL quartiers historiques", geometrie=f"MEL quartier {q['id']}",
             methode="nom normalisé" if exacts else "inclusion du nom")
    if not exacts:
        p["alerte"].append(f"quartier rattaché par inclusion du nom ({q['nom']}) : à confirmer")
    return True


def proposer(forme, source_bnr, annee_min, refs):
    k = cle(reconstruire(forme))
    kf = cle(forme)
    est_voie = bool(TYPE_VOIE.search(forme)) or source_bnr == "rue"
    p = {"categorie": "", "source_cible": source_bnr, "libelle_retenu": forme, "ref_nom": "", "ref_id": "",
         "ref_origine": "", "geometrie": "", "methode": "", "alerte": [], "remarque_auto": ""}

    decision = DECISIONS.get((forme, source_bnr))
    if decision:
        p.update({c: v for c, v in decision.items() if c != "zone"})
        p["methode"] = "décision manuelle"
        if "zone" in decision:
            z = refs["espaces"][cle(decision["zone"])][0]
            p.update(ref_nom=z["toponyme"], ref_id=z["cleabs"], ref_origine="BD TOPO zone_d_activite",
                     geometrie=decision.get("geometrie") or "BD TOPO " + z["cleabs"])
        return p

    # Une forme indexée comme quartier est d'abord cherchée parmi les quartiers ; une
    # forme indexée comme adresse ("17 Grand'Place") reste une adresse (point).
    if source_bnr == "quartier" and rattacher_quartier(p, forme, refs, inclusion=True):
        return p
    if source_bnr == "adresse":
        p.update(categorie="adresse", source_cible="adresse")
        return p

    def rattacher_voie(v, methode):
        p.update(categorie="voie", source_cible="rue", ref_nom=v["ref_nom"], ref_id=v["ref_id"],
                 ref_origine=v["ref_origine"], geometrie=v["geometrie"], methode=methode)
        if v["creation"] > CREATION_INITIALE_TOPO and (annee_min is None or annee_min < int(v["creation"][:4])):
            p["alerte"].append(f"voie actuelle créée en {v['creation'][:4]}" + (f", documents dès {annee_min}" if annee_min else ", documents sans date"))

    if ESPACE_PUBLIC.search(forme) and rattacher_espace(p, (k, kf), source_bnr, refs):
        return p

    for c in refs["concordance"].get(k, []) if est_voie else []:
        cible = voie(cle(c["voie_actuelle"]), refs)
        homonyme = voie(k, refs)
        if c["type"] == "variante" and cible and not homonyme:
            rattacher_voie(cible, f"concordance (variante de {c['voie_actuelle']})")
            return p
        if c["type"] == "renommage" and cible:
            ancien_homonyme = homonyme and homonyme["creation"] <= CREATION_INITIALE_TOPO
            if ancien_homonyme:
                rattacher_voie(homonyme, "nom normalisé")
                p["alerte"].append(f"nom en partie ou jadis porté par {c['voie_actuelle']} (concordance)")
            else:
                rattacher_voie(cible, f"concordance (ancien nom de {c['voie_actuelle']})")
                if homonyme:
                    p["alerte"].append(f"homonyme actuel {homonyme['ref_nom']} ({homonyme['ref_id']}), créé en {homonyme['creation'][:4]}")
            return p

    if est_voie and forme in refs["corrections"]:
        v = voie(cle(refs["corrections"][forme]), refs)
        if v:
            rattacher_voie(v, "CORRECTIONS_RUE")
            return p

    if est_voie:
        v = voie(k, refs)
        if v:
            rattacher_voie(v, "nom normalisé")
            return p

    if rattacher_espace(p, (k, kf), source_bnr, refs):
        return p

    m = re.fullmatch(r"(.+?)\s*\(([^)]+)\)", forme.strip())
    nom_lieu, qualif = (m.group(1), m.group(2)) if m else (forme, "")
    if not est_voie:
        commune = refs["communes"].get(cle(nom_lieu))
        if commune and qualif in ("", "Nord"):
            p.update(categorie="commune", source_cible=source_bnr, ref_nom=commune["nom_officiel"],
                     ref_id=f"INSEE {commune['code_insee']}", ref_origine="BD TOPO commune", methode="nom normalisé")
            return p
        if cle(nom_lieu) in PAYS:
            p.update(categorie="pays", methode="liste")
            return p
        if qualif and qualif != "Nord":
            p.update(categorie="lieu hors Nord", methode="qualificatif entre parenthèses")
            return p
        if source_bnr == "" and rattacher_quartier(p, forme, refs, inclusion=False):
            return p

    if est_voie:
        p.update(categorie="voie", source_cible="rue")
        p["alerte"].append("absente des sources actuelles (nom ancien ?)")
    elif source_bnr == "quartier":
        p["categorie"] = "quartier"
        p["alerte"].append("quartier absent des quartiers historiques MEL")
    else:
        p["categorie"] = "à identifier"
    return p


def main():
    if "--bdtopo" in sys.argv:
        extraire_bdtopo(sys.argv[sys.argv.index("--bdtopo") + 1])
    if not exists(VOIES_GEOJSON):
        sys.exit("Extraits BD TOPO absents : relancer avec --bdtopo <fichier .gpkg du Nord>.")
    refs = charger_references()
    geos = charger_geometries()

    annees = annees_par_composant()
    formes = defaultdict(lambda: {"occ": 0, "ir": set(), "annees": [], "coords": Counter()})
    with open(ORIGINE_CSV, encoding="utf-8") as f:
        for l in csv.DictReader(f):
            if not l["geogname"] or l["source"] == "chrono":
                continue
            fo = formes[(l["geogname"], l["source"])]
            fo["occ"] += 1
            fo["ir"].add(l["eadid"].replace("FR595126101_", ""))
            fo["annees"] += annees.get((l["eadid"], l["c_id"]), [])
            if l["coord"]:
                fo["coords"][l["coord"]] += 1

    manuel = {}
    if exists(SORTIE):
        with open(SORTIE, encoding="utf-8") as f:
            anciennes = list(csv.DictReader(f))
        if anciennes and "validation" in anciennes[0]:
            manuel = {(l["forme_bnr"], l["source_bnr"]): l for l in anciennes}

    lignes = []
    for (forme, source_bnr), fo in formes.items():
        annee_min = min(fo["annees"]) if fo["annees"] else None
        p = proposer(forme, source_bnr, annee_min, refs)
        if source_bnr == "quartier" and p["categorie"] != "quartier":
            p["alerte"].append(f"indexé comme quartier dans les IR, rattaché comme {p['categorie']}")
        p["libelle_retenu"], p["nom_ancien"] = libelle_final(forme, p, refs)
        coord = fo["coords"].most_common(1)[0][0] if fo["coords"] else ""
        polygone = bool(re.match(r"\s*\[\s*\[", coord))
        if not p["geometrie"] and coord:
            p["geometrie"] = "polygone d'origine" if polygone else "point d'origine"
        garde = manuel.get((forme, source_bnr), {})
        coord_calculee = coordonnees(p, coord, geos)
        lignes.append({
            "forme_bnr": forme, "source_bnr": source_bnr, "occurrences": fo["occ"], "ir": ", ".join(sorted(fo["ir"])),
            "annee_min": annee_min or "", "annee_max": max(fo["annees"]) if fo["annees"] else "",
            "coord_origine": ("polygone" if polygone else "point") if coord else "",
            **{c: p[c] for c in ("categorie", "source_cible", "libelle_retenu", "nom_ancien", "ref_nom", "ref_id", "ref_origine", "geometrie", "methode")},
            "coord": coord_calculee,
            "coord_manuel": garde.get("coord_manuel", ""),
            "alerte": " ; ".join(p["alerte"]),
            "validation": garde.get("validation", ""),
            "remarque": garde.get("remarque") or p["remarque_auto"],
        })
    # Un même objet ne garde qu'un libellé quand ses variantes ne diffèrent que par les
    # accents ou la casse : la plus accentuée ("Avenue Le Notre" / "Avenue Le Nôtre").
    par_objet = defaultdict(list)
    for l in lignes:
        if l["ref_id"] and l["categorie"] == "voie":
            par_objet[(l["ref_id"], sans_accents(l["libelle_retenu"]).lower())].append(l)
    for variantes in par_objet.values():
        retenu = max(variantes, key=lambda l: (len(l["libelle_retenu"].encode()), l["occurrences"]))["libelle_retenu"]
        for l in variantes:
            l["libelle_retenu"] = retenu
    for l in lignes:
        l["a_traiter"] = a_traiter(l)

    ordre = ["voie", "espace public", "adresse", "quartier", "commune", "lieu hors Nord", "pays", "non toponyme", "à identifier"]
    lignes.sort(key=lambda l: (ordre.index(l["categorie"]) if l["categorie"] in ordre else 99, not l["ref_id"], -l["occurrences"]))
    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES, lineterminator="\n")
        writer.writeheader()
        writer.writerows(lignes)

    print(f"{len(lignes)} formes, {sum(l['occurrences'] for l in lignes)} geogname → {SORTIE}")
    stats = defaultdict(lambda: [0, 0, 0, 0])
    for l in lignes:
        s = stats[l["categorie"]]
        s[0] += 1
        s[1] += l["occurrences"]
        s[2] += bool(l["ref_id"])
        s[3] += bool(l["alerte"])
    print(f"{'catégorie':16s} {'formes':>7s} {'geogname':>9s} {'rattachées':>11s} {'alertes':>8s}")
    for c in ordre:
        if c in stats:
            print(f"{c:16s} {stats[c][0]:7d} {stats[c][1]:9d} {stats[c][2]:11d} {stats[c][3]:8d}")


if __name__ == "__main__":
    main()
