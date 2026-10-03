"""
Propose le référentiel des geogname : une ligne par forme BnR (libellé +
@source d'origine), rattachée quand c'est possible à un objet de référence
actuel — voie (TOPO / BD TOPO), espace public, quartier ou commune.

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

Ordre de rattachement : décision manuelle, concordance (renommage /
variante), CORRECTIONS_RUE, voie par nom normalisé (TOPO + BD TOPO),
espace public, commune, quartier. Les alertes signalent les cas à trancher
(renommage, voie actuelle plus récente que les documents…).

Sortie : data/geo/referentiel_geogname.csv. Les colonnes validation et
remarque, remplies à la main, sont conservées d'une exécution à l'autre.

À lancer depuis la racine du dépôt :
    python scripts/ead/geogname_referentiel.py [--bdtopo <fichier.gpkg>]
"""
import ast
import csv
import json
import re
import sqlite3
import sys
from collections import defaultdict
from os.path import dirname, exists, join

from pyproj import Transformer
from shapely import wkb
from shapely.geometry import mapping
from shapely.ops import transform

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
SORTIE = join(GEO, "referentiel_geogname.csv")

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
COLONNES = [
    "forme_bnr", "source_bnr", "occurrences", "ir", "annee_min", "annee_max", "coord_origine",
    "categorie", "source_cible", "libelle_retenu", "ref_nom", "ref_id", "ref_origine", "geometrie",
    "methode", "alerte", "validation", "remarque",
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

    with open(CONCORDANCE_CSV, encoding="utf-8") as f:
        concordance = defaultdict(list)
        for c in csv.DictReader(f):
            concordance[cle(c["ancien_nom"])].append(c)

    src = open(GEOGNAME2GEO, encoding="utf-8").read()
    corrections = ast.literal_eval(re.search(r"CORRECTIONS_RUE = (\{.*?\n\})", src, re.S).group(1))
    return {
        "topo_par_cle": topo_par_cle, "topo_par_id": topo_par_id, "bd_par_cle": bd_par_cle, "bd_par_ban": bd_par_ban,
        "espaces": espaces, "quartiers": quartiers, "communes": communes, "concordance": concordance,
        "corrections": corrections, "mel_par_cle": mel_par_cle,
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
        z = refs["quartiers"].get(kf) or refs["quartiers"].get(cle(re.sub(r"^(l'|la |le |les )", "", forme, flags=re.I)))
        if z and source_bnr in ("quartier", ""):
            p.update(categorie="quartier", source_cible="quartier", ref_nom=" | ".join(sorted({x["toponyme"] for x in z})),
                     ref_id=" | ".join(x["cleabs"] for x in z), ref_origine="BD TOPO zone_d_habitation",
                     geometrie="BD TOPO " + " | ".join(x["cleabs"] for x in z), methode="nom normalisé")
            return p

    if est_voie:
        p.update(categorie="voie", source_cible="rue")
        p["alerte"].append("absente des référentiels actuels (nom ancien ?)")
    elif source_bnr == "quartier":
        p["categorie"] = "quartier"
        p["alerte"].append("quartier absent de la BD TOPO")
    else:
        p["categorie"] = "à identifier"
    return p


def main():
    if "--bdtopo" in sys.argv:
        extraire_bdtopo(sys.argv[sys.argv.index("--bdtopo") + 1])
    if not exists(VOIES_GEOJSON):
        sys.exit("Extraits BD TOPO absents : relancer avec --bdtopo <fichier .gpkg du Nord>.")
    refs = charger_references()

    annees = annees_par_composant()
    formes = defaultdict(lambda: {"occ": 0, "ir": set(), "annees": [], "coord": ""})
    with open(ORIGINE_CSV, encoding="utf-8") as f:
        for l in csv.DictReader(f):
            if not l["geogname"] or l["source"] == "chrono":
                continue
            fo = formes[(l["geogname"], l["source"])]
            fo["occ"] += 1
            fo["ir"].add(l["eadid"].replace("FR595126101_", ""))
            fo["annees"] += annees.get((l["eadid"], l["c_id"]), [])
            fo["coord"] = fo["coord"] or l["coord"]

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
        coord = fo["coord"]
        if not p["geometrie"] and coord:
            p["geometrie"] = "point d'origine" if not coord.lstrip("[ ").startswith("[") else "polygone d'origine"
        garde = manuel.get((forme, source_bnr), {})
        lignes.append({
            "forme_bnr": forme, "source_bnr": source_bnr, "occurrences": fo["occ"], "ir": ", ".join(sorted(fo["ir"])),
            "annee_min": annee_min or "", "annee_max": max(fo["annees"]) if fo["annees"] else "",
            "coord_origine": "point" if coord and not coord.lstrip("[ ").startswith("[") else ("polygone" if coord else ""),
            **{c: p[c] for c in ("categorie", "source_cible", "libelle_retenu", "ref_nom", "ref_id", "ref_origine", "geometrie", "methode")},
            "alerte": " ; ".join(p["alerte"]),
            "validation": garde.get("validation", ""),
            "remarque": garde.get("remarque") or p["remarque_auto"],
        })
    ordre = ["voie", "espace public", "quartier", "commune", "lieu hors Nord", "pays", "non toponyme", "à identifier"]
    lignes.sort(key=lambda l: (ordre.index(l["categorie"]) if l["categorie"] in ordre else 99, not l["ref_id"], -l["occurrences"]))
    with open(SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
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
