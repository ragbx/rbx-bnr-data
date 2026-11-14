"""
Sites candidats : repère dans les IR les notices qui citent un bâtiment ou un
site de Roubaix (église, hôpital, gare, hôtel de ville, salle, musée,
école, parc…) et les rapproche des points du référentiel des lieux.

Étape 1 de la méthode « sites » (2026-11-12) : lecture seule des IR, rien
n'est écrit dans les IR ni dans le référentiel.

Trois sources dans chaque notice <c>, de la plus sûre à la moins sûre :
  - corpname : collectivité indexée qui désigne un bâtiment (mot de type :
    église, salle, théâtre…) ou qualifiée « (Roubaix, Nord) » ;
  - subject : sujet RAMEAU qui nomme un bâtiment ;
  - texte : unittitle et scopecontent, un mot de type suivi d'un nom
    (« église Saint-Martin », « hôpital de la Fraternité ») ; quelques sites
    sont pris sans nom (hôtel de ville, mairie, conservatoire, Condition
    Publique, Colisée, Cave aux Poètes, ENSAIT). Un mot de type précédé d'un
    type de voie (« rue de l'Hospice », « place de la Gare ») est ignoré.

Les formes sont réduites à une clé (cle() du référentiel, sans « Roubaix »,
« Nord ») ; une clé = un site candidat. Chaque clé est comparée aux points du
référentiel (en service puis écartés) : « exact » si même clé, « approché »
si les mots distinctifs de l'un sont tous dans l'autre, avec même famille ;
« variante connue » pour quelques noms d'un même site listés dans VARIANTES
(mairie → hôtel de ville, conservatoire national → conservatoire, La Piscine…).

Alertes (à lire, rien n'est écarté) : voie homonyme, nom de quartier,
autre ville (forme ou contexte de la notice), nom de personne indexé dans
la même notice.

Sorties (data/geo/) :
  - propositions_sites_<date>.csv : une ligne par site candidat, triée par
    nombre de notices ; colonnes manuelles decision, lieu_manuel, remarque
    conservées d'une exécution à l'autre (clé : site_cle) ;
  - propositions_sites_<date>_notices.csv : une ligne par notice × site.

À lancer depuis la racine du dépôt :
    python scripts/ead/sites_candidats.py [--date AAAAMMJJ]
"""
import argparse
import csv
import glob
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from os.path import basename, dirname, exists, join

from lxml import etree
from shapely import wkt
from shapely.geometry import Point
from shapely.ops import transform

sys.path.insert(0, dirname(__file__))
from referentiel_lieux_roubaix import (BAN_CSV, GEO, SORTIE as REFERENTIEL, SORTIE_ECARTES, VERS_L93,  # noqa: E402
                                       VERS_WGS84, cle, cle_voie)

IR_GLOB = join("results", "ead", "ead_cor", "bnr2mnesysgeo", "*.xml")
COMMUNES_PAYS = join(GEO, "referentiel_communes_pays.csv")
MANUELLES = ["decision", "lieu_manuel", "remarque"]

# Familles de sites : mot de type (sur la clé) → famille.
FAMILLES = [
    ("culte", r"eglise|chapelle|basilique|temple|synagogue|mosquee|couvent"),
    ("santé", r"hopital|hospice|clinique|sanatorium|maternite|dispensaire|hospital"),
    ("gare", r"gare"),
    ("mairie", r"hotel ville|mairie"),
    ("spectacle", r"theatre|salle|cinema|colisee|cave aux poetes|casino|kursaal"),
    ("culture", r"musee|piscine$|bibliotheque|mediatheque|conservatoire|archives|condition publique|centre culturel"),
    ("enseignement", r"ecole|lycee|college|institut|ensait|universite"),
    ("parc", r"parc|square|jardin"),
    ("sport", r"stade|velodrome|piscine|gymnase|hippodrome"),
    ("cimetière", r"cimetiere"),
    ("justice", r"tribunal|prison|caserne|commissariat|palais justice"),
    ("demeure", r"chateau|villa|hotel particulier"),
    ("industrie", r"usine|filature|tissage|peignage|teinturerie|brasserie|manufacture"),
]
# Étapes 2-3 : type de lien.
# IR d'images (le site cité en titre est ce que montre l'image) et IR d'événements (affiches, programmes).
IR_IMAGES = {"MED_CP", "MED_PHO", "MED_PHD", "MED_PBI", "MED_PAR", "MED_IMA", "ARA_CPS", "MUS_VAI", "MED_PLA"}
IR_EVENEMENTS = {"MED_AFF", "MED_MAR", "MUS_EVE", "MED_EPH"}
# Rue indexée dans la même notice à moins de DISTANCE_RUE_M du site : le site est le lieu (ou le sujet).
DISTANCE_RUE_M = 100
# Clés qui désignent une organisation, pas un bâtiment.
ORGANISATIONS = r"^(compagnie|association|societe|amis|club|ensemble|orchestre|chorale|groupe|federation|comite|union|cercle|harmonie)\b"
# Institutions qui peuvent être lieu ou organisateur sur une affiche.
INSTITUTIONS = r"^(conservatoire|mediatheque|bibliotheque|musee|piscine|archives|mairie|hotel ville|ecole|ensait|institut|lycee|college|centre culturel|manufacture|gymnase)\b"
LIENS_RETENUS = {"sujet", "lieu"}
# Adresses et dates dans les titres et les notes (« situé au 37 rue du Collège », « créée en 1895 »).
ADRESSE = re.compile(
    r"\b(\d{1,4})\s*(bis|ter)?\s*,?\s+((?:rue|place|boulevard|bd|avenue|quai|chemin|impasse|all[ée]e|grand[- ]rue"
    r"|grande[- ]rue|grand[- ]place|parvis|cour|passage|contour|square|sentier)\b[^,.;()]{0,45})", re.I)
DATE = re.compile(
    r"\b(cr[ée]{2}e?s?|fond[ée]e?s?|construite?s?|b[âa]tie?s?|inaugur[ée]e?s?|d[ée]truite?s?|d[ée]molie?s?"
    r"|ouverte?s?|ferm[ée]e?s?)\s+(?:en|le|vers)\s+[^.;]{0,20}?\b(1[6-9]\d\d|20[0-2]\d)\b", re.I)
# Adresse de la note à plus de DISTANCE_ADRESSE_M du point proposé : alerte (numérotation ou point faux ?).
DISTANCE_ADRESSE_M = 150
# Sites retenus sur une affiche SEULEMENT si la rue indexée dans la notice les confirme (lieu_id → motif).
LIEU_SI_RUE = {
    # Conservatoire : décision utilisateur du 2026-11-12 (« Le Conservatoire en balade » = organisateur).
    "bdtopo:SURFACTI0000002005143745": "conservatoire : lieu seulement si la rue est indexée (décision 2026-11-12)",
    # Musées et archives : même règle, décision utilisateur du 2026-11-12. La médiathèque n'a pas
    # encore de point : à ajouter ici quand elle en aura un.
    "bdtopo:SURFACTI0000000056485496": "La Piscine : lieu seulement si la rue est indexée (décision 2026-11-12)",
    "bdtopo:SURFACTI0000000216730012": "La Manufacture : lieu seulement si la rue est indexée (décision 2026-11-12)",
    "bdtopo:SURFACTI0000002222498347": "ANMT : lieu seulement si la rue est indexée (décision 2026-11-12)",
}

# Variantes connues d'un même site (clé du candidat → clé du point).
VARIANTES = {
    "mairie": "hotel ville",
    "conservatoire": "conservatoire a rayonnement departemental",
    "conservatoire national": "conservatoire a rayonnement departemental",
    "salle wattremez": "salle watremez",
    "musee piscine": "piscine",
    "piscine musee art et industrie andre diligent": "piscine",
    "musee art et industrie": "piscine",
    "musee art et industrie andre diligent": "piscine",
}
MOTS_TYPE = set("""eglise chapelle basilique temple synagogue mosquee couvent hopital hospice clinique
sanatorium maternite dispensaire hospital gare hotel ville mairie theatre salle cinema colisee casino kursaal
musee bibliotheque mediatheque conservatoire archives centre culturel ecole lycee college institut
universite parc square jardin stade velodrome piscine gymnase hippodrome cimetiere tribunal prison caserne
commissariat palais justice chateau villa particulier usine filature tissage peignage teinturerie brasserie
manufacture ancien ancienne nouveau nouvelle grand grande vieux vieille""".split())

TYPE_TEXTE = re.compile(
    r"(?<![\w-])(?:[ée]glise|chapelle|basilique|temple|synagogue|mosqu[ée]e|couvent|h[ôo]pital|hospice|clinique"
    r"|sanatorium|maternit[ée]|dispensaire|gare|th[ée][âa]tre|salle|cin[ée]ma|mus[ée]e|biblioth[èe]que"
    r"|m[ée]diath[èe]que|lyc[ée]e|[ée]cole|coll[èe]ge|institut|parc|square|jardin|stade|v[ée]lodrome|piscine"
    r"|cimeti[èe]re|caserne|prison|tribunal|ch[âa]teau|usine|filature|tissage|peignage|teinturerie|brasserie)s?(?![\w-])",
    re.I)
SANS_NOM = re.compile(
    r"(?<![\w-])(?:h[ôo]tel de ville|mairie|conservatoire|condition publique|colis[ée]e|cave aux po[èe]tes|ENSAIT)(?![\w-])",
    re.I)
# Insensible à la casse pour les mots, pas pour la majuscule du nom qui suit (« Gare Lille-Flandres »,
# « gare de Fives » : gare nommée, exclue ici).
GARE_SEULE = re.compile(
    r"(?<![\w-])(?i:gare)(?![\w-])"
    r"(?!\s+(?:(?i:de|du|des|aux?)\s+|(?i:d)['’]\s*)?[A-ZÀ-Ý])"
    r"(?!\s+(?i:de|aux?)\s+(?i:marchandises|triage))(?!\s+(?i:routi[èe]re|d['’]eau))")
VOIE_AVANT = re.compile(
    r"\b(?:rue|place|avenue|boulevard|bd|quai|chemin|impasse|all[ée]e|passage|sentier|pont|route|cour|cit[ée]"
    r"|r[ée]sidence|carrefour|contour|parvis|promenade)\s+(?:de\s+la\s+|de\s+l['’]\s*|du\s+|des\s+|de\s+|d['’]\s*)?$",
    re.I)
TYPES_VOIE = r"^(rue|place|avenue|boulevard|bd|quai|chemin|impasse|allee|passage|sentier|pont|route|cour|cite|residence|square|parvis|promenade|esplanade|clos|hameau|carrefour|contour|mail|rond point|villa) "
LIAISONS = {"de", "du", "des", "la", "le", "les", "l", "d", "saint", "sainte", "st", "ste", "et", "aux", "au", "en"}
JETON = re.compile(r"[\w'’-]+|[^\s\w]")


def txt(e):
    return " ".join(" ".join(e.itertext()).split()) if e is not None else ""


def cle_site(forme):
    k = cle(forme.replace("œ", "oe").replace("Œ", "Oe"))
    k = re.sub(r"\b(roubaix|nord|roubaisien|roubaisienne)\b", " ", k)
    return re.sub(r"\s+", " ", k).strip()


def famille(k):
    for nom, rx in FAMILLES:
        if re.search(r"\b(?:" + rx + r")\b", k):
            return nom
    return ""


def distinctifs(k):
    return {m for m in k.split() if m not in MOTS_TYPE}


def capter_nom(texte, debut):
    """Nom qui suit un mot de type : liaisons et mots à majuscule, 6 jetons au plus."""
    jetons, pris = JETON.findall(texte[debut:debut + 120]), []
    for i, j in enumerate(jetons):
        base = re.sub(r"^[ldLD]['’]", "", j)
        suivant = jetons[i + 1] if i + 1 < len(jetons) else ""
        if j.lower() == "et" and (not suivant[:1].isupper() or suivant == "Ville") and not re.match(r"^[dD]['’]", suivant):
            break  # « et la Grand Place », « et la Ville » : autre entité
        if j.lower().strip("'’") in LIAISONS or re.match(r"^[ldLD]['’]$", j):
            pris.append(j)
        elif base[:1].isupper():
            pris.append(j)
        else:
            break
        if len(pris) >= 6:
            break
    while pris and (pris[-1].lower().strip("'’") in LIAISONS or re.match(r"^[ldLD]['’]$", pris[-1])):
        pris.pop()
    if not any(re.sub(r"^[ldLD]['’]", "", j)[:1].isupper() for j in pris):
        return ""
    return " ".join(pris).replace(" '", "'").replace("' ", "'").replace("’ ", "’")


def formes_texte(texte):
    """(forme, texte qui précède, début de titre ?) pour chaque site cité."""
    out = []
    for m in TYPE_TEXTE.finditer(texte):
        if VOIE_AVANT.search(texte[max(0, m.start() - 30):m.start()]):
            continue
        nom = capter_nom(texte, m.end())
        if nom:
            out.append((f"{m.group(0)} {nom}", texte[max(0, m.start() - 60):m.start()], debut(texte, m.start())))
    for m in SANS_NOM.finditer(texte):
        if VOIE_AVANT.search(texte[max(0, m.start() - 30):m.start()]):
            continue
        nom = ""
        suite = re.match(r"\s+(?:de|d['’])\s*([A-ZÀ-Ý][\w-]+)", texte[m.end():m.end() + 40])
        if suite:
            nom = " de " + suite.group(1)
        out.append((m.group(0) + nom, texte[max(0, m.start() - 60):m.start()], debut(texte, m.start())))
    # « La Gare » sans nom (cartes postales MED_CP, MED_PBI…) : la gare de Roubaix. Pas « Alma-Gare »
    # (quartier), ni « chef de gare », ni la gare de marchandises ; une gare nommée (« gare de Lille »)
    # passe par TYPE_TEXTE.
    for m in GARE_SEULE.finditer(texte):
        avant = texte[max(0, m.start() - 30):m.start()]
        if VOIE_AVANT.search(avant) or re.search(r"(?i)(alma[- ]|chef\s+de\s+)$", avant):
            continue
        out.append((m.group(0), texte[max(0, m.start() - 60):m.start()], debut(texte, m.start())))
    return out


def debut(texte, i):
    """Le site ouvre-t-il le texte (« Roubaix - L'église Saint-Martin », « Hôtel de ville, façade ») ?"""
    avant = re.sub(r"(?i)^\s*(roubaix\s*(\(nord\))?\s*[-–:.,]?\s*)?", "", texte[:i])
    avant = re.sub(r"(?i)^\s*(\d+\s*[-–.]\s*)?(l['’]\s*|la\s+|le\s+|les\s+|vue\s+(de\s+l['’]|de\s+la\s+|du\s+|des\s+)?)?", "", avant)
    return avant.strip() == ""


def forme_corpname(v):
    k = cle(v)
    if famille(k) or re.search(r"\(roubaix", v, re.I):
        return v
    return ""


def lire_csv(f):
    with open(f, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def charger_points():
    points = []
    for fichier, statut in ((REFERENTIEL, "référentiel"), (SORTIE_ECARTES, "écarté")):
        for r in lire_csv(fichier):
            if r["thesaurus"] != "points":
                continue
            lib = r.get("libelle_manuel") or r["libelle"]
            k = cle_site(lib)
            w = r.get("coord_manuel") or r.get("coord_wkt") or ""
            points.append({"lieu_id": r["lieu_id"], "libelle": lib, "categorie": r["categorie"], "statut": statut,
                           "cle": k, "famille": famille(k) or famille(cle(r["categorie"])), "dist": distinctifs(k),
                           "point": l93(w), "emprise": l93(r.get("emprise_wkt") or "") or l93(w)})
    return points


def l93(w):
    if not w or not w.strip().upper().startswith(("POINT", "LINE", "MULTI", "POLYGON")):
        return None
    return transform(VERS_L93, wkt.loads(w))


def geometries_thesaurus(thesaurus):
    """libellé → géométrie Lambert 93, pour les lieux en service d'un thésaurus."""
    out = {}
    for r in lire_csv(REFERENTIEL):
        if r["thesaurus"] == thesaurus:
            g = l93(r.get("coord_manuel") or r.get("coord_wkt") or "")
            if g is not None:
                out[r.get("libelle_manuel") or r["libelle"]] = g
    return out


def type_de_lien(n, site, rues_geom):
    """Étapes 2-3 : (lien proposé, motif) pour un lien notice × site.

    sujet : le site est ce que montre ou décrit la notice ;
    lieu : le site accueille l'événement de la notice ;
    organisateur : le site (l'institution) organise, édite, imprime… ;
    lieu ou organisateur : affiche d'une institution, sans indice ;
    mention : simple citation dans le texte ;
    exclu : faux positif (autre ville, organisation qui n'est pas un bâtiment).
    """
    if n["contexte"] == "autre ville (forme)":
        return "exclu", "autre ville dans la forme"
    if re.match(ORGANISATIONS, n["site_cle"]):
        return "exclu", "organisation, pas un bâtiment"
    if n["role"]:
        return "organisateur", f"corpname role={n['role']}"
    # La rue indexée dans la notice confirme le site quand elle passe à côté.
    if site is not None and n["rues"]:
        dists = [(site.distance(rues_geom[r]), r) for r in n["rues"].split(" | ") if r in rues_geom]
        if dists:
            d, r = min(dists)
            n["distance_rue_m"] = round(d)
            if d <= DISTANCE_RUE_M:
                lien = "sujet" if n["ir"] in IR_IMAGES or n["champ"] == "subject" else "lieu"
                return lien, f"rue de la notice à {round(d)} m ({r})"
    if n["champ"] == "subject":
        return "sujet", "sujet RAMEAU"
    if n["champ"] in ("titre", "scope") and re.search(r"(?i)\b(propos[ée]e?s?|organis[ée]e?s?|pr[ée]sent[ée]e?s?|offert|produit|[ée]dit[ée]e?s?|imprim[ée]e?s?)\s+par\s+(la\s+|le\s+|l['’]\s*|les\s+)?$", n["avant"]):
        return "organisateur", "« … par » avant le site"
    if n["ir"] in IR_IMAGES and n["champ"] == "titre":
        return "sujet", f"titre d'une image ({n['ir']})"
    if n["champ"] in ("titre", "scope") and re.search(r"(?i)\b(à|au|aux|en|dans|devant|derrière|sur|salle)\s+(la\s+|le\s+|l['’]\s*|les\s+)?$", n["avant"]):
        return ("lieu", "« à / au / dans » avant le site") if n["ir"] in IR_EVENEMENTS else ("mention", "« à / au / dans » avant le site")
    if n["champ"] == "titre" and n["debut"]:
        return "sujet", "le titre commence par le site"
    if n["champ"] == "corpname" and n["ir"] in IR_EVENEMENTS:
        if re.match(INSTITUTIONS, n["site_cle"]):
            return "lieu ou organisateur", "institution sur une affiche, sans rue proche"
        return "lieu", "salle ou bâtiment sur une affiche"
    if n["champ"] == "corpname" and n["ir"] in IR_IMAGES:
        return "sujet", f"corpname d'une image ({n['ir']})"
    return "mention", "cité, rôle non déterminé"


def aligner(k, fam, points):
    exacts = [p for p in points if p["cle"] == k]
    if exacts:
        # Homonymes (« Hôpital Victor Provo » : l'hôpital et la station de tramway) : même famille d'abord.
        exacts.sort(key=lambda p: (p["statut"] != "référentiel", p["famille"] != fam or famille(cle(p["categorie"])) != fam))
        return "exact", exacts
    d = distinctifs(k)
    proches = [p for p in points if d and p["dist"] and p["famille"] == fam and (d <= p["dist"] or p["dist"] <= d)]
    # Site sans nom distinctif (hôtel de ville, gare…) : même famille, mot de type identique.
    if not d:
        proches = [p for p in points if not p["dist"] and p["cle"] == k]
    if proches:
        proches.sort(key=lambda p: (p["statut"] != "référentiel", famille(cle(p["categorie"])) != fam,
                                    abs(len(p["dist"]) - len(d))))
        return "approché", proches
    # Variantes connues : même site sous un autre nom (proposition, à valider comme les autres).
    if k in VARIANTES:
        memes = [p for p in points if p["cle"] == VARIANTES[k]]
        if memes:
            return "variante connue", memes
    return "aucune", []


def phrases(texte):
    return [p for p in re.split(r"(?<=[.;!?])\s+", texte) if p.strip()]


def phrase_du_site(ph, k, fam):
    """La phrase parle-t-elle du site ? Ses mots distinctifs, son mot de type ou « établissement / bâtiment /
    édifice » ; pas une phrase sur une société (adresse de l'expéditeur d'une lettre à en-tête)."""
    mots = set(cle(ph).split())
    d = distinctifs(k)
    lie = (d and d <= mots) or k.split(" ")[0] in mots or bool(mots & {"etablissement", "batiment", "edifice"})
    if fam != "industrie" and mots & {"societe", "entreprise", "fabrique", "firme", "magasin", "commerce", "ets",
                                       "etablissements", "maison"}:
        lie = False
    return lie


def adresses_dates(k, fam, titre, scope):
    """Adresses (numéro, rep, voie) et dates (« créée en 1895 ») des phrases du titre et de la note qui parlent
    du site."""
    adresses, dates = [], []
    for texte in (titre, scope):
        for ph in phrases(texte):
            if not phrase_du_site(ph, k, fam):
                continue
            for num, rep, voie in ADRESSE.findall(ph):
                voie = re.split(r"\s+(?:à|a|et|puis|en|dans|qui|où|ou|par|pour|avec|sur|entre|jusqu\S*|depuis|-)\s+",
                                voie.strip())[0].strip(" -")
                adresses.append((num, (rep or "").lower(), voie))
            dates += [f"{m.group(1).lower()} {m.group(2)}" for m in DATE.finditer(ph)]
    return adresses, dates


def charger_ban():
    """clé de voie → {(numéro, rep): (x, y) Lambert 93}."""
    ban = defaultdict(dict)
    with open(BAN_CSV, encoding="utf-8") as fh:
        for r in csv.DictReader(fh, delimiter=";"):
            ban[cle_voie(r["nom_voie"])][(r["numero"], r["rep"].lower())] = (float(r["x"]), float(r["y"]))
    return ban


def geocoder(adresse, ban):
    """(statut, Point L93 ou None) d'une adresse (numéro, rep, voie), par la BAN (numérotation actuelle)."""
    num, rep, voie = adresse
    numeros = ban.get(cle_voie(voie))
    if numeros is None:
        return "voie inconnue de la BAN", None
    xy = numeros.get((num, rep)) or numeros.get((num, ""))
    if xy is None:
        return "numéro absent de la BAN", None
    return "trouvée", Point(xy)


def villes_autres():
    """Clés des communes et pays hors Roubaix (nom sans qualificatif)."""
    out = set()
    for r in lire_csv(COMMUNES_PAYS):
        nom = re.sub(r"\s*\(.*\)\s*$", "", r["libelle"])
        k = cle(nom)
        if k and k != "roubaix" and len(k) > 3:
            out.add(k)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    a = ap.parse_args()
    sortie = join(GEO, f"propositions_sites_{a.date}.csv")
    sortie_notices = join(GEO, f"propositions_sites_{a.date}_notices.csv")

    points = charger_points()
    voies = {cle(r.get("libelle_manuel") or r["libelle"]): (r.get("libelle_manuel") or r["libelle"])
             for f in (REFERENTIEL, SORTIE_ECARTES) for r in lire_csv(f) if r["thesaurus"] == "rues"}
    voies_sans_type = defaultdict(list)
    for k, lib in voies.items():
        voies_sans_type[re.sub(TYPES_VOIE, "", k)].append(lib)
    quartiers = {cle_site(r.get("libelle_manuel") or r["libelle"]): (r.get("libelle_manuel") or r["libelle"])
                 for r in lire_csv(REFERENTIEL) if r["thesaurus"] == "quartiers"}
    autres = villes_autres()

    lignes_notices, sites = [], defaultdict(lambda: {"formes": Counter(), "champs": Counter(), "ir": Counter(),
                                                     "notices": set(), "contexte": Counter(), "personne": 0,
                                                     "exemple": ""})
    for f in sorted(glob.glob(IR_GLOB)):
        ir = re.sub(r"^FR\d+_", "", basename(f))[:-4]
        for c in etree.parse(f).iter("c"):
            did = c.find("did")
            if did is None:
                continue
            titre = txt(did.find("unittitle"))
            scope = " ".join(txt(x) for x in c.findall("scopecontent"))
            # Contexte géographique : geogname de la notice et de ses ancêtres.
            geo = [x for n in [c, *c.iterancestors("c")] for x in n.findall("controlaccess/geogname")]
            terr = {cle(re.sub(r"\s*\(.*\)\s*$", "", txt(x))) for x in geo if "territoires" in x.get("source", "")}
            rbx = any(re.search(r"rbx_|quartier|rue", x.get("source", "")) for x in geo) or "roubaix" in terr
            ctx_autre = bool(terr - {"roubaix", "nord", "france"}) and not rbx
            personnes = [cle(txt(x)) for x in c.findall("controlaccess/persname")]
            rues = " | ".join(txt(x) for x in c.findall("controlaccess/geogname") if "voies" in x.get("source", ""))

            trouves = []  # (forme, champ, role, texte avant, début de titre)
            for x in c.findall("controlaccess/corpname"):
                v = forme_corpname(txt(x))
                if v:
                    trouves.append((v, "corpname", x.get("role", ""), "", False))
            for x in c.findall("controlaccess/subject"):
                v = txt(x)
                # Sujet qui nomme un bâtiment : « Théâtre Louis Richard », « Musée Weerts (Roubaix, Nord) »,
                # pas un thème (« Manufacture et industrie textile », « Pièces de théâtre »).
                k_v = cle_site(v)
                debut_type = famille(k_v.split(" ")[0]) and re.search(r"\s[A-ZÀ-Ý]", v.split("(")[0].split("--")[0])
                if famille(k_v) and distinctifs(k_v) and (debut_type or re.search(r"roubaix", v, re.I)):
                    trouves.append((v, "subject", "", "", False))
            for champ, t in (("titre", titre), ("scope", scope)):
                for v, avant, deb in formes_texte(t):
                    trouves.append((v, champ, "", avant, deb))

            vus = set()
            for forme, champ, role, avant, deb in trouves:
                k = cle_site(forme)
                if not k or not famille(k) and champ != "corpname":
                    continue
                s = sites[k]
                s["formes"][forme] += 1
                s["champs"][champ] += 1
                if k in vus:
                    continue
                vus.add(k)
                s["notices"].add((ir, c.get("id")))
                s["ir"][ir] += 1
                d = distinctifs(k)
                if any(v in k.split() or re.search(rf"\b{re.escape(v)}\b", k) for v in autres if len(v.split()) <= 3) \
                        and not re.search(r"\(roubaix", forme, re.I):
                    contexte = "autre ville (forme)"
                elif ctx_autre:
                    contexte = "autre ville (notice)"
                elif rbx or re.search(r"roubaix", forme, re.I):
                    contexte = "Roubaix"
                else:
                    contexte = "?"
                s["contexte"][contexte] += 1
                if d and any(d <= set(p.split()) for p in personnes):
                    s["personne"] += 1
                if not s["exemple"]:
                    s["exemple"] = f"{ir} {c.get('id')} : {titre[:120]}"
                extrait = ""
                for t in (titre, scope):
                    i = cle(t).find(cle(forme))
                    if i >= 0:
                        extrait = t[:200]
                        break
                adresses, dates = adresses_dates(k, famille(k), titre, scope)
                lignes_notices.append({"ir": ir, "c_id": c.get("id"), "site_cle": k, "forme": forme, "champ": champ,
                                       "role": role, "contexte": contexte, "lien": "", "motif": "",
                                       "lieu_id": "", "rues": rues, "distance_rue_m": "", "avant": avant,
                                       "debut": deb, "adresses": adresses, "adresse": "", "adresse_ban": "",
                                       "distance_adresse_m": "", "dates": " | ".join(dates),
                                       "unittitle": titre[:300], "extrait": extrait})

    anciennes = {}
    if exists(sortie):
        anciennes = {r["site_cle"]: r for r in lire_csv(sortie)}

    rues_geom = geometries_thesaurus("rues")
    quartiers_geom = geometries_thesaurus("quartiers")
    ban = charger_ban()
    par_site = defaultdict(list)
    for n in lignes_notices:
        par_site[n["site_cle"]].append(n)

    lignes = []
    for k, s in sites.items():
        fam = famille(k)
        corresp, cands = aligner(k, fam, points)
        if s["contexte"]["autre ville (forme)"] == len(s["notices"]):
            corresp, cands = "autre ville", []  # « gare de Lille » : pas de point de Roubaix
        manuel = anciennes.get(k, {}).get("lieu_manuel", "").strip()
        decision = anciennes.get(k, {}).get("decision", "").strip()
        if manuel:
            choix = [p for p in points if manuel in (p["lieu_id"], p["libelle"])]
            corresp, cands = ("manuel", choix) if choix else ("manuel introuvable", [])
        elif decision == "non":
            corresp, cands = "refusé à la validation", []
        p0 = cands[0] if cands else {}
        # Étapes 2-3 : type de lien de chaque notice.
        liens = Counter()
        for n in par_site[k]:
            n["lien"], n["motif"] = type_de_lien(n, p0.get("emprise"), rues_geom)
            n["lieu_id"] = p0.get("lieu_id", "")
            if n["lieu_id"] in LIEU_SI_RUE and n["ir"] in IR_EVENEMENTS and n["lien"] in LIENS_RETENUS \
                    and not n["motif"].startswith("rue de la notice"):
                n["lien"], n["motif"] = "lieu ou organisateur", LIEU_SI_RUE[n["lieu_id"]]
            liens[n["lien"]] += 1
        # Adresses des titres et des notes, géocodées par la BAN.
        adresses, geoms, dates = Counter(), {}, Counter()
        for n in par_site[k]:
            dates.update(d for d in n["dates"].split(" | ") if d)
            for a in n["adresses"]:
                statut, g = geocoder(a, ban)
                texte = f"{a[0]}{(' ' + a[1]) if a[1] else ''} {a[2]}"
                if not n["adresse"] or (g is not None and n["adresse_ban"] != "trouvée"):
                    n["adresse"], n["adresse_ban"] = texte, statut
                    if g is not None and p0.get("emprise") is not None:
                        n["distance_adresse_m"] = round(p0["emprise"].distance(g))
                if g is not None and n["lien"] != "exclu":
                    adresses[texte] += 1
                    geoms[texte] = g
        adresse_note, distance_adresse, adresse_wkt, quartier_adresse = "", "", "", ""
        if adresses:
            adresse_note, nb = adresses.most_common(1)[0]
            g = geoms[adresse_note]
            adresse_wkt = transform(VERS_WGS84, g).wkt
            if p0.get("emprise") is not None:
                distance_adresse = round(p0["emprise"].distance(g))
            q = [lib for lib, qg in quartiers_geom.items() if qg.covers(g)]
            quartier_adresse = " | ".join(q) if q else "hors quartiers"
            adresse_note = f"{adresse_note} ({nb})" + "".join(f" | {a} ({c_})" for a, c_ in adresses.most_common(4)[1:])
        quartier = ""
        if p0.get("point") is not None:
            q = [lib for lib, g in quartiers_geom.items() if g.covers(p0["point"].centroid)]
            quartier = " | ".join(q) if q else "hors quartiers"
        retenus = sum(liens[x] for x in LIENS_RETENUS)
        if decision == "non" and not manuel:
            proposition = "refusé"
        elif decision in ("oui", "autre point") and p0:
            proposition = "validé"
        elif decision == "à revoir":
            proposition = "à revoir"
        elif not p0:
            proposition = "sans point" if corresp != "autre ville" else "exclu"
        elif retenus == 0:
            proposition = "exclu"
        elif corresp in ("exact", "variante connue", "manuel"):
            proposition = "sûr"
        else:
            proposition = "probable"
        alertes = []
        sans_type = re.sub(r"^\S+ ", "", k)
        homonymes = voies_sans_type.get(sans_type, []) + ([voies[k]] if k in voies else [])
        if homonymes:
            alertes.append("voie homonyme : " + " | ".join(sorted(set(homonymes))))
        d = " ".join(sorted(distinctifs(k)))
        for qk, ql in quartiers.items():
            if qk and set(qk.split()) <= distinctifs(k):
                alertes.append(f"nom de quartier : {ql}")
        if s["personne"]:
            alertes.append(f"personne indexée dans {s['personne']} notice(s)")
        nb_autre = sum(n for c_, n in s["contexte"].items() if c_.startswith("autre"))
        if nb_autre:
            alertes.append(f"autre ville : {nb_autre} notice(s)")
        if distance_adresse != "" and distance_adresse > DISTANCE_ADRESSE_M:
            alertes.append(f"adresse de la note à {distance_adresse} m du point")
        if adresse_wkt and not p0:
            g = geoms[adresses.most_common(1)[0][0]]
            proches = sorted((p["emprise"].distance(g), p["libelle"]) for p in points if p["emprise"] is not None
                             and p["emprise"].distance(g) <= 30)
            alertes.append("pas de point, mais une adresse dans les notes"
                           + (f" ; point à l'adresse : {proches[0][1]} ({round(proches[0][0])} m)" if proches else ""))
        ligne = {
            "site_cle": k, "famille": fam or "(corpname Roubaix)", "forme": s["formes"].most_common(1)[0][0],
            "nb_notices": len(s["notices"]), "proposition": proposition, "nb_retenus": retenus,
            "liens": " | ".join(f"{x} {c_}" for x, c_ in liens.most_common()),
            "quartier_point": quartier,
            "adresse_note": adresse_note, "distance_adresse_m": distance_adresse,
            "quartier_adresse": quartier_adresse, "adresse_wkt": adresse_wkt,
            "dates_note": " | ".join(f"{d_} ({c_})" for d_, c_ in dates.most_common(4)),
            "nb_corpname": s["champs"]["corpname"], "nb_subject": s["champs"]["subject"],
            "nb_titre": s["champs"]["titre"], "nb_scope": s["champs"]["scope"],
            "contexte": " | ".join(f"{c_} {n}" for c_, n in s["contexte"].most_common()),
            "ir": " | ".join(f"{i} {n}" for i, n in s["ir"].most_common()),
            "correspondance": corresp, "lieu_id_propose": p0.get("lieu_id", ""),
            "libelle_propose": p0.get("libelle", ""), "statut_lieu": p0.get("statut", ""),
            "autres_lieux": " | ".join(f"{p['libelle']} ({p['statut']})" for p in cands[1:6]),
            "alertes": " ; ".join(alertes),
            "variantes": " | ".join(f"{v} ({n})" for v, n in s["formes"].most_common(8)),
            "exemple": s["exemple"],
        }
        old = anciennes.get(k, {})
        for m in MANUELLES:
            ligne[m] = old.get(m, "")
        lignes.append(ligne)
    lignes.sort(key=lambda r: (-r["nb_notices"], r["site_cle"]))

    with open(sortie, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=list(lignes[0]))
        w.writeheader()
        w.writerows(lignes)
    for n in lignes_notices:
        n["adresses"] = " | ".join(f"{a[0]}{(' ' + a[1]) if a[1] else ''} {a[2]}" for a in n["adresses"])
    with open(sortie_notices, "w", newline="", encoding="utf-8") as fo:
        w = csv.DictWriter(fo, fieldnames=list(lignes_notices[0]))
        w.writeheader()
        w.writerows(lignes_notices)

    print(f"{len(lignes)} sites candidats, {len(lignes_notices)} liens notice × site → {sortie}")
    print("correspondance :", dict(Counter(r["correspondance"] for r in lignes)))
    print("proposition :", dict(Counter(r["proposition"] for r in lignes)))
    print("liens :", dict(Counter(n["lien"] for n in lignes_notices)))
    print("notices avec un lien retenu (sujet/lieu) vers un point :",
          len({(n["ir"], n["c_id"]) for n in lignes_notices if n["lien"] in LIENS_RETENUS and n["lieu_id"]}))
    avec_point = {r["site_cle"] for r in lignes if r["lieu_id_propose"]}
    print("notices liées à un point proposé :",
          len({(n["ir"], n["c_id"]) for n in lignes_notices if n["site_cle"] in avec_point}))


if __name__ == "__main__":
    main()
