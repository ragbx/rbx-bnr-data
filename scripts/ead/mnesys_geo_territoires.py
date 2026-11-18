"""
<geogname> de communes et de pays (« Hazebrouck (Nord) », « Tournai (Belgique) »…) des inventaires Mnesys
(--serie fi : séries Fi ; --serie mnesys : tous les IR) que le thésaurus des territoires
(data/geo/referentiel_communes_pays.csv) ne connaît pas — propositions de concepts à créer (lecture seule).
Piste E1 du diagnostic fi_geogname_alignement.py.

Formes retenues : celles que le référentiel n'aligne pas (statut « hors référentiel » ou « lieu écarté »), de nature
« commune ou pays » (mnesys_geo_sites.nature : « Nom (précision) » avec la précision entre parenthèses), absentes des
propositions déjà produites par les autres familles. Les lignes que les fichiers « sites » ont listées sans proposition
pour ces communes (fi_sites : 64 formes) sont reprises ici (lignes inertes, voir mnesys_geo_commun.formes_exclues).

Pour chaque forme, on extrait le nom et la précision, puis :

  - précision = département français (fautes tolérées : « Bouche-du-Rhône ») : commune cherchée dans la liste des
    communes du département (geo.api.gouv.fr, une requête par département) ; lieu_id = code INSEE ; libellé RAMEAU
    « Nom (Département) » avec le nom officiel ; parent « Département (France) » ; point = centre de la commune. Les
    anciens départements (Seine, Seine-et-Oise) sont cherchés dans les départements actuels. Nom identique : haute ;
    nom approché (faute de saisie), incomplet (« Arleux » → Arleux-en-Gohelle) ou complété : moyenne ;
  - précision = pays (Belgique, Allemagne, Italie, Angleterre, Grèce, Portugal, Canada, États-Unis, Algérie…) : lieu
    cherché dans Wikidata, restreint au pays (propriété P17, avec un point P625 ; gares, cimetières, sections de commune
    écartés ou classés après les villes) ; lieu_id = identifiant Wikidata (QID) ; libellé « Nom (Pays) » avec le nom
    français de Wikidata (à défaut anglais ou « mul ») ; parent = le pays ; point = coordonnées Wikidata ;
  - pas de précision (« Hem », « Verviers ») : seule commune française de ce nom, sinon Wikidata (pays connus) si un
    seul lieu porte ce nom ; toujours « manuelle » ;
  - sinon (continent, ancien État, précision inconnue) : aucune proposition.

Les conventions de la concordance BnR du 2026-10-29 sont appliquées par la table MANUELLES : commune disparue ou
quartier rattaché à la commune actuelle (Lomme → Lille, Malo-les-Bains → Dunkerque…), toujours « manuelle ». Quelques
fautes de saisie évidentes sont corrigées par CORRECTIONS_NOM (« Münchengladbach »…), signalées dans le motif.

Colonnes : methode (« automatique » : nom identique à celui de la source, département ou pays confirmé, une seule
candidate ; « manuelle » : le reste ; « aucune »), confiance (haute / moyenne), motif, lieu_propose, lieu_id,
categorie, parent, parent_au_referentiel (« non » : le département ou le pays est à créer aussi),
insee, wikidata, coord_wkt, source_donnees, concept_au_referentiel (« oui » : le concept existe déjà, la forme est une
variante à rattacher et mnesys_geo_applique.py peut l'appliquer tout de suite ; « non » : concept à créer, la
forme reste « en attente » jusqu'à sa création), nb_formes_concept (nombre de formes du fichier qui visent le même
concept), autres_candidats, remplace_source, decision, remarque.

Règle d'application : celle des autres familles (mnesys_geo_applique.py). Un concept à créer ne peut pas être appliqué :
la création dans referentiel_communes_pays.csv est une étape à part (sauvegarde, accord de l'utilisateur).

Les réponses utiles des deux services (communes par département, résumé des entités Wikidata retenues) sont gardées
dans results/ead/indexation/<série>_territoires_sources_<date>.json : relancé avec le même fichier, le script ne
contacte plus personne (--hors-ligne l'interdit). Seuls des noms de communes et de pays sont envoyés.

Sortie : results/ead/indexation/<série>_territoires_propositions_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après les autres familles) :
    python scripts/ead/mnesys_geo_territoires.py [--serie fi|mnesys] [--date AAAAMMJJ] [--hors-ligne]
"""
import argparse
import csv
import json
import re
import sys
import time
import unicodedata
from collections import Counter
from datetime import date
from difflib import SequenceMatcher
from os.path import dirname, exists, join
from urllib.error import URLError
from urllib.parse import unquote_plus, urlencode
from urllib.request import Request, urlopen

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import (  # noqa: E402
    INDEXATION, SERIES, chemin_propositions, formes_exclues, lire_formes, propositions_a_exclure,
)
from mnesys_geo_sites import nature  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, COMMUNES_PAYS, REFERENTIEL, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

GEO_API = "https://geo.api.gouv.fr"
WIKIDATA = "https://www.wikidata.org/w/api.php"
AGENT = "recherche-communes/1.0"
PARSE = re.compile(r"^(?P<nom>.+?)\s*,?\s*\((?P<prec>[^()]+)\)\s*[.,;]?\s*$")
ARTICLE = re.compile(r"^(le|la|les|l)\s+")
# description Wikidata : ville ou commune (0), lieu habité secondaire (1), ce qui n'en est pas (écarté) ; le reste (2) est
# une région ou un pays
FORT = re.compile(r"\b(city|ville|commune|municipality|municipalit[ée]|capital|capitale|chef-lieu)\b", re.I)
FAIBLE = re.compile(r"section|district|village|localit|établissement|settlement|ancienne|borough|town|bourg|quartier|"
                    r"portuaire|harbou?r|authority|\barea\b|\bzone\b|\bîle\b|island|archipel", re.I)
# « town in Ghent municipality » : ville ou section d'une commune dont l'entité principale est un autre élément
SECONDAIRE = re.compile(r"\b(?:town|city|village|settlement|locality|localit[ée])\b.*\b(?:in|de|du)\b.*\b(?:municipality|commune)\b", re.I)
NON_VILLE = re.compile(r"gare|station|cimeti|a[ée]roport|airport|[ée]glise|church|cath[ée]drale|cathedral|club|m[ée]tro|"
                       r"plan[èe]te|asteroid|minor planet|constituency|circonscription|dragueur|danse|monument|ch[âa]teau|"
                       r"canal|rivi[èe]re|river|football|stade|parlement|parliament|arrondissement administratif|"
                       r"ligne|route|famille|family", re.I)
SEUIL = 0.85
INCLUSION = 0.88      # les mots de l'un sont inclus dans ceux de l'autre (nom incomplet ou complété)
# ancien département -> départements actuels (codes) qui peuvent contenir la commune
ANCIENS = {"seine": ["75", "92", "93", "94"], "seine et oise": ["78", "91", "95", "92", "93", "94"]}
# pays -> (libellé du thésaurus, identifiants Wikidata acceptés pour la propriété « pays »)
PAYS = {
    "belgique": ("Belgique", {"Q31"}), "allemagne": ("Allemagne", {"Q183"}), "italie": ("Italie", {"Q38"}),
    "angleterre": ("Angleterre", {"Q145", "Q21"}), "grece": ("Grèce", {"Q41"}), "portugal": ("Portugal", {"Q45"}),
    "canada": ("Canada", {"Q16"}), "etats unis": ("États-Unis", {"Q30"}), "pays bas": ("Pays-Bas", {"Q55"}),
    "suisse": ("Suisse", {"Q39"}), "espagne": ("Espagne", {"Q29"}), "luxembourg": ("Luxembourg", {"Q32"}),
    "algerie": ("Algérie", {"Q262"}), "tunisie": ("Tunisie", {"Q948"}), "hollande": ("Pays-Bas", {"Q55"}),
    "usa": ("États-Unis", {"Q30"}), "macedoine": ("Macédoine du Nord", {"Q221"}),
}
PAYS_DE = {q: v[0] for v in PAYS.values() for q in v[1]}   # QID -> libellé du pays
# fautes de saisie évidentes dans le nom d'un lieu étranger : forme -> nom cherché
CORRECTIONS_NOM = {"Münchengladbach": "Mönchengladbach", "Middelkerque": "Middelkerke", "Dieghem": "Diegem",
                   "Gendbrugge Lez Gand": "Gentbrugge", "Uccles": "Uccle"}
# conventions de la concordance BnR (2026-10-29) : commune disparue, quartier ou lieu rattaché à la commune actuelle
# forme -> (nom de la commune actuelle, code du département, motif)
MANUELLES = {
    "Lomme (Nord)": ("Lille", "59", "commune associée à Lille : rattachée à la commune actuelle (convention du 2026-10-29)"),
    "Malo-les-Bains (Nord)": ("Dunkerque", "59", "Malo-les-Bains est rattachée à Dunkerque (convention du 2026-10-29)"),
    "Dunkerque-Malo-les-Bains (Nord)": ("Dunkerque", "59", "Malo-les-Bains est rattachée à Dunkerque (convention du 2026-10-29)"),
    "Mézières (Ardennes)": ("Charleville-Mézières", "08", "commune fusionnée en 1966 : rattachée à la commune actuelle"),
    "Lille Five, (Nord)": ("Lille", "59", "quartier de Lille (Fives) : rattaché à la commune"),
    "Lille Saint Maurice Pellevoisin, (Nord)": ("Lille", "59", "quartier de Lille : rattaché à la commune"),
    "Mont Cassel (Nord)": ("Cassel", "59", "le mont Cassel est un lieu de la commune de Cassel"),
    "Hardelot (Pas-de-Calais)": ("Neufchâtel-Hardelot", "62", "Hardelot est une station de Neufchâtel-Hardelot"),
    "Le Touquet (Pas-de-Calais)": ("Le Touquet-Paris-Plage", "62", "nom complet de la commune"),
    "Saint-Pol-sur-Mer (Nord)": ("Dunkerque", "59", "Saint-Pol-sur-Mer est rattachée à Dunkerque depuis 2010 (convention du 2026-10-29)"),
    "Loos-lez-Lille (Nord)": ("Loos", "59", "nom officiel : Loos"),
    "Etaples-sur-Mer (Pas-de-Calais)": ("Étaples", "62", "nom officiel : Étaples"),
    "Troyes, (Aude)": ("Troyes", "10", "département corrigé : Troyes est dans l'Aube, non dans l'Aude"),
    "Kremlin-Bicêtre (Seine)": ("Le Kremlin-Bicêtre", "94", "ancien département de la Seine : commune actuelle du Val-de-Marne"),
}
# forme -> (QID Wikidata, libellé, motif)
MANUELLES_ETRANGER = {
    "Monaco, (Principauté de Monaco)": ("Q235", "Monaco", "la Principauté de Monaco est l'État de Monaco"),
}
ORDRE_METHODE = {"manuelle": 0, "aucune": 1, "automatique": 2}
ORDRE_CONFIANCE = {"moyenne": 0, "haute": 1, "": 2}
COLONNES = [
    "forme", "occurrences", "ir", "sources_mnesys", "nature", "methode", "confiance", "motif", "nom_extrait", "precision",
    "lieu_propose", "lieu_id", "categorie", "parent", "parent_au_referentiel", "insee", "wikidata", "coord_wkt",
    "source_donnees", "concept_au_referentiel", "nb_formes_concept", "autres_candidats", "remplace_source",
    "decision", "remarque",
]


def norm(texte):
    """Sans accents, ligatures, casse ni ponctuation ; articles initiaux retirés."""
    texte = texte.replace("œ", "oe").replace("Œ", "Oe").replace("æ", "ae")
    s = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode().lower()
    return ARTICLE.sub("", " ".join(re.sub(r"[^a-z0-9]+", " ", s).split()))


def ressemblance(a, b):
    return SequenceMatcher(None, norm(a), norm(b)).ratio()


class Sources:
    """Appels aux deux services, avec cache JSON réduit à ce qui sert (trace, et rejeu sans réseau)."""

    def __init__(self, chemin, hors_ligne):
        self.chemin, self.hors_ligne = chemin, hors_ligne
        self.cache = json.load(open(chemin, encoding="utf-8")) if exists(chemin) else {}
        self._convertir_ancien_cache()

    def _convertir_ancien_cache(self):
        """Ancien format (réponses brutes, clés = URL) : on garde les recherches Wikidata, utiles et sans surcoût."""
        for cle in [k for k in self.cache if k.startswith("http")]:
            valeur = self.cache.pop(cle)
            if "wbsearchentities" in cle and isinstance(valeur, dict):
                params = dict(re.findall(r"[?&]([a-z]+)=([^&]*)", cle))
                self.cache.setdefault(f"wd:recherche:{params['language']}:{unquote_plus(params['search'])}",
                                      [r["id"] for r in valeur.get("search", [])])

    def _appel(self, url, **params):
        if self.hors_ligne:
            self.enregistrer()
            sys.exit(f"Absent du cache (--hors-ligne) : {url} {params}")
        cle = url + "?" + urlencode(sorted(params.items()))
        pause = 1.2 if url.startswith(WIKIDATA) else 0.15   # Wikidata limite le débit (HTTP 429)
        for essai in range(6):
            try:
                time.sleep(pause)
                with urlopen(Request(cle, headers={"User-Agent": AGENT}), timeout=30) as r:
                    return json.load(r)
            except (URLError, TimeoutError) as e:
                if essai == 5:
                    self.enregistrer()
                    sys.exit(f"Service injoignable ({e}) : {cle}")
                retry = getattr(e, "headers", None)
                time.sleep(max(int((retry.get("Retry-After") if retry else 0) or 0), 10 * (essai + 1)))

    def departements(self):
        if "geo:departements" not in self.cache:
            self.cache["geo:departements"] = self._appel(f"{GEO_API}/departements", fields="nom,code")
        return self.cache["geo:departements"]

    @staticmethod
    def _commune(c):
        return {"nom": c["nom"], "code": c["code"], "dep": c["codeDepartement"],
                "point": [round(v, 4) for v in c["centre"]["coordinates"]]}

    def communes_departement(self, code):
        cle = f"geo:communes:{code}"
        if cle not in self.cache:
            self.cache[cle] = [self._commune(c) for c in self._appel(
                f"{GEO_API}/communes", codeDepartement=code, fields="nom,code,codeDepartement,centre")
                if c.get("centre")]
        return self.cache[cle]

    def communes_nom(self, nom):
        cle = f"geo:nom:{nom}"
        if cle not in self.cache:
            self.cache[cle] = [self._commune(c) for c in self._appel(
                f"{GEO_API}/communes", nom=nom, limit="8", fields="nom,code,codeDepartement,centre") if c.get("centre")]
        return self.cache[cle]

    def wd_recherche(self, nom, langue):
        cle = f"wd:recherche:{langue}:{nom}"
        if cle not in self.cache:
            r = self._appel(WIKIDATA, action="wbsearchentities", search=nom, language=langue, uselang=langue,
                            type="item", limit="10", format="json")
            self.cache[cle] = [x["id"] for x in r.get("search", [])]
        return self.cache[cle]

    def wd_entites(self, ids):
        """QID -> résumé (libellés fr, en, mul ; description ; pays P17 ; point P625)."""
        manquants = [i for i in ids if f"wd:entite:{i}" not in self.cache]
        for debut in range(0, len(manquants), 40):
            lot = manquants[debut:debut + 40]
            r = self._appel(WIKIDATA, action="wbgetentities", ids="|".join(lot), props="claims|labels|descriptions",
                            languages="fr|en|mul", format="json")["entities"]
            for i in lot:
                e = r.get(i, {})
                valeurs = lambda p: [c["mainsnak"]["datavalue"]["value"] for c in e.get("claims", {}).get(p, [])
                                     if "datavalue" in c["mainsnak"]]
                points = valeurs("P625")
                lab, des = e.get("labels", {}), e.get("descriptions", {})
                self.cache[f"wd:entite:{i}"] = {
                    "fr": lab.get("fr", {}).get("value", ""), "en": lab.get("en", {}).get("value", ""),
                    "mul": lab.get("mul", {}).get("value", ""),
                    "desc": (des.get("fr") or des.get("en") or {}).get("value", ""),
                    "pays": [v["id"] for v in valeurs("P17")],
                    "point": [round(points[0]["longitude"], 4), round(points[0]["latitude"], 4)] if points else None}
        return {i: self.cache[f"wd:entite:{i}"] for i in ids}

    def enregistrer(self):
        with open(self.chemin, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def departement(precision, departements):
    """(département, corrigé ?) pour une précision, ou (None, False)."""
    c = norm(precision)
    for d in departements:
        if norm(d["nom"]) == c:
            return d, False
    meilleur = max(departements, key=lambda d: ressemblance(precision, d["nom"]))
    return (meilleur, True) if ressemblance(precision, meilleur["nom"]) >= 0.88 else (None, False)


def score_commune(nom, officiel):
    """Ressemblance entre un nom saisi et un nom officiel ; INCLUSION si les mots de l'un sont inclus dans l'autre."""
    s = ressemblance(nom, officiel)
    a, b = set(norm(nom).split()), set(norm(officiel).split())
    return max(s, INCLUSION) if s < 1 and a and b and (a < b or b < a) else s


def communes_candidates(src, nom, codes_departements):
    """(score, commune) des communes des départements, de la meilleure à la moins bonne (6 au plus)."""
    res = {}
    for code in codes_departements:
        for c in src.communes_departement(code):
            res[c["code"]] = (score_commune(nom, c["nom"]), c)
    return sorted(res.values(), key=lambda x: -x[0])[:6]


def lieux_wikidata(src, nom, qids_pays, connus=()):
    """Lieux Wikidata du ou des pays demandés : (score, classe, rang, résumé), du meilleur au moins bon.
    classe 0 : ville ou commune ; 1 : lieu habité secondaire (section, district…) ; 2 : autre (région, pays…).
    À score égal, un concept déjà au thésaurus des territoires (QID de `connus`) passe devant."""
    ids = []
    for langue in ("fr", "en"):
        ids += [i for i in src.wd_recherche(nom, langue) if i not in ids]
    if not ids:
        return []
    res = []
    for rang, (i, e) in enumerate(src.wd_entites(ids).items()):
        if not (set(e["pays"]) & qids_pays) or not e["point"] or NON_VILLE.search(e["desc"]):
            continue
        etiquette = e["fr"] or e["en"] or e["mul"]
        classe = (1 if SECONDAIRE.search(e["desc"]) else 0 if FORT.search(e["desc"]) else
                  1 if FAIBLE.search(e["desc"]) else 2)
        res.append((ressemblance(nom, etiquette), classe, rang, {**e, "qid": i, "etiquette": etiquette, "classe": classe}))
    return sorted(res, key=lambda x: (-round(x[0], 2), x[3]["qid"] not in connus, x[1], x[2]))


def point(lon, lat):
    return f"POINT ({round(lon, 4)} {round(lat, 4)})"


def proposer_commune(ligne, c, dep_nom, notes, haute):
    ligne.update(
        methode="automatique" if haute else "manuelle", confiance="haute" if haute else "moyenne",
        motif="; ".join(notes) or "même nom", lieu_propose=f"{c['nom']} ({dep_nom})", lieu_id=c["code"],
        categorie="commune", parent=f"{dep_nom} (France)", insee=c["code"], coord_wkt=point(*c["point"]),
        source_donnees="geo.api.gouv.fr")


def proposer_etranger(ligne, w, libelle_pays, notes, haute):
    ligne.update(
        methode="automatique" if haute else "manuelle", confiance="haute" if haute else "moyenne",
        motif="; ".join(notes) or "même nom", lieu_propose=f"{w['etiquette']} ({libelle_pays})", lieu_id=w["qid"],
        categorie="ville étrangère" if w["classe"] < 2 else "région ou pays (à vérifier)", parent=libelle_pays,
        wikidata=w["qid"], coord_wkt=point(*w["point"]), source_donnees="Wikidata")


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR (défaut : fi)")
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    p.add_argument("--hors-ligne", action="store_true", help="n'interroge aucun service : réponses du cache seulement")
    p.add_argument("--exclure", nargs="*", help="CSV de propositions dont les formes sont exclues "
                   "(défaut : toutes les autres familles, séries précédentes comprises)")
    args = p.parse_args()
    sortie = chemin_propositions(args.serie, "territoires", args.date)
    src = Sources(join(INDEXATION, f"{args.serie}_territoires_sources_{args.date}.json"), args.hors_ligne)
    exclure = args.exclure if args.exclure is not None else propositions_a_exclure(args.serie, "territoires")
    exclues = formes_exclues(exclure)

    formes = lire_formes(args.serie)
    lieux, ecartes, territoires = lire(REFERENTIEL), lire(SORTIE_ECARTES), lire(COMMUNES_PAYS)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    par_id = {l["lieu_id"]: l for l in territoires}
    libelles = {norm(l["libelle"]): l for l in territoires}
    departements = src.departements()
    nom_dep = {d["code"]: d["nom"] for d in departements}
    tous_pays = set().union(*(v[1] for v in PAYS.values()))

    rows = []
    try:
        for forme, d in formes.items():
            if forme in exclues:
                continue
            r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
            if r["statut"] not in ("hors référentiel", "lieu écarté") or nature(forme) != "commune ou pays":
                continue
            m = PARSE.match(forme.strip())
            nom, precision = (m.group("nom").strip(" ,"), m.group("prec").strip()) if m else (forme.strip(" ,."), "")
            ligne = {c: "" for c in COLONNES}
            ligne.update(
                forme=forme, occurrences=d["occ"],
                ir=", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
                sources_mnesys=", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
                nom_extrait=nom, precision=precision, methode="aucune")
            dep, corrige = departement(precision, departements) if precision else (None, False)
            pays = PAYS.get(norm(precision)) if precision else None
            corrige_pays = False
            if precision and not dep and pays is None and norm(precision) not in ANCIENS:   # « Allemangne »
                meilleur = max(PAYS, key=lambda k: ressemblance(precision, k))
                if ressemblance(precision, meilleur) >= 0.88:
                    pays, corrige_pays = PAYS[meilleur], True
            autres = []

            if forme in MANUELLES_ETRANGER:
                qid, libelle, motif = MANUELLES_ETRANGER[forme]
                w = src.wd_entites([qid])[qid]
                ligne.update(methode="manuelle", confiance="moyenne", motif=motif, lieu_propose=libelle, lieu_id=qid,
                             categorie="pays", wikidata=qid, coord_wkt=point(*w["point"]), source_donnees="Wikidata")
            elif dep or norm(precision) in ANCIENS:
                codes = [dep["code"]] if dep else ANCIENS[norm(precision)]
                nom_cherche, motif_manuel = (nom, "") if forme not in MANUELLES else (MANUELLES[forme][0], MANUELLES[forme][2])
                if forme in MANUELLES:
                    codes = [MANUELLES[forme][1]]
                candidates = communes_candidates(src, nom_cherche, codes)
                if candidates and candidates[0][0] >= SEUIL:
                    score, c = candidates[0]
                    ex_aequo = [x for x in candidates[1:] if x[0] >= score - 0.02]
                    notes = [motif_manuel] if motif_manuel else []
                    if ex_aequo:
                        ligne["motif"] = ("plusieurs communes possibles : " + " ; ".join(
                            f"{x[1]['nom']}" for x in [candidates[0]] + ex_aequo) + " (voir autres_candidats)")
                        candidates = candidates
                    else:
                        exact = norm(nom_cherche) == norm(c["nom"])
                        if corrige:
                            notes.append(f"département corrigé : « {precision} » → {nom_dep[c['dep']]}")
                        if not dep:
                            notes.append(f"ancien département « {precision} » : commune du {nom_dep[c['dep']]}")
                        if not exact:
                            notes.append(f"nom {'approché' if score > INCLUSION else 'incomplet ou complété'} : "
                                         f"« {nom_cherche} » → « {c['nom']} » ({score:.2f})")
                        proposer_commune(ligne, c, nom_dep[c["dep"]], notes,
                                         haute=exact and not motif_manuel and not corrige and bool(dep))
                else:
                    ligne["motif"] = ("aucune commune proche dans le département" if not candidates else
                                      "candidates trop éloignées (voir autres_candidats)")
                if ligne["methode"] != "automatique":   # la même commune ailleurs en France (« Arleux » du Nord)
                    vus = {x[1]["code"] for x in candidates}
                    candidates += [(score_commune(nom_cherche, c["nom"]), c) for c in src.communes_nom(nom_cherche)
                                   if c["code"] not in vus]
                autres = [f"{c['nom']} ({nom_dep[c['dep']]}) (id {c['code']} ; {s:.2f} ; {point(*c['point'])})"
                          for s, c in candidates if c["code"] != ligne["lieu_id"]]
            elif pays or not precision:
                nom_cherche = CORRECTIONS_NOM.get(nom, nom)
                qids = pays[1] if pays else tous_pays
                if not precision:   # sans précision : une commune française de ce nom, sinon un lieu étranger
                    exactes = [c for c in src.communes_nom(nom) if norm(c["nom"]) == norm(nom)]
                    if len(exactes) == 1:
                        c = exactes[0]
                        proposer_commune(ligne, c, nom_dep[c["dep"]],
                                         ["forme sans précision : seule commune française de ce nom"], haute=False)
                        qids = set()
                candidates = lieux_wikidata(src, nom_cherche, qids, set(par_id)) if qids else []
                if candidates and not ligne["lieu_id"]:
                    score, classe, _, w = candidates[0]
                    voisines = [x for x in candidates[1:] if x[0] >= score - 0.05 and x[1] == classe]
                    if w["qid"] in par_id:   # concept déjà validé au thésaurus : pas d'hésitation
                        voisines = []
                    libelle_pays = pays[0] if pays else next((PAYS_DE[q] for q in w["pays"] if q in PAYS_DE), "")
                    exact = score >= SEUIL and norm(nom_cherche) == norm(w["etiquette"])
                    notes = []
                    if nom_cherche != nom:
                        notes.append(f"orthographe corrigée : « {nom} » → « {nom_cherche} »")
                    if corrige_pays:
                        notes.append(f"pays corrigé : « {precision} » → {libelle_pays}")
                    if not precision:
                        notes.append("forme sans précision : seul lieu de ce nom dans les pays connus")
                    if not exact:
                        notes.append(f"nom différent dans Wikidata : « {w['etiquette']} » ({score:.2f})")
                    if voisines:
                        notes.append("plusieurs lieux de nom voisin")
                    if classe == 2:
                        notes.append(f"pas une ville d'après Wikidata ({w['desc']}) : région ou pays à vérifier")
                    if (score >= SEUIL or len(candidates) == 1) and libelle_pays and not (not precision and voisines):
                        proposer_etranger(ligne, w, libelle_pays, notes, haute=(
                            exact and not voisines and classe == 0 and not corrige_pays and bool(pays)
                            and nom_cherche == nom))
                    else:
                        ligne["motif"] = ("plusieurs lieux de ce nom (voir autres_candidats)" if score >= SEUIL else
                                          "candidates trop éloignées (voir autres_candidats)")
                elif not ligne["lieu_id"]:
                    ligne["motif"] = ("forme sans précision entre parenthèses" if not precision else
                                      f"aucun lieu de ce nom dans Wikidata pour « {pays[0]} »")
                autres = [f"{w['etiquette']} (id {w['qid']} ; {s:.2f} ; {w['desc']} ; {point(*w['point'])})"
                          for s, _, _, w in candidates if w["qid"] != ligne["lieu_id"]]
            else:
                ligne["motif"] = "précision inconnue (ni département ni pays)"
            ligne["autres_candidats"] = " | ".join(autres[:8])

            if ligne["lieu_id"]:
                existant = par_id.get(ligne["lieu_id"]) or libelles.get(norm(ligne["lieu_propose"]))
                if existant:   # le concept existe : variante à rattacher, applicable tout de suite
                    ligne["lieu_id"], ligne["lieu_propose"] = existant["lieu_id"], existant["libelle"]
                ligne["concept_au_referentiel"] = "oui" if existant else "non"
                ligne["parent_au_referentiel"] = "oui" if (not ligne["parent"] or norm(ligne["parent"]) in libelles) else "non"
                ligne["remplace_source"] = "oui" if any(s != "(aucune)" for s in d["src"]) else ""
            rows.append(ligne)
            if len(rows) % 25 == 0:
                print(f"  {len(rows)} formes traitées…", file=sys.stderr, flush=True)
    finally:
        src.enregistrer()

    groupes = Counter(r["lieu_id"] for r in rows if r["lieu_id"])
    for r in rows:
        r["nb_formes_concept"] = groupes[r["lieu_id"]] if r["lieu_id"] else ""
    rows.sort(key=lambda r: (ORDRE_METHODE[r["methode"]], ORDRE_CONFIANCE[r["confiance"]], -r["occurrences"], r["forme"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot = sum(d["occ"] for d in formes.values())
    print(f"{len(rows)} formes ({sum(r['occurrences'] for r in rows)} geogname) sur {len(formes)} formes, "
          f"{tot} geogname → {sortie}")
    for m in ORDRE_METHODE:
        for c in ORDRE_CONFIANCE:
            rs = [r for r in rows if r["methode"] == m and r["confiance"] == c]
            if rs:
                print(f"  {m:11s} {c or '-':8s} {len(rs):3d} formes  {sum(r['occurrences'] for r in rs):4d} occ")
    prop = [r for r in rows if r["lieu_id"]]
    nouveaux = {r["lieu_id"]: r for r in prop if r["concept_au_referentiel"] == "non"}
    print(f"  {len(prop)} formes avec un lieu → {len(set(r['lieu_id'] for r in prop))} concepts, dont "
          f"{len(nouveaux)} à créer ({sum(r['occurrences'] for r in prop if r['concept_au_referentiel'] == 'non')} occ) ; "
          f"{sum(1 for r in prop if r['concept_au_referentiel'] == 'oui')} formes sur un concept existant")
    parents = {r["parent"] for r in nouveaux.values() if r["parent_au_referentiel"] == "non"}
    print(f"  parents à créer ({len(parents)}) :", ", ".join(sorted(parents)))
    print("  catégories des concepts à créer :", dict(Counter(r["categorie"] for r in nouveaux.values())))
    print(f"  réponses gardées dans {src.chemin}")


if __name__ == "__main__":
    main()
