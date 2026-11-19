"""
<geogname> de sites (bâtiments, monuments, courées…) des inventaires Mnesys (--serie fi : séries Fi, data/ead/mnesys/FRAM59100_*Fi*.xml ; --serie mnesys : tous les IR)
que le référentiel des lieux n'aligne pas (statut « hors référentiel »
ou « lieu écarté ») — propositions (lecture seule). Piste E2 du diagnostic fi_geogname_alignement.py.

Les formes qui sont des communes ou des pays (« Gand (Belgique) », « Hem (Nord) ») ne sont plus proposées ici :
elles relèvent de la famille « territoires » (mnesys_geo_territoires.py ; les fichiers fi_sites déjà validés les
listent encore, sans proposition : lignes inertes, reprises par la famille « territoires »).

Exception : « Ville de Roubaix », « Ville de Lille » (…) désignent la commune : si le reste de la forme est une
commune ou un pays du thésaurus des territoires, c'est elle qui est proposée (origine « territoires »,
confiance haute), sans chercher de lieu dont le nom la contiendrait.

Pour les autres, on cherche les lieux dont un nom ressemble à la forme (nom seul : type de voie, articles et
accents ignorés, mots dans le désordre ; 0,94 si les mots de l'un sont inclus dans l'autre, à condition qu'il
y en ait au moins deux ou que le reste soit un qualificatif (« Veuve »…) ; sinon 0,80 : candidate listée, jamais
proposée) dans trois fonds, du plus au moins sûr :

  - référentiel  : lieux du référentiel (points, sites ; rues avec une légère pénalité si la forme n'a pas de
                   type de voie) : le lieu_id s'applique tel quel avec mnesys_geo_applique.py ;
  - écarté       : lieux « ne pas conserver » du référentiel (points BD TOPO écartés) : à réintégrer ;
  - OSM          : éléments nommés d'OpenStreetMap (data/geo/osm_roubaix_lieux.json) : lieu à créer
                   (lieux_ajoutes.csv) avec la géométrie d'OSM ; lieu_id « osm:<type>/<id> ».

Colonnes : thesaurus_bnr_geo (thésaurus bnr_geo dont le @source sera écrit : bnr_geo_rbx_voies, _sites,
_quartiers ou bnr_geo_territoires ; un lieu OSM à créer serait un site : bnr_geo_rbx_sites), type_osm
(type de l'objet dans OpenStreetMap, information seulement), score, motif (« même nom », « nom complété… », « faute de saisie probable »), confiance (haute :
score ≥ 0,9, la candidate couvre tous les mots de la forme, type compatible, pas d'hésitation ; moyenne : sinon),
origine, action_referentiel (vide si le lieu est déjà au référentiel ; « réintégrer (écarté) » ;
« créer (OSM) »), autres_candidats (« id <identifiant complet> ; origine ; score » et thésaurus bnr_geo : copier l'identifiant
complet, préfixe compris, dans lieu_id pour choisir la candidate).

Toute proposition est « manuelle » : appliquée seulement si decision vaut « oui ». Un lieu_id d'origine
« écarté » ou « OSM » n'existe pas encore dans le référentiel : mnesys_geo_applique.py le laisse en attente
(« lieu absent du référentiel ») tant que le lieu n'a pas été réintégré ou créé, ce qui exige de modifier le
référentiel (à demander à l'utilisateur, avec sauvegarde). Rien n'est écrit dans les IR, le référentiel ni
la concordance.

Sont exclues : les formes déjà alignées et celles des propositions de quartiers, de reprise et de voies.

Sortie : results/ead/indexation/<série>_sites_propositions_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/mnesys_geo_sites.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
"""
import argparse
import csv
import glob
import json
import re
import sys
import unicodedata
from collections import Counter
from datetime import date
from difflib import SequenceMatcher
from os.path import dirname, join

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import SERIES, chemin_propositions, lire_formes, formes_decidees, propositions_a_exclure  # noqa: E402
from geogname_applique_concordance import SOURCES  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, COMMUNES_PAYS, REFERENTIEL, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

OSM = join("data", "geo", "osm_roubaix_lieux.json")
TYPES = {"rue", "avenue", "boulevard", "place", "impasse", "allee", "square", "cour", "couree", "courree", "courre", "ilot", "cite",
         "passage", "quai", "pont", "chemin", "contour", "route", "sentier"}
MOTS_VIDES = {"de", "du", "des", "la", "le", "les", "l", "d", "aux", "au", "et", "en", "sur", "a"}
COMMUNE = re.compile(r"\([^)]+\)\s*,?\s*$|,\s*\(")
COMMUNES_SANS_PRECISION = {"hem", "verviers", "monchengladbach", "lourdes", "le nouvion en thierache"}
INCLUSION = 0.94      # mots de l'un inclus dans l'autre (au moins 2 mots, ou le reste est un qualificatif)
INCLUSION_FAIBLE = 0.80  # un seul mot inclus dans un nom plus long : candidate listée, jamais proposée
QUALIFICATIFS = {"veuve", "general", "marechal", "president", "docteur", "professeur", "abbe", "pere", "colonel",
                 "commandant", "capitaine", "lieutenant", "maire", "ancien", "ancienne", "nouveau", "vieux", "vieil"}
SEUIL = 0.85          # en dessous, aucune proposition (candidates seulement listées)
BONUS_TYPE = 0.06     # à classement égal, la candidate de même type de voie que la forme passe devant
NB_CANDIDATES = 6
ECART = 0.05          # 2e candidate à moins de ECART de la 1re : hésitation
PENALITE = {"référentiel": 0.0, "écarté": 0.01, "OSM": 0.02}  # à ressemblance égale, le plus sûr passe devant
PENALITE_RUE = 0.04   # rue du référentiel pour une forme sans type de voie
# thésaurus du référentiel -> thésaurus bnr_geo dont le @source sera écrit dans l'IR (mnesys_geo_applique.py)
THESAURUS_BNR = {k: v.replace("thesaurus--SLASH--", "").removesuffix(".xml") for k, v in SOURCES.items()}
ACTION = {"référentiel": "", "écarté": "réintégrer (écarté)", "OSM": "créer (OSM)", "territoires": ""}
VILLE_DE = re.compile(r"^ville\s+(?:de\s+|d')\s*(.+)$", re.I)   # « Ville de Roubaix » : la commune
ORDRE_METHODE = {"manuelle": 0, "aucune": 1}
ORDRE_CONFIANCE = {"haute": 0, "moyenne": 1, "": 2}
COLONNES = [
    "forme", "occurrences", "ir", "sources_mnesys", "nature", "methode", "confiance", "motif", "lieu_propose",
    "nom_correspondant", "lieu_id", "thesaurus_bnr_geo", "origine", "type_osm", "action_referentiel", "score", "autres_candidats", "remplace_source",
    "decision", "remarque",
]


def decouper(texte):
    """(mots du nom sans type ni mots vides, types de voie) d'un libellé."""
    s = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode().lower()
    mots = [m[:-1] if m.endswith("s") and m[:-1] in TYPES else m for m in re.sub(r"[^a-z0-9]+", " ", s).split()]
    return [m for m in mots if m not in TYPES and m not in MOTS_VIDES], {m for m in mots if m in TYPES}


def cle(texte):
    return " ".join(re.sub(r"[^a-z0-9]+", " ", unicodedata.normalize("NFKD", texte).encode("ascii", "ignore")
                           .decode().lower()).split())


def ressemblance(a, b):
    na, nb = decouper(a)[0], decouper(b)[0]
    if not na or not nb:
        return 0.0
    s = SequenceMatcher(None, " ".join(sorted(na)), " ".join(sorted(nb))).ratio()
    if {m for m in na if m.isdigit()} != {m for m in nb if m.isdigit()}:
        return min(s, 0.7)  # « Roubaix 2000 » n'est pas « Roubaix-2 »
    court, long = (set(na), set(nb)) if len(set(na)) <= len(set(nb)) else (set(nb), set(na))
    if court <= long and court != long:
        fort = len(court) >= 2 or (long - court) <= QUALIFICATIFS
        return max(s, INCLUSION if fort else INCLUSION_FAIBLE)
    return s


def classer(forme, libelle):
    na, nb = decouper(forme)[0], decouper(libelle)[0]
    return ("même nom" if sorted(na) == sorted(nb) else
            "nom complété ou abrégé (titre, prénom, mot en plus)" if set(na) <= set(nb) or set(nb) <= set(na) else
            "faute de saisie probable")


def nature(forme):
    if re.match(r"(universite|ecole|institut|lycee|college|musee)\b", cle(forme)):
        return "courée" if re.search(r"\bcour+e+\b", cle(forme)) else "site"   # « Université de… (Roubaix) » : un site
    if COMMUNE.search(forme) or cle(re.sub(r"[.,;\s]+$", "", forme)) in COMMUNES_SANS_PRECISION:
        return "commune ou pays"
    return "courée" if re.search(r"\bcour+e+\b", cle(forme)) else "site"


def fonds(lieux, ecartes):
    """(origine, id, libellé, thésaurus du référentiel, noms, type OSM) de chaque lieu des trois fonds.
    Un lieu OSM à créer serait un site : thésaurus « points » (bnr_geo_rbx_sites)."""
    pool = []
    for origine, liste in (("référentiel", lieux), ("écarté", ecartes)):
        for l in liste:
            if l["thesaurus"] == "quartiers":
                continue
            ns = {n for ch in CHAMPS for c, _ in ch for n in noms(l, c)} | {l["libelle"]}
            pool.append((origine, l["lieu_id"], l["libelle"], l["thesaurus"], sorted(n for n in ns if n), ""))
    for e in json.load(open(OSM, encoding="utf-8"))["elements"]:
        t = e.get("tags", {})
        if t.get("name") and "highway" not in t:
            type_osm = next((t[k] for k in ("amenity", "tourism", "historic", "man_made", "shop", "building", "leisure")
                             if k in t), "")
            ns = [t["name"]] + [t[k] for k in ("alt_name", "old_name", "short_name") if k in t]
            pool.append(("OSM", f"osm:{e['type']}/{e['id']}", t["name"], "points", ns, type_osm))
    return pool


def communes_connues():
    """cle(nom) -> commune ou pays du thésaurus des territoires, pour les noms n'appartenant qu'à un seul concept."""
    index = {}
    for l in lire(COMMUNES_PAYS):
        for n in [l["libelle"]] + re.split(r"\s*\|\s*", l["formes_ir"] or ""):
            if n.strip():
                index.setdefault(cle(n), set()).add(l["lieu_id"])
    par_id = {l["lieu_id"]: l for l in lire(COMMUNES_PAYS)}
    return {k: par_id[next(iter(v))] for k, v in index.items() if len(v) == 1}


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR (défaut : fi)")
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    p.add_argument("--exclure", nargs="*", help="CSV de propositions dont les formes sont exclues "
                   "(défaut : séries précédentes, familles quartiers, reprise et voies, les plus récents)")
    args = p.parse_args()
    sortie = chemin_propositions(args.serie, "sites", args.date)
    exclure = args.exclure if args.exclure is not None else propositions_a_exclure(args.serie, "sites")
    exclues = set()
    for fichier in exclure:
        with open(fichier, encoding="utf-8") as f:
            exclues |= {r["forme"] for r in csv.DictReader(f)}
    exclues |= formes_decidees()  # formes déjà décidées dans la table (mnesys_geo_verse.py)

    formes = lire_formes(args.serie)
    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    pool = fonds(lieux, ecartes)
    communes = communes_connues()

    rows = []
    for forme, d in formes.items():
        if forme in exclues:
            continue
        r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
        if r["statut"] not in ("hors référentiel", "lieu écarté"):
            continue
        genre = nature(forme)
        if genre == "commune ou pays":   # famille « territoires » (mnesys_geo_territoires.py)
            continue
        candidates = []
        ville = VILLE_DE.match(forme.strip())
        commune = communes.get(cle(ville.group(1))) if ville else None
        if commune:   # « Ville de Roubaix » : la commune, pas un lieu qui contiendrait ce nom
            candidates = [(1.0, 1.0, commune["libelle"], commune["lieu_id"], "territoires", "communes_pays",
                           commune["libelle"], "")]
        else:
            types_forme = decouper(forme)[1]
            vus = set()
            for origine, lieu_id, libelle, thesaurus, ns, type_osm in pool:
                s, nom = max((ressemblance(forme, n), n) for n in ns)
                if s < 0.6:
                    continue
                ajuste = (s - PENALITE[origine] - (PENALITE_RUE if thesaurus == "rues" and not types_forme else 0.0)
                          + (BONUS_TYPE if types_forme & decouper(nom)[1] else 0.0))
                if cle(libelle) in vus:
                    continue
                vus.add(cle(libelle))
                candidates.append((ajuste, s, libelle, lieu_id, origine, thesaurus, nom, type_osm))
            candidates.sort(key=lambda c: -c[0])
            candidates = candidates[:NB_CANDIDATES]
        hesite = (len(candidates) > 1 and candidates[0][0] - candidates[1][0] < ECART)
        best = candidates[0] if candidates else None
        methode, confiance, motif = "manuelle", "", ""
        if commune:
            confiance, motif = "haute", "« Ville de » + commune du thésaurus des territoires"
        elif not best or best[1] < SEUIL:
            methode, motif = "aucune", ("candidates trop éloignées (voir autres_candidats)" if best else "aucune candidate")
        else:
            fm, nm = decouper(forme), decouper(best[6])
            complete = set(fm[0]) <= set(nm[0]) or len(fm[0]) == len(nm[0])   # la candidate couvre la forme
            meme_type = not fm[1] or not nm[1] or bool(fm[1] & nm[1])
            confiance = "haute" if best[1] >= 0.9 and complete and meme_type and not hesite else "moyenne"
            motif = classer(forme, best[6]) + ("" if meme_type else " ; autre type de voie ou de lieu") + (f" ; hésitation avec « {candidates[1][2]} » ({candidates[1][1]:.2f})"
                                                if hesite else "")
        proposee = methode == "manuelle"
        rows.append({
            "forme": forme, "occurrences": d["occ"],
            "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
            "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
            "nature": genre, "methode": methode, "confiance": confiance, "motif": motif,
            "lieu_propose": best[2] if proposee else "",
            "nom_correspondant": best[6] if proposee and cle(best[6]) != cle(best[2]) else "",
            "lieu_id": best[3] if proposee else "",
            "thesaurus_bnr_geo": THESAURUS_BNR[best[5]] if proposee else "",
            "origine": best[4] if proposee else "", "type_osm": best[7] if proposee else "",
            "action_referentiel": ACTION[best[4]] if proposee else "",
            "score": f"{best[1]:.2f}" if best else "",
            "autres_candidats": " | ".join(f"{c[2]} (id {c[3]} ; {c[4]} ; {c[1]:.2f}) → {THESAURUS_BNR[c[5]]}" for c in candidates
                                           if not (proposee and c[3] == best[3])),
            "remplace_source": "oui" if proposee and best[4] in ("référentiel", "territoires")
                               and any(s != "(aucune)" for s in d["src"]) else "",
            "decision": "", "remarque": "",
        })
    rows.sort(key=lambda r: (ORDRE_METHODE[r["methode"]], ORDRE_CONFIANCE[r["confiance"]], -r["occurrences"], r["forme"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot = sum(d["occ"] for d in formes.values())
    print(f"{len(rows)} formes ({sum(r['occurrences'] for r in rows)} geogname) sur {len(formes)} formes, "
          f"{tot} geogname → {sortie}")
    print("  nature :", {n: (sum(1 for r in rows if r["nature"] == n), sum(r["occurrences"] for r in rows if r["nature"] == n))
                        for n in ("site", "courée")}, "(formes, occ)")
    for m in ORDRE_METHODE:
        for c in ORDRE_CONFIANCE:
            rs = [r for r in rows if r["methode"] == m and r["confiance"] == c]
            if rs:
                print(f"  {m:9s} {c or '-':8s} {len(rs):3d} formes  {sum(r['occurrences'] for r in rs):3d} occ")
    prop = [r for r in rows if r["lieu_id"]]
    print("  propositions par origine :", dict(Counter(r["origine"] for r in prop)),
          "| occ :", {o: sum(r["occurrences"] for r in prop if r["origine"] == o)
                       for o in ("référentiel", "écarté", "OSM", "territoires")})


if __name__ == "__main__":
    main()
