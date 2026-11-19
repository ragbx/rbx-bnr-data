"""
<geogname> de voies des inventaires Mnesys (--serie fi : séries Fi, data/ead/mnesys/FRAM59100_*Fi*.xml ; --serie mnesys : tous les IR) que le
référentiel des lieux n'aligne pas (statut « sans lieu » de aligner()) — propositions (lecture seule),
à valider puis à appliquer avec mnesys_geo_applique.py. Pour chaque forme, la meilleure des candidates de
aligner() est choisie par ressemblance du nom seul (type de voie, articles et accents ignorés, mots
dans le désordre) :

  - score  : ressemblance du nom (0 à 1) ; 0,94 au moins si les mots de l'un sont inclus dans l'autre
             (« Foch » / Maréchal Foch ; « Catteau » / Pierre Catteau) ; à classement égal, la candidate de
             même type de voie que la forme passe devant ;
  - motif  : « même nom », « nom complété ou abrégé (titre, prénom) » ou « faute de saisie probable », suivi
             de « autre type de voie (rue → place) » quand les types diffèrent ;
  - confiance : haute (score ≥ 0,9, type de voie compatible, pas d'autre candidate à moins de 0,05),
             moyenne (score ≥ 0,8 ; le motif signale alors le type différent ou l'hésitation) ;
  - concordance_bnr : statut de la même forme dans data/geo/concordance_geogname_lieux.csv quand elle y est
             sans lieu (couche historique : rue disparue, décision déjà prise côté BN-R) : rien n'est proposé.

Toute proposition est « manuelle » : elle n'est appliquée que si decision vaut « oui ». Sans candidate
assez proche (score < 0,8) ou déjà tranchée « sans lieu » côté BN-R, la méthode est « aucune » : les
candidates sont listées dans autres_candidats, un lieu_id peut être saisi à la main (puis decision « oui »).

Sont exclues : les formes alignées par le référentiel et celles des propositions de quartiers et de
reprise. Rien n'est écrit dans les IR, le référentiel ni la concordance.

Sortie : results/ead/indexation/<série>_voies_propositions_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/mnesys_geo_voies.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
"""
import argparse
import csv
import glob
import re
import sys
import unicodedata
from collections import defaultdict
from datetime import date
from difflib import SequenceMatcher
from os.path import dirname, join

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import SERIES, chemin_propositions, lire_formes, formes_decidees, propositions_a_exclure  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, REFERENTIEL, SORTIE as CONCORDANCE, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

TYPES = {"rue", "avenue", "boulevard", "place", "impasse", "allee", "square", "cour", "cite", "passage", "quai",
         "pont", "chemin", "contour", "route", "sentier", "parc", "promenade", "ruelle", "villa", "esplanade"}
MOTS_VIDES = {"de", "du", "des", "la", "le", "les", "l", "d", "aux", "au", "et", "en", "sur"}
CANDIDATE = re.compile(r"^(?P<libelle>.+?) \[(?P<id>[^\]]+)\](?P<ecarte> \(écarté\))?$")
INCLUSION = 0.94    # mots de la forme inclus dans le nom du lieu, ou l'inverse (« Foch » / Maréchal Foch)
SEUIL = 0.8         # en dessous, aucune proposition (candidates seulement listées)
BONUS_TYPE = 0.06   # à classement égal, la candidate qui a le même type de voie que la forme passe devant
ECART = 0.05        # si la 2e candidate est à moins de ECART de la 1re : confiance « moyenne », hésitation signalée
ORDRE_METHODE = {"manuelle": 0, "aucune": 1}
ORDRE_CONFIANCE = {"haute": 0, "moyenne": 1, "": 2}
COLONNES = [
    "forme", "occurrences", "ir", "sources_mnesys", "methode", "confiance", "motif", "lieu_propose", "lieu_id",
    "thesaurus", "score", "autres_candidats", "concordance_bnr", "remplace_source", "decision", "remarque",
]


def decouper(texte):
    """(mots du nom sans type ni mots vides, types de voie) d'un libellé."""
    s = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode().lower()
    mots = re.sub(r"[^a-z0-9]+", " ", s).split()
    return [m for m in mots if m not in TYPES and m not in MOTS_VIDES], {m for m in mots if m in TYPES}


def ressemblance(a, b):
    na, nb = decouper(a)[0], decouper(b)[0]
    if not na or not nb:
        return 0.0
    s = SequenceMatcher(None, " ".join(sorted(na)), " ".join(sorted(nb))).ratio()
    return max(s, INCLUSION) if set(na) <= set(nb) or set(nb) <= set(na) else s


def classer(forme, libelle, score):
    """Motif d'une proposition : nom identique, complété (titre, prénom) ou faute de saisie ; type de voie."""
    na, nb = decouper(forme)[0], decouper(libelle)[0]
    nom = ("même nom" if sorted(na) == sorted(nb) else
           "nom complété ou abrégé (titre, prénom)" if set(na) <= set(nb) or set(nb) <= set(na) else
           "faute de saisie probable")
    tf, tc = decouper(forme)[1], decouper(libelle)[1]
    if tf and tc and not tf & tc:
        nom += f", autre type de voie ({'/'.join(sorted(tf))} → {'/'.join(sorted(tc))})"
    return nom


def norm(texte):
    return " ".join(texte.replace("\\'", "'").split())


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR (défaut : fi)")
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    p.add_argument("--exclure", nargs="*", help="CSV de propositions dont les formes sont exclues "
                   "(défaut : séries précédentes, familles quartiers et reprise, les plus récents)")
    args = p.parse_args()
    sortie = chemin_propositions(args.serie, "voies", args.date)
    exclure = args.exclure if args.exclure is not None else propositions_a_exclure(args.serie, "voies")
    exclues = set()
    for fichier in exclure:
        with open(fichier, encoding="utf-8") as f:
            exclues |= {r["forme"] for r in csv.DictReader(f)}
    exclues |= formes_decidees()  # formes déjà décidées dans la table (mnesys_geo_verse.py)

    formes = lire_formes(args.serie)
    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    par_id = {l["lieu_id"]: l for l in lieux}
    conc = defaultdict(list)
    for c in lire(CONCORDANCE):
        conc[norm(c["forme"])].append(c)

    rows = []
    for forme, d in formes.items():
        if forme in exclues:
            continue
        r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
        if r["statut"] != "sans lieu":
            continue
        types_forme = decouper(forme)[1]
        candidates = []
        for texte in r["candidats"]:
            m = CANDIDATE.match(texte)
            if m and not m["ecarte"] and m["id"] in par_id:
                score = ressemblance(forme, m["libelle"])
                bonus = BONUS_TYPE if types_forme & decouper(m["libelle"])[1] else 0.0
                candidates.append((score + bonus, score, m["libelle"], m["id"]))
        candidates.sort(key=lambda c: -c[0])
        hesite = (f"hésitation avec « {candidates[1][2]} » ({candidates[1][1]:.2f})"
                  if len(candidates) > 1 and candidates[0][0] - candidates[1][0] < ECART else "")
        candidates = [(s, l, i) for _, s, l, i in candidates]
        score, libelle, lieu_id = candidates[0] if candidates else (0.0, "", "")
        types_cand = decouper(libelle)[1]
        meme_type = not types_forme or not types_cand or bool(types_forme & types_cand)
        sans_lieu_bnr = [c for c in conc.get(norm(forme), []) if c["statut"] != "aligné"]
        concordance_bnr = f"{sans_lieu_bnr[0]['statut']} ({sans_lieu_bnr[0]['remarque'][:80]})" if sans_lieu_bnr else ""

        methode, confiance, motif = "manuelle", "", ""
        if concordance_bnr:
            methode, motif = "aucune", "déjà laissée sans lieu côté BN-R (couche historique)"
        elif score < SEUIL:
            methode, motif = "aucune", ("candidates trop éloignées (voir autres_candidats)" if candidates
                                        else "aucune candidate")
        else:
            confiance = "haute" if score >= 0.9 and meme_type and not hesite else "moyenne"
            motif = classer(forme, libelle, score) + (f" ; {hesite}" if hesite else "")
        proposee = methode == "manuelle"
        rows.append({
            "forme": forme, "occurrences": d["occ"],
            "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
            "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
            "methode": methode, "confiance": confiance, "motif": motif,
            "lieu_propose": libelle if proposee else "", "lieu_id": lieu_id if proposee else "",
            "thesaurus": par_id[lieu_id]["thesaurus"] if proposee else "",
            "score": f"{score:.2f}" if candidates else "",
            "autres_candidats": " | ".join(f"{l} [{i}] ({s:.2f})" for s, l, i in candidates
                                           if not (proposee and i == lieu_id)),
            "concordance_bnr": concordance_bnr,
            "remplace_source": "oui" if proposee and any(s != "(aucune)" for s in d["src"]) else "",
            "decision": "", "remarque": "",
        })
    rows.sort(key=lambda r: (ORDRE_METHODE[r["methode"]], ORDRE_CONFIANCE[r["confiance"]], -r["occurrences"], r["forme"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot = sum(d["occ"] for d in formes.values())
    print(f"{len(rows)} formes « sans lieu » ({sum(r['occurrences'] for r in rows)} geogname) sur {len(formes)} formes, "
          f"{tot} geogname → {sortie}")
    for m in ORDRE_METHODE:
        for c in ORDRE_CONFIANCE:
            rs = [r for r in rows if r["methode"] == m and r["confiance"] == c]
            if rs:
                print(f"  {m:9s} {c or '-':8s} {len(rs):3d} formes  {sum(r['occurrences'] for r in rs):3d} occ")
    rs = [r for r in rows if r["remplace_source"]]
    print(f"  @source Mnesys existant remplacé : {len(rs)} forme(s) {[r['forme'] for r in rs]}")


if __name__ == "__main__":
    main()
