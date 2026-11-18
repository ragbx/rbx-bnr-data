"""
<geogname> des inventaires Mnesys (--serie fi : séries Fi, data/ead/mnesys/FRAM59100_*Fi*.xml ; --serie mnesys : tous les IR) que le
référentiel des lieux n'aligne pas, mais qui se résolvent sans nouveau travail de fond — propositions
(lecture seule), à appliquer ensuite avec mnesys_geo_applique.py. Deux origines :

  - concordance BnR : la forme exacte est dans data/geo/concordance_geogname_lieux.csv avec un lieu
    retenu (statut « aligné ») : décision déjà prise côté BN-R (« Roubaix » → Roubaix (Nord),
    « Henri Carette, rue » → Rue Henri Carrette…). Les lignes sans @source sont préférées ; si la
    concordance donne plusieurs lieux, rien n'est proposé (motif « conflit ») ;
  - territoires : la forme (ponctuation finale et casse ignorées : « Tourcoing, (Nord) ») est le
    libellé ou une forme connue d'un concept de data/geo/referentiel_communes_pays.csv. Si seule la
    précision diffère (« Paris (Île de France) » / Paris (France)), proposition « manuelle ».

Méthodes (comme mnesys_geo_quartiers.py) : « automatique » = appliquée sauf decision « non » ; « manuelle » =
appliquée seulement si decision « oui » (voie rattachée à une commune par la BN-R, précision
différente) ; « aucune » = conflit, rien de proposé (lieu_id peut être saisi, puis decision « oui »).
Colonne remplace_source : « oui » si une occurrence porte déjà un @source Mnesys ; il est alors remplacé
(décision du 2026-11-18 pour les quartiers, étendue ici : à contredire dans decision si besoin).

Sont exclues : les formes déjà alignées par le référentiel et celles des propositions de quartiers.
Rien n'est écrit dans les IR, le référentiel ni la concordance.

Sortie : results/ead/indexation/<série>_reprise_propositions_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/mnesys_geo_reprise.py [--serie fi|mnesys] [--date AAAAMMJJ] [--exclure CSV ...]
"""
import argparse
import csv
import glob
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from datetime import date
from os.path import dirname, join

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import SERIES, chemin_propositions, lire_formes, propositions_a_exclure  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, COMMUNES_PAYS, REFERENTIEL, SORTIE as CONCORDANCE, SORTIE_ECARTES, TYPE_VOIE,
    aligner, cle_voie, indexer, lire, noms,
)

ORDRE = {"manuelle": 0, "aucune": 1, "automatique": 2}  # à valider d'abord
COLONNES = [
    "forme", "occurrences", "ir", "sources_mnesys", "origine", "methode", "motif", "lieu_propose",
    "lieu_id", "thesaurus", "remplace_source", "decision", "remarque",
]


def cle(texte):
    """Clé de comparaison : sans accents, casse ni ponctuation."""
    s = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def norm(texte):
    return " ".join(texte.replace("\\'", "'").split())


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR (défaut : fi)")
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    p.add_argument("--exclure", nargs="*", help="CSV de propositions dont les formes sont exclues "
                   "(défaut : séries précédentes et famille quartiers, les plus récents)")
    args = p.parse_args()
    sortie = chemin_propositions(args.serie, "reprise", args.date)
    exclure = args.exclure if args.exclure is not None else propositions_a_exclure(args.serie, "reprise")
    exclues = set()
    for fichier in filter(None, exclure):
        with open(fichier, encoding="utf-8") as f:
            exclues |= {r["forme"] for r in csv.DictReader(f)}

    formes = lire_formes(args.serie)
    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    territoires = lire(COMMUNES_PAYS)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    par_id = {l["lieu_id"]: l for l in lieux + territoires}

    conc = defaultdict(list)
    for c in lire(CONCORDANCE):
        if c["statut"] == "aligné" and c["lieu_id"] in par_id:
            conc[norm(c["forme"])].append(c)
    exact, precision = defaultdict(set), defaultdict(set)
    for l in territoires:
        exact[cle(l["libelle"])].add(l["lieu_id"])
        for f in re.split(r"\s*\|\s*", l["formes_ir"] or ""):
            if f.strip():
                exact[cle(f)].add(l["lieu_id"])
        precision[cle(re.sub(r"\s*\(.*\)$", "", l["libelle"]))].add(l["lieu_id"])

    rows, deja_occ = [], 0
    for forme, d in formes.items():
        r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
        if r["statut"] == "aligné" or forme in exclues:
            deja_occ += d["occ"]
            continue
        base = re.sub(r"[.,;\s]+$", "", forme)
        origine = motif = ""
        methode, ids = "automatique", set()
        lignes = conc.get(norm(forme), [])
        ids = {c["lieu_id"] for c in ([c for c in lignes if not c["source"]] or lignes)}
        if len(ids) == 1:
            origine = "concordance BnR"
        elif len(ids) > 1:
            rows.append(_ligne(forme, d, "concordance BnR", "aucune", "conflit : plusieurs lieux dans la concordance",
                               None, " | ".join(sorted(par_id[i]["libelle"] for i in ids))))
            continue
        else:
            ids = exact.get(cle(base), set())
            if len(ids) == 1:
                origine = "territoires"
            elif "(" in base and (ids := precision.get(cle(re.sub(r"\s*\(.*$", "", base)), set())):
                origine, methode, motif = "territoires", "manuelle", "seule la précision diffère : à confirmer"
            else:
                continue
        lieu = par_id[next(iter(ids))] if len(ids) == 1 else None
        if lieu is None:  # précision ambiguë : plusieurs concepts du même nom
            rows.append(_ligne(forme, d, origine, "aucune", "plusieurs concepts de ce nom", None,
                               " | ".join(sorted(par_id[i]["libelle"] for i in ids))))
            continue
        if lieu["thesaurus"] == "communes_pays" and TYPE_VOIE.search(forme):
            methode, motif = "manuelle", "voie rattachée à une commune par la BN-R : à confirmer pour les Fi"
        rows.append(_ligne(forme, d, origine, methode, motif, lieu))
    rows.sort(key=lambda r: (ORDRE[r["methode"]], -r["occurrences"], r["forme"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot = sum(d["occ"] for d in formes.values())
    prop = [r for r in rows if r["lieu_id"]]
    print(f"{len(rows)} formes proposées ({sum(r['occurrences'] for r in rows)} geogname) sur {len(formes)} formes, "
          f"{tot} geogname → {sortie}")
    for o in ("concordance BnR", "territoires"):
        for m in ORDRE:
            rs = [r for r in rows if r["origine"] == o and r["methode"] == m]
            if rs:
                print(f"  {o:16s} {m:12s} {len(rs):3d} formes  {sum(r['occurrences'] for r in rs):3d} occ")
    auto = sum(r["occurrences"] for r in prop if r["methode"] == "automatique")
    tout = sum(r["occurrences"] for r in prop)
    print(f"  déjà alignées ou en quartiers : {deja_occ} occ ({deja_occ / tot:.1%}) ; "
          f"+ automatiques {auto} → {(deja_occ + auto) / tot:.1%} ; + manuelles validées → {(deja_occ + tout) / tot:.1%}")
    rs = [r for r in prop if r["remplace_source"]]
    print(f"  @source Mnesys existant remplacé : {len(rs)} forme(s) "
          f"{[(r['forme'], r['sources_mnesys']) for r in rs]}")


def _ligne(forme, d, origine, methode, motif, lieu, candidats=""):
    return {
        "forme": forme, "occurrences": d["occ"],
        "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
        "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
        "origine": origine, "methode": methode, "motif": motif,
        "lieu_propose": lieu["libelle"] if lieu else candidats,
        "lieu_id": lieu["lieu_id"] if lieu else "",
        "thesaurus": lieu["thesaurus"] if lieu else "",
        "remplace_source": "oui" if lieu and any(s != "(aucune)" for s in d["src"]) else "",
        "decision": "", "remarque": "",
    }


if __name__ == "__main__":
    main()
