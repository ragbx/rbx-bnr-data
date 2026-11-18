"""
<geogname> « quartier » des inventaires Mnesys (--serie fi : séries Fi, data/ead/mnesys/FRAM59100_*Fi*.xml ; --serie mnesys : tous les IR)
à aligner sur le thésaurus des quartiers (bnr_geo_rbx_quartiers) — étape 1 : propositions (lecture seule).

Forme visée : « X, quartier de… » ou « Quartier X » (motif QUARTIER de mnesys_geo_commun.py).
Le mot « quartier » levant l'homonymie avec la rue, on retire ce motif et on aligne le reste sur le
référentiel avec aligner() de geogname_concordance_lieux.py (source « quartier »). Trois méthodes :

  - automatique : le reste s'aligne sur un lieu du thésaurus « quartiers » ;
  - manuelle    : le reste ne s'aligne pas, mais une variante évidente est dans MANUELLES
                  (« 3 Ponts » → Trois Ponts…) : à valider par l'utilisateur ;
  - aucune      : pas de quartier correspondant (« Alouette » : seulement une rue) ; rien n'est
                  proposé, la forme reste telle quelle.

Colonne remplace_source : « oui » si la forme porte déjà un @source Mnesys (3Fi : thesaurus/index_Lieux.xml) ;
décision du 2026-11-18 : ce @source est remplacé par celui du thésaurus des quartiers.
Colonnes decision / remarque : à remplir (« non » exclut la ligne de l'étape 2).

Rien n'est écrit dans les IR, le référentiel ni la concordance.

Sortie : results/ead/indexation/<série>_quartiers_propositions_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/mnesys_geo_quartiers.py [--serie fi|mnesys] [--date AAAAMMJJ]
"""
import argparse
import csv
import glob
import sys
from collections import Counter, defaultdict
from datetime import date
from os.path import basename, dirname, join

from lxml import etree

sys.path.insert(0, dirname(__file__))
from mnesys_geo_commun import QUARTIER, SERIES, chemin_propositions, lire_formes, propositions_a_exclure  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, REFERENTIEL, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

# Formes « quartier » que l'alignement par le texte ne résout pas : libellé du quartier du référentiel.
MANUELLES = {
    "Centre, quartier": "Centre-Ville",
    "Quartier Centre": "Centre-Ville",
    "Alma, quartier de l'": "Alma Gare",
    "3 Ponts, quartier des": "Trois Ponts",
    "Saint-Elisabeth, quartier": "Sainte-Élisabeth",
    "Hutin, quartier du": "Hutin Oran Cartigny",
    "Nouveaux Roubaix, quartier du": "Nouveau Roubaix",
    # série mnesys (étape 4) : même décision que « Alouette » des Fi, faute de saisie, Oran dans Hutin-Oran-Cartigny
    "Alouette, quartier de": "Espérance",
    "Fosse aux Chêne, quartier de la": "Fosse-aux-Chênes",
    "Oran, quartier de": "Hutin Oran Cartigny",
}
ORDRE = {"manuelle": 0, "aucune": 1, "automatique": 2}  # à valider d'abord
COLONNES = [
    "forme", "occurrences", "ir", "sources_mnesys", "methode", "quartier_propose", "lieu_id",
    "remplace_source", "candidats", "decision", "remarque",
]


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--serie", choices=SERIES, default="fi", help="série d'IR (défaut : fi)")
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    args = p.parse_args()
    sortie = chemin_propositions(args.serie, "quartiers", args.date)
    exclues = set()
    for fichier in propositions_a_exclure(args.serie, "quartiers"):
        with open(fichier, encoding="utf-8") as fh:
            exclues |= {r["forme"] for r in csv.DictReader(fh)}

    formes = lire_formes(args.serie)
    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    quartiers = {l["libelle"]: l for l in lieux if l["thesaurus"] == "quartiers"}
    absents = [q for q in MANUELLES.values() if q not in quartiers]
    if absents:
        sys.exit(f"Quartier absent du référentiel : {absents}")

    rows = []
    for forme, d in formes.items():
        if forme in exclues or not QUARTIER.search(forme):
            continue
        reste = QUARTIER.sub("", forme).strip()
        r = aligner(reste, "quartier", lieux, ecartes, index, index_ec, cles)
        quartier, methode = None, "aucune"
        if r["statut"] == "aligné" and r["lieu"]["thesaurus"] == "quartiers":
            quartier, methode = r["lieu"], "automatique"
        elif forme in MANUELLES:
            quartier, methode = quartiers[MANUELLES[forme]], "manuelle"
        rows.append({
            "forme": forme, "occurrences": d["occ"],
            "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
            "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
            "methode": methode,
            "quartier_propose": quartier["libelle"] if quartier else "",
            "lieu_id": quartier["lieu_id"] if quartier else "",
            "remplace_source": "oui" if any(s != "(aucune)" for s in d["src"]) and quartier else "",
            "candidats": "" if quartier else " | ".join(r["candidats"]),
            "decision": "", "remarque": "",
        })
    rows.sort(key=lambda r: (ORDRE[r["methode"]], -r["occurrences"], r["forme"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot_o = sum(d["occ"] for d in formes.values())
    print(f"{len(rows)} formes « quartier » sur {len(formes)}, {sum(r['occurrences'] for r in rows)} geogname "
          f"sur {tot_o} → {sortie}")
    for m in ORDRE:
        rs = [r for r in rows if r["methode"] == m]
        print(f"  {m:12s} {len(rs):3d} formes  {sum(r['occurrences'] for r in rs):3d} occ")
    prop = [r for r in rows if r["quartier_propose"]]
    par_ir = Counter()
    for r in prop:
        for part in r["ir"].split(", "):
            ir, n = part.rsplit(":", 1)
            par_ir[ir] += int(n)
    print("  par IR :", dict(par_ir.most_common()))
    print(f"  quartiers atteints : {len({r['lieu_id'] for r in prop})} / {len(quartiers)}")
    rs = [r for r in prop if r["remplace_source"]]
    print(f"  @source Mnesys existant remplacé : {len(rs)} forme(s) {[r['forme'] for r in rs]}")


if __name__ == "__main__":
    main()
