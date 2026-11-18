"""
Diagnostic (lecture seule) : à quel point les <geogname> des inventaires Mnesys des séries
Fi (data/ead/mnesys/FRAM59100_*Fi*.xml : 1AEFi, 1NumFi, 3Fi, 4Fi… 10Fi) se croisent avec
le référentiel des lieux de Roubaix.

Alignement par le texte seul, avec la fonction aligner() de geogname_concordance_lieux.py
(forme remise à l'endroit, clé sans accents/articles, libellé puis variantes puis anciens
noms). Les @source des Fi ne sont pas quartier/rue/adresse (91 % sans @source, sinon
thésaurus Mnesys index_Lieux / FRAM59100_THES_000004) : source vide pour aligner().
Les @coord éventuels sont ignorés. Rien n'est écrit dans le référentiel ni la concordance.

Colonnes ajoutées :
  - sources_mnesys : @source rencontrés pour la forme (et nombre) ;
  - dans_concordance_bnr / concordance_statut / concordance_libelle : la même forme
    exacte dans data/geo/concordance_geogname_lieux.csv (décisions déjà prises côté BnR) ;
  - piste_quartier : pour une forme « X, quartier de… » non alignée, le quartier trouvé
    en retirant « , quartier… » (piste, pas une décision).

Sortie : results/ead/indexation/fi_geogname_alignement_<date>.csv + bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/fi_geogname_alignement.py [--date AAAAMMJJ]
"""
import argparse
import csv
import glob
import re
import sys
from collections import Counter, defaultdict
from datetime import date
from os.path import basename, dirname, join

from lxml import etree

sys.path.insert(0, dirname(__file__))
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, REFERENTIEL, SORTIE as CONCORDANCE, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

IR_FI = join("data", "ead", "mnesys", "FRAM59100_*Fi*.xml")
QUARTIER = re.compile(r",\s*quartier\b.*$|^quartier\s+", re.I)


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    args = p.parse_args()
    sortie = join("results", "ead", "indexation", f"fi_geogname_alignement_{args.date}.csv")

    formes = defaultdict(lambda: {"occ": 0, "ir": Counter(), "src": Counter()})
    for f in sorted(glob.glob(IR_FI)):
        ir = basename(f).replace("FRAM59100_", "").replace(".xml", "")
        for g in etree.parse(f).iter("{*}geogname", "geogname"):
            t = "".join(g.itertext()).strip()
            if t:
                formes[t]["occ"] += 1
                formes[t]["ir"][ir] += 1
                formes[t]["src"][g.get("source", "(aucune)")] += 1

    lieux, ecartes = lire(REFERENTIEL), lire(SORTIE_ECARTES)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    conc = defaultdict(list)
    for c in lire(CONCORDANCE):
        conc[c["forme"].strip()].append(c)

    rows = []
    for forme, d in formes.items():
        r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
        lieu = r["lieu"] or {}
        piste = ""
        if r["statut"] != "aligné" and QUARTIER.search(forme):
            q = aligner(QUARTIER.sub("", forme).strip(), "quartier", lieux, ecartes, index, index_ec, cles)
            piste = (q["lieu"] or {}).get("libelle", "") if q["statut"] == "aligné" else ""
        cc = conc.get(forme, [])
        rows.append({
            "forme": forme, "occurrences": d["occ"],
            "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common()),
            "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
            "statut": r["statut"], "alignement": r["alignement"],
            "lieu_id": lieu.get("lieu_id", ""), "thesaurus": lieu.get("thesaurus", ""),
            "libelle": lieu.get("libelle", ""), "candidats": " | ".join(r["candidats"]),
            "piste_quartier": piste,
            "dans_concordance_bnr": "oui" if cc else "",
            "concordance_statut": " / ".join(sorted({c["statut"] for c in cc})),
            "concordance_libelle": " / ".join(sorted({c["libelle_retenu"] for c in cc if c["libelle_retenu"]})),
        })
    rows.sort(key=lambda r: (r["statut"], -r["occurrences"]))
    with open(sortie, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    tot_f, tot_o = len(rows), sum(r["occurrences"] for r in rows)
    print(f"{tot_f} formes, {tot_o} geogname → {sortie}")
    for s in sorted({r["statut"] for r in rows}):
        rs = [r for r in rows if r["statut"] == s]
        o = sum(r["occurrences"] for r in rs)
        print(f"  {s:18s} {len(rs):5d} formes ({len(rs) / tot_f:5.1%})  {o:5d} occ ({o / tot_o:5.1%})")
    al = [r for r in rows if r["statut"] == "aligné"]
    print("  thésaurus :", Counter(r["thesaurus"] for r in al).most_common())
    print(f"  lieux distincts atteints : {len({r['lieu_id'] for r in al})} / {len(lieux)}")
    par_ir = defaultdict(lambda: [0, 0])
    for r in rows:
        for part in r["ir"].split(", "):
            ir, n = part.rsplit(":", 1)
            par_ir[ir][1] += int(n)
            par_ir[ir][0] += int(n) if r["statut"] == "aligné" else 0
    for ir, (a, t) in sorted(par_ir.items(), key=lambda x: -x[1][1]):
        print(f"  {ir:22s} {a:5d}/{t:5d} ({a / t:5.1%})")
    pq = [r for r in rows if r["piste_quartier"]]
    print(f"pistes quartier : {len(pq)} formes ({sum(r['occurrences'] for r in pq)} occ)")
    cc = [r for r in rows if r["statut"] != "aligné" and r["concordance_libelle"]]
    print(f"non alignées mais résolues à la main dans la concordance BnR : {len(cc)} formes "
          f"({sum(r['occurrences'] for r in cc)} occ)")


if __name__ == "__main__":
    main()
