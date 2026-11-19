"""
Diagnostic (lecture seule) : état de l'alignement des <geogname> de TOUS les inventaires Mnesys
(data/ead/mnesys/FRAM59100_*.xml) sur le référentiel des lieux de Roubaix, les territoires et les décisions
déjà prises. Généralise fi_geogname_alignement.py (limité aux séries Fi) et le complète : décisions déjà
validées, thésaurus des territoires, nature des formes restant à traiter.

Chaque forme (texte du <geogname>, sans autre transformation) reçoit une catégorie, dans cet ordre :

  1. référentiel   : alignée par le texte sur un lieu du référentiel (aligner() de geogname_concordance_lieux.py,
                     sans @source) ;
  2. décision validée : forme validée dans les fichiers <série>_<famille>_propositions_<date>.csv (decision « oui »,
                     ou méthode « automatique » sans decision « non ») ;
  3. concordance BnR : forme exacte de data/geo/concordance_geogname_lieux.csv avec un lieu retenu ;
  4. territoires   : libellé ou forme connue d'un concept de data/geo/referentiel_communes_pays.csv
                     (casse et ponctuation finale ignorées) ;
  5. à traiter     : le reste, avec sa nature : quartier (« X, quartier de… »), commune ou pays, voie
                     (« sans lieu » : faute de saisie ou voie absente), lieu écarté, courée, site.

Les IR qui ne s'analysent pas (esperluettes non échappées) sont listés ; ils n'ont aucun <geogname>.
Rien n'est écrit dans les IR, le référentiel ni la concordance.

Sorties (results/ead/indexation/) :
  mnesys_geo_diagnostic_formes_<date>.csv : une ligne par forme ;
  mnesys_geo_diagnostic_ir_<date>.csv     : une ligne par IR (occurrences par catégorie).
+ bilan à l'écran.

À lancer depuis la racine du dépôt (après referentiel_lieux_roubaix.py) :
    python scripts/ead/mnesys_geo_diagnostic.py [--date AAAAMMJJ]
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
from mnesys_geo_commun import QUARTIER, lire_table, propositions_appliquees  # noqa: E402
from mnesys_geo_sites import nature  # noqa: E402
from geogname_concordance_lieux import (  # noqa: E402
    CHAMPS, COMMUNES_PAYS, REFERENTIEL, SORTIE as CONCORDANCE, SORTIE_ECARTES, aligner, cle_voie, indexer, lire, noms,
)

IR_MNESYS = join("data", "ead", "mnesys", "FRAM59100_*.xml")
CATEGORIES = ["référentiel", "décision validée", "concordance BnR", "territoires", "à traiter"]


def cle(texte):
    """Clé de comparaison : sans accents, casse ni ponctuation."""
    import unicodedata
    s = unicodedata.normalize("NFKD", texte).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


def norm(texte):
    return " ".join(texte.replace("\\'", "'").split())


def lire_formes():
    """forme -> occurrences, par IR, par @source ; liste des IR illisibles."""
    formes = defaultdict(lambda: {"occ": 0, "ir": Counter(), "src": Counter()})
    illisibles = []
    for f in sorted(glob.glob(IR_MNESYS)):
        ir = basename(f).replace("FRAM59100_", "").replace(".xml", "")
        try:
            arbre = etree.parse(f)
        except etree.XMLSyntaxError:
            illisibles.append(ir)
            continue
        for g in arbre.iter("geogname"):
            t = "".join(g.itertext()).strip()
            if t:
                formes[t]["occ"] += 1
                formes[t]["ir"][ir] += 1
                formes[t]["src"][g.get("source", "(aucune)")] += 1
    return formes, illisibles


def decisions_validees():
    """forme -> lieu_id des propositions validées (plus récent fichier de chaque série et famille) : decision
    « oui », ou méthode « automatique » sans « non »."""
    decisions = {}
    for fichier in propositions_appliquees():
        with open(fichier, encoding="utf-8") as f:
            for p in csv.DictReader(f):
                d = p["decision"].strip().lower()
                if p["lieu_id"] and d != "non" and (d == "oui" or p["methode"] == "automatique"):
                    decisions[p["forme"]] = p["lieu_id"]
    for forme, t in lire_table().items():  # la table de décisions (mnesys_geo_verse.py) l'emporte
        if t["decision"] == "oui" and t["lieu_id"]:
            decisions[forme] = t["lieu_id"]
        else:
            decisions.pop(forme, None)
    return decisions


def classer(formes):
    """Catégorie et lieu de chaque forme ({forme: {occ, ir, src}}) : liste de lignes du diagnostic, dans l'ordre de
    l'en-tête (référentiel, décision validée, concordance BnR, territoires, à traiter). Utilisé aussi par
    archives_carte_notices.py (carte)."""
    lieux, ecartes, territoires = lire(REFERENTIEL), lire(SORTIE_ECARTES), lire(COMMUNES_PAYS)
    index, index_ec = indexer(lieux), indexer(ecartes)
    cles = {l["lieu_id"]: {cle_voie(n) for ch in CHAMPS for c, _ in ch for n in noms(l, c)} for l in lieux}
    par_id = {l["lieu_id"]: l for l in lieux + territoires}
    decisions = decisions_validees()
    conc = defaultdict(list)
    for c in lire(CONCORDANCE):
        if c["statut"] == "aligné" and c["lieu_id"] in par_id:
            conc[norm(c["forme"])].append(c)
    exact = defaultdict(set)
    for l in territoires:
        exact[cle(l["libelle"])].add(l["lieu_id"])
        for f in re.split(r"\s*\|\s*", l["formes_ir"] or ""):
            if f.strip():
                exact[cle(f)].add(l["lieu_id"])

    rows = []
    for forme, d in formes.items():
        r = aligner(forme, "", lieux, ecartes, index, index_ec, cles)
        lieu = None
        if r["statut"] == "aligné":
            categorie, lieu = "référentiel", r["lieu"]
        elif forme in decisions:
            categorie, lieu = "décision validée", par_id.get(decisions[forme])
        elif conc.get(norm(forme)):
            lignes = conc[norm(forme)]
            ids = {c["lieu_id"] for c in ([c for c in lignes if not c["source"]] or lignes)}
            categorie, lieu = "concordance BnR", par_id[next(iter(ids))] if len(ids) == 1 else None
        elif len(exact.get(cle(re.sub(r"[.,;\s]+$", "", forme)), ())) == 1:
            categorie = "territoires"
            lieu = par_id[next(iter(exact[cle(re.sub(r"[.,;\s]+$", "", forme))]))]
        else:
            categorie = "à traiter"
        n = ""
        if categorie == "à traiter":
            n = ("quartier" if QUARTIER.search(forme) else
                 "commune ou pays" if nature(forme) == "commune ou pays" else
                 "voie" if r["statut"] == "sans lieu" else
                 "lieu écarté" if r["statut"] == "lieu écarté" else nature(forme))
        rows.append({
            "forme": forme, "occurrences": d["occ"], "nb_ir": len(d["ir"]),
            "ir": ", ".join(f"{k}:{v}" for k, v in d["ir"].most_common(6)) + (" …" if len(d["ir"]) > 6 else ""),
            "sources_mnesys": ", ".join(f"{k}:{v}" for k, v in d["src"].most_common()),
            "categorie": categorie, "nature": n,
            "lieu": lieu["libelle"] if lieu else "", "lieu_id": lieu["lieu_id"] if lieu else "",
            "thesaurus": lieu["thesaurus"] if lieu else "",
        })
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--date", default=date.today().strftime("%Y%m%d"))
    args = p.parse_args()
    base = join("results", "ead", "indexation")
    sortie_formes = join(base, f"mnesys_geo_diagnostic_formes_{args.date}.csv")
    sortie_ir = join(base, f"mnesys_geo_diagnostic_ir_{args.date}.csv")

    formes, illisibles = lire_formes()
    rows = classer(formes)
    rows.sort(key=lambda r: (CATEGORIES.index(r["categorie"]), -r["occurrences"], r["forme"]))
    with open(sortie_formes, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(rows)

    # par IR : occurrences par catégorie
    categorie_de = {r["forme"]: r["categorie"] for r in rows}
    par_ir = defaultdict(Counter)
    for forme, d in formes.items():
        for ir, n in d["ir"].items():
            par_ir[ir][categorie_de[forme]] += n
    lignes_ir = []
    for ir, c in par_ir.items():
        tot = sum(c.values())
        ligne = {"ir": ir, "geogname": tot, "formes": sum(1 for f, d in formes.items() if ir in d["ir"])}
        ligne.update({cat: c[cat] for cat in CATEGORIES})
        ligne["taux_deja_resolu"] = f"{(tot - c['à traiter']) / tot:.1%}"
        lignes_ir.append(ligne)
    lignes_ir.sort(key=lambda r: -r["geogname"])
    with open(sortie_ir, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(lignes_ir[0]), lineterminator="\n")
        w.writeheader()
        w.writerows(lignes_ir)

    tot = sum(r["occurrences"] for r in rows)
    print(f"{len(par_ir)} IR, {tot} geogname, {len(rows)} formes → {sortie_formes}, {sortie_ir}")
    print(f"IR illisibles (sans geogname) : {len(illisibles)}")
    for cat in CATEGORIES:
        rs = [r for r in rows if r["categorie"] == cat]
        o = sum(r["occurrences"] for r in rs)
        print(f"  {cat:16s} {len(rs):5d} formes  {o:6d} occ ({o / tot:5.1%})")
    a_traiter = [r for r in rows if r["categorie"] == "à traiter"]
    print("à traiter par nature :")
    for n, rs in sorted(defaultdict(list, {k: [r for r in a_traiter if r["nature"] == k]
                                           for k in {r["nature"] for r in a_traiter}}).items(),
                        key=lambda x: -sum(r["occurrences"] for r in x[1])):
        print(f"  {n:16s} {len(rs):4d} formes  {sum(r['occurrences'] for r in rs):5d} occ")
    print("IR par occurrences à traiter :")
    for l in sorted(lignes_ir, key=lambda r: -r["à traiter"])[:8]:
        print(f"  {l['ir']:22s} {l['à traiter']:5d} à traiter sur {l['geogname']:5d} ({l['taux_deja_resolu']} résolu)")


if __name__ == "__main__":
    main()
