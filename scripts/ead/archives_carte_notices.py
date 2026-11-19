"""Notices des Archives municipales (inventaires Mnesys) par rue, quartier et site de Roubaix, pour les fiches de la
carte « Bn-r : accès par plan » (carte_leaflet.py ; 2026-11-19, demande utilisateur).

Lecture seule des IR d'origine (data/ead/mnesys/FRAM59100_*.xml). Chaque <geogname> non vide est rattaché à un lieu
par sa forme, comme dans mnesys_geo_diagnostic.py (classer) :
  - alignée par le texte sur le référentiel des lieux, ou décision validée (table de décisions
    data/geo/concordance_mnesys_lieux.csv « oui », propositions automatiques non refusées), ou concordance BnR ;
  - une forme refusée dans la table (« non ») et les formes « à traiter » sont écartées ;
  - seuls les lieux de Roubaix sont gardés : thésaurus quartiers, rues, points (les communes et pays non).
Choix de l'utilisateur (2026-11-19) : alignés + validés ; les notices des archives n'ont pas de lien (pas d'ARK) :
la fiche montre la cote et le titre.

Sortie : results/ead/indexation/archives_carte_notices.csv — thesaurus, lieu_id, libelle, ir, c_id, cote, date,
normal, titre, forme ; une ligne par lieu × notice. Les <geogname> hors de tout <c> (archdesc) sont comptés.

    python scripts/ead/archives_carte_notices.py      (env conda rbx-bnr-data)
    python scripts/ead/carte_leaflet.py
"""
import csv
import sys
from collections import Counter
from glob import glob
from os.path import basename, dirname, join, realpath

from lxml import etree

sys.path.insert(0, dirname(realpath(__file__)))
from mnesys_geo_commun import SERIES, lire_formes, lire_table  # noqa: E402
from mnesys_geo_diagnostic import classer  # noqa: E402
from geogname_concordance_lieux import REFERENTIEL, lire  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
SORTIE = join(RACINE, "results/ead/indexation/archives_carte_notices.csv")
ROUBAIX = ("quartiers", "rues", "points")
COLONNES = ["thesaurus", "lieu_id", "libelle", "ir", "c_id", "cote", "date", "normal", "titre", "forme"]


def texte(e):
    return " ".join("".join(e.itertext()).split()) if e is not None else ""


def main():
    refuses = {f for f, t in lire_table().items() if t["decision"] == "non"}
    libelle = {l["lieu_id"]: l.get("libelle_manuel", "").strip() or l["libelle"] for l in lire(REFERENTIEL)}
    lieu_de = {r["forme"]: r for r in classer(lire_formes("mnesys"))
               if r["categorie"] != "à traiter" and r["thesaurus"] in ROUBAIX and r["forme"] not in refuses}

    lignes, vues, hors_c, ecartees = [], set(), Counter(), Counter()
    for f in sorted(glob(SERIES["mnesys"]["ir"])):
        ir = basename(f)[:-4].removeprefix("FRAM59100_")
        try:
            arbre = etree.parse(f)
        except etree.XMLSyntaxError:   # esperluettes non échappées : aucun <geogname>
            continue
        for g in arbre.iter("geogname"):
            forme = texte(g)
            if not forme:
                continue
            r = lieu_de.get(forme)
            if r is None:
                ecartees[forme] += 1
                continue
            c = next(g.iterancestors("c"), None)
            if c is None:
                hors_c[ir] += 1
                continue
            cle = (r["lieu_id"], ir, c.get("id"))
            if cle in vues:
                continue
            vues.add(cle)
            did = c.find("did")
            ud = did.find("unitdate") if did is not None else None
            lignes.append({"thesaurus": r["thesaurus"], "lieu_id": r["lieu_id"],
                           "libelle": libelle.get(r["lieu_id"], r["lieu"]), "ir": ir, "c_id": c.get("id", ""),
                           "cote": texte(did.find("unitid")) if did is not None else "",
                           "date": texte(ud), "normal": (ud.get("normal") or "") if ud is not None else "",
                           "titre": texte(did.find("unittitle")) if did is not None else "", "forme": forme})

    lignes.sort(key=lambda l: (l["thesaurus"], l["libelle"], l["ir"], l["normal"] or l["date"] or "9999", l["cote"]))
    with open(SORTIE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, COLONNES, lineterminator="\n")
        w.writeheader()
        w.writerows(lignes)

    print(f"→ {SORTIE}")
    print(f"  {len(lignes)} lignes lieu × notice ; lieux : "
          f"{dict(Counter(t for t, _ in {(l['thesaurus'], l['lieu_id']) for l in lignes}))} ; "
          f"notices : {len({(l['ir'], l['c_id']) for l in lignes})} dans {len({l['ir'] for l in lignes})} IR")
    print(f"  <geogname> écartés (à traiter, refusés, hors Roubaix) : {sum(ecartees.values())} ; "
          f"hors de tout <c> : {sum(hors_c.values())} {dict(hors_c) if hors_c else ''}")
    print(f"  sans titre : {sum(1 for l in lignes if not l['titre'])} ; sans cote : {sum(1 for l in lignes if not l['cote'])}")


if __name__ == "__main__":
    main()
