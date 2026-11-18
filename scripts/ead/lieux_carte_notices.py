"""Notices des rues, quartiers et sites indexés dans les IR, pour les fiches de la carte « Bn-r : accès par plan »
(carte_leaflet.py ; 2026-11-16, même principe que les sites validés de sites_carte.py : liste des notices).

Lecture seule des IR réécrits (results/ead/ead_cor/bnr2mnesysgeo/) : chaque <geogname> des thésaurus de Roubaix
(quartiers, voies, sites) qui a un @coord — les mêmes que geogname_ir2kml.py place sur la carte — est rattaché à la
notice (<c>) qui le contient ; titre, date et ARK publication:previous sont pris dans les IR tels qu'en ligne
(results/ead/ead_cor/bnr2mnesys/, comme sites_carte.py). Les <geogname> placés hors de tout <c> sont comptés.

Sortie : results/ead/indexation/lieux_carte_notices.csv — thesaurus (quartiers, rues, points), libelle, ir, c_id,
date, titre, ark ; une ligne par lieu × notice, notices triées par date puis titre.

    python scripts/ead/lieux_carte_notices.py      (env conda rbx-bnr-data)
    python scripts/ead/carte_leaflet.py
"""
import csv
import re
import sys
from collections import Counter, defaultdict
from glob import glob
from os.path import dirname, join, realpath

from lxml import etree

sys.path.insert(0, dirname(realpath(__file__)))
from geogname_ir2kml import IR_DIR, THESAURUS, texte  # noqa: E402
from sites_carte import notices_ir  # noqa: E402

RACINE = dirname(dirname(dirname(realpath(__file__))))
SORTIE = join(RACINE, "results/ead/indexation/lieux_carte_notices.csv")


def main():
    ir = notices_ir()
    liens, hors_c = defaultdict(dict), Counter()
    for f in sorted(glob(join(IR_DIR, "*.xml"))):
        ir_court = re.sub(r"^FR\d+_", "", f.replace("\\", "/").rsplit("/", 1)[1][:-4])
        for g in etree.parse(f).iter("{*}geogname"):
            source = g.get("source", "").replace("--SLASH--", "/").split("/")[-1]
            if source not in THESAURUS or not g.get("coord"):
                continue
            thesaurus = THESAURUS[source][0]
            c = next((a for a in g.iterancestors() if etree.QName(a).localname == "c"), None)
            if c is None:
                hors_c[thesaurus] += 1
                continue
            cle = (ir_court, c.get("id"))
            if cle not in liens[(thesaurus, texte(g))]:
                date, titre, ark, normal = ir.get(cle, ("", "", "", ""))
                liens[(thesaurus, texte(g))][cle] = {"date": date, "titre": titre, "ark": ark, "_tri": normal or date}

    lignes = []
    for (thesaurus, libelle), notices in sorted(liens.items()):
        for (ir_court, c_id), n in sorted(notices.items(), key=lambda x: (x[1]["_tri"] or "9999", x[1]["titre"])):
            lignes.append({"thesaurus": thesaurus, "libelle": libelle, "ir": ir_court, "c_id": c_id,
                           "date": n["date"], "titre": n["titre"], "ark": n["ark"]})
    with open(SORTIE, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, ["thesaurus", "libelle", "ir", "c_id", "date", "titre", "ark"])
        w.writeheader()
        w.writerows(lignes)

    print(f"→ {SORTIE} : {len(lignes)} liens lieu × notice")
    for t in sorted({l["thesaurus"] for l in lignes}):
        sel = [l for l in lignes if l["thesaurus"] == t]
        print(f"  {t} : {len({l['libelle'] for l in sel})} lieux, {len(sel)} liens, {sum(1 for l in sel if l['ark'])} avec ARK"
              f", {sum(1 for l in sel if not l['titre'])} sans titre")
    if hors_c:
        print(f"  <geogname> hors de tout <c> (non rattachés) : {dict(hors_c)}")


if __name__ == "__main__":
    main()
