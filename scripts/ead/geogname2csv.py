"""
Extrait tous les <geogname> des IR dans un CSV unique, une ligne par élément.

Colonnes : eadid (identifiant de l'IR), c_id (@id du <c> ancêtre le plus
proche, vide si le geogname est indexé au niveau de l'archdesc), geogname
(texte), source (@source), coord (@coord). Les attributs absents donnent une
cellule vide.

Sortie : results/ead/indexation/rbx-bnr_geogname.csv

À lancer depuis la racine du dépôt.
"""
import csv
from glob import glob
from os.path import join

from lxml import etree

EAD_FOLDER = join("results", "ead", "ead_cor", "bnr2mnesys")
CSV_SORTIE = join("results", "ead", "indexation", "rbx-bnr_geogname.csv")

lignes = []
for path in sorted(glob(join(EAD_FOLDER, "*.xml"))):
    root = etree.parse(path).getroot()
    eadid = root.xpath("//eadheader/eadid/text()")
    eadid = eadid[0].strip() if eadid else ""
    for geogname in root.iter("geogname"):
        composant = geogname.xpath("ancestor::c[1]")
        lignes.append(
            {
                "eadid": eadid,
                "c_id": composant[0].get("id", "") if composant else "",
                "geogname": "".join(geogname.itertext()).strip(),
                "source": geogname.get("source", ""),
                "coord": geogname.get("coord", ""),
            }
        )

with open(CSV_SORTIE, "w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=["eadid", "c_id", "geogname", "source", "coord"])
    writer.writeheader()
    writer.writerows(lignes)

print(f"{len(lignes)} geogname → {CSV_SORTIE}")
