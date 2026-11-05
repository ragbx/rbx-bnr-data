"""
Extraction brute des <geogname> des IR : une ligne par élément, valeurs
telles qu'elles figurent dans les IR, sans rapprochement avec l'état
d'origine ni rattachement à un référentiel (filaire, TOPO, BD TOPO,
concordance des renommages).

Colonnes : eadid, c_id (@id du <c> ancêtre le plus proche, vide au niveau
de l'archdesc), geogname (texte), source (@source), coord (@coord).

Sortie : results/ead/indexation/rbx-bnr_geogname_brut.csv

À lancer depuis la racine du dépôt.
"""
import csv
import sys
from os.path import dirname, join

sys.path.insert(0, dirname(__file__))
from geogname2csv import COLONNES, EAD_FOLDER, extraire  # noqa: E402

CSV_SORTIE = join("results", "ead", "indexation", "rbx-bnr_geogname_brut.csv")

if __name__ == "__main__":
    lignes = extraire(EAD_FOLDER)
    with open(CSV_SORTIE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLONNES)
        writer.writeheader()
        writer.writerows(lignes)
    print(f"{len(lignes)} geogname → {CSV_SORTIE}")
