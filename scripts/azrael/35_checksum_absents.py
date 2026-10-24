"""Calcul des checksum_md5 des fichiers az absents du ref (lit le disque).

Variante de 30_compute_checksum.py appliquée à la liste des fichiers présents sur
az mais absents du ref précédent (appariement name+path, hors BNR_VERIF/CONVERSIONS).

Entrée  : results/ref/tmp/az_absents_ref_{AZ_DATE}.csv.gz
Sortie  : results/ref/tmp/az_absents_ref_{AZ_DATE}_cs.csv.gz (+ checksum_md5)

Usage : python scripts/azrael/35_checksum_absents.py [root_path]
    root_path : racine des fichiers azrael (défaut "../bnr").
"""

import sys
from os.path import join

import pandas as pd
from bnr.azrael import Azrael2list

AZ_DATE = "20261023"

# root_path = '\\\\ntrbx.local\mediatheque\BNR'
root_path = sys.argv[1] if len(sys.argv) > 1 else "../bnr"
print(root_path)

df = pd.read_csv(join("results", "ref", "tmp", f"az_absents_ref_{AZ_DATE}.csv.gz"))
df["checksum_md5"] = pd.NA
df["uuid"] = pd.NA

az2list = Azrael2list(root_path=root_path, code_disk=None, az=df)
az2list.get_all_checksum(new_checksum_file_name=f"absents_checksum_{AZ_DATE}")
az2list.az = az2list.az.drop(columns=["filename"])
print("checksum manquants :", az2list.az["checksum_md5"].isna().sum(), "/", len(az2list.az))
az2list.save_list(filename=join("results", "ref", "tmp", f"az_absents_ref_{AZ_DATE}_cs.csv.gz"))
