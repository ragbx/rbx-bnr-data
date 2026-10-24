"""Nouveau ref 20261023 — étape 1 : fichiers az absents de l'ancien ref.

Appariement az <-> ancien ref sur (name, path), puis comparaison de la taille :
    - present           : même name, path et size -> la ligne de l'ancien ref est reprise
    - taille_differente : même name et path, size différente -> à recalculer
    - nouveau           : absent de l'ancien ref -> ligne à créer

Exclusions côté az : .db / .lnk (comme le pipeline) et BNR_VERIF/CONVERSIONS.
Le ref écrit les chemins sans '/' initial, az avec : on retire le '/' côté az.

Entrées : results/ref/_ref_files_{OLD}.csv.gz, data/az/bnr_azrael_{NEW}.csv.gz
Sorties : results/ref/tmp/az_motif_{NEW}.csv.gz        (tous les fichiers az + motif)
          results/ref/tmp/az_absents_ref_{NEW}.csv.gz  (nouveau + taille_differente,
                                                        entrée de 35_checksum_absents.py)
"""

from os.path import join

import pandas as pd

OLD_REF_DATE = "20260630"
NEW_REF_DATE = "20261023"
TMP_DIR = join("results", "ref", "tmp")

ref = pd.read_csv(join("results", "ref", f"_ref_files_{OLD_REF_DATE}.csv.gz"), low_memory=False)
az = pd.read_csv(join("data", "az", f"bnr_azrael_{NEW_REF_DATE}.csv.gz"))
az = az[~az["name"].str[-3:].isin([".db", "lnk"])].copy()
az["path"] = az["path"].str.lstrip("/")
az = az[~(az["path"] + "/").str.startswith("BNR_VERIF/CONVERSIONS/")]

cles = ["name", "path"]
ref_k = ref[cles + ["size"]].drop_duplicates(cles).rename(columns={"size": "ref_size"})
m = az.merge(ref_k, on=cles, how="left", indicator=True)

m["motif"] = "nouveau"
m.loc[(m["_merge"] == "both") & (m["size"] != m["ref_size"]), "motif"] = "taille_differente"
m.loc[(m["_merge"] == "both") & (m["size"] == m["ref_size"]), "motif"] = "present"
m = m[list(az.columns) + ["motif"]]
print(m["motif"].value_counts().to_dict())

ref_az = ref[ref["source2s3"] == "az"][cles].drop_duplicates()
disparus = ref_az.merge(az[cles], on=cles, how="left", indicator=True)
print("ref az disparus d'az :", (disparus["_merge"] == "left_only").sum())

m.to_csv(join(TMP_DIR, f"az_motif_{NEW_REF_DATE}.csv.gz"), index=False)
m[m["motif"] != "present"].to_csv(join(TMP_DIR, f"az_absents_ref_{NEW_REF_DATE}.csv.gz"), index=False)
