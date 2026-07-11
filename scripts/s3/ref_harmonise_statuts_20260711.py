#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Harmonise les conservation_statut du ref 20260630 (37 colonnes, post-fusion
fins du 2026-07-11) — mapping validé par l'utilisateur :
  - doublons : casse unifiée, origine conservée (DOUBLON / DOUBLON AZ / DOUBLON S3-AZ)
  - famille « DDE - NE PAS GARDER » fusionnée dans À SUPPRIMER (…)
  - les 29 « À TRANSFERER » nus rejoignent « À TRANSFERER APRES VALIDATION »
  - corbeilles (2 libellés) et marqueurs à décision (EN LIGNE - À TRANSFERER ?,
    INCONNU FRAD59, S3_KEY À CONSTRUIRE, DOUBLON - À VOIR, VOIR MARIE) intouchés
Les lignes renommées sont datées du jour dans date_maj_ligne.
Backup en _old4 avant écriture (--apply).
"""
import datetime
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old4.csv.gz"
TODAY = datetime.date.today().isoformat()

MAP = {
    "À SUPPRIMER (DOUBLONS)": "À SUPPRIMER (DOUBLON)",
    "À SUPPRIMER (doublons AZ)": "À SUPPRIMER (DOUBLON AZ)",
    "À SUPPRIMER (doublons S3 - az)": "À SUPPRIMER (DOUBLON S3-AZ)",
    "DDE - NE PAS GARDER (DOUBLONS AZ)": "À SUPPRIMER (DOUBLON AZ)",
    "DDE - NE PAS GARDER (DIFFUSION)": "À SUPPRIMER (DIFFUSION)",
    "DDE - NE PAS GARDER (FILE_TYPE)": "À SUPPRIMER (FILE_TYPE)",
    "À TRANSFERER": "À TRANSFERER APRES VALIDATION",
}

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)
assert "s3_key_cible" in ref.columns and "date_maj_ligne" in ref.columns

chg = ref["conservation_statut"].isin(MAP)
print("lignes à renommer:", int(chg.sum()))
print("avant:", ref.loc[chg, "conservation_statut"].value_counts().to_dict())
ref["conservation_statut"] = ref["conservation_statut"].replace(MAP)
ref.loc[chg, "date_maj_ligne"] = TODAY
assert not ref["conservation_statut"].isin(MAP).any(), "libellé source restant"

print("\n== conservation_statut harmonisés ==")
for k, v in ref["conservation_statut"].value_counts().items():
    print(f"  {v:>9}  {k!r}")

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
