#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Mise en cohérence v2 des conservation_statut du ref 20260630 (35 colonnes) —
familles À SUPPRIMER et CORBEILLE, mapping validé par l'utilisateur 2026-07-11 :
  - CORBEILLE (étiquetage ancien, AMR_EC) -> CORBEILLE (DIFFUSION)
  - À SUPPRIMER (remplacement par Verif / Tampon) -> À SUPPRIMER (REMPLACEMENT)
  - À SUPPRIMER (Tests) -> À SUPPRIMER (TESTS)
  - À SUPPRIMER (nu, 686 tif MED_PAR) -> À SUPPRIMER (MED_PAR)
Remplacement par valeur EXACTE (les motifs existants ne sont pas touchés).
Backup en _old6 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old6.csv.gz"

MAP = {
    "CORBEILLE": "CORBEILLE (DIFFUSION)",
    "À SUPPRIMER (remplacement par Verif / Tampon)": "À SUPPRIMER (REMPLACEMENT)",
    "À SUPPRIMER (Tests)": "À SUPPRIMER (TESTS)",
    "À SUPPRIMER": "À SUPPRIMER (MED_PAR)",
}

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)
assert "s3_key_cible" not in ref.columns, "ref attendu à 35 colonnes"

chg = ref["conservation_statut"].isin(MAP)
print("lignes à renommer:", int(chg.sum()))
print("avant:", ref.loc[chg, "conservation_statut"].value_counts().to_dict())
# garde-fou : le nu À SUPPRIMER ne concerne que MED_PAR
assert (ref.loc[ref["conservation_statut"] == "À SUPPRIMER", "corpus_code"] == "MED_PAR").all()
ref["conservation_statut"] = ref["conservation_statut"].replace(MAP)
assert not ref["conservation_statut"].isin(["CORBEILLE", "À SUPPRIMER",
    "À SUPPRIMER (Tests)", "À SUPPRIMER (remplacement par Verif / Tampon)"]).any()

print("\n== conservation_statut après ==")
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
