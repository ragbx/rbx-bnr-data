#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
MED_MS (2026-07-09) : corpus laissé de côté du backlog transfert (pollué par
248 `RBX_AMR_PR_FLO_*` mal étiquetés + 125 noms dupliqués). Sur demande,
on marque les 614 lignes « EN LIGNE - À TRANSFERER ? » pour arbitrage humain :
conservation_statut -> « EN LIGNE - À TRANSFERER ? VOIR ELISE ».
Aucune autre colonne touchée (ni s3_key, ni s3_key_cible, ni publication).

Source .gz lue et réécrite en place : results/ref/_ref_files_20260630_dedup.csv.gz
"""
import sys
import pandas as pd

GZ = "results/ref/_ref_files_20260630_dedup.csv.gz"
OLD = "EN LIGNE - À TRANSFERER ?"
NEW = "EN LIGNE - À TRANSFERER ? VOIR ELISE"

df = pd.read_csv(GZ, dtype=str, keep_default_na=False)
print("shape lue:", df.shape)

mask = (df["corpus_code"] == "MED_MS") & (df["conservation_statut"] == OLD)
print("MED_MS à marquer:", int(mask.sum()))
assert mask.sum() == 614, f"attendu 614, obtenu {int(mask.sum())}"

df.loc[mask, "conservation_statut"] = NEW

if "--apply" in sys.argv:
    df.to_csv(GZ, index=False, compression="gzip")
    print("GZ réécrite:", GZ, df.shape)
    print("MED_MS conservation:", df.loc[df["corpus_code"] == "MED_MS", "conservation_statut"].value_counts().to_dict())
else:
    print("DRY-RUN (ajouter --apply pour réécrire la .gz)")
