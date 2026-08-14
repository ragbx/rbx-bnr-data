#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Remplace le statut CORBEILLE (DIFFUSION) par À SUPPRIMER (DIFFUSION) dans le
ref 20260630 (demande utilisateur 2026-07-11) — la corbeille rejoint la famille
de purge, il n'y a plus de famille corbeille.
Backup en _old7 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old7.csv.gz"
SRC, DST = "CORBEILLE (DIFFUSION)", "À SUPPRIMER (DIFFUSION)"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)
chg = ref["conservation_statut"] == SRC
avant_dst = int((ref["conservation_statut"] == DST).sum())
print(f"{SRC!r}: {int(chg.sum())} | {DST!r} avant: {avant_dst}")
ref.loc[chg, "conservation_statut"] = DST
apres_dst = int((ref["conservation_statut"] == DST).sum())
assert apres_dst == avant_dst + int(chg.sum())
assert not (ref["conservation_statut"] == SRC).any()
print(f"{DST!r} après: {apres_dst} | statuts distincts:", ref["conservation_statut"].nunique())

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
