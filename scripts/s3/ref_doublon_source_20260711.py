#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Requalifie la « zone grise » de l'audit doublons du 2026-07-11 (cf. mémoire
audit-doublons-20260711) : les À SUPPRIMER (DOUBLON*) d'AMR_EC et AMR_GUE sans
jumeau de contenu (checksum_md5) NI de nom conservé — doublons *de source*
(anciennes campagnes de numérisation), invérifiables mécaniquement — reçoivent
le libellé d'alerte distinct :

    À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)

But : ne pas les confondre avec les doublons sûrs au moment de la purge
physique ; un contrôle documentaire par échantillon doit précéder.
Backup en _old<n> (premier libre) avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
NEW = "À SUPPRIMER (DOUBLON SOURCE - À VERIFIER)"
KEPT = ("TRANSFERT_S3_OK", "À TRANSFERER APRES VALIDATION")

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)

kept = ref["conservation_statut"].isin(KEPT)
dbl = ref["conservation_statut"].str.startswith("À SUPPRIMER (DOUBLON")
kept_ck = set(ref.loc[kept, "checksum_md5"])
kept_nm = set(ref.loc[kept, "name"].str.lower())
orphan = dbl & ~ref["checksum_md5"].isin(kept_ck) & ~ref["name"].str.lower().isin(kept_nm)
sel = orphan & ref["corpus_code"].isin(["AMR_EC", "AMR_GUE"])
print("sélection:", ref.loc[sel, "corpus_code"].value_counts().to_dict(),
      "| statuts sources:", ref.loc[sel, "conservation_statut"].value_counts().to_dict())
assert int((sel & (ref["corpus_code"] == "AMR_EC")).sum()) == 11972, "AMR_EC != 11972"
assert int((sel & (ref["corpus_code"] == "AMR_GUE")).sum()) == 682, "AMR_GUE != 682"

ref.loc[sel, "conservation_statut"] = NEW
print("statuts distincts après:", ref["conservation_statut"].nunique())
print(f"« {NEW} » :", int((ref["conservation_statut"] == NEW).sum()))

if "--apply" in sys.argv:
    n = 3
    while os.path.exists(f"results/ref/_ref_files_20260630_old{n}.csv.gz"):
        n += 1
    bak = f"results/ref/_ref_files_20260630_old{n}.csv.gz"
    shutil.copy2(REF, bak)
    print("backup:", bak)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
