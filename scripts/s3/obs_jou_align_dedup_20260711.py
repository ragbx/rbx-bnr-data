#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Aligne les 419 OBS_JOU « EN LIGNE - À TRANSFERER ? » restants du ref sur les
décisions prises le 2026-07-09 dans le fichier _dedup (supprimé depuis, absent
de git) — groupes recalculés avec les règles déterministes documentées dans
obs_jou_s3_key_cible.py :

  A = 36  doublons de CONTENU (même checksum_md5 qu'un tif OBS_JOU déjà versé,
          copie 3 chiffres d'un nom 4 chiffres) -> « À SUPPRIMER (DOUBLON S3-AZ) »
          (équivalent harmonisé du « À SUPPRIMER » nu du 09/07) + publication « jamais »
  R = 383 masters uniques -> s3_key plate OBS/OBS_JOU/<name>
          + « À TRANSFERER APRES VALIDATION », publication inchangée

(B = 134 appariés jpg déjà traités le 2026-07-11 par obs_jou_masters_appariement.)
Backup en _old9 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old9.csv.gz"
AUDIT = "results/ref/_obs_jou_align_dedup_audit_20260711.csv"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)

oj = ref["corpus_code"] == "OBS_JOU"
el = oj & (ref["conservation_statut"] == "EN LIGNE - À TRANSFERER ?")
assert int(el.sum()) == 419, f"EN LIGNE restants = {int(el.sum())}"
assert (ref.loc[el, "extension"].str.lower() == ".tif").all()
assert (ref.loc[el, "s3_key"] == "").all()

# A : contenu déjà versé (checksum identique à un OBS_JOU TRANSFERT_S3_OK .tif)
versed = oj & (ref["conservation_statut"] == "TRANSFERT_S3_OK") \
            & ref["s3_key"].str.lower().str.endswith(".tif")
vck = set(ref.loc[versed, "checksum_md5"])
a = el & ref["checksum_md5"].isin(vck)
r = el & ~a
print("A (doublons de contenu):", int(a.sum()), "| R (masters uniques):", int(r.sum()))
assert int(a.sum()) == 36 and int(r.sum()) == 383, "partition != 36/383 (décision 09/07)"

# R : clé plate, aucune collision
newk = "OBS/OBS_JOU/" + ref.loc[r, "name"]
exist = set(ref.loc[ref["s3_key"] != "", "s3_key"])
assert newk.is_unique and not (set(newk) & exist), "collision de clé"

ref.loc[a, "conservation_statut"] = "À SUPPRIMER (DOUBLON S3-AZ)"
ref.loc[a, "publication_statut"] = "jamais"
ref.loc[r, "s3_key"] = newk
ref.loc[r, "conservation_statut"] = "À TRANSFERER APRES VALIDATION"
assert (ref.loc[r, "s3_uploaded"] == "False").all()

print("statuts OBS_JOU après:", ref.loc[oj, "conservation_statut"].value_counts().to_dict())
audit = ref.loc[a | r, ["name", "path", "checksum_md5", "conservation_statut",
                        "publication_statut", "s3_key"]]
audit.to_csv(AUDIT, index=False)
print("audit:", AUDIT, audit.shape)

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
