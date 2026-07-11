#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Appariement conservation/diffusion (2026-07-11) : les 134 masters tif OBS_JOU
« EN LIGNE - À TRANSFERER ? » dont le jpg dérivé (même stem) est déjà versé
reçoivent leur s3_key plate `OBS/OBS_JOU/<name>` et passent
« À TRANSFERER APRES VALIDATION » (s3_uploaded=False inchangé).

Reprend la décision du 2026-07-09 (obs_jou_s3_key_cible.py, groupe B, appliquée
alors au seul fichier _dedup) : « verser le master tif à côté du jpg en ligne »,
règle plate OBS confirmée par les 1 240 clés réelles.
Périmètre STRICT : les 134 appariés (groupes A/R du script de 2026-07-09 non repris).
Backup en _old8 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old8.csv.gz"
AUDIT = "results/ref/_obs_jou_appariement_audit_20260711.csv"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False)
print("ref lu:", ref.shape)

oj = ref["corpus_code"] == "OBS_JOU"
el = oj & (ref["conservation_statut"] == "EN LIGNE - À TRANSFERER ?")
assert int(el.sum()) == 553, f"EN LIGNE OBS_JOU = {int(el.sum())}"
assert (ref.loc[el, "extension"].str.lower() == ".tif").all()
assert (ref.loc[el, "s3_key"] == "").all()

# jpg keyés du corpus, stem de clé (plate : OBS/OBS_JOU/<name>)
jk = ref.loc[oj & (ref["s3_key"].str.lower().str.endswith(".jpg")), "s3_key"]
jstems = set(jk.str.rsplit("/", n=1).str[-1].str.rsplit(".", n=1).str[0].str.lower())
nstem = ref.loc[el, "name"].str.rsplit(".", n=1).str[0]
b = el & nstem.reindex(ref.index).notna() & nstem.reindex(ref.index).str.lower().isin(jstems)
print("masters appariés à un jpg en ligne:", int(b.sum()))
assert int(b.sum()) == 134, f"attendu 134, obtenu {int(b.sum())}"

newk = "OBS/OBS_JOU/" + ref.loc[b, "name"]
exist = set(ref.loc[ref["s3_key"] != "", "s3_key"])
assert newk.is_unique and not (set(newk) & exist), "collision de clé"

ref.loc[b, "s3_key"] = newk
ref.loc[b, "conservation_statut"] = "À TRANSFERER APRES VALIDATION"
assert (ref.loc[b, "s3_uploaded"] == "False").all(), "s3_uploaded attendu False"
print("statuts OBS_JOU après:", ref.loc[oj, "conservation_statut"].value_counts().to_dict())
print("publication des 134 (inchangée):", ref.loc[b, "publication_statut"].value_counts().to_dict())

audit = ref.loc[b, ["name", "path", "conservation_statut", "publication_statut", "s3_key"]]
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
