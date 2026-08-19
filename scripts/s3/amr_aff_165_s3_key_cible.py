#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Construit le s3_key des 24 TIFF master AMR_AFF apportés par le versement
AMR_CADN_165-0500 (RBX_AMR_AFF_001_D_0973 à 0991) et bascule leur
conservation_statut de "À TRANSFERER APRES VALIDATION" vers "À TRANSFERER".

Ces 24 fichiers font suite exacte (sans recouvrement de numérotation, sans
collision de checksum) à la série AMR_AFF déjà publiée via source2s3='az'
(clé plate AMR/AMR_AFF/<name>, dernier numéro 0972). Même motif appliqué ici.

Backup en _old3 avant écriture (--apply).
"""
import os
import shutil
import sys
import pandas as pd

REF = "results/ref/_ref_files_20260630.csv.gz"
BAK = "results/ref/_ref_files_20260630_old3.csv.gz"

ref = pd.read_csv(REF, dtype=str, keep_default_na=False, low_memory=False)
print("ref lu:", ref.shape)

mask = (
    (ref["conservation_statut"] == "À TRANSFERER APRES VALIDATION")
    & (ref["source2s3"] == "AMR_CADN_165-0500")
    & (ref["corpus_code"] == "AMR_AFF")
)
print("lignes ciblées:", int(mask.sum()))
assert mask.sum() == 24, f"attendu 24 lignes, trouvé {mask.sum()}"
assert (ref.loc[mask, "s3_key"] == "").all(), "s3_key déjà renseignée sur des lignes ciblées"

ref.loc[mask, "s3_key"] = "AMR/AMR_AFF/" + ref.loc[mask, "name"]
ref.loc[mask, "conservation_statut"] = "À TRANSFERER"

print("\n== aperçu ==")
print(ref.loc[mask, ["name", "s3_key", "conservation_statut"]].to_string())

if "--apply" in sys.argv:
    assert not os.path.exists(BAK), f"{BAK} existe déjà"
    shutil.copy2(REF, BAK)
    print("backup:", BAK)
    ref.to_csv(REF, index=False, compression="gzip")
    print("écrit:", REF, ref.shape)
else:
    print("DRY-RUN (--apply pour écrire)")
